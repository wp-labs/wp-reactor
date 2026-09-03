use std::sync::Arc;

use orion_error::conversion::ToStructError;

use wf_config::WindowConfig;
use wf_engine::window::{WindowDef, WindowParams};
use wf_lang::WindowSchema;
use wf_lang::field_usage::WindowFieldUsage;

use crate::error::{RuntimeReason, RuntimeResult};
use crate::receiver::schema::window_schema_to_arrow;

/// Convert a [`WindowSchema`] (parsed from `.wfs`) together with its
/// [`WindowConfig`] (resolved from `wfusion.toml`) into a [`WindowDef`]
/// that can be fed to [`WindowRegistry::build`].
///
/// `usage` supplies the per-window event field whitelist computed from all
/// compiled rules (see `wf_lang::field_usage`). Windows whose rules may scan
/// all fields keep `materialize_fields = None` (full materialization); the
/// rest materialize only the fields rules actually read.
pub(crate) fn schema_to_window_def(
    ws: &WindowSchema,
    config: &WindowConfig,
    usage: &WindowFieldUsage,
) -> RuntimeResult<WindowDef> {
    // 1. Build Arrow Schema
    let schema = window_schema_to_arrow(ws)?;

    // 2. Find time column index
    let time_col_index = ws.time_field.as_ref().map(|tf| {
        schema
            .fields()
            .iter()
            .position(|f| f.name() == tf)
            .expect("time_field not found in schema fields")
    });

    // 3. Build WindowParams
    // The window's time column must always be materialized: rule event-time
    // extraction reads it from the event (`event_time_nanos`). Excluding it
    // zeroes the watermark and breaks instance expiry / window semantics.
    let materialize_fields = usage
        .filter_for(&ws.name, ws.fields.iter().map(|f| f.name.as_str()))
        .map(|mut set| {
            if let Some(time_field) = &ws.time_field {
                set.insert(time_field.clone());
            }
            set
        })
        .map(Arc::new);
    let params = WindowParams {
        name: ws.name.clone(),
        schema: Arc::clone(&schema),
        time_col_index,
        over: ws.over,
        materialize_fields,
        defer_materialization: usage.defer_materialization.contains(&ws.name),
    };

    Ok(WindowDef {
        params,
        streams: ws.streams.clone(),
        config: config.clone(),
    })
}

/// Resolve each `WindowSchema` against the matching `WindowConfig` (by name).
///
/// Returns an error if a schema's window name has no corresponding config entry.
pub(crate) fn schemas_to_window_defs(
    schemas: &[WindowSchema],
    configs: &[WindowConfig],
    usage: &WindowFieldUsage,
) -> RuntimeResult<Vec<WindowDef>> {
    let mut defs = Vec::with_capacity(schemas.len());
    for ws in schemas {
        let Some(config) = configs.iter().find(|c| c.name == ws.name) else {
            return RuntimeReason::Bootstrap
                .to_err()
                .with_detail(format!(
                    "window {:?} found in .wfs schema but not in wfusion.toml [window.{}]",
                    ws.name, ws.name
                ))
                .err();
        };
        defs.push(schema_to_window_def(ws, config, usage)?);
    }
    Ok(defs)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};
    use std::time::Duration;
    use wf_config::{DistMode, EvictPolicy, LatePolicy};
    use wf_lang::{BaseType, FieldDef, FieldType};

    fn test_config(name: &str) -> WindowConfig {
        WindowConfig {
            name: name.into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(5).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        }
    }

    #[test]
    fn test_schema_to_window_def() {
        let ws = WindowSchema {
            name: "auth_events".to_string(),
            streams: vec!["syslog".to_string()],
            time_field: Some("ts".to_string()),
            over: Duration::from_secs(300),
            fields: vec![
                FieldDef {
                    name: "ts".to_string(),
                    field_type: FieldType::Base(BaseType::Time),
                },
                FieldDef {
                    name: "src_ip".to_string(),
                    field_type: FieldType::Base(BaseType::Ip),
                },
                FieldDef {
                    name: "count".to_string(),
                    field_type: FieldType::Base(BaseType::Digit),
                },
                FieldDef {
                    name: "success".to_string(),
                    field_type: FieldType::Base(BaseType::Bool),
                },
                FieldDef {
                    name: "score".to_string(),
                    field_type: FieldType::Base(BaseType::Float),
                },
            ],
        };

        let config = test_config("auth_events");
        let def = schema_to_window_def(&ws, &config, &WindowFieldUsage::default()).unwrap();

        assert_eq!(def.params.name, "auth_events");
        assert_eq!(def.streams, vec!["syslog"]);
        assert_eq!(def.params.over, Duration::from_secs(300));

        let schema = def.params.schema;
        assert_eq!(schema.fields().len(), 5);
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8); // Ip → Utf8
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        assert_eq!(schema.field(3).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(4).data_type(), &DataType::Float64);
    }

    #[test]
    fn structured_field_types_map_to_utf8_storage() {
        use wf_engine::match_engine::{
            WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY, WFL_FIELD_TYPE_OBJECT,
        };

        let ws = WindowSchema {
            name: "alerts".to_string(),
            streams: vec![],
            time_field: None,
            over: Duration::ZERO,
            fields: vec![
                FieldDef {
                    name: "risk_context".to_string(),
                    field_type: FieldType::Object,
                },
                FieldDef {
                    name: "tags".to_string(),
                    field_type: FieldType::ArrayAny,
                },
                FieldDef {
                    name: "ports".to_string(),
                    field_type: FieldType::Array(BaseType::Digit),
                },
            ],
        };

        let config = test_config("alerts");
        let def = schema_to_window_def(&ws, &config, &WindowFieldUsage::default()).unwrap();
        let schema = def.params.schema;

        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(
            schema.field(0).metadata().get(WFL_FIELD_TYPE_METADATA_KEY),
            Some(&WFL_FIELD_TYPE_OBJECT.to_string())
        );
        assert_eq!(
            schema.field(1).metadata().get(WFL_FIELD_TYPE_METADATA_KEY),
            Some(&WFL_FIELD_TYPE_ARRAY.to_string())
        );
        assert_eq!(
            schema.field(2).metadata().get(WFL_FIELD_TYPE_METADATA_KEY),
            Some(&WFL_FIELD_TYPE_ARRAY.to_string())
        );
    }

    #[test]
    fn test_time_col_index() {
        let ws = WindowSchema {
            name: "win".to_string(),
            streams: vec![],
            time_field: Some("event_time".to_string()),
            over: Duration::from_secs(60),
            fields: vec![
                FieldDef {
                    name: "id".to_string(),
                    field_type: FieldType::Base(BaseType::Digit),
                },
                FieldDef {
                    name: "event_time".to_string(),
                    field_type: FieldType::Base(BaseType::Time),
                },
                FieldDef {
                    name: "msg".to_string(),
                    field_type: FieldType::Base(BaseType::Chars),
                },
            ],
        };

        let config = test_config("win");
        let def = schema_to_window_def(&ws, &config, &WindowFieldUsage::default()).unwrap();
        assert_eq!(def.params.time_col_index, Some(1));
    }

    #[test]
    fn test_no_time_field() {
        let ws = WindowSchema {
            name: "static_win".to_string(),
            streams: vec![],
            time_field: None,
            over: Duration::ZERO,
            fields: vec![FieldDef {
                name: "data".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            }],
        };

        let config = test_config("static_win");
        let def = schema_to_window_def(&ws, &config, &WindowFieldUsage::default()).unwrap();
        assert_eq!(def.params.time_col_index, None);
    }
}
