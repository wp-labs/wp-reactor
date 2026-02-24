use std::sync::Arc;

use anyhow::Result;

use wf_config::WindowConfig;
use wf_core::window::{WindowDef, WindowParams};
use wf_lang::{BaseType, FieldType, WindowSchema};
use wp_arrow::schema::{FieldDef as WpFieldDef, WpDataType, to_arrow_schema};

/// Convert a [`WindowSchema`] (parsed from `.wfs`) together with its
/// [`WindowConfig`] (resolved from `wfusion.toml`) into a [`WindowDef`]
/// that can be fed to [`WindowRegistry::build`].
pub fn schema_to_window_def(ws: &WindowSchema, config: &WindowConfig) -> Result<WindowDef> {
    // 1. Convert wf-lang FieldDef → wp-arrow FieldDef
    let wp_fields: Vec<WpFieldDef> = ws
        .fields
        .iter()
        .map(|f| WpFieldDef::new(&f.name, base_type_to_wp(&f.field_type)))
        .collect();

    // 2. Build Arrow Schema
    let schema = to_arrow_schema(&wp_fields)
        .map_err(|e| anyhow::anyhow!("schema conversion failed for {:?}: {e}", ws.name))?;

    // 3. Find time column index
    let time_col_index = ws.time_field.as_ref().map(|tf| {
        schema
            .fields()
            .iter()
            .position(|f| f.name() == tf)
            .expect("time_field not found in schema fields")
    });

    // 4. Build WindowParams
    let params = WindowParams {
        name: ws.name.clone(),
        schema: Arc::new(schema),
        time_col_index,
        over: ws.over,
    };

    Ok(WindowDef {
        params,
        streams: ws.streams.clone(),
        config: config.clone(),
    })
}

fn base_type_to_wp(ft: &FieldType) -> WpDataType {
    match ft {
        FieldType::Base(bt) => match bt {
            BaseType::Chars => WpDataType::Chars,
            BaseType::Digit => WpDataType::Digit,
            BaseType::Float => WpDataType::Float,
            BaseType::Bool => WpDataType::Bool,
            BaseType::Time => WpDataType::Time,
            BaseType::Ip => WpDataType::Ip,
            BaseType::Hex => WpDataType::Hex,
        },
        FieldType::Array(bt) => {
            let inner = base_type_to_wp(&FieldType::Base(bt.clone()));
            WpDataType::Array(Box::new(inner))
        }
    }
}

/// Resolve each `WindowSchema` against the matching `WindowConfig` (by name).
///
/// Returns an error if a schema's window name has no corresponding config entry.
pub fn schemas_to_window_defs(
    schemas: &[WindowSchema],
    configs: &[WindowConfig],
) -> Result<Vec<WindowDef>> {
    let mut defs = Vec::with_capacity(schemas.len());
    for ws in schemas {
        let config = configs.iter().find(|c| c.name == ws.name).ok_or_else(|| {
            anyhow::anyhow!(
                "window {:?} found in .wfs schema but not in wfusion.toml [window.{}]",
                ws.name,
                ws.name
            )
        })?;
        defs.push(schema_to_window_def(ws, config)?);
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
    use wf_lang::FieldDef;

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
        let def = schema_to_window_def(&ws, &config).unwrap();

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
        let def = schema_to_window_def(&ws, &config).unwrap();
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
        let def = schema_to_window_def(&ws, &config).unwrap();
        assert_eq!(def.params.time_col_index, None);
    }
}
