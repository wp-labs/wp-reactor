use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use wf_engine::match_engine::{
    WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY, WFL_FIELD_TYPE_OBJECT,
    wfl_structured_field_kind,
};
use wf_lang::{BaseType, FieldType, WindowSchema};

use crate::error::{RuntimeReason, RuntimeResult};
use orion_error::conversion::ToStructError;

pub(super) fn validate_batch_schema_for_stream(
    schemas: &[WindowSchema],
    stream_name: &str,
    batch_schema: &Schema,
) -> RuntimeResult<()> {
    let expected = resolve_stream_schema(schemas, stream_name)?;
    if !schemas_are_compatible_for_stream(expected.as_ref(), batch_schema) {
        return RuntimeReason::data_error()
            .to_err()
            .with_detail(format!(
                "arrow source schema mismatch for stream {:?}",
                stream_name
            ))
            .err();
    }
    Ok(())
}

pub(crate) fn schemas_are_compatible_for_stream(expected: &Schema, actual: &Schema) -> bool {
    if expected == actual {
        return true;
    }
    if expected.fields().len() != actual.fields().len() {
        return false;
    }
    expected
        .fields()
        .iter()
        .zip(actual.fields())
        .all(|(expected, actual)| fields_are_compatible_for_stream(expected, actual))
}

fn fields_are_compatible_for_stream(expected: &Field, actual: &Field) -> bool {
    if expected.name() != actual.name() {
        return false;
    }
    match wfl_structured_field_kind(expected) {
        Some(expected_kind) => structured_field_is_compatible(expected_kind, actual),
        _ => expected == actual,
    }
}

fn structured_field_is_compatible(expected_kind: &str, actual: &Field) -> bool {
    match actual.data_type() {
        DataType::Utf8 => match wfl_structured_field_kind(actual) {
            Some(actual_kind) => actual_kind == expected_kind,
            None => true,
        },
        DataType::Struct(_) => expected_kind == WFL_FIELD_TYPE_OBJECT,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            expected_kind == WFL_FIELD_TYPE_ARRAY
        }
        _ => false,
    }
}

pub(crate) fn resolve_stream_schema(
    schemas: &[WindowSchema],
    stream_name: &str,
) -> RuntimeResult<SchemaRef> {
    maybe_resolve_stream_schema(schemas, stream_name)?.ok_or_else(|| {
        RuntimeReason::data_error()
            .to_err()
            .with_detail(format!("no schema subscribed for stream {:?}", stream_name))
    })
}

pub(crate) fn maybe_resolve_stream_schema(
    schemas: &[WindowSchema],
    stream_name: &str,
) -> RuntimeResult<Option<SchemaRef>> {
    let mut schema: Option<SchemaRef> = None;
    for ws in schemas {
        if !ws.streams.iter().any(|s| s == stream_name) {
            continue;
        }
        let candidate = window_schema_to_arrow(ws)?;
        if let Some(existing) = &schema {
            if existing.as_ref() != candidate.as_ref() {
                return RuntimeReason::data_error()
                    .to_err()
                    .with_detail(format!(
                        "stream {:?} maps to inconsistent schemas (window {:?})",
                        stream_name, ws.name
                    ))
                    .err();
            }
        } else {
            schema = Some(candidate);
        }
    }
    Ok(schema)
}

pub(crate) fn window_schema_to_arrow(ws: &WindowSchema) -> RuntimeResult<SchemaRef> {
    let mut fields = Vec::with_capacity(ws.fields.len());
    for field in &ws.fields {
        fields.push(field_to_arrow(&field.name, &field.field_type));
    }
    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn field_to_arrow(name: &str, field_type: &FieldType) -> Field {
    let field = Field::new(name, field_type_to_arrow(field_type), true);
    match structured_field_metadata_value(field_type) {
        Some(value) => field.with_metadata(HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            value.to_string(),
        )])),
        None => field,
    }
}

pub(crate) fn field_type_to_arrow(ft: &FieldType) -> DataType {
    match ft {
        FieldType::Base(base) => base_type_to_arrow(base),
        FieldType::ArrayAny | FieldType::Array(_) | FieldType::Object => DataType::Utf8,
    }
}

fn structured_field_metadata_value(ft: &FieldType) -> Option<&'static str> {
    match ft {
        FieldType::Object => Some(WFL_FIELD_TYPE_OBJECT),
        FieldType::ArrayAny | FieldType::Array(_) => Some(WFL_FIELD_TYPE_ARRAY),
        FieldType::Base(_) => None,
    }
}

fn base_type_to_arrow(base: &BaseType) -> DataType {
    match base {
        BaseType::Chars | BaseType::Ip | BaseType::Hex => DataType::Utf8,
        BaseType::Digit => DataType::Int64,
        BaseType::Float => DataType::Float64,
        BaseType::Bool => DataType::Boolean,
        BaseType::Time => DataType::Timestamp(TimeUnit::Nanosecond, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wf_lang::FieldDef;

    fn field_def(name: &str, field_type: FieldType) -> FieldDef {
        FieldDef {
            name: name.to_string(),
            field_type,
        }
    }

    fn window(name: &str, streams: &[&str], fields: Vec<FieldDef>) -> WindowSchema {
        WindowSchema {
            name: name.to_string(),
            streams: streams.iter().map(|s| s.to_string()).collect(),
            time_field: None,
            over: std::time::Duration::ZERO,
            fields,
        }
    }

    #[test]
    fn type_conversions_and_metadata() {
        assert_eq!(base_type_to_arrow(&BaseType::Digit), DataType::Int64);
        assert_eq!(
            base_type_to_arrow(&BaseType::Time),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(field_type_to_arrow(&FieldType::Object), DataType::Utf8);
        // 结构化字段带类型元数据
        let f = field_to_arrow("obj", &FieldType::Object);
        assert_eq!(
            f.metadata()
                .get(WFL_FIELD_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some(WFL_FIELD_TYPE_OBJECT)
        );
        let f = field_to_arrow("arr", &FieldType::ArrayAny);
        assert_eq!(
            f.metadata()
                .get(WFL_FIELD_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some(WFL_FIELD_TYPE_ARRAY)
        );
        // 基础字段无元数据
        assert!(
            field_to_arrow("n", &FieldType::Base(BaseType::Digit))
                .metadata()
                .is_empty()
        );
    }

    #[test]
    fn schema_compatibility_matrix() {
        let a = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        assert!(schemas_are_compatible_for_stream(&a, &a.clone()));
        // 列数不同 → 不兼容
        let b = Schema::new(Vec::<Field>::new());
        assert!(!schemas_are_compatible_for_stream(&a, &b));
        // 类型不同 → 不兼容
        let c = Schema::new(vec![Field::new("x", DataType::Utf8, true)]);
        assert!(!schemas_are_compatible_for_stream(&a, &c));
        // 结构化字段: object 期待 vs struct 列 → 兼容; vs list 列 → 不兼容
        let obj_meta = HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            WFL_FIELD_TYPE_OBJECT.to_string(),
        )]);
        let exp_obj = Schema::new(vec![
            Field::new("v", DataType::Utf8, true).with_metadata(obj_meta),
        ]);
        let actual_struct = Schema::new(vec![Field::new(
            "v",
            DataType::Struct(arrow::datatypes::Fields::from([std::sync::Arc::new(
                Field::new("k", DataType::Utf8, true),
            )])),
            true,
        )]);
        assert!(schemas_are_compatible_for_stream(&exp_obj, &actual_struct));
        let actual_list = Schema::new(vec![Field::new(
            "v",
            DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            ))),
            true,
        )]);
        assert!(!schemas_are_compatible_for_stream(&exp_obj, &actual_list));
    }

    #[test]
    fn resolve_stream_schema_matches_windows() {
        let schemas = vec![
            window(
                "w1",
                &["s1", "s2"],
                vec![field_def("a", FieldType::Base(BaseType::Digit))],
            ),
            window(
                "w2",
                &["s3"],
                vec![field_def("b", FieldType::Base(BaseType::Float))],
            ),
        ];
        let r = resolve_stream_schema(&schemas, "s1").expect("resolve s1");
        assert_eq!(r.fields().len(), 1);
        assert_eq!(r.field(0).name(), "a");
        // 未订阅流 → None; resolve 报错
        assert!(
            maybe_resolve_stream_schema(&schemas, "nope")
                .unwrap()
                .is_none()
        );
        assert!(resolve_stream_schema(&schemas, "nope").is_err());
        // 两窗订阅同流但 schema 不一致 → 报错
        let dup = vec![
            window(
                "w1",
                &["s"],
                vec![field_def("a", FieldType::Base(BaseType::Digit))],
            ),
            window(
                "w2",
                &["s"],
                vec![field_def("a", FieldType::Base(BaseType::Float))],
            ),
        ];
        assert!(resolve_stream_schema(&dup, "s").is_err());
    }
}
