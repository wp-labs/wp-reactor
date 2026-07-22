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
