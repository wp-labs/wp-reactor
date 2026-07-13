use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use wf_lang::{BaseType, FieldType, WindowSchema};

use crate::error::{RuntimeReason, RuntimeResult};
use orion_error::conversion::ToStructError;

pub(super) fn validate_batch_schema_for_stream(
    schemas: &[WindowSchema],
    stream_name: &str,
    batch_schema: &Schema,
) -> RuntimeResult<()> {
    let expected = resolve_stream_schema(schemas, stream_name)?;
    if expected.as_ref() != batch_schema {
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

pub(crate) fn resolve_stream_schema(
    schemas: &[WindowSchema],
    stream_name: &str,
) -> RuntimeResult<SchemaRef> {
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
    schema.ok_or_else(|| {
        RuntimeReason::data_error()
            .to_err()
            .with_detail(format!("no schema subscribed for stream {:?}", stream_name))
    })
}

fn window_schema_to_arrow(ws: &WindowSchema) -> RuntimeResult<SchemaRef> {
    let mut fields = Vec::with_capacity(ws.fields.len());
    for field in &ws.fields {
        fields.push(Field::new(
            &field.name,
            field_type_to_arrow(&field.field_type),
            true,
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) fn field_type_to_arrow(ft: &FieldType) -> DataType {
    match ft {
        FieldType::Base(base) => base_type_to_arrow(base),
        FieldType::ArrayAny | FieldType::Array(_) | FieldType::Object => DataType::Utf8,
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
