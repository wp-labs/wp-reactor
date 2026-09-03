use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use orion_error::conversion::SourceRawErr;
use wf_data::time::parse_json_timestamp_nanos;

use crate::error::{RuntimeReason, RuntimeResult};
use orion_error::conversion::ToStructError;

pub(crate) fn build_record_batch_from_json(
    schema: &SchemaRef,
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> RuntimeResult<RecordBatch> {
    let mut builders: Vec<ColumnBuilder> = schema
        .fields()
        .iter()
        .map(|f| ColumnBuilder::new(f.data_type(), rows.len()))
        .collect::<RuntimeResult<Vec<_>>>()?;
    for row in rows {
        for (idx, field) in schema.fields().iter().enumerate() {
            builders[idx].push(row.get(field.name()))?;
        }
    }
    let columns: Vec<ArrayRef> = builders.into_iter().map(ColumnBuilder::finish).collect();
    RecordBatch::try_new(schema.clone(), columns).source_raw_err(
        RuntimeReason::data_error(),
        "build file source record batch",
    )
}

#[derive(::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Runtime", module = "Runtime.Receiver")]
enum ColumnBuilder {
    Utf8(Vec<Option<String>>),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    TimeNanos(Vec<Option<i64>>),
}

impl ColumnBuilder {
    fn new(data_type: &DataType, cap: usize) -> RuntimeResult<Self> {
        Ok(match data_type {
            DataType::Utf8 => Self::Utf8(Vec::with_capacity(cap)),
            DataType::Int64 => Self::Int64(Vec::with_capacity(cap)),
            DataType::Float64 => Self::Float64(Vec::with_capacity(cap)),
            DataType::Boolean => Self::Bool(Vec::with_capacity(cap)),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                Self::TimeNanos(Vec::with_capacity(cap))
            }
            _ => {
                return RuntimeReason::data_error()
                    .to_err()
                    .with_detail(format!("unsupported file-source field type: {data_type:?}"))
                    .err();
            }
        })
    }

    fn push(&mut self, value: Option<&serde_json::Value>) -> RuntimeResult<()> {
        match self {
            Self::Utf8(col) => col.push(parse_utf8(value)),
            Self::Int64(col) => col.push(parse_i64(value)),
            Self::Float64(col) => col.push(parse_f64(value)),
            Self::Bool(col) => col.push(parse_bool(value)),
            Self::TimeNanos(col) => col.push(value.and_then(parse_json_timestamp_nanos)),
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Utf8(col) => Arc::new(StringArray::from(col)),
            Self::Int64(col) => Arc::new(Int64Array::from(col)),
            Self::Float64(col) => Arc::new(Float64Array::from(col)),
            Self::Bool(col) => Arc::new(BooleanArray::from(col)),
            Self::TimeNanos(col) => Arc::new(TimestampNanosecondArray::from(col)),
        }
    }
}

fn parse_utf8(v: Option<&serde_json::Value>) -> Option<String> {
    let v = v?;
    match v {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        _ => Some(v.to_string()),
    }
}

fn parse_i64(v: Option<&serde_json::Value>) -> Option<i64> {
    let v = v?;
    match v {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn parse_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    let v = v?;
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_bool(v: Option<&serde_json::Value>) -> Option<bool> {
    let v = v?;
    match v {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}
