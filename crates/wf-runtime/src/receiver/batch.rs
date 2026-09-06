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
    parse_str_or_number(v, |n| n.as_i64(), |s| s.parse::<i64>().ok())
}

fn parse_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    parse_str_or_number(v, |n| n.as_f64(), |s| s.parse::<f64>().ok())
}

/// 数字或字符串 → T（i64/f64 共用骨架; 其它 JSON 形态 / null → None）。
fn parse_str_or_number<T>(
    v: Option<&serde_json::Value>,
    from_number: impl FnOnce(&serde_json::Number) -> Option<T>,
    from_str: impl FnOnce(&str) -> Option<T>,
) -> Option<T> {
    match v? {
        serde_json::Value::Number(n) => from_number(n),
        serde_json::Value::String(s) => from_str(s),
        _ => None,
    }
}

fn parse_bool(v: Option<&serde_json::Value>) -> Option<bool> {
    match v? {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::String(s) => parse_bool_text(s),
        _ => None,
    }
}

/// 布尔文本（去空白/大小写后）→ bool；仅接受 true/false 与 1/0。
fn parse_bool_text(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn scalar_parse_helpers() {
        assert_eq!(
            parse_utf8(Some(&serde_json::json!("a"))),
            Some("a".to_string())
        );
        assert_eq!(
            parse_utf8(Some(&serde_json::json!(5))),
            Some("5".to_string())
        );
        assert_eq!(parse_utf8(Some(&serde_json::json!(null))), None);
        assert_eq!(parse_utf8(None), None);

        assert_eq!(parse_i64(Some(&serde_json::json!(12))), Some(12));
        assert_eq!(parse_i64(Some(&serde_json::json!("12"))), Some(12));
        assert_eq!(parse_i64(Some(&serde_json::json!(1.5))), None); // 浮点不作 i64
        assert_eq!(parse_i64(Some(&serde_json::json!("x"))), None);

        assert_eq!(parse_f64(Some(&serde_json::json!(1.5))), Some(1.5));
        assert_eq!(parse_f64(Some(&serde_json::json!("2.5"))), Some(2.5));
        assert_eq!(parse_f64(Some(&serde_json::json!("x"))), None);

        assert_eq!(parse_bool(Some(&serde_json::json!(true))), Some(true));
        assert_eq!(parse_bool(Some(&serde_json::json!(" True "))), Some(true));
        assert_eq!(parse_bool(Some(&serde_json::json!("1"))), Some(true));
        assert_eq!(parse_bool(Some(&serde_json::json!("0"))), Some(false));
        assert_eq!(parse_bool(Some(&serde_json::json!("yes"))), None);
        assert_eq!(parse_bool(Some(&serde_json::json!(7))), None);
    }

    #[test]
    fn json_to_record_batch_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("ok", DataType::Boolean, true),
        ]));
        let mut rows = Vec::new();
        for (id, name, ok) in [(1i64, "a", true), (2, "b", false)] {
            let mut m = serde_json::Map::new();
            m.insert("id".into(), serde_json::json!(id));
            m.insert("name".into(), serde_json::json!(name));
            m.insert("ok".into(), serde_json::json!(ok));
            rows.push(m);
        }
        let batch = build_record_batch_from_json(&schema, &rows).expect("build batch");
        assert_eq!(batch.num_rows(), 2);
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id col");
        assert_eq!(id.value(0), 1);
        assert_eq!(id.value(1), 2);
        let name = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name col");
        assert_eq!(name.value(0), "a");
        let ok = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("ok col");
        assert!(!ok.value(1));
    }

    #[test]
    fn column_builder_rejects_unsupported_type() {
        let err = ColumnBuilder::new(&DataType::Date32, 1);
        assert!(err.is_err());
    }

    #[test]
    fn bool_text_parsing_covers_whitespace_and_case() {
        assert_eq!(parse_bool_text("true"), Some(true));
        assert_eq!(parse_bool_text(" TRUE "), Some(true));
        assert_eq!(parse_bool_text("1"), Some(true));
        assert_eq!(parse_bool_text("False"), Some(false));
        assert_eq!(parse_bool_text("0\t"), Some(false));
        assert_eq!(parse_bool_text("yes"), None);
        assert_eq!(parse_bool_text(""), None);
        assert_eq!(
            parse_bool(Some(&serde_json::json!("  FALSE "))),
            Some(false)
        );
    }

    #[test]
    fn json_to_record_batch_handles_timestamp_float_and_missing_keys() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("score", DataType::Float64, true),
            Field::new("tag", DataType::Utf8, true),
        ]));
        let rows = vec![
            serde_json::json!({
                "ts": "2023-11-14T22:13:20Z",
                "score": 1.5,
                "tag": "a",
            }),
            // 缺键 → 该格 null；score 为数字字符串；tag 为 JSON null
            serde_json::json!({
                "score": "2.5",
                "tag": null,
            }),
        ];
        let rows: Vec<serde_json::Map<String, serde_json::Value>> = rows
            .into_iter()
            .map(|v| v.as_object().expect("object").clone())
            .collect();
        let batch = build_record_batch_from_json(&schema, &rows).expect("build batch");
        assert_eq!(batch.num_rows(), 2);
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        assert_eq!(ts.value(0), 1_700_000_000_000_000_000);
        assert!(ts.is_null(1), "缺键 → null");
        let score = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("score col");
        assert_eq!(score.value(0), 1.5);
        assert_eq!(score.value(1), 2.5);
        let tag = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("tag col");
        assert_eq!(tag.value(0), "a");
        assert!(tag.is_null(1), "JSON null → null");
    }
}
