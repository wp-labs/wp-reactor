use std::collections::HashMap;

use arrow::array::{
    Array, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, LargeListArray, ListArray,
    StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;

use super::match_engine::{Event, Value};

pub const WFL_FIELD_TYPE_METADATA_KEY: &str = "wf.wfl.field_type";
pub const WFL_FIELD_TYPE_OBJECT: &str = "object";
pub const WFL_FIELD_TYPE_ARRAY: &str = "array";

pub fn is_wfl_structured_field(field: &Field) -> bool {
    wfl_structured_field_kind(field).is_some()
}

pub fn wfl_structured_field_kind(field: &Field) -> Option<&str> {
    match field
        .metadata()
        .get(WFL_FIELD_TYPE_METADATA_KEY)
        .map(String::as_str)
    {
        Some(WFL_FIELD_TYPE_OBJECT | WFL_FIELD_TYPE_ARRAY) => field
            .metadata()
            .get(WFL_FIELD_TYPE_METADATA_KEY)
            .map(String::as_str),
        _ => None,
    }
}

/// Convert an Arrow [`RecordBatch`] into a `Vec<Event>`, one per row.
///
/// Each column is mapped to an [`Event`] field by column name. Null values
/// are skipped (the field is simply absent from that row's event).
///
/// | Arrow Type           | → | CEP Value               |
/// |----------------------|---|-------------------------|
/// | Int64                | → | Value::Number(i as f64) |
/// | Float64              | → | Value::Number(f)        |
/// | Utf8                 | → | Value::Str(s)           |
/// | Boolean              | → | Value::Bool(b)          |
/// | Timestamp(Ns, _)     | → | Value::Number(ns as f64)|
/// | Struct               | → | Value::Object           |
/// | List/LargeList       | → | Value::Array            |
pub fn batch_to_events(batch: &RecordBatch) -> Vec<Event> {
    batch_to_events_with(batch, None)
}

/// Like [`batch_to_events`], but only materializes the listed field names.
///
/// Used by the window layer to avoid materializing schema fields no rule
/// reads (see wf-lang `field_usage`), the dominant peak RSS on wide windows.
/// Fields absent from the batch schema are ignored.
pub fn batch_to_events_filtered(
    batch: &RecordBatch,
    fields: &std::collections::HashSet<String>,
) -> Vec<Event> {
    batch_to_events_with(batch, Some(fields))
}

fn batch_to_events_with(
    batch: &RecordBatch,
    only_fields: Option<&std::collections::HashSet<String>>,
) -> Vec<Event> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let mut events = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let mut fields = HashMap::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            if let Some(only) = only_fields
                && !only.contains(field.name())
            {
                continue;
            }
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            if let Some(val) = extract_field_value(field, col.as_ref(), row) {
                fields.insert(field.name().clone(), val);
            }
        }
        events.push(Event { fields });
    }
    events
}

/// Convert an Arrow [`RecordBatch`] into timestamped rows for asof join.
///
/// Each row becomes `(timestamp_nanos, fields)`. The timestamp is extracted
/// as a raw `i64` from the column at `time_col_index`. Rows with a null
/// timestamp are skipped. All columns (including the time column) are
/// included in the fields map via [`extract_value`], so the time field
/// remains available in the join context.
pub fn batch_to_timestamped_rows(
    batch: &RecordBatch,
    time_col_index: usize,
) -> Vec<(i64, HashMap<String, Value>)> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let ts_col = batch.column(time_col_index);
    let Some(ts_array) = ts_col.as_any().downcast_ref::<TimestampNanosecondArray>() else {
        return Vec::new();
    };

    let mut rows = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        if ts_array.is_null(row) {
            continue;
        }
        let ts = ts_array.value(row);
        let mut fields = HashMap::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            if let Some(val) = extract_field_value(field, col.as_ref(), row) {
                fields.insert(field.name().clone(), val);
            }
        }
        rows.push((ts, fields));
    }
    rows
}

fn extract_field_value(field: &Field, col: &dyn Array, row: usize) -> Option<Value> {
    if let Some(kind) = wfl_structured_field_kind(field)
        && matches!(col.data_type(), DataType::Utf8)
    {
        let arr = col.as_any().downcast_ref::<StringArray>()?;
        return serde_json::from_str::<serde_json::Value>(arr.value(row))
            .ok()
            .and_then(|value| json_to_structured_value(kind, value));
    }
    extract_value(col, row)
}

fn extract_value(col: &dyn Array, row: usize) -> Option<Value> {
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>()?;
            Some(Value::Number(arr.value(row) as f64))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>()?;
            Some(Value::Number(arr.value(row)))
        }
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>()?;
            Some(Value::Str(arr.value(row).to_string()))
        }
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>()?;
            Some(Value::Bool(arr.value(row)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>()?;
            Some(Value::Number(arr.value(row) as f64))
        }
        DataType::Struct(_) => {
            let arr = col.as_any().downcast_ref::<StructArray>()?;
            let mut fields = HashMap::new();
            for (field, child) in arr.fields().iter().zip(arr.columns()) {
                if child.is_null(row) {
                    continue;
                }
                if let Some(value) = extract_value(child.as_ref(), row) {
                    fields.insert(field.name().clone(), value);
                }
            }
            Some(Value::Object(fields))
        }
        DataType::List(_) => {
            let arr = col.as_any().downcast_ref::<ListArray>()?;
            Some(Value::Array(extract_list_values(arr.value(row).as_ref())))
        }
        DataType::LargeList(_) => {
            let arr = col.as_any().downcast_ref::<LargeListArray>()?;
            Some(Value::Array(extract_list_values(arr.value(row).as_ref())))
        }
        DataType::FixedSizeList(_, _) => {
            let arr = col.as_any().downcast_ref::<FixedSizeListArray>()?;
            Some(Value::Array(extract_list_values(arr.value(row).as_ref())))
        }
        _ => None,
    }
}

fn json_to_value(value: serde_json::Value) -> Option<Value> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(v) => Some(Value::Bool(v)),
        serde_json::Value::Number(v) => v.as_f64().map(Value::Number),
        serde_json::Value::String(v) => Some(Value::Str(v)),
        serde_json::Value::Array(values) => Some(Value::Array(
            values.into_iter().filter_map(json_to_value).collect(),
        )),
        serde_json::Value::Object(fields) => Some(Value::Object(
            fields
                .into_iter()
                .filter_map(|(key, value)| json_to_value(value).map(|value| (key, value)))
                .collect(),
        )),
    }
}

fn json_to_structured_value(kind: &str, value: serde_json::Value) -> Option<Value> {
    match (kind, value) {
        (WFL_FIELD_TYPE_OBJECT, serde_json::Value::Object(fields)) => {
            json_to_value(serde_json::Value::Object(fields))
        }
        (WFL_FIELD_TYPE_ARRAY, serde_json::Value::Array(values)) => {
            json_to_value(serde_json::Value::Array(values))
        }
        _ => None,
    }
}

fn extract_list_values(values: &dyn Array) -> Vec<Value> {
    let mut out = Vec::with_capacity(values.len());
    for idx in 0..values.len() {
        if values.is_null(idx) {
            continue;
        }
        if let Some(value) = extract_value(values, idx) {
            out.push(value);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Int64Type, Schema};
    use std::sync::Arc;

    fn make_schema(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_batch_to_events_basic() {
        let schema = make_schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42, 99])) as ArrayRef,
                Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
            ],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert_eq!(events.len(), 2);

        assert_eq!(events[0].fields["id"], Value::Number(42.0));
        assert_eq!(events[0].fields["name"], Value::Str("alice".to_string()));
        assert_eq!(events[0].fields["active"], Value::Bool(true));

        assert_eq!(events[1].fields["id"], Value::Number(99.0));
        assert_eq!(events[1].fields["name"], Value::Str("bob".to_string()));
        assert_eq!(events[1].fields["active"], Value::Bool(false));
    }

    #[test]
    fn test_batch_to_events_timestamp() {
        let schema = make_schema(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]);
        let nanos: i64 = 1_700_000_000_000_000_000;
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![nanos])) as ArrayRef],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].fields["ts"], Value::Number(nanos as f64));
    }

    #[test]
    fn test_batch_to_events_nulls() {
        let schema = make_schema(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![None, Some("bob")])) as ArrayRef,
            ],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert_eq!(events.len(), 2);

        // Row 0: id=1, name is null (skipped)
        assert_eq!(events[0].fields["id"], Value::Number(1.0));
        assert!(!events[0].fields.contains_key("name"));

        // Row 1: id is null (skipped), name="bob"
        assert!(!events[1].fields.contains_key("id"));
        assert_eq!(events[1].fields["name"], Value::Str("bob".to_string()));
    }

    #[test]
    fn test_batch_to_events_empty() {
        let schema = make_schema(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![] as Vec<i64>)) as ArrayRef],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert!(events.is_empty());
    }

    #[test]
    fn test_batch_to_events_float64() {
        let schema = make_schema(vec![Field::new("score", DataType::Float64, false)]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![3.21, 9.87])) as ArrayRef],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].fields["score"], Value::Number(3.21));
        assert_eq!(events[1].fields["score"], Value::Number(9.87));
    }

    #[test]
    fn test_batch_to_events_struct_and_list() {
        let tags =
            ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(10), Some(20)])]);
        let detection = StructArray::from(vec![(
            Arc::new(Field::new("severity", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![10])) as ArrayRef,
        )]);
        let extension = StructArray::from(vec![
            (
                Arc::new(Field::new(
                    "detection",
                    detection.data_type().clone(),
                    false,
                )),
                Arc::new(detection) as ArrayRef,
            ),
            (
                Arc::new(Field::new("tags", tags.data_type().clone(), true)),
                Arc::new(tags) as ArrayRef,
            ),
            (
                Arc::new(Field::new("ignored", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ]);
        let schema = make_schema(vec![Field::new(
            "extension",
            extension.data_type().clone(),
            false,
        )]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(extension) as ArrayRef]).unwrap();

        let events = batch_to_events(&batch);
        let Value::Object(extension) = &events[0].fields["extension"] else {
            panic!("expected extension object");
        };
        let Some(Value::Object(detection)) = extension.get("detection") else {
            panic!("expected nested detection object, got {extension:?}");
        };
        assert_eq!(detection.get("severity"), Some(&Value::Number(10.0)));
        assert_eq!(
            extension.get("tags"),
            Some(&Value::Array(vec![
                Value::Number(10.0),
                Value::Number(20.0)
            ]))
        );
        assert!(!extension.contains_key("ignored"));
    }

    #[test]
    fn test_batch_to_events_parses_structured_utf8_json_only_with_metadata() {
        let structured_field = Field::new("extension", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_OBJECT.to_string(),
            )]),
        );
        let schema = make_schema(vec![
            structured_field,
            Field::new("plain", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![r#"{"severity":10,"tags":["ssh"]}"#])) as ArrayRef,
                Arc::new(StringArray::from(vec![r#"{"severity":10}"#])) as ArrayRef,
            ],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        let Value::Object(extension) = &events[0].fields["extension"] else {
            panic!("expected extension object");
        };
        assert_eq!(extension.get("severity"), Some(&Value::Number(10.0)));
        assert_eq!(
            extension.get("tags"),
            Some(&Value::Array(vec![Value::Str("ssh".to_string())]))
        );
        assert_eq!(
            events[0].fields["plain"],
            Value::Str(r#"{"severity":10}"#.to_string())
        );
    }

    #[test]
    fn test_batch_to_events_parses_structured_array_utf8_json_with_metadata() {
        let structured_field = Field::new("ports", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_ARRAY.to_string(),
            )]),
        );
        let schema = make_schema(vec![
            structured_field,
            Field::new("plain", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![r#"[22,2222]"#])) as ArrayRef,
                Arc::new(StringArray::from(vec![r#"[22,2222]"#])) as ArrayRef,
            ],
        )
        .unwrap();

        let events = batch_to_events(&batch);
        assert_eq!(
            events[0].fields["ports"],
            Value::Array(vec![Value::Number(22.0), Value::Number(2222.0)])
        );
        assert_eq!(
            events[0].fields["plain"],
            Value::Str(r#"[22,2222]"#.to_string())
        );
    }
}
