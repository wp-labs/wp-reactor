use std::collections::HashMap;

use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

use super::match_engine::{Event, Value};

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
pub fn batch_to_events(batch: &RecordBatch) -> Vec<Event> {
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    let mut events = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let mut fields = HashMap::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            if col.is_null(row) {
                continue;
            }
            if let Some(val) = extract_value(col.as_ref(), row) {
                fields.insert(field.name().clone(), val);
            }
        }
        events.push(Event { fields });
    }
    events
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
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Schema};
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
}
