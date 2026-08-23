//! Round-2 coverage-fill tests for the event bridge (`event_bridge.rs`): the
//! columnar/row conversion branches the earlier suites leave cold — unsupported
//! column types dropped by `extract_value`, filtered materialization with
//! out-of-range indices, timestamped join rows over a non-timestamp time
//! column, multi-batch join-row builders with null cells, timezone-aware
//! timestamp columns, and the projected `ColumnarEvent::to_event` lane.
use std::sync::Arc;

use std::collections::HashSet;

use arrow::array::{
    ArrayRef, Date32Array, Float32Array, Int16Array, Int64Array, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::match_engine::match_engine::Value;
use crate::match_engine::{
    ColumnarEvent, FieldSource, batch_to_events, build_field_index, columnar_join_rows,
    columnar_timestamped_join_rows, materialize_rows_filtered,
};

fn make_schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

// ---------------------------------------------------------------------------
// extract_value — unsupported column types map to None (field dropped)
// ---------------------------------------------------------------------------

#[test]
fn batch_to_events_drops_unsupported_column_types() {
    let schema = make_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("i16", DataType::Int16, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("d32", DataType::Date32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int16Array::from(vec![Some(3), None])) as ArrayRef,
            Arc::new(Float32Array::from(vec![Some(1.5), Some(2.5)])) as ArrayRef,
            Arc::new(Date32Array::from(vec![Some(19000), Some(19001)])) as ArrayRef,
        ],
    )
    .unwrap();

    let events = batch_to_events(&batch);
    assert_eq!(events.len(), 2);
    // Int64 survives; Int16/Float32/Date32 have no conversion → omitted.
    assert_eq!(events[0].fields["id"], Value::Number(1.0));
    assert!(!events[0].fields.contains_key("i16"));
    assert!(!events[0].fields.contains_key("f32"));
    assert!(!events[0].fields.contains_key("d32"));
    assert_eq!(events[1].fields["id"], Value::Number(2.0));
}

#[test]
fn batch_to_events_timestamp_with_timezone_uses_millis_f64_value() {
    // `extract_value` matches `Timestamp(Ns, _)` — the timezone-aware variant
    // must convert the same as the bare one.
    let schema = make_schema(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        false,
    )]);
    let nanos: i64 = 1_700_000_000_000_000_000;
    let tz_array = TimestampNanosecondArray::from(vec![nanos]).with_timezone("UTC");
    let batch = RecordBatch::try_new(schema, vec![Arc::new(tz_array) as ArrayRef]).unwrap();

    let events = batch_to_events(&batch);
    assert_eq!(events[0].fields["ts"], Value::Number(nanos as f64));
}

// ---------------------------------------------------------------------------
// materialize_rows_filtered — projection + out-of-range indices
// ---------------------------------------------------------------------------

#[test]
fn materialize_rows_filtered_projects_and_skips_out_of_range() {
    let schema = make_schema(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
    )
    .unwrap();

    let only_id: HashSet<String> = HashSet::from(["id".to_string()]);
    // Index 5 is out of range → skipped; row 1 is materialized with only "id".
    let events = materialize_rows_filtered(&batch, &[1, 5], &only_id);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].fields["id"], Value::Number(2.0));
    assert!(!events[0].fields.contains_key("name"));

    // Same indices with no filter → both fields materialized for row 0 and 2.
    let events = crate::match_engine::materialize_rows(&batch, &[0, 2]);
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].fields["name"], Value::Str("c".into()));
}

// ---------------------------------------------------------------------------
// columnar_join_rows — multiple batches, null cells, empty batches
// ---------------------------------------------------------------------------

#[test]
fn columnar_join_rows_across_batches_with_null_cells() {
    let schema = make_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let b1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
        ],
    )
    .unwrap();
    let b2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(3)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("c")])) as ArrayRef,
        ],
    )
    .unwrap();

    let rows = columnar_join_rows(vec![b1, b2], None);
    assert_eq!(rows.len(), 3, "rows across both batches");
    assert_eq!(rows[0].field_value("id"), Some(Value::Number(1.0)));
    // Null cell in batch 1 row 1 → None (matches the eager null-drop).
    assert_eq!(rows[1].field_value("id"), None);
    assert_eq!(rows[1].field_value("name"), Some(Value::Str("b".into())));
    assert_eq!(rows[2].field_value("id"), Some(Value::Number(3.0)));

    // A batch of zero rows contributes nothing.
    let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )])));
    assert!(columnar_join_rows(vec![empty], None).is_empty());
}

// ---------------------------------------------------------------------------
// columnar_timestamped_join_rows — non-timestamp time column
// ---------------------------------------------------------------------------

#[test]
fn timestamped_join_rows_skip_rows_when_time_column_is_not_timestamp() {
    let schema = make_schema(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_000, 2_000])) as ArrayRef,
            Arc::new(Int64Array::from(vec![42, 99])) as ArrayRef,
        ],
    )
    .unwrap();

    // `batch_raw_ts_nanos` only understands Timestamp(Ns): every row is
    // skipped, so no join rows are produced.
    let rows = columnar_timestamped_join_rows(vec![batch], 0, None);
    assert!(rows.is_empty());
}

// ---------------------------------------------------------------------------
// ColumnarEvent — projected to_event lane with null cells
// ---------------------------------------------------------------------------

#[test]
fn columnar_event_projected_to_event_skips_null_and_unprojected_cells() {
    let schema = make_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(7), None])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_000),
                Some(2_000),
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let index = build_field_index(&batch);
    let proj: Arc<HashSet<String>> =
        Arc::new(HashSet::from(["id".to_string(), "name".to_string()]));
    let ce = ColumnarEvent::with_index_projected(&batch, 1, Arc::clone(&index), Some(proj));

    // field_names respects the projection.
    let mut names = ce.field_names();
    names.sort_unstable();
    assert_eq!(names, vec!["id", "name"]);
    // Null id cell → None despite being projected.
    assert_eq!(ce.field_value("id"), None);
    assert_eq!(ce.field_value("name"), Some(Value::Str("y".into())));

    // to_event materializes only projected, non-null fields.
    let ev = ce.to_event();
    assert!(ev.fields.contains_key("name"));
    assert!(!ev.fields.contains_key("id"), "null cell dropped");
    assert!(!ev.fields.contains_key("ts"), "unprojected column dropped");
}
