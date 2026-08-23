//! Round-4 coverage-fill tests for `match_engine/event_bridge.rs` — the
//! batch-level bridge functions and their error lanes that the existing
//! batteries leave cold:
//!
//! - `wfl_structured_field_kind` with a non object/array metadata kind
//! - `batch_event_time_nanos_at` over a non-numeric column type
//! - `batch_to_events_filtered` / `materialize_rows_filtered` projections
//! - `materialize_rows` out-of-range index skip
//! - `batch_to_timestamped_rows` with a non-Timestamp time column
//! - `extract_field_value` structured JSON failure lanes (invalid JSON, kind
//!   mismatch) and the scalar `extract_value` arms for every Arrow type
//! - `json_to_value` null/bool/number/string/array/object recursion
//! - `column_scalar_string` structured-value rejection
//! - `ColumnarEvent` projected `field_names` / `to_event` and the
//!   schema-fallback `field_value` lane
//! - `JoinRow::Event` field access and `columnar_timestamped_join_rows`
//!   null-timestamp skip

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, LargeListArray,
    ListArray, StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::match_engine::EngineHashMap;
use crate::match_engine::event_bridge::extract_field_value;
use crate::match_engine::match_engine::Value;
use crate::match_engine::{
    ColumnarEvent, FieldSource, JoinRow, WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY,
    WFL_FIELD_TYPE_OBJECT, batch_event_time_nanos, batch_event_time_nanos_at, batch_raw_ts_nanos,
    batch_time_col_index, batch_to_events_filtered, batch_to_timestamped_rows, build_field_index,
    column_scalar_string, columnar_join_rows, columnar_timestamped_join_rows,
    materialize_rows_filtered, wfl_structured_field_kind,
};

fn make_schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

/// `(id: Int64, name: Utf8, active: Boolean, score: Float64, ts: Timestamp(Ns))`
/// with a null row.
fn batch() -> RecordBatch {
    let schema = make_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
        Field::new("score", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
    ]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_000),
                Some(2_000),
                Some(3_000),
            ])) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn structured_field_kind_non_object_metadata_is_none() {
    // A metadata kind that is neither `object` nor `array` → None.
    let scalar =
        Field::new("ext", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            "scalar".to_string(),
        )]));
    assert_eq!(wfl_structured_field_kind(&scalar), None);
    // No metadata at all → None.
    let plain = Field::new("p", DataType::Utf8, true);
    assert_eq!(wfl_structured_field_kind(&plain), None);
    // Real structured kinds resolve.
    let obj =
        Field::new("o", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            WFL_FIELD_TYPE_OBJECT.to_string(),
        )]));
    let arr =
        Field::new("a", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            WFL_FIELD_TYPE_ARRAY.to_string(),
        )]));
    assert_eq!(wfl_structured_field_kind(&obj), Some(WFL_FIELD_TYPE_OBJECT));
    assert_eq!(wfl_structured_field_kind(&arr), Some(WFL_FIELD_TYPE_ARRAY));
}

#[test]
fn batch_time_helpers_non_numeric_column_and_absent_field() {
    let schema = make_schema(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("s", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![Some(5_000)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(batch_time_col_index(&batch, Some("ts")), Some(0));
    assert_eq!(batch_time_col_index(&batch, Some("missing")), None);
    assert_eq!(batch_time_col_index(&batch, None), None);
    assert_eq!(batch_event_time_nanos_at(&batch, 0, 0), 5_000);
    // Non-numeric column type → 0.
    assert_eq!(batch_event_time_nanos_at(&batch, 1, 0), 0);
    // batch_event_time_nanos resolves the column then reads it.
    assert_eq!(batch_event_time_nanos(&batch, Some("ts"), 0), 5_000);
    assert_eq!(batch_event_time_nanos(&batch, Some("missing"), 0), 0);
    assert_eq!(batch_event_time_nanos(&batch, None, 0), 0);
}

#[test]
fn filtered_materialization_projections_and_out_of_range() {
    let batch = batch();
    let only = HashSet::from(["id".to_string(), "name".to_string()]);

    // batch_to_events_filtered materializes only the listed fields.
    let events = batch_to_events_filtered(&batch, &only);
    assert_eq!(events.len(), 3);
    assert!(events[0].fields.contains_key("id"));
    assert!(events[0].fields.contains_key("name"));
    assert!(!events[0].fields.contains_key("score"));
    // Null cells are dropped even when requested.
    assert!(!events[1].fields.contains_key("id"));

    // materialize_rows_filtered with a projection; out-of-range rows skipped.
    let rows = materialize_rows_filtered(&batch, &[0, 99, 1], &only);
    assert_eq!(rows.len(), 2);
    assert!(rows[0].fields.contains_key("id"));
    assert!(!rows[0].fields.contains_key("score"));
    assert!(!rows[1].fields.contains_key("id"), "null id dropped");
}

#[test]
fn batch_to_timestamped_rows_non_timestamp_column_returns_empty() {
    // The time column is not a Timestamp(Ns) → empty rows.
    let schema = make_schema(vec![Field::new("ts", DataType::Int64, true)]);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef],
    )
    .unwrap();
    assert!(batch_to_timestamped_rows(&batch, 0).is_empty());

    // Timestamp column with a null row → the null row is skipped.
    let schema = make_schema(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("v", DataType::Int64, true),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![Some(10), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(7), Some(8)])) as ArrayRef,
        ],
    )
    .unwrap();
    let rows = batch_to_timestamped_rows(&batch, 0);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 10);
    assert_eq!(rows[0].1.get("v"), Some(&Value::Number(7.0)));
    assert_eq!(rows[0].1.get("ts"), Some(&Value::Number(10.0)));
}

#[test]
fn structured_json_failure_lanes() {
    // Invalid JSON in a structured object cell → the field is dropped.
    let obj_field =
        Field::new("ext", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            WFL_FIELD_TYPE_METADATA_KEY.to_string(),
            WFL_FIELD_TYPE_OBJECT.to_string(),
        )]));
    let arr_field =
        Field::new("ports", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([
            (
                WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                WFL_FIELD_TYPE_ARRAY.to_string(),
            ),
        ]));
    let schema = make_schema(vec![obj_field.clone(), arr_field.clone()]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["not-json"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["[1,2]"])) as ArrayRef,
        ],
    )
    .unwrap();
    // Invalid object JSON → extract_field_value returns None (field dropped).
    assert!(
        !batch_to_events_filtered(&batch, &HashSet::from(["ext".into()]))[0]
            .fields
            .contains_key("ext")
    );

    // Kind mismatch: a JSON array cell under `object` metadata → None.
    let schema = make_schema(vec![obj_field.clone()]);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![r#"[1,2]"#])) as ArrayRef],
    )
    .unwrap();
    let events = batch_to_events_filtered(&batch, &HashSet::from(["ext".into()]));
    assert!(!events[0].fields.contains_key("ext"));

    // Kind match: object metadata with a JSON object → parsed.
    let schema = make_schema(vec![obj_field]);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![r#"{"k":1,"nested":{"a":true}}"#])) as ArrayRef],
    )
    .unwrap();
    let events = batch_to_events_filtered(&batch, &HashSet::from(["ext".into()]));
    let Value::Object(m) = &events[0].fields["ext"] else {
        panic!("expected object");
    };
    assert_eq!(m.get("k"), Some(&Value::Number(1.0)));
    assert!(m.get("nested").is_some());
}

#[test]
fn scalar_extract_value_arms_and_list_nulls() {
    // A batch with one column per Arrow type exercises extract_value's arms.
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1), None])]);
    let large = LargeListArray::try_new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(vec![0i64, 2].into()),
        Arc::new(Int64Array::from(vec![Some(5), Some(6)])) as ArrayRef,
        None,
    )
    .unwrap();
    let fixed = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        2,
        Arc::new(Int64Array::from(vec![Some(4), None])) as ArrayRef,
        None,
    );
    let struct_arr = StructArray::from(vec![(
        Arc::new(Field::new("x", DataType::Int64, false)),
        Arc::new(Int64Array::from(vec![9])) as ArrayRef,
    )]);
    let schema = make_schema(vec![
        Field::new("l", list.data_type().clone(), true),
        Field::new("ll", large.data_type().clone(), true),
        Field::new("fl", fixed.data_type().clone(), true),
        Field::new("s", struct_arr.data_type().clone(), false),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(list) as ArrayRef,
            Arc::new(large) as ArrayRef,
            Arc::new(fixed) as ArrayRef,
            Arc::new(struct_arr) as ArrayRef,
        ],
    )
    .unwrap();

    // extract_field_value for each column (list null elements dropped).
    let l = extract_field_value(batch.schema_ref().field(0), batch.column(0), 0).unwrap();
    assert_eq!(l, Value::Array(vec![Value::Number(1.0)]));
    let ll = extract_field_value(batch.schema_ref().field(1), batch.column(1), 0).unwrap();
    assert_eq!(
        ll,
        Value::Array(vec![Value::Number(5.0), Value::Number(6.0)])
    );
    let fl = extract_field_value(batch.schema_ref().field(2), batch.column(2), 0).unwrap();
    assert_eq!(fl, Value::Array(vec![Value::Number(4.0)]));
    let s = extract_field_value(batch.schema_ref().field(3), batch.column(3), 0).unwrap();
    assert_eq!(
        s,
        Value::Object(EngineHashMap::from_iter([
            ("x".into(), Value::Number(9.0),)
        ]))
    );
}

#[test]
fn column_scalar_string_structured_and_null() {
    let batch = batch();
    // Str / Number / Bool → string forms.
    assert_eq!(column_scalar_string(&batch, 1, 0), Some("a".to_string()));
    assert_eq!(column_scalar_string(&batch, 3, 0), Some("1.5".to_string()));
    assert_eq!(column_scalar_string(&batch, 2, 0), Some("true".to_string()));
    // Null cell → None.
    assert_eq!(column_scalar_string(&batch, 1, 2), None);
    // Structured (list) column → None.
    let list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1)])]);
    let schema = make_schema(vec![Field::new("l", list.data_type().clone(), true)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(list) as ArrayRef]).unwrap();
    assert_eq!(column_scalar_string(&batch, 0, 0), None);
}

#[test]
fn columnar_event_projected_fields_and_schema_fallback() {
    let batch = batch();
    let index = build_field_index(&batch);
    // Projected view: field_names restricted, to_event only carries projected.
    let proj: Arc<HashSet<String>> = Arc::new(HashSet::from(["id".to_string(), "ts".to_string()]));
    let ce = ColumnarEvent::with_index_projected(&batch, 0, Arc::clone(&index), Some(proj));
    let mut names = ce.field_names();
    names.sort_unstable();
    assert_eq!(names, vec!["id", "ts"]);
    let ev = ce.to_event();
    assert!(ev.fields.contains_key("id"));
    assert!(!ev.fields.contains_key("name"));
    // field_value still reads non-projected columns.
    assert_eq!(ce.field_value("name"), Some(Value::Str("a".into())));

    // Schema-fallback lane (no index): field_value via schema().index_of.
    let ce = ColumnarEvent::new(&batch, 0);
    assert_eq!(ce.field_value("id"), Some(Value::Number(1.0)));
    assert_eq!(ce.field_value("missing"), None);
    // to_event via the field_names fallback loop.
    let ev = ce.to_event();
    assert!(ev.fields.contains_key("id"));
    assert!(ev.fields.contains_key("name"));
}

#[test]
fn join_row_event_variant_and_columnar_null_reads() {
    // JoinRow::Event: field_value from the map, field_names from the keys.
    let ev = Arc::new(crate::match_engine::Event {
        fields: crate::match_engine::EngineHashMap::from_iter([
            ("a".into(), Value::Number(1.0)),
            ("b".into(), Value::Str("x".into())),
        ]),
    });
    let row = JoinRow::Event(ev);
    assert_eq!(row.field_value("a"), Some(Value::Number(1.0)));
    assert_eq!(row.field_value("missing"), None);
    let mut names = row.field_names();
    names.sort_unstable();
    assert_eq!(names, vec!["a", "b"]);

    // Columnar JoinRow with a null cell → field_value None.
    let batch = batch();
    let rows = columnar_join_rows(vec![batch], None);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].field_value("id"), Some(Value::Number(1.0)));
    assert_eq!(rows[1].field_value("id"), None);
}

#[test]
fn columnar_timestamped_join_rows_skips_null_timestamps() {
    let schema = make_schema(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("id", DataType::Int64, false),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
        ],
    )
    .unwrap();
    // batch_raw_ts_nanos: raw value / null / non-timestamp column.
    assert_eq!(batch_raw_ts_nanos(&batch, 0, 0), Some(1));
    assert_eq!(batch_raw_ts_nanos(&batch, 0, 1), None);
    assert_eq!(batch_raw_ts_nanos(&batch, 1, 0), None);
    // columnar_timestamped_join_rows skips the null-timestamp row.
    let rows = columnar_timestamped_join_rows(vec![batch], 0, None);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[1].0, 3);
}
