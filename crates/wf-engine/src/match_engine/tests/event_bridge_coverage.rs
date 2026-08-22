//! Event-bridge coverage: the `ColumnarEvent`/`JoinRow`/`extract_value`
//! branches the main suites reach only indirectly — the pre-resolved
//! `value_at` lane, the `batch()`/`row()` accessors, list/struct cells with
//! null children, empty-batch join-row builders, and structured-JSON cells
//! that contain JSON `null` (dropped, matching `json_to_value`).

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Int64Array, LargeListArray, ListArray, StringArray,
    StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::match_engine::match_engine::Value;
use crate::match_engine::{
    ColumnarEvent, FieldSource, WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY,
    WFL_FIELD_TYPE_OBJECT, batch_to_events, build_field_index, columnar_join_rows,
    columnar_timestamped_join_rows, materialize_rows, wfl_structured_field_kind,
};

fn make_schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}

/// `(id: Int64, name: Utf8, ts: Timestamp(Ns))` batch with a null row.
fn batch() -> RecordBatch {
    let schema = make_schema(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
    ]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
            Arc::new(arrow::array::TimestampNanosecondArray::from(vec![
                Some(1_000),
                Some(2_000),
                Some(3_000),
            ])) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn columnar_event_value_at_and_batch_row_accessors() {
    let batch = batch();
    let index = build_field_index(&batch);
    let ce = ColumnarEvent::with_index(&batch, 0, Arc::clone(&index));

    // Pre-resolved index lane: identical to field_value for the same column.
    assert_eq!(ce.value_at(0), Some(Value::Number(1.0)));
    assert_eq!(ce.value_at(1), Some(Value::Str("a".into())));
    // Null cell → None.
    assert_eq!(
        ColumnarEvent::with_index(&batch, 1, Arc::clone(&index)).value_at(0),
        None
    );
    // A column index outside the schema panics (batch access); hot paths only
    // pass in-schema indices resolved from the field index.

    // pub(crate) batch()/row() accessors (used by the on-each columnar path).
    assert_eq!(ce.batch().num_rows(), 3);
    assert_eq!(ce.row(), 0);

    // field_value_str inherent method: Str → text, Number → "".
    assert_eq!(ce.field_value_str("name"), "a");
    assert_eq!(ce.field_value_str("id"), "");
    assert_eq!(ce.field_value_str("missing"), "");
}

#[test]
fn columnar_event_no_index_to_event_matches_materialized() {
    // `ColumnarEvent::new` (no index) materializes via the schema fallback and
    // `field_names`; `to_event` must reproduce `materialize_rows`.
    let batch = batch();
    let ce = ColumnarEvent::new(&batch, 0);
    let events = materialize_rows(&batch, &[0]);
    assert_eq!(ce.to_event(), events[0]);
    // A row whose cells are all null → empty event map.
    let empty = ColumnarEvent::new(&batch, 1).to_event();
    assert!(
        empty.fields.contains_key("name"),
        "null cells dropped, present ones kept"
    );
    assert!(!empty.fields.contains_key("id"));
}

#[test]
fn list_cells_with_null_elements_are_skipped() {
    // List / LargeList / FixedSizeList: null elements inside a list are
    // dropped, matching the row-based `batch_to_events` output.
    let list =
        ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1), None, Some(3)])]);
    let large = LargeListArray::try_new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(vec![0i64, 3].into()),
        Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])) as ArrayRef,
        None,
    )
    .unwrap();
    let fixed = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        2,
        Arc::new(Int64Array::from(vec![Some(4), None])) as ArrayRef,
        None,
    );
    let schema = make_schema(vec![
        Field::new("tags", list.data_type().clone(), true),
        Field::new("big", large.data_type().clone(), true),
        Field::new("pair", fixed.data_type().clone(), true),
    ]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(list) as ArrayRef,
            Arc::new(large) as ArrayRef,
            Arc::new(fixed) as ArrayRef,
        ],
    )
    .unwrap();
    let events = batch_to_events(&batch);
    assert_eq!(
        events[0].fields["tags"],
        Value::Array(vec![Value::Number(1.0), Value::Number(3.0)])
    );
    assert_eq!(
        events[0].fields["big"],
        Value::Array(vec![Value::Number(7.0), Value::Number(9.0)])
    );
    assert_eq!(
        events[0].fields["pair"],
        Value::Array(vec![Value::Number(4.0)])
    );
}

#[test]
fn struct_cell_with_null_child_drops_the_child() {
    // A struct whose child column is null at this row → the member is omitted
    // (same null-drop semantics as the flat columns).
    let child = Int64Array::from(vec![Some(1), None]);
    let struct_arr = StructArray::from(vec![(
        Arc::new(Field::new("x", DataType::Int64, true)),
        Arc::new(child) as ArrayRef,
    )]);
    let schema = make_schema(vec![Field::new("s", struct_arr.data_type().clone(), false)]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(struct_arr) as ArrayRef]).unwrap();
    let events = batch_to_events(&batch);
    assert_eq!(events.len(), 2);
    let Value::Object(m) = &events[1].fields["s"] else {
        panic!("expected object");
    };
    assert!(m.is_empty(), "null child dropped");
}

#[test]
fn empty_batch_join_row_builders_return_empty() {
    assert!(columnar_join_rows(vec![], None).is_empty());
    assert!(columnar_timestamped_join_rows(vec![], 0, None).is_empty());
    // A batch with zero rows also produces zero join rows.
    let empty = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )])));
    assert!(columnar_join_rows(vec![empty], None).is_empty());
}

#[test]
fn structured_json_cells_with_json_null_are_dropped() {
    // JSON `null` inside a structured cell maps to None (field dropped), and
    // a top-level `null` cell is dropped entirely — matching `json_to_value`.
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
    assert_eq!(
        wfl_structured_field_kind(&obj_field),
        Some(WFL_FIELD_TYPE_OBJECT)
    );
    let schema = make_schema(vec![obj_field, arr_field]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![r#"{"a":null,"b":2}"#])) as ArrayRef,
            Arc::new(StringArray::from(vec![r#"[1,null,3]"#])) as ArrayRef,
        ],
    )
    .unwrap();
    let events = batch_to_events(&batch);
    let Value::Object(m) = &events[0].fields["ext"] else {
        panic!("expected object");
    };
    assert_eq!(m.len(), 1, "JSON null member dropped");
    assert_eq!(m.get("b"), Some(&Value::Number(2.0)));
    assert_eq!(
        events[0].fields["ports"],
        Value::Array(vec![Value::Number(1.0), Value::Number(3.0)])
    );
}

#[test]
fn projection_restricts_field_names_on_columnar_join_rows() {
    // The columnar JoinRow's `field_names` honors the projection while
    // `field_value` still reads any column (join conditions).
    let batch = batch();
    let proj: Arc<HashSet<String>> = Arc::new(HashSet::from(["id".to_string()]));
    let rows = columnar_join_rows(vec![batch], Some(proj));
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].field_names(), vec!["id"]);
    assert_eq!(rows[0].field_value("name"), Some(Value::Str("a".into())));
    // Null cell on the columnar view → None.
    assert_eq!(rows[1].field_value("id"), None);
}
