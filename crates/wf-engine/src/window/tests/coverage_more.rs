//! Round-2 coverage-fill tests for `window/fanout.rs` — the fanout table /
//! columnar-key paths the in-module `tests` and `window/tests/coverage_extra.rs`
//! do not reach:
//!
//! - `has_subscribers` / `window_is_sharded` presence checks;
//! - broadcasting to a window with no subscribers (early return);
//! - `broadcast_with_batch` and `broadcast_batch_only` on unsharded
//!   subscriptions (batch + materialize_fields delivery);
//! - `precompute_shard_rows` with the key column absent from the schema
//!   (all rows → shard 0);
//! - `scope_key_from_column` lanes: Timestamp(Ns) / Float64 / Boolean and the
//!   unsupported-type fallback via `column_scalar` (incl. null);
//! - `scope_key_columnar` with an empty key list → `ScopeKey::Empty`;
//! - sharded broadcasts with an empty batch (no push → empty send set);
//! - sharded row-based broadcast with a missing key field → shard 0.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, StructArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use wf_lang::ast::FieldRef;

use crate::match_engine::{EngineHashMap, Event, ScopeKey, Value};
use crate::window::{RuleFanout, RulePush, scope_key_columnar, scope_key_from_column};

fn event(id: &str) -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("id".into(), Value::Str(id.into()));
    Event { fields }
}

fn keys() -> Vec<FieldRef> {
    vec![FieldRef::Simple("id".into())]
}

fn str_batch(values: Vec<Option<&str>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(values)) as ArrayRef],
    )
    .unwrap()
}

#[test]
fn has_subscribers_and_window_is_sharded_presence() {
    let fanout = RuleFanout::new();
    assert!(!fanout.has_subscribers("win"), "nothing registered yet");
    assert!(!fanout.window_is_sharded("win"), "no sharding registered");

    let (tx, _rx) = mpsc::channel::<RulePush>(8);
    fanout.register("win", tx);
    assert!(fanout.has_subscribers("win"));
    assert!(!fanout.has_subscribers("other"));
    assert!(
        !fanout.window_is_sharded("win"),
        "sharding table is separate"
    );

    fanout.register_window_sharding("win", Arc::from(keys().into_boxed_slice()), 2);
    assert!(fanout.window_is_sharded("win"));
    assert!(!fanout.window_is_sharded("other"));
}

#[tokio::test]
async fn broadcast_to_unregistered_window_is_a_noop() {
    let fanout = RuleFanout::new();
    let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("x"))]);
    // Must complete without panic and without delivering anything.
    fanout.broadcast("missing", &events, 0).await;
}

#[tokio::test]
async fn broadcast_with_batch_delivers_batch_and_materialize_fields() {
    let fanout = RuleFanout::new();
    let (tx, mut rx) = mpsc::channel::<RulePush>(8);
    fanout.register("win", tx);

    let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("k1"))]);
    let batch = str_batch(vec![Some("k1")]);
    let materialize: Arc<HashSet<String>> = Arc::new(HashSet::from(["id".to_string()]));
    fanout
        .broadcast_with_batch("win", &events, &batch, Some(&materialize), 7)
        .await;

    let push = rx.try_recv().expect("single subscriber receives a push");
    assert_eq!(&*push.window_name, "win");
    assert_eq!(push.seq, 7);
    assert!(
        push.events
            .as_ref()
            .is_some_and(|e| Arc::ptr_eq(e, &events))
    );
    assert!(
        push.batch.as_ref().is_some_and(|b| **b == batch),
        "deferred push carries a clone of the raw batch"
    );
    assert_eq!(push.materialize_fields.as_ref().map(|m| m.len()), Some(1));
    assert!(push.shard_rows.is_none());
}

#[tokio::test]
async fn broadcast_batch_only_on_unsharded_subscription_delivers_raw_batch() {
    let fanout = RuleFanout::new();
    let (tx, mut rx) = mpsc::channel::<RulePush>(8);
    fanout.register("win", tx);

    let batch = str_batch(vec![Some("k1"), Some("k2")]);
    fanout
        .broadcast_batch_only("win", &batch, None, None, 3)
        .await;

    let push = rx.try_recv().expect("unsharded push");
    assert!(push.events.is_none(), "deferred push carries no events");
    assert!(push.batch.is_some());
    assert_eq!(push.batch.as_ref().unwrap().num_rows(), 2);
    assert_eq!(push.seq, 3);
}

#[test]
fn precompute_shard_rows_missing_key_column_puts_every_row_on_shard_zero() {
    // Key column absent from the schema → `partition_rows_by_key` returns
    // None → the all-shard-0 fallback (matches row-based missing-key).
    let schema = Arc::new(Schema::new(vec![Field::new(
        "other",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef],
    )
    .unwrap();

    let fanout = RuleFanout::new();
    fanout.register_window_sharding(
        "win",
        Arc::from(vec![FieldRef::Simple("id".into())].into_boxed_slice()),
        3,
    );
    let rows = fanout
        .precompute_shard_rows("win", &batch)
        .expect("sharded window partitions even without the key column");
    assert_eq!(rows.len(), 3);
    let mut all: Vec<u32> = rows.iter().flatten().copied().collect();
    all.sort_unstable();
    assert_eq!(all, vec![0, 1, 2], "every row lands somewhere exactly once");
    // And specifically: all rows are on shard 0 (missing key).
    assert_eq!(rows[0], vec![0, 1, 2]);
    assert!(rows[1].is_empty());
    assert!(rows[2].is_empty());
}

#[test]
fn scope_key_from_column_type_lanes() {
    // Timestamp(Nanosecond) → ScopeKey::Int (raw value).
    let ts_schema = Arc::new(Schema::new(vec![Field::new(
        "t",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    let ts_batch = RecordBatch::try_new(
        ts_schema,
        vec![Arc::new(TimestampNanosecondArray::from(vec![
            Some(1_700_000_000_000_000_000),
            None,
        ])) as ArrayRef],
    )
    .unwrap();
    assert_eq!(
        scope_key_from_column(&ts_batch, 0, 0),
        Some(ScopeKey::Int(1_700_000_000_000_000_000))
    );
    assert_eq!(scope_key_from_column(&ts_batch, 0, 1), None);

    // Float64 → ScopeKey from the f64 Value (finite → Number lane).
    let f_schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
    let f_batch = RecordBatch::try_new(
        f_schema,
        vec![Arc::new(Float64Array::from(vec![Some(2.5), None])) as ArrayRef],
    )
    .unwrap();
    assert_eq!(
        scope_key_from_column(&f_batch, 0, 0),
        Some(ScopeKey::from_value(&Value::Number(2.5)))
    );
    assert_eq!(scope_key_from_column(&f_batch, 0, 1), None);

    // Boolean → ScopeKey::Str("true"/"false").
    let b_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Boolean, true)]));
    let b_batch = RecordBatch::try_new(
        b_schema,
        vec![Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as ArrayRef],
    )
    .unwrap();
    assert_eq!(
        scope_key_from_column(&b_batch, 0, 0),
        Some(ScopeKey::Str("true".into()))
    );
    assert_eq!(
        scope_key_from_column(&b_batch, 0, 1),
        Some(ScopeKey::Str("false".into()))
    );

    // Unsupported type (Struct) → fallback via column_scalar → from_value
    // (scope_key_from_column only special-cases Int64 / Timestamp(Ns) /
    // Float64 / Utf8 / Boolean; Struct is read through extract_value).
    let child_field = Field::new("k", DataType::Int64, true);
    let struct_field = Field::new(
        "s",
        DataType::Struct(Fields::from(vec![child_field.clone()])),
        true,
    );
    let s_schema = Arc::new(Schema::new(vec![struct_field]));
    let struct_col: StructArray = vec![(
        Arc::new(child_field),
        Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef,
    )]
    .into();
    let s_batch = RecordBatch::try_new(s_schema, vec![Arc::new(struct_col) as ArrayRef]).unwrap();
    assert_eq!(
        scope_key_from_column(&s_batch, 0, 0),
        Some(ScopeKey::Str("[object]".into()))
    );
    // Null cell in the fallback path → column_scalar returns None.
    let null_struct = StructArray::new_null(
        Fields::from(vec![Field::new("k", DataType::Int64, true)]),
        1,
    );
    let s_batch =
        RecordBatch::try_new(s_batch.schema(), vec![Arc::new(null_struct) as ArrayRef]).unwrap();
    assert_eq!(scope_key_from_column(&s_batch, 0, 0), None);
}

#[test]
fn scope_key_columnar_empty_key_list_returns_empty() {
    let batch = str_batch(vec![Some("k1")]);
    assert_eq!(scope_key_columnar(&batch, &[], 0), Some(ScopeKey::Empty));
}

#[test]
fn scope_key_columnar_multi_key_pairs_in_order() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(5)])) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(
        scope_key_columnar(&batch, &[0, 1], 0),
        Some(ScopeKey::Pair(
            Box::new(ScopeKey::Str("x".into())),
            Box::new(ScopeKey::Int(5)),
        ))
    );
    // A null key column anywhere → None (shard 0).
    let batch = RecordBatch::try_new(
        batch.schema(),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(5)])) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(scope_key_columnar(&batch, &[0, 1], 0), None);
}

#[tokio::test]
async fn sharded_broadcast_with_empty_batch_sends_nothing() {
    let fanout = RuleFanout::new();
    let (tx0, mut rx0) = mpsc::channel::<RulePush>(8);
    let (tx1, mut rx1) = mpsc::channel::<RulePush>(8);
    fanout.register_sharded("win", vec![tx0, tx1], Arc::from(keys().into_boxed_slice()));
    let empty = str_batch(vec![]);
    fanout
        .broadcast_batch_only("win", &empty, None, None, 0)
        .await;
    assert!(rx0.try_recv().is_err(), "no rows → no shard push");
    assert!(rx1.try_recv().is_err(), "no rows → no shard push");
}

#[tokio::test]
async fn sharded_row_broadcast_with_missing_key_routes_to_shard_zero() {
    let fanout = RuleFanout::new();
    let (tx0, mut rx0) = mpsc::channel::<RulePush>(8);
    let (tx1, mut rx1) = mpsc::channel::<RulePush>(8);
    fanout.register_sharded("win", vec![tx0, tx1], Arc::from(keys().into_boxed_slice()));

    // Event without the key field "id" → missing key → shard 0.
    let no_key: Event = Event {
        fields: EngineHashMap::default(),
    };
    let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(no_key)]);
    fanout.broadcast("win", &events, 0).await;

    let got0 = rx0
        .try_recv()
        .map(|p| p.events.as_ref().map(|e| e.len()).unwrap_or(0));
    assert_eq!(got0, Ok(1), "missing-key event lands on shard 0");
    assert!(rx1.try_recv().is_err(), "other shard receives nothing");
}

#[tokio::test]
async fn broadcast_with_batch_sharded_row_path_leaves_batch_none() {
    // Row-based sharded broadcast partitions pre-materialized events; the raw
    // batch is not forwarded (row indices no longer match the whole batch).
    let fanout = RuleFanout::new();
    let (tx0, mut rx0) = mpsc::channel::<RulePush>(8);
    let (tx1, _rx1) = mpsc::channel::<RulePush>(8);
    fanout.register_sharded("win", vec![tx0, tx1], Arc::from(keys().into_boxed_slice()));
    let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(event("k1"))]);
    let batch = str_batch(vec![Some("k1")]);
    fanout
        .broadcast_with_batch("win", &events, &batch, None, 0)
        .await;
    let push = rx0.try_recv().expect("shard 0 receives the sub-batch");
    assert!(push.events.is_some());
    assert!(
        push.batch.is_none(),
        "row-based sharded push drops the batch"
    );
    assert!(push.shard_rows.is_none());
}
