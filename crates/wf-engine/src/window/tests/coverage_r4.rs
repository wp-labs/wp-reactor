//! Round-4 coverage tests for the window layer: fanout sharded-subscription
//! pruning (closed shards), the deferred columnar broadcast's missing-key
//! fallback, registry runtime add/replace/provider/debug lanes, join-index
//! null/missing-key handling, the asof scan path over un-timestamped rows,
//! and `content_bytes` accounting for the exotic Arrow column types the
//! earlier suites skip.
//!
//! Only test code lives here — no production logic is modified.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray, NullArray,
    StringArray, StructArray, Time32MillisecondArray, Time64NanosecondArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
use wf_lang::ast::FieldRef;

use crate::match_engine::{AsofLookup, Event, JoinKey, Value};
use crate::window::buffer::{Window, WindowParams, content_bytes};
use crate::window::provider::ProviderWindow;
use crate::window::{RuleFanout, RulePush, WindowDef, WindowRegistry};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn event(id: &str) -> Event {
    let mut fields = crate::match_engine::EngineHashMap::default();
    fields.insert("id".into(), Value::Str(id.into()));
    Event { fields }
}

fn no_key_event() -> Event {
    Event {
        fields: crate::match_engine::EngineHashMap::default(),
    }
}

fn keys() -> Vec<FieldRef> {
    vec![FieldRef::Simple("id".into())]
}

fn ts_value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("value", DataType::Int64, true),
    ]))
}

fn make_ts_value_batch(times: &[Option<i64>], values: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(
        ts_value_schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn test_config(max_bytes: usize) -> WindowConfig {
    WindowConfig {
        name: "test".into(),
        mode: DistMode::Local,
        max_window_bytes: max_bytes.into(),
        over_cap: Duration::from_secs(3600).into(),
        evict_policy: EvictPolicy::TimeFirst,
        watermark: Duration::from_secs(5).into(),
        allowed_lateness: Duration::from_secs(0).into(),
        late_policy: LatePolicy::Drop,
        table: None,
    }
}

fn test_window(over_secs: u64, max_bytes: usize) -> Window {
    Window::new(
        WindowParams {
            name: "test_win".into(),
            schema: ts_value_schema(),
            time_col_index: Some(0),
            over: Duration::from_secs(over_secs),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(max_bytes),
    )
}

fn registry_def(name: &str, streams: Vec<&str>) -> WindowDef {
    let mut config = test_config(usize::MAX);
    config.name = name.to_string();
    WindowDef {
        params: WindowParams {
            name: name.into(),
            schema: ts_value_schema(),
            time_col_index: Some(0),
            over: Duration::from_secs(60),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: streams.into_iter().map(String::from).collect(),
        config,
    }
}

// ---------------------------------------------------------------------------
// fanout.rs — sharded subscription pruning + deferred fallback
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sharded_broadcast_prunes_closed_shard_and_keeps_open_shards() {
    let fanout = RuleFanout::new();
    let (closed_tx, closed_rx) = mpsc::channel::<RulePush>(8);
    let (open_tx, mut open_rx) = mpsc::channel::<RulePush>(8);
    drop(closed_rx); // shard 0 shut down
    fanout.register_sharded(
        "win",
        vec![closed_tx, open_tx],
        Arc::from(keys().into_boxed_slice()),
    );

    // A missing-key event deterministically lands on shard 0 (the closed one):
    // its send fails → the sharded prune path (`shards.retain`) runs.
    let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(no_key_event())]);
    fanout.broadcast("win", &events, 0).await;

    // The surviving shard must still receive subsequent broadcasts.
    let again: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(no_key_event())]);
    fanout.broadcast("win", &again, 1).await;
    let push = open_rx.try_recv().expect("open shard still receives");
    assert_eq!(push.seq, 1);

    // And the closed shard no longer receives anything: nothing else was sent.
    assert!(open_rx.try_recv().is_err());
}

#[tokio::test]
async fn sharded_broadcast_all_shards_closed_removes_window() {
    let fanout = RuleFanout::new();
    let (tx0, rx0) = mpsc::channel::<RulePush>(8);
    let (tx1, rx1) = mpsc::channel::<RulePush>(8);
    drop(rx0);
    drop(rx1);
    fanout.register_sharded("win", vec![tx0, tx1], Arc::from(keys().into_boxed_slice()));

    // First broadcast: shard 0 (missing-key event) fails → pruned; shard 1
    // remains in the table. Second broadcast: the lone remaining shard fails
    // too → the whole window entry is removed.
    let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(no_key_event())]);
    for _ in 0..2 {
        fanout.broadcast("win", &events, 0).await;
    }

    // No delivery channel left: broadcast is a no-op and nothing panics.
    fanout.broadcast("win", &events, 1).await;
    assert!(!fanout.has_subscribers("win"), "empty window pruned");
}

#[tokio::test]
async fn broadcast_batch_only_missing_key_column_falls_back_to_shard_zero() {
    // Deferred columnar broadcast whose batch lacks the key column entirely →
    // `partition_rows_by_key` returns None → every row lands on shard 0.
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
    let (tx0, mut rx0) = mpsc::channel(8);
    let (tx1, mut rx1) = mpsc::channel(8);
    fanout.register_sharded("win", vec![tx0, tx1], Arc::from(keys().into_boxed_slice()));

    fanout
        .broadcast_batch_only("win", &batch, None, None, 0)
        .await;

    let push = rx0
        .try_recv()
        .expect("shard 0 receives the fallback subset");
    let rows = push.shard_rows.expect("shard_rows set");
    assert_eq!(&*rows, &vec![0, 1, 2], "all rows fall back to shard 0");
    assert!(rx1.try_recv().is_err(), "shard 1 receives nothing");
}

// ---------------------------------------------------------------------------
// registry.rs — Debug / runtime add / replace / provider lanes
// ---------------------------------------------------------------------------

#[test]
#[allow(deprecated)]
fn registry_debug_runtime_add_replace_and_provider_lanes() {
    let mut reg = WindowRegistry::build(vec![registry_def("win_a", vec!["s1"])]).unwrap();

    // Debug impl (previously skipped).
    let debug = format!("{reg:?}");
    assert!(debug.contains("window_count"), "{debug}");

    // Runtime add: success then duplicate-name error.
    reg.try_add_window(registry_def("win_b", vec!["s2"]))
        .unwrap();
    assert!(reg.contains("win_b"));
    let err = reg
        .try_add_window(registry_def("win_b", vec!["s3"]))
        .expect_err("duplicate add must fail");
    assert!(err.to_string().contains("duplicate window name"));

    // The new window is routable and snapshot-able.
    let schema = ts_value_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000])),
            Arc::new(Int64Array::from(vec![7])),
        ],
    )
    .unwrap();
    reg.route("s2", batch).unwrap();
    assert_eq!(reg.snapshot("win_b").unwrap().len(), 1);

    // Runtime replace: existing name succeeds, missing name errors.
    reg.try_replace_window(registry_def("win_a", vec!["s1b"]))
        .unwrap();
    assert!(reg.contains("win_a"));
    let err = reg
        .try_replace_window(registry_def("missing", vec![]))
        .expect_err("replace of a non-existent window must fail");
    assert!(
        err.to_string()
            .contains("cannot replace non-existent window")
    );

    // Provider registration: success then duplicate rejection.
    reg.register_provider(
        "prov".into(),
        ProviderWindow::new("t".into(), "q".into(), None),
    )
    .unwrap();
    let err = reg
        .register_provider(
            "prov".into(),
            ProviderWindow::new("t".into(), "q".into(), None),
        )
        .expect_err("duplicate provider must fail");
    assert!(err.to_string().contains("duplicate provider window"));
    assert!(reg.get_provider("prov").is_some());
    assert!(reg.provider_snapshot("prov").is_some());
    assert!(reg.provider_snapshot("nope").is_none());
}

// ---------------------------------------------------------------------------
// buffer/mod.rs — join-index edge rows + asof scan over null timestamps
// ---------------------------------------------------------------------------

#[test]
fn join_index_skips_null_key_cells_and_missing_key_column() {
    // Null key cell → the row is not indexed (`col.is_null` continue).
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    let batch = make_ts_value_batch(
        &[Some(1_000_000), Some(2_000_000), Some(3_000_000)],
        &[Some(42), None, Some(43)],
    );
    win.append(batch).unwrap();
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(1),
        "non-null key indexed"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(43), None).map(|v| v.len()),
        Some(1),
        "later non-null key indexed"
    );

    // Missing key column in the batch → `index_batch` early-returns; the
    // append still succeeds (the window schema matches the batch).
    let no_key_schema = Arc::new(Schema::new(vec![Field::new(
        "other",
        DataType::Int64,
        true,
    )]));
    let win2 = Window::new(
        WindowParams {
            name: "no_key_win".into(),
            schema: no_key_schema.clone(),
            time_col_index: None,
            over: Duration::from_secs(60),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(usize::MAX),
    );
    win2.set_join_key("value".into());
    let batch = RecordBatch::try_new(
        no_key_schema,
        vec![Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrayRef],
    )
    .unwrap();
    win2.append(batch).unwrap();
    assert_eq!(win2.total_rows(), 2);
}

#[test]
fn join_asof_scan_skips_un_timestamped_rows() {
    // Rows for key 42: (null ts), (ts=1000), (ts=2000). max_ts=2000 >
    // event_time=1500 → the linear-scan path runs; the null-ts row is
    // skipped and ts=1000 (in [min_ts, event_time]) wins.
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    let batch = make_ts_value_batch(
        &[None, Some(1_000), Some(2_000)],
        &[Some(42), Some(42), Some(42)],
    );
    win.append(batch).unwrap();

    match win.join_lookup_asof(&JoinKey::Int(42), 1_500, 0, None) {
        AsofLookup::Hit(row) => match row {
            crate::match_engine::JoinRow::Columnar { row: r, .. } => {
                assert_eq!(r, 1, "scan path must pick the ts=1000 row")
            }
            _ => panic!("expected a columnar join row"),
        },
        _ => panic!("expected a scan-path Hit"),
    }

    // The fast path (max_ts <= event_time) still works after the scan path.
    assert!(matches!(
        win.join_lookup_asof(&JoinKey::Int(42), 5_000, 0, None),
        AsofLookup::Hit(_)
    ));
}

#[test]
fn extract_time_range_all_null_batch_appends_with_sentinel() {
    let win = test_window(3600, usize::MAX);
    let batch = make_ts_value_batch(&[None, None], &[Some(1), Some(2)]);
    win.append(batch).unwrap();
    assert_eq!(win.total_rows(), 2);
    assert_eq!(win.batch_count(), 1);
}

// ---------------------------------------------------------------------------
// buffer/mod.rs — content_bytes for exotic column types
// ---------------------------------------------------------------------------

/// Sum `content_bytes` over one batch per column group, exercising the
/// `column_content_bytes` arms the existing suites skip (fixed-width ints,
/// intervals, decimals, large utf8, binary, lists, maps, dictionary fallback).
#[test]
fn content_bytes_covers_exotic_column_types() {
    let n = 3usize;

    // Fixed-width small types.
    let batch = batch_from(vec![
        Arc::new(NullArray::new(n)),
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        Arc::new(Int8Array::from(vec![Some(1), None, Some(-1)])),
        Arc::new(UInt8Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(Int16Array::from(vec![Some(1), None, Some(-1)])),
        Arc::new(UInt16Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(Int32Array::from(vec![Some(1), None, Some(-1)])),
        Arc::new(UInt32Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(Float32Array::from(vec![Some(1.5), None, Some(-1.5)])),
        Arc::new(Date32Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(Time32MillisecondArray::from(vec![Some(1), None, Some(2)])),
    ]);
    assert!(content_bytes(&batch) > 0);

    // 8-byte + interval + decimal types.
    let batch = batch_from(vec![
        Arc::new(Int64Array::from(vec![Some(1), None, Some(-1)])),
        Arc::new(UInt64Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(Float64Array::from(vec![Some(1.5), None, Some(-1.5)])),
        Arc::new(Date64Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(Time64NanosecondArray::from(vec![Some(1), None, Some(2)])),
        Arc::new(TimestampNanosecondArray::from(vec![Some(1), None, Some(2)])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            Some(arrow::datatypes::IntervalMonthDayNano::new(1, 2, 3)),
            None,
            Some(arrow::datatypes::IntervalMonthDayNano::new(4, 5, 6)),
        ])),
        Arc::new(IntervalDayTimeArray::from(vec![
            Some(arrow::datatypes::IntervalDayTime::new(1, 2)),
            None,
            Some(arrow::datatypes::IntervalDayTime::new(3, 4)),
        ])),
        Arc::new(
            Decimal128Array::from(vec![Some(123_i128), None, Some(456_i128)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
    ]);
    assert!(content_bytes(&batch) > 0);

    // Large utf8 / binary / fixed-size-binary.
    let batch = batch_from(vec![
        Arc::new(LargeStringArray::from(vec![
            Some("hello"),
            None,
            Some("world"),
        ])),
        Arc::new(BinaryArray::from(vec![
            Some(b"abc".as_slice()),
            None,
            Some(b"x".as_slice()),
        ])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(b"abc".as_slice()),
            None,
            Some(b"xy".as_slice()),
        ])),
        Arc::new(FixedSizeBinaryArray::new(
            2,
            Buffer::from(vec![1u8, 2, 3, 4, 5, 6]),
            None,
        )),
    ]);
    assert!(content_bytes(&batch) > 0);

    // List / LargeList / FixedSizeList / Map (the dictionary `_` arm is
    // covered below with an explicit Dictionary column).
    let list = ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
    ]);
    let large = LargeListArray::try_new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 2, 2, 3])),
        Arc::new(Int64Array::from(vec![Some(7), Some(8), Some(9)])),
        None,
    )
    .unwrap();
    let fixed = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        2,
        Arc::new(Int64Array::from(vec![
            Some(4),
            None,
            Some(6),
            Some(7),
            Some(8),
            Some(9),
        ])),
        None,
    );
    // MapArray::new 的 field 参数是 **entries 字段**（Struct 类型，arrow
    // map_array.rs 内部构造同款）；batch schema 的外层 Map 字段由
    // batch_from 从 col.data_type() 自动推导。
    let entries_field = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ])),
        false,
    );
    let entries: StructArray = vec![
        (
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("value", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        ),
    ]
    .into();
    let map = MapArray::new(
        Arc::new(entries_field),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 2, 3])),
        entries,
        None,
        false, // ordered — matches the `DataType::Map(.., false)` field
    );
    let batch = batch_from(vec![
        Arc::new(list),
        Arc::new(large),
        Arc::new(fixed),
        Arc::new(map),
    ]);
    assert!(content_bytes(&batch) > 0);

    // Dictionary column → the `_` arm (upper-bound estimate).
    let dict: DictionaryArray<Int32Type> =
        DictionaryArray::from_iter(vec![Some("a"), None, Some("b")]);
    let batch = batch_from(vec![Arc::new(dict)]);
    assert!(content_bytes(&batch) > 0);
}

/// Build a batch whose schema is derived from the columns' data types.
fn batch_from(columns: Vec<ArrayRef>) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| Field::new(format!("c{i}"), col.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).unwrap()
}
