use crate::match_engine::{AsofLookup, JoinKey, Value};
use crate::window::buffer::Window;
use crate::window::buffer::types::AppendOutcome;
use crate::window::buffer::types::WindowParams;
use crate::window::buffer::{content_bytes, events_bytes};
use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
use wf_lang::ast::FieldRef;

use crate::window::RuleFanout;
use crate::window::WindowProgress;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn test_schema_no_time() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]))
}

fn make_batch(schema: &SchemaRef, times: &[i64], values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn make_batch_no_time(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .unwrap()
}

fn test_config(max_bytes: usize) -> WindowConfig {
    WindowConfig {
        name: "test".into(),
        mode: wf_config::DistMode::Local,
        max_window_bytes: max_bytes.into(),
        over_cap: Duration::from_secs(3600).into(),
        evict_policy: wf_config::EvictPolicy::TimeFirst,
        watermark: Duration::from_secs(5).into(),
        allowed_lateness: Duration::from_secs(0).into(),
        late_policy: wf_config::LatePolicy::Drop,
        table: None,
    }
}

fn test_window(over_secs: u64, max_bytes: usize) -> Window {
    let schema = test_schema();
    Window::new(
        WindowParams {
            name: "test_win".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(over_secs),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(max_bytes),
    )
}

// -- 1. append_and_evict_expired ----------------------------------------

#[test]
fn append_and_evict_expired() {
    let win = test_window(10, usize::MAX);
    let schema = win.schema().clone();

    let t1 = 1_000_000_000; // 1 s
    let t2 = 5_000_000_000; // 5 s
    let t3 = 12_000_000_000; // 12 s

    win.append(make_batch(&schema, &[t1], &[100])).unwrap();
    win.append(make_batch(&schema, &[t2], &[200])).unwrap();
    win.append(make_batch(&schema, &[t3], &[300])).unwrap();
    assert_eq!(win.batch_count(), 3);
    assert_eq!(win.total_rows(), 3);

    // cutoff = 12s - 10s = 2s → batch1 (max=1s) < 2s → evicted
    win.evict_expired(12_000_000_000);
    assert_eq!(win.batch_count(), 2);
    assert_eq!(win.total_rows(), 2);

    // cutoff = 16s - 10s = 6s → batch2 (max=5s) < 6s → evicted
    win.evict_expired(16_000_000_000);
    assert_eq!(win.batch_count(), 1);
    assert_eq!(win.total_rows(), 1);
}

// -- 2. snapshot_is_independent_of_mutations ----------------------------

#[test]
fn snapshot_is_independent_of_mutations() {
    let win = test_window(60, usize::MAX);
    let schema = win.schema().clone();

    win.append(make_batch(&schema, &[1_000_000_000], &[100]))
        .unwrap();
    win.append(make_batch(&schema, &[2_000_000_000], &[200]))
        .unwrap();

    let snap = win.snapshot();
    assert_eq!(snap.len(), 2);

    // Mutate the window after snapshot.
    win.append(make_batch(&schema, &[3_000_000_000], &[300]))
        .unwrap();
    assert_eq!(win.batch_count(), 3);

    // Snapshot is unchanged.
    assert_eq!(snap.len(), 2);
    assert_eq!(snap[0].num_rows(), 1);
    assert_eq!(snap[1].num_rows(), 1);
}

// -- 3. empty_batch_is_skipped ------------------------------------------

#[test]
fn empty_batch_is_skipped() {
    let win = test_window(60, usize::MAX);
    let schema = win.schema().clone();

    win.append(make_batch(&schema, &[], &[])).unwrap();
    assert!(win.is_empty());
    assert_eq!(win.total_rows(), 0);
    assert_eq!(win.memory_usage(), 0);
}

// -- 4. schema_mismatch_rejected ----------------------------------------

#[test]
fn schema_mismatch_rejected() {
    let win = test_window(60, usize::MAX);

    let wrong_schema = Arc::new(Schema::new(vec![Field::new(
        "different",
        DataType::Int64,
        false,
    )]));
    let wrong_batch = RecordBatch::try_new(
        wrong_schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    assert!(win.append(wrong_batch).is_err());
}

// -- 5. memory_eviction_on_append ---------------------------------------

#[test]
fn memory_eviction_on_append() {
    let schema = test_schema();

    // Measure the size of one batch.
    let probe = make_batch(&schema, &[1_000_000_000], &[100]);
    let one_batch_size = content_bytes(&probe);

    // Allow room for exactly 2 batches.
    let max_bytes = one_batch_size * 2;
    let win = Window::new(
        WindowParams {
            name: "mem_win".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(max_bytes),
    );

    win.append(probe).unwrap();
    assert_eq!(win.batch_count(), 1);

    win.append(make_batch(win.schema(), &[2_000_000_000], &[200]))
        .unwrap();
    assert_eq!(win.batch_count(), 2);

    // Third batch exceeds budget → oldest evicted.
    win.append(make_batch(win.schema(), &[3_000_000_000], &[300]))
        .unwrap();
    assert_eq!(win.batch_count(), 2);
    assert!(win.memory_usage() <= max_bytes);
}

/// Per-window memory eviction must respect the consumption floor: a batch a
/// live consumer has not yet acked is never dropped on append, even when the
/// window exceeds `max_window_bytes`. This is the per-window analogue of the
/// evictor's floor-respecting sweep — the q3 root cause (append dropped the
/// oldest batch regardless of the pull rule's read cursor).
#[test]
fn memory_eviction_respects_ack_floor() {
    let schema = test_schema();
    let probe = make_batch(&schema, &[1_000_000_000], &[100]);
    let one_batch_size = content_bytes(&probe);
    let max_bytes = one_batch_size * 2;
    let win = Window::new(
        WindowParams {
            name: "mem_ack".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(max_bytes),
    );

    // A live consumer that has not acked anything yet (floor = 0).
    let progress = Arc::new(WindowProgress::new());
    win.set_progress(Arc::clone(&progress));
    let slot = progress.register();

    // Fill to exactly the 2-batch budget.
    win.append(probe.clone()).unwrap(); // seq 0
    win.append(make_batch(win.schema(), &[2_000_000_000], &[200]))
        .unwrap(); // seq 1
    assert_eq!(win.batch_count(), 2);

    // Third append exceeds the budget, but the oldest batch (seq 0) is unacked
    // (floor = 0), so nothing may be evicted → the window transiently exceeds
    // `max_window_bytes` rather than dropping unread data.
    win.append(make_batch(win.schema(), &[3_000_000_000], &[300]))
        .unwrap(); // seq 2
    assert_eq!(
        win.batch_count(),
        3,
        "unacked batches must survive per-window eviction"
    );

    // Consumer acks past the first two batches (floor = 2); a further append
    // may now reclaim seq 0 and seq 1, but must keep seq 2 (still unacked).
    slot.store(2, Ordering::Release);
    win.append(make_batch(win.schema(), &[4_000_000_000], &[400]))
        .unwrap(); // seq 3
    assert_eq!(
        win.batch_count(),
        2,
        "only acked batches (seq 0,1) should be reclaimed; seq 2,3 survive"
    );
}

/// Time eviction must bump the content generation so a cached `window.has()`
/// distinct-value set invalidates (otherwise it goes stale after a sweep).
#[test]
fn eviction_bumps_generation() {
    let win = test_window(10, usize::MAX);
    let schema = win.schema().clone();

    let g0 = win.generation();
    win.append(make_batch(&schema, &[1_000_000_000], &[100]))
        .unwrap();
    let g1 = win.generation();
    assert!(g1 > g0, "append bumps generation");

    // cutoff = 12s - 10s = 2s; batch max=1s < 2s → evicted (acked floor = MAX).
    win.evict_expired(12_000_000_000);
    let g2 = win.generation();
    assert!(g2 > g1, "time eviction must bump generation");

    // evict_oldest_acked must too.
    win.append(make_batch(&schema, &[20_000_000_000], &[200]))
        .unwrap();
    let g3 = win.generation();
    assert!(win.evict_oldest_acked(u64::MAX).is_some());
    assert!(
        win.generation() > g3,
        "acked memory eviction must bump generation"
    );
}

// -- 6. no_time_col_window ----------------------------------------------

#[test]
fn no_time_col_window() {
    let schema = test_schema_no_time();
    let win = Window::new(
        WindowParams {
            name: "output_win".into(),
            schema: schema.clone(),
            time_col_index: None,
            over: Duration::from_secs(60),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(usize::MAX),
    );

    win.append(make_batch_no_time(&schema, &[100, 200]))
        .unwrap();
    assert_eq!(win.batch_count(), 1);
    assert_eq!(win.total_rows(), 2);

    // evict_expired is no-op for no-time-column windows.
    win.evict_expired(i64::MAX);
    assert_eq!(win.batch_count(), 1);
    assert_eq!(win.total_rows(), 2);
}

// -- 7. evict_on_empty_window_is_noop -----------------------------------

#[test]
fn evict_on_empty_window_is_noop() {
    let win = test_window(60, usize::MAX);
    win.evict_expired(i64::MAX);
    assert!(win.is_empty());
}

// -- 8. memory_usage_tracks_correctly -----------------------------------

#[test]
fn memory_usage_tracks_correctly() {
    let win = test_window(3600, usize::MAX);
    let schema = win.schema().clone();
    assert_eq!(win.memory_usage(), 0);

    let b1 = make_batch(&schema, &[1_000_000_000], &[100]);
    let b1_size = content_bytes(&b1);
    win.append(b1).unwrap();
    assert_eq!(win.memory_usage(), b1_size);

    let b2 = make_batch(&schema, &[2_000_000_000, 3_000_000_000], &[200, 300]);
    let b2_size = content_bytes(&b2);
    win.append(b2).unwrap();
    assert_eq!(win.memory_usage(), b1_size + b2_size);
}

// -- 9. multi_row_batch_time_range --------------------------------------

#[test]
fn multi_row_batch_time_range() {
    let win = test_window(10, usize::MAX);
    let schema = win.schema().clone();

    // Rows at 1s, 5s, 8s — batch max time is 8s.
    win.append(make_batch(
        &schema,
        &[1_000_000_000, 5_000_000_000, 8_000_000_000],
        &[10, 20, 30],
    ))
    .unwrap();
    assert_eq!(win.batch_count(), 1);

    // cutoff = 15s - 10s = 5s → batch max=8s >= 5s → NOT evicted
    win.evict_expired(15_000_000_000);
    assert_eq!(win.batch_count(), 1);

    // cutoff = 19s - 10s = 9s → batch max=8s < 9s → evicted
    win.evict_expired(19_000_000_000);
    assert_eq!(win.batch_count(), 0);
}

// -- 10. append_with_watermark_on_time ------------------------------------

#[test]
fn append_with_watermark_on_time() {
    // watermark delay = 5s, allowed_lateness = 0s
    let win = test_window(3600, usize::MAX);
    let schema = win.schema().clone();

    // Initial watermark is i64::MIN. Batch at 10s:
    //   watermark = max(MIN, 10s - 5s) = 5s
    //   min_event_time(10s) >= 5s → on time
    let outcome = win
        .append_with_watermark(make_batch(&schema, &[10_000_000_000], &[1]))
        .unwrap();
    assert!(matches!(outcome, AppendOutcome::Appended));
    assert_eq!(win.batch_count(), 1);
    assert_eq!(win.watermark_nanos(), 5_000_000_000);
}

// -- 11. append_with_watermark_drop_late ----------------------------------

#[test]
fn append_with_watermark_drop_late() {
    // watermark delay = 5s, allowed_lateness = 0s, late_policy = Drop
    let win = test_window(3600, usize::MAX);
    let schema = win.schema().clone();

    // Send fresh batch at 20s → watermark = 15s
    win.append_with_watermark(make_batch(&schema, &[20_000_000_000], &[1]))
        .unwrap();
    assert_eq!(win.watermark_nanos(), 15_000_000_000);

    // Send old batch at 5s → 5s < 15s → DroppedLate
    let outcome = win
        .append_with_watermark(make_batch(&schema, &[5_000_000_000], &[2]))
        .unwrap();
    assert!(matches!(outcome, AppendOutcome::DroppedLate));
    // Only the first batch should be in the window.
    assert_eq!(win.batch_count(), 1);
}

// -- 12. watermark_advances_monotonically ---------------------------------

#[test]
fn watermark_advances_monotonically() {
    let win = test_window(3600, usize::MAX);
    let schema = win.schema().clone();

    // Batch at 20s → watermark = 15s
    win.append_with_watermark(make_batch(&schema, &[20_000_000_000], &[1]))
        .unwrap();
    assert_eq!(win.watermark_nanos(), 15_000_000_000);

    // Batch at 10s (on-time since 10s >= 15s - 0s is false... wait:
    //   10s < 15s → late → DroppedLate). The watermark should NOT regress.
    //   candidate = 10s - 5s = 5s; max(15s, 5s) = 15s → unchanged
    let _ = win
        .append_with_watermark(make_batch(&schema, &[10_000_000_000], &[2]))
        .unwrap();
    assert_eq!(win.watermark_nanos(), 15_000_000_000);

    // Batch at 30s → watermark = max(15s, 25s) = 25s
    win.append_with_watermark(make_batch(&schema, &[30_000_000_000], &[3]))
        .unwrap();
    assert_eq!(win.watermark_nanos(), 25_000_000_000);
}

// -- 13. append_with_watermark_schema_mismatch_rejected --------------------

#[test]
fn append_with_watermark_schema_mismatch_rejected() {
    let win = test_window(3600, usize::MAX);

    let wrong_schema = Arc::new(Schema::new(vec![Field::new(
        "different",
        DataType::Int64,
        false,
    )]));
    let wrong_batch = RecordBatch::try_new(
        wrong_schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    // Must return Err, not panic.
    assert!(win.append_with_watermark(wrong_batch).is_err());
}

// -- 14. read_since_normal -----------------------------------------------

#[test]
fn read_since_normal() {
    let win = test_window(3600, usize::MAX);
    let schema = win.schema().clone();

    assert_eq!(win.next_seq(), 0);
    win.append(make_batch(&schema, &[1_000_000_000], &[100]))
        .unwrap();
    win.append(make_batch(&schema, &[2_000_000_000], &[200]))
        .unwrap();
    win.append(make_batch(&schema, &[3_000_000_000], &[300]))
        .unwrap();
    assert_eq!(win.next_seq(), 3);

    // Read from cursor 0 → all 3 batches
    let (batches, cursor, gap) = win.read_since(0);
    assert_eq!(batches.len(), 3);
    assert_eq!(cursor, 3);
    assert!(!gap);

    // Read from cursor 1 → last 2 batches
    let (batches, cursor, gap) = win.read_since(1);
    assert_eq!(batches.len(), 2);
    assert_eq!(cursor, 3);
    assert!(!gap);

    // Read from cursor 3 → no new batches
    let (batches, cursor, gap) = win.read_since(3);
    assert!(batches.is_empty());
    assert_eq!(cursor, 3);
    assert!(!gap);
}

// -- 15. read_since_gap_detection ----------------------------------------

#[test]
fn read_since_gap_detection() {
    let schema = test_schema();
    let probe = make_batch(&schema, &[1_000_000_000], &[100]);
    let one_batch_size = content_bytes(&probe);
    // Allow room for exactly 2 batches → oldest evicted when 3rd arrives.
    let max_bytes = one_batch_size * 2;
    let win = Window::new(
        WindowParams {
            name: "gap_win".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(max_bytes),
    );

    win.append(probe).unwrap(); // seq 0
    win.append(make_batch(win.schema(), &[2_000_000_000], &[200]))
        .unwrap(); // seq 1
    win.append(make_batch(win.schema(), &[3_000_000_000], &[300]))
        .unwrap(); // seq 2 → seq 0 evicted

    // Cursor 0 was evicted → gap
    let (batches, cursor, gap) = win.read_since(0);
    assert!(gap);
    assert_eq!(batches.len(), 2); // seq 1 and 2
    assert_eq!(cursor, 3);
}

// -- 16. read_since_empty_window -----------------------------------------

#[test]
fn read_since_empty_window() {
    let win = test_window(3600, usize::MAX);
    let (batches, cursor, gap) = win.read_since(0);
    assert!(batches.is_empty());
    assert_eq!(cursor, 0);
    assert!(!gap);
}

// -- 17. read_since_cursor_ahead -----------------------------------------

#[test]
fn read_since_cursor_ahead() {
    let win = test_window(3600, usize::MAX);
    let schema = win.schema().clone();
    win.append(make_batch(&schema, &[1_000_000_000], &[100]))
        .unwrap();

    // Cursor ahead of newest → no data, no gap
    let (batches, cursor, gap) = win.read_since(999);
    assert!(batches.is_empty());
    assert_eq!(cursor, 999);
    assert!(!gap);
}

// -- 18. content_bytes_ipc_roundtrip_does_not_inflate -----------------------

/// #18 regression: an object/struct-heavy batch that Arrow IPC decode inflates
/// to several times its content (padded buffer allocations) must be accounted
/// by *content* bytes, so a single big frame doesn't blow past `max_window_bytes`
/// and get silently dropped by window memory eviction.
#[test]
fn content_bytes_ipc_roundtrip_does_not_inflate() {
    let n = 100_000usize;
    let obj_field = Field::new(
        "obj",
        DataType::Struct(Fields::from(vec![
            Field::new("sip", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ])),
        false,
    );
    let schema = Arc::new(Schema::new(vec![obj_field]));

    let sip: StringArray = (0..n).map(|_| Some("10.0.0.1")).collect();
    let score: Int64Array = (0..n).map(|_| Some(42)).collect();
    let obj = StructArray::from(vec![
        (
            Arc::new(Field::new("sip", DataType::Utf8, false)),
            Arc::new(sip) as ArrayRef,
        ),
        (
            Arc::new(Field::new("score", DataType::Int64, false)),
            Arc::new(score) as ArrayRef,
        ),
    ]);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(obj)]).unwrap();

    // Round-trip through Arrow IPC — the same path the engine uses between the
    // producer and the rule window.
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let mut reader = StreamReader::try_new(Cursor::new(&buf), None).unwrap();
    let decoded = reader.next().unwrap().expect("one decoded batch");

    let content = content_bytes(&decoded);
    let inflated = decoded.get_array_memory_size();

    // Content ≈ 100k rows × (8 utf8 + 4 offset + 8 int64) = 2.0MB. It must
    // track the actual data, not the padded allocations IPC decode produces.
    let expected = n * (8 + 4 + 8);
    assert!(
        content.abs_diff(expected) <= expected / 10,
        "content bytes {content} should track actual data (~{expected}), got inflated allocation {inflated}"
    );
    assert!(
        inflated > content * 3,
        "IPC decode should inflate well beyond content bytes: inflated={inflated}, content={content}"
    );
}

// -- events_bytes: parsed-event memory accounting ----------------------------

/// An `object` field is a JSON-encoded Utf8 column; parsing it into
/// `Value::Object(HashMap)` allocates per-entry key/bucket/hash overhead, so
/// the retained footprint is many× the JSON string for small objects. The
/// window retains both the Arrow batch and these parsed events, so
/// `events_bytes` must push the window's byte accounting well past
/// `content_bytes` or eviction fires at the wrong water level
/// (wp-labs/wp-reactor#20).
#[test]
fn events_bytes_tracks_object_field_footprint() {
    use crate::match_engine::batch_to_events;

    let n = 10_000usize;
    let obj_field = Field::new("conn_info", DataType::Utf8, false).with_metadata(
        std::collections::HashMap::from([("wf.wfl.field_type".to_string(), "object".to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![obj_field]));

    let short_json = r#"{"sip":"10.0.0.1","detail":"a","nested":{"k":1,"s":"b"}}"#;
    // Same key set, only the `detail` value lengthens → same table capacities,
    // so any estimate increase is strictly the heap string bytes.
    let long_json = format!(
        r#"{{"sip":"10.0.0.1","detail":"{}","nested":{{"k":1,"s":"b"}}}}"#,
        "x".repeat(200)
    );

    let json_bytes = short_json.len();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(
            (0..n)
                .map(|_| Some(short_json.to_string()))
                .collect::<StringArray>(),
        )],
    )
    .unwrap();
    let parsed: Vec<Arc<crate::match_engine::Event>> =
        batch_to_events(&batch).into_iter().map(Arc::new).collect();
    let est = events_bytes(&parsed);

    // The parsed HashMap representation must exceed the raw JSON content (the
    // #20 undercount: content_bytes alone reports only the string bytes)...
    assert!(
        est > n * json_bytes,
        "events_bytes {est} should exceed JSON content {} (~{} bytes/event)",
        n * json_bytes,
        est / n
    );
    // ...and each small-object event carries a bounded per-key overhead — a
    // sane per-event cap (well under the 256MB window caps in the eps_obj
    // scenario) without drifting into IPC-style multi-hundred× inflation.
    let per_event = est / n;
    assert!(
        (100..4096).contains(&per_event),
        "per-event estimate {per_event} should be sane for a ~{json_bytes}B JSON object"
    );

    // Heap-allocated (long) strings must be charged, so a long detail field
    // raises the per-event estimate.
    let batch_long = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(
            (0..n)
                .map(|_| Some(long_json.clone()))
                .collect::<StringArray>(),
        )],
    )
    .unwrap();
    let parsed_long: Vec<Arc<crate::match_engine::Event>> = batch_to_events(&batch_long)
        .into_iter()
        .map(Arc::new)
        .collect();
    let est_long = events_bytes(&parsed_long);
    assert!(
        est_long > est,
        "long nested string should raise the estimate: long={est_long} short={est}"
    );
}

/// `Value::Array` must be charged recursively: an object field carrying a long
/// array costs more than the same field with a short array (same key set, so
/// the map-table capacity is identical and any increase is the array itself).
#[test]
fn events_bytes_recurses_into_nested_arrays() {
    use crate::match_engine::batch_to_events;

    let n = 100usize;
    let obj_field = Field::new("conn_info", DataType::Utf8, false).with_metadata(
        std::collections::HashMap::from([("wf.wfl.field_type".to_string(), "object".to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![obj_field]));

    let short = r#"{"tags":["a","b"]}"#;
    let long = format!(
        r#"{{"tags":[{}]}}"#,
        (0..50).map(|_| "\"x\"").collect::<Vec<_>>().join(",")
    );

    let est_short = events_bytes(
        &batch_to_events(
            &RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(
                    (0..n).map(|_| Some(short)).collect::<StringArray>(),
                )],
            )
            .unwrap(),
        )
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>(),
    );
    let est_long = events_bytes(
        &batch_to_events(
            &RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(
                    (0..n).map(|_| Some(long.clone())).collect::<StringArray>(),
                )],
            )
            .unwrap(),
        )
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>(),
    );

    assert!(est_short > 0, "array-bearing event must be charged");
    assert!(
        est_long > est_short,
        "longer nested array should raise the estimate: long={est_long} short={est_short}"
    );
}

/// #20 regression: a window's byte accounting must include the parsed-event
/// footprint (`content_bytes` + `events_bytes`), not just the Arrow content.
///
/// Two windows with the *same* cap that fits exactly one batch's real footprint
/// (content + parsed events). The content-only accounting path retains **both**
/// batches — claiming 2×content bytes while actually holding 2×(content+events)
/// real memory (the undercount that let RSS run away). The accurate path evicts
/// down to one batch, keeping the window at or under the cap.
#[test]
fn window_evicts_on_parsed_event_footprint_not_content() {
    use crate::match_engine::batch_to_events;

    let n = 100usize;
    let obj_field = Field::new("conn_info", DataType::Utf8, false).with_metadata(
        std::collections::HashMap::from([("wf.wfl.field_type".to_string(), "object".to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        obj_field,
    ]));

    let json = r#"{"sip":"10.0.0.1","dip":"172.16.5.9","nested":{"k":1}}"#;
    let times: TimestampNanosecondArray =
        (0..n).map(|i| Some(1_000_000_000i64 + i as i64)).collect();
    let objs: StringArray = (0..n).map(|_| Some(json)).collect();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(times), Arc::new(objs)]).unwrap();
    let parsed: Vec<Arc<crate::match_engine::Event>> =
        batch_to_events(&batch).into_iter().map(Arc::new).collect();

    let content = content_bytes(&batch);
    let events = events_bytes(&parsed);
    assert!(
        events > content,
        "object fields must dominate the footprint"
    );

    // Cap fits exactly one batch's *combined* footprint. Content-only accounting
    // for two batches stays under it (the undercount); combined accounting does not.
    let cap = content + events + 10;
    assert!(
        2 * content <= cap,
        "content-only accounting should stay under cap"
    );
    assert!(content + events <= cap, "one batch's real footprint fits");
    assert!(
        2 * (content + events) > cap,
        "two batches' real footprint exceeds cap"
    );

    let make = |name: &str, cap: usize| {
        Window::new(
            WindowParams {
                name: name.into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            WindowConfig {
                name: name.into(),
                mode: DistMode::Local,
                max_window_bytes: cap.into(),
                over_cap: Duration::from_secs(3600).into(),
                evict_policy: EvictPolicy::TimeFirst,
                watermark: Duration::from_secs(0).into(),
                // Wide lateness so the second batch (same timestamp window,
                // min < first batch's advanced watermark) is not dropped as late.
                allowed_lateness: Duration::from_secs(3600).into(),
                late_policy: LatePolicy::Drop,
                table: None,
            },
        )
    };

    // Old behavior: append_parsed computes content_bytes only → undercounts →
    // retains both batches even though the real footprint is 2× the cap.
    let content_only = make("content_only", cap);
    for _ in 0..2 {
        content_only
            .append_with_watermark_parsed(batch.clone(), Arc::new(parsed.clone()))
            .unwrap();
    }
    assert_eq!(
        content_only.total_rows(),
        2 * n,
        "content-only accounting must retain both batches (the #20 undercount)"
    );
    assert!(
        content_only.memory_usage() <= cap,
        "content-only accounting reports {} <= cap (but real footprint is 2× that)",
        content_only.memory_usage()
    );

    // New behavior: byte_size includes the parsed events → eviction fires on the
    // real footprint → the window holds exactly one batch.
    let accurate = make("accurate", cap);
    for _ in 0..2 {
        accurate
            .append_with_watermark_parsed_sized(
                batch.clone(),
                Arc::new(parsed.clone()),
                content + events,
                None,
            )
            .unwrap();
    }
    assert_eq!(
        accurate.total_rows(),
        n,
        "accurate accounting must evict the oldest batch to stay under the cap"
    );
    assert!(accurate.memory_usage() <= cap);
}

/// Fast-path append (`append_with_watermark_sized`, no pre-parsed events) must
/// leave the batch's `parsed_events` *uninitialized*, so a consumer reading via
/// `events_since()` still lazily parses the real events — a later subscriber
/// (hot reload) must not see empty events for batches that arrived while the
/// window had no rule consumers.
#[test]
fn sized_append_keeps_events_lazily_parseable() {
    let schema = test_schema();
    let batch = make_batch(&schema, &[1_000_000_000, 2_000_000_000], &[42, 99]);
    let content = content_bytes(&batch);
    let cap = content + 10;

    let win = Window::new(
        WindowParams {
            name: "lazy".into(),
            schema: schema.clone(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        WindowConfig {
            name: "lazy".into(),
            mode: DistMode::Local,
            max_window_bytes: cap.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(0).into(),
            allowed_lateness: Duration::from_secs(3600).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        },
    );
    win.append_with_watermark_sized(batch, content, None)
        .unwrap();

    // events_since lazily parses the batch → real events, not empty.
    let (events_list, cursor, gap) = win.events_since(0);
    assert!(!gap, "no cursor gap");
    assert_eq!(events_list.len(), 1, "one batch of events");
    assert_eq!(
        events_list[0].len(),
        2,
        "both rows must be lazily parsed into events"
    );
    assert_eq!(cursor, 1, "cursor advances past the batch");
}

/// Regression: the columnar/deferred append path (`append_with_watermark_sized`,
/// `events = None`) must persist the parse-side precomputed `shard_rows` into
/// the window log. The pull-model rule tasks read their per-shard row subset
/// from `read_since_with_shard(shard_index)` — if the `(None, _)` arm of
/// `append_with_watermark_inner` dropped `shard_rows` (the Q2 30M pull
/// over-production bug, ~9×), every pull shard would process the WHOLE batch.
#[test]
fn sized_append_persists_shard_rows_for_pull() {
    let schema = test_schema();
    // 3 rows; shard 0 owns rows {0, 2}, shard 1 owns row {1}.
    let batch = make_batch(
        &schema,
        &[1_000_000_000, 2_000_000_000, 3_000_000_000],
        &[42, 99, 7],
    );
    let content = content_bytes(&batch);

    let win = Window::new(
        WindowParams {
            name: "sharded".into(),
            schema: schema.clone(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        WindowConfig {
            name: "sharded".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(0).into(),
            allowed_lateness: Duration::from_secs(3600).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        },
    );
    let shard_rows: Option<Arc<Vec<Vec<u32>>>> = Some(Arc::from(vec![vec![0u32, 2], vec![1u32]]));
    win.append_with_watermark_sized(batch, content, shard_rows)
        .unwrap();

    // Shard 0 must pull only its own rows {0, 2}.
    let (_batches, per_shard, _cursor, _gap) = win.read_since_with_shard(0, Some(0));
    assert_eq!(per_shard.len(), 1, "one batch");
    assert_eq!(
        per_shard[0].as_ref().map(|v| v.as_slice()),
        Some(&[0u32, 2][..]),
        "shard 0 owns rows {{0, 2}}"
    );

    // Shard 1 must pull only row {1}.
    let (_batches, per_shard, _cursor, _gap) = win.read_since_with_shard(0, Some(1));
    assert_eq!(
        per_shard[0].as_ref().map(|v| v.as_slice()),
        Some(&[1u32][..]),
        "shard 1 owns row {{1}}"
    );

    // Unpartitioned pull (shard_index = None) sees the whole batch.
    let (_batches, per_shard, _cursor, _gap) = win.read_since_with_shard(0, None);
    assert!(
        per_shard[0].is_none(),
        "unsharded pull gets no row subset (processes whole batch)"
    );
}

/// M1 pull-model invariant (P2 zero re-partition) across **multiple batches
/// and multiple shards**: `read_since_with_shard(shard_index)` returns exactly
/// the per-shard row subset stored in the window log, and the cross-shard
/// (batch × row) union must cover every row **exactly once** — no loss, no
/// duplication. The partition is computed once at write time (here via the
/// production `precompute_shard_rows`) and each shard pulls only its own slice.
#[test]
fn pull_sharded_multi_batch_zero_repartition_union() {
    let schema = test_schema(); // ts(col0), value(col1)
    let fanout = RuleFanout::new();
    fanout.register_window_sharding(
        "auth_events",
        Arc::from(vec![FieldRef::Simple("value".into())].into_boxed_slice()),
        2,
    );

    let win = test_window(3600, usize::MAX);

    const NBATCH: u32 = 3;
    const NROW: u32 = 5;
    for b in 0..NBATCH {
        let times: Vec<i64> = (0..NROW)
            .map(|i| 1_700_000_000_000_000_000i64 + (b * NROW + i) as i64)
            .collect();
        let values: Vec<i64> = (0..NROW).map(|i| (b * NROW + i) as i64).collect();
        let batch = make_batch(&schema, &times, &values);
        // Parse-stage precompute: partition this batch once by the match key.
        let shard_rows = fanout
            .precompute_shard_rows("auth_events", &batch)
            .expect("sharded window has a partition");
        let size = content_bytes(&batch);
        win.append_with_watermark_sized(batch, size, Some(Arc::new(shard_rows.to_vec())))
            .unwrap();
    }

    // Every shard reads ALL batches but only its own row subset. The union
    // across shards must equal the full (batch, row) grid exactly once.
    let mut seen: std::collections::HashSet<(usize, u32)> = std::collections::HashSet::new();
    let mut duplicate = false;
    for shard in 0..2usize {
        let (batches, per_shard, cursor, gap) = win.read_since_with_shard(0, Some(shard));
        assert!(!gap, "no eviction before first read");
        assert_eq!(
            batches.len(),
            NBATCH as usize,
            "every shard sees all batches"
        );
        assert_eq!(cursor, NBATCH as u64, "cursor advances to newest+1");
        for (k, subset) in per_shard.iter().enumerate() {
            let rows = subset.as_ref().expect("shard subset present for batch {k}");
            for &r in rows.iter() {
                if !seen.insert((k, r)) {
                    duplicate = true;
                }
            }
        }
    }
    assert!(!duplicate, "each row must belong to exactly one shard");
    let mut all: Vec<(usize, u32)> = seen.into_iter().collect();
    all.sort();
    let expected: Vec<(usize, u32)> = (0..NBATCH as usize)
        .flat_map(|k| (0..NROW as usize).map(move |r| (k, r as u32)))
        .collect();
    assert_eq!(
        all, expected,
        "union of all shards covers every row exactly once (zero re-partition)"
    );
}

/// M1 regression anchor for consumption-floor safety: if a batch is evicted
/// before the pull cursor reads it, `read_since_with_shard` must report
/// `gap = true` (cursor < oldest_seq) and resume from the oldest surviving
/// batch, while still advancing the cursor to `newest + 1`. A cursor that has
/// caught up (== floor) reads cleanly with no gap.
#[test]
fn pull_gap_detected_when_batch_evicted_before_read() {
    let schema = test_schema();
    let win = test_window(3600, usize::MAX);
    for b in 0..3u32 {
        let times = vec![1_700_000_000_000_000_000i64 + b as i64; 2];
        let values = vec![10i64 + b as i64, 20 + b as i64];
        let batch = make_batch(&schema, &times, &values);
        let size = content_bytes(&batch);
        win.append_with_watermark_sized(batch, size, None).unwrap();
    }
    assert_eq!(win.batch_count(), 3, "three batches appended");

    // Drop the oldest batch (memory eviction ignores the consumption floor).
    assert!(win.evict_oldest().is_some());

    // Cursor still at 0 → 0 < oldest_seq(=1) → gap.
    let (batches, _per, cursor, gap) = win.read_since_with_shard(0, None);
    assert!(gap, "cursor 0 must detect gap after front eviction");
    assert_eq!(batches.len(), 2, "only the surviving batches are returned");
    assert_eq!(cursor, 3, "cursor still advances to newest+1");

    // A cursor that caught up to the floor reads cleanly, no gap.
    let (batches2, _per2, cursor2, gap2) = win.read_since_with_shard(1, None);
    assert!(!gap2, "cursor at floor reads without gap");
    assert_eq!(batches2.len(), 2);
    assert_eq!(cursor2, 3);
}

// -- join index ------------------------------------------------------------

#[test]
fn join_index_maintained_on_append_and_evict() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    // Append two batches with overlapping key values.
    win.append(make_batch(
        &test_schema(),
        &[1_000_000, 2_000_000],
        &[42, 43],
    ))
    .unwrap();
    win.append(make_batch(
        &test_schema(),
        &[3_000_000, 4_000_000],
        &[42, 44],
    ))
    .unwrap();

    // Lookup by key: value 42 has 2 rows, 44 has 1, 999 has none.
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(2),
        "two rows with value 42 indexed"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(44), None).map(|v| v.len()),
        Some(1),
        "one row with value 44 indexed"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(999), None).map(|v| v.len()),
        Some(0),
        "indexed but no match → empty (not None)"
    );

    // Expire all batches: over=3600s, now=4000s → cutoff=400s >> event times
    // (1-4ms), so all batches are time-evicted and index entries removed.
    win.evict_expired(4_000_000_000_000);
    assert!(
        win.join_lookup(&JoinKey::Int(42), None)
            .is_none_or(|v| v.is_empty()),
        "index cleared after eviction"
    );
}

#[test]
fn join_key_from_value_conversion() {
    use crate::match_engine::EngineHashMap;
    assert_eq!(
        JoinKey::from_value(&Value::Number(42.0)),
        Some(JoinKey::Int(42)),
        "number → Int"
    );
    assert_eq!(
        JoinKey::from_value(&Value::Str("abc".into())),
        Some(JoinKey::Str("abc".into())),
        "string → Str"
    );
    assert_eq!(
        JoinKey::from_value(&Value::Bool(true)),
        Some(JoinKey::Bool(true)),
        "bool → Bool"
    );
    assert_eq!(
        JoinKey::from_value(&Value::Array(vec![])),
        None,
        "array → None (rejected at compile time)"
    );
    assert_eq!(
        JoinKey::from_value(&Value::Object(EngineHashMap::default())),
        None,
        "object → None"
    );
}

#[test]
fn join_index_absent_without_set_join_key() {
    let win = test_window(3600, usize::MAX);
    assert!(
        win.join_lookup(&JoinKey::Int(1), None).is_none(),
        "no join index → None (caller falls back to scan)"
    );
    // The asof fast path must also fall back (not Miss) without an index: the
    // caller then runs the full timestamped scan.
    assert!(matches!(
        win.join_lookup_asof(&JoinKey::Int(1), 5_000_000_000, 0, None),
        AsofLookup::Fallback
    ));
}

#[test]
fn join_index_built_for_existing_batches_on_set_join_key() {
    let win = test_window(3600, usize::MAX);
    // Data appended before the window is configured as a join target.
    win.append(make_batch(
        &test_schema(),
        &[1_000_000, 2_000_000],
        &[42, 43],
    ))
    .unwrap();
    win.append(make_batch(&test_schema(), &[3_000_000], &[44]))
        .unwrap();
    win.set_join_key("value".into());
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(1),
        "existing rows indexed by set_join_key"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(44), None).map(|v| v.len()),
        Some(1),
        "rows from a later batch indexed"
    );
}

#[test]
fn join_index_updated_on_oldest_eviction() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    win.append(make_batch(
        &test_schema(),
        &[1_000_000, 2_000_000],
        &[42, 43],
    ))
    .unwrap();
    win.append(make_batch(
        &test_schema(),
        &[3_000_000, 4_000_000],
        &[44, 45],
    ))
    .unwrap();

    // evict_oldest (memory-pressure path) must drop the first batch's keys.
    assert!(
        win.evict_oldest().is_some(),
        "evict_oldest returns byte size"
    );
    assert!(
        win.join_lookup(&JoinKey::Int(42), None)
            .is_none_or(|v| v.is_empty()),
        "key 42 (first batch) removed after evict_oldest"
    );
    assert!(
        win.join_lookup(&JoinKey::Int(43), None)
            .is_none_or(|v| v.is_empty()),
        "key 43 (first batch) removed after evict_oldest"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(44), None).map(|v| v.len()),
        Some(1),
        "key 44 (second batch) still indexed"
    );
}

#[test]
fn join_index_duplicate_key_keeps_all_rows() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    // Two rows with the same key 42 in different batches.
    win.append(make_batch(&test_schema(), &[1_000_000], &[42]))
        .unwrap();
    win.append(make_batch(&test_schema(), &[2_000_000], &[42]))
        .unwrap();
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(2),
        "both rows with key 42 kept"
    );
    // Evict one batch → one row remains.
    win.evict_oldest();
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(1),
        "one row removed on evict, one kept"
    );
}

#[test]
fn join_index_stays_columnar_without_materializing_parsed_events() {
    // The columnar join index (set_join_key + append + lookup) must never
    // trigger `TimedBatch::events()`, so a join-target window with no rule
    // subscription keeps its batches fully columnar — the Q22 `person_events`
    // RSS win. `join_lookup` works off the `(batch, row)` locators directly.
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    win.append(make_batch(
        &test_schema(),
        &[1_000_000, 2_000_000],
        &[42, 43],
    ))
    .unwrap();

    // Columnar lookup still works.
    let rows = win
        .join_lookup(&JoinKey::Int(42), None)
        .expect("indexed window should return rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].field_value("value"), Some(Value::Number(42.0)));

    // And the batch's `parsed_events` stayed uninitialized.
    assert!(
        !win.any_parsed_events_materialized(),
        "join index must not materialize parsed events"
    );
}

#[test]
fn join_lookup_asof_max_fast_path() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    // Same key 42 at ts=1s and ts=3s → per-key max_ts = 3s.
    win.append(make_batch(&test_schema(), &[1_000_000_000], &[42]))
        .unwrap();
    win.append(make_batch(&test_schema(), &[3_000_000_000], &[42]))
        .unwrap();

    // Fast-path hit: max_ts=3s falls within [2s, 5s] → returns the latest row.
    match win.join_lookup_asof(&JoinKey::Int(42), 5_000_000_000, 2_000_000_000, None) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(3_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }

    // max_ts too old (3s < min_ts=4s) → Miss (no scan needed).
    assert!(matches!(
        win.join_lookup_asof(&JoinKey::Int(42), 5_000_000_000, 4_000_000_000, None),
        AsofLookup::Miss
    ));
    // Miss must be consistent with the fallback scan: every candidate ts is
    // below min_ts, so `find_asof_row` would also return `None`.
    let cands = win.join_lookup_timestamped(&JoinKey::Int(42), None).unwrap();
    assert!(
        cands.iter().all(|(ts, _)| *ts < 4_000_000_000),
        "Miss implies all candidate timestamps are below the asof lower bound"
    );

    // max_ts too new (3s > event_time=2s): a smaller row (ts=1s) qualifies, so
    // the index scans and returns it directly — no caller-side fallback scan.
    match win.join_lookup_asof(&JoinKey::Int(42), 2_000_000_000, 0, None) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(1_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit for max_ts > event_time, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit for max_ts > event_time, got Fallback"),
    }

    // Unknown key → Miss.
    assert!(matches!(
        win.join_lookup_asof(&JoinKey::Int(99), 5_000_000_000, 0, None),
        AsofLookup::Miss
    ));

    // Boundary: max_ts == min_ts (3s == 3s) → still a hit (inclusive lower bound).
    match win.join_lookup_asof(&JoinKey::Int(42), 5_000_000_000, 3_000_000_000, None) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(3_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit at inclusive lower bound, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit at inclusive lower bound, got Fallback"),
    }

    // Boundary: max_ts == event_time (3s == 3s) → still a hit (inclusive upper bound).
    match win.join_lookup_asof(&JoinKey::Int(42), 3_000_000_000, 2_000_000_000, None) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(3_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit at inclusive upper bound, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit at inclusive upper bound, got Fallback"),
    }
}

#[test]
fn join_lookup_asof_max_scans_when_max_is_future() {
    // When a key's running max_ts is newer than the event time (person X has a
    // future event in the same/next bucket), the index must still return the
    // greatest timestamp <= event_time — without falling back to the caller's
    // full candidate scan. Rows are appended out of time order within/across
    // batches, so the scan must not assume sorted order.
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    // Key 42 at ts = 5s, 1s, 9s, 3s (append order != ts order; max_ts = 9s).
    for ts in [
        5_000_000_000i64,
        1_000_000_000,
        9_000_000_000,
        3_000_000_000,
    ] {
        win.append(make_batch(&test_schema(), &[ts], &[42]))
            .unwrap();
    }

    // event_time=7s, min_ts=0: max_ts(9s) > 7s → scan picks 5s (greatest ≤ 7s).
    match win.join_lookup_asof(&JoinKey::Int(42), 7_000_000_000, 0, None) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(5_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }

    // Tight window [4s, 6s]: 5s qualifies, 3s/1s below, 9s above → 5s.
    match win.join_lookup_asof(&JoinKey::Int(42), 6_000_000_000, 4_000_000_000, None) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(5_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }

    // No candidate in [8s, 9s] below event_time=9s: max_ts==9s (== event_time)
    // is the fast-path hit, not the scan path.
    match win.join_lookup_asof(&JoinKey::Int(42), 9_000_000_000, 8_000_000_000, None) {
        AsofLookup::Hit(row) => {
            assert_eq!(row.field_value("ts"), Some(Value::Number(9_000_000_000.0)));
        }
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }

    // No candidate in [7.5s, 8.5s] (max_ts=9s > event_time=8.5s, all rows ≤7.5s
    // or =9s are outside [7.5s,8.5s]) → Miss.
    assert!(matches!(
        win.join_lookup_asof(&JoinKey::Int(42), 8_500_000_000, 7_500_000_000, None),
        AsofLookup::Miss
    ));
}

#[test]
fn join_lookup_asof_max_miss_without_timestamps() {
    // A join-indexed window with no time column has no per-row timestamps, so
    // the asof fast path must report `Miss` (the timestamped scan would also
    // return no candidates, so `find_asof_row` would be `None`).
    let schema = test_schema_no_time();
    let win = Window::new(
        WindowParams {
            name: "no_time".into(),
            schema: schema.clone(),
            time_col_index: None,
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(usize::MAX),
    );
    win.set_join_key("value".into());
    win.append(make_batch_no_time(&schema, &[42])).unwrap();

    assert!(matches!(
        win.join_lookup_asof(&JoinKey::Int(42), 5_000_000_000, 0, None),
        AsofLookup::Miss
    ));
}

// -- Eager-drop regression (window log reclamation) ------------------------
//
// History: A.1 replaced the `VecDeque<TimedBatch>` window log with a lock-free
// `SkipMap<u64, TimedBatch>`, whose `remove` only unlinks the node and defers
// the value's destructor into crossbeam-epoch garbage bags. A quiet system
// never advanced the epoch, so evicted batches — including their pre-parsed
// `Arc<Vec<Arc<Event>>>` — stayed resident while window gauges read healthy
// (the 2026-08-16 RSS regression: ~6M evicted events / ~2.3 GiB retained).
//
// The log is now a `RwLock<BTreeMap<u64, TimedBatch>>`: removal returns the
// owned value and dropping it destroys the batch eagerly, with no collector
// to drive.
//
// The contract under test: once a batch has been evicted (gone from
// `batch_count`/`total_rows`), the engine holds no reference to its parsed
// events the moment the eviction call returns.

fn parsed_events(n: usize) -> Arc<Vec<Arc<crate::match_engine::Event>>> {
    Arc::new(
        (0..n)
            .map(|_| {
                Arc::new(crate::match_engine::Event {
                    fields: Default::default(),
                })
            })
            .collect(),
    )
}

/// Eviction drops the batch synchronously: by the time the eviction call
/// returns, the given events `Arc` must be referenced by the test alone.
/// No spins, no collector — a strict immediate assertion.
fn assert_events_released(events: &Arc<Vec<Arc<crate::match_engine::Event>>>) {
    assert_eq!(
        Arc::strong_count(events),
        1,
        "evicted batch's parsed events must be dropped by the eviction call \
         itself, not retained (deferred reclamation regression)"
    );
}

/// Time eviction must release the evicted batch's parsed events.
#[test]
fn time_evicted_batch_releases_parsed_events() {
    let win = test_window(10, usize::MAX);
    let schema = win.schema().clone();

    let first = parsed_events(3);
    win.append_parsed_sized(
        make_batch(&schema, &[1_000_000_000], &[100]),
        Arc::clone(&first),
        4096,
        None,
    )
    .unwrap();
    win.append_parsed_sized(
        make_batch(&schema, &[12_000_000_000], &[300]),
        parsed_events(3),
        4096,
        None,
    )
    .unwrap();
    assert_eq!(win.batch_count(), 2);

    // cutoff = 12s - 10s = 2s → batch1 (max=1s) evicted.
    win.evict_expired(12_000_000_000);
    assert_eq!(win.batch_count(), 1);

    assert_events_released(&first);
}

/// Memory eviction (append-side pressure) must release them too.
#[test]
fn memory_evicted_batch_releases_parsed_events() {
    let win = test_window(3600, 6144);
    let schema = win.schema().clone();

    let first = parsed_events(2);
    win.append_parsed_sized(
        make_batch(&schema, &[1_000_000_000], &[100]),
        Arc::clone(&first),
        4096,
        None,
    )
    .unwrap();
    // Second 4KiB batch pushes current_bytes (8192) over max (6144) → first
    // evicted; the remaining 4096 is back under the cap so eviction stops.
    win.append_parsed_sized(
        make_batch(&schema, &[2_000_000_000], &[200]),
        parsed_events(2),
        4096,
        None,
    )
    .unwrap();
    assert_eq!(win.batch_count(), 1);

    assert_events_released(&first);
}

/// `evict_oldest` (explicit memory-pressure path) must release them too.
#[test]
fn evict_oldest_releases_parsed_events() {
    let win = test_window(3600, usize::MAX);
    let schema = win.schema().clone();

    let first = parsed_events(2);
    win.append_parsed_sized(
        make_batch(&schema, &[1_000_000], &[42]),
        Arc::clone(&first),
        4096,
        None,
    )
    .unwrap();
    win.append_parsed_sized(
        make_batch(&schema, &[2_000_000], &[43]),
        parsed_events(2),
        4096,
        None,
    )
    .unwrap();

    win.evict_oldest();
    assert_eq!(win.batch_count(), 1);

    assert_events_released(&first);
}

// ---------------------------------------------------------------------------
// Concurrency diagnostics (q5 pull-mode freeze): these tests reproduce the
// lock-shape of the freeze — 30 pull rule tasks share the window log read lock
// while the single-writer actor takes the write lock on append — and assert the
// writer is not starved.
// ---------------------------------------------------------------------------

/// A platform `RwLock` must not starve a writer under a sustained read burst:
/// q5 runs 30 pull rule tasks that read the shared window log concurrently
/// against one actor writer. If the writer starves, append stalls, the 64 MiB
/// window byte budget exhausts, and the whole pipeline freezes. This test
/// measures the writer's worst-case wait under a 30-reader burst.
#[test]
fn rwlock_writer_not_starved_by_readers() {
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::thread;
    use std::time::{Duration, Instant};

    let lock = Arc::new(RwLock::new(0u64));
    let stop = Arc::new(AtomicBool::new(false));

    // 30 readers: brief read + a short "rule processing" pause, mirroring the
    // pull-loop's read_since_with_shard followed by batch processing.
    let mut readers = Vec::new();
    for _ in 0..30 {
        let lock = Arc::clone(&lock);
        let stop = Arc::clone(&stop);
        readers.push(thread::spawn(move || {
            while !stop.load(AtomicOrdering::Relaxed) {
                let _g = lock.read().unwrap();
                thread::sleep(Duration::from_micros(50));
            }
        }));
    }

    let mut max_wait = Duration::ZERO;
    let mut total = Duration::ZERO;
    let n = 200u64;
    for _ in 0..n {
        let t0 = Instant::now();
        let mut w = lock.write().unwrap();
        let wait = t0.elapsed();
        max_wait = max_wait.max(wait);
        total += wait;
        *w += 1;
        drop(w);
    }

    stop.store(true, AtomicOrdering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    let avg = total / n as u32;
    // If the platform starves the writer, max_wait grows unboundedly under a
    // continuous read burst. 500ms is generous for a non-starving lock.
    assert!(
        max_wait < Duration::from_millis(500),
        "writer starved: max write-lock wait {max_wait:?} (avg {avg:?})"
    );
}

/// `read_since_with_shard` must return the correct per-shard row subset. This
/// also pins the current behaviour: the returned `Arc<Vec<u32>>` is a **deep
/// copy** of the stored subset (the stored type is `Arc<Vec<Vec<u32>>>`, so a
/// zero-copy `Arc::clone` of the inner list is not yet possible). The deep copy
/// runs inside the log read lock; under 30 pull tasks it lengthens every read
/// critical section and amplifies the q5 pull-freeze.
#[test]
fn read_since_with_shard_returns_correct_subset() {
    let schema = test_schema();
    let per_shard: Arc<Vec<Vec<u32>>> = Arc::new(vec![vec![0, 2], vec![1, 3]]);
    let win = Window::new(
        WindowParams {
            name: "sharded".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(usize::MAX),
    );
    win.append_sized(
        make_batch(win.schema(), &[1_000_000_000, 2_000_000_000], &[10, 20]),
        4096,
        Some(Arc::clone(&per_shard)),
    )
    .unwrap();

    let (_, rows, _, _) = win.read_since_with_shard(0, Some(0));
    let returned = rows.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(returned.len(), 1, "one batch → one shard subset");
    assert_eq!(
        returned[0].as_ref().as_slice(),
        &[0u32, 2],
        "shard 0 must see its own row indices"
    );

    // Unsharded pull returns `None` for every batch (whole-batch processing).
    let (_, rows, _, _) = win.read_since_with_shard(0, None);
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].is_none(),
        "unsharded pull must not request a shard subset"
    );
}
