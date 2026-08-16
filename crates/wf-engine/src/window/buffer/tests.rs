use crate::match_engine::{JoinKey, Value};
use crate::window::buffer::{content_bytes, events_bytes};
use crate::window::buffer::Window;
use crate::window::buffer::types::AppendOutcome;
use crate::window::buffer::types::WindowParams;
use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};

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
        },
        test_config(max_bytes),
    )
}

// -- 1. append_and_evict_expired ----------------------------------------

#[test]
fn append_and_evict_expired() {
    let mut win = test_window(10, usize::MAX);
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
    win.evict_expired(12_000_000_000, u64::MAX);
    assert_eq!(win.batch_count(), 2);
    assert_eq!(win.total_rows(), 2);

    // cutoff = 16s - 10s = 6s → batch2 (max=5s) < 6s → evicted
    win.evict_expired(16_000_000_000, u64::MAX);
    assert_eq!(win.batch_count(), 1);
    assert_eq!(win.total_rows(), 1);
}

// -- 2. snapshot_is_independent_of_mutations ----------------------------

#[test]
fn snapshot_is_independent_of_mutations() {
    let mut win = test_window(60, usize::MAX);
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
    let mut win = test_window(60, usize::MAX);
    let schema = win.schema().clone();

    win.append(make_batch(&schema, &[], &[])).unwrap();
    assert!(win.is_empty());
    assert_eq!(win.total_rows(), 0);
    assert_eq!(win.memory_usage(), 0);
}

// -- 4. schema_mismatch_rejected ----------------------------------------

#[test]
fn schema_mismatch_rejected() {
    let mut win = test_window(60, usize::MAX);

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
    let mut win = Window::new(
        WindowParams {
            name: "mem_win".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
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

// -- 6. no_time_col_window ----------------------------------------------

#[test]
fn no_time_col_window() {
    let schema = test_schema_no_time();
    let mut win = Window::new(
        WindowParams {
            name: "output_win".into(),
            schema: schema.clone(),
            time_col_index: None,
            over: Duration::from_secs(60),
            materialize_fields: None,
        },
        test_config(usize::MAX),
    );

    win.append(make_batch_no_time(&schema, &[100, 200]))
        .unwrap();
    assert_eq!(win.batch_count(), 1);
    assert_eq!(win.total_rows(), 2);

    // evict_expired is no-op for no-time-column windows.
    win.evict_expired(i64::MAX, u64::MAX);
    assert_eq!(win.batch_count(), 1);
    assert_eq!(win.total_rows(), 2);
}

// -- 7. evict_on_empty_window_is_noop -----------------------------------

#[test]
fn evict_on_empty_window_is_noop() {
    let mut win = test_window(60, usize::MAX);
    win.evict_expired(i64::MAX, u64::MAX);
    assert!(win.is_empty());
}

// -- 8. memory_usage_tracks_correctly -----------------------------------

#[test]
fn memory_usage_tracks_correctly() {
    let mut win = test_window(3600, usize::MAX);
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
    let mut win = test_window(10, usize::MAX);
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
    win.evict_expired(15_000_000_000, u64::MAX);
    assert_eq!(win.batch_count(), 1);

    // cutoff = 19s - 10s = 9s → batch max=8s < 9s → evicted
    win.evict_expired(19_000_000_000, u64::MAX);
    assert_eq!(win.batch_count(), 0);
}

// -- 10. append_with_watermark_on_time ------------------------------------

#[test]
fn append_with_watermark_on_time() {
    // watermark delay = 5s, allowed_lateness = 0s
    let mut win = test_window(3600, usize::MAX);
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
    let mut win = test_window(3600, usize::MAX);
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
    let mut win = test_window(3600, usize::MAX);
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
    let mut win = test_window(3600, usize::MAX);

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
    let mut win = test_window(3600, usize::MAX);
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
    let mut win = Window::new(
        WindowParams {
            name: "gap_win".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
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
    let mut win = test_window(3600, usize::MAX);
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
        (Arc::new(Field::new("sip", DataType::Utf8, false)), Arc::new(sip) as ArrayRef),
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
        std::collections::HashMap::from([(
            "wf.wfl.field_type".to_string(),
            "object".to_string(),
        )]),
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
            (0..n).map(|_| Some(short_json.to_string())).collect::<StringArray>(),
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
            (0..n).map(|_| Some(long_json.clone())).collect::<StringArray>(),
        )],
    )
    .unwrap();
    let parsed_long: Vec<Arc<crate::match_engine::Event>> =
        batch_to_events(&batch_long).into_iter().map(Arc::new).collect();
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
        std::collections::HashMap::from([(
            "wf.wfl.field_type".to_string(),
            "object".to_string(),
        )]),
    );
    let schema = Arc::new(Schema::new(vec![obj_field]));

    let short = r#"{"tags":["a","b"]}"#;
    let long = format!(
        r#"{{"tags":[{}]}}"#,
        (0..50).map(|_| "\"x\"").collect::<Vec<_>>().join(",")
    );

    let est_short = events_bytes(
        &batch_to_events(&RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new((0..n).map(|_| Some(short)).collect::<StringArray>())],
        )
        .unwrap())
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>(),
    );
    let est_long = events_bytes(
        &batch_to_events(&RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new((0..n).map(|_| Some(long.clone())).collect::<StringArray>())],
        )
        .unwrap())
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
        std::collections::HashMap::from([(
            "wf.wfl.field_type".to_string(),
            "object".to_string(),
        )]),
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
    assert!(events > content, "object fields must dominate the footprint");

    // Cap fits exactly one batch's *combined* footprint. Content-only accounting
    // for two batches stays under it (the undercount); combined accounting does not.
    let cap = content + events + 10;
    assert!(2 * content <= cap, "content-only accounting should stay under cap");
    assert!(content + events <= cap, "one batch's real footprint fits");
    assert!(2 * (content + events) > cap, "two batches' real footprint exceeds cap");

    let make = |name: &str, cap: usize| {
        Window::new(
            WindowParams {
                name: name.into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
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
    let mut content_only = make("content_only", cap);
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
    let mut accurate = make("accurate", cap);
    for _ in 0..2 {
        accurate
            .append_with_watermark_parsed_sized(
                batch.clone(),
                Arc::new(parsed.clone()),
                content + events,
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

    let mut win = Window::new(
        WindowParams {
            name: "lazy".into(),
            schema: schema.clone(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
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
    win.append_with_watermark_sized(batch, content).unwrap();

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

// -- join index ------------------------------------------------------------

#[test]
fn join_index_maintained_on_append_and_evict() {
    let mut win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    // Append two batches with overlapping key values.
    win.append(make_batch(&test_schema(), &[1_000_000, 2_000_000], &[42, 43]))
        .unwrap();
    win.append(make_batch(&test_schema(), &[3_000_000, 4_000_000], &[42, 44]))
        .unwrap();

    // Lookup by key: value 42 has 2 rows, 44 has 1, 999 has none.
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42)).map(|v| v.len()),
        Some(2),
        "two rows with value 42 indexed"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(44)).map(|v| v.len()),
        Some(1),
        "one row with value 44 indexed"
    );
    assert!(
        win.join_lookup(&JoinKey::Int(999)).is_none(),
        "no match → None"
    );

    // Expire all batches: over=3600s, now=4000s → cutoff=400s >> event times
    // (1-4ms), so all batches are time-evicted and index entries removed.
    win.evict_expired(4_000_000_000_000, u64::MAX);
    assert!(
        win.join_lookup(&JoinKey::Int(42)).is_none_or(|v| v.is_empty()),
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
        win.join_lookup(&JoinKey::Int(1)).is_none(),
        "no join index → None (caller falls back to scan)"
    );
}

#[test]
fn join_index_built_for_existing_batches_on_set_join_key() {
    let mut win = test_window(3600, usize::MAX);
    // Data appended before the window is configured as a join target.
    win.append(make_batch(&test_schema(), &[1_000_000, 2_000_000], &[42, 43]))
        .unwrap();
    win.append(make_batch(&test_schema(), &[3_000_000], &[44]))
        .unwrap();
    win.set_join_key("value".into());
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42)).map(|v| v.len()),
        Some(1),
        "existing rows indexed by set_join_key"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(44)).map(|v| v.len()),
        Some(1),
        "rows from a later batch indexed"
    );
}

#[test]
fn join_index_updated_on_oldest_eviction() {
    let mut win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    win.append(make_batch(&test_schema(), &[1_000_000, 2_000_000], &[42, 43]))
        .unwrap();
    win.append(make_batch(&test_schema(), &[3_000_000, 4_000_000], &[44, 45]))
        .unwrap();

    // evict_oldest (memory-pressure path) must drop the first batch's keys.
    assert!(win.evict_oldest().is_some(), "evict_oldest returns byte size");
    assert!(
        win.join_lookup(&JoinKey::Int(42)).is_none_or(|v| v.is_empty()),
        "key 42 (first batch) removed after evict_oldest"
    );
    assert!(
        win.join_lookup(&JoinKey::Int(43)).is_none_or(|v| v.is_empty()),
        "key 43 (first batch) removed after evict_oldest"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(44)).map(|v| v.len()),
        Some(1),
        "key 44 (second batch) still indexed"
    );
}

#[test]
fn join_index_duplicate_key_keeps_all_rows() {
    let mut win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    // Two rows with the same key 42 in different batches.
    win.append(make_batch(&test_schema(), &[1_000_000], &[42]))
        .unwrap();
    win.append(make_batch(&test_schema(), &[2_000_000], &[42]))
        .unwrap();
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42)).map(|v| v.len()),
        Some(2),
        "both rows with key 42 kept"
    );
    // Evict one batch → one row remains.
    win.evict_oldest();
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42)).map(|v| v.len()),
        Some(1),
        "one row removed on evict, one kept"
    );
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
    win.append_parsed_sized(make_batch(&schema, &[1_000_000_000], &[100]), Arc::clone(&first), 4096)
        .unwrap();
    win.append_parsed_sized(
        make_batch(&schema, &[12_000_000_000], &[300]),
        parsed_events(3),
        4096,
    )
    .unwrap();
    assert_eq!(win.batch_count(), 2);

    // cutoff = 12s - 10s = 2s → batch1 (max=1s) evicted.
    win.evict_expired(12_000_000_000, u64::MAX);
    assert_eq!(win.batch_count(), 1);

    assert_events_released(&first);
}

/// Memory eviction (append-side pressure) must release them too.
#[test]
fn memory_evicted_batch_releases_parsed_events() {
    let win = test_window(3600, 6144);
    let schema = win.schema().clone();

    let first = parsed_events(2);
    win.append_parsed_sized(make_batch(&schema, &[1_000_000_000], &[100]), Arc::clone(&first), 4096)
        .unwrap();
    // Second 4KiB batch pushes current_bytes (8192) over max (6144) → first
    // evicted; the remaining 4096 is back under the cap so eviction stops.
    win.append_parsed_sized(
        make_batch(&schema, &[2_000_000_000], &[200]),
        parsed_events(2),
        4096,
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
    win.append_parsed_sized(make_batch(&schema, &[1_000_000], &[42]), Arc::clone(&first), 4096)
        .unwrap();
    win.append_parsed_sized(
        make_batch(&schema, &[2_000_000], &[43]),
        parsed_events(2),
        4096,
    )
    .unwrap();

    win.evict_oldest();
    assert_eq!(win.batch_count(), 1);

    assert_events_released(&first);
}
