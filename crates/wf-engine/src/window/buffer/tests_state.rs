//! 窗口簿记状态测试（2026-09-04 自 tests.rs 拆出；`#[path]` 兄弟子模块）：
//! watermark / per-source committed frontier（时间水位簿记），content_bytes /
//! events_bytes / allocated_bytes（内存字节口径）。

use super::*;

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

// -- 10b. committed_frontier_ns（跨源提交乱序修复） -------------------------

/// 健全提交前沿 = 各 source 已提交 max 的 min（2026-08-25 跨源提交乱序修复）：
/// 全局 `max_event_time` 会被任一 source 的晚 batch 提前推高，deferred 评估
/// gate 用它会在右行未提交时提前评估 → 假 miss（30M q4 over=30m -860）。
/// `committed_frontier_ns` 才是"右行完整性"的健全判据。
#[test]
fn committed_frontier_tracks_per_source_min() {
    // allowed_lateness=60s：跨 source 乱序的旧 batch 不丢（生产 = 30m）。
    let mut cfg = test_config(usize::MAX);
    cfg.allowed_lateness = Duration::from_secs(60).into();
    let schema = test_schema();
    let win = Window::new(
        WindowParams {
            name: "test_win".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        cfg,
    );

    // 无 per-source 记录（非 actor 路径）→ 回退全局 max（旧行为）。
    win.append_with_watermark(make_batch(&win.schema().clone(), &[10_000_000_000], &[1]))
        .unwrap();
    assert_eq!(win.max_event_time_nanos(), 10_000_000_000);
    assert_eq!(win.committed_frontier_ns(), 10_000_000_000);

    // actor 路径：source A 提交到 50s，source B 提交到 20s → 前沿 = 20s。
    let src_a: Arc<str> = Arc::from("ingress#1");
    let src_b: Arc<str> = Arc::from("ingress#2");
    let schema = win.schema().clone();
    win.append_with_watermark_sized_from(
        make_batch(&schema, &[50_000_000_000], &[2]),
        0,
        None,
        Arc::clone(&src_a),
    )
    .unwrap();
    win.append_with_watermark_sized_from(
        make_batch(&schema, &[20_000_000_000], &[3]),
        0,
        None,
        Arc::clone(&src_b),
    )
    .unwrap();
    assert_eq!(
        win.max_event_time_nanos(),
        50_000_000_000,
        "全局 max 被 source A 的晚 batch 推高（跨源乱序）"
    );
    assert_eq!(
        win.committed_frontier_ns(),
        20_000_000_000,
        "健全前沿 = min(按源已提交) = 20s——source B 的行只提交到 20s"
    );

    // source B 追平 → 前沿推进；随后 source A 继续 → 前沿跟随较慢者。
    win.append_with_watermark_sized_from(
        make_batch(&schema, &[60_000_000_000], &[4]),
        0,
        None,
        Arc::clone(&src_b),
    )
    .unwrap();
    assert_eq!(win.committed_frontier_ns(), 50_000_000_000);
    win.append_with_watermark_sized_from(
        make_batch(&schema, &[70_000_000_000], &[5]),
        0,
        None,
        Arc::clone(&src_a),
    )
    .unwrap();
    assert_eq!(win.committed_frontier_ns(), 60_000_000_000);
}

/// 订阅窗（events=Some 的 parsed 路径）同样记录 per-source 前沿——
/// `commit_appended_batch` 的 parsed 分支曾丢 source（2026-08-25 review 修复）：
/// 目标窗有规则订阅时（如 q4a 订阅 auction_events、q8 的 deferred 目标正是它），
/// 不记录会让前沿回退全局 max → 跨源乱序修复失效。
#[test]
fn committed_frontier_records_parsed_sized_from() {
    let mut cfg = test_config(usize::MAX);
    cfg.allowed_lateness = Duration::from_secs(60).into();
    let schema = test_schema();
    let win = Window::new(
        WindowParams {
            name: "test_win".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        cfg,
    );
    let src_a: Arc<str> = Arc::from("ingress#1");
    let src_b: Arc<str> = Arc::from("ingress#2");
    let schema = win.schema().clone();
    let events = Arc::new(vec![]);

    win.append_with_watermark_parsed_sized_from(
        make_batch(&schema, &[40_000_000_000], &[1]),
        Arc::clone(&events),
        0,
        None,
        Arc::clone(&src_a),
    )
    .unwrap();
    win.append_with_watermark_parsed_sized_from(
        make_batch(&schema, &[10_000_000_000], &[2]),
        Arc::clone(&events),
        0,
        None,
        Arc::clone(&src_b),
    )
    .unwrap();
    assert_eq!(
        win.max_event_time_nanos(),
        40_000_000_000,
        "parsed 路径同样推进全局 max"
    );
    assert_eq!(
        win.committed_frontier_ns(),
        10_000_000_000,
        "parsed 路径同样按源记录前沿（min = source B 的 10s）"
    );
}

/// DroppedLate（乱序旧 batch 被迟到策略丢弃）**不**记录 per-source——
/// 否则被丢的行会污染前沿（看似已提交、实际不在窗口里）。
#[test]
fn committed_frontier_ignores_dropped_late() {
    let cfg = test_config(usize::MAX);
    // allowed_lateness=0 + Drop：乱序旧 batch 必被丢。
    let schema = test_schema();
    let win = Window::new(
        WindowParams {
            name: "test_win".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        cfg,
    );
    let src: Arc<str> = Arc::from("ingress#1");
    let schema = win.schema().clone();

    // 先推进 watermark：batch @ 100s → watermark = 95s（delay 5s）。
    win.append_with_watermark(make_batch(&schema, &[100_000_000_000], &[1]))
        .unwrap();
    // 旧 batch @ 10s < watermark(95s) → DroppedLate。
    let outcome = win
        .append_with_watermark_sized_from(
            make_batch(&schema, &[10_000_000_000], &[2]),
            0,
            None,
            Arc::clone(&src),
        )
        .unwrap();
    assert!(matches!(outcome.0, AppendOutcome::DroppedLate));
    assert_eq!(
        win.committed_frontier_ns(),
        100_000_000_000,
        "被丢的乱序 batch（10s）不记录 per-source——回退全局 max（100s），\
         若被记录则 frontier 会被 10s 拉低"
    );
}

/// q3 根因回归（nexmark_pk，2026-08-30）：**首次** append 期间 `committed_frontier_ns`
/// 不得领先于 join 索引内容。旧实现在 `append_inner`（含 join 索引构建）**之前**
/// 推进 `max_event_time_nanos`，而 per-source 提交前沿在 append 完成后才记录——
/// 首次 append（per-source 为空）时 `committed_frontier_ns` 回退到全局 max，把
/// 尚未建索引的批次 max 报为已提交 → eager join gate 提前放行 → snapshot join
/// 与目标窗建索引并发 → 静默 miss（buffer 有行、索引没有，q3 丢 0~16 早期
/// auction，oracle 字段级对拍定位）。
///
/// 本用例用并发采样器验证不变式：只要 `committed_frontier_ns() > i64::MIN`（目标
/// 窗报告了真实提交水位），join 索引就必须已包含首批全部探测键。修复后
/// max_event_time 只在 append（含索引）完成后推进，采样器永远看不到
/// 「frontier 已推进、索引仍空」的中间态；旧实现会在索引构建期间观察到它。
#[test]
fn committed_frontier_never_leads_the_join_index() {
    use std::sync::Arc as StdArc;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    let win = StdArc::new(test_window(3600, usize::MAX));
    win.set_join_key("value".into());
    let schema = win.schema().clone();

    // 大批首次 append：把 index_batch 的临界区拉长到几十 ms，让采样器稳定
    // 落入「旧实现下 frontier 已推进、索引未建完」的窗口。
    let n = 400_000usize;
    let times: Vec<i64> = (0..n as i64).map(|i| 10_000_000_000i64 + i).collect();
    let values: Vec<i64> = (0..n as i64).collect();
    let batch = make_batch(&schema, &times, &values);
    // 探测键散布在不同分片：旧实现索引逐片写入，任意时刻大概率有分片未写。
    let probes: Vec<JoinKey> = vec![
        JoinKey::Int(0),
        JoinKey::Int(97_000),
        JoinKey::Int(199_999),
        JoinKey::Int(300_001),
        JoinKey::Int(399_999),
    ];

    let stop = StdArc::new(AtomicBool::new(false));
    let seen_bad = StdArc::new(AtomicBool::new(false));
    let w = StdArc::clone(&win);
    let s = StdArc::clone(&stop);
    let bad = StdArc::clone(&seen_bad);
    let probes_s = probes.clone();
    let sampler = std::thread::spawn(move || {
        while !s.load(AtomicOrdering::Relaxed) {
            let f = w.committed_frontier_ns();
            if f != i64::MIN {
                // 报告了真实提交水位 → 索引必须已含全部探测行（修复后成立；
                // 旧实现索引构建期间此处会命中空索引）。
                let all_present = probes_s.iter().all(|k| {
                    w.join_lookup("value", k, None)
                        .is_some_and(|rows| !rows.is_empty())
                });
                if !all_present {
                    bad.store(true, AtomicOrdering::Relaxed);
                }
            }
        }
    });

    let src: Arc<str> = Arc::from("ingress#1");
    win.append_with_watermark_sized_from(batch, 0, None, src)
        .unwrap();

    stop.store(true, AtomicOrdering::Relaxed);
    sampler.join().expect("sampler thread join");
    assert!(
        !seen_bad.load(AtomicOrdering::Relaxed),
        "committed_frontier_ns 领先于 join 索引内容（首次 append 期间把未提交 max 报为已提交）"
    );

    // 提交后收敛：frontier = per-source = 批次 max，探测键全部可查。
    let max = 10_000_000_000i64 + n as i64 - 1;
    assert_eq!(win.committed_frontier_ns(), max);
    for k in &probes {
        assert!(
            win.join_lookup("value", k, None)
                .is_some_and(|r| !r.is_empty()),
            "提交后探测键 {k:?} 必须可查"
        );
    }
}

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

/// 会计保真度（2026-08-25）：`allocated_usage()` 报**实际分配** Arrow 缓冲字节
/// （含 null bitmap / offsets / 容量舍入），`memory_usage()` 报逻辑内容
/// （`content_bytes`，驱逐与 mailbox 预算口径）。
///
/// 存在理由：内存分账需要一个不漏 bitmap/offsets 又不重复计共享缓冲的口径
/// （`get_array_memory_size` 会按列重复计整块分配，实测把 content 1.58GB 的窗口
/// 报成 17.97GB）。本测试钉死三条：分配 ≥ 内容、随 append 增长、随驱逐回落到 0
/// （最后一条防止记账单调虚增造出假"泄漏"信号）。
#[test]
fn allocated_usage_tracks_real_buffers_and_drops_on_evict() {
    let win = test_window(10, usize::MAX);
    let schema = win.schema().clone();

    assert_eq!(win.allocated_usage(), 0, "空窗分配字节为 0");

    let t1 = 1_000_000_000;
    let t2 = 5_000_000_000;
    win.append(make_batch(&schema, &[t1], &[100])).unwrap();
    let a1 = win.allocated_usage();
    let c1 = win.memory_usage();
    assert!(a1 > 0, "append 后分配字节须 > 0");
    assert!(
        a1 >= c1,
        "实际分配 ({a1}) 必须 ≥ 逻辑内容 ({c1})——含 null bitmap/offsets/容量舍入"
    );

    win.append(make_batch(&schema, &[t2], &[200])).unwrap();
    assert!(win.allocated_usage() > a1, "第二批 append 后分配字节须增长");

    // 时间驱逐两批（now 推到 over 之后）→ 分配字节必须回落到 0（无泄漏记账）。
    win.evict_expired(t2 + 11_000_000_000);
    assert_eq!(win.batch_count(), 0, "sanity: 两批都已过期驱逐");
    assert_eq!(
        win.allocated_usage(),
        0,
        "驱逐后分配字节必须归零（记账与 current_bytes 同步）"
    );
    assert_eq!(win.memory_usage(), 0, "内容字节同样归零");
}

/// `allocated_bytes` 的两条硬约束（2026-08-25 会计保真度，两者都踩过坑）：
///
/// 1. **≥ content_bytes**：必须计入 null bitmap / offsets（`content_bytes` 漏掉，
///    新建批次会低估这两项）。
/// 2. **共享缓冲不得重复计**：IPC 解码批次各列是同一帧体的零拷贝切片，
///    `RecordBatch::get_array_memory_size()` 按列累加整个底层容量 → 实测把
///    content 1.58GB 的窗口报成 17.97GB（11.4×），甚至超过进程 peak_commit。
///    本测试用 `slice()` 造出共享缓冲的多列批次，断言不出现成倍膨胀。
#[test]
fn allocated_bytes_counts_bitmaps_but_not_shared_buffers_twice() {
    use crate::window::allocated_bytes;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    // ① 独立缓冲 + 含空值 → 必须 ≥ content（多出 null bitmap）。
    let with_nulls = Int64Array::from(vec![Some(1i64), None, Some(3)]);
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let b1 = RecordBatch::try_new(schema, vec![Arc::new(with_nulls) as ArrayRef]).unwrap();
    let a1 = allocated_bytes(&b1);
    let c1 = content_bytes(&b1);
    assert!(
        a1 >= c1,
        "allocated ({a1}) 必须 ≥ content ({c1})：null bitmap/offsets 不能漏"
    );

    // ② 同一底层缓冲被两列共享（slice 零拷贝）→ 不得按列重复累加。
    let base = Int64Array::from((0..1024i64).collect::<Vec<_>>());
    let left = base.slice(0, 512);
    let right = base.slice(512, 512);
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("l", DataType::Int64, true),
        Field::new("r", DataType::Int64, true),
    ]));
    let b2 = RecordBatch::try_new(
        schema2,
        vec![Arc::new(left) as ArrayRef, Arc::new(right) as ArrayRef],
    )
    .unwrap();
    let a2 = allocated_bytes(&b2);
    let naive = b2.get_array_memory_size();
    // 两个 512 元素切片共享一个 1024×8B = 8KiB 分配；实际引用合计 ≈ 8KiB。
    assert!(
        a2 <= 12 * 1024,
        "共享缓冲不得成倍膨胀（allocated={a2}，朴素 get_array_memory_size={naive}）"
    );
    assert!(
        a2 >= 8 * 1024,
        "两个 512×i64 切片实际引用应 ≈ 8KiB（allocated={a2}）"
    );
}

/// R1 review 补盲（2026-08-25）：`allocated_bytes` 的两个**真实 IPC 形态**边界，
/// 前一版测试用 `Array::slice()`（各列共享同一 Buffer 对象、ptr 相同 → 走去重
/// 分支）覆盖不到：
///
/// 1. **同一分配的不同区间**（IPC 解码的真实形态：每列一个
///    `Buffer::slice_with_length`，ptr 各异但同属一块分配）→ 必须**按引用长度
///    求和**，不得把整块分配重复计入（`get_array_memory_size` 正是这么错的）。
/// 2. **`RecordBatch::slice()` 后**：arrow-rs 的 slice 会收缩 buffer 的 ptr/len，
///    所以本函数报的是**实际引用范围**（准确，不高报）。
///    （review 时曾误以为会按整块高报，实测否定——此处钉死真实行为。）
#[test]
fn allocated_bytes_sums_disjoint_slices_and_tracks_sliced_extent() {
    use crate::window::allocated_bytes;
    use arrow::array::Int64Array;
    use arrow::buffer::Buffer;
    use arrow::datatypes::{DataType, Field, Schema};

    // ① 一块 1024×i64 = 8KiB 分配，切成两段各 4KiB 供两列使用（ptr 不同）。
    let base: Buffer = Buffer::from_vec((0..1024i64).collect::<Vec<_>>());
    let left = base.slice_with_length(0, 4096);
    let right = base.slice_with_length(4096, 4096);
    let a_left = Int64Array::new(left.into(), None);
    let a_right = Int64Array::new(right.into(), None);
    let schema = Arc::new(Schema::new(vec![
        Field::new("l", DataType::Int64, false),
        Field::new("r", DataType::Int64, false),
    ]));
    let b = RecordBatch::try_new(
        schema,
        vec![Arc::new(a_left) as ArrayRef, Arc::new(a_right) as ArrayRef],
    )
    .unwrap();
    let a = allocated_bytes(&b);
    assert!(
        (8192..=9216).contains(&a),
        "两段不相交切片应求和 ≈8KiB（实际 {a}）——不得按列重复计整块分配"
    );

    // ② 切片后的批次：arrow-rs 的 slice 收缩 ptr/len → 本函数准确反映子集。
    let sliced = b.slice(0, 256);
    let a_sliced = allocated_bytes(&sliced);
    assert_eq!(
        a_sliced,
        2 * 256 * 8,
        "切片后应只计实际引用范围（两列 × 256 行 × 8B），实际 {a_sliced}"
    );
    assert!(a_sliced < a, "切片子集（{a_sliced}）必须小于全量（{a}）");
}
