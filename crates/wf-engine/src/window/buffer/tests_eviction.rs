//! buffer 驱逐与保留测试（2026-09-04 自 tests.rs 拆出；`#[path]` 兄弟子模块，
//! `use super::*` 共享父模块 harness）：时间/内存驱逐、retention pin 与 ack
//! floor、驱逐即释放 parsed 事件（Eager-drop regression）。

use super::*;

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

/// 驱逐 WARN 的后缀格式（2026-08-31）：非 join 窗口（无 pin）省略
/// `join_pin_floor_ns`（而不是打印无意义的 i64::MAX）；join pin 生效时精确
/// 输出。锁住格式，防止退回旧的误导字段名/恒输出 i64::MAX。
#[test]
fn eviction_warn_omits_pin_floor_for_plain_windows() {
    // 无 pin：无论 retention_ns 是什么（i64::MAX / 0）都不打印该字段。
    assert_eq!(Window::eviction_warn_pin_suffix(false, i64::MAX), "");
    assert_eq!(Window::eviction_warn_pin_suffix(false, 0), "");
    // 有 pin：精确输出（含负值——pin 从 i64::MIN 起，负前沿合法）。
    assert_eq!(
        Window::eviction_warn_pin_suffix(true, 3_000_000_000),
        ", join_pin_floor_ns=3000000000"
    );
    assert_eq!(
        Window::eviction_warn_pin_suffix(true, -5),
        ", join_pin_floor_ns=-5"
    );
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

/// D4 保留 pin：**无任何 pull 消费者**（ack floor = u64::MAX，即 join 目标窗口的
/// 处境）时，内存驱逐仍需尊重 pin 发布的事件时间前沿——这是 q9/q4a 30M
/// −62%（bid 字节上限丢掉 deferred 到期评估还要用的 bid）的防网。
/// 双向断言：前沿之前的行仍可驱逐（不过度保留），前沿之后的行不可驱逐。
#[test]
fn memory_eviction_respects_retention_pin() {
    let schema = test_schema();
    let probe = make_batch(&schema, &[1_000_000_000], &[100]);
    let one_batch_size = content_bytes(&probe);
    let max_bytes = one_batch_size * 2;
    let win = Window::new(
        WindowParams {
            name: "mem_pin".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(max_bytes),
    );

    // 关键：不注册任何消费者槽位（ack floor = u64::MAX，全部可驱逐），
    // 只有一个保留 pin——完全复现 join 目标窗口的处境。
    let progress = Arc::new(WindowProgress::new());
    win.set_progress(Arc::clone(&progress));
    let pin = progress.register_retention_pin();
    assert_eq!(
        win.retention_floor_ns(),
        i64::MIN,
        "刚注册的 pin fail-safe 全保留（读者尚未发布前沿）"
    );

    // 前沿 = 3s：只需要事件时间 ≥ 3s 的行。
    pin.store(3_000_000_000, Ordering::Release);
    assert_eq!(win.retention_floor_ns(), 3_000_000_000);

    win.append(probe.clone()).unwrap(); // seq 0, ts 1s
    win.append(make_batch(win.schema(), &[2_000_000_000], &[200]))
        .unwrap(); // seq 1, ts 2s
    assert_eq!(win.batch_count(), 2);

    // 超预算：seq 0（max 1s < 3s）不在前沿内 → 仍可驱逐。pin 不能变成
    // 「什么都不丢」的内存泄洏。
    win.append(make_batch(win.schema(), &[3_000_000_000], &[300]))
        .unwrap(); // seq 2, ts 3s
    assert_eq!(
        win.batch_count(),
        2,
        "前沿之前的 batch 必须仍可被内存驱逐（否则 pin 就是内存泄洏）"
    );
    assert!(win.memory_usage() <= max_bytes);

    // 前沿回退到 1s（比如新挂起了一个 lo_ns 更早的实例）：现存 batch 全在
    // 前沿内 → 超预算也不得丢，窗口瞬时超出 max_window_bytes。
    pin.store(1_000_000_000, Ordering::Release);
    win.append(make_batch(win.schema(), &[4_000_000_000], &[400]))
        .unwrap(); // seq 3, ts 4s
    assert_eq!(
        win.batch_count(),
        3,
        "pin 住的行必须存活（宁可瞬时超预算，也不静默丢 join 目标数据）"
    );
    assert!(win.memory_usage() > max_bytes);

    // 释放 pin（EOS）→ 恢复完全可驱逐，内存回到预算内。
    pin.store(i64::MAX, Ordering::Release);
    win.append(make_batch(win.schema(), &[5_000_000_000], &[500]))
        .unwrap(); // seq 4, ts 5s
    assert_eq!(win.batch_count(), 2, "pin 释放后内存驱逐恢复");
    assert!(win.memory_usage() <= max_bytes);
}

/// D4：spawn 阶段预注册的 pin 必须在读者（异步规则任务）启动**之前**就生效。
///
/// q4 30M 回归：规则任务是 `tokio::spawn` 的、在 future 内自构，而摄入紧接
/// `spawn_rule_tasks` 开始——pin 在 future 里注册会与首批 append 竞争，当时非确定性
/// 丢 0~6% 输出（启动期 5 vs 48 次驱逐清扫）。
#[test]
fn preregistered_pin_protects_before_the_reader_starts() {
    let schema = test_schema();
    let probe = make_batch(&schema, &[1_000_000_000], &[100]);
    let max_bytes = content_bytes(&probe) * 2;
    let win = Window::new(
        WindowParams {
            name: "parked_pin".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(max_bytes),
    );
    let progress = Arc::new(WindowProgress::new());
    win.set_progress(Arc::clone(&progress));

    // spawn 阶段（同步）：声明这是 deferred join 目标，读者尚未启动。
    win.preregister_retention_pin();
    assert_eq!(win.retention_floor_ns(), i64::MIN);

    // 首批数据已经在灌入——超预算也不得丢（这些行可能正是 deferred 到期
    // 评估要用的）。
    for (i, ts) in [1_000_000_000, 2_000_000_000, 3_000_000_000]
        .into_iter()
        .enumerate()
    {
        win.append(make_batch(win.schema(), &[ts], &[100 * (i as i64 + 1)]))
            .unwrap();
    }
    assert_eq!(
        win.batch_count(),
        3,
        "读者启动前的 append 必须受预注册 pin 保护"
    );

    // 读者启动：取走 pin 并发布真实前沿（只需≥ 3s）。
    let pin = win.take_retention_pin().expect("parked pin");
    pin.store(3_000_000_000, Ordering::Release);
    win.append(make_batch(win.schema(), &[4_000_000_000], &[400]))
        .unwrap();
    assert_eq!(win.batch_count(), 2, "发布前沿后，前沿之前的行应被回收");
    assert!(win.memory_usage() <= max_bytes);

    // 取过一次后不再有寄存 pin；后续调用自己注册一个（分片规则的其余分片）。
    let extra = win.take_retention_pin().expect("fresh pin");
    assert_eq!(
        win.retention_floor_ns(),
        i64::MIN,
        "新注册的分片 pin 同样 fail-safe 全保留"
    );
    drop(extra);
}

/// D4：全局内存上限路径（驱逐器的 `evict_oldest_acked`）同样尊重保留 pin——
/// 否则按窗预算护住的行会从全局 `max_total_bytes` 那一侧被抽走。
#[test]
fn evict_oldest_acked_respects_retention_pin() {
    let schema = test_schema();
    let win = Window::new(
        WindowParams {
            name: "global_pin".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(usize::MAX),
    );
    let progress = Arc::new(WindowProgress::new());
    win.set_progress(Arc::clone(&progress));
    let pin = progress.register_retention_pin();
    pin.store(2_000_000_000, Ordering::Release);

    win.append(make_batch(win.schema(), &[1_000_000_000], &[100]))
        .unwrap(); // seq 0, ts 1s
    win.append(make_batch(win.schema(), &[3_000_000_000], &[300]))
        .unwrap(); // seq 1, ts 3s

    // seq 0（max 1s < 2s）在前沿之前 → 可回收。
    assert!(
        win.evict_oldest_acked(u64::MAX).is_some(),
        "前沿之前的 batch 应可被全局内存回收"
    );
    // seq 1（max 3s ≥ 2s）被 pin 住 → 报不可回收（调用方转而施加背压）。
    assert!(
        win.evict_oldest_acked(u64::MAX).is_none(),
        "pin 住的 batch 不得被全局内存上限丢弃"
    );
    assert_eq!(win.batch_count(), 1);
}

/// D4（2026-08-25 闭环）：**时间驱逐同样尊重保留 pin**——deferred join 的
/// 挂起实例需要 `[lo, hi]` 内的右行，`over` 只是内存参数，绝不能因调小 over
/// 删掉评估还要用的行（100M q4 over=1h 精确 / over=30m 欠发 6-9k 的根因）。
/// 只删整体在 pin 之前的批；`event_time_range.1 ≥ pin` 的批保留到评估后。
#[test]
fn evict_expired_respects_retention_pin() {
    let schema = test_schema();
    let win = Window::new(
        WindowParams {
            name: "time_pin".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(10),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(usize::MAX),
    );
    let progress = Arc::new(WindowProgress::new());
    win.set_progress(Arc::clone(&progress));
    let pin = progress.register_retention_pin();

    // 无 pin 约束（pin 释放到 i64::MAX = 无 pin；注册默认 i64::MIN 是 fail-safe
    // 全保留，不是"无 pin"）时行为与旧实现一致：时间驱逐自由删过期批。
    pin.store(i64::MAX, Ordering::Release);
    win.append(make_batch(win.schema(), &[1_000_000_000], &[100]))
        .unwrap(); // seq 0, ts 1s
    win.append(make_batch(win.schema(), &[15_000_000_000], &[300]))
        .unwrap(); // seq 1, ts 15s
    // cutoff = 30s - 10s = 20s；两批都 < 20s → 全删（无 pin 不拦）。
    win.evict_expired(30_000_000_000);
    assert_eq!(win.batch_count(), 0, "无 pin 时时间驱逐行为不变");

    // 重新 append：pin = 12s → seq0(1s) 在前沿前可删，seq1(15s) 被 pin 住。
    win.append(make_batch(win.schema(), &[1_000_000_000], &[100]))
        .unwrap(); // seq 2, ts 1s
    win.append(make_batch(win.schema(), &[15_000_000_000], &[300]))
        .unwrap(); // seq 3, ts 15s
    pin.store(12_000_000_000, Ordering::Release);
    // cutoff = 20s：两批时间上都够老，但 seq3（max 15s ≥ pin 12s）被 pin 挡住。
    win.evict_expired(30_000_000_000);
    assert_eq!(
        win.batch_count(),
        1,
        "pin 住的批不得被时间驱逐（over 是内存参数，不能删评估还要用的行）"
    );

    // 释放 pin → 时间驱逐恢复：seq3（15s < 20s）现在可删。
    pin.store(i64::MAX, Ordering::Release);
    win.evict_expired(30_000_000_000);
    assert_eq!(win.batch_count(), 0, "释放 pin 后时间驱逐恢复正常");
}

/// `front_pinned_by_retention` 直接单测：空窗口 / 无 pin / front 在前沿内 /
/// front 在前沿外 / 释放后 五种状态。evictor 用它做候选选择，语义必须精确。
#[test]
fn front_pinned_by_retention_states() {
    let schema = test_schema();
    let win = Window::new(
        WindowParams {
            name: "front_pin".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(usize::MAX),
    );
    let progress = Arc::new(WindowProgress::new());
    win.set_progress(Arc::clone(&progress));

    assert!(!win.front_pinned_by_retention(), "空窗口 → false");

    win.append(make_batch(win.schema(), &[1_000_000_000], &[100]))
        .unwrap(); // front: ts 1s
    assert!(
        !win.front_pinned_by_retention(),
        "无 pin → false（同 pin 前行为）"
    );

    let pin = progress.register_retention_pin();
    pin.store(2_000_000_000, Ordering::Release);
    assert!(
        !win.front_pinned_by_retention(),
        "front (max 1s) < 前沿 2s → 可驱逐，false"
    );

    pin.store(1_000_000_000, Ordering::Release);
    assert!(
        win.front_pinned_by_retention(),
        "front (max 1s) >= 前沿 1s → pin 住，true"
    );

    pin.store(i64::MAX, Ordering::Release);
    assert!(!win.front_pinned_by_retention(), "释放后 → false");
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
