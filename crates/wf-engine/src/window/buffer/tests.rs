use crate::match_engine::{AsofLookup, JoinKey, Value};
use crate::window::buffer::Window;
use crate::window::buffer::JOIN_INDEX_SHARDS;
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
    let cands = win
        .join_lookup_timestamped(&JoinKey::Int(42), None)
        .unwrap();
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

// -- join index 分片（2026-08-25，q4 100M 断崖修复） ------------------------
//
// `JoinIndex` 从单锁整表改为 64 片独立 RwLock：写者（index_batch）逐片短暂
// 持写锁、查找只锁 key 所在片。deferred_bench `index_contention` 实测单锁在
// 写者活跃时读者吞吐塌到 2%（0.15M vs 7.98M ops/s 天花板），分片恢复 43–46×。
// 以下测试保护分片的三个不变量：选片确定性/摊开、跨片查找/驱逐正确、asof
// 快路径跨片可用。

#[test]
fn join_index_shards_spread_and_deterministic() {
    use crate::window::buffer::JoinIndex;

    // 片数必须是 2 的幂（选片 = hash & mask）。
    let mask = JOIN_INDEX_SHARDS - 1;
    assert_eq!(
        JOIN_INDEX_SHARDS & mask,
        0,
        "shard count must be a power of two"
    );

    // 确定性：同一 key 恒落同一片，且落在界内。
    let a = JoinIndex::shard_of(&JoinKey::Int(42), mask);
    let b = JoinIndex::shard_of(&JoinKey::Int(42), mask);
    assert_eq!(a, b, "same key must map to the same shard");
    assert!(a < JOIN_INDEX_SHARDS);

    // 实际摊开：256 个连续 int key 必须覆盖至少一半分片（防选片塌缩——
    // 塌缩会让分片退化成单锁，重演 q4 100M 锁竞争）。
    let mut seen = std::collections::HashSet::new();
    for k in 0..256i64 {
        seen.insert(JoinIndex::shard_of(&JoinKey::Int(k), mask));
    }
    assert!(
        seen.len() >= 32,
        "256 consecutive int keys must spread across >=32 shards, got {}",
        seen.len()
    );

    // Str/Bool 键也落在界内。
    assert!(JoinIndex::shard_of(&JoinKey::Str("abc".into()), mask) < JOIN_INDEX_SHARDS);
    assert!(JoinIndex::shard_of(&JoinKey::Bool(true), mask) < JOIN_INDEX_SHARDS);
}

/// 跨片不变量：append 到全部分片上的行都能查到；驱逐后全部消失；asof 快
/// 路径跨片可用（每片维护自己的 max_ts，必须与整表语义一致）。
#[test]
fn join_index_sharded_lookup_evict_and_asof_span_all_shards() {
    use crate::window::buffer::JoinIndex;

    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());

    // 512 个递增 key：按 shard_of 挑出覆盖所有片的键集（防测试数据碰巧
    // 只落少数片而漏测真实分布）。
    let mask = JOIN_INDEX_SHARDS - 1;
    let mut per_shard: Vec<Vec<i64>> = vec![Vec::new(); JOIN_INDEX_SHARDS];
    let mut k = 0i64;
    for (s, shard) in per_shard.iter_mut().enumerate() {
        loop {
            if JoinIndex::shard_of(&JoinKey::Int(k), mask) == s {
                shard.push(k);
                k += 1;
                break;
            }
            k += 1;
        }
    }
    let keys: Vec<i64> = per_shard.into_iter().flatten().collect();
    assert!(keys.len() >= 64, "one representative key per shard");

    // 每个 key 两行（不同 ts）→ lookup 行数 = 2。
    for (i, key) in keys.iter().enumerate() {
        let ts = 1_000_000_000i64 + (i as i64) * 100;
        win.append(make_batch(&test_schema(), &[ts], &[*key])).unwrap();
        win.append(make_batch(&test_schema(), &[ts + 50], &[*key]))
            .unwrap();
    }
    for key in &keys {
        assert_eq!(
            win.join_lookup(&JoinKey::Int(*key), None).map(|v| v.len()),
            Some(2),
            "key {key} must be found with both rows (regardless of shard)"
        );
        // asof：两行都在 [ts, ts+50]，event_time 取后行 → 命中后行。
        match win.join_lookup_asof(
            &JoinKey::Int(*key),
            i64::MAX,
            i64::MIN,
            None,
        ) {
            AsofLookup::Hit(row) => {
                assert_eq!(row.field_value("value"), Some(Value::Number(*key as f64)));
            }
            AsofLookup::Miss => panic!("expected asof Hit for key {key}, got Miss"),
            AsofLookup::Fallback => panic!("expected asof Hit for key {key}, got Fallback"),
        }
    }

    // 驱逐所有 batch（时间驱逐，over=3600s）→ 全部分片清空。
    win.evict_expired(4_000_000_000_000);
    for key in &keys {
        assert!(
            win.join_lookup(&JoinKey::Int(*key), None)
                .is_none_or(|v| v.is_empty()),
            "key {key} must be removed from its shard after eviction"
        );
    }
}

// -- join index 增量驱逐（batch_keys registry，2026-08-25 q4 100M 主因修复）--
//
// 旧 `remove_batch` 每驱逐一批就全索引扫描（retain + max_ts 重算，O(全行数)——
// 100M 时 33M 行 × 每批，evictor 线程独占一核）。新实现按 `batch_keys[seq]`
// 只清该批贡献过的 key。以下测试保护增量语义：跨批 key 只删本批行、未触及
// key 完全不受影响、max_ts 在 max 行被删后正确回落。

#[test]
fn join_index_incremental_remove_only_touches_the_evicted_batch() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    // 4 个单行批：seq0=42, seq1=43, seq2=42（跨批 key）, seq3=44
    for (ts, v) in [1_000_000i64, 2_000_000, 3_000_000, 4_000_000]
        .into_iter()
        .zip([42i64, 43, 42, 44])
    {
        win.append(make_batch(&test_schema(), &[ts], &[v])).unwrap();
    }
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(2),
        "both batches' rows visible before eviction"
    );

    // 弹掉 seq0（key 42 的第一行）→ 42 只剩 seq2 的一行；43/44 不受影响。
    win.evict_oldest();
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(1),
        "cross-batch key keeps only the surviving batch's row"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(43), None).map(|v| v.len()),
        Some(1),
        "untouched batch's key must be unaffected"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(44), None).map(|v| v.len()),
        Some(1),
        "untouched key must be unaffected by another batch's eviction"
    );

    // 弹掉 seq1（key 43）→ 43 清空；42/44 仍在。
    win.evict_oldest();
    assert!(
        win.join_lookup(&JoinKey::Int(43), None)
            .is_none_or(|v| v.is_empty()),
        "evicted batch's sole key must be cleared"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(1)
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(44), None).map(|v| v.len()),
        Some(1)
    );

    // 弹掉 seq2（第二个 42）→ 42 清空；44 仍在。
    win.evict_oldest();
    assert!(
        win.join_lookup(&JoinKey::Int(42), None)
            .is_none_or(|v| v.is_empty()),
        "last 42 row removed"
    );
    assert_eq!(
        win.join_lookup(&JoinKey::Int(44), None).map(|v| v.len()),
        Some(1)
    );
}

#[test]
fn join_index_incremental_remove_recomputes_max_ts() {
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    // 同 key 42：seq0 在 ts=5s，seq1 在 ts=1s（max_ts 缓存 = 5s）。
    win.append(make_batch(&test_schema(), &[5_000_000_000], &[42]))
        .unwrap();
    win.append(make_batch(&test_schema(), &[1_000_000_000], &[42]))
        .unwrap();
    match win.join_lookup_asof(&JoinKey::Int(42), 9_000_000_000, 0, None) {
        AsofLookup::Hit(row) => assert_eq!(
            row.field_value("ts"),
            Some(Value::Number(5_000_000_000.0)),
            "max_ts = 5s before eviction"
        ),
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }

    // 驱逐 ts=5s 的批 → max_ts 必须回落到剩余行的 1s（asof 不再命中旧 max）。
    win.evict_oldest();
    match win.join_lookup_asof(&JoinKey::Int(42), 9_000_000_000, 0, None) {
        AsofLookup::Hit(row) => assert_eq!(
            row.field_value("ts"),
            Some(Value::Number(1_000_000_000.0)),
            "max_ts must drop to the surviving row after the max row is evicted"
        ),
        AsofLookup::Miss => panic!("expected Hit, got Miss"),
        AsofLookup::Fallback => panic!("expected Hit, got Fallback"),
    }
    assert_eq!(
        win.join_lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(1)
    );
}

/// 增量驱逐的 **registry 缺失回退**（防御路径）：未注册 seq 的 `remove_batch`
/// 走全量扫描（no-op），已注册 seq 正常增量删除；registry 条目随驱逐清理。
/// 直接构造 `JoinIndex`（buffer 私有字段，测试子模块可见）以注入缺失态——
/// 生产路径（append 必注册）难触发，此为防回归兜底。
#[test]
fn join_index_remove_batch_fallback_when_registry_missing() {
    use crate::window::buffer::JoinIndex;

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Int64, false),
    ]));
    let index = JoinIndex {
        key_field: "value".into(),
        projection: None,
        shards: (0..JOIN_INDEX_SHARDS)
            .map(|_| parking_lot::RwLock::new(crate::match_engine::EngineHashMap::default()))
            .collect(),
        mask: JOIN_INDEX_SHARDS - 1,
        batch_keys: parking_lot::RwLock::new(crate::match_engine::EngineHashMap::default()),
    };
    let batch = Arc::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1_000_000_000i64])),
                Arc::new(Int64Array::from(vec![42i64])),
            ],
        )
        .unwrap(),
    );
    let ts_list = vec![Some(1_000_000_000i64)];
    index.index_batch(&batch, &ts_list, 7);

    // 已注册：lookup 命中。
    assert_eq!(
        index.lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(1),
        "indexed row visible"
    );

    // 未注册 seq：回退全量扫描，无匹配行 → 行保留（no-op，不 panic）。
    index.remove_batch(99);
    assert_eq!(
        index.lookup(&JoinKey::Int(42), None).map(|v| v.len()),
        Some(1),
        "registry-miss removal must not touch unrelated rows"
    );

    // 已注册 seq：增量删除 + registry 条目清理。
    index.remove_batch(7);
    assert!(
        index.lookup(&JoinKey::Int(42), None).is_none_or(|v| v.is_empty()),
        "registered seq removal must clear the rows"
    );
    assert!(
        index.batch_keys.read().is_empty(),
        "batch_keys entry must be dropped on removal"
    );
}

/// 并发 append + 驱逐 + lookup 无死锁/无 panic（分片锁 + batch_keys 锁的锁序
/// 回归：index_batch 先片锁后 registry 锁、remove_batch 先 registry 后片锁，
/// 均不跨锁等待另一把——此测试钉死交错路径）。
#[test]
fn join_index_concurrent_append_evict_lookup_no_deadlock() {
    use std::sync::atomic::AtomicBool;
    use std::thread;

    let win = Arc::new(test_window(3600, usize::MAX));
    win.set_join_key("value".into());
    let stop = Arc::new(AtomicBool::new(false));

    // 写者：持续 append（key 循环，跨分片分布）。
    let w = Arc::clone(&win);
    let sw = Arc::clone(&stop);
    let writer = thread::spawn(move || {
        let mut k = 0i64;
        while !sw.load(Ordering::Relaxed) {
            let times: Vec<i64> =
                (0..32).map(|i| 1_000_000_000i64 + i * 1000 + k * 32).collect();
            let vals: Vec<i64> = (0..32).map(|i| (k + i) % 16).collect();
            w.append(make_batch(w.schema(), &times, &vals)).unwrap();
            k += 1;
        }
    });

    // 驱逐者：持续弹 front（时间驱逐同路径：remove_batch_from_index）。
    let e = Arc::clone(&win);
    let se = Arc::clone(&stop);
    let evictor = thread::spawn(move || {
        while !se.load(Ordering::Relaxed) {
            e.evict_oldest();
        }
    });

    // 读者（主线程）：持续随机 key 查找（join_lookup + asof 两路径）。
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut hit = 0usize;
    for _ in 0..20_000 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let key = JoinKey::Int(((state >> 33) % 16) as i64);
        if win.join_lookup(&key, None).is_some() {
            hit += 1;
        }
        let _ = win.join_lookup_asof(&key, i64::MAX, i64::MIN, None);
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer join");
    evictor.join().expect("evictor join");
    // 只断言无死锁/无 panic（hit 数随交错变化，无固定期望）。
    std::hint::black_box(hit);
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

// -- join 索引 append 微基准（2026-08-24 q9/q4 性能归因） ---------------------
//
// q9/q4 的 join 目标（bid_events，30M 时 27.6M 行）每次 append 都要 index_batch
// （逐行按 key 建列式 JoinIndex）；q8 的 join 目标只有 auction_events 1.8M 行，
// 差 15×，正好对应 q9（~0.9M EPS）比 q8（23M EPS）慢 ~25×。本基准量出 append
// 有无 join 索引的 ns/row 差，把「索引维护」从「纯 append」里分离出来。
//
// 运行：
//   cargo test --release -p wf-engine join_index_append_bench -- --ignored --nocapture
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine join_index_append_bench -- --ignored --nocapture"]
fn join_index_append_bench() {
    use std::time::Instant;

    const N: usize = 1_000_000;
    const BATCH: usize = 2000; // 对齐 bench 帧大小（每批 ~2000 行）

    // 数据：ts 递增（避免驱逐触发，隔离纯 append+index），value 0..9999（10k 去重键，
    // 近似 q9 bid.auction 的去重度）。
    fn make(win: &Window, batch_idx: usize) -> RecordBatch {
        let schema = win.schema().clone();
        let base = batch_idx * BATCH;
        let times: Vec<i64> = (0..BATCH).map(|i| (base + i) as i64 * 100_000).collect();
        let values: Vec<i64> = (0..BATCH).map(|i| (base + i) as i64 % 10_000).collect();
        make_batch(&schema, &times, &values)
    }

    // baseline：无 join 索引（纯 append）
    let win = test_window(3600, usize::MAX);
    let start = Instant::now();
    for b in 0..(N / BATCH) {
        win.append(make(&win, b)).unwrap();
    }
    let baseline_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // join 目标：set_join_key 后 append（append + index_batch）
    let win = test_window(3600, usize::MAX);
    win.set_join_key("value".into());
    let start = Instant::now();
    for b in 0..(N / BATCH) {
        win.append(make(&win, b)).unwrap();
    }
    let indexed_ns = start.elapsed().as_nanos() as f64 / N as f64;

    // set_join_key 初始建索引（空窗 → 无行，仅建空结构；真实引擎 spawn 时调用，
    // 不计入每行成本，但单独量一下以防 rebuild 开销被误读）。
    let win = test_window(3600, usize::MAX);
    let start = Instant::now();
    win.set_join_key("value".into());
    let set_key_ns = start.elapsed().as_nanos() as f64;

    eprintln!("[join-index-append-bench] N={N}, batch={BATCH}, keys=10000");
    eprintln!(
        "[join-index-append-bench] {:<28} {:>9.1} ns/row  ({:>6.2}M rows/s)",
        "append (no index)",
        baseline_ns,
        1e9 / baseline_ns / 1e6
    );
    eprintln!(
        "[join-index-append-bench] {:<28} {:>9.1} ns/row  ({:>6.2}M rows/s)",
        "append + join index",
        indexed_ns,
        1e9 / indexed_ns / 1e6
    );
    eprintln!(
        "[join-index-append-bench] {:<28} {:>9.1} ns/row  ({:>5.1}% overhead)",
        "index_batch (diff)",
        indexed_ns - baseline_ns,
        (indexed_ns - baseline_ns) / baseline_ns * 100.0
    );
    eprintln!(
        "[join-index-append-bench] set_join_key (empty) = {:.1} ns",
        set_key_ns
    );
}
