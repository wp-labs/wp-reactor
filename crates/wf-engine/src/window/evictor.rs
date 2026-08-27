use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::registry::WindowRegistry;
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// EvictReport
// ---------------------------------------------------------------------------

/// Per-window eviction counts.
#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct WindowEvictCount {
    pub window_name: String,
    pub time_evicted: usize,
}

/// Summary of a single [`Evictor::run_once`] call.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct EvictReport {
    pub windows_scanned: usize,
    pub batches_time_evicted: usize,
    pub batches_memory_evicted: usize,
    pub per_window_evicted: Vec<WindowEvictCount>,
    /// Set when the aggregate memory stayed over `max_total_bytes` but no
    /// window had a safe-to-drop (fully-acked) oldest batch. The caller
    /// (window actor) is expected to apply backpressure (stop accepting new
    /// appends) rather than let the evictor lose data a pull rule has not
    /// yet read.
    pub memory_pressure: bool,
}

/// memory_pressure（全局超限且无可驱逐批）持续多久后强制放行 append。
///
/// 兜底判据（2026-08-26 q20 join 窗死锁修复）：主判据是「全部消费者追平」
/// （规则无事可做 → 立即放行）；此超时防主判据失效的极端场景（如消费者
/// 注册了但永不推进 ack）。10s 给规则追平留足时间（q13 式背压通常远快于
/// 此），超过即视为死锁倾向。放行 = 宁可窗口瞬时超限驻留（`min_acked`
/// 保护未读批，不丢数据），也不让上游（mailbox/parse/receiver）因 gate
/// 永久停车而冻结。
pub const GATE_PRESSURE_RELEASE_AFTER: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// EvictionGate
// ---------------------------------------------------------------------------

/// Shared state bridging the periodic [`Evictor`] and the window actors'
/// append path for **memory backpressure**.
///
/// The evictor runs on a timer and reclaims memory via time-based eviction
/// gated by the consumption floor (every live consumer has acked a batch
/// before it is dropped). When global memory is still over budget but no
/// window has a *safe-to-drop* oldest batch, the evictor cannot reclaim
/// without losing unread pull data — so instead the actors must stop
/// appending (the "conveyor belt stops") until the evictor's next sweep
/// frees space. [`EvictionGate::freed`] is the [`Notify`] the evictor
/// signals after each sweep; actors park on it while over budget.
///
/// **死锁打破（2026-08-26 q20 join 窗死锁修复）**：停车假设「规则最终追平 →
/// 驱逐恢复」。当某窗不可驱逐（如 over_cap=2d 的 join 目标窗）时，全局 cap
/// 永不满足，停车会退化为永久冻结（上游 mailbox/parse/receiver 全停）。
/// 故 evictor 每轮额外评估 [`EvictionGate::force_release`]：全部消费者追平
/// （规则无事可做）立即放行，memory_pressure 持续超时（默认 10s）兜底
/// 放行——宁可窗口瞬时超限驻留（`min_acked` 保护未读批，不丢数据），
/// 也不让数据流停死。全局回落 cap 以下后清除，恢复正常背压。
pub struct EvictionGate {
    /// Global memory budget across all windows (bytes).
    pub max_total_bytes: usize,
    /// Latest aggregate window memory across all windows (bytes),
    /// recomputed by the evictor on every sweep. Window actors read this on
    /// their hot append path — a single atomic load — to decide whether to
    /// apply memory backpressure (stop appending) instead of waiting on the
    /// evictor to free space via `freed`.
    pub current_bytes: AtomicUsize,
    /// Signaled by the evictor after every sweep so parked actors re-check.
    pub freed: Notify,
    /// memory_pressure 首次连续上报时刻（`None` = 当前无 pressure）。超时
    /// 兜底放行判据，见 [`GATE_PRESSURE_RELEASE_AFTER`]。
    pressure_since: Mutex<Option<Instant>>,
    /// memory_pressure 持续多久后强制放行（超时兜底）。默认
    /// [`GATE_PRESSURE_RELEASE_AFTER`]；测试可注入短值。
    pub pressure_release_after: std::time::Duration,
    /// 强制放行标志：窗口 actor 停车循环读取，置位则跳过停车继续 append。
    /// 由 evictor 每轮评估（全部消费者追平 or pressure 超时兜底）；全局
    /// 回落 cap 以下时清除。
    force_release: AtomicBool,
}

impl EvictionGate {
    pub fn new(max_total_bytes: usize) -> Self {
        Self {
            max_total_bytes,
            current_bytes: AtomicUsize::new(0),
            freed: Notify::new(),
            pressure_since: Mutex::new(None),
            pressure_release_after: GATE_PRESSURE_RELEASE_AFTER,
            force_release: AtomicBool::new(false),
        }
    }

    /// 窗口 actor 停车循环的放行查询：`true` 表示跳过停车直接 append。
    #[inline]
    pub fn force_release(&self) -> bool {
        self.force_release.load(Ordering::Relaxed)
    }

    /// 测试钩子：直接置位/清除放行标志（actor 集成测试用它模拟 evictor
    /// 的评估结果，验证 `commit_append` 停车循环的实际放行行为）。
    #[cfg(test)]
    pub(crate) fn debug_set_force_release(&self, v: bool) {
        self.force_release.store(v, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Evictor
// ---------------------------------------------------------------------------

/// Periodic evictor that enforces time-based and global-memory-based eviction
/// across all windows in a [`WindowRegistry`].
pub struct Evictor {
    gate: Arc<EvictionGate>,
}

impl Evictor {
    pub fn new(gate: Arc<EvictionGate>) -> Self {
        Self { gate }
    }

    /// Run one eviction cycle.
    ///
    /// **Phase 1 — time eviction**: calls [`Window::evict_expired`] on every
    /// window, removing batches whose max event time is older than
    /// `now_nanos - over`. This is purely event-time based and does not wait
    /// on the consumption floor — a slow rule that falls behind simply skips
    /// the evicted batches on its next pull (`gap_detected`).
    ///
    /// **Phase 2 — memory eviction (floor-respecting, lossless)**: while the
    /// aggregate memory across all windows exceeds `max_total_bytes`, evicts
    /// the oldest batch of the largest window **whose oldest batch is
    /// already fully acked** (`oldest_seq < min_acked`). A batch a live
    /// consumer has not yet read is never dropped. When no window has a
    /// safe-to-drop oldest batch, the sweep stops and sets
    /// `memory_pressure`: the actor applies backpressure (stops appending)
    /// instead of losing unread pull data. This replaces the old lossy
    /// "evict oldest regardless of floor" backstop that broke pull-mode
    /// window delivery.
    pub fn run_once(&self, registry: &WindowRegistry, _now_nanos: i64) -> EvictReport {
        let mut report = EvictReport {
            windows_scanned: 0,
            batches_time_evicted: 0,
            batches_memory_evicted: 0,
            per_window_evicted: Vec::new(),
            memory_pressure: false,
        };

        // Phase 1: time eviction — event-time based, gated on the pull
        // consumption floor so a lagging rule task never loses unread batches
        // (cursor gap). With no pull consumers (push mode) the floor is
        // u64::MAX and every expired batch is dropped, as before.
        let names: Vec<String> = registry.window_names();

        for name in &names {
            report.windows_scanned += 1;
            let win = registry.get_window(name).unwrap();
            let before = win.batch_count();
            // Time eviction must cut on the window's **event-time watermark**,
            // not wall clock. Nexmark event times are absolute (~2026-01-01),
            // so a wall-clock cutoff would treat every batch as expired on
            // every sweep, high-frequency-evicting all batches and starving
            // the actor's append write lock (the q5 pull-freeze).
            let floor = registry
                .progress(name)
                .map(|p| p.min_acked())
                .unwrap_or(u64::MAX);
            win.evict_expired_acked(win.watermark_nanos(), floor);
            let evicted = before - win.batch_count();
            report.batches_time_evicted += evicted;
            if evicted > 0 {
                report.per_window_evicted.push(WindowEvictCount {
                    window_name: name.clone(),
                    time_evicted: evicted,
                });
            }
        }

        // Phase 2: memory eviction — floor-respecting and lossless.
        //
        // We only ever drop a window's oldest batch when every live consumer
        // has already acked past it. If no window has a safe-to-drop oldest
        // batch, we stop and flag `memory_pressure` so the actor applies
        // backpressure (stops appending) instead of us losing unread pull
        // data.
        //
        // `total` tracks the live aggregate after Phase-1 time eviction and is
        // decremented on each reclaim, then published to `gate.current_bytes`
        // so window actors can cheaply observe the global budget on their hot
        // path (a single atomic load instead of walking every window per
        // append).
        let mut total = 0usize;
        for name in &names {
            total += registry.get_window(name).unwrap().memory_usage();
        }

        while total > self.gate.max_total_bytes {
            // Pick the largest window whose oldest batch is fully acked
            // (safe to drop). Preferring the largest gives the fastest
            // memory relief without ever touching unacked data.
            let mut target: Option<&str> = None;
            let mut target_mem = 0usize;
            for name in &names {
                let win = registry.get_window(name).unwrap();
                let floor = registry
                    .progress(name)
                    .map(|p| p.min_acked())
                    .unwrap_or(u64::MAX);
                // D4: a window whose front batch is held by a retention pin is
                // not a reclaim candidate (its rows may still be needed by a
                // deferred join's pending evaluations). Including it would make
                // `evict_oldest_acked` return `None` and the sweep would stop
                // without signalling `memory_pressure`, letting the engine
                // append past the global cap unchecked.
                if win.oldest_seq().is_some_and(|s| s < floor) && !win.front_pinned_by_retention() {
                    let mem = win.memory_usage();
                    if mem > target_mem {
                        target_mem = mem;
                        target = Some(name);
                    }
                }
            }

            match target {
                Some(name) => {
                    let win = registry.get_window(name).unwrap();
                    let floor = registry
                        .progress(name)
                        .map(|p| p.min_acked())
                        .unwrap_or(u64::MAX);
                    match win.evict_oldest_acked(floor) {
                        Some(reclaimed) => {
                            total = total.saturating_sub(reclaimed);
                            report.batches_memory_evicted += 1;
                        }
                        // Defensive: the candidate check should have excluded
                        // unacked / pinned fronts. If it still returns `None`
                        // (window raced to empty, or a pin landed between the
                        // scan and the call), treat it as memory pressure —
                        // never silently continue appending over the cap.
                        None => {
                            report.memory_pressure = true;
                            break;
                        }
                    }
                }
                None => {
                    // Over budget but nothing safe to drop: signal
                    // backpressure rather than lose unread pull data.
                    report.memory_pressure = true;
                    break;
                }
            }
        }

        // 死锁打破判定（2026-08-26 q20 join 窗死锁修复）。全局超限时：
        // 1. 立即放行：所有窗口消费者已追平（`min_acked >= next_seq`，规则
        //    无事可做）——继续停车只会让上游数据流永久冻结（join 目标窗如
        //    over_cap=2d 的 auction_events 不可驱逐时，全局 cap 永不满足）。
        // 2. 超时兜底：memory_pressure（无可驱逐批）持续超过
        //    [`GATE_PRESSURE_RELEASE_AFTER`]。
        // 放行 = 宁可窗口瞬时超限驻留（`min_acked` 保护未读批，不丢数据），
        // 也不让 gate 永久停车；规则追平后驱逐恢复，内存自然回落。
        // 全局回落 cap 以下 → 清除标志恢复正常背压。
        if total > self.gate.max_total_bytes {
            let all_caught_up = names.iter().all(|name| {
                let win = registry.get_window(name).unwrap();
                let floor = registry
                    .progress(name)
                    .map(|p| p.min_acked())
                    .unwrap_or(u64::MAX);
                // 无消费者窗口 `min_acked` = u64::MAX（天然追平，可驱逐）。
                floor >= win.next_seq()
            });
            if all_caught_up {
                self.gate.force_release.store(true, Ordering::Relaxed);
                *self.gate.pressure_since.lock().unwrap() = None;
            } else if report.memory_pressure {
                let mut since = self.gate.pressure_since.lock().unwrap();
                let now = Instant::now();
                let first = since.get_or_insert(now);
                if now.duration_since(*first) >= self.gate.pressure_release_after {
                    self.gate.force_release.store(true, Ordering::Relaxed);
                }
            }
        } else {
            self.gate.force_release.store(false, Ordering::Relaxed);
            *self.gate.pressure_since.lock().unwrap() = None;
        }

        // Publish the post-eviction aggregate so actors see the reclaimed
        // space immediately, then wake any actor parked on the memory
        // backpressure gate; it re-checks and resumes appending once space
        // is available.
        self.gate.current_bytes.store(total, Ordering::Relaxed);
        self.gate.freed.notify_waiters();

        report
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::window::buffer::content_bytes;
    use crate::window::{WindowDef, WindowParams};
    use arrow::array::{Int64Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("value", DataType::Int64, false),
        ]))
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

    fn test_config() -> WindowConfig {
        WindowConfig {
            name: "default".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(5).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        }
    }

    // -- 1. evictor_time_eviction ---------------------------------------------

    #[test]
    fn evictor_time_eviction() {
        let schema = test_schema();
        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "win_a".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(10),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        // Append 1s, 5s, then 25s to advance the event-time watermark to
        // 25s - 5s(watermark delay) = 20s. Time eviction now cuts on the
        // window's watermark (not an externally passed wall clock).
        {
            let win = reg.get_window("win_a").unwrap();
            win.append_with_watermark(make_batch(&schema, &[1_000_000_000], &[100]))
                .unwrap();
            win.append_with_watermark(make_batch(&schema, &[5_000_000_000], &[200]))
                .unwrap();
            win.append_with_watermark(make_batch(&schema, &[25_000_000_000], &[300]))
                .unwrap();
            assert_eq!(win.batch_count(), 3);
        }

        // watermark=20s, cutoff = 20s - 10s = 10s → batches (max 1s, 5s) < 10s → evicted
        let evictor = Evictor::new(Arc::new(EvictionGate::new(usize::MAX)));
        let report = evictor.run_once(&reg, 0);

        assert_eq!(report.windows_scanned, 1);
        assert_eq!(report.batches_time_evicted, 2);
        assert_eq!(report.batches_memory_evicted, 0);

        let win = reg.get_window("win_a").unwrap();
        assert_eq!(win.batch_count(), 1, "25s batch (>= cutoff 10s) survives");
    }

    // -- 2. evictor_global_memory_cap -----------------------------------------

    #[test]
    fn evictor_global_memory_cap() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = content_bytes(&probe);

        let reg = WindowRegistry::build(vec![
            WindowDef {
                params: WindowParams {
                    name: "win_a".into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(3600),
                    materialize_fields: None,
                    defer_materialization: false,
                },
                streams: vec![],
                config: test_config(),
            },
            WindowDef {
                params: WindowParams {
                    name: "win_b".into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(3600),
                    materialize_fields: None,
                    defer_materialization: false,
                },
                streams: vec![],
                config: test_config(),
            },
        ])
        .unwrap();

        // win_a gets 2 batches, win_b gets 1 → total = 3 * one_batch_size
        {
            let w = reg.get_window("win_a").unwrap();
            w.append(make_batch(&schema, &[1_000_000_000], &[100]))
                .unwrap();
            w.append(make_batch(&schema, &[2_000_000_000], &[200]))
                .unwrap();
        }
        {
            let w = reg.get_window("win_b").unwrap();
            w.append(make_batch(&schema, &[3_000_000_000], &[300]))
                .unwrap();
        }

        // Cap at 2 batches. now=0 → no time eviction.
        let evictor = Evictor::new(Arc::new(EvictionGate::new(one_batch_size * 2)));
        let report = evictor.run_once(&reg, 0);

        assert_eq!(report.batches_time_evicted, 0);
        assert_eq!(report.batches_memory_evicted, 1);

        // Total memory should be under cap.
        let total: usize = ["win_a", "win_b"]
            .iter()
            .map(|n| {
                let w = reg.get_window(n).unwrap();
                w.memory_usage()
            })
            .sum();
        assert!(total <= one_batch_size * 2);
    }

    // -- 3. evictor_long_running_memory_stabilization -------------------------

    /// Simulates continuous data injection over a long period with periodic
    /// eviction. Validates that window memory stabilizes (does not grow
    /// unbounded) once the over-window fills up.
    ///
    /// Scenario:
    ///   - window over = 10s, new batch every 2s (5 batches per over window)
    ///   - run for 2000 iterations (4000s simulated time, 400 over-windows)
    ///   - after warmup (50 iterations), memory should oscillate around
    ///     ~5 batches' worth of data
    ///
    /// Run with: cargo test -p wf-engine --features mem_test evictor_long
    #[cfg(feature = "mem_test")]
    #[test]
    fn evictor_long_running_memory_stabilization() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = content_bytes(&probe);

        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "data".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(10),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        let evictor = Evictor::new(Arc::new(EvictionGate::new(usize::MAX)));

        let mut memory_samples: Vec<usize> = Vec::new();
        let mut batch_counts: Vec<usize> = Vec::new();

        let total_iterations = 2000;
        let step_nanos = 2_000_000_000i64; // 2s per step
        let over_nanos = 10_000_000_000i64; // 10s over window
        // Retained span = over + watermark delay (test_config: 5s): the
        // watermark lags the newest event time by the delay, so the time sweep
        // keeps events within (over + delay) of the watermark.
        let watermark_delay_nanos = 5_000_000_000i64;
        let expected_batches_per_window =
            ((over_nanos + watermark_delay_nanos) / step_nanos) as usize; // ~7

        for i in 0..total_iterations {
            let now = (i as i64 + 1) * step_nanos;

            // Inject one batch at current time. `append_with_watermark` so the
            // event-time watermark advances (the evictor's time sweep cuts on
            // the window's watermark, not an external wall clock — q5 fix); a
            // plain `append` leaves the watermark uninitialized and nothing is
            // ever expired.
            {
                let win = reg.get_window("data").unwrap();
                let value = i as i64 * 10;
                win.append_with_watermark(make_batch(&schema, &[now], &[value]))
                    .unwrap();
            }

            // Run eviction.
            evictor.run_once(&reg, now);

            // Sample metrics after every eviction.
            {
                let win = reg.get_window("data").unwrap();
                memory_samples.push(win.memory_usage());
                batch_counts.push(win.batch_count());
            }
        }

        // ---- Assertions ----

        // 1. After warmup (first 50 iterations), memory should never exceed
        //    ~8 batches (7 retained = over + watermark delay, +1 grace).
        let warmup = 10;
        let max_after_warmup = memory_samples[warmup..].iter().max().copied().unwrap();
        assert!(
            max_after_warmup <= one_batch_size * (expected_batches_per_window + 1),
            "memory should stabilize after warmup: max={max_after_warmup} > {} ({} batches + 1 grace)",
            one_batch_size * (expected_batches_per_window + 1),
            expected_batches_per_window
        );

        // 2. Batch count should NOT grow unbounded.
        let max_batches_after_warmup = batch_counts[warmup..].iter().max().copied().unwrap();
        assert!(
            max_batches_after_warmup <= expected_batches_per_window + 1,
            "batch count should stabilize: max={max_batches_after_warmup} > {}",
            expected_batches_per_window + 1
        );

        // 3. Memory samples in the LAST 50 iterations should oscillate within
        //    a tight range (not trending up). The max in the second half
        //    should be <= the max in a broader window after warmup.
        let second_half = memory_samples.len() / 2;
        let max_second_half = memory_samples[second_half..].iter().max().copied().unwrap();
        assert!(
            max_second_half <= one_batch_size * (expected_batches_per_window + 1),
            "memory in second half should not grow: max={max_second_half}"
        );

        // 4. The final memory should be within the expected range (not
        //    accumulating over time).
        let final_memory = memory_samples.last().copied().unwrap();
        assert!(
            final_memory >= one_batch_size
                && final_memory <= one_batch_size * (expected_batches_per_window + 1),
            "final memory should be in range [1, {}] batches, got {final_memory} bytes (~{} batches)",
            expected_batches_per_window + 1,
            final_memory / one_batch_size
        );

        // 5. Batch count should NOT grow beyond the over window capacity.
        //     After warmup, max batches should be <= expected + 2.
        let max_batches = batch_counts[warmup..].iter().max().copied().unwrap();
        assert!(
            max_batches <= expected_batches_per_window + 2,
            "batch count should not grow: max={max_batches} > {}",
            expected_batches_per_window + 2
        );

        // 6. The final batch count should be in the expected range
        //     (not accumulating over time).
        let final_batches = batch_counts.last().copied().unwrap();
        assert!(
            final_batches >= 1 && final_batches <= expected_batches_per_window + 2,
            "final batch count should be in range [1, {}], got {final_batches}",
            expected_batches_per_window + 2
        );
    }

    // -- 4. evictor_long_running_with_snapshots ---------------------------------

    /// Similar to the long-running test above, but additionally simulates
    /// rule-like snapshot reads on each iteration. The snapshots are
    /// dropped before the next iteration — this verifies that Arc references
    /// from snapshot() / read_since() are released and don't prevent
    /// eviction memory from being reclaimed.
    ///
    /// Run with: cargo test -p wf-engine --features mem_test evictor_long
    #[cfg(feature = "mem_test")]
    #[test]
    fn evictor_long_running_with_snapshots() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = content_bytes(&probe);

        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "data".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(10),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        let evictor = Evictor::new(Arc::new(EvictionGate::new(usize::MAX)));
        let step_nanos = 2_000_000_000i64;
        let over_nanos = 10_000_000_000i64;
        // Retained span = over + watermark delay (test_config: 5s).
        let expected_batches_per_window = ((over_nanos + 5_000_000_000i64) / step_nanos) as usize;

        let total_iterations = 2000;
        let mut memory_samples: Vec<usize> = Vec::new();

        // Hold a cursor to simulate read_since behavior.
        let mut cursor: u64 = 0;

        for i in 0..total_iterations {
            let now = (i as i64 + 1) * step_nanos;

            // Inject. `append_with_watermark` so the time sweep sees an
            // advancing event-time watermark (see the long-running test).
            {
                let win = reg.get_window("data").unwrap();
                win.append_with_watermark(make_batch(&schema, &[now], &[(i * 10) as i64]))
                    .unwrap();
            }

            // Simulate rule reading: take a snapshot, process, drop.
            {
                let win = reg.get_window("data").unwrap();
                let (_batches, new_cursor, _gap) = win.read_since(cursor);
                cursor = new_cursor;
                // _batches is dropped here — Arc refcount decremented.
            }

            // Evict.
            evictor.run_once(&reg, now);

            // Sample.
            {
                let win = reg.get_window("data").unwrap();
                memory_samples.push(win.memory_usage());
            }
        }

        // Memory after warmup should stabilize — snapshots don't leak.
        let warmup = 50;
        let max_after_warmup = memory_samples[warmup..].iter().max().copied().unwrap();
        assert!(
            max_after_warmup <= one_batch_size * (expected_batches_per_window + 1),
            "memory should stabilize with snapshots: max={max_after_warmup}"
        );
    }

    // -- 5. evictor_long_running_multi_window ----------------------------------

    /// Three windows with different over durations, running simultaneously.
    /// Validates that per-window eviction works independently and global
    /// memory doesn't leak.
    ///
    /// Run with: cargo test -p wf-engine --features mem_test evictor_long
    #[cfg(feature = "mem_test")]
    #[test]
    fn evictor_long_running_multi_window() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = content_bytes(&probe);

        let reg = WindowRegistry::build(vec![
            WindowDef {
                params: WindowParams {
                    name: "short".into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(5), // 5s over
                    materialize_fields: None,
                    defer_materialization: false,
                },
                streams: vec![],
                config: test_config(),
            },
            WindowDef {
                params: WindowParams {
                    name: "medium".into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(20), // 20s over
                    materialize_fields: None,
                    defer_materialization: false,
                },
                streams: vec![],
                config: test_config(),
            },
            WindowDef {
                params: WindowParams {
                    name: "alert".into(),
                    schema: schema.clone(),
                    time_col_index: None, // no time column → never time-evicted
                    over: Duration::ZERO,
                    materialize_fields: None,
                    defer_materialization: false,
                },
                streams: vec![],
                config: test_config(),
            },
        ])
        .unwrap();

        // Alert window needs memory eviction since it has no time col.
        // Global cap = 10 batches' worth (shared across all windows).
        let evictor = Evictor::new(Arc::new(EvictionGate::new(one_batch_size * 10)));
        let step_nanos = 1_000_000_000i64; // 1s per step

        let total_iterations = 3000;
        let warmup = 100;

        let mut memory_short: Vec<usize> = Vec::new();
        let mut memory_medium: Vec<usize> = Vec::new();
        let mut memory_alert: Vec<usize> = Vec::new();

        for i in 0..total_iterations {
            let now = (i as i64 + 1) * step_nanos;

            // Inject into all three windows.
            for name in &["short", "medium", "alert"] {
                let win = reg.get_window(name).unwrap();
                win.append(make_batch(&schema, &[now], &[(i * 10) as i64]))
                    .unwrap();
            }

            evictor.run_once(&reg, now);

            for (name, samples) in [
                ("short", &mut memory_short),
                ("medium", &mut memory_medium),
                ("alert", &mut memory_alert),
            ] {
                let win = reg.get_window(name).unwrap();
                samples.push(win.memory_usage());
            }
        }

        // 1. Short window (over=5s) — max ~5 batches after warmup.
        let max_short = memory_short[warmup..].iter().max().copied().unwrap();
        assert!(
            max_short <= one_batch_size * 6,
            "short window (over=5s) memory should stabilize: max={max_short}"
        );

        // 2. Medium window (over=20s) — max ~20 batches after warmup.
        let max_medium = memory_medium[warmup..].iter().max().copied().unwrap();
        assert!(
            max_medium <= one_batch_size * 21,
            "medium window (over=20s) memory should stabilize: max={max_medium}"
        );

        // 3. Alert window (no time eviction) — should be kept in check by
        //    the global memory cap (10 batches).
        let max_alert = memory_alert.iter().max().copied().unwrap();
        let global_cap_batches = 10;
        assert!(
            max_alert <= one_batch_size * (global_cap_batches + 2),
            "alert window memory should be bounded by global cap: max={max_alert}"
        );

        // 4. Total memory across all windows should not grow unbounded.
        let final_total: usize = memory_short.last().copied().unwrap()
            + memory_medium.last().copied().unwrap()
            + memory_alert.last().copied().unwrap();
        let max_total: usize = memory_short[warmup..]
            .iter()
            .zip(memory_medium[warmup..].iter())
            .zip(memory_alert[warmup..].iter())
            .map(|((s, m), a)| s + m + a)
            .max()
            .unwrap();
        assert!(
            final_total <= one_batch_size * 40,
            "total memory should not grow unbounded: final={final_total}, max={max_total}"
        );
    }

    // -- 6. evictor_burst_then_drain ------------------------------------------

    /// Simulates a traffic burst followed by a quiet period. Validates that
    /// memory is fully released after the over window expires.
    ///
    /// Scenario:
    ///   - window over = 10s
    ///   - Burst: 100 batches injected at t=0s → peak memory ≈ 100 batches
    ///   - Drain: advance now from 0s → 20s in steps, evict each step
    ///   - After t > 10s (over expired), memory should drop to 0
    ///
    /// This directly answers: "does the window release memory after a burst?"
    ///
    /// Run with: cargo test -p wf-engine --features mem_test evictor_burst
    #[cfg(feature = "mem_test")]
    #[test]
    fn evictor_burst_then_drain() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = content_bytes(&probe);

        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "data".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(10),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        let evictor = Evictor::new(Arc::new(EvictionGate::new(usize::MAX)));
        // Drain injects one batch every 2s; retained span = over(10s) + watermark
        // delay(5s) → ~7 batches.
        let expected_batches_per_window =
            ((10_000_000_000i64 + 5_000_000_000i64) / 2_000_000_000i64) as usize;

        // Phase 1 — Burst: inject 100 batches, all at t = 1s
        let burst_count = 100;
        for i in 0..burst_count {
            let win = reg.get_window("data").unwrap();
            win.append(make_batch(&schema, &[1_000_000_000], &[i as i64]))
                .unwrap();
        }

        let win = reg.get_window("data").unwrap();
        let peak_memory = win.memory_usage();
        let peak_batches = win.batch_count();

        assert!(
            peak_batches == burst_count as usize,
            "after burst: expected {burst_count} batches, got {peak_batches}"
        );
        assert!(
            peak_memory >= one_batch_size * (burst_count as usize),
            "peak memory too low: {peak_memory} < {} * {burst_count}",
            one_batch_size
        );

        // Phase 2 — Drain: the evictor's time sweep cuts on the window's
        // **event-time watermark** (q5 fix), so advancing the wall clock alone
        // cannot expire the burst — inject one watermark-advancing batch per
        // step to simulate new events arriving. The burst batches (t=1s) expire
        // once the watermark passes 11s (cutoff = watermark - 10s > 1s).
        let drain_steps = 12usize;
        let mut memory_samples: Vec<usize> = vec![peak_memory];
        for step in 1..=drain_steps {
            let now_nanos = (step as i64 + 1) * 2_000_000_000i64;
            {
                let win = reg.get_window("data").unwrap();
                win.append_with_watermark(make_batch(&schema, &[now_nanos], &[999 + step as i64]))
                    .unwrap();
            }
            evictor.run_once(&reg, now_nanos);

            let win = reg.get_window("data").unwrap();
            memory_samples.push(win.memory_usage());
        }

        // ---- Assertions ----

        // 1. The burst (100 same-timestamp batches) is fully evicted once the
        //    watermark advances past 11s — memory drops back to the live
        //    window's span (~over + watermark delay batches), far below peak.
        let win = reg.get_window("data").unwrap();
        let final_memory = win.memory_usage();
        let final_batches = win.batch_count();
        assert!(
            final_memory < peak_memory,
            "burst must be drained: final {final_memory} >= peak {peak_memory}"
        );
        assert!(
            final_batches <= expected_batches_per_window + 2,
            "after drain, batch count should be within the live window: got {final_batches} (expected <= {} + 2)",
            expected_batches_per_window
        );
        assert!(
            final_memory <= one_batch_size * (expected_batches_per_window + 2),
            "after drain, memory should be within the live window: got {final_memory} (> {} * {} + 2)",
            one_batch_size,
            expected_batches_per_window
        );
    }

    // -- 7. evictor_empty_registry --------------------------------------------

    #[test]
    fn evictor_empty_registry() {
        let reg = WindowRegistry::build(vec![]).unwrap();
        let evictor = Evictor::new(Arc::new(EvictionGate::new(1024)));
        let report = evictor.run_once(&reg, 0);

        assert_eq!(report.windows_scanned, 0);
        assert_eq!(report.batches_time_evicted, 0);
        assert_eq!(report.batches_memory_evicted, 0);
    }

    // -- 8. evictor_time_eviction_respects_consumption_floor ------------------

    /// Phase 1 time eviction is event-time based and **gated on the pull
    /// consumption floor**: a slow shard that has not yet acked its tail keeps
    /// those (expired) batches in the log — they are dropped only once every
    /// consumer has acked past them (then the memory-pressure phase reclaims
    /// them). This is the pull-mode counterpart of the q3 memory-floor fix:
    /// a lagging rule task must never observe a cursor gap from the evictor
    /// sweep.
    #[test]
    fn evictor_time_eviction_respects_consumption_floor() {
        let schema = test_schema();
        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "data".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                // All batches expire once now > 1s + 1s = 2s.
                over: Duration::from_secs(1),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        // Two pull-rule shards consuming this window, like a sharded match rule.
        let progress = reg.progress("data").expect("progress table exists");
        let fast = progress.register();
        let slow = progress.register();

        // 10 batches, all at t = 1s (event time); advance the watermark to
        // 3s so cutoff = 3s - 1s(over) = 2s, making every 1s batch expired.
        let n: u64 = 10;
        for i in 0..n {
            let win = reg.get_window("data").unwrap();
            win.append(make_batch(&schema, &[1_000_000_000], &[i as i64]))
                .unwrap();
        }
        reg.get_window("data")
            .unwrap()
            .set_watermark_for_test(3_000_000_000);

        // Fast shard acked everything; slow shard still lags (only acked
        // through seq 3 → min_acked = 4). Batches seq ≥ 4 are expired but
        // unread — they must NOT be time-evicted.
        fast.store(n, Ordering::Release);
        slow.store(4, Ordering::Release);

        let evictor = Evictor::new(Arc::new(EvictionGate::new(usize::MAX))); // disable Phase 2
        evictor.run_once(&reg, 3_000_000_000i64); // now=3s > 2s, all expired

        let win = reg.get_window("data").unwrap();
        assert_eq!(
            win.batch_count() as u64,
            n - 4,
            "time eviction keeps the slow shard's unacked (expired) batches"
        );
        assert_eq!(
            win.oldest_seq(),
            Some(4),
            "only seq < min_acked (4) may be time-evicted"
        );
    }

    // -- 9. evictor_memory_eviction_respects_consumption_floor ----------------

    /// Phase 2 memory eviction must NOT drop a batch that a registered,
    /// live consumer has not yet acknowledged — even under memory pressure.
    ///
    /// This is the q3 pull root cause: `Evictor` phase 2 calls
    /// `Window::evict_oldest`, which deliberately ignores the consumption
    /// floor. In push mode that is harmless (data was already broadcast to
    /// rule channels before landing in the log); in pull mode the rule task
    /// depends on the log remaining readable, so a floor-less memory sweep
    /// silently drops unread batches -> lost alerts + cursor_gap.
    ///
    /// This test guards the corrected behaviour: with a live (unacked)
    /// consumer, the memory backstop must leave its unacked batches in place.
    #[test]
    fn evictor_memory_eviction_respects_consumption_floor() {
        let schema = test_schema();
        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "data".into(),
                schema: schema.clone(),
                // over = 0 disables Phase 1 so we isolate Phase 2.
                time_col_index: Some(0),
                over: Duration::ZERO,
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        // A pull-rule shard registers its consumption slot but has not read
        // anything yet (slot stays at 0).
        let _consumer = reg
            .progress("data")
            .expect("progress table exists")
            .register();

        // 10 batches, all live (no time eviction possible).
        let n: u64 = 10;
        for i in 0..n {
            let win = reg.get_window("data").unwrap();
            win.append(make_batch(&schema, &[1_000_000_000], &[i as i64]))
                .unwrap();
        }

        // Force Phase 2: global cap of 0 bytes => every batch is "over budget".
        let evictor = Evictor::new(Arc::new(EvictionGate::new(0)));
        evictor.run_once(&reg, 3_000_000_000i64);

        let win = reg.get_window("data").unwrap();
        assert_eq!(
            win.batch_count() as u64,
            n,
            "memory eviction must not drop batches a registered consumer has not acked"
        );
    }

    // -- 10. D4: evictor_global_memory_cap_pinned_window_signals_pressure ------

    /// D4 review regression (2026-08-24): the global evictor's candidate scan
    /// checked only the ack floor, so a window whose front batch is held by a
    /// **retention pin** was still selected; `evict_oldest_acked` then returned
    /// `None` and the old code silently `break` — over the cap with nothing
    /// reclaimed and **no `memory_pressure`**, the engine would keep appending
    /// unchecked (OOM risk). A pinned window must be excluded from candidates;
    /// when nothing is reclaimable the sweep must report pressure.
    #[test]
    fn evictor_global_memory_cap_pinned_window_signals_pressure() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = content_bytes(&probe);

        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "win_a".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        // 3 batches, cap at 2 → over budget, front batch pinned at 1s.
        for ts in [1_000_000_000, 2_000_000_000, 3_000_000_000] {
            reg.get_window("win_a")
                .unwrap()
                .append(make_batch(&schema, &[ts], &[100]))
                .unwrap();
        }
        let pin = reg
            .get_window("win_a")
            .unwrap()
            .register_retention_pin()
            .expect("registry windows are wired to progress");
        pin.store(1_000_000_000, Ordering::Release);

        let evictor = Evictor::new(Arc::new(EvictionGate::new(one_batch_size * 2)));
        let report = evictor.run_once(&reg, 0);

        assert_eq!(
            report.batches_memory_evicted, 0,
            "pinned front batch must not be memory-evicted"
        );
        assert!(
            report.memory_pressure,
            "over the global cap with only a pinned window reclaimable → must signal backpressure"
        );
        assert_eq!(reg.get_window("win_a").unwrap().batch_count(), 3);

        // Pin released (EOS): the same sweep now reclaims down to the cap.
        pin.store(i64::MAX, Ordering::Release);
        let report = evictor.run_once(&reg, 0);
        assert_eq!(report.batches_memory_evicted, 1);
        assert!(!report.memory_pressure);
    }

    // -- 11. D4: evictor_global_memory_cap_skips_pinned_window -----------------

    /// Pinned windows must be skipped as reclaim candidates **without** being
    /// mistaken for global pressure while other windows are still reclaimable:
    /// the sweep reclaims from the unpinned window and reports no pressure.
    #[test]
    fn evictor_global_memory_cap_skips_pinned_window() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = content_bytes(&probe);

        let mut defs = vec![];
        for name in ["pinned", "free"] {
            defs.push(WindowDef {
                params: WindowParams {
                    name: name.into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(3600),
                    materialize_fields: None,
                    defer_materialization: false,
                },
                streams: vec![],
                config: test_config(),
            });
        }
        let reg = WindowRegistry::build(defs).unwrap();

        // Both windows hold 3 batches; cap at 4 → need 2 reclaimed, all from
        // the unpinned one.
        for name in ["pinned", "free"] {
            for ts in [1_000_000_000, 2_000_000_000, 3_000_000_000] {
                reg.get_window(name)
                    .unwrap()
                    .append(make_batch(&schema, &[ts], &[100]))
                    .unwrap();
            }
        }
        let pin = reg
            .get_window("pinned")
            .unwrap()
            .register_retention_pin()
            .expect("wired");
        pin.store(1_000_000_000, Ordering::Release);

        let evictor = Evictor::new(Arc::new(EvictionGate::new(one_batch_size * 4)));
        let report = evictor.run_once(&reg, 0);

        assert_eq!(
            report.batches_memory_evicted, 2,
            "reclaim from the free window"
        );
        assert!(
            !report.memory_pressure,
            "reclaimable windows exist → no pressure despite a pinned window"
        );
        assert_eq!(
            reg.get_window("pinned").unwrap().batch_count(),
            3,
            "pinned untouched"
        );
        assert_eq!(reg.get_window("free").unwrap().batch_count(), 1);
    }

    // -- 12. gate 死锁打破（2026-08-26 q20 join 窗死锁修复） ----------------

    /// 构造「全局超限 + 无可驱逐（pin 窗）+ 全部消费者已追平」：
    /// evictor 每轮报 memory_pressure，但消费者追平 = 规则无事可做，继续
    /// 停车只会让上游数据流永久冻结（join 目标窗不可驱逐时全局 cap 永不
    /// 满足）→ 必须立即放行 append。
    #[test]
    fn gate_force_release_when_all_consumers_caught_up_but_over_cap() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = content_bytes(&probe);

        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "win".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        // 2 批，cap = 1 批 → 超限；front 批被 pin 住（join 目标窗）→ 不可驱逐。
        for ts in [1_000_000_000, 2_000_000_000] {
            reg.get_window("win")
                .unwrap()
                .append(make_batch(&schema, &[ts], &[100]))
                .unwrap();
        }
        let pin = reg
            .get_window("win")
            .unwrap()
            .register_retention_pin()
            .expect("wired");
        pin.store(1_000_000_000, Ordering::Release);
        // 消费者已 ack 全部 2 批（追平）——规则无事可做。slot 必须被持有
        // （drop 后 strong_count=0 → min_acked 退化为 u64::MAX，会误触发
        // 追平判据）。
        let slot = reg.progress("win").unwrap().register();
        slot.store(2, Ordering::Release);

        let gate = Arc::new(EvictionGate::new(one_batch_size));
        let evictor = Evictor::new(Arc::clone(&gate));
        let report = evictor.run_once(&reg, 0);

        assert!(report.memory_pressure, "无可驱逐 → pressure");
        assert!(
            gate.force_release(),
            "全部消费者追平 + 全局超限 → 立即放行（死锁打破）"
        );
    }

    /// 超时兜底：消费者**未**追平（规则慢）但 memory_pressure 持续超过
    /// `pressure_release_after` → 仍须放行（防「消费者注册了但永不推进
    /// ack」等追平判据失效的极端场景）。放行 = 宁可窗口超限驻留（min_acked
    /// 保护未读批，不丢数据），也不让数据流停死。
    #[test]
    fn gate_pressure_timeout_force_releases() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = content_bytes(&probe);

        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "win".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        for ts in [1_000_000_000, 2_000_000_000] {
            reg.get_window("win")
                .unwrap()
                .append(make_batch(&schema, &[ts], &[100]))
                .unwrap();
        }
        let pin = reg
            .get_window("win")
            .unwrap()
            .register_retention_pin()
            .expect("wired");
        pin.store(1_000_000_000, Ordering::Release);
        // 消费者只 ack 了第 1 批（未追平）→ 追平判据不触发，走超时兜底。
        let slot = reg.progress("win").unwrap().register();
        slot.store(1, Ordering::Release);

        let mut gate = EvictionGate::new(one_batch_size);
        gate.pressure_release_after = Duration::from_millis(50);
        let gate = Arc::new(gate);
        let evictor = Evictor::new(Arc::clone(&gate));

        evictor.run_once(&reg, 0);
        assert!(!gate.force_release(), "pressure 首轮 → 仅开始计时，不放行");

        std::thread::sleep(Duration::from_millis(80));
        evictor.run_once(&reg, 0);
        assert!(
            gate.force_release(),
            "memory_pressure 持续超时 → 强制放行（死锁打破）"
        );
    }

    /// 放行状态在全局回落 cap 以下时清除：恢复正常背压语义（后续若再超限
    /// 且消费者在追，仍按原机制停车等待）。
    #[test]
    fn gate_release_cleared_when_under_cap() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = content_bytes(&probe);

        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "win".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        for ts in [1_000_000_000, 2_000_000_000] {
            reg.get_window("win")
                .unwrap()
                .append(make_batch(&schema, &[ts], &[100]))
                .unwrap();
        }
        let pin = reg
            .get_window("win")
            .unwrap()
            .register_retention_pin()
            .expect("wired");
        pin.store(1_000_000_000, Ordering::Release);
        let slot = reg.progress("win").unwrap().register();
        slot.store(2, Ordering::Release);

        let gate = Arc::new(EvictionGate::new(one_batch_size));
        let evictor = Evictor::new(Arc::clone(&gate));

        // 追平 + 超限 → 放行置位。
        evictor.run_once(&reg, 0);
        assert!(gate.force_release());

        // pin 释放（join 读完后）→ 同一轮驱逐回收 → 全局回落 cap 以下。
        pin.store(i64::MAX, Ordering::Release);
        let report = evictor.run_once(&reg, 0);
        assert!(!report.memory_pressure);
        assert!(
            !gate.force_release(),
            "全局回落 cap 以下 → 清除放行标志，恢复正常背压"
        );
    }
}
