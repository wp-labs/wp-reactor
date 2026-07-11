use super::registry::WindowRegistry;

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
}

// ---------------------------------------------------------------------------
// Evictor
// ---------------------------------------------------------------------------

/// Periodic evictor that enforces time-based and global-memory-based eviction
/// across all windows in a [`WindowRegistry`].
pub struct Evictor {
    max_total_bytes: usize,
}

impl Evictor {
    pub fn new(max_total_bytes: usize) -> Self {
        Self { max_total_bytes }
    }

    /// Run one eviction cycle.
    ///
    /// **Phase 1 — time eviction**: calls [`Window::evict_expired`] on every
    /// window, removing batches whose max event time is older than
    /// `now_nanos - over`.
    ///
    /// **Phase 2 — memory eviction**: while the aggregate memory across all
    /// windows exceeds `max_total_bytes`, evicts the oldest batch from the
    /// window with the most memory.
    pub fn run_once(&self, registry: &WindowRegistry, now_nanos: i64) -> EvictReport {
        let mut report = EvictReport {
            windows_scanned: 0,
            batches_time_evicted: 0,
            batches_memory_evicted: 0,
            per_window_evicted: Vec::new(),
        };

        // Phase 1: time eviction
        let names: Vec<String> = registry.window_names();

        for name in &names {
            report.windows_scanned += 1;
            let win_lock = registry.get_window(name).unwrap();
            let mut win = win_lock.write().expect("window lock poisoned");
            let before = win.batch_count();
            win.evict_expired(now_nanos);
            let evicted = before - win.batch_count();
            report.batches_time_evicted += evicted;
            if evicted > 0 {
                report.per_window_evicted.push(WindowEvictCount {
                    window_name: name.clone(),
                    time_evicted: evicted,
                });
            }
        }

        // Phase 2: memory eviction
        loop {
            let mut total = 0usize;
            let mut largest_name: Option<&str> = None;
            let mut largest_mem = 0usize;

            for name in &names {
                let win_lock = registry.get_window(name).unwrap();
                let win = win_lock.read().expect("window lock poisoned");
                let mem = win.memory_usage();
                total += mem;
                if mem > largest_mem {
                    largest_mem = mem;
                    largest_name = Some(name);
                }
            }

            if total <= self.max_total_bytes {
                break;
            }

            match largest_name {
                Some(name) => {
                    let win_lock = registry.get_window(name).unwrap();
                    let mut win = win_lock.write().expect("window lock poisoned");
                    if win.evict_oldest().is_none() {
                        break;
                    }
                    report.batches_memory_evicted += 1;
                }
                None => break,
            }
        }

        report
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::window::{WindowDef, WindowParams};
    use arrow::array::{Int64Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
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
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        // Manually append two batches at 1s and 5s.
        {
            let win_lock = reg.get_window("win_a").unwrap();
            let mut win = win_lock.write().unwrap();
            win.append(make_batch(&schema, &[1_000_000_000], &[100]))
                .unwrap();
            win.append(make_batch(&schema, &[5_000_000_000], &[200]))
                .unwrap();
            assert_eq!(win.batch_count(), 2);
        }

        // now=20s, cutoff = 20s - 10s = 10s → both batches (max 1s, 5s) < 10s → evicted
        let evictor = Evictor::new(usize::MAX);
        let report = evictor.run_once(&reg, 20_000_000_000);

        assert_eq!(report.windows_scanned, 1);
        assert_eq!(report.batches_time_evicted, 2);
        assert_eq!(report.batches_memory_evicted, 0);

        let win_lock = reg.get_window("win_a").unwrap();
        let win = win_lock.read().unwrap();
        assert!(win.is_empty());
    }

    // -- 2. evictor_global_memory_cap -----------------------------------------

    #[test]
    fn evictor_global_memory_cap() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = probe.get_array_memory_size();

        let reg = WindowRegistry::build(vec![
            WindowDef {
                params: WindowParams {
                    name: "win_a".into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(3600),
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
                },
                streams: vec![],
                config: test_config(),
            },
        ])
        .unwrap();

        // win_a gets 2 batches, win_b gets 1 → total = 3 * one_batch_size
        {
            let lock = reg.get_window("win_a").unwrap();
            let mut w = lock.write().unwrap();
            w.append(make_batch(&schema, &[1_000_000_000], &[100]))
                .unwrap();
            w.append(make_batch(&schema, &[2_000_000_000], &[200]))
                .unwrap();
        }
        {
            let lock = reg.get_window("win_b").unwrap();
            let mut w = lock.write().unwrap();
            w.append(make_batch(&schema, &[3_000_000_000], &[300]))
                .unwrap();
        }

        // Cap at 2 batches. now=0 → no time eviction.
        let evictor = Evictor::new(one_batch_size * 2);
        let report = evictor.run_once(&reg, 0);

        assert_eq!(report.batches_time_evicted, 0);
        assert_eq!(report.batches_memory_evicted, 1);

        // Total memory should be under cap.
        let total: usize = ["win_a", "win_b"]
            .iter()
            .map(|n| {
                let lock = reg.get_window(n).unwrap();
                let w = lock.read().unwrap();
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
    ///   - run for 200 iterations (400s simulated time, 40 over-windows)
    ///   - after warmup (10 iterations), memory should oscillate around
    ///     ~5 batches' worth of data
    #[test]
    fn evictor_long_running_memory_stabilization() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = probe.get_array_memory_size();

        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "data".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(10),
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        let evictor = Evictor::new(usize::MAX);

        // Track memory samples across the run.
        let mut memory_samples: Vec<usize> = Vec::new();
        let mut batch_counts: Vec<usize> = Vec::new();

        let total_iterations = 200;
        let step_nanos = 2_000_000_000i64; // 2s per step
        let over_nanos = 10_000_000_000i64; // 10s over window
        let expected_batches_per_window = (over_nanos / step_nanos) as usize; // ~5

        for i in 0..total_iterations {
            let now = (i as i64 + 1) * step_nanos;

            // Inject one batch at current time.
            {
                let win_lock = reg.get_window("data").unwrap();
                let mut win = win_lock.write().unwrap();
                let value = i as i64 * 10;
                win.append(make_batch(&schema, &[now], &[value])).unwrap();
            }

            // Run eviction.
            evictor.run_once(&reg, now);

            // Sample metrics after every eviction.
            {
                let win_lock = reg.get_window("data").unwrap();
                let win = win_lock.read().unwrap();
                memory_samples.push(win.memory_usage());
                batch_counts.push(win.batch_count());
            }
        }

        // ---- Assertions ----

        // 1. After warmup (first 10 iterations), memory should never exceed
        //    ~6 batches (5 for over window + 1 grace for timing).
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
    #[test]
    fn evictor_long_running_with_snapshots() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = probe.get_array_memory_size();

        let reg = WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "data".into(),
                schema: schema.clone(),
                time_col_index: Some(0),
                over: Duration::from_secs(10),
            },
            streams: vec![],
            config: test_config(),
        }])
        .unwrap();

        let evictor = Evictor::new(usize::MAX);
        let step_nanos = 2_000_000_000i64;
        let over_nanos = 10_000_000_000i64;
        let expected_batches_per_window = (over_nanos / step_nanos) as usize;

        let total_iterations = 200;
        let mut memory_samples: Vec<usize> = Vec::new();

        // Hold a cursor to simulate read_since behavior.
        let mut cursor: u64 = 0;

        for i in 0..total_iterations {
            let now = (i as i64 + 1) * step_nanos;

            // Inject.
            {
                let win_lock = reg.get_window("data").unwrap();
                let mut win = win_lock.write().unwrap();
                win.append(make_batch(&schema, &[now], &[(i * 10) as i64]))
                    .unwrap();
            }

            // Simulate rule reading: take a snapshot, process, drop.
            {
                let win_lock = reg.get_window("data").unwrap();
                let win = win_lock.read().unwrap();
                let (_batches, new_cursor, _gap) = win.read_since(cursor);
                cursor = new_cursor;
                // _batches is dropped here — Arc refcount decremented.
            }

            // Evict.
            evictor.run_once(&reg, now);

            // Sample.
            {
                let win_lock = reg.get_window("data").unwrap();
                let win = win_lock.read().unwrap();
                memory_samples.push(win.memory_usage());
            }
        }

        // Memory after warmup should stabilize — snapshots don't leak.
        let warmup = 10;
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
    #[test]
    fn evictor_long_running_multi_window() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = probe.get_array_memory_size();

        let reg = WindowRegistry::build(vec![
            WindowDef {
                params: WindowParams {
                    name: "short".into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(5), // 5s over
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
                },
                streams: vec![],
                config: test_config(),
            },
        ])
        .unwrap();

        // Alert window needs memory eviction since it has no time col.
        // Global cap = 10 batches' worth (shared across all windows).
        let evictor = Evictor::new(one_batch_size * 10);
        let step_nanos = 1_000_000_000i64; // 1s per step

        let total_iterations = 300;
        let warmup = 30;

        let mut memory_short: Vec<usize> = Vec::new();
        let mut memory_medium: Vec<usize> = Vec::new();
        let mut memory_alert: Vec<usize> = Vec::new();

        for i in 0..total_iterations {
            let now = (i as i64 + 1) * step_nanos;

            // Inject into all three windows.
            for name in &["short", "medium", "alert"] {
                let win_lock = reg.get_window(name).unwrap();
                let mut win = win_lock.write().unwrap();
                win.append(make_batch(&schema, &[now], &[(i * 10) as i64]))
                    .unwrap();
            }

            evictor.run_once(&reg, now);

            for (name, samples) in [
                ("short", &mut memory_short),
                ("medium", &mut memory_medium),
                ("alert", &mut memory_alert),
            ] {
                let win_lock = reg.get_window(name).unwrap();
                let win = win_lock.read().unwrap();
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

    // -- 6. evictor_empty_registry --------------------------------------------

    #[test]
    fn evictor_empty_registry() {
        let reg = WindowRegistry::build(vec![]).unwrap();
        let evictor = Evictor::new(1024);
        let report = evictor.run_once(&reg, 0);

        assert_eq!(report.windows_scanned, 0);
        assert_eq!(report.batches_time_evicted, 0);
        assert_eq!(report.batches_memory_evicted, 0);
    }
}
