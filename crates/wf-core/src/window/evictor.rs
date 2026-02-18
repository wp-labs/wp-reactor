use super::registry::WindowRegistry;

// ---------------------------------------------------------------------------
// EvictReport
// ---------------------------------------------------------------------------

/// Summary of a single [`Evictor::run_once`] call.
pub struct EvictReport {
    pub windows_scanned: usize,
    pub batches_time_evicted: usize,
    pub batches_memory_evicted: usize,
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
        };

        // Phase 1: time eviction
        let names: Vec<String> = registry.window_names().map(String::from).collect();

        for name in &names {
            report.windows_scanned += 1;
            let win_lock = registry.get_window(name).unwrap();
            let mut win = win_lock.write().expect("window lock poisoned");
            let before = win.batch_count();
            win.evict_expired(now_nanos);
            report.batches_time_evicted += before - win.batch_count();
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
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
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

    // -- 3. evictor_empty_registry --------------------------------------------

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
