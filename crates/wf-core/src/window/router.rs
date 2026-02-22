use anyhow::Result;
use arrow::record_batch::RecordBatch;
use wf_config::DistMode;

use super::buffer::AppendOutcome;
use super::registry::WindowRegistry;

// ---------------------------------------------------------------------------
// RouteReport
// ---------------------------------------------------------------------------

/// Summary of a single [`Router::route`] call.
pub struct RouteReport {
    pub delivered: usize,
    pub dropped_late: usize,
    pub skipped_non_local: usize,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Watermark-aware routing layer that wraps a [`WindowRegistry`].
///
/// For each subscriber of a stream name the router checks the distribution mode:
/// - `Local` → calls [`Window::append_with_watermark`].
/// - non-`Local` → skips (counted in `RouteReport::skipped_non_local`).
pub struct Router {
    registry: WindowRegistry,
}

impl Router {
    pub fn new(registry: WindowRegistry) -> Self {
        Self { registry }
    }

    /// Route a batch to all windows subscribed to `stream_name`.
    pub fn route(&self, stream_name: &str, batch: RecordBatch) -> Result<RouteReport> {
        let mut report = RouteReport {
            delivered: 0,
            dropped_late: 0,
            skipped_non_local: 0,
        };

        let subs = self.registry.subscribers_of(stream_name);

        for (window_name, mode) in subs {
            if !matches!(mode, DistMode::Local) {
                report.skipped_non_local += 1;
                continue;
            }

            let win_lock = self
                .registry
                .get_window(window_name)
                .expect("subscription references non-existent window");
            let outcome = {
                let mut win = win_lock.write().expect("window lock poisoned");
                win.append_with_watermark(batch.clone())?
            };

            match outcome {
                AppendOutcome::Appended => {
                    report.delivered += 1;
                    // Notify after releasing the write lock so waiters can
                    // immediately acquire a read lock.
                    if let Some(notify) = self.registry.get_notifier(window_name) {
                        notify.notify_waiters();
                    }
                }
                AppendOutcome::DroppedLate => report.dropped_late += 1,
            }
        }

        Ok(report)
    }

    /// Borrow the inner registry.
    pub fn registry(&self) -> &WindowRegistry {
        &self.registry
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
    use std::sync::Arc;
    use std::time::Duration;
    use wf_config::{EvictPolicy, LatePolicy, WindowConfig};

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

    fn test_config(mode: DistMode) -> WindowConfig {
        WindowConfig {
            name: "default".into(),
            mode,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(5).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: LatePolicy::Drop,
        }
    }

    fn make_def(name: &str, streams: Vec<&str>, mode: DistMode) -> WindowDef {
        WindowDef {
            params: WindowParams {
                name: name.into(),
                schema: test_schema(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
            },
            streams: streams.into_iter().map(String::from).collect(),
            config: test_config(mode),
        }
    }

    // -- 1. route_delivers_to_local_windows -----------------------------------

    #[test]
    fn route_delivers_to_local_windows() {
        let reg = WindowRegistry::build(vec![make_def("win_a", vec!["events"], DistMode::Local)])
            .unwrap();
        let router = Router::new(reg);

        let schema = test_schema();
        let report = router
            .route("events", make_batch(&schema, &[10_000_000_000], &[42]))
            .unwrap();

        assert_eq!(report.delivered, 1);
        assert_eq!(report.dropped_late, 0);
        assert_eq!(report.skipped_non_local, 0);

        let snap = router.registry().snapshot("win_a").unwrap();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].num_rows(), 1);
    }

    // -- 2. route_skips_non_local ---------------------------------------------

    #[test]
    fn route_skips_non_local() {
        let reg = WindowRegistry::build(vec![make_def(
            "win_rep",
            vec!["data"],
            DistMode::Replicated,
        )])
        .unwrap();
        let router = Router::new(reg);

        let schema = test_schema();
        let report = router
            .route("data", make_batch(&schema, &[10_000_000_000], &[1]))
            .unwrap();

        assert_eq!(report.delivered, 0);
        assert_eq!(report.dropped_late, 0);
        assert_eq!(report.skipped_non_local, 1);

        let snap = router.registry().snapshot("win_rep").unwrap();
        assert!(snap.is_empty());
    }

    // -- 3. route_drops_late_data ---------------------------------------------

    #[test]
    fn route_drops_late_data() {
        let reg =
            WindowRegistry::build(vec![make_def("win_late", vec!["stream"], DistMode::Local)])
                .unwrap();
        let router = Router::new(reg);

        let schema = test_schema();

        // First batch at 20s → watermark = 15s, delivered.
        let r1 = router
            .route("stream", make_batch(&schema, &[20_000_000_000], &[1]))
            .unwrap();
        assert_eq!(r1.delivered, 1);

        // Late batch at 5s → 5s < 15s → DroppedLate.
        let r2 = router
            .route("stream", make_batch(&schema, &[5_000_000_000], &[2]))
            .unwrap();
        assert_eq!(r2.dropped_late, 1);
        assert_eq!(r2.delivered, 0);

        // Only the first batch remains.
        let snap = router.registry().snapshot("win_late").unwrap();
        assert_eq!(snap.len(), 1);
    }

    // -- 4. route_unknown_stream_noop -----------------------------------------

    #[test]
    fn route_unknown_stream_noop() {
        let reg =
            WindowRegistry::build(vec![make_def("win_x", vec!["known"], DistMode::Local)]).unwrap();
        let router = Router::new(reg);

        let schema = test_schema();
        let report = router
            .route("unknown", make_batch(&schema, &[10_000_000_000], &[1]))
            .unwrap();

        assert_eq!(report.delivered, 0);
        assert_eq!(report.dropped_late, 0);
        assert_eq!(report.skipped_non_local, 0);
    }
}
