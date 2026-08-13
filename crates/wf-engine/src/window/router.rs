use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use wf_config::DistMode;

use crate::error::CoreResult;
use crate::match_engine::{batch_to_events, batch_to_events_filtered};

use super::buffer::AppendOutcome;
use super::fanout::RuleFanout;
use super::registry::WindowRegistry;

// ---------------------------------------------------------------------------
// RouteReport
// ---------------------------------------------------------------------------

/// Per-window route outcome.
#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct WindowRouteOutcome {
    pub window_name: String,
    pub rows: usize,
    pub late: bool,
}

/// Summary of a single [`Router::route`] call.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct RouteReport {
    pub delivered: usize,
    pub dropped_late: usize,
    pub skipped_non_local: usize,
    pub per_window: Vec<WindowRouteOutcome>,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Watermark-aware routing layer that wraps a [`WindowRegistry`].
///
/// For each subscriber of a stream name the router checks the distribution mode:
/// - `Local` → calls [`Window::append_with_watermark`].
/// - non-`Local` → skips (counted in `RouteReport::skipped_non_local`).
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct Router {
    registry: WindowRegistry,
    /// Rule-channel fan-out: after each successful append, the router broadcasts
    /// the parsed `Arc<Vec<Event>>` to every rule subscribed to that window.
    /// Kept alongside the registry (not inside it) so the MoJu model surface is
    /// unchanged.
    rule_fanout: Arc<RuleFanout>,
}

impl Router {
    pub fn new(registry: WindowRegistry) -> Self {
        Self {
            registry,
            rule_fanout: RuleFanout::new(),
        }
    }

    /// Borrow the rule-channel fan-out table (for rule-task registration).
    pub fn fanout(&self) -> &Arc<RuleFanout> {
        &self.rule_fanout
    }

    /// Route a batch to all windows subscribed to `stream_name`.
    pub fn route(&self, stream_name: &str, batch: RecordBatch) -> CoreResult<RouteReport> {
        let rows = batch.num_rows();
        let mut report = RouteReport {
            delivered: 0,
            dropped_late: 0,
            skipped_non_local: 0,
            per_window: Vec::new(),
        };

        let subs = self.registry.subscribers_of(stream_name);

        for (window_name, mode) in subs {
            if !matches!(mode, DistMode::Local) {
                report.skipped_non_local += 1;
                continue;
            }

            let win_lock = self
                .registry
                .get_window(&window_name)
                .expect("subscription references non-existent window");
            // Parse the batch to events *outside* the window lock. The read lock
            // is held only for an O(1) Arc clone of the materialize-fields set
            // (it never changes after construction); parsing itself happens with
            // no window lock held. Rule tasks reading this batch later hit the
            // already-set OnceLock with zero contention.
            let materialize = {
                let win = win_lock.read().expect("window lock poisoned");
                win.materialize_fields.clone()
            };
            let parsed = Arc::new(match materialize.as_deref() {
                Some(fields) => batch_to_events_filtered(&batch, fields),
                None => batch_to_events(&batch),
            });
            let outcome = {
                let mut win = win_lock.write().expect("window lock poisoned");
                win.append_with_watermark_parsed(batch.clone(), Arc::clone(&parsed))?
            };

            match outcome {
                AppendOutcome::Appended => {
                    report.delivered += 1;
                    report.per_window.push(WindowRouteOutcome {
                        window_name: window_name.to_string(),
                        rows,
                        late: false,
                    });
                    // Push the shared parsed Arc to every rule subscribed to
                    // this window (R1 bridge: rules consume via channel instead
                    // of `window.read()`).
                    self.rule_fanout.broadcast(&window_name, &parsed);
                    if let Some(notify) = self.registry.get_notifier(&window_name) {
                        notify.notify_waiters();
                    }
                }
                AppendOutcome::DroppedLate => {
                    report.dropped_late += 1;
                    report.per_window.push(WindowRouteOutcome {
                        window_name: window_name.to_string(),
                        rows,
                        late: true,
                    });
                }
            }
        }

        Ok(report)
    }

    /// Append a rule-emitted batch to an intermediate (pipeline) window and
    /// broadcast its parsed events to subscribing rules.
    ///
    /// This is the `|>` counterpart of [`Self::route`]: external sources reach
    /// windows via `route`, while intermediate windows are written by upstream
    /// rule tasks (`emit_window_record`). Keeping the parse + append + broadcast
    /// together here means downstream rules on the push path receive the events
    /// without a window read, and the pull path keeps working via the notifier.
    pub fn append_intermediate(
        &self,
        window_name: &str,
        batch: RecordBatch,
    ) -> CoreResult<AppendOutcome> {
        let win_lock = self
            .registry
            .get_window(window_name)
            .expect("intermediate window must exist");
        let materialize = {
            let win = win_lock.read().expect("window lock poisoned");
            win.materialize_fields.clone()
        };
        let parsed = Arc::new(match materialize.as_deref() {
            Some(fields) => batch_to_events_filtered(&batch, fields),
            None => batch_to_events(&batch),
        });
        let outcome = {
            let mut win = win_lock.write().expect("window lock poisoned");
            win.append_with_watermark_parsed(batch, Arc::clone(&parsed))?
        };
        if matches!(outcome, AppendOutcome::Appended) {
            self.rule_fanout.broadcast(window_name, &parsed);
        }
        Ok(outcome)
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
            table: None,
        }
    }

    fn make_def(name: &str, streams: Vec<&str>, mode: DistMode) -> WindowDef {
        WindowDef {
            params: WindowParams {
                name: name.into(),
                schema: test_schema(),
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
                materialize_fields: None,
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

    // -- 5. append_intermediate_broadcasts_to_rule_channels -------------------

    #[test]
    fn append_intermediate_broadcasts_to_rule_channels() {
        let reg = WindowRegistry::build(vec![make_def("win_pipe", vec![], DistMode::Local)])
            .unwrap();
        let router = Router::new(reg);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        router.fanout().register("win_pipe", tx);

        let schema = test_schema();
        let outcome = router
            .append_intermediate("win_pipe", make_batch(&schema, &[10_000_000_000], &[42]))
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::Appended));

        let push = rx
            .try_recv()
            .expect("intermediate append should broadcast parsed events");
        assert_eq!(&*push.window_name, "win_pipe");
        assert_eq!(push.events.len(), 1);
    }
}
