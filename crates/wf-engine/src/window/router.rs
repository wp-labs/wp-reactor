use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use wf_config::DistMode;

use crate::error::CoreResult;
use crate::match_engine::{Event, batch_to_events, batch_to_events_filtered};

use super::buffer::{AppendOutcome, content_bytes, events_bytes};
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

/// Parsed events for one local window, produced by the parallel parse stage.
#[derive(Clone)]
pub struct ParsedWindow {
    pub window_name: String,
    /// Pre-parsed events. `Some` when a rule consumes this window; `None` for
    /// windows no rule reads — the events are not materialized (the parse-side
    /// dominant cost) and the window's `parsed_events` stays uninitialized so a
    /// *future* subscriber still gets real events via the lazy `OnceLock`.
    pub events: Option<Arc<Vec<Arc<Event>>>>,
    /// Retained bytes of `events` (the `HashMap<SmolStr, Value>` representation),
    /// computed here so the ordered commit path skips the O(rows×cols) accounting.
    /// The window holds this footprint alongside the Arrow batch, so it must be
    /// part of the window's byte accounting or memory eviction fires far past the
    /// real water level (wp-labs/wp-reactor#20).
    pub events_bytes: usize,
}

/// Parsed events for every local window of a stream.
///
/// Produced by [`Router::route_parse`] (the parallelizable half of routing) and
/// consumed by [`Router::route_commit`] (the ordered, watermark-aware half).
pub struct ParsedRoute {
    pub windows: Vec<ParsedWindow>,
    pub skipped_non_local: usize,
    /// Arrow *content* byte size of the batch, computed once in the parallel
    /// parse stage. The per-window `events_bytes` is added in `route_commit`,
    /// so each window is charged its own retained footprint.
    pub content_bytes: usize,
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
    /// the parsed `Arc<Vec<Arc<Event>>>` to every rule subscribed to that window.
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
    ///
    /// Equivalent to [`Self::route_parse`] followed by [`Self::route_commit`];
    /// kept for callers that parse + commit in one place (file sources, tests,
    /// and the R2 rollback path).
    pub async fn route(&self, stream_name: &str, batch: RecordBatch) -> CoreResult<RouteReport> {
        let parsed = self.route_parse(stream_name, &batch);
        self.route_commit(batch, parsed).await
    }

    /// Parse a batch into per-window events without mutating any window.
    ///
    /// This is the parallelizable half of routing: it only reads window metadata
    /// (the materialize-fields whitelist) and parses the Arrow batch once per
    /// local window. The result is passed to [`Self::route_commit`], which does
    /// the ordered, watermark-aware append + broadcast.
    pub fn route_parse(&self, stream_name: &str, batch: &RecordBatch) -> ParsedRoute {
        let mut windows = Vec::new();
        let mut skipped_non_local = 0;

        for (window_name, mode) in self.registry.subscribers_of(stream_name) {
            if !matches!(mode, DistMode::Local) {
                skipped_non_local += 1;
                continue;
            }

            // No rule consumes this window (no fanout channel registered) →
            // skip event materialization + its accounting entirely. Event
            // HashMap materialization is allocation-heavy and dominated the
            // parse side on windows no rule reads. Events stay `None` so the
            // window's lazy OnceLock is left uninitialized — a later subscriber
            // (hot reload) still gets real events via `events_since()`.
            if !self.rule_fanout.has_subscribers(&window_name) {
                windows.push(ParsedWindow {
                    events_bytes: 0,
                    window_name,
                    events: None,
                });
                continue;
            }

            let win_lock = self
                .registry
                .get_window(&window_name)
                .expect("subscription references non-existent window");
            // Parse the batch to events *outside* the window lock. The read lock
            // is held only for an O(1) Arc clone of the materialize-fields set
            // (it never changes after construction); parsing itself happens with
            // no window lock held.
            let materialize = {
                let win = win_lock.read().expect("window lock poisoned");
                win.materialize_fields.clone()
            };
            let events = Arc::new(
                match materialize.as_deref() {
                    Some(fields) => batch_to_events_filtered(batch, fields),
                    None => batch_to_events(batch),
                }
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>(),
            );
            windows.push(ParsedWindow {
                events_bytes: events_bytes(&events),
                window_name,
                events: Some(events),
            });
        }

        ParsedRoute {
            // Arrow content is shared (Arc) across every window subscribed to
            // this stream, so it is charged once here; each window adds its own
            // parsed-event footprint in `route_commit`.
            content_bytes: if windows.is_empty() { 0 } else { content_bytes(batch) },
            windows,
            skipped_non_local,
        }
    }

    /// Append a pre-parsed batch to its windows (watermark-aware) and broadcast
    /// to rule channels, in the order given.
    ///
    /// This is the ordered half of routing; the parse workers run it via a
    /// single commit worker that re-sequences batches, so watermark advancement
    /// and rule delivery stay in source order even though parsing is parallel.
    pub async fn route_commit(
        &self,
        batch: RecordBatch,
        parsed: ParsedRoute,
    ) -> CoreResult<RouteReport> {
        let rows = batch.num_rows();
        let content_bytes = parsed.content_bytes;
        let mut report = RouteReport {
            delivered: 0,
            dropped_late: 0,
            skipped_non_local: parsed.skipped_non_local,
            per_window: Vec::new(),
        };

        for window in parsed.windows {
            let win_lock = self
                .registry
                .get_window(&window.window_name)
                .expect("subscription references non-existent window");
            // Materialized windows hand the pre-parsed events to the append;
            // fast-path windows (no rule subscriber) pass `None` so the batch's
            // `parsed_events` stays uninitialized (lazily parsed if a rule ever
            // subscribes later).
            let (outcome, batch_seq) = match &window.events {
                Some(events) => {
                    let mut win = win_lock.write().expect("window lock poisoned");
                    let outcome = win.append_with_watermark_parsed_sized(
                        batch.clone(),
                        Arc::clone(events),
                        content_bytes + window.events_bytes,
                    )?;
                    // The just-appended batch's seq (consumers ack seq+1).
                    (outcome, win.next_seq() - 1)
                }
                None => {
                    let mut win = win_lock.write().expect("window lock poisoned");
                    let outcome = win.append_with_watermark_sized(batch.clone(), content_bytes)?;
                    (outcome, win.next_seq() - 1)
                }
            };

            match outcome {
                AppendOutcome::Appended => {
                    report.delivered += 1;
                    report.per_window.push(WindowRouteOutcome {
                        window_name: window.window_name.clone(),
                        rows,
                        late: false,
                    });
                    // Push the shared parsed Arc to every rule subscribed to
                    // this window (R1 bridge: rules consume via channel instead
                    // of `window.read()`). Fast-path windows have no subscribers
                    // (the events are `None`), so broadcast is skipped.
                    if let Some(events) = &window.events {
                        self.rule_fanout
                            .broadcast(&window.window_name, events, batch_seq)
                            .await;
                    }
                    if let Some(notify) = self.registry.get_notifier(&window.window_name) {
                        notify.notify_waiters();
                    }
                }
                AppendOutcome::DroppedLate => {
                    report.dropped_late += 1;
                    report.per_window.push(WindowRouteOutcome {
                        window_name: window.window_name.clone(),
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
    pub async fn append_intermediate(
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
        let parsed = Arc::new(
            match materialize.as_deref() {
                Some(fields) => batch_to_events_filtered(&batch, fields),
                None => batch_to_events(&batch),
            }
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
        );
        let (outcome, batch_seq) = {
            let mut win = win_lock.write().expect("window lock poisoned");
            // Rule-emitted (intermediate) batches are small, so the O(rows×cols)
            // accounting can run inline; include the parsed-event footprint so
            // intermediate windows evict at the same water level as source ones.
            let byte_size = content_bytes(&batch) + events_bytes(&parsed);
            let outcome =
                win.append_with_watermark_parsed_sized(batch, Arc::clone(&parsed), byte_size)?;
            (outcome, win.next_seq() - 1)
        };
        if matches!(outcome, AppendOutcome::Appended) {
            self.rule_fanout.broadcast(window_name, &parsed, batch_seq).await;
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
    use crate::match_engine::{Event, batch_to_events, batch_to_events_filtered};
    use crate::window::buffer::{content_bytes, events_bytes};
    use crate::window::{WindowDef, WindowParams};
    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use std::collections::HashSet;
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

    #[tokio::test]
    async fn route_delivers_to_local_windows() {
        let reg = WindowRegistry::build(vec![make_def("win_a", vec!["events"], DistMode::Local)])
            .unwrap();
        let router = Router::new(reg);

        let schema = test_schema();
        let report = router
            .route("events", make_batch(&schema, &[10_000_000_000], &[42])).await
            .unwrap();

        assert_eq!(report.delivered, 1);
        assert_eq!(report.dropped_late, 0);
        assert_eq!(report.skipped_non_local, 0);

        let snap = router.registry().snapshot("win_a").unwrap();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].num_rows(), 1);
    }

    // -- 2. route_skips_non_local ---------------------------------------------

    #[tokio::test]
    async fn route_skips_non_local() {
        let reg = WindowRegistry::build(vec![make_def(
            "win_rep",
            vec!["data"],
            DistMode::Replicated,
        )])
        .unwrap();
        let router = Router::new(reg);

        let schema = test_schema();
        let report = router
            .route("data", make_batch(&schema, &[10_000_000_000], &[1])).await
            .unwrap();

        assert_eq!(report.delivered, 0);
        assert_eq!(report.dropped_late, 0);
        assert_eq!(report.skipped_non_local, 1);

        let snap = router.registry().snapshot("win_rep").unwrap();
        assert!(snap.is_empty());
    }

    // -- 3. route_drops_late_data ---------------------------------------------

    #[tokio::test]
    async fn route_drops_late_data() {
        let reg =
            WindowRegistry::build(vec![make_def("win_late", vec!["stream"], DistMode::Local)])
                .unwrap();
        let router = Router::new(reg);

        let schema = test_schema();

        // First batch at 20s → watermark = 15s, delivered.
        let r1 = router
            .route("stream", make_batch(&schema, &[20_000_000_000], &[1])).await
            .unwrap();
        assert_eq!(r1.delivered, 1);

        // Late batch at 5s → 5s < 15s → DroppedLate.
        let r2 = router
            .route("stream", make_batch(&schema, &[5_000_000_000], &[2])).await
            .unwrap();
        assert_eq!(r2.dropped_late, 1);
        assert_eq!(r2.delivered, 0);

        // Only the first batch remains.
        let snap = router.registry().snapshot("win_late").unwrap();
        assert_eq!(snap.len(), 1);
    }

    // -- 4. route_unknown_stream_noop -----------------------------------------

    #[tokio::test]
    async fn route_unknown_stream_noop() {
        let reg =
            WindowRegistry::build(vec![make_def("win_x", vec!["known"], DistMode::Local)]).unwrap();
        let router = Router::new(reg);

        let schema = test_schema();
        let report = router
            .route("unknown", make_batch(&schema, &[10_000_000_000], &[1])).await
            .unwrap();

        assert_eq!(report.delivered, 0);
        assert_eq!(report.dropped_late, 0);
        assert_eq!(report.skipped_non_local, 0);
    }

    // -- 5. append_intermediate_broadcasts_to_rule_channels -------------------

    #[tokio::test]
    async fn append_intermediate_broadcasts_to_rule_channels() {
        let reg = WindowRegistry::build(vec![make_def("win_pipe", vec![], DistMode::Local)])
            .unwrap();
        let router = Router::new(reg);

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        router.fanout().register("win_pipe", tx);

        let schema = test_schema();
        let outcome = router
            .append_intermediate("win_pipe", make_batch(&schema, &[10_000_000_000], &[42])).await
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::Appended));

        let push = rx
            .try_recv()
            .expect("intermediate append should broadcast parsed events");
        assert_eq!(&*push.window_name, "win_pipe");
        assert_eq!(push.events.len(), 1);
    }

    // -- 6. route_charges_events_bytes_per_window ----------------------------

    /// A batch with a JSON `object` field, 100 rows, ts + conn_info columns.
    fn object_batch() -> (SchemaRef, RecordBatch) {
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
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    (0..100)
                        .map(|i| Some(1_000_000_000i64 + i as i64))
                        .collect::<TimestampNanosecondArray>(),
                ),
                Arc::new((0..100).map(|_| Some(json)).collect::<StringArray>()),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    /// Each window must be charged its own parsed-event footprint: a window that
    /// materializes the object field reports `content_bytes + events_bytes`; one
    /// that skips it reports content plus only the materialized fields' events.
    #[tokio::test]
    async fn route_charges_events_bytes_per_window() {
        let (schema, batch) = object_batch();

        let all_events: Vec<Arc<Event>> =
            batch_to_events(&batch).into_iter().map(Arc::new).collect();
        let ts_events: Vec<Arc<Event>> = batch_to_events_filtered(
            &batch,
            &HashSet::from(["ts".to_string()]),
        )
        .into_iter()
        .map(Arc::new)
        .collect();
        let all_bytes = events_bytes(&all_events);
        let ts_bytes = events_bytes(&ts_events);
        assert!(
            all_bytes > ts_bytes,
            "materializing the object field must dominate the event footprint"
        );

        let reg = WindowRegistry::build(vec![
            WindowDef {
                params: WindowParams {
                    name: "win_all".into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(3600),
                    materialize_fields: None,
                },
                streams: vec!["events".into()],
                config: test_config(DistMode::Local),
            },
            WindowDef {
                params: WindowParams {
                    name: "win_ts".into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(3600),
                    materialize_fields: Some(Arc::new(HashSet::from(["ts".to_string()]))),
                },
                streams: vec!["events".into()],
                config: test_config(DistMode::Local),
            },
        ])
        .unwrap();
        let router = Router::new(reg);
        // Register rule subscribers so the fast path (skip materialization for
        // windows without rule consumers) does not fire — these windows are read.
        let (tx_all, _rx_all) = tokio::sync::mpsc::channel(8);
        let (tx_ts, _rx_ts) = tokio::sync::mpsc::channel(8);
        router.fanout().register("win_all", tx_all);
        router.fanout().register("win_ts", tx_ts);
        router.route("events", batch).await.unwrap();

        let mem_all = router
            .registry()
            .get_window("win_all")
            .unwrap()
            .read()
            .unwrap()
            .memory_usage();
        let mem_ts = router
            .registry()
            .get_window("win_ts")
            .unwrap()
            .read()
            .unwrap()
            .memory_usage();

        let content = content_bytes(
            &router
                .registry()
                .snapshot("win_all")
                .unwrap()
                .pop()
                .expect("batch retained"),
        );
        assert_eq!(
            mem_all,
            content + all_bytes,
            "all-fields window must be charged content + full event footprint"
        );
        assert_eq!(
            mem_ts,
            content + ts_bytes,
            "materialized-subset window must be charged content + subset footprint"
        );
        assert!(mem_all > mem_ts);
    }

    // -- 7. route_evicts_on_combined_footprint --------------------------------

    /// #20 fix end-to-end: the router charges `content_bytes + events_bytes`, so
    /// a window capped just above the content-only size drops the batch, while a
    /// cap that fits the combined footprint retains it. Before the fix the first
    /// window would have retained the batch (content ≤ cap) even though its real
    /// footprint ran past the cap.
    #[tokio::test]
    async fn route_evicts_on_combined_footprint() {
        let (schema, batch) = object_batch();

        let content = content_bytes(&batch);
        let all_events: Vec<Arc<Event>> =
            batch_to_events(&batch).into_iter().map(Arc::new).collect();
        let all_bytes = events_bytes(&all_events);
        assert!(content + all_bytes > content + 1);

        let config = |cap: usize| WindowConfig {
            name: "w".into(),
            mode: DistMode::Local,
            max_window_bytes: cap.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(0).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        };
        let reg = WindowRegistry::build(vec![
            WindowDef {
                params: WindowParams {
                    name: "under_real".into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(3600),
                    materialize_fields: None,
                },
                streams: vec!["events".into()],
                config: config(content + 1),
            },
            WindowDef {
                params: WindowParams {
                    name: "fits_real".into(),
                    schema: schema.clone(),
                    time_col_index: Some(0),
                    over: Duration::from_secs(3600),
                    materialize_fields: None,
                },
                streams: vec!["events".into()],
                config: config(content + all_bytes + 10),
            },
        ])
        .unwrap();
        let router = Router::new(reg);
        // Register rule subscribers so events are materialized (the fast path
        // would otherwise skip them and the windows would not be evicted).
        let (tx_under, _rx_under) = tokio::sync::mpsc::channel(8);
        let (tx_fits, _rx_fits) = tokio::sync::mpsc::channel(8);
        router.fanout().register("under_real", tx_under);
        router.fanout().register("fits_real", tx_fits);
        router.route("events", batch).await.unwrap();

        let under = router.registry().get_window("under_real").unwrap();
        assert_eq!(
            under.read().unwrap().total_rows(),
            0,
            "real footprint (content+events) > cap → batch dropped"
        );
        let fits = router.registry().get_window("fits_real").unwrap();
        assert_eq!(
            fits.read().unwrap().total_rows(),
            100,
            "combined footprint fits → batch retained"
        );
    }
}
