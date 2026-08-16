use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use arrow::record_batch::RecordBatch;
use wf_config::DistMode;

use crate::error::CoreResult;
use crate::match_engine::{Event, batch_to_events, batch_to_events_filtered};

use super::actor::{WindowMailbox, WindowMsg, acquire_window_budget};
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
    /// Per-window actor mailboxes (subscription model). Empty in sync mode —
    /// parse workers then fall back to the ordered commit worker (tests,
    /// embedded use). Populated once at boot before any source starts, after
    /// which parse workers dispatch directly to the window actors. Cold map:
    /// written only during bootstrap (and hot-added windows, which fall back
    /// to inline commit until restarted).
    mailboxes: RwLock<HashMap<String, WindowMailbox>>,
    /// Contiguous per-(source, window) sequence allocators for the actor path
    /// (see [`Self::next_window_seqs`]). Keyed `(source, window_name)`.
    /// Written only from the serialized source-side frame builder, so the
    /// std `Mutex` is uncontended in practice.
    window_seq: Mutex<HashMap<(Arc<str>, String), u64>>,
}

impl Router {
    pub fn new(registry: WindowRegistry) -> Self {
        Self {
            registry,
            rule_fanout: RuleFanout::new(),
            mailboxes: RwLock::new(HashMap::new()),
            window_seq: Mutex::new(HashMap::new()),
        }
    }

    /// Borrow the rule-channel fan-out table (for rule-task registration).
    pub fn fanout(&self) -> &Arc<RuleFanout> {
        &self.rule_fanout
    }

    /// Register the actor mailbox for `window_name` (bootstrap only).
    pub fn register_mailbox(&self, window_name: &str, mailbox: WindowMailbox) {
        self.mailboxes
            .write()
            .expect("mailbox lock poisoned")
            .insert(window_name.to_string(), mailbox);
    }

    /// Clone the actor mailbox for `window_name`, if one is registered.
    pub fn mailbox(&self, window_name: &str) -> Option<WindowMailbox> {
        self.mailboxes
            .read()
            .expect("mailbox lock poisoned")
            .get(window_name)
            .cloned()
    }

    /// Whether any window actor mailbox is registered (i.e. the runtime is in
    /// actor mode — parse workers dispatch directly instead of funnelling
    /// through the ordered commit worker).
    pub fn has_mailboxes(&self) -> bool {
        !self
            .mailboxes
            .read()
            .expect("mailbox lock poisoned")
            .is_empty()
    }

    /// Allocate the next contiguous seq per **(source, window)** for a frame
    /// of `stream_name` — one entry for every local window subscribed to the
    /// stream.
    ///
    /// Must be called where frames of a source are still in order (the
    /// source-side frame builder, before the parse pool's parallel workers
    /// can reorder them): the window actor's reorder cursor expects a
    /// gap-free 0,1,2,… per (source, window). A *global* per-source frame
    /// counter does **not** satisfy that — a window only receives the subset
    /// of frames carrying its stream, so the counter has holes from the
    /// window's perspective and every frame after the first hole parks in the
    /// actor's pending map forever, pinning the window byte budget until the
    /// pipeline deadlocks.
    ///
    /// No-op (empty vec) in sync mode: the ordered commit worker re-sequences
    /// on the global per-source seq instead.
    pub fn next_window_seqs(&self, source: &str, stream_name: &str) -> Vec<(String, u64)> {
        if !self.has_mailboxes() {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut seqs = self.window_seq.lock().expect("window_seq lock poisoned");
        for (window_name, mode) in self.registry.subscribers_of(stream_name) {
            if !matches!(mode, DistMode::Local) {
                continue;
            }
            let cursor = seqs
                .entry((Arc::from(source), window_name.clone()))
                .or_insert(0);
            out.push((window_name, *cursor));
            *cursor += 1;
        }
        out
    }

    /// Dispatch a parsed batch to its windows: actor mailboxes when
    /// registered (production), inline ordered commit otherwise.
    ///
    /// This is the parse worker's production path — it replaces the commit
    /// worker funnel. For every subscribed window the worker acquires the
    /// window's byte budget (backpressure: a slow window actor stalls only
    /// its own stream's producers) and sends the shared batch (zero copy).
    /// `window_seqs` carries the per-(source, window) contiguous seqs
    /// allocated in source order by [`Self::next_window_seqs`]; the actor
    /// restores that order (parallel parse workers may dispatch out of
    /// order).
    ///
    /// Windows without a mailbox (sync mode, hot-added windows) are committed
    /// inline via [`Self::commit_window`] — the caller-side metrics and error
    /// semantics of the old commit worker apply to that fallback only.
    pub async fn dispatch_parsed(
        &self,
        source: Arc<str>,
        seq: u64,
        window_seqs: Vec<(String, u64)>,
        batch: RecordBatch,
        parsed: ParsedRoute,
    ) {
        let content_bytes = parsed.content_bytes;
        for window in parsed.windows {
            match self.mailbox(&window.window_name) {
                Some(mailbox) => {
                    let byte_size = content_bytes + window.events_bytes;
                    // Per-(source, window) contiguous seq; fall back to the
                    // global frame seq only if allocation missed this window
                    // (theoretical hot-add race — reorder then parks, which
                    // is the pre-fix behaviour, not a correctness hazard for
                    // the boot-time-registered production path).
                    let wseq = window_seqs
                        .iter()
                        .find(|(name, _)| *name == window.window_name)
                        .map(|(_, s)| *s)
                        .unwrap_or(seq);
                    let permits =
                        acquire_window_budget(&mailbox.budget, mailbox.budget_bytes, byte_size)
                            .await;
                    let msg = WindowMsg::Append {
                        source: Arc::clone(&source),
                        seq: wseq,
                        batch: batch.clone(),
                        events: window.events,
                        byte_size,
                        permits,
                    };
                    let send_result = mailbox.tx.send(msg).await;
                    if send_result.is_err() {
                        // Actor gone (shutdown). Dropping the message releases
                        // its budget permits; remaining windows are skipped.
                        break;
                    }
                }
                None => {
                    let window_name = window.window_name.clone();
                    if let Err(e) = self.commit_window(&batch, window, content_bytes).await {
                        log::warn!(
                            "router: inline commit for window {:?} failed: {}",
                            window_name,
                            e
                        );
                    }
                }
            }
        }
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

            let win = self
                .registry
                .get_window(&window_name)
                .expect("subscription references non-existent window");
            // Parse the batch to events with no window synchronization at all:
            // `materialize_fields` is immutable after construction, and the
            // window's data plane is lock-free (nothing to wait on here).
            let materialize = win.materialize_fields.clone();
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
        let content_bytes = parsed.content_bytes;
        let mut report = RouteReport {
            delivered: 0,
            dropped_late: 0,
            skipped_non_local: parsed.skipped_non_local,
            per_window: Vec::new(),
        };

        for window in parsed.windows {
            let outcome = self.commit_window(&batch, window, content_bytes).await?;
            if outcome.late {
                report.dropped_late += 1;
            } else {
                report.delivered += 1;
            }
            report.per_window.push(outcome);
        }

        Ok(report)
    }

    /// Append one pre-parsed window batch (watermark-aware) and broadcast to
    /// rule subscribers. Shared by the ordered commit path
    /// ([`Self::route_commit`]) and the inline fallback of
    /// [`Self::dispatch_parsed`]. `content_bytes` is the Arrow content size
    /// computed once by `route_parse` (shared across windows of one stream).
    async fn commit_window(
        &self,
        batch: &RecordBatch,
        window: ParsedWindow,
        content_bytes: usize,
    ) -> CoreResult<WindowRouteOutcome> {
        let rows = batch.num_rows();
        let win = self
            .registry
            .get_window(&window.window_name)
            .expect("subscription references non-existent window");
        // Lock-free append. Materialized windows hand the pre-parsed events
        // to the append; fast-path windows (no rule subscriber) pass `None`
        // so the batch's `parsed_events` stays uninitialized (lazily parsed
        // if a rule ever subscribes later). The append returns the assigned
        // batch seq (consumers ack seq+1).
        let (outcome, batch_seq) = match &window.events {
            Some(events) => win.append_with_watermark_parsed_sized(
                batch.clone(),
                Arc::clone(events),
                content_bytes + window.events_bytes,
            )?,
            None => win.append_with_watermark_sized(batch.clone(), content_bytes)?,
        };

        match outcome {
            AppendOutcome::Appended => {
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
                Ok(WindowRouteOutcome {
                    window_name: window.window_name.clone(),
                    rows,
                    late: false,
                })
            }
            AppendOutcome::DroppedLate => Ok(WindowRouteOutcome {
                window_name: window.window_name.clone(),
                rows,
                late: true,
            }),
        }
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
        let win = self
            .registry
            .get_window(window_name)
            .expect("intermediate window must exist");
        let materialize = win.materialize_fields.clone();
        let parsed = Arc::new(
            match materialize.as_deref() {
                Some(fields) => batch_to_events_filtered(&batch, fields),
                None => batch_to_events(&batch),
            }
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
        );
        // Rule-emitted (intermediate) batches are small, so the O(rows×cols)
        // accounting can run inline; include the parsed-event footprint so
        // intermediate windows evict at the same water level as source ones.
        let byte_size = content_bytes(&batch) + events_bytes(&parsed);
        let (outcome, batch_seq) =
            win.append_with_watermark_parsed_sized(batch, Arc::clone(&parsed), byte_size)?;
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
            .memory_usage();
        let mem_ts = router
            .registry()
            .get_window("win_ts")
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
            under.total_rows(),
            0,
            "real footprint (content+events) > cap → batch dropped"
        );
        let fits = router.registry().get_window("fits_real").unwrap();
        assert_eq!(
            fits.total_rows(),
            100,
            "combined footprint fits → batch retained"
        );
    }

    // -- 8. next_window_seqs is contiguous per (source, window) ---------------

    /// Regression for the actor-path deadlock: a *global* per-source frame
    /// seq has holes from any single window's perspective (a window only
    /// receives its own stream's frames), so the actor's reorder cursor parks
    /// every frame after the first hole forever. `next_window_seqs` must
    /// allocate a gap-free 0,1,2,… per window even when streams interleave.
    #[test]
    fn next_window_seqs_contiguous_per_window_across_interleaved_streams() {
        use tokio::sync::{mpsc, Semaphore};

        use crate::window::WINDOW_CHANNEL_DEPTH;

        let reg = WindowRegistry::build(vec![
            make_def("win_a", vec!["stream_a"], DistMode::Local),
            make_def("win_p", vec!["stream_p"], DistMode::Local),
        ])
        .unwrap();
        let router = Router::new(reg);
        // Actor mode gate: without mailboxes the allocator returns nothing.
        assert!(router.next_window_seqs("src", "stream_a").is_empty());

        // Register one mailbox per window (actors themselves are irrelevant
        // for seq allocation).
        for name in ["win_a", "win_p"] {
            let (tx, _rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
            router.register_mailbox(
                name,
                WindowMailbox {
                    tx,
                    budget: Arc::new(Semaphore::new(1024)),
                    budget_bytes: 1024,
                },
            );
        }

        // Interleave frames like the nexmark generator: 3 of stream_a, then
        // 3 of stream_p, then stream_a again.
        let a1 = router.next_window_seqs("src", "stream_a");
        let a2 = router.next_window_seqs("src", "stream_a");
        let a3 = router.next_window_seqs("src", "stream_a");
        let p1 = router.next_window_seqs("src", "stream_p");
        let p2 = router.next_window_seqs("src", "stream_p");
        let p3 = router.next_window_seqs("src", "stream_p");
        let a4 = router.next_window_seqs("src", "stream_a");

        assert_eq!(a1, vec![("win_a".to_string(), 0)]);
        assert_eq!(a2, vec![("win_a".to_string(), 1)]);
        assert_eq!(a3, vec![("win_a".to_string(), 2)]);
        assert_eq!(p1, vec![(("win_p".to_string()), 0)]);
        assert_eq!(p2, vec![(("win_p".to_string()), 1)]);
        assert_eq!(p3, vec![(("win_p".to_string()), 2)]);
        // stream_p frames in between must not punch holes in win_a's seqs.
        assert_eq!(a4, vec![("win_a".to_string(), 3)]);

        // A second source gets independent cursors.
        let b1 = router.next_window_seqs("other", "stream_a");
        assert_eq!(b1, vec![("win_a".to_string(), 0)]);
    }
}
