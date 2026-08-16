use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::new_null_array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use orion_error::conversion::{SourceRawErr, ToStructError};
use tokio::sync::mpsc;

use wf_engine::alert::{AlertColumnBuilder, OutputRecord};
use wf_engine::match_engine::{
    CepStateMachine, CloseReason, Event, RuleExecutor, StepResult, close_is_qualified,
};
use wf_engine::normalize_epoch_timestamp_float_nanos;
use wf_engine::window::{Router, RulePush};
use wf_lang::plan::ConvPlan;
use wf_lang::wfu_meta::{WFU_ID, WFU_INTERMEDIATE_META_FIELDS, WfuIntermediateMetaField};

use crate::alert_task::SinkFanout;
use crate::engine_task::conv_stage::{ConvCloseBatch, ConvShardSink};
use crate::error::{RuntimeReason, RuntimeResult};
use crate::metrics::RuntimeMetrics;

use super::TASK_SEQ;
use super::task_types::{RuleTaskConfig, WindowSource};
use super::window_lookup::RegistryLookup;

const PIPE_EVENT_TIME_FIELD: &str = "__wf_pipe_ts";
const DEBUG_DETAIL_LIMIT: usize = 20;

/// Pull-path pending rows: (alias, cursor, event arcs) per source window.
type PendingAliasRows = Vec<(String, u64, Vec<Arc<Vec<Arc<Event>>>>)>;
/// Staged pipe batch: (window name, events) or `None` when nothing staged.
type PendingEventBatch = Option<(Arc<str>, Arc<Vec<Arc<Event>>>)>;
/// Batch the allocation-heavy per-alert telemetry (detail map + e2e latency
/// histogram): only 1 in N emitted alerts updates those, the exact total is
/// always counted.
const EMIT_METRIC_SAMPLE_INTERVAL: u32 = 64;
/// Flush size for the batched alert sink delivery (amortizes per-alert fan-out).
const ALERT_BATCH_SIZE: usize = 256;

/// Columnar accumulation of pending alerts, grouped by yield target.
///
/// Records go straight from `OutputRecord` into per-field columns (no
/// per-row `DataRecord` materialization on the emit path); `flush_alerts`
/// seals each target's builder into one `AlertColumnBatch` for the sink
/// channel. See `AlertColumnBatch` for the memory rationale.
#[derive(Default)]
struct PendingAlertColumns {
    /// Yield targets are few (typically 1-2 per rule) — a linear scan beats
    /// hashing the target string on every append.
    by_target: Vec<(std::sync::Arc<str>, AlertColumnBuilder)>,
    count: usize,
}

/// Current wall-clock epoch nanos.
fn wall_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[derive(Debug, Default)]
struct RuleBatchDebugStats {
    input_events: usize,
    alias_passed: usize,
    alias_rejected: usize,
    accumulated: usize,
    advanced: usize,
    matched: usize,
    output_emitted: usize,
    output_none: usize,
    intermediate_emitted: usize,
    errors: usize,
    detail_logged: usize,
    detail_suppressed: usize,
}

impl RuleBatchDebugStats {
    fn can_log_detail(&self) -> bool {
        self.detail_logged < DEBUG_DETAIL_LIMIT
    }

    fn allow_detail(&mut self) -> bool {
        if self.detail_logged < DEBUG_DETAIL_LIMIT {
            self.detail_logged += 1;
            true
        } else {
            self.detail_suppressed += 1;
            false
        }
    }

    fn count_output(&mut self, record: &OutputRecord, intermediate_targets: &HashSet<String>) {
        if intermediate_targets.contains(&*record.yield_target) {
            self.intermediate_emitted += 1;
        } else {
            self.output_emitted += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// RuleTask -- runtime state for a single rule
// ---------------------------------------------------------------------------

/// Holds all mutable state for one rule's processing loop.
///
/// Each `RuleTask` owns its `CepStateMachine` exclusively (no `Arc<Mutex>`).
pub(super) struct RuleTask {
    pub(super) task_id: String,
    machine: Option<CepStateMachine>,
    each_alias: Option<String>,
    each_time_field: Option<String>,
    executor: RuleExecutor,
    conv_plan: Option<ConvPlan>,
    /// P2c: raw-close routing to the conv stage (sharded conv rules).
    conv_sink: Option<ConvShardSink>,
    pub(super) sources: Vec<WindowSource>,
    /// window_name -> Vec<alias>: pre-computed from stream_aliases + window sources.
    aliases: HashMap<String, Vec<String>>,
    /// window_name -> Vec<alias>: aux bind aliases first, then event aliases.
    ordered_aliases: HashMap<String, Vec<String>>,
    /// window_name -> cursor: tracks read position per window.
    pub(super) cursors: HashMap<String, u64>,
    /// Sink delivery fanout: each emitted alert is broadcast to the per-sink
    /// channels resolved by yield_target.
    sink_fanout: Arc<SinkFanout>,
    /// Shared router for WindowLookup (joins + has()).
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    intermediate_targets: HashSet<String>,
    /// Output/intermediate relay targets (pipe design). The emit path uses this
    /// to identify pipes and route emits through the pipe abstraction.
    pipe_registry: Arc<wf_engine::pipe::PipeRegistry>,
    /// End-of-stream counter (incremented on each EOS event). The task flushes
    /// instances on every EOS but keeps running so a daemon can accept
    /// multiple finite inputs.
    pub(super) eos_flush: tokio::sync::watch::Receiver<u64>,
    /// Wall clock when events were last processed. When input goes idle, this
    /// stays put so the periodic timeout scan can advance the effective watermark
    /// by the elapsed wall time — letting instances expire per their window TTL
    /// even without new events (window semantics, not just event-time).
    last_activity_wall: std::time::Instant,
    /// Push-mode input channel (R1). When `Some`, the rule consumes pushed
    /// `Arc<Vec<Arc<Event>>>` instead of pulling from the window read lock; when
    /// `None`, the task falls back to the legacy notify + pull loop. Consumed
    /// once by `run_rule_task`.
    pub(super) push_rx: Option<mpsc::Receiver<RulePush>>,
    /// Monotonic batch sequence for pushed batches (debug event refs only).
    pushed_seq: u64,
    /// Profiling accumulators (nanos) for locating the rule-task bottleneck.
    advance_nanos: u64,
    scan_nanos: u64,
    emit_nanos: u64,
    /// Finer emit split: execute_match / to_data_record / fanout handoff.
    /// The to_data_record time is also exported as the `alert.serialize_nanos`
    /// metric (summed across the run).
    exec_nanos: u64,
    serialize_nanos: std::sync::atomic::AtomicU64,
    fanout_nanos: std::sync::atomic::AtomicU64,
    /// Last wall-clock dump of the profiling accumulators (throttled log).
    last_profile_dump: std::time::Instant,
    /// Wall-clock nanos cached once per batch — avoids a `SystemTime::now()`
    /// syscall on every emitted alert.
    cached_wall_nanos: AtomicU64,
    /// Consumption-progress slots by window name. After fully processing a
    /// batch the task acks `seq + 1` (push path: `RulePush.seq`; pull path:
    /// the window batch seq). The evictor uses the minimum over all slots as
    /// the time-eviction floor, so sweeps can never drop unconsumed data.
    /// Released to `u64::MAX` on drop.
    progress: HashMap<String, std::sync::Arc<std::sync::atomic::AtomicU64>>,
    /// Countdown for sampling the allocation-heavy per-alert telemetry.
    emit_sample_remaining: AtomicU32,
    /// Serialize-timing sampler state (1-in-`EMIT_METRIC_SAMPLE_INTERVAL`),
    /// see `emit`.
    serialize_sample_remaining: AtomicU32,
    /// Last value reported to the `rule_instances` gauge. The gauge is the sum
    /// across a rule's shards, so each shard reports the delta since its last
    /// report (P2b).
    last_reported_instances: AtomicI64,
    /// Batched alert delivery: per-yield-target columnar builders flushed to
    /// the sink writers when the batch fills / at EOS. The record→columns
    /// append runs on this thread by design — see [`Self::emit`].
    /// `Mutex` so emit can stay `&self` while RuleTask stays `Sync`.
    pending_alerts: std::sync::Mutex<PendingAlertColumns>,
    /// Intermediate (pipe) relay staging (rule-side channelization): rows
    /// emitted to an intermediate target accumulate in typed column buffers
    /// and are flushed once per input batch — one N-row `RecordBatch`, one
    /// `batch_to_events`, one fanout broadcast — instead of a single-row
    /// Arrow batch + channel send per row. Same relay semantics as the old
    /// per-row `emit_window_record` (pure relay, no window store, seq
    /// `u64::MAX`). `Mutex` so emit can stay `&self`.
    pipe_state: std::sync::Mutex<PipeState>,
    /// On-each rules emitting to a plain sink target use the direct-write
    /// column path (plan C2): the executor appends straight into the
    /// columnar builder with no per-record `OutputRecord`. Intermediate
    /// pipe targets keep the record path for evaluation but stage the rows
    /// columnar-ly for batched relay ([`Self::flush_pipes`]). Constant for
    /// the task's lifetime — decided once here.
    each_direct: bool,
}

impl Drop for RuleTask {
    fn drop(&mut self) {
        // Release the consumption-progress slots so a task going away does
        // not pin its windows' time-eviction floor forever.
        for slot in self.progress.values() {
            wf_engine::window::WindowProgress::release(slot);
        }
    }
}

impl RuleTask {
    pub(super) fn new(
        config: RuleTaskConfig,
    ) -> (
        Self,
        tokio_util::sync::CancellationToken,
        std::time::Duration,
    ) {
        let RuleTaskConfig {
            machine,
            each_alias,
            each_time_field,
            executor,
            window_sources,
            sink_fanout,
            cancel,
            timeout_scan_interval,
            router,
            metrics,
            intermediate_targets,
            pipe_registry,
            eos_flush,
            push_rx,
            progress,
            conv_sink,
        } = config;
        let aliases: HashMap<String, Vec<String>> = window_sources
            .iter()
            .map(|src| (src.window_name.clone(), src.aliases.clone()))
            .collect();
        let ordered_aliases: HashMap<String, Vec<String>> = aliases
            .iter()
            .map(|(window_name, aliases)| {
                let ordered = aliases
                    .iter()
                    .filter(|alias| executor.is_aux_bind_alias(alias.as_str()))
                    .chain(
                        aliases
                            .iter()
                            .filter(|alias| !executor.is_aux_bind_alias(alias.as_str())),
                    )
                    .cloned()
                    .collect();
                (window_name.clone(), ordered)
            })
            .collect();

        // Initialize cursors to current position (skip historical data).
        let cursors: HashMap<String, u64> = window_sources
            .iter()
            .map(|src| {
                let seq = src.window.next_seq();
                (src.window_name.clone(), seq)
            })
            .collect();

        let seq = TASK_SEQ.fetch_add(1, Ordering::Relaxed);
        let rule_name = executor.plan().name.clone();
        let task_id = format!("{}#{}", rule_name, seq);
        let conv_plan = executor.plan().conv_plan.clone();
        // Direct-write on-each emit only when the target is a sink target:
        // intermediate pipes still consume full `OutputRecord` rows.
        let each_direct = executor.plan().each_plan.is_some()
            && !intermediate_targets.contains(executor.plan().yield_plan.target.as_str());

        let task = Self {
            task_id,
            machine,
            each_alias,
            each_time_field,
            executor,
            conv_plan,
            conv_sink,
            sources: window_sources,
            aliases,
            ordered_aliases,
            sink_fanout,
            cursors,
            router,
            metrics,
            intermediate_targets,
            pipe_registry,
            eos_flush,
            last_activity_wall: std::time::Instant::now(),
            push_rx,
            pushed_seq: 0,
            progress,
            advance_nanos: 0,
            scan_nanos: 0,
            emit_nanos: 0,
            exec_nanos: 0,
            serialize_nanos: std::sync::atomic::AtomicU64::new(0),
            fanout_nanos: std::sync::atomic::AtomicU64::new(0),
            last_profile_dump: std::time::Instant::now(),
            cached_wall_nanos: AtomicU64::new(wall_nanos()),
            emit_sample_remaining: AtomicU32::new(EMIT_METRIC_SAMPLE_INTERVAL),
            serialize_sample_remaining: AtomicU32::new(EMIT_METRIC_SAMPLE_INTERVAL),
            last_reported_instances: AtomicI64::new(0),
            pending_alerts: std::sync::Mutex::new(PendingAlertColumns::default()),
            pipe_state: std::sync::Mutex::new(PipeState::Uninit),
            each_direct,
        };
        (task, cancel, timeout_scan_interval)
    }

    fn rule_name(&self) -> &str {
        self.executor.plan().name.as_str()
    }

    fn instance_count(&self) -> usize {
        self.machine
            .as_ref()
            .map(|machine| machine.instance_count())
            .unwrap_or(0)
    }

    // -- Data processing ----------------------------------------------------

    /// Read new batches from all windows, convert to events, and advance
    /// the state machine.
    pub(super) async fn pull_and_advance(&mut self) {
        // Collect new events per window first (this phase only takes disjoint
        // field borrows), then process each batch — which needs `&mut self` and
        // would otherwise conflict with the `&self.sources` iteration.
        let mut pending: PendingAliasRows = Vec::new();
        for source in &self.sources {
            let cursor = self.cursors.get(&source.window_name).copied().unwrap_or(0);
            let (events_list, new_cursor, gap) = {
                // Shared parsed events: the window parses each batch once and
                // hands every rule the same Arc (wp-reactor#19). Lock-free
                // cursor read — no window lock involved.
                let win = &source.window;
                let result = win.events_since(cursor);
                wf_debug!(pipe,
                    task_id = %self.task_id,
                    window = %source.window_name,
                    cursor = cursor,
                    new_cursor = result.1,
                    batches = result.0.len(),
                    gap = result.2,
                    "events_since"
                );
                result
            };

            if gap {
                wf_warn!(pipe,
                    task_id = %self.task_id,
                    window = %source.window_name,
                    "cursor gap detected — some data was lost to eviction"
                );
                if let Some(metrics) = &self.metrics {
                    metrics.inc_rule_cursor_gap(
                        self.executor.plan().name.as_str(),
                        &source.window_name,
                    );
                }
            }
            self.cursors.insert(source.window_name.clone(), new_cursor);

            let first_batch_seq = new_cursor.saturating_sub(events_list.len() as u64);
            pending.push((source.window_name.clone(), first_batch_seq, events_list));
        }

        for (window_name, first_batch_seq, events_list) in pending {
            for (batch_index, events) in events_list.iter().enumerate() {
                let batch_seq = first_batch_seq + batch_index as u64;
                self.process_batch(&window_name, batch_seq, events).await;
                // Ack consumption so time eviction may reclaim this batch.
                if let Some(slot) = self.progress.get(&window_name) {
                    slot.store(batch_seq + 1, std::sync::atomic::Ordering::Release);
                }
            }
        }
        self.update_rule_instances_metric();
    }

    /// Process a single parsed batch (shared `Arc`) against the state machine.
    ///
    /// This is the per-batch body shared by the legacy pull path
    /// ([`Self::pull_and_advance`]) and the push path (channel recv). `batch_seq`
    /// is used only for debug event references.
    pub(super) async fn process_batch(
        &mut self,
        window_name: &str,
        batch_seq: u64,
        events: &Arc<Vec<Arc<Event>>>,
    ) {
        let Some(aliases) = self.aliases.get(window_name) else {
            return;
        };
        let Some(ordered_aliases) = self.ordered_aliases.get(window_name) else {
            return;
        };
        let mut stats = RuleBatchDebugStats {
            input_events: events.len(),
            ..RuleBatchDebugStats::default()
        };
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        let rule_name = debug_enabled.then(|| self.rule_name().to_string());
        let rule_name_for_log = rule_name.as_deref().unwrap_or("");
        let aliases_for_log = if debug_enabled {
            Some(aliases.join(","))
        } else {
            None
        };
        if debug_enabled {
            let instances_before = self.instance_count();
            wf_debug!(pipe,
                rule = %rule_name_for_log,
                stage = 0,
                window = %window_name,
                batch_seq = batch_seq,
                rows = events.len(),
                aliases = %aliases_for_log.as_deref().unwrap_or(""),
                instances_before = instances_before,
                "rule batch started"
            );
        }
        if let Some(metrics) = &self.metrics {
            metrics.add_rule_events(self.executor.plan().name.as_str(), events.len());
        }
        // Track the last wall-clock moment events were processed, so the
        // periodic timeout scan can advance the watermark across idle gaps.
        if !events.is_empty() {
            self.last_activity_wall = std::time::Instant::now();
            // Cache wall time for the emit path's e2e-latency sample.
            self.cached_wall_nanos
                .store(wall_nanos(), Ordering::Relaxed);
        }
        let lookup = RegistryLookup(&self.router);
        // on-each: events within a batch share the window schema, so the
        // sorted field order used for wfx_id hashing is computed once per
        // batch instead of collected + sorted per event.
        let each_field_order: Vec<&smol_str::SmolStr> =
            match (self.executor.plan().each_plan.is_some(), events.first()) {
                (true, Some(first)) => {
                    let mut names: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
                    names.sort_unstable();
                    names
                }
                _ => Vec::new(),
            };
        // Batch-level emit timestamp: all events in this batch share one
        // (nanos, formatted) pair — the executor caches the formatted string
        // and Arc-shares it across every record it builds this batch.
        let batch_emit_nanos = self.cached_wall_nanos.load(Ordering::Relaxed) as i64;
        // Plan C2 batching: when the per-event detail logs are off, collect
        // the each-direct rows and emit them in one vectorized pass after
        // the loop (debug runs keep the per-event path for exact detail).
        let mut each_direct_rows: Vec<(&wf_engine::match_engine::Event, i64)> = Vec::new();
        // P2③: for conv-sink shards, aggregate raw closes across the whole batch
        // and send ONE ConvCloseBatch (with the max event-time watermark) after
        // the loop — avoids a per-event bounded(32) channel send on the hot path.
        let mut conv_closes: Vec<wf_engine::match_engine::CloseOutput> = Vec::new();
        let mut conv_max_wm: i64 = 0;
        for (row_index, event) in events.iter().enumerate() {
            if let Some(machine) = &mut self.machine {
                let event_nanos = machine.event_time_nanos(event);
                let _scan_start = Instant::now();
                // P2c: shards of a conv rule emit raw closes to the conv stage
                // (aggregation window); inline conv is applied only on the
                // legacy single-machine path.
                let (routed, closes) = if self.conv_sink.is_some() {
                    let raw = machine.scan_expired_at(event_nanos);
                    // Barrier watermark must reflect the scan's watermark (the
                    // event time) — the machine's cached watermark only advances
                    // during `advance`, which runs after the scan.
                    conv_max_wm = conv_max_wm.max(event_nanos);
                    conv_closes.extend(raw.into_iter().filter(close_is_qualified));
                    (true, Vec::new())
                } else {
                    (
                        false,
                        machine.scan_expired_at_with_conv(event_nanos, self.conv_plan.as_ref()),
                    )
                };
                self.scan_nanos += _scan_start.elapsed().as_nanos() as u64;
                let _advance_start = Instant::now();
                let mut matched = Vec::new();
                for alias in ordered_aliases {
                    if !self
                        .executor
                        .event_matches_alias(alias, event, Some(&lookup))
                    {
                        if debug_enabled {
                            stats.alias_rejected += 1;
                        }
                        if debug_enabled && stats.allow_detail() {
                            let event_ref = event_debug_ref(event, batch_seq, row_index);
                            wf_debug!(pipe,
                                rule = %rule_name_for_log,
                                stage = 0,
                                window = %window_name,
                                alias = %alias,
                                event_ref = %event_ref,
                                reason = "bind_filter_false",
                                "rule event rejected"
                            );
                        }
                        continue;
                    }
                    if debug_enabled {
                        stats.alias_passed += 1;
                    }
                    let should_capture_progress = debug_enabled && stats.can_log_detail();
                    let (step_result, progress) = if should_capture_progress {
                        let outcome = machine.advance_at_with_progress(
                            alias,
                            event,
                            event_nanos,
                            Some(&lookup),
                        );
                        (outcome.result, outcome.progress)
                    } else {
                        (
                            machine.advance_at_with(alias, event, event_nanos, Some(&lookup)),
                            None,
                        )
                    };
                    match step_result {
                        StepResult::Accumulate => {
                            if debug_enabled {
                                stats.accumulated += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                let instances = machine.instance_count();
                                let event_ref = event_debug_ref(event, batch_seq, row_index);
                                if let Some(progress) = progress.as_ref() {
                                    wf_debug!(pipe,
                                        rule = %rule_name_for_log,
                                        stage = 0,
                                        window = %window_name,
                                        alias = %alias,
                                        event_ref = %event_ref,
                                        scope_key = %debug_scope_key(&progress.scope_key),
                                        machine_id = %progress.machine_id,
                                        step_index = progress.step_index,
                                        step_label = progress.step_label.as_deref().unwrap_or(""),
                                        branch_index = progress.branch_index,
                                        threshold_checked_branches = progress.threshold_checked_branches,
                                        measure_value = progress.measure_value,
                                        cmp = %progress.cmp,
                                        threshold = %progress.threshold,
                                        instances = instances,
                                        "rule event accumulated"
                                    );
                                } else {
                                    wf_debug!(pipe,
                                        rule = %rule_name_for_log,
                                        stage = 0,
                                        window = %window_name,
                                        alias = %alias,
                                        event_ref = %event_ref,
                                        instances = instances,
                                        "rule event accumulated"
                                    );
                                }
                            }
                        }
                        StepResult::Advance => {
                            if debug_enabled {
                                stats.advanced += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                let instances = machine.instance_count();
                                let event_ref = event_debug_ref(event, batch_seq, row_index);
                                if let Some(progress) = progress.as_ref() {
                                    wf_debug!(pipe,
                                        rule = %rule_name_for_log,
                                        stage = 0,
                                        window = %window_name,
                                        alias = %alias,
                                        event_ref = %event_ref,
                                        scope_key = %debug_scope_key(&progress.scope_key),
                                        machine_id = %progress.machine_id,
                                        step_index = progress.step_index,
                                        step_label = progress.step_label.as_deref().unwrap_or(""),
                                        branch_index = progress.branch_index,
                                        threshold_checked_branches = progress.threshold_checked_branches,
                                        measure_value = progress.measure_value,
                                        cmp = %progress.cmp,
                                        threshold = %progress.threshold,
                                        instances = instances,
                                        "rule step advanced"
                                    );
                                } else {
                                    wf_debug!(pipe,
                                        rule = %rule_name_for_log,
                                        stage = 0,
                                        window = %window_name,
                                        alias = %alias,
                                        event_ref = %event_ref,
                                        instances = instances,
                                        "rule step advanced"
                                    );
                                }
                            }
                        }
                        StepResult::Matched(ctx) => {
                            if debug_enabled {
                                stats.matched += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                let event_ref = event_debug_ref(event, batch_seq, row_index);
                                let step = ctx.step_data.last();
                                wf_debug!(pipe,
                                    rule = %rule_name_for_log,
                                    stage = 0,
                                    window = %window_name,
                                    alias = %alias,
                                    event_ref = %event_ref,
                                    scope_key = %debug_scope_key(&ctx.scope_key),
                                    machine_id = %ctx.machine_id,
                                    matched_steps = ctx.step_data.len(),
                                    step_label = step.and_then(|s| s.label.as_deref()).unwrap_or(""),
                                    measure_value = step.map(|s| s.measure_value).unwrap_or_default(),
                                    "rule matched"
                                );
                            }
                            matched.push(ctx);
                        }
                    }
                }
                self.advance_nanos += _advance_start.elapsed().as_nanos() as u64;
                let _emit_start = Instant::now();

                // When routed to the conv stage, the inline close processing is
                // skipped (the closes were already sent in the scan step).
                if !routed {
                    for close in &closes {
                        match self.executor.execute_close_with_joins(close, &lookup) {
                            Ok(Some(record)) => {
                                if debug_enabled {
                                    stats.count_output(&record, &self.intermediate_targets);
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_emitted(
                                        "execute_close",
                                        "close",
                                        output_kind(&record, &self.intermediate_targets),
                                        &record,
                                        close.scope_key.as_slice(),
                                    );
                                }
                                self.emit(record).await;
                            }
                            Ok(None) => {
                                if debug_enabled {
                                    stats.output_none += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_suppressed(
                                        rule_name_for_log,
                                        "execute_close",
                                        Some(close.scope_key.as_slice()),
                                    );
                                }
                            }
                            Err(e) => {
                                if debug_enabled {
                                    stats.errors += 1;
                                }
                                wf_warn!(
                                    pipe,
                                    rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                    stage = 0,
                                    phase = "execute_close",
                                    scope_key = %debug_scope_key(&close.scope_key),
                                    error = %e,
                                    "rule output failed"
                                )
                            }
                        }
                    }
                }

                for ctx in matched {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_rule_match(self.rule_name());
                    }
                    let _exec_start = Instant::now();
                    match self.executor.execute_match_with_joins(&ctx, &lookup) {
                        Ok(Some(record)) => {
                            self.exec_nanos += _exec_start.elapsed().as_nanos() as u64;
                            if debug_enabled {
                                stats.count_output(&record, &self.intermediate_targets);
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_emitted(
                                    "execute_match",
                                    "event",
                                    output_kind(&record, &self.intermediate_targets),
                                    &record,
                                    ctx.scope_key.as_slice(),
                                );
                            }
                            self.emit(record).await;
                        }
                        Ok(None) => {
                            if debug_enabled {
                                stats.output_none += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                log_output_suppressed(
                                    rule_name_for_log,
                                    "execute_match",
                                    Some(ctx.scope_key.as_slice()),
                                );
                            }
                        }
                        Err(e) => {
                            if debug_enabled {
                                stats.errors += 1;
                            }
                            wf_warn!(
                                pipe,
                                rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                stage = 0,
                                phase = "execute_match",
                                scope_key = %debug_scope_key(&ctx.scope_key),
                                error = %e,
                                "rule output failed"
                            )
                        }
                    }
                }
                self.emit_nanos += _emit_start.elapsed().as_nanos() as u64;
            } else if let Some(alias) = self
                .each_alias
                .as_ref()
                .filter(|alias| aliases.iter().any(|candidate| candidate == *alias))
            {
                if self
                    .executor
                    .event_matches_alias(alias, event, Some(&lookup))
                {
                    if debug_enabled {
                        stats.alias_passed += 1;
                    }
                    let event_nanos = event_time_nanos(event, self.each_time_field.as_deref());
                    if self.each_direct {
                        if !debug_enabled {
                            // Plan C2 batched: defer to the vectorized pass
                            // after the loop (same rows, same flush cadence).
                            each_direct_rows.push((event.as_ref(), event_nanos));
                            continue;
                        }
                        // Plan C2 per-event path (debug detail on): the
                        // executor appends straight into the columnar
                        // builder — no per-record OutputRecord.
                        match self
                            .emit_each_direct(
                                event,
                                event_nanos,
                                &lookup,
                                &each_field_order,
                                batch_emit_nanos,
                            )
                            .await
                        {
                            Ok(true) => {
                                if debug_enabled {
                                    stats.output_emitted += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    wf_debug!(pipe,
                                        rule = %rule_name_for_log,
                                        stage = 0,
                                        phase = "execute_each",
                                        target = %self.executor.static_yield_target(),
                                        output_kind = "alert",
                                        "rule output emitted (direct)"
                                    );
                                }
                            }
                            Ok(false) => {
                                if debug_enabled {
                                    stats.output_none += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_suppressed(rule_name_for_log, "execute_each", None);
                                }
                            }
                            Err(e) => {
                                if debug_enabled {
                                    stats.errors += 1;
                                }
                                wf_warn!(
                                    pipe,
                                    rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                    stage = 0,
                                    phase = "execute_each",
                                    error = %e,
                                    "rule output failed"
                                )
                            }
                        }
                    } else {
                        match self.executor.execute_each_with_joins(
                            event,
                            event_nanos,
                            &lookup,
                            &each_field_order,
                            batch_emit_nanos,
                        ) {
                            Ok(Some(record)) => {
                                if debug_enabled {
                                    stats.count_output(&record, &self.intermediate_targets);
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_emitted(
                                        "execute_each",
                                        "event",
                                        output_kind(&record, &self.intermediate_targets),
                                        &record,
                                        &[],
                                    );
                                }
                                self.emit(record).await;
                            }
                            Ok(None) => {
                                if debug_enabled {
                                    stats.output_none += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_suppressed(rule_name_for_log, "execute_each", None);
                                }
                            }
                            Err(e) => {
                                if debug_enabled {
                                    stats.errors += 1;
                                }
                                wf_warn!(
                                    pipe,
                                    rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                    stage = 0,
                                    phase = "execute_each",
                                    error = %e,
                                    "rule output failed"
                                )
                            }
                        }
                    }
                } else {
                    if debug_enabled {
                        stats.alias_rejected += 1;
                    }
                    if debug_enabled && stats.allow_detail() {
                        let event_ref = event_debug_ref(event, batch_seq, row_index);
                        wf_debug!(pipe,
                            rule = %rule_name_for_log,
                            stage = 0,
                            window = %window_name,
                            alias = %alias,
                            event_ref = %event_ref,
                            reason = "bind_filter_false",
                            "rule event rejected"
                        );
                    }
                }
            }
        }
        // P2③: one aggregated ConvCloseBatch per batch for conv-sink shards,
        // using the max event-time watermark as the barrier. (Replaces per-event
        // sends — the per-event path saturated the bounded(32) channel.)
        if self.conv_sink.is_some()
            && let Some(sink) = self.conv_sink.as_ref()
        {
            // P3-D: if the conv stage is gone (channel closed), the closes are
            // dropped — log it rather than fail silently.
            let sent = sink
                .tx
                .send(ConvCloseBatch {
                    closes: std::mem::take(&mut conv_closes),
                    watermark: conv_max_wm,
                    drained: false,
                    barrier_index: sink.barrier_index,
                })
                .await;
            if sent.is_err() {
                log::debug!("conv sink channel closed — conv batch dropped");
            }
        }
        // Vectorized on-each direct emit for the collected rows. Segment
        // size = ALERT_BATCH_SIZE keeps the flush cadence and the pending
        // memory bound of the per-event path.
        if !each_direct_rows.is_empty() {
            self.emit_each_direct_batch(
                &each_direct_rows,
                &lookup,
                &each_field_order,
                batch_emit_nanos,
            )
            .await;
        }
        if debug_enabled {
            let instances_after = self.instance_count();
            wf_debug!(pipe,
                rule = %rule_name_for_log,
                stage = 0,
                window = %window_name,
                batch_seq = batch_seq,
                input = stats.input_events,
                alias_passed = stats.alias_passed,
                alias_rejected = stats.alias_rejected,
                accumulated = stats.accumulated,
                advanced = stats.advanced,
                matched = stats.matched,
                outputs = stats.output_emitted,
                output_none = stats.output_none,
                intermediate_outputs = stats.intermediate_emitted,
                errors = stats.errors,
                instances_after = instances_after,
                detail_logged = stats.detail_logged,
                detail_suppressed = stats.detail_suppressed,
                "rule batch summary"
            );
            if stats.detail_suppressed > 0 {
                wf_debug!(pipe,
                    rule = %rule_name_for_log,
                    stage = 0,
                    window = %window_name,
                    batch_seq = batch_seq,
                    detail_logged = stats.detail_logged,
                    detail_suppressed = stats.detail_suppressed,
                    "rule event details suppressed"
                );
            }
        }
        self.dump_profiling();
        // Deliver any accumulated alert batch (bounds delivery latency to one
        // event batch and flushes test expectations without an explicit EOS).
        self.flush_alerts().await;
        // Same latency bound for staged intermediate (pipe) rows.
        self.flush_pipes().await;
    }

    /// Log the cumulative advance/scan/emit profiler accumulators once per
    /// second (throttled) so a run's phase split can be read from the log.
    fn dump_profiling(&mut self) {
        if self.last_profile_dump.elapsed() < Duration::from_secs(1) {
            return;
        }
        self.last_profile_dump = std::time::Instant::now();
        wf_info!(pipe,
            rule = %self.rule_name(),
            phase = "profile",
            scan_nanos = self.scan_nanos,
            advance_nanos = self.advance_nanos,
            exec_nanos = self.exec_nanos,
            serialize_nanos = self.serialize_nanos.load(Ordering::Relaxed),
            fanout_nanos = self.fanout_nanos.load(Ordering::Relaxed),
            emit_nanos = self.emit_nanos,
            "rule profiling"
        );
    }

    /// Update the periodic per-rule instance-count gauge.
    ///
    /// P2b: the gauge is the sum across a rule's shards, so each shard reports
    /// the delta since its last report. On drain (flush/EOS) the count drops to
    /// zero and the final delta reconciles the shard's contribution to zero.
    fn update_rule_instances_metric(&self) {
        if let Some(metrics) = &self.metrics {
            let rule_name = self.executor.plan().name.as_str();
            let cur = self
                .machine
                .as_ref()
                .map(|machine| machine.instance_count() as i64)
                .unwrap_or(0);
            let last = self.last_reported_instances.swap(cur, Ordering::Relaxed);
            let delta = cur - last;
            if delta != 0 {
                metrics.adjust_rule_instances(rule_name, delta);
            }
        }
    }

    /// Process a single pushed batch, advancing the per-task push sequence.
    pub(super) async fn process_push(&mut self, push: RulePush) {
        let seq = self.pushed_seq;
        self.pushed_seq += 1;
        let window_name = push.window_name.clone();
        let push_seq = push.seq;
        self.process_batch(window_name.as_ref(), seq, &push.events)
            .await;
        // Ack the window batch seq so time eviction may reclaim it (the
        // `seq` above is only a per-task debug counter).
        if let Some(slot) = self.progress.get(window_name.as_ref()) {
            // saturating: relay pushes carry seq = u64::MAX (no window batch
            // behind them) — MAX + 1 would overflow and wrap to 0.
            slot.store(
                push_seq.saturating_add(1),
                std::sync::atomic::Ordering::Release,
            );
        }
    }

    /// Consume and process all currently-buffered pushed batches.
    ///
    /// Used by the push loop to drain the channel before a flush (EOS/cancel).
    /// After the source reports EOS no further pushes arrive, so draining via
    /// `try_recv` until empty is complete.
    pub(super) async fn drain_push_channel(&mut self, rx: &mut mpsc::Receiver<RulePush>) {
        while let Ok(push) = rx.try_recv() {
            self.process_push(push).await;
        }
        self.update_rule_instances_metric();
    }

    // -- Timeout & shutdown -------------------------------------------------

    /// Scan for expired state machine instances and emit alerts.
    pub(super) async fn scan_timeouts(&mut self) {
        let Some(machine) = &self.machine else {
            return;
        };
        self.cached_wall_nanos
            .store(wall_nanos(), Ordering::Relaxed);
        // Advance the effective watermark by the wall-clock time elapsed since the
        // last event was processed. This lets instances expire per their window TTL
        // even when input is completely idle (window semantics, not just event-time).
        let effective_watermark = machine
            .watermark_nanos()
            .saturating_add(self.last_activity_wall.elapsed().as_nanos() as i64);
        let started = Instant::now();
        let lookup = RegistryLookup(&self.router);
        // P2c: shards of a conv rule route raw closes to the conv stage.
        let (rule_name, closes, routed) = {
            let machine = self.machine.as_mut().expect("checked above");
            let rule_name = machine.rule_name().to_string();
            if self.conv_sink.is_some() {
                let raw = machine.scan_expired_at(effective_watermark);
                // Barrier watermark = the effective (wall-clock advanced) scan
                // watermark, so an idle shard still advances its barrier and the
                // conv stage can seal buckets for the whole rule (without this,
                // an idle shard's stale barrier starves sealing forever).
                let watermark = effective_watermark;
                let qualifying: Vec<_> = raw.into_iter().filter(close_is_qualified).collect();
                if let Some(sink) = self.conv_sink.as_ref() {
                    // P3-D: log when the conv stage is gone (closes dropped).
                    if sink
                        .tx
                        .send(ConvCloseBatch {
                            closes: qualifying,
                            watermark,
                            drained: false,
                            barrier_index: sink.barrier_index,
                        })
                        .await
                        .is_err()
                    {
                        log::debug!("conv sink channel closed — scan batch dropped");
                    }
                }
                (rule_name, Vec::new(), true)
            } else {
                (
                    rule_name,
                    machine.scan_expired_at_with_conv(effective_watermark, self.conv_plan.as_ref()),
                    false,
                )
            }
        };
        let mut stats = RuleBatchDebugStats::default();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        // When routed to the conv stage, skip inline close processing.
        if !routed {
            for close in &closes {
                match self.executor.execute_close_with_joins(close, &lookup) {
                    Ok(Some(record)) => {
                        if debug_enabled {
                            stats.count_output(&record, &self.intermediate_targets);
                        }
                        if debug_enabled && stats.allow_detail() {
                            log_output_emitted(
                                "execute_close",
                                "close",
                                output_kind(&record, &self.intermediate_targets),
                                &record,
                                close.scope_key.as_slice(),
                            );
                        }
                        self.emit(record).await;
                    }
                    Ok(None) => {
                        if debug_enabled {
                            stats.output_none += 1;
                        }
                        if debug_enabled && stats.allow_detail() {
                            log_output_suppressed(
                                &rule_name,
                                "execute_close",
                                Some(close.scope_key.as_slice()),
                            );
                        }
                    }
                    Err(e) => {
                        if debug_enabled {
                            stats.errors += 1;
                        }
                        wf_warn!(
                            pipe,
                            task_id = %self.task_id,
                            rule = %rule_name,
                            stage = 0,
                            phase = "execute_close",
                            scope_key = %debug_scope_key(&close.scope_key),
                            error = %e,
                            "rule output failed"
                        )
                    }
                }
            }
        }
        if debug_enabled {
            let instances_after = self.instance_count();
            wf_debug!(
                pipe,
                task_id = %self.task_id,
                rule = %rule_name,
                stage = 0,
                closes = closes.len(),
                outputs = stats.output_emitted,
                output_none = stats.output_none,
                intermediate_outputs = stats.intermediate_emitted,
                errors = stats.errors,
                instances_after = instances_after,
                detail_logged = stats.detail_logged,
                detail_suppressed = stats.detail_suppressed,
                "rule timeout scan summary"
            );
            if stats.detail_suppressed > 0 {
                wf_debug!(
                    pipe,
                    task_id = %self.task_id,
                    rule = %rule_name,
                    stage = 0,
                    detail_logged = stats.detail_logged,
                    detail_suppressed = stats.detail_suppressed,
                    "rule event details suppressed"
                );
            }
        }
        // Re-anchor the O(1) per-instance base-cost memory estimate to the exact
        // sum of live instance state (accumulated field_values / distinct_set
        // growth is otherwise invisible to the running estimate).
        if let Some(machine) = self.machine.as_mut() {
            machine.recalibrate_memory();
        }
        if let Some(metrics) = &self.metrics {
            metrics.observe_rule_scan_timeout(&rule_name, started.elapsed());
            self.update_rule_instances_metric();
        }
        // Timeout closes may have staged intermediate rows — deliver them.
        self.flush_pipes().await;
    }

    /// Close all active instances (shutdown flush) and emit alerts.
    pub(super) async fn flush(&mut self) {
        let Some(_) = &self.machine else {
            return;
        };
        self.cached_wall_nanos
            .store(wall_nanos(), Ordering::Relaxed);
        let started = Instant::now();
        let lookup = RegistryLookup(&self.router);
        // P2c: on flush a conv-rule shard routes ALL remaining raw closes to the
        // conv stage and publishes a drained barrier (i64::MAX via the batch).
        let (rule_name, closes, routed) = {
            let machine = self.machine.as_mut().expect("checked above");
            let rule_name = machine.rule_name().to_string();
            if self.conv_sink.is_some() {
                let raw = machine.close_all(CloseReason::Flush);
                let watermark = machine.watermark_nanos();
                let qualifying: Vec<_> = raw.into_iter().filter(close_is_qualified).collect();
                if let Some(sink) = self.conv_sink.as_ref() {
                    // P3-D: log when the conv stage is gone (drained closes dropped).
                    if sink
                        .tx
                        .send(ConvCloseBatch {
                            closes: qualifying,
                            watermark,
                            drained: true,
                            barrier_index: sink.barrier_index,
                        })
                        .await
                        .is_err()
                    {
                        log::debug!("conv sink channel closed — drained flush dropped");
                    }
                }
                (rule_name, Vec::new(), true)
            } else {
                (
                    rule_name,
                    machine.close_all_with_conv(CloseReason::Flush, self.conv_plan.as_ref()),
                    false,
                )
            }
        };
        let mut stats = RuleBatchDebugStats::default();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        // When routed to the conv stage, skip inline close processing.
        if !routed {
            for close in &closes {
                match self.executor.execute_close_with_joins(close, &lookup) {
                    Ok(Some(record)) => {
                        if debug_enabled {
                            stats.count_output(&record, &self.intermediate_targets);
                        }
                        if debug_enabled && stats.allow_detail() {
                            log_output_emitted(
                                "execute_close",
                                "close",
                                output_kind(&record, &self.intermediate_targets),
                                &record,
                                close.scope_key.as_slice(),
                            );
                        }
                        self.emit(record).await;
                    }
                    Ok(None) => {
                        if debug_enabled {
                            stats.output_none += 1;
                        }
                        if debug_enabled && stats.allow_detail() {
                            log_output_suppressed(
                                &rule_name,
                                "execute_close",
                                Some(close.scope_key.as_slice()),
                            );
                        }
                    }
                    Err(e) => {
                        if debug_enabled {
                            stats.errors += 1;
                        }
                        wf_warn!(
                            pipe,
                            task_id = %self.task_id,
                            rule = %rule_name,
                            stage = 0,
                            phase = "execute_close",
                            scope_key = %debug_scope_key(&close.scope_key),
                            error = %e,
                            "rule output failed"
                        )
                    }
                }
            }
        }
        if debug_enabled {
            let instances_after = self.instance_count();
            wf_debug!(
                pipe,
                task_id = %self.task_id,
                rule = %rule_name,
                stage = 0,
                closes = closes.len(),
                outputs = stats.output_emitted,
                output_none = stats.output_none,
                intermediate_outputs = stats.intermediate_emitted,
                errors = stats.errors,
                instances_after = instances_after,
                detail_logged = stats.detail_logged,
                detail_suppressed = stats.detail_suppressed,
                "rule flush summary"
            );
            if stats.detail_suppressed > 0 {
                wf_debug!(
                    pipe,
                    task_id = %self.task_id,
                    rule = %rule_name,
                    stage = 0,
                    detail_logged = stats.detail_logged,
                    detail_suppressed = stats.detail_suppressed,
                    "rule event details suppressed"
                );
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics.observe_rule_flush(&rule_name, started.elapsed());
            self.update_rule_instances_metric();
        }
        // Drain the batched alert delivery after close emissions.
        self.flush_alerts().await;
        // Drain staged intermediate rows after close emissions (each rules
        // early-return above — their rows are covered by the per-batch
        // flush in `process_batch`).
        self.flush_pipes().await;
    }

    // -- Alert emission -----------------------------------------------------

    async fn emit(&self, record: OutputRecord) {
        if self.intermediate_targets.contains(&*record.yield_target) {
            self.stage_pipe_record(record);
            return;
        }
        if let Some(metrics) = &self.metrics {
            // Exact total is cheap (one relaxed atomic); the allocation-heavy
            // detail map + e2e histogram are sampled 1-in-N (batch).
            metrics.inc_alert_emitted_total(&record.rule_name);
            let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
            let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
            if sample == 0 {
                self.emit_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                metrics.inc_alert_emitted_detail(
                    &record.rule_name,
                    &record.machine_id,
                    &record.scope_key,
                );
                let e2e_nanos = now_nanos.saturating_sub(record.event_time_nanos.max(0) as u64);
                metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
            } else {
                self.emit_sample_remaining
                    .store(sample - 1, Ordering::Relaxed);
            }
        }
        // Append straight into the per-target columnar batch, sealed and
        // flushed to the sink writers when it fills (amortizing the
        // per-alert fan-out mechanics, matching the wp-motor batch model).
        // The conversion stays on this thread on purpose: records allocated
        // here and freed on a sink thread drive mimalloc into its
        // abandoned-page reclaim path — measured ~2x rule-throughput loss.
        //
        // Serialize timing is sampled 1-in-`EMIT_METRIC_SAMPLE_INTERVAL` and
        // scaled back up (same sampling pattern as the e2e metrics): two
        // clock_gettime calls per record measured ~2.5% of on-CPU samples,
        // and the per-record timing only feeds diagnostics, not semantics.
        // (The metric covers the record→columns append, the successor of the
        // old to_data_record conversion.)
        let time_this = {
            let rem = self
                .serialize_sample_remaining
                .fetch_sub(1, Ordering::Relaxed);
            if rem == 1 {
                self.serialize_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                true
            } else {
                false
            }
        };
        let _ser_start = time_this.then(Instant::now);
        let (append_result, should_flush) = {
            let mut pending = self.pending_alerts.lock().unwrap();
            // Linear target lookup (targets are few); avoids hashing the
            // target string for every appended record.
            let slot = pending
                .by_target
                .iter_mut()
                .find(|(target, _)| *target == record.yield_target);
            let builder = match slot {
                Some((_, builder)) => builder,
                None => {
                    pending.by_target.push((
                        std::sync::Arc::clone(&record.yield_target),
                        AlertColumnBuilder::new(std::sync::Arc::clone(&record.yield_target)),
                    ));
                    let last = pending.by_target.len() - 1;
                    &mut pending.by_target[last].1
                }
            };
            let result = builder.append_record(&record);
            if result.is_ok() {
                pending.count += 1;
            }
            (result, pending.count >= ALERT_BATCH_SIZE)
        };
        if let Err(e) = append_result {
            if let Some(metrics) = &self.metrics {
                metrics.inc_alert_serialize_failed();
            }
            log::warn!("alert export error: {e}");
            return;
        }
        if let Some(start) = _ser_start {
            let elapsed = start.elapsed().as_nanos() as u64;
            let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64;
            self.serialize_nanos.fetch_add(scaled, Ordering::Relaxed);
            if let Some(metrics) = &self.metrics {
                metrics.add_alert_serialize_nanos(scaled);
            }
        }
        if should_flush {
            self.flush_alerts().await;
        }
    }

    /// Direct-write on-each emit (plan C2): the executor evaluates the event
    /// and appends the row straight into the per-target columnar builder —
    /// no per-record `OutputRecord` materialization. Mirrors [`Self::emit`]'s
    /// telemetry (exact totals, 1-in-N sampled detail/e2e, sampled serialize
    /// timing) and batch-flush trigger.
    ///
    /// One diagnostic difference from the record path: the sampled detail's
    /// machine id is extracted from the pre-join event (joins that rebind
    /// the machine-id field would show a different label). Only affects the
    /// metric label, not semantics.
    async fn emit_each_direct(
        &self,
        event: &Event,
        event_nanos: i64,
        lookup: &RegistryLookup<'_>,
        field_order: &[&smol_str::SmolStr],
        batch_emit_nanos: i64,
    ) -> wf_engine::error::CoreResult<bool> {
        // Serialize timing is sampled 1-in-N and scaled back up (same
        // pattern as `emit`; covers the eval + column append).
        let time_this = {
            let rem = self
                .serialize_sample_remaining
                .fetch_sub(1, Ordering::Relaxed);
            if rem == 1 {
                self.serialize_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                true
            } else {
                false
            }
        };
        let _ser_start = time_this.then(Instant::now);
        let (result, should_flush) = {
            let mut pending = self.pending_alerts.lock().unwrap();
            // Linear target lookup via the plan-constant Arc (targets are
            // few); first append creates the builder.
            let target = self.executor.static_yield_target();
            let slot = pending
                .by_target
                .iter_mut()
                .find(|(existing, _)| **existing == **target);
            let builder = match slot {
                Some((_, builder)) => builder,
                None => {
                    pending.by_target.push((
                        std::sync::Arc::clone(target),
                        AlertColumnBuilder::new(std::sync::Arc::clone(target)),
                    ));
                    let last = pending.by_target.len() - 1;
                    &mut pending.by_target[last].1
                }
            };
            let result = self.executor.execute_each_direct(
                event,
                event_nanos,
                lookup,
                field_order,
                batch_emit_nanos,
                builder,
            );
            if let Ok(true) = &result {
                pending.count += 1;
            }
            (result, pending.count >= ALERT_BATCH_SIZE)
        };
        if let Ok(true) = &result {
            if let Some(metrics) = &self.metrics {
                // Exact total is cheap; the allocation-heavy detail map +
                // e2e histogram are sampled 1-in-N.
                metrics.inc_alert_emitted_total(self.rule_name());
                let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
                let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                if sample == 0 {
                    self.emit_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    metrics.inc_alert_emitted_detail(
                        self.rule_name(),
                        &RuleExecutor::machine_id_of(event),
                        self.rule_name(),
                    );
                    let e2e_nanos = now_nanos.saturating_sub(event_nanos.max(0) as u64);
                    metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                } else {
                    self.emit_sample_remaining
                        .store(sample - 1, Ordering::Relaxed);
                }
            }
        } else if let Err(e) = &result {
            if let Some(metrics) = &self.metrics {
                metrics.inc_alert_serialize_failed();
            }
            log::warn!("alert export error: {e}");
        }
        if let Some(start) = _ser_start {
            let elapsed = start.elapsed().as_nanos() as u64;
            let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64;
            self.serialize_nanos.fetch_add(scaled, Ordering::Relaxed);
            if let Some(metrics) = &self.metrics {
                metrics.add_alert_serialize_nanos(scaled);
            }
        }
        if should_flush {
            self.flush_alerts().await;
        }
        result
    }

    /// Batched direct-write on-each emit (build_each_direct vectorization):
    /// runs [`RuleExecutor::execute_each_direct_batch`] over the events the
    /// main loop collected for this rule, in segments of `ALERT_BATCH_SIZE`
    /// events so the flush cadence and the pending-alerts memory bound stay
    /// identical to the per-event path.
    ///
    /// Telemetry mirrors [`Self::emit_each_direct`]: exact `emitted_total`
    /// per appended row (via the appended-index list, outside the builder
    /// lock), 1-in-N sampled detail/e2e per appended row, and serialize
    /// timing sampled per segment and scaled by the per-call average (a
    /// segment covers many "calls", so the scaled estimate stays comparable
    /// to the per-event path's accounting).
    async fn emit_each_direct_batch(
        &self,
        rows: &[(&wf_engine::match_engine::Event, i64)],
        lookup: &RegistryLookup<'_>,
        field_order: &[&smol_str::SmolStr],
        batch_emit_nanos: i64,
    ) {
        let mut appended_idx: Vec<usize> = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let end = (start + ALERT_BATCH_SIZE).min(rows.len());
            let segment = &rows[start..end];
            let calls = segment.len();
            let time_this = {
                let rem = self
                    .serialize_sample_remaining
                    .fetch_sub(1, Ordering::Relaxed);
                if rem == 1 {
                    self.serialize_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            };
            let _ser_start = time_this.then(Instant::now);
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                // Linear target lookup via the plan-constant Arc — same as
                // the per-event path.
                let target = self.executor.static_yield_target();
                let slot = pending
                    .by_target
                    .iter_mut()
                    .find(|(existing, _)| **existing == **target);
                let builder = match slot {
                    Some((_, builder)) => builder,
                    None => {
                        pending.by_target.push((
                            std::sync::Arc::clone(target),
                            AlertColumnBuilder::new(std::sync::Arc::clone(target)),
                        ));
                        let last = pending.by_target.len() - 1;
                        &mut pending.by_target[last].1
                    }
                };
                let outcome = self.executor.execute_each_direct_batch(
                    segment,
                    lookup,
                    field_order,
                    batch_emit_nanos,
                    builder,
                    &mut appended_idx,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            // Per-row telemetry outside the builder lock (exact totals,
            // 1-in-N sampled detail/e2e — same accounting as the per-event
            // path).
            if let Some(metrics) = &self.metrics {
                for &idx in appended_idx.iter() {
                    metrics.inc_alert_emitted_total(self.rule_name());
                    let (event, event_nanos) = segment[idx];
                    let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
                    let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                    if sample == 0 {
                        self.emit_sample_remaining
                            .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                        metrics.inc_alert_emitted_detail(
                            self.rule_name(),
                            &RuleExecutor::machine_id_of(event),
                            self.rule_name(),
                        );
                        let e2e_nanos = now_nanos.saturating_sub(event_nanos.max(0) as u64);
                        metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                    } else {
                        self.emit_sample_remaining
                            .store(sample - 1, Ordering::Relaxed);
                    }
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_serialize_failed();
                }
            }
            if let Some(ser_start) = _ser_start {
                let elapsed = ser_start.elapsed().as_nanos() as u64;
                // A segment covers `calls` per-event "calls"; scale the
                // sampled segment time back to the per-call average × the
                // sample interval so the accumulator stays comparable with
                // the per-event path's accounting.
                let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64 / calls.max(1) as u64;
                self.serialize_nanos.fetch_add(scaled, Ordering::Relaxed);
                if let Some(metrics) = &self.metrics {
                    metrics.add_alert_serialize_nanos(scaled);
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
            start = end;
        }
    }

    /// Flush the accumulated columnar alert batches to the sink writers,
    /// grouped by yield_target. Each sink receives one `AlertBatch` (a single
    /// channel send) of columnar records, amortizing the per-alert resolve /
    /// try_send / blocking that dominated the q1 pass-through emit path.
    async fn flush_alerts(&self) {
        let pending = {
            let mut guarded = self.pending_alerts.lock().unwrap();
            if guarded.count == 0 {
                return;
            }
            std::mem::take(&mut *guarded)
        };
        let _fan_start = Instant::now();
        for (target, mut builder) in pending.by_target {
            let records_len = builder.len();
            let sink_groups = self.sink_fanout.resolve(&target);
            if sink_groups.is_empty() {
                if let Some(metrics) = &self.metrics {
                    metrics.add_alert_no_sink_records(records_len as u64);
                }
                self.sink_fanout.warn_if_no_sink(&target);
                continue;
            }
            let batch = crate::alert_task::AlertBatch::Columns(Arc::new(builder.finish()));
            for (sink_ptr, channels) in sink_groups.iter() {
                // Round-robin across this sink's parallel writers.
                let idx = self.sink_fanout.next_index(*sink_ptr, channels.len());
                let tx = &channels[idx];
                match tx.try_send(batch.clone()) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(batch)) => {
                        if let Some(metrics) = &self.metrics {
                            metrics.inc_alert_channel_full();
                        }
                        // Fall back to blocking send (backpressure).
                        if let Err(e) = tx.send(batch).await {
                            if let Some(metrics) = &self.metrics {
                                metrics.inc_alert_channel_send_failed();
                            }
                            wf_warn!(pipe, error = %e, "alert channel closed");
                        }
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        // Channel is closed — drop the batch
                        if let Some(metrics) = &self.metrics {
                            metrics.inc_alert_channel_send_failed();
                        }
                        wf_warn!(pipe, rule = %target, "alert channel closed, dropping alert batch");
                    }
                }
            }
        }
        self.fanout_nanos
            .fetch_add(_fan_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    /// Stage an intermediate-target row into the columnar pipe buffer
    /// (rule-side channelization). [`Self::flush_pipes`] turns the staged
    /// rows into one batch + one fanout broadcast at the end of the input
    /// batch — the relay semantics of the old per-row `emit_window_record`
    /// (pure relay, no window store, seq `u64::MAX`) with the per-row Arrow
    /// assembly and channel sends amortized away.
    fn stage_pipe_record(&self, record: OutputRecord) {
        let mut guard = self.pipe_state.lock().unwrap();
        match &mut *guard {
            PipeState::Dead => {}
            PipeState::Staging(stager) => {
                if let Err(e) = stager.push_record(&record) {
                    wf_warn!(
                        pipe,
                        task_id = %self.task_id,
                        rule = %record.rule_name,
                        target = %record.yield_target,
                        output_kind = "intermediate",
                        error = %e,
                        "stage internal pipeline row failed"
                    );
                }
            }
            PipeState::Uninit => {
                // Resolve the pipe shape once, lazily (pipe registry schema
                // first, window fallback — same resolution order and failure
                // semantics as the old per-row path).
                let target = Arc::clone(&record.yield_target);
                match resolve_pipe_shape(&self.pipe_registry, &self.router, &target) {
                    Some((schema, time_col_index)) => {
                        let mut stager = PipeBatchStager::new(target, schema, time_col_index);
                        if let Err(e) = stager.push_record(&record) {
                            wf_warn!(
                                pipe,
                                task_id = %self.task_id,
                                rule = %record.rule_name,
                                output_kind = "intermediate",
                                error = %e,
                                "stage internal pipeline row failed"
                            );
                        }
                        *guard = PipeState::Staging(stager);
                    }
                    None => {
                        wf_warn!(
                            pipe,
                            task_id = %self.task_id,
                            rule = %record.rule_name,
                            target = %target,
                            output_kind = "intermediate",
                            reason = "missing_internal_window",
                            "missing internal pipeline window"
                        );
                        *guard = PipeState::Dead;
                    }
                }
            }
        }
    }

    /// Flush staged intermediate rows: build one N-row `RecordBatch`, parse
    /// it to events once, and hand it to the pipe's downstream-rule
    /// subscribers with a single broadcast. Called at the end of every
    /// input batch (and on timeout/flush emissions), so delivery latency is
    /// bounded exactly like the batched sink-alert delivery.
    async fn flush_pipes(&self) {
        let built = {
            let mut guard = self.pipe_state.lock().unwrap();
            match &mut *guard {
                PipeState::Staging(stager) => match stager.take_events() {
                    Ok(built) => built,
                    Err(e) => {
                        wf_warn!(
                            pipe,
                            task_id = %self.task_id,
                            output_kind = "intermediate",
                            error = %e,
                            "build internal pipeline batch failed, dropping staged rows"
                        );
                        None
                    }
                },
                _ => None,
            }
        };
        if let Some((target, events)) = built {
            let fan_start = Instant::now();
            self.router
                .fanout()
                .broadcast(&target, &events, u64::MAX)
                .await;
            self.fanout_nanos
                .fetch_add(fan_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }
}

fn event_debug_ref(
    event: &wf_engine::match_engine::Event,
    batch_seq: u64,
    row_index: usize,
) -> String {
    event
        .fields
        .get("event_id")
        .or_else(|| event.fields.get(WFU_ID))
        .or_else(|| event.fields.get("id"))
        .map(value_debug_string)
        .unwrap_or_else(|| format!("batch:{batch_seq}/row:{row_index}"))
}

fn value_debug_string(value: &wf_engine::match_engine::Value) -> String {
    match value {
        wf_engine::match_engine::Value::Number(value) => value.to_string(),
        wf_engine::match_engine::Value::Str(value) => value.to_string(),
        wf_engine::match_engine::Value::Bool(value) => value.to_string(),
        wf_engine::match_engine::Value::Array(_) | wf_engine::match_engine::Value::Object(_) => {
            "<structured>".to_string()
        }
    }
}

fn debug_scope_key(scope_key: &[wf_engine::match_engine::Value]) -> String {
    scope_key
        .iter()
        .map(value_debug_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn log_output_emitted(
    phase: &'static str,
    origin: &'static str,
    output_kind: &'static str,
    record: &OutputRecord,
    scope_key: &[wf_engine::match_engine::Value],
) {
    wf_debug!(
        pipe,
        rule = %record.rule_name,
        stage = 0,
        phase = phase,
        origin = origin,
        target = %record.yield_target,
        scope_key = %debug_scope_key(scope_key),
        output_kind = output_kind,
        "rule output emitted"
    );
}

fn output_kind(record: &OutputRecord, intermediate_targets: &HashSet<String>) -> &'static str {
    if intermediate_targets.contains(&*record.yield_target) {
        "intermediate"
    } else {
        "alert"
    }
}

fn log_output_suppressed(
    rule_name: &str,
    phase: &'static str,
    scope_key: Option<&[wf_engine::match_engine::Value]>,
) {
    let scope_present = scope_key.is_some();
    wf_debug!(
        pipe,
        rule = %rule_name,
        stage = 0,
        phase = phase,
        scope_key = %scope_key.map(debug_scope_key).unwrap_or_else(|| "<none>".to_string()),
        scope_present = scope_present,
        reason = "executor_returned_none",
        "rule output suppressed"
    );
}

/// Columnar staging state for the intermediate-target emit path
/// (rule-side channelization).
enum PipeState {
    /// No intermediate row emitted yet; the pipe shape resolves lazily on
    /// first use (the pipe registry may still be populating at boot).
    Uninit,
    /// Shape resolved; rows accumulate in the column buffers until the next
    /// [`RuleTask::flush_pipes`].
    Staging(PipeBatchStager),
    /// Target window/pipe missing (warned once); rows are dropped — the
    /// same terminal behavior as the old per-row fallback.
    Dead,
}

/// Per-column staging buffer. The variant is chosen once from the pipe
/// schema; every row appends exactly one value (or null).
enum PipeCol {
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    Utf8(Vec<Option<String>>),
    Timestamp(Vec<Option<i64>>),
    /// Column types outside the supported coercion matrix stage as null —
    /// same fallback arm as `value_to_single_row_array`.
    Null {
        data_type: DataType,
        len: usize,
    },
}

/// Resolved shape of an intermediate pipe target: the relay schema and its
/// time column (pipe registry first, window fallback).
fn resolve_pipe_shape(
    pipe_registry: &Arc<wf_engine::pipe::PipeRegistry>,
    router: &Arc<Router>,
    target: &Arc<str>,
) -> Option<(arrow::datatypes::SchemaRef, Option<usize>)> {
    match pipe_registry.get(target) {
        // Pipe registered with a real schema (normal boot) → use it.
        Some(pipe) if !pipe.schema.fields().is_empty() => Some((pipe.schema, pipe.time_col_index)),
        // Pipe absent or built without schemas (e.g. the reload path builds
        // the registry with no window schemas) → fall back to the window,
        // which is always populated with the correct schema + time column.
        _ => router
            .registry()
            .get_window(target)
            .map(|win| (win.schema().clone(), win.time_col_index())),
    }
}

impl PipeBatchStager {
    fn new(
        target: Arc<str>,
        schema: arrow::datatypes::SchemaRef,
        time_col_index: Option<usize>,
    ) -> Self {
        let cols = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Int64 => PipeCol::Int64(Vec::new()),
                DataType::Float64 => PipeCol::Float64(Vec::new()),
                DataType::Boolean => PipeCol::Bool(Vec::new()),
                DataType::Utf8 => PipeCol::Utf8(Vec::new()),
                DataType::Timestamp(_, _) => PipeCol::Timestamp(Vec::new()),
                other => PipeCol::Null {
                    data_type: other.clone(),
                    len: 0,
                },
            })
            .collect();
        Self {
            target,
            schema,
            time_col_index,
            cols,
            rows: 0,
        }
    }

    /// Stage one emitted row. The coercion matrix mirrors
    /// `value_to_single_row_array` exactly (including the event-time
    /// fallbacks for the pipe event-time field and the schema's time
    /// column), so a flushed batch is byte-identical to concatenating the
    /// per-row batches the old path produced.
    fn push_record(&mut self, record: &OutputRecord) -> RuntimeResult<()> {
        let event_time_nanos = record.event_time_nanos;
        let fields = record_window_fields(record);
        for (idx, field) in self.schema.fields().iter().enumerate() {
            let value = fields
                .iter()
                .find(|(name, _)| **name == *field.name())
                .map(|(_, value)| value);
            if field.name() == PIPE_EVENT_TIME_FIELD {
                match &mut self.cols[idx] {
                    PipeCol::Timestamp(v) => v.push(Some(event_time_nanos)),
                    PipeCol::Null { len, .. } => *len += 1,
                    _ => unreachable!("event-time field must be Timestamp"),
                }
                continue;
            }
            let col = &mut self.cols[idx];
            match col {
                PipeCol::Int64(v) => v.push(match value {
                    Some(wf_engine::match_engine::Value::Number(n)) => Some(*n as i64),
                    _ => None,
                }),
                PipeCol::Float64(v) => v.push(match value {
                    Some(wf_engine::match_engine::Value::Number(n)) => Some(*n),
                    _ => None,
                }),
                PipeCol::Bool(v) => v.push(match value {
                    Some(wf_engine::match_engine::Value::Bool(b)) => Some(*b),
                    _ => None,
                }),
                PipeCol::Utf8(v) => {
                    v.push(match value {
                        Some(wf_engine::match_engine::Value::Str(s)) => Some(s.to_string()),
                        Some(wf_engine::match_engine::Value::Number(n)) => Some(n.to_string()),
                        Some(wf_engine::match_engine::Value::Bool(b)) => Some(b.to_string()),
                        Some(
                            value @ (wf_engine::match_engine::Value::Array(_)
                            | wf_engine::match_engine::Value::Object(_)),
                        ) => Some(value_to_json_string(value)?),
                        _ => None,
                    });
                }
                PipeCol::Timestamp(v) => v.push(match value {
                    Some(wf_engine::match_engine::Value::Number(n)) => {
                        normalize_epoch_timestamp_float_nanos(*n)
                    }
                    // The schema's time column falls back to the row's event
                    // time when the yield did not provide one.
                    None if self.time_col_index == Some(idx) => Some(event_time_nanos),
                    _ => None,
                }),
                PipeCol::Null { len, .. } => *len += 1,
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// Build the staged rows into one batch and parse it to events,
    /// resetting the buffers. Returns `None` when nothing is staged.
    fn take_events(&mut self) -> RuntimeResult<PendingEventBatch> {
        if self.rows == 0 {
            return Ok(None);
        }
        let arrays: Vec<arrow::array::ArrayRef> = self
            .cols
            .iter_mut()
            .map(|col| match col {
                PipeCol::Int64(v) => Ok(std::sync::Arc::new(arrow::array::Int64Array::from(
                    std::mem::take(v),
                )) as arrow::array::ArrayRef),
                PipeCol::Float64(v) => Ok(std::sync::Arc::new(arrow::array::Float64Array::from(
                    std::mem::take(v),
                )) as arrow::array::ArrayRef),
                PipeCol::Bool(v) => Ok(std::sync::Arc::new(arrow::array::BooleanArray::from(
                    std::mem::take(v),
                )) as arrow::array::ArrayRef),
                PipeCol::Utf8(v) => Ok(std::sync::Arc::new(arrow::array::StringArray::from(
                    std::mem::take(v),
                )) as arrow::array::ArrayRef),
                PipeCol::Timestamp(v) => Ok(std::sync::Arc::new(
                    arrow::array::TimestampNanosecondArray::from(std::mem::take(v)),
                ) as arrow::array::ArrayRef),
                PipeCol::Null { data_type, len } => {
                    let array = new_null_array(data_type, *len);
                    *len = 0;
                    Ok(array)
                }
            })
            .collect::<RuntimeResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new(std::sync::Arc::clone(&self.schema), arrays)
            .source_raw_err(RuntimeReason::Bootstrap, "build internal pipeline batch")?;
        self.rows = 0;
        let events: Arc<Vec<Arc<Event>>> = Arc::new(
            wf_engine::match_engine::batch_to_events(&batch)
                .into_iter()
                .map(Arc::new)
                .collect(),
        );
        Ok(Some((Arc::clone(&self.target), events)))
    }
}

struct PipeBatchStager {
    target: Arc<str>,
    schema: arrow::datatypes::SchemaRef,
    time_col_index: Option<usize>,
    cols: Vec<PipeCol>,
    rows: usize,
}

fn record_window_fields(
    record: &OutputRecord,
) -> Vec<(std::sync::Arc<str>, wf_engine::match_engine::Value)> {
    let mut fields = record.yield_fields.clone();
    let existing: HashSet<&str> = fields.iter().map(|(name, _)| &**name).collect();
    let missing_meta: Vec<WfuIntermediateMetaField> = WFU_INTERMEDIATE_META_FIELDS
        .iter()
        .copied()
        .filter(|field| !existing.contains(field.name()))
        .collect();
    for field in missing_meta {
        fields.push((
            std::sync::Arc::from(field.name()),
            record_wfu_intermediate_meta_value(record, field),
        ));
    }
    fields
}

fn record_wfu_intermediate_meta_value(
    record: &OutputRecord,
    field: WfuIntermediateMetaField,
) -> wf_engine::match_engine::Value {
    use wf_engine::match_engine::Value;
    use wf_lang::wfu_meta::WfuIntermediateMetaField;

    match field {
        WfuIntermediateMetaField::RuleName => Value::Str(record.rule_name.clone().into()),
        WfuIntermediateMetaField::Score => Value::Number(record.score),
        WfuIntermediateMetaField::EntityType => Value::Str(record.entity_type.clone().into()),
        WfuIntermediateMetaField::EntityId => Value::Str(record.entity_id.clone().into()),
    }
}

fn event_time_nanos(event: &wf_engine::match_engine::Event, time_field: Option<&str>) -> i64 {
    time_field
        .and_then(|field| event.fields.get(field))
        .and_then(|value| match value {
            wf_engine::match_engine::Value::Number(n) => Some(*n as i64),
            _ => None,
        })
        .unwrap_or(0)
}

fn value_to_json_string(value: &wf_engine::match_engine::Value) -> RuntimeResult<String> {
    serde_json::to_string(&value_to_json(value)?).source_raw_err(
        RuntimeReason::Bootstrap,
        "serialize structured pipeline value",
    )
}

fn value_to_json(value: &wf_engine::match_engine::Value) -> RuntimeResult<serde_json::Value> {
    match value {
        wf_engine::match_engine::Value::Number(n) if n.is_finite() => {
            Ok(serde_json::Value::from(*n))
        }
        wf_engine::match_engine::Value::Number(_) => RuntimeReason::Bootstrap
            .to_err()
            .with_detail("structured numeric value must be finite")
            .err(),
        wf_engine::match_engine::Value::Str(s) => Ok(serde_json::Value::from(s.as_str())),
        wf_engine::match_engine::Value::Bool(b) => Ok(serde_json::Value::from(*b)),
        wf_engine::match_engine::Value::Array(items) => Ok(serde_json::Value::Array(
            items
                .iter()
                .map(value_to_json)
                .collect::<RuntimeResult<Vec<_>>>()?,
        )),
        wf_engine::match_engine::Value::Object(items) => {
            let mut object = serde_json::Map::new();
            let mut keys: Vec<_> = items.keys().collect();
            keys.sort();
            for key in keys {
                if let Some(value) = items.get(key) {
                    object.insert(key.to_string(), value_to_json(value)?);
                }
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}

#[cfg(test)]
mod debug_stats_tests {
    use super::*;
    use wf_engine::alert::AlertOrigin;

    fn output_record(target: &str) -> OutputRecord {
        OutputRecord {
            wfx_id: "id".to_string(),
            rule_name: "rule".into(),
            score: 1.0,
            entity_type: "ip".into(),
            entity_id: "10.0.0.1".to_string(),
            origin: AlertOrigin::Event,
            fired_at: "2026-01-01T00:00:00Z".to_string(),
            emit_time: "2026-01-01T00:00:00Z".into(),
            matched_rows: Vec::new(),
            summary: "".into(),
            yield_target: target.into(),
            yield_fields: Vec::new(),
            yield_field_types: Vec::new().into(),
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: "".into(),
        }
    }

    #[test]
    fn detail_budget_caps_at_first_twenty_entries() {
        let mut stats = RuleBatchDebugStats::default();

        for _ in 0..DEBUG_DETAIL_LIMIT {
            assert!(stats.allow_detail());
        }

        assert!(!stats.allow_detail());
        assert!(!stats.allow_detail());
        assert_eq!(stats.detail_logged, DEBUG_DETAIL_LIMIT);
        assert_eq!(stats.detail_suppressed, 2);
    }

    #[test]
    fn exhausted_detail_budget_still_counts_suppressed_entries() {
        let mut stats = RuleBatchDebugStats::default();

        for _ in 0..DEBUG_DETAIL_LIMIT {
            assert!(stats.can_log_detail());
            assert!(stats.allow_detail());
        }

        assert!(!stats.can_log_detail());
        assert!(!stats.allow_detail());
        assert_eq!(stats.detail_logged, DEBUG_DETAIL_LIMIT);
        assert_eq!(stats.detail_suppressed, 1);
    }

    #[test]
    fn output_counts_split_alert_and_intermediate_targets() {
        let mut stats = RuleBatchDebugStats::default();
        let intermediate_targets = HashSet::from(["internal_events".to_string()]);

        stats.count_output(&output_record("alerts"), &intermediate_targets);
        stats.count_output(&output_record("internal_events"), &intermediate_targets);
        stats.count_output(&output_record("alerts"), &intermediate_targets);

        assert_eq!(stats.output_emitted, 2);
        assert_eq!(stats.intermediate_emitted, 1);
    }
}

#[cfg(test)]
mod pipe_stager_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use wf_engine::alert::AlertOrigin;
    use wf_engine::match_engine::Value;

    fn record_with(
        target: &str,
        event_time_nanos: i64,
        yield_fields: Vec<(Arc<str>, Value)>,
    ) -> OutputRecord {
        OutputRecord {
            wfx_id: format!("id-{event_time_nanos}"),
            rule_name: "pipe_s1".into(),
            score: 1.0,
            entity_type: "ip".into(),
            entity_id: "10.0.0.1".to_string(),
            origin: AlertOrigin::Event,
            fired_at: "2026-01-01T00:00:00Z".to_string(),
            emit_time: "2026-01-01T00:00:00Z".into(),
            matched_rows: Vec::new(),
            summary: "".into(),
            yield_target: target.into(),
            yield_fields,
            yield_field_types: Vec::new().into(),
            event_time_nanos,
            machine_id: String::new(),
            scope_key: "".into(),
        }
    }

    /// Covers every arm of the coercion matrix: the pipe event-time field,
    /// the time column (with and without an explicit value), all supported
    /// scalar columns, Utf8 coercions of non-string values, type-mismatch
    /// rows (-> null), and an unsupported column type (Date32 -> null).
    fn stager_schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                PIPE_EVENT_TIME_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("n_i", DataType::Int64, true),
            Field::new("n_f", DataType::Float64, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("blob", DataType::Utf8, true),
            Field::new("unsupported", DataType::Date32, true),
        ]))
    }

    fn varied_records() -> Vec<OutputRecord> {
        vec![
            // All fields present, happy path.
            record_with(
                "t",
                1_000,
                vec![
                    (
                        "event_time".into(),
                        Value::Number(1_700_000_000_000_000_000.0),
                    ),
                    ("n_i".into(), Value::Number(7.0)),
                    ("n_f".into(), Value::Number(1.5)),
                    ("flag".into(), Value::Bool(true)),
                    ("label".into(), Value::Str("x".into())),
                    (
                        "blob".into(),
                        Value::Array(vec![Value::Number(1.0), Value::Str("a".into())]),
                    ),
                ],
            ),
            // Missing scalars -> null; time column absent -> event-time
            // fallback; Utf8 coercion of Number.
            record_with(
                "t",
                2_000,
                vec![
                    ("n_f".into(), Value::Number(2.0)),
                    ("label".into(), Value::Number(42.0)),
                ],
            ),
            // Type mismatches -> null; Utf8 coercion of Bool.
            record_with(
                "t",
                3_000,
                vec![
                    ("n_i".into(), Value::Str("zz".into())),
                    ("flag".into(), Value::Number(1.0)),
                    ("label".into(), Value::Bool(true)),
                ],
            ),
        ]
    }

    /// Direct semantic assertions on the staging coercion matrix (the old
    /// per-row `build_pipeline_batch` path is gone; its behaviour lives on
    /// exactly in `push_record`).
    #[test]
    fn staged_batch_coercion_matrix() {
        let schema = stager_schema();
        let records = varied_records();
        let mut stager = PipeBatchStager::new("t".into(), Arc::clone(&schema), Some(1));
        for record in &records {
            stager.push_record(record).expect("stage row");
        }
        let (_, staged) = stager.take_events().unwrap().expect("rows staged");
        assert_eq!(staged.len(), records.len());

        // Row 0 — every field present, happy path.
        let f = &staged[0].fields;
        assert_eq!(f.get(PIPE_EVENT_TIME_FIELD), Some(&Value::Number(1_000.0)));
        assert_eq!(
            f.get("event_time"),
            Some(&Value::Number(1_700_000_000_000_000_000.0))
        );
        assert_eq!(f.get("n_i"), Some(&Value::Number(7.0)));
        assert_eq!(f.get("n_f"), Some(&Value::Number(1.5)));
        assert_eq!(f.get("flag"), Some(&Value::Bool(true)));
        assert_eq!(f.get("label"), Some(&Value::Str("x".into())));
        assert_eq!(f.get("blob"), Some(&Value::Str(r#"[1.0,"a"]"#.into())));
        assert_eq!(f.get("unsupported"), None, "Date32 column stages as null");

        // Row 1 — missing scalars -> null (field absent); Utf8 coercion of
        // Number; the time column falls back to the record event time.
        let f = &staged[1].fields;
        assert_eq!(f.get(PIPE_EVENT_TIME_FIELD), Some(&Value::Number(2_000.0)));
        assert_eq!(
            f.get("event_time"),
            Some(&Value::Number(2_000.0)),
            "missing time-col value must fall back to event_time_nanos"
        );
        assert_eq!(f.get("n_i"), None);
        assert_eq!(f.get("n_f"), Some(&Value::Number(2.0)));
        assert_eq!(f.get("flag"), None);
        assert_eq!(f.get("label"), Some(&Value::Str("42".into())));
        assert_eq!(f.get("blob"), None);

        // Row 2 — type mismatches -> null; Utf8 coercion of Bool; a row
        // without any time value gets its own event_time_nanos.
        let f = &staged[2].fields;
        assert_eq!(f.get(PIPE_EVENT_TIME_FIELD), Some(&Value::Number(3_000.0)));
        assert_eq!(f.get("event_time"), Some(&Value::Number(3_000.0)));
        assert_eq!(f.get("n_i"), None, "Str into Int64 stages as null");
        assert_eq!(f.get("flag"), None, "Number into Bool stages as null");
        assert_eq!(f.get("label"), Some(&Value::Str("true".into())));
    }

    /// A non-finite number inside a structured (Array/Object) value must
    /// fail the row instead of serializing `NaN` into JSON.
    #[test]
    fn staged_row_rejects_non_finite_number_inside_structured_value() {
        let schema = stager_schema();
        let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
        let record = record_with(
            "t",
            0,
            vec![(
                "blob".into(),
                Value::Object(
                    [("score".into(), Value::Number(f64::NAN))]
                        .into_iter()
                        .collect(),
                ),
            )],
        );
        let err = stager
            .push_record(&record)
            .expect_err("non-finite structured number should fail");
        assert!(
            err.to_string()
                .contains("structured numeric value must be finite")
        );
    }

    /// An explicit epoch-seconds/millis float yield for a Timestamp column
    /// is normalized to epoch nanos.
    #[test]
    fn staged_timestamp_preserves_time_yield_as_epoch_nanos() {
        let schema = stager_schema();
        let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
        let ts = 1_700_000_000_123_000_000i64;
        let record = record_with(
            "t",
            0,
            vec![("event_time".into(), Value::Number(1_700_000_000_123.0))],
        );
        stager.push_record(&record).expect("stage row");
        let (_, staged) = stager.take_events().unwrap().expect("rows staged");
        assert_eq!(
            staged[0].fields.get("event_time"),
            Some(&Value::Number(ts as f64)),
            "float epoch yield must normalize to exact epoch nanos"
        );
    }

    /// Flushing empties the buffers: a second flush is a no-op and later
    /// rows start a fresh batch (per-input-batch flush boundary).
    #[test]
    fn stager_take_resets_buffers_between_flushes() {
        let schema = stager_schema();
        let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));

        assert!(
            stager.take_events().unwrap().is_none(),
            "fresh stager flush is a no-op"
        );

        stager
            .push_record(&record_with(
                "t",
                5,
                vec![("label".into(), Value::Str("a".into()))],
            ))
            .unwrap();
        stager
            .push_record(&record_with(
                "t",
                6,
                vec![("label".into(), Value::Str("b".into()))],
            ))
            .unwrap();
        let first = stager.take_events().unwrap().expect("rows staged");
        assert_eq!(first.1.len(), 2);

        assert!(
            stager.take_events().unwrap().is_none(),
            "buffers must reset after take"
        );

        stager
            .push_record(&record_with(
                "t",
                7,
                vec![("label".into(), Value::Str("c".into()))],
            ))
            .unwrap();
        let second = stager
            .take_events()
            .unwrap()
            .expect("row staged after reset");
        assert_eq!(second.1.len(), 1);
        assert_eq!(
            second.1[0].fields.get("label"),
            Some(&Value::Str("c".into()))
        );
    }

    /// Rows across MANY input batches coalesce only up to the flush point:
    /// a long run keeps column alignment (no drift, no cross-contamination).
    #[test]
    fn stager_column_alignment_holds_over_many_rows() {
        let schema = stager_schema();
        let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
        let rows = 500usize;
        for i in 0..rows {
            stager
                .push_record(&record_with(
                    "t",
                    i as i64,
                    vec![
                        ("n_i".into(), Value::Number(i as f64)),
                        ("label".into(), Value::Str(format!("row-{i}").into())),
                        ("flag".into(), Value::Bool(i % 2 == 0)),
                    ],
                ))
                .unwrap();
        }
        let (_, events) = stager.take_events().unwrap().expect("rows staged");
        assert_eq!(events.len(), rows);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.fields.get("n_i"), Some(&Value::Number(i as f64)));
            assert_eq!(
                event.fields.get("label"),
                Some(&Value::Str(format!("row-{i}").into()))
            );
            assert_eq!(event.fields.get("flag"), Some(&Value::Bool(i % 2 == 0)));
        }
    }
}
