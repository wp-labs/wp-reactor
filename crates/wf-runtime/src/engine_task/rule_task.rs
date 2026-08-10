use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    new_null_array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use orion_error::conversion::{SourceRawErr, ToStructError};
use tokio::sync::mpsc;

use wf_engine::alert::OutputRecord;
use wf_engine::match_engine::{CepStateMachine, CloseReason, RuleExecutor, StepResult};
use wf_engine::normalize_epoch_timestamp_float_nanos;
use wf_engine::window::{AppendOutcome, Router};
use wf_lang::plan::ConvPlan;
use wf_lang::wfu_meta::{WFU_ID, WFU_INTERMEDIATE_META_FIELDS, WfuIntermediateMetaField};

use crate::error::{RuntimeReason, RuntimeResult};
use crate::metrics::RuntimeMetrics;

use super::TASK_SEQ;
use super::task_types::{RuleTaskConfig, WindowSource};
use super::window_lookup::RegistryLookup;

const PIPE_EVENT_TIME_FIELD: &str = "__wf_pipe_ts";
const DEBUG_DETAIL_LIMIT: usize = 20;

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
        if intermediate_targets.contains(&record.yield_target) {
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
    pub(super) sources: Vec<WindowSource>,
    /// window_name -> Vec<alias>: pre-computed from stream_aliases + window sources.
    aliases: HashMap<String, Vec<String>>,
    /// window_name -> Vec<alias>: aux bind aliases first, then event aliases.
    ordered_aliases: HashMap<String, Vec<String>>,
    alert_tx: mpsc::Sender<OutputRecord>,
    /// window_name -> cursor: tracks read position per window.
    pub(super) cursors: HashMap<String, u64>,
    /// Shared router for WindowLookup (joins + has()).
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    intermediate_targets: HashSet<String>,
    /// Wall clock when events were last processed. When input goes idle, this
    /// stays put so the periodic timeout scan can advance the effective watermark
    /// by the elapsed wall time — letting instances expire per their window TTL
    /// even without new events (window semantics, not just event-time).
    last_activity_wall: std::time::Instant,
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
            alert_tx,
            cancel,
            timeout_scan_interval,
            router,
            metrics,
            intermediate_targets,
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
                let seq = src.window.read().expect("lock poisoned").next_seq();
                (src.window_name.clone(), seq)
            })
            .collect();

        let seq = TASK_SEQ.fetch_add(1, Ordering::Relaxed);
        let rule_name = executor.plan().name.clone();
        let task_id = format!("{}#{}", rule_name, seq);
        let conv_plan = executor.plan().conv_plan.clone();

        let task = Self {
            task_id,
            machine,
            each_alias,
            each_time_field,
            executor,
            conv_plan,
            sources: window_sources,
            aliases,
            ordered_aliases,
            alert_tx,
            cursors,
            router,
            metrics,
            intermediate_targets,
            last_activity_wall: std::time::Instant::now(),
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
        for source in &self.sources {
            let cursor = self.cursors.get(&source.window_name).copied().unwrap_or(0);
            let (events_list, new_cursor, gap) = {
                let win = source.window.read().expect("lock poisoned");
                // Shared parsed events: the window parses each batch once and
                // hands every rule the same Arc (wp-reactor#19).
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

            let Some(aliases) = self.aliases.get(&source.window_name) else {
                continue;
            };
            let Some(ordered_aliases) = self.ordered_aliases.get(&source.window_name) else {
                continue;
            };

            let first_batch_seq = new_cursor.saturating_sub(events_list.len() as u64);
            for (batch_index, events) in events_list.iter().enumerate() {
                let batch_seq = first_batch_seq + batch_index as u64;
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
                        window = %source.window_name,
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
                }
                let lookup = RegistryLookup(&self.router);
                for (row_index, event) in events.iter().enumerate() {
                    if let Some(machine) = &mut self.machine {
                        let event_nanos = machine.event_time_nanos(event);
                        let closes =
                            machine.scan_expired_at_with_conv(event_nanos, self.conv_plan.as_ref());
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
                                        window = %source.window_name,
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
                                    machine.advance_at_with(
                                        alias,
                                        event,
                                        event_nanos,
                                        Some(&lookup),
                                    ),
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
                                        let event_ref =
                                            event_debug_ref(event, batch_seq, row_index);
                                        if let Some(progress) = progress.as_ref() {
                                            wf_debug!(pipe,
                                                rule = %rule_name_for_log,
                                                stage = 0,
                                                window = %source.window_name,
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
                                                window = %source.window_name,
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
                                        let event_ref =
                                            event_debug_ref(event, batch_seq, row_index);
                                        if let Some(progress) = progress.as_ref() {
                                            wf_debug!(pipe,
                                                rule = %rule_name_for_log,
                                                stage = 0,
                                                window = %source.window_name,
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
                                                window = %source.window_name,
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
                                        let event_ref =
                                            event_debug_ref(event, batch_seq, row_index);
                                        let step = ctx.step_data.last();
                                        wf_debug!(pipe,
                                            rule = %rule_name_for_log,
                                            stage = 0,
                                            window = %source.window_name,
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

                        for ctx in matched {
                            if let Some(metrics) = &self.metrics {
                                metrics.inc_rule_match(self.rule_name());
                            }
                            match self.executor.execute_match_with_joins(&ctx, &lookup) {
                                Ok(Some(record)) => {
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
                            let event_nanos =
                                event_time_nanos(event, self.each_time_field.as_deref());
                            match self
                                .executor
                                .execute_each_with_joins(event, event_nanos, &lookup)
                            {
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
                                        log_output_suppressed(
                                            rule_name_for_log,
                                            "execute_each",
                                            None,
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
                                        phase = "execute_each",
                                        error = %e,
                                        "rule output failed"
                                    )
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
                                    window = %source.window_name,
                                    alias = %alias,
                                    event_ref = %event_ref,
                                    reason = "bind_filter_false",
                                    "rule event rejected"
                                );
                            }
                        }
                    }
                }
                if debug_enabled {
                    let instances_after = self.instance_count();
                    wf_debug!(pipe,
                        rule = %rule_name_for_log,
                        stage = 0,
                        window = %source.window_name,
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
                            window = %source.window_name,
                            batch_seq = batch_seq,
                            detail_logged = stats.detail_logged,
                            detail_suppressed = stats.detail_suppressed,
                            "rule event details suppressed"
                        );
                    }
                }
            }
        }
        if let Some(metrics) = &self.metrics {
            let rule_name = self.executor.plan().name.as_str();
            let instances = self
                .machine
                .as_ref()
                .map(|machine| machine.instance_count())
                .unwrap_or(0);
            metrics.set_rule_instances(rule_name, instances);
        }
    }

    // -- Timeout & shutdown -------------------------------------------------

    /// Scan for expired state machine instances and emit alerts.
    pub(super) async fn scan_timeouts(&mut self) {
        let Some(machine) = &self.machine else {
            return;
        };
        // Advance the effective watermark by the wall-clock time elapsed since the
        // last event was processed. This lets instances expire per their window TTL
        // even when input is completely idle (window semantics, not just event-time).
        let effective_watermark = machine
            .watermark_nanos()
            .saturating_add(self.last_activity_wall.elapsed().as_nanos() as i64);
        let started = Instant::now();
        let lookup = RegistryLookup(&self.router);
        let (rule_name, closes) = {
            let machine = self.machine.as_mut().expect("checked above");
            (
                machine.rule_name().to_string(),
                machine.scan_expired_at_with_conv(effective_watermark, self.conv_plan.as_ref()),
            )
        };
        let mut stats = RuleBatchDebugStats::default();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
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
        if let Some(metrics) = &self.metrics {
            let instances = self
                .machine
                .as_ref()
                .map(|machine| machine.instance_count())
                .unwrap_or(0);
            metrics.observe_rule_scan_timeout(&rule_name, started.elapsed());
            metrics.set_rule_instances(&rule_name, instances);
        }
    }

    /// Close all active instances (shutdown flush) and emit alerts.
    pub(super) async fn flush(&mut self) {
        let Some(_) = &self.machine else {
            return;
        };
        let started = Instant::now();
        let lookup = RegistryLookup(&self.router);
        let (rule_name, closes) = {
            let machine = self.machine.as_mut().expect("checked above");
            (
                machine.rule_name().to_string(),
                machine.close_all_with_conv(CloseReason::Flush, self.conv_plan.as_ref()),
            )
        };
        let mut stats = RuleBatchDebugStats::default();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
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
            let instances = self
                .machine
                .as_ref()
                .map(|machine| machine.instance_count())
                .unwrap_or(0);
            metrics.observe_rule_flush(&rule_name, started.elapsed());
            metrics.set_rule_instances(&rule_name, instances);
        }
    }

    // -- Alert emission -----------------------------------------------------

    async fn emit(&self, record: OutputRecord) {
        if self.intermediate_targets.contains(&record.yield_target) {
            self.emit_window_record(record);
            return;
        }
        if let Some(metrics) = &self.metrics {
            metrics.inc_alert_emitted(&record.rule_name, &record.machine_id, &record.scope_key);
            // E2E latency from event occurrence to alert emission
            let now_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            let e2e_nanos = now_nanos.saturating_sub(record.event_time_nanos.max(0) as u64);
            metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
        }
        match self.alert_tx.try_send(record) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(record)) => {
                if let Some(metrics) = &self.metrics {
                    metrics.inc_alert_channel_full();
                }
                // Fall back to blocking send
                if let Err(e) = self.alert_tx.send(record).await {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_alert_channel_send_failed();
                    }
                    wf_warn!(pipe, error = %e, "alert channel closed");
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(record)) => {
                // Channel is closed — drop the record
                if let Some(metrics) = &self.metrics {
                    metrics.inc_alert_channel_send_failed();
                }
                wf_warn!(pipe, rule = %record.rule_name, "alert channel closed, dropping alert");
            }
        }
    }

    fn emit_window_record(&self, record: OutputRecord) {
        let Some(win_lock) = self.router.registry().get_window(&record.yield_target) else {
            wf_warn!(
                pipe,
                task_id = %self.task_id,
                rule = %record.rule_name,
                target = %record.yield_target,
                output_kind = "intermediate",
                reason = "missing_internal_window",
                "missing internal pipeline window"
            );
            return;
        };

        let (schema, time_col_index) = {
            let win = win_lock.read().expect("lock poisoned");
            (win.schema().clone(), win.time_col_index())
        };
        let batch = match build_pipeline_batch(
            schema,
            time_col_index,
            record.event_time_nanos,
            &record_window_fields(&record),
        ) {
            Ok(batch) => batch,
            Err(e) => {
                wf_warn!(
                    pipe,
                    task_id = %self.task_id,
                    rule = %record.rule_name,
                    target = %record.yield_target,
                    output_kind = "intermediate",
                    error = %e,
                    "build internal pipeline row failed"
                );
                return;
            }
        };

        let outcome = {
            let mut win = win_lock.write().expect("lock poisoned");
            match win.append_with_watermark(batch) {
                Ok(outcome) => outcome,
                Err(e) => {
                    wf_warn!(
                        pipe,
                        task_id = %self.task_id,
                        rule = %record.rule_name,
                        target = %record.yield_target,
                        output_kind = "intermediate",
                        error = %e,
                        "append internal pipeline row failed"
                    );
                    return;
                }
            }
        };

        match outcome {
            AppendOutcome::Appended => {
                wf_debug!(
                    pipe,
                    task_id = %self.task_id,
                    rule = %record.rule_name,
                    target = %record.yield_target,
                    output_kind = "intermediate",
                    "internal pipeline row written"
                );
                if let Some(notify) = self.router.registry().get_notifier(&record.yield_target) {
                    notify.notify_waiters();
                }
            }
            AppendOutcome::DroppedLate => {
                wf_warn!(
                    pipe,
                    task_id = %self.task_id,
                    rule = %record.rule_name,
                    target = %record.yield_target,
                    output_kind = "intermediate",
                    reason = "dropped_late",
                    "intermediate window row dropped as late data"
                );
            }
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
        wf_engine::match_engine::Value::Str(value) => value.clone(),
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
    if intermediate_targets.contains(&record.yield_target) {
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

pub(super) fn build_pipeline_batch(
    schema: arrow::datatypes::SchemaRef,
    time_col_index: Option<usize>,
    event_time_nanos: i64,
    yield_fields: &[(String, wf_engine::match_engine::Value)],
) -> RuntimeResult<RecordBatch> {
    let values: HashMap<&str, &wf_engine::match_engine::Value> =
        yield_fields.iter().map(|(k, v)| (k.as_str(), v)).collect();
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            if field.name() == PIPE_EVENT_TIME_FIELD {
                return Ok(
                    Arc::new(TimestampNanosecondArray::from(vec![Some(event_time_nanos)]))
                        as ArrayRef,
                );
            }
            let value = values.get(field.name().as_str()).copied();
            if time_col_index == Some(idx) && value.is_none() {
                return Ok(
                    Arc::new(TimestampNanosecondArray::from(vec![Some(event_time_nanos)]))
                        as ArrayRef,
                );
            }
            value_to_single_row_array(field.data_type(), value)
        })
        .collect::<RuntimeResult<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays)
        .source_raw_err(RuntimeReason::Bootstrap, "build internal pipeline batch")
}

fn record_window_fields(record: &OutputRecord) -> Vec<(String, wf_engine::match_engine::Value)> {
    let mut fields = record.yield_fields.clone();
    let existing: HashSet<String> = fields.iter().map(|(name, _)| name.clone()).collect();
    for field in WFU_INTERMEDIATE_META_FIELDS.iter().copied() {
        if !existing.contains(field.name()) {
            fields.push((
                field.name().to_string(),
                record_wfu_intermediate_meta_value(record, field),
            ));
        }
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
        WfuIntermediateMetaField::RuleName => Value::Str(record.rule_name.clone()),
        WfuIntermediateMetaField::Score => Value::Number(record.score),
        WfuIntermediateMetaField::EntityType => Value::Str(record.entity_type.clone()),
        WfuIntermediateMetaField::EntityId => Value::Str(record.entity_id.clone()),
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

fn value_to_single_row_array(
    data_type: &DataType,
    value: Option<&wf_engine::match_engine::Value>,
) -> RuntimeResult<ArrayRef> {
    match (data_type, value) {
        (DataType::Int64, Some(wf_engine::match_engine::Value::Number(n))) => {
            Ok(Arc::new(Int64Array::from(vec![Some(*n as i64)])))
        }
        (DataType::Float64, Some(wf_engine::match_engine::Value::Number(n))) => {
            Ok(Arc::new(Float64Array::from(vec![Some(*n)])))
        }
        (DataType::Boolean, Some(wf_engine::match_engine::Value::Bool(b))) => {
            Ok(Arc::new(BooleanArray::from(vec![Some(*b)])))
        }
        (DataType::Utf8, Some(wf_engine::match_engine::Value::Str(s))) => {
            Ok(Arc::new(StringArray::from(vec![Some(s.as_str())])))
        }
        (DataType::Utf8, Some(wf_engine::match_engine::Value::Number(n))) => {
            Ok(Arc::new(StringArray::from(vec![Some(n.to_string())])))
        }
        (DataType::Utf8, Some(wf_engine::match_engine::Value::Bool(b))) => {
            Ok(Arc::new(StringArray::from(vec![Some(b.to_string())])))
        }
        (
            DataType::Utf8,
            Some(
                value @ (wf_engine::match_engine::Value::Array(_)
                | wf_engine::match_engine::Value::Object(_)),
            ),
        ) => Ok(Arc::new(StringArray::from(vec![Some(
            value_to_json_string(value)?,
        )]))),
        (DataType::Timestamp(_, _), Some(wf_engine::match_engine::Value::Number(n))) => {
            let nanos = normalize_epoch_timestamp_float_nanos(*n);
            Ok(Arc::new(TimestampNanosecondArray::from(vec![nanos])))
        }
        _ => Ok(new_null_array(data_type, 1)),
    }
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
        wf_engine::match_engine::Value::Str(s) => Ok(serde_json::Value::from(s.clone())),
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
                    object.insert(key.clone(), value_to_json(value)?);
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
            rule_name: "rule".to_string(),
            score: 1.0,
            entity_type: "ip".to_string(),
            entity_id: "10.0.0.1".to_string(),
            origin: AlertOrigin::Event,
            fired_at: "2026-01-01T00:00:00Z".to_string(),
            emit_time: "2026-01-01T00:00:00Z".to_string(),
            matched_rows: Vec::new(),
            summary: String::new(),
            yield_target: target.to_string(),
            yield_fields: Vec::new(),
            yield_field_types: Vec::new(),
            event_time_nanos: 0,
            machine_id: String::new(),
            scope_key: String::new(),
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
