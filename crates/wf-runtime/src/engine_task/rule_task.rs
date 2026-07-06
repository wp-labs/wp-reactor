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
use orion_error::conversion::SourceRawErr;
use tokio::sync::mpsc;

use wf_engine::alert::OutputRecord;
use wf_engine::match_engine::{
    CepStateMachine, CloseReason, RuleExecutor, StepResult, batch_to_events,
};
use wf_engine::window::{AppendOutcome, Router};
use wf_lang::plan::ConvPlan;

use crate::error::{RuntimeReason, RuntimeResult};
use crate::metrics::RuntimeMetrics;

use super::TASK_SEQ;
use super::task_types::{RuleTaskConfig, WindowSource};
use super::window_lookup::RegistryLookup;

const PIPE_EVENT_TIME_FIELD: &str = "__wf_pipe_ts";
type WindowSystemFieldGetter = fn(&OutputRecord) -> wf_engine::match_engine::Value;
type WindowSystemField = (&'static str, WindowSystemFieldGetter);

const WINDOW_SYSTEM_FIELDS: &[WindowSystemField] = &[
    ("__wfu_score", |record| {
        wf_engine::match_engine::Value::Number(record.score)
    }),
    ("__wfu_rule_name", |record| {
        wf_engine::match_engine::Value::Str(record.rule_name.clone())
    }),
    ("__wfu_entity_type", |record| {
        wf_engine::match_engine::Value::Str(record.entity_type.clone())
    }),
    ("__wfu_entity_id", |record| {
        wf_engine::match_engine::Value::Str(record.entity_id.clone())
    }),
];

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
    alert_tx: mpsc::Sender<OutputRecord>,
    /// window_name -> cursor: tracks read position per window.
    pub(super) cursors: HashMap<String, u64>,
    /// Shared router for WindowLookup (joins + has()).
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    intermediate_targets: HashSet<String>,
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
            alert_tx,
            cursors,
            router,
            metrics,
            intermediate_targets,
        };
        (task, cancel, timeout_scan_interval)
    }

    // -- Data processing ----------------------------------------------------

    /// Read new batches from all windows, convert to events, and advance
    /// the state machine.
    pub(super) async fn pull_and_advance(&mut self) {
        for source in &self.sources {
            let cursor = self.cursors.get(&source.window_name).copied().unwrap_or(0);
            let (batches, new_cursor, gap) = {
                let win = source.window.read().expect("lock poisoned");
                let result = win.read_since(cursor);
                wf_debug!(pipe,
                    task_id = %self.task_id,
                    window = %source.window_name,
                    cursor = cursor,
                    new_cursor = result.1,
                    batches = result.0.len(),
                    gap = result.2,
                    "read_since"
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

            for batch in &batches {
                let events = batch_to_events(batch);
                if let Some(metrics) = &self.metrics {
                    metrics.add_rule_events(self.executor.plan().name.as_str(), events.len());
                }
                let lookup = RegistryLookup(&self.router);
                for event in &events {
                    if let Some(machine) = &mut self.machine {
                        let event_nanos = machine.event_time_nanos(event);
                        let closes =
                            machine.scan_expired_at_with_conv(event_nanos, self.conv_plan.as_ref());
                        let rule_name = machine.rule_name().to_string();
                        let mut matched = Vec::new();
                        let ordered_aliases: Vec<&String> =
                            aliases
                                .iter()
                                .filter(|alias| self.executor.is_aux_bind_alias(alias.as_str()))
                                .chain(aliases.iter().filter(|alias| {
                                    !self.executor.is_aux_bind_alias(alias.as_str())
                                }))
                                .collect();
                        for alias in ordered_aliases {
                            if !self
                                .executor
                                .event_matches_alias(alias, event, Some(&lookup))
                            {
                                continue;
                            }
                            if let StepResult::Matched(ctx) =
                                machine.advance_at_with(alias, event, event_nanos, Some(&lookup))
                            {
                                matched.push(ctx);
                            }
                        }

                        for close in &closes {
                            match self.executor.execute_close_with_joins(close, &lookup) {
                                Ok(Some(record)) => self.emit(record).await,
                                Ok(None) => {}
                                Err(e) => {
                                    wf_warn!(pipe, task_id = %self.task_id, error = %e, "execute_close error")
                                }
                            }
                        }

                        for ctx in matched {
                            if let Some(metrics) = &self.metrics {
                                metrics.inc_rule_match(&rule_name);
                            }
                            match self.executor.execute_match_with_joins(&ctx, &lookup) {
                                Ok(Some(record)) => self.emit(record).await,
                                Ok(None) => {}
                                Err(e) => {
                                    wf_warn!(pipe, task_id = %self.task_id, error = %e, "execute_match error")
                                }
                            }
                        }
                    } else if self.each_alias.as_ref().is_some_and(|alias| {
                        aliases.iter().any(|candidate| candidate == alias)
                            && self
                                .executor
                                .event_matches_alias(alias, event, Some(&lookup))
                    }) {
                        let event_nanos = event_time_nanos(event, self.each_time_field.as_deref());
                        match self
                            .executor
                            .execute_each_with_joins(event, event_nanos, &lookup)
                        {
                            Ok(Some(record)) => self.emit(record).await,
                            Ok(None) => {}
                            Err(e) => {
                                wf_warn!(pipe, task_id = %self.task_id, error = %e, "execute_each error")
                            }
                        }
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
        let Some(_) = &self.machine else {
            return;
        };
        let started = Instant::now();
        let lookup = RegistryLookup(&self.router);
        let (rule_name, closes) = {
            let machine = self.machine.as_mut().expect("checked above");
            (
                machine.rule_name().to_string(),
                machine
                    .scan_expired_at_with_conv(machine.watermark_nanos(), self.conv_plan.as_ref()),
            )
        };
        for close in &closes {
            match self.executor.execute_close_with_joins(close, &lookup) {
                Ok(Some(record)) => self.emit(record).await,
                Ok(None) => {}
                Err(e) => {
                    wf_warn!(pipe, task_id = %self.task_id, error = %e, "execute_close error")
                }
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
        let mut emitted = 0usize;
        let lookup = RegistryLookup(&self.router);
        let (rule_name, closes) = {
            let machine = self.machine.as_mut().expect("checked above");
            (
                machine.rule_name().to_string(),
                machine.close_all_with_conv(CloseReason::Flush, self.conv_plan.as_ref()),
            )
        };
        for close in &closes {
            match self.executor.execute_close_with_joins(close, &lookup) {
                Ok(Some(record)) => {
                    self.emit(record).await;
                    emitted += 1;
                }
                Ok(None) => {}
                Err(e) => {
                    wf_warn!(pipe, task_id = %self.task_id, error = %e, "execute_close flush error")
                }
            }
        }
        if emitted > 0 {
            wf_debug!(pipe, task_id = %self.task_id, alerts = emitted, "flush complete");
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
                target = %record.yield_target,
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
                    target = %record.yield_target,
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
                        target = %record.yield_target,
                        error = %e,
                        "append internal pipeline row failed"
                    );
                    return;
                }
            }
        };

        match outcome {
            AppendOutcome::Appended => {
                if let Some(notify) = self.router.registry().get_notifier(&record.yield_target) {
                    notify.notify_waiters();
                }
            }
            AppendOutcome::DroppedLate => {
                wf_warn!(
                    pipe,
                    task_id = %self.task_id,
                    target = %record.yield_target,
                    "intermediate window row dropped as late data"
                );
            }
        }
    }
}

fn build_pipeline_batch(
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
                return Arc::new(TimestampNanosecondArray::from(vec![Some(event_time_nanos)]))
                    as ArrayRef;
            }
            let value = values.get(field.name().as_str()).copied();
            if time_col_index == Some(idx) && value.is_none() {
                return Arc::new(TimestampNanosecondArray::from(vec![Some(event_time_nanos)]))
                    as ArrayRef;
            }
            value_to_single_row_array(field.data_type(), value)
        })
        .collect();
    RecordBatch::try_new(schema, arrays)
        .source_raw_err(RuntimeReason::Bootstrap, "build internal pipeline batch")
}

fn record_window_fields(record: &OutputRecord) -> Vec<(String, wf_engine::match_engine::Value)> {
    let mut fields = record.yield_fields.clone();
    let existing: HashSet<String> = fields.iter().map(|(name, _)| name.clone()).collect();
    for (name, builder) in WINDOW_SYSTEM_FIELDS {
        if !existing.contains(*name) {
            fields.push(((*name).to_string(), builder(record)));
        }
    }
    fields
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
) -> ArrayRef {
    match (data_type, value) {
        (DataType::Int64, Some(wf_engine::match_engine::Value::Number(n))) => {
            Arc::new(Int64Array::from(vec![Some(*n as i64)]))
        }
        (DataType::Float64, Some(wf_engine::match_engine::Value::Number(n))) => {
            Arc::new(Float64Array::from(vec![Some(*n)]))
        }
        (DataType::Boolean, Some(wf_engine::match_engine::Value::Bool(b))) => {
            Arc::new(BooleanArray::from(vec![Some(*b)]))
        }
        (DataType::Utf8, Some(wf_engine::match_engine::Value::Str(s))) => {
            Arc::new(StringArray::from(vec![Some(s.as_str())]))
        }
        (DataType::Utf8, Some(wf_engine::match_engine::Value::Number(n))) => {
            Arc::new(StringArray::from(vec![Some(n.to_string())]))
        }
        (DataType::Utf8, Some(wf_engine::match_engine::Value::Bool(b))) => {
            Arc::new(StringArray::from(vec![Some(b.to_string())]))
        }
        (DataType::Timestamp(_, _), Some(wf_engine::match_engine::Value::Number(n))) => {
            Arc::new(TimestampNanosecondArray::from(vec![Some(*n as i64)]))
        }
        _ => new_null_array(data_type, 1),
    }
}
