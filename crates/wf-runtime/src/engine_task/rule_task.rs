use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    new_null_array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use wf_core::alert::OutputRecord;
use wf_core::rule::{CepStateMachine, CloseReason, RuleExecutor, StepResult, batch_to_events};
use wf_core::window::{AppendOutcome, Router};
use wf_lang::plan::ConvPlan;

use crate::metrics::RuntimeMetrics;

use super::TASK_SEQ;
use super::task_types::{RuleTaskConfig, WindowSource};
use super::window_lookup::RegistryLookup;

const PIPE_WINDOW_PREFIX: &str = "__wf_pipe_";
const PIPE_EVENT_TIME_FIELD: &str = "__wf_pipe_ts";

// ---------------------------------------------------------------------------
// RuleTask -- runtime state for a single rule
// ---------------------------------------------------------------------------

/// Holds all mutable state for one rule's processing loop.
///
/// Each `RuleTask` owns its `CepStateMachine` exclusively (no `Arc<Mutex>`).
pub(super) struct RuleTask {
    pub(super) task_id: String,
    machine: CepStateMachine,
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
            executor,
            window_sources,
            stream_aliases,
            alert_tx,
            cancel,
            timeout_scan_interval,
            router,
            metrics,
        } = config;

        // Pre-compute aliases per window: for each window, collect all
        // aliases from all streams that flow into it (deduplicated).
        let aliases: HashMap<String, Vec<String>> = window_sources
            .iter()
            .map(|src| {
                let window_aliases: Vec<String> = src
                    .stream_names
                    .iter()
                    .flat_map(|s| stream_aliases.get(s).into_iter().flatten())
                    .cloned()
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                (src.window_name.clone(), window_aliases)
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
        let task_id = format!("{}#{}", machine.rule_name(), seq);
        let conv_plan = executor.plan().conv_plan.clone();

        let task = Self {
            task_id,
            machine,
            executor,
            conv_plan,
            sources: window_sources,
            aliases,
            alert_tx,
            cursors,
            router,
            metrics,
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
                    metrics.inc_rule_cursor_gap(self.machine.rule_name(), &source.window_name);
                }
            }
            self.cursors.insert(source.window_name.clone(), new_cursor);

            let Some(aliases) = self.aliases.get(&source.window_name) else {
                continue;
            };

            for batch in &batches {
                let events = batch_to_events(batch);
                if let Some(metrics) = &self.metrics {
                    metrics.add_rule_events(self.machine.rule_name(), events.len());
                }
                let lookup = RegistryLookup(&self.router);
                for event in &events {
                    for alias in aliases {
                        if let StepResult::Matched(ctx) =
                            self.machine.advance_with(alias, event, Some(&lookup))
                        {
                            if let Some(metrics) = &self.metrics {
                                metrics.inc_rule_match(self.machine.rule_name());
                            }
                            match self.executor.execute_match_with_joins(&ctx, &lookup) {
                                Ok(record) => self.emit(record).await,
                                Err(e) => {
                                    wf_warn!(pipe, task_id = %self.task_id, error = %e, "execute_match error")
                                }
                            }
                        }
                    }
                }
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics.set_rule_instances(self.machine.rule_name(), self.machine.instance_count());
        }
    }

    // -- Timeout & shutdown -------------------------------------------------

    /// Scan for expired state machine instances and emit alerts.
    pub(super) async fn scan_timeouts(&mut self) {
        let lookup = RegistryLookup(&self.router);
        for close in &self
            .machine
            .scan_expired_at_with_conv(self.machine.watermark_nanos(), self.conv_plan.as_ref())
        {
            match self.executor.execute_close_with_joins(close, &lookup) {
                Ok(Some(record)) => self.emit(record).await,
                Ok(None) => {}
                Err(e) => {
                    wf_warn!(pipe, task_id = %self.task_id, error = %e, "execute_close error")
                }
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics.set_rule_instances(self.machine.rule_name(), self.machine.instance_count());
        }
    }

    /// Close all active instances (shutdown flush) and emit alerts.
    pub(super) async fn flush(&mut self) {
        let mut emitted = 0usize;
        let lookup = RegistryLookup(&self.router);
        for close in &self
            .machine
            .close_all_with_conv(CloseReason::Flush, self.conv_plan.as_ref())
        {
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
            metrics.set_rule_instances(self.machine.rule_name(), self.machine.instance_count());
        }
    }

    // -- Alert emission -----------------------------------------------------

    async fn emit(&self, record: OutputRecord) {
        if record.yield_target.starts_with(PIPE_WINDOW_PREFIX) {
            self.emit_pipeline_stage(record);
            return;
        }
        if let Some(metrics) = &self.metrics {
            metrics.inc_alert_emitted(&record.rule_name);
        }
        if let Err(e) = self.alert_tx.send(record).await {
            if let Some(metrics) = &self.metrics {
                metrics.inc_alert_channel_send_failed();
            }
            wf_warn!(pipe, error = %e, "alert channel closed");
        }
    }

    fn emit_pipeline_stage(&self, record: OutputRecord) {
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
            &record.yield_fields,
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
                    "internal pipeline row dropped as late data"
                );
            }
        }
    }
}

fn build_pipeline_batch(
    schema: arrow::datatypes::SchemaRef,
    time_col_index: Option<usize>,
    event_time_nanos: i64,
    yield_fields: &[(String, wf_core::rule::Value)],
) -> anyhow::Result<RecordBatch> {
    let values: HashMap<&str, &wf_core::rule::Value> =
        yield_fields.iter().map(|(k, v)| (k.as_str(), v)).collect();
    let arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            if time_col_index == Some(idx) || field.name() == PIPE_EVENT_TIME_FIELD {
                return Arc::new(TimestampNanosecondArray::from(vec![Some(event_time_nanos)]))
                    as ArrayRef;
            }
            let value = values.get(field.name().as_str()).copied();
            value_to_single_row_array(field.data_type(), value)
        })
        .collect();
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn value_to_single_row_array(
    data_type: &DataType,
    value: Option<&wf_core::rule::Value>,
) -> ArrayRef {
    match (data_type, value) {
        (DataType::Int64, Some(wf_core::rule::Value::Number(n))) => {
            Arc::new(Int64Array::from(vec![Some(*n as i64)]))
        }
        (DataType::Float64, Some(wf_core::rule::Value::Number(n))) => {
            Arc::new(Float64Array::from(vec![Some(*n)]))
        }
        (DataType::Boolean, Some(wf_core::rule::Value::Bool(b))) => {
            Arc::new(BooleanArray::from(vec![Some(*b)]))
        }
        (DataType::Utf8, Some(wf_core::rule::Value::Str(s))) => {
            Arc::new(StringArray::from(vec![Some(s.as_str())]))
        }
        (DataType::Utf8, Some(wf_core::rule::Value::Number(n))) => {
            Arc::new(StringArray::from(vec![Some(n.to_string())]))
        }
        (DataType::Utf8, Some(wf_core::rule::Value::Bool(b))) => {
            Arc::new(StringArray::from(vec![Some(b.to_string())]))
        }
        (DataType::Timestamp(_, _), Some(wf_core::rule::Value::Number(n))) => {
            Arc::new(TimestampNanosecondArray::from(vec![Some(*n as i64)]))
        }
        _ => new_null_array(data_type, 1),
    }
}
