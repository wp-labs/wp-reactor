use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use wf_core::alert::OutputRecord;
use wf_core::rule::{CepStateMachine, CloseReason, RuleExecutor, StepResult, batch_to_events};
use wf_core::window::Router;
use wf_lang::plan::ConvPlan;

use super::TASK_SEQ;
use super::task_types::{RuleTaskConfig, WindowSource};
use super::window_lookup::RegistryLookup;

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
            }
            self.cursors.insert(source.window_name.clone(), new_cursor);

            let Some(aliases) = self.aliases.get(&source.window_name) else {
                continue;
            };

            for batch in &batches {
                let events = batch_to_events(batch);
                let lookup = RegistryLookup(&self.router);
                for event in &events {
                    for alias in aliases {
                        if let StepResult::Matched(ctx) =
                            self.machine.advance_with(alias, event, Some(&lookup))
                        {
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
    }

    // -- Alert emission -----------------------------------------------------

    async fn emit(&self, record: OutputRecord) {
        if let Err(e) = self.alert_tx.send(record).await {
            wf_warn!(pipe, error = %e, "alert channel closed");
        }
    }
}
