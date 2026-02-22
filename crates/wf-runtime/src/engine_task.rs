use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::Poll;
use std::time::Duration;

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use wf_core::alert::AlertRecord;
use wf_core::rule::{CepStateMachine, CloseReason, RuleExecutor, StepResult, batch_to_events};
use wf_core::window::Window;

// ---------------------------------------------------------------------------
// WindowSource — one window a rule engine reads from
// ---------------------------------------------------------------------------

pub(crate) struct WindowSource {
    pub window_name: String,
    pub window: Arc<RwLock<Window>>,
    pub notify: Arc<Notify>,
    /// Stream names that flow into this window.
    pub stream_names: Vec<String>,
}

// ---------------------------------------------------------------------------
// EngineTaskConfig — everything an engine task needs to run
// ---------------------------------------------------------------------------

pub(crate) struct EngineTaskConfig {
    pub machine: CepStateMachine,
    pub executor: RuleExecutor,
    pub window_sources: Vec<WindowSource>,
    /// stream_name → Vec<alias>: which CEP aliases receive events from each stream.
    pub stream_aliases: HashMap<String, Vec<String>>,
    pub alert_tx: mpsc::Sender<AlertRecord>,
    pub cancel: CancellationToken,
    pub timeout_scan_interval: Duration,
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/// Run a single engine task until cancelled.
///
/// Each engine task owns its `CepStateMachine` exclusively (no `Arc<Mutex>`).
/// It wakes on any window notification, reads new batches via cursor-based
/// `read_since()`, converts them to events, and advances the state machine.
///
/// Uses `Notified::enable()` to register waiters before reading data, ensuring
/// no notifications are lost between data checks and waits.
pub(crate) async fn run_engine_task(config: EngineTaskConfig) -> anyhow::Result<()> {
    let EngineTaskConfig {
        mut machine,
        executor,
        window_sources,
        stream_aliases,
        alert_tx,
        cancel,
        timeout_scan_interval,
    } = config;

    let rule_name = machine.rule_name().to_owned();

    // Pre-compute aliases per window source: for each window, collect all
    // aliases from all streams that flow into it.
    let aliases_per_window: HashMap<String, Vec<String>> = window_sources
        .iter()
        .map(|src| {
            let aliases: Vec<String> = src
                .stream_names
                .iter()
                .flat_map(|s| stream_aliases.get(s).into_iter().flatten())
                .cloned()
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            (src.window_name.clone(), aliases)
        })
        .collect();

    // Initialize cursors to current position (skip historical data).
    let mut cursors: HashMap<String, u64> = HashMap::new();
    for source in &window_sources {
        let win = source.window.read().expect("lock poisoned");
        cursors.insert(source.window_name.clone(), win.next_seq());
    }

    let mut timeout_interval = tokio::time::interval(timeout_scan_interval);

    loop {
        // Create notified futures and enable them BEFORE reading data.
        // enable() registers a waiter so that notify_waiters() called after
        // this point will wake the future, even if we haven't .await'd it yet.
        let mut futs: Vec<Pin<Box<tokio::sync::futures::Notified<'_>>>> = window_sources
            .iter()
            .map(|s| Box::pin(s.notify.notified()))
            .collect();
        for fut in &mut futs {
            fut.as_mut().enable();
        }

        // Process all available data.
        process_new_data(
            &window_sources,
            &aliases_per_window,
            &mut machine,
            &executor,
            &alert_tx,
            &mut cursors,
        )
        .await;

        // Wait for next wake: notification, timeout, or cancellation.
        tokio::select! {
            biased;

            _ = poll_any_notified(&mut futs) => {
                // Data arrived — loop back to read it.
            }
            _ = timeout_interval.tick() => {
                scan_and_emit_timeouts(&mut machine, &executor, &alert_tx).await;
            }
            _ = cancel.cancelled() => {
                // Drain: read all windows one last time.
                process_new_data(
                    &window_sources, &aliases_per_window,
                    &mut machine, &executor, &alert_tx, &mut cursors,
                ).await;
                // Flush: close all active instances.
                flush_all(&mut machine, &executor, &alert_tx).await;
                wf_debug!(pipe, rule = %rule_name, "engine task shutdown complete");
                break;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Poll a set of pre-enabled Notified futures; resolves when any one fires.
async fn poll_any_notified(futs: &mut [Pin<Box<tokio::sync::futures::Notified<'_>>>]) {
    if futs.is_empty() {
        std::future::pending::<()>().await;
        return;
    }

    std::future::poll_fn(|cx| {
        for fut in futs.iter_mut() {
            if fut.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    })
    .await;
}

/// Read new data from all window sources and advance the state machine.
async fn process_new_data(
    window_sources: &[WindowSource],
    aliases_per_window: &HashMap<String, Vec<String>>,
    machine: &mut CepStateMachine,
    executor: &RuleExecutor,
    alert_tx: &mpsc::Sender<AlertRecord>,
    cursors: &mut HashMap<String, u64>,
) {
    for source in window_sources {
        let cursor = cursors.get(&source.window_name).copied().unwrap_or(0);

        let (batches, new_cursor, gap) = {
            let win = source.window.read().expect("lock poisoned");
            let result = win.read_since(cursor);
            wf_debug!(pipe,
                rule = %machine.rule_name(),
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
                rule = %machine.rule_name(),
                window = %source.window_name,
                "cursor gap detected — some data was lost to eviction"
            );
        }

        cursors.insert(source.window_name.clone(), new_cursor);

        let aliases = match aliases_per_window.get(&source.window_name) {
            Some(a) => a,
            None => continue,
        };

        for batch in &batches {
            let events = batch_to_events(batch);
            for event in &events {
                for alias in aliases {
                    match machine.advance(alias, event) {
                        StepResult::Matched(ctx) => match executor.execute_match(&ctx) {
                            Ok(record) => emit_alert(alert_tx, record).await,
                            Err(e) => wf_warn!(pipe, error = %e, "execute_match error"),
                        },
                        StepResult::Advance | StepResult::Accumulate => {}
                    }
                }
            }
        }
    }
}

/// Scan for expired instances and emit any resulting alerts.
async fn scan_and_emit_timeouts(
    machine: &mut CepStateMachine,
    executor: &RuleExecutor,
    alert_tx: &mpsc::Sender<AlertRecord>,
) {
    let close_outputs = machine.scan_expired();
    for close in &close_outputs {
        match executor.execute_close(close) {
            Ok(Some(record)) => emit_alert(alert_tx, record).await,
            Ok(None) => {}
            Err(e) => wf_warn!(pipe, error = %e, "execute_close error"),
        }
    }
}

/// Close all active instances on shutdown and emit any resulting alerts.
async fn flush_all(
    machine: &mut CepStateMachine,
    executor: &RuleExecutor,
    alert_tx: &mpsc::Sender<AlertRecord>,
) {
    let close_outputs = machine.close_all(CloseReason::Flush);
    let mut total_alerts = 0usize;
    for close in &close_outputs {
        match executor.execute_close(close) {
            Ok(Some(record)) => {
                emit_alert(alert_tx, record).await;
                total_alerts += 1;
            }
            Ok(None) => {}
            Err(e) => wf_warn!(pipe, error = %e, "execute_close flush error"),
        }
    }
    if total_alerts > 0 {
        wf_debug!(pipe, alerts = total_alerts, "engine flush_all complete");
    }
}

async fn emit_alert(tx: &mpsc::Sender<AlertRecord>, record: AlertRecord) {
    if let Err(e) = tx.send(record).await {
        wf_warn!(pipe, error = %e, "alert channel closed");
    }
}
