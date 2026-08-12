mod rule_task;
mod task_types;
mod window_lookup;

#[cfg(test)]
mod tests;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::task::Poll;

use tokio::sync::Notify;

pub(crate) use task_types::{RuleTaskConfig, WindowSource};

use crate::error::RuntimeResult;

static TASK_SEQ: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run a single rule task until cancelled.
///
/// Wakes on window notifications, reads new batches via cursor-based
/// `read_since()`, converts them to events, and advances the state machine.
///
/// Uses `Notified::enable()` to register waiters before reading data,
/// ensuring no notifications are lost between data checks and waits.
pub(crate) async fn run_rule_task(config: RuleTaskConfig) -> RuntimeResult<()> {
    let (mut task, cancel, timeout_scan_interval) = rule_task::RuleTask::new(config);
    let task_id = task.task_id.clone();
    let mut timeout_tick = tokio::time::interval(timeout_scan_interval);
    let mut eos = task.eos_flush.clone();

    // Clone Arc<Notify> handles outside the struct so that notification
    // registration borrows `notifiers` (not `task`), allowing `&mut task`
    // for processing in the same loop iteration.
    let notifiers: Vec<Arc<Notify>> = task.sources.iter().map(|s| Arc::clone(&s.notify)).collect();

    loop {
        let mut notifications = register_notifications(&notifiers);
        task.pull_and_advance().await;

        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                task.pull_and_advance().await;
                task.flush().await;
                wf_debug!(pipe, task_id = %task_id, "rule task shutdown complete");
                break;
            }
            // End-of-stream: input sources reported the stream ended. Flush the
            // trailing instances (EOS-driven finalization) but keep running so a
            // daemon can accept a subsequent finite input.
            _ = eos.changed() => {
                if *eos.borrow() {
                    task.pull_and_advance().await;
                    task.flush().await;
                    wf_debug!(pipe, task_id = %task_id, "rule task EOS flush complete");
                }
            }
            _ = timeout_tick.tick() => task.scan_timeouts().await,
            _ = wait_any(&mut notifications) => {}
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Notification helpers
// ---------------------------------------------------------------------------

/// Register notification waiters and enable them immediately.
///
/// Must be called BEFORE [`rule_task::RuleTask::pull_and_advance`] to avoid missing
/// notifications between data reads and waits.
fn register_notifications(
    notifiers: &[Arc<Notify>],
) -> Vec<Pin<Box<tokio::sync::futures::Notified<'_>>>> {
    let mut notified: Vec<_> = notifiers.iter().map(|n| Box::pin(n.notified())).collect();
    for n in &mut notified {
        n.as_mut().enable();
    }
    notified
}

/// Resolve when any pre-enabled Notified future fires.
async fn wait_any(notified: &mut [Pin<Box<tokio::sync::futures::Notified<'_>>>]) {
    if notified.is_empty() {
        std::future::pending::<()>().await;
        return;
    }
    std::future::poll_fn(|cx| {
        for n in notified.iter_mut() {
            if n.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    })
    .await;
}
