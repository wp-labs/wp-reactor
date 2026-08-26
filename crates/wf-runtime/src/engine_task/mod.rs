mod conv_stage;
pub(crate) mod rule_task;
mod stats_task;
mod task_types;
mod window_lookup;

#[cfg(test)]
mod columnar_join_integration_tests;
#[cfg(test)]
mod coverage_extra;
#[cfg(test)]
mod deferred_integration_tests;
#[cfg(test)]
mod hop_integration_tests;
#[cfg(test)]
mod provider_join_integration_tests;
#[cfg(test)]
mod stats_task_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod where_integration_tests;

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::task::Poll;

use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;
use wf_engine::window::RulePush;

pub(crate) use conv_stage::{ConvCloseBatch, ConvShardSink, ConvStageConfig, run_conv_stage_task};
pub(crate) use stats_task::StatsPartial;
pub(crate) use stats_task::run_stats_task;
pub(crate) use task_types::{RuleTaskConfig, StatsTaskConfig, WindowSource};

use crate::error::RuntimeResult;

use rule_task::RuleTask;

static TASK_SEQ: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Run a single rule task until cancelled.
///
/// Two data paths are supported, selected by whether the config carries a push
/// channel (`RuleTaskConfig::push_rx`):
///
/// * **push** (R1, `Some`): the rule consumes `Arc<Vec<Arc<Event>>>` from its channel
///   and advances the state machine — no window read lock on the data path.
/// * **pull** (legacy, `None`): wakes on window notifications and reads new
///   batches via cursor-based `events_since()`.
///
/// Both paths keep the periodic timeout scan, EOS flush, and shutdown flush.
/// `root_cancel` is the reactor's root cancellation token (distinct from the
/// rule group's own `RuleTaskConfig::cancel`): at full shutdown both fire and
/// the window actors drain, so the pull loop waits for them before the final
/// flush; on a hot reload only the rule token fires and the actors keep
/// running, so the wait is skipped.
pub(crate) async fn run_rule_task(
    config: RuleTaskConfig,
    root_cancel: CancellationToken,
) -> RuntimeResult<()> {
    let (mut task, cancel, timeout_scan_interval) = rule_task::RuleTask::new(config);
    let task_id = task.task_id.clone();
    let mut timeout_tick = tokio::time::interval(timeout_scan_interval);
    let mut eos = task.eos_flush.clone();

    if let Some(rx) = task.push_rx.take() {
        run_push_loop(&mut task, rx, cancel, &mut eos, &mut timeout_tick, &task_id).await
    } else {
        run_pull_loop(
            &mut task,
            cancel,
            root_cancel,
            &mut eos,
            &mut timeout_tick,
            &task_id,
        )
        .await
    }
}

/// Push data path: consume `Arc<Vec<Arc<Event>>>` from the rule's channel.
async fn run_push_loop(
    task: &mut RuleTask,
    mut rx: mpsc::Receiver<RulePush>,
    cancel: CancellationToken,
    eos: &mut watch::Receiver<u64>,
    timeout_tick: &mut tokio::time::Interval,
    task_id: &str,
) -> RuntimeResult<()> {
    loop {
        tokio::select! {
            biased;
            // Shutdown has top priority: once cancelled we stop accepting new
            // pushes and drain what's buffered. With the channel kept full by
            // an ingest burst, `rx.recv()` would otherwise starve the cancel
            // branch and extend shutdown by the whole backlog.
            _ = cancel.cancelled() => {
                // 2026-08-23 q13：shutdown 竞态——中间管道生产者（上游规则
                // flush_pipes 广播）可能晚于本任务 cancel；单次 drain 会漏掉
                // 生产者稍后投递的剩余批次（q13b 只收到部分广播）。drain 后
                // 短暂重试（每轮 50ms，最多 ~1s）直到通道排空，再 flush 收口。
                for _ in 0..20 {
                    task.drain_push_channel(&mut rx).await;
                    if rx.is_empty() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                task.flush().await;
                wf_debug!(pipe, task_id = %task_id, "rule task shutdown complete");
                break;
            }
            push = rx.recv() => {
                match push {
                    Some(push) => {
                        task.process_push(push).await;
                    }
                    // All producers dropped (channel closed): drain + flush.
                    None => {
                        task.drain_push_channel(&mut rx).await;
                        task.flush().await;
                        break;
                    }
                }
            }
            // End-of-stream: input sources reported the stream ended. Flush the
            // trailing instances but keep running so a daemon can accept a
            // subsequent finite input.
            _ = eos.changed() => {
                if *eos.borrow() > 0 {
                    task.drain_push_channel(&mut rx).await;
                    task.flush().await;
                    wf_debug!(pipe, task_id = %task_id, "rule task EOS flush complete");
                }
            }
            _ = timeout_tick.tick() => task.scan_timeouts().await,
        }
    }
    Ok(())
}

/// Legacy pull data path: notify + cursor-based `events_since()`.
///
/// Uses `Notified::enable()` to register waiters before reading data,
/// ensuring no notifications are lost between data checks and waits.
async fn run_pull_loop(
    task: &mut RuleTask,
    cancel: CancellationToken,
    root_cancel: CancellationToken,
    eos: &mut watch::Receiver<u64>,
    timeout_tick: &mut tokio::time::Interval,
    task_id: &str,
) -> RuntimeResult<()> {
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
                // Full shutdown (root cancelled): the window actors are also
                // draining — wait for them to commit their queued tail so the
                // flush below runs against a complete machine. On a hot reload
                // only the rule token fires and the actors keep running, so
                // skip the wait (their data is re-pulled by the new generation).
                if root_cancel.is_cancelled() {
                    task.wait_shutdown_drain().await;
                }
                task.pull_and_advance().await;
                task.flush().await;
                wf_debug!(pipe, task_id = %task_id, "rule task shutdown complete");
                break;
            }
            // End-of-stream: input sources reported the stream ended. Flush the
            // trailing instances (EOS-driven finalization) but keep running so a
            // daemon can accept a subsequent finite input. The counter is
            // incremented per EOS event; 0 means no EOS yet.
            _ = eos.changed() => {
                if *eos.borrow() > 0 {
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
