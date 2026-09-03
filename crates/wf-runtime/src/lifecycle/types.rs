use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use orion_error::conversion::{SourceErr, ToStructError};
use orion_error::prelude::*;
use wf_engine::match_engine::RuleExecutor;
use wf_lang::plan::{LimitsPlan, MatchPlan, StatsPlan};

use crate::error::{RuntimeReason, RuntimeResult};

/// Hard upper bound for joining one task group at shutdown (warp-parse
/// wait_grace_down_with_timeout pattern). Task-internal drain budgets are
/// best-effort — a task busy in a non-cancellable await may not check its
/// cancel token in time — so the join layer force-aborts any task that
/// doesn't exit within this window. It is the guarantee that shutdown always
/// terminates.
///
/// 300s（原 60s）: stats 执行器在 shutdown 时执行 close flush（构建千万级
/// alert——q18 100M ≈ 2940 万条, 流式 drain 仍需分钟级; q19 30M ≈ 8M 条
/// ~13s）。60s 会在 flush 完成前 abort rules/alert group → 尾部窗口产出丢失
/// （q18 100M 实测 EMIT 0）。300s 覆盖 stats flush 构建 + sink 消费（正常
/// 路径; 卡死任务仍会被 abort 兜底）。bench kill 宽限须同步调大
/// （SIGTERM 后 ≥ 300s 再 SIGKILL, 见 bench.sh kill_daemon）。
pub(crate) const GROUP_JOIN_TIMEOUT: Duration = Duration::from_secs(300);

/// Max time to wait for an aborted task to actually unwind. `abort()` only
/// cancels at the task's next yield point — a task chewing through a large
/// batch in synchronous CPU work (or blocked in a non-cancellable syscall)
/// may not yield for a while. Shutdown must not block on that: after this
/// budget the handle is detached and the runtime's final drop reaps the task
/// at process exit.
const ABORT_CONFIRM_TIMEOUT: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// TaskGroup — named collection of async tasks for ordered shutdown
// ---------------------------------------------------------------------------

/// A named group of async tasks that are shut down together.
///
/// Groups are assembled in *start order* and joined in *reverse order*
/// (LIFO) during shutdown, mirroring the dependency graph:
///
///   start:  alert → evictor → rules → receiver (→ metrics)
///   join:   (metrics →) receiver → rules → alert → evictor
///
/// This ensures upstream producers exit before downstream consumers,
/// and consumers can drain all in-flight work before the reactor stops.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.ReactorLifecycle")]
pub(crate) struct TaskGroup {
    pub(super) name: &'static str,
    handles: Vec<JoinHandle<RuntimeResult<()>>>,
}

impl TaskGroup {
    pub(crate) fn new(name: &'static str) -> Self {
        Self {
            name,
            handles: Vec::new(),
        }
    }

    pub(super) fn push(&mut self, handle: JoinHandle<RuntimeResult<()>>) {
        self.handles.push(handle);
    }

    /// Join all tasks in this group, returning the first error.
    ///
    /// Two-phase shutdown (warp-parse `wait_grace_down_with_timeout` pattern):
    ///
    /// - **Before `cancel` fires**: join handles with *no* time bound. Daemon
    ///   tasks are long-lived by design, so the timeout must not apply here —
    ///   the group watchers are spawned at startup and a deadline armed at
    ///   that point would abort every task a few seconds after boot.
    /// - **Once `cancel` fires**: the whole group shares one deadline
    ///   ([`GROUP_JOIN_TIMEOUT`]); a task that doesn't exit in time (ignored
    ///   cancellation / stuck on a non-cancellable await) is force-aborted so
    ///   shutdown can never hang. The shared deadline (not per-handle) keeps
    ///   the group's total join time bounded regardless of how many tasks it
    ///   holds.
    pub(super) async fn wait(self, cancel: CancellationToken) -> RuntimeResult<()> {
        // Armed only when shutdown is requested; shared by all remaining
        // handles from that moment on.
        let mut deadline: Option<Instant> = None;
        let mut first_error: Option<StructError<RuntimeReason>> = None;
        let mut handles = self.handles;
        let total = handles.len();
        let mut i = 0usize;
        // Phase 1 (no time bound) + phase 2a (grace window): join in order.
        // On grace expiry, abort ALL remaining handles at once — aborting one
        // at a time would serialize each task's unwind (sync work finishes
        // concurrently once every abort lands).
        while i < total {
            let joined = match deadline {
                Some(dl) => {
                    let remaining = dl.saturating_duration_since(Instant::now());
                    tokio::select! {
                        biased;
                        r = &mut handles[i] => Some(r),
                        _ = tokio::time::sleep(remaining) => None,
                    }
                }
                None => {
                    tokio::select! {
                        biased;
                        r = &mut handles[i] => Some(r),
                        _ = cancel.cancelled() => {
                            deadline = Some(Instant::now() + GROUP_JOIN_TIMEOUT);
                            continue;
                        }
                    }
                }
            };
            let Some(r) = joined else {
                log::warn!(
                    "task group {:?} join timed out after {:?}, aborting {} remaining task(s)",
                    self.name,
                    GROUP_JOIN_TIMEOUT,
                    total - i
                );
                break;
            };
            let result = r
                .map_err(|e| {
                    RuntimeReason::Shutdown
                        .to_err()
                        .with_detail(format!("task join error: {e}"))
                })
                .and_then(|inner| inner.source_err(RuntimeReason::Shutdown, "task failed"));
            if let Err(err) = result
                && first_error.is_none()
            {
                first_error = Some(err);
            }
            i += 1;
        }
        // Phase 2b: abort everything left, then confirm with a bounded wait
        // per handle. `abort()` only cancels at the task's next yield point —
        // a task chewing through a large batch in synchronous CPU work (or
        // blocked in a non-cancellable syscall) may not yield for a while.
        // Shutdown must not block on that: after the confirm budget the
        // handle is detached and the runtime's final drop reaps the task at
        // process exit.
        let aborted = total - i;
        for handle in &handles[i..] {
            handle.abort();
        }
        for handle in &mut handles[i..] {
            let _ = tokio::time::timeout(ABORT_CONFIRM_TIMEOUT, handle).await;
        }
        log::info!(
            "task group {:?} shutdown complete: tasks={} aborted={}",
            self.name,
            total,
            aborted
        );
        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn wait_aborts_task_that_never_exits() {
        // A task that yields forever and never returns. After shutdown is
        // requested the join layer must force-abort it after
        // GROUP_JOIN_TIMEOUT instead of hanging shutdown.
        let mut group = TaskGroup::new("test_hung");
        group.push(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }));
        let cancel = CancellationToken::new();
        let wait = tokio::spawn(group.wait(cancel.clone()));
        // Deterministic under the paused clock: let `wait()` complete its
        // first poll (parking on the plain join) BEFORE requesting shutdown.
        tokio::task::yield_now().await;
        cancel.cancel();
        // Let `wait()` observe the cancellation and arm the shared deadline.
        tokio::task::yield_now().await;
        // Advance the mock clock past the join timeout; this lets the join
        // layer's sleep branch fire, abort the hung task, and return Ok.
        tokio::time::advance(GROUP_JOIN_TIMEOUT + Duration::from_millis(50)).await;
        let result = wait.await.expect("wait task should finish after abort");
        assert!(result.is_ok(), "wait should force-abort and return Ok");
    }

    #[tokio::test(start_paused = true)]
    async fn wait_does_not_abort_before_shutdown() {
        // Regression: group watchers are spawned at startup. The join timeout
        // must NOT be armed before shutdown is requested — a long-running
        // task that outlives GROUP_JOIN_TIMEOUT must survive until it
        // completes naturally.
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = completed.clone();
        let mut group = TaskGroup::new("test_long_lived");
        group.push(tokio::spawn(async move {
            tokio::time::sleep(GROUP_JOIN_TIMEOUT * 3).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }));
        let cancel = CancellationToken::new();
        let wait = tokio::spawn(group.wait(cancel.clone()));
        tokio::task::yield_now().await;
        // Advance well past GROUP_JOIN_TIMEOUT with NO shutdown requested:
        // the task must still be alive (no premature abort).
        tokio::time::advance(GROUP_JOIN_TIMEOUT * 2).await;
        assert!(
            !completed.load(std::sync::atomic::Ordering::SeqCst),
            "task must not be aborted before shutdown is requested"
        );
        // Let it finish naturally (still no cancel) — wait returns Ok.
        tokio::time::advance(GROUP_JOIN_TIMEOUT * 2).await;
        let result = wait
            .await
            .expect("wait should finish on natural completion");
        assert!(result.is_ok());
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn wait_returns_when_all_tasks_complete() {
        let mut group = TaskGroup::new("test_fast");
        group.push(tokio::spawn(async { Ok(()) }));
        group.push(tokio::spawn(async { Ok(()) }));
        let result = group.wait(CancellationToken::new()).await;
        assert!(result.is_ok());
    }
}

// ---------------------------------------------------------------------------
// RunRule — one per compiled rule (construction interface)
// ---------------------------------------------------------------------------

#[allow(clippy::large_enum_variant)] // Match carries the compiled MatchPlan; boxing it would churn the hot path
#[derive(::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Runtime", module = "Runtime.ReactorLifecycle")]
pub(crate) enum RunRuleKind {
    Match {
        match_plan: MatchPlan,
        time_field: Option<String>,
        limits: Option<LimitsPlan>,
    },
    Each {
        alias: String,
        time_field: Option<String>,
    },
    Stats {
        stats_plan: StatsPlan,
        time_field: Option<String>,
    },
}

/// Pairs a rule execution kind with its [`RuleExecutor`] and precomputed
/// routing from stream names to CEP aliases.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.ReactorLifecycle")]
pub(crate) struct RunRule {
    pub kind: RunRuleKind,
    pub executor: RuleExecutor,
    /// `window_name → Vec<alias>` — which aliases should receive events from
    /// each bound window.
    pub window_aliases: HashMap<String, Vec<String>>,
}

// ---------------------------------------------------------------------------
// BootstrapData — compiled artifacts from config-loading phase
// ---------------------------------------------------------------------------

/// Compiled artifacts from the config-loading phase, ready for task spawning.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.ReactorLifecycle")]
pub(crate) struct BootstrapData {
    pub rules: Vec<RunRule>,
    pub router: std::sync::Arc<wf_engine::window::Router>,
    pub dispatcher: std::sync::Arc<wf_engine::sink::SinkDispatcher>,
    pub schema_count: usize,
    pub schemas: Vec<wf_lang::WindowSchema>,
    /// Compiled runtime window configs (from `config.windows` plus pipeline
    /// internal `|>` windows). Cached so `apply_reload` can use boot-time
    /// configs as the `current` side of the topology diff (L3).
    #[allow(dead_code)]
    pub window_configs: Vec<wf_config::WindowConfig>,
    pub intermediate_targets: HashSet<String>,
    pub external_runtime: Option<std::sync::Arc<crate::external::ExternalRuntime>>,
}
