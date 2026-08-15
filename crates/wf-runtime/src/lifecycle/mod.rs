mod bootstrap;
pub(crate) mod compile;
pub(crate) mod parse_pool;
mod reload;
mod signal;
mod spawn;
pub(crate) mod types;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use orion_error::conversion::ToStructError;
use orion_error::op_context;
use orion_error::prelude::*;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use wf_config::{FusionConfig, FusionReloadPlan, RawFusionConfigTree};
use wf_engine::window::Router;

use crate::error::{RuntimeReason, RuntimeResult};
use crate::metrics::{RuntimeMetrics, maybe_build_metrics};

// Re-export public API
pub use crate::hot_reload::{PreparedRuleReload, ReloadPreparation, prepare_reload};
pub use signal::{ShutdownTrigger, wait_for_signal};

// `ReloadOutcome`, `ReloadRequest`, and `RuntimeControlHandle` are defined as
// `pub` items in this module below; they are reachable from the crate root as
// `wf_runtime::lifecycle::*` with no extra re-export needed.

use bootstrap::load_and_compile;
use spawn::{
    spawn_alert_task, spawn_evictor_task, spawn_metrics_task, spawn_receiver_task, spawn_rule_tasks,
};
use types::TaskGroup;

fn mode_name(mode: wf_config::FusionMode) -> &'static str {
    match mode {
        wf_config::FusionMode::Daemon => "daemon",
        wf_config::FusionMode::Batch => "batch",
    }
}

// ---------------------------------------------------------------------------
// Reload outcome
// ---------------------------------------------------------------------------

/// Upper bound on how long [`Reactor::apply_reload`] will wait for old rule
/// tasks to drain & flush before giving up and spawning the new generation.
///
/// Why a timeout is mandatory: a rule task's `emit()` falls back to a
/// blocking `mpsc::send().await` when the alert channel is full, and that
/// send does **not** respond to cancellation. Under downstream backpressure
/// an old rule task's shutdown flush can therefore block forever, which
/// would hang the whole hot-swap. The timeout bounds this; on expiry the
/// stale task is left to finish (or be reaped) in the background while the
/// new rule tasks take over.
const DEFAULT_RELOAD_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Result of an [`Reactor::apply_reload`] attempt.
#[derive(::moju_derive::MoJu, Debug)]
#[moju(
    kind = "state",
    domain = "Orchestra",
    module = "Orchestra.ReactorLifecycle"
)]
pub enum ReloadOutcome {
    /// Reload was applied: old rule tasks were swapped for a freshly compiled
    /// generation sharing the existing windows/router/sinks.
    Applied(FusionReloadPlan),
    /// Reload was refused without touching the running tasks. The plan lists
    /// every change that requires a full restart.
    Blocked(FusionReloadPlan),
}

/// Capacity of the reload control channel. Reload is a low-frequency,
/// operator-driven operation, so a tiny buffer is plenty; excess concurrent
/// requests simply queue and are serviced strictly in order by the Reactor's
/// control loop (giving the serialisation guarantee validated in P1 tests).
const RELOAD_CONTROL_CHANNEL_CAPACITY: usize = 8;

/// Process exit code used when the engine requests a full restart (L4).
/// A supervisor (systemd / docker / shell script) should interpret this as
/// "re-launch the same binary with the same arguments".
pub const RESTART_EXIT_CODE: i32 = 75;

/// Outcome of [`Reactor::run`], indicating why the control loop exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// Normal shutdown (SIGINT / SIGTERM / all handles dropped).
    Normal,
    /// A [`RuntimeControlHandle::request_restart`] call was received. The
    /// caller should exit with [`RESTART_EXIT_CODE`] so a supervisor can
    /// re-launch the process.
    RestartRequested,
}

/// A reload request sent over the control channel to the running Reactor.
///
/// The `reply` oneshot carries both the [`ReloadOutcome`] (on success) and any
/// `RuntimeResult` error (e.g. a config compile failure), so the caller can
/// distinguish *blocked* reloads (`Ok(Blocked)`) from *failed* reloads (`Err`).
#[derive(Debug)]
pub enum ReloadRequest {
    /// Reload the rule set from the given (raw + effective) config.
    Reload {
        raw: RawFusionConfigTree,
        config: Box<FusionConfig>,
        reply: oneshot::Sender<RuntimeResult<ReloadOutcome>>,
    },
    /// Request a graceful shutdown + restart (L4 full reload). The Reactor
    /// will cancel all tasks, drain, and `run()` will return
    /// `Ok(RunOutcome::RestartRequested)`.
    Restart {
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
}

/// Handle to a running [`Reactor`], clonable and safe to share across tasks
/// (e.g. with an admin HTTP server). Calls are forwarded to the Reactor's
/// single-threaded control loop over an mpsc channel, so reload requests are
/// inherently **serialised** — no two reloads run concurrently.
///
/// Also exposes the root [`CancellationToken`] (for the existing `status`
/// route's `accepting` field) without letting the holder cancel the engine.
#[derive(Clone)]
pub struct RuntimeControlHandle {
    tx: mpsc::Sender<ReloadRequest>,
    cancel: CancellationToken,
}

impl RuntimeControlHandle {
    /// Request a hot reload with the given config. Awaits the Reactor's reply.
    ///
    /// Returns `Err` only if the Reactor has shut down (channel closed) or if
    /// reload preparation itself failed (e.g. config compile error); a
    /// topology-blocked reload returns `Ok(ReloadOutcome::Blocked(..))`.
    pub async fn apply_reload(
        &self,
        raw: RawFusionConfigTree,
        config: FusionConfig,
    ) -> RuntimeResult<ReloadOutcome> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ReloadRequest::Reload {
                raw,
                config: Box::new(config),
                reply: reply_tx,
            })
            .await
            .map_err(|_| {
                RuntimeReason::Shutdown
                    .to_err()
                    .with_detail("reactor control channel closed — engine has shut down")
            })?;
        reply_rx.await.map_err(|_| {
            RuntimeReason::Shutdown
                .to_err()
                .with_detail("reactor dropped the reload reply — engine shutting down")
        })?
    }

    /// Clone of the root cancellation token. While not cancelled the engine is
    /// accepting input; the admin `status` route reads `is_cancelled()` to
    /// report `accepting`.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Request a graceful shutdown + restart (L4 full reload). Returns
    /// `Ok(())` once the Reactor has acknowledged the request; the actual
    /// shutdown + exit will follow asynchronously (the admin API can still
    /// respond to the HTTP caller before the process exits).
    pub async fn request_restart(&self) -> RuntimeResult<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ReloadRequest::Restart { reply: reply_tx })
            .await
            .map_err(|_| {
                RuntimeReason::Shutdown
                    .to_err()
                    .with_detail("reactor control channel closed — engine has shut down")
            })?;
        reply_rx.await.map_err(|_| {
            RuntimeReason::Shutdown
                .to_err()
                .with_detail("reactor dropped the restart reply — engine shutting down")
        })??;
        Ok(())
    }
}

impl RuntimeControlHandle {
    /// Construct a handle from its parts.
    ///
    /// Intended for tests and embedders that drive their own control loop.
    /// Normal callers obtain a handle via [`Reactor::control_handle`].
    pub fn new(tx: mpsc::Sender<ReloadRequest>, cancel: CancellationToken) -> Self {
        Self { tx, cancel }
    }
}

// ---------------------------------------------------------------------------
// Reactor — the top-level lifecycle handle
// ---------------------------------------------------------------------------

/// Manages the full lifecycle of the CEP runtime: bootstrap, run, graceful
/// shutdown, and (rule-only) hot reload.
///
/// Task groups are stored in start order and joined in reverse (LIFO)
/// during [`wait`](Self::wait), ensuring correct drain sequencing:
/// receiver stops first, then rule tasks drain and flush, then alert
/// sink flushes to disk, and finally background tasks stop.
///
/// The rule group is tracked separately from the other watchers so it can be
/// hot-swapped by [`apply_reload`](Self::apply_reload) without restarting the
/// receiver, alert sink, evictor, or metrics tasks. CEP window state lives in
/// the shared `Arc<Router>`/registry, so swapping rule tasks does not lose
/// in-flight window data.
#[derive(::moju_derive::MoJu)]
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.ReactorLifecycle"
)]
pub struct Reactor {
    pub(crate) cancel: CancellationToken,
    /// Dedicated token for rule tasks only. A child of `cancel`, so a root
    /// shutdown still propagates to rules; cancelling it in isolation
    /// (during reload) stops just the rule tasks.
    pub(crate) rule_cancel: CancellationToken,
    /// Non-reloadable groups in start order: `[alert, evictor]`.
    head_watchers: Vec<JoinHandle<RuntimeResult<()>>>,
    /// Non-reloadable groups in start order: `[receiver, metrics]`.
    tail_watchers: Vec<JoinHandle<RuntimeResult<()>>>,
    /// Rule group supervisor handle — hot-swappable.
    pub(crate) rule_watch: JoinHandle<RuntimeResult<()>>,
    /// Stale rule-generation supervisors whose drain timed out during a prior
    /// reload. Each is `abort()`-ed at detach time (so it releases its
    /// `alert_tx` clone promptly) and then awaited (`reap`-ed) at the next
    /// reload or at `wait()` to reclaim the task. Bounded to at most one entry
    /// between reloads because the previous one is reaped at the start of each
    /// swap.
    pub(crate) detached_rule_watchers: Vec<JoinHandle<RuntimeResult<()>>>,
    /// Shared artifacts reused across rule generations.
    pub(crate) router: Arc<Router>,
    pub(crate) sink_fanout: Option<Arc<crate::alert_task::SinkFanout>>,
    pub(crate) metrics: Option<Arc<RuntimeMetrics>>,
    pub(crate) intermediate_targets: HashSet<String>,
    /// EOS sender shared with rule generations (reload keeps it).
    pub(crate) eos_tx: watch::Sender<u64>,
    /// Reload baseline: the raw + effective config currently running, plus the
    /// base dir used to resolve rule/schema files.
    pub(crate) current_raw: RawFusionConfigTree,
    pub(crate) current_config: FusionConfig,
    pub(crate) base_dir: PathBuf,
    pub(crate) reload_drain_timeout: Duration,
    /// Inbound reload requests, drained by [`run`](Self::run). The sending half
    /// is handed out via [`control_handle`](Self::control_handle).
    control_rx: mpsc::Receiver<ReloadRequest>,
    /// Sender half kept so [`control_handle`] can clone it on demand.
    control_tx: mpsc::Sender<ReloadRequest>,
    #[allow(dead_code)]
    _external_runtime: Option<std::sync::Arc<crate::external::ExternalRuntime>>,
}

impl Reactor {
    /// Bootstrap the entire runtime from a [`FusionConfig`] (and its raw tree)
    /// and a base directory (for resolving relative `.wfs` / `.wfl` paths).
    ///
    /// The raw config tree is retained as the reload baseline so that later
    /// [`apply_reload`](Self::apply_reload) calls can diff against it.
    #[tracing::instrument(name = "engine.start", skip_all, fields(mode = %mode_name(config.mode)))]
    pub async fn start(
        config: FusionConfig,
        raw: RawFusionConfigTree,
        base_dir: &std::path::Path,
    ) -> RuntimeResult<Self> {
        let mut op = op_context!("engine-bootstrap").with_auto_log();
        op.record("mode", mode_name(config.mode));
        op.record("base_dir", base_dir.display().to_string().as_str());

        let cancel = CancellationToken::new();
        // Child of root: cancelling the root (shutdown) propagates to rules,
        // while cancelling `rule_cancel` alone (reload) stops only rules.
        let rule_cancel = cancel.child_token();

        // Phase 1: Load config & compile rules + build sink dispatcher
        let data = load_and_compile(&config, base_dir).await?;
        wf_info!(
            sys,
            schemas = data.schema_count,
            rules = data.rules.len(),
            "engine bootstrap complete"
        );

        let rule_names: Vec<String> = data
            .rules
            .iter()
            .map(|rule| rule.executor.plan().name.clone())
            .collect();
        let window_names: Vec<String> = data.router.registry().window_names();
        let source_names: Vec<String> = config
            .sources
            .iter()
            .enumerate()
            .map(|(i, s)| s.effective_name(i))
            .collect();
        let source_types: BTreeMap<String, String> = config
            .sources
            .iter()
            .enumerate()
            .filter(|(_, s)| s.enabled)
            .map(|(i, s)| (s.effective_name(i), s.kind().to_string()))
            .collect();
        let metrics = maybe_build_metrics(
            &config.metrics,
            &rule_names,
            &window_names,
            &source_names,
            source_types,
        );

        // Phase 2: Spawn task groups.
        //   head (start order): alert → evictor
        //   rule:  rules            (hot-swappable, tracked separately)
        //   tail (start order): receiver → metrics
        let mut head_watchers: Vec<JoinHandle<RuntimeResult<()>>> = Vec::with_capacity(2);
        let mut tail_watchers: Vec<JoinHandle<RuntimeResult<()>>> = Vec::with_capacity(2);

        let (sink_fanout, alert_group) =
            spawn_alert_task(data.dispatcher.clone(), metrics.clone(), cancel.clone());
        head_watchers.push(watch_group(alert_group, cancel.clone()));

        head_watchers.push(watch_group(
            spawn_evictor_task(&config, &data.router, cancel.child_token(), metrics.clone()),
            cancel.clone(),
        ));

        // End-of-stream counter shared with the rule tasks: incremented each
        // time the input sources report the stream ended (EOS-driven
        // finalization). Rules flush trailing instances on every EOS but keep
        // running.
        let (eos_tx, _) = tokio::sync::watch::channel(0u64);

        // Pipe registry: every rule's yield target (output / `|>` intermediate)
        // is a pipe (pipe design, P1b). Built from the compiled rule plans so the
        // rule task can route emits through the pipe abstraction.
        let plans: Vec<_> = data.rules.iter().map(|r| r.executor.plan()).collect();
        let pipe_registry = bootstrap::build_pipe_registry(&plans, &data.schemas);

        let rule_group = spawn_rule_tasks(
            data.rules,
            &data.router,
            &data.intermediate_targets,
            pipe_registry,
            sink_fanout.clone(),
            rule_cancel.clone(),
            metrics.clone(),
            eos_tx.clone(),
            config.runtime.rule_parallelism,
        );
        let rule_watch = watch_group(rule_group, cancel.clone());

        let receiver_group = spawn_receiver_task(
            &config,
            data.router.clone(),
            cancel.clone(),
            metrics.clone(),
            &data.schemas,
            base_dir,
        )
        .await?;
        tail_watchers.push(watch_receiver_group(
            receiver_group,
            cancel.clone(),
            config.mode == wf_config::FusionMode::Batch,
            eos_tx.clone(),
        ));
        tail_watchers.push(watch_group(
            spawn_metrics_task(
                &config,
                &data.router,
                cancel.child_token(),
                metrics.clone(),
                Some(data.dispatcher.clone()),
            )
            .await?,
            cancel.clone(),
        ));

        op.mark_suc();
        // Reload control channel: the receiver lives on the Reactor (drained by
        // `run`); the sender is handed out via `control_handle`.
        let (control_tx, control_rx) = mpsc::channel(RELOAD_CONTROL_CHANNEL_CAPACITY);
        Ok(Self {
            cancel,
            rule_cancel,
            head_watchers,
            tail_watchers,
            rule_watch,
            detached_rule_watchers: Vec::new(),
            router: data.router,
            sink_fanout: Some(sink_fanout),
            metrics,
            intermediate_targets: data.intermediate_targets,
            eos_tx,
            current_raw: raw,
            current_config: config,
            base_dir: base_dir.to_path_buf(),
            reload_drain_timeout: DEFAULT_RELOAD_DRAIN_TIMEOUT,
            control_rx,
            control_tx,
            _external_runtime: data.external_runtime,
        })
    }

    /// Return a clonable handle that lets other tasks request reloads (and read
    /// the root cancel token for the `accepting` status). Safe to call before
    /// or while [`run`](Self::run) is driving the control loop.
    pub fn control_handle(&self) -> RuntimeControlHandle {
        RuntimeControlHandle {
            tx: self.control_tx.clone(),
            cancel: self.cancel.clone(),
        }
    }

    /// Drive the reactor until shutdown: serialise inbound reload requests,
    /// then drain & join all task groups.
    ///
    /// This is the self-driven replacement for the old
    /// `wait_for_signal(cancel); reactor.shutdown(); reactor.wait()` sequence.
    /// A background task watches OS signals (SIGINT/SIGTERM) and cancels the
    /// root token; the loop here also exits on `cancel.cancelled()` (covering
    /// internal shutdown via [`shutdown`](Self::shutdown)). Reload requests are
    /// serviced one at a time — a slow reload simply queues later ones.
    ///
    /// After the loop exits, [`wait`](Self::wait) performs the LIFO task drain.
    ///
    /// Returns [`RunOutcome::RestartRequested`] when a restart was requested
    /// via [`RuntimeControlHandle::request_restart`].
    pub async fn run(mut self) -> RuntimeResult<RunOutcome> {
        // Signal watcher: on SIGINT/SIGTERM (or an internal cancel) it cancels
        // the root token, which breaks the loop below. Detached so it lives
        // only as long as needed; `wait_for_signal` already handles registration.
        let sig_cancel = self.cancel.clone();
        let signal_task = tokio::spawn(async move {
            let _trigger = wait_for_signal(sig_cancel).await;
            // wait_for_signal cancels the token itself on signal; nothing else
            // to do here. `_trigger` is dropped.
        });

        let mut restart_requested = false;

        loop {
            tokio::select! {
                biased;
                // Shutdown requested (signal or internal). Stop servicing.
                _ = self.cancel.cancelled() => {
                    wf_info!(sys, "reactor control loop exiting: shutdown requested");
                    break;
                }
                req = self.control_rx.recv() => match req {
                    Some(ReloadRequest::Reload { raw, config, reply }) => {
                        // Mark the reply consumed regardless of outcome: if the
                        // caller hung up we still run the reload (best effort).
                        let outcome = self.apply_reload(raw, *config).await;
                        if reply.send(outcome).is_err() {
                            wf_warn!(
                                sys,
                                "reload caller dropped the reply before completion"
                            );
                        }
                    }
                    Some(ReloadRequest::Restart { reply }) => {
                        wf_info!(
                            sys,
                            "reactor control loop exiting: restart requested"
                        );
                        // Acknowledge the restart request, then break out of
                        // the control loop. The caller already knows a restart
                        // is coming and can respond to the HTTP client before
                        // `wait()` blocks on drain.
                        let _ = reply.send(Ok(()));
                        restart_requested = true;
                        break;
                    }
                    None => {
                        // All control handles dropped — no one can request a
                        // reload anymore. Shut down (typical only at end of life).
                        wf_info!(
                            sys,
                            "reactor control loop exiting: all control handles dropped"
                        );
                        break;
                    }
                }
            }
        }

        // Ensure the engine is cancelled (idempotent if a signal already did),
        // then reap the signal watcher task and join everything.
        self.cancel.cancel();
        signal_task.abort();
        self.wait().await?;
        wf_info!(sys, "reactor shutdown complete: all task groups joined");

        if restart_requested {
            Ok(RunOutcome::RestartRequested)
        } else {
            Ok(RunOutcome::Normal)
        }
    }

    /// Request graceful shutdown of all tasks.
    pub fn shutdown(&self) {
        wf_info!(sys, "initiating graceful shutdown");
        self.cancel.cancel();
    }

    /// Wait for all task groups to complete after shutdown.
    ///
    /// Groups are joined in reverse start order (LIFO): tail (`metrics`,
    /// `receiver`) → `rules` → head (`evictor`, `alert`), preserving the
    /// original drain sequencing (receiver stops, then rules drain & flush,
    /// then the alert sink flushes to disk last).
    pub async fn wait(mut self) -> RuntimeResult<()> {
        let mut first_error: Option<StructError<RuntimeReason>> = None;

        // Drop the Reactor's own alert sender first. The alert consumer task
        // only exits once *every* sender is gone; retaining it here (added for
        // reload) would keep the channel open and deadlock `wait` on the alert
        // supervisor. By shutdown time reload is finished, so this sender is no
        // longer needed. The rule tasks (joined below, before head/alert) still
        // hold their own clones, so they can finish flushing before the channel
        // closes and the alert task drains & exits last.
        self.sink_fanout.take();

        // tail: metrics → receiver, then rule, then head: evictor → alert.
        while let Some(handle) = self.tail_watchers.pop() {
            if let Err(err) = join_supervisor(handle).await
                && first_error.is_none()
            {
                first_error = Some(err);
            }
        }
        let rule_watch = std::mem::replace(&mut self.rule_watch, tokio::spawn(async { Ok(()) }));
        if let Err(err) = join_supervisor(rule_watch).await
            && first_error.is_none()
        {
            first_error = Some(err);
        }
        // Reap any detached stale generations before joining head (alert). They
        // were abort()-ed at detach time, so they only need the alert task
        // (still running here, joined last as part of head) to drain the
        // channel and unblock a lingering blocking `send().await` — after which
        // they release their `alert_tx` clones and the alert channel can close.
        self.reap_detached_rule_watchers().await;
        while let Some(handle) = self.head_watchers.pop() {
            if let Err(err) = join_supervisor(handle).await
                && first_error.is_none()
            {
                first_error = Some(err);
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }

    /// Returns a clone of the root cancellation token (for signal integration).
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

/// Join a task-group supervisor handle, wrapping both join failures and the
/// supervisor's own errors as `Shutdown`-reasoned errors (mirrors the original
/// inline `wait` semantics).
async fn join_supervisor(handle: JoinHandle<RuntimeResult<()>>) -> RuntimeResult<()> {
    let result = handle.await.map_err(|e| {
        RuntimeReason::Shutdown
            .to_err()
            .with_detail(format!("supervisor join error: {e}"))
    })?;
    result.source_err(RuntimeReason::Shutdown, "supervisor failed")?;
    Ok(())
}

fn watch_group(group: TaskGroup, cancel: CancellationToken) -> JoinHandle<RuntimeResult<()>> {
    let name = group.name;
    tokio::spawn(async move {
        wf_debug!(sys, task_group = name, "watching task group");
        let result = group.wait(cancel.clone()).await;
        if result.is_err() && !cancel.is_cancelled() {
            cancel.cancel();
        }
        result?;
        wf_debug!(sys, task_group = name, "task group finished");
        Ok(())
    })
}

fn watch_receiver_group(
    receiver_group: TaskGroup,
    cancel: CancellationToken,
    auto_shutdown: bool,
    eos_tx: watch::Sender<u64>,
) -> JoinHandle<RuntimeResult<()>> {
    let name = receiver_group.name;
    tokio::spawn(async move {
        wf_debug!(sys, task_group = name, "watching task group");
        let result = receiver_group.wait(cancel.clone()).await;
        if result.is_ok() {
            // EOS-driven finalization: input sources reported the stream ended.
            // Rules flush their trailing instances but keep running (a daemon
            // can accept a subsequent finite input). The counter increments per
            // EOS so multiple finite inputs each trigger a flush.
            wf_info!(sys, task_group = name, "receiver completed; signaling EOS flush");
            let n = *eos_tx.borrow();
            let _ = eos_tx.send(n + 1);
        }
        if result.is_err() && !cancel.is_cancelled() {
            cancel.cancel();
        } else if auto_shutdown && result.is_ok() && !cancel.is_cancelled() {
            wf_info!(
                sys,
                task_group = name,
                "batch receiver completed; initiating automatic shutdown"
            );
            cancel.cancel();
        }
        result?;
        wf_debug!(sys, task_group = name, "task group finished");
        Ok(())
    })
}
