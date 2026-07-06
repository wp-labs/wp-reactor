mod bootstrap;
pub(crate) mod compile;
mod signal;
mod spawn;
pub(crate) mod types;

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use orion_error::conversion::ToStructError;
use orion_error::op_context;
use orion_error::prelude::*;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use wf_config::{FusionConfig, FusionReloadPlan, RawFusionConfigTree};
use wf_engine::alert::OutputRecord;
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
    cancel: CancellationToken,
    /// Dedicated token for rule tasks only. A child of `cancel`, so a root
    /// shutdown still propagates to rules; cancelling it in isolation
    /// (during reload) stops just the rule tasks.
    rule_cancel: CancellationToken,
    /// Non-reloadable groups in start order: `[alert, evictor]`.
    head_watchers: Vec<JoinHandle<RuntimeResult<()>>>,
    /// Non-reloadable groups in start order: `[receiver, metrics]`.
    tail_watchers: Vec<JoinHandle<RuntimeResult<()>>>,
    /// Rule group supervisor handle — hot-swappable.
    rule_watch: JoinHandle<RuntimeResult<()>>,
    /// Stale rule-generation supervisors whose drain timed out during a prior
    /// reload. Each is `abort()`-ed at detach time (so it releases its
    /// `alert_tx` clone promptly) and then awaited (`reap`-ed) at the next
    /// reload or at `wait()` to reclaim the task. Bounded to at most one entry
    /// between reloads because the previous one is reaped at the start of each
    /// swap.
    detached_rule_watchers: Vec<JoinHandle<RuntimeResult<()>>>,
    /// Shared artifacts reused across rule generations.
    router: Arc<Router>,
    alert_tx: Option<mpsc::Sender<OutputRecord>>,
    metrics: Option<Arc<RuntimeMetrics>>,
    intermediate_targets: HashSet<String>,
    /// Reload baseline: the raw + effective config currently running, plus the
    /// base dir used to resolve rule/schema files.
    current_raw: RawFusionConfigTree,
    current_config: FusionConfig,
    base_dir: PathBuf,
    reload_drain_timeout: Duration,
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

        let (alert_tx, alert_group) = spawn_alert_task(data.dispatcher.clone(), metrics.clone());
        head_watchers.push(watch_group(alert_group, cancel.clone()));

        head_watchers.push(watch_group(
            spawn_evictor_task(&config, &data.router, cancel.child_token(), metrics.clone()),
            cancel.clone(),
        ));

        let rule_group = spawn_rule_tasks(
            data.rules,
            &data.router,
            &data.intermediate_targets,
            alert_tx.clone(),
            rule_cancel.clone(),
            metrics.clone(),
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
            rule_cancel.clone(),
            config.mode == wf_config::FusionMode::Batch,
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
            alert_tx: Some(alert_tx),
            metrics,
            intermediate_targets: data.intermediate_targets,
            current_raw: raw,
            current_config: config,
            base_dir: base_dir.to_path_buf(),
            reload_drain_timeout: DEFAULT_RELOAD_DRAIN_TIMEOUT,
            control_rx,
            control_tx,
            _external_runtime: data.external_runtime,
        })
    }

    /// Hot-reload the rule set from a new (raw + effective) config.
    ///
    /// Only rule-internal logic changes are eligible: if the reload would alter
    /// the window/schema topology or any restart-required setting, it is
    /// refused as [`ReloadOutcome::Blocked`] and the running tasks are left
    /// untouched. On success the old rule tasks are drained (bounded by
    /// `reload_drain_timeout`) and replaced by a fresh generation that shares
    /// the existing windows/router/sinks — so CEP window state is preserved.
    ///
    /// See `docs/design/admin_api_reload_design.md` §4 for the full rationale,
    /// including why the drain is bounded by a timeout.
    pub async fn apply_reload(
        &mut self,
        next_raw: RawFusionConfigTree,
        next_config: FusionConfig,
    ) -> RuntimeResult<ReloadOutcome> {
        let prep = prepare_reload(
            &self.current_raw,
            &self.current_config,
            next_raw,
            next_config,
            &self.base_dir,
        )?;
        match prep {
            ReloadPreparation::Blocked(plan) => {
                wf_info!(
                    sys,
                    blockers = plan.requires_restart.len(),
                    "reload blocked — requires restart"
                );
                Ok(ReloadOutcome::Blocked(plan))
            }
            ReloadPreparation::Ready(ready) => {
                let next_rules = ready.next_rules;
                let next_intermediate_targets = ready.next_intermediate_targets.clone();
                self.swap_rule_tasks(next_rules, next_intermediate_targets)
                    .await?;
                // Advance the reload baseline to what is now running.
                self.current_raw = ready.next_raw;
                self.current_config = ready.next_config;
                self.intermediate_targets = ready.next_intermediate_targets;
                wf_info!(sys, "reload applied — rule generation swapped");
                Ok(ReloadOutcome::Applied(ready.plan))
            }
        }
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

        if restart_requested {
            Ok(RunOutcome::RestartRequested)
        } else {
            Ok(RunOutcome::Normal)
        }
    }

    /// Cancel the current rule generation, bound its drain by
    /// `reload_drain_timeout`, then spawn the next generation sharing the
    /// existing shared artifacts.
    ///
    /// On drain timeout the stale supervisor is `abort()`-ed and retained in
    /// `detached_rule_watchers` (rather than blocking the reload forever).
    /// Aborting forces the task to drop — including its `alert_tx` clone —
    /// even if it is stuck in `emit()`'s non-cancellable blocking
    /// `mpsc::send().await` under alert-channel backpressure. The handle is
    /// then reaped at the start of the next swap or in `wait()`, bounding the
    /// leak to at most one stale generation. See
    /// [`DEFAULT_RELOAD_DRAIN_TIMEOUT`] for why the wait is bounded.
    async fn swap_rule_tasks(
        &mut self,
        new_rules: Vec<types::RunRule>,
        new_intermediate_targets: HashSet<String>,
    ) -> RuntimeResult<()> {
        // (0) Reap any stale generation detached by a *previous* timed-out
        //     reload. Those handles were abort()-ed at detach time, so they
        //     resolve promptly here (the alert task is still running and drains
        //     the channel, unblocking any lingering blocking send).
        self.reap_detached_rule_watchers().await;

        // (a) Signal only the rule tasks to shut down (drain + flush).
        self.rule_cancel.cancel();

        // (b) Bound the wait for the old rule supervisor. `emit()`'s blocking
        //     send does not honour cancellation, so an unbounded await could
        //     hang under alert-channel backpressure. We use `select!` (not
        //     `tokio::time::timeout`) so that on expiry we *retain* the
        //     `JoinHandle` — `timeout` would consume and drop it, leaking the
        //     task *and* its `alert_tx` clone (which would later hang
        //     `wait()`).
        let old_rule_watch = std::mem::replace(
            &mut self.rule_watch,
            // Placeholder until the new generation is spawned below.
            tokio::spawn(async { Ok(()) }),
        );
        let mut old_rule_watch = old_rule_watch;
        tokio::select! {
            biased;
            joined = &mut old_rule_watch => {
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        wf_warn!(
                            sys,
                            error = %err.render(),
                            "old rule generation reported an error during reload drain"
                        );
                    }
                    Err(join_err) => {
                        wf_warn!(
                            sys,
                            error = %join_err,
                            "old rule generation join failed during reload drain"
                        );
                    }
                }
            }
            _ = tokio::time::sleep(self.reload_drain_timeout) => {
                // Drain timed out: the old supervisor is still pending. abort()
                // it so its task (and any rule task stuck in a blocking
                // `send().await`) is dropped, releasing its `alert_tx` clone —
                // otherwise it would keep the alert channel open and hang a
                // future `wait()`. Retain the handle so it can be reaped; it
                // resolves once the (still-running) alert task drains the
                // channel and unblocks the send.
                old_rule_watch.abort();
                self.detached_rule_watchers.push(old_rule_watch);
                wf_warn!(
                    sys,
                    timeout_secs = self.reload_drain_timeout.as_secs(),
                    detached = self.detached_rule_watchers.len(),
                    "reload drain timed out; aborted and detached old rule generation"
                );
            }
        }

        // (c) Fresh token for the new generation (still a child of the root,
        //     so a later root shutdown still propagates).
        self.rule_cancel = self.cancel.child_token();

        // (d) Spawn the new rule generation, reusing the shared
        //     router/alert_tx/metrics so window state is preserved.
        let group = spawn_rule_tasks(
            new_rules,
            &self.router,
            &new_intermediate_targets,
            self.alert_tx.clone().unwrap_or_else(|| {
                // Reactor is shutting down (alert_tx already taken in `wait`).
                // Create a closed channel so the new rule generation's emits
                // are dropped rather than blocking a real reload.
                let (_tx, rx) = mpsc::channel::<OutputRecord>(1);
                drop(rx);
                _tx
            }),
            self.rule_cancel.clone(),
            self.metrics.clone(),
        );
        self.rule_watch = watch_group(group, self.cancel.clone());
        Ok(())
    }

    /// Reap all detached rule supervisors: each was `abort()`-ed at detach
    /// time, so `await`-ing them here just reclaims the task. Errors are
    /// logged (a stale generation failing after detach must not abort the
    /// reload or the shutdown). The alert consumer is assumed to still be
    /// running so it can drain the channel and unblock any detached rule task
    /// that was stuck in a blocking `send().await`.
    async fn reap_detached_rule_watchers(&mut self) {
        while let Some(handle) = self.detached_rule_watchers.pop() {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    wf_warn!(
                        sys,
                        error = %err.render(),
                        "detached rule generation reported an error during reap"
                    );
                }
                Err(join_err) if join_err.is_cancelled() => {
                    // Expected: this is the abort() we issued at detach time.
                }
                Err(join_err) => {
                    wf_warn!(
                        sys,
                        error = %join_err,
                        "detached rule generation join failed during reap"
                    );
                }
            }
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
        self.alert_tx.take();

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
        let result = group.wait().await;
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
    rule_cancel: CancellationToken,
    auto_shutdown: bool,
) -> JoinHandle<RuntimeResult<()>> {
    let name = receiver_group.name;
    tokio::spawn(async move {
        wf_debug!(sys, task_group = name, "watching task group");
        let result = receiver_group.wait().await;
        rule_cancel.cancel();
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

#[cfg(test)]
mod reload_tests {
    use std::path::{Path, PathBuf};

    use wf_config::{ConfigVarContext, FusionConfigLoader};

    use super::*;

    fn make_temp_dir(name: &str) -> PathBuf {
        let unique = format!(
            "wf-runtime-reactor-{}-{}-{}",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    fn write_file(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("failed to create parent dir");
        }
        std::fs::write(path, content).expect("failed to write test file");
    }

    /// Minimal `wfusion.toml` pointing at `schemas/` + `rules/`, using a **file**
    /// source over an empty seed file. The file source reads to EOF and
    /// completes immediately, but in daemon mode (no auto-shutdown) the
    /// reactor stays up — so we can exercise `apply_reload` while it runs,
    /// and `wait()` returns cleanly on shutdown because the receiver task is
    /// already finished. (A TCP source would block the daemon `wait()` since
    /// its accept loop is not cancellation-aware.)
    fn fusion_toml(schemas: &str, rules: &str) -> String {
        format!(
            r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "file"
name = "seed"
path = "seed.ndjson"
stream = "syslog"
data_format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "{schemas}"
rules = "{rules}"

[vars]
FAIL_THRESHOLD = "3"
"#
        )
    }

    const SECURITY_SCHEMA: &str = r#"
window auth_events {
    stream = "syslog"
    time = event_time
    over = 5m

    fields {
        sip: ip
        username: chars
        action: chars
        event_time: time
    }
}

window security_alerts {
    over = 0
    fields {
        sip: ip
        fail_count: digit
        message: chars
    }
}
"#;

    const BRUTE_FORCE_RULE: &str = r#"
rule brute_force_then_scan {
  events {
    fail : auth_events && action == "failed"
  }

  match<sip:5m> {
    on event {
      fail | count >= ${FAIL_THRESHOLD:3};
    }
    and close {
      fail | count >= 1;
    }
  } -> score(70.0)

  entity(ip, fail.sip)

  yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
    message = fmt("{} brute force detected", fail.sip)
  )
}
"#;

    /// A *rule-only* change: same name, same topology, different threshold
    /// logic (score 99 instead of 70). `prepare_reload` classifies this as
    /// `Ready` because neither the schema set nor the window layout changes.
    const BRUTE_FORCE_RULE_V2: &str = r#"
rule brute_force_then_scan {
  events {
    fail : auth_events && action == "failed"
  }

  match<sip:5m> {
    on event {
      fail | count >= ${FAIL_THRESHOLD:3};
    }
    and close {
      fail | count >= 1;
    }
  } -> score(99.0)

  entity(ip, fail.sip)

  yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
    message = fmt("{} brute force detected", fail.sip)
  )
}
"#;

    /// Sink layout: one catch-all file group routed to every window. Without a
    /// real sink the bootstrap guard (`no sinks configured`) rejects startup.
    fn write_sink_layout(root: &Path) {
        write_file(
            &root.join("connectors/sink.d/file_json.toml"),
            r#"
[[connectors]]
id = "file_json"
type = "file"
allow_override = ["file"]

[connectors.params]
fmt = "json"
file = "default.jsonl"
"#,
        );
        write_file(&root.join("sinks/defaults.toml"), "tags = [\"env:dev\"]\n");
        write_file(
            &root.join("sinks/business.d/catch_all.toml"),
            r#"
[sink_group]
name = "catch_all"
windows = ["*"]

[[sink_group.sinks]]
connect = "file_json"
name = "all_alerts"

[sink_group.sinks.params]
file = "all.jsonl"
"#,
        );
    }

    /// Write the standard windows.toml referenced by `fusion_toml`.
    fn write_window_config(root: &Path) {
        write_file(
            &root.join("models/windows.toml"),
            r#"[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.auth_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"

[window.security_alerts]
mode = "local"
max_window_bytes = "64MB"
over_cap = "1h"
"#,
        );
    }

    /// Build a runnable reactor fixture: wfusion.toml + schema + one rule +
    /// sinks. Returns the dir and the loaded (raw, config) baseline.
    async fn bootstrap_reactor(rule: &'static str) -> (PathBuf, Reactor) {
        let root = make_temp_dir("reactor");
        write_file(
            &root.join("wfusion.toml"),
            &fusion_toml("schemas/*.wfs", "rules/*.wfl"),
        );
        write_file(&root.join("schemas/security.wfs"), SECURITY_SCHEMA);
        write_file(&root.join("rules/brute_force.wfl"), rule);
        // Empty seed file: file source reads EOF immediately and completes.
        write_file(&root.join("seed.ndjson"), "");
        write_sink_layout(&root);
        write_window_config(&root);

        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let raw = loader.load_raw().expect("load raw");
        let config = loader.load().expect("load config");
        let reactor = Reactor::start(config, raw, &root)
            .await
            .expect("reactor start");
        (root, reactor)
    }

    #[tokio::test]
    async fn apply_reload_swaps_rules_when_topology_unchanged() {
        let (root, mut reactor) = bootstrap_reactor(BRUTE_FORCE_RULE).await;

        // Reload with the v2 rule (score 99). Same schema/window topology → Ready.
        // prepare_reload recompiles from disk, so write the new rule first.
        write_file(&root.join("rules/brute_force.wfl"), BRUTE_FORCE_RULE_V2);
        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let next_raw = loader.load_raw().expect("load next raw");
        let next_config = loader.load().expect("load next config");

        match reactor.apply_reload(next_raw, next_config).await {
            Ok(ReloadOutcome::Applied(_plan)) => {
                // Swap completed and the reactor remains servable.
            }
            other => panic!("expected Applied, got {other:?}"),
        }

        reactor.shutdown();
        reactor.wait().await.expect("clean shutdown after reload");

        let _ = std::fs::remove_dir_all(root);
    }

    /// Regression for M1: when the old rule generation cannot drain within the
    /// bound (here simulated by a 1ms drain timeout — too short for any real
    /// drain), `swap_rule_tasks` must abort+detach the stale supervisor rather
    /// than hang, and a subsequent `wait()` must still terminate (the detached
    /// task's `alert_tx` clone is released via `abort()`, so the alert channel
    /// can close). Before the fix this test hung forever on `wait()`.
    #[tokio::test]
    async fn apply_reload_aborts_stale_generation_and_wait_still_terminates() {
        let (root, mut reactor) = bootstrap_reactor(BRUTE_FORCE_RULE).await;
        // Force the drain to always time out → exercise the abort/detach path.
        reactor.reload_drain_timeout = std::time::Duration::from_millis(1);

        write_file(&root.join("rules/brute_force.wfl"), BRUTE_FORCE_RULE_V2);
        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let next_raw = loader.load_raw().expect("load next raw");
        let next_config = loader.load().expect("load next config");

        match reactor.apply_reload(next_raw, next_config).await {
            Ok(ReloadOutcome::Applied(_)) => {}
            other => panic!("expected Applied even when drain times out, got {other:?}"),
        }
        // A stale generation was aborted+detached; it must be reaped here.
        reactor.shutdown();
        // Bound the whole wait defensively; before the M1 fix this hung.
        let waited = tokio::time::timeout(std::time::Duration::from_secs(15), reactor.wait()).await;
        assert!(
            waited.is_ok(),
            "wait() did not terminate within 15s — detached task likely leaked an alert_tx clone"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    /// A rule whose `|>` pipeline creates internal windows, altering the
    /// compiled runtime schema/window set. `prepare_reload` classifies the
    /// simple→pipeline switch as `Blocked` (topology change requires restart).
    /// Mirrors the proven trigger in `hot_reload::tests`.
    const PIPELINE_RULE: &str = r#"
rule repeated_fail_bursts {
  events {
    e : auth_events && action == "failed"
  }

  match<sip,username:5m:fixed> {
    on event {
      e | count >= 1;
    }
    and close {
      burst: e | count >= 3;
    }
  }
  |> match<sip:30m:fixed> {
    on event {
      _in | count >= 1;
    }
    and close {
      users: _in.username | distinct | count >= 2;
    }
  } -> score(85.0)

  entity(ip, _in.sip)

  yield security_alerts (
    sip = _in.sip,
    fail_count = 2,
    message = fmt("{} multi-user fail bursts", _in.sip)
  )
}
"#;

    #[tokio::test]
    /// Rules-only changes (different rule directory) should now be **applied**
    /// (not blocked), since L2 supports adding new windows at runtime. Pipeline
    /// rules that compile to a different window set are hot-swappable.
    async fn apply_reload_applied_when_rules_change() {
        // Two rule directories whose compiled window sets differ. The simple
        // rule uses only the declared windows; the pipeline rule's `|>` stage
        // creates internal pipeline windows. With L2 incremental reload, this
        // is now supported.
        let root = make_temp_dir("reactor-rules-change");
        write_file(
            &root.join("wfusion.toml"),
            &fusion_toml("schemas/*.wfs", "rules/v1/*.wfl"),
        );
        write_file(&root.join("schemas/security.wfs"), SECURITY_SCHEMA);
        write_file(&root.join("rules/v1/brute_force.wfl"), BRUTE_FORCE_RULE);
        write_file(
            &root.join("rules/v2/repeated_fail_bursts.wfl"),
            PIPELINE_RULE,
        );
        write_file(&root.join("seed.ndjson"), "");
        write_sink_layout(&root);
        write_window_config(&root);

        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let raw = loader.load_raw().expect("load raw");
        let config = loader.load().expect("load config");
        let mut reactor = Reactor::start(config, raw, &root)
            .await
            .expect("reactor start");

        // Next config: repoint rules glob at the v2 (pipeline) directory.
        write_file(
            &root.join("wfusion.toml"),
            &fusion_toml("schemas/*.wfs", "rules/v2/*.wfl"),
        );
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(&root));
        let next_raw = loader.load_raw().expect("load next raw");
        let next_config = loader.load().expect("load next config");

        let outcome = reactor
            .apply_reload(next_raw, next_config)
            .await
            .expect("apply_reload should succeed");
        assert!(
            matches!(outcome, ReloadOutcome::Applied(_)),
            "rules-only change should be hot-reloadable, got {outcome:?}"
        );

        reactor.shutdown();
        reactor
            .wait()
            .await
            .expect("clean shutdown after rules reload");

        let _ = std::fs::remove_dir_all(root);
    }

    // -- P1: control-channel / RuntimeControlHandle --------------------------

    /// Reload a config from a fixture dir, returning (raw, config). Reused by
    /// the control-channel tests.
    fn load_next(root: &Path) -> (wf_config::RawFusionConfigTree, wf_config::FusionConfig) {
        let ctx = ConfigVarContext::new();
        let cfg_path = root.join("wfusion.toml");
        let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(root));
        (
            loader.load_raw().expect("load next raw"),
            loader.load().expect("load next config"),
        )
    }

    /// Cross-task reload through `RuntimeControlHandle` over the control
    /// channel (P1): the handle is moved to a separate task, the reactor is
    /// driven by `run()` in another, and the reload reply round-trips back.
    #[tokio::test]
    async fn control_handle_apply_reload_round_trips_across_tasks() {
        let (root, reactor) = bootstrap_reactor(BRUTE_FORCE_RULE).await;
        let control = reactor.control_handle();
        // Drive the control loop (signal watcher + reload select + wait).
        let run_task = tokio::spawn(async move { reactor.run().await });

        // From a distinct task, request a reload via the handle.
        write_file(&root.join("rules/brute_force.wfl"), BRUTE_FORCE_RULE_V2);
        let reload_root = root.clone();
        let ctrl = control.clone();
        let requester = tokio::spawn(async move {
            let (next_raw, next_config) = load_next(&reload_root);
            ctrl.apply_reload(next_raw, next_config).await
        });

        match requester.await.expect("requester panicked") {
            Ok(ReloadOutcome::Applied(_)) => {}
            other => panic!("expected Applied via control handle, got {other:?}"),
        }

        // Shut down via the handle's token and let `run` finish.
        control.cancel_token().cancel();
        run_task
            .await
            .expect("run task panicked")
            .expect("run returned an error after reload");

        let _ = std::fs::remove_dir_all(root);
    }

    /// Concurrent reload requests are serialised by the single Reactor control
    /// loop: both complete successfully and the channel/loop never deadlocks.
    #[tokio::test]
    async fn control_handle_serialises_concurrent_reloads() {
        let (root, reactor) = bootstrap_reactor(BRUTE_FORCE_RULE).await;
        let control = reactor.control_handle();
        let run_task = tokio::spawn(async move { reactor.run().await });

        // Two concurrent reload requests for the same (rule-only) change.
        write_file(&root.join("rules/brute_force.wfl"), BRUTE_FORCE_RULE_V2);
        let (ra, ca) = (root.clone(), control.clone());
        let (rb, cb) = (root.clone(), control.clone());
        let t1 = tokio::spawn(async move {
            let (raw, cfg) = load_next(&ra);
            ca.apply_reload(raw, cfg).await
        });
        let t2 = tokio::spawn(async move {
            let (raw, cfg) = load_next(&rb);
            cb.apply_reload(raw, cfg).await
        });
        let (o1, o2) = (t1.await.expect("t1"), t2.await.expect("t2"));
        // Both must resolve (no deadlock). One is Applied; the other is either
        // Applied again (idempotent v2→v2) or Applied — both acceptable so long
        // as neither is an Err or a hang.
        for (i, o) in [o1, o2].into_iter().enumerate() {
            match o {
                Ok(ReloadOutcome::Applied(_)) | Ok(ReloadOutcome::Blocked(_)) => {}
                other => panic!("concurrent reload #{i} did not resolve cleanly: {other:?}"),
            }
        }

        control.cancel_token().cancel();
        run_task
            .await
            .expect("run task panicked")
            .expect("run error");

        let _ = std::fs::remove_dir_all(root);
    }
}
