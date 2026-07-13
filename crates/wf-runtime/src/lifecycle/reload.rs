use std::collections::HashSet;

use tokio::sync::mpsc;
use wf_config::{FusionConfig, RawFusionConfigTree};
use wf_engine::alert::OutputRecord;

use crate::error::RuntimeResult;
use crate::hot_reload::{ReloadPreparation, prepare_reload};
use crate::lifecycle::spawn::spawn_rule_tasks;
use crate::lifecycle::types::RunRule;

use super::{Reactor, ReloadOutcome, watch_group};

impl Reactor {
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
    /// [`super::DEFAULT_RELOAD_DRAIN_TIMEOUT`] for why the wait is bounded.
    async fn swap_rule_tasks(
        &mut self,
        new_rules: Vec<RunRule>,
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
    pub(crate) async fn reap_detached_rule_watchers(&mut self) {
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
}
