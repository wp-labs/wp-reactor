mod bootstrap;
mod compile;
mod signal;
mod spawn;
mod types;

use std::net::SocketAddr;

use orion_error::op_context;
use orion_error::prelude::*;
use tokio_util::sync::CancellationToken;

use wf_config::FusionConfig;

use crate::error::RuntimeResult;

// Re-export public API
pub use signal::wait_for_signal;

use bootstrap::load_and_compile;
use spawn::{spawn_alert_task, spawn_evictor_task, spawn_receiver_task, spawn_rule_tasks};
use types::TaskGroup;

// ---------------------------------------------------------------------------
// Reactor — the top-level lifecycle handle
// ---------------------------------------------------------------------------

/// Manages the full lifecycle of the CEP runtime: bootstrap, run, and
/// graceful shutdown.
///
/// Task groups are stored in start order and joined in reverse (LIFO)
/// during [`wait`](Self::wait), ensuring correct drain sequencing:
/// receiver stops first, then rule tasks drain and flush, then alert
/// sink flushes to disk, and finally evictor stops.
pub struct Reactor {
    cancel: CancellationToken,
    /// Separate cancel token for rule tasks — triggered only after the
    /// receiver has fully stopped, ensuring all in-flight data is drained.
    rule_cancel: CancellationToken,
    groups: Vec<TaskGroup>,
    listen_addr: SocketAddr,
}

impl Reactor {
    /// Bootstrap the entire runtime from a [`FusionConfig`] and a base
    /// directory (for resolving relative `.wfs` / `.wfl` file paths).
    #[tracing::instrument(name = "engine.start", skip_all, fields(listen = %config.server.listen))]
    pub async fn start(config: FusionConfig, base_dir: &std::path::Path) -> RuntimeResult<Self> {
        let mut op = op_context!("engine-bootstrap").with_auto_log();
        op.record("listen", config.server.listen.as_str());
        op.record("base_dir", base_dir.display().to_string().as_str());

        let cancel = CancellationToken::new();
        let rule_cancel = CancellationToken::new();

        // Phase 1: Load config & compile rules + build sink dispatcher
        let data = load_and_compile(&config, base_dir).await?;
        wf_info!(
            sys,
            schemas = data.schema_count,
            rules = data.rules.len(),
            "engine bootstrap complete"
        );

        // Phase 2: Spawn task groups (start order: alert → evictor → rules → receiver)
        let mut groups: Vec<TaskGroup> = Vec::with_capacity(4);

        let (alert_tx, alert_group) = spawn_alert_task(data.dispatcher);
        groups.push(alert_group);

        groups.push(spawn_evictor_task(
            &config,
            &data.router,
            cancel.child_token(),
        ));

        let rule_group = spawn_rule_tasks(
            data.rules,
            &data.router,
            &data.schemas,
            alert_tx,
            &config,
            rule_cancel.child_token(),
        );
        groups.push(rule_group);

        let (listen_addr, receiver_group) =
            spawn_receiver_task(&config, data.router, cancel.clone()).await?;
        groups.push(receiver_group);

        op.mark_suc();
        Ok(Self {
            cancel,
            rule_cancel,
            groups,
            listen_addr,
        })
    }

    /// Returns the local address the engine is listening on.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Request graceful shutdown of all tasks.
    pub fn shutdown(&self) {
        wf_info!(sys, "initiating graceful shutdown");
        self.cancel.cancel();
    }

    /// Wait for all task groups to complete after shutdown.
    ///
    /// Groups are joined in LIFO order (reverse of start order):
    /// receiver → rules → alert → evictor.
    ///
    /// Two-phase shutdown: the receiver is joined first, ensuring all
    /// in-flight data has been routed to windows. Only then are the rule
    /// tasks cancelled so they can do a final drain + flush.
    pub async fn wait(mut self) -> RuntimeResult<()> {
        while let Some(group) = self.groups.pop() {
            let name = group.name;
            wf_debug!(sys, task_group = name, "waiting for task group to finish");
            group.wait().await?;
            wf_debug!(sys, task_group = name, "task group finished");

            if name == "receiver" {
                // Receiver fully stopped — all data is in windows.
                // Now signal engine tasks to do their final drain + flush.
                self.rule_cancel.cancel();
            }
        }
        Ok(())
    }

    /// Returns a clone of the root cancellation token (for signal integration).
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}
