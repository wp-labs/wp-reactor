use std::collections::HashMap;

use tokio::task::JoinHandle;

use orion_error::prelude::*;
use wf_core::rule::{CepStateMachine, RuleExecutor};

use crate::error::{RuntimeReason, RuntimeResult};

// ---------------------------------------------------------------------------
// TaskGroup — named collection of async tasks for ordered shutdown
// ---------------------------------------------------------------------------

/// A named group of async tasks that are shut down together.
///
/// Groups are assembled in *start order* and joined in *reverse order*
/// (LIFO) during shutdown, mirroring the dependency graph:
///
///   start:  alert → evictor → rules → receiver
///   join:   receiver → rules → alert → evictor
///
/// This ensures upstream producers exit before downstream consumers,
/// and consumers can drain all in-flight work before the reactor stops.
pub(crate) struct TaskGroup {
    pub(super) name: &'static str,
    handles: Vec<JoinHandle<anyhow::Result<()>>>,
}

impl TaskGroup {
    pub(super) fn new(name: &'static str) -> Self {
        Self {
            name,
            handles: Vec::new(),
        }
    }

    pub(super) fn push(&mut self, handle: JoinHandle<anyhow::Result<()>>) {
        self.handles.push(handle);
    }

    /// Join all tasks in this group, returning the first error.
    pub(super) async fn wait(self) -> RuntimeResult<()> {
        for handle in self.handles {
            handle
                .await
                .map_err(|e| {
                    StructError::from(RuntimeReason::Shutdown)
                        .with_detail(format!("task join error: {e}"))
                })?
                .owe(RuntimeReason::Shutdown)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RunRule — one per compiled rule (construction interface)
// ---------------------------------------------------------------------------

/// Pairs a [`CepStateMachine`] with its [`RuleExecutor`] and precomputed
/// routing from stream names to CEP aliases.
pub(super) struct RunRule {
    pub machine: CepStateMachine,
    pub executor: RuleExecutor,
    /// `stream_name → Vec<alias>` — which aliases should receive events from
    /// each stream name.
    pub stream_aliases: HashMap<String, Vec<String>>,
}

// ---------------------------------------------------------------------------
// BootstrapData — compiled artifacts from config-loading phase
// ---------------------------------------------------------------------------

/// Compiled artifacts from the config-loading phase, ready for task spawning.
pub(super) struct BootstrapData {
    pub rules: Vec<RunRule>,
    pub router: std::sync::Arc<wf_core::window::Router>,
    pub alert_sink: std::sync::Arc<dyn wf_core::alert::AlertSink>,
    pub schema_count: usize,
    pub schemas: Vec<wf_lang::WindowSchema>,
}
