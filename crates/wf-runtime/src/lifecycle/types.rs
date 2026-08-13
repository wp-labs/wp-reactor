use std::collections::{HashMap, HashSet};

use tokio::task::JoinHandle;

use orion_error::conversion::{SourceErr, ToStructError};
use orion_error::prelude::*;
use wf_engine::match_engine::RuleExecutor;
use wf_lang::plan::{LimitsPlan, MatchPlan};

use crate::error::{RuntimeReason, RuntimeResult};

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
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.TaskOrchestration"
)]
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
    pub(super) async fn wait(self) -> RuntimeResult<()> {
        let mut first_error: Option<StructError<RuntimeReason>> = None;
        for handle in self.handles {
            let result = handle
                .await
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
        }
        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RunRule — one per compiled rule (construction interface)
// ---------------------------------------------------------------------------

#[derive(::moju_derive::MoJu)]
#[moju(
    kind = "state",
    domain = "Orchestra",
    module = "Orchestra.ReactorLifecycle"
)]
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
}

/// Pairs a rule execution kind with its [`RuleExecutor`] and precomputed
/// routing from stream names to CEP aliases.
#[derive(::moju_derive::MoJu)]
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.ReactorLifecycle"
)]
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
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.ReactorLifecycle"
)]
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
