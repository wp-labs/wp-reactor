use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use wf_config::FusionConfig;
use wf_core::alert::{AlertSink, FileAlertSink};
use wf_core::rule::{CepStateMachine, RuleExecutor};
use wf_core::window::{Evictor, Router, WindowRegistry};

use crate::evictor_task;
use crate::receiver::Receiver;
use crate::scheduler::{RuleEngine, Scheduler, build_stream_aliases};
use crate::schema_bridge::schemas_to_window_defs;

// ---------------------------------------------------------------------------
// FusionEngine — the top-level lifecycle handle
// ---------------------------------------------------------------------------

/// Manages the full lifecycle of the CEP runtime: bootstrap, run, and
/// graceful shutdown.
pub struct FusionEngine {
    cancel: CancellationToken,
    join_handles: Vec<JoinHandle<Result<()>>>,
}

impl FusionEngine {
    /// Bootstrap the entire runtime from a [`FusionConfig`] and a base
    /// directory (for resolving relative `.wfs` / `.wfl` file paths).
    pub async fn start(config: FusionConfig, base_dir: &Path) -> Result<Self> {
        let cancel = CancellationToken::new();

        // 1. Load .wfs files → Vec<WindowSchema>
        let mut all_schemas = Vec::new();
        for wfs_path in &config.runtime.window_schemas {
            let full_path = base_dir.join(wfs_path);
            let content = std::fs::read_to_string(&full_path)
                .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", full_path.display()))?;
            let schemas = wf_lang::parse_wfs(&content)?;
            all_schemas.extend(schemas);
        }

        // 2. Preprocess .wfl with config.vars → parse → Vec<WflFile>
        let mut all_rule_plans = Vec::new();
        for wfl_path in &config.runtime.wfl_rules {
            let full_path = base_dir.join(wfl_path);
            let raw = std::fs::read_to_string(&full_path)
                .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", full_path.display()))?;
            let preprocessed = wf_lang::preprocess_vars(&raw, &config.vars)
                .map_err(|e| anyhow::anyhow!("preprocess error in {}: {e}", full_path.display()))?;
            let wfl_file = wf_lang::parse_wfl(&preprocessed)?;
            let plans = wf_lang::compile_wfl(&wfl_file, &all_schemas)?;
            all_rule_plans.extend(plans);
        }

        // 3. Cross-validate over vs over_cap
        let window_overs: std::collections::HashMap<String, Duration> = all_schemas
            .iter()
            .map(|ws| (ws.name.clone(), ws.over))
            .collect();
        wf_config::validate_over_vs_over_cap(&config.windows, &window_overs)?;

        // 4. Schema bridge: WindowSchema × WindowConfig → Vec<WindowDef>
        let window_defs = schemas_to_window_defs(&all_schemas, &config.windows)?;

        // 5. WindowRegistry::build → registry
        let registry = WindowRegistry::build(window_defs)?;

        // 6. Router::new(registry)
        let router = Arc::new(Router::new(registry));

        // 7. Build RuleEngines (precompute stream_tag → alias routing)
        let mut engines = Vec::with_capacity(all_rule_plans.len());
        for plan in all_rule_plans {
            let stream_aliases = build_stream_aliases(&plan.binds, &all_schemas);
            let machine = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone());
            let executor = RuleExecutor::new(plan);
            engines.push(RuleEngine {
                machine,
                executor,
                stream_aliases,
            });
        }

        // 8. Create bounded event channel
        let (event_tx, event_rx) = mpsc::channel(4096);

        // 9. AlertSink — use the first file:// sink URI, or a default path
        let alert_path = resolve_alert_path(&config, base_dir);
        let alert_sink: Arc<dyn AlertSink> = Arc::new(FileAlertSink::open(&alert_path)?);

        // 10. Spawn tasks
        let mut join_handles = Vec::new();

        // a. Evictor task
        let evictor = Evictor::new(config.window_defaults.max_total_bytes.as_bytes());
        let evict_interval = config.window_defaults.evict_interval.as_duration();
        let evictor_cancel = cancel.child_token();
        let evictor_router = Arc::clone(&router);
        join_handles.push(tokio::spawn(async move {
            evictor_task::run_evictor(evictor, evictor_router, evict_interval, evictor_cancel)
                .await;
            Ok(())
        }));

        // b. Scheduler task
        let scheduler = Scheduler::new(event_rx, engines, alert_sink, cancel.child_token());
        join_handles.push(tokio::spawn(async move { scheduler.run().await }));

        // c. Receiver task (last — starts accepting data)
        let receiver = Receiver::bind_with_event_tx(
            &config.server.listen,
            Arc::clone(&router),
            event_tx,
        )
        .await?;
        // Wire the receiver's internal cancel to our root cancel
        let receiver_cancel = receiver.cancel_token();
        let root_cancel = cancel.clone();
        tokio::spawn(async move {
            root_cancel.cancelled().await;
            receiver_cancel.cancel();
        });
        join_handles.push(tokio::spawn(async move { receiver.run().await }));

        Ok(Self {
            cancel,
            join_handles,
        })
    }

    /// Request graceful shutdown of all tasks.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }

    /// Wait for all tasks to complete after shutdown.
    pub async fn wait(self) -> Result<()> {
        for handle in self.join_handles {
            handle.await??;
        }
        Ok(())
    }

    /// Returns a clone of the root cancellation token (for signal integration).
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

/// Extract the alert output path from the first `file://` sink URI.
/// Falls back to `{base_dir}/alerts.jsonl` if no file sink is configured.
fn resolve_alert_path(config: &FusionConfig, base_dir: &Path) -> std::path::PathBuf {
    for sink_uri in &config.alert.sinks {
        if let Some(path) = sink_uri.strip_prefix("file://") {
            return std::path::PathBuf::from(path);
        }
    }
    base_dir.join("alerts.jsonl")
}

/// Register Ctrl-C / SIGTERM handling and cancel the engine on signal.
pub async fn wait_for_signal(cancel: CancellationToken) {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for Ctrl-C");
    log::info!("received shutdown signal, initiating graceful shutdown");
    cancel.cancel();
}
