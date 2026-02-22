use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use orion_error::op_context;
use orion_error::prelude::*;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use wf_config::{FusionConfig, SinkUri, resolve_glob};
use wf_core::alert::{AlertSink, FanOutSink, FileAlertSink};
use wf_core::rule::{CepStateMachine, RuleExecutor};
use wf_core::window::{Evictor, Router, WindowRegistry};

use crate::alert_task;
use crate::error::{RuntimeReason, RuntimeResult};
use crate::evictor_task;
use crate::receiver::Receiver;
use crate::scheduler::{RuleEngine, Scheduler, SchedulerCommand, build_stream_aliases};
use crate::schema_bridge::schemas_to_window_defs;

// ---------------------------------------------------------------------------
// TaskGroup — named collection of async tasks for ordered shutdown
// ---------------------------------------------------------------------------

/// A named group of async tasks that are shut down together.
///
/// Groups are assembled in *start order* and joined in *reverse order*
/// (LIFO) during shutdown, mirroring the dependency graph:
///
///   start:  alert → infra → scheduler → receiver
///   join:   receiver → scheduler → alert → infra
///
/// This ensures upstream producers exit before downstream consumers,
/// and consumers can drain all in-flight work before the engine stops.
pub(crate) struct TaskGroup {
    name: &'static str,
    handles: Vec<JoinHandle<anyhow::Result<()>>>,
}

impl TaskGroup {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            handles: Vec::new(),
        }
    }

    fn push(&mut self, handle: JoinHandle<anyhow::Result<()>>) {
        self.handles.push(handle);
    }

    /// Join all tasks in this group, returning the first error.
    async fn wait(self) -> RuntimeResult<()> {
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
// FusionEngine — the top-level lifecycle handle
// ---------------------------------------------------------------------------

/// Manages the full lifecycle of the CEP runtime: bootstrap, run, and
/// graceful shutdown.
///
/// Task groups are stored in start order and joined in reverse (LIFO)
/// during [`wait`](Self::wait), ensuring correct drain sequencing:
/// receiver stops first, then scheduler drains events, then alert sink
/// flushes to disk, and finally infrastructure (evictor) stops.
pub struct FusionEngine {
    cancel: CancellationToken,
    groups: Vec<TaskGroup>,
    listen_addr: SocketAddr,
    cmd_tx: mpsc::Sender<SchedulerCommand>,
}

impl FusionEngine {
    /// Bootstrap the entire runtime from a [`FusionConfig`] and a base
    /// directory (for resolving relative `.wfs` / `.wfl` file paths).
    #[tracing::instrument(name = "engine.start", skip_all, fields(listen = %config.server.listen))]
    pub async fn start(config: FusionConfig, base_dir: &Path) -> RuntimeResult<Self> {
        let mut op = op_context!("engine-bootstrap").with_auto_log();
        op.record("listen", config.server.listen.as_str());
        op.record("base_dir", base_dir.display().to_string().as_str());

        let cancel = CancellationToken::new();

        // 1. Load .wfs files → Vec<WindowSchema>
        let all_schemas = load_schemas(&config.runtime.schemas, base_dir)?;

        // 2. Preprocess .wfl with config.vars → parse → compile → Vec<RulePlan>
        let all_rule_plans =
            compile_rules(&config.runtime.rules, base_dir, &config.vars, &all_schemas)?;

        // 3. Cross-validate over vs over_cap
        let window_overs: std::collections::HashMap<String, Duration> = all_schemas
            .iter()
            .map(|ws| (ws.name.clone(), ws.over))
            .collect();
        wf_config::validate_over_vs_over_cap(&config.windows, &window_overs).owe_conf()?;
        wf_debug!(
            conf,
            windows = config.windows.len(),
            "over vs over_cap validation passed"
        );

        // 4. Schema bridge: WindowSchema × WindowConfig → Vec<WindowDef>
        let window_defs = schemas_to_window_defs(&all_schemas, &config.windows)
            .owe(RuntimeReason::Bootstrap)?;

        // 5. WindowRegistry::build → registry
        let registry = WindowRegistry::build(window_defs).err_conv()?;

        // 6. Router::new(registry)
        let router = Arc::new(Router::new(registry));

        // 7. Build RuleEngines (precompute stream_name → alias routing)
        let engines = build_rule_engines(all_rule_plans, &all_schemas);

        // 8. Create bounded event channel
        wf_info!(
            sys,
            schemas = all_schemas.len(),
            rules = engines.len(),
            windows = config.windows.len(),
            "engine bootstrap complete"
        );
        let (event_tx, event_rx) = mpsc::channel(4096);

        // ---------------------------------------------------------------
        // Task groups — assembled in start order, joined LIFO on shutdown.
        //
        //   start:  alert → infra → scheduler → receiver
        //   join:   receiver → scheduler → alert → infra
        // ---------------------------------------------------------------
        let mut groups: Vec<TaskGroup> = Vec::with_capacity(4);

        // 9. Alert pipeline — build sink, create channel, spawn consumer task.
        //    The alert task has no cancel token; it exits when the scheduler
        //    drops its Sender after drain + flush, ensuring zero alert loss.
        let alert_sink = build_alert_sink(&config, base_dir)?;
        let (alert_tx, alert_rx) = mpsc::channel(alert_task::ALERT_CHANNEL_CAPACITY);
        let mut alert_group = TaskGroup::new("alert");
        alert_group.push(tokio::spawn(async move {
            alert_task::run_alert_sink(alert_rx, alert_sink).await;
            Ok(())
        }));
        groups.push(alert_group);

        // 10. Evictor task
        let evictor = Evictor::new(config.window_defaults.max_total_bytes.as_bytes());
        let evict_interval = config.window_defaults.evict_interval.as_duration();
        let evictor_cancel = cancel.child_token();
        let evictor_router = Arc::clone(&router);
        let mut infra_group = TaskGroup::new("infra");
        infra_group.push(tokio::spawn(async move {
            evictor_task::run_evictor(evictor, evictor_router, evict_interval, evictor_cancel)
                .await;
            Ok(())
        }));
        groups.push(infra_group);

        // 11. Control command channel + Scheduler task
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let scheduler = Scheduler::new(
            event_rx,
            engines,
            alert_tx,
            cancel.child_token(),
            config.runtime.executor_parallelism,
            config.runtime.rule_exec_timeout.as_duration(),
            cmd_rx,
        );
        let mut scheduler_group = TaskGroup::new("scheduler");
        scheduler_group.push(tokio::spawn(async move { scheduler.run().await }));
        groups.push(scheduler_group);

        // 12. Receiver task (last — starts accepting data)
        let receiver =
            Receiver::bind_with_event_tx(&config.server.listen, Arc::clone(&router), event_tx)
                .await
                .owe_sys()?;
        let listen_addr = receiver.local_addr().owe_sys()?;
        // Wire the receiver's internal cancel to our root cancel
        let receiver_cancel = receiver.cancel_token();
        let root_cancel = cancel.clone();
        tokio::spawn(async move {
            root_cancel.cancelled().await;
            receiver_cancel.cancel();
        });
        let mut receiver_group = TaskGroup::new("receiver");
        receiver_group.push(tokio::spawn(async move { receiver.run().await }));
        groups.push(receiver_group);

        op.mark_suc();
        Ok(Self {
            cancel,
            groups,
            listen_addr,
            cmd_tx,
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
    /// receiver → scheduler → alert → infra.
    pub async fn wait(mut self) -> RuntimeResult<()> {
        while let Some(group) = self.groups.pop() {
            let name = group.name;
            wf_debug!(sys, task_group = name, "waiting for task group to finish");
            group.wait().await?;
            wf_debug!(sys, task_group = name, "task group finished");
        }
        Ok(())
    }

    /// Returns a clone of the root cancellation token (for signal integration).
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Returns a clone of the scheduler command sender.
    pub fn command_sender(&self) -> mpsc::Sender<SchedulerCommand> {
        self.cmd_tx.clone()
    }
}

// ---------------------------------------------------------------------------
// Compile-phase helpers — pure data transforms extracted from start()
// ---------------------------------------------------------------------------

/// Load all `.wfs` schema files matching `glob_pattern` under `base_dir`.
fn load_schemas(
    glob_pattern: &str,
    base_dir: &Path,
) -> RuntimeResult<Vec<wf_lang::WindowSchema>> {
    let wfs_paths = resolve_glob(glob_pattern, base_dir).owe_conf()?;
    let mut all_schemas = Vec::new();
    for full_path in &wfs_paths {
        let content = std::fs::read_to_string(full_path)
            .owe_sys()
            .position(full_path.display().to_string())?;
        let schemas = wf_lang::parse_wfs(&content)
            .owe(RuntimeReason::Bootstrap)
            .position(full_path.display().to_string())?;
        wf_debug!(conf, file = %full_path.display(), schemas = schemas.len(), "loaded schema file");
        all_schemas.extend(schemas);
    }
    Ok(all_schemas)
}

/// Load, preprocess, parse, and compile all `.wfl` rule files matching
/// `glob_pattern` under `base_dir`, substituting `vars` and validating
/// against the given `schemas`.
fn compile_rules(
    glob_pattern: &str,
    base_dir: &Path,
    vars: &std::collections::HashMap<String, String>,
    schemas: &[wf_lang::WindowSchema],
) -> RuntimeResult<Vec<wf_lang::plan::RulePlan>> {
    let wfl_paths = resolve_glob(glob_pattern, base_dir).owe_conf()?;
    let mut all_rule_plans = Vec::new();
    for full_path in &wfl_paths {
        let raw = std::fs::read_to_string(full_path)
            .owe_sys()
            .position(full_path.display().to_string())?;
        let preprocessed = wf_lang::preprocess_vars(&raw, vars)
            .owe_data()
            .position(full_path.display().to_string())?;
        let wfl_file = wf_lang::parse_wfl(&preprocessed)
            .owe(RuntimeReason::Bootstrap)
            .position(full_path.display().to_string())?;
        let plans = wf_lang::compile_wfl(&wfl_file, schemas)
            .owe(RuntimeReason::Bootstrap)?;
        wf_debug!(conf, file = %full_path.display(), rules = plans.len(), "compiled rule file");
        all_rule_plans.extend(plans);
    }
    Ok(all_rule_plans)
}

/// Build [`RuleEngine`] instances from compiled plans, pre-computing stream
/// alias routing and constructing the CEP state machines.
fn build_rule_engines(
    plans: Vec<wf_lang::plan::RulePlan>,
    schemas: &[wf_lang::WindowSchema],
) -> Vec<RuleEngine> {
    let mut engines = Vec::with_capacity(plans.len());
    for plan in plans {
        let stream_aliases = build_stream_aliases(&plan.binds, schemas);
        let time_field = resolve_time_field(&plan.binds, schemas);
        let machine =
            CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), time_field);
        let executor = RuleExecutor::new(plan);
        engines.push(RuleEngine {
            machine,
            executor,
            stream_aliases,
        });
    }
    engines
}

/// Resolve the event-time field name for a rule from its first bind's window schema.
fn resolve_time_field(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &[wf_lang::WindowSchema],
) -> Option<String> {
    binds.first().and_then(|bind| {
        schemas
            .iter()
            .find(|ws| ws.name == bind.window)
            .and_then(|ws| ws.time_field.clone())
    })
}

/// Build the alert sink from config, supporting multiple file:// destinations.
///
/// Relative `file://` paths are resolved against `base_dir` (typically the
/// directory containing `wfusion.toml`), so that `file://alerts/wf-alerts.jsonl`
/// lands next to the config rather than relative to CWD.
fn build_alert_sink(config: &FusionConfig, base_dir: &Path) -> RuntimeResult<Arc<dyn AlertSink>> {
    let mut op = op_context!("build-alert-sink").with_auto_log();
    let uris = config.alert.parsed_sinks().owe_conf()?;
    let mut sinks: Vec<Box<dyn AlertSink>> = Vec::new();
    for uri in uris {
        match uri {
            SinkUri::File { path } => {
                let path = if path.is_relative() {
                    base_dir.join(&path)
                } else {
                    path
                };
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).owe_sys()?;
                }
                sinks.push(Box::new(FileAlertSink::open(&path).err_conv()?));
                op.record("sink_path", path.display().to_string().as_str());
                wf_debug!(conf, path = %path.display(), "opened alert file sink");
            }
        }
    }
    op.mark_suc();
    Ok(if sinks.len() == 1 {
        Arc::from(sinks.into_iter().next().unwrap())
    } else {
        Arc::new(FanOutSink::new(sinks))
    })
}

/// Register Ctrl-C (SIGINT) and SIGTERM handling; cancel the engine on first
/// signal received.
pub async fn wait_for_signal(cancel: CancellationToken) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("failed to listen for SIGTERM");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                wf_info!(sys, signal = "SIGINT", "received signal, initiating graceful shutdown");
            }
            _ = sigterm.recv() => {
                wf_info!(sys, signal = "SIGTERM", "received signal, initiating graceful shutdown");
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl-C");
        wf_info!(
            sys,
            "received shutdown signal, initiating graceful shutdown"
        );
    }
    cancel.cancel();
}
