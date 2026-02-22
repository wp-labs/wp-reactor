use std::collections::HashMap;
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
use wf_core::alert::{AlertRecord, AlertSink, FanOutSink, FileAlertSink};
use wf_core::rule::{CepStateMachine, RuleExecutor};
use wf_core::window::{Evictor, Router, WindowRegistry};

use crate::alert_task;
use crate::engine_task::{RuleTaskConfig, WindowSource, run_rule_task};
use crate::error::{RuntimeReason, RuntimeResult};
use crate::evictor_task;
use crate::receiver::Receiver;
use crate::schema_bridge::schemas_to_window_defs;

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
// RunRule — one per compiled rule (construction interface)
// ---------------------------------------------------------------------------

/// Pairs a [`CepStateMachine`] with its [`RuleExecutor`] and precomputed
/// routing from stream names to CEP aliases.
pub(crate) struct RunRule {
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
struct BootstrapData {
    rules: Vec<RunRule>,
    router: Arc<Router>,
    alert_sink: Arc<dyn AlertSink>,
    schema_count: usize,
    schemas: Vec<wf_lang::WindowSchema>,
}

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
    pub async fn start(config: FusionConfig, base_dir: &Path) -> RuntimeResult<Self> {
        let mut op = op_context!("engine-bootstrap").with_auto_log();
        op.record("listen", config.server.listen.as_str());
        op.record("base_dir", base_dir.display().to_string().as_str());

        let cancel = CancellationToken::new();
        let rule_cancel = CancellationToken::new();

        // Phase 1: Load config & compile rules
        let data = load_and_compile(&config, base_dir)?;
        wf_info!(
            sys,
            schemas = data.schema_count,
            rules = data.rules.len(),
            "engine bootstrap complete"
        );

        // Phase 2: Spawn task groups (start order: alert → evictor → rules → receiver)
        let mut groups: Vec<TaskGroup> = Vec::with_capacity(4);

        let (alert_tx, alert_group) = spawn_alert_task(data.alert_sink);
        groups.push(alert_group);

        groups.push(spawn_evictor_task(&config, &data.router, cancel.child_token()));

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

// ---------------------------------------------------------------------------
// Phase 1: load_and_compile — pure data transforms
// ---------------------------------------------------------------------------

/// Load schemas, compile rules, validate config, build engines and alert sink.
fn load_and_compile(config: &FusionConfig, base_dir: &Path) -> RuntimeResult<BootstrapData> {
    // 1. Load .wfs files → Vec<WindowSchema>
    let all_schemas = load_schemas(&config.runtime.schemas, base_dir)?;

    // 2. Preprocess .wfl with config.vars → parse → compile → Vec<RulePlan>
    let all_rule_plans =
        compile_rules(&config.runtime.rules, base_dir, &config.vars, &all_schemas)?;

    // 3. Cross-validate over vs over_cap
    let window_overs: HashMap<String, Duration> = all_schemas
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
    let window_defs =
        schemas_to_window_defs(&all_schemas, &config.windows).owe(RuntimeReason::Bootstrap)?;

    // 5. WindowRegistry::build → registry
    let registry = WindowRegistry::build(window_defs).err_conv()?;

    // 6. Router::new(registry)
    let router = Arc::new(Router::new(registry));

    // 7. Build RunRules (precompute stream_name → alias routing)
    let rules = build_run_rules(&all_rule_plans, &all_schemas);

    // 8. Build alert sink
    let alert_sink = build_alert_sink(config, base_dir)?;

    let schema_count = all_schemas.len();
    Ok(BootstrapData {
        rules,
        router,
        alert_sink,
        schema_count,
        schemas: all_schemas,
    })
}

// ---------------------------------------------------------------------------
// Phase 2: task spawn helpers — each creates channel + spawns task
// ---------------------------------------------------------------------------

/// Spawn the alert pipeline: build channel, spawn consumer task.
/// Returns (alert_tx, task_group).
fn spawn_alert_task(alert_sink: Arc<dyn AlertSink>) -> (mpsc::Sender<AlertRecord>, TaskGroup) {
    let (alert_tx, alert_rx) = mpsc::channel(alert_task::ALERT_CHANNEL_CAPACITY);
    let mut group = TaskGroup::new("alert");
    group.push(tokio::spawn(async move {
        alert_task::run_alert_sink(alert_rx, alert_sink).await;
        Ok(())
    }));
    (alert_tx, group)
}

/// Spawn the periodic window evictor task.
fn spawn_evictor_task(
    config: &FusionConfig,
    router: &Arc<Router>,
    cancel: CancellationToken,
) -> TaskGroup {
    let evictor = Evictor::new(config.window_defaults.max_total_bytes.as_bytes());
    let evict_interval = config.window_defaults.evict_interval.as_duration();
    let router = Arc::clone(router);
    let mut group = TaskGroup::new("evictor");
    group.push(tokio::spawn(async move {
        evictor_task::run_evictor(evictor, router, evict_interval, cancel).await;
        Ok(())
    }));
    group
}

/// Spawn one independent task per compiled rule.
///
/// Each rule task owns its `CepStateMachine` exclusively (no `Arc<Mutex>`).
/// It subscribes to window notifications and uses cursor-based `read_since()`
/// to pull new batches.
fn spawn_rule_tasks(
    rules: Vec<RunRule>,
    router: &Arc<Router>,
    schemas: &[wf_lang::WindowSchema],
    alert_tx: mpsc::Sender<AlertRecord>,
    _config: &FusionConfig,
    cancel: CancellationToken,
) -> TaskGroup {
    let mut group = TaskGroup::new("rules");
    let timeout_scan_interval = Duration::from_secs(1);

    for rule in rules {
        let window_sources =
            resolve_window_sources(&rule.stream_aliases, schemas, router.registry());

        let task_config = RuleTaskConfig {
            machine: rule.machine,
            executor: rule.executor,
            window_sources,
            stream_aliases: rule.stream_aliases,
            alert_tx: alert_tx.clone(),
            cancel: cancel.child_token(),
            timeout_scan_interval,
        };

        group.push(tokio::spawn(async move {
            run_rule_task(task_config).await
        }));
    }

    // Drop our copy of alert_tx so the alert channel closes when all rule
    // tasks finish.
    drop(alert_tx);

    group
}

/// Resolve which windows a rule needs to subscribe to, based on its
/// stream_aliases (stream → alias mapping) and the window schemas (which
/// define which streams flow into each window).
fn resolve_window_sources(
    stream_aliases: &HashMap<String, Vec<String>>,
    schemas: &[wf_lang::WindowSchema],
    registry: &WindowRegistry,
) -> Vec<WindowSource> {
    // Collect all stream names this engine cares about.
    let interested_streams: std::collections::HashSet<&str> =
        stream_aliases.keys().map(|s| s.as_str()).collect();

    // For each window schema, check if any of its streams match.
    let mut seen_windows = std::collections::HashSet::new();
    let mut sources = Vec::new();

    for ws in schemas {
        if seen_windows.contains(&ws.name) {
            continue;
        }
        let matching_streams: Vec<String> = ws
            .streams
            .iter()
            .filter(|s| interested_streams.contains(s.as_str()))
            .cloned()
            .collect();
        if matching_streams.is_empty() {
            continue;
        }
        if let Some(window) = registry.get_window(&ws.name)
            && let Some(notify) = registry.get_notifier(&ws.name)
        {
            sources.push(WindowSource {
                window_name: ws.name.clone(),
                window: Arc::clone(window),
                notify: Arc::clone(notify),
                stream_names: matching_streams,
            });
            seen_windows.insert(ws.name.clone());
        }
    }

    sources
}

/// Bind the receiver and spawn its task.
/// Returns (listen_addr, task_group).
async fn spawn_receiver_task(
    config: &FusionConfig,
    router: Arc<Router>,
    cancel: CancellationToken,
) -> RuntimeResult<(SocketAddr, TaskGroup)> {
    let receiver = Receiver::bind(&config.server.listen, router)
        .await
        .owe_sys()?;
    let listen_addr = receiver.local_addr().owe_sys()?;
    let receiver_cancel = receiver.cancel_token();
    tokio::spawn(async move {
        cancel.cancelled().await;
        receiver_cancel.cancel();
    });
    let mut group = TaskGroup::new("receiver");
    group.push(tokio::spawn(async move { receiver.run().await }));
    Ok((listen_addr, group))
}

// ---------------------------------------------------------------------------
// Compile-phase helpers — pure data transforms extracted from start()
// ---------------------------------------------------------------------------

/// Load all `.wfs` schema files matching `glob_pattern` under `base_dir`.
fn load_schemas(glob_pattern: &str, base_dir: &Path) -> RuntimeResult<Vec<wf_lang::WindowSchema>> {
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
        let plans = wf_lang::compile_wfl(&wfl_file, schemas).owe(RuntimeReason::Bootstrap)?;
        wf_debug!(conf, file = %full_path.display(), rules = plans.len(), "compiled rule file");
        all_rule_plans.extend(plans);
    }
    Ok(all_rule_plans)
}

/// Build [`RunRule`] instances from compiled plans, pre-computing stream
/// alias routing and constructing the CEP state machines.
fn build_run_rules(
    plans: &[wf_lang::plan::RulePlan],
    schemas: &[wf_lang::WindowSchema],
) -> Vec<RunRule> {
    let mut rules = Vec::with_capacity(plans.len());
    for plan in plans {
        let stream_aliases = build_stream_aliases(&plan.binds, schemas);
        let time_field = resolve_time_field(&plan.binds, schemas);
        let machine = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), time_field);
        let executor = RuleExecutor::new(plan.clone());
        rules.push(RunRule {
            machine,
            executor,
            stream_aliases,
        });
    }
    rules
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

/// Build stream_name → alias routing for a rule, given its binds and the
/// window schemas.
pub(crate) fn build_stream_aliases(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &[wf_lang::WindowSchema],
) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for bind in binds {
        if let Some(ws) = schemas.iter().find(|s| s.name == bind.window) {
            for stream_name in &ws.streams {
                map.entry(stream_name.clone())
                    .or_default()
                    .push(bind.alias.clone());
            }
        }
    }
    map
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
