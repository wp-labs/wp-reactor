use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::{Arc, Once};
use std::time::Duration;

use orion_error::conversion::{SourceErr, ToStructError};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use wf_config::FusionConfig;
use wf_engine::match_engine::{CepStateMachine, SharedLimits};
use wf_engine::sink::SinkDispatcher;
use wf_engine::window::{
    EvictionGate, Evictor, Router, RulePush, WINDOW_CHANNEL_DEPTH, WindowAppendReport,
    WindowMailbox, WindowMsg, WindowRegistry, run_window_actor,
};

/// Bounded capacity of each rule push channel (a channel carries whole batches
/// of parsed events, `Arc<Vec<Arc<Event>>>`). A full channel blocks the
/// window actor's broadcast — backpressure — instead of buffering unboundedly
/// (50M sustained inject grew RSS to ~13GB with unbounded channels).
///
/// Tuning note (q5 100M freeze, ISSUE_q5_100m_freeze.md): the window actor is
/// a single writer that, per batch, `join_sends` a 30-way broadcast to every
/// rule channel. If ANY rule task pauses transiently (GC / lock contention /
/// residual `recalibrate_memory` scan), its channel fills and the actor's
/// broadcast blocks — stalling *all* window commits, which then backs up the
/// mailbox, exhausts the byte budget, and stops the receiver, so
/// `append_total` can never reach TOTAL (the ~99M tail freeze). A deeper
/// channel lets the actor absorb a transient pause without stalling; it is
/// memory-bounded because each queued `RulePush` keeps its `Arc<RecordBatch>`
/// alive until the (slow) rule task consumes it. 256 (~3.5s of backlog at the
/// q5 ingest rate) covers transient pauses without the unbounded-channel RSS
/// blow-up; raise further only if a *sustained* single-shard skew is observed.
pub(crate) const RULE_CHANNEL_CAPACITY: usize = 256;
use wf_lang::ast::FieldRef;

use crate::alert_task;
use crate::engine_task::{
    ConvCloseBatch, ConvShardSink, ConvStageConfig, RuleTaskConfig, StatsTaskConfig, WindowSource,
    run_conv_stage_task, run_rule_task, run_stats_task,
};
use crate::error::{RuntimeReason, RuntimeResult};
use crate::evictor_task;
use crate::metrics::{MetricsRecord, MonRecv, RuntimeMetrics, run_metrics_task};
use crate::receiver::{
    DEFAULT_STREAM_TAG_FIELD, ReplayRoute, replay_arrow_framed_file, replay_arrow_ipc_file,
    replay_csv_file, replay_ndjson_file, resolve_stream_schema,
};
use crate::source::DataSourceBatchSource;
use wf_connector_api::BatchSource;
use wp_core_connectors::sources::batch::arrow::WireFormat;
use wp_model_core::model::{DataRecord, DataType, Field, FieldStorage, Value};

use super::parse_pool::{
    IngestLimiter, ParseItem, push_decoded_batch, spawn_parse_pool_with_preread,
};
use super::types::{RunRule, RunRuleKind, TaskGroup};

// ---------------------------------------------------------------------------
// Phase 2: task spawn helpers — each creates channel + spawns task
// ---------------------------------------------------------------------------

/// Spawn the alert pipeline: one bounded channel + consumer task per
/// [`alert_task::ALERT_CONSUMERS`]. Rule tasks round-robin their emits across
/// the returned senders so output processing is not capped by a single
/// consumer. Returns (alert_txs, task_group).
pub(super) fn spawn_alert_task(
    dispatcher: Arc<SinkDispatcher>,
    metrics: Option<Arc<RuntimeMetrics>>,
    cancel: CancellationToken,
) -> (Arc<alert_task::SinkFanout>, TaskGroup) {
    let mut group = TaskGroup::new("alert");
    let mut by_sink = HashMap::new();

    // Error sinks first: their senders feed the escalation list.
    let mut error_txs: Vec<mpsc::Sender<alert_task::AlertBatch>> = Vec::new();
    for sink in dispatcher.error_sinks() {
        let (tx, rx) = mpsc::channel::<alert_task::AlertBatch>(alert_task::SINK_CHANNEL_CAPACITY);
        error_txs.push(tx);
        let sink = Arc::clone(sink);
        let metrics = metrics.clone();
        let cancel = cancel.child_token();
        group.push(tokio::spawn(async move {
            alert_task::run_sink_consumer(rx, sink, Arc::new(Vec::new()), metrics, cancel).await;
            Ok(())
        }));
    }
    let error_txs = Arc::new(error_txs);

    // Regular + default sinks (everything except error and monitor).
    let error_ptrs: HashSet<usize> = dispatcher
        .error_sinks()
        .iter()
        .map(|s| Arc::as_ptr(s) as usize)
        .collect();
    let monitor_ptrs: HashSet<usize> = dispatcher
        .monitor_sinks()
        .iter()
        .map(|s| Arc::as_ptr(s) as usize)
        .collect();
    for sink in dispatcher.all_sinks() {
        let ptr = Arc::as_ptr(sink) as usize;
        if error_ptrs.contains(&ptr) || monitor_ptrs.contains(&ptr) {
            continue;
        }
        // Parallel writers (sink group `parallel`): one bounded channel + one
        // consumer per writer, so the alert fan-out is not capped by a single
        // consumer draining every record serially.
        let writers = sink.parallel.max(1);
        let mut senders = Vec::with_capacity(writers);
        for _ in 0..writers {
            let (tx, rx) =
                mpsc::channel::<alert_task::AlertBatch>(alert_task::SINK_CHANNEL_CAPACITY);
            senders.push(tx);
            let sink = Arc::clone(sink);
            let error_txs = Arc::clone(&error_txs);
            let metrics = metrics.clone();
            let cancel = cancel.child_token();
            group.push(tokio::spawn(async move {
                alert_task::run_sink_consumer(rx, sink, error_txs, metrics, cancel).await;
                Ok(())
            }));
        }
        by_sink.insert(ptr, senders);
    }

    let fanout = Arc::new(alert_task::SinkFanout::new(by_sink, dispatcher));
    (fanout, group)
}

/// Floor for the per-window actor channel byte budget — smaller values would
/// stall the pipeline on even one modest batch.
const MIN_WINDOW_BUFFER_BYTES: usize = 4 * 1024 * 1024;

/// Spawn one single-writer actor per window plus its bounded mailbox, and
/// register the mailboxes on the router (subscription model: each window
/// "subscribes" to its stream's parse output via the channel).
///
/// Must run **before** the parse pool and rule tasks spawn: the parse pool
/// switches to direct dispatch as soon as any mailbox is registered, and rule
/// emits must find the mailboxes in place.
///
/// The per-window byte budget (`runtime.window_buffer_bytes`, default
/// 64 MiB) is the explicit backpressure that replaces the removed window
/// write lock's implicit serialization: in-flight bytes per window are
/// bounded by construction instead of by lock queueing.
pub(super) fn spawn_window_actors(
    config: &FusionConfig,
    router: &Arc<Router>,
    gate: Arc<EvictionGate>,
    cancel: CancellationToken,
    metrics: Option<Arc<RuntimeMetrics>>,
) -> TaskGroup {
    let buffer_bytes = config
        .runtime
        .window_buffer_bytes
        .max(MIN_WINDOW_BUFFER_BYTES);
    let mut group = TaskGroup::new("window_actors");

    let report: WindowAppendReport = match &metrics {
        Some(m) => {
            let m = Arc::clone(m);
            Arc::new(move |window, rows, late| m.report_window_append(window, rows, late))
        }
        None => Arc::new(|_, _, _| {}),
    };

    let fanout = Arc::clone(router.fanout());
    for name in router.registry().window_names() {
        let Some(win) = router.registry().get_window(&name) else {
            continue;
        };
        let Some(notify) = router.registry().get_notifier(&name) else {
            continue;
        };
        let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
        router.register_mailbox(
            &name,
            WindowMailbox {
                tx,
                budget: Arc::new(tokio::sync::Semaphore::new(buffer_bytes)),
                budget_bytes: buffer_bytes,
            },
        );
        let name: Arc<str> = Arc::from(name.as_str());
        let report = Arc::clone(&report);
        let fanout = Arc::clone(&fanout);
        let gate = Arc::clone(&gate);
        let cancel = cancel.child_token();
        group.push(tokio::spawn(async move {
            run_window_actor(
                name,
                win,
                Arc::clone(&gate),
                fanout,
                notify,
                rx,
                cancel,
                Some(report),
            )
            .await;
            Ok(())
        }));
    }

    group
}

/// Spawn the periodic window evictor task.
pub(super) fn spawn_evictor_task(
    config: &FusionConfig,
    router: &Arc<Router>,
    gate: Arc<EvictionGate>,
    cancel: CancellationToken,
    metrics: Option<Arc<RuntimeMetrics>>,
) -> TaskGroup {
    let evictor = Evictor::new(Arc::clone(&gate));
    let evict_interval = config.window_defaults.evict_interval.as_duration();
    let router = Arc::clone(router);
    let mut group = TaskGroup::new("evictor");
    group.push(tokio::spawn(async move {
        evictor_task::run_evictor(evictor, router, evict_interval, cancel, metrics).await;
        Ok(())
    }));
    group
}

/// Register one consumption-progress slot per consumed window for a rule
/// task (see [`wf_engine::window::WindowProgress`]).
///
/// The task acks `seq + 1` per processed batch; time-based eviction only
/// removes batches every live consumer has acked, so sweeps can no longer
/// drop unconsumed data.
fn register_progress(
    router: &Arc<Router>,
    window_sources: &[WindowSource],
) -> HashMap<String, std::sync::Arc<std::sync::atomic::AtomicU64>> {
    window_sources
        .iter()
        .map(|src| {
            let slot = router
                .registry()
                .progress(&src.window_name)
                .expect("progress table exists for every window")
                .register();
            (src.window_name.clone(), slot)
        })
        .collect()
}

/// Spawn one independent task per compiled rule.
///
/// Each rule task owns its `CepStateMachine` exclusively (no `Arc<Mutex>`).
/// It subscribes to window notifications and uses cursor-based `read_since()`
/// to pull new batches.
#[allow(clippy::too_many_arguments)]
pub(super) fn spawn_rule_tasks(
    rules: Vec<RunRule>,
    router: &Arc<Router>,
    intermediate_targets: &HashSet<String>,
    pipe_registry: std::sync::Arc<wf_engine::pipe::PipeRegistry>,
    sink_fanout: Arc<alert_task::SinkFanout>,
    cancel: CancellationToken,
    metrics: Option<Arc<RuntimeMetrics>>,
    eos_tx: watch::Sender<u64>,
    shard_count: usize,
) -> TaskGroup {
    let mut group = TaskGroup::new("rules");
    let timeout_scan_interval = Duration::from_secs(1);
    let shard_count = shard_count.max(1);
    // M1 window-actor-pull-model.md §5: default **pull**; the legacy push
    // broadcast (channel + fanout) is retained as an emergency fallback behind
    // `WFUSION_WINDOW_DISPATCH=push` (byte-identical production behavior, 256
    // stall止血 kept). Pull eliminates the actor single-writer stall that froze
    // q5 100M.
    let use_push = std::env::var("WFUSION_WINDOW_DISPATCH")
        .map(|v| v.eq_ignore_ascii_case("push"))
        .unwrap_or(false);

    for rule in rules {
        let window_sources = resolve_window_sources(&rule.window_aliases, router.registry());

        match rule.kind {
            RunRuleKind::Stats {
                stats_plan,
                time_field,
            } => {
                // 声明式窗口统计: 空键单实例（P1）; 带 key 分片为 P2。
                // 消费 fanout 投递的 raw RecordBatch（push）或 window log（pull）, 列式
                // process_batch（失败回退行式）, 固定窗口 close → alert 复用。
                let stats = wf_engine::match_engine::StatsExecutor::new(stats_plan);
                let push_rx = if use_push {
                    let (push_tx, push_rx) = mpsc::channel::<RulePush>(RULE_CHANNEL_CAPACITY);
                    for source in &window_sources {
                        router
                            .fanout()
                            .register(&source.window_name, push_tx.clone());
                    }
                    Some(push_rx)
                } else {
                    None
                };
                let progress = register_progress(router, &window_sources);
                let task_config = StatsTaskConfig {
                    stats,
                    executor: rule.executor.clone(),
                    window_sources,
                    sink_fanout: Arc::clone(&sink_fanout),
                    cancel: cancel.child_token(),
                    router: Arc::clone(router),
                    metrics: metrics.clone(),
                    time_field,
                    timeout_scan_interval,
                    intermediate_targets: intermediate_targets.clone(),
                    pipe_registry: Arc::clone(&pipe_registry),
                    eos_flush: eos_tx.subscribe(),
                    push_rx,
                    progress: progress.clone(),
                    shard_index: None,
                    shard_count: 1,
                };
                group.push(tokio::spawn(
                    async move { run_stats_task(task_config).await },
                ));
            }
            RunRuleKind::Each { alias, time_field } => {
                // Stateless each rule. Terminal-output rules (yield target is
                // NOT an intermediate pipe) shard across `shard_count` workers
                // via whole-batch round-robin: no per-event state, so batch
                // reordering is harmless and each `Arc` batch goes to exactly
                // one worker (zero copy, exact metrics, unique wfx_id). Rules
                // feeding an intermediate pipe stay single-worker — a
                // downstream state machine must not see same-key events out
                // of order.
                let target = rule.executor.plan().yield_plan.target.clone();
                let shardable = shard_count > 1 && !intermediate_targets.contains(&target);

                if shardable {
                    let mut shard_txs = Vec::with_capacity(shard_count);
                    for shard_idx in 0..shard_count {
                        // Push mode only: create the delivery channel. Pull mode
                        // carries no channel — the task pulls the shared window
                        // log directly (whole-batch round-robin gated by seq).
                        let push_rx = if use_push {
                            let (push_tx, push_rx) =
                                mpsc::channel::<RulePush>(RULE_CHANNEL_CAPACITY);
                            shard_txs.push(push_tx);
                            Some(push_rx)
                        } else {
                            None
                        };
                        let progress = register_progress(router, &window_sources);
                        let task_config = RuleTaskConfig {
                            machine: None,
                            each_alias: Some(alias.clone()),
                            each_time_field: time_field.clone(),
                            executor: rule.executor.clone(),
                            window_sources: window_sources.clone(),
                            sink_fanout: Arc::clone(&sink_fanout),
                            cancel: cancel.child_token(),
                            timeout_scan_interval,
                            router: Arc::clone(router),
                            metrics: metrics.clone(),
                            intermediate_targets: intermediate_targets.clone(),
                            pipe_registry: Arc::clone(&pipe_registry),
                            eos_flush: eos_tx.subscribe(),
                            push_rx,
                            shard_index: Some(shard_idx),
                            shard_count,
                            progress: progress.clone(),
                            conv_sink: None,
                        };
                        group.push(tokio::spawn(
                            async move { run_rule_task(task_config).await },
                        ));
                    }
                    if use_push {
                        for source in &window_sources {
                            router
                                .fanout()
                                .register_round_robin(&source.window_name, shard_txs.clone());
                        }
                    }
                } else {
                    let push_rx = if use_push {
                        let (push_tx, push_rx) = mpsc::channel::<RulePush>(RULE_CHANNEL_CAPACITY);
                        for source in &window_sources {
                            router
                                .fanout()
                                .register(&source.window_name, push_tx.clone());
                        }
                        Some(push_rx)
                    } else {
                        None
                    };
                    let progress = register_progress(router, &window_sources);
                    let task_config = RuleTaskConfig {
                        machine: None,
                        each_alias: Some(alias),
                        each_time_field: time_field,
                        executor: rule.executor,
                        window_sources,
                        sink_fanout: Arc::clone(&sink_fanout),
                        cancel: cancel.child_token(),
                        timeout_scan_interval,
                        router: Arc::clone(router),
                        metrics: metrics.clone(),
                        intermediate_targets: intermediate_targets.clone(),
                        pipe_registry: Arc::clone(&pipe_registry),
                        eos_flush: eos_tx.subscribe(),
                        push_rx,
                        shard_index: None,
                        shard_count: 1,
                        progress: progress.clone(),
                        conv_sink: None,
                    };
                    group.push(tokio::spawn(
                        async move { run_rule_task(task_config).await },
                    ));
                }
            }
            RunRuleKind::Match {
                match_plan,
                time_field,
                limits,
            } => {
                let name = rule.executor.plan().name.clone();
                let conv_plan = rule.executor.plan().conv_plan.clone();
                let conv_window = rule.executor.plan().conv_window.clone();
                let yield_target = rule.executor.plan().yield_plan.target.clone();
                // P2a + P2c: shard rules with a match key and no *inline* conv.
                // A fixed-window conv rule with a generated conv window becomes
                // shardable; sliding/session conv stays inline. Conv rules that
                // yield to an intermediate pipe stay inline too (the conv stage
                // emits final sink output only).
                let has_inline_conv = conv_plan.is_some() && conv_window.is_none();
                let conv_to_pipe =
                    conv_window.is_some() && intermediate_targets.contains(yield_target.as_str());
                let shardable = !match_plan.keys.is_empty()
                    && !has_inline_conv
                    && !conv_to_pipe
                    && shard_count > 1;

                if shardable {
                    let keys: Arc<[FieldRef]> = match_plan.keys.clone().into();
                    // M1 pull model: register the window's key partition so the
                    // parse stage computes the per-shard row subset once and
                    // stores it on the log (P2 zero re-partition). The pull
                    // rule task then pulls only its `shard_rows[i]` subset.
                    // Harmless in push mode (the broadcast path resolves the
                    // partition from its own delivery subscription instead).
                    for source in &window_sources {
                        router.fanout().register_window_sharding(
                            &source.window_name,
                            Arc::clone(&keys),
                            shard_count,
                        );
                    }
                    // P2b: one shared rate-limit/budget handle across all shards
                    // (only when the rule carries limits).
                    let shared_limits = limits.as_ref().map(|_| SharedLimits::new());
                    // P2c: a sharded conv rule gets a shared watermark barrier and
                    // one conv-stage task that aggregates raw closes across shards.
                    let conv_ctx = match &conv_window {
                        Some(cw) => {
                            let (tx, rx) = mpsc::channel::<ConvCloseBatch>(RULE_CHANNEL_CAPACITY);
                            let barrier: Arc<Vec<AtomicI64>> = Arc::new(
                                (0..shard_count).map(|_| AtomicI64::new(i64::MIN)).collect(),
                            );
                            let stage_config = ConvStageConfig {
                                executor: rule.executor.clone(),
                                conv_plan: conv_plan.clone(),
                                keys: Arc::clone(&keys),
                                over: cw.over,
                                limits: limits.clone(),
                                shared_limits: shared_limits.clone(),
                                barrier: Arc::clone(&barrier),
                                sink_fanout: Arc::clone(&sink_fanout),
                                router: Arc::clone(router),
                                metrics: metrics.clone(),
                                rx,
                                cancel: cancel.child_token(),
                                eos: eos_tx.subscribe(),
                                timeout_scan_interval,
                            };
                            group.push(tokio::spawn(async move {
                                run_conv_stage_task(stage_config).await
                            }));
                            Some((tx, barrier))
                        }
                        None => None,
                    };
                    let mut shard_txs = Vec::with_capacity(shard_count);
                    for shard_idx in 0..shard_count {
                        let mut machine = match &shared_limits {
                            Some(shared) => CepStateMachine::with_limits_shared(
                                name.clone(),
                                match_plan.clone(),
                                time_field.clone(),
                                limits.clone(),
                                Arc::clone(shared),
                            ),
                            None => CepStateMachine::with_limits(
                                name.clone(),
                                match_plan.clone(),
                                time_field.clone(),
                                limits.clone(),
                            ),
                        };
                        if conv_ctx.is_some() {
                            // Emit raw closes to the conv stage (aggregation window).
                            machine.set_raw_conv_mode();
                        }
                        // Push mode only: create the delivery channel. Pull mode
                        // carries no channel — the task pulls its `shard_rows[i]`
                        // subset directly from the shared window log.
                        let push_rx = if use_push {
                            let (push_tx, push_rx) =
                                mpsc::channel::<RulePush>(RULE_CHANNEL_CAPACITY);
                            shard_txs.push(push_tx);
                            Some(push_rx)
                        } else {
                            None
                        };
                        let progress = register_progress(router, &window_sources);
                        let conv_sink = conv_ctx.as_ref().map(|(tx, _barrier)| ConvShardSink {
                            tx: tx.clone(),
                            barrier_index: shard_idx,
                        });
                        let task_config = RuleTaskConfig {
                            machine: Some(machine),
                            each_alias: None,
                            each_time_field: None,
                            executor: rule.executor.clone(),
                            window_sources: window_sources.clone(),
                            sink_fanout: Arc::clone(&sink_fanout),
                            cancel: cancel.child_token(),
                            timeout_scan_interval,
                            router: Arc::clone(router),
                            metrics: metrics.clone(),
                            intermediate_targets: intermediate_targets.clone(),
                            pipe_registry: Arc::clone(&pipe_registry),
                            eos_flush: eos_tx.subscribe(),
                            push_rx,
                            shard_index: Some(shard_idx),
                            shard_count,
                            progress: progress.clone(),
                            conv_sink,
                        };
                        group.push(tokio::spawn(
                            async move { run_rule_task(task_config).await },
                        ));
                    }
                    if use_push {
                        for source in &window_sources {
                            router.fanout().register_sharded(
                                &source.window_name,
                                shard_txs.clone(),
                                Arc::clone(&keys),
                            );
                        }
                    }
                } else {
                    let machine =
                        CepStateMachine::with_limits(name, match_plan, time_field, limits);
                    let push_rx = if use_push {
                        let (push_tx, push_rx) = mpsc::channel::<RulePush>(RULE_CHANNEL_CAPACITY);
                        for source in &window_sources {
                            router
                                .fanout()
                                .register(&source.window_name, push_tx.clone());
                        }
                        Some(push_rx)
                    } else {
                        None
                    };
                    let progress = register_progress(router, &window_sources);
                    let task_config = RuleTaskConfig {
                        machine: Some(machine),
                        each_alias: None,
                        each_time_field: None,
                        executor: rule.executor,
                        window_sources,
                        sink_fanout: Arc::clone(&sink_fanout),
                        cancel: cancel.child_token(),
                        timeout_scan_interval,
                        router: Arc::clone(router),
                        metrics: metrics.clone(),
                        intermediate_targets: intermediate_targets.clone(),
                        pipe_registry: Arc::clone(&pipe_registry),
                        eos_flush: eos_tx.subscribe(),
                        push_rx,
                        shard_index: None,
                        shard_count: 1,
                        progress: progress.clone(),
                        conv_sink: None,
                    };
                    group.push(tokio::spawn(
                        async move { run_rule_task(task_config).await },
                    ));
                }
            }
        }
    }

    // Drop our copy; the Reactor holds the master fanout so the sink channels
    // stay open until shutdown.
    drop(sink_fanout);

    group
}

/// Resolve which windows a rule needs to subscribe to, based on its direct
/// bind.window → alias mapping.
pub(super) fn resolve_window_sources(
    window_aliases: &HashMap<String, Vec<String>>,
    registry: &WindowRegistry,
) -> Vec<WindowSource> {
    let mut sources = Vec::new();

    for (window_name, aliases) in window_aliases {
        if let Some(window) = registry.get_window(window_name)
            && let Some(notify) = registry.get_notifier(window_name)
        {
            sources.push(WindowSource {
                window_name: window_name.clone(),
                window,
                notify,
                aliases: aliases.clone(),
            });
        }
    }

    sources
}

/// Bind the receiver and spawn its tasks.
/// Returns the receiver task group.
pub(super) async fn spawn_receiver_task(
    config: &FusionConfig,
    router: Arc<Router>,
    cancel: CancellationToken,
    metrics: Option<Arc<RuntimeMetrics>>,
    schemas: &[wf_lang::WindowSchema],
    base_dir: &Path,
) -> RuntimeResult<TaskGroup> {
    let mut group = TaskGroup::new("receiver");
    let mut spawned = 0usize;
    let schema_catalog = Arc::new(schemas.to_vec());
    register_builtin_external_sources();

    // R2/actor: parse worker pool — external sources push decoded batches
    // here and N parallel parse workers run `route_parse`, then dispatch each
    // window's batch directly to its window actor mailbox (registered by
    // `spawn_window_actors` before this call). Ordering is per-source: each
    // source config entry owns a seq counter assigned serially in its receive
    // loop(s), and the window actor re-orders per source. The preread byte
    // budget bounds total decoded-batch residency in the pipeline regardless
    // of frame size.
    let (parse_tx, preread) = spawn_parse_pool_with_preread(
        &router,
        metrics.clone(),
        config.runtime.parse_parallelism,
        &mut group,
        config.runtime.parse_buffer_bytes,
    );
    let ingest_limiter = config.runtime.max_ingest_rate.map(IngestLimiter::new);

    for (source_idx, source) in config.sources.iter().enumerate() {
        if !source.enabled {
            continue;
        }
        let source_name = source.effective_name(source_idx);
        // Resolve connect → kind if needed
        let kind = if let Some(ref conn) = source.connect {
            resolve_connector_kind(conn).unwrap_or_else(|| {
                // Fallback: try legacy source_type
                source.kind().to_string()
            })
        } else {
            source.kind().to_string()
        };
        match kind.as_str() {
            "file" => {
                let path_str = source.params.get("path").map(|s| s.as_str()).unwrap_or("");
                let path = resolve_source_path(base_dir, path_str);
                let stream = source_stream_tag(source).to_string();
                let stream_tag_field = source
                    .params
                    .get("stream_tag_field")
                    .cloned()
                    .unwrap_or_else(|| DEFAULT_STREAM_TAG_FIELD.to_string());
                let router = Arc::clone(&router);
                let metrics = metrics.clone();
                let parse_tx = parse_tx.clone();
                let preread = preread.clone();
                // Per-source seq: serial assignment inside this source's
                // replay loop keeps batches ordered for the window actor's
                // per-source reorder cursor.
                let parse_seq = Arc::new(AtomicU64::new(0));
                let limiter = ingest_limiter.clone();
                let cancel = cancel.child_token();
                let format = source_data_format(source).to_string();
                let schemas = Arc::clone(&schema_catalog);
                let source_name = source_name.clone();
                group.push(tokio::spawn(async move {
                    match format.as_str() {
                        "ndjson" => {
                            replay_ndjson_file(
                                &path,
                                ReplayRoute {
                                    stream_name: &stream,
                                    stream_tag_field: &stream_tag_field,
                                },
                                &source_name,
                                schemas.as_slice(),
                                router,
                                metrics,
                                parse_tx.clone(),
                                preread.clone(),
                                Arc::clone(&parse_seq),
                                cancel,
                            )
                            .await?
                        }
                        "csv" => {
                            replay_csv_file(
                                &path,
                                ReplayRoute {
                                    stream_name: &stream,
                                    stream_tag_field: &stream_tag_field,
                                },
                                &source_name,
                                schemas.as_slice(),
                                router,
                                metrics,
                                parse_tx.clone(),
                                preread.clone(),
                                Arc::clone(&parse_seq),
                                cancel,
                            )
                            .await?
                        }
                        "arrow_framed" => {
                            replay_arrow_framed_file(
                                &path,
                                &stream,
                                &source_name,
                                schemas.as_slice(),
                                router,
                                metrics,
                                parse_tx.clone(),
                                preread.clone(),
                                Arc::clone(&parse_seq),
                                cancel,
                                limiter,
                            )
                            .await?
                        }
                        "arrow_ipc" => {
                            replay_arrow_ipc_file(
                                &path,
                                &stream,
                                &source_name,
                                schemas.as_slice(),
                                router,
                                metrics,
                                parse_tx.clone(),
                                preread.clone(),
                                Arc::clone(&parse_seq),
                                cancel,
                            )
                            .await?
                        }
                        _ => {
                            return Err(RuntimeReason::system_error()
                                .to_err()
                                .with_detail(format!("unsupported format: {format}")));
                        }
                    }
                    Ok(())
                }));
                spawned += 1;
            }
            _ => {
                // Per-source seq counter (see the file branch above): one
                // counter shared by all handles of this source entry.
                let parse_seq = Arc::new(AtomicU64::new(0));
                spawned += spawn_external_source_tasks(
                    source,
                    &kind,
                    spawned,
                    base_dir,
                    &schema_catalog,
                    &router,
                    metrics.clone(),
                    cancel.child_token(),
                    &mut group,
                    parse_tx.clone(),
                    preread.clone(),
                    parse_seq,
                    ingest_limiter.clone(),
                )
                .await?;
            }
        }
    }

    if spawned == 0 {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail("no enabled sources configured")
            .err();
    }

    Ok(group)
}

fn resolve_source_path(base_dir: &Path, path: &str) -> PathBuf {
    let p = Path::new(path);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        base_dir.join(p)
    }
}

fn source_data_format(source: &wf_config::SourceConfig) -> &str {
    source
        .params
        .get("data_format")
        .or_else(|| source.params.get("format"))
        .map(|s| s.as_str())
        .unwrap_or("ndjson")
}

fn source_stream_tag(source: &wf_config::SourceConfig) -> &str {
    source
        .params
        .get("stream_tag")
        .map(|s| s.as_str())
        .unwrap_or("")
}

/// Resolve a connector id (e.g. `"kafka_src"`) to its kind (e.g. `"kafka"`)
/// via the global connector registry.
fn resolve_connector_kind(connector_id: &str) -> Option<String> {
    wp_core_connectors::registry::registered_source_defs()
        .into_iter()
        .find(|def| def.id == connector_id)
        .map(|def| def.kind)
}

fn register_builtin_external_sources() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        wp_core_connectors::sources::register_file_factory();
        wp_core_connectors::sources::tcp::register_tcp_factory();
        wp_core_connectors::sources::syslog::register_syslog_factory();
    });
}

#[allow(clippy::too_many_arguments)]
async fn spawn_external_source_tasks(
    source: &wf_config::SourceConfig,
    source_kind: &str,
    source_idx: usize,
    base_dir: &Path,
    schemas: &Arc<Vec<wf_lang::WindowSchema>>,
    router: &Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    cancel: CancellationToken,
    group: &mut TaskGroup,
    parse_tx: tokio::sync::mpsc::Sender<ParseItem>,
    preread: super::parse_pool::PrereadBudget,
    parse_seq: Arc<AtomicU64>,
    ingest_limiter: Option<Arc<IngestLimiter>>,
) -> RuntimeResult<usize> {
    let Some(factory) = wp_core_connectors::registry::get_source_factory(source_kind) else {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!(
                "no factory registered for source kind {source_kind:?}"
            ))
            .err();
    };

    let stream_name = source_stream_tag(source).to_string();
    let stream_tag_field = source
        .params
        .get("stream_tag_field")
        .cloned()
        .unwrap_or_else(|| DEFAULT_STREAM_TAG_FIELD.to_string());
    let format = WireFormat::from_data_format(Some(source_data_format(source)));

    // Arrow formats carry their own schema in the IPC stream; only NDJSON
    // needs a pre-resolved window schema.
    let schema_needs_resolve = matches!(format, WireFormat::Ndjson) && !stream_name.is_empty();
    let schema = if schema_needs_resolve {
        resolve_stream_schema(schemas.as_slice(), &stream_name)?
    } else {
        // Empty schema placeholder — Arrow data carries its own schema.
        Arc::new(arrow::datatypes::Schema::empty())
    };
    let mut params = wp_connector_api::ParamMap::new();
    for (key, value) in &source.params {
        params.insert(key.clone(), source_param_to_json(value));
    }
    let source_spec = wp_connector_api::SourceSpec {
        name: source.effective_name(source_idx),
        kind: source_kind.to_string(),
        connector_id: source.connect.clone().unwrap_or_default(),
        params,
        tags: Vec::new(),
    };

    factory.validate_spec(&source_spec).source_err(
        RuntimeReason::Bootstrap,
        format!("validate source {:?}", source_spec.name),
    )?;

    let mut svc = factory
        .build(
            &source_spec,
            &wp_connector_api::SourceBuildCtx::new(base_dir.to_path_buf()),
        )
        .await
        .source_err(
            RuntimeReason::Bootstrap,
            format!("build source {:?}", source_spec.name),
        )?;

    let mut spawned = 0usize;
    if let Some(mut acceptor) = svc.acceptor.take() {
        let cancel = cancel.child_token();
        group.push(tokio::spawn(async move {
            let (ctrl_tx, ctrl_rx) = async_broadcast::broadcast(1);
            tokio::select! {
                result = acceptor.acceptor.accept_connection(ctrl_rx) => {
                    result.map_err(|e| RuntimeReason::system_error().to_err().with_source(e))
                }
                _ = cancel.cancelled() => {
                    let _ = ctrl_tx.broadcast(wp_connector_api::ControlEvent::Stop).await;
                    Ok(())
                },
            }
        }));
        spawned += 1;
    }

    for mut handle in svc.sources {
        let router = Arc::clone(router);
        let metrics = metrics.clone();
        let cancel = cancel.child_token();
        let stream_name = stream_name.clone();
        let stream_tag_field = stream_tag_field.clone();
        let source_name = source.effective_name(source_idx);
        let source_kind = source_kind.to_string();
        let schema = Arc::clone(&schema);
        let schemas = Arc::clone(schemas);
        let parse_tx = parse_tx.clone();
        let preread = preread.clone();
        let parse_seq = Arc::clone(&parse_seq);
        let limiter = ingest_limiter.clone();
        group.push(tokio::spawn(async move {
            // Start the source if needed (e.g. TCP source checks started flag).
            let (_ctrl_tx, ctrl_rx) = async_broadcast::broadcast(1);
            let _ = handle.source.start(ctrl_rx).await;

            // Wrap the raw DataSource as a BatchSource — all Arrow IPC / NDJSON
            // decode happens inside the adapter, returning Vec<RecordBatch>.
            let mut batch_source = DataSourceBatchSource::new(
                handle.metadata.name.clone(),
                handle.source,
                schema,
                format,
                schemas,
                stream_tag_field.clone(),
                matches!(format, WireFormat::Ndjson) && stream_name.trim().is_empty(),
            );

            let mut consecutive_errors: u32 = 0;
            'outer: loop {
                tokio::select! {
                    result = batch_source.receive_batch() => match result {
                        Ok(batches) => {
                            consecutive_errors = 0;
                            for miss in batch_source.take_window_misses() {
                                crate::receiver::report_window_miss(
                                    &source_name,
                                    &source_kind,
                                    &miss,
                                    metrics.as_ref(),
                                    Some(router.as_ref()),
                                );
                            }
                            if batches.is_empty() {
                                continue;
                            }
                            for rb in batches {
                                // For ArrowFramed, prefer the per-frame tag
                                // (stream name embedded in the wp_arrow IPC header)
                                // when no explicit stream is configured.
                                let route_stream =
                                    if stream_name.is_empty() {
                                        batch_source
                                            .next_stream_tag()
                                            .unwrap_or_else(|| stream_name.clone())
                                    } else {
                                        stream_name.clone()
                                    };
                                if router.registry().subscribers_of(&route_stream).is_empty() {
                                    let route_tag_field = if stream_name.is_empty()
                                        && matches!(format, WireFormat::ArrowFramed)
                                    {
                                        "wp_arrow_tag"
                                    } else {
                                        stream_tag_field.as_str()
                                    };
                                    crate::receiver::record_batch_window_miss(
                                        &source_name,
                                        &source_kind,
                                        route_tag_field,
                                        &route_stream,
                                        rb.num_rows(),
                                        metrics.as_ref(),
                                        Some(router.as_ref()),
                                    );
                                    continue;
                                }
                                // Project + hand off to the parse worker pool.
                                // The source no longer parses (batch_to_events);
                                // it only decodes, projects, and pushes (R2/R3).
                                if !push_decoded_batch(
                                    &parse_tx,
                                    &preread,
                                    &parse_seq,
                                    &source_name,
                                    &route_stream,
                                    rb,
                                    router.as_ref(),
                                    metrics.as_ref(),
                                    limiter.as_deref(),
                                )
                                .await
                                {
                                    // Parse pool shut down.
                                    break 'outer;
                                }
                            }
                        }
                        Err(e) => {
                            // EOF: source has ended — stop the task.
                            if e.reason() == &wf_connector_api::SourceReason::EOF {
                                wf_debug!(
                                    conn,
                                    kind = %source_kind,
                                    stream = %stream_name,
                                    "source reached EOF"
                                );
                                break;
                            }
                            if consecutive_errors == 0 {
                                wf_warn!(
                                    conn,
                                    kind = %source_kind,
                                    stream = %stream_name,
                                    error = %e,
                                    "source receive error, will retry"
                                );
                            }
                            if let Some(metrics) = &metrics {
                                metrics.inc_receiver_decode_error();
                                metrics.inc_receiver_source_decode_error(&source_name);
                            }
                            consecutive_errors = consecutive_errors.saturating_add(1);
                            let delay = if consecutive_errors <= 1 {
                                std::time::Duration::from_millis(500)
                            } else {
                                std::time::Duration::from_secs(5)
                            };
                            tokio::time::sleep(delay).await;
                        }
                    },
                    _ = cancel.cancelled() => break,
                }
            }
            Ok(())
        }));
        spawned += 1;
    }

    if spawned == 0 {
        return RuntimeReason::Bootstrap
            .to_err()
            .with_detail(format!(
                "source kind {:?} built no readable source handles",
                source_kind
            ))
            .err();
    }

    Ok(spawned)
}

fn source_param_to_json(value: &str) -> serde_json::Value {
    let trimmed = value.trim();
    match trimmed {
        "true" => return serde_json::Value::Bool(true),
        "false" => return serde_json::Value::Bool(false),
        _ => {}
    }
    if let Ok(parsed) = trimmed.parse::<i64>() {
        return serde_json::Value::Number(parsed.into());
    }
    if let Ok(parsed) = trimmed.parse::<f64>()
        && let Some(number) = serde_json::Number::from_f64(parsed)
    {
        return serde_json::Value::Number(number);
    }
    serde_json::Value::String(value.to_string())
}

pub(super) async fn spawn_metrics_task(
    config: &FusionConfig,
    router: &Arc<Router>,
    cancel: CancellationToken,
    metrics: Option<Arc<RuntimeMetrics>>,
    dispatcher: Option<Arc<SinkDispatcher>>,
) -> RuntimeResult<TaskGroup> {
    let mut group = TaskGroup::new("metrics");
    if !config.metrics.enabled {
        return Ok(group);
    }
    let Some(metrics) = metrics else {
        return Ok(group);
    };
    let router_clone = Arc::clone(router);
    let metrics_config = config.metrics.clone();

    // Create monitor channel if dispatcher is available
    let mon_send = match dispatcher {
        Some(ref d) if d.has_monitor_sinks() => {
            let (tx, rx) = mpsc::channel::<Vec<MetricsRecord>>(64);
            let d = Arc::clone(d);
            tokio::spawn(async move {
                run_monitor_consumer(rx, d).await;
            });
            Some(tx)
        }
        _ => None,
    };

    group.push(tokio::spawn(async move {
        run_metrics_task(metrics, metrics_config, router_clone, cancel, mon_send)
            .await
            .source_err(RuntimeReason::system_error(), "run metrics task")?;
        Ok(())
    }));
    Ok(group)
}

async fn run_monitor_consumer(mut rx: MonRecv, dispatcher: Arc<SinkDispatcher>) {
    while let Some(records) = rx.recv().await {
        for record in records {
            let data = metrics_record_to_data_record(&record);
            dispatcher.dispatch_to_monitor(&data).await;
        }
    }
    // Monitor channel closed: stop the monitor sinks.
    dispatcher.stop_monitor_sinks().await;
}

fn metrics_record_to_data_record(record: &MetricsRecord) -> DataRecord {
    let mut out = DataRecord::default();
    for (key, value) in &record.fields {
        let field = Field::new(DataType::Chars, key, Value::from(value.as_str()));
        out.push(FieldStorage::from_owned(field));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::source_param_to_json;

    #[test]
    fn source_param_to_json_preserves_connector_types() {
        assert_eq!(source_param_to_json("5514"), serde_json::json!(5514));
        assert_eq!(source_param_to_json("true"), serde_json::json!(true));
        assert_eq!(source_param_to_json("1.5"), serde_json::json!(1.5));
        assert_eq!(
            source_param_to_json("0.0.0.0"),
            serde_json::json!("0.0.0.0")
        );
    }
}
