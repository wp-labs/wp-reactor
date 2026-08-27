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
use wf_lang::ast::Expr;
use wf_lang::plan::RulePlan;

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
pub(crate) const RULE_CHANNEL_CAPACITY: usize = 64;
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
/// stats spill 文件路径（M4）: `WF_SPILL_DIR`（默认 `spill`）下的
/// `spill_{rule}_{pid}{_shard}.rb`。窗口级生命周期：close 后 `cleanup` 删除；
/// 进程异常退出残留由下次启动清理（设计 §8 时机④）。
/// `shard`：key 分片时每片独立文件（分片 executor 各自独立 spill）。
fn spill_file_path(rule_name: &str, shard: Option<usize>) -> PathBuf {
    let dir = std::env::var("WF_SPILL_DIR").unwrap_or_else(|_| "spill".to_string());
    let dir_path = Path::new(&dir);
    if let Err(e) = std::fs::create_dir_all(dir_path) {
        log::warn!("spill 目录创建失败 {}: {e}", dir_path.display());
    }
    let safe: String = rule_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    match shard {
        Some(s) => dir_path.join(format!("spill_{safe}_{}_{s}.rb", std::process::id())),
        None => dir_path.join(format!("spill_{safe}_{}.rb", std::process::id())),
    }
}

/// stats last/top（P4, Q18/Q19）的行字段提取子集: yield/entity 引用字段 ∪ 度量
/// 字段。桶键字段不入行（close 已单独注入 scope_key）。`None` = 全部 schema 列
/// （计划无 last/top 时无需提取——`None` 让执行器跳过整行提取）。
fn stats_row_fields(
    plan: &RulePlan,
    stats_plan: &wf_lang::plan::StatsPlan,
) -> Option<std::sync::Arc<HashSet<String>>> {
    let has_row_measures = stats_plan.measures.iter().any(|m| {
        matches!(
            m.agg,
            wf_lang::plan::StatsAggPlan::Last | wf_lang::plan::StatsAggPlan::Top
        )
    });
    if !has_row_measures {
        return None;
    }
    let mut fields = HashSet::new();
    for f in &plan.yield_plan.fields {
        collect_expr_field_names(&f.value, &mut fields);
    }
    collect_expr_field_names(&plan.entity_plan.entity_id_expr, &mut fields);
    collect_expr_field_names(&plan.score_plan.expr, &mut fields);
    for m in &stats_plan.measures {
        if let Some(fr) = &m.field {
            let n = wf_engine::match_engine::field_ref_name(fr);
            if !n.is_empty() {
                fields.insert(n.to_string());
            }
        }
    }
    // 桶键字段不入行（P5+ 优化: close 已从 scope_key 注入 field_values, 行字段
    // 重复存一份纯属浪费——Q18 键 bidder/auction 去掉后子集 6 → 4 字段, 提取量
    // 与内存 -33%）。只排除简单字段键; 函数键（tier/bucket）yield 不直接读。
    // 注意: 若 last/top 度量字段恰为桶键（如 last(b.auction)）, 其度量值会退化
    // 0.0——yield 读 b.auction 仍经 scope_key 注入, 实际输出不受影响。
    for k in &stats_plan.keys {
        if let Expr::Field(fr) = k {
            let n = wf_engine::match_engine::field_ref_name(fr);
            if !n.is_empty() {
                fields.remove(n);
            }
        }
    }
    Some(std::sync::Arc::new(fields))
}

/// 表达式内全部字段名（stats 规则无 join, 所有引用均属 `b` 别名）。
fn collect_expr_field_names(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::Field(fr) => {
            let n = wf_engine::match_engine::field_ref_name(fr);
            if !n.is_empty() {
                out.insert(n.to_string());
            }
        }
        Expr::BinOp { left, right, .. } => {
            collect_expr_field_names(left, out);
            collect_expr_field_names(right, out);
        }
        Expr::FuncCall { args, .. } => {
            for a in args {
                collect_expr_field_names(a, out);
            }
        }
        Expr::Neg(inner) => collect_expr_field_names(inner, out),
        Expr::Not(inner) => collect_expr_field_names(inner, out),
        _ => {}
    }
}

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

/// 同 [`register_progress`]，但注册为 **row-partitioned** 消费者（key / 行号
/// 分片的 match/stats 任务）：每片只处理自己的行子集，一个批次只有在**所有**
/// 分片都 ack 过之后才算被完全消费——完成判定用 min（2026-08-25 review：
/// `wait_for_data_drain` 的 `max||min` 会在最快分片追平时提前排空，慢分片
/// 仍在处理）。驱逐保护（全局 min over 两组）不受影响。
fn register_row_partitioned_progress(
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
                .register_row_partitioned();
            (src.window_name.clone(), slot)
        })
        .collect()
}

/// 收集被**顺序敏感**下游消费的 intermediate target（2026-08-25 review）：
/// Match 状态机（同 key 保序）与 stats 的 last/top 聚合（按行序取极值）都
/// 不允许上游分片乱序——它们的中间窗上游 each 规则必须保持单 worker。
/// 可交换 stats（count/sum/min/max/distinct）与 on-each 下游容忍乱序。
fn collect_order_sensitive_targets(
    rules: &[RunRule],
    intermediate_targets: &HashSet<String>,
) -> HashSet<String> {
    rules
        .iter()
        .filter(|r| match &r.kind {
            RunRuleKind::Match { .. } => true,
            RunRuleKind::Stats { stats_plan, .. } => stats_plan.measures.iter().any(|m| {
                matches!(
                    m.agg,
                    wf_lang::plan::StatsAggPlan::Last | wf_lang::plan::StatsAggPlan::Top
                )
            }),
            RunRuleKind::Each { .. } => false,
        })
        .flat_map(|r| r.executor.plan().binds.iter().map(|b| b.window.clone()))
        .filter(|w| intermediate_targets.contains(w))
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

    // 被**顺序敏感**下游消费的 intermediate target（2026-08-24 q4 分片 +
    // 2026-08-25 review 补 stats last/top）：这些下游对同 key 事件顺序敏感
    // （保序），上游 each 规则分片输出乱序会破坏语义 → 相关 target 的上游
    // each 规则保持单 worker。
    // - Match 状态机：同 key 事件必须按序进入（2026-08-24）；
    // - stats 的 last/top 聚合：按行序取极值，跨片乱序会选错行。
    // stats 可交换聚合（count/sum/min/max/distinct）与 on-each 下游容忍乱序。
    let order_sensitive_targets = collect_order_sensitive_targets(&rules, intermediate_targets);

    for rule in rules {
        // join 目标窗口接索引（2026-08 RSS/EPS 归因：生产此前未接线
        // `set_join_key` → join_lookup 每事件全量扫描 → q13 等 join 查询
        // CPU 瓶颈 + 消费跟不上 → pull 模式 ack floor 阻止时间驱逐 → 积压
        // RSS 线性涨）。索引按键的 batch seq 维护（M2 seq-cut），pull 模式
        // 也走 O(1) 路径。多条件 join 索引首条件右字段（与 first_join_key 一致）。
        for join in &rule.executor.plan().joins {
            let Some(key) = join
                .conds
                .first()
                .map(|c| wf_engine::match_engine::field_ref_name(&c.right))
            else {
                continue;
            };
            if key.is_empty() {
                continue;
            }
            // 2026-08-23 q13：provider 窗口（side_input）同样接 join 索引——
            // 无索引时 join_lookup 全表扫描（10000 行 × 每事件 → q13b 卡死）。
            if let Some(win) = router.registry().get_window(&join.right_window) {
                win.set_join_key(key.to_string());
                // D4：join 目标窗口在此**同步**预注册保留 pin（deferred `emit at` 与
                // snapshot/asof 两类读者都要）。必须在这里（而不是任务 future 内）：
                // `spawn_rule_tasks` 返回后立即 `spawn_receiver_task` 开始摄入，而
                // 规则任务是 `tokio::spawn` 的，在 future 里注册会与首批 append 竞争
                // ——q4 30M 因此非确定性丢 0~6% 输出（启动期 5 vs 48 次驱逐清扫，
                // 2026-08-24）。
                // - deferred（`emit at`）：需要 `[lo_ns, hi_ns]` 内的行直到到期评估，
                //   按挂起集合推进前沿（见 `DeferredRuntime::publish_retention_floor`）；
                // - snapshot/asof：语义 = join 时刻的完整状态，驱动事件可引用任意老
                //   的实体行（q3 join person / q6·q20 join auction）——**无法按事件
                //   时间精确化**，全保留（i64::MIN）直到任务结束，pin drop 自动释放。
                if join.emit_at.is_some()
                    || matches!(
                        join.mode,
                        wf_lang::ast::JoinMode::Snapshot | wf_lang::ast::JoinMode::Asof { .. }
                    )
                {
                    win.preregister_retention_pin();
                }
            } else if let Some(pw) = router.registry().get_provider(&join.right_window) {
                pw.write()
                    .expect("provider window lock poisoned")
                    .set_join_key(key.to_string());
            }
        }

        let window_sources = resolve_window_sources(&rule.window_aliases, router.registry());

        match rule.kind {
            RunRuleKind::Stats {
                stats_plan,
                time_field,
            } => {
                // 声明式窗口统计: 消费 fanout 投递的 raw RecordBatch（push）或
                // window log（pull）, 列式 process_batch（失败回退行式）, 固定窗口
                // close → alert 复用。
                // 分片（P2）: 桶键全为简单字段（fanout 分片按字段）且 shard_count>1
                // → 按键分片多任务（同 key 同片, 片内桶不跨片拆分）; 空键/含函数键
                // → 单实例。
                // last/top（P4, Q18/Q19）: 行字段提取子集 = yield/entity 引用字段 ∪
                // 度量字段——Q18 5.29M 桶 × 整行 8 字段会到 ~19GB, 子集可降 4× 以上。
                let row_fields = stats_row_fields(rule.executor.plan(), &stats_plan);
                let mut stats = wf_engine::match_engine::StatsExecutor::with_row_fields(
                    stats_plan.clone(),
                    row_fields.clone(),
                );
                // 状态内存 guard（2026-08-25）: 规则 `limits.max_memory` →
                // StatsExecutor 超限拒收新键桶（内存有界 + 每窗口告警 + 计数）。
                // None = 不设防（未写 limits 的规则保持原行为）。
                let state_mem_limit: Option<usize> = rule
                    .executor
                    .plan()
                    .limits_plan
                    .as_ref()
                    .and_then(|l| l.max_memory_bytes);
                stats.set_memory_limit(&rule.executor.plan().name, state_mem_limit);
                // 状态外溢（M4, `docs/design/stats-state-spill-redb.md`）:
                // `limits { spill = "redb" }` → redb 落盘、内存只留活跃子集。
                // 仅**单实例**可用（分片组合暂不支持, 见设计 §10）——分片/输入分片
                // 分支下配置了 spill 则告警并忽略。
                let spill_cfg = rule
                    .executor
                    .plan()
                    .limits_plan
                    .as_ref()
                    .and_then(|l| l.spill.as_ref().map(|_| l.max_spill_bytes));
                if let Some(max_spill_bytes) = spill_cfg {
                    stats.set_spill_redb(
                        spill_file_path(&rule.executor.plan().name, None),
                        max_spill_bytes,
                    );
                }
                let field_keys: Vec<FieldRef> = stats
                    .plan
                    .keys
                    .iter()
                    .filter_map(|k| match k {
                        wf_lang::ast::Expr::Field(fr) => Some(fr.clone()),
                        _ => None,
                    })
                    .collect();
                let shardable = !field_keys.is_empty()
                    && field_keys.len() == stats.plan.keys.len()
                    && shard_count > 1;
                // 输入行索引分区（空键 stats, 2026-08-24 q15）: 空键 + 度量全部
                // 可交换（count/sum/min/max/distinct——last/top 行序敏感不可归并）
                // + pull 模式 → 按行号均匀切分多任务, close 时协调片（shard 0）
                // 收齐各片 raw 状态归并后统一 emit（`StatsExecutor::merge_partial`）。
                // push 模式暂不输入分片（broadcast 按 key 分区, 无 index 分区路径）。
                let input_shardable = field_keys.is_empty()
                    && !use_push
                    && shard_count > 1
                    && stats.plan.measures.iter().all(|m| {
                        !matches!(
                            m.agg,
                            wf_lang::plan::StatsAggPlan::Last | wf_lang::plan::StatsAggPlan::Top
                        )
                    });
                if shardable {
                    let keys: Arc<[FieldRef]> = field_keys.into();
                    // M1 pull: 注册 window 键分区, parse 阶段预计算每片行子集。
                    for source in &window_sources {
                        router.fanout().register_window_sharding(
                            &source.window_name,
                            Arc::clone(&keys),
                            shard_count,
                        );
                    }
                    let mut shard_txs = Vec::with_capacity(shard_count);
                    for shard_idx in 0..shard_count {
                        let push_rx = if use_push {
                            let (push_tx, push_rx) =
                                mpsc::channel::<RulePush>(RULE_CHANNEL_CAPACITY);
                            shard_txs.push(push_tx);
                            Some(push_rx)
                        } else {
                            None
                        };
                        let progress = register_row_partitioned_progress(router, &window_sources);
                        let mut shard_stats =
                            wf_engine::match_engine::StatsExecutor::with_row_fields(
                                stats_plan.clone(),
                                row_fields.clone(),
                            );
                        shard_stats.set_memory_limit(&rule.executor.plan().name, state_mem_limit);
                        // key 分片: 每片独立 executor（无跨片 merge）——spill 按片独立
                        // 启用（每片独立文件 + 每片独立写 worker = 多 worker 多文件）。
                        if let Some(max_spill_bytes) = spill_cfg {
                            shard_stats.set_spill_redb(
                                spill_file_path(&rule.executor.plan().name, Some(shard_idx)),
                                max_spill_bytes,
                            );
                        }
                        let task_config = StatsTaskConfig {
                            stats: shard_stats,
                            executor: rule.executor.clone(),
                            window_sources: window_sources.clone(),
                            sink_fanout: Arc::clone(&sink_fanout),
                            cancel: cancel.child_token(),
                            router: Arc::clone(router),
                            metrics: metrics.clone(),
                            time_field: time_field.clone(),
                            timeout_scan_interval,
                            intermediate_targets: intermediate_targets.clone(),
                            pipe_registry: Arc::clone(&pipe_registry),
                            eos_flush: eos_tx.subscribe(),
                            push_rx,
                            progress: progress.clone(),
                            shard_index: Some(shard_idx),
                            shard_count,
                            merge_rx: None,
                            merge_tx: None,
                        };
                        group.push(tokio::spawn(
                            async move { run_stats_task(task_config).await },
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
                } else if input_shardable {
                    // 输入行索引分区注册（parse 阶段按行号预计算每片行子集）。
                    for source in &window_sources {
                        router
                            .fanout()
                            .register_window_index_sharding(&source.window_name, shard_count);
                    }
                    let (merge_tx, merge_rx) =
                        mpsc::channel::<crate::engine_task::StatsPartial>(shard_count.max(8));
                    let mut merge_rx_opt = Some(merge_rx);
                    for shard_idx in 0..shard_count {
                        let push_rx = None;
                        let progress = register_row_partitioned_progress(router, &window_sources);
                        let mut shard_stats =
                            wf_engine::match_engine::StatsExecutor::with_row_fields(
                                stats_plan.clone(),
                                row_fields.clone(),
                            );
                        shard_stats.set_memory_limit(&rule.executor.plan().name, state_mem_limit);
                        if spill_cfg.is_some() {
                            log::warn!(
                                "stats spill 与输入分片组合暂不支持（规则 {}）——本片忽略 spill 配置",
                                rule.executor.plan().name
                            );
                        }
                        let task_config = StatsTaskConfig {
                            stats: shard_stats,
                            executor: rule.executor.clone(),
                            window_sources: window_sources.clone(),
                            sink_fanout: Arc::clone(&sink_fanout),
                            cancel: cancel.child_token(),
                            router: Arc::clone(router),
                            metrics: metrics.clone(),
                            time_field: time_field.clone(),
                            timeout_scan_interval,
                            intermediate_targets: intermediate_targets.clone(),
                            pipe_registry: Arc::clone(&pipe_registry),
                            eos_flush: eos_tx.subscribe(),
                            push_rx,
                            progress: progress.clone(),
                            shard_index: Some(shard_idx),
                            shard_count,
                            // 协调片 = shard 0: 持接收端, close 时收齐 N-1 归并后 emit;
                            // 其余片持发送端, close 时发自身 raw 状态且不 emit。
                            merge_rx: if shard_idx == 0 {
                                merge_rx_opt.take()
                            } else {
                                None
                            },
                            merge_tx: if shard_idx == 0 {
                                None
                            } else {
                                Some(merge_tx.clone())
                            },
                        };
                        group.push(tokio::spawn(
                            async move { run_stats_task(task_config).await },
                        ));
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
                        merge_rx: None,
                        merge_tx: None,
                    };
                    group.push(tokio::spawn(
                        async move { run_stats_task(task_config).await },
                    ));
                }
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
                // P3：deferred join（emit at）规则单 worker——挂起队列是 per-task 状态，
                // 与 round-robin 分片冲突（设计 §9 风险 5）。
                // 2026-08-24 q4 放宽：§9 风险 5 的分片互斥实为 **join-then-key** 场景
                // （路由键来自 join 侧，键在路由前不可得）；deferred 且路由键在驱动
                // 事件上（`key_join.is_none()`）的规则可整批轮转分片——每 worker 独立
                // 挂起队列/watermark，驱动事件整批分配（每个驱动事件恰一次 → 同
                // worker，pending 无跨 worker 依赖），join 目标窗口共享（并发
                // lookup）。到期输出跨 worker 乱序：仅当下游无 Match 状态机
                // （stats/on-each 可交换聚合）时允许。q4a/q8/q9 均满足。
                let plan = rule.executor.plan();
                let deferred = plan.joins.iter().any(|j| j.emit_at.is_some());
                let deferred_shardable = deferred
                    && plan.match_plan.key_join.is_none()
                    && !order_sensitive_targets.contains(&target);
                // bind **中间管道窗口**（上游 yield 的中间窗口）的 each 规则：
                // - 2026-08-23 起强制单 worker（push 广播订阅）——当时的 round-robin
                //   分片走 **pull 光标**，下游独立游标消费滞后于 pipe append（shutdown
                //   时只消化部分批次，q13b 10M 只处理 ~40%、EMIT 不足）。
                // - 2026-08-25 q13 分片：**push 模式 round-robin** 没有该竞态——flush_pipes
                //   广播带真实窗口 seq，每个批次**恰一次**投递到唯一 shard 通道（无共享
                //   游标、无重复、无漏投），stateless each 跨批乱序无害；有界通道背压
                //   传导到上游（q13a → bid_events），内存有界。安全条件：规则非 deferred
                //   （挂起队列是 per-task 状态）、且其输出目标不被 Match 状态机消费
                //   （下游保序敏感）。
                let consumes_intermediate = plan
                    .binds
                    .iter()
                    .any(|b| intermediate_targets.contains(&b.window));
                let intermediate_shard_safe = consumes_intermediate
                    && !deferred
                    && !order_sensitive_targets.contains(&target);
                // **yield 中间管道窗口**（本规则是上游生产者）的 each 规则：
                // 2026-08-25：q13a 分片放开（列式化后分配量级大降）——EPS
                // 1.52M→3.88M 但 RSS 27-30GB 线性增长（窗口内存正常 ~4GB，
                // 疑窗外分配：events 物化/并发分配，用 MIMALLOC_SHOW_STATS
                // 定位中）。安全条件与消费分片同款：非 deferred、输出目标
                // 不被顺序敏感下游消费。
                let yields_intermediate = intermediate_targets.contains(&target);
                let intermediate_producer_shard_safe =
                    !deferred && !order_sensitive_targets.contains(&target);
                let shardable = shard_count > 1
                    && (!consumes_intermediate || intermediate_shard_safe)
                    && (!yields_intermediate || intermediate_producer_shard_safe)
                    && (!deferred || deferred_shardable);
                // 中间窗消费者的投递必须是 push（广播直接投递；pull+Notify 有
                // append/等待时序竞态，见 2026-08-23 注释）——分片版走 round-robin
                // 订阅，单 worker 版走单 sender 广播订阅。
                let wants_push = use_push || consumes_intermediate;

                if shardable {
                    wf_info!(pipe,
                        rule = %plan.name,
                        shards = shard_count,
                        mode = if wants_push { "push-rr" } else { "pull-rr" },
                        kind = "each",
                        "rule sharded"
                    );
                    let mut shard_txs = Vec::with_capacity(shard_count);
                    for shard_idx in 0..shard_count {
                        // Push mode only (or intermediate-window consumers, which
                        // are always push — see `wants_push`): create the delivery
                        // channel. Pull mode carries no channel — the task pulls
                        // the shared window log directly (whole-batch round-robin
                        // gated by seq).
                        let push_rx = if wants_push {
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
                    if wants_push {
                        for source in &window_sources {
                            router
                                .fanout()
                                .register_round_robin(&source.window_name, shard_txs.clone());
                        }
                    }
                } else {
                    wf_info!(pipe,
                        rule = %plan.name,
                        kind = "each",
                        reason = if consumes_intermediate && !intermediate_shard_safe {
                            "intermediate-consumer-unsafe"
                        } else if yields_intermediate && !intermediate_producer_shard_safe {
                            "intermediate-producer-unsafe"
                        } else if deferred && !deferred_shardable {
                            "deferred-unsafe"
                        } else {
                            "single-worker"
                        },
                        "rule single-worker"
                    );
                    // 2026-08-23 q13：bind 中间管道窗口的 each 规则强制 push（fanout
                    // 广播订阅）——flush_pipes 的 broadcast_with_batch 直接投递，
                    // 规避 pull+Notify 的通知竞态（append 与 wait 时序错位时下游
                    // 消费停滞：q13b 只处理已拉取批次，EMIT 严重不足）。广播带
                    // **真实窗口批次 seq**（2026-08-23 修），process_push 的 ack
                    // 反映真实消费进度（saturating 防 MAX+1 回绕）。
                    let push_rx = if use_push || consumes_intermediate {
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
                // A fixed/hop-window conv rule with a generated conv window becomes
                // shardable（hop：桶对齐 slide、封口 size，2026-08-24）; sliding/
                // session conv stays inline. Conv rules that yield to an
                // intermediate pipe stay inline too (the conv stage emits final
                // sink output only).
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
                                // hop：桶对齐 = slide（封口长度仍 = over = size）。
                                bucket_align: cw.slide.unwrap_or(cw.over),
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
                        let progress = register_row_partitioned_progress(router, &window_sources);
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
        let has_window = registry.get_window(window_name).is_some();
        let has_notify = registry.get_notifier(window_name).is_some();
        if let Some(window) = registry.get_window(window_name)
            && let Some(notify) = registry.get_notifier(window_name)
        {
            sources.push(WindowSource {
                window_name: window_name.clone(),
                window,
                notify,
                aliases: aliases.clone(),
            });
        } else {
            wf_warn!(
                conf,
                window = %window_name,
                has_window = has_window,
                has_notify = has_notify,
                "rule window source skipped — window or notifier missing in registry"
            );
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
    // 在途量分账（2026-08-25）：把 preread 预算句柄装给 metrics，周期采样输出
    // `parse.inflight_bytes` / `parse.budget_bytes`——供 `peak_commit − Σwindow_bytes`
    // 的 ~14.7GB 未归因逐段对账（q13 内存 issue §5）。
    if let Some(m) = &metrics {
        let budget = preread.clone();
        wf_info!(
            sys,
            used = budget.used_bytes(),
            capacity = budget.capacity_bytes(),
            "parse inflight gauge provider installed"
        );
        m.set_parse_inflight_provider(move || (budget.used_bytes(), budget.capacity_bytes()));
    }
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
    // Monitor channel closed: this consumer exits, but the monitor sinks are
    // stopped by `Reactor::wait` after the final metrics export (the shutdown
    // flush emits land after the metrics task's last tick, so stopping the
    // sinks here would drop the tail-of-stream counters from metrics.ndjson).
}

pub(crate) fn metrics_record_to_data_record(record: &MetricsRecord) -> DataRecord {
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

    #[test]
    fn stats_row_fields_excludes_key_fields() {
        // P5+ 优化: 桶键字段不入行（close 已从 scope_key 注入 field_values）——
        // Q18 键 bidder/auction 从行字段子集排除, 即使 yield/entity 也引用它们。
        use std::collections::HashMap;
        use wf_lang::ast::{CloseMode, FieldRef, MatchMode};
        use wf_lang::plan::{
            BindPlan, EntityPlan, MatchPlan, ScorePlan, StatsAggPlan, StatsMeasurePlan,
            StatsOutputShapePlan, StatsPlan, WindowSpec, YieldField, YieldPlan,
        };

        let stats_plan = StatsPlan {
            window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
            keys: vec![
                wf_lang::ast::Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
                wf_lang::ast::Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
            ],
            output_shape: StatsOutputShapePlan::Rows,
            measures: vec![StatsMeasurePlan {
                label: "last_price".into(),
                source_alias: "b".into(),
                where_expr: None,
                agg: StatsAggPlan::Last,
                field: Some(FieldRef::Qualified("b".into(), "price".into())),
                arg: None,
            }],
            tracked_bind_fields: HashMap::new(),
        };
        let plan = super::RulePlan {
            name: "q18".into(),
            binds: vec![BindPlan {
                alias: "b".into(),
                window: "bid_events".into(),
                filter: None,
            }],
            lets: vec![],
            match_plan: MatchPlan {
                keys: vec![],
                key_map: None,
                key_join: None,
                window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(10)),
                event_steps: vec![],
                close_steps: vec![],
                close_mode: CloseMode::And,
                match_mode: MatchMode::Seq,
                accu: false,
                seq: None,
                tracked_bind_aliases: std::collections::HashSet::new(),
                tracked_bind_fields: HashMap::new(),
                tracked_plain_fields: std::collections::HashSet::new(),
                needs_field_history: false,
                trigger_event_needed: false,
            },
            each_plan: None,
            stats_plan: Some(stats_plan.clone()),
            joins: vec![],
            r#where: None,
            entity_plan: EntityPlan {
                entity_type: "digit".into(),
                entity_id_expr: wf_lang::ast::Expr::Field(FieldRef::Qualified(
                    "b".into(),
                    "auction".into(),
                )),
            },
            yield_plan: YieldPlan {
                target: "alerts".into(),
                version: None,
                fields: vec![
                    YieldField {
                        name: "id".into(),
                        value: wf_lang::ast::Expr::Field(FieldRef::Qualified(
                            "b".into(),
                            "auction".into(),
                        )),
                    },
                    YieldField {
                        name: "detail".into(),
                        value: wf_lang::ast::Expr::Field(FieldRef::Qualified(
                            "b".into(),
                            "bidder".into(),
                        )),
                    },
                ],
            },
            score_plan: ScorePlan {
                expr: wf_lang::ast::Expr::Number(1.0),
            },
            pattern_origin: None,
            conv_plan: None,
            limits_plan: None,
            conv_window: None,
        };
        let row_fields = super::stats_row_fields(&plan, &stats_plan).expect("last 度量应产出子集");
        assert!(row_fields.contains("price"), "度量字段保留");
        assert!(
            !row_fields.contains("bidder"),
            "桶键不入行（即使 yield 引用）"
        );
        assert!(
            !row_fields.contains("auction"),
            "桶键不入行（即使 entity/yield 引用）"
        );
    }
}

#[cfg(test)]
#[path = "spawn_coverage.rs"]
mod spawn_coverage;
#[cfg(test)]
#[path = "spawn_coverage_more.rs"]
mod spawn_coverage_more;
#[cfg(test)]
#[path = "spawn_r4.rs"]
mod spawn_r4;
