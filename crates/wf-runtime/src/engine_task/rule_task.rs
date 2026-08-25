use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{BooleanArray, new_null_array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use orion_error::conversion::{SourceRawErr, ToStructError};
use tokio::sync::mpsc;

use wf_engine::alert::{AlertColumnBatch, AlertColumnBuilder, OutputRecord};
use wf_engine::match_engine::{
    CepStateMachine, CloseReason, ColumnarEvent, Event, FieldIndex, FieldSource, GuardMasks,
    RuleExecutor, StepResult, batch_event_time_nanos_at, batch_time_col_index, batch_to_events,
    batch_to_events_filtered, build_field_index, close_is_qualified,
};
use wf_engine::normalize_epoch_timestamp_float_nanos;
use wf_engine::window::{Router, RulePush};
use wf_lang::plan::{ConvPlan, WindowSpec};
use wf_lang::wfu_meta::{WFU_ID, WFU_INTERMEDIATE_META_FIELDS, WfuIntermediateMetaField};

use crate::alert_task::SinkFanout;
use crate::engine_task::conv_stage::{ConvCloseBatch, ConvShardSink};
use crate::error::{RuntimeReason, RuntimeResult};
use crate::metrics::RuntimeMetrics;

use wf_engine::match_engine::DeferredPending;

use super::TASK_SEQ;
use super::task_types::{RuleTaskConfig, WindowSource};
use super::window_lookup::RegistryLookup;

const PIPE_EVENT_TIME_FIELD: &str = "__wf_pipe_ts";
const DEBUG_DETAIL_LIMIT: usize = 20;

// 规则相位 profile 计时开关（scan/advance/emit/close_exec/exec 每行 Instant::now
// + elapsed，仅为 dump_profiling 日志服务）。采样实测（qradar c_* 家族）时钟调用
// 占活跃 CPU ~7.6%——默认保持开启（兼容既有诊断），压测场景经
// [`set_rule_profiling`] 关闭（零时钟开销）。
static RULE_PROFILING: AtomicBool = AtomicBool::new(true);

/// 开关规则相位计时（false = 热路径免 clock_gettime，相位日志归零）。
pub fn set_rule_profiling(enabled: bool) {
    RULE_PROFILING.store(enabled, Ordering::Relaxed);
}

#[inline]
fn rule_profiling() -> Option<Instant> {
    RULE_PROFILING.load(Ordering::Relaxed).then(Instant::now)
}

/// Pull-path pending rows: (alias, cursor, event arcs) per source window.
/// Pulled batches for one round of [`RuleTask::pull_and_advance`], collected
/// up front so the subsequent `process_batch` (`&mut self`) calls don't
/// conflict with the `&self.sources` borrow. Tuple:
/// `(window_name, first_batch_seq, batches, shard_rows_per_batch,
///  materialize_fields, key_partitioned, new_cursor)`.
type PendingAliasRows = Vec<(
    String,
    u64,
    Vec<RecordBatch>,
    Vec<Option<Arc<Vec<u32>>>>,
    Option<Arc<HashSet<String>>>,
    bool,
    u64,
)>;
/// Staged pipe batch: (window name, events) or `None` when nothing staged.
type PendingEventBatch = Option<(Arc<str>, Arc<Vec<Arc<Event>>>, RecordBatch)>;
/// Batch the allocation-heavy per-alert telemetry (detail map + e2e latency
/// histogram): only 1 in N emitted alerts updates those, the exact total is
/// always counted.
const EMIT_METRIC_SAMPLE_INTERVAL: u32 = 64;
/// Flush size for the batched alert sink delivery (amortizes per-alert fan-out).
const ALERT_BATCH_SIZE: usize = 4096;

/// Deferred-materialization row source for one batch (L2): the event time of
/// every row (for the watermark/expiry scan) plus the bind-filter hit rows in
/// ascending batch-row order. Hit rows are fed to the state machine as
/// [`ColumnarEvent`] views (P3 FieldView) — no per-row HashMap materialization.
struct DeferredRows<'a> {
    times: Vec<i64>,
    hit_indices: Vec<u32>,
    batch: &'a RecordBatch,
    index: Arc<FieldIndex>,
    /// The window's `materialize_fields` read-set projection: the columnar
    /// `to_event()` materializes only these fields on emit, matching the eager
    /// deferred path's projected trigger event.
    projection: Option<Arc<HashSet<String>>>,
}

/// A per-row field source for the state-machine loop: either the eager
/// materialized [`Event`] or a deferred [`ColumnarEvent`] view (P3 FieldView).
/// Implements [`FieldSource`] so the generic machine consumes either unchanged.
enum RowEvent<'a> {
    Eager(&'a Event),
    Columnar(ColumnarEvent<'a>),
}

impl FieldSource for RowEvent<'_> {
    fn field_value(&self, name: &str) -> Option<wf_engine::match_engine::Value> {
        match self {
            RowEvent::Eager(e) => e.field_value(name),
            RowEvent::Columnar(c) => c.field_value(name),
        }
    }

    fn field_names(&self) -> Vec<&str> {
        match self {
            RowEvent::Eager(e) => e.field_names(),
            RowEvent::Columnar(c) => c.field_names(),
        }
    }

    fn to_event(&self) -> Event {
        match self {
            RowEvent::Eager(e) => e.to_event(),
            RowEvent::Columnar(c) => c.to_event(),
        }
    }
}

/// Columnar accumulation of pending alerts, grouped by yield target.
///
/// Records go straight from `OutputRecord` into per-field columns (no
/// per-row `DataRecord` materialization on the emit path); `flush_alerts`
/// seals each target's builder into one `AlertColumnBatch` for the sink
/// channel. See `AlertColumnBatch` for the memory rationale.
#[derive(Default)]
struct PendingAlertColumns {
    /// Yield targets are few (typically 1-2 per rule) — a linear scan beats
    /// hashing the target string on every append.
    by_target: Vec<(std::sync::Arc<str>, AlertColumnBuilder)>,
    count: usize,
}

/// P3：deferred join（`emit at`）运行时状态——挂起队列 + 事件时间 watermark。
///
/// 驱动事件到达时挂起（expiry = `emit at` 求值），watermark ≥ expiry 时到期评估
/// （asof_candidates → 区间过滤 → reduce/exists → 输出）。watermark 仅由事件时间
/// 驱动（不叠加墙钟）——replay 对拍依赖事件时间序，墙钟推进会提前触发。
struct DeferredRuntime {
    /// 挂起实例（每驱动行一条，设计 §5.2）。
    pending: Vec<DeferredPending>,
    /// 到期评估 miss 的实例（join 目标窗口 append 滞后，2026-08-23 q8 排查）：
    /// 到期时 join 目标可能未追平（引擎流式 vs oracle 预加载），EOS flush 时
    /// 重试一次（届时所有数据已 ingest、目标窗口完整）——q8 引擎 33k vs
    /// oracle 82k 的根因修复。EOS 重试后仍 miss = 真 miss（auction 不存在）。
    missed: Vec<DeferredPending>,
    /// 事件时间 watermark（本规则驱动流 max event ts）。
    watermark: i64,
    /// 驱动 join 索引（规则内第一个带 `emit at` 的 join；v1 单 deferred join）。
    join_idx: usize,
    /// D4 保留 pin：向 join 目标窗口发布「本规则还可能需要的最早事件时间」，
    /// 内存驱逐据此拒绝丢弃这些行（join 目标读者没有 pull 消费者槽位，
    /// `min_acked` 保护不到它 —— q9/q4a 30M −62% 的根因）。`None` = 窗口未接
    /// progress 表（单测窗口）或 join 目标不是 buffer 窗口（provider 静态表
    /// 不驱逐，无需 pin）。
    retention_pin: Option<Arc<std::sync::atomic::AtomicI64>>,
    /// 存活挂起（pending + missed）的 min(lo_ns) 缓存（2026-08-25 q4 100M）：
    /// `publish_retention_floor` 不再每次全量扫 O(n)。插入时 O(1) 更新；
    /// scan 移出前缀后若最小项被移出则标 dirty，publish 时重扫——因 min lo
    /// 项几乎总是最早挂起（数据时间单调），dirty 极少，摊销 O(1)。
    lo_min: i64,
    lo_min_dirty: bool,
}

impl DeferredRuntime {
    /// D4：把本规则的保留前沿发布到 join 目标窗口。
    ///
    /// 前沿 = 存活挂起实例的 `min(lo_ns)`——每个实例需要 `[lo_ns, hi_ns]` 内的
    /// 右窗行，比最早的 `lo_ns` 更旧的行任何实例都用不到。无挂起（含 missed）时
    /// 退回本规则 watermark：**这依赖驱动流事件时间单调**——未来挂起实例由更晚
    /// 的驱动事件产生，其 `lo_ns` 不会早于 watermark。若驱动流乱序（多生产者
    /// 交错摄入），乱序到达的实例会在评估时 miss 进 `missed`，把前沿拉回其
    /// `lo_ns`（见下）——乱序深度大于首个 miss 之前的窗口期才可能丢行；
    /// 实测（nexmark 30M 10 生产者）首 miss 极早发生，前沿随即被拉低，无丢失。
    ///
    /// ⚠ watermark 尚未初始化（`i64::MIN`，还没见过驱动事件）时**就发布
    /// `i64::MIN` = 全保留**：此时本规则还不知道自己的前沿，不能放行。曾把它
    /// 映射成 `i64::MAX`（“无所需”），结果启动时的定时扫描（1s 间隔）先于首批
    /// 驱动事件触发，把刚预注册的 pin 立即释放 → q4 30M 仍丢 0.67% 输出
    ///（2026-08-24 实测：驱逐告警里 `retention_floor_ns=i64::MAX`）。
    ///
    /// “驱动流始终无数据 → 永久 pin”不会无界增长：pin 只阻断**内存上限**驱逐，
    /// `over` 的时间驱逐故意忽略 pin（见 `evict_expired`），所以保留量的上界仍是
    /// `over`。EOS 时另有 `release_retention_floor` 显式释放。
    ///
    /// `missed`（到期 miss、EOS 重试）**也计入前沿**：它们 miss 的原因是 join 目标
    /// append 滞后（需要的行还没到），那些行稍后才落地，必须活到 EOS 重试那一刻。
    /// 曾把 `missed` 排除在外（“pin 住更旧的行救不了它”），q4 30M 因此仍丢 0.67%
    /// 输出（1,661,399 vs 1,672,559）——q4a 比 q9 多一条 q4b 规则、任务更慢 → miss 更
    /// 多 → 更依赖 EOS 重试，而那 5 次内存驱逐正好抽走了它们要重试的行（同次 q9
    /// 同样 5 次驱逐却 identical，差别就在 miss 量）。
    ///
    /// 内存上界不变：`missed` 把前沿冻结在早期事件时间，但保留量仍由 `over` 的
    /// 时间驱逐封顶（时间驱逐故意忽略 pin）。
    fn publish_retention_floor(&mut self) {
        let Some(pin) = &self.retention_pin else {
            return;
        };
        // 2026-08-25 q4 100M：缓存 min(lo_ns)（插入 O(1) 更新；dirty 才重扫）
        // ——旧的每 batch 全量扫 pending+missed 在 33M 挂起下是第二个 O(n²)。
        // 缓存仅在维护路径上可靠：直接构造/绕过维护（测试、未来新路径）时
        // `lo_min == i64::MAX` 且集合非空 → 退回全量扫（正确性兜底，罕见）。
        let cache_trustworthy = !(self.lo_min == i64::MAX
            && (!self.pending.is_empty() || !self.missed.is_empty()));
        let floor = if self.lo_min_dirty || !cache_trustworthy {
            let lo = self
                .pending
                .iter()
                .chain(self.missed.iter())
                .map(|p| p.lo_ns)
                .min()
                .unwrap_or(self.watermark);
            // 重扫后同步缓存（后续 publish 免扫）。
            self.lo_min = lo;
            self.lo_min_dirty = false;
            lo
        } else if self.pending.is_empty() && self.missed.is_empty() {
            // 空集 → watermark（与全量语义一致：更旧的行未来实例也用不到）。
            self.watermark
        } else {
            // 非空 → 历史 min lo_ns（插入时 O(1) 维护的单调不增下界）。
            // 到期项移出不标 dirty（见 scan_deferred 注释）→ 缓存可能停在
            // 已退场实例的更小 lo_ns → 只偏保守（≤ 真实 min），不会丢行；
            // 空集分支已单独处理，非空时 lo_min ≤ 任意存活实例的 lo_ns。
            self.lo_min
        };
        pin.store(floor, std::sync::atomic::Ordering::Release);
    }

    /// 释放 pin（EOS/关停）：窗口恢复完全可驱逐。
    fn release_retention_floor(&self) {
        if let Some(pin) = &self.retention_pin {
            pin.store(i64::MAX, std::sync::atomic::Ordering::Release);
        }
    }
}

/// Current wall-clock epoch nanos.
fn wall_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[derive(Debug, Default)]
struct RuleBatchDebugStats {
    input_events: usize,
    alias_passed: usize,
    alias_rejected: usize,
    accumulated: usize,
    advanced: usize,
    matched: usize,
    output_emitted: usize,
    output_none: usize,
    intermediate_emitted: usize,
    errors: usize,
    detail_logged: usize,
    detail_suppressed: usize,
}

impl RuleBatchDebugStats {
    fn can_log_detail(&self) -> bool {
        self.detail_logged < DEBUG_DETAIL_LIMIT
    }

    fn allow_detail(&mut self) -> bool {
        if self.detail_logged < DEBUG_DETAIL_LIMIT {
            self.detail_logged += 1;
            true
        } else {
            self.detail_suppressed += 1;
            false
        }
    }

    fn count_output(&mut self, record: &OutputRecord, intermediate_targets: &HashSet<String>) {
        if intermediate_targets.contains(&*record.yield_target) {
            self.intermediate_emitted += 1;
        } else {
            self.output_emitted += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// RuleTask -- runtime state for a single rule
// ---------------------------------------------------------------------------

/// Holds all mutable state for one rule's processing loop.
///
/// Each `RuleTask` owns its `CepStateMachine` exclusively (no `Arc<Mutex>`).
pub(super) struct RuleTask {
    pub(super) task_id: String,
    machine: Option<CepStateMachine>,
    each_alias: Option<String>,
    each_time_field: Option<String>,
    executor: RuleExecutor,
    conv_plan: Option<ConvPlan>,
    /// P2c: raw-close routing to the conv stage (sharded conv rules).
    conv_sink: Option<ConvShardSink>,
    pub(super) sources: Vec<WindowSource>,
    /// window_name -> Vec<alias>: pre-computed from stream_aliases + window sources.
    aliases: HashMap<String, Vec<String>>,
    /// window_name -> Vec<alias>: aux bind aliases first, then event aliases.
    ordered_aliases: HashMap<String, Vec<String>>,
    /// window_name -> cursor: tracks read position per window.
    pub(super) cursors: HashMap<String, u64>,
    /// Sink delivery fanout: each emitted alert is broadcast to the per-sink
    /// channels resolved by yield_target.
    sink_fanout: Arc<SinkFanout>,
    /// Shared router for WindowLookup (joins + has()).
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    intermediate_targets: HashSet<String>,
    /// Output/intermediate relay targets (pipe design). The emit path uses this
    /// to identify pipes and route emits through the pipe abstraction.
    pipe_registry: Arc<wf_engine::pipe::PipeRegistry>,
    /// End-of-stream counter (incremented on each EOS event). The task flushes
    /// instances on every EOS but keeps running so a daemon can accept
    /// multiple finite inputs.
    pub(super) eos_flush: tokio::sync::watch::Receiver<u64>,
    /// Wall clock when events were last processed. When input goes idle, this
    /// stays put so the periodic timeout scan can advance the effective watermark
    /// by the elapsed wall time — letting instances expire per their window TTL
    /// even without new events (window semantics, not just event-time).
    last_activity_wall: std::time::Instant,
    /// Periodic timeout-scan interval: the wall-clock expiry advance is **capped**
    /// at this (see `scan_timeouts`) so a slow/backpressured pipeline cannot race
    /// the effective watermark far ahead of event time and snowball into a huge
    /// single expiry sweep that starves pushes.
    timeout_scan_interval: std::time::Duration,
    /// Push-mode input channel (R1). When `Some`, the rule consumes pushed
    /// `Arc<Vec<Arc<Event>>>` instead of pulling from the window read lock; when
    /// `None`, the task falls back to the legacy notify + pull loop. Consumed
    /// once by `run_rule_task`.
    pub(super) push_rx: Option<mpsc::Receiver<RulePush>>,
    /// Monotonic batch sequence for pushed batches (debug event refs only).
    pushed_seq: u64,
    /// Pull-model shard identity (M1, window-actor-pull-model.md §3.1).
    /// `Some(i)` for a sharded rule task: a key-partitioned (match) task pulls
    /// only its `TimedBatch.shard_rows[i]` row subset; an on-each round-robin
    /// task uses `i` as the whole-batch round-robin index. `None` = unsharded.
    shard_index: Option<usize>,
    /// Total shard count this rule is split across (1 when unsharded). Used by
    /// the on-each round-robin gate (`seq % shard_count == shard_index`).
    shard_count: usize,
    /// Profiling accumulators (nanos) for locating the rule-task bottleneck.
    advance_nanos: u64,
    scan_nanos: u64,
    emit_nanos: u64,
    /// Finer emit split: execute_match / to_data_record / fanout handoff.
    /// The to_data_record time is also exported as the `alert.serialize_nanos`
    /// metric (summed across the run).
    exec_nanos: u64,
    /// Finer emit split: execute_close_with_joins (close path output record
    /// construction) — the q12 hot spot; kept separate from `emit_nanos` so the
    /// per-record build vs. the batch append hand-off can be read from the
    /// profiling dump.
    close_exec_nanos: u64,
    serialize_nanos: std::sync::atomic::AtomicU64,
    fanout_nanos: std::sync::atomic::AtomicU64,
    /// Last wall-clock dump of the profiling accumulators (throttled log).
    last_profile_dump: std::time::Instant,
    /// Wall-clock nanos cached once per batch — avoids a `SystemTime::now()`
    /// syscall on every emitted alert.
    cached_wall_nanos: AtomicU64,
    /// Consumption-progress slots by window name. After fully processing a
    /// batch the task acks `seq + 1` (push path: `RulePush.seq`; pull path:
    /// the window batch seq). The evictor uses the minimum over all slots as
    /// the time-eviction floor, so sweeps can never drop unconsumed data.
    /// Released to `u64::MAX` on drop.
    progress: HashMap<String, std::sync::Arc<std::sync::atomic::AtomicU64>>,
    /// Countdown for sampling the allocation-heavy per-alert telemetry.
    emit_sample_remaining: AtomicU32,
    /// Serialize-timing sampler state (1-in-`EMIT_METRIC_SAMPLE_INTERVAL`),
    /// see `emit`.
    serialize_sample_remaining: AtomicU32,
    /// Last value reported to the `rule_instances` gauge. The gauge is the sum
    /// across a rule's shards, so each shard reports the delta since its last
    /// report (P2b).
    last_reported_instances: AtomicI64,
    /// Batched alert delivery: per-yield-target columnar builders flushed to
    /// the sink writers when the batch fills / at EOS. The record→columns
    /// append runs on this thread by design — see [`Self::emit`].
    /// `Mutex` so emit can stay `&self` while RuleTask stays `Sync`.
    pending_alerts: std::sync::Mutex<PendingAlertColumns>,
    /// Intermediate (pipe) relay staging (rule-side channelization): rows
    /// emitted to an intermediate target accumulate in typed column buffers
    /// and are flushed once per input batch — one N-row `RecordBatch`, one
    /// `batch_to_events`, one fanout broadcast — instead of a single-row
    /// Arrow batch + channel send per row. Same relay semantics as the old
    /// per-row `emit_window_record` (pure relay, no window store, seq
    /// `u64::MAX`). `Mutex` so emit can stay `&self`.
    pipe_state: std::sync::Mutex<PipeState>,
    /// On-each rules emitting to a plain sink target use the direct-write
    /// column path (plan C2): the executor appends straight into the
    /// columnar builder with no per-record `OutputRecord`. Intermediate
    /// pipe targets keep the record path for evaluation but stage the rows
    /// columnar-ly for batched relay ([`Self::flush_pipes`]). Constant for
    /// the task's lifetime — decided once here.
    each_direct: bool,
    /// P3：deferred join（`emit at`）挂起队列与到期调度（无 `emit at` 时 `None`）。
    deferred: Option<DeferredRuntime>,
    /// D4 扩展：snapshot/asof join 目标窗口的保留 pin（全保留，见构造处注释）。
    /// 任务存活期间持有强引用；drop 时自动释放（Weak 死 → 窗口恢复可驱逐）。
    /// snapshot/asof join 目标窗口的保留 pin 句柄（D4 扩展）: 取回窗口的
    /// `Arc<AtomicI64>` 并**持有到任务结束**——Arc 活着 = 窗口 progress 里的 pin
    /// 活着（驱逐 `retention_floor_ns` 尊重它）; drop 时 Arc 释放、pin 自动移除。
    /// 从不读: RAII 句柄, 持有即目的（值恒 i64::MIN = 全保留, 见 new() 注释）。
    #[allow(dead_code)]
    snapshot_pins: Vec<Arc<AtomicI64>>,
}

impl Drop for RuleTask {
    fn drop(&mut self) {
        // Release the consumption-progress slots so a task going away does
        // not pin its windows' time-eviction floor forever.
        for slot in self.progress.values() {
            wf_engine::window::WindowProgress::release(slot);
        }
    }
}

impl RuleTask {
    pub(super) fn new(
        config: RuleTaskConfig,
    ) -> (
        Self,
        tokio_util::sync::CancellationToken,
        std::time::Duration,
    ) {
        let RuleTaskConfig {
            machine,
            each_alias,
            each_time_field,
            executor,
            window_sources,
            sink_fanout,
            cancel,
            timeout_scan_interval,
            router,
            metrics,
            intermediate_targets,
            pipe_registry,
            eos_flush,
            push_rx,
            progress,
            conv_sink,
            shard_index,
            shard_count,
        } = config;
        let aliases: HashMap<String, Vec<String>> = window_sources
            .iter()
            .map(|src| (src.window_name.clone(), src.aliases.clone()))
            .collect();
        let ordered_aliases: HashMap<String, Vec<String>> = aliases
            .iter()
            .map(|(window_name, aliases)| {
                let ordered = aliases
                    .iter()
                    .filter(|alias| executor.is_aux_bind_alias(alias.as_str()))
                    .chain(
                        aliases
                            .iter()
                            .filter(|alias| !executor.is_aux_bind_alias(alias.as_str())),
                    )
                    .cloned()
                    .collect();
                (window_name.clone(), ordered)
            })
            .collect();

        // Initialize cursors to current position (skip historical data).
        let cursors: HashMap<String, u64> = window_sources
            .iter()
            .map(|src| {
                let seq = src.window.next_seq();
                (src.window_name.clone(), seq)
            })
            .collect();

        let seq = TASK_SEQ.fetch_add(1, Ordering::Relaxed);
        let rule_name = executor.plan().name.clone();
        let task_id = format!("{}#{}", rule_name, seq);
        let conv_plan = executor.plan().conv_plan.clone();
        // Direct-write on-each emit only when the target is a sink target:
        // intermediate pipes still consume full `OutputRecord` rows.
        let each_direct = executor.plan().each_plan.is_some()
            && !intermediate_targets.contains(executor.plan().yield_plan.target.as_str());
        // P3：第一个带 `emit at` 的 join 是 deferred 驱动（v1 单 deferred join，设计 §9 风险 5）
        let deferred = executor
            .plan()
            .joins
            .iter()
            .position(|j| j.emit_at.is_some())
            .map(|join_idx| {
                // D4：在 join 目标窗口取走保留 pin（spawn 阶段已同步预注册，避免与首批
                // append 竞争；无预注册时当场注册一个）。deferred 规则不从右窗 pull
                //（只做点查询）→ 无消费者槽位 → `min_acked` 对它报 u64::MAX（全部可
                // 驱逐），字节上限一旦成为约束就会静默丢掉到期评估还要用的行
                //（q9/q4a 30M −62%，2026-08-24）。pin 按自身评估前沿推进，见
                // `publish_retention_floor`。
                let retention_pin = router
                    .registry()
                    .get_window(&executor.plan().joins[join_idx].right_window)
                    .and_then(|w| w.take_retention_pin());
                DeferredRuntime {
                    pending: Vec::new(),
                    missed: Vec::new(),
                    watermark: i64::MIN,
                    join_idx,
                    retention_pin,
                    lo_min: i64::MAX,
                    lo_min_dirty: false,
                }
            });
        // D4 扩展（2026-08-24）：snapshot/asof join 目标窗口同样持有保留 pin。
        // snapshot 语义 = join 时刻的完整状态，驱动事件可引用**任意老**的实体行
        // （q3 join person / q6·q20 join auction）——无法像 deferred 那样按
        // `min(lo_ns)` 精确化前沿，只能全保留（`i64::MIN`）直到任务结束（Arc drop
        // → Weak 死 → 自动释放）。实体表目标（person/auction @30M 全量 ~470MB）内存
        // 代价可忽略，且由 `over` 时间驱逐封顶；这正是「2GB 字节上限恰好够大」
        // 背后的预算兵役的引擎化——上限收紧或数据变大时不再静默丢输出。
        let snapshot_pins: Vec<Arc<AtomicI64>> = executor
            .plan()
            .joins
            .iter()
            .filter(|j| {
                j.emit_at.is_none()
                    && matches!(
                        j.mode,
                        wf_lang::ast::JoinMode::Snapshot | wf_lang::ast::JoinMode::Asof { .. }
                    )
            })
            .filter_map(|j| {
                router
                    .registry()
                    .get_window(&j.right_window)
                    .and_then(|w| w.take_retention_pin())
            })
            .collect();

        let task = Self {
            task_id,
            machine,
            each_alias,
            each_time_field,
            executor,
            conv_plan,
            conv_sink,
            sources: window_sources,
            aliases,
            ordered_aliases,
            sink_fanout,
            cursors,
            router,
            metrics,
            intermediate_targets,
            pipe_registry,
            eos_flush,
            last_activity_wall: std::time::Instant::now(),
            timeout_scan_interval,
            push_rx,
            pushed_seq: 0,
            shard_index,
            shard_count,
            progress,
            advance_nanos: 0,
            scan_nanos: 0,
            emit_nanos: 0,
            exec_nanos: 0,
            close_exec_nanos: 0,
            serialize_nanos: std::sync::atomic::AtomicU64::new(0),
            fanout_nanos: std::sync::atomic::AtomicU64::new(0),
            last_profile_dump: std::time::Instant::now(),
            cached_wall_nanos: AtomicU64::new(wall_nanos()),
            emit_sample_remaining: AtomicU32::new(EMIT_METRIC_SAMPLE_INTERVAL),
            serialize_sample_remaining: AtomicU32::new(EMIT_METRIC_SAMPLE_INTERVAL),
            last_reported_instances: AtomicI64::new(0),
            pending_alerts: std::sync::Mutex::new(PendingAlertColumns::default()),
            pipe_state: std::sync::Mutex::new(PipeState::Uninit),
            each_direct,
            deferred,
            snapshot_pins,
        };
        (task, cancel, timeout_scan_interval)
    }

    fn rule_name(&self) -> &str {
        self.executor.plan().name.as_str()
    }

    fn instance_count(&self) -> usize {
        self.machine
            .as_ref()
            .map(|machine| machine.instance_count())
            .unwrap_or(0)
    }

    // -- Data processing ----------------------------------------------------

    /// Read new batches from all windows and advance the state machine.
    ///
    /// **Pull-model (M1, window-actor-pull-model.md §3.3).** Columnar pull:
    /// each window's shared `RecordBatch` Arcs are read once (zero data copy)
    /// and fed straight into [`Self::process_batch`]'s columnar entry point —
    /// replacing the legacy `events_since` row-based path. Sharding is handled
    /// without re-partitioning:
    ///
    /// - *Key-partitioned (match) windows* — the parse stage already computed
    ///   the per-shard row subset and stored it in `TimedBatch.shard_rows`. A
    ///   sharded task pulls only its `shard_rows[i]` rows (P2 zero re-partition);
    ///   `read_since_with_shard(cursor, Some(i))` returns that subset.
    /// - *On-each round-robin / unsharded windows* — `read_since_with_shard`
    ///   returns the whole batch; a round-robin task processes a batch only
    ///   when `seq % shard_count == shard_index` (whole-batch round-robin,
    ///   identical to the legacy `register_round_robin` semantics).
    pub(super) async fn pull_and_advance(&mut self) {
        // Phase 1: collect pulled batches per window. This phase only takes
        // disjoint field borrows, so it must stay free of `&mut self` calls
        // (the `&self.sources` borrow would conflict with `process_batch`).
        let mut pending: PendingAliasRows = Vec::new();
        for source in &self.sources {
            let cursor = self.cursors.get(&source.window_name).copied().unwrap_or(0);
            // Key-partitioned (match) windows yield per-shard row subsets;
            // everything else (on-each round-robin, unsharded) pulls whole
            // batches and is gated below.
            let key_partitioned = self.router.fanout().window_is_sharded(&source.window_name);
            let pull_shard = if key_partitioned {
                self.shard_index
            } else {
                None
            };
            let (batches, shard_rows_per_batch, new_cursor, gap) =
                source.window.read_since_with_shard(cursor, pull_shard);
            wf_debug!(pipe,
                task_id = %self.task_id,
                window = %source.window_name,
                cursor = cursor,
                new_cursor = new_cursor,
                batches = batches.len(),
                gap = gap,
                "read_since_with_shard"
            );

            if gap {
                wf_warn!(pipe,
                    task_id = %self.task_id,
                    window = %source.window_name,
                    "cursor gap detected — some data was lost to eviction"
                );
                if let Some(metrics) = &self.metrics {
                    metrics.inc_rule_cursor_gap(
                        self.executor.plan().name.as_str(),
                        &source.window_name,
                    );
                }
            }
            let first_batch_seq = new_cursor.saturating_sub(batches.len() as u64);
            let materialize_fields = source.window.materialize_fields().cloned();
            pending.push((
                source.window_name.clone(),
                first_batch_seq,
                batches,
                shard_rows_per_batch,
                materialize_fields,
                key_partitioned,
                new_cursor,
            ));
        }

        // Phase 2: advance read cursors (separate from phase 1 so the mutable
        // borrow does not fight the `&self.sources` iteration above).
        for (window, _, _, _, _, _, new_cursor) in &pending {
            self.cursors.insert(window.clone(), *new_cursor);
        }

        // Phase 3: process each pulled batch (`&mut self`).
        for (
            window,
            first_batch_seq,
            batches,
            shard_rows_per_batch,
            materialize_fields,
            key_partitioned,
            new_cursor,
        ) in pending
        {
            for (batch_index, batch) in batches.iter().enumerate() {
                let batch_seq = first_batch_seq + batch_index as u64;
                let shard_rows = shard_rows_per_batch
                    .get(batch_index)
                    .and_then(|opt| opt.as_deref())
                    .map(|rows| rows.as_slice());
                // Key-partitioned windows are expected to carry a precomputed
                // per-shard row subset for *every* batch they own; a `None` here
                // means this batch fell back to whole-batch processing (missing
                // `shard_rows` — e.g. a hot-reload batch or a changed shard
                // count). When several shard instances hit the same gap they
                // each process the whole batch → cross-shard duplicate
                // consumption. That is the lossless-but-duplicating defensive
                // trade-off (duplicates are masked by at-least-once acking), but
                // surface it so a recurring fallback is not silent.
                if key_partitioned && shard_rows.is_none() {
                    wf_warn!(pipe,
                        task_id = %self.task_id,
                        window = %window,
                        shard = ?self.shard_index,
                        batch_seq = batch_seq,
                        "key-partitioned batch missing shard_rows — fell back to whole-batch processing (possible cross-shard duplicate)"
                    );
                }
                // Key-partitioned: `shard_rows` already restricts this task to
                // its own rows, so every pulled batch is processed. Otherwise
                // gate whole-batch (on-each round-robin / unsharded) tasks.
                let should_process = key_partitioned
                    || self.shard_count <= 1
                    || (batch_seq % self.shard_count as u64)
                        == self.shard_index.unwrap_or(0) as u64;
                if !should_process {
                    continue;
                }
                self.process_batch(
                    &window,
                    batch_seq,
                    Some(batch_seq),
                    None,
                    Some(batch),
                    shard_rows,
                    materialize_fields.as_deref(),
                )
                .await;
            }
            // Ack the READ position (`new_cursor`) — the shared-log cursor this
            // task just advanced to — rather than only the last batch it
            // *processed*. For key-partitioned rules every pulled batch is
            // processed so this equals `next_seq` when drained; for whole-batch
            // round-robin (on-each) each batch is owned by exactly one shard, so
            // acking the read position is what lets `min_acked` reach `next_seq`
            // once every shard has pulled the shared log — the true "rules
            // drained" signal. The cross-shard `min_acked` remains the eviction
            // floor: a slow shard still holds the floor below any batch it has
            // not yet read, so no owned batch is ever evicted early.
            if let Some(slot) = self.progress.get(&window) {
                slot.store(new_cursor, std::sync::atomic::Ordering::Release);
            }
        }
        self.update_rule_instances_metric();
    }

    /// Process a single parsed batch (shared `Arc`) against the state machine.
    ///
    /// This is the per-batch body shared by the legacy pull path
    /// ([`Self::pull_and_advance`]) and the push path (channel recv). `batch_seq`
    /// is used only for debug event references.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn process_batch(
        &mut self,
        window_name: &str,
        batch_seq: u64,
        lookup_max_seq: Option<u64>,
        events: Option<&Arc<Vec<Arc<Event>>>>,
        batch: Option<&RecordBatch>,
        shard_rows: Option<&[u32]>,
        materialize_fields: Option<&HashSet<String>>,
    ) {
        // perf-diag cut_rules 门控：规则求值直通（ack 保留——`pull_and_advance`
        // 在 process_batch 返回后推进 cursor，append/ack 仍在 floor 档收敛）。
        // 哨兵窗口由独立哨兵任务处理，不经过本函数，天然豁免。
        if crate::perf_diag::perf_cut_rules() {
            return;
        }
        let Some(aliases) = self.aliases.get(window_name) else {
            return;
        };
        let Some(ordered_aliases) = self.ordered_aliases.get(window_name) else {
            return;
        };
        // L2 deferred materialization: when the producer broadcast only the raw
        // batch, materialize only the rows the bind filter accepts. The time
        // column is still scanned over every row (watermark/expiry), but the
        // per-row Event is only built for hit rows (Q2 hit ~0.8%).
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);

        // Columnar bind-filter masks, one per alias, computed once per batch
        // from the raw `RecordBatch` (zero-copy). `None` (the inner) means the
        // alias has no filter or a non-columnar filter → fall back to the
        // per-event interpreted path at each row.
        let columnar_masks: HashMap<String, Option<BooleanArray>> = match batch {
            Some(batch) => aliases
                .iter()
                .map(|alias| {
                    (
                        alias.clone(),
                        self.executor.bind_filter_columnar_mask(alias, batch),
                    )
                })
                .collect(),
            None => HashMap::new(),
        };
        // Columnar branch-guard masks for the state machine's event steps.
        let branch_masks = match batch {
            Some(batch) => self.executor.branch_guard_masks(batch),
            None => GuardMasks::default(),
        };

        // Deferral is safe only for the state-machine path when debug detail
        // logging is off (rejected rows have no Event to render a debug ref
        // from) and every bind filter of this window is columnar (a missing
        // mask in the deferred path accepts all rows — a non-columnar filter
        // would silently lose its rejection). The raw batch must be present;
        // relay/push pushes that also carry materialized `events` now prefer
        // the columnar path too — the materialized events are only used as the
        // emit-path trigger projection (byte-identical via `materialize_fields`),
        // so carrying them is no longer a reason to force eager per-row
        // materialization (q15 eager_row 1.1µs vs deferred 326ns, 2026-08-22).
        let defer_materialize = batch.is_some()
            && self.machine.is_some()
            && !debug_enabled
            && self.executor.bind_filters_columnar_safe(window_name);

        // On-each columnar fast path: no per-row `Event` materialization —
        // field values are read straight from the Arrow columns. Byte-identical
        // to the eager path (deferred-vs-columnar 对拍 test locks it).
        // Independent of `defer_materialize` (that requires a state machine;
        // Q1 on-each has none).
        let columnar_each = !debug_enabled
            && self.machine.is_none()
            && self.each_direct
            && events.is_none()
            && batch.is_some()
            && self.executor.each_plan_columnar_safe()
            // deferred（emit at）规则不得走列式 each 快路径：挂起/到期评估在
            // 行循环里（deferred_pending_for → scan_deferred）。q8 等 on each +
            // deferred 若走快路径会被列式 join 当 Snapshot 即时输出——deferred
            // 语义丢失（2026-08-23 验证基线暴露：q8 引擎 33k vs oracle 82k）。
            && self.deferred.is_none();

        // Row domain: a **sharded** deferred push only owns the rows partitioned
        // to this shard (`shard_rows`); an unsharded push scans the whole batch.
        // Both the lazy-materialization scan and the main state-machine loop
        // iterate this domain. `DeferredRows` always uses **absolute** batch-row
        // indices (times / hit_indices), so all downstream consumers are
        // unchanged; shard-external rows are simply never iterated here.
        // Row count for the unsharded domain: relay / eager pushes carry
        // materialized `events` (batch is None) so their count comes from the
        // events; deferred pushes carry the raw batch.
        let num_rows = events
            .map(|e| e.len())
            .unwrap_or_else(|| batch.map(|b| b.num_rows()).unwrap_or(0));
        let row_domain: Vec<usize> = match shard_rows {
            Some(rows) => rows.iter().map(|&r| r as usize).collect(),
            None => (0..num_rows).collect(),
        };

        let deferred: Option<DeferredRows> = if defer_materialize {
            let batch = batch.expect("deferral requires the raw batch");
            let time_field = self.machine.as_ref().and_then(|m| m.time_field());
            // Scan needs the event time for every row (watermark/expiry); read
            // it straight from the time column with the same f64 round-trip the
            // eager path uses (`extract_event_time`). Resolve the column once,
            // then read per row over `row_domain` (whole batch for unsharded,
            // this shard's subset for sharded).
            //
            // `times` / `hit` / `hit_indices` are all **row-domain-relative**
            // (length == `row_domain.len()`; slot i covers `row_domain[i]`), so
            // a sharded push allocates only its own shard's rows — not the whole
            // batch. Absolute batch rows are recovered from `row_domain` at the
            // point they are needed (materialization, hit matching below).
            let time_col_index = batch_time_col_index(batch, time_field);
            let mut times = vec![0; row_domain.len()];
            if let Some(col_idx) = time_col_index {
                for (i, &row) in row_domain.iter().enumerate() {
                    times[i] = batch_event_time_nanos_at(batch, col_idx, row);
                }
            }
            // Hit = any alias's columnar bind filter accepts this row. The
            // window-level defer flag guarantees every alias here is columnar;
            // a missing mask is a defensive fallback that materializes all rows.
            let mut hit = vec![false; row_domain.len()];
            for alias in aliases.iter() {
                match columnar_masks.get(alias) {
                    Some(Some(mask)) => {
                        for (i, &row) in row_domain.iter().enumerate() {
                            hit[i] |= mask.value(row);
                        }
                    }
                    _ => {
                        for h in hit.iter_mut() {
                            *h = true;
                        }
                    }
                }
            }
            // Row-domain-relative hit positions.
            let hit_indices: Vec<u32> = (0..row_domain.len())
                .filter(|&i| hit[i])
                .map(|i| i as u32)
                .collect();
            // P3 FieldView: hit rows are fed to the state machine straight from
            // the columns — no HashMap materialization. The batch-level field
            // index makes `ColumnarEvent::field_value` O(1) per read; the
            // `materialize_fields` projection keeps the emit-path trigger event
            // byte-identical to the eager deferred path (projected). (The
            // `columnar_each` early path is machine-free, so this branch never
            // runs for it; `materialize_rows[_filtered]` stays only on the
            // eager path below.)
            let index = build_field_index(batch);
            let projection = materialize_fields.map(|f| Arc::new(f.clone()));
            Some(DeferredRows {
                times,
                hit_indices,
                batch,
                index,
                projection,
            })
        } else {
            None
        };

        // Eager events (full materialization), used by the non-deferred machine
        // path and the `on each` path.
        let eager_events: Option<Arc<Vec<Arc<Event>>>> = if defer_materialize || columnar_each {
            None
        } else {
            Some(match events {
                Some(events) => Arc::clone(events),
                None => {
                    let batch = batch.expect("deferred materialization requires the raw batch");
                    let events = match materialize_fields {
                        Some(fields) => batch_to_events_filtered(batch, fields),
                        None => batch_to_events(batch),
                    };
                    Arc::new(events.into_iter().map(Arc::new).collect())
                }
            })
        };

        let input_events = deferred
            .as_ref()
            .map(|d| d.times.len())
            .unwrap_or_else(|| eager_events.as_ref().map_or(0, |e| e.len()));

        let mut stats = RuleBatchDebugStats {
            input_events,
            ..RuleBatchDebugStats::default()
        };
        let rule_name = debug_enabled.then(|| self.rule_name().to_string());
        let rule_name_for_log = rule_name.as_deref().unwrap_or("");
        let aliases_for_log = if debug_enabled {
            Some(aliases.join(","))
        } else {
            None
        };
        if debug_enabled {
            let instances_before = self.instance_count();
            wf_debug!(pipe,
                rule = %rule_name_for_log,
                stage = 0,
                window = %window_name,
                batch_seq = batch_seq,
                rows = input_events,
                aliases = %aliases_for_log.as_deref().unwrap_or(""),
                instances_before = instances_before,
                "rule batch started"
            );
        }
        if let Some(metrics) = &self.metrics {
            metrics.add_rule_events(self.executor.plan().name.as_str(), input_events);
        }
        // Track the last wall-clock moment events were processed, so the
        // periodic timeout scan can advance the watermark across idle gaps.
        if input_events > 0 {
            self.last_activity_wall = std::time::Instant::now();
            // Cache wall time for the emit path's e2e-latency sample.
            self.cached_wall_nanos
                .store(wall_nanos(), Ordering::Relaxed);
        }
        // M2 (seq-watermark consistency): bound window_lookup to the seq of the
        // batch being processed (pull model only — push keeps the legacy full-
        // window view via `lookup_max_seq = None`). The watermark is scoped to
        // *this* source window only: join targets are independent windows and
        // must not be bounded by this window's seq. See
        // window-actor-pull-model.md §3.5.
        let lookup =
            RegistryLookup::with_source_watermark(&self.router, lookup_max_seq, window_name);
        // on-each: events within a batch share the window schema, so the
        // sorted field order used for wfx_id hashing is computed once per
        // batch instead of collected + sorted per event.
        let each_field_order: Vec<&smol_str::SmolStr> = match (
            self.executor.plan().each_plan.is_some(),
            eager_events.as_ref().and_then(|events| events.first()),
        ) {
            (true, Some(first)) => {
                let mut names: Vec<&smol_str::SmolStr> = first.fields.keys().collect();
                names.sort_unstable();
                names
            }
            _ => Vec::new(),
        };
        // Batch-level emit timestamp: all events in this batch share one
        // (nanos, formatted) pair — the executor caches the formatted string
        // and Arc-shares it across every record it builds this batch.
        let batch_emit_nanos = self.cached_wall_nanos.load(Ordering::Relaxed) as i64;
        // On-each columnar fast path: skip the per-row loop entirely. Hit rows
        // come from the (absent-or-columnar) bind-filter masks — with the gate
        // in `each_plan_columnar_safe`, a `None` mask means no filter (every
        // row passes, exactly like `event_matches_alias` with no filter).
        if columnar_each {
            let batch = batch.expect("columnar each requires the raw batch");
            let num_rows = batch.num_rows();
            let mut hit = vec![false; num_rows];
            for alias in aliases.iter() {
                match columnar_masks.get(alias) {
                    Some(Some(mask)) => {
                        for (row, h) in hit.iter_mut().enumerate() {
                            *h |= mask.value(row);
                        }
                    }
                    _ => {
                        hit.fill(true);
                    }
                }
            }
            let hit_indices: Vec<u32> = (0..num_rows)
                .filter(|&row| hit[row])
                .map(|row| row as u32)
                .collect();
            let time_col_index = batch_time_col_index(batch, self.each_time_field.as_deref());
            let col_events: Vec<ColumnarEvent<'_>> = hit_indices
                .iter()
                .map(|&row| ColumnarEvent::new(batch, row as usize))
                .collect();
            let rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
                .iter()
                .zip(hit_indices.iter())
                .map(|(ev, &row)| {
                    let row = row as usize;
                    let event_nanos = time_col_index
                        .map(|col| batch_event_time_nanos_at(batch, col, row))
                        .unwrap_or(0);
                    (ev, event_nanos)
                })
                .collect();
            // Metrics parity: the eager path reported the input count before
            // the loop; the unconditional add with 0 is a no-op, so add the
            // real count here.
            if let Some(metrics) = &self.metrics {
                metrics.add_rule_events(self.executor.plan().name.as_str(), rows.len());
            }
            // 列式 each 分流：无活 join（q1 等）走无 join 列式路径；活 join
            // （q20 等，each_join_plan 已解析）走列式 join 富化路径（批级
            // join_lookup + 列式右窗字段读，免每事件 Event clone —— 2026-08-23
            // 列式 join 富化，q20 2.5M/s → 列式量级）。
            if self.executor.live_joins().is_empty() {
                self.emit_each_direct_batch_columnar(&rows, batch_emit_nanos)
                    .await;
            } else {
                self.emit_each_direct_batch_columnar_join(&rows, &lookup, batch_emit_nanos)
                    .await;
            }
            return;
        }
        // Plan C2 batching: when the per-event detail logs are off, collect
        // the each-direct rows and emit them in one vectorized pass after
        // the loop (debug runs keep the per-event path for exact detail).
        let mut each_direct_rows: Vec<(&wf_engine::match_engine::Event, i64)> = Vec::new();
        // P2③: for conv-sink shards, aggregate raw closes across the whole batch
        // and send ONE ConvCloseBatch (with the max event-time watermark) after
        // the loop — avoids a per-event bounded(32) channel send on the hot path.
        let mut conv_closes: Vec<wf_engine::match_engine::CloseOutput> = Vec::new();
        // Columnar close emit (L4): gate-passing rules accumulate raw closes
        // here and emit them vectorized after the row loop — see
        // `execute_close_direct_batch_columnar`. Debug detail off keeps the
        // per-close log/counts (same gate shape as the on-each columnar path).
        let close_columnar = !debug_enabled && self.executor.close_plan_columnar_safe();
        let mut columnar_closes: Vec<wf_engine::match_engine::CloseOutput> = Vec::new();
        // Columnar match emit (2026-08-23, q6 形态): score 常量 + 输出全
        // Lit/Field + 输出字段不引用非键右窗 —— 命中 ctx 批量直写 builder，免
        // 每命中 OutputRecord 中间物化（`match_plan_columnar_safe` 门控）。
        // 批级 owned 累积（move，零成本）：行内 extend，行循环后统一列式——
        // 避免每命中一次 pending 锁（q6 每行恰命中 1 个，行内批处理反而更慢）。
        let match_columnar = !debug_enabled && self.executor.match_plan_columnar_safe();
        let mut match_rows: Vec<wf_engine::match_engine::MatchedContext> = Vec::new();
        // Records produced by the match/close paths accumulate here and are
        // appended to the pending columnar builder in one lock per
        // ALERT_BATCH_SIZE group (see [`Self::emit_batch`]) — the per-record
        // lock + target lookup was measurable on the q12 close fan-out hot
        // path (emit_nanos dominated the profiling budget).
        let mut staged_outputs: Vec<OutputRecord> = Vec::new();
        // Records produced by the match/close paths accumulate here and are
        // appended to the pending columnar builder in one lock per
        // ALERT_BATCH_SIZE group (see [`Self::emit_batch`]) — the per-record
        // lock + target lookup was measurable on the q12 close fan-out hot
        // path (emit_nanos dominated the profiling budget).
        let mut conv_max_wm: i64 = 0;
        let mut hit_cursor = 0usize;
        // 批级 join-then-key 预解析（2026-08-23，q4/q6：advance 88.8% 的 join
        // 取键热路径——每 bid 一次索引 lookup + values_equal 复核 + key 字段
        // 物化）。对一批事件按驱动 key 去重 lookup，每行得到预解析 scope key，
        // advance 传入跳过内部每事件解析。非 key_join 规则 → None（原逻辑）。
        let key_join_plan: Option<wf_lang::plan::JoinKeyPlan> = self
            .machine
            .as_ref()
            .and_then(|m| m.plan().key_join.clone());
        let key_overrides: Option<Vec<Option<Vec<wf_engine::match_engine::Value>>>> =
            match (&key_join_plan, batch) {
                (Some(kjp), Some(b)) => {
                    Some(precompute_join_then_keys(b, &row_domain, kjp, &lookup))
                }
                _ => None,
            };
        // Iterate the row domain: `i` is the position within `row_domain`
        // (matches the row-domain-relative `DeferredRows` times/hit_indices),
        // `row_index` is the absolute batch row it maps to.
        for (i, &row_index) in row_domain.iter().enumerate() {
            let event: Option<&Arc<Event>> = match (&deferred, &eager_events) {
                // Deferred hit rows are served as ColumnarEvent views inside the
                // machine branch — no materialized hit events here.
                (Some(_), _) => None,
                (None, Some(events)) => Some(&events[row_index]),
                (None, None) => None,
            };
            if let Some(machine) = &mut self.machine {
                let event_nanos = match (&deferred, event) {
                    (Some(d), _) => d.times[i],
                    (None, Some(event)) => machine.event_time_nanos(event),
                    (None, None) => {
                        unreachable!("machine rows are always materialized when eager")
                    }
                };
                let _scan_start = rule_profiling();
                // P2c: shards of a conv rule emit raw closes to the conv stage
                // (aggregation window); inline conv is applied only on the
                // legacy single-machine path.
                // Hop 窗口：每 slide 边界恰有一个窗口到期（关闭数受窗口内键数
                // 约束），用无界预算一次性收口——1024 预算会把同一窗口的关闭
                // 拆成多批，inline conv 逐批 top-1 造成同窗口重复 EMIT。
                let hop = matches!(machine.plan().window_spec, WindowSpec::Hop { .. });
                let (routed, closes) = if self.conv_sink.is_some() {
                    let raw = if hop {
                        machine.scan_expired_at_skip_non_alerting_unbounded(event_nanos)
                    } else {
                        machine.scan_expired_at_skip_non_alerting(event_nanos)
                    };
                    // Barrier watermark must reflect the scan's watermark (the
                    // event time) — the machine's cached watermark only advances
                    // during `advance`, which runs after the scan.
                    conv_max_wm = conv_max_wm.max(event_nanos);
                    conv_closes.extend(raw.into_iter().filter(close_is_qualified));
                    (true, Vec::new())
                } else if hop {
                    (
                        false,
                        machine.scan_expired_at_with_conv_skip_non_alerting_unbounded(
                            event_nanos,
                            self.conv_plan.as_ref(),
                        ),
                    )
                } else {
                    (
                        false,
                        machine.scan_expired_at_with_conv_skip_non_alerting(
                            event_nanos,
                            self.conv_plan.as_ref(),
                        ),
                    )
                };
                if let Some(_scan_t) = _scan_start {
                    self.scan_nanos += _scan_t.elapsed().as_nanos() as u64;
                }
                // Non-hit rows only need the time-column scan above (watermark /
                // expiry) plus the close emission below; there is no Event to
                // advance and no bind filter would accept them, so skip the
                // state-machine step but keep the close path.
                let mut matched = Vec::new();
                // Resolve the per-row source: ColumnarEvent for deferred hit rows
                // (P3 FieldView — no HashMap materialization), else the eager
                // event. Debug and defer are mutually exclusive, so the Eager arm
                // is the only one that appears with debug detail enabled.
                let row_event: Option<RowEvent<'_>> = if let Some(d) = &deferred {
                    (hit_cursor < d.hit_indices.len() && d.hit_indices[hit_cursor] as usize == i)
                        .then(|| {
                            hit_cursor += 1;
                            RowEvent::Columnar(ColumnarEvent::with_index_projected(
                                d.batch,
                                row_index,
                                Arc::clone(&d.index),
                                d.projection.clone(),
                            ))
                        })
                } else {
                    event.map(|ev| RowEvent::Eager(ev.as_ref()))
                };
                if let Some(row_event) = row_event {
                    let _advance_start = rule_profiling();
                    for alias in ordered_aliases {
                        if !alias_accepts(
                            &self.executor,
                            &columnar_masks,
                            alias,
                            row_index,
                            &row_event,
                            &lookup,
                        ) {
                            if debug_enabled {
                                stats.alias_rejected += 1;
                            }
                            if debug_enabled && stats.allow_detail() {
                                let event_ref =
                                    row_event_debug_ref(&row_event, batch_seq, row_index);
                                wf_debug!(pipe,
                                    rule = %rule_name_for_log,
                                    stage = 0,
                                    window = %window_name,
                                    alias = %alias,
                                    event_ref = %event_ref,
                                    reason = "bind_filter_false",
                                    "rule event rejected"
                                );
                            }
                            continue;
                        }
                        if debug_enabled {
                            stats.alias_passed += 1;
                        }
                        let should_capture_progress = debug_enabled && stats.can_log_detail();
                        let (step_result, progress) = if should_capture_progress {
                            // debug 路径走内部解析（结果与预解析一致——批级共享同一
                            // lookup + values_equal 语义）。
                            let outcome = machine.advance_at_with_progress(
                                alias,
                                &row_event,
                                event_nanos,
                                Some(&lookup),
                            );
                            (outcome.result, outcome.progress)
                        } else {
                            (
                                machine.advance_at_with_masks_key(
                                    alias,
                                    &row_event,
                                    event_nanos,
                                    Some(&lookup),
                                    row_index,
                                    Some(&branch_masks),
                                    key_overrides.as_ref().map(|ko| &ko[i]),
                                ),
                                None,
                            )
                        };
                        match step_result {
                            StepResult::Accumulate => {
                                if debug_enabled {
                                    stats.accumulated += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    let instances = machine.instance_count();
                                    let event_ref =
                                        row_event_debug_ref(&row_event, batch_seq, row_index);
                                    if let Some(progress) = progress.as_ref() {
                                        wf_debug!(pipe,
                                            rule = %rule_name_for_log,
                                            stage = 0,
                                            window = %window_name,
                                            alias = %alias,
                                            event_ref = %event_ref,
                                            scope_key = %debug_scope_key(&progress.scope_key),
                                            machine_id = %progress.machine_id,
                                            step_index = progress.step_index,
                                            step_label = progress.step_label.as_deref().unwrap_or(""),
                                            branch_index = progress.branch_index,
                                            threshold_checked_branches = progress.threshold_checked_branches,
                                            measure_value = progress.measure_value,
                                            cmp = %progress.cmp,
                                            threshold = %progress.threshold,
                                            instances = instances,
                                            "rule event accumulated"
                                        );
                                    } else {
                                        wf_debug!(pipe,
                                            rule = %rule_name_for_log,
                                            stage = 0,
                                            window = %window_name,
                                            alias = %alias,
                                            event_ref = %event_ref,
                                            instances = instances,
                                            "rule event accumulated"
                                        );
                                    }
                                }
                            }
                            StepResult::Advance => {
                                if debug_enabled {
                                    stats.advanced += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    let instances = machine.instance_count();
                                    let event_ref =
                                        row_event_debug_ref(&row_event, batch_seq, row_index);
                                    if let Some(progress) = progress.as_ref() {
                                        wf_debug!(pipe,
                                            rule = %rule_name_for_log,
                                            stage = 0,
                                            window = %window_name,
                                            alias = %alias,
                                            event_ref = %event_ref,
                                            scope_key = %debug_scope_key(&progress.scope_key),
                                            machine_id = %progress.machine_id,
                                            step_index = progress.step_index,
                                            step_label = progress.step_label.as_deref().unwrap_or(""),
                                            branch_index = progress.branch_index,
                                            threshold_checked_branches = progress.threshold_checked_branches,
                                            measure_value = progress.measure_value,
                                            cmp = %progress.cmp,
                                            threshold = %progress.threshold,
                                            instances = instances,
                                            "rule step advanced"
                                        );
                                    } else {
                                        wf_debug!(pipe,
                                            rule = %rule_name_for_log,
                                            stage = 0,
                                            window = %window_name,
                                            alias = %alias,
                                            event_ref = %event_ref,
                                            instances = instances,
                                            "rule step advanced"
                                        );
                                    }
                                }
                            }
                            StepResult::Matched(ctx) => {
                                if debug_enabled {
                                    stats.matched += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    let event_ref =
                                        row_event_debug_ref(&row_event, batch_seq, row_index);
                                    let step = ctx.step_data.last();
                                    wf_debug!(pipe,
                                        rule = %rule_name_for_log,
                                        stage = 0,
                                        window = %window_name,
                                        alias = %alias,
                                        event_ref = %event_ref,
                                        scope_key = %debug_scope_key(&ctx.scope_key),
                                        machine_id = %ctx.machine_id,
                                        matched_steps = ctx.step_data.len(),
                                        step_label = step.and_then(|s| s.label.as_deref()).unwrap_or(""),
                                        measure_value = step.map(|s| s.measure_value).unwrap_or_default(),
                                        "rule matched"
                                    );
                                }
                                matched.push(ctx);
                            }
                        }
                    }
                    if let Some(_advance_t) = _advance_start {
                        self.advance_nanos += _advance_t.elapsed().as_nanos() as u64;
                    }
                }
                let _emit_start = rule_profiling();

                // When routed to the conv stage, the inline close processing is
                // skipped (the closes were already sent in the scan step).
                // Columnar-safety gate: gate-passing rules accumulate raw
                // closes across the batch and emit them vectorized after the
                // row loop (see the batch close emit below) — the q12 close
                // fan-out hot path (per-close OutputRecord + synthetic ctx
                // build measured ~95% of execute_close_with_joins).
                if close_columnar && !routed {
                    columnar_closes.extend(closes);
                } else if !routed {
                    for close in &closes {
                        let _close_exec_start = rule_profiling();
                        let result = self.executor.execute_close_with_joins(close, &lookup);
                        if let Some(_close_t) = _close_exec_start {
                            self.close_exec_nanos += _close_t.elapsed().as_nanos() as u64;
                        }
                        match result {
                            Ok(Some(record)) => {
                                if debug_enabled {
                                    stats.count_output(&record, &self.intermediate_targets);
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_emitted(
                                        "execute_close",
                                        "close",
                                        output_kind(&record, &self.intermediate_targets),
                                        &record,
                                        close.scope_key.as_slice(),
                                    );
                                }
                                self.stage_or_emit_record(&mut staged_outputs, record).await;
                            }
                            Ok(None) => {
                                if debug_enabled {
                                    stats.output_none += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_suppressed(
                                        rule_name_for_log,
                                        "execute_close",
                                        Some(close.scope_key.as_slice()),
                                    );
                                }
                            }
                            Err(e) => {
                                if debug_enabled {
                                    stats.errors += 1;
                                }
                                wf_warn!(
                                    pipe,
                                    rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                    stage = 0,
                                    phase = "execute_close",
                                    scope_key = %debug_scope_key(&close.scope_key),
                                    error = %e,
                                    "rule output failed"
                                )
                            }
                        }
                    }
                }

                if match_columnar {
                    // 列式：move 整行命中到批级累积（零成本），批后统一
                    // 直写 builder——跳过 join 执行（门控保证输出不引用非键
                    // 右窗字段，join 已在 advance 阶段完成）。
                    if let Some(metrics) = &self.metrics {
                        for _ in 0..matched.len() {
                            metrics.inc_rule_match(self.rule_name());
                        }
                    }
                    match_rows.extend(matched);
                } else {
                    for ctx in &matched {
                        if let Some(metrics) = &self.metrics {
                            metrics.inc_rule_match(self.rule_name());
                        }
                        let _exec_start = rule_profiling();
                        match self.executor.execute_match_with_joins_at(
                            ctx,
                            &lookup,
                            batch_emit_nanos,
                        ) {
                            Ok(Some(record)) => {
                                if let Some(_exec_t) = _exec_start {
                                    self.exec_nanos += _exec_t.elapsed().as_nanos() as u64;
                                }
                                if debug_enabled {
                                    stats.count_output(&record, &self.intermediate_targets);
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_emitted(
                                        "execute_match",
                                        "event",
                                        output_kind(&record, &self.intermediate_targets),
                                        &record,
                                        ctx.scope_key.as_slice(),
                                    );
                                }
                                self.stage_or_emit_record(&mut staged_outputs, record).await;
                            }
                            Ok(None) => {
                                if debug_enabled {
                                    stats.output_none += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_suppressed(
                                        rule_name_for_log,
                                        "execute_match",
                                        Some(ctx.scope_key.as_slice()),
                                    );
                                }
                            }
                            Err(e) => {
                                if debug_enabled {
                                    stats.errors += 1;
                                }
                                wf_warn!(
                                    pipe,
                                    rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                    stage = 0,
                                    phase = "execute_match",
                                    scope_key = %debug_scope_key(&ctx.scope_key),
                                    error = %e,
                                    "rule output failed"
                                )
                            }
                        }
                    }
                }
                if let Some(_emit_t) = _emit_start {
                    self.emit_nanos += _emit_t.elapsed().as_nanos() as u64;
                }
            } else if let Some(alias) = self
                .each_alias
                .as_ref()
                .filter(|alias| aliases.iter().any(|candidate| candidate == *alias))
            {
                // The each path never defers materialization — `event` is always
                // present for these rows.
                let event = event.expect("each path is always eager");
                if alias_accepts(
                    &self.executor,
                    &columnar_masks,
                    alias,
                    row_index,
                    event.as_ref(),
                    &lookup,
                ) {
                    if debug_enabled {
                        stats.alias_passed += 1;
                    }
                    let event_nanos = event_time_nanos(event, self.each_time_field.as_deref());
                    // P3：deferred join（`emit at`）——驱动事件挂起（expiry = emit at），
                    // 不即时输出；到期评估在批次尾的 `scan_deferred`（设计 §5.2）。
                    if self.deferred.is_some() {
                        if let Some(deferred) = self.deferred.as_mut()
                            && let Some(pending) = self.executor.deferred_pending_for(
                                deferred.join_idx,
                                event,
                                event_nanos,
                            )
                        {
                            deferred.watermark = deferred.watermark.max(event_nanos);
                            // 2026-08-25 q4 100M：pending 保持按 expiry 升序——
                            // scan_deferred 据此只取到期前缀（O(due)）而非全量
                            // 扫（O(n)，33M 挂起 × 2740 batch 卡死 28×）。驱动流
                            // 事件时间单调时 expiry 也单调（emit at = expires 随
                            // 事件时间），追加即有序 O(1)；乱序驱动二分插入兜底。
                            let expiry = pending.expiry_nanos;
                            let pos = deferred.pending.partition_point(|p| {
                                p.expiry_nanos <= expiry
                            });
                            // lo_min 缓存：插入 O(1) 更新（publish 免全量扫）。
                            // 用插入项的 lo_ns（区间下界）；pending 有序后 min lo
                            // 项几乎总是最早挂起（数据时间单调），dirty 极少。
                            let lo_ns = pending.lo_ns;
                            deferred.pending.insert(pos, pending);
                            deferred.lo_min = deferred.lo_min.min(lo_ns);
                        }
                        if debug_enabled {
                            stats.advanced += 1;
                        }
                        continue;
                    }
                    if self.each_direct {
                        if !debug_enabled {
                            // Plan C2 batched: defer to the vectorized pass
                            // after the loop (same rows, same flush cadence).
                            each_direct_rows.push((event.as_ref(), event_nanos));
                            continue;
                        }
                        // Plan C2 per-event path (debug detail on): the
                        // executor appends straight into the columnar
                        // builder — no per-record OutputRecord.
                        match self
                            .emit_each_direct(
                                event,
                                event_nanos,
                                &lookup,
                                &each_field_order,
                                batch_emit_nanos,
                            )
                            .await
                        {
                            Ok(true) => {
                                if debug_enabled {
                                    stats.output_emitted += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    wf_debug!(pipe,
                                        rule = %rule_name_for_log,
                                        stage = 0,
                                        phase = "execute_each",
                                        target = %self.executor.static_yield_target(),
                                        output_kind = "alert",
                                        "rule output emitted (direct)"
                                    );
                                }
                            }
                            Ok(false) => {
                                if debug_enabled {
                                    stats.output_none += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_suppressed(rule_name_for_log, "execute_each", None);
                                }
                            }
                            Err(e) => {
                                if debug_enabled {
                                    stats.errors += 1;
                                }
                                wf_warn!(
                                    pipe,
                                    rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                    stage = 0,
                                    phase = "execute_each",
                                    error = %e,
                                    "rule output failed"
                                )
                            }
                        }
                    } else {
                        match self.executor.execute_each_with_joins(
                            event,
                            event_nanos,
                            &lookup,
                            &each_field_order,
                            batch_emit_nanos,
                        ) {
                            Ok(Some(record)) => {
                                if debug_enabled {
                                    stats.count_output(&record, &self.intermediate_targets);
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_emitted(
                                        "execute_each",
                                        "event",
                                        output_kind(&record, &self.intermediate_targets),
                                        &record,
                                        &[],
                                    );
                                }
                                self.emit(record).await;
                            }
                            Ok(None) => {
                                if debug_enabled {
                                    stats.output_none += 1;
                                }
                                if debug_enabled && stats.allow_detail() {
                                    log_output_suppressed(rule_name_for_log, "execute_each", None);
                                }
                            }
                            Err(e) => {
                                if debug_enabled {
                                    stats.errors += 1;
                                }
                                wf_warn!(
                                    pipe,
                                    rule = %rule_name.as_deref().unwrap_or_else(|| self.rule_name()),
                                    stage = 0,
                                    phase = "execute_each",
                                    error = %e,
                                    "rule output failed"
                                )
                            }
                        }
                    }
                } else {
                    if debug_enabled {
                        stats.alias_rejected += 1;
                    }
                    if debug_enabled && stats.allow_detail() {
                        let event_ref = event_debug_ref(event, batch_seq, row_index);
                        wf_debug!(pipe,
                            rule = %rule_name_for_log,
                            stage = 0,
                            window = %window_name,
                            alias = %alias,
                            event_ref = %event_ref,
                            reason = "bind_filter_false",
                            "rule event rejected"
                        );
                    }
                }
            }
        }
        // P2③: one aggregated ConvCloseBatch per batch for conv-sink shards,
        // using the max event-time watermark as the barrier. (Replaces per-event
        // sends — the per-event path saturated the bounded(32) channel.)
        if self.conv_sink.is_some()
            && let Some(sink) = self.conv_sink.as_ref()
        {
            // P3-D: if the conv stage is gone (channel closed), the closes are
            // dropped — log it rather than fail silently.
            let sent = sink
                .tx
                .send(ConvCloseBatch {
                    closes: std::mem::take(&mut conv_closes),
                    watermark: conv_max_wm,
                    drained: false,
                    barrier_index: sink.barrier_index,
                })
                .await;
            if sent.is_err() {
                log::debug!("conv sink channel closed — conv batch dropped");
            }
        }
        // Vectorized on-each direct emit for the collected rows. Segment
        // size = ALERT_BATCH_SIZE keeps the flush cadence and the pending
        // memory bound of the per-event path.
        if !each_direct_rows.is_empty() {
            self.emit_each_direct_batch(
                &each_direct_rows,
                &lookup,
                &each_field_order,
                batch_emit_nanos,
            )
            .await;
        }
        // P3：deferred join 到期扫描（本批次事件时间 watermark 已推进）
        if self.deferred.is_some()
            && let Some(wm) = self.deferred.as_ref().map(|d| d.watermark)
        {
            self.scan_deferred(wm, batch_emit_nanos, true).await;
            // D4：到期实例已退场 → 把新的保留前沿发布给 join 目标窗口（批次
            // 级，不在行循环里）。扫描后发布：前沿尽可能向前，窗口尽早释放。
            if let Some(d) = self.deferred.as_mut() {
                d.publish_retention_floor();
            }
        }
        if debug_enabled {
            let instances_after = self.instance_count();
            wf_debug!(pipe,
                rule = %rule_name_for_log,
                stage = 0,
                window = %window_name,
                batch_seq = batch_seq,
                input = stats.input_events,
                alias_passed = stats.alias_passed,
                alias_rejected = stats.alias_rejected,
                accumulated = stats.accumulated,
                advanced = stats.advanced,
                matched = stats.matched,
                outputs = stats.output_emitted,
                output_none = stats.output_none,
                intermediate_outputs = stats.intermediate_emitted,
                errors = stats.errors,
                instances_after = instances_after,
                detail_logged = stats.detail_logged,
                detail_suppressed = stats.detail_suppressed,
                "rule batch summary"
            );
            if stats.detail_suppressed > 0 {
                wf_debug!(pipe,
                    rule = %rule_name_for_log,
                    stage = 0,
                    window = %window_name,
                    batch_seq = batch_seq,
                    detail_logged = stats.detail_logged,
                    detail_suppressed = stats.detail_suppressed,
                    "rule event details suppressed"
                );
            }
        }
        self.dump_profiling();
        // Columnar match emit (q6 形态): one pending lock, one target lookup,
        // one columnar batch commit — no per-match OutputRecord. Metrics mirror
        // the per-record path (exact totals; serialize-failed for eval failures).
        if match_columnar && !match_rows.is_empty() {
            let row_refs: Vec<&wf_engine::match_engine::MatchedContext> =
                match_rows.iter().collect();
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                let target = self.executor.static_yield_target();
                let slot = pending
                    .by_target
                    .iter_mut()
                    .find(|(existing, _)| *existing == *target);
                let builder = match slot {
                    Some((_, builder)) => builder,
                    None => {
                        pending.by_target.push((
                            std::sync::Arc::clone(target),
                            AlertColumnBuilder::new(std::sync::Arc::clone(target)),
                        ));
                        let last = pending.by_target.len() - 1;
                        &mut pending.by_target[last].1
                    }
                };
                let mut appended_idx = Vec::new();
                let outcome = self.executor.execute_match_direct_batch_columnar(
                    &row_refs,
                    batch_emit_nanos,
                    builder,
                    &mut appended_idx,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            if let Some(metrics) = &self.metrics {
                for _ in 0..outcome.appended {
                    metrics.inc_alert_emitted_total(self.rule_name());
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_serialize_failed();
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
        }
        // Vectorized close emit for gate-passing rules (L4): one pending lock,
        // one target lookup, one columnar batch commit — no per-close
        // OutputRecord / synthetic ctx. Metrics mirror the per-record path
        // (exact totals; serialize-failed increments for eval failures).
        if close_columnar && !columnar_closes.is_empty() {
            let _close_exec_start = rule_profiling();
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                let target = self.executor.static_yield_target();
                let slot = pending
                    .by_target
                    .iter_mut()
                    .find(|(existing, _)| *existing == *target);
                let builder = match slot {
                    Some((_, builder)) => builder,
                    None => {
                        pending.by_target.push((
                            std::sync::Arc::clone(target),
                            AlertColumnBuilder::new(std::sync::Arc::clone(target)),
                        ));
                        let last = pending.by_target.len() - 1;
                        &mut pending.by_target[last].1
                    }
                };
                let outcome = self.executor.execute_close_direct_batch_columnar(
                    &columnar_closes,
                    builder,
                    batch_emit_nanos,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            if let Some(_close_t) = _close_exec_start {
                self.close_exec_nanos += _close_t.elapsed().as_nanos() as u64;
            }
            if let Some(metrics) = &self.metrics {
                for _ in 0..outcome.appended {
                    metrics.inc_alert_emitted_total(self.rule_name());
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_serialize_failed();
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
        }
        // Deliver any remaining staged outputs (same cadence as the per-event
        // flush — bounds delivery latency to one event batch).
        if !staged_outputs.is_empty() {
            self.emit_batch(std::mem::take(&mut staged_outputs)).await;
        }
        // Deliver any accumulated alert batch (bounds delivery latency to one
        // event batch and flushes test expectations without an explicit EOS).
        self.flush_alerts().await;
        // Same latency bound for staged intermediate (pipe) rows.
        self.flush_pipes().await;
    }

    /// Log the cumulative advance/scan/emit profiler accumulators once per
    /// second (throttled) so a run's phase split can be read from the log.
    fn dump_profiling(&mut self) {
        if self.last_profile_dump.elapsed() < Duration::from_secs(1) {
            return;
        }
        self.last_profile_dump = std::time::Instant::now();
        wf_info!(pipe,
            rule = %self.rule_name(),
            phase = "profile",
            scan_nanos = self.scan_nanos,
            advance_nanos = self.advance_nanos,
            exec_nanos = self.exec_nanos,
            close_exec_nanos = self.close_exec_nanos,
            serialize_nanos = self.serialize_nanos.load(Ordering::Relaxed),
            fanout_nanos = self.fanout_nanos.load(Ordering::Relaxed),
            emit_nanos = self.emit_nanos,
            "rule profiling"
        );
    }

    /// Update the periodic per-rule instance-count gauge.
    ///
    /// P2b: the gauge is the sum across a rule's shards, so each shard reports
    /// the delta since its last report. On drain (flush/EOS) the count drops to
    /// zero and the final delta reconciles the shard's contribution to zero.
    fn update_rule_instances_metric(&self) {
        if let Some(metrics) = &self.metrics {
            let rule_name = self.executor.plan().name.as_str();
            let cur = self
                .machine
                .as_ref()
                .map(|machine| machine.instance_count() as i64)
                .unwrap_or(0);
            let last = self.last_reported_instances.swap(cur, Ordering::Relaxed);
            let delta = cur - last;
            if delta != 0 {
                metrics.adjust_rule_instances(rule_name, delta);
            }
        }
    }

    /// Process a single pushed batch, advancing the per-task push sequence.
    pub(super) async fn process_push(&mut self, push: RulePush) {
        let seq = self.pushed_seq;
        self.pushed_seq += 1;
        let window_name = push.window_name.clone();
        let push_seq = push.seq;
        self.process_batch(
            window_name.as_ref(),
            seq,
            None,
            push.events.as_ref(),
            push.batch.as_deref(),
            push.shard_rows.as_deref().map(|rows| rows.as_slice()),
            push.materialize_fields.as_deref(),
        )
        .await;
        // Ack the window batch seq so time eviction may reclaim it (the
        // `seq` above is only a per-task debug counter).
        if let Some(slot) = self.progress.get(window_name.as_ref()) {
            // saturating: relay pushes carry seq = u64::MAX (no window batch
            // behind them) — MAX + 1 would overflow and wrap to 0.
            slot.store(
                push_seq.saturating_add(1),
                std::sync::atomic::Ordering::Release,
            );
        }
    }

    /// Consume and process all currently-buffered pushed batches.
    ///
    /// Used by the push loop to drain the channel before a flush (EOS/cancel).
    /// After the source reports EOS no further pushes arrive, so draining via
    /// `try_recv` until empty is complete.
    pub(super) async fn drain_push_channel(&mut self, rx: &mut mpsc::Receiver<RulePush>) {
        while let Ok(push) = rx.try_recv() {
            self.process_push(push).await;
        }
        self.update_rule_instances_metric();
    }

    // -- Timeout & shutdown -------------------------------------------------

    /// Scan for expired state machine instances and emit alerts.
    /// P3：deferred join 到期扫描——触发 `expiry ≤ wm` 的挂起实例，评估并输出。
    ///
    /// `wm`：事件时间 watermark（批次尾 / scan_timeouts / flush 收口）；
    /// `emit_time_nanos`：输出记录的墙钟 emit 时间。空集（Q9 无 bid）不输出。
    /// `gate_on_target`：运行期为 true 时把评估前沿压到 join 目标窗口的 append
    /// 位置（见函数内注释——100M q4a 欠发根治）；flush 收口为 false（数据已全
    /// 量 ingest，miss 由 EOS 重试兜底，不能 gate 掉尾部 pending）。
    async fn scan_deferred(&mut self, wm: i64, emit_time_nanos: i64, gate_on_target: bool) {
        let Some(deferred) = self.deferred.as_mut() else {
            return;
        };
        let join_idx = deferred.join_idx;
        // 2026-08-25 q4 100M 欠发根治：运行期评估前沿 = min(驱动 watermark,
        // join 目标窗口 append 位置)。驱动 watermark 只反映**驱动流**（q4a =
        // auction）处理到哪；bid 等 join 目标由同一输入另行 append，存在管道
        // 滞后——按驱动 wm 到期评估时目标窗口可能还没 append 该实例的右行
        // （探针实锤：运行期 hit≈50%、cand0≈34-66%，越靠流尾命中率越高）。
        // 后果：miss 积压进 `missed`（RSS 随总量增长，30M/100M 非窗口内存差
        // ~9.2GB），且 100M 下 EOS 重试时早段右行已被 over 时间驱逐 → 欠发
        // ~63%（oracle 5.58M vs 2.07M）。改为等目标窗口 raw max event time
        // ≥ expiry 才评估：右行全在场且新鲜（刚 append，驱逐够不着）→ 运行期
        // 命中、missed 不积压、EMIT 正确。目标窗口不存在/未 append（i64::MIN）
        // → 不 gate（防御：保持旧行为，等 flush 收口）。
        let eff_wm = if gate_on_target {
            let target_wm = self
                .router
                .registry()
                .get_window(&self.executor.plan().joins[join_idx].right_window)
                .map(|w| w.max_event_time_nanos())
                .unwrap_or(i64::MAX);
            wm.min(target_wm)
        } else {
            wm
        };
        // 取到期实例（块内释放 `deferred` 借用，避免与 `self.executor`/`self.emit` 冲突）
        // 2026-08-25 q4 100M：pending 按 expiry 升序 → 到期项是前缀，
        // `partition_point` O(log n) 定位 + drain 前缀 O(due)——替代旧的
        // 全量遍历重建（O(n)/batch，33M 挂起 × 2740 batch 卡死 28×）。
        let due: Vec<DeferredPending> = {
            let split = deferred.pending.partition_point(|p| p.expiry_nanos <= eff_wm);
            // 到期项移出**不**标 dirty：lo_min 缓存是插入时单调不增的 min
            //（历史最小 lo_ns），移出后仍偏保守（更小）→ 安全，无需重扫。
            // 仅 missed 集合被清空重建（reevaluate_deferred_missed）时才需
            // 重扫（2026-08-25 q4 100M：有到期即标 dirty 会让 publish 每 batch
            // 全量重扫 → O(n²)）。
            deferred.pending.drain(..split).collect()
        };
        if due.is_empty() {
            return;
        }
        let lookup = RegistryLookup::new(&self.router);
        let mut stats = RuleBatchDebugStats::default();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        // 到期 miss 的收集——join 目标窗口可能 append 滞后（引擎流式 vs oracle
        // 预加载），留到 EOS flush 重试（届时目标完整）；真 miss 重试后仍 miss。
        let mut missed_this = Vec::new();
        for p in due {
            match self
                .executor
                .execute_deferred_join(join_idx, &p, &lookup, emit_time_nanos)
            {
                Ok(Some(record)) => {
                    if debug_enabled {
                        stats.count_output(&record, &self.intermediate_targets);
                    }
                    if debug_enabled && stats.allow_detail() {
                        log_output_emitted(
                            "execute_deferred",
                            "deferred",
                            output_kind(&record, &self.intermediate_targets),
                            &record,
                            &[],
                        );
                    }
                    self.emit(record).await;
                }
                Ok(None) => {
                    // 到期 miss：join 目标窗口可能未追平（append 滞后）——
                    // 留到 EOS 重试（届时目标完整）。真 miss 重试后仍 miss。
                    missed_this.push(p);
                    if debug_enabled {
                        stats.output_none += 1;
                    }
                    if debug_enabled && stats.allow_detail() {
                        log_output_suppressed(self.rule_name(), "execute_deferred", None);
                    }
                }
                Err(e) => {
                    if debug_enabled {
                        stats.errors += 1;
                    }
                    wf_warn!(
                        pipe,
                        task_id = %self.task_id,
                        rule = %self.rule_name(),
                        stage = 0,
                        phase = "execute_deferred",
                        error = %e,
                        "deferred join output failed"
                    );
                }
            }
        }
        if !missed_this.is_empty()
            && let Some(deferred) = self.deferred.as_mut()
        {
            // missed 的 lo_ns 也计入前沿（它们需要的行稍后才落地，必须活到
            // EOS 重试）——同步维护 lo_min 缓存。
            for p in &missed_this {
                deferred.lo_min = deferred.lo_min.min(p.lo_ns);
            }
            deferred.missed.extend(missed_this);
        }
    }

    /// EOS 重试：到期评估 miss 的 deferred 实例（join 目标 append 滞后）。
    ///
    /// 重试**仍 miss** 的实例保留回 `missed`，不在此处判定为真 miss：flush 的
    /// 调用方可能是 keep-running EOS（窗口 actors 仍在排空 mailbox，目标窗口
    /// 可能不完整——shutdown 路径因 LIFO 排序无此问题，但 daemon 接收有限输入
    /// 的 EOS 场景是真实竞态，2026-08-23 复现测试锁定）。保留后由窗口确认
    /// 完整时的下一次 flush 再评估——命中补输出，仍 miss 为真 miss（此时任务
    /// 即将退出，保留与否无差别）。命中则补输出。
    async fn reevaluate_deferred_missed(&mut self) {
        let missed = {
            let Some(deferred) = self.deferred.as_mut() else {
                return;
            };
            std::mem::take(&mut deferred.missed)
        };
        if missed.is_empty() {
            return;
        }
        let join_idx = self
            .deferred
            .as_ref()
            .expect("deferred state exists")
            .join_idx;
        let lookup = RegistryLookup::new(&self.router);
        let missed_len = missed.len();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        let mut hit = 0usize;
        let mut still_miss = Vec::with_capacity(missed_len.min(64));
        for p in missed {
            match self
                .executor
                .execute_deferred_join(join_idx, &p, &lookup, wall_nanos() as i64)
            {
                Ok(Some(record)) => {
                    hit += 1;
                    if debug_enabled {
                        log_output_emitted(
                            "execute_deferred",
                            "deferred-eos-retry",
                            output_kind(&record, &self.intermediate_targets),
                            &record,
                            &[],
                        );
                    }
                    self.emit(record).await;
                }
                // 仍 miss：不判定为真 miss——窗口可能仍不完整（keep-running
                // EOS 竞态）。保留回 missed，等下一次 flush（窗口完整后）。
                Ok(None) => still_miss.push(p),
                Err(e) => {
                    wf_warn!(
                        pipe,
                        task_id = %self.task_id,
                        rule = %self.rule_name(),
                        stage = 0,
                        phase = "execute_deferred_eos_retry",
                        error = %e,
                        "deferred join EOS retry failed"
                    );
                }
            }
        }
        let still_miss_len = still_miss.len();
        if !still_miss.is_empty()
            && let Some(deferred) = self.deferred.as_mut()
        {
            deferred.missed.extend(still_miss);
        }
        // missed 集合被取空重建——lo_min 缓存失效（含被命中移除的项）。
        // EOS 收尾路径，全量重扫可接受（罕见调用）。
        if let Some(d) = self.deferred.as_mut() {
            d.lo_min_dirty = true;
        }
        if hit > 0 && debug_enabled {
            wf_debug!(
                pipe,
                task_id = %self.task_id,
                rule = %self.rule_name(),
                missed = missed_len,
                hit = hit,
                still_miss = still_miss_len,
                "deferred EOS retry: missed instances re-evaluated (still-miss preserved for the next flush)"
            );
        }
    }

    pub(super) async fn scan_timeouts(&mut self) {
        // P3：deferred join 规则（无 machine）——事件时间 watermark 到期扫描
        //（不叠加墙钟：replay 对拍依赖事件时间序，墙钟推进会提前触发）。
        if self.machine.is_none() && self.deferred.is_some() {
            let wm = self
                .deferred
                .as_ref()
                .map(|d| d.watermark)
                .unwrap_or(i64::MIN);
            if wm > i64::MIN {
                self.scan_deferred(wm, wall_nanos() as i64, true).await;
            }
            // D4：空闲/超时扫描也发布保留前沿（到期实例可能已在此退场）。
            // 注：尚未见过驱动事件时这里会发布 i64::MIN（全保留），而不是释放——
            // 参见 `publish_retention_floor` 的 ⚠ 注释。
            if let Some(d) = self.deferred.as_mut() {
                d.publish_retention_floor();
            }
            return;
        }
        let Some(machine) = &self.machine else {
            return;
        };
        self.cached_wall_nanos
            .store(wall_nanos(), Ordering::Relaxed);
        // 2026-08-23 q11 修复：session 窗口是纯事件时间语义（gap = 事件时间
        // 间隔、会话随事件延长）——墙钟推进会把数据末尾未超时的尾部会话提前
        // 扫出（10M replay 多 204/197095≈0.1%），与 deferred 分支同源（replay
        // 对拍依赖事件时间序，墙钟推进会提前触发）。session 不叠加墙钟；
        // fixed/sliding/hop 保留墙钟兜底（q16 30M 尾桶收口依赖该扫）。
        let event_watermark = machine.watermark_nanos();
        let effective_watermark = if matches!(machine.plan().window_spec, WindowSpec::Session(_)) {
            event_watermark
        } else {
            // Advance the effective watermark by the wall-clock time elapsed since the
            // last event was processed — but **capped at one scan interval**. This
            // lets idle instances expire per their window TTL (window semantics, not
            // just event-time), while bounding each sweep: a slow/backpressured
            // pipeline cannot accumulate minutes of wall-clock and snowball into a
            // huge single expiry sweep that starves push consumption (q5/q6/q7 froze
            // at ~22-25M appends on 30M data before this cap).
            event_watermark.saturating_add(
                self.last_activity_wall
                    .elapsed()
                    .min(self.timeout_scan_interval)
                    .as_nanos() as i64,
            )
        };
        let started = Instant::now();
        // No input batch is being processed here (timeout scan), so the window
        // lookups read the full window (no `max_seq` watermark).
        let lookup = RegistryLookup::new(&self.router);
        // P2c: shards of a conv rule route raw closes to the conv stage.
        let (rule_name, closes, routed) = {
            let machine = self.machine.as_mut().expect("checked above");
            let rule_name = machine.rule_name().to_string();
            if self.conv_sink.is_some() {
                // Timeout scan runs off the event hot path (pipeline idle), so it
                // uses the **unbounded** expiry budget: fixed-window rules whose
                // final bucket expires past the last event time depend on this
                // sweep to close (q16 30M dropped the final bucket otherwise).
                let raw = machine.scan_expired_at_skip_non_alerting_unbounded(effective_watermark);
                // Barrier watermark = the effective (wall-clock advanced) scan
                // watermark, so an idle shard still advances its barrier and the
                // conv stage can seal buckets for the whole rule (without this,
                // an idle shard's stale barrier starves sealing forever).
                let watermark = effective_watermark;
                let qualifying: Vec<_> = raw.into_iter().filter(close_is_qualified).collect();
                if let Some(sink) = self.conv_sink.as_ref() {
                    // P3-D: log when the conv stage is gone (closes dropped).
                    if sink
                        .tx
                        .send(ConvCloseBatch {
                            closes: qualifying,
                            watermark,
                            drained: false,
                            barrier_index: sink.barrier_index,
                        })
                        .await
                        .is_err()
                    {
                        log::debug!("conv sink channel closed — scan batch dropped");
                    }
                }
                (rule_name, Vec::new(), true)
            } else {
                (
                    rule_name,
                    machine.scan_expired_at_with_conv_skip_non_alerting_unbounded(
                        effective_watermark,
                        self.conv_plan.as_ref(),
                    ),
                    false,
                )
            }
        };
        let mut stats = RuleBatchDebugStats::default();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        // When routed to the conv stage, skip inline close processing.
        if !routed {
            for close in &closes {
                match self.executor.execute_close_with_joins(close, &lookup) {
                    Ok(Some(record)) => {
                        if debug_enabled {
                            stats.count_output(&record, &self.intermediate_targets);
                        }
                        if debug_enabled && stats.allow_detail() {
                            log_output_emitted(
                                "execute_close",
                                "close",
                                output_kind(&record, &self.intermediate_targets),
                                &record,
                                close.scope_key.as_slice(),
                            );
                        }
                        self.emit(record).await;
                    }
                    Ok(None) => {
                        if debug_enabled {
                            stats.output_none += 1;
                        }
                        if debug_enabled && stats.allow_detail() {
                            log_output_suppressed(
                                &rule_name,
                                "execute_close",
                                Some(close.scope_key.as_slice()),
                            );
                        }
                    }
                    Err(e) => {
                        if debug_enabled {
                            stats.errors += 1;
                        }
                        wf_warn!(
                            pipe,
                            task_id = %self.task_id,
                            rule = %rule_name,
                            stage = 0,
                            phase = "execute_close",
                            scope_key = %debug_scope_key(&close.scope_key),
                            error = %e,
                            "rule output failed"
                        )
                    }
                }
            }
        }
        if debug_enabled {
            let instances_after = self.instance_count();
            wf_debug!(
                pipe,
                task_id = %self.task_id,
                rule = %rule_name,
                stage = 0,
                closes = closes.len(),
                outputs = stats.output_emitted,
                output_none = stats.output_none,
                intermediate_outputs = stats.intermediate_emitted,
                errors = stats.errors,
                instances_after = instances_after,
                detail_logged = stats.detail_logged,
                detail_suppressed = stats.detail_suppressed,
                "rule timeout scan summary"
            );
            if stats.detail_suppressed > 0 {
                wf_debug!(
                    pipe,
                    task_id = %self.task_id,
                    rule = %rule_name,
                    stage = 0,
                    detail_logged = stats.detail_logged,
                    detail_suppressed = stats.detail_suppressed,
                    "rule event details suppressed"
                );
            }
        }
        // Re-anchor the O(1) per-instance base-cost memory estimate to the exact
        // sum of live instance state (accumulated field_values / distinct_set
        // growth is otherwise invisible to the running estimate).
        if let Some(machine) = self.machine.as_mut() {
            machine.recalibrate_memory();
        }
        if let Some(metrics) = &self.metrics {
            metrics.observe_rule_scan_timeout(&rule_name, started.elapsed());
            self.update_rule_instances_metric();
        }
        // Timeout closes may have staged intermediate rows — deliver them.
        self.flush_pipes().await;
    }

    /// Close all active instances (shutdown flush) and emit alerts.
    pub(super) async fn flush(&mut self) {
        // P3：deferred join 规则——EOS/关闭时触发剩余挂起实例
        // （reason=deferred）。按最终事件时间 watermark 到期扫描（与 oracle 一致）：
        // 尾部 expiry > 最终事件时间的实例窗口未完成（事件时间域），不输出——
        // 用 i64::MAX 强评会多出尾部桶（Q8 实证：82446 → 83274，+828 条，
        // oracle/mod.rs EOS 水位扫注释同源）。missed（到期时 join 目标 append
        // 滞后）在窗口完整后重试一次，仍 miss 为真 miss。
        //
        // 2026-08-24 q4/q9 分片后：worker 自身 watermark 停在**最后批次**的事件
        // 时间（其他 worker 拿到更晚批次）→ 只用自身 watermark 会漏掉
        // expiry ≤ 数据末尾的 pending（q4 30M 丢 869 条实测）。改用**驱动窗口
        // 的全局最终事件时间**（共享窗口 max_event_time = true global data
        // tail）——与单 worker 的最终 watermark 同语义：expiry ≤ 末尾全评估，
        // > 末尾不输出。
        if self.machine.is_none() && self.deferred.is_some() {
            let final_wm = self
                .sources
                .iter()
                .map(|s| s.window.max_event_time_nanos())
                .max()
                .unwrap_or(i64::MIN);
            if final_wm > i64::MIN {
                self.scan_deferred(final_wm, wall_nanos() as i64, false).await;
            }
            // EOS 重试（2026-08-23 q8 修复）：到期时 join 目标窗口 append 滞后
            // 的 miss 实例——EOS 后所有数据已 ingest、目标窗口完整，重试命中
            // （真 miss 重试后仍不输出）。oracle 预加载完整窗口即此理想值。
            self.reevaluate_deferred_missed().await;
            // D4：EOS 后本规则不再需要右窗任何行 → 释放保留 pin（窗口恢复完全
            // 可驱逐，关停阶段不再顶着字节预算）。
            if let Some(d) = self.deferred.as_ref() {
                d.release_retention_floor();
            }
            self.flush_alerts().await;
            self.flush_pipes().await;
            return;
        }
        let Some(_) = &self.machine else {
            return;
        };
        self.cached_wall_nanos
            .store(wall_nanos(), Ordering::Relaxed);
        let started = Instant::now();
        // Shutdown flush is not processing any single input batch, so window
        // lookups read the full window (no `max_seq` watermark).
        let lookup = RegistryLookup::new(&self.router);
        // P2c: on flush a conv-rule shard routes ALL remaining raw closes to the
        // conv stage and publishes a drained barrier (i64::MAX via the batch).
        let (rule_name, closes, routed) = {
            let machine = self.machine.as_mut().expect("checked above");
            let rule_name = machine.rule_name().to_string();
            // 2026-08-23 q11 修复（分片尾部边界）：机器水位 = 本 shard 最后
            // 处理行，分片下落后全局数据末尾（尾部几行 bid 的 bidder 在其它
            // shard）——尾部会话 `last_event+gap ≤ 全局末尾` 的会被 close_all
            // 误判未完整而跳过（q11 10M 实测少 1/197095≈0.0005%）。用窗口的
            // raw `max_event_time`（全局末尾）先补扫一次（unbounded，off hot
            // path），再 close_all 收口剩余（expiry > 全局末尾 的仍跳过）。
            let machine_wm = machine.watermark_nanos();
            let final_wm = self
                .sources
                .iter()
                .map(|src| src.window.max_event_time_nanos())
                .max()
                .unwrap_or(machine_wm)
                .max(machine_wm);
            if self.conv_sink.is_some() {
                let mut extra = Vec::new();
                if final_wm > machine_wm {
                    extra = machine.scan_expired_at_skip_non_alerting_unbounded(final_wm);
                }
                let raw = machine.close_all(CloseReason::Flush);
                let watermark = final_wm.max(machine.watermark_nanos());
                let mut qualifying: Vec<_> = raw.into_iter().filter(close_is_qualified).collect();
                qualifying.extend(extra.into_iter().filter(close_is_qualified));
                if let Some(sink) = self.conv_sink.as_ref() {
                    // P3-D: log when the conv stage is gone (drained closes dropped).
                    if sink
                        .tx
                        .send(ConvCloseBatch {
                            closes: qualifying,
                            watermark,
                            drained: true,
                            barrier_index: sink.barrier_index,
                        })
                        .await
                        .is_err()
                    {
                        log::debug!("conv sink channel closed — drained flush dropped");
                    }
                }
                (rule_name, Vec::new(), true)
            } else {
                let mut closes = Vec::new();
                if final_wm > machine_wm {
                    closes = machine.scan_expired_at_with_conv_skip_non_alerting_unbounded(
                        final_wm,
                        self.conv_plan.as_ref(),
                    );
                }
                closes.extend(
                    machine.close_all_with_conv(CloseReason::Flush, self.conv_plan.as_ref()),
                );
                (rule_name, closes, false)
            }
        };
        let mut stats = RuleBatchDebugStats::default();
        let debug_enabled = tracing::enabled!(tracing::Level::DEBUG);
        // When routed to the conv stage, skip inline close processing.
        if !routed {
            for close in &closes {
                match self.executor.execute_close_with_joins(close, &lookup) {
                    Ok(Some(record)) => {
                        if debug_enabled {
                            stats.count_output(&record, &self.intermediate_targets);
                        }
                        if debug_enabled && stats.allow_detail() {
                            log_output_emitted(
                                "execute_close",
                                "close",
                                output_kind(&record, &self.intermediate_targets),
                                &record,
                                close.scope_key.as_slice(),
                            );
                        }
                        self.emit(record).await;
                    }
                    Ok(None) => {
                        if debug_enabled {
                            stats.output_none += 1;
                        }
                        if debug_enabled && stats.allow_detail() {
                            log_output_suppressed(
                                &rule_name,
                                "execute_close",
                                Some(close.scope_key.as_slice()),
                            );
                        }
                    }
                    Err(e) => {
                        if debug_enabled {
                            stats.errors += 1;
                        }
                        wf_warn!(
                            pipe,
                            task_id = %self.task_id,
                            rule = %rule_name,
                            stage = 0,
                            phase = "execute_close",
                            scope_key = %debug_scope_key(&close.scope_key),
                            error = %e,
                            "rule output failed"
                        )
                    }
                }
            }
        }
        if debug_enabled {
            let instances_after = self.instance_count();
            wf_debug!(
                pipe,
                task_id = %self.task_id,
                rule = %rule_name,
                stage = 0,
                closes = closes.len(),
                outputs = stats.output_emitted,
                output_none = stats.output_none,
                intermediate_outputs = stats.intermediate_emitted,
                errors = stats.errors,
                instances_after = instances_after,
                detail_logged = stats.detail_logged,
                detail_suppressed = stats.detail_suppressed,
                "rule flush summary"
            );
            if stats.detail_suppressed > 0 {
                wf_debug!(
                    pipe,
                    task_id = %self.task_id,
                    rule = %rule_name,
                    stage = 0,
                    detail_logged = stats.detail_logged,
                    detail_suppressed = stats.detail_suppressed,
                    "rule event details suppressed"
                );
            }
        }
        if let Some(metrics) = &self.metrics {
            metrics.observe_rule_flush(&rule_name, started.elapsed());
            self.update_rule_instances_metric();
        }
        // Drain the batched alert delivery after close emissions.
        self.flush_alerts().await;
        // Drain staged intermediate rows after close emissions (each rules
        // early-return above — their rows are covered by the per-batch
        // flush in `process_batch`).
        self.flush_pipes().await;
    }

    // -- Alert emission -----------------------------------------------------

    async fn emit(&self, record: OutputRecord) {
        if self.intermediate_targets.contains(&*record.yield_target) {
            // 2026-08-23 q4：intermediate 输出也计入 `emitted_total`——
            // verify-nexmark 读 EMIT 对拍，中间窗口行数（q4a→auction_finals
            // 的输出量）是内层语义的体现，不计则 verify 读到 0（oracle 557,204）。
            // alert detail/e2e 不采样（intermediate 非最终告警）。
            if let Some(metrics) = &self.metrics {
                metrics.inc_alert_emitted_total(&record.rule_name);
            }
            // perf-diag cut_output 门控：emitted 计数保留，跳过 pipe 装载。
            if crate::perf_diag::perf_cut_output() {
                return;
            }
            self.stage_pipe_record(record);
            return;
        }
        if let Some(metrics) = &self.metrics {
            // Exact total is cheap (one relaxed atomic); the allocation-heavy
            // detail map + e2e histogram are sampled 1-in-N (batch).
            metrics.inc_alert_emitted_total(&record.rule_name);
            let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
            let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
            if sample == 0 {
                self.emit_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                metrics.inc_alert_emitted_detail(
                    &record.rule_name,
                    &record.machine_id,
                    &record.scope_key,
                );
                let e2e_nanos = now_nanos.saturating_sub(record.event_time_nanos.max(0) as u64);
                metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
            } else {
                self.emit_sample_remaining
                    .store(sample - 1, Ordering::Relaxed);
            }
        }
        // perf-diag cut_output 门控：emitted 计数已保留（上面），跳过
        // serialize/append/stage/commit/fanout——输出链整体直通。
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        // Append straight into the per-target columnar batch, sealed and
        // flushed to the sink writers when it fills (amortizing the
        // per-alert fan-out mechanics, matching the wp-motor batch model).
        // The conversion stays on this thread on purpose: records allocated
        // here and freed on a sink thread drive mimalloc into its
        // abandoned-page reclaim path — measured ~2x rule-throughput loss.
        //
        // Serialize timing is sampled 1-in-`EMIT_METRIC_SAMPLE_INTERVAL` and
        // scaled back up (same sampling pattern as the e2e metrics): two
        // clock_gettime calls per record measured ~2.5% of on-CPU samples,
        // and the per-record timing only feeds diagnostics, not semantics.
        // (The metric covers the record→columns append, the successor of the
        // old to_data_record conversion.)
        let time_this = {
            let rem = self
                .serialize_sample_remaining
                .fetch_sub(1, Ordering::Relaxed);
            if rem == 1 {
                self.serialize_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                true
            } else {
                false
            }
        };
        let _ser_start = time_this.then(Instant::now);
        let (append_result, should_flush) = {
            let mut pending = self.pending_alerts.lock().unwrap();
            // Linear target lookup (targets are few); avoids hashing the
            // target string for every appended record.
            let slot = pending
                .by_target
                .iter_mut()
                .find(|(target, _)| *target == record.yield_target);
            let builder = match slot {
                Some((_, builder)) => builder,
                None => {
                    pending.by_target.push((
                        std::sync::Arc::clone(&record.yield_target),
                        AlertColumnBuilder::new(std::sync::Arc::clone(&record.yield_target)),
                    ));
                    let last = pending.by_target.len() - 1;
                    &mut pending.by_target[last].1
                }
            };
            let result = builder.append_record(&record);
            if result.is_ok() {
                pending.count += 1;
            }
            (result, pending.count >= ALERT_BATCH_SIZE)
        };
        if let Err(e) = append_result {
            if let Some(metrics) = &self.metrics {
                metrics.inc_alert_serialize_failed();
            }
            log::warn!("alert export error: {e}");
            return;
        }
        if let Some(start) = _ser_start {
            let elapsed = start.elapsed().as_nanos() as u64;
            let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64;
            self.serialize_nanos.fetch_add(scaled, Ordering::Relaxed);
            if let Some(metrics) = &self.metrics {
                metrics.add_alert_serialize_nanos(scaled);
            }
        }
        if should_flush {
            self.flush_alerts().await;
        }
    }

    /// Batch twin of [`Self::emit`]: append a whole group of already-built
    /// records to the per-target columnar builder under **one** pending lock
    /// and one target lookup, flushing when the pending batch fills. Records
    /// are appended in order; telemetry is exact (same counters as
    /// [`Self::emit`]); the serialize timing covers the whole group and is
    /// sampled 1-in-`EMIT_METRIC_SAMPLE_INTERVAL` (scaled by group size) —
    /// same diagnostic shape as the per-record sampler.
    ///
    /// The q12-style close/match fan-out emits one record per closed window;
    /// per-record lock + target lookup + await-poll was measurable on the
    /// profiling hot path (emit_nanos dominated the q12 batch budget), while
    /// the append itself is a Vec push per column.
    async fn emit_batch(&self, records: Vec<OutputRecord>) {
        let n = records.len();
        if n == 0 {
            return;
        }
        // Exact totals + sampled detail/e2e — identical accounting to
        // [`Self::emit`] (the sampler state lives on the rule task, so the
        // cadence is unchanged whether records arrive one-by-one or in a group).
        if let Some(metrics) = &self.metrics {
            let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
            for record in &records {
                metrics.inc_alert_emitted_total(&record.rule_name);
                let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                if sample == 0 {
                    self.emit_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    metrics.inc_alert_emitted_detail(
                        &record.rule_name,
                        &record.machine_id,
                        &record.scope_key,
                    );
                    let e2e_nanos = now_nanos.saturating_sub(record.event_time_nanos.max(0) as u64);
                    metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                } else {
                    self.emit_sample_remaining
                        .store(sample - 1, Ordering::Relaxed);
                }
            }
        }
        // Split off intermediate (pipe) targets — same relay semantics as the
        // per-record path, before any sink append.
        let mut pipe_records = Vec::new();
        let mut sink_records: Vec<OutputRecord> = Vec::with_capacity(n);
        for record in records {
            if self.intermediate_targets.contains(&*record.yield_target) {
                pipe_records.push(record);
            } else {
                sink_records.push(record);
            }
        }
        // perf-diag cut_output 门控：emitted 计数已保留，pipe/sink 输出链直通。
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        for record in pipe_records {
            self.stage_pipe_record(record);
        }
        if sink_records.is_empty() {
            return;
        }
        let time_this = {
            let rem = self
                .serialize_sample_remaining
                .fetch_sub(1, Ordering::Relaxed);
            if rem == 1 {
                self.serialize_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                true
            } else {
                false
            }
        };
        let _ser_start = time_this.then(Instant::now);
        let should_flush = {
            let mut pending = self.pending_alerts.lock().unwrap();
            let target = &sink_records[0].yield_target;
            let slot = pending
                .by_target
                .iter_mut()
                .find(|(existing, _)| *existing == *target);
            let builder = match slot {
                Some((_, builder)) => builder,
                None => {
                    pending.by_target.push((
                        std::sync::Arc::clone(target),
                        AlertColumnBuilder::new(std::sync::Arc::clone(target)),
                    ));
                    let last = pending.by_target.len() - 1;
                    &mut pending.by_target[last].1
                }
            };
            let mut failed = 0usize;
            for record in &sink_records {
                if builder.append_record(record).is_err() {
                    failed += 1;
                }
            }
            pending.count += sink_records.len() - failed;
            if failed > 0
                && let Some(metrics) = &self.metrics
            {
                for _ in 0..failed {
                    metrics.inc_alert_serialize_failed();
                }
            }
            pending.count >= ALERT_BATCH_SIZE
        };
        if let Some(start) = _ser_start {
            // Sampled 1-in-64 *batches* (the sampler decrements once per group),
            // so the report scales the group's append time by
            // EMIT_METRIC_SAMPLE_INTERVAL only — multiplying by the group size
            // as well double-counted (group duration already covers all n rows).
            let elapsed = start.elapsed().as_nanos() as u64;
            let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64;
            self.serialize_nanos.fetch_add(scaled, Ordering::Relaxed);
            if let Some(metrics) = &self.metrics {
                metrics.add_alert_serialize_nanos(scaled);
            }
        }
        if should_flush {
            self.flush_alerts().await;
        }
    }

    /// Accumulate one produced record into `staged`, draining through
    /// [`Self::emit_batch`] once the group reaches [`ALERT_BATCH_SIZE`] — keeps
    /// the flush cadence and pending memory bound of the per-record path.
    async fn stage_or_emit_record(&self, staged: &mut Vec<OutputRecord>, record: OutputRecord) {
        staged.push(record);
        if staged.len() >= ALERT_BATCH_SIZE {
            self.emit_batch(std::mem::take(staged)).await;
        }
    }

    /// Direct-write on-each emit (plan C2): the executor evaluates the event
    /// and appends the row straight into the per-target columnar builder —
    /// no per-record `OutputRecord` materialization. Mirrors [`Self::emit`]'s
    /// telemetry (exact totals, 1-in-N sampled detail/e2e, sampled serialize
    /// timing) and batch-flush trigger.
    ///
    /// One diagnostic difference from the record path: the sampled detail's
    /// machine id is extracted from the pre-join event (joins that rebind
    /// the machine-id field would show a different label). Only affects the
    /// metric label, not semantics.
    async fn emit_each_direct(
        &self,
        event: &Event,
        event_nanos: i64,
        lookup: &RegistryLookup<'_>,
        field_order: &[&smol_str::SmolStr],
        batch_emit_nanos: i64,
    ) -> wf_engine::error::CoreResult<bool> {
        // perf-diag cut_output 门控：on-each 直接写路径在 append 前整体直通
        // （该路径的 emitted 计数与 append 耦合，无法保留计数而切 append）。
        if crate::perf_diag::perf_cut_output() {
            return Ok(false);
        }
        // Serialize timing is sampled 1-in-N and scaled back up (same
        // pattern as `emit`; covers the eval + column append).
        let time_this = {
            let rem = self
                .serialize_sample_remaining
                .fetch_sub(1, Ordering::Relaxed);
            if rem == 1 {
                self.serialize_sample_remaining
                    .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                true
            } else {
                false
            }
        };
        let _ser_start = time_this.then(Instant::now);
        let (result, should_flush) = {
            let mut pending = self.pending_alerts.lock().unwrap();
            // Linear target lookup via the plan-constant Arc (targets are
            // few); first append creates the builder.
            let target = self.executor.static_yield_target();
            let slot = pending
                .by_target
                .iter_mut()
                .find(|(existing, _)| **existing == **target);
            let builder = match slot {
                Some((_, builder)) => builder,
                None => {
                    pending.by_target.push((
                        std::sync::Arc::clone(target),
                        AlertColumnBuilder::new(std::sync::Arc::clone(target)),
                    ));
                    let last = pending.by_target.len() - 1;
                    &mut pending.by_target[last].1
                }
            };
            let result = self.executor.execute_each_direct(
                event,
                event_nanos,
                lookup,
                field_order,
                batch_emit_nanos,
                builder,
            );
            if let Ok(true) = &result {
                pending.count += 1;
            }
            (result, pending.count >= ALERT_BATCH_SIZE)
        };
        if let Ok(true) = &result {
            if let Some(metrics) = &self.metrics {
                // Exact total is cheap; the allocation-heavy detail map +
                // e2e histogram are sampled 1-in-N.
                metrics.inc_alert_emitted_total(self.rule_name());
                let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
                let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                if sample == 0 {
                    self.emit_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    metrics.inc_alert_emitted_detail(
                        self.rule_name(),
                        &RuleExecutor::machine_id_of(event),
                        self.rule_name(),
                    );
                    let e2e_nanos = now_nanos.saturating_sub(event_nanos.max(0) as u64);
                    metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                } else {
                    self.emit_sample_remaining
                        .store(sample - 1, Ordering::Relaxed);
                }
            }
        } else if let Err(e) = &result {
            if let Some(metrics) = &self.metrics {
                metrics.inc_alert_serialize_failed();
            }
            log::warn!("alert export error: {e}");
        }
        if let Some(start) = _ser_start {
            let elapsed = start.elapsed().as_nanos() as u64;
            let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64;
            self.serialize_nanos.fetch_add(scaled, Ordering::Relaxed);
            if let Some(metrics) = &self.metrics {
                metrics.add_alert_serialize_nanos(scaled);
            }
        }
        if should_flush {
            self.flush_alerts().await;
        }
        result
    }

    /// Batched direct-write on-each emit (build_each_direct vectorization):
    /// runs [`RuleExecutor::execute_each_direct_batch`] over the events the
    /// main loop collected for this rule, in segments of `ALERT_BATCH_SIZE`
    /// events so the flush cadence and the pending-alerts memory bound stay
    /// identical to the per-event path.
    ///
    /// Telemetry mirrors [`Self::emit_each_direct`]: exact `emitted_total`
    /// per appended row (via the appended-index list, outside the builder
    /// lock), 1-in-N sampled detail/e2e per appended row, and serialize
    /// timing sampled per segment and scaled by the per-call average (a
    /// segment covers many "calls", so the scaled estimate stays comparable
    /// to the per-event path's accounting).
    async fn emit_each_direct_batch(
        &self,
        rows: &[(&wf_engine::match_engine::Event, i64)],
        lookup: &RegistryLookup<'_>,
        field_order: &[&smol_str::SmolStr],
        batch_emit_nanos: i64,
    ) {
        // perf-diag cut_output 门控（见 [`Self::emit_each_direct`]）。
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        let mut appended_idx: Vec<usize> = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let end = (start + ALERT_BATCH_SIZE).min(rows.len());
            let segment = &rows[start..end];
            let calls = segment.len();
            let time_this = {
                let rem = self
                    .serialize_sample_remaining
                    .fetch_sub(1, Ordering::Relaxed);
                if rem == 1 {
                    self.serialize_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            };
            let _ser_start = time_this.then(Instant::now);
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                // Linear target lookup via the plan-constant Arc — same as
                // the per-event path.
                let target = self.executor.static_yield_target();
                let slot = pending
                    .by_target
                    .iter_mut()
                    .find(|(existing, _)| **existing == **target);
                let builder = match slot {
                    Some((_, builder)) => builder,
                    None => {
                        pending.by_target.push((
                            std::sync::Arc::clone(target),
                            AlertColumnBuilder::new(std::sync::Arc::clone(target)),
                        ));
                        let last = pending.by_target.len() - 1;
                        &mut pending.by_target[last].1
                    }
                };
                let outcome = self.executor.execute_each_direct_batch(
                    segment,
                    lookup,
                    field_order,
                    batch_emit_nanos,
                    builder,
                    &mut appended_idx,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            // Per-row telemetry outside the builder lock (exact totals,
            // 1-in-N sampled detail/e2e — same accounting as the per-event
            // path).
            if let Some(metrics) = &self.metrics {
                for &idx in appended_idx.iter() {
                    metrics.inc_alert_emitted_total(self.rule_name());
                    let (event, event_nanos) = segment[idx];
                    let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
                    let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                    if sample == 0 {
                        self.emit_sample_remaining
                            .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                        metrics.inc_alert_emitted_detail(
                            self.rule_name(),
                            &RuleExecutor::machine_id_of(event),
                            self.rule_name(),
                        );
                        let e2e_nanos = now_nanos.saturating_sub(event_nanos.max(0) as u64);
                        metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                    } else {
                        self.emit_sample_remaining
                            .store(sample - 1, Ordering::Relaxed);
                    }
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_serialize_failed();
                }
            }
            if let Some(ser_start) = _ser_start {
                let elapsed = ser_start.elapsed().as_nanos() as u64;
                // A segment covers `calls` per-event "calls"; scale the
                // sampled segment time back to the per-call average × the
                // sample interval so the accumulator stays comparable with
                // the per-event path's accounting.
                let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64 / calls.max(1) as u64;
                self.serialize_nanos.fetch_add(scaled, Ordering::Relaxed);
                if let Some(metrics) = &self.metrics {
                    metrics.add_alert_serialize_nanos(scaled);
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
            start = end;
        }
    }

    /// Columnar twin of [`Self::emit_each_direct_batch`]: same flush cadence /
    /// pending bound / telemetry accounting, but the executor reads field
    /// values straight from the Arrow columns via [`ColumnarEvent`] (no
    /// per-row `Event` materialization). Caller gates on
    /// `each_plan_columnar_safe()`.
    async fn emit_each_direct_batch_columnar(
        &self,
        rows: &[(&ColumnarEvent<'_>, i64)],
        batch_emit_nanos: i64,
    ) {
        // perf-diag cut_output 门控（见 [`Self::emit_each_direct`]）。
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        let mut appended_idx: Vec<usize> = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let end = (start + ALERT_BATCH_SIZE).min(rows.len());
            let segment = &rows[start..end];
            let calls = segment.len();
            let time_this = {
                let rem = self
                    .serialize_sample_remaining
                    .fetch_sub(1, Ordering::Relaxed);
                if rem == 1 {
                    self.serialize_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            };
            let _ser_start = time_this.then(Instant::now);
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                let target = self.executor.static_yield_target();
                let slot = pending
                    .by_target
                    .iter_mut()
                    .find(|(existing, _)| **existing == **target);
                let builder = match slot {
                    Some((_, builder)) => builder,
                    None => {
                        pending.by_target.push((
                            std::sync::Arc::clone(target),
                            AlertColumnBuilder::new(std::sync::Arc::clone(target)),
                        ));
                        let last = pending.by_target.len() - 1;
                        &mut pending.by_target[last].1
                    }
                };
                let outcome = self.executor.execute_each_direct_batch_columnar(
                    segment,
                    batch_emit_nanos,
                    builder,
                    &mut appended_idx,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            // Per-row telemetry outside the builder lock — same accounting as
            // the Event-based batch path; the machine_id comes from the column.
            if let Some(metrics) = &self.metrics {
                for &idx in appended_idx.iter() {
                    metrics.inc_alert_emitted_total(self.rule_name());
                    let (event, event_nanos) = segment[idx];
                    let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
                    let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                    if sample == 0 {
                        self.emit_sample_remaining
                            .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                        metrics.inc_alert_emitted_detail(
                            self.rule_name(),
                            &event.field_value_str(wf_engine::match_engine::MACHINE_ID),
                            self.rule_name(),
                        );
                        let e2e_nanos = now_nanos.saturating_sub(event_nanos.max(0) as u64);
                        metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                    } else {
                        self.emit_sample_remaining
                            .store(sample - 1, Ordering::Relaxed);
                    }
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_serialize_failed();
                }
            }
            if let Some(ser_start) = _ser_start {
                let elapsed = ser_start.elapsed().as_nanos() as u64;
                let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64 / calls.max(1) as u64;
                self.serialize_nanos.fetch_add(scaled, Ordering::Relaxed);
                if let Some(metrics) = &self.metrics {
                    metrics.add_alert_serialize_nanos(scaled);
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
            start = end;
        }
    }

    /// Columnar join-enrichment emit (2026-08-23): [`Self::emit_each_direct_batch_columnar`]
    /// for the live-join case — same batching/telemetry/flush, but the executor
    /// runs the batch-level join lookup + columnar right-window reads
    /// (`execute_each_direct_batch_columnar_join`). The per-row telemetry's
    /// machine_id still comes from the driving event column.
    async fn emit_each_direct_batch_columnar_join(
        &self,
        rows: &[(&ColumnarEvent<'_>, i64)],
        lookup: &RegistryLookup<'_>,
        batch_emit_nanos: i64,
    ) {
        // perf-diag cut_output 门控（见 [`Self::emit_each_direct`]）。
        if crate::perf_diag::perf_cut_output() {
            return;
        }
        let mut appended_idx: Vec<usize> = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let end = (start + ALERT_BATCH_SIZE).min(rows.len());
            let segment = &rows[start..end];
            let calls = segment.len();
            let time_this = {
                let rem = self
                    .serialize_sample_remaining
                    .fetch_sub(1, Ordering::Relaxed);
                if rem == 1 {
                    self.serialize_sample_remaining
                        .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                    true
                } else {
                    false
                }
            };
            let _ser_start = time_this.then(Instant::now);
            let (outcome, should_flush) = {
                let mut pending = self.pending_alerts.lock().unwrap();
                let target = self.executor.static_yield_target();
                let slot = pending
                    .by_target
                    .iter_mut()
                    .find(|(existing, _)| **existing == **target);
                let builder = match slot {
                    Some((_, builder)) => builder,
                    None => {
                        pending.by_target.push((
                            std::sync::Arc::clone(target),
                            AlertColumnBuilder::new(std::sync::Arc::clone(target)),
                        ));
                        let last = pending.by_target.len() - 1;
                        &mut pending.by_target[last].1
                    }
                };
                let outcome = self.executor.execute_each_direct_batch_columnar_join(
                    segment,
                    lookup,
                    batch_emit_nanos,
                    builder,
                    &mut appended_idx,
                );
                pending.count += outcome.appended;
                (outcome, pending.count >= ALERT_BATCH_SIZE)
            };
            // Per-row telemetry outside the builder lock — same accounting as
            // the join-free columnar path; machine_id comes from the column.
            if let Some(metrics) = &self.metrics {
                for &idx in appended_idx.iter() {
                    metrics.inc_alert_emitted_total(self.rule_name());
                    let (event, event_nanos) = segment[idx];
                    let now_nanos = self.cached_wall_nanos.load(Ordering::Relaxed);
                    let sample = self.emit_sample_remaining.load(Ordering::Relaxed);
                    if sample == 0 {
                        self.emit_sample_remaining
                            .store(EMIT_METRIC_SAMPLE_INTERVAL, Ordering::Relaxed);
                        metrics.inc_alert_emitted_detail(
                            self.rule_name(),
                            &event.field_value_str(wf_engine::match_engine::MACHINE_ID),
                            self.rule_name(),
                        );
                        let e2e_nanos = now_nanos.saturating_sub(event_nanos.max(0) as u64);
                        metrics.observe_event_e2e_latency(Duration::from_nanos(e2e_nanos));
                    } else {
                        self.emit_sample_remaining
                            .store(sample - 1, Ordering::Relaxed);
                    }
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_serialize_failed();
                }
            }
            if let Some(ser_start) = _ser_start {
                let elapsed = ser_start.elapsed().as_nanos() as u64;
                let scaled = elapsed * EMIT_METRIC_SAMPLE_INTERVAL as u64 / calls.max(1) as u64;
                self.serialize_nanos.fetch_add(scaled, Ordering::Relaxed);
                if let Some(metrics) = &self.metrics {
                    metrics.add_alert_serialize_nanos(scaled);
                }
            }
            if should_flush {
                self.flush_alerts().await;
            }
            start = end;
        }
    }

    /// Flush the accumulated columnar alert batches to the sink writers,
    /// grouped by yield_target. Each sink receives one `AlertBatch` (a single
    /// channel send) of columnar records, amortizing the per-alert resolve /
    /// try_send / blocking that dominated the q1 pass-through emit path.
    async fn flush_alerts(&self) {
        // Builder-lifetime optimization: only the sealed columns leave the
        // pending slot — the `AlertColumnBuilder` itself stays resident for
        // the rule task's lifetime (its `staged` buffer keeps its capacity;
        // the layout cache is re-resolved on the next first row, see
        // `finish()`). Previously the whole pending (builder included) was
        // taken and dropped every flush, re-instantiating the builder every
        // ALERT_BATCH_SIZE rows.
        let batches: Vec<(Arc<str>, AlertColumnBatch)> = {
            let mut guarded = self.pending_alerts.lock().unwrap();
            if guarded.count == 0 {
                return;
            }
            guarded.count = 0;
            guarded
                .by_target
                .iter_mut()
                .map(|(target, builder)| (Arc::clone(target), builder.finish()))
                .collect()
        };
        let _fan_start = Instant::now();
        for (target, batch) in batches {
            let records_len = batch.len();
            let sink_groups = self.sink_fanout.resolve(&target);
            if sink_groups.is_empty() {
                if let Some(metrics) = &self.metrics {
                    metrics.add_alert_no_sink_records(records_len as u64);
                }
                self.sink_fanout.warn_if_no_sink(&target);
                continue;
            }
            let batch = crate::alert_task::AlertBatch::Columns(Arc::new(batch));
            for (sink_ptr, channels) in sink_groups.iter() {
                // Round-robin across this sink's parallel writers.
                let idx = self.sink_fanout.next_index(*sink_ptr, channels.len());
                let tx = &channels[idx];
                match tx.try_send(batch.clone()) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(batch)) => {
                        if let Some(metrics) = &self.metrics {
                            metrics.inc_alert_channel_full();
                        }
                        // Fall back to blocking send (backpressure).
                        if let Err(e) = tx.send(batch).await {
                            if let Some(metrics) = &self.metrics {
                                metrics.inc_alert_channel_send_failed();
                            }
                            wf_warn!(pipe, error = %e, "alert channel closed");
                        }
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        // Channel is closed — drop the batch
                        if let Some(metrics) = &self.metrics {
                            metrics.inc_alert_channel_send_failed();
                        }
                        wf_warn!(pipe, rule = %target, "alert channel closed, dropping alert batch");
                    }
                }
            }
        }
        self.fanout_nanos
            .fetch_add(_fan_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }

    /// Stage an intermediate-target row into the columnar pipe buffer
    /// (rule-side channelization). [`Self::flush_pipes`] turns the staged
    /// rows into one batch + one fanout broadcast at the end of the input
    /// batch — the relay semantics of the old per-row `emit_window_record`
    /// (pure relay, no window store, seq `u64::MAX`) with the per-row Arrow
    /// assembly and channel sends amortized away.
    fn stage_pipe_record(&self, record: OutputRecord) {
        let mut guard = self.pipe_state.lock().unwrap();
        match &mut *guard {
            PipeState::Dead => {}
            PipeState::Staging(stager) => {
                if let Err(e) = stager.push_record(&record) {
                    wf_warn!(
                        pipe,
                        task_id = %self.task_id,
                        rule = %record.rule_name,
                        target = %record.yield_target,
                        output_kind = "intermediate",
                        error = %e,
                        "stage internal pipeline row failed"
                    );
                }
            }
            PipeState::Uninit => {
                // Resolve the pipe shape once, lazily (pipe registry schema
                // first, window fallback — same resolution order and failure
                // semantics as the old per-row path).
                let target = Arc::clone(&record.yield_target);
                match resolve_pipe_shape(&self.pipe_registry, &self.router, &target) {
                    Some((schema, time_col_index)) => {
                        let mut stager = PipeBatchStager::new(target, schema, time_col_index);
                        if let Err(e) = stager.push_record(&record) {
                            wf_warn!(
                                pipe,
                                task_id = %self.task_id,
                                rule = %record.rule_name,
                                output_kind = "intermediate",
                                error = %e,
                                "stage internal pipeline row failed"
                            );
                        }
                        *guard = PipeState::Staging(stager);
                    }
                    None => {
                        wf_warn!(
                            pipe,
                            task_id = %self.task_id,
                            rule = %record.rule_name,
                            target = %target,
                            output_kind = "intermediate",
                            reason = "missing_internal_window",
                            "missing internal pipeline window"
                        );
                        *guard = PipeState::Dead;
                    }
                }
            }
        }
    }

    /// Flush staged intermediate rows: build one N-row `RecordBatch`, parse
    /// it to events once, and hand it to the pipe's downstream-rule
    /// subscribers with a single broadcast. Called at the end of every
    /// input batch (and on timeout/flush emissions), so delivery latency is
    /// bounded exactly like the batched sink-alert delivery.
    async fn flush_pipes(&self) {
        let built = {
            let mut guard = self.pipe_state.lock().unwrap();
            match &mut *guard {
                PipeState::Staging(stager) => match stager.take_events() {
                    Ok(built) => built,
                    Err(e) => {
                        wf_warn!(
                            pipe,
                            task_id = %self.task_id,
                            output_kind = "intermediate",
                            error = %e,
                            "build internal pipeline batch failed, dropping staged rows"
                        );
                        None
                    }
                },
                _ => None,
            }
        };
        if let Some((target, events, batch)) = built {
            // 2026-08-23 q4 修复：pipe relay 若只广播（纯 relay，无窗口存储），
            // **pull 模式**的列式下游（stats 任务从窗口读）收不到——
            // q4a→auction_finals→q4b(stats) 默认 pull 双规则链断链（q4b EMIT=0）。
            // 修复：append 到目标窗口（带分片行子集，供 pull 分片消费方读）+
            // 广播（带 batch，供 push 消费方收）。两者共享同一批次，无复制。
            if let Some(win) = self.router.registry().get_window(target.as_ref()) {
                let shard_rows = self
                    .router
                    .fanout()
                    .precompute_shard_rows(target.as_ref(), &batch);
                // 2026-08-23 q13：广播带**真实窗口批次 seq**（append 返回）——
                // 此前固定 u64::MAX 使下游 push 规则的 ack 不反映真实消费进度，
                // 窗口 acked_lag 恒 0，bench 完成判定（等待 lag 归零）在中间
                // 管道下游未消费完时就 SIGTERM（q13b 只处理 2/25 批）。
                let seq = win
                    .append_with_watermark_sized(
                        batch.clone(),
                        wf_engine::window::content_bytes(&batch),
                        shard_rows.map(|s| {
                            let v: Vec<Vec<u32>> = s.iter().cloned().collect();
                            std::sync::Arc::new(v)
                        }),
                    )
                    .map(|(_, seq)| seq)
                    .unwrap_or(0);
                // 2026-08-23 q13：直接 append（不走窗口 actor）不触发窗口 Notify——
                // pull 模型下游（bind 中间窗口的 rule_task）靠 Notify 唤醒，漏通知
                // 则消费停滞（q13b 只处理已拉取的部分，EMIT 严重不足）。append 后
                // 显式 notify_waiters，与 actor 路径的通知语义对齐。
                if let Some(notifier) = self.router.registry().get_notifier(target.as_ref()) {
                    notifier.notify_waiters();
                }
                let fan_start = Instant::now();
                self.router
                    .fanout()
                    .broadcast_with_batch(&target, &events, &batch, None, seq)
                    .await;
                self.fanout_nanos
                    .fetch_add(fan_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        }
    }
}

/// Whether `alias`'s bind filter accepts `row` of the current batch, using the
/// precomputed columnar mask when available and falling back to the per-event
/// interpreted path otherwise.
fn alias_accepts(
    executor: &RuleExecutor,
    masks: &HashMap<String, Option<BooleanArray>>,
    alias: &str,
    row: usize,
    event: &dyn FieldSource,
    lookup: &RegistryLookup<'_>,
) -> bool {
    match masks.get(alias) {
        Some(Some(mask)) => mask.value(row),
        _ => executor.event_matches_alias(alias, event, Some(lookup)),
    }
}

fn event_debug_ref(
    event: &wf_engine::match_engine::Event,
    batch_seq: u64,
    row_index: usize,
) -> String {
    event
        .fields
        .get("event_id")
        .or_else(|| event.fields.get(WFU_ID))
        .or_else(|| event.fields.get("id"))
        .map(value_debug_string)
        .unwrap_or_else(|| format!("batch:{batch_seq}/row:{row_index}"))
}

/// Debug rendering for a [`RowEvent`]: the Eager arm delegates to the event's
/// fields; the Columnar arm has no materialized fields and is mutually
/// exclusive with debug detail (deferral requires `!debug_enabled`).
fn row_event_debug_ref(ev: &RowEvent<'_>, batch_seq: u64, row_index: usize) -> String {
    match ev {
        RowEvent::Eager(e) => event_debug_ref(e, batch_seq, row_index),
        RowEvent::Columnar(_) => format!("batch:{batch_seq}/row:{row_index}"),
    }
}

fn value_debug_string(value: &wf_engine::match_engine::Value) -> String {
    match value {
        wf_engine::match_engine::Value::Number(value) => value.to_string(),
        wf_engine::match_engine::Value::Str(value) => value.to_string(),
        wf_engine::match_engine::Value::Bool(value) => value.to_string(),
        wf_engine::match_engine::Value::Array(_) | wf_engine::match_engine::Value::Object(_) => {
            "<structured>".to_string()
        }
    }
}

fn debug_scope_key(scope_key: &[wf_engine::match_engine::Value]) -> String {
    scope_key
        .iter()
        .map(value_debug_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn log_output_emitted(
    phase: &'static str,
    origin: &'static str,
    output_kind: &'static str,
    record: &OutputRecord,
    scope_key: &[wf_engine::match_engine::Value],
) {
    wf_debug!(
        pipe,
        rule = %record.rule_name,
        stage = 0,
        phase = phase,
        origin = origin,
        target = %record.yield_target,
        scope_key = %debug_scope_key(scope_key),
        output_kind = output_kind,
        "rule output emitted"
    );
}

fn output_kind(record: &OutputRecord, intermediate_targets: &HashSet<String>) -> &'static str {
    if intermediate_targets.contains(&*record.yield_target) {
        "intermediate"
    } else {
        "alert"
    }
}

fn log_output_suppressed(
    rule_name: &str,
    phase: &'static str,
    scope_key: Option<&[wf_engine::match_engine::Value]>,
) {
    let scope_present = scope_key.is_some();
    wf_debug!(
        pipe,
        rule = %rule_name,
        stage = 0,
        phase = phase,
        scope_key = %scope_key.map(debug_scope_key).unwrap_or_else(|| "<none>".to_string()),
        scope_present = scope_present,
        reason = "executor_returned_none",
        "rule output suppressed"
    );
}

/// Columnar staging state for the intermediate-target emit path
/// (rule-side channelization).
enum PipeState {
    /// No intermediate row emitted yet; the pipe shape resolves lazily on
    /// first use (the pipe registry may still be populating at boot).
    Uninit,
    /// Shape resolved; rows accumulate in the column buffers until the next
    /// [`RuleTask::flush_pipes`].
    Staging(PipeBatchStager),
    /// Target window/pipe missing (warned once); rows are dropped — the
    /// same terminal behavior as the old per-row fallback.
    Dead,
}

/// Per-column staging buffer. The variant is chosen once from the pipe
/// schema; every row appends exactly one value (or null).
enum PipeCol {
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Bool(Vec<Option<bool>>),
    Utf8(Vec<Option<String>>),
    Timestamp(Vec<Option<i64>>),
    /// Column types outside the supported coercion matrix stage as null —
    /// same fallback arm as `value_to_single_row_array`.
    Null {
        data_type: DataType,
        len: usize,
    },
}

/// Resolved shape of an intermediate pipe target: the relay schema and its
/// time column (pipe registry first, window fallback).
fn resolve_pipe_shape(
    pipe_registry: &Arc<wf_engine::pipe::PipeRegistry>,
    router: &Arc<Router>,
    target: &Arc<str>,
) -> Option<(arrow::datatypes::SchemaRef, Option<usize>)> {
    match pipe_registry.get(target) {
        // Pipe registered with a real schema (normal boot) → use it.
        Some(pipe) if !pipe.schema.fields().is_empty() => Some((pipe.schema, pipe.time_col_index)),
        // Pipe absent or built without schemas (e.g. the reload path builds
        // the registry with no window schemas) → fall back to the window,
        // which is always populated with the correct schema + time column.
        _ => router
            .registry()
            .get_window(target)
            .map(|win| (win.schema().clone(), win.time_col_index())),
    }
}

impl PipeBatchStager {
    fn new(
        target: Arc<str>,
        schema: arrow::datatypes::SchemaRef,
        time_col_index: Option<usize>,
    ) -> Self {
        let cols = schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                DataType::Int64 => PipeCol::Int64(Vec::new()),
                DataType::Float64 => PipeCol::Float64(Vec::new()),
                DataType::Boolean => PipeCol::Bool(Vec::new()),
                DataType::Utf8 => PipeCol::Utf8(Vec::new()),
                DataType::Timestamp(_, _) => PipeCol::Timestamp(Vec::new()),
                other => PipeCol::Null {
                    data_type: other.clone(),
                    len: 0,
                },
            })
            .collect();
        Self {
            target,
            schema,
            time_col_index,
            cols,
            rows: 0,
        }
    }

    /// Stage one emitted row. The coercion matrix mirrors
    /// `value_to_single_row_array` exactly (including the event-time
    /// fallbacks for the pipe event-time field and the schema's time
    /// column), so a flushed batch is byte-identical to concatenating the
    /// per-row batches the old path produced.
    fn push_record(&mut self, record: &OutputRecord) -> RuntimeResult<()> {
        let event_time_nanos = record.event_time_nanos;
        let fields = record_window_fields(record);
        for (idx, field) in self.schema.fields().iter().enumerate() {
            let value = fields
                .iter()
                .find(|(name, _)| **name == *field.name())
                .map(|(_, value)| value);
            if field.name() == PIPE_EVENT_TIME_FIELD {
                match &mut self.cols[idx] {
                    PipeCol::Timestamp(v) => v.push(Some(event_time_nanos)),
                    PipeCol::Null { len, .. } => *len += 1,
                    _ => unreachable!("event-time field must be Timestamp"),
                }
                continue;
            }
            let col = &mut self.cols[idx];
            match col {
                PipeCol::Int64(v) => v.push(match value {
                    Some(wf_engine::match_engine::Value::Number(n)) => Some(*n as i64),
                    _ => None,
                }),
                PipeCol::Float64(v) => v.push(match value {
                    Some(wf_engine::match_engine::Value::Number(n)) => Some(*n),
                    _ => None,
                }),
                PipeCol::Bool(v) => v.push(match value {
                    Some(wf_engine::match_engine::Value::Bool(b)) => Some(*b),
                    _ => None,
                }),
                PipeCol::Utf8(v) => {
                    v.push(match value {
                        Some(wf_engine::match_engine::Value::Str(s)) => Some(s.to_string()),
                        Some(wf_engine::match_engine::Value::Number(n)) => Some(n.to_string()),
                        Some(wf_engine::match_engine::Value::Bool(b)) => Some(b.to_string()),
                        Some(
                            value @ (wf_engine::match_engine::Value::Array(_)
                            | wf_engine::match_engine::Value::Object(_)),
                        ) => Some(value_to_json_string(value)?),
                        _ => None,
                    });
                }
                PipeCol::Timestamp(v) => v.push(match value {
                    Some(wf_engine::match_engine::Value::Number(n)) => {
                        normalize_epoch_timestamp_float_nanos(*n)
                    }
                    // The schema's time column falls back to the row's event
                    // time when the yield did not provide one.
                    None if self.time_col_index == Some(idx) => Some(event_time_nanos),
                    _ => None,
                }),
                PipeCol::Null { len, .. } => *len += 1,
            }
        }
        self.rows += 1;
        Ok(())
    }

    /// Build the staged rows into one batch and parse it to events,
    /// resetting the buffers. Returns `None` when nothing is staged.
    fn take_events(&mut self) -> RuntimeResult<PendingEventBatch> {
        if self.rows == 0 {
            return Ok(None);
        }
        let arrays: Vec<arrow::array::ArrayRef> = self
            .cols
            .iter_mut()
            .map(|col| match col {
                PipeCol::Int64(v) => Ok(std::sync::Arc::new(arrow::array::Int64Array::from(
                    std::mem::take(v),
                )) as arrow::array::ArrayRef),
                PipeCol::Float64(v) => Ok(std::sync::Arc::new(arrow::array::Float64Array::from(
                    std::mem::take(v),
                )) as arrow::array::ArrayRef),
                PipeCol::Bool(v) => Ok(std::sync::Arc::new(arrow::array::BooleanArray::from(
                    std::mem::take(v),
                )) as arrow::array::ArrayRef),
                PipeCol::Utf8(v) => Ok(std::sync::Arc::new(arrow::array::StringArray::from(
                    std::mem::take(v),
                )) as arrow::array::ArrayRef),
                PipeCol::Timestamp(v) => Ok(std::sync::Arc::new(
                    arrow::array::TimestampNanosecondArray::from(std::mem::take(v)),
                ) as arrow::array::ArrayRef),
                PipeCol::Null { data_type, len } => {
                    let array = new_null_array(data_type, *len);
                    *len = 0;
                    Ok(array)
                }
            })
            .collect::<RuntimeResult<Vec<_>>>()?;
        let batch = RecordBatch::try_new(std::sync::Arc::clone(&self.schema), arrays)
            .source_raw_err(RuntimeReason::Bootstrap, "build internal pipeline batch")?;
        self.rows = 0;
        let events: Arc<Vec<Arc<Event>>> = Arc::new(
            wf_engine::match_engine::batch_to_events(&batch)
                .into_iter()
                .map(Arc::new)
                .collect(),
        );
        Ok(Some((Arc::clone(&self.target), events, batch)))
    }
}

struct PipeBatchStager {
    target: Arc<str>,
    schema: arrow::datatypes::SchemaRef,
    time_col_index: Option<usize>,
    cols: Vec<PipeCol>,
    rows: usize,
}

fn record_window_fields(
    record: &OutputRecord,
) -> Vec<(std::sync::Arc<str>, wf_engine::match_engine::Value)> {
    let mut fields = record.yield_fields.clone();
    let existing: HashSet<&str> = fields.iter().map(|(name, _)| &**name).collect();
    let missing_meta: Vec<WfuIntermediateMetaField> = WFU_INTERMEDIATE_META_FIELDS
        .iter()
        .copied()
        .filter(|field| !existing.contains(field.name()))
        .collect();
    for field in missing_meta {
        fields.push((
            std::sync::Arc::from(field.name()),
            record_wfu_intermediate_meta_value(record, field),
        ));
    }
    fields
}

fn record_wfu_intermediate_meta_value(
    record: &OutputRecord,
    field: WfuIntermediateMetaField,
) -> wf_engine::match_engine::Value {
    use wf_engine::match_engine::Value;
    use wf_lang::wfu_meta::WfuIntermediateMetaField;

    match field {
        WfuIntermediateMetaField::RuleName => Value::Str(record.rule_name.clone().into()),
        WfuIntermediateMetaField::Score => Value::Number(record.score),
        WfuIntermediateMetaField::EntityType => Value::Str(record.entity_type.clone().into()),
        WfuIntermediateMetaField::EntityId => Value::Str(record.entity_id.clone().into()),
    }
}

fn event_time_nanos(event: &wf_engine::match_engine::Event, time_field: Option<&str>) -> i64 {
    time_field
        .and_then(|field| event.fields.get(field))
        .and_then(|value| match value {
            wf_engine::match_engine::Value::Number(n) => Some(*n as i64),
            _ => None,
        })
        .unwrap_or(0)
}

fn value_to_json_string(value: &wf_engine::match_engine::Value) -> RuntimeResult<String> {
    serde_json::to_string(&value_to_json(value)?).source_raw_err(
        RuntimeReason::Bootstrap,
        "serialize structured pipeline value",
    )
}

fn value_to_json(value: &wf_engine::match_engine::Value) -> RuntimeResult<serde_json::Value> {
    match value {
        wf_engine::match_engine::Value::Number(n) if n.is_finite() => {
            Ok(serde_json::Value::from(*n))
        }
        wf_engine::match_engine::Value::Number(_) => RuntimeReason::Bootstrap
            .to_err()
            .with_detail("structured numeric value must be finite")
            .err(),
        wf_engine::match_engine::Value::Str(s) => Ok(serde_json::Value::from(s.as_str())),
        wf_engine::match_engine::Value::Bool(b) => Ok(serde_json::Value::from(*b)),
        wf_engine::match_engine::Value::Array(items) => Ok(serde_json::Value::Array(
            items
                .iter()
                .map(value_to_json)
                .collect::<RuntimeResult<Vec<_>>>()?,
        )),
        wf_engine::match_engine::Value::Object(items) => {
            let mut object = serde_json::Map::new();
            let mut keys: Vec<_> = items.keys().collect();
            keys.sort();
            for key in keys {
                if let Some(value) = items.get(key) {
                    object.insert(key.to_string(), value_to_json(value)?);
                }
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}

#[cfg(test)]
mod debug_stats_tests {
    use super::*;
    use wf_engine::alert::AlertOrigin;

    fn output_record(target: &str) -> OutputRecord {
        OutputRecord {
            wfx_id: "id".to_string(),
            rule_name: "rule".into(),
            score: 1.0,
            entity_type: "ip".into(),
            entity_id: "10.0.0.1".to_string(),
            origin: AlertOrigin::Event,
            fired_at: "2026-01-01T00:00:00Z".to_string(),
            emit_time: "2026-01-01T00:00:00Z".into(),
            matched_rows: Vec::new(),
            summary: "".into(),
            yield_target: target.into(),
            yield_fields: Vec::new(),
            yield_field_types: Vec::new().into(),
            event_time_nanos: 0,
            machine_id: Arc::from(""),
            scope_key: "".into(),
        }
    }

    #[test]
    fn detail_budget_caps_at_first_twenty_entries() {
        let mut stats = RuleBatchDebugStats::default();

        for _ in 0..DEBUG_DETAIL_LIMIT {
            assert!(stats.allow_detail());
        }

        assert!(!stats.allow_detail());
        assert!(!stats.allow_detail());
        assert_eq!(stats.detail_logged, DEBUG_DETAIL_LIMIT);
        assert_eq!(stats.detail_suppressed, 2);
    }

    #[test]
    fn exhausted_detail_budget_still_counts_suppressed_entries() {
        let mut stats = RuleBatchDebugStats::default();

        for _ in 0..DEBUG_DETAIL_LIMIT {
            assert!(stats.can_log_detail());
            assert!(stats.allow_detail());
        }

        assert!(!stats.can_log_detail());
        assert!(!stats.allow_detail());
        assert_eq!(stats.detail_logged, DEBUG_DETAIL_LIMIT);
        assert_eq!(stats.detail_suppressed, 1);
    }

    #[test]
    fn output_counts_split_alert_and_intermediate_targets() {
        let mut stats = RuleBatchDebugStats::default();
        let intermediate_targets = HashSet::from(["internal_events".to_string()]);

        stats.count_output(&output_record("alerts"), &intermediate_targets);
        stats.count_output(&output_record("internal_events"), &intermediate_targets);
        stats.count_output(&output_record("alerts"), &intermediate_targets);

        assert_eq!(stats.output_emitted, 2);
        assert_eq!(stats.intermediate_emitted, 1);
    }
}

#[cfg(test)]
mod pipe_stager_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use wf_engine::alert::AlertOrigin;
    use wf_engine::match_engine::Value;

    fn record_with(
        target: &str,
        event_time_nanos: i64,
        yield_fields: Vec<(Arc<str>, Value)>,
    ) -> OutputRecord {
        OutputRecord {
            wfx_id: format!("id-{event_time_nanos}"),
            rule_name: "pipe_s1".into(),
            score: 1.0,
            entity_type: "ip".into(),
            entity_id: "10.0.0.1".to_string(),
            origin: AlertOrigin::Event,
            fired_at: "2026-01-01T00:00:00Z".to_string(),
            emit_time: "2026-01-01T00:00:00Z".into(),
            matched_rows: Vec::new(),
            summary: "".into(),
            yield_target: target.into(),
            yield_fields,
            yield_field_types: Vec::new().into(),
            event_time_nanos,
            machine_id: Arc::from(""),
            scope_key: "".into(),
        }
    }

    /// Covers every arm of the coercion matrix: the pipe event-time field,
    /// the time column (with and without an explicit value), all supported
    /// scalar columns, Utf8 coercions of non-string values, type-mismatch
    /// rows (-> null), and an unsupported column type (Date32 -> null).
    fn stager_schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                PIPE_EVENT_TIME_FIELD,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("n_i", DataType::Int64, true),
            Field::new("n_f", DataType::Float64, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("blob", DataType::Utf8, true),
            Field::new("unsupported", DataType::Date32, true),
        ]))
    }

    fn varied_records() -> Vec<OutputRecord> {
        vec![
            // All fields present, happy path.
            record_with(
                "t",
                1_000,
                vec![
                    (
                        "event_time".into(),
                        Value::Number(1_700_000_000_000_000_000.0),
                    ),
                    ("n_i".into(), Value::Number(7.0)),
                    ("n_f".into(), Value::Number(1.5)),
                    ("flag".into(), Value::Bool(true)),
                    ("label".into(), Value::Str("x".into())),
                    (
                        "blob".into(),
                        Value::Array(vec![Value::Number(1.0), Value::Str("a".into())]),
                    ),
                ],
            ),
            // Missing scalars -> null; time column absent -> event-time
            // fallback; Utf8 coercion of Number.
            record_with(
                "t",
                2_000,
                vec![
                    ("n_f".into(), Value::Number(2.0)),
                    ("label".into(), Value::Number(42.0)),
                ],
            ),
            // Type mismatches -> null; Utf8 coercion of Bool.
            record_with(
                "t",
                3_000,
                vec![
                    ("n_i".into(), Value::Str("zz".into())),
                    ("flag".into(), Value::Number(1.0)),
                    ("label".into(), Value::Bool(true)),
                ],
            ),
        ]
    }

    /// Direct semantic assertions on the staging coercion matrix (the old
    /// per-row `build_pipeline_batch` path is gone; its behaviour lives on
    /// exactly in `push_record`).
    #[test]
    fn staged_batch_coercion_matrix() {
        let schema = stager_schema();
        let records = varied_records();
        let mut stager = PipeBatchStager::new("t".into(), Arc::clone(&schema), Some(1));
        for record in &records {
            stager.push_record(record).expect("stage row");
        }
        let (_, staged, _) = stager.take_events().unwrap().expect("rows staged");
        assert_eq!(staged.len(), records.len());

        // Row 0 — every field present, happy path.
        let f = &staged[0].fields;
        assert_eq!(f.get(PIPE_EVENT_TIME_FIELD), Some(&Value::Number(1_000.0)));
        assert_eq!(
            f.get("event_time"),
            Some(&Value::Number(1_700_000_000_000_000_000.0))
        );
        assert_eq!(f.get("n_i"), Some(&Value::Number(7.0)));
        assert_eq!(f.get("n_f"), Some(&Value::Number(1.5)));
        assert_eq!(f.get("flag"), Some(&Value::Bool(true)));
        assert_eq!(f.get("label"), Some(&Value::Str("x".into())));
        assert_eq!(f.get("blob"), Some(&Value::Str(r#"[1.0,"a"]"#.into())));
        assert_eq!(f.get("unsupported"), None, "Date32 column stages as null");

        // Row 1 — missing scalars -> null (field absent); Utf8 coercion of
        // Number; the time column falls back to the record event time.
        let f = &staged[1].fields;
        assert_eq!(f.get(PIPE_EVENT_TIME_FIELD), Some(&Value::Number(2_000.0)));
        assert_eq!(
            f.get("event_time"),
            Some(&Value::Number(2_000.0)),
            "missing time-col value must fall back to event_time_nanos"
        );
        assert_eq!(f.get("n_i"), None);
        assert_eq!(f.get("n_f"), Some(&Value::Number(2.0)));
        assert_eq!(f.get("flag"), None);
        assert_eq!(f.get("label"), Some(&Value::Str("42".into())));
        assert_eq!(f.get("blob"), None);

        // Row 2 — type mismatches -> null; Utf8 coercion of Bool; a row
        // without any time value gets its own event_time_nanos.
        let f = &staged[2].fields;
        assert_eq!(f.get(PIPE_EVENT_TIME_FIELD), Some(&Value::Number(3_000.0)));
        assert_eq!(f.get("event_time"), Some(&Value::Number(3_000.0)));
        assert_eq!(f.get("n_i"), None, "Str into Int64 stages as null");
        assert_eq!(f.get("flag"), None, "Number into Bool stages as null");
        assert_eq!(f.get("label"), Some(&Value::Str("true".into())));
    }

    /// A non-finite number inside a structured (Array/Object) value must
    /// fail the row instead of serializing `NaN` into JSON.
    #[test]
    fn staged_row_rejects_non_finite_number_inside_structured_value() {
        let schema = stager_schema();
        let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
        let record = record_with(
            "t",
            0,
            vec![(
                "blob".into(),
                Value::Object(
                    [("score".into(), Value::Number(f64::NAN))]
                        .into_iter()
                        .collect(),
                ),
            )],
        );
        let err = stager
            .push_record(&record)
            .expect_err("non-finite structured number should fail");
        assert!(
            err.to_string()
                .contains("structured numeric value must be finite")
        );
    }

    /// An explicit epoch-seconds/millis float yield for a Timestamp column
    /// is normalized to epoch nanos.
    #[test]
    fn staged_timestamp_preserves_time_yield_as_epoch_nanos() {
        let schema = stager_schema();
        let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
        let ts = 1_700_000_000_123_000_000i64;
        let record = record_with(
            "t",
            0,
            vec![("event_time".into(), Value::Number(1_700_000_000_123.0))],
        );
        stager.push_record(&record).expect("stage row");
        let (_, staged, _) = stager.take_events().unwrap().expect("rows staged");
        assert_eq!(
            staged[0].fields.get("event_time"),
            Some(&Value::Number(ts as f64)),
            "float epoch yield must normalize to exact epoch nanos"
        );
    }

    /// Flushing empties the buffers: a second flush is a no-op and later
    /// rows start a fresh batch (per-input-batch flush boundary).
    #[test]
    fn stager_take_resets_buffers_between_flushes() {
        let schema = stager_schema();
        let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));

        assert!(
            stager.take_events().unwrap().is_none(),
            "fresh stager flush is a no-op"
        );

        stager
            .push_record(&record_with(
                "t",
                5,
                vec![("label".into(), Value::Str("a".into()))],
            ))
            .unwrap();
        stager
            .push_record(&record_with(
                "t",
                6,
                vec![("label".into(), Value::Str("b".into()))],
            ))
            .unwrap();
        let first = stager.take_events().unwrap().expect("rows staged");
        assert_eq!(first.1.len(), 2);

        assert!(
            stager.take_events().unwrap().is_none(),
            "buffers must reset after take"
        );

        stager
            .push_record(&record_with(
                "t",
                7,
                vec![("label".into(), Value::Str("c".into()))],
            ))
            .unwrap();
        let second = stager
            .take_events()
            .unwrap()
            .expect("row staged after reset");
        assert_eq!(second.1.len(), 1);
        assert_eq!(
            second.1[0].fields.get("label"),
            Some(&Value::Str("c".into()))
        );
    }

    /// Rows across MANY input batches coalesce only up to the flush point:
    /// a long run keeps column alignment (no drift, no cross-contamination).
    #[test]
    fn stager_column_alignment_holds_over_many_rows() {
        let schema = stager_schema();
        let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
        let rows = 500usize;
        for i in 0..rows {
            stager
                .push_record(&record_with(
                    "t",
                    i as i64,
                    vec![
                        ("n_i".into(), Value::Number(i as f64)),
                        ("label".into(), Value::Str(format!("row-{i}").into())),
                        ("flag".into(), Value::Bool(i % 2 == 0)),
                    ],
                ))
                .unwrap();
        }
        let (_, events, _) = stager.take_events().unwrap().expect("rows staged");
        assert_eq!(events.len(), rows);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.fields.get("n_i"), Some(&Value::Number(i as f64)));
            assert_eq!(
                event.fields.get("label"),
                Some(&Value::Str(format!("row-{i}").into()))
            );
            assert_eq!(event.fields.get("flag"), Some(&Value::Bool(i % 2 == 0)));
        }
    }
}

/// 批级 join-then-key（Path A）scope key 预解析（2026-08-23，q4/q6）：
/// 实现已迁至 `wf_engine::match_engine::precompute_join_then_keys`（与
/// `CepStateMachine::advance_at_with_masks_key` 的 key_override 配套），见
/// crates/wf-engine/src/match_engine/match_engine/join_then_key.rs 模块文档。
fn precompute_join_then_keys(
    batch: &arrow::record_batch::RecordBatch,
    row_domain: &[usize],
    kjp: &wf_lang::plan::JoinKeyPlan,
    windows: &impl wf_engine::match_engine::WindowLookup,
) -> Vec<Option<Vec<wf_engine::match_engine::Value>>> {
    wf_engine::match_engine::precompute_join_then_keys(batch, row_domain, kjp, windows)
}

#[cfg(test)]
mod retention_pin_tests {
    use super::*;

    fn pending(lo_ns: i64) -> DeferredPending {
        DeferredPending {
            key_field: "auction".into(),
            key: wf_engine::match_engine::Value::Number(1.0),
            lo_ns,
            hi_ns: lo_ns + 1_000_000_000,
            lo_open: false,
            hi_open: false,
            expiry_nanos: lo_ns + 1_000_000_000,
            left: wf_engine::match_engine::Event {
                fields: Default::default(),
            },
        }
    }

    fn runtime(pin: &Arc<AtomicI64>) -> DeferredRuntime {
        DeferredRuntime {
            pending: Vec::new(),
            missed: Vec::new(),
            watermark: i64::MIN,
            join_idx: 0,
            retention_pin: Some(Arc::clone(pin)),
            lo_min: i64::MAX,
            lo_min_dirty: false,
        }
    }

    /// 未见过驱动事件时必须发布 `i64::MIN`（全保留），**不能**发布 `i64::MAX`。
    ///
    /// 回归防网：曾把“watermark 未初始化”映射成“无所需”，启动时的定时扫描（1s
    /// 间隔）先于首批驱动事件触发，把刚预注册的 pin 立即释放 → q4 30M 丢 0.67%
    /// 输出（2026-08-24）。
    #[test]
    fn uninitialized_watermark_publishes_fully_pinned() {
        let pin = Arc::new(AtomicI64::new(i64::MIN));
        let mut rt = runtime(&pin);
        rt.publish_retention_floor();
        assert_eq!(
            pin.load(Ordering::Acquire),
            i64::MIN,
            "还没见过驱动事件 → 不知道自己的前沿 → 必须全保留"
        );
    }

    /// 无挂起且 watermark 已推进 → 前沿 = watermark（更旧的行未来实例也用不到）。
    #[test]
    fn empty_pending_publishes_watermark() {
        let pin = Arc::new(AtomicI64::new(i64::MIN));
        let mut rt = runtime(&pin);
        rt.watermark = 5_000;
        rt.publish_retention_floor();
        assert_eq!(pin.load(Ordering::Acquire), 5_000);
    }

    /// 前沿 = `pending ∪ missed` 的 `min(lo_ns)`。
    ///
    /// 回归防网：曾将 `missed`（EOS 重试）排除在外，它们要重试的右窗行被内存
    /// 驱逐抽走 → q4 30M 丢 0.67% 输出（1,661,399 vs 1,672,559，2026-08-24）。
    #[test]
    fn floor_covers_both_pending_and_missed() {
        let pin = Arc::new(AtomicI64::new(i64::MIN));
        let mut rt = runtime(&pin);
        rt.watermark = 9_000;
        rt.pending = vec![pending(7_000), pending(8_000)];
        rt.missed = vec![pending(3_000)];
        rt.publish_retention_floor();
        assert_eq!(
            pin.load(Ordering::Acquire),
            3_000,
            "missed 实例的 lo_ns 必须参与前沿（它们在 EOS 还要重试）"
        );

        // 没有 missed 时回到 pending 的最小 lo_ns。
        // （生产路径 missed 清空经 `reevaluate_deferred_missed` 标 dirty；
        //  测试直接清空需同步标 dirty 模拟该路径。）
        rt.missed.clear();
        rt.lo_min_dirty = true;
        rt.publish_retention_floor();
        assert_eq!(pin.load(Ordering::Acquire), 7_000);
    }

    /// EOS 释放 → `i64::MAX`（窗口恢复完全可驱逐）。
    #[test]
    fn release_unpins_the_window() {
        let pin = Arc::new(AtomicI64::new(1_234));
        let rt = runtime(&pin);
        rt.release_retention_floor();
        assert_eq!(pin.load(Ordering::Acquire), i64::MAX);
    }
}

#[cfg(test)]
#[path = "rule_task_coverage.rs"]
mod rule_task_coverage;
#[cfg(test)]
#[path = "rule_task_coverage_more.rs"]
mod rule_task_coverage_more;
#[cfg(test)]
mod rule_task_key_join_tests;
#[cfg(test)]
#[path = "rule_task_r4.rs"]
mod rule_task_r4;
