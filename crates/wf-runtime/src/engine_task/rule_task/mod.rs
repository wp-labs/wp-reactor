//! 规则执行任务：把 wf-engine 的批量执行（RuleExecutor / CepStateMachine /
//! 列式-解释双路径）接到窗口读侧与 emit 侧，含任务级并发、背压与诊断采样。
//! 引擎任务总览见 `crate` lib.rs 的 `//!` 导航；相关设计：
//! `docs/design/concurrency-scaling.md`、`columnar-execution-design.md`。

mod debug;
mod stager;

// 生产拆件（2026-09-04）：run=构造+pull/push 通路与批次编排；rows=行循环族；
// scan=收口/周期扫描/批诊断；emit=emit 相位家族与输出诊断（mod.rs 主体保持类型/
// 结构定义与可见性接线，见 §拆分注释）。

mod rule_task_emit;
mod rule_task_rows;
mod rule_task_run;
mod rule_task_scan;

use debug::RuleBatchDebugStats;
use stager::{PipeBatchStager, PipeStagerSink, PipeState, resolve_pipe_shape};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{BooleanArray, Float64Array, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use wf_engine::alert::{AlertColumnBatch, AlertColumnBuilder, OutputRecord};
use wf_engine::match_engine::{
    CepStateMachine, CloseReason, ColumnarEvent, DeferredLeft, Event, ExecutionPath,
    ExecutionPathContext, FieldIndex, FieldSource, GuardMasks, JoinRow, RuleExecutor, StepResult,
    TriggerEvent, batch_event_time_nanos_at, batch_time_col_index, batch_to_events,
    batch_to_events_filtered, build_field_index, close_is_qualified,
};
use wf_engine::window::{Router, RulePush};
use wf_lang::plan::{ConvPlan, WindowSpec};
use wf_lang::wfu_meta::WFU_ID;

use crate::alert_task::SinkFanout;
use crate::engine_task::conv_stage::{ConvCloseBatch, ConvShardSink};
use crate::metrics::RuntimeMetrics;

use wf_engine::match_engine::DeferredPending;

use super::TASK_SEQ;
use super::task_types::{RuleTaskConfig, WindowSource};
use super::window_lookup::RegistryLookup;

// 拆件子模块的自由函数 re-bind（2026-09-04）：实现随行循环族（rows）/emit 家族
// （emit）外移后，这些名字经本绑定供各子模块 `use super::*` 与测试模块 glob 继承——
// lib 构建本文件无直接调用点，`#[allow(unused_imports)]`（StatsBucket / compile_diag
// 先例）。
#[allow(unused_imports)]
use rule_task_emit::{
    debug_scope_key, event_debug_ref, event_time_nanos, log_output_emitted, log_output_suppressed,
    output_kind, row_event_debug_ref, value_debug_string,
};
#[allow(unused_imports)]
use rule_task_rows::{advance_machine_row_aliases, alias_accepts, scan_expired_and_route_closes};

// 规则相位 profile 计时开关（scan/advance/emit/close_exec/exec 每行 Instant::now
// + elapsed，仅为 dump_profiling 日志服务）。采样实测（qradar c_* 家族）时钟调用
// 占活跃 CPU ~7.6%——默认保持开启（兼容既有诊断），压测场景经
// [`set_rule_profiling`] 关闭（零时钟开销）。
static RULE_PROFILING: AtomicBool = AtomicBool::new(true);

/// 开关规则相位计时（false = 热路径免 clock_gettime，相位日志归零）。
pub(crate) fn set_rule_profiling(enabled: bool) {
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
/// 中间窗 flush 产物（2026-08-25 q13 分片内存）：`events` 为 `Some` 时下游存在
/// Single/Sharded 订阅（row-path 契约），`None` 时已裁剪为 batch-only
/// （RoundRobin-only/无订阅）。
type PipeFlushBatch = (Arc<str>, Option<Arc<Vec<Arc<Event>>>>, RecordBatch);
/// Batch the allocation-heavy per-alert telemetry (detail map + e2e latency
/// histogram): only 1 in N emitted alerts updates those, the exact total is
/// always counted.
const EMIT_METRIC_SAMPLE_INTERVAL: u32 = 64;
/// Flush size for the batched alert sink delivery (amortizes per-alert fan-out).
const ALERT_BATCH_SIZE: usize = 4096;
/// Full-shutdown drain: how long a rule task waits for its source windows'
/// actors to commit their queued tail before flushing anyway (safety net —
/// normally the drain completes in milliseconds). See
/// [`RuleTask::wait_shutdown_drain`].
pub(super) const SHUTDOWN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// Full-shutdown drain poll interval while waiting for the actors.
pub(super) const SHUTDOWN_DRAIN_POLL: std::time::Duration = std::time::Duration::from_millis(10);

/// Deferred-materialization row source for one batch (L2): the event time of
/// every row (for the watermark/expiry scan) plus the bind-filter hit rows in
/// ascending batch-row order. Hit rows are fed to the state machine as
/// [`ColumnarEvent`] views (P3 FieldView) — no per-row HashMap materialization.
struct DeferredRows<'a> {
    times: Vec<i64>,
    hit_indices: Vec<u32>,
    batch: &'a RecordBatch,
    /// Owned batch snapshot (M3 §11.6): fire capture builds an owned columnar
    /// [`TriggerEvent`] from it — no per-fire `to_event()`. Arc clone per batch
    /// is a cheap refcount (+ column-vec clone); shares arrays with `batch`.
    batch_arc: Arc<RecordBatch>,
    index: Arc<FieldIndex>,
    /// The window's `materialize_fields` read-set projection: the columnar
    /// `to_event()` materializes only these fields on emit, matching the eager
    /// deferred path's projected trigger event.
    projection: Option<Arc<HashSet<String>>>,
}

/// P4 gap-1（q4/q8/q9）：deferred join 驱动列的批级共享视图——Arc batch + 字段
/// 索引 + 投影。行循环按行号懒建 `JoinRow::Columnar`（Arc 克隆共享），挂起队列
/// 持该视图直到到期评估，免 eager_events 逐行 Event 物化。
struct DeferredColumnarBatch {
    batch: Arc<RecordBatch>,
    index: Arc<FieldIndex>,
    projection: Option<Arc<HashSet<String>>>,
}

/// 批处理行域（P 批级临时 Vec 消减，2026-08-26，q5 采样归因）：原先每批构建
/// `Vec<usize>`——分片批 = `shard_rows` 的 u32→usize 转换（q5 10k 行/批 × 300 批
/// × 10 shard ≈ 240MB 分配+转换 churn），未分片 = 恒等 `(0..n)` 纯浪费。改为
/// 借用枚举 + `row_at(i)` 索引（O(1)，语义同原 `row_domain[i]`）；仅 key_join
/// 规则（q4/q6 形状）需要绝对行号切片时 `to_vec()` 物化。
enum RowDomain<'a> {
    /// 分片：本 shard 的行子集（绝对批行号，由 parse 阶段预计算）。
    Sharded(&'a [u32]),
    /// 未分片：全批恒等行域。
    Full(usize),
}

impl<'a> RowDomain<'a> {
    fn len(&self) -> usize {
        match self {
            RowDomain::Sharded(rows) => rows.len(),
            RowDomain::Full(n) => *n,
        }
    }

    /// 域内序号 i 对应的绝对批行号（与旧 `row_domain[i]` 一致）。
    fn row_at(&self, i: usize) -> usize {
        match self {
            RowDomain::Sharded(rows) => rows[i] as usize,
            RowDomain::Full(_) => i,
        }
    }

    /// key_join 预解析需要绝对行号切片（q4/q6 形状；q5 无 key_join 不触达）。
    fn to_vec(&self) -> Vec<usize> {
        match self {
            RowDomain::Sharded(rows) => rows.iter().map(|&r| r as usize).collect(),
            RowDomain::Full(n) => (0..*n).collect(),
        }
    }
}

/// Machine 行循环的批级不变上下文（H-3，2026-09-03）：行域/行源/掩码/键预解析与
/// 日志 emit 常量，整批恒定。纯 Copy（全部共享借用）——方法内解构出与原局部同名
/// 绑定即可复用行体；可变收集器不走此结构（行循环后 `process_batch` 尾部仍消费）。
#[derive(Clone, Copy)]
struct MachineRowsCtx<'a> {
    window_name: &'a str,
    batch_seq: u64,
    lookup_max_seq: Option<u64>,
    batch_emit_nanos: i64,
    debug_enabled: bool,
    row_domain: &'a RowDomain<'a>,
    /// DeferredRows（DeferredMachine 专用）或 eager events（`build_deferred_rows` 之后
    /// 与 `eager_events` 至多一个为 Some）。
    deferred: Option<&'a DeferredRows<'a>>,
    eager_events: Option<&'a Arc<Vec<Arc<Event>>>>,
    columnar_masks: &'a HashMap<String, Option<BooleanArray>>,
    branch_masks: &'a GuardMasks,
    key_overrides: Option<&'a [Option<Vec<wf_engine::match_engine::Value>>]>,
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

impl PendingAlertColumns {
    /// 目标列 builder get-or-create（emit 路径 8 处同构，集中实现）：
    /// targets 少，线性扫；未命中尾插新建。返回借自 `self`（不借 target）。
    fn builder_for(&mut self, target: &Arc<str>) -> &mut AlertColumnBuilder {
        let idx = self
            .by_target
            .iter()
            .position(|(existing, _)| existing == target);
        let idx = match idx {
            Some(idx) => idx,
            None => {
                self.by_target.push((
                    std::sync::Arc::clone(target),
                    AlertColumnBuilder::new(std::sync::Arc::clone(target)),
                ));
                self.by_target.len() - 1
            }
        };
        &mut self.by_target[idx].1
    }
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
    /// 存活**挂起（未评估）**的 min(lo_ns) 缓存（2026-08-25 q4 100M）：
    /// `publish_retention_floor` 不再每次全量扫 O(n)。插入时 O(1) 更新；
    /// scan 移出前缀后若最小项被移出则标 dirty，publish 时重扫——因 min lo
    /// 项几乎总是最早挂起（数据时间单调），dirty 极少，摊销 O(1)。
    /// （missed 不再参与 pin/lo_min——评估 gate 后运行期 miss 即真 miss。）
    lo_min: i64,
    lo_min_dirty: bool,
}

impl DeferredRuntime {
    /// D4：把本规则的保留前沿发布到 join 目标窗口。
    ///
    /// 前沿 = 存活**挂起（未评估）**实例的 `min(lo_ns)`——每个实例需要
    /// `[lo_ns, hi_ns]` 内的右窗行，比最早的 `lo_ns` 更旧的行任何实例都用不到。
    /// 无挂起时退回本规则 watermark：**这依赖驱动流事件时间单调**——未来挂起
    /// 实例由更晚的驱动事件产生，其 `lo_ns` 不会早于 watermark。
    ///
    /// ⚠ watermark 尚未初始化（`i64::MIN`，还没见过驱动事件）时**就发布
    /// `i64::MIN` = 全保留**：此时本规则还不知道自己的前沿，不能放行。曾把它
    /// 映射成 `i64::MAX`（“无所需”），结果启动时的定时扫描（1s 间隔）先于首批
    /// 驱动事件触发，把刚预注册的 pin 立即释放 → q4 30M 仍丢 0.67% 输出
    ///（2026-08-24 实测：驱逐告警里 `retention_floor_ns=i64::MAX`）。
    ///
    /// 2026-08-25（D4 闭环）：**时间驱逐也尊重 pin**（`evict_expired_impl` 与
    /// `evict_oldest_acked` 同款检查）——`over` 只是内存参数，绝不能因调小 over
    /// 删掉评估还要用的行（100M q4 over=1h 精确 / over=30m 欠发 6-9k 的根因）。
    /// 保留量的上界 = max(`over` 窗口, 评估前沿之后)——评估及时时前沿 ≈ watermark，
    /// 窗反而更小；驱动停摆时前沿冻结、窗随 watermark 增长（正确的代价，EOS 时
    /// `release_retention_floor` 显式释放）。
    ///
    /// `missed`（已评估 miss、待 EOS 重试）**不再计入前沿**：评估 gate 保证运行期
    /// 评估时目标窗已追平（target_wm ≥ expiry）→ 运行期 miss 即真 miss（右行确实
    /// 不在区间内），EOS 重试只做确认、不需要保留行。missed 参与 pin 会把前沿拖
    /// 到全流最早的真 miss lo（100M 真 miss ~8.7% 分布全流）→ 时间驱逐全被挡住。
    /// （曾把 missed 计入前沿：那是目标 append 滞后时代的假 miss 保护——gate
    /// 落地后假 miss 消失，该保护随之退役。）
    fn publish_retention_floor(&mut self) {
        let Some(pin) = &self.retention_pin else {
            return;
        };
        // 2026-08-25 q4 100M：缓存 min(lo_ns)（插入 O(1) 更新；dirty 才重扫）
        // ——旧的每 batch 全量扫 pending 在 33M 挂起下是第二个 O(n²)。
        // 缓存仅在维护路径上可靠：直接构造/绕过维护（测试、未来新路径）时
        // `lo_min == i64::MAX` 且集合非空 → 退回全量扫（正确性兜底，罕见）。
        let cache_trustworthy = !(self.lo_min == i64::MAX && !self.pending.is_empty());
        let floor = if self.lo_min_dirty || !cache_trustworthy {
            let lo = self
                .pending
                .iter()
                .map(|p| p.lo_ns)
                .min()
                .unwrap_or(self.watermark);
            // 重扫后同步缓存（后续 publish 免扫）。
            self.lo_min = lo;
            self.lo_min_dirty = false;
            lo
        } else if self.pending.is_empty() {
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

// ---------------------------------------------------------------------------
// RuleTask -- runtime state for a single rule
// ---------------------------------------------------------------------------

/// Holds all mutable state for one rule's processing loop.
///
/// Each `RuleTask` owns its `CepStateMachine` exclusively (no `Arc<Mutex>`).
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.EngineTask")]
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
    /// 墙钟“信用”锚点：每次真实事件批处理后拨到当前时刻；idle 后 scan_timeouts
    /// 用它量度可消费的墙钟（每次最多消费 `timeout_scan_interval`）。
    last_activity_wall: std::time::Instant,
    /// 已消费的墙钟信用（ns）：idle 期间每次 `scan_timeouts` 把 min(流逝, interval)
    /// 累加进来、真实事件批处理时清零（事件时间本身推进会覆盖 idle 推进）→
    /// effective watermark = 机器事件 watermark + 该信用，多次扫描累计到窗口 TTL
    /// （memory_stability：daemon 1s 扫描下 TTL 60s 的 idle 实例必须能释放；旧实现
    /// 无此字段，每次扫描从冻结锚点量「总 idle」再 min(·, interval)，累计钉死）。
    wall_advance_ns: i64,
    /// 周期扫描间隔：单次 `scan_timeouts` 的墙钟推进**上限**（见 `scan_timeouts`），
    /// 慢/背压管道无法一次把 effective watermark 甩到事件时间之前很远、雪崩成
    /// 单次巨型过期扫描饿死 push 消费。
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
    /// 2026-08-29 q1/q20 all 模式分片误拉修复：本规则是否**自己** key 分片消费
    /// （仅 sharded match 规则为 true；见 `RuleTaskConfig::key_partitioned`）。
    /// `pull_and_advance` 用它替代全局 `window_is_sharded` 决定拉取行子集。
    key_partitioned: bool,
    /// 2026-08-29 q6/q20 snapshot join 竞态 gate 的等待目标右窗（去重，构造时
    /// 解析）：live_joins 的即时 join + key_join（join-then-key）的右窗。
    eager_join_targets: Vec<String>,
    /// 2026-08-30 gate 尾部优化：上次 bail（frontier 停滞放行）时的目标窗
    /// frontier。后续批若目标窗 frontier 未再推进 → 直接跳过等待（目标流已
    /// 排空/结束，结论同上次 bail）。frontier 推进即失效。Mutex 供 &self
    /// 访问（与 pending_alerts 同模式，跨 await 不持锁）。
    last_bailed_frontier: std::sync::Mutex<Option<(String, i64)>>,
    /// Profiling accumulators (nanos) for locating the rule-task bottleneck.
    advance_nanos: u64,
    scan_nanos: u64,
    emit_nanos: u64,
    /// Finer emit split: execute_match / record→列 append / fanout handoff.
    /// The append time is also exported as the `alert.append_nanos` metric
    /// (summed across the run).
    exec_nanos: u64,
    /// Finer emit split: execute_close_with_joins (close path output record
    /// construction) — the q12 hot spot; kept separate from `emit_nanos` so the
    /// per-record build vs. the batch append hand-off can be read from the
    /// profiling dump.
    close_exec_nanos: u64,
    append_nanos: std::sync::atomic::AtomicU64,
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
    /// Append-timing sampler state (1-in-`EMIT_METRIC_SAMPLE_INTERVAL`),
    /// see `emit`.
    append_sample_remaining: AtomicU32,
    /// Last value reported to the `rule_instances` gauge. The gauge is the sum
    /// across a rule's shards, so each shard reports the delta since its last
    /// report (P2b).
    last_reported_instances: AtomicI64,
    /// Last value reported to the `rule_memory_bytes` gauge（同 delta 语义：
    /// 内存可增可减 + recalibrate 校正，收口时最后一次上报对冲归零）。
    last_reported_memory: AtomicI64,
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
mod debug_stats_tests;
#[cfg(test)]
mod pipe_stager_tests;
#[cfg(test)]
mod retention_pin_tests;
#[cfg(test)]
mod row_domain_tests;

#[cfg(test)]
#[path = "../rule_task_bench.rs"]
mod rule_task_bench;
#[cfg(test)]
#[path = "../rule_task_coverage.rs"]
mod rule_task_coverage;
#[cfg(test)]
#[path = "../rule_task_coverage_more.rs"]
mod rule_task_coverage_more;
#[cfg(test)]
mod rule_task_key_join_tests;
#[cfg(test)]
#[path = "../rule_task_r4.rs"]
mod rule_task_r4;
