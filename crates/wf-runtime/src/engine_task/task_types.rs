use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;

use wf_engine::match_engine::{CepStateMachine, RuleExecutor, StatsExecutor};
use wf_engine::window::{Router, RulePush, Window};

use crate::alert_task::SinkFanout;
use crate::engine_task::conv_stage::ConvShardSink;
use crate::metrics::RuntimeMetrics;

// ---------------------------------------------------------------------------
// WindowSource -- one window a rule task reads from
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct WindowSource {
    pub window_name: String,
    pub window: Arc<Window>,
    pub notify: Arc<Notify>,
    /// Rule aliases that consume rows from this window.
    pub aliases: Vec<String>,
}

// ---------------------------------------------------------------------------
// RuleTaskConfig -- everything needed to construct a RuleTask
// ---------------------------------------------------------------------------

pub(crate) struct RuleTaskConfig {
    pub machine: Option<CepStateMachine>,
    pub each_alias: Option<String>,
    pub each_time_field: Option<String>,
    pub executor: RuleExecutor,
    pub window_sources: Vec<WindowSource>,
    /// Sink delivery fanout: the rule task broadcasts each emitted alert to the
    /// per-sink channels (resolved by yield_target).
    pub sink_fanout: Arc<SinkFanout>,
    pub cancel: CancellationToken,
    pub timeout_scan_interval: Duration,
    /// Shared router for WindowLookup (joins + has()).
    pub router: Arc<Router>,
    pub metrics: Option<Arc<RuntimeMetrics>>,
    /// Yield targets that should be written back into windows for downstream rules.
    pub intermediate_targets: HashSet<String>,
    /// Output/intermediate relay targets (pipe design): every rule's yield target
    /// as a pipe. Used by the emit path to route through the pipe abstraction.
    pub pipe_registry: std::sync::Arc<wf_engine::pipe::PipeRegistry>,
    /// End-of-stream counter: incremented each time the input sources report
    /// the stream ended. The rule task flushes its instances on every EOS
    /// (counter change) but keeps running so a daemon can accept multiple
    /// finite inputs.
    pub eos_flush: watch::Receiver<u64>,
    /// Push-mode input channel. When `Some`, the rule task consumes pushed
    /// `Arc<Vec<Arc<Event>>>` from it instead of pulling from the window read lock
    /// (R1). When `None`, the task falls back to the legacy notify + pull loop.
    pub push_rx: Option<mpsc::Receiver<RulePush>>,
    /// Pull-model shard identity (M1). `Some(i)` for a sharded rule task;
    /// `None` when unsharded. See `RuleTask::shard_index` for semantics.
    pub shard_index: Option<usize>,
    /// Total shard count this rule is split across (1 when unsharded).
    pub shard_count: usize,
    /// 2026-08-29 q1/q20 all 模式分片误拉修复：本规则是否**自己** key 分片消费
    /// （match 规则的 `match<key>` 行子集）。仅 sharded match 分支为 true；
    /// on-each round-robin 与单 worker 均为 false。`pull_and_advance` 用它决定
    /// 拉取行子集（`shard_rows[i]`）还是整批——不能用全局
    /// `window_is_sharded`（同窗口被其它 match 规则注册分片时，round-robin 规则
    /// 会被误当 key-partitioned 拉取别人的行子集 → 走行式路径 + 偶发丢行）。
    pub key_partitioned: bool,
    /// Consumption-progress slots by window name. The task acks `seq + 1`
    /// after fully processing a batch; on drop the slots are released so a
    /// shutdown task cannot pin window memory. Gates time-based eviction.
    pub progress: std::collections::HashMap<String, Arc<AtomicU64>>,
    /// P2c: when the rule is a shard of a sharded conv rule, this sink carries
    /// raw qualifying closes to the conv stage (aggregation window). `None`
    /// otherwise (inline conv path).
    pub conv_sink: Option<ConvShardSink>,
}

// ---------------------------------------------------------------------------
// StatsTaskConfig -- 声明式窗口统计任务（stats 形态, P1 步骤④c）
// ---------------------------------------------------------------------------

/// Everything needed to construct a [`StatsTask`](crate::engine_task::stats_task::StatsTask).
///
/// 与 `RuleTaskConfig` 同构但简化: 无状态机/on-each/conv——stats 执行器消费
/// fanout 投递的 raw RecordBatch（列式 `process_batch`, 失败回退行式）, 固定
/// 窗口按事件时间 watermark 越过边界时 close 并复用 alert 构建产出。
pub(crate) struct StatsTaskConfig {
    /// 窗口统计执行器（含编译后的 StatsPlan）。
    pub stats: StatsExecutor,
    /// 复用 alert 构建（execute_close_with_joins → OutputRecord）。
    pub executor: RuleExecutor,
    pub window_sources: Vec<WindowSource>,
    pub sink_fanout: Arc<SinkFanout>,
    pub cancel: CancellationToken,
    pub router: Arc<Router>,
    pub metrics: Option<Arc<RuntimeMetrics>>,
    /// 事件时间字段（batch 时间列解析）。
    pub time_field: Option<String>,
    pub timeout_scan_interval: Duration,
    pub intermediate_targets: HashSet<String>,
    // 预留：stats → 中间流（pipe）relay（emit_close_record 现记录丢弃）与分片
    // stats（当前仅 shard_index 用于 pull）——与 RuleTaskConfig 形状对齐。
    #[allow(dead_code)]
    pub pipe_registry: std::sync::Arc<wf_engine::pipe::PipeRegistry>,
    pub eos_flush: watch::Receiver<u64>,
    pub push_rx: Option<mpsc::Receiver<RulePush>>,
    pub progress: std::collections::HashMap<String, Arc<AtomicU64>>,
    pub shard_index: Option<usize>,
    #[allow(dead_code)]
    pub shard_count: usize,
    /// 输入分区分片归并（空键 stats, 2026-08-24 q15）: 协调片（shard 0）持
    /// `merge_rx` 收齐各片 partial 后合并再 emit; 非协调片持 `merge_tx` 在
    /// 窗口 close 时发送自身 partial 且不 emit; 未分片两者皆 `None`。
    pub merge_rx: Option<mpsc::Receiver<super::stats_task::StatsPartial>>,
    pub merge_tx: Option<mpsc::Sender<super::stats_task::StatsPartial>>,
    /// 分片共享的批级 where mask 缓存（2026-08-27 q17）: 同一规则所有片共享
    /// 一个 `Arc<StatsMaskCache>`——首片算 mask, 其余片 Arc 命中（免 S× 重复
    /// 向量化求值）。None = 不缓存（未分片/单实例规则）。
    pub mask_cache: Option<std::sync::Arc<wf_engine::match_engine::StatsMaskCache>>,
}
