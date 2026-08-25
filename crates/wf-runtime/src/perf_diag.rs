//! 性能诊断模式（perf-diag）：门控切口 + 内置哨兵（漂流瓶）+ 诊断档状态机。
//!
//! 机制设计见 `docs/design/perf-diag-mode-design.md`。核心概念：
//!
//! - **进入诊断 = 启动参数** `wfusion daemon --perf-diag conf/perf-diag.toml`，
//!   生产不带参数即全关（`wfusion.toml` 零污染）。
//! - **诊断档 = 只有禁止开关**：`cut_rules`（规则求值）/ `cut_output`（输出链）。
//! - **切换 = sentinel 驱动自切换**：wfgen 每批帧尾追加 `__wf_sentinel` 帧
//!   （载荷自描述 `{round, n, start_ns}`）；哨兵处理时引擎补 `emit_ns` 并把
//!   `{round, n, start_ns, emit_ns}` 四元组经 alert 链落盘（豁免门控）→
//!   EPS = n / (emit_ns − start_ns) 直接可算。
//! - **完成信号**：点 k 生效（门控翻转 + 规则 reload 完成）后写
//!   `{"type":"stage","current":k}` 记录到同一 ndjson；wfgen 读到
//!   `stage{current=k}` 才发 round k（无竞态）。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use wf_config::{FusionConfig, PerfConfig, PerfStage, RawFusionConfigTree};
use wf_engine::alert::{AlertColumnBuilder, AlertOrigin, OutputRecord};
use wf_engine::match_engine::Value;
use wf_engine::window::{Router, RulePush};
use wf_lang::{BaseType, FieldType};

use crate::alert_task::{AlertBatch, SinkFanout};
use crate::error::RuntimeResult;
use crate::lifecycle::RuntimeControlHandle;

// ---------------------------------------------------------------------------
// 常量与全局门控
// ---------------------------------------------------------------------------

/// 内置哨兵流/窗口名（诊断模式下自动注册，不依赖用户 .wfs）。
pub const PERF_SENTINEL_STREAM: &str = "__wf_sentinel";
/// 哨兵记录 sink 的 yield target（= 窗口名，case 的 sink group 按此匹配）。
pub const PERF_SENTINEL_WINDOW: &str = "__wf_sentinel";

/// 诊断模式总开关（`--perf-diag` 传入）。
static PERF_DIAG_ENABLED: AtomicBool = AtomicBool::new(false);
/// 门控：禁止规则求值（`RuleTask::process_batch` 直通，ack 保留）。
static PERF_CUT_RULES: AtomicBool = AtomicBool::new(false);
/// 门控：禁止输出链（emit 不 serialize/stage/commit/fanout）。
static PERF_CUT_OUTPUT: AtomicBool = AtomicBool::new(false);
/// 门控：禁止窗口 append（解码后即丢——测「注入 + 解码」前序段; 哨兵流豁免）。
static PERF_CUT_APPEND: AtomicBool = AtomicBool::new(false);
/// 诊断档列表（启动时 set，只读；测试可重复初始化）。
static PERF_STAGES: std::sync::RwLock<Vec<PerfStage>> = std::sync::RwLock::new(Vec::new());

/// 初始化诊断模式全局状态——**仅当 `--perf-diag <path>` 启动参数存在时调用**
/// （wfusion CLI 已解析并 load 配置文件）。入口即参数本身：
///
/// - 注册哨兵窗口 + 应用**初始门控** = `stages[0]` 的门控（无档 → 全 false）；
/// - 顶层 `diag`/`cut_rules`/`cut_output` 是历史遗留字段，已从配置移除。
pub fn init_perf_diag(config: &PerfConfig) {
    *PERF_STAGES.write().expect("perf stages lock poisoned") = config.stages.clone();
    PERF_DIAG_ENABLED.store(true, Ordering::Relaxed);
    let (cut_rules, cut_output, cut_append) = match config.stages.first() {
        Some(stage) => (stage.cut_rules, stage.cut_output, stage.cut_append),
        None => (false, false, false),
    };
    set_perf_cuts(cut_rules, cut_output, cut_append);
}

/// 复位诊断模式全局状态——无 `--perf-diag` 时调用（生产启动零污染）。
pub fn reset_perf_diag() {
    PERF_DIAG_ENABLED.store(false, Ordering::Relaxed);
    *PERF_STAGES.write().expect("perf stages lock poisoned") = Vec::new();
    set_perf_cuts(false, false, false);
}

/// 原子门控翻转（诊断档状态机专用，不进 reload diff）。
pub fn set_perf_cuts(cut_rules: bool, cut_output: bool, cut_append: bool) {
    PERF_CUT_RULES.store(cut_rules, Ordering::Relaxed);
    PERF_CUT_OUTPUT.store(cut_output, Ordering::Relaxed);
    PERF_CUT_APPEND.store(cut_append, Ordering::Relaxed);
}

/// 诊断模式是否开启。
pub fn perf_diag_enabled() -> bool {
    PERF_DIAG_ENABLED.load(Ordering::Relaxed)
}

/// 是否禁止规则求值（cut_rules 门控）。
#[inline]
pub fn perf_cut_rules() -> bool {
    PERF_CUT_RULES.load(Ordering::Relaxed)
}

/// 是否禁止输出链（cut_output 门控）。
#[inline]
pub fn perf_cut_output() -> bool {
    PERF_CUT_OUTPUT.load(Ordering::Relaxed)
}

/// 是否禁止窗口 append（cut_append 门控）。
#[inline]
pub fn perf_cut_append() -> bool {
    PERF_CUT_APPEND.load(Ordering::Relaxed)
}

/// 当前诊断档列表（空 = 非诊断模式或单点）。
fn perf_stages() -> Arc<Vec<PerfStage>> {
    Arc::new(
        PERF_STAGES
            .read()
            .expect("perf stages lock poisoned")
            .clone(),
    )
}

// ---------------------------------------------------------------------------
// 哨兵载荷解析与记录构建
// ---------------------------------------------------------------------------

/// 一条哨兵测量记录（四元组齐备，EPS 直接可算）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentinelRecord {
    /// wfgen 轮次（= 诊断档下标）。
    pub round: i64,
    /// 本批发送量（wfgen 记账，载荷自描述）。
    pub n: i64,
    /// wfgen 发送开始时钟（同机可比的 epoch nanos）。
    pub start_ns: i64,
    /// 引擎哨兵 emit 时刻（复用引擎墙钟）。
    pub emit_ns: i64,
}

impl SentinelRecord {
    /// EPS = n / (emit_ns − start_ns)。时间差非正（时钟异常/同批）返回 `None`。
    pub fn eps(&self) -> Option<f64> {
        let dt = self.emit_ns.saturating_sub(self.start_ns);
        if dt <= 0 {
            return None;
        }
        Some(self.n as f64 * 1e9 / dt as f64)
    }
}

/// 从哨兵窗口 batch 解析哨兵记录。列缺失/类型不符的字段按 0 处理（防御）；
/// `emit_ns` 由引擎侧补入。
pub fn parse_sentinel_batch(
    batch: &arrow::record_batch::RecordBatch,
    emit_ns: i64,
) -> Vec<SentinelRecord> {
    use arrow::array::{Array, Int64Array};
    let round_col = batch
        .column_by_name("round")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let n_col = batch
        .column_by_name("n")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let start_col = batch
        .column_by_name("start_ns")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let rows = batch.num_rows();
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let round = round_col
            .and_then(|c| (!c.is_null(row)).then(|| c.value(row)))
            .unwrap_or(0);
        let n = n_col
            .and_then(|c| (!c.is_null(row)).then(|| c.value(row)))
            .unwrap_or(0);
        let start_ns = start_col
            .and_then(|c| (!c.is_null(row)).then(|| c.value(row)))
            .unwrap_or(0);
        out.push(SentinelRecord {
            round,
            n,
            start_ns,
            emit_ns,
        });
    }
    out
}

/// 构建哨兵记录 OutputRecord（yield target = `__wf_sentinel` → 文件 sink）。
///
/// `start_ns`/`emit_ns` 以字符串（Chars）携带：epoch nanos ≈ 1.7e18 超出 f64
/// 精确范围（ulp ≈ 256ns），走 Digit/Number 会丢精度；字符串由 wfgen 解析回
/// i64，四元组全程精确。
pub fn sentinel_record_output(rec: &SentinelRecord) -> OutputRecord {
    OutputRecord {
        wfx_id: format!("perf-sentinel-{}", rec.round),
        rule_name: Arc::from(PERF_SENTINEL_STREAM),
        score: 0.0,
        entity_type: Arc::from("perf"),
        entity_id: format!("round-{}", rec.round),
        origin: AlertOrigin::Event,
        fired_at: String::new(),
        emit_time: String::new().into(),
        matched_rows: Vec::new(),
        summary: String::new().into(),
        yield_target: Arc::from(PERF_SENTINEL_WINDOW),
        yield_fields: vec![
            ("record_type".into(), Value::Str("sentinel".into())),
            ("round".into(), Value::Number(rec.round as f64)),
            ("n".into(), Value::Number(rec.n as f64)),
            (
                "start_ns".into(),
                Value::Str(rec.start_ns.to_string().into()),
            ),
            ("emit_ns".into(), Value::Str(rec.emit_ns.to_string().into())),
        ],
        yield_field_types: Arc::from(vec![
            ("round".into(), FieldType::Base(BaseType::Digit)),
            ("n".into(), FieldType::Base(BaseType::Digit)),
            ("start_ns".into(), FieldType::Base(BaseType::Chars)),
            ("emit_ns".into(), FieldType::Base(BaseType::Chars)),
        ]),
        event_time_nanos: 0,
        machine_id: Arc::from("perf"),
        scope_key: "perf".into(),
    }
}

/// 构建切换完成信号 OutputRecord：`{"type":"stage","current":k}`。
pub fn stage_record_output(current: usize) -> OutputRecord {
    OutputRecord {
        wfx_id: format!("perf-stage-{current}"),
        rule_name: Arc::from(PERF_SENTINEL_STREAM),
        score: 0.0,
        entity_type: Arc::from("perf"),
        entity_id: format!("stage-{current}"),
        origin: AlertOrigin::Event,
        fired_at: String::new(),
        emit_time: String::new().into(),
        matched_rows: Vec::new(),
        summary: String::new().into(),
        yield_target: Arc::from(PERF_SENTINEL_WINDOW),
        yield_fields: vec![
            ("record_type".into(), Value::Str("stage".into())),
            ("current".into(), Value::Number(current as f64)),
        ],
        yield_field_types: Arc::from(vec![("current".into(), FieldType::Base(BaseType::Digit))]),
        event_time_nanos: 0,
        machine_id: Arc::from("perf"),
        scope_key: "perf".into(),
    }
}

/// 引擎墙钟（epoch nanos）——哨兵 emit 时刻（与 wfgen 同机可比）。
pub fn wall_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

// ---------------------------------------------------------------------------
// 诊断档状态机控制器
// ---------------------------------------------------------------------------

/// 规则子集热 reload 的基线（`runtime.rules` 变化时用现有 reload 通道）。
#[derive(Debug, Clone)]
struct ReloadBaseline {
    raw: RawFusionConfigTree,
    config: FusionConfig,
}

/// 一次切换的结果（供哨兵任务写 `stage{current}` 完成信号）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedStage {
    /// 已生效诊断档下标（= 完成信号的 `current` 值）。
    pub index: usize,
    /// 是否触发了规则子集 reload。
    pub reloaded: bool,
}

/// 诊断档状态机：sentinel(round=k) emit → 应用点 k+1（门控翻转 + 可选规则
/// 子集 reload）→ 返回后由哨兵任务写 `stage{current=k+1}` 完成信号。
///
/// 同步语义：`on_sentinel` 返回时点 k+1 已生效（含 reload 完成）——wfgen 读到
/// `sentinel{round=k}` 记录时点 k+1 已切换，无竞态。
pub struct PerfDiagController {
    stages: Arc<Vec<PerfStage>>,
    current: AtomicUsize,
    /// 规则子集 reload 通道（Reactor 启动后注入；未注入 = 不触发 reload）。
    control: std::sync::RwLock<Option<RuntimeControlHandle>>,
    /// reload 基线（启动后注入；未注入 = 不触发 reload）。
    baseline: std::sync::Mutex<Option<ReloadBaseline>>,
}

impl PerfDiagController {
    /// 从全局诊断配置构建控制器。非诊断模式/无诊断档 → 永不切换。
    pub fn new() -> Arc<Self> {
        let stages = perf_stages();
        Arc::new(Self {
            stages,
            current: AtomicUsize::new(0),
            control: std::sync::RwLock::new(None),
            baseline: std::sync::Mutex::new(None),
        })
    }

    /// 注入 reload 通道 + 基线（Reactor::start 完成后调用；`control_handle` 在
    /// `run` 驱动控制循环前即可安全调用）。
    pub fn set_reload_handle(
        &self,
        handle: RuntimeControlHandle,
        raw: RawFusionConfigTree,
        config: FusionConfig,
    ) {
        *self.control.write().expect("control lock poisoned") = Some(handle);
        *self.baseline.lock().expect("baseline lock poisoned") =
            Some(ReloadBaseline { raw, config });
    }

    /// 当前已生效诊断档下标（0 = 初始档已生效；`usize::MAX` = 无诊断档）。
    pub fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// 是否有后续诊断档可切换。
    pub fn has_next(&self) -> bool {
        self.current() + 1 < self.stages.len()
    }

    /// sentinel(round=k) 完成 → 应用档 k+1（幂等：重复轮次/越界不动作）。
    ///
    /// 返回 `Some(AppliedStage)` 表示本次真的切换到了新档；否则 `None`。
    pub async fn on_sentinel(&self, round: i64) -> Option<AppliedStage> {
        if round < 0 {
            return None;
        }
        let target = round as usize + 1;
        let cur = self.current();
        // 幂等：重复轮次（--rounds N）不重复应用同一目标档。
        if target <= cur {
            return None;
        }
        let stage = self.stages.get(target)?.clone();
        // 1. 原子门控翻转（先于 reload——新数据即吃新门控）。
        set_perf_cuts(stage.cut_rules, stage.cut_output, stage.cut_append);
        // 2. 规则子集变化（非空且不同于基线）→ 触发现有 runtime.rules 热 reload。
        let mut reloaded = false;
        let rules = stage.rules.as_deref().unwrap_or("");
        // 先把控制句柄克隆出来（不跨 await 持锁——std 锁非 async）。
        let reload_handle = self.control.read().expect("control lock poisoned").clone();
        if !rules.is_empty() && reload_handle.is_some() {
            let changed = {
                let baseline = self.baseline.lock().expect("baseline lock poisoned");
                baseline
                    .as_ref()
                    .map(|b| b.config.runtime.rules != rules)
                    .unwrap_or(false)
            };
            if changed && let Some(handle) = reload_handle {
                // 构造下一份配置：仅 runtime.rules 指向规则子集文件。
                // changed 成立的前提是基线存在（changed 由基线推导），故这里
                // baseline 必为 Some——用 expect 表达不变量。
                let (next_raw, next_config) = {
                    let baseline = self.baseline.lock().expect("baseline lock poisoned");
                    let b = baseline.as_ref().expect("changed implies baseline present");
                    let mut raw = b.raw.clone();
                    raw.set_runtime_rules(rules);
                    let mut config = b.config.clone();
                    config.runtime.rules = rules.to_string();
                    (raw, config)
                };
                match handle.apply_reload(next_raw, next_config).await {
                    Ok(_outcome) => {
                        // Applied（规则已换）/ Blocked（拓扑变化拒绝）都算切换完成：
                        // 门控已翻，完成信号照写。基线推进到新 rules，防重复 reload。
                        let mut baseline = self.baseline.lock().expect("baseline lock poisoned");
                        if let Some(b) = baseline.as_mut() {
                            b.config.runtime.rules = rules.to_string();
                            b.raw.set_runtime_rules(rules);
                        }
                        reloaded = true;
                    }
                    Err(_) => {
                        // reload 失败（引擎关停等）→ 门控已翻，仍推进（日志由
                        // apply_reload 侧给出）；不重试。
                        reloaded = false;
                    }
                }
            }
        }
        self.current.store(target, Ordering::Relaxed);
        Some(AppliedStage {
            index: target,
            reloaded,
        })
    }
}

// ---------------------------------------------------------------------------
// 哨兵任务（内置哨兵规则）
// ---------------------------------------------------------------------------

/// 哨兵任务配置。
pub(crate) struct SentinelTaskConfig {
    pub router: Arc<Router>,
    pub sink_fanout: Arc<SinkFanout>,
    pub controller: Arc<PerfDiagController>,
    pub cancel: CancellationToken,
    /// 推送订阅接收端（由 Reactor::start 在 spawn 前注册好——订阅必然先于
    /// receiver 接受连接，杜绝首个哨兵帧漏投）。
    pub rx: mpsc::Receiver<RulePush>,
}

/// 哨兵任务：消费 `__wf_sentinel` 窗口的推送——写四元组记录（alert 链落盘，
/// 豁免所有 perf 门控）+ 驱动诊断档状态机。
///
/// **批末语义**：哨兵帧与数据帧同 TCP 连接、同源 seq 有序（哨兵是"批末最后
/// 一条"）。规则消费是异步的（pull），哨兵在数据窗**排空**（全部规则 ack 追平
/// next_seq）后才 emit——`emit_ns` ≈ 该批真实处理结束时刻。
pub(crate) async fn run_sentinel_task(config: SentinelTaskConfig) -> RuntimeResult<()> {
    let SentinelTaskConfig {
        router,
        sink_fanout,
        controller,
        cancel,
        mut rx,
    } = config;

    // 启动即写初始完成信号 `stage{current=k}`（k = 已生效诊断档）——wfgen 在
    // 发送第 k 轮数据前轮询该记录，保证无竞态。
    emit_sentinel_records(
        vec![stage_record_output(controller.current())],
        &sink_fanout,
    )
    .await;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            push = rx.recv() => {
                let Some(push) = push else { break };
                // 数据窗排空再 emit：sentinel 记录 = 批末真实处理完成时刻。
                wait_for_data_drain(&router, &cancel).await;
                process_sentinel_push(push, &sink_fanout, &controller).await;
            }
        }
    }
    Ok(())
}

/// 等待所有数据窗排空：**全部批次已被消费**。完成判定是**分组的**
/// （`WindowProgress::completion_gap`，2026-08-25 review）：
/// - whole-batch（round-robin / 单消费者）窗口：每批恰被其归属 shard 消费，
///   完成 = 组内 **max** 追平（min 恒停在最慢 shard，q13 分片卡尾的教训）；
/// - row-partitioned（key / 行号分片 match/stats）窗口：每批要**所有**分片
///   处理完才算消费，完成 = 组内 **min** 追平（max = 最快分片，追平不代表
///   慢分片处理完——旧 `max||min` 会让哨兵在最快分片处提前排空）。
///
/// 驱逐保护仍用 `min_acked`（未读不驱逐）。无消费者的窗口视为已排空；
/// 哨兵窗自身除外。
async fn wait_for_data_drain(router: &Arc<Router>, cancel: &CancellationToken) {
    loop {
        let drained = router.registry().window_names().iter().all(|name| {
            if name == PERF_SENTINEL_WINDOW {
                return true;
            }
            let Some(win) = router.registry().get_window(name) else {
                return true; // provider 窗（无 buffer log）→ 无消费语义
            };
            let next = win.next_seq();
            match router.registry().progress(name) {
                Some(progress) => progress.completion_gap(next) == 0,
                None => true, // 无消费者 → 已排空
            }
        });
        if drained {
            return;
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
        }
    }
}

/// 处理一条哨兵推送：解析记录 → 逐条应用状态机（先）→ 写完成信号 + 四元组
/// （同批 builder、同 sink 通道，文件内顺序 = 切换信号先于哨兵记录）。
async fn process_sentinel_push(
    push: RulePush,
    sink_fanout: &Arc<SinkFanout>,
    controller: &Arc<PerfDiagController>,
) {
    let Some(batch) = push.batch else {
        return;
    };
    let emit_ns = wall_nanos();
    let records = parse_sentinel_batch(&batch, emit_ns);
    if records.is_empty() {
        return;
    }
    // 哨兵窗口自身不豁免事件时间处理：按 round 去重后的第一条触发切换，其余
    // （重复轮次）由控制器幂等短路。
    for rec in &records {
        let applied = controller.on_sentinel(rec.round).await;
        let mut out: Vec<OutputRecord> = Vec::with_capacity(2);
        if let Some(applied) = applied {
            out.push(stage_record_output(applied.index));
        }
        out.push(sentinel_record_output(rec));
        emit_sentinel_records(out, sink_fanout).await;
    }
}

/// 把哨兵/完成信号记录经 alert 链投递到 `__wf_sentinel` 的 sink（文件落盘）。
/// 常数量处理开销（每批 1-2 条），豁免 cut_rules/cut_output 门控。
async fn emit_sentinel_records(records: Vec<OutputRecord>, sink_fanout: &Arc<SinkFanout>) {
    if records.is_empty() {
        return;
    }
    let mut builder = AlertColumnBuilder::new(Arc::from(PERF_SENTINEL_WINDOW));
    for record in &records {
        if builder.append_record(record).is_err() {
            log::warn!("perf sentinel record append failed");
        }
    }
    if builder.is_empty() {
        return;
    }
    let batch = AlertBatch::Columns(Arc::new(builder.finish()));
    let sink_groups = sink_fanout.resolve(PERF_SENTINEL_WINDOW);
    if sink_groups.is_empty() {
        sink_fanout.warn_if_no_sink(PERF_SENTINEL_WINDOW);
        return;
    }
    for (sink_ptr, channels) in sink_groups.iter() {
        let idx = sink_fanout.next_index(*sink_ptr, channels.len());
        let tx = &channels[idx];
        if tx.try_send(batch.clone()).is_err() {
            // 背压：哨兵信号不能丢（完成信号缺失会让 wfgen 挂死等待）。
            if tx.send(batch.clone()).await.is_err() {
                log::warn!("perf sentinel channel closed, dropping sentinel batch");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    // `serial()` 的序列化锁是**故意**跨 await 持有的：门控/诊断档是进程级全局状态，
    // 测试必须在 await 期间也独占（否则其它测试会插入改写全局门控）。std Mutex 在
    // 测试场景下短期持有无实际风险，clippy 的 await_holding_lock 属误报，模块级豁免。
    #![allow(clippy::await_holding_lock)]
    use super::*;
    use crate::lifecycle::ReloadOutcome;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::time::Duration;

    /// 门控/诊断档是进程级全局状态：串行化涉全局的测试，避免并行污染。
    static TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn serial() -> std::sync::MutexGuard<'static, ()> {
        // 测试内 panic（如异步断言失败）会污染互斥锁：恢复后继续串行。
        TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner())
    }

    // -- 门控与初始化 -----------------------------------------------------

    #[test]
    fn init_disabled_resets_everything() {
        let _g = serial();
        reset_perf_diag();
        assert!(!perf_diag_enabled());
        assert!(!perf_cut_rules());
        assert!(!perf_cut_output());
        set_perf_cuts(true, true, true);
        reset_perf_diag();
        assert!(!perf_cut_rules(), "reset 必须复位门控");
        assert!(!perf_cut_output());
        assert!(!perf_cut_append());
    }

    #[test]
    fn init_diag_with_stages_applies_first_stage_gates() {
        let _g = serial();
        let cfg = PerfConfig {
            stages: vec![
                PerfStage {
                    name: "floor".into(),
                    cut_rules: true,
                    cut_output: true,
                    cut_append: false,
                    rules: None,
                },
                PerfStage {
                    name: "full".into(),
                    cut_rules: false,
                    cut_output: false,
                    cut_append: false,
                    rules: None,
                },
            ],
        };
        init_perf_diag(&cfg);
        assert!(perf_diag_enabled());
        assert!(perf_cut_rules(), "stages[0] gates apply at startup");
        assert!(perf_cut_output());
        // 复位，避免污染其它测试。
        reset_perf_diag();
    }

    #[test]
    fn init_without_stages_defaults_gates_false() {
        let _g = serial();
        let cfg = PerfConfig::default();
        init_perf_diag(&cfg);
        assert!(perf_diag_enabled(), "--perf-diag 即入口");
        assert!(!perf_cut_rules(), "无点 → 初始门控全 false");
        assert!(!perf_cut_output());
        reset_perf_diag();
    }

    #[test]
    fn set_perf_cuts_flips_both_gates() {
        let _g = serial();
        reset_perf_diag();
        set_perf_cuts(true, false, true);
        assert!(perf_cut_rules());
        assert!(!perf_cut_output());
        assert!(perf_cut_append());
        set_perf_cuts(false, true, false);
        assert!(!perf_cut_rules());
        assert!(perf_cut_output());
        assert!(!perf_cut_append());
        // 复位，避免污染其它测试：全局 static 门控，并行测试的 emit 会
        // 被 `perf_cut_output()` 早退丢输出（2026-08-25 实测：deferred_q8
        // EOS 重试 emit 被切 → 断言 left=[]）。
        reset_perf_diag();
    }

    // -- 哨兵载荷解析 -------------------------------------------------------

    fn sentinel_batch(rounds: &[i64], ns: &[i64], starts: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("round", DataType::Int64, false),
            Field::new("n", DataType::Int64, false),
            Field::new("start_ns", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(rounds.to_vec())),
                Arc::new(Int64Array::from(ns.to_vec())),
                Arc::new(Int64Array::from(starts.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn parse_sentinel_batch_reads_columns_and_injects_emit_ns() {
        let batch = sentinel_batch(&[0, 1], &[100, 200], &[1_000, 2_000]);
        let records = parse_sentinel_batch(&batch, 9_999);
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0],
            SentinelRecord {
                round: 0,
                n: 100,
                start_ns: 1_000,
                emit_ns: 9_999,
            }
        );
        assert_eq!(records[1].round, 1);
        assert_eq!(records[1].n, 200);
        assert_eq!(records[1].start_ns, 2_000);
        assert_eq!(records[1].emit_ns, 9_999);
    }

    #[test]
    fn parse_sentinel_batch_empty_is_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("round", DataType::Int64, false),
            Field::new("n", DataType::Int64, false),
            Field::new("start_ns", DataType::Int64, false),
        ]));
        let cols: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())) as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(Vec::<i64>::new())) as Arc<dyn arrow::array::Array>,
            Arc::new(Int64Array::from(Vec::<i64>::new())) as Arc<dyn arrow::array::Array>,
        ];
        let batch = RecordBatch::try_new(schema, cols).unwrap();
        assert!(parse_sentinel_batch(&batch, 0).is_empty());
    }

    #[test]
    fn parse_sentinel_batch_missing_columns_default_to_zero() {
        // 只含 round 列的 batch：n/start_ns 按 0 处理。
        let schema = Arc::new(Schema::new(vec![Field::new(
            "round",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7i64]))]).unwrap();
        let records = parse_sentinel_batch(&batch, 42);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].round, 7);
        assert_eq!(records[0].n, 0);
        assert_eq!(records[0].start_ns, 0);
        assert_eq!(records[0].emit_ns, 42);
    }

    // -- EPS ----------------------------------------------------------------

    #[test]
    fn eps_computes_n_over_elapsed_seconds() {
        let rec = SentinelRecord {
            round: 0,
            n: 1_000_000,
            start_ns: 0,
            emit_ns: 100_000_000, // 0.1s
        };
        let eps = rec.eps().unwrap();
        assert!((eps - 10_000_000.0).abs() < 1.0, "eps = {eps}");
    }

    #[test]
    fn eps_none_when_emit_not_after_start() {
        let rec = SentinelRecord {
            round: 0,
            n: 100,
            start_ns: 200,
            emit_ns: 100,
        };
        assert!(rec.eps().is_none(), "emit_ns <= start_ns → None");
        let rec = SentinelRecord {
            round: 0,
            n: 100,
            start_ns: 200,
            emit_ns: 200,
        };
        assert!(rec.eps().is_none(), "zero elapsed → None");
    }

    // -- 记录构建 -----------------------------------------------------------

    #[test]
    fn sentinel_output_carries_four_tuple_fields() {
        let rec = SentinelRecord {
            round: 2,
            n: 500,
            start_ns: 1_111,
            emit_ns: 2_222,
        };
        let out = sentinel_record_output(&rec);
        assert_eq!(&*out.yield_target, PERF_SENTINEL_WINDOW);
        let fields: std::collections::HashMap<&str, Value> = out
            .yield_fields
            .iter()
            .map(|(k, v)| (&**k, v.clone()))
            .collect();
        assert_eq!(fields.get("round"), Some(&Value::Number(2.0)));
        assert_eq!(fields.get("n"), Some(&Value::Number(500.0)));
        // start_ns/emit_ns 以字符串携带（epoch nanos 超出 f64 精确范围）。
        assert_eq!(fields.get("start_ns"), Some(&Value::Str("1111".into())));
        assert_eq!(fields.get("emit_ns"), Some(&Value::Str("2222".into())));
        assert_eq!(
            fields.get("record_type"),
            Some(&Value::Str("sentinel".into()))
        );
        // 类型标注：round/n → Digit，start_ns/emit_ns → Chars（JSON 精确整数/字符串）。
        let types: std::collections::HashMap<&str, &FieldType> = out
            .yield_field_types
            .iter()
            .map(|(k, v)| (&**k, v))
            .collect();
        assert_eq!(types.get("round"), Some(&&FieldType::Base(BaseType::Digit)));
        assert_eq!(
            types.get("start_ns"),
            Some(&&FieldType::Base(BaseType::Chars))
        );
        assert_eq!(
            types.get("emit_ns"),
            Some(&&FieldType::Base(BaseType::Chars))
        );
    }

    #[test]
    fn stage_output_carries_current_index() {
        let out = stage_record_output(3);
        let fields: std::collections::HashMap<&str, Value> = out
            .yield_fields
            .iter()
            .map(|(k, v)| (&**k, v.clone()))
            .collect();
        assert_eq!(fields.get("current"), Some(&Value::Number(3.0)));
        assert_eq!(fields.get("record_type"), Some(&Value::Str("stage".into())));
    }

    // -- 诊断档状态机 -------------------------------------------------------

    fn test_config(stages: Vec<PerfStage>) -> PerfConfig {
        PerfConfig { stages }
    }

    fn floor_stage() -> PerfStage {
        PerfStage {
            name: "floor".into(),
            cut_rules: true,
            cut_output: true,
            cut_append: false,
            rules: None,
        }
    }

    fn rules_stage() -> PerfStage {
        PerfStage {
            name: "rules".into(),
            cut_rules: false,
            cut_output: true,
            cut_append: false,
            rules: None,
        }
    }

    fn full_stage() -> PerfStage {
        PerfStage {
            name: "full".into(),
            cut_rules: false,
            cut_output: false,
            cut_append: false,
            rules: None,
        }
    }

    /// 无门控 stage（cut 全 false）——只测控制器/哨兵状态机、不测门控翻转的
    /// 测试用它，避免并行测试期间全局 `PERF_CUT_*` 被拉高（会切断其它测试的
    /// 规则求值/输出链，2026-08-25 实测：deferred_q8 EOS 重试 emit 被切 →
    /// 断言 left=[]；lifecycle metrics 规则求值被切 → emitted_total=0）。
    fn no_cut_stage(name: &str) -> PerfStage {
        PerfStage {
            name: name.into(),
            cut_rules: false,
            cut_output: false,
            cut_append: false,
            rules: None,
        }
    }

    #[tokio::test]
    async fn controller_applies_next_stage_on_sentinel() {
        let _g = serial();
        init_perf_diag(&test_config(vec![
            floor_stage(),
            rules_stage(),
            full_stage(),
        ]));
        let controller = PerfDiagController::new();
        assert_eq!(controller.current(), 0, "startup applies stages[0]");
        assert!(controller.has_next());

        // round=0 完成 → 应用点 1（rules：cut_rules=false, cut_output=true）
        let applied = controller.on_sentinel(0).await.expect("transition");
        assert_eq!(applied.index, 1);
        assert!(!applied.reloaded);
        assert_eq!(controller.current(), 1);
        assert!(!perf_cut_rules(), "stage 1: rules 求值恢复");
        assert!(perf_cut_output(), "stage 1: 输出仍切");
        assert!(controller.has_next());

        // round=1 完成 → 应用点 2（full：全开）
        let applied = controller.on_sentinel(1).await.expect("transition");
        assert_eq!(applied.index, 2);
        assert_eq!(controller.current(), 2);
        assert!(!perf_cut_rules());
        assert!(!perf_cut_output());
        assert!(!controller.has_next(), "最后一个点之后无切换");

        // 越界：round=2 → None（无点 3）
        assert!(controller.on_sentinel(2).await.is_none());
        // 重复轮次：round=1 再来一次 → None（幂等）
        assert!(controller.on_sentinel(1).await.is_none());
        assert_eq!(controller.current(), 2);
        reset_perf_diag();
    }

    #[tokio::test]
    async fn controller_idempotent_on_repeat_rounds() {
        let _g = serial();
        init_perf_diag(&test_config(vec![no_cut_stage("a"), no_cut_stage("b")]));
        let controller = PerfDiagController::new();
        // 同一 round 重复（--rounds 2）：只切换一次。
        assert!(controller.on_sentinel(0).await.is_some());
        assert!(
            controller.on_sentinel(0).await.is_none(),
            "repeat round must not re-apply"
        );
        assert_eq!(controller.current(), 1);
        reset_perf_diag();
    }

    #[tokio::test]
    async fn controller_noop_without_stages() {
        let _g = serial();
        init_perf_diag(&PerfConfig::default());
        let controller = PerfDiagController::new();
        assert_eq!(controller.current(), 0);
        assert!(!controller.has_next());
        assert!(controller.on_sentinel(0).await.is_none());
        reset_perf_diag();
    }

    #[tokio::test]
    async fn controller_negative_round_is_noop() {
        let _g = serial();
        init_perf_diag(&test_config(vec![no_cut_stage("a"), no_cut_stage("b")]));
        let controller = PerfDiagController::new();
        assert!(controller.on_sentinel(-1).await.is_none());
        assert_eq!(controller.current(), 0);
        reset_perf_diag();
    }

    // -- 数据窗排空等待 ----------------------------------------------------

    fn drain_router() -> (Arc<Router>, Arc<wf_engine::window::Window>) {
        use arrow::datatypes::Schema;
        use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
        use wf_engine::window::{WindowDef, WindowParams, WindowRegistry};

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let def = WindowDef {
            params: WindowParams {
                name: "data_win".into(),
                schema: Arc::clone(&schema),
                time_col_index: None,
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec!["s".into()],
            config: WindowConfig {
                name: "data_win".into(),
                mode: DistMode::Local,
                max_window_bytes: usize::MAX.into(),
                over_cap: Duration::from_secs(3600).into(),
                evict_policy: EvictPolicy::TimeFirst,
                watermark: Duration::from_secs(1).into(),
                allowed_lateness: Duration::from_secs(0).into(),
                late_policy: LatePolicy::Drop,
                table: None,
            },
        };
        let registry = WindowRegistry::build(vec![def]).unwrap();
        let router = Arc::new(Router::new(registry));
        let win = router.registry().get_window("data_win").unwrap();
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        win.append_with_watermark_sized(batch, 8, None).unwrap();
        (router, win)
    }

    #[tokio::test]
    async fn data_drain_waits_until_all_consumers_ack() {
        let (router, win) = drain_router();
        assert_eq!(win.next_seq(), 1, "appended one batch");
        let cancel = CancellationToken::new();
        // 活消费者槽（未 ack）：min_acked=0 < next_seq=1 → 排空等待应阻塞。
        let slot = router.registry().progress("data_win").unwrap().register();
        let wait = tokio::spawn({
            let router = Arc::clone(&router);
            async move { wait_for_data_drain(&router, &cancel).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!wait.is_finished(), "未 ack 时排空等待必须阻塞");
        // 消费者 ack 追平 → 排空返回。
        slot.store(win.next_seq(), std::sync::atomic::Ordering::Release);
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("ack 后排空等待应返回")
            .unwrap();
    }

    #[tokio::test]
    async fn data_drain_cancellation_unblocks() {
        let (router, _win) = drain_router();
        let cancel = CancellationToken::new();
        let wait = tokio::spawn({
            let router = Arc::clone(&router);
            let cancel = cancel.clone();
            async move { wait_for_data_drain(&router, &cancel).await }
        });
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("cancel 后排空等待应返回")
            .unwrap();
    }

    /// Row-partitioned（key/行号分片）窗口：最快分片追平 **不代表** 排空——
    /// 慢分片仍在处理自己的行子集（2026-08-25 review 修复：旧 `max||min`
    /// 在最快分片处提前排空）。
    #[tokio::test]
    async fn data_drain_waits_for_slowest_row_shard() {
        let (router, win) = drain_router();
        let next = win.next_seq();
        assert_eq!(next, 1, "appended one batch");
        let cancel = CancellationToken::new();
        let progress = router.registry().progress("data_win").unwrap();
        // 两个 key 分片消费者：fast 已追平 next_seq，slow 还在 0。
        let fast = progress.register_row_partitioned();
        let slow = progress.register_row_partitioned();
        fast.store(next, std::sync::atomic::Ordering::Release);
        let wait = tokio::spawn({
            let router = Arc::clone(&router);
            async move { wait_for_data_drain(&router, &cancel).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !wait.is_finished(),
            "最快分片已追平但慢分片未处理 → 必须阻塞（旧 max||min 会提前排空）"
        );
        slow.store(next, std::sync::atomic::Ordering::Release);
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("全部分片追平后排空应返回")
            .unwrap();
    }

    /// Round-robin（whole-batch）分片窗口：min 恒停在最慢 shard（每片只 ack
    /// 自己的批次），排空只能看 max（q13 分片卡尾的修复——不能被 min 卡死）。
    #[tokio::test]
    async fn data_drain_completes_when_round_robin_max_catches_up() {
        let (router, win) = drain_router();
        let next = win.next_seq();
        assert_eq!(next, 1, "appended one batch");
        let cancel = CancellationToken::new();
        let progress = router.registry().progress("data_win").unwrap();
        // 2 个 round-robin shard：拿到最后一批（seq=0）的 shard ack=1；
        // 另一个 shard 只 ack 自己最后一批（next=1 时它没有批次 → 停在 0）。
        let owner = progress.register();
        let _other = progress.register();
        owner.store(next, std::sync::atomic::Ordering::Release);
        assert_eq!(progress.min_acked(), 0, "min 停滞在无批次 shard");
        let wait = tokio::spawn({
            let router = Arc::clone(&router);
            async move { wait_for_data_drain(&router, &cancel).await }
        });
        tokio::time::timeout(Duration::from_secs(2), wait)
            .await
            .expect("round-robin max 追平即排空（不能被 min 卡死）")
            .unwrap();
    }

    // -- 哨兵任务与记录投递 ------------------------------------------------

    #[test]
    fn wall_nanos_advances() {
        let a = wall_nanos();
        std::thread::sleep(Duration::from_millis(2));
        let b = wall_nanos();
        assert!(a > 0);
        assert!(b > a, "wall clock must advance");
    }

    fn test_fanout() -> (Arc<SinkFanout>, tokio::sync::mpsc::Receiver<AlertBatch>) {
        use std::collections::HashMap;
        let (tx, rx) = tokio::sync::mpsc::channel::<AlertBatch>(8);
        let mut cache: HashMap<String, _> = HashMap::new();
        cache.insert(
            PERF_SENTINEL_WINDOW.to_string(),
            Arc::new(vec![(1usize, Arc::new(vec![tx]))]),
        );
        (SinkFanout::from_resolved(cache), rx)
    }

    #[tokio::test]
    async fn emit_sentinel_records_sends_to_sink() {
        let (fanout, mut rx) = test_fanout();
        let rec = SentinelRecord {
            round: 0,
            n: 100,
            start_ns: 1_000,
            emit_ns: 2_000,
        };
        emit_sentinel_records(
            vec![stage_record_output(1), sentinel_record_output(&rec)],
            &fanout,
        )
        .await;
        let batch = rx.try_recv().expect("record must reach the sink channel");
        assert_eq!(batch.len(), 2, "stage + sentinel 两条记录");
        assert!(rx.try_recv().is_err(), "只有一批");
    }

    #[tokio::test]
    async fn emit_sentinel_records_empty_is_noop() {
        let (fanout, mut rx) = test_fanout();
        emit_sentinel_records(Vec::new(), &fanout).await;
        assert!(rx.try_recv().is_err(), "空记录不发送");
    }

    #[tokio::test]
    async fn emit_sentinel_records_no_sink_returns_quietly() {
        let fanout = SinkFanout::closed();
        let rec = SentinelRecord {
            round: 0,
            n: 1,
            start_ns: 1,
            emit_ns: 2,
        };
        emit_sentinel_records(vec![sentinel_record_output(&rec)], &fanout).await;
        // 无 sink：warn 一次后静默返回（不 panic）。
    }

    #[tokio::test]
    async fn emit_sentinel_records_skips_unbuildable_record() {
        let (fanout, mut rx) = test_fanout();
        // 保留前缀字段（__wfu_*）→ append_record 失败 → 跳过整批。
        let mut out = sentinel_record_output(&SentinelRecord {
            round: 0,
            n: 1,
            start_ns: 1,
            emit_ns: 2,
        });
        out.yield_fields
            .push(("__wfu_reserved".into(), Value::Number(1.0)));
        emit_sentinel_records(vec![out], &fanout).await;
        assert!(rx.try_recv().is_err(), "构建失败 → 不发送");
    }

    #[tokio::test]
    async fn emit_sentinel_records_falls_back_to_blocking_send() {
        // 容量 1 通道预占满：try_send 满 → 阻塞 send 等接收端排空后成功。
        use std::collections::HashMap;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<AlertBatch>(1);
        tx.try_send(AlertBatch::Rows(Arc::new(Vec::new())))
            .expect("占满槽位");
        let mut cache: HashMap<String, _> = HashMap::new();
        cache.insert(
            PERF_SENTINEL_WINDOW.to_string(),
            Arc::new(vec![(1usize, Arc::new(vec![tx]))]),
        );
        let fanout = SinkFanout::from_resolved(cache);
        let drainer = tokio::spawn(async move {
            let _dummy = rx.recv().await; // 先排掉占位
            rx.recv().await // 哨兵批
        });
        let rec = SentinelRecord {
            round: 0,
            n: 1,
            start_ns: 1,
            emit_ns: 2,
        };
        emit_sentinel_records(vec![sentinel_record_output(&rec)], &fanout).await;
        let batch = drainer.await.unwrap().expect("阻塞 send 送达");
        assert_eq!(batch.len(), 1);
    }

    fn sentinel_push(batch: Option<RecordBatch>) -> RulePush {
        RulePush {
            window_name: Arc::from(PERF_SENTINEL_WINDOW),
            events: None,
            batch: batch.map(Arc::new),
            materialize_fields: None,
            seq: 0,
            shard_rows: None,
        }
    }

    #[tokio::test]
    async fn process_sentinel_push_missing_batch_is_noop() {
        let (fanout, mut rx) = test_fanout();
        let controller = PerfDiagController::new();
        process_sentinel_push(sentinel_push(None), &fanout, &controller).await;
        assert!(rx.try_recv().is_err(), "无 batch 不产出");
    }

    #[tokio::test]
    async fn process_sentinel_push_empty_batch_is_noop() {
        let (fanout, mut rx) = test_fanout();
        let controller = PerfDiagController::new();
        let empty = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "round",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        )
        .unwrap();
        process_sentinel_push(sentinel_push(Some(empty)), &fanout, &controller).await;
        assert!(rx.try_recv().is_err(), "0 行不产出");
    }

    #[tokio::test]
    async fn process_sentinel_push_writes_stage_then_sentinel() {
        let _g = serial();
        init_perf_diag(&test_config(vec![no_cut_stage("a"), no_cut_stage("b")]));
        let (fanout, mut rx) = test_fanout();
        let controller = PerfDiagController::new();
        let batch = sentinel_batch(&[0], &[100], &[1_000]);
        process_sentinel_push(sentinel_push(Some(batch)), &fanout, &controller).await;
        // 先 stage{current=1}（切换完成信号）后 sentinel{round=0}：同批两记录。
        let first = rx.try_recv().expect("第一批（stage + sentinel）");
        assert_eq!(first.len(), 2);
        assert!(rx.try_recv().is_err());
        reset_perf_diag();
    }

    #[tokio::test]
    async fn run_sentinel_task_processes_then_exits_on_cancel() {
        let (router, _win) = drain_router();
        let _g = serial();
        init_perf_diag(&test_config(vec![no_cut_stage("a")]));
        let controller = PerfDiagController::new();
        let (fanout, mut rx) = test_fanout();
        let (tx, rx_ch) = tokio::sync::mpsc::channel::<RulePush>(8);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run_sentinel_task(SentinelTaskConfig {
            router,
            sink_fanout: fanout,
            controller: controller.clone(),
            cancel: cancel.clone(),
            rx: rx_ch,
        }));
        // 启动即写 stage{current=0} 初始信号（轮询等任务完成启动）。
        let init = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(batch) = rx.try_recv() {
                    return batch;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("初始 stage 记录");
        assert_eq!(init.len(), 1);
        // 投一条哨兵 → 处理后 cancel → 任务返回。
        tx.send(sentinel_push(Some(sentinel_batch(&[0], &[10], &[1]))))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("task 应退出")
            .expect("join ok");
        assert!(result.is_ok());
        reset_perf_diag();
    }

    #[tokio::test]
    async fn controller_reloads_rules_subset_via_control_handle() {
        let _g = serial();
        init_perf_diag(&test_config(vec![
            no_cut_stage("floor"),
            PerfStage {
                name: "c_family".into(),
                cut_rules: false,
                cut_output: false,
                cut_append: false,
                rules: Some("models/rules/c_family.wfl".into()),
            },
        ]));
        let controller = PerfDiagController::new();

        // 基线：临时 wfusion.toml（真实 loader 产物）。
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("models")).unwrap();
        std::fs::write(
            dir.path().join("models/windows.toml"),
            r#"[window_defaults]
evict_interval = "1s"
max_window_bytes = "512MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "1s"
allowed_lateness = "30m"
late_policy = "drop"
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("wfusion.toml"),
            r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "file"
name = "seed"
path = "seed.ndjson"
stream_tag = "syslog"
data_format = "ndjson"

[runtime]
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/basic.wfl"
"#,
        )
        .unwrap();
        let cfg_path = dir.path().join("wfusion.toml");
        let ctx = wf_config::ConfigVarContext::new();
        let loader = wf_config::FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(dir.path()));
        let base_raw = loader.load_raw().expect("load raw");
        let base_config = loader.load().expect("load config");
        assert_eq!(base_config.runtime.rules, "rules/basic.wfl");

        // 模拟 Reactor 侧的 reload 消费者：收到请求 → 校验 rules → 回 Applied。
        let (tx, mut rx) = mpsc::channel::<crate::lifecycle::ReloadRequest>(8);
        let handle = RuntimeControlHandle::new(tx, CancellationToken::new());
        controller.set_reload_handle(handle, base_raw, base_config);

        let consumer = tokio::spawn(async move {
            let req = rx.recv().await.expect("reload request");
            match req {
                crate::lifecycle::ReloadRequest::Reload { config, reply, .. } => {
                    assert_eq!(config.runtime.rules, "models/rules/c_family.wfl");
                    let plan = wf_config::FusionReloadPlan::default();
                    let _ = reply.send(Ok(ReloadOutcome::Applied(plan)));
                }
                other => panic!("unexpected request: {other:?}"),
            }
        });

        let applied = controller
            .on_sentinel(0)
            .await
            .expect("transition with reload");
        assert_eq!(applied.index, 1);
        assert!(applied.reloaded, "rules subset change must trigger reload");
        consumer.await.unwrap();

        // 基线已推进：再触发同一目标（重复轮次）→ 幂等短路，不再 reload。
        assert!(controller.on_sentinel(0).await.is_none());
        reset_perf_diag();
    }

    /// 构造带真实 loader 基线 + 空控制通道的控制器（reload 路径测试用）。
    async fn controller_with_baseline(
        stages: Vec<PerfStage>,
    ) -> (
        Arc<PerfDiagController>,
        tokio::sync::mpsc::Receiver<crate::lifecycle::ReloadRequest>,
    ) {
        init_perf_diag(&test_config(stages));
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("models")).unwrap();
        std::fs::write(
            dir.path().join("models/windows.toml"),
            r#"[window_defaults]
evict_interval = "1s"
max_window_bytes = "512MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "1s"
allowed_lateness = "30m"
late_policy = "drop"
"#,
        )
        .unwrap();
        let cfg_path = dir.path().join("wfusion.toml");
        std::fs::write(
            &cfg_path,
            r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "file"
name = "seed"
path = "seed.ndjson"
stream_tag = "syslog"
data_format = "ndjson"

[runtime]
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/basic.wfl"
"#,
        )
        .unwrap();
        let ctx = wf_config::ConfigVarContext::new();
        let loader = wf_config::FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(dir.path()));
        let raw = loader.load_raw().expect("load raw");
        let config = loader.load().expect("load config");
        let controller = PerfDiagController::new();
        let (tx, rx) = tokio::sync::mpsc::channel::<crate::lifecycle::ReloadRequest>(8);
        controller.set_reload_handle(
            RuntimeControlHandle::new(tx, CancellationToken::new()),
            raw,
            config,
        );
        (controller, rx)
    }

    #[tokio::test]
    async fn controller_reload_failure_still_advances() {
        let _g = serial();
        let (controller, mut rx) = controller_with_baseline(vec![
            no_cut_stage("floor"),
            PerfStage {
                name: "c_family".into(),
                cut_rules: false,
                cut_output: false,
                cut_append: false,
                rules: Some("models/rules/c_family.wfl".into()),
            },
        ])
        .await;
        let consumer = tokio::spawn(async move {
            let req = rx.recv().await.expect("reload request");
            match req {
                crate::lifecycle::ReloadRequest::Reload { reply, .. } => {
                    use orion_error::conversion::ToStructError;
                    let _ = reply.send(Err(crate::error::RuntimeReason::Shutdown.to_err()));
                }
                other => panic!("unexpected: {other:?}"),
            }
        });
        let applied = controller.on_sentinel(0).await.expect("reload 失败仍切换");
        consumer.await.unwrap();
        assert_eq!(applied.index, 1);
        assert!(!applied.reloaded, "reload 失败 → reloaded=false");
        assert_eq!(controller.current(), 1, "门控已翻即算切换完成");
        reset_perf_diag();
    }

    #[tokio::test]
    async fn controller_reload_same_rules_is_noop_reload() {
        let _g = serial();
        // 目标点 rules 与基线相同 → changed=false → 不触发 reload（reloaded=false）。
        let (controller, mut rx) = controller_with_baseline(vec![
            no_cut_stage("floor"),
            PerfStage {
                name: "same_rules".into(),
                cut_rules: false,
                cut_output: false,
                cut_append: false,
                rules: Some("rules/basic.wfl".into()),
            },
        ])
        .await;
        let applied = controller.on_sentinel(0).await.expect("transition");
        assert_eq!(applied.index, 1);
        assert!(!applied.reloaded, "rules 未变 → 不 reload");
        // 无 reload 请求发出。
        assert!(
            tokio::time::timeout(Duration::from_millis(100), rx.recv())
                .await
                .is_err(),
            "同 rules 不得触发 reload"
        );
        reset_perf_diag();
    }

    #[tokio::test]
    async fn controller_reload_without_baseline_applies_without_reload() {
        let _g = serial();
        init_perf_diag(&test_config(vec![
            no_cut_stage("floor"),
            PerfStage {
                name: "c_family".into(),
                cut_rules: false,
                cut_output: false,
                cut_append: false,
                rules: Some("models/rules/c_family.wfl".into()),
            },
        ]));
        let controller = PerfDiagController::new();
        // 注入控制句柄但无基线（set_reload_handle 未调用）：changed 由基线推导，
        // 基线缺失 → 不触发 reload，门控仍翻转、切换正常完成。
        let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::lifecycle::ReloadRequest>(8);
        *controller.control.write().unwrap() =
            Some(RuntimeControlHandle::new(tx, CancellationToken::new()));
        let applied = controller.on_sentinel(0).await.expect("无基线也应切换");
        assert_eq!(applied.index, 1);
        assert!(!applied.reloaded);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), rx.recv())
                .await
                .is_err(),
            "无基线不触发 reload"
        );
        reset_perf_diag();
    }
}
