//! Stats 任务（P1 步骤④c daemon 接线）。
//!
//! 与 rule_task 平级的 stats 执行形态: 消费 fanout 投递的 raw `RecordBatch`
//! （push 通道或 pull window log）, `StatsExecutor` 列式归并（`process_batch`,
//! 前置不满足时回退行式 `process_rows`）, 固定窗口按事件时间 watermark 越过
//! 边界时 close——合成 CloseOutput → 复用 `RuleExecutor::execute_close_with_joins`
//! 构建 OutputRecord → sink_fanout 投递。处理完 ack 进度 slot（与 rule_task
//! 对齐, 否则慢 stats 会卡住窗口驱逐 cursor-gap）。
//!
//! 窗口语义（对齐 CEP fixed 桶）: `bucket_start = (t / dur) * dur`; 空桶无事件
//! 不产出（与 CEP 无实例即无输出一致）。close 判定用批次最大事件时间（单调
//! watermark）。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use wf_engine::alert::{AlertColumnBatch, AlertColumnBuilder};
use wf_engine::match_engine::{
    CloseOutput, CloseReason, EngineHashMap, RuleExecutor, ScopeKey, StatsExecutor, StepData,
    Value, batch_event_time_nanos_at, batch_time_col_index, batch_to_events, field_ref_name,
    materialize_rows,
};
use wf_engine::window::{Router, RulePush};
use wf_lang::ast::{CloseMode, Expr};
use wf_lang::plan::{StatsAggPlan, WindowSpec};

use crate::alert_task::SinkFanout;
use crate::error::RuntimeResult;
use crate::metrics::RuntimeMetrics;

use super::TASK_SEQ;
use super::task_types::{StatsTaskConfig, WindowSource};
use super::{register_notifications, wait_any};

/// 分块 emit 阈值（P5+ q19 close 峰值优化）: builder 累计达该条数即投递并重建。
/// 100 万条/批——投递仍批量（非逐条, 不引入 §9.9 前的逐桶 await 回压）, 但
/// 单次构建峰值从全窗口（q19 30M ≈ 7.94M 条）降到阈值量级。
const EMIT_CHUNK: usize = 1_000_000;

/// 输入分区分片的窗口 partial（空键 stats, 2026-08-24 q15）:
/// 非协调片在窗口 close 时发送的**原始累加状态**。
/// `(window_start_nanos, window_end_nanos, 桶原始状态, 本片事件数)`——协调片
/// 收齐 N-1 个后 `StatsExecutor::merge_partial` 合并再 close emit。
/// 空窗（无事件）也发送空 partial, 协调片据此计数, 不会死锁。
pub(crate) type StatsPartial = (
    i64,
    i64,
    Vec<(
        wf_engine::match_engine::ScopeKey,
        Vec<wf_engine::match_engine::StatsAccum>,
    )>,
    u64,
);

/// pull 模式每次源拉取 = (窗口名, 批次, 每批分片行子集, 新游标, 是否 gap)。
type PendingStatsPull = (
    String,
    Vec<RecordBatch>,
    Vec<Option<Arc<Vec<u32>>>>,
    u64,
    bool,
);

/// stats 任务: 消费批次 → 归并 → 固定窗口 close → alert。
pub(super) struct StatsTask {
    task_id: String,
    stats: StatsExecutor,
    executor: RuleExecutor,
    cancel: CancellationToken,
    sources: Vec<WindowSource>,
    sink_fanout: Arc<SinkFanout>,
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    time_field: Option<String>,
    intermediate_targets: HashSet<String>,
    eos_flush: watch::Receiver<u64>,
    push_rx: Option<mpsc::Receiver<RulePush>>,
    progress: HashMap<String, Arc<AtomicU64>>,
    cursors: HashMap<String, u64>,
    shard_index: Option<usize>,
    shard_count: usize,
    /// 输入分区分片归并（空键 stats）: 协调片收 partial 的接收端（shard 0）;
    /// 非协调片发送端; 未分片两者皆 None。
    merge_rx: Option<mpsc::Receiver<StatsPartial>>,
    merge_tx: Option<mpsc::Sender<StatsPartial>>,
    /// 当前窗口（fixed 桶, bucket 对齐首个事件; `None` = 尚未见事件）。
    window_start: Option<i64>,
    /// 当前窗口结束（= window_start + dur; close 判定 watermark >= window_end）。
    window_end: Option<i64>,
    /// 单调事件时间 watermark（批次最大时间, 不倒退）。
    last_watermark: i64,
    /// 上次真实事件批处理的墙钟时刻（scan_timeouts 兜底推进用）。
    last_activity_wall: std::time::Instant,
    /// 周期性超时扫描间隔（墙钟兜底推进 watermark 关闭尾部窗口）。
    timeout_scan_interval: std::time::Duration,
    /// 已上报 metrics 的超限拒收累计值（delta 记账——`over_limit_new_buckets`
    /// 跨窗口累计, close 上报必须发增量, 否则重复计数）。
    last_reported_over_limit: u64,
}

impl StatsTask {
    pub(super) fn new(config: StatsTaskConfig) -> (Self, CancellationToken) {
        let StatsTaskConfig {
            stats,
            executor,
            window_sources,
            sink_fanout,
            cancel,
            router,
            metrics,
            time_field,
            timeout_scan_interval,
            intermediate_targets,
            pipe_registry: _,
            eos_flush,
            push_rx,
            progress,
            shard_index,
            shard_count,
            merge_rx,
            merge_tx,
        } = config;
        let seq = TASK_SEQ.fetch_add(1, Ordering::Relaxed);
        let task_id = format!("{}#{}", executor.plan().name, seq);
        let task_cancel = cancel.clone();
        let task = Self {
            task_id,
            stats,
            executor,
            cancel: task_cancel,
            sources: window_sources,
            sink_fanout,
            router,
            metrics,
            time_field,
            intermediate_targets,
            eos_flush,
            push_rx,
            progress,
            cursors: HashMap::new(),
            shard_index,
            shard_count,
            merge_rx,
            merge_tx,
            window_start: None,
            window_end: None,
            last_watermark: i64::MIN,
            last_activity_wall: std::time::Instant::now(),
            timeout_scan_interval,
            last_reported_over_limit: 0,
        };
        (task, cancel)
    }

    fn rule_name(&self) -> &str {
        self.executor.plan().name.as_str()
    }

    // ------------------------------------------------------------------
    // 数据路径: pull（默认）与 push（WFUSION_WINDOW_DISPATCH=push）
    // ------------------------------------------------------------------

    /// Pull 模式: 读 window log 的新批次并归并（镜像 rule_task::pull_and_advance）。
    pub(super) async fn pull_and_process(&mut self) {
        // 先收集 pending（只借 &self.sources）, 再逐个 &mut self 处理
        // （镜像 rule_task 的分相, 避免借用冲突）。
        let mut pending: Vec<PendingStatsPull> = Vec::new();
        for source in &self.sources {
            let cursor = self.cursors.get(&source.window_name).copied().unwrap_or(0);
            // 分片（P2）: 带 key 规则每片只拉自己的 shard_rows 子集; 空键单实例拉全批。
            let (batches, shard_rows_per_batch, new_cursor, gap) = source
                .window
                .read_since_with_shard(cursor, self.shard_index);
            pending.push((
                source.window_name.clone(),
                batches,
                shard_rows_per_batch,
                new_cursor,
                gap,
            ));
        }
        for (window, batches, shard_rows_per_batch, new_cursor, gap) in pending {
            if gap {
                wf_warn!(pipe,
                    task_id = %self.task_id,
                    window = %window,
                    "stats cursor gap detected — some data was lost to eviction"
                );
                if let Some(metrics) = &self.metrics {
                    metrics.inc_rule_cursor_gap(self.rule_name(), &window);
                }
            }
            for (batch_index, batch) in batches.iter().enumerate() {
                // 分片: 本片只归并自己的行子集; `None` = 全批（空键/未分片）。
                let shard_rows = shard_rows_per_batch
                    .get(batch_index)
                    .and_then(|opt| opt.as_deref())
                    .map(|rows| rows.as_slice());
                self.process_batch_from(&window, batch, shard_rows).await;
            }
            self.cursors.insert(window.clone(), new_cursor);
            // ack 读位置（同 rule_task: 让 min_acked 到达 next_seq 释放驱逐）
            if let Some(slot) = self.progress.get(window.as_str()) {
                slot.store(new_cursor, Ordering::Release);
            }
        }
    }

    /// Push 模式: 消费一个投递批次（raw batch, events=None 走 defer 路径）。
    pub(super) async fn process_push(&mut self, push: RulePush) {
        let push_seq = push.seq;
        if let Some(batch) = push.batch {
            // 分片广播携带本片行子集（fanout 按键分区）; 未分片为 None。
            self.process_batch_from(
                &push.window_name,
                &batch,
                push.shard_rows.as_deref().map(|v| v.as_slice()),
            )
            .await;
        }
        if let Some(slot) = self.progress.get(push.window_name.as_ref()) {
            slot.store(push_seq.saturating_add(1), Ordering::Release);
        }
    }

    pub(super) async fn drain_push_channel(&mut self, rx: &mut mpsc::Receiver<RulePush>) {
        while let Ok(push) = rx.try_recv() {
            self.process_push(push).await;
        }
    }

    /// 核心: 归并一个批次（可选行子集, P2 分片）+ 推进固定窗口。
    ///
    /// **窗口归属对齐 CEP 逐事件语义**: 批次行按事件时间单调（v5 数据排序保证）,
    /// 先扫时间列按窗口边界**切段**, 每段归并到其所属窗口后再推进——避免整批
    /// 归属到推进后的窗口（10s 窗口下批跨边界时尾部 ~17k 行错归到下一窗, Q12
    /// 44% 组合发散; 总计数仍守恒, 但逐桶值错误）。
    async fn process_batch_from(
        &mut self,
        window_name: &str,
        batch: &RecordBatch,
        shard_rows: Option<&[u32]>,
    ) {
        self.last_activity_wall = std::time::Instant::now();
        let max_time = batch_max_time(batch, self.time_field.as_deref());
        if max_time > self.last_watermark {
            self.last_watermark = max_time; // 单调; scan_timeouts 兜底推进用
        }
        // 无时间列 / 非 fixed 窗口: 退化单段（不推进窗口, 原行为）。
        let (Some(time_col), Some(dur_nanos)) = (
            batch_time_col_index(batch, self.time_field.as_deref()),
            self.window_dur_nanos(),
        ) else {
            self.accumulate_segment(batch, shard_rows).await;
            let _ = window_name;
            return;
        };
        // 行域（升序; 分片子集由 fanout 分区保序, 全批即 0..n）。
        let domain: Vec<u32> = match shard_rows {
            Some(rows) => rows.to_vec(),
            None => (0..batch.num_rows() as u32).collect(),
        };
        let n = domain.len();
        let mut i = 0;
        while i < n {
            // 段起点: 首个事件开窗（bucket 对齐）, 否则推进到段首行时间
            // （跨多窗由 advance_window 循环逐个 close, 空窗不产出）。
            let first_t = batch_event_time_nanos_at(batch, time_col, domain[i] as usize);
            match self.window_end {
                None => {
                    let bucket_start = (first_t / dur_nanos) * dur_nanos;
                    self.window_start = Some(bucket_start);
                    self.window_end = Some(bucket_start + dur_nanos);
                }
                Some(end) if first_t >= end => {
                    self.advance_window(first_t).await;
                }
                Some(_) => {}
            }
            // 段范围: 同一窗口内连续行（t < window_end）。时间列单调 → 段内
            // 行共享同一窗口, 整段一次列式归并（行子集复用 Blocker 1 机制）。
            let end = self.window_end.expect("window established");
            let mut j = i;
            while j < n && batch_event_time_nanos_at(batch, time_col, domain[j] as usize) < end {
                j += 1;
            }
            self.accumulate_segment(batch, Some(&domain[i..j])).await;
            i = j;
        }
        let _ = window_name;
    }

    /// 归并一个行段（列式优先, 前置不满足回退行式, 语义等价对拍锁定）。
    /// `seg = None` = 全批; `Some(rows)` = 仅行域内的行（分片/窗口段）。
    async fn accumulate_segment(&mut self, batch: &RecordBatch, seg: Option<&[u32]>) {
        // perf-diag cut_rules 门控：归并直通（watermark/窗口推进/ack 保留——
        // 这些在 process_batch_from/process_push 层, floor 档收敛）。stats 查询
        // （q15-q19）与 rule_task 对齐——否则 floor 档仍跑全量归并, 墙梯失真
        // （2026-08-25: 补 stats cuts 缺口）。
        if crate::perf_diag::perf_cut_rules() {
            return;
        }
        let stats_ok = self.stats.process_batch_rows(batch, seg);
        if !stats_ok {
            // 回退行式: 只物化行域内的行（与列式行域一致）。
            let events = match seg {
                Some(rows) => materialize_rows(batch, rows),
                None => batch_to_events(batch),
            };
            let rows: Vec<HashMap<String, wf_engine::match_engine::Value>> = events
                .into_iter()
                .map(|ev| {
                    ev.fields
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect()
                })
                .collect();
            let extract = |row: &HashMap<String, wf_engine::match_engine::Value>, name: &str| {
                row.get(name).cloned()
            };
            self.stats.process_rows(&rows, extract);
        }
    }

    // ------------------------------------------------------------------
    // 窗口推进（fixed 桶, 对齐 CEP）
    // ------------------------------------------------------------------

    fn window_dur_nanos(&self) -> Option<i64> {
        match self.stats.plan.window_spec {
            WindowSpec::Fixed(dur) => dur.as_nanos().try_into().ok(),
            _ => None, // session/sliding 为 P2/P3
        }
    }

    /// 按 watermark 推进固定窗口; watermark 越过 window_end 时 close 并开新桶。
    ///
    /// 循环条件每次重读 `self.window_end`（不可用循环外绑定的局部值——close 后
    /// 窗口推进, 局部值不变会无限 close 死循环）。空桶（无事件）不产出, 与 CEP
    /// 无实例一致。
    async fn advance_window(&mut self, watermark: i64) {
        let Some(dur_nanos) = self.window_dur_nanos() else {
            return;
        };
        let Some(window_end) = self.window_end else {
            // 首个事件: bucket 对齐起点
            let bucket_start = (watermark / dur_nanos) * dur_nanos;
            self.window_start = Some(bucket_start);
            self.window_end = Some(bucket_start + dur_nanos);
            return;
        };
        if watermark < window_end {
            return; // 仍在当前窗口
        }
        while let (Some(start), Some(end)) = (self.window_start, self.window_end) {
            if watermark < end {
                break;
            }
            self.close_current_window(start, end).await;
            let next_start = (watermark / dur_nanos) * dur_nanos;
            if next_start == start {
                // 防呆: 极短 dur 下无法前进 → 强制推进一个窗口避免死循环
                self.window_start = Some(start + dur_nanos);
                self.window_end = Some(end + dur_nanos);
                break;
            }
            self.window_start = Some(next_start);
            self.window_end = Some(next_start + dur_nanos);
        }
    }

    /// close 当前窗口: 冻结度量值 → 按桶合成 CloseOutput → alert 构建 → 投递。
    ///
    /// 带 key（P2）: 每桶一条 alert; 桶键拆解为 `scope_key` + 键字段值注入
    /// `field_values`（yield 可读分组键字段, 如 Q12 的 bidder）。
    ///
    /// **空窗不产出**（设计 §6/§7: 空桶无事件不产出, 对齐 CEP 无实例即无输出）:
    /// 空键规则预建 Empty 桶（全 0 累加器）, 无 guard 直接 close 会产出全 0 alert。
    /// 分段归并下事件推进路径本不会空窗 close, 此 guard 是显式不变式 + 未来
    /// session/sliding 路径的防线。
    async fn close_current_window(&mut self, window_start: i64, window_end: i64) {
        // 输入分区分片（空键 stats, q15）: 非协调片提取 raw 状态发送后不 emit
        // （协调片合并后统一产出）; 协调片（shard 0）收齐 N-1 个 partial 合并
        // 后再走正常 close+emit。空窗也发空 partial——协调片按 N-1 次 recv 计数,
        // 空 partial 合并无效果, 避免死锁。
        if let Some(tx) = &self.merge_tx {
            let (buckets, count) = self.stats.take_partial();
            let _ = tx.send((window_start, window_end, buckets, count)).await;
            // 分片也上报自己的拒收增量: 被拒键不在 partial 里（take_partial 只导出
            // 已建桶）, 协调片看不到——不报则分片拒收永远丢失（metrics 低估）。
            self.report_over_limit(window_start, window_end);
            return;
        }
        if let Some(rx) = &mut self.merge_rx {
            for _ in 1..self.shard_count.max(1) {
                // 片退出（tx drop / cancel）时 recv 返回 None——不能 panic（协调片
                // 崩会拖垮整个 daemon）; 放弃该窗口的剩余合并（输出可能不完整,
                // 仅发生在 shutdown/异常场景）。cancel 时同样放弃——避免关闭时序
                // 不一致（某片已退、协调片还在等）时的死锁。
                let partial = tokio::select! {
                    p = rx.recv() => p,
                    _ = self.cancel.cancelled() => None,
                };
                let Some((ws, we, buckets, count)) = partial else {
                    wf_warn!(pipe,
                        task_id = %self.task_id,
                        window_start = window_start,
                        window_end = window_end,
                        remaining = self.shard_count.saturating_sub(1) as u64,
                        "stats merge channel closed / cancelled before all shards sent — skipping merge for this window"
                    );
                    break;
                };
                let empty = buckets.is_empty() && count == 0;
                if !empty && (ws != window_start || we != window_end) {
                    wf_warn!(pipe,
                        task_id = %self.task_id,
                        window_start = window_start,
                        window_end = window_end,
                        partial_start = ws,
                        partial_end = we,
                        "stats input-shard partial window mismatch (should not happen)"
                    );
                }
                self.stats.merge_partial(buckets, count);
            }
        }
        if self.stats.window.event_count == 0 {
            // 全被拒窗口（event_count 0）同样要上报拒收——不延迟到下一有数据窗口。
            self.report_over_limit(window_start, window_end);
            return;
        }
        let labels: Vec<String> = self
            .stats
            .plan
            .measures
            .iter()
            .map(|m| m.label.clone())
            .collect();
        let key_fields: Vec<String> = self
            .stats
            .plan
            .keys
            .iter()
            .filter_map(|k| match k {
                Expr::Field(fr) => Some(field_ref_name(fr).to_string()),
                _ => None,
            })
            .collect();
        let lookup = super::window_lookup::RegistryLookup::new(&self.router);
        // **批量 emit**: 逐桶/逐条目 record 按 yield_target 合并进同一
        // AlertColumnBatch, 每窗口一次投递（消除 per-record await 回压）。
        // 列式 close（L4, 2026-08-25）: 门控通过（常量 score + 简单 entity +
        // Lit/Field/纯字段 General yield, 含 q15-q19 的 fmt detail）→ 收集全部
        // CloseOutput 一次性 `execute_close_direct_batch_columnar`——跳过逐条
        // 的 ctx build / joins / where / OutputRecord（close 路径输出链瓶颈）。
        let columnar_close = self.executor.close_plan_columnar_safe();
        let mut columnar_closes: Vec<CloseOutput> = Vec::new();
        let mut builders: HashMap<Arc<str>, AlertColumnBuilder> = HashMap::new();
        // rich 路径（last/top, Q18/Q19）: 每桶每度量一个值列表, top 产生 N 条目
        // （rank 序）→ 每条目一条 alert, 行字段注入 yield 的 `b.*`。
        let has_row_measures = self
            .stats
            .plan
            .measures
            .iter()
            .any(|m| matches!(m.agg, StatsAggPlan::Last | StatsAggPlan::Top));
        if crate::perf_diag::perf_cut_output() {
            // perf-diag 输出链直通：仍 close 窗口（取桶 + 度量计算 = 归并段
            // 成本, 状态正确重置防泄漏）, 跳过 alert 构建/序列化/投递（输出
            // 段成本）——full 档增量 = 纯输出链（2026-08-25 补 stats cuts）。
            if has_row_measures {
                let _ = self.stats.close_window_by_bucket_rows();
            } else {
                let _ = self.stats.close_window_by_bucket();
            }
            self.report_over_limit(window_start, window_end);
            return;
        }
        if has_row_measures {
            // 行字段列名（P5 紧凑化: 列数组按此列序存储; 生产经 spawn 恒有子集）
            let row_names = self.stats.row_field_names().cloned();
            for bucket in self.stats.close_window_by_bucket_rows() {
                let n_records = bucket.measures.iter().map(Vec::len).max().unwrap_or(1);
                for k in 0..n_records {
                    // 空条目度量（如 top(0) 无产出）安全读取: 越界/空 → 0.0/None,
                    // 否则取 min(k, len-1) 的携带语义（标量跨 top 条目重复）。
                    let values: Vec<f64> = bucket
                        .measures
                        .iter()
                        .map(|m| {
                            m.get(usize::min(k, m.len().saturating_sub(1)))
                                .map_or(0.0, |e| e.measure_value)
                        })
                        .collect();
                    let row_fields: Vec<Option<&std::sync::Arc<[Option<Value>]>>> = bucket
                        .measures
                        .iter()
                        .map(|m| {
                            m.get(usize::min(k, m.len().saturating_sub(1)))
                                .and_then(|e| e.row_fields.as_ref())
                        })
                        .collect();
                    let close = build_stats_close_output(
                        self.rule_name(),
                        &values,
                        &labels,
                        &row_fields,
                        row_names.as_deref().map(|v| v.as_slice()),
                        window_start,
                        window_end,
                        &bucket.key,
                        &key_fields,
                    );
                    if columnar_close {
                        columnar_closes.push(close);
                    } else {
                        self.emit_close_record(&close, &lookup, &mut builders).await;
                    }
                }
            }
        } else {
            // 标量快路径（Q12/16/17 原样）: 每桶 1 条
            let none_rows: Vec<Option<&std::sync::Arc<[Option<Value>]>>> = vec![None; labels.len()];
            for (scope_key, values) in self.stats.close_window_by_bucket() {
                let close = build_stats_close_output(
                    self.rule_name(),
                    &values,
                    &labels,
                    &none_rows,
                    None,
                    window_start,
                    window_end,
                    &scope_key,
                    &key_fields,
                );
                if columnar_close {
                    columnar_closes.push(close);
                } else {
                    self.emit_close_record(&close, &lookup, &mut builders).await;
                }
            }
        }
        if columnar_close && !columnar_closes.is_empty() {
            // 批量列式 close: 单 target（静态 yield_target）builder, 一次投递。
            // emit_time 用窗级墙钟（emit_time 不喂语义, 与 L4 文档一致）。
            let emit_time_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as i64;
            let target = self.executor.static_yield_target();
            let builder = builders
                .entry(Arc::clone(target))
                .or_insert_with(|| AlertColumnBuilder::new(Arc::clone(target)));
            let outcome = self
                .executor
                .execute_close_direct_batch_columnar(&columnar_closes, builder, emit_time_nanos);
            if let Some(metrics) = &self.metrics {
                for _ in 0..outcome.appended {
                    metrics.inc_alert_emitted_total(self.rule_name());
                }
                for _ in 0..outcome.failed {
                    metrics.inc_alert_serialize_failed();
                }
            }
        }
        for (target, mut builder) in builders {
            if builder.is_empty() {
                continue;
            }
            let batch = builder.finish();
            self.dispatch_columns(&target, batch).await;
        }
        // 状态内存 guard 告警（协调片/单片; merge_tx 分片早退分支在上面已单独上报）:
        // close 重置窗口但拒收计数跨窗口保留——读累计值求本窗增量, 上报 metrics +
        // 每窗一次 wf_warn（带窗口区间 + pipe 追踪, 比 executor 内部 log 更显眼）。
        self.report_over_limit(window_start, window_end);
    }

    /// 上报本窗超限拒收增量（metrics + 每窗一次 wf_warn）。同步（无 await）:
    /// 读跨窗口累计计数求 delta（`over_limit_new_buckets` 在 close/reset 后保留,
    /// 必须发增量防重复计数）。协调片/单片/分片（merge_tx）共用。
    fn report_over_limit(&mut self, window_start: i64, window_end: i64) {
        let over_limit = self.stats.window.over_limit_new_buckets();
        let delta = over_limit.saturating_sub(self.last_reported_over_limit);
        if delta > 0 {
            self.last_reported_over_limit = over_limit;
            if let Some(metrics) = &self.metrics {
                metrics.inc_rule_stats_over_limit(self.rule_name(), delta);
            }
            wf_warn!(pipe,
                task_id = %self.task_id,
                rule = %self.rule_name(),
                window_start = window_start,
                window_end = window_end,
                over_limit_rows = delta,
                "stats 状态内存超限——本窗拒收 {} 行（新桶尝试; 累计 {} 行; 已有桶继续累积）",
                delta, over_limit
            );
        }
    }

    /// 单条 CloseOutput → record → 批量 builder（指标计数 + 中间流丢弃 + append）。
    ///
    /// **分块 emit（P5+）**: builder 达到 [`EMIT_CHUNK`] 条立即投递并重建——
    /// 避免一次性构建全窗口 alert 的峰值内存（q19 30M close 7.94M 条一次构建
    /// RSS 23GB; 分块后峰值 ≈ 阈值 × 条均 + sink 在途缓冲）。投递仍是批量
    /// （100 万条/批, 非逐条）, 不引入 §9.9 修复前的逐桶 await 回压。
    async fn emit_close_record(
        &self,
        close: &CloseOutput,
        lookup: &super::window_lookup::RegistryLookup<'_>,
        builders: &mut HashMap<Arc<str>, AlertColumnBuilder>,
    ) {
        match self.executor.execute_close_with_joins(close, lookup) {
            Ok(Some(record)) => {
                if let Some(metrics) = &self.metrics {
                    metrics.inc_alert_emitted_total(&record.rule_name);
                }
                if self.intermediate_targets.contains(&*record.yield_target) {
                    // P1: stats → 中间流（pipe）为后续扩展; 先记录丢弃
                    wf_debug!(pipe,
                        task_id = %self.task_id,
                        rule = %record.rule_name,
                        target = %record.yield_target,
                        output_kind = "intermediate",
                        "stats intermediate output not yet supported (P2)"
                    );
                    return;
                }
                let target = Arc::clone(&record.yield_target);
                let builder = builders
                    .entry(Arc::clone(&target))
                    .or_insert_with(|| AlertColumnBuilder::new(Arc::clone(&target)));
                if let Err(e) = builder.append_record(&record) {
                    wf_warn!(pipe, rule = %record.rule_name, error = %e, "stats alert serialize failed");
                }
                if builder.len() >= EMIT_CHUNK {
                    let batch = builder.finish();
                    self.dispatch_columns(&target, batch).await;
                    builders.remove(&target);
                }
            }
            Ok(None) => {}
            Err(e) => {
                wf_warn!(pipe,
                    task_id = %self.task_id,
                    rule = %self.rule_name(),
                    phase = "stats_close",
                    error = %e,
                    "stats alert build failed"
                );
            }
        }
    }

    // ------------------------------------------------------------------
    // 超时扫描（墙钟兜底, 对齐 CEP scan_timeouts）
    // ------------------------------------------------------------------

    /// 周期性扫描: 用墙钟流逝（capped at 一个扫描间隔）兜底推进 watermark,
    /// 关闭事件时间未越过边界的**尾部窗口**（replay 数据跨度恰好 ≤ 窗口时长时
    /// 事件时间到不了边界——CEP 靠 scan_timeouts 的 wall 推进关闭, stats 同）。
    ///
    /// 语义对齐 CEP: 空窗口不产出（无实例无输出）; 关闭后**清空窗口状态**
    /// （不设新桶）——墙钟持续推进若不重置会每 tick 关闭一个空窗口循环产出。
    pub(super) async fn scan_timeouts(&mut self) {
        let Some(end) = self.window_end else {
            return; // 无窗口（无数据 / 已收尾）
        };
        let effective_watermark = self.last_watermark.saturating_add(
            self.last_activity_wall
                .elapsed()
                .min(self.timeout_scan_interval)
                .as_nanos() as i64,
        );
        if effective_watermark < end {
            return;
        }
        let has_data = self.stats.window.event_count > 0;
        if has_data && let (Some(start), Some(end)) = (self.window_start, self.window_end) {
            self.close_current_window(start, end).await;
        }
        // 清空窗口状态, 等待真实事件重新开桶（避免空窗口循环产出）。
        self.window_start = None;
        self.window_end = None;
    }

    // ------------------------------------------------------------------
    // 输出
    // ------------------------------------------------------------------

    /// 批量列式 alert 投递（每窗口 close 一次; 桶记录已按 yield_target 合并进
    /// 同一 AlertColumnBatch, 见 [`Self::close_current_window`]）。
    async fn dispatch_columns(&self, target: &str, batch: AlertColumnBatch) {
        let records_len = batch.len();
        let sink_groups = self.sink_fanout.resolve(target);
        if sink_groups.is_empty() {
            if let Some(metrics) = &self.metrics {
                metrics.add_alert_no_sink_records(records_len as u64);
            }
            self.sink_fanout.warn_if_no_sink(target);
            return;
        }
        let batch = crate::alert_task::AlertBatch::Columns(Arc::new(batch));
        for (sink_ptr, channels) in sink_groups.iter() {
            let idx = self.sink_fanout.next_index(*sink_ptr, channels.len());
            let tx = &channels[idx];
            match tx.try_send(batch.clone()) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(batch)) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_alert_channel_full();
                    }
                    if let Err(e) = tx.send(batch).await {
                        if let Some(metrics) = &self.metrics {
                            metrics.inc_alert_channel_send_failed();
                        }
                        wf_warn!(pipe, error = %e, "stats alert channel closed");
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.inc_alert_channel_send_failed();
                    }
                    wf_warn!(pipe, rule = %target, "stats alert channel closed, dropping alert batch");
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // flush（EOS / 取消 / 通道关闭时收尾）
    // ------------------------------------------------------------------

    /// close 残留窗口（事件时间未越过边界的尾部窗口）。
    pub(super) async fn flush(&mut self) {
        // 输入分区分片非协调片: 无当前窗口也发空 partial（协调片 flush 在等
        // N-1 个——空窗哨兵窗口 (MIN, MIN), 协调片按空 partial 合并跳过）。
        if let Some(tx) = &self.merge_tx {
            let (start, end) = match (self.window_start, self.window_end) {
                (Some(s), Some(e)) => (s, e),
                _ => (i64::MIN, i64::MIN),
            };
            let (buckets, count) = self.stats.take_partial();
            let _ = tx.send((start, end, buckets, count)).await;
            // 分片尾部窗口同样上报拒收增量（见 close_current_window merge_tx 分支）。
            self.report_over_limit(start, end);
            self.window_start = None;
            self.window_end = None;
            return;
        }
        let (Some(start), Some(end)) = (self.window_start, self.window_end) else {
            return;
        };
        self.close_current_window(start, end).await;
        self.window_start = None;
        self.window_end = None;
    }
}

/// 批次最大事件时间（时间列缺失时返回 i64::MIN——不推进 watermark）。
fn batch_max_time(batch: &RecordBatch, time_field: Option<&str>) -> i64 {
    let Some(time_field) = time_field else {
        return i64::MIN;
    };
    let Some(col_idx) = batch_time_col_index(batch, Some(time_field)) else {
        return i64::MIN;
    };
    let n = batch.num_rows();
    let mut max = i64::MIN;
    for row in 0..n {
        let t = batch_event_time_nanos_at(batch, col_idx, row);
        if t > max {
            max = t;
        }
    }
    max
}

/// 合成 CloseOutput（fixed 窗口）: close_step_data = 每 measure 一个 StepData;
/// 带 key 时桶键拆解为 `scope_key`, 键字段值注入首个 StepData 的 field_values
/// （yield 读分组键字段; build_eval_context 的 narrow/all 分支都注入字段）。
/// `row_fields`（每度量一个, last/top 用）: 行字段列数组按 `row_names` 列序展开
/// 注入其所在度量的 StepData——yield 经 field_values 读 `b.*`（如 Q18 最后一条
/// bid 的 price/channel; P5 紧凑化: 列数组而非 HashMap, 列序 = 提取同序）。
#[allow(clippy::too_many_arguments)] // 合成 CloseOutput: 规则名/值/label/行字段/窗界/键 6 组参数
fn build_stats_close_output(
    rule_name: &str,
    values: &[f64],
    labels: &[String],
    row_fields: &[Option<&std::sync::Arc<[Option<Value>]>>],
    row_names: Option<&[String]>,
    window_start: i64,
    window_end: i64,
    scope_key: &ScopeKey,
    key_fields: &[String],
) -> CloseOutput {
    let scope_values = scope_key_to_values(scope_key);
    let mut first_field_values = EngineHashMap::<String, Vec<Value>>::default();
    if !key_fields.is_empty() {
        for (kf, kv) in key_fields.iter().zip(scope_values.iter()) {
            first_field_values.insert(kf.clone(), vec![kv.clone()]);
        }
    }
    let close_step_data = values
        .iter()
        .zip(labels.iter())
        .enumerate()
        .map(|(i, (v, label))| {
            // 键字段注入首个 StepData; last/top 行字段列数组（P5 紧凑化）按
            // row_names 列序展开注入其所在度量 StepData——yield 读 `b.*`。
            let mut fv = if i == 0 {
                first_field_values.clone()
            } else {
                EngineHashMap::default()
            };
            if let (Some(names), Some(row)) = (row_names, row_fields.get(i).copied().flatten()) {
                for (pos, val) in row.iter().enumerate() {
                    if let Some(v) = val
                        && let Some(name) = names.get(pos)
                    {
                        fv.insert(name.clone(), vec![v.clone()]);
                    }
                }
            }
            StepData {
                satisfied_branch_index: 0,
                label: Some(label.clone()),
                measure_value: *v,
                event_first_time_nanos: Some(window_start),
                event_last_time_nanos: Some(window_end),
                collected_values: vec![],
                field_values: fv,
            }
        })
        .collect();
    CloseOutput {
        rule_name: rule_name.to_string(),
        scope_key: scope_values,
        machine_id: String::new(),
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
        close_step_data,
        bind_data: vec![],
        watermark_nanos: window_end,
        last_event_nanos: window_end,
        event_first_time_nanos: window_start,
        event_last_time_nanos: window_end,
        window_start_time_nanos: window_start,
        window_end_time_nanos: window_end,
    }
}

/// 桶键拆解为字段值列表（Pair 先序展开, 顺序与 keys 一致）。
fn scope_key_to_values(key: &ScopeKey) -> Vec<wf_engine::match_engine::Value> {
    match key {
        ScopeKey::Empty => vec![],
        ScopeKey::Int(i) => vec![wf_engine::match_engine::Value::Number(*i as f64)],
        ScopeKey::Float(b) => vec![wf_engine::match_engine::Value::Number(f64::from_bits(*b))],
        ScopeKey::Str(s) => vec![wf_engine::match_engine::Value::Str(s.clone())],
        ScopeKey::Pair(a, b) => {
            let mut v = scope_key_to_values(a);
            v.extend(scope_key_to_values(b));
            v
        }
    }
}

/// 主循环: push 通道优先（WFUSION_WINDOW_DISPATCH=push）, 否则 pull window log。
pub(crate) async fn run_stats_task(config: StatsTaskConfig) -> RuntimeResult<()> {
    let (mut task, cancel) = StatsTask::new(config);
    let task_id = task.task_id.clone();
    let mut eos = task.eos_flush.clone();
    let timeout_scan_interval = task.timeout_scan_interval;
    let mut timeout_tick = tokio::time::interval(timeout_scan_interval);

    if let Some(rx) = task.push_rx.take() {
        run_stats_push_loop(&mut task, rx, cancel, &mut eos, &mut timeout_tick, &task_id).await
    } else {
        run_stats_pull_loop(&mut task, cancel, &mut eos, &mut timeout_tick, &task_id).await
    }
}

async fn run_stats_push_loop(
    task: &mut StatsTask,
    mut rx: mpsc::Receiver<RulePush>,
    cancel: CancellationToken,
    eos: &mut watch::Receiver<u64>,
    timeout_tick: &mut tokio::time::Interval,
    task_id: &str,
) -> RuntimeResult<()> {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                task.drain_push_channel(&mut rx).await;
                task.flush().await;
                wf_debug!(pipe, task_id = %task_id, "stats task shutdown complete");
                break;
            }
            push = rx.recv() => {
                match push {
                    Some(push) => task.process_push(push).await,
                    None => {
                        task.drain_push_channel(&mut rx).await;
                        task.flush().await;
                        break;
                    }
                }
            }
            _ = eos.changed() => {
                if *eos.borrow() > 0 {
                    task.drain_push_channel(&mut rx).await;
                    task.flush().await;
                    wf_debug!(pipe, task_id = %task_id, "stats task EOS flush complete");
                }
            }
            _ = timeout_tick.tick() => task.scan_timeouts().await,
        }
    }
    Ok(())
}

async fn run_stats_pull_loop(
    task: &mut StatsTask,
    cancel: CancellationToken,
    eos: &mut watch::Receiver<u64>,
    timeout_tick: &mut tokio::time::Interval,
    task_id: &str,
) -> RuntimeResult<()> {
    let notifiers: Vec<Arc<tokio::sync::Notify>> =
        task.sources.iter().map(|s| Arc::clone(&s.notify)).collect();
    loop {
        let mut notifications = register_notifications(&notifiers);
        task.pull_and_process().await;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                task.flush().await;
                wf_debug!(pipe, task_id = %task_id, "stats task shutdown complete");
                break;
            }
            _ = wait_any(&mut notifications) => {}
            _ = eos.changed() => {
                if *eos.borrow() > 0 {
                    task.pull_and_process().await;
                    task.flush().await;
                    wf_debug!(pipe, task_id = %task_id, "stats task EOS flush complete");
                }
            }
            _ = timeout_tick.tick() => task.scan_timeouts().await,
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "stats_task_coverage.rs"]
mod stats_task_coverage;
#[cfg(test)]
#[path = "stats_task_coverage_more.rs"]
mod stats_task_coverage_more;
#[cfg(test)]
#[path = "stats_task_r4.rs"]
mod stats_task_r4;
