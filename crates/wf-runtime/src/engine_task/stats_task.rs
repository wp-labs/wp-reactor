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

use wf_engine::alert::{AlertColumnBatch, AlertColumnBuilder, OutputRecord};
use wf_engine::match_engine::{
    CloseOutput, CloseReason, RuleExecutor, StatsExecutor, StepData, batch_event_time_nanos_at,
    batch_time_col_index, batch_to_events,
};
use wf_engine::window::{Router, RulePush};
use wf_lang::ast::CloseMode;
use wf_lang::plan::WindowSpec;

use crate::alert_task::SinkFanout;
use crate::error::RuntimeResult;
use crate::metrics::RuntimeMetrics;

use super::TASK_SEQ;
use super::task_types::{StatsTaskConfig, WindowSource};
use super::{register_notifications, wait_any};

/// stats 任务: 消费批次 → 归并 → 固定窗口 close → alert。
pub(super) struct StatsTask {
    task_id: String,
    stats: StatsExecutor,
    executor: RuleExecutor,
    sources: Vec<WindowSource>,
    sink_fanout: Arc<SinkFanout>,
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    time_field: Option<String>,
    intermediate_targets: HashSet<String>,
    pipe_registry: std::sync::Arc<wf_engine::pipe::PipeRegistry>,
    eos_flush: watch::Receiver<u64>,
    push_rx: Option<mpsc::Receiver<RulePush>>,
    progress: HashMap<String, Arc<AtomicU64>>,
    cursors: HashMap<String, u64>,
    shard_index: Option<usize>,
    shard_count: usize,
    /// 当前 fixed 窗口起点（bucket 对齐首个事件; `None` = 尚未见事件）。
    window_start: Option<i64>,
    /// 当前窗口结束（= window_start + dur; close 判定 watermark >= window_end）。
    window_end: Option<i64>,
    /// 单调事件时间 watermark（批次最大时间, 不倒退）。
    last_watermark: i64,
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
            intermediate_targets,
            pipe_registry,
            eos_flush,
            push_rx,
            progress,
            shard_index,
            shard_count,
        } = config;
        let seq = TASK_SEQ.fetch_add(1, Ordering::Relaxed);
        let task_id = format!("{}#{}", executor.plan().name, seq);
        let task = Self {
            task_id,
            stats,
            executor,
            sources: window_sources,
            sink_fanout,
            router,
            metrics,
            time_field,
            intermediate_targets,
            pipe_registry,
            eos_flush,
            push_rx,
            progress,
            cursors: HashMap::new(),
            shard_index,
            shard_count,
            window_start: None,
            window_end: None,
            last_watermark: i64::MIN,
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
        let mut pending: Vec<(String, Vec<RecordBatch>, u64, bool)> = Vec::new();
        for source in &self.sources {
            let cursor = self.cursors.get(&source.window_name).copied().unwrap_or(0);
            // stats P1 空键单实例: 不按 key 分片; 多实例/分片为 P2。
            let (batches, _shard_rows, new_cursor, gap) =
                source.window.read_since_with_shard(cursor, None);
            pending.push((source.window_name.clone(), batches, new_cursor, gap));
        }
        for (window, batches, new_cursor, gap) in pending {
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
            for batch in &batches {
                self.process_batch_from(&window, batch).await;
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
            self.process_batch_from(&push.window_name, &batch).await;
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

    /// 核心: 归并一个批次 + 按批次最大事件时间推进固定窗口。
    async fn process_batch_from(&mut self, window_name: &str, batch: &RecordBatch) {
        // 先推进窗口（可能触发 close 产出）——用批次最大事件时间做 watermark。
        let max_time = batch_max_time(batch, self.time_field.as_deref());
        if max_time > self.last_watermark {
            self.last_watermark = max_time;
            self.advance_window(max_time).await;
        }
        // 归并: 列式优先, 前置不满足回退行式（语义等价, 对拍锁定）。
        let stats_ok = self.stats.process_batch(batch);
        if !stats_ok {
            let events = batch_to_events(batch);
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
        let _ = window_name;
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
        loop {
            let (Some(start), Some(end)) = (self.window_start, self.window_end) else {
                break;
            };
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

    /// close 当前窗口: 冻结度量值 → 合成 CloseOutput → alert 构建 → 投递。
    async fn close_current_window(&mut self, window_start: i64, window_end: i64) {
        let values = self.stats.close_window();
        let labels: Vec<String> = self
            .stats
            .plan
            .measures
            .iter()
            .map(|m| m.label.clone())
            .collect();
        let close =
            build_stats_close_output(self.rule_name(), &values, &labels, window_start, window_end);
        let lookup = super::window_lookup::RegistryLookup::new(&self.router);
        match self.executor.execute_close_with_joins(&close, &lookup) {
            Ok(Some(record)) => {
                self.emit_record(record).await;
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
    // 输出
    // ------------------------------------------------------------------

    /// 单条列式 alert 投递（stats close 低频, 每窗口一次; 不引入 pending 机制）。
    async fn emit_record(&self, record: OutputRecord) {
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
        let mut builder = AlertColumnBuilder::new(Arc::clone(&record.yield_target));
        if let Err(e) = builder.append_record(&record) {
            wf_warn!(pipe, rule = %record.rule_name, error = %e, "stats alert serialize failed");
            return;
        }
        let batch = builder.finish();
        self.dispatch_columns(&record.yield_target, batch).await;
    }

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

/// 合成 CloseOutput（空键 fixed 窗口; close_step_data = 每 measure 一个 StepData）。
fn build_stats_close_output(
    rule_name: &str,
    values: &[f64],
    labels: &[String],
    window_start: i64,
    window_end: i64,
) -> CloseOutput {
    let close_step_data = values
        .iter()
        .zip(labels.iter())
        .map(|(v, label)| StepData {
            satisfied_branch_index: 0,
            label: Some(label.clone()),
            measure_value: *v,
            event_first_time_nanos: Some(window_start),
            event_last_time_nanos: Some(window_end),
            collected_values: vec![],
            field_values: Default::default(),
        })
        .collect();
    CloseOutput {
        rule_name: rule_name.to_string(),
        scope_key: vec![],
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

/// 主循环: push 通道优先（WFUSION_WINDOW_DISPATCH=push）, 否则 pull window log。
pub(crate) async fn run_stats_task(config: StatsTaskConfig) -> RuntimeResult<()> {
    let (mut task, cancel) = StatsTask::new(config);
    let task_id = task.task_id.clone();
    let mut eos = task.eos_flush.clone();

    if let Some(rx) = task.push_rx.take() {
        run_stats_push_loop(&mut task, rx, cancel, &mut eos, &task_id).await
    } else {
        run_stats_pull_loop(&mut task, cancel, &mut eos, &task_id).await
    }
}

async fn run_stats_push_loop(
    task: &mut StatsTask,
    mut rx: mpsc::Receiver<RulePush>,
    cancel: CancellationToken,
    eos: &mut watch::Receiver<u64>,
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
        }
    }
    Ok(())
}

async fn run_stats_pull_loop(
    task: &mut StatsTask,
    cancel: CancellationToken,
    eos: &mut watch::Receiver<u64>,
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
        }
    }
    Ok(())
}
