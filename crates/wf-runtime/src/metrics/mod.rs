use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use wf_config::MetricsConfig;
use wf_engine::window::{EvictReport, RouteReport};

/// 分配器/进程级内存分账（`window_bytes` 与进程峰值之间的缺口归因）。
pub mod alloc_stats;
#[cfg(test)]
mod coverage_extra;
#[cfg(test)]
mod coverage_r4;
mod sampling;
mod server;
#[cfg(test)]
mod tests;
// 结构拆件（2026-09-04，#[path] sibling，相对本目录）：
// `counters` —— RuntimeMetrics 全量 impl（计数/注册/采样/drain 下沉）；
// `records` —— MetricsSnapshot → MetricsRecord 序列化（to_records 下沉）。
// struct 声明/类型别名/直方图与记录 helper 留本层收口：子树模块（sampling/
// server/测试与新子模块）经 `use super::*` 复用私有字段与方法（可见性只向下流）。
#[path = "counters.rs"]
mod counters;
#[path = "records.rs"]
mod records;

// Re-export pub items that were originally at crate::metrics::* level
pub use self::server::run_metrics_task;

type AlertDetailCounts = BTreeMap<String, AtomicU64>;
type AlertDetailByMachine = BTreeMap<String, AlertDetailCounts>;
type AlertDetailByRule = BTreeMap<String, AlertDetailByMachine>;
type ReceiverMissCounts = BTreeMap<String, AtomicU64>;
type ReceiverMissBySource = BTreeMap<String, ReceiverMissCounts>;

/// Shard count for the `alert_emitted_detail` map. Each rule hashes to exactly
/// one shard, so the per-alert update lock is uncontended across rule tasks
/// (previously all rules fought over a single global `Mutex`, and the 1s metric
/// drain held that same lock while iterating the whole scope space).
const ALERT_DETAIL_SHARDS: usize = 64;
/// Per-rule cap on distinct scope keys tracked in alert detail. Beyond this,
/// new scopes count only toward `alert_emitted_total` (the authoritative per-
/// rule total). Bounds both memory and per-interval ndjson volume on
/// high-cardinality rules (e.g. a pass-through rule keyed on a wide id space).
const ALERT_DETAIL_MAX_SCOPES_PER_RULE: usize = 1024;

/// FNV-1a over the rule name → shard index. Cheap, deterministic within a
/// process, and keeps one rule pinned to one shard.
fn alert_detail_shard(rule: &str) -> usize {
    let mut hash: usize = 0xcbf2_9ce4_8422_2325;
    for byte in rule.bytes() {
        hash ^= byte as usize;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash % ALERT_DETAIL_SHARDS
}

const DEFAULT_HISTOGRAM_BUCKETS_SECONDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0,
];

/// Lock-free histogram with fixed buckets.
///
/// Each observation increments exactly one bucket (non-cumulative storage).
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Metrics")]
struct Histogram {
    upper_bounds_nanos: Vec<u64>,
    bucket_counts: Vec<AtomicU64>,
    sum_nanos: AtomicU64,
}

impl Histogram {
    fn from_seconds_bounds(bounds: &[f64]) -> Self {
        let upper_bounds_nanos = bounds
            .iter()
            .map(|sec| (*sec * 1_000_000_000.0) as u64)
            .collect::<Vec<_>>();
        let bucket_counts = (0..=upper_bounds_nanos.len())
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        Self {
            upper_bounds_nanos,
            bucket_counts,
            sum_nanos: AtomicU64::new(0),
        }
    }

    fn observe_duration(&self, elapsed: Duration) {
        let nanos = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
        let idx = self
            .upper_bounds_nanos
            .iter()
            .position(|bound| nanos <= *bound)
            .unwrap_or(self.upper_bounds_nanos.len());
        self.bucket_counts[idx].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            upper_bounds_nanos: self.upper_bounds_nanos.clone(),
            bucket_counts: self
                .bucket_counts
                .iter()
                .map(|v| v.load(Ordering::Relaxed))
                .collect(),
            sum_seconds: self.sum_nanos.load(Ordering::Relaxed) as f64 / 1_000_000_000.0,
        }
    }
}

#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Metrics")]
struct HistogramSnapshot {
    upper_bounds_nanos: Vec<u64>,
    bucket_counts: Vec<u64>,
    #[allow(dead_code)]
    sum_seconds: f64,
}

/// A single metrics data point — lightweight key-value pairs for sink transport.
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Metrics")]
pub struct MetricsRecord {
    pub fields: Vec<(String, String)>,
}

#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Metrics")]
pub(crate) struct MetricsSnapshot {
    source_types: BTreeMap<String, String>,
    receiver_connections: u64,
    receiver_frames: u64,
    receiver_source_rows: BTreeMap<String, u64>,
    receiver_source_machine_rows: BTreeMap<String, BTreeMap<String, u64>>,
    receiver_source_decode_errors: BTreeMap<String, u64>,
    receiver_source_read_errors: BTreeMap<String, u64>,
    receiver_window_misses: BTreeMap<String, BTreeMap<String, u64>>,
    router_route_calls: u64,
    router_delivered: u64,
    router_dropped_late: u64,
    router_skipped_non_local: u64,
    router_source_route_errors: BTreeMap<String, u64>,
    rule_events: BTreeMap<String, u64>,
    rule_matches: BTreeMap<String, u64>,
    rule_instances: BTreeMap<String, u64>,
    rule_memory_bytes: BTreeMap<String, u64>,
    rule_cursor_gaps: BTreeMap<String, BTreeMap<String, u64>>,
    rule_stats_over_limit: BTreeMap<String, u64>,
    alert_emitted: BTreeMap<String, u64>,
    alert_emitted_detail: BTreeMap<String, BTreeMap<String, BTreeMap<String, u64>>>,
    alert_channel_send_failed: u64,
    alert_sink_dispatch_failed: u64,
    alert_channel_full: u64,
    alert_channel_depth: u64,
    alert_append_failed: u64,
    alert_dispatch: u64,
    alert_no_sink_records: u64,
    alert_drain_dropped_records: u64,
    alert_escalate_failed: u64,
    alert_append_nanos: u64,
    evictor_sweeps: u64,
    evictor_time_evicted: u64,
    evictor_memory_evicted: u64,
    window_memory_bytes: BTreeMap<String, u64>,
    /// 窗口实际分配字节（会计保真度，2026-08-25）。
    window_allocated_bytes: BTreeMap<String, u64>,
    /// 每窗 fanout 通道排队批数 / 总容量（输出链在途量，2026-08-26）。
    window_fanout_queued: BTreeMap<String, u64>,
    window_fanout_capacity: BTreeMap<String, u64>,
    /// 每窗 mailbox 在途字节（已用预算）——在途量可观测性（2026-08-25）。
    window_mailbox_inflight: BTreeMap<String, u64>,
    /// 每窗 mailbox 预算容量。
    window_mailbox_budget: BTreeMap<String, u64>,
    window_capacity_bytes: BTreeMap<String, u64>,
    window_rows: BTreeMap<String, u64>,
    window_batches: BTreeMap<String, u64>,
    window_acked_lag: BTreeMap<String, u64>,
    window_append: BTreeMap<String, u64>,
    window_evict: BTreeMap<String, u64>,
    window_late: BTreeMap<String, u64>,
    receiver_decode_latency: HistogramSnapshot,
    alert_dispatch_latency: HistogramSnapshot,
    event_e2e_latency: HistogramSnapshot,
    rule_scan_timeout: BTreeMap<String, HistogramSnapshot>,
    rule_flush: BTreeMap<String, HistogramSnapshot>,
    /// 分配器读数（未装入 provider 时 `None`，快照略过这些指标）。
    alloc: Option<alloc_stats::AllocStats>,
}

// `to_records`（摊平成 `MetricsRecord`）下沉在 `records` 子模块；metric* 记录
// 构建 helper 收口于此（`records` 与测试经 `use super::*` 共用）。
fn metric(stage: &str, name: &str, label: &str, value: u64) -> MetricsRecord {
    let mut fields = vec![("stage".into(), stage.into()), ("name".into(), name.into())];
    if !label.is_empty() {
        fields.push(("label".into(), label.into()));
    }
    fields.push(("value".into(), value.to_string()));
    MetricsRecord { fields }
}

fn metric_with_type(
    stage: &str,
    name: &str,
    label: &str,
    source_type: &str,
    value: u64,
) -> MetricsRecord {
    let mut fields = vec![("stage".into(), stage.into()), ("name".into(), name.into())];
    if !label.is_empty() {
        fields.push(("label".into(), label.into()));
    }
    if !source_type.is_empty() {
        fields.push(("source_type".into(), source_type.into()));
    }
    fields.push(("value".into(), value.to_string()));
    MetricsRecord { fields }
}

fn metric_source_reason(
    stage: &str,
    name: &str,
    label: &str,
    source_type: &str,
    reason: &str,
    value: u64,
) -> MetricsRecord {
    let mut fields = vec![
        ("stage".into(), stage.into()),
        ("name".into(), name.into()),
        ("label".into(), label.into()),
    ];
    if !source_type.is_empty() {
        fields.push(("source_type".into(), source_type.into()));
    }
    fields.push(("reason".into(), reason.into()));
    fields.push(("value".into(), value.to_string()));
    MetricsRecord { fields }
}

fn metric_double(stage: &str, name: &str, rule: &str, window: &str, value: u64) -> MetricsRecord {
    MetricsRecord {
        fields: vec![
            ("stage".into(), stage.into()),
            ("name".into(), name.into()),
            ("rule".into(), rule.into()),
            ("window".into(), window.into()),
            ("value".into(), value.to_string()),
        ],
    }
}

fn hist_p50(stage: &str, name: &str, label: &str, h: &HistogramSnapshot) -> MetricsRecord {
    let p50 = percentile(h, 0.50);
    let mut fields = vec![
        ("stage".into(), stage.into()),
        ("name".into(), format!("{}_p50", name)),
    ];
    if !label.is_empty() {
        fields.push(("label".into(), label.into()));
    }
    fields.push(("value".into(), format!("{:.6}", p50)));
    MetricsRecord { fields }
}

fn hist_p99(stage: &str, name: &str, label: &str, h: &HistogramSnapshot) -> MetricsRecord {
    let p99 = percentile(h, 0.99);
    let mut fields = vec![
        ("stage".into(), stage.into()),
        ("name".into(), format!("{}_p99", name)),
    ];
    if !label.is_empty() {
        fields.push(("label".into(), label.into()));
    }
    fields.push(("value".into(), format!("{:.6}", p99)));
    MetricsRecord { fields }
}

fn percentile(h: &HistogramSnapshot, p: f64) -> f64 {
    let total: u64 = h.bucket_counts.iter().sum();
    if total == 0 {
        return 0.0;
    }
    let target = (total as f64 * p).ceil() as u64;
    let mut cumulative = 0u64;
    for (i, count) in h.bucket_counts.iter().enumerate() {
        cumulative += count;
        if cumulative >= target {
            let lower = if i == 0 {
                0.0
            } else {
                h.upper_bounds_nanos[i - 1] as f64 / 1_000_000_000.0
            };
            let upper = if i < h.upper_bounds_nanos.len() {
                h.upper_bounds_nanos[i] as f64 / 1_000_000_000.0
            } else {
                lower * 2.0
            };
            let count_in_bucket = *count as f64;
            let excess = cumulative.saturating_sub(target) as f64;
            let frac = if count_in_bucket > 0.0 {
                1.0 - (excess / count_in_bucket)
            } else {
                0.0
            };
            return lower + (upper - lower) * frac;
        }
    }
    h.upper_bounds_nanos
        .last()
        .map(|v| *v as f64 / 1_000_000_000.0)
        .unwrap_or(0.0)
}

#[derive(::moju_derive::MoJu, Clone, Copy)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Metrics")]
pub(crate) struct IntervalRates {
    row_s: f64,
    late_s: f64,
    rules_s: f64,
    sm_s: f64,
    out_s: f64,
    memory_bytes: u64,
}

#[derive(::moju_derive::MoJu, Clone, Copy)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Metrics")]
pub(crate) struct IntervalSnapshot {
    at: Instant,
    rx_rows: u64,
    dropped_late: u64,
    rule_matches: u64,
    rule_instances: u64,
    alert_dispatch: u64,
    window_bytes: u64,
}

#[derive(::moju_derive::MoJu, Clone, Copy)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Metrics")]
pub(crate) struct TotalCounts {
    rows: u64,
    late: u64,
    rules: u64,
    out: u64,
    sm_delta: i64,
}

#[derive(::moju_derive::MoJu, Default)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Metrics")]
pub(crate) struct RunSummary {
    interval_count: u64,
    sum_row_s: f64,
    sum_late_s: f64,
    sum_rules_s: f64,
    sum_sm_s: f64,
    sum_out_s: f64,
    sum_memory_bytes: f64,
    max_row_s: f64,
    max_late_s: f64,
    max_rules_s: f64,
    max_sm_s: f64,
    max_out_s: f64,
    max_memory_bytes: u64,
}

impl RunSummary {
    fn observe(&mut self, rates: IntervalRates) {
        self.interval_count += 1;
        self.sum_row_s += rates.row_s;
        self.sum_late_s += rates.late_s;
        self.sum_rules_s += rates.rules_s;
        self.sum_sm_s += rates.sm_s;
        self.sum_out_s += rates.out_s;
        self.sum_memory_bytes += rates.memory_bytes as f64;

        self.max_row_s = self.max_row_s.max(rates.row_s);
        self.max_late_s = self.max_late_s.max(rates.late_s);
        self.max_rules_s = self.max_rules_s.max(rates.rules_s);
        self.max_sm_s = self.max_sm_s.max(rates.sm_s);
        self.max_out_s = self.max_out_s.max(rates.out_s);
        self.max_memory_bytes = self.max_memory_bytes.max(rates.memory_bytes);
    }

    fn table(&self, totals: Option<TotalCounts>) -> Option<String> {
        if self.interval_count == 0 {
            return None;
        }
        let n = self.interval_count as f64;
        let avg_row_s = self.sum_row_s / n;
        let avg_late_s = self.sum_late_s / n;
        let avg_rules_s = self.sum_rules_s / n;
        let avg_sm_s = self.sum_sm_s / n;
        let avg_out_s = self.sum_out_s / n;
        let avg_mem = format_bytes((self.sum_memory_bytes / n).round() as u64);
        let max_mem = format_bytes(self.max_memory_bytes);

        let mut out = format!(
            "\n+---------+-----------+-----------+-----------+-----------+-------------+-----------+\n\
             | stat    | row/s     | late/s    | rules/s   | sm/s      | memory      | out/s     |\n\
             +---------+-----------+-----------+-----------+-----------+-------------+-----------+\n\
             | avg     | {avg_row_s:>9.1} | {avg_late_s:>9.1} | {avg_rules_s:>9.1} | {avg_sm_s:>9.1} | {avg_mem:>11} | {avg_out_s:>9.1} |\n\
             | max     | {max_row_s:>9.1} | {max_late_s:>9.1} | {max_rules_s:>9.1} | {max_sm_s:>9.1} | {max_mem:>11} | {max_out_s:>9.1} |\n\
             +---------+-----------+-----------+-----------+-----------+-------------+-----------+",
            max_row_s = self.max_row_s,
            max_late_s = self.max_late_s,
            max_rules_s = self.max_rules_s,
            max_sm_s = self.max_sm_s,
            max_out_s = self.max_out_s,
        );

        if let Some(total) = totals {
            let sm_delta = if total.sm_delta >= 0 {
                format!("+{}", total.sm_delta)
            } else {
                total.sm_delta.to_string()
            };
            out.push_str(&format!(
                "\n+---------+------------+------------+------------+------------+------------+\n\
                 | total   | rows       | late       | rules      | sm_delta   | out        |\n\
                 +---------+------------+------------+------------+------------+------------+\n\
                 | count   | {rows:>10} | {late:>10} | {rules:>10} | {sm_delta:>10} | {out_cnt:>10} |\n\
                 +---------+------------+------------+------------+------------+------------+",
                rows = total.rows,
                late = total.late,
                rules = total.rules,
                out_cnt = total.out,
            ));
        }

        Some(out)
    }
}

/// Shared runtime metrics store.
///
/// Counters are lock-free atomics. Label sets (`rule`, `window`) are fixed at
/// startup to keep hot-path updates allocation-free.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Runtime", module = "Runtime.Metrics")]
pub struct RuntimeMetrics {
    receiver_connections_total: AtomicU64,
    receiver_frames_total: AtomicU64,
    receiver_rows_total: AtomicU64,
    receiver_decode_errors_total: AtomicU64,
    receiver_read_errors_total: AtomicU64,

    source_types: BTreeMap<String, String>,

    receiver_source_rows_total: BTreeMap<String, AtomicU64>,
    receiver_source_machine_rows: Mutex<BTreeMap<String, BTreeMap<String, AtomicU64>>>,
    receiver_source_decode_errors_total: BTreeMap<String, AtomicU64>,
    receiver_source_read_errors_total: BTreeMap<String, AtomicU64>,
    receiver_window_miss_total: Mutex<ReceiverMissBySource>,

    router_route_calls_total: AtomicU64,
    router_delivered_total: AtomicU64,
    router_dropped_late_total: AtomicU64,
    router_skipped_non_local_total: AtomicU64,
    router_source_route_errors_total: BTreeMap<String, AtomicU64>,

    rule_events_total: BTreeMap<String, AtomicU64>,
    rule_matches_total: BTreeMap<String, AtomicU64>,
    /// Gauge, summed across a rule's shards via delta reports (P2b).
    rule_instances: BTreeMap<String, AtomicI64>,
    /// Gauge of estimated live-instance memory（`limits.max_memory` 会计值），
    /// 跨 shard delta 上报求和（2026-09-04 导出：max_memory 可用实测校准）。
    rule_memory_bytes: BTreeMap<String, AtomicI64>,
    rule_cursor_gap_total: BTreeMap<String, BTreeMap<String, AtomicU64>>,
    /// 状态内存 guard 超限拒收新键桶累计（stats 规则, close 时按窗口增量上报）。
    rule_stats_over_limit_total: BTreeMap<String, AtomicU64>,

    alert_emitted_total: BTreeMap<String, AtomicU64>,
    alert_emitted_detail_shards: Vec<Mutex<AlertDetailByRule>>,
    alert_channel_send_failed_total: AtomicU64,
    alert_sink_dispatch_failed_total: AtomicU64,
    alert_channel_full_total: AtomicU64,
    alert_channel_depth: AtomicU64,
    alert_append_failed_total: AtomicU64,
    alert_dispatch_total: AtomicU64,
    alert_no_sink_records_total: AtomicU64,
    alert_drain_dropped_records_total: AtomicU64,
    alert_escalate_failed_total: AtomicU64,
    alert_append_nanos: AtomicU64,

    evictor_sweeps_total: AtomicU64,
    evictor_time_evicted_total: AtomicU64,
    evictor_memory_evicted_total: AtomicU64,

    window_memory_bytes: BTreeMap<String, AtomicU64>,
    /// 每窗 fanout 通道排队批数 / 总容量（输出链在途量，2026-08-26）。
    window_fanout_queued: BTreeMap<String, AtomicU64>,
    window_fanout_capacity: BTreeMap<String, AtomicU64>,
    /// 窗口**实际分配**字节（`Window::allocated_usage`）——`memory_bytes` 是逻辑
    /// 内容口径（不含 bitmap/offsets）；内存分账用这个交叉校验。
    window_allocated_bytes: BTreeMap<String, AtomicU64>,
    /// 每窗 mailbox 在途字节 / 预算容量（周期采样，同 window 其他 gauge）。
    window_mailbox_inflight: BTreeMap<String, AtomicU64>,
    window_mailbox_budget: BTreeMap<String, AtomicU64>,
    window_capacity_bytes: BTreeMap<String, AtomicU64>,
    window_rows: BTreeMap<String, AtomicU64>,
    window_batches: BTreeMap<String, AtomicU64>,
    /// Gauge: number of batches appended but not yet acked by the slowest live
    /// consumer (`next_seq - min_acked`). `0` for an unconsumed window (min_acked
    /// = u64::MAX) or a fully-consumed window. The bench uses the sum over input
    /// windows as the pull-model "rules drained" completion signal.
    window_acked_lag: BTreeMap<String, AtomicU64>,
    window_append_total: BTreeMap<String, AtomicU64>,
    window_evict_total: BTreeMap<String, AtomicU64>,
    window_late_total: BTreeMap<String, AtomicU64>,

    receiver_decode_seconds: Histogram,
    alert_dispatch_seconds: Histogram,
    event_e2e_latency_seconds: Histogram,
    rule_scan_timeout_seconds: BTreeMap<String, Histogram>,
    rule_flush_seconds: BTreeMap<String, Histogram>,
}

// `RuntimeMetrics` 的全量 impl（计数/注册/采样/drain）下沉在 `counters` 子模块。
fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut idx = 0usize;
    while value >= 1024.0 && idx < UNITS.len() - 1 {
        value /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{}{}", bytes, UNITS[idx])
    } else {
        format!("{value:.1}{}", UNITS[idx])
    }
}

pub type MonSend = tokio::sync::mpsc::Sender<Vec<MetricsRecord>>;
pub type MonRecv = tokio::sync::mpsc::Receiver<Vec<MetricsRecord>>;

pub fn maybe_build_metrics(
    config: &MetricsConfig,
    rule_names: &[String],
    window_names: &[String],
    source_names: &[String],
    source_types: BTreeMap<String, String>,
) -> Option<Arc<RuntimeMetrics>> {
    if !config.enabled {
        return None;
    }
    Some(Arc::new(RuntimeMetrics::new(
        rule_names,
        window_names,
        source_names,
        source_types,
    )))
}
