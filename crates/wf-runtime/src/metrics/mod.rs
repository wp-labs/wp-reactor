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
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.RuntimeMetrics"
)]
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
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.RuntimeMetrics"
)]
struct HistogramSnapshot {
    upper_bounds_nanos: Vec<u64>,
    bucket_counts: Vec<u64>,
    #[allow(dead_code)]
    sum_seconds: f64,
}

/// A single metrics data point — lightweight key-value pairs for sink transport.
#[derive(Debug, Clone)]
pub struct MetricsRecord {
    pub fields: Vec<(String, String)>,
}

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

impl MetricsSnapshot {
    #[allow(clippy::vec_init_then_push)]
    pub fn to_records(&self) -> Vec<MetricsRecord> {
        let mut out = Vec::new();
        out.push(metric(
            "receiver",
            "connections_total",
            "",
            self.receiver_connections,
        ));
        out.push(metric("receiver", "frames_total", "", self.receiver_frames));
        for (source, v) in &self.receiver_source_rows {
            // Skip per-source output when per-machine breakdown is available
            if !self.receiver_source_machine_rows.contains_key(source) {
                let source_type = self.source_types.get(source).cloned().unwrap_or_default();
                out.push(metric_with_type(
                    "receiver",
                    "rows_total",
                    source,
                    &source_type,
                    *v,
                ));
            }
        }
        for (source, by_machine) in &self.receiver_source_machine_rows {
            let source_type = self.source_types.get(source).cloned().unwrap_or_default();
            for (machine, v) in by_machine {
                out.push(MetricsRecord {
                    fields: vec![
                        ("stage".into(), "receiver".into()),
                        ("name".into(), "rows_total".into()),
                        ("label".into(), source.clone()),
                        ("source_type".into(), source_type.clone()),
                        ("machine".into(), machine.clone()),
                        ("value".into(), v.to_string()),
                    ],
                });
            }
        }
        for (source, v) in &self.receiver_source_decode_errors {
            out.push(metric("receiver", "decode_errors_total", source, *v));
        }
        for (source, v) in &self.receiver_source_read_errors {
            out.push(metric("receiver", "read_errors_total", source, *v));
        }
        for (source, by_reason) in &self.receiver_window_misses {
            let source_type = self.source_types.get(source).cloned().unwrap_or_default();
            for (reason, v) in by_reason {
                out.push(metric_source_reason(
                    "receiver",
                    "window_miss_total",
                    source,
                    &source_type,
                    reason,
                    *v,
                ));
            }
        }
        out.push(metric(
            "router",
            "route_calls_total",
            "",
            self.router_route_calls,
        ));
        out.push(metric(
            "router",
            "delivered_total",
            "",
            self.router_delivered,
        ));
        out.push(metric(
            "router",
            "dropped_late_total",
            "",
            self.router_dropped_late,
        ));
        out.push(metric(
            "router",
            "skipped_non_local_total",
            "",
            self.router_skipped_non_local,
        ));
        for (source, v) in &self.router_source_route_errors {
            out.push(metric("router", "route_errors_total", source, *v));
        }
        out.push(metric("evictor", "sweeps_total", "", self.evictor_sweeps));
        out.push(metric(
            "evictor",
            "time_evicted_total",
            "",
            self.evictor_time_evicted,
        ));
        out.push(metric(
            "evictor",
            "memory_evicted_total",
            "",
            self.evictor_memory_evicted,
        ));
        // 内存分账（装入 provider 时才产出）：与各窗 `window.memory_bytes`
        // 对比即可区分"引擎真持有"（peak_commit ≫ 窗口合计）与
        // "段区/OS 伪影"（peak_commit ≈ 窗口合计但 peak_rss 远大）。
        if let Some(a) = self.alloc {
            out.push(metric("alloc", "current_rss_bytes", "", a.current_rss));
            out.push(metric("alloc", "peak_rss_bytes", "", a.peak_rss));
            out.push(metric(
                "alloc",
                "current_commit_bytes",
                "",
                a.current_commit,
            ));
            out.push(metric("alloc", "peak_commit_bytes", "", a.peak_commit));
            out.push(metric("alloc", "page_faults_total", "", a.page_faults));
        }
        out.push(metric(
            "alert",
            "channel_send_failed_total",
            "",
            self.alert_channel_send_failed,
        ));
        out.push(metric(
            "alert",
            "sink_dispatch_failed_total",
            "",
            self.alert_sink_dispatch_failed,
        ));
        out.push(metric(
            "alert",
            "channel_full_total",
            "",
            self.alert_channel_full,
        ));
        out.push(metric(
            "alert",
            "channel_depth",
            "",
            self.alert_channel_depth,
        ));
        out.push(metric(
            "alert",
            "append_failed_total",
            "",
            self.alert_append_failed,
        ));
        out.push(metric("alert", "dispatch_total", "", self.alert_dispatch));
        out.push(metric(
            "alert",
            "no_sink_records_total",
            "",
            self.alert_no_sink_records,
        ));
        out.push(metric(
            "alert",
            "drain_dropped_records_total",
            "",
            self.alert_drain_dropped_records,
        ));
        out.push(metric(
            "alert",
            "escalate_failed_total",
            "",
            self.alert_escalate_failed,
        ));
        out.push(metric("alert", "append_nanos", "", self.alert_append_nanos));
        for (rule, v) in &self.rule_events {
            out.push(metric("rule", "events_total", rule, *v));
        }
        for (rule, v) in &self.rule_matches {
            out.push(metric("rule", "matches_total", rule, *v));
        }
        for (rule, v) in &self.rule_instances {
            out.push(metric("rule", "instances", rule, *v));
        }
        for (rule, windows) in &self.rule_cursor_gaps {
            for (window, v) in windows {
                out.push(metric_double("rule", "cursor_gap_total", rule, window, *v));
            }
        }
        for (rule, v) in &self.rule_stats_over_limit {
            out.push(metric("rule", "stats_over_limit_total", rule, *v));
        }
        // Exact per-rule emitted totals are always exported. (Previously the
        // exact total was dropped when the rule had detail rows, and the
        // 1-in-64 sampled per-scope counts were exported under the same
        // `emitted_total` name — making emission look ~64x lower than it was.)
        for (rule, v) in &self.alert_emitted {
            out.push(metric("alert", "emitted_total", rule, *v));
        }
        // Sampled (1/64) per-machine/per-scope breakdown, exported under its
        // own name so it can never be mistaken for the authoritative total.
        for (rule, by_machine) in &self.alert_emitted_detail {
            for (machine, by_scope) in by_machine {
                for (scope, v) in by_scope {
                    out.push(MetricsRecord {
                        fields: vec![
                            ("stage".into(), "alert".into()),
                            ("name".into(), "emitted_detail".into()),
                            ("label".into(), rule.clone()),
                            ("machine".into(), machine.clone()),
                            ("scope_key".into(), scope.clone()),
                            ("value".into(), v.to_string()),
                        ],
                    });
                }
            }
        }
        for (window, v) in &self.window_memory_bytes {
            out.push(metric("window", "memory_bytes", window, *v));
        }
        // 会计保真度（2026-08-25）：`memory_bytes` 是 content_bytes（逻辑内容，
        // 驱逐/mailbox 预算口径），不含 null bitmap / offsets；`allocated_bytes`
        // 按缓冲去重后累加实际引用长度。生产实测两者基本相等（1.00×），该指标的
        // 作用是持续证明 content 口径没有系统性低估。
        for (window, v) in &self.window_allocated_bytes {
            out.push(metric("window", "allocated_bytes", window, *v));
        }
        // 输出链在途量（2026-08-26）：规则分片通道排队批数/容量。diag 墙梯把
        // q13 的 12.5GB 增量定位到输出链，而窗口会计只解释 4.1GB——这两个 gauge
        // 判断"分片通道是否接近满"（10 分片 × 256 槽 × 3.45MB ≈ 8.8GB 满队）。
        for (window, v) in &self.window_fanout_queued {
            out.push(metric("window", "fanout_queued_batches", window, *v));
        }
        for (window, v) in &self.window_fanout_capacity {
            out.push(metric("window", "fanout_capacity_batches", window, *v));
        }
        // 在途量分账（2026-08-25）：每窗 mailbox 已用预算 + parse pool 预读预算。
        // ❗ mailbox 在途与 `memory_bytes` **可能重叠**（Arrow 缓冲经 Arc 共享：
        // 已 append 但未释放 permits 的批次两边都算）——对账时不得直接相加。
        for (window, v) in &self.window_mailbox_inflight {
            out.push(metric("window", "mailbox_inflight_bytes", window, *v));
        }
        for (window, v) in &self.window_mailbox_budget {
            out.push(metric("window", "mailbox_budget_bytes", window, *v));
        }
        for (window, v) in &self.window_capacity_bytes {
            out.push(metric("window", "window_capacity_bytes", window, *v));
        }
        for (window, v) in &self.window_rows {
            out.push(metric("window", "rows", window, *v));
        }
        for (window, v) in &self.window_batches {
            out.push(metric("window", "batches", window, *v));
        }
        for (window, v) in &self.window_acked_lag {
            out.push(metric("window", "acked_lag", window, *v));
        }
        for (window, v) in &self.window_append {
            out.push(metric("window", "append_total", window, *v));
        }
        for (window, v) in &self.window_evict {
            out.push(metric("window", "evict_total", window, *v));
        }
        for (window, v) in &self.window_late {
            out.push(metric("window", "late_total", window, *v));
        }
        for (rule, h) in &self.rule_scan_timeout {
            out.push(hist_p50("rule", "scan_timeout_seconds", rule, h));
            out.push(hist_p99("rule", "scan_timeout_seconds", rule, h));
        }
        for (rule, h) in &self.rule_flush {
            out.push(hist_p50("rule", "flush_seconds", rule, h));
            out.push(hist_p99("rule", "flush_seconds", rule, h));
        }
        out.push(hist_p50(
            "receiver",
            "decode_seconds",
            "",
            &self.receiver_decode_latency,
        ));
        out.push(hist_p99(
            "receiver",
            "decode_seconds",
            "",
            &self.receiver_decode_latency,
        ));
        out.push(hist_p50(
            "alert",
            "dispatch_seconds",
            "",
            &self.alert_dispatch_latency,
        ));
        out.push(hist_p99(
            "alert",
            "dispatch_seconds",
            "",
            &self.alert_dispatch_latency,
        ));
        out.push(hist_p50(
            "event",
            "e2e_latency_seconds",
            "",
            &self.event_e2e_latency,
        ));
        out.push(hist_p99(
            "event",
            "e2e_latency_seconds",
            "",
            &self.event_e2e_latency,
        ));
        out
    }
}

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
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.RuntimeMetrics"
)]
pub(crate) struct IntervalRates {
    row_s: f64,
    late_s: f64,
    rules_s: f64,
    sm_s: f64,
    out_s: f64,
    memory_bytes: u64,
}

#[derive(::moju_derive::MoJu, Clone, Copy)]
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.RuntimeMetrics"
)]
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
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.RuntimeMetrics"
)]
pub(crate) struct TotalCounts {
    rows: u64,
    late: u64,
    rules: u64,
    out: u64,
    sm_delta: i64,
}

#[derive(::moju_derive::MoJu, Default)]
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.RuntimeMetrics"
)]
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
#[moju(
    kind = "struct",
    domain = "Orchestra",
    module = "Orchestra.RuntimeMetrics"
)]
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

impl RuntimeMetrics {
    fn total_rule_matches(&self) -> u64 {
        self.rule_matches_total
            .values()
            .map(|v| v.load(Ordering::Relaxed))
            .sum()
    }

    fn total_rule_instances(&self) -> u64 {
        self.rule_instances
            .values()
            .map(|v| v.load(Ordering::Relaxed))
            .map(|v| v.max(0) as u64)
            .sum()
    }

    fn total_alert_dispatch(&self) -> u64 {
        self.alert_dispatch_total.load(Ordering::Relaxed)
    }

    fn total_window_bytes(&self) -> u64 {
        self.window_memory_bytes
            .values()
            .map(|v| v.load(Ordering::Relaxed))
            .sum()
    }

    pub(crate) fn interval_snapshot(&self, at: Instant) -> IntervalSnapshot {
        IntervalSnapshot {
            at,
            rx_rows: self.receiver_rows_total.load(Ordering::Relaxed),
            dropped_late: self.router_dropped_late_total.load(Ordering::Relaxed),
            rule_matches: self.total_rule_matches(),
            rule_instances: self.total_rule_instances(),
            alert_dispatch: self.total_alert_dispatch(),
            window_bytes: self.total_window_bytes(),
        }
    }

    pub(crate) fn interval_rates(
        &self,
        prev: IntervalSnapshot,
        curr: IntervalSnapshot,
    ) -> Option<IntervalRates> {
        let secs = (curr.at - prev.at).as_secs_f64();
        if secs <= 0.0 {
            return None;
        }

        Some(IntervalRates {
            row_s: curr.rx_rows.saturating_sub(prev.rx_rows) as f64 / secs,
            late_s: curr.dropped_late.saturating_sub(prev.dropped_late) as f64 / secs,
            rules_s: curr.rule_matches.saturating_sub(prev.rule_matches) as f64 / secs,
            sm_s: (curr.rule_instances as f64 - prev.rule_instances as f64) / secs,
            out_s: curr.alert_dispatch.saturating_sub(prev.alert_dispatch) as f64 / secs,
            memory_bytes: curr.window_bytes,
        })
    }

    pub(crate) fn interval_table(&self, rates: IntervalRates) -> String {
        let mem = format_bytes(rates.memory_bytes);
        format!(
            "\n+-----------+-----------+-----------+-----------+-------------+-----------+\n\
             | row/s     | late/s    | rules/s   | sm/s      | memory      | out/s     |\n\
             +-----------+-----------+-----------+-----------+-------------+-----------+\n\
             | {row_s:>9.1} | {late_s:>9.1} | {rules_s:>9.1} | {sm_s:>9.1} | {mem:>11} | {out_s:>9.1} |\n\
             +-----------+-----------+-----------+-----------+-------------+-----------+",
            row_s = rates.row_s,
            late_s = rates.late_s,
            rules_s = rates.rules_s,
            sm_s = rates.sm_s,
            out_s = rates.out_s,
        )
    }

    pub fn new(
        rule_names: &[String],
        window_names: &[String],
        source_names: &[String],
        source_types: BTreeMap<String, String>,
    ) -> Self {
        let make_rule_map = || {
            rule_names
                .iter()
                .map(|name| (name.clone(), AtomicU64::new(0)))
                .collect::<BTreeMap<_, _>>()
        };
        // Signed gauge map for `rule_instances` (sum across shards, P2b).
        let make_rule_map_i64 = || {
            rule_names
                .iter()
                .map(|name| (name.clone(), AtomicI64::new(0)))
                .collect::<BTreeMap<_, _>>()
        };
        let make_rule_hist_map = || {
            rule_names
                .iter()
                .map(|name| {
                    (
                        name.clone(),
                        Histogram::from_seconds_bounds(DEFAULT_HISTOGRAM_BUCKETS_SECONDS),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let make_window_map = || {
            window_names
                .iter()
                .map(|name| (name.clone(), AtomicU64::new(0)))
                .collect::<BTreeMap<_, _>>()
        };
        let make_source_map = || {
            source_names
                .iter()
                .map(|name| (name.clone(), AtomicU64::new(0)))
                .collect::<BTreeMap<_, _>>()
        };
        let mut gap_map = BTreeMap::new();
        for rule in rule_names {
            let mut by_window = BTreeMap::new();
            for window in window_names {
                by_window.insert(window.clone(), AtomicU64::new(0));
            }
            gap_map.insert(rule.clone(), by_window);
        }

        Self {
            receiver_connections_total: AtomicU64::new(0),
            receiver_frames_total: AtomicU64::new(0),
            receiver_rows_total: AtomicU64::new(0),
            receiver_decode_errors_total: AtomicU64::new(0),
            receiver_read_errors_total: AtomicU64::new(0),
            source_types,
            receiver_source_rows_total: make_source_map(),
            receiver_source_machine_rows: Mutex::new(BTreeMap::new()),
            receiver_source_decode_errors_total: make_source_map(),
            receiver_source_read_errors_total: make_source_map(),
            receiver_window_miss_total: Mutex::new(BTreeMap::new()),
            router_route_calls_total: AtomicU64::new(0),
            router_delivered_total: AtomicU64::new(0),
            router_dropped_late_total: AtomicU64::new(0),
            router_skipped_non_local_total: AtomicU64::new(0),
            router_source_route_errors_total: make_source_map(),
            rule_events_total: make_rule_map(),
            rule_matches_total: make_rule_map(),
            rule_instances: make_rule_map_i64(),
            rule_cursor_gap_total: gap_map,
            rule_stats_over_limit_total: make_rule_map(),
            alert_emitted_total: make_rule_map(),
            alert_emitted_detail_shards: (0..ALERT_DETAIL_SHARDS)
                .map(|_| Mutex::new(BTreeMap::new()))
                .collect(),
            alert_channel_send_failed_total: AtomicU64::new(0),
            alert_sink_dispatch_failed_total: AtomicU64::new(0),
            alert_channel_full_total: AtomicU64::new(0),
            alert_channel_depth: AtomicU64::new(0),
            alert_append_failed_total: AtomicU64::new(0),
            alert_dispatch_total: AtomicU64::new(0),
            alert_no_sink_records_total: AtomicU64::new(0),
            alert_drain_dropped_records_total: AtomicU64::new(0),
            alert_escalate_failed_total: AtomicU64::new(0),
            alert_append_nanos: AtomicU64::new(0),
            evictor_sweeps_total: AtomicU64::new(0),
            evictor_time_evicted_total: AtomicU64::new(0),
            evictor_memory_evicted_total: AtomicU64::new(0),
            window_memory_bytes: make_window_map(),
            window_allocated_bytes: make_window_map(),
            window_fanout_queued: make_window_map(),
            window_fanout_capacity: make_window_map(),
            window_mailbox_inflight: make_window_map(),
            window_mailbox_budget: make_window_map(),
            window_capacity_bytes: make_window_map(),
            window_rows: make_window_map(),
            window_batches: make_window_map(),
            window_acked_lag: make_window_map(),
            window_append_total: make_window_map(),
            window_evict_total: make_window_map(),
            window_late_total: make_window_map(),
            receiver_decode_seconds: Histogram::from_seconds_bounds(
                DEFAULT_HISTOGRAM_BUCKETS_SECONDS,
            ),
            alert_dispatch_seconds: Histogram::from_seconds_bounds(
                DEFAULT_HISTOGRAM_BUCKETS_SECONDS,
            ),
            event_e2e_latency_seconds: Histogram::from_seconds_bounds(
                DEFAULT_HISTOGRAM_BUCKETS_SECONDS,
            ),
            rule_scan_timeout_seconds: make_rule_hist_map(),
            rule_flush_seconds: make_rule_hist_map(),
        }
    }

    pub fn inc_receiver_connection(&self) {
        self.receiver_connections_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_receiver_frame(&self, rows: usize) {
        self.receiver_frames_total.fetch_add(1, Ordering::Relaxed);
        self.receiver_rows_total
            .fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn add_receiver_source_frame(&self, source: &str, rows: usize) {
        if let Some(v) = self.receiver_source_rows_total.get(source) {
            v.fetch_add(rows as u64, Ordering::Relaxed);
        }
    }

    pub fn add_receiver_source_machine_rows(&self, source: &str, machine_id: &str, rows: usize) {
        if machine_id.is_empty() {
            return;
        }
        let mut map = self.receiver_source_machine_rows.lock().unwrap();
        let by_machine = map.entry(source.to_string()).or_default();
        let v = by_machine
            .entry(machine_id.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        v.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn inc_receiver_decode_error(&self) {
        self.receiver_decode_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_receiver_source_decode_error(&self, source: &str) {
        if let Some(v) = self.receiver_source_decode_errors_total.get(source) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn observe_receiver_decode(&self, elapsed: Duration) {
        self.receiver_decode_seconds.observe_duration(elapsed);
    }

    pub fn inc_receiver_read_error(&self) {
        self.receiver_read_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_receiver_source_read_error(&self, source: &str) {
        if let Some(v) = self.receiver_source_read_errors_total.get(source) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn add_receiver_window_miss(&self, source: &str, reason: &str, count: usize) {
        if count == 0 {
            return;
        }
        let mut map = self.receiver_window_miss_total.lock().unwrap();
        let by_reason = map.entry(source.to_string()).or_default();
        let v = by_reason
            .entry(reason.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        v.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn inc_router_route_call(&self) {
        self.router_route_calls_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Window-actor path report for one appended window batch (the actor-side
    /// equivalent of folding one `WindowRouteOutcome` into the route totals).
    pub fn report_window_append(&self, window: &str, rows: usize, late: bool) {
        if late {
            self.router_dropped_late_total
                .fetch_add(1, Ordering::Relaxed);
            self.add_window_late(window, rows as u64);
        } else {
            self.router_delivered_total.fetch_add(1, Ordering::Relaxed);
            self.add_window_append(window, rows as u64);
        }
    }

    /// Skip count for non-local subscribers, reported by the parse worker on
    /// the direct-dispatch path.
    pub fn add_router_skipped(&self, count: usize) {
        self.router_skipped_non_local_total
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn add_route_report(&self, report: &RouteReport) {
        self.router_delivered_total
            .fetch_add(report.delivered as u64, Ordering::Relaxed);
        self.router_dropped_late_total
            .fetch_add(report.dropped_late as u64, Ordering::Relaxed);
        self.router_skipped_non_local_total
            .fetch_add(report.skipped_non_local as u64, Ordering::Relaxed);
        for w in &report.per_window {
            if w.late {
                self.add_window_late(&w.window_name, w.rows as u64);
            } else {
                self.add_window_append(&w.window_name, w.rows as u64);
            }
        }
    }

    pub fn inc_route_error(&self, source: &str) {
        if let Some(v) = self.router_source_route_errors_total.get(source) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn add_rule_events(&self, rule: &str, count: usize) {
        if let Some(v) = self.rule_events_total.get(rule) {
            v.fetch_add(count as u64, Ordering::Relaxed);
        }
    }

    pub fn inc_rule_match(&self, rule: &str) {
        if let Some(v) = self.rule_matches_total.get(rule) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Adjust the `rule_instances` gauge by a signed delta.
    ///
    /// Each shard of a sharded rule reports `current_count - last_reported`
    /// so the gauge is the *sum* across shards (P2b). A single reporter
    /// (shards=1) yields the same numeric value as the old overwriting store.
    pub fn adjust_rule_instances(&self, rule: &str, delta: i64) {
        if let Some(v) = self.rule_instances.get(rule) {
            v.fetch_add(delta, Ordering::Relaxed);
        }
    }

    pub fn inc_rule_cursor_gap(&self, rule: &str, window: &str) {
        if let Some(by_window) = self.rule_cursor_gap_total.get(rule)
            && let Some(v) = by_window.get(window)
        {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 状态内存 guard 超限拒收增量（stats close 时按窗口 delta 上报; 0 增量自动 no-op）。
    pub fn inc_rule_stats_over_limit(&self, rule: &str, delta: u64) {
        if delta == 0 {
            return;
        }
        if let Some(v) = self.rule_stats_over_limit_total.get(rule) {
            v.fetch_add(delta, Ordering::Relaxed);
        }
    }

    pub fn inc_alert_emitted(&self, rule: &str, machine_id: &str, scope_key: &str) {
        self.inc_alert_emitted_total(rule);
        self.inc_alert_emitted_detail(rule, machine_id, scope_key);
    }

    /// Cheap exact total: one relaxed atomic increment, no allocation.
    pub fn inc_alert_emitted_total(&self, rule: &str) {
        if let Some(v) = self.alert_emitted_total.get(rule) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Per-machine/per-scope detail — allocation + sharded mutex per call.
    /// Callers on hot alert paths should sample this (see rule_task emit).
    pub fn inc_alert_emitted_detail(&self, rule: &str, machine_id: &str, scope_key: &str) {
        if machine_id.is_empty() && scope_key.is_empty() {
            return;
        }
        let machine = if machine_id.is_empty() {
            "-"
        } else {
            machine_id
        };
        let scope = if scope_key.is_empty() { "-" } else { scope_key };
        // Sharded by rule: only this rule's task contends on its own shard.
        let mut map = self.alert_emitted_detail_shards[alert_detail_shard(rule)]
            .lock()
            .unwrap();
        let by_machine = map.entry(rule.to_string()).or_default();
        let by_scope = by_machine.entry(machine.to_string()).or_default();
        if by_scope.len() >= ALERT_DETAIL_MAX_SCOPES_PER_RULE && !by_scope.contains_key(scope) {
            // Bounded: count only in alert_emitted_total for new scopes.
            return;
        }
        let v = by_scope
            .entry(scope.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        v.fetch_add(1, Ordering::Relaxed);
    }

    fn drain_source_machine_rows(&self) -> BTreeMap<String, BTreeMap<String, u64>> {
        let map = self.receiver_source_machine_rows.lock().unwrap();
        let mut result = BTreeMap::new();
        for (source, by_machine) in map.iter() {
            let drained: BTreeMap<String, u64> = by_machine
                .iter()
                .map(|(machine, v)| (machine.clone(), v.swap(0, Ordering::Relaxed)))
                .filter(|(_, v)| *v > 0)
                .collect();
            if !drained.is_empty() {
                result.insert(source.clone(), drained);
            }
        }
        result
    }

    fn drain_emitted_detail(&self) -> BTreeMap<String, BTreeMap<String, BTreeMap<String, u64>>> {
        let mut result = BTreeMap::new();
        for shard in &self.alert_emitted_detail_shards {
            // Swap the whole shard out so the lock is held only for the O(1)
            // `take`, never while iterating a large scope space (the previous
            // global lock was held across the full drain every interval).
            let taken = {
                let mut map = shard.lock().unwrap();
                std::mem::take(&mut *map)
            };
            for (rule, by_machine) in taken {
                let mut machine_map = BTreeMap::new();
                for (machine, by_scope) in by_machine {
                    let drained: BTreeMap<String, u64> = by_scope
                        .into_iter()
                        .map(|(scope, v)| (scope, v.into_inner()))
                        .filter(|(_, v)| *v > 0)
                        .collect();
                    if !drained.is_empty() {
                        machine_map.insert(machine, drained);
                    }
                }
                if !machine_map.is_empty() {
                    result.insert(rule, machine_map);
                }
            }
        }
        result
    }

    fn drain_receiver_window_misses(&self) -> BTreeMap<String, BTreeMap<String, u64>> {
        let map = self.receiver_window_miss_total.lock().unwrap();
        let mut result = BTreeMap::new();
        for (source, by_reason) in map.iter() {
            let drained: BTreeMap<String, u64> = by_reason
                .iter()
                .map(|(reason, v)| (reason.clone(), v.swap(0, Ordering::Relaxed)))
                .filter(|(_, v)| *v > 0)
                .collect();
            if !drained.is_empty() {
                result.insert(source.clone(), drained);
            }
        }
        result
    }

    pub fn inc_alert_channel_send_failed(&self) {
        self.alert_channel_send_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_sink_dispatch_failed(&self) {
        self.alert_sink_dispatch_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_alert_append_failed(&self) {
        self.alert_append_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records delivered to a yield target that has no matching sink and were
    /// dropped (silent degradation made explicit, ⑤).
    pub fn add_alert_no_sink_records(&self, records: u64) {
        self.alert_no_sink_records_total
            .fetch_add(records, Ordering::Relaxed);
    }

    /// Records still buffered in a sink channel when the shutdown drain budget
    /// ran out and were dropped.
    pub fn add_sink_drain_dropped_records(&self, records: u64) {
        self.alert_drain_dropped_records_total
            .fetch_add(records, Ordering::Relaxed);
    }

    /// Escalation of a failed dispatch batch to the error sinks failed.
    pub fn inc_alert_escalate_failed(&self) {
        self.alert_escalate_failed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Nanoseconds spent on the record→列 append（`AlertColumnBuilder::append_record`）
    /// in the rule workers' emit path（worker 侧输出构建; 与 sink 侧序列化区分,
    /// per-run counter, drained each export interval）。
    pub fn add_alert_append_nanos(&self, nanos: u64) {
        self.alert_append_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    pub fn inc_alert_dispatch(&self) {
        self.alert_dispatch_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_alert_dispatch(&self, elapsed: Duration) {
        self.alert_dispatch_seconds.observe_duration(elapsed);
    }

    pub fn observe_rule_scan_timeout(&self, rule: &str, elapsed: Duration) {
        if let Some(hist) = self.rule_scan_timeout_seconds.get(rule) {
            hist.observe_duration(elapsed);
        }
    }

    pub fn inc_alert_channel_full(&self) {
        self.alert_channel_full_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn set_alert_channel_depth(&self, depth: u64) {
        self.alert_channel_depth.store(depth, Ordering::Relaxed);
    }
    pub fn observe_event_e2e_latency(&self, elapsed: Duration) {
        self.event_e2e_latency_seconds.observe_duration(elapsed);
    }
    pub fn add_window_append(&self, window: &str, count: u64) {
        if let Some(c) = self.window_append_total.get(window) {
            c.fetch_add(count, Ordering::Relaxed);
        }
    }
    pub fn add_window_evict(&self, window: &str, count: u64) {
        if let Some(c) = self.window_evict_total.get(window) {
            c.fetch_add(count, Ordering::Relaxed);
        }
    }
    pub fn add_window_late(&self, window: &str, count: u64) {
        if let Some(c) = self.window_late_total.get(window) {
            c.fetch_add(count, Ordering::Relaxed);
        }
    }
    pub fn observe_rule_flush(&self, rule: &str, elapsed: Duration) {
        if let Some(hist) = self.rule_flush_seconds.get(rule) {
            hist.observe_duration(elapsed);
        }
    }
    pub(crate) fn snapshot(&self) -> MetricsSnapshot {
        let source_types = self.source_types.clone();
        MetricsSnapshot {
            source_types,
            receiver_connections: self.drain_counter(&self.receiver_connections_total),
            receiver_frames: self.drain_counter(&self.receiver_frames_total),
            receiver_source_rows: self.drain_map(&self.receiver_source_rows_total),
            receiver_source_machine_rows: self.drain_source_machine_rows(),
            receiver_source_decode_errors: self
                .drain_map(&self.receiver_source_decode_errors_total),
            receiver_source_read_errors: self.drain_map(&self.receiver_source_read_errors_total),
            receiver_window_misses: self.drain_receiver_window_misses(),
            router_route_calls: self.drain_counter(&self.router_route_calls_total),
            router_delivered: self.drain_counter(&self.router_delivered_total),
            router_dropped_late: self.drain_counter(&self.router_dropped_late_total),
            router_skipped_non_local: self.drain_counter(&self.router_skipped_non_local_total),
            router_source_route_errors: self.drain_map(&self.router_source_route_errors_total),
            rule_events: self.drain_map(&self.rule_events_total),
            rule_matches: self.drain_map(&self.rule_matches_total),
            rule_instances: self.read_gauge_map(&self.rule_instances),
            rule_cursor_gaps: self.drain_gap_map(&self.rule_cursor_gap_total),
            rule_stats_over_limit: self.drain_map(&self.rule_stats_over_limit_total),
            alert_emitted: self.drain_map(&self.alert_emitted_total),
            alert_emitted_detail: self.drain_emitted_detail(),
            alert_channel_send_failed: self.drain_counter(&self.alert_channel_send_failed_total),
            alert_sink_dispatch_failed: self.drain_counter(&self.alert_sink_dispatch_failed_total),
            alert_channel_full: self.drain_counter(&self.alert_channel_full_total),
            alert_channel_depth: self.alert_channel_depth.load(Ordering::Relaxed),
            alert_append_failed: self.drain_counter(&self.alert_append_failed_total),
            alert_dispatch: self.drain_counter(&self.alert_dispatch_total),
            alert_no_sink_records: self.drain_counter(&self.alert_no_sink_records_total),
            alert_drain_dropped_records: self
                .drain_counter(&self.alert_drain_dropped_records_total),
            alert_escalate_failed: self.drain_counter(&self.alert_escalate_failed_total),
            alert_append_nanos: self.drain_counter(&self.alert_append_nanos),
            evictor_sweeps: self.drain_counter(&self.evictor_sweeps_total),
            evictor_time_evicted: self.drain_counter(&self.evictor_time_evicted_total),
            evictor_memory_evicted: self.drain_counter(&self.evictor_memory_evicted_total),
            window_memory_bytes: self.read_map(&self.window_memory_bytes),
            window_allocated_bytes: self.read_map(&self.window_allocated_bytes),
            window_fanout_queued: self.read_map(&self.window_fanout_queued),
            window_fanout_capacity: self.read_map(&self.window_fanout_capacity),
            window_mailbox_inflight: self.read_map(&self.window_mailbox_inflight),
            window_mailbox_budget: self.read_map(&self.window_mailbox_budget),
            window_capacity_bytes: self.read_map(&self.window_capacity_bytes),
            window_rows: self.read_map(&self.window_rows),
            window_batches: self.read_map(&self.window_batches),
            window_acked_lag: self.read_map(&self.window_acked_lag),
            window_append: self.drain_map(&self.window_append_total),
            window_evict: self.drain_map(&self.window_evict_total),
            window_late: self.drain_map(&self.window_late_total),
            receiver_decode_latency: self.receiver_decode_seconds.snapshot(),
            alert_dispatch_latency: self.alert_dispatch_seconds.snapshot(),
            event_e2e_latency: self.event_e2e_latency_seconds.snapshot(),
            rule_scan_timeout: self
                .rule_scan_timeout_seconds
                .iter()
                .map(|(k, v)| (k.clone(), v.snapshot()))
                .collect(),
            rule_flush: self
                .rule_flush_seconds
                .iter()
                .map(|(k, v)| (k.clone(), v.snapshot()))
                .collect(),
            // 分配器读数（provider 未装入则 None）。
            alloc: alloc_stats::read(),
        }
    }
    fn drain_counter(&self, c: &AtomicU64) -> u64 {
        c.swap(0, Ordering::Relaxed)
    }
    fn drain_map(&self, m: &BTreeMap<String, AtomicU64>) -> BTreeMap<String, u64> {
        m.iter()
            .map(|(k, v)| (k.clone(), v.swap(0, Ordering::Relaxed)))
            .collect()
    }
    fn drain_gap_map(
        &self,
        m: &BTreeMap<String, BTreeMap<String, AtomicU64>>,
    ) -> BTreeMap<String, BTreeMap<String, u64>> {
        m.iter()
            .map(|(rule, windows)| {
                (
                    rule.clone(),
                    windows
                        .iter()
                        .map(|(w, v)| (w.clone(), v.swap(0, Ordering::Relaxed)))
                        .collect(),
                )
            })
            .collect()
    }
    fn read_map(&self, m: &BTreeMap<String, AtomicU64>) -> BTreeMap<String, u64> {
        m.iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect()
    }

    /// Read the signed `rule_instances` gauge, clamping negatives to zero.
    fn read_gauge_map(&self, m: &BTreeMap<String, AtomicI64>) -> BTreeMap<String, u64> {
        m.iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed).max(0) as u64))
            .collect()
    }

    pub fn add_evict_report(&self, report: &EvictReport) {
        self.evictor_sweeps_total.fetch_add(1, Ordering::Relaxed);
        self.evictor_time_evicted_total
            .fetch_add(report.batches_time_evicted as u64, Ordering::Relaxed);
        self.evictor_memory_evicted_total
            .fetch_add(report.batches_memory_evicted as u64, Ordering::Relaxed);
        for w in &report.per_window_evicted {
            self.add_window_evict(&w.window_name, w.time_evicted as u64);
        }
    }

    pub(crate) fn summary_line(&self) -> String {
        format!(
            "rx_rows={} routed={} dropped_late={} matches={} alerts={} window_bytes={}",
            self.receiver_rows_total.load(Ordering::Relaxed),
            self.router_delivered_total.load(Ordering::Relaxed),
            self.router_dropped_late_total.load(Ordering::Relaxed),
            self.total_rule_matches(),
            self.alert_emitted_total
                .values()
                .map(|v| v.load(Ordering::Relaxed))
                .sum::<u64>(),
            self.total_window_bytes()
        )
    }
}

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
