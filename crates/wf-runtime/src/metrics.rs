use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::error::RuntimeResult;
use tokio_util::sync::CancellationToken;
use wf_config::MetricsConfig;
use wf_engine::window::{EvictReport, RouteReport, Router};

type AlertDetailCounts = BTreeMap<String, AtomicU64>;
type AlertDetailByMachine = BTreeMap<String, AlertDetailCounts>;
type AlertDetailByRule = BTreeMap<String, AlertDetailByMachine>;

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
    router_route_calls: u64,
    router_delivered: u64,
    router_dropped_late: u64,
    router_skipped_non_local: u64,
    router_source_route_errors: BTreeMap<String, u64>,
    rule_events: BTreeMap<String, u64>,
    rule_matches: BTreeMap<String, u64>,
    rule_instances: BTreeMap<String, u64>,
    rule_cursor_gaps: BTreeMap<String, BTreeMap<String, u64>>,
    alert_emitted: BTreeMap<String, u64>,
    alert_emitted_detail: BTreeMap<String, BTreeMap<String, BTreeMap<String, u64>>>,
    alert_channel_send_failed: u64,
    alert_sink_dispatch_failed: u64,
    alert_channel_full: u64,
    alert_channel_depth: u64,
    alert_serialize_failed: u64,
    alert_dispatch: u64,
    evictor_sweeps: u64,
    evictor_time_evicted: u64,
    evictor_memory_evicted: u64,
    window_memory_bytes: BTreeMap<String, u64>,
    window_capacity_bytes: BTreeMap<String, u64>,
    window_rows: BTreeMap<String, u64>,
    window_batches: BTreeMap<String, u64>,
    window_append: BTreeMap<String, u64>,
    window_evict: BTreeMap<String, u64>,
    window_late: BTreeMap<String, u64>,
    receiver_decode_latency: HistogramSnapshot,
    alert_dispatch_latency: HistogramSnapshot,
    event_e2e_latency: HistogramSnapshot,
    rule_scan_timeout: BTreeMap<String, HistogramSnapshot>,
    rule_flush: BTreeMap<String, HistogramSnapshot>,
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
            "serialize_failed_total",
            "",
            self.alert_serialize_failed,
        ));
        out.push(metric("alert", "dispatch_total", "", self.alert_dispatch));
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
        for (rule, v) in &self.alert_emitted {
            if !self.alert_emitted_detail.contains_key(rule) {
                out.push(metric("alert", "emitted_total", rule, *v));
            }
        }
        for (rule, by_machine) in &self.alert_emitted_detail {
            for (machine, by_scope) in by_machine {
                for (scope, v) in by_scope {
                    out.push(MetricsRecord {
                        fields: vec![
                            ("stage".into(), "alert".into()),
                            ("name".into(), "emitted_total".into()),
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
        for (window, v) in &self.window_capacity_bytes {
            out.push(metric("window", "window_capacity_bytes", window, *v));
        }
        for (window, v) in &self.window_rows {
            out.push(metric("window", "rows", window, *v));
        }
        for (window, v) in &self.window_batches {
            out.push(metric("window", "batches", window, *v));
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
struct IntervalRates {
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
struct IntervalSnapshot {
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
struct TotalCounts {
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
struct RunSummary {
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

    router_route_calls_total: AtomicU64,
    router_delivered_total: AtomicU64,
    router_dropped_late_total: AtomicU64,
    router_skipped_non_local_total: AtomicU64,
    router_source_route_errors_total: BTreeMap<String, AtomicU64>,

    rule_events_total: BTreeMap<String, AtomicU64>,
    rule_matches_total: BTreeMap<String, AtomicU64>,
    rule_instances: BTreeMap<String, AtomicU64>,
    rule_cursor_gap_total: BTreeMap<String, BTreeMap<String, AtomicU64>>,

    alert_emitted_total: BTreeMap<String, AtomicU64>,
    alert_emitted_detail_total: Mutex<AlertDetailByRule>,
    alert_channel_send_failed_total: AtomicU64,
    alert_sink_dispatch_failed_total: AtomicU64,
    alert_channel_full_total: AtomicU64,
    alert_channel_depth: AtomicU64,
    alert_serialize_failed_total: AtomicU64,
    alert_dispatch_total: AtomicU64,

    evictor_sweeps_total: AtomicU64,
    evictor_time_evicted_total: AtomicU64,
    evictor_memory_evicted_total: AtomicU64,

    window_memory_bytes: BTreeMap<String, AtomicU64>,
    window_capacity_bytes: BTreeMap<String, AtomicU64>,
    window_rows: BTreeMap<String, AtomicU64>,
    window_batches: BTreeMap<String, AtomicU64>,
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

    fn interval_snapshot(&self, at: Instant) -> IntervalSnapshot {
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

    fn interval_rates(
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

    fn interval_table(&self, rates: IntervalRates) -> String {
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
            router_route_calls_total: AtomicU64::new(0),
            router_delivered_total: AtomicU64::new(0),
            router_dropped_late_total: AtomicU64::new(0),
            router_skipped_non_local_total: AtomicU64::new(0),
            router_source_route_errors_total: make_source_map(),
            rule_events_total: make_rule_map(),
            rule_matches_total: make_rule_map(),
            rule_instances: make_rule_map(),
            rule_cursor_gap_total: gap_map,
            alert_emitted_total: make_rule_map(),
            alert_emitted_detail_total: Mutex::new(BTreeMap::new()),
            alert_channel_send_failed_total: AtomicU64::new(0),
            alert_sink_dispatch_failed_total: AtomicU64::new(0),
            alert_channel_full_total: AtomicU64::new(0),
            alert_channel_depth: AtomicU64::new(0),
            alert_serialize_failed_total: AtomicU64::new(0),
            alert_dispatch_total: AtomicU64::new(0),
            evictor_sweeps_total: AtomicU64::new(0),
            evictor_time_evicted_total: AtomicU64::new(0),
            evictor_memory_evicted_total: AtomicU64::new(0),
            window_memory_bytes: make_window_map(),
            window_capacity_bytes: make_window_map(),
            window_rows: make_window_map(),
            window_batches: make_window_map(),
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

    pub fn inc_router_route_call(&self) {
        self.router_route_calls_total
            .fetch_add(1, Ordering::Relaxed);
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

    pub fn set_rule_instances(&self, rule: &str, count: usize) {
        if let Some(v) = self.rule_instances.get(rule) {
            v.store(count as u64, Ordering::Relaxed);
        }
    }

    pub fn inc_rule_cursor_gap(&self, rule: &str, window: &str) {
        if let Some(by_window) = self.rule_cursor_gap_total.get(rule)
            && let Some(v) = by_window.get(window)
        {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn inc_alert_emitted(&self, rule: &str, machine_id: &str, scope_key: &str) {
        if let Some(v) = self.alert_emitted_total.get(rule) {
            v.fetch_add(1, Ordering::Relaxed);
        }
        if machine_id.is_empty() && scope_key.is_empty() {
            return;
        }
        let machine = if machine_id.is_empty() {
            "-"
        } else {
            machine_id
        };
        let scope = if scope_key.is_empty() { "-" } else { scope_key };
        let mut map = self.alert_emitted_detail_total.lock().unwrap();
        let by_machine = map.entry(rule.to_string()).or_default();
        let by_scope = by_machine.entry(machine.to_string()).or_default();
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
        let map = self.alert_emitted_detail_total.lock().unwrap();
        let mut result = BTreeMap::new();
        for (rule, by_machine) in map.iter() {
            let mut machine_map = BTreeMap::new();
            for (machine, by_scope) in by_machine.iter() {
                let drained: BTreeMap<String, u64> = by_scope
                    .iter()
                    .map(|(scope, v)| (scope.clone(), v.swap(0, Ordering::Relaxed)))
                    .filter(|(_, v)| *v > 0)
                    .collect();
                if !drained.is_empty() {
                    machine_map.insert(machine.clone(), drained);
                }
            }
            if !machine_map.is_empty() {
                result.insert(rule.clone(), machine_map);
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

    pub fn inc_alert_serialize_failed(&self) {
        self.alert_serialize_failed_total
            .fetch_add(1, Ordering::Relaxed);
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
            router_route_calls: self.drain_counter(&self.router_route_calls_total),
            router_delivered: self.drain_counter(&self.router_delivered_total),
            router_dropped_late: self.drain_counter(&self.router_dropped_late_total),
            router_skipped_non_local: self.drain_counter(&self.router_skipped_non_local_total),
            router_source_route_errors: self.drain_map(&self.router_source_route_errors_total),
            rule_events: self.drain_map(&self.rule_events_total),
            rule_matches: self.drain_map(&self.rule_matches_total),
            rule_instances: self.read_map(&self.rule_instances),
            rule_cursor_gaps: self.drain_gap_map(&self.rule_cursor_gap_total),
            alert_emitted: self.drain_map(&self.alert_emitted_total),
            alert_emitted_detail: self.drain_emitted_detail(),
            alert_channel_send_failed: self.drain_counter(&self.alert_channel_send_failed_total),
            alert_sink_dispatch_failed: self.drain_counter(&self.alert_sink_dispatch_failed_total),
            alert_channel_full: self.drain_counter(&self.alert_channel_full_total),
            alert_channel_depth: self.alert_channel_depth.load(Ordering::Relaxed),
            alert_serialize_failed: self.drain_counter(&self.alert_serialize_failed_total),
            alert_dispatch: self.drain_counter(&self.alert_dispatch_total),
            evictor_sweeps: self.drain_counter(&self.evictor_sweeps_total),
            evictor_time_evicted: self.drain_counter(&self.evictor_time_evicted_total),
            evictor_memory_evicted: self.drain_counter(&self.evictor_memory_evicted_total),
            window_memory_bytes: self.read_map(&self.window_memory_bytes),
            window_capacity_bytes: self.read_map(&self.window_capacity_bytes),
            window_rows: self.read_map(&self.window_rows),
            window_batches: self.read_map(&self.window_batches),
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

    /// Periodically sample expensive window gauges to keep scrape path light.
    pub fn sample_windows(&self, router: &Router) {
        for window_name in router.registry().window_names() {
            if let Some(win_lock) = router.registry().get_window(&window_name) {
                let win = win_lock.read().expect("window lock poisoned");
                if let Some(v) = self.window_memory_bytes.get(&window_name) {
                    v.store(win.memory_usage() as u64, Ordering::Relaxed);
                }
                if let Some(v) = self.window_capacity_bytes.get(&window_name) {
                    v.store(win.max_window_bytes() as u64, Ordering::Relaxed);
                }
                if let Some(v) = self.window_rows.get(&window_name) {
                    v.store(win.total_rows() as u64, Ordering::Relaxed);
                }
                if let Some(v) = self.window_batches.get(&window_name) {
                    v.store(win.batch_count() as u64, Ordering::Relaxed);
                }
            }
        }
    }

    fn summary_line(&self) -> String {
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

pub async fn run_metrics_task(
    metrics: Arc<RuntimeMetrics>,
    config: MetricsConfig,
    router: Arc<Router>,
    cancel: CancellationToken,
    mon_send: Option<MonSend>,
) -> RuntimeResult<()> {
    wf_info!(
        sys,
        listen = %config.prometheus_listen,
        interval = %config.report_interval,
        "metrics exporter started"
    );

    metrics.sample_windows(&router);
    let mut tick = tokio::time::interval(config.report_interval.as_duration());
    tick.tick().await;
    let task_started = Instant::now();
    let mut prev = metrics.interval_snapshot(Instant::now());
    let start = prev;
    let mut run_summary = RunSummary::default();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {
                metrics.sample_windows(&router);
                wf_info!(res, summary = %metrics.summary_line(), "metrics snapshot");
                let curr = metrics.interval_snapshot(Instant::now());
                if let Some(rates) = metrics.interval_rates(prev, curr) {
                    run_summary.observe(rates);
                    wf_info!(res, "{}", metrics.interval_table(rates));
                }
                prev = curr;

                if let Some(ref sender) = mon_send {
                    let snap = metrics.snapshot();
                    let records = snap.to_records();
                    if sender.try_send(records).is_err() {
                        wf_debug!(sys, "monitor channel full, dropping metrics snapshot");
                    }
                }
            }
        }
    }

    // Include the last partial interval before shutdown in final stats.
    metrics.sample_windows(&router);
    let final_snap = metrics.interval_snapshot(Instant::now());
    if let Some(rates) = metrics.interval_rates(prev, final_snap) {
        run_summary.observe(rates);
    }
    let totals = TotalCounts {
        rows: final_snap.rx_rows.saturating_sub(start.rx_rows),
        late: final_snap.dropped_late.saturating_sub(start.dropped_late),
        rules: final_snap.rule_matches.saturating_sub(start.rule_matches),
        out: final_snap
            .alert_dispatch
            .saturating_sub(start.alert_dispatch),
        sm_delta: final_snap.rule_instances as i64 - start.rule_instances as i64,
    };

    if let Some(table) = run_summary.table(Some(totals)) {
        wf_info!(
            res,
            runtime = ?task_started.elapsed(),
            intervals = run_summary.interval_count,
            "{}",
            table
        );
    }
    Ok(())
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_summary_table_includes_totals_when_provided() {
        let mut summary = RunSummary::default();
        summary.observe(IntervalRates {
            row_s: 100.0,
            late_s: 2.0,
            rules_s: 10.0,
            sm_s: 1.5,
            out_s: 4.0,
            memory_bytes: 1024,
        });
        let table = summary
            .table(Some(TotalCounts {
                rows: 500,
                late: 10,
                rules: 50,
                out: 20,
                sm_delta: -3,
            }))
            .expect("summary table should render");
        assert!(table.contains("| avg     |"));
        assert!(table.contains("| max     |"));
        assert!(table.contains("| total   | rows"));
        assert!(table.contains("| count   |        500"));
        assert!(table.contains("        -3"));
    }

    // -- percentile -----------------------------------------------------------

    #[test]
    fn percentile_p50_returns_median() {
        let hist = Histogram::from_seconds_bounds(&[0.001, 0.005, 0.01]);
        hist.observe_duration(Duration::from_micros(500)); // 0.0005s → bucket 0
        hist.observe_duration(Duration::from_micros(3000)); // 0.003s  → bucket 1
        let snap = hist.snapshot();
        let p50 = percentile(&snap, 0.50);
        assert!(p50 > 0.0001 && p50 < 0.005);
    }

    #[test]
    fn percentile_p99_returns_high_end() {
        let hist = Histogram::from_seconds_bounds(&[0.001, 0.005, 0.01]);
        // 85 fast, 15 slow → p99 should reach the slow bucket
        for _ in 0..85 {
            hist.observe_duration(Duration::from_micros(500));
        }
        for _ in 0..15 {
            hist.observe_duration(Duration::from_millis(10));
        }
        let snap = hist.snapshot();
        let p99 = percentile(&snap, 0.99);
        assert!(p99 >= 0.005); // 15% in top bucket pulls p99 up
    }

    #[test]
    fn percentile_empty_returns_zero() {
        let hist = Histogram::from_seconds_bounds(&[0.001]);
        let snap = hist.snapshot();
        assert_eq!(percentile(&snap, 0.50), 0.0);
        assert_eq!(percentile(&snap, 0.99), 0.0);
    }

    // -- snapshot drain -------------------------------------------------------

    #[test]
    fn snapshot_drains_counters_preserves_gauges() {
        let metrics = RuntimeMetrics::new(
            &["r1".to_string()],
            &["w1".to_string()],
            &[],
            BTreeMap::new(),
        );
        metrics.inc_receiver_connection();
        metrics.inc_receiver_connection();
        metrics.inc_rule_match("r1");
        assert_eq!(metrics.snapshot().receiver_connections, 2);
        // After drain, counter resets to 0
        assert_eq!(metrics.snapshot().receiver_connections, 0);
    }

    #[test]
    fn snapshot_window_append_resets() {
        let metrics = RuntimeMetrics::new(
            &["r1".to_string()],
            &["w1".to_string()],
            &[],
            BTreeMap::new(),
        );
        metrics.add_window_append("w1", 100);
        metrics.add_window_append("w1", 200);
        assert_eq!(metrics.snapshot().window_append.get("w1"), Some(&300));
        assert_eq!(metrics.snapshot().window_append.get("w1"), Some(&0));
    }

    // -- per-window route counters --------------------------------------------

    #[test]
    fn add_route_report_tracks_per_window_append() {
        use wf_engine::window::WindowRouteOutcome;
        let metrics = RuntimeMetrics::new(
            &["r1".to_string()],
            &["win_a".to_string()],
            &[],
            BTreeMap::new(),
        );
        let report = RouteReport {
            delivered: 1,
            dropped_late: 0,
            skipped_non_local: 0,
            per_window: vec![WindowRouteOutcome {
                window_name: "win_a".into(),
                rows: 42,
                late: false,
            }],
        };
        metrics.add_route_report(&report);
        assert_eq!(metrics.snapshot().window_append.get("win_a"), Some(&42));
    }

    #[test]
    fn add_route_report_tracks_per_window_late() {
        use wf_engine::window::WindowRouteOutcome;
        let metrics = RuntimeMetrics::new(
            &["r1".to_string()],
            &["win_a".to_string()],
            &[],
            BTreeMap::new(),
        );
        let report = RouteReport {
            delivered: 0,
            dropped_late: 1,
            skipped_non_local: 0,
            per_window: vec![WindowRouteOutcome {
                window_name: "win_a".into(),
                rows: 10,
                late: true,
            }],
        };
        metrics.add_route_report(&report);
        assert_eq!(metrics.snapshot().window_late.get("win_a"), Some(&10));
    }

    // -- per-window evict counters --------------------------------------------

    #[test]
    fn add_evict_report_tracks_per_window_eviction() {
        use wf_engine::window::WindowEvictCount;
        let metrics = RuntimeMetrics::new(
            &["r1".to_string()],
            &["win_a".to_string()],
            &[],
            BTreeMap::new(),
        );
        let report = EvictReport {
            windows_scanned: 1,
            batches_time_evicted: 2,
            batches_memory_evicted: 1,
            per_window_evicted: vec![WindowEvictCount {
                window_name: "win_a".into(),
                time_evicted: 2,
            }],
        };
        metrics.add_evict_report(&report);
        assert_eq!(metrics.snapshot().window_evict.get("win_a"), Some(&2));
    }

    // -- channel backpressure -------------------------------------------------

    #[test]
    fn alert_channel_depth_reads_current() {
        let metrics = RuntimeMetrics::new(
            &["r1".to_string()],
            &["w1".to_string()],
            &[],
            BTreeMap::new(),
        );
        metrics.set_alert_channel_depth(3);
        assert_eq!(metrics.snapshot().alert_channel_depth, 3);
        metrics.set_alert_channel_depth(0);
        assert_eq!(metrics.snapshot().alert_channel_depth, 0);
    }

    #[test]
    fn alert_channel_full_increments() {
        let metrics = RuntimeMetrics::new(
            &["r1".to_string()],
            &["w1".to_string()],
            &[],
            BTreeMap::new(),
        );
        metrics.inc_alert_channel_full();
        metrics.inc_alert_channel_full();
        assert_eq!(metrics.snapshot().alert_channel_full, 2);
        assert_eq!(metrics.snapshot().alert_channel_full, 0);
    }

    // -- E2E latency ----------------------------------------------------------

    #[test]
    fn observe_event_e2e_latency_records() {
        let metrics = RuntimeMetrics::new(
            &["r1".to_string()],
            &["w1".to_string()],
            &[],
            BTreeMap::new(),
        );
        metrics.observe_event_e2e_latency(Duration::from_secs(1));
        let snap = metrics.snapshot();
        // Should have one observation in the 1s bucket
        let total: u64 = snap.event_e2e_latency.bucket_counts.iter().sum();
        assert_eq!(total, 1);
    }

    // -- to_records -----------------------------------------------------------

    #[test]
    fn to_records_produces_expected_structure() {
        let metrics = RuntimeMetrics::new(
            &["r1".to_string()],
            &["w1".to_string()],
            &[],
            BTreeMap::new(),
        );
        metrics.inc_rule_match("r1");
        metrics.add_window_append("w1", 100);
        let snap = metrics.snapshot();
        let records = snap.to_records();
        assert!(!records.is_empty());
        // Each record should have stage, name, value fields
        for r in &records {
            let keys: Vec<&str> = r.fields.iter().map(|(k, _)| k.as_str()).collect();
            assert!(keys.contains(&"stage"));
            assert!(keys.contains(&"name"));
            assert!(keys.contains(&"value"));
        }
    }

    #[test]
    fn receiver_source_machine_rows() {
        let m = RuntimeMetrics::new(&[], &[], &["s1".to_string()], BTreeMap::new());
        m.add_receiver_source_machine_rows("s1", "10.0.0.1", 100);
        m.add_receiver_source_machine_rows("s1", "10.0.0.1", 50);
        let snap = m.snapshot();
        assert_eq!(
            snap.receiver_source_machine_rows
                .get("s1")
                .unwrap()
                .get("10.0.0.1"),
            Some(&150)
        );
        // drain clears
        assert!(m.snapshot().receiver_source_machine_rows.is_empty());
        // empty machine_id is skipped
        m.add_receiver_source_machine_rows("s1", "", 100);
        assert!(m.snapshot().receiver_source_machine_rows.is_empty());
    }

    #[test]
    fn alert_emitted_detail() {
        let m = RuntimeMetrics::new(&["r1".to_string()], &[], &[], BTreeMap::new());
        m.inc_alert_emitted("r1", "10.0.0.1", "sip=10.0.0.1");
        m.inc_alert_emitted("r1", "10.0.0.1", "sip=10.0.0.1");
        let snap = m.snapshot();
        let detail = snap
            .alert_emitted_detail
            .get("r1")
            .unwrap()
            .get("10.0.0.1")
            .unwrap();
        assert_eq!(detail.get("sip=10.0.0.1"), Some(&2));
        // drain clears
        assert!(m.snapshot().alert_emitted_detail.is_empty());
        // empty machine_id → "-"
        m.inc_alert_emitted("r1", "", "key=val");
        assert!(
            m.snapshot()
                .alert_emitted_detail
                .get("r1")
                .unwrap()
                .contains_key("-")
        );
    }

    #[test]
    fn alert_counters() {
        let m = RuntimeMetrics::new(&["r1".to_string()], &[], &[], BTreeMap::new());
        m.inc_sink_dispatch_failed();
        m.inc_sink_dispatch_failed();
        assert_eq!(m.snapshot().alert_sink_dispatch_failed, 2);
        assert_eq!(m.snapshot().alert_sink_dispatch_failed, 0);
        // plain emitted_total when no detail
        m.inc_alert_emitted("r1", "", "");
        let records = m.snapshot().to_records();
        let emitted: Vec<_> = records
            .iter()
            .filter(|r| {
                r.fields
                    .iter()
                    .any(|(k, v)| k == "name" && v == "emitted_total")
            })
            .collect();
        assert_eq!(emitted.len(), 1);
    }
}
