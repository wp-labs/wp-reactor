use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use wf_config::MetricsConfig;
use wf_core::window::{EvictReport, RouteReport, Router};

const DEFAULT_HISTOGRAM_BUCKETS_SECONDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0,
];

/// Lock-free histogram with fixed buckets.
///
/// Each observation increments exactly one bucket (non-cumulative storage).
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

struct HistogramSnapshot {
    upper_bounds_nanos: Vec<u64>,
    bucket_counts: Vec<u64>,
    sum_seconds: f64,
}

/// Shared runtime metrics store.
///
/// Counters are lock-free atomics. Label sets (`rule`, `window`) are fixed at
/// startup to keep hot-path updates allocation-free.
pub struct RuntimeMetrics {
    receiver_connections_total: AtomicU64,
    receiver_frames_total: AtomicU64,
    receiver_rows_total: AtomicU64,
    receiver_decode_errors_total: AtomicU64,
    receiver_read_errors_total: AtomicU64,

    router_route_calls_total: AtomicU64,
    router_delivered_total: AtomicU64,
    router_dropped_late_total: AtomicU64,
    router_skipped_non_local_total: AtomicU64,
    router_route_errors_total: AtomicU64,

    rule_events_total: BTreeMap<String, AtomicU64>,
    rule_matches_total: BTreeMap<String, AtomicU64>,
    rule_instances: BTreeMap<String, AtomicU64>,
    rule_cursor_gap_total: BTreeMap<String, BTreeMap<String, AtomicU64>>,

    alert_emitted_total: BTreeMap<String, AtomicU64>,
    alert_channel_send_failed_total: AtomicU64,
    alert_serialize_failed_total: AtomicU64,
    alert_dispatch_total: AtomicU64,

    evictor_sweeps_total: AtomicU64,
    evictor_time_evicted_total: AtomicU64,
    evictor_memory_evicted_total: AtomicU64,

    window_memory_bytes: BTreeMap<String, AtomicU64>,
    window_rows: BTreeMap<String, AtomicU64>,
    window_batches: BTreeMap<String, AtomicU64>,

    receiver_decode_seconds: Histogram,
    alert_dispatch_seconds: Histogram,
    rule_scan_timeout_seconds: BTreeMap<String, Histogram>,
    rule_flush_seconds: BTreeMap<String, Histogram>,
}

impl RuntimeMetrics {
    pub fn new(rule_names: &[String], window_names: &[String]) -> Self {
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
            router_route_calls_total: AtomicU64::new(0),
            router_delivered_total: AtomicU64::new(0),
            router_dropped_late_total: AtomicU64::new(0),
            router_skipped_non_local_total: AtomicU64::new(0),
            router_route_errors_total: AtomicU64::new(0),
            rule_events_total: make_rule_map(),
            rule_matches_total: make_rule_map(),
            rule_instances: make_rule_map(),
            rule_cursor_gap_total: gap_map,
            alert_emitted_total: make_rule_map(),
            alert_channel_send_failed_total: AtomicU64::new(0),
            alert_serialize_failed_total: AtomicU64::new(0),
            alert_dispatch_total: AtomicU64::new(0),
            evictor_sweeps_total: AtomicU64::new(0),
            evictor_time_evicted_total: AtomicU64::new(0),
            evictor_memory_evicted_total: AtomicU64::new(0),
            window_memory_bytes: make_window_map(),
            window_rows: make_window_map(),
            window_batches: make_window_map(),
            receiver_decode_seconds: Histogram::from_seconds_bounds(
                DEFAULT_HISTOGRAM_BUCKETS_SECONDS,
            ),
            alert_dispatch_seconds: Histogram::from_seconds_bounds(
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

    pub fn inc_receiver_decode_error(&self) {
        self.receiver_decode_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_receiver_decode(&self, elapsed: Duration) {
        self.receiver_decode_seconds.observe_duration(elapsed);
    }

    pub fn inc_receiver_read_error(&self) {
        self.receiver_read_errors_total
            .fetch_add(1, Ordering::Relaxed);
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
    }

    pub fn inc_route_error(&self) {
        self.router_route_errors_total
            .fetch_add(1, Ordering::Relaxed);
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

    pub fn inc_alert_emitted(&self, rule: &str) {
        if let Some(v) = self.alert_emitted_total.get(rule) {
            v.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn inc_alert_channel_send_failed(&self) {
        self.alert_channel_send_failed_total
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

    pub fn observe_rule_flush(&self, rule: &str, elapsed: Duration) {
        if let Some(hist) = self.rule_flush_seconds.get(rule) {
            hist.observe_duration(elapsed);
        }
    }

    pub fn add_evict_report(&self, report: &EvictReport) {
        self.evictor_sweeps_total.fetch_add(1, Ordering::Relaxed);
        self.evictor_time_evicted_total
            .fetch_add(report.batches_time_evicted as u64, Ordering::Relaxed);
        self.evictor_memory_evicted_total
            .fetch_add(report.batches_memory_evicted as u64, Ordering::Relaxed);
    }

    /// Periodically sample expensive window gauges to keep scrape path light.
    pub fn sample_windows(&self, router: &Router) {
        for window_name in router.registry().window_names() {
            if let Some(win_lock) = router.registry().get_window(window_name) {
                let win = win_lock.read().expect("window lock poisoned");
                if let Some(v) = self.window_memory_bytes.get(window_name) {
                    v.store(win.memory_usage() as u64, Ordering::Relaxed);
                }
                if let Some(v) = self.window_rows.get(window_name) {
                    v.store(win.total_rows() as u64, Ordering::Relaxed);
                }
                if let Some(v) = self.window_batches.get(window_name) {
                    v.store(win.batch_count() as u64, Ordering::Relaxed);
                }
            }
        }
    }

    fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(16 * 1024);
        let mut rendered_types = BTreeSet::new();

        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_receiver_connections_total",
            self.receiver_connections_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_receiver_frames_total",
            self.receiver_frames_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_receiver_rows_total",
            self.receiver_rows_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_receiver_decode_errors_total",
            self.receiver_decode_errors_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_receiver_read_errors_total",
            self.receiver_read_errors_total.load(Ordering::Relaxed),
        );
        self.render_histogram(
            &mut out,
            &mut rendered_types,
            "wf_receiver_decode_seconds",
            &self.receiver_decode_seconds,
        );

        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_router_route_calls_total",
            self.router_route_calls_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_router_delivered_total",
            self.router_delivered_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_router_dropped_late_total",
            self.router_dropped_late_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_router_skipped_non_local_total",
            self.router_skipped_non_local_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_router_route_errors_total",
            self.router_route_errors_total.load(Ordering::Relaxed),
        );

        for (rule, value) in &self.rule_events_total {
            self.render_counter_labeled(
                &mut out,
                &mut rendered_types,
                "wf_rule_events_total",
                &[("rule", rule)],
                value.load(Ordering::Relaxed),
            );
        }
        for (rule, value) in &self.rule_matches_total {
            self.render_counter_labeled(
                &mut out,
                &mut rendered_types,
                "wf_rule_matches_total",
                &[("rule", rule)],
                value.load(Ordering::Relaxed),
            );
        }
        for (rule, value) in &self.rule_instances {
            self.render_gauge_labeled(
                &mut out,
                &mut rendered_types,
                "wf_rule_instances",
                &[("rule", rule)],
                value.load(Ordering::Relaxed),
            );
        }
        for (rule, by_window) in &self.rule_cursor_gap_total {
            for (window, value) in by_window {
                self.render_counter_labeled(
                    &mut out,
                    &mut rendered_types,
                    "wf_rule_cursor_gap_total",
                    &[("rule", rule), ("window", window)],
                    value.load(Ordering::Relaxed),
                );
            }
        }

        for (rule, value) in &self.alert_emitted_total {
            self.render_counter_labeled(
                &mut out,
                &mut rendered_types,
                "wf_alert_emitted_total",
                &[("rule", rule)],
                value.load(Ordering::Relaxed),
            );
        }
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_alert_channel_send_failed_total",
            self.alert_channel_send_failed_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_alert_serialize_failed_total",
            self.alert_serialize_failed_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_alert_dispatch_total",
            self.alert_dispatch_total.load(Ordering::Relaxed),
        );
        self.render_histogram(
            &mut out,
            &mut rendered_types,
            "wf_alert_dispatch_seconds",
            &self.alert_dispatch_seconds,
        );

        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_evictor_sweeps_total",
            self.evictor_sweeps_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_evictor_time_evicted_total",
            self.evictor_time_evicted_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            &mut rendered_types,
            "wf_evictor_memory_evicted_total",
            self.evictor_memory_evicted_total.load(Ordering::Relaxed),
        );

        for (rule, histogram) in &self.rule_scan_timeout_seconds {
            self.render_histogram_labeled(
                &mut out,
                &mut rendered_types,
                "wf_rule_scan_timeout_seconds",
                &[("rule", rule)],
                histogram,
            );
        }
        for (rule, histogram) in &self.rule_flush_seconds {
            self.render_histogram_labeled(
                &mut out,
                &mut rendered_types,
                "wf_rule_flush_seconds",
                &[("rule", rule)],
                histogram,
            );
        }

        for (window, value) in &self.window_memory_bytes {
            self.render_gauge_labeled(
                &mut out,
                &mut rendered_types,
                "wf_window_memory_bytes",
                &[("window", window)],
                value.load(Ordering::Relaxed),
            );
        }
        for (window, value) in &self.window_rows {
            self.render_gauge_labeled(
                &mut out,
                &mut rendered_types,
                "wf_window_rows",
                &[("window", window)],
                value.load(Ordering::Relaxed),
            );
        }
        for (window, value) in &self.window_batches {
            self.render_gauge_labeled(
                &mut out,
                &mut rendered_types,
                "wf_window_batches",
                &[("window", window)],
                value.load(Ordering::Relaxed),
            );
        }

        out
    }

    fn render_counter(
        &self,
        out: &mut String,
        rendered_types: &mut BTreeSet<String>,
        name: &str,
        value: u64,
    ) {
        self.render_type_once(out, rendered_types, name, "counter");
        let _ = writeln!(out, "{name} {value}");
    }

    fn render_gauge_labeled(
        &self,
        out: &mut String,
        rendered_types: &mut BTreeSet<String>,
        name: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        self.render_type_once(out, rendered_types, name, "gauge");
        let _ = writeln!(out, "{name}{} {value}", format_labels(labels));
    }

    fn render_counter_labeled(
        &self,
        out: &mut String,
        rendered_types: &mut BTreeSet<String>,
        name: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        self.render_type_once(out, rendered_types, name, "counter");
        let _ = writeln!(out, "{name}{} {value}", format_labels(labels));
    }

    fn render_histogram(
        &self,
        out: &mut String,
        rendered_types: &mut BTreeSet<String>,
        name: &str,
        histogram: &Histogram,
    ) {
        self.render_histogram_labeled(out, rendered_types, name, &[], histogram);
    }

    fn render_histogram_labeled(
        &self,
        out: &mut String,
        rendered_types: &mut BTreeSet<String>,
        name: &str,
        labels: &[(&str, &str)],
        histogram: &Histogram,
    ) {
        let snapshot = histogram.snapshot();
        self.render_type_once(out, rendered_types, name, "histogram");
        let mut cumulative = 0u64;
        for (idx, upper_bound_nanos) in snapshot.upper_bounds_nanos.iter().enumerate() {
            cumulative = cumulative.saturating_add(snapshot.bucket_counts[idx]);
            let le = format!("{:.6}", *upper_bound_nanos as f64 / 1_000_000_000.0);
            let mut all_labels = labels.to_vec();
            all_labels.push(("le", le.as_str()));
            let _ = writeln!(
                out,
                "{name}_bucket{} {cumulative}",
                format_labels(&all_labels),
            );
        }
        cumulative = cumulative.saturating_add(
            *snapshot
                .bucket_counts
                .last()
                .expect("histogram must include +Inf bucket"),
        );
        let mut all_labels = labels.to_vec();
        all_labels.push(("le", "+Inf"));
        let _ = writeln!(
            out,
            "{name}_bucket{} {cumulative}",
            format_labels(&all_labels),
        );
        let _ = writeln!(
            out,
            "{name}_sum{} {}",
            format_labels(labels),
            snapshot.sum_seconds
        );
        let _ = writeln!(out, "{name}_count{} {}", format_labels(labels), cumulative);
    }

    fn render_type_once(
        &self,
        out: &mut String,
        rendered_types: &mut BTreeSet<String>,
        name: &str,
        kind: &str,
    ) {
        if rendered_types.insert(name.to_string()) {
            let _ = writeln!(out, "# TYPE {name} {kind}");
        }
    }

    fn summary_line(&self) -> String {
        let total_window_bytes: u64 = self
            .window_memory_bytes
            .values()
            .map(|v| v.load(Ordering::Relaxed))
            .sum();
        format!(
            "rx_rows={} routed={} dropped_late={} matches={} alerts={} window_bytes={}",
            self.receiver_rows_total.load(Ordering::Relaxed),
            self.router_delivered_total.load(Ordering::Relaxed),
            self.router_dropped_late_total.load(Ordering::Relaxed),
            self.rule_matches_total
                .values()
                .map(|v| v.load(Ordering::Relaxed))
                .sum::<u64>(),
            self.alert_emitted_total
                .values()
                .map(|v| v.load(Ordering::Relaxed))
                .sum::<u64>(),
            total_window_bytes
        )
    }
}

fn format_labels(labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut out = String::from("{");
    for (idx, (key, value)) in labels.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(key);
        out.push('=');
        out.push('"');
        for ch in value.chars() {
            match ch {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                '\n' => out.push_str("\\n"),
                _ => out.push(ch),
            }
        }
        out.push('"');
    }
    out.push('}');
    out
}

pub async fn run_metrics_task(
    metrics: Arc<RuntimeMetrics>,
    config: MetricsConfig,
    listener: TcpListener,
    router: Arc<Router>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    wf_info!(
        sys,
        listen = %config.prometheus_listen,
        interval = %config.report_interval,
        "metrics exporter started"
    );

    metrics.sample_windows(&router);
    let mut tick = tokio::time::interval(config.report_interval.as_duration());
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {
                metrics.sample_windows(&router);
                wf_info!(res, summary = %metrics.summary_line(), "metrics snapshot");
            }
            result = listener.accept() => {
                let (stream, _) = result?;
                let metrics = Arc::clone(&metrics);
                tokio::spawn(async move {
                    if let Err(e) = serve_metrics_connection(stream, metrics).await {
                        wf_debug!(sys, error = %e, "metrics connection handling failed");
                    }
                });
            }
        }
    }
    Ok(())
}

async fn serve_metrics_connection(
    mut stream: TcpStream,
    metrics: Arc<RuntimeMetrics>,
) -> anyhow::Result<()> {
    let mut req_buf = [0u8; 512];
    let req_n = match timeout(Duration::from_secs(2), stream.read(&mut req_buf)).await {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => return Err(e.into()),
        Err(_) => return Ok(()),
    };
    let is_metrics = req_n > 0
        && std::str::from_utf8(&req_buf[..req_n])
            .unwrap_or("")
            .starts_with("GET /metrics");

    if is_metrics {
        let body = metrics.render_prometheus();
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        timeout(Duration::from_secs(2), stream.write_all(header.as_bytes())).await??;
        timeout(Duration::from_secs(2), stream.write_all(body.as_bytes())).await??;
    } else {
        timeout(
            Duration::from_secs(2),
            stream.write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            ),
        )
        .await??;
    }
    let _ = timeout(Duration::from_secs(1), stream.shutdown()).await;
    Ok(())
}

pub fn maybe_build_metrics(
    config: &MetricsConfig,
    rule_names: &[String],
    window_names: &[String],
) -> Option<Arc<RuntimeMetrics>> {
    if !config.enabled {
        return None;
    }
    Some(Arc::new(RuntimeMetrics::new(rule_names, window_names)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_occurrences(haystack: &str, needle: &str) -> usize {
        haystack.match_indices(needle).count()
    }

    #[test]
    fn renders_type_line_once_per_metric_family() {
        let metrics = RuntimeMetrics::new(
            &["r1".to_string(), "r2".to_string()],
            &["w1".to_string(), "w2".to_string()],
        );
        let text = metrics.render_prometheus();
        assert_eq!(
            count_occurrences(&text, "# TYPE wf_rule_events_total counter"),
            1
        );
        assert_eq!(count_occurrences(&text, "# TYPE wf_window_rows gauge"), 1);
        assert_eq!(
            count_occurrences(&text, "# TYPE wf_rule_flush_seconds histogram"),
            1
        );
    }

    #[test]
    fn histogram_count_matches_inf_bucket() {
        let metrics = RuntimeMetrics::new(&["r1".to_string()], &["w1".to_string()]);
        metrics.observe_receiver_decode(Duration::from_millis(3));
        metrics.observe_receiver_decode(Duration::from_millis(7));
        let text = metrics.render_prometheus();
        assert!(text.contains("wf_receiver_decode_seconds_bucket{le=\"+Inf\"} 2"));
        assert!(text.contains("wf_receiver_decode_seconds_count 2"));
    }
}
