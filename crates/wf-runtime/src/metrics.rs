use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use wf_config::MetricsConfig;
use wf_core::window::{EvictReport, RouteReport, Router};

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
}

impl RuntimeMetrics {
    pub fn new(rule_names: &[String], window_names: &[String]) -> Self {
        let make_rule_map = || {
            rule_names
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

    pub fn add_evict_report(&self, report: &EvictReport) {
        self.evictor_sweeps_total.fetch_add(1, Ordering::Relaxed);
        self.evictor_time_evicted_total
            .fetch_add(report.batches_time_evicted as u64, Ordering::Relaxed);
        self.evictor_memory_evicted_total
            .fetch_add(report.batches_memory_evicted as u64, Ordering::Relaxed);
    }

    fn render_prometheus(&self, router: &Router) -> String {
        let mut out = String::with_capacity(16 * 1024);

        self.render_counter(
            &mut out,
            "wf_receiver_connections_total",
            self.receiver_connections_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_receiver_frames_total",
            self.receiver_frames_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_receiver_rows_total",
            self.receiver_rows_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_receiver_decode_errors_total",
            self.receiver_decode_errors_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_receiver_read_errors_total",
            self.receiver_read_errors_total.load(Ordering::Relaxed),
        );

        self.render_counter(
            &mut out,
            "wf_router_route_calls_total",
            self.router_route_calls_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_router_delivered_total",
            self.router_delivered_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_router_dropped_late_total",
            self.router_dropped_late_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_router_skipped_non_local_total",
            self.router_skipped_non_local_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_router_route_errors_total",
            self.router_route_errors_total.load(Ordering::Relaxed),
        );

        for (rule, value) in &self.rule_events_total {
            self.render_counter_labeled(
                &mut out,
                "wf_rule_events_total",
                &[("rule", rule)],
                value.load(Ordering::Relaxed),
            );
        }
        for (rule, value) in &self.rule_matches_total {
            self.render_counter_labeled(
                &mut out,
                "wf_rule_matches_total",
                &[("rule", rule)],
                value.load(Ordering::Relaxed),
            );
        }
        for (rule, value) in &self.rule_instances {
            self.render_gauge_labeled(
                &mut out,
                "wf_rule_instances",
                &[("rule", rule)],
                value.load(Ordering::Relaxed),
            );
        }
        for (rule, by_window) in &self.rule_cursor_gap_total {
            for (window, value) in by_window {
                self.render_counter_labeled(
                    &mut out,
                    "wf_rule_cursor_gap_total",
                    &[("rule", rule), ("window", window)],
                    value.load(Ordering::Relaxed),
                );
            }
        }

        for (rule, value) in &self.alert_emitted_total {
            self.render_counter_labeled(
                &mut out,
                "wf_alert_emitted_total",
                &[("rule", rule)],
                value.load(Ordering::Relaxed),
            );
        }
        self.render_counter(
            &mut out,
            "wf_alert_channel_send_failed_total",
            self.alert_channel_send_failed_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_alert_serialize_failed_total",
            self.alert_serialize_failed_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_alert_dispatch_total",
            self.alert_dispatch_total.load(Ordering::Relaxed),
        );

        self.render_counter(
            &mut out,
            "wf_evictor_sweeps_total",
            self.evictor_sweeps_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_evictor_time_evicted_total",
            self.evictor_time_evicted_total.load(Ordering::Relaxed),
        );
        self.render_counter(
            &mut out,
            "wf_evictor_memory_evicted_total",
            self.evictor_memory_evicted_total.load(Ordering::Relaxed),
        );

        for window_name in router.registry().window_names() {
            if let Some(win_lock) = router.registry().get_window(window_name) {
                let win = win_lock.read().expect("window lock poisoned");
                self.render_gauge_labeled(
                    &mut out,
                    "wf_window_memory_bytes",
                    &[("window", window_name)],
                    win.memory_usage() as u64,
                );
                self.render_gauge_labeled(
                    &mut out,
                    "wf_window_rows",
                    &[("window", window_name)],
                    win.total_rows() as u64,
                );
                self.render_gauge_labeled(
                    &mut out,
                    "wf_window_batches",
                    &[("window", window_name)],
                    win.batch_count() as u64,
                );
            }
        }

        out
    }

    fn render_counter(&self, out: &mut String, name: &str, value: u64) {
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name} {value}");
    }

    fn render_gauge_labeled(
        &self,
        out: &mut String,
        name: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        let _ = writeln!(out, "# TYPE {name} gauge");
        let _ = writeln!(out, "{name}{} {value}", format_labels(labels));
    }

    fn render_counter_labeled(
        &self,
        out: &mut String,
        name: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        let _ = writeln!(out, "# TYPE {name} counter");
        let _ = writeln!(out, "{name}{} {value}", format_labels(labels));
    }

    fn summary_line(&self, router: &Router) -> String {
        let mut total_window_bytes = 0usize;
        for window_name in router.registry().window_names() {
            if let Some(win_lock) = router.registry().get_window(window_name) {
                let win = win_lock.read().expect("window lock poisoned");
                total_window_bytes += win.memory_usage();
            }
        }
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
    router: Arc<Router>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&config.prometheus_listen).await?;
    wf_info!(
        sys,
        listen = %config.prometheus_listen,
        interval = %config.report_interval,
        "metrics exporter started"
    );

    let mut tick = tokio::time::interval(config.report_interval.as_duration());
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tick.tick() => {
                wf_info!(res, summary = %metrics.summary_line(&router), "metrics snapshot");
            }
            result = listener.accept() => {
                let (mut stream, _) = result?;
                let body = metrics.render_prometheus(&router);
                let mut req_buf = [0u8; 512];
                let req_n = stream.read(&mut req_buf).await.unwrap_or(0);
                let is_metrics = req_n > 0
                    && std::str::from_utf8(&req_buf[..req_n]).unwrap_or("").starts_with("GET /metrics");
                if is_metrics {
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(header.as_bytes()).await?;
                    stream.write_all(body.as_bytes()).await?;
                } else {
                    stream
                        .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await?;
                }
                stream.shutdown().await?;
            }
        }
    }
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
