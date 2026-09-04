//! `MetricsSnapshot` → `MetricsRecord` 序列化（metrics 结构拆件，2026-09-04）。
//!
//! 把一次 `snapshot()` 拍下的 [`MetricsSnapshot`]（struct 声明留父层，字段对
//! metrics 子树可见）摊平成 sink/监控通道传输用的扁平 key-value 记录。记录
//! 构建 helper（`metric`/`metric_with_type`/`metric_source_reason`/
//! `metric_double`/`hist_p50`/`hist_p99`/`percentile`）留父层收口——测试与
//! 本模块都经 `use super::*` 取用。

use super::*;

impl MetricsSnapshot {
    #[allow(clippy::vec_init_then_push)]
    pub(crate) fn to_records(&self) -> Vec<MetricsRecord> {
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
        for (rule, v) in &self.rule_memory_bytes {
            out.push(metric("rule", "memory_bytes", rule, *v));
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
