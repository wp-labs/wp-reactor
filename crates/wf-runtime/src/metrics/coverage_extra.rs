//! metrics/mod.rs 覆盖测试（注册于 metrics/mod.rs）。
//!
//! 覆盖点:
//! - 全部计数器/直方图方法 → `snapshot()` 各字段。
//! - `report_window_append` 正常/迟到分支; `inc_alert_emitted_detail` 作用域上限。
//! - `to_records` 的 per-source/machine/double 记录与直方图 p50/p99 记录。
//! - `interval_snapshot` / `interval_rates`（含 0 间隔 None）/ `interval_table` /
//!   `summary_line` / `RunSummary::table` / `format_bytes` / `maybe_build_metrics`。

use super::*;
use std::time::Duration;

fn metrics(rule_names: &[&str], window_names: &[&str], source_names: &[&str]) -> RuntimeMetrics {
    let rule_names: Vec<String> = rule_names.iter().map(|s| s.to_string()).collect();
    let window_names: Vec<String> = window_names.iter().map(|s| s.to_string()).collect();
    let source_names: Vec<String> = source_names.iter().map(|s| s.to_string()).collect();
    let mut source_types = BTreeMap::new();
    source_types.insert("s1".to_string(), "file".to_string());
    RuntimeMetrics::new(&rule_names, &window_names, &source_names, source_types)
}

#[test]
fn counters_flow_through_snapshot() {
    let m = metrics(&["r1"], &["w1"], &["s1"]);

    m.inc_receiver_connection();
    m.add_receiver_frame(10);
    m.add_receiver_source_frame("s1", 7);
    m.add_receiver_source_machine_rows("s1", "10.0.0.1", 3);
    m.inc_receiver_decode_error();
    m.inc_receiver_source_decode_error("s1");
    m.observe_receiver_decode(Duration::from_millis(1));
    m.inc_receiver_read_error();
    m.inc_receiver_source_read_error("s1");
    m.add_receiver_window_miss("s1", "unknown_stream_schema", 2);

    m.inc_router_route_call();
    m.add_router_skipped(3);
    m.inc_route_error("s1");
    m.report_window_append("w1", 5, false);
    m.report_window_append("w1", 1, true);

    m.add_rule_events("r1", 4);
    m.inc_rule_match("r1");
    m.adjust_rule_instances("r1", 2);
    m.inc_rule_cursor_gap("r1", "w1");

    m.inc_alert_emitted_total("r1");
    m.inc_alert_emitted("r1", "10.0.0.1", "sip=10.0.0.1");
    m.inc_alert_channel_send_failed();
    m.inc_sink_dispatch_failed();
    m.inc_alert_append_failed();
    m.add_alert_no_sink_records(2);
    m.add_sink_drain_dropped_records(1);
    m.inc_alert_escalate_failed();
    m.add_alert_append_nanos(123);
    m.inc_alert_dispatch();
    m.observe_alert_dispatch(Duration::from_millis(2));
    m.observe_rule_scan_timeout("r1", Duration::from_millis(3));
    m.observe_rule_flush("r1", Duration::from_millis(4));
    m.inc_alert_channel_full();
    m.set_alert_channel_depth(3);
    m.observe_event_e2e_latency(Duration::from_millis(5));
    m.add_window_evict("w1", 2);
    m.add_window_late("w1", 1);

    let snap = m.snapshot();
    assert_eq!(snap.receiver_connections, 1);
    assert_eq!(snap.receiver_frames, 1);
    assert_eq!(snap.receiver_source_rows.get("s1"), Some(&7));
    assert_eq!(
        snap.receiver_source_machine_rows
            .get("s1")
            .and_then(|m| m.get("10.0.0.1")),
        Some(&3)
    );
    assert_eq!(snap.receiver_source_decode_errors.get("s1"), Some(&1));
    assert_eq!(snap.receiver_source_read_errors.get("s1"), Some(&1));
    assert_eq!(
        snap.receiver_window_misses
            .get("s1")
            .and_then(|m| m.get("unknown_stream_schema")),
        Some(&2)
    );

    assert_eq!(snap.router_route_calls, 1);
    assert_eq!(snap.router_skipped_non_local, 3);
    assert_eq!(snap.router_source_route_errors.get("s1"), Some(&1));
    assert_eq!(snap.router_delivered, 1);
    assert_eq!(snap.router_dropped_late, 1);
    assert_eq!(snap.window_append.get("w1"), Some(&5));
    // add_window_late (1) + report_window_append late (1).
    assert_eq!(snap.window_late.get("w1"), Some(&2));

    assert_eq!(snap.rule_events.get("r1"), Some(&4));
    assert_eq!(snap.rule_matches.get("r1"), Some(&1));
    assert_eq!(snap.rule_instances.get("r1"), Some(&2));
    assert_eq!(
        snap.rule_cursor_gaps.get("r1").and_then(|w| w.get("w1")),
        Some(&1)
    );

    assert_eq!(snap.alert_emitted.get("r1"), Some(&2));
    assert_eq!(snap.alert_channel_send_failed, 1);
    assert_eq!(snap.alert_sink_dispatch_failed, 1);
    assert_eq!(snap.alert_channel_full, 1);
    assert_eq!(snap.alert_channel_depth, 3);
    assert_eq!(snap.alert_append_failed, 1);
    assert_eq!(snap.alert_dispatch, 1);
    assert_eq!(snap.alert_no_sink_records, 2);
    assert_eq!(snap.alert_drain_dropped_records, 1);
    assert_eq!(snap.alert_escalate_failed, 1);
    assert_eq!(snap.alert_append_nanos, 123);

    assert_eq!(snap.window_evict.get("w1"), Some(&2));

    // Second snapshot: counters are drained, gauges persist.
    let snap2 = m.snapshot();
    assert_eq!(snap2.receiver_connections, 0);
    assert_eq!(snap2.router_delivered, 0);
    assert_eq!(snap2.rule_instances.get("r1"), Some(&2));
    assert_eq!(snap2.alert_channel_depth, 3);
}

#[test]
fn report_window_append_branches() {
    let m = metrics(&[], &["w1"], &[]);
    m.report_window_append("w1", 3, false);
    m.report_window_append("w1", 2, true);
    let snap = m.snapshot();
    assert_eq!(snap.router_delivered, 1);
    assert_eq!(snap.router_dropped_late, 1);
    assert_eq!(snap.window_append.get("w1"), Some(&3));
    assert_eq!(snap.window_late.get("w1"), Some(&2));
}

#[test]
fn inc_alert_emitted_detail_bounds_scope_cap() {
    let m = metrics(&["r1"], &[], &[]);
    let cap = ALERT_DETAIL_MAX_SCOPES_PER_RULE;
    // Feed `cap + 5` distinct scopes in one interval: the exact total counts
    // everything, the detail map is capped at `cap`.
    for i in 0..(cap + 5) {
        m.inc_alert_emitted("r1", "m1", &format!("scope-{i}"));
    }
    let snap = m.snapshot();
    assert_eq!(snap.alert_emitted.get("r1"), Some(&((cap + 5) as u64)));
    let detail = snap
        .alert_emitted_detail
        .get("r1")
        .and_then(|m| m.get("m1"))
        .expect("detail rows");
    assert_eq!(detail.len(), cap);

    // The detail map resets per interval (drained by snapshot): a new scope
    // in the next interval is tracked again.
    m.inc_alert_emitted("r1", "m1", "scope-new");
    let snap = m.snapshot();
    assert_eq!(snap.alert_emitted.get("r1"), Some(&1));
    let detail = snap
        .alert_emitted_detail
        .get("r1")
        .and_then(|m| m.get("m1"))
        .expect("detail rows");
    assert_eq!(detail.len(), 1);
}

#[test]
fn to_records_covers_machine_double_and_hist_records() {
    let m = metrics(&["r1"], &["w1"], &["s1"]);
    m.add_receiver_source_frame("s1", 1);
    m.add_receiver_source_machine_rows("s1", "10.0.0.1", 5);
    m.inc_rule_cursor_gap("r1", "w1");
    m.observe_rule_scan_timeout("r1", Duration::from_millis(2));
    m.observe_rule_flush("r1", Duration::from_millis(3));
    m.observe_event_e2e_latency(Duration::from_millis(4));

    let records = m.snapshot().to_records();
    let names: Vec<&str> = records.iter().map(|r| r.field("name").unwrap()).collect();

    // Per-machine rows_total record with the machine label.
    let machine_record = records
        .iter()
        .find(|r| {
            r.fields
                .iter()
                .any(|(k, v)| k == "machine" && v == "10.0.0.1")
        })
        .expect("machine record");
    assert_eq!(machine_record.field("name"), Some("rows_total"));
    assert_eq!(machine_record.field("source_type"), Some("file"));

    // Double-label cursor gap record.
    let gap = records
        .iter()
        .find(|r| r.field("name") == Some("cursor_gap_total"))
        .expect("cursor gap record");
    assert_eq!(gap.field("rule"), Some("r1"));
    assert_eq!(gap.field("window"), Some("w1"));
    assert_eq!(gap.field("value"), Some("1"));

    // Histogram p50/p99 records.
    assert!(names.contains(&"scan_timeout_seconds_p50"));
    assert!(names.contains(&"scan_timeout_seconds_p99"));
    assert!(names.contains(&"flush_seconds_p50"));
    assert!(names.contains(&"e2e_latency_seconds_p99"));

    // Per-source rows_total is suppressed when a per-machine breakdown exists.
    let per_source_rows = records
        .iter()
        .filter(|r| {
            r.field("name") == Some("rows_total")
                && r.fields.iter().any(|(k, v)| k == "label" && v == "s1")
                && !r.fields.iter().any(|(k, _)| k == "machine")
        })
        .count();
    assert_eq!(per_source_rows, 0);
}

#[test]
fn interval_snapshot_and_rates() {
    let m = metrics(&["r1"], &["w1"], &[]);
    m.add_receiver_frame(100);
    m.inc_rule_match("r1");
    m.inc_alert_dispatch();
    m.add_window_append("w1", 10);
    m.adjust_rule_instances("r1", 4);

    let now = Instant::now();
    let prev = m.interval_snapshot(now);
    // Increment AFTER the baseline snapshot so the deltas are measured.
    m.add_receiver_frame(100);
    m.inc_rule_match("r1");
    m.inc_alert_dispatch();
    m.add_window_append("w1", 10);
    m.adjust_rule_instances("r1", 4);
    let curr = m.interval_snapshot(now + Duration::from_secs(2));

    let rates = m.interval_rates(prev, curr).expect("rates over 2s");
    assert_eq!(rates.row_s, 50.0);
    assert_eq!(rates.rules_s, 0.5);
    assert_eq!(rates.out_s, 0.5);
    assert_eq!(rates.sm_s, 2.0);
    // No window gauges updated → window_bytes is 0.
    assert_eq!(rates.memory_bytes, 0);

    // Zero-length interval → None.
    assert!(m.interval_rates(prev, prev).is_none());
}

#[test]
fn interval_table_and_summary_line_render() {
    let m = metrics(&["r1"], &["w1"], &[]);
    m.add_receiver_frame(10);
    m.inc_rule_match("r1");
    let rates = IntervalRates {
        row_s: 100.0,
        late_s: 1.0,
        rules_s: 2.0,
        sm_s: 0.5,
        out_s: 3.0,
        memory_bytes: 2048,
    };
    let table = m.interval_table(rates);
    assert!(table.contains("row/s"));
    assert!(table.contains("2.0KB"));

    let line = m.summary_line();
    assert!(line.contains("rx_rows=10"), "got: {line}");
    assert!(line.contains("matches=1"), "got: {line}");
}

#[test]
fn run_summary_table_variants() {
    let mut summary = RunSummary::default();
    // Empty summary → None.
    assert!(summary.table(None).is_none());

    summary.observe(IntervalRates {
        row_s: 1.0,
        late_s: 0.0,
        rules_s: 2.0,
        sm_s: 3.0,
        out_s: 4.0,
        memory_bytes: 1024,
    });
    // Without totals.
    let t1 = summary.table(None).expect("table");
    assert!(t1.contains("| avg     |"));

    // With totals: negative sm_delta renders with a minus sign.
    let t2 = summary
        .table(Some(TotalCounts {
            rows: 1,
            late: 0,
            rules: 2,
            out: 4,
            sm_delta: -2,
        }))
        .expect("table with totals");
    assert!(t2.contains("| total   | rows"));
    assert!(t2.contains("-2"));

    // Positive sm_delta renders with a plus sign.
    let t3 = summary
        .table(Some(TotalCounts {
            rows: 1,
            late: 0,
            rules: 2,
            out: 4,
            sm_delta: 5,
        }))
        .expect("table with totals");
    assert!(t3.contains("+5"));
}

#[test]
fn format_bytes_units() {
    assert_eq!(format_bytes(0), "0B");
    assert_eq!(format_bytes(1023), "1023B");
    assert_eq!(format_bytes(1024), "1.0KB");
    assert_eq!(format_bytes(1536), "1.5KB");
    assert_eq!(format_bytes(1024 * 1024), "1.0MB");
    assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0GB");
    assert_eq!(format_bytes(1024u64.pow(4)), "1.0TB");
}

#[test]
fn maybe_build_metrics_disabled_and_enabled() {
    let mut config = wf_config::MetricsConfig {
        enabled: false,
        ..Default::default()
    };
    assert!(maybe_build_metrics(&config, &[], &[], &[], BTreeMap::new()).is_none());

    config.enabled = true;
    let built = maybe_build_metrics(&config, &["r1".into()], &[], &[], BTreeMap::new());
    let built = built.expect("built");
    built.inc_rule_match("r1");
    assert_eq!(built.snapshot().rule_matches.get("r1"), Some(&1));
}

#[test]
fn metric_helpers_emit_expected_fields() {
    // `metric` with an empty label omits the label field.
    let rec = metric("stage", "name", "", 7);
    assert!(!rec.fields.iter().any(|(k, _)| k == "label"));
    let rec = metric("stage", "name", "lbl", 7);
    assert_eq!(rec.field("label"), Some("lbl"));

    // metric_with_type omits source_type when empty.
    let rec = metric_with_type("s", "n", "l", "", 1);
    assert!(!rec.fields.iter().any(|(k, _)| k == "source_type"));
    let rec = metric_with_type("s", "n", "l", "file", 1);
    assert_eq!(rec.field("source_type"), Some("file"));

    // metric_source_reason always carries reason.
    let rec = metric_source_reason("s", "n", "l", "file", "why", 2);
    assert_eq!(rec.field("reason"), Some("why"));

    // metric_double carries rule + window labels.
    let rec = metric_double("s", "n", "r1", "w1", 3);
    assert_eq!(rec.field("rule"), Some("r1"));
    assert_eq!(rec.field("window"), Some("w1"));
}

impl MetricsRecord {
    fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}
