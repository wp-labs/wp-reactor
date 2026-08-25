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

#[test]
fn snapshot_stats_over_limit_drains() {
    let metrics = RuntimeMetrics::new(
        &["r1".to_string()],
        &["w1".to_string()],
        &[],
        BTreeMap::new(),
    );
    // 增量语义: 多次上报累加（stats close 每窗 delta）。
    metrics.inc_rule_stats_over_limit("r1", 3);
    metrics.inc_rule_stats_over_limit("r1", 2);
    let snap = metrics.snapshot();
    assert_eq!(snap.rule_stats_over_limit.get("r1"), Some(&5));
    // 0 增量 no-op。
    metrics.inc_rule_stats_over_limit("r1", 0);
    // 导出记录带 `stats_over_limit_total` 名。
    let records = snap.to_records();
    assert!(records.iter().any(|r| {
        r.fields.iter().any(|(k, v)| k == "name" && v == "stats_over_limit_total")
            && r.fields.iter().any(|(k, v)| k == "label" && v == "r1")
            && r.fields.iter().any(|(k, v)| k == "value" && v == "5")
    }));
    // drain → 0。
    assert_eq!(metrics.snapshot().rule_stats_over_limit.get("r1"), Some(&0));
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
        memory_pressure: false,
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
fn receiver_window_miss_metrics_are_grouped_by_source_and_reason() {
    let m = RuntimeMetrics::new(&[], &[], &["s1".to_string()], BTreeMap::new());
    m.add_receiver_window_miss("s1", "unknown_stream_schema", 2);
    m.add_receiver_window_miss("s1", "missing_stream_tag_field", 1);

    let records = m.snapshot().to_records();
    let value_for = |reason: &str| -> u64 {
        records
            .iter()
            .find(|record| {
                record
                    .fields
                    .iter()
                    .any(|(k, v)| k == "name" && v == "window_miss_total")
                    && record.fields.iter().any(|(k, v)| k == "label" && v == "s1")
                    && record
                        .fields
                        .iter()
                        .any(|(k, v)| k == "reason" && v == reason)
            })
            .and_then(|record| {
                record
                    .fields
                    .iter()
                    .find(|(k, _)| k == "value")
                    .and_then(|(_, v)| v.parse().ok())
            })
            .unwrap_or(0)
    };

    assert_eq!(value_for("unknown_stream_schema"), 2);
    assert_eq!(value_for("missing_stream_tag_field"), 1);
    assert!(m.snapshot().receiver_window_misses.is_empty());
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

#[test]
fn rule_instances_gauge_sums_deltas_across_shards() {
    let m = RuntimeMetrics::new(&["r1".to_string()], &[], &[], BTreeMap::new());
    // Two shards report their live counts as deltas (P2b): 3 + 2 = 5.
    m.adjust_rule_instances("r1", 3);
    m.adjust_rule_instances("r1", 2);
    assert_eq!(m.snapshot().rule_instances["r1"], 5);
    // Shard1's count drops to 1 → delta -2.
    m.adjust_rule_instances("r1", -2);
    assert_eq!(m.snapshot().rule_instances["r1"], 3);
    // Shutdown reconcile: both drain to 0.
    m.adjust_rule_instances("r1", -3);
    assert_eq!(m.snapshot().rule_instances["r1"], 0);
    // Overshoot below zero clamps to zero (gauge never goes negative).
    m.adjust_rule_instances("r1", -5);
    assert_eq!(m.snapshot().rule_instances["r1"], 0);
}
