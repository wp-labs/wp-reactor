use super::*;

// ===========================================================================
// Join: snapshot mode enriches eval context
// ===========================================================================

#[test]
fn join_snapshot_enriches_context() {
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_join",
        match_plan,
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    rule_plan.joins = vec![snapshot_join("geo_lookup", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    let mut wl = MockWindowLookup::new();
    wl.add_snapshot(
        "geo_lookup",
        vec![row(vec![
            ("ip", str_val("10.0.0.1")),
            ("country", str_val("US")),
            ("city", str_val("NYC")),
        ])],
    );

    let matched = MatchedContext {
        rule_name: "r_join".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 1.0,
        }],
        event_time_nanos: 0,
    };

    let alert = exec.execute_match_with_joins(&matched, &wl).unwrap();
    assert_eq!(alert.rule_name, "r_join");
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
}

// ===========================================================================
// Join: entity from joined field
// ===========================================================================

#[test]
fn join_entity_from_joined_field() {
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_join_entity",
        match_plan,
        Expr::Number(80.0),
        "host",
        // entity_id uses "hostname" which comes from the join
        Expr::Field(FieldRef::Simple("hostname".to_string())),
    );
    rule_plan.joins = vec![snapshot_join("asset_db", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    let mut wl = MockWindowLookup::new();
    wl.add_snapshot(
        "asset_db",
        vec![row(vec![
            ("ip", str_val("10.0.0.1")),
            ("hostname", str_val("web-server-01")),
        ])],
    );

    let matched = MatchedContext {
        rule_name: "r_join_entity".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
        }],
        event_time_nanos: 0,
    };

    let alert = exec.execute_match_with_joins(&matched, &wl).unwrap();
    assert_eq!(alert.entity_id, "web-server-01");
}

// ===========================================================================
// Join: no matching row — entity falls back to non-joined fields
// ===========================================================================

#[test]
fn join_no_match_falls_through() {
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_join_norow",
        match_plan,
        Expr::Number(50.0),
        "ip",
        // entity_id uses "sip" which exists in eval context from keys
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    rule_plan.joins = vec![snapshot_join("asset_db", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    // Window has rows but none match "10.0.0.1"
    let mut wl = MockWindowLookup::new();
    wl.add_snapshot(
        "asset_db",
        vec![row(vec![
            ("ip", str_val("192.168.1.1")),
            ("hostname", str_val("other-host")),
        ])],
    );

    let matched = MatchedContext {
        rule_name: "r_join_norow".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
        }],
        event_time_nanos: 0,
    };

    // No join match — entity falls back to "sip" from keys
    let alert = exec.execute_match_with_joins(&matched, &wl).unwrap();
    assert_eq!(alert.entity_id, "10.0.0.1");
}

// ===========================================================================
// Join: close with joins
// ===========================================================================

#[test]
fn join_close_with_joins() {
    use crate::rule::match_engine::{CloseOutput, CloseReason};

    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_join_close",
        match_plan,
        Expr::Number(60.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    rule_plan.joins = vec![snapshot_join("asset_db", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    let mut wl = MockWindowLookup::new();
    wl.add_snapshot(
        "asset_db",
        vec![row(vec![("ip", str_val("10.0.0.1")), ("risk", num(95.0))])],
    );

    let close = CloseOutput {
        rule_name: "r_join_close".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 3.0,
        }],
        close_step_data: vec![],
        watermark_nanos: 0,
        last_event_nanos: 0,
    };

    let alert = exec.execute_close_with_joins(&close, &wl).unwrap().unwrap();
    assert_eq!(alert.close_reason.as_deref(), Some("timeout"));
    assert!((alert.score - 60.0).abs() < f64::EPSILON);
}

// ===========================================================================
// Join asof: picks the latest row before event time
// ===========================================================================

#[test]
fn join_asof_picks_latest_before_event_time() {
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_asof",
        match_plan,
        // Score uses the joined "risk" field
        Expr::Field(FieldRef::Simple("risk".to_string())),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    rule_plan.joins = vec![asof_join("threat_intel", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    let event_time: i64 = 1_000_000_000; // 1s in nanos

    let mut wl = MockWindowLookup::new();
    wl.add_timestamped_snapshot(
        "threat_intel",
        vec![
            // Row at 200ms — older, matching
            (
                200_000_000,
                row(vec![
                    ("ip", str_val("10.0.0.1")),
                    ("risk", num(50.0)),
                ]),
            ),
            // Row at 800ms — newer, matching → should be picked
            (
                800_000_000,
                row(vec![
                    ("ip", str_val("10.0.0.1")),
                    ("risk", num(90.0)),
                ]),
            ),
            // Row at 2s — after event time → should be excluded
            (
                2_000_000_000,
                row(vec![
                    ("ip", str_val("10.0.0.1")),
                    ("risk", num(99.0)),
                ]),
            ),
        ],
    );

    let matched = MatchedContext {
        rule_name: "r_asof".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
        }],
        event_time_nanos: event_time,
    };

    let alert = exec.execute_match_with_joins(&matched, &wl).unwrap();
    // Should pick the row at 800ms with risk=90.0
    assert!((alert.score - 90.0).abs() < f64::EPSILON);
}

// ===========================================================================
// Join asof within: filters out rows outside the within window
// ===========================================================================

#[test]
fn join_asof_within_filters_old_rows() {
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_asof_within",
        match_plan,
        Expr::Field(FieldRef::Simple("risk".to_string())),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    // within 500ms
    rule_plan.joins = vec![asof_join_within(
        "threat_intel",
        "sip",
        "ip",
        Duration::from_millis(500),
    )];

    let exec = RuleExecutor::new(rule_plan);

    let event_time: i64 = 1_000_000_000; // 1s in nanos

    let mut wl = MockWindowLookup::new();
    wl.add_timestamped_snapshot(
        "threat_intel",
        vec![
            // Row at 200ms — within would require >= 500ms, so this is too old
            (
                200_000_000,
                row(vec![
                    ("ip", str_val("10.0.0.1")),
                    ("risk", num(50.0)),
                ]),
            ),
            // Row at 600ms — within range [500ms, 1000ms] → should be picked
            (
                600_000_000,
                row(vec![
                    ("ip", str_val("10.0.0.1")),
                    ("risk", num(75.0)),
                ]),
            ),
        ],
    );

    let matched = MatchedContext {
        rule_name: "r_asof_within".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
        }],
        event_time_nanos: event_time,
    };

    let alert = exec.execute_match_with_joins(&matched, &wl).unwrap();
    // Should pick the row at 600ms (the only one within the window)
    assert!((alert.score - 75.0).abs() < f64::EPSILON);
}

// ===========================================================================
// Join asof: no timestamp support → graceful skip (no match)
// ===========================================================================

#[test]
fn join_asof_no_timestamp_support_skips() {
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_asof_nots",
        match_plan,
        Expr::Number(42.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    rule_plan.joins = vec![asof_join("no_ts_window", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    // MockWindowLookup with NO timestamped_snapshots for "no_ts_window"
    // → snapshot_with_timestamps returns None → join is skipped
    let wl = MockWindowLookup::new();

    let matched = MatchedContext {
        rule_name: "r_asof_nots".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
        }],
        event_time_nanos: 1_000_000_000,
    };

    // Join produces no match, but alert still works with score=42
    let alert = exec.execute_match_with_joins(&matched, &wl).unwrap();
    assert!((alert.score - 42.0).abs() < f64::EPSILON);
}

// ===========================================================================
// Join asof: close path uses last_event_nanos, not watermark
// ===========================================================================

#[test]
fn join_asof_close_uses_last_event_nanos() {
    use crate::rule::match_engine::{CloseOutput, CloseReason};

    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut rule_plan = simple_rule_plan(
        "r_asof_close",
        match_plan,
        // Score uses the joined "risk" field
        Expr::Field(FieldRef::Simple("risk".to_string())),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    rule_plan.joins = vec![asof_join("threat_intel", "sip", "ip")];

    let exec = RuleExecutor::new(rule_plan);

    // Instance last saw an event at 1s.
    // Global watermark advanced to 5s (other instances pushed it forward).
    // Right table has a row at 3s — after last_event but before watermark.
    let last_event: i64 = 1_000_000_000;
    let watermark: i64 = 5_000_000_000;

    let mut wl = MockWindowLookup::new();
    wl.add_timestamped_snapshot(
        "threat_intel",
        vec![
            // Row at 500ms — before last_event → eligible
            (
                500_000_000,
                row(vec![
                    ("ip", str_val("10.0.0.1")),
                    ("risk", num(60.0)),
                ]),
            ),
            // Row at 3s — after last_event, before watermark → must NOT match
            (
                3_000_000_000,
                row(vec![
                    ("ip", str_val("10.0.0.1")),
                    ("risk", num(99.0)),
                ]),
            ),
        ],
    );

    let close = CloseOutput {
        rule_name: "r_asof_close".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Flush,
        event_ok: true,
        close_ok: true,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 1.0,
        }],
        close_step_data: vec![],
        watermark_nanos: watermark,
        last_event_nanos: last_event,
    };

    let alert = exec
        .execute_close_with_joins(&close, &wl)
        .unwrap()
        .unwrap();
    // Should pick the row at 500ms (risk=60), NOT the row at 3s (risk=99)
    assert!(
        (alert.score - 60.0).abs() < f64::EPSILON,
        "expected score 60.0 from 500ms row, got {}",
        alert.score
    );
}
