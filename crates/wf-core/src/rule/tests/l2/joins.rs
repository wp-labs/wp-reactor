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
    };

    let alert = exec.execute_close_with_joins(&close, &wl).unwrap().unwrap();
    assert_eq!(alert.close_reason.as_deref(), Some("timeout"));
    assert!((alert.score - 60.0).abs() < f64::EPSILON);
}
