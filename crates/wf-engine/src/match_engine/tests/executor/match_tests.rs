use wf_lang::ast::{BinOp, Expr, FieldRef};
use wf_lang::plan::EachPlan;

use crate::match_engine::RuleExecutor;

use super::helpers::{default_match_plan, default_matched_context};
use super::super::helpers::*;

// =========================================================================
// Test 1: execute_match – static score
// =========================================================================

#[test]
fn execute_match_static_score() {
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Qualified("fail".to_string(), "sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();

    let alert = exec.execute_match(&matched).unwrap();

    assert_eq!(alert.rule_name, "r1");
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
    assert_eq!(alert.entity_type, "ip");
    assert_eq!(alert.entity_id, "10.0.0.1");
    assert_eq!(alert.origin, crate::alert::AlertOrigin::Event);
    assert!(alert.matched_rows.is_empty());
    assert!(alert.fired_at.ends_with('Z'));
}

#[test]
fn execute_each_wfx_id_changes_with_event_content() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".to_string(), "sip".to_string())),
    );
    plan.binds[0].alias = "e".to_string();
    plan.each_plan = Some(EachPlan {
        alias: "e".to_string(),
        filter: None,
    });
    let exec = RuleExecutor::new(plan);

    let left = event(vec![("sip", str_val("10.0.0.1"))]);
    let right = event(vec![("sip", str_val("10.0.0.2"))]);

    let left_alert = exec.execute_each(&left, 1_000_000).unwrap().unwrap();
    let right_alert = exec.execute_each(&right, 1_000_000).unwrap().unwrap();

    assert_ne!(left_alert.wfx_id, right_alert.wfx_id);
}

// =========================================================================
// Test 2: execute_match – arithmetic score
// =========================================================================

#[test]
fn execute_match_arithmetic_score() {
    let score_expr = Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(Expr::Number(50.0)),
        right: Box::new(Expr::Number(20.0)),
    };
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        score_expr,
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();

    let alert = exec.execute_match(&matched).unwrap();
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
}

// =========================================================================
// Test 3: execute_match – entity from simple key
// =========================================================================

#[test]
fn execute_match_entity_simple_key() {
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();

    let alert = exec.execute_match(&matched).unwrap();
    assert_eq!(alert.entity_id, "10.0.0.1");
}

// =========================================================================
// Test 4: execute_match – no keys (global scope)
// =========================================================================

#[test]
fn execute_match_no_keys() {
    use crate::match_engine::match_engine::{MatchedContext, StepData};

    let match_plan = simple_plan(vec![], vec![step(vec![branch("fail", count_ge(1.0))])]);
    // Use a literal string as entity since there are no key fields
    let plan = simple_rule_plan(
        "r1",
        match_plan,
        Expr::Number(50.0),
        "global",
        Expr::StringLit("all".to_string()),
    );
    let exec = RuleExecutor::new(plan);
    let matched = MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec![],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: std::collections::HashMap::new(),
        }],
        bind_data: vec![],
        event_time_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
    };

    let alert = exec.execute_match(&matched).unwrap();
    assert_eq!(alert.entity_id, "all");
    assert!(alert.summary.contains("global"));
}

// =========================================================================
// Test 5: execute_match – composite keys
// =========================================================================

#[test]
fn execute_match_composite_keys() {
    use crate::match_engine::match_engine::{MatchedContext, StepData};

    let match_plan = simple_plan(
        vec![simple_key("sip"), simple_key("dip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let plan = simple_rule_plan(
        "r1",
        match_plan,
        Expr::Number(60.0),
        "ip_pair",
        Expr::Field(FieldRef::Simple("dip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1"), str_val("10.0.0.2")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: std::collections::HashMap::new(),
        }],
        bind_data: vec![],
        event_time_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
    };

    let alert = exec.execute_match(&matched).unwrap();
    assert_eq!(alert.entity_id, "10.0.0.2");
    // wfx_id should be a 16-hex-char content hash
    assert_eq!(alert.wfx_id.len(), 16);
    assert!(alert.wfx_id.chars().all(|c| c.is_ascii_hexdigit()));
}
