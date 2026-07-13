use wf_lang::ast::{BinOp, Expr, FieldRef};

use crate::match_engine::RuleExecutor;
use crate::match_engine::Value;
use crate::match_engine::match_engine::{MatchedContext, StepData};

use super::super::helpers::*;
use super::helpers::{default_match_plan, default_matched_context};

// =========================================================================
// Test 9: score clamped to [0, 100]
// =========================================================================

#[test]
fn score_clamped_to_range() {
    let plan_high = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(150.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec_high = RuleExecutor::new(plan_high);
    let matched = default_matched_context();

    let alert = exec_high.execute_match(&matched).unwrap();
    assert!((alert.score - 100.0).abs() < f64::EPSILON);

    let plan_low = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(-10.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec_low = RuleExecutor::new(plan_low);

    let alert = exec_low.execute_match(&matched).unwrap();
    assert!(alert.score.abs() < f64::EPSILON); // 0.0
}

// =========================================================================
// Test 10: entity eval failure – nonexistent field
// =========================================================================

#[test]
fn entity_eval_failure() {
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        // Reference a field that doesn't exist in scope_key
        Expr::Field(FieldRef::Simple("nonexistent".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();

    let result = exec.execute_match(&matched);
    // fallback: missing entity field produces empty string instead of error
    assert!(result.is_ok());
}

// =========================================================================
// Test 11: wfx_id deterministic
// =========================================================================

#[test]
fn wfx_id_deterministic() {
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();

    let alert1 = exec.execute_match(&matched).unwrap();
    let alert2 = exec.execute_match(&matched).unwrap();

    // Same inputs produce the same content hash
    assert_eq!(alert1.wfx_id, alert2.wfx_id);
    // 16 hex characters
    assert_eq!(alert1.wfx_id.len(), 16);
    assert!(alert1.wfx_id.chars().all(|c| c.is_ascii_hexdigit()));
}

// =========================================================================
// Test 12: summary format
// =========================================================================

#[test]
fn summary_format() {
    let plan = simple_rule_plan(
        "brute_force",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = MatchedContext {
        rule_name: "brute_force".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 5.0,
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
    assert!(alert.summary.contains("brute_force"));
    assert!(alert.summary.contains("sip=10.0.0.1"));
    assert!(alert.summary.contains("fail=5.0"));
}

// =========================================================================
// Test 13: numeric key preserves type in eval context
// =========================================================================

#[test]
fn numeric_key_preserves_type_in_eval_context() {
    // Use dport=443 as a numeric key, score = dport / 100.
    // If dport is correctly preserved as Value::Number, score = 443/100 = 4.43.
    // If dport were stringified ("443") then the division would fail.
    let match_plan = simple_plan(
        vec![simple_key("dport")],
        vec![step(vec![branch("conn", count_ge(1.0))])],
    );
    let score_expr = Expr::BinOp {
        op: BinOp::Div,
        left: Box::new(Expr::Field(FieldRef::Simple("dport".to_string()))),
        right: Box::new(Expr::Number(100.0)),
    };
    let plan = simple_rule_plan(
        "r_numeric_key",
        match_plan,
        score_expr,
        "port",
        Expr::Field(FieldRef::Simple("dport".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = MatchedContext {
        rule_name: "r_numeric_key".to_string(),
        scope_key: vec![num(443.0)],
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
    // score = 443.0 / 100.0 = 4.43, clamped to [0, 100]
    assert!((alert.score - 4.43).abs() < f64::EPSILON);
    assert_eq!(alert.entity_id, "443");
}

// =========================================================================
// Test 14: label does not overwrite key in eval context
// =========================================================================

#[test]
fn label_cannot_overwrite_key_in_eval_context() {
    // Key "sip" = "10.0.0.1" (string), label also named "sip" with measure 99.0.
    // entity(ip, sip) should resolve to "10.0.0.1" (the key), not "99" (the label).
    let match_plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch_with_label("fail", "sip", count_ge(1.0))])],
    );
    let plan = simple_rule_plan(
        "r1",
        match_plan,
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("sip".to_string()),
            measure_value: 99.0,
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
    // Key must win: entity_id should be "10.0.0.1", not "99"
    assert_eq!(alert.entity_id, "10.0.0.1");
}

// =========================================================================
// Test 15: wfx_id is valid 16-hex format with no separators
// =========================================================================

#[test]
fn wfx_id_hex_format() {
    let match_plan = simple_plan(
        vec![simple_key("tag")],
        vec![step(vec![branch("src", count_ge(1.0))])],
    );
    let plan = simple_rule_plan(
        "r1",
        match_plan,
        Expr::Number(50.0),
        "tag",
        Expr::Field(FieldRef::Simple("tag".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let matched = MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("a,b|c")],
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
    // wfx_id is exactly 16 hex characters, no separators
    assert_eq!(alert.wfx_id.len(), 16);
    assert!(
        alert.wfx_id.chars().all(|c| c.is_ascii_hexdigit()),
        "wfx_id should be all hex digits, got: {}",
        alert.wfx_id
    );
    // No structural separators
    assert!(!alert.wfx_id.contains('|'));
    assert!(!alert.wfx_id.contains('#'));
}

// -- build_machine_id / build_scope_key ---------------------------------

#[test]
fn build_machine_id_and_scope_key() {
    let plan = simple_rule_plan(
        "test_rule",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".to_string(), "sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    assert_eq!(exec.build_machine_id(""), "test_rule");
    assert_eq!(exec.build_machine_id("10.0.0.1"), "10.0.0.1");
    assert_eq!(
        exec.build_scope_key(
            &[
                FieldRef::Simple("sip".to_string()),
                FieldRef::Simple("user".to_string())
            ],
            &[
                Value::Str("10.0.0.1".to_string()),
                Value::Str("admin".to_string())
            ],
        ),
        "sip=10.0.0.1,user=admin"
    );
}
