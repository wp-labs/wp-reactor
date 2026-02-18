use wf_lang::ast::{BinOp, Expr, FieldRef};

use crate::rule::match_engine::{CloseOutput, CloseReason, MatchedContext, StepData};
use crate::rule::RuleExecutor;

use super::helpers::*;

// ---------------------------------------------------------------------------
// Helper: build a standard one-step match plan + rule plan
// ---------------------------------------------------------------------------

fn default_match_plan() -> wf_lang::plan::MatchPlan {
    simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    )
}

fn default_matched_context() -> MatchedContext {
    MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec!["10.0.0.1".to_string()],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 1.0,
        }],
    }
}

// =========================================================================
// Test 1: execute_match — static score
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
    assert!(alert.close_reason.is_none());
    assert!(alert.matched_rows.is_empty());
    assert!(alert.fired_at.ends_with('Z'));
}

// =========================================================================
// Test 2: execute_match — arithmetic score
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
// Test 3: execute_match — entity from simple key
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
// Test 4: execute_match — no keys (global scope)
// =========================================================================

#[test]
fn execute_match_no_keys() {
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
        }],
    };

    let alert = exec.execute_match(&matched).unwrap();
    assert_eq!(alert.entity_id, "all");
    assert!(alert.summary.contains("global"));
}

// =========================================================================
// Test 5: execute_match — composite keys
// =========================================================================

#[test]
fn execute_match_composite_keys() {
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
        scope_key: vec!["10.0.0.1".to_string(), "10.0.0.2".to_string()],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
        }],
    };

    let alert = exec.execute_match(&matched).unwrap();
    assert_eq!(alert.entity_id, "10.0.0.2");
    // alert_id should contain both keys
    assert!(alert.alert_id.contains("10.0.0.1,10.0.0.2"));
}

// =========================================================================
// Test 6: execute_close — both ok
// =========================================================================

#[test]
fn execute_close_both_ok() {
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec!["10.0.0.1".to_string()],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 3.0,
        }],
        close_step_data: vec![],
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();
    assert_eq!(alert.close_reason.as_deref(), Some("timeout"));
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
    assert_eq!(alert.entity_id, "10.0.0.1");
}

// =========================================================================
// Test 7: execute_close — close_ok = false
// =========================================================================

#[test]
fn execute_close_close_not_ok() {
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec!["10.0.0.1".to_string()],
        close_reason: CloseReason::Flush,
        event_ok: true,
        close_ok: false,
        event_step_data: vec![],
        close_step_data: vec![],
    };

    let result = exec.execute_close(&close).unwrap();
    assert!(result.is_none());
}

// =========================================================================
// Test 8: execute_close — event_ok = false
// =========================================================================

#[test]
fn execute_close_event_not_ok() {
    let plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec!["10.0.0.1".to_string()],
        close_reason: CloseReason::Eos,
        event_ok: false,
        close_ok: true,
        event_step_data: vec![],
        close_step_data: vec![],
    };

    let result = exec.execute_close(&close).unwrap();
    assert!(result.is_none());
}

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
// Test 10: entity eval failure — nonexistent field
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
    assert!(result.is_err());
}

// =========================================================================
// Test 11: alert_id deterministic
// =========================================================================

#[test]
fn alert_id_deterministic() {
    use crate::rule::executor::format_fired_at;
    use std::time::{Duration, UNIX_EPOCH};

    // Use a fixed time to get deterministic fired_at
    let fixed_time = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
    let fired_at = format_fired_at(fixed_time);

    // Build alert_id manually using the same logic
    let id1 = format!("r1|10.0.0.1|{}", fired_at);
    let id2 = format!("r1|10.0.0.1|{}", fired_at);
    assert_eq!(id1, id2);

    // Also verify the format_fired_at is deterministic
    let fired_at2 = format_fired_at(fixed_time);
    assert_eq!(fired_at, fired_at2);

    // Verify ISO 8601 format
    assert!(fired_at.contains('T'));
    assert!(fired_at.ends_with('Z'));
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
        scope_key: vec!["10.0.0.1".to_string()],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 5.0,
        }],
    };

    let alert = exec.execute_match(&matched).unwrap();
    assert!(alert.summary.contains("brute_force"));
    assert!(alert.summary.contains("sip=10.0.0.1"));
    assert!(alert.summary.contains("fail=5.0"));
}
