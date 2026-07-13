use wf_lang::ast::{Expr, FieldRef};

use crate::match_engine::RuleExecutor;
use crate::match_engine::match_engine::{CloseOutput, CloseReason};

use super::super::helpers::*;
use super::helpers::default_match_plan;

// =========================================================================
// Test 6: execute_close – both ok
// =========================================================================

#[test]
fn execute_close_both_ok() {
    use crate::match_engine::match_engine::StepData;
    use wf_lang::ast::CloseMode;

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
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 3.0,
            event_first_time_nanos: Some(1_000),
            event_last_time_nanos: Some(3_000),
            collected_values: Vec::new(),
            field_values: std::collections::HashMap::new(),
        }],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 123,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();
    assert_eq!(alert.origin.as_str(), "close:timeout");
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
    assert_eq!(alert.entity_id, "10.0.0.1");
    assert_eq!(alert.event_time_nanos, 123);
}

// =========================================================================
// Test 7: execute_close – close_ok = false
// =========================================================================

#[test]
fn execute_close_close_not_ok() {
    use wf_lang::ast::CloseMode;

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
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Flush,
        event_ok: true,
        close_ok: false,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 0,
    };

    let result = exec.execute_close(&close).unwrap();
    assert!(result.is_none());
}

// =========================================================================
// Test 8: execute_close – event_ok = false
// =========================================================================

#[test]
fn execute_close_event_not_ok() {
    use wf_lang::ast::CloseMode;

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
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Eos,
        event_ok: false,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 0,
    };

    let result = exec.execute_close(&close).unwrap();
    assert!(result.is_none());
}

#[test]
fn execute_close_score_can_use_count_alias() {
    use crate::match_engine::match_engine::StepData;
    use wf_lang::ast::CloseMode;

    let plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("x", count_ge(1.0))])],
        ),
        Expr::FuncCall {
            qualifier: None,
            name: "count".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("x".to_string()))],
        },
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 3.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: std::collections::HashMap::new(),
        }],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 123,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert_eq!(alert.score, 3.0);
}
