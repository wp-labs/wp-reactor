use wf_lang::ast::{Expr, FieldRef};

use crate::match_engine::EngineHashMap;
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
            field_values: EngineHashMap::default(),
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
            field_values: EngineHashMap::default(),
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

// =========================================================================
// Nested field paths (wp-labs/warp-fusion#64) — close yield path
// =========================================================================

#[test]
fn execute_close_yield_nested_path_via_bind_data() {
    use std::collections::HashMap;

    use wf_lang::ast::{CloseMode, PathSegment, SystemVar};
    use wf_lang::plan::YieldField;
    use wf_lang::{BaseType, FieldType};

    use crate::match_engine::Value;
    use crate::match_engine::match_engine::{BindData, StepData};

    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.binds[0].alias = "e".to_string();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "uid".to_string(),
            value: Expr::Field(FieldRef::Path {
                alias: "e".to_string(),
                segments: vec![
                    PathSegment::Field("roles_obj".to_string()),
                    PathSegment::Field("source".to_string()),
                    PathSegment::Field("process".to_string()),
                    PathSegment::Field("uid".to_string()),
                ],
            }),
        },
        YieldField {
            name: "fail_count".to_string(),
            value: Expr::SystemVar(SystemVar::Score),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("uid".to_string(), FieldType::Base(BaseType::Chars)),
            ("fail_count".to_string(), FieldType::Base(BaseType::Digit)),
        ]),
    );
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
            field_values: EngineHashMap::default(),
        }],
        close_step_data: vec![],
        bind_data: vec![BindData {
            alias: "e".to_string(),
            count: 3,
            field_values: EngineHashMap::from_iter([(
                "roles_obj".to_string(),
                vec![Value::Object(EngineHashMap::from_iter([(
                    "source".into(),
                    Value::Object(EngineHashMap::from_iter([(
                        "process".into(),
                        Value::Object(EngineHashMap::from_iter([(
                            "uid".into(),
                            str_val("d22b3fbcb9e77cb86834f6a18e2e0f68"),
                        )])),
                    )])),
                )]))],
            )]),
        }],
        watermark_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 123,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "uid")
            .map(|(_, v)| v.clone()),
        Some(str_val("d22b3fbcb9e77cb86834f6a18e2e0f68")),
        "close-rule nested path leaf must be extracted from tracked bind field"
    );
    assert_eq!(alert.score, 70.0);
}

#[test]
fn execute_close_yield_nested_path_missing_bind_omits() {
    use std::collections::HashMap;

    use wf_lang::ast::{CloseMode, PathSegment};
    use wf_lang::plan::YieldField;
    use wf_lang::{BaseType, FieldType};

    use crate::match_engine::match_engine::StepData;

    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.binds[0].alias = "e".to_string();
    plan.yield_plan.fields = vec![YieldField {
        name: "risk_score".to_string(),
        value: Expr::Field(FieldRef::Path {
            alias: "e".to_string(),
            segments: vec![
                PathSegment::Field("roles_obj".to_string()),
                PathSegment::Field("risk".to_string()),
            ],
        }),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("risk_score".to_string(), FieldType::Base(BaseType::Float))]),
    );
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
            field_values: EngineHashMap::default(),
        }],
        close_step_data: vec![],
        bind_data: vec![], // no roles_obj collected
        watermark_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 123,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert!(
        !alert.yield_fields.iter().any(|(n, _)| n == "risk_score"),
        "missing nested path into a float target must be omitted in close yield"
    );
}
