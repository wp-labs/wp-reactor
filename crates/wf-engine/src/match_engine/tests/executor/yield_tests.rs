use wf_lang::ast::{BinOp, Expr, FieldRef, ObjectItem, SystemVar};
use wf_lang::plan::{EachPlan, StepPlan, YieldField};

use crate::match_engine::RuleExecutor;
use crate::match_engine::Value;
use crate::match_engine::match_engine::{
    BindData, CloseOutput, CloseReason, StepData,
};

use super::helpers::{default_match_plan, default_matched_context};
use super::super::helpers::*;

// =========================================================================
// Each yield tests
// =========================================================================

#[test]
fn execute_each_yield_can_reference_score() {
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
    plan.yield_plan.fields = vec![YieldField {
        name: "risk_score".to_string(),
        value: Expr::SystemVar(SystemVar::Score),
    }];
    let exec = RuleExecutor::new(plan);

    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "risk_score")
            .map(|(_, value)| value.clone()),
        Some(num(10.0))
    );
}

#[test]
fn execute_each_yield_can_reference_time_system_vars() {
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
    plan.yield_plan.fields = vec![
        YieldField {
            name: "first_seen".to_string(),
            value: Expr::SystemVar(SystemVar::EventFirstTime),
        },
        YieldField {
            name: "last_seen".to_string(),
            value: Expr::SystemVar(SystemVar::EventLastTime),
        },
        YieldField {
            name: "evidence_start_time".to_string(),
            value: Expr::SystemVar(SystemVar::EvidenceStartTime),
        },
        YieldField {
            name: "evidence_end_time".to_string(),
            value: Expr::SystemVar(SystemVar::EvidenceEndTime),
        },
        YieldField {
            name: "rule_window_start".to_string(),
            value: Expr::SystemVar(SystemVar::WindowStartTime),
        },
        YieldField {
            name: "rule_window_end".to_string(),
            value: Expr::SystemVar(SystemVar::WindowEndTime),
        },
        YieldField {
            name: "latest_analysis_time".to_string(),
            value: Expr::SystemVar(SystemVar::EmitTime),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let event_time = 1_234_000_000;

    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), event_time)
        .unwrap()
        .unwrap();

    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value.clone())
    };
    let event_time_ms = event_time / 1_000_000;
    assert_eq!(field("first_seen"), Some(num(event_time_ms as f64)));
    assert_eq!(field("last_seen"), Some(num(event_time_ms as f64)));
    assert_eq!(
        field("evidence_start_time"),
        Some(num(event_time_ms as f64))
    );
    assert_eq!(field("evidence_end_time"), Some(num(event_time_ms as f64)));
    assert_eq!(field("rule_window_start"), Some(num(event_time_ms as f64)));
    assert_eq!(field("rule_window_end"), Some(num(event_time_ms as f64)));

    let Some(Value::Number(emit_time_ms)) = field("latest_analysis_time") else {
        panic!("missing latest_analysis_time");
    };
    assert!(emit_time_ms > 0.0);
    assert!(alert.emit_time.ends_with('Z'));
}

#[test]
fn execute_each_yield_evaluates_structured_object_and_array_literals() {
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
    plan.yield_plan.fields = vec![YieldField {
        name: "risk_context".to_string(),
        value: Expr::Object(vec![
            ObjectItem {
                targets: vec!["score".to_string()],
                type_hint: None,
                value: Expr::SystemVar(SystemVar::Score),
            },
            ObjectItem {
                targets: vec!["source".to_string()],
                type_hint: None,
                value: Expr::Field(FieldRef::Qualified("e".to_string(), "sip".to_string())),
            },
            ObjectItem {
                targets: vec!["tags".to_string()],
                type_hint: None,
                value: Expr::Array(vec![
                    Expr::StringLit("bruteforce".to_string()),
                    Expr::Field(FieldRef::Qualified("e".to_string(), "action".to_string())),
                ]),
            },
        ]),
    }];
    let exec = RuleExecutor::new(plan);

    let alert = exec
        .execute_each(
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("action", str_val("failed")),
            ]),
            1_000_000,
        )
        .unwrap()
        .unwrap();

    let value = alert
        .yield_fields
        .iter()
        .find(|(name, _)| name == "risk_context")
        .map(|(_, value)| value)
        .expect("risk_context");
    let Value::Object(fields) = value else {
        panic!("expected object value, got {value:?}");
    };
    assert_eq!(fields.get("score"), Some(&num(10.0)));
    assert_eq!(fields.get("source"), Some(&str_val("10.0.0.1")));
    assert_eq!(
        fields.get("tags"),
        Some(&Value::Array(vec![
            str_val("bruteforce"),
            str_val("failed")
        ]))
    );
}

#[test]
fn execute_each_yield_failure_is_not_silent() {
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
    plan.yield_plan.fields = vec![YieldField {
        name: "missing".to_string(),
        value: Expr::Field(FieldRef::Simple("does_not_exist".to_string())),
    }];
    let exec = RuleExecutor::new(plan);

    let output = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();

    // fallback: missing field in yield produces empty string
    let field_value = output
        .yield_fields
        .iter()
        .find(|(k, _)| k == "missing")
        .map(|(_, v)| v.clone());
    assert_eq!(field_value, Some(Value::Str("".to_string())));
}

// =========================================================================
// Match yield tests
// =========================================================================

#[test]
fn execute_match_yield_can_reference_score() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "risk_score".to_string(),
        value: Expr::SystemVar(SystemVar::Score),
    }];
    let exec = RuleExecutor::new(plan);

    let alert = exec.execute_match(&default_matched_context()).unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "risk_score")
            .map(|(_, value)| value.clone()),
        Some(num(70.0))
    );
}

#[test]
fn execute_match_yield_can_reference_time_system_vars() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "first_seen".to_string(),
            value: Expr::SystemVar(SystemVar::EventFirstTime),
        },
        YieldField {
            name: "last_seen".to_string(),
            value: Expr::SystemVar(SystemVar::EventLastTime),
        },
        YieldField {
            name: "rule_window_start".to_string(),
            value: Expr::SystemVar(SystemVar::WindowStartTime),
        },
        YieldField {
            name: "rule_window_end".to_string(),
            value: Expr::SystemVar(SystemVar::WindowEndTime),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let mut matched = default_matched_context();
    matched.event_first_time_nanos = 1_000_000_000;
    matched.event_last_time_nanos = 3_000_000_000;
    matched.window_start_time_nanos = 500_000_000;
    matched.window_end_time_nanos = 5_500_000_000;

    let alert = exec.execute_match(&matched).unwrap();

    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value.clone())
    };
    assert_eq!(field("first_seen"), Some(num(1_000.0)));
    assert_eq!(field("last_seen"), Some(num(3_000.0)));
    assert_eq!(field("rule_window_start"), Some(num(500.0)));
    assert_eq!(field("rule_window_end"), Some(num(5_500.0)));
}

#[test]
fn execute_match_yield_can_use_score_inside_builtin_expr() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.126),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "rounded".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "round".to_string(),
                args: vec![Expr::SystemVar(SystemVar::Score), Expr::Number(1.0)],
            },
        },
        YieldField {
            name: "message".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "concat".to_string(),
                args: vec![
                    Expr::StringLit("risk=".to_string()),
                    Expr::SystemVar(SystemVar::Score),
                ],
            },
        },
    ];
    let exec = RuleExecutor::new(plan);

    let alert = exec.execute_match(&default_matched_context()).unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "rounded")
            .map(|(_, value)| value.clone()),
        Some(num(70.1))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "message")
            .map(|(_, value)| value.clone()),
        Some(str_val("risk=70.126"))
    );
}

#[test]
fn execute_match_yield_failure_is_not_silent() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "missing".to_string(),
        value: Expr::Field(FieldRef::Simple("does_not_exist".to_string())),
    }];
    let exec = RuleExecutor::new(plan);

    let output = exec.execute_match(&default_matched_context()).unwrap();

    // fallback: missing field in yield produces empty string
    let field_value = output
        .yield_fields
        .iter()
        .find(|(k, _)| k == "missing")
        .map(|(_, v)| v.clone());
    assert_eq!(field_value, Some(Value::Str("".to_string())));
}

// =========================================================================
// Close yield tests
// =========================================================================

#[test]
fn execute_close_yield_can_reference_score() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "risk_score".to_string(),
        value: Expr::SystemVar(SystemVar::Score),
    }];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
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

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "risk_score")
            .map(|(_, value)| value.clone()),
        Some(num(70.0))
    );
}

#[test]
fn execute_close_yield_can_reference_time_system_vars() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "first_seen".to_string(),
            value: Expr::SystemVar(SystemVar::EventFirstTime),
        },
        YieldField {
            name: "last_seen".to_string(),
            value: Expr::SystemVar(SystemVar::EventLastTime),
        },
        YieldField {
            name: "evidence_start_time".to_string(),
            value: Expr::SystemVar(SystemVar::EvidenceStartTime),
        },
        YieldField {
            name: "evidence_end_time".to_string(),
            value: Expr::SystemVar(SystemVar::EvidenceEndTime),
        },
        YieldField {
            name: "rule_window_start".to_string(),
            value: Expr::SystemVar(SystemVar::WindowStartTime),
        },
        YieldField {
            name: "rule_window_end".to_string(),
            value: Expr::SystemVar(SystemVar::WindowEndTime),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: 3.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: std::collections::HashMap::new(),
        }],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 10_000_000_000,
        event_first_time_nanos: 1_000_000_000,
        event_last_time_nanos: 3_000_000_000,
        window_start_time_nanos: 500_000_000,
        window_end_time_nanos: 10_000_000_000,
        machine_id: String::new(),
        last_event_nanos: 3_000_000_000,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value.clone())
    };
    assert_eq!(field("first_seen"), Some(num(1_000.0)));
    assert_eq!(field("last_seen"), Some(num(3_000.0)));
    assert_eq!(field("evidence_start_time"), Some(num(1_000.0)));
    assert_eq!(field("evidence_end_time"), Some(num(3_000.0)));
    assert_eq!(field("rule_window_start"), Some(num(500.0)));
    assert_eq!(field("rule_window_end"), Some(num(10_000.0)));
}

#[test]
fn execute_close_yield_can_use_count_label_inside_if_and_concat() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch_with_label("x", "hi", count_ge(1.0))])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let count_hi = Expr::FuncCall {
        qualifier: None,
        name: "count".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("hi".to_string()))],
    };
    plan.yield_plan.fields = vec![
        YieldField {
            name: "high_event_count".to_string(),
            value: count_hi.clone(),
        },
        YieldField {
            name: "status".to_string(),
            value: Expr::IfThenElse {
                cond: Box::new(Expr::BinOp {
                    op: BinOp::Ge,
                    left: Box::new(count_hi.clone()),
                    right: Box::new(Expr::Number(2.0)),
                }),
                then_expr: Box::new(Expr::StringLit("high".to_string())),
                else_expr: Box::new(Expr::StringLit("low".to_string())),
            },
        },
        YieldField {
            name: "message".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "concat".to_string(),
                args: vec![Expr::StringLit("cnt=".to_string()), count_hi],
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("hi".to_string()),
            measure_value: 2.0,
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

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "high_event_count")
            .map(|(_, value)| value.clone()),
        Some(num(2.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "status")
            .map(|(_, value)| value.clone()),
        Some(str_val("high"))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "message")
            .map(|(_, value)| value.clone()),
        Some(str_val("cnt=2"))
    );
}

#[test]
fn execute_close_yield_can_use_avg_on_field() {
    use wf_lang::plan::BranchPlan;

    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![StepPlan {
                branches: vec![BranchPlan {
                    label: None,
                    source: "x".to_string(),
                    field: None,
                    guard: None,
                    agg: count_ge(1.0),
                }],
            }],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    let avg_risk = Expr::FuncCall {
        qualifier: None,
        name: "avg".to_string(),
        args: vec![Expr::Field(FieldRef::Qualified(
            "x".to_string(),
            "risk_score".to_string(),
        ))],
    };
    plan.yield_plan.fields = vec![
        YieldField {
            name: "avg_risk_score".to_string(),
            value: avg_risk.clone(),
        },
        YieldField {
            name: "message".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "concat".to_string(),
                args: vec![Expr::StringLit("avg=".to_string()), avg_risk],
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 2.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: std::collections::HashMap::from([(
                "risk_score".to_string(),
                vec![num(20.0), num(40.0)],
            )]),
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

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "avg_risk_score")
            .map(|(_, value)| value.clone()),
        Some(num(30.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "message")
            .map(|(_, value)| value.clone()),
        Some(str_val("avg=30"))
    );
}

#[test]
fn execute_close_yield_can_use_bind_alias_aggregates() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("x", count_ge(1.0))])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "source_avg".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "avg".to_string(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "x".to_string(),
                    "risk_score".to_string(),
                ))],
            },
        },
        YieldField {
            name: "high_event_count".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "count".to_string(),
                args: vec![Expr::Field(FieldRef::Simple("hi".to_string()))],
            },
        },
        YieldField {
            name: "elevated_avg".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "avg".to_string(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "elevated".to_string(),
                    "risk_score".to_string(),
                ))],
            },
        },
        YieldField {
            name: "first_high_action".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "first".to_string(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "hi".to_string(),
                    "action".to_string(),
                ))],
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 2.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: std::collections::HashMap::from([(
                "risk_score".to_string(),
                vec![num(90.0), num(70.0)],
            )]),
        }],
        close_step_data: vec![],
        bind_data: vec![
            BindData {
                alias: "x".to_string(),
                count: 2,
                field_values: std::collections::HashMap::from([(
                    "risk_score".to_string(),
                    vec![num(90.0), num(70.0)],
                )]),
            },
            BindData {
                alias: "hi".to_string(),
                count: 1,
                field_values: std::collections::HashMap::from([(
                    "action".to_string(),
                    vec![str_val("block")],
                )]),
            },
            BindData {
                alias: "elevated".to_string(),
                count: 2,
                field_values: std::collections::HashMap::from([(
                    "risk_score".to_string(),
                    vec![num(90.0), num(70.0)],
                )]),
            },
        ],
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
            .find(|(name, _)| name == "high_event_count")
            .map(|(_, value)| value.clone()),
        Some(num(1.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "elevated_avg")
            .map(|(_, value)| value.clone()),
        Some(num(80.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "first_high_action")
            .map(|(_, value)| value.clone()),
        Some(str_val("block"))
    );
}

#[test]
fn execute_match_yield_can_use_bind_alias_aggregates() {
    use crate::match_engine::match_engine::MatchedContext;

    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("x", count_ge(2.0))])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "source_avg".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "avg".to_string(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "x".to_string(),
                    "risk_score".to_string(),
                ))],
            },
        },
        YieldField {
            name: "high_event_count".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "count".to_string(),
                args: vec![Expr::Field(FieldRef::Simple("hi".to_string()))],
            },
        },
        YieldField {
            name: "elevated_avg".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "avg".to_string(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "elevated".to_string(),
                    "risk_score".to_string(),
                ))],
            },
        },
        YieldField {
            name: "last_high_action".to_string(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "last".to_string(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "hi".to_string(),
                    "action".to_string(),
                ))],
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let matched = MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 2.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: std::collections::HashMap::new(),
        }],
        bind_data: vec![
            BindData {
                alias: "x".to_string(),
                count: 2,
                field_values: std::collections::HashMap::from([(
                    "risk_score".to_string(),
                    vec![num(90.0), num(70.0)],
                )]),
            },
            BindData {
                alias: "hi".to_string(),
                count: 1,
                field_values: std::collections::HashMap::from([(
                    "action".to_string(),
                    vec![str_val("block")],
                )]),
            },
            BindData {
                alias: "elevated".to_string(),
                count: 2,
                field_values: std::collections::HashMap::from([(
                    "risk_score".to_string(),
                    vec![num(90.0), num(70.0)],
                )]),
            },
        ],
        event_time_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
    };

    let alert = exec.execute_match(&matched).unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "source_avg")
            .map(|(_, value)| value.clone()),
        Some(num(80.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "high_event_count")
            .map(|(_, value)| value.clone()),
        Some(num(1.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "elevated_avg")
            .map(|(_, value)| value.clone()),
        Some(num(80.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "last_high_action")
            .map(|(_, value)| value.clone()),
        Some(str_val("block"))
    );
}

#[test]
fn execute_close_yield_can_use_fmt_with_count() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".to_string())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "message".to_string(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".to_string(),
            args: vec![
                Expr::StringLit("{} failed {} times".to_string()),
                Expr::Field(FieldRef::Qualified("fail".to_string(), "sip".to_string())),
                Expr::FuncCall {
                    qualifier: None,
                    name: "count".to_string(),
                    args: vec![Expr::Field(FieldRef::Simple("fail".to_string()))],
                },
            ],
        },
    }];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 3.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: std::collections::HashMap::from([(
                "sip".to_string(),
                vec![
                    str_val("10.0.0.1"),
                    str_val("10.0.0.1"),
                    str_val("10.0.0.1"),
                ],
            )]),
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

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "message")
            .map(|(_, value)| value.clone()),
        Some(str_val("10.0.0.1 failed 3 times"))
    );
}

// =========================================================================
// Close emission regression
// =========================================================================

/// Reproduces the close-emission path for a port_scan-like rule:
/// - CloseMode::And, tracked bind alias "c"
/// - Event step matches (event_ok=true), close step passes (close_ok=true)
/// - Yield references bind alias field `c.sip`
/// - Verifies execute_close produces an OutputRecord with the correct field.
#[test]
fn execute_close_yield_resolves_tracked_bind_alias_field() {
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};
    use std::collections::HashSet;
    use wf_lang::ast::Expr;
    use wf_lang::plan::{BindPlan, EntityPlan, RulePlan, ScorePlan, YieldPlan};

    // Build a port_scan-like MatchPlan
    let mut match_plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("c", count_ge(2.0))])],
        vec![step(vec![branch("c", count_ge(2.0))])],
        std::time::Duration::from_secs(60),
    );
    // Compiler fix: tracked_bind_aliases must contain "c" so
    // collect_alias_event populates field_values (including sip).
    match_plan.tracked_bind_aliases = HashSet::from(["c".to_string()]);

    let rule_plan = RulePlan {
        name: "port_scan".to_string(),
        binds: vec![BindPlan {
            alias: "c".to_string(),
            window: "conn_events".to_string(),
            filter: None,
        }],
        match_plan: match_plan.clone(),
        each_plan: None,
        joins: vec![],
        entity_plan: EntityPlan {
            entity_type: "ip".to_string(),
            entity_id_expr: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "c".into(),
                "sip".into(),
            )),
        },
        yield_plan: YieldPlan {
            target: "network_alerts".to_string(),
            version: None,
            fields: vec![YieldField {
                name: "sip".to_string(),
                value: Expr::Field(wf_lang::ast::FieldRef::Qualified("c".into(), "sip".into())),
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(80.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let exec = RuleExecutor::new(rule_plan);
    let mut sm = CepStateMachine::new("port_scan".to_string(), match_plan, None);

    let base: i64 = 1_700_000_000 * 1_000_000_000i64;
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // First event: accumulates, does not match yet
    assert_eq!(sm.advance_at("c", &e, base), StepResult::Accumulate);
    // Second event: event step matches -> Advance (CloseMode::And)
    assert_eq!(sm.advance_at("c", &e, base + 1), StepResult::Advance);

    // Close the instance — close_all drains all active instances
    let outputs = sm.close_all(CloseReason::Timeout);
    assert!(
        !outputs.is_empty(),
        "close_all should produce at least one output"
    );
    let close = &outputs[0];
    assert!(close.event_ok, "event_ok must be true");
    assert!(close.close_ok, "close_ok must be true");

    // Execute close — this is the path from scan_timeouts → emit
    let result = exec
        .execute_close(close)
        .expect("execute_close should succeed");
    assert!(
        result.is_some(),
        "close should produce an alert (not Ok(None))"
    );

    let alert = result.unwrap();
    assert_eq!(alert.rule_name, "port_scan");
    assert_eq!(alert.entity_id, "10.0.0.1");

    // The yield field c.sip must be resolved from the tracked bind alias
    let sip = alert
        .yield_fields
        .iter()
        .find(|(k, _)| k == "sip")
        .map(|(_, v)| v);
    assert_eq!(
        sip,
        Some(&Value::Str("10.0.0.1".into())),
        "yield field c.sip should resolve to the event's sip value"
    );
}

// =========================================================================
// Stat context yield tests
// =========================================================================

#[test]
fn execute_match_yield_can_use_stat_context_functions() {
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};
    use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

    let input_window = WindowSchema {
        name: "auth_events".to_string(),
        streams: vec!["auth_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    };
    let output_window = WindowSchema {
        name: "out".to_string(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "window_events".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "matched_events".to_string(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "trigger_count".to_string(),
                field_type: FieldType::Base(BaseType::Float),
            },
        ],
    };
    let source = r#"
rule stat_rule {
    events { auth : auth_events }
    match<sip:5m> {
        on event { fail: auth | count >= 2; }
    } -> score(70.0)
    entity(ip, auth.sip)
    yield out (
        sip = auth.sip,
        window_events = stat.count(window_event(auth)),
        matched_events = stat.count(match_event(fail)),
        trigger_count = stat.value(trigger(fail))
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[input_window, output_window])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    assert!(plan.match_plan.tracked_bind_aliases.contains("auth"));

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(
        sm.advance_at("auth", &e, 1_000_000_000),
        StepResult::Accumulate
    );
    let StepResult::Matched(matched) = sm.advance_at("auth", &e, 2_000_000_000) else {
        panic!("expected match");
    };

    let alert = exec.execute_match(&matched).expect("alert");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("window_events"), Some(num(2.0)));
    assert_eq!(field("matched_events"), Some(num(2.0)));
    assert_eq!(field("trigger_count"), Some(num(2.0)));
}

#[test]
fn execute_close_yield_can_use_stat_final_value() {
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};
    use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

    let input_window = WindowSchema {
        name: "auth_events".to_string(),
        streams: vec!["auth_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    };
    let output_window = WindowSchema {
        name: "out".to_string(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "final_hits".to_string(),
                field_type: FieldType::Base(BaseType::Float),
            },
        ],
    };
    let source = r#"
rule stat_close_rule {
    events { req : auth_events  resp : auth_events }
    match<sip:5m> {
        on event { start: req | count >= 1; }
        and close { final_hits: resp | count >= 2; }
    } -> score(70.0)
    entity(ip, req.sip)
    yield out (
        sip = req.sip,
        final_hits = stat.value(final(final_hits))
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[input_window, output_window])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance_at("req", &e, 1_000_000_000), StepResult::Advance);
    assert_eq!(
        sm.advance_at("resp", &e, 2_000_000_000),
        StepResult::Accumulate
    );
    assert_eq!(
        sm.advance_at("resp", &e, 3_000_000_000),
        StepResult::Accumulate
    );

    let close = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Flush)
        .expect("close output");
    assert!(close.event_ok);
    assert!(close.close_ok);

    let alert = exec
        .execute_close(&close)
        .expect("close should execute")
        .expect("close should emit alert");
    let final_hits = alert
        .yield_fields
        .iter()
        .find(|(field_name, _)| field_name == "final_hits")
        .map(|(_, value)| value.clone());

    assert_eq!(final_hits, Some(num(2.0)));
}
