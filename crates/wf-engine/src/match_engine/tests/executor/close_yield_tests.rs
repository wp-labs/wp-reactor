//! yield_tests.rs 拆出的 close（收尾执行路径）yield 输出与 close 发射回归测试
//! （2026-09-04；`#[path]` 子模块，经父模块 `use super::*` 复用其导入）。
//!
//! 覆盖：score/时间系统变量/首匹配时间、count 标签与 concat/fmt、avg 与
//! bind-alias 聚合（含对比用 match 变体）、tracked bind 字段解析（port_scan 式
//! close 发射回归）。

use super::*;

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
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "risk_score".into(),
            value: Expr::SystemVar(SystemVar::Score),
        },
        YieldField {
            name: "close_reason".into(),
            value: Expr::WfuMeta(WfuMetaField::CloseReason),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".into(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".into()),
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
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 123,
        row_fields: None,
        row_field_names: None,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "risk_score")
            .map(|(_, value)| value.clone()),
        Some(num(70.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "close_reason")
            .map(|(_, value)| value.clone()),
        Some(str_val("timeout"))
    );
}

#[test]
fn execute_close_yield_can_reference_time_system_vars() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "first_seen".into(),
            value: Expr::SystemVar(SystemVar::EventFirstTime),
        },
        YieldField {
            name: "last_seen".into(),
            value: Expr::SystemVar(SystemVar::EventLastTime),
        },
        YieldField {
            name: "evidence_start_time".into(),
            value: Expr::SystemVar(SystemVar::EvidenceStartTime),
        },
        YieldField {
            name: "evidence_end_time".into(),
            value: Expr::SystemVar(SystemVar::EvidenceEndTime),
        },
        YieldField {
            name: "rule_window_start".into(),
            value: Expr::SystemVar(SystemVar::WindowStartTime),
        },
        YieldField {
            name: "rule_window_end".into(),
            value: Expr::SystemVar(SystemVar::WindowEndTime),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".into(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".into()),
            measure_value: 3.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 10_000_000_000,
        event_first_time_nanos: 1_000_000_000,
        event_last_time_nanos: 3_000_000_000,
        // 候选事件跨度与命中证据跨度独立（issue #82 方案 A）。
        evidence_first_time_nanos: 2_000_000_000,
        evidence_last_time_nanos: 4_000_000_000,
        first_match_time_nanos: None,
        window_start_time_nanos: 500_000_000,
        window_end_time_nanos: 10_000_000_000,
        machine_id: String::new(),
        last_event_nanos: 3_000_000_000,
        row_fields: None,
        row_field_names: None,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };
    assert_eq!(field("first_seen"), Some(num(1_000.0)));
    assert_eq!(field("last_seen"), Some(num(3_000.0)));
    assert_eq!(field("evidence_start_time"), Some(num(2_000.0)));
    assert_eq!(field("evidence_end_time"), Some(num(4_000.0)));
    assert_eq!(field("rule_window_start"), Some(num(500.0)));
    assert_eq!(field("rule_window_end"), Some(num(10_000.0)));
}

#[test]
fn execute_close_yield_can_reference_first_match_time() {
    // issue #82：close 输出路径的 @first_match_time 从 CloseOutput 直通输出。
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "first_match_time".into(),
        value: Expr::SystemVar(SystemVar::FirstMatchTime),
    }];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".into(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".into()),
            measure_value: 3.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 10_000_000_000,
        event_first_time_nanos: 1_000_000_000,
        event_last_time_nanos: 3_000_000_000,
        first_match_time_nanos: Some(2_000_000_000),
        evidence_first_time_nanos: 1_000_000_000,
        evidence_last_time_nanos: 3_000_000_000,
        window_start_time_nanos: 500_000_000,
        window_end_time_nanos: 10_000_000_000,
        machine_id: String::new(),
        last_event_nanos: 3_000_000_000,
        row_fields: None,
        row_field_names: None,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };
    assert_eq!(
        field("first_match_time"),
        Some(num(2_000.0)),
        "close 输出 @first_match_time = 首次命中处理墙钟"
    );
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
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let count_hi = Expr::FuncCall {
        qualifier: None,
        name: "count".into(),
        args: vec![Expr::Field(FieldRef::Simple("hi".into()))],
    };
    plan.yield_plan.fields = vec![
        YieldField {
            name: "high_event_count".into(),
            value: count_hi.clone(),
        },
        YieldField {
            name: "status".into(),
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
            name: "message".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "concat".into(),
                args: vec![Expr::StringLit("cnt=".to_string()), count_hi],
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".into(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("hi".into()),
            measure_value: 2.0,
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
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 123,
        row_fields: None,
        row_field_names: None,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "high_event_count")
            .map(|(_, value)| value.clone()),
        Some(num(2.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "status")
            .map(|(_, value)| value.clone()),
        Some(str_val("high"))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "message")
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
                    source: "x".into(),
                    field: None,
                    guard: None,
                    agg: count_ge(1.0),
                }],
            }],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    let avg_risk = Expr::FuncCall {
        qualifier: None,
        name: "avg".into(),
        args: vec![Expr::Field(FieldRef::Qualified(
            "x".into(),
            "risk_score".into(),
        ))],
    };
    plan.yield_plan.fields = vec![
        YieldField {
            name: "avg_risk_score".into(),
            value: avg_risk.clone(),
        },
        YieldField {
            name: "message".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "concat".into(),
                args: vec![Expr::StringLit("avg=".to_string()), avg_risk],
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".into(),
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
            field_values: EngineHashMap::from_iter([(
                "risk_score".into(),
                vec![num(20.0), num(40.0)],
            )]),
        }],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 123,
        row_fields: None,
        row_field_names: None,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "avg_risk_score")
            .map(|(_, value)| value.clone()),
        Some(num(30.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "message")
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
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "source_avg".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "avg".into(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "x".into(),
                    "risk_score".into(),
                ))],
            },
        },
        YieldField {
            name: "high_event_count".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "count".into(),
                args: vec![Expr::Field(FieldRef::Simple("hi".into()))],
            },
        },
        YieldField {
            name: "elevated_avg".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "avg".into(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "elevated".into(),
                    "risk_score".into(),
                ))],
            },
        },
        YieldField {
            name: "first_high_action".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "first".into(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "hi".into(),
                    "action".into(),
                ))],
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".into(),
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
            field_values: EngineHashMap::from_iter([(
                "risk_score".into(),
                vec![num(90.0), num(70.0)],
            )]),
        }],
        close_step_data: vec![],
        bind_data: vec![
            BindData {
                alias: "x".into(),
                count: 2,
                field_values: EngineHashMap::from_iter([(
                    "risk_score".into(),
                    vec![num(90.0), num(70.0)],
                )]),
            },
            BindData {
                alias: "hi".into(),
                count: 1,
                field_values: EngineHashMap::from_iter([("action".into(), vec![str_val("block")])]),
            },
            BindData {
                alias: "elevated".into(),
                count: 2,
                field_values: EngineHashMap::from_iter([(
                    "risk_score".into(),
                    vec![num(90.0), num(70.0)],
                )]),
            },
        ],
        watermark_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 123,
        row_fields: None,
        row_field_names: None,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "high_event_count")
            .map(|(_, value)| value.clone()),
        Some(num(1.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "elevated_avg")
            .map(|(_, value)| value.clone()),
        Some(num(80.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "first_high_action")
            .map(|(_, value)| value.clone()),
        Some(str_val("block"))
    );
}

#[test]
fn execute_match_yield_can_use_bind_alias_aggregates() {
    use crate::match_engine::cep::MatchedContext;

    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("x", count_ge(2.0))])],
        ),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "source_avg".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "avg".into(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "x".into(),
                    "risk_score".into(),
                ))],
            },
        },
        YieldField {
            name: "high_event_count".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "count".into(),
                args: vec![Expr::Field(FieldRef::Simple("hi".into()))],
            },
        },
        YieldField {
            name: "elevated_avg".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "avg".into(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "elevated".into(),
                    "risk_score".into(),
                ))],
            },
        },
        YieldField {
            name: "last_high_action".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "last".into(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "hi".into(),
                    "action".into(),
                ))],
            },
        },
    ];
    let exec = RuleExecutor::new(plan);
    let matched = MatchedContext {
        rule_name: "r1".into(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 2.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: Vec::new(),
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![
            BindData {
                alias: "x".into(),
                count: 2,
                field_values: EngineHashMap::from_iter([(
                    "risk_score".into(),
                    vec![num(90.0), num(70.0)],
                )]),
            },
            BindData {
                alias: "hi".into(),
                count: 1,
                field_values: EngineHashMap::from_iter([("action".into(), vec![str_val("block")])]),
            },
            BindData {
                alias: "elevated".into(),
                count: 2,
                field_values: EngineHashMap::from_iter([(
                    "risk_score".into(),
                    vec![num(90.0), num(70.0)],
                )]),
            },
        ],
        event_time_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        trigger_event: None,
    };

    let alert = exec.execute_match(&matched).unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "source_avg")
            .map(|(_, value)| value.clone()),
        Some(num(80.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "high_event_count")
            .map(|(_, value)| value.clone()),
        Some(num(1.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "elevated_avg")
            .map(|(_, value)| value.clone()),
        Some(num(80.0))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "last_high_action")
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
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "message".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("{} failed {} times".to_string()),
                Expr::Field(FieldRef::Qualified("fail".into(), "sip".into())),
                Expr::FuncCall {
                    qualifier: None,
                    name: "count".into(),
                    args: vec![Expr::Field(FieldRef::Simple("fail".into()))],
                },
            ],
        },
    }];
    let exec = RuleExecutor::new(plan);
    let close = CloseOutput {
        rule_name: "r1".into(),
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
            field_values: EngineHashMap::from_iter([(
                "sip".into(),
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
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 123,
        row_fields: None,
        row_field_names: None,
    };

    let alert = exec.execute_close(&close).unwrap().unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "message")
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
    use crate::match_engine::cep::{CepStateMachine, StepResult};
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
    match_plan.tracked_bind_aliases = HashSet::from(["c".into()]);

    let rule_plan = RulePlan {
        conv_window: None,
        name: "port_scan".into(),
        binds: vec![BindPlan {
            alias: "c".into(),
            window: "conn_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "c".into(),
                "sip".into(),
            )),
        },
        yield_plan: YieldPlan {
            target: "network_alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "sip".into(),
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
    let mut sm = CepStateMachine::new("port_scan".into(), match_plan, None);

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
    assert_eq!(&*alert.rule_name, "port_scan");
    assert_eq!(alert.entity_id, "10.0.0.1");

    // The yield field c.sip must be resolved from the tracked bind alias
    let sip = alert
        .yield_fields
        .iter()
        .find(|(k, _)| &**k == "sip")
        .map(|(_, v)| v);
    assert_eq!(
        sip,
        Some(&Value::Str("10.0.0.1".into())),
        "yield field c.sip should resolve to the event's sip value"
    );
}
