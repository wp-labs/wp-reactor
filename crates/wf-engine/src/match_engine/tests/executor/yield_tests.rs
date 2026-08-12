use std::collections::HashMap;

use wf_config::OutputConfig;
use wf_lang::ast::{BinOp, Expr, FieldRef, ObjectItem, PathSegment, SystemVar};
use wf_lang::plan::{EachPlan, StepPlan, YieldField};
use wf_lang::wfu_meta::WfuMetaField;
use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use crate::match_engine::Value;
use crate::match_engine::match_engine::{BindData, CloseOutput, CloseReason, StepData};
use crate::match_engine::{RuleExecutor, RuleExecutorOptions};

use super::super::helpers::*;
use super::helpers::{default_match_plan, default_matched_context};

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
fn execute_each_yield_coerces_score_to_target_chars_field() {
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
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("risk_score".to_string(), FieldType::Base(BaseType::Chars))]),
    );

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
        Some(str_val("10"))
    );
}

#[test]
fn execute_each_yield_validates_time_target_and_keeps_epoch_millis_value() {
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
        name: "first_seen".to_string(),
        value: Expr::SystemVar(SystemVar::EventFirstTime),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("first_seen".to_string(), FieldType::Base(BaseType::Time))]),
    );

    let alert = exec
        .execute_each(
            &event(vec![("sip", str_val("10.0.0.1"))]),
            1_700_000_000_123_000_000,
        )
        .unwrap()
        .unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "first_seen")
            .map(|(_, value)| value.clone()),
        Some(num(1_700_000_000_123.0))
    );
}

#[test]
fn execute_each_yield_rejects_chars_to_time_target() {
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
        name: "first_seen".to_string(),
        value: Expr::StringLit("2023-11-14 22:13:20".to_string()),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("first_seen".to_string(), FieldType::Base(BaseType::Time))]),
    );

    let err = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .expect_err("chars to time must require explicit parsing");

    assert!(
        err.to_string().contains("explicit time expression"),
        "{err}"
    );
}

#[test]
fn execute_each_strftime_uses_project_default_time_format() {
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
        name: "event_year".to_string(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "strftime".to_string(),
            args: vec![Expr::SystemVar(SystemVar::EventFirstTime)],
        },
    }];
    let exec = RuleExecutor::new_with_options(
        plan,
        RuleExecutorOptions {
            yield_field_types: HashMap::from([(
                "event_year".to_string(),
                FieldType::Base(BaseType::Chars),
            )]),
            output: OutputConfig {
                time_format: "%Y".to_string(),
                ..OutputConfig::default()
            },
        },
    );

    let alert = exec
        .execute_each(
            &event(vec![("sip", str_val("10.0.0.1"))]),
            1_700_000_000_123_000_000,
        )
        .unwrap()
        .unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "event_year")
            .map(|(_, value)| value.clone()),
        Some(str_val("2023"))
    );
}

#[test]
fn execute_each_yield_can_map_wfu_meta_to_plain_fields() {
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
            name: "rule_name".to_string(),
            value: Expr::WfuMeta(WfuMetaField::RuleName),
        },
        YieldField {
            name: "score".to_string(),
            value: Expr::WfuMeta(WfuMetaField::Score),
        },
    ];
    let exec = RuleExecutor::new(plan);

    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();

    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value.clone())
    };
    assert_eq!(field("rule_name"), Some(str_val("r1")));
    assert_eq!(field("score"), Some(num(10.0)));
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
fn execute_each_yield_merges_input_object_with_extension() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "extensions".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "merge".into(),
            args: vec![
                Expr::Field(FieldRef::Qualified(
                    "e".into(),
                    "extension".into(),
                )),
                Expr::Object(vec![
                    ObjectItem {
                        targets: vec!["source".to_string()],
                        type_hint: None,
                        value: Expr::StringLit("wfl".to_string()),
                    },
                    ObjectItem {
                        targets: vec!["severity".to_string()],
                        type_hint: None,
                        value: Expr::Number(10.0),
                    },
                ]),
            ],
        },
    }];
    let exec = RuleExecutor::new(plan);

    let mut extension = HashMap::new();
    extension.insert("severity".into(), num(3.0));
    extension.insert("rules".into(), Value::Array(vec![str_val("webshell")]));
    let alert = exec
        .execute_each(
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("extension", Value::Object(extension)),
            ]),
            1_000_000,
        )
        .unwrap()
        .unwrap();

    let value = alert
        .yield_fields
        .iter()
        .find(|(name, _)| name == "extensions")
        .map(|(_, value)| value)
        .expect("extensions");
    let Value::Object(fields) = value else {
        panic!("expected object value, got {value:?}");
    };
    assert_eq!(fields.get("source"), Some(&str_val("wfl")));
    assert_eq!(fields.get("severity"), Some(&num(10.0)));
    assert_eq!(
        fields.get("rules"),
        Some(&Value::Array(vec![str_val("webshell")]))
    );
}

#[test]
fn execute_each_yield_passes_input_object_through() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "extensions".into(),
        value: Expr::Field(FieldRef::Qualified(
            "e".into(),
            "extension".into(),
        )),
    }];
    let exec = RuleExecutor::new(plan);

    let mut detection = HashMap::new();
    detection.insert("severity".into(), num(10.0));
    detection.insert(
        "tags".into(),
        Value::Array(vec![str_val("os:linux"), str_val("webshell")]),
    );
    let mut extension = HashMap::new();
    extension.insert("detection".into(), Value::Object(detection));

    let alert = exec
        .execute_each(
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("extension", Value::Object(extension)),
            ]),
            1_000_000,
        )
        .unwrap()
        .unwrap();

    let value = alert
        .yield_fields
        .iter()
        .find(|(name, _)| name == "extensions")
        .map(|(_, value)| value)
        .expect("extensions");
    let Value::Object(fields) = value else {
        panic!("expected object value, got {value:?}");
    };
    let Some(Value::Object(detection)) = fields.get("detection") else {
        panic!("expected nested detection object, got {fields:?}");
    };
    assert_eq!(detection.get("severity"), Some(&num(10.0)));
    assert_eq!(
        detection.get("tags"),
        Some(&Value::Array(vec![
            str_val("os:linux"),
            str_val("webshell")
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
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "missing".into(),
        value: Expr::Field(FieldRef::Simple("does_not_exist".into())),
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
    assert_eq!(field_value, Some(Value::Str("".into())));
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
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "risk_score".into(),
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
            name: "rule_window_start".into(),
            value: Expr::SystemVar(SystemVar::WindowStartTime),
        },
        YieldField {
            name: "rule_window_end".into(),
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
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "rounded".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "round".into(),
                args: vec![Expr::SystemVar(SystemVar::Score), Expr::Number(1.0)],
            },
        },
        YieldField {
            name: "message".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "concat".into(),
                args: vec![
                    Expr::StringLit("risk=".to_string()),
                    Expr::SystemVar(SystemVar::Score),
                ],
            },
        },
        YieldField {
            name: "rule_message".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "concat".into(),
                args: vec![
                    Expr::WfuMeta(WfuMetaField::RuleName),
                    Expr::StringLit("-alert".to_string()),
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
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "rule_message")
            .map(|(_, value)| value.clone()),
        Some(str_val("r1-alert"))
    );
}

#[test]
fn execute_match_yield_failure_is_not_silent() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "missing".into(),
        value: Expr::Field(FieldRef::Simple("does_not_exist".into())),
    }];
    let exec = RuleExecutor::new(plan);

    let output = exec.execute_match(&default_matched_context()).unwrap();

    // fallback: missing field in yield produces empty string
    let field_value = output
        .yield_fields
        .iter()
        .find(|(k, _)| k == "missing")
        .map(|(_, v)| v.clone());
    assert_eq!(field_value, Some(Value::Str("".into())));
}

#[test]
fn execute_match_missing_optional_float_field_is_omitted_not_fatal() {
    // wp-labs/warp-fusion#62, match path: a typed optional float field that is
    // missing from the event must be omitted, not fail the match output.
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "attacker_latitude".into(),
        value: Expr::Field(FieldRef::Qualified(
            "e".into(),
            "attacker_latitude".into(),
        )),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([(
            "attacker_latitude".into(),
            FieldType::Base(BaseType::Float),
        )]),
    );

    let output = exec.execute_match(&default_matched_context()).unwrap();
    assert!(
        !output
            .yield_fields
            .iter()
            .any(|(k, _)| k == "attacker_latitude"),
        "missing typed float field should be omitted from match output"
    );
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
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| name == "close_reason")
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
            field_values: std::collections::HashMap::from([(
                "risk_score".into(),
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
            field_values: std::collections::HashMap::from([(
                "risk_score".into(),
                vec![num(90.0), num(70.0)],
            )]),
        }],
        close_step_data: vec![],
        bind_data: vec![
            BindData {
                alias: "x".into(),
                count: 2,
                field_values: std::collections::HashMap::from([(
                    "risk_score".into(),
                    vec![num(90.0), num(70.0)],
                )]),
            },
            BindData {
                alias: "hi".into(),
                count: 1,
                field_values: std::collections::HashMap::from([(
                    "action".into(),
                    vec![str_val("block")],
                )]),
            },
            BindData {
                alias: "elevated".into(),
                count: 2,
                field_values: std::collections::HashMap::from([(
                    "risk_score".into(),
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
            field_values: std::collections::HashMap::new(),
        }],
        bind_data: vec![
            BindData {
                alias: "x".into(),
                count: 2,
                field_values: std::collections::HashMap::from([(
                    "risk_score".into(),
                    vec![num(90.0), num(70.0)],
                )]),
            },
            BindData {
                alias: "hi".into(),
                count: 1,
                field_values: std::collections::HashMap::from([(
                    "action".into(),
                    vec![str_val("block")],
                )]),
            },
            BindData {
                alias: "elevated".into(),
                count: 2,
                field_values: std::collections::HashMap::from([(
                    "risk_score".into(),
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
            field_values: std::collections::HashMap::from([(
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
    match_plan.tracked_bind_aliases = HashSet::from(["c".into()]);

    let rule_plan = RulePlan {
        name: "port_scan".into(),
        binds: vec![BindPlan {
            alias: "c".into(),
            window: "conn_events".into(),
            filter: None,
        }],
        match_plan: match_plan.clone(),
        each_plan: None,
        joins: vec![],
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
        name: "auth_events".into(),
        streams: vec!["auth_stream".into()],
        time_field: Some("event_time".into()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "event_time".into(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    };
    let output_window = WindowSchema {
        name: "out".into(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "window_events".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "matched_events".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "trigger_count".into(),
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

fn evidence_input_window() -> WindowSchema {
    WindowSchema {
        name: "auth_events".into(),
        streams: vec!["auth_stream".into()],
        time_field: Some("event_time".into()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "event_id".into(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "event_time".into(),
                field_type: FieldType::Base(BaseType::Time),
            },
            FieldDef {
                name: "weight".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    }
}

fn evidence_output_window() -> WindowSchema {
    WindowSchema {
        name: "out".into(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "event_count".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "evidences".into(),
                field_type: FieldType::Array(BaseType::Chars),
            },
        ],
    }
}

fn evidence_event(event_id: &str) -> Value {
    str_val(event_id)
}

#[test]
fn execute_match_yield_collects_window_event_ids() {
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};

    let source = r#"
rule evidence_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event { hit: s | count >= 6; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    assert!(
        plan.match_plan
            .tracked_bind_fields
            .get("s")
            .is_some_and(|fields| fields.contains("event_id"))
    );

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let mut matched = None;
    for i in 0..6 {
        let event_id = format!("evt_{:03}", i + 1);
        let step = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", evidence_event(&event_id)),
            ]),
            (i as i64 + 1) * 1_000_000_000,
        );
        if i < 5 {
            assert_eq!(step, StepResult::Accumulate);
        } else {
            let StepResult::Matched(ctx) = step else {
                panic!("sixth event should trigger");
            };
            matched = Some(ctx);
        }
    }

    let alert = exec
        .execute_match(&matched.expect("matched context"))
        .expect("alert");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("event_count"), Some(num(6.0)));
    assert_eq!(
        field("evidences"),
        Some(Value::Array(vec![
            str_val("evt_001"),
            str_val("evt_002"),
            str_val("evt_003"),
            str_val("evt_004"),
            str_val("evt_005"),
            str_val("evt_006"),
        ]))
    );
}

#[test]
fn execute_match_accu_outputs_running_count_and_accumulating_evidence() {
    // `on event<accu>` end-to-end (wp-labs/warp-fusion#65): 5 events, threshold
    // 2 → 4 alerts with event_count 2,3,4,5 and evidence growing each fire.
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};

    let source = r#"
rule accu_evidence {
    events { s : auth_events }
    match<sip:100s> {
        on event<accu> { hit: s | count >= 2; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    assert!(plan.match_plan.accu, "on event<accu> must set plan.accu");

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let mut counts = Vec::new();
    let mut evidences = Vec::new();
    for i in 0..5 {
        let event_id = format!("evt_{:03}", i + 1);
        let step = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", evidence_event(&event_id)),
            ]),
            (i as i64 + 1) * 1_000_000_000,
        );
        if let StepResult::Matched(ctx) = step {
            let alert = exec.execute_match(&ctx).expect("alert");
            let field = |name: &str| {
                alert
                    .yield_fields
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, v)| v.clone())
            };
            counts.push(field("event_count"));
            evidences.push(field("evidences"));
        }
    }

    assert_eq!(
        counts,
        vec![
            Some(num(2.0)),
            Some(num(3.0)),
            Some(num(4.0)),
            Some(num(5.0)),
        ],
        "accu must output the running cumulative count"
    );
    assert_eq!(
        evidences,
        vec![
            Some(Value::Array(vec![str_val("evt_001"), str_val("evt_002")])),
            Some(Value::Array(vec![
                str_val("evt_001"),
                str_val("evt_002"),
                str_val("evt_003"),
            ])),
            Some(Value::Array(vec![
                str_val("evt_001"),
                str_val("evt_002"),
                str_val("evt_003"),
                str_val("evt_004"),
            ])),
            Some(Value::Array(vec![
                str_val("evt_001"),
                str_val("evt_002"),
                str_val("evt_003"),
                str_val("evt_004"),
                str_val("evt_005"),
            ])),
        ],
        "accu evidence must accumulate across fires"
    );
}

#[test]
fn execute_match_yield_dedups_window_event_ids() {
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};

    let source = r#"
rule evidence_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event { hit: s | count >= 6; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let ids = [
        "evt_001", "evt_002", "evt_002", "evt_003", "evt_001", "evt_004",
    ];
    let mut matched = None;
    for (i, event_id) in ids.iter().enumerate() {
        let step = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", evidence_event(event_id)),
            ]),
            (i as i64 + 1) * 1_000_000_000,
        );
        if i < 5 {
            assert_eq!(step, StepResult::Accumulate);
        } else {
            let StepResult::Matched(ctx) = step else {
                panic!("sixth event should trigger");
            };
            matched = Some(ctx);
        }
    }

    let alert = exec
        .execute_match(&matched.expect("matched context"))
        .expect("alert");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("event_count"), Some(num(6.0)));
    assert_eq!(
        field("evidences"),
        Some(Value::Array(vec![
            str_val("evt_001"),
            str_val("evt_002"),
            str_val("evt_003"),
            str_val("evt_004"),
        ]))
    );
}

#[test]
fn execute_match_yield_missing_window_event_ids_returns_empty_evidences() {
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};

    let source = r#"
rule evidence_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event { hit: s.weight | sum >= 6; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    assert!(
        plan.match_plan
            .tracked_bind_fields
            .get("s")
            .is_some_and(|fields| fields.contains("event_id"))
    );

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let mut matched = None;
    for i in 0..6 {
        let step = sm.advance_at(
            "s",
            &event(vec![("sip", str_val("10.0.0.1")), ("weight", num(1.0))]),
            (i as i64 + 1) * 1_000_000_000,
        );
        if i < 5 {
            assert_eq!(step, StepResult::Accumulate);
        } else {
            let StepResult::Matched(ctx) = step else {
                panic!("sixth event should trigger");
            };
            matched = Some(ctx);
        }
    }

    let alert = exec
        .execute_match(&matched.expect("matched context"))
        .expect("alert");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("event_count"), Some(num(6.0)));
    assert_eq!(field("evidences"), Some(Value::Array(vec![])));
}

#[test]
fn execute_match_yield_caps_window_event_ids_to_recent_sample() {
    use crate::match_engine::match_engine::CepStateMachine;

    let source = r#"
rule evidence_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event { hit: s | count >= 2065; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let mut matched = None;
    for i in 0..2065 {
        let event_id = format!("evt_{:04}", i);
        if let crate::match_engine::match_engine::StepResult::Matched(ctx) = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", evidence_event(&event_id)),
            ]),
            (i as i64 + 1) * 1_000_000,
        ) {
            matched = Some(ctx);
        }
    }

    let alert = exec
        .execute_match(&matched.expect("matched context"))
        .expect("alert");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("event_count"), Some(num(2065.0)));
    let Some(Value::Array(evidences)) = field("evidences") else {
        panic!("evidences should be an array");
    };
    assert_eq!(evidences.len(), 1024);
    assert_eq!(evidences.first(), Some(&str_val("evt_1041")));
    assert_eq!(evidences.last(), Some(&str_val("evt_2064")));
}

#[test]
fn execute_close_yield_collects_window_event_ids() {
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};

    let source = r#"
rule evidence_close_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event { hit: s | count >= 6; }
        and close { final_hit: s | count >= 6; }
    } -> score(70.0)
    entity(ip, s.sip)
    yield out (
        sip = s.sip,
        event_count = stat.count(window_event(s)),
        evidences = collect_set(s.event_id)
    )
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[evidence_input_window(), evidence_output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    for i in 0..6 {
        let event_id = format!("evt_{:03}", i + 1);
        let step = sm.advance_at(
            "s",
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("event_id", evidence_event(&event_id)),
            ]),
            (i as i64 + 1) * 1_000_000_000,
        );
        if i < 5 {
            assert_eq!(step, StepResult::Accumulate);
        } else {
            assert_eq!(step, StepResult::Advance);
        }
    }

    let outputs = sm.close_all(CloseReason::Timeout);
    assert_eq!(outputs.len(), 1);
    let alert = exec
        .execute_close(&outputs[0])
        .expect("close should execute")
        .expect("close should emit");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| field_name == name)
            .map(|(_, value)| value.clone())
    };

    assert_eq!(field("event_count"), Some(num(6.0)));
    assert_eq!(
        field("evidences"),
        Some(Value::Array(vec![
            str_val("evt_001"),
            str_val("evt_002"),
            str_val("evt_003"),
            str_val("evt_004"),
            str_val("evt_005"),
            str_val("evt_006"),
        ]))
    );
}

#[test]
fn execute_close_yield_can_use_stat_final_value() {
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};
    use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

    let input_window = WindowSchema {
        name: "auth_events".into(),
        streams: vec!["auth_stream".into()],
        time_field: Some("event_time".into()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "event_time".into(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    };
    let output_window = WindowSchema {
        name: "out".into(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "final_hits".into(),
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

// =========================================================================
// Missing optional fields (wp-labs/warp-fusion#62)
// =========================================================================

#[test]
fn execute_each_missing_optional_float_field_is_omitted_not_fatal() {
    // A yield passthrough of an optional float field that is missing from the
    // input must omit the field from the output record instead of failing the
    // whole record. Explicit NaN/Infinity must still fail (handled in the
    // coercion branch), but "absent" is not a data-format error.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "attacker_latitude".into(),
        value: Expr::Field(FieldRef::Qualified(
            "e".into(),
            "attacker_latitude".into(),
        )),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([(
            "attacker_latitude".into(),
            FieldType::Base(BaseType::Float),
        )]),
    );

    // Input event has no `attacker_latitude` field at all.
    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .expect("missing optional field must not fail the yield")
        .expect("on each should still emit an output record");

    assert!(
        !alert
            .yield_fields
            .iter()
            .any(|(name, _)| name == "attacker_latitude"),
        "missing optional float field should be omitted from output"
    );
}

// =========================================================================
// Missing optional fields — present / explicit-NaN / other-fields cases
// (wp-labs/warp-fusion#62)

#[test]
fn execute_each_present_float_field_outputs_normally() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "attacker_latitude".into(),
        value: Expr::Field(FieldRef::Qualified(
            "e".into(),
            "attacker_latitude".into(),
        )),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([(
            "attacker_latitude".into(),
            FieldType::Base(BaseType::Float),
        )]),
    );

    // Present and finite → the field is output unchanged.
    let alert = exec
        .execute_each(
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("attacker_latitude", num(37.7749)),
            ]),
            1_000_000,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "attacker_latitude")
            .map(|(_, v)| v.clone()),
        Some(num(37.7749))
    );
}

#[test]
fn execute_each_explicit_nan_float_still_fails() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "attacker_latitude".into(),
        value: Expr::Number(f64::NAN),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([(
            "attacker_latitude".into(),
            FieldType::Base(BaseType::Float),
        )]),
    );

    // Explicit NaN is a genuine data-format error, not an absent value.
    let result = exec.execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000);
    assert!(result.is_err(), "explicit NaN must still fail the yield");
}

#[test]
fn execute_each_missing_optional_field_keeps_other_fields() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "attacker_latitude".into(),
            value: Expr::Field(FieldRef::Qualified(
                "e".into(),
                "attacker_latitude".into(),
            )),
        },
        YieldField {
            name: "sip".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            (
                "attacker_latitude".into(),
                FieldType::Base(BaseType::Float),
            ),
            ("sip".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );

    // `attacker_latitude` missing; `sip` present. Only the missing one is
    // omitted; `sip` still emits.
    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();
    assert!(
        !alert
            .yield_fields
            .iter()
            .any(|(n, _)| n == "attacker_latitude"),
        "missing float field omitted"
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "sip")
            .map(|(_, v)| v.clone()),
        Some(str_val("10.0.0.1")),
        "present sibling field still emitted"
    );
}

#[test]
fn execute_each_missing_optional_digit_field_is_omitted() {
    // The empty-string guard applies to every non-chars base type, not just float.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "fail_count".into(),
        value: Expr::Field(FieldRef::Qualified(
            "e".into(),
            "fail_count".into(),
        )),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("fail_count".into(), FieldType::Base(BaseType::Digit))]),
    );

    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();
    assert!(!alert.yield_fields.iter().any(|(n, _)| n == "fail_count"));
}

#[test]
fn execute_each_missing_chars_field_degrades_to_empty_string() {
    // Chars is exempt from the omit guard: a missing chars field still degrades
    // to the empty-string fallback (unchanged behavior).
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "message".into(),
        value: Expr::Field(FieldRef::Qualified("e".into(), "message".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("message".into(), FieldType::Base(BaseType::Chars))]),
    );

    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "message")
            .map(|(_, v)| v.clone()),
        Some(Value::Str(String::new().into()))
    );
}

#[test]
fn execute_close_missing_optional_float_field_is_omitted_not_fatal() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "attacker_latitude".into(),
        value: Expr::Field(FieldRef::Qualified(
            "e".into(),
            "attacker_latitude".into(),
        )),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([(
            "attacker_latitude".into(),
            FieldType::Base(BaseType::Float),
        )]),
    );
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

    let alert = exec
        .execute_close(&close)
        .expect("close yield must not fail on a missing optional field")
        .expect("close should emit an output record");
    assert!(
        !alert
            .yield_fields
            .iter()
            .any(|(n, _)| n == "attacker_latitude"),
        "missing typed float field should be omitted from close output"
    );
}

// =========================================================================
// Nested field paths (wp-labs/warp-fusion#64)
// =========================================================================

fn nested_each_executor() -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "uid".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "e".into(),
                segments: vec![
                    PathSegment::Field("roles_obj".into()),
                    PathSegment::Field("source".into()),
                    PathSegment::Field("process".into()),
                    PathSegment::Field("uid".into()),
                ],
            }),
        },
        YieldField {
            name: "sip".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
    ];
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("uid".into(), FieldType::Base(BaseType::Chars)),
            ("sip".into(), FieldType::Base(BaseType::Chars)),
        ]),
    )
}

fn nested_roles_value() -> Value {
    Value::Object(HashMap::from([(
        "source".into(),
        Value::Object(HashMap::from([(
            "process".into(),
            Value::Object(HashMap::from([(
                "uid".into(),
                str_val("d22b3fbcb9e77cb86834f6a18e2e0f68"),
            )])),
        )])),
    )]))
}

#[test]
fn execute_each_yield_nested_object_path() {
    let exec = nested_each_executor();
    let alert = exec
        .execute_each(
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("roles_obj", nested_roles_value()),
            ]),
            1_000_000,
        )
        .unwrap()
        .unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "uid")
            .map(|(_, v)| v.clone()),
        Some(str_val("d22b3fbcb9e77cb86834f6a18e2e0f68")),
        "nested path leaf must be extracted"
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "sip")
            .map(|(_, v)| v.clone()),
        Some(str_val("10.0.0.1")),
        "sibling field still emitted"
    );
}

#[test]
fn execute_each_yield_nested_path_missing_yields_empty() {
    let exec = nested_each_executor();
    // No `roles_obj` field in the input event.
    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();

    // A chars-targeted missing nested path degrades to the empty string (same
    // convention as missing scalar chars fields) and must not fail the record.
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "uid")
            .map(|(_, v)| v.clone()),
        Some(Value::Str(String::new().into())),
        "missing nested path must not fail the record"
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "sip")
            .map(|(_, v)| v.clone()),
        Some(str_val("10.0.0.1"))
    );
}

#[test]
fn execute_each_yield_nested_missing_numeric_omits_field() {
    // A non-chars target omits a missing nested path entirely (issue #64).
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "risk_score".into(),
        value: Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("risk".into()),
            ],
        }),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("risk_score".into(), FieldType::Base(BaseType::Float))]),
    );

    let alert = exec
        .execute_each(&event(vec![("sip", str_val("10.0.0.1"))]), 1_000_000)
        .unwrap()
        .unwrap();

    assert!(
        !alert.yield_fields.iter().any(|(n, _)| n == "risk_score"),
        "missing nested path into a float target must be omitted, not fail the record"
    );
}

#[test]
fn execute_each_yield_nested_array_index() {
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "process_name".into(),
        value: Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("related".into()),
                PathSegment::Index(0),
                PathSegment::Field("process".into()),
                PathSegment::Field("name".into()),
            ],
        }),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("process_name".into(), FieldType::Base(BaseType::Chars))]),
    );

    let alert = exec
        .execute_each(
            &event(vec![(
                "roles_obj",
                Value::Object(HashMap::from([(
                    "related".into(),
                    Value::Array(vec![Value::Object(HashMap::from([(
                        "process".into(),
                        Value::Object(HashMap::from([("name".into(), str_val("evil.exe"))])),
                    )]))]),
                )])),
            )]),
            1_000_000,
        )
        .unwrap()
        .unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "process_name")
            .map(|(_, v)| v.clone()),
        Some(str_val("evil.exe"))
    );
}

#[test]
fn execute_each_yield_nested_array_out_of_bounds_omits() {
    // Same array-index plan; index 5 is out of bounds on a 1-element array.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "process_name".into(),
        value: Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("related".into()),
                PathSegment::Index(5),
                PathSegment::Field("name".into()),
            ],
        }),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("process_name".into(), FieldType::Base(BaseType::Chars))]),
    );

    let alert = exec
        .execute_each(
            &event(vec![(
                "roles_obj",
                Value::Object(HashMap::from([(
                    "related".into(),
                    Value::Array(vec![str_val("x")]),
                )])),
            )]),
            1_000_000,
        )
        .unwrap()
        .unwrap();

    // Chars-targeted out-of-bounds degrades to the empty string; it must not
    // fail the record.
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "process_name")
            .map(|(_, v)| v.clone()),
        Some(Value::Str(String::new().into())),
        "out-of-bounds array index must not fail the record"
    );
}

#[test]
fn execute_each_yield_nested_path_in_arithmetic() {
    // The nested path is a normal sub-expression: usable inside arithmetic.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "double_risk".into(),
        value: Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Field(FieldRef::Path {
                alias: "e".into(),
                segments: vec![
                    PathSegment::Field("roles_obj".into()),
                    PathSegment::Field("risk".into()),
                ],
            })),
            right: Box::new(Expr::Number(2.0)),
        },
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("double_risk".into(), FieldType::Base(BaseType::Float))]),
    );

    let alert = exec
        .execute_each(
            &event(vec![(
                "roles_obj",
                Value::Object(HashMap::from([("risk".into(), num(21.0))])),
            )]),
            1_000_000,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "double_risk")
            .map(|(_, v)| v.clone()),
        Some(num(42.0)),
        "nested path must compose inside arithmetic"
    );
}

#[test]
fn execute_each_yield_nested_path_inside_object_literal() {
    // Structured yields compose: a nested path can feed an object literal member.
    let mut plan = simple_rule_plan(
        "r1",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "ctx".into(),
        value: Expr::Object(vec![
            ObjectItem {
                targets: vec!["uid".to_string()],
                type_hint: None,
                value: Expr::Field(FieldRef::Path {
                    alias: "e".into(),
                    segments: vec![
                        PathSegment::Field("roles_obj".into()),
                        PathSegment::Field("source".into()),
                        PathSegment::Field("process".into()),
                        PathSegment::Field("uid".into()),
                    ],
                }),
            },
            ObjectItem {
                targets: vec!["sip".to_string()],
                type_hint: None,
                value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
            },
        ]),
    }];
    let exec = RuleExecutor::new(plan);

    let alert = exec
        .execute_each(
            &event(vec![
                ("sip", str_val("10.0.0.1")),
                ("roles_obj", nested_roles_value()),
            ]),
            1_000_000,
        )
        .unwrap()
        .unwrap();

    let Value::Object(fields) = alert
        .yield_fields
        .iter()
        .find(|(n, _)| n == "ctx")
        .map(|(_, v)| v)
        .expect("ctx yield field")
    else {
        panic!("expected object value");
    };
    assert_eq!(
        fields.get("uid"),
        Some(&str_val("d22b3fbcb9e77cb86834f6a18e2e0f68"))
    );
    assert_eq!(fields.get("sip"), Some(&str_val("10.0.0.1")));
}

#[test]
fn execute_each_bind_filter_nested_path() {
    // Each-plan filters evaluate via the expression layer, so a nested path
    // guards whether an event produces an alert.
    fn plan_with_filter(filter: Expr) -> RuleExecutor {
        let mut plan = simple_rule_plan(
            "r1",
            simple_plan(vec![], vec![]),
            Expr::Number(10.0),
            "ip",
            Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        );
        plan.binds[0].alias = "e".into();
        plan.each_plan = Some(EachPlan {
            alias: "e".into(),
            filter: Some(filter),
        });
        plan.yield_plan.fields = vec![YieldField {
            name: "uid".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "e".into(),
                segments: vec![
                    PathSegment::Field("roles_obj".into()),
                    PathSegment::Field("source".into()),
                    PathSegment::Field("process".into()),
                    PathSegment::Field("uid".into()),
                ],
            }),
        }];
        RuleExecutor::new_with_yield_field_types(
            plan,
            HashMap::from([("uid".into(), FieldType::Base(BaseType::Chars))]),
        )
    }

    let target = "d22b3fbcb9e77cb86834f6a18e2e0f68";
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("source".into()),
                PathSegment::Field("process".into()),
                PathSegment::Field("uid".into()),
            ],
        })),
        right: Box::new(Expr::StringLit(target.to_string())),
    };
    let exec = plan_with_filter(filter);

    // Matching nested value → alert fires with the extracted uid.
    let matching = exec
        .execute_each(&event(vec![("roles_obj", nested_roles_value())]), 1_000_000)
        .unwrap()
        .unwrap();
    assert_eq!(
        matching
            .yield_fields
            .iter()
            .find(|(n, _)| n == "uid")
            .map(|(_, v)| v.clone()),
        Some(str_val(target))
    );

    // Different nested value → filtered out, no alert.
    let skipped = exec
        .execute_each(
            &event(vec![(
                "roles_obj",
                Value::Object(HashMap::from([(
                    "source".into(),
                    Value::Object(HashMap::from([(
                        "process".into(),
                        Value::Object(HashMap::from([("uid".into(), str_val("other"))])),
                    )])),
                )])),
            )]),
            1_000_000,
        )
        .unwrap();
    assert!(
        skipped.is_none(),
        "filter must drop non-matching nested uid"
    );
}

#[test]
fn execute_match_yield_nested_path_via_bind_tracking() {
    // Match-rule path (issue #64): the compiler tracks the root object field per
    // bind, so the match context carries `roles_obj` and the nested path extracts.
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Qualified("fail".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.match_plan.tracked_bind_fields.insert(
        "e".into(),
        std::collections::HashSet::from(["roles_obj".into()]),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "uid".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "e".into(),
                segments: vec![
                    PathSegment::Field("roles_obj".into()),
                    PathSegment::Field("source".into()),
                    PathSegment::Field("process".into()),
                    PathSegment::Field("uid".into()),
                ],
            }),
        },
        YieldField {
            name: "sip".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("uid".into(), FieldType::Base(BaseType::Chars)),
            ("sip".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );

    let mut matched = default_matched_context();
    matched.bind_data = vec![BindData {
        alias: "e".into(),
        count: 1,
        field_values: HashMap::from([("roles_obj".into(), vec![nested_roles_value()])]),
    }];
    let alert = exec.execute_match(&matched).unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "uid")
            .map(|(_, v)| v.clone()),
        Some(str_val("d22b3fbcb9e77cb86834f6a18e2e0f68")),
        "match-rule nested path leaf must be extracted from tracked bind field"
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| n == "sip")
            .map(|(_, v)| v.clone()),
        Some(str_val("10.0.0.1"))
    );
}

#[test]
fn execute_match_yield_nested_path_missing_bind_omits() {
    // Bind state without `roles_obj`: a numeric-targeted nested path is omitted
    // and the alert still fires (issue #64).
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(70.0),
        "ip",
        Expr::Field(FieldRef::Qualified("fail".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.match_plan.tracked_bind_fields.insert(
        "e".into(),
        std::collections::HashSet::from(["roles_obj".into()]),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "risk_score".into(),
        value: Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("risk".into()),
            ],
        }),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("risk_score".into(), FieldType::Base(BaseType::Float))]),
    );

    let matched = default_matched_context(); // empty bind_data → no roles_obj
    let alert = exec.execute_match(&matched).unwrap();

    assert!(
        !alert.yield_fields.iter().any(|(n, _)| n == "risk_score"),
        "missing nested path into a float target must be omitted in match yield"
    );
}

#[test]
fn execute_match_yield_nested_path_inside_object_literal_full_pipeline() {
    // End-to-end: the WFL compiler tracks the root of a path nested inside an
    // `object { }` yield member, so the match context carries `roles_obj` and
    // the structured yield extracts it (wp-labs/warp-fusion#64).
    use crate::match_engine::match_engine::{CepStateMachine, StepResult};

    let input_window = WindowSchema {
        name: "auth_events".into(),
        streams: vec!["auth_stream".into()],
        time_field: Some("event_time".into()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "roles_obj".into(),
                field_type: FieldType::Object,
            },
            FieldDef {
                name: "event_time".into(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    };
    let output_window = WindowSchema {
        name: "out".into(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(3600),
        fields: vec![FieldDef {
            name: "ctx".into(),
            field_type: FieldType::Object,
        }],
    };

    let source = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    entity(ip, e.sip)
    yield out (ctx = object { uid = e.roles_obj.source.process.uid; })
}
"#;
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[input_window, output_window])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    assert!(
        plan.match_plan
            .tracked_bind_fields
            .get("e")
            .is_some_and(|fields| fields.contains("roles_obj")),
        "compiler must track the root of an object-literal nested path"
    );

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), None);
    let step = sm.advance_at(
        "e",
        &event(vec![
            ("sip", str_val("10.0.0.1")),
            ("roles_obj", nested_roles_value()),
        ]),
        1_000_000_000,
    );
    let StepResult::Matched(ctx) = step else {
        panic!("single event with count >= 1 should trigger a match");
    };
    let alert = exec.execute_match(&ctx).expect("alert");

    let Value::Object(fields) = alert
        .yield_fields
        .iter()
        .find(|(n, _)| n == "ctx")
        .map(|(_, v)| v)
        .expect("ctx yield field")
    else {
        panic!("expected object value");
    };
    assert_eq!(
        fields.get("uid"),
        Some(&str_val("d22b3fbcb9e77cb86834f6a18e2e0f68")),
        "object-literal nested path must extract in a match rule"
    );
}
