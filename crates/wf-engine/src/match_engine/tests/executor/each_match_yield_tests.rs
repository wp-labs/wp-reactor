//! yield_tests.rs 拆出的 each / match（命中前执行路径）yield 输出测试（2026-09-04；
//! `#[path]` 子模块，经父模块 `use super::*` 复用其导入）。
//!
//! 覆盖：score/字段引用与目标类型强制、时间系统变量与首匹配时间、结构化对象/
//! 数组字面量与对象合并、内建表达式内引用 score、失败回退（不静默）与缺失可选
//! 字段省略（match 路径）。

use super::*;

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
            .find(|(name, _)| &**name == "risk_score")
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
            .find(|(name, _)| &**name == "risk_score")
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
            .find(|(name, _)| &**name == "first_seen")
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
            .find(|(name, _)| &**name == "event_year")
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
            .find(|(field_name, _)| &**field_name == name)
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
            .find(|(field_name, _)| &**field_name == name)
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
        .find(|(name, _)| &**name == "risk_context")
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
                Expr::Field(FieldRef::Qualified("e".into(), "extension".into())),
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

    let mut extension = EngineHashMap::default();
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
        .find(|(name, _)| &**name == "extensions")
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
        value: Expr::Field(FieldRef::Qualified("e".into(), "extension".into())),
    }];
    let exec = RuleExecutor::new(plan);

    let mut detection = EngineHashMap::default();
    detection.insert("severity".into(), num(10.0));
    detection.insert(
        "tags".into(),
        Value::Array(vec![str_val("os:linux"), str_val("webshell")]),
    );
    let mut extension = EngineHashMap::default();
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
        .find(|(name, _)| &**name == "extensions")
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
        .find(|(k, _)| &**k == "missing")
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
            .find(|(name, _)| &**name == "risk_score")
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
        YieldField {
            name: "evidence_start_time".into(),
            value: Expr::SystemVar(SystemVar::EvidenceStartTime),
        },
        YieldField {
            name: "evidence_end_time".into(),
            value: Expr::SystemVar(SystemVar::EvidenceEndTime),
        },
    ];
    let exec = RuleExecutor::new(plan);
    let mut matched = default_matched_context();
    matched.event_first_time_nanos = 1_000_000_000;
    matched.event_last_time_nanos = 3_000_000_000;
    matched.window_start_time_nanos = 500_000_000;
    matched.window_end_time_nanos = 5_500_000_000;
    // 候选事件跨度与命中证据跨度独立（issue #82 方案 A）。
    matched.evidence_first_time_nanos = 2_000_000_000;
    matched.evidence_last_time_nanos = 4_000_000_000;

    let alert = exec.execute_match(&matched).unwrap();

    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };
    assert_eq!(field("first_seen"), Some(num(1_000.0)));
    assert_eq!(field("last_seen"), Some(num(3_000.0)));
    assert_eq!(field("rule_window_start"), Some(num(500.0)));
    assert_eq!(field("rule_window_end"), Some(num(5_500.0)));
    assert_eq!(field("evidence_start_time"), Some(num(2_000.0)));
    assert_eq!(field("evidence_end_time"), Some(num(4_000.0)));
}

#[test]
fn execute_match_yield_can_reference_first_match_time() {
    // issue #82：@first_match_time 从 MatchedContext（首次命中墙钟）直通输出。
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
    let mut matched = default_matched_context();
    // 事件时间（event_time_nanos）与处理墙钟不同——@first_match_time 必须取
    // 处理墙钟字段，而不是事件/窗口时间。
    matched.event_first_time_nanos = 1_000_000_000;
    matched.window_start_time_nanos = 500_000_000;
    matched.first_match_time_nanos = Some(2_000_000_000);

    let alert = exec.execute_match(&matched).unwrap();

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
        "@first_match_time = 首次命中处理墙钟（≠ event_first_time 1_000）"
    );
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
            .find(|(name, _)| &**name == "rounded")
            .map(|(_, value)| value.clone()),
        Some(num(70.1))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "message")
            .map(|(_, value)| value.clone()),
        Some(str_val("risk=70.126"))
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(name, _)| &**name == "rule_message")
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
        .find(|(k, _)| &**k == "missing")
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
        value: Expr::Field(FieldRef::Qualified("e".into(), "attacker_latitude".into())),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("attacker_latitude".into(), FieldType::Base(BaseType::Float))]),
    );

    let output = exec.execute_match(&default_matched_context()).unwrap();
    assert!(
        !output
            .yield_fields
            .iter()
            .any(|(k, _)| &**k == "attacker_latitude"),
        "missing typed float field should be omitted from match output"
    );
}
