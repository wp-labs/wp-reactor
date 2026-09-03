//! yield_tests.rs 拆出的嵌套字段路径（#64）测试（2026-09-04；`#[path]` 子模块，
//! 经父模块 `use super::*` 复用其导入）。
//!
//! 覆盖：each/match 路径的嵌套路径取值、缺失与越界降级、算术与对象字面量内引用、
//! bind filter 门控与 bind tracking 下的嵌套路径解析。

use super::*;

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
    Value::Object(EngineHashMap::from_iter([(
        "source".into(),
        Value::Object(EngineHashMap::from_iter([(
            "process".into(),
            Value::Object(EngineHashMap::from_iter([(
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
            .find(|(n, _)| &**n == "uid")
            .map(|(_, v)| v.clone()),
        Some(str_val("d22b3fbcb9e77cb86834f6a18e2e0f68")),
        "nested path leaf must be extracted"
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "sip")
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
            .find(|(n, _)| &**n == "uid")
            .map(|(_, v)| v.clone()),
        Some(Value::Str(String::new().into())),
        "missing nested path must not fail the record"
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "sip")
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
        !alert.yield_fields.iter().any(|(n, _)| &**n == "risk_score"),
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
                Value::Object(EngineHashMap::from_iter([(
                    "related".into(),
                    Value::Array(vec![Value::Object(EngineHashMap::from_iter([(
                        "process".into(),
                        Value::Object(EngineHashMap::from_iter([(
                            "name".into(),
                            str_val("evil.exe"),
                        )])),
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
            .find(|(n, _)| &**n == "process_name")
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
                Value::Object(EngineHashMap::from_iter([(
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
            .find(|(n, _)| &**n == "process_name")
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
                Value::Object(EngineHashMap::from_iter([("risk".into(), num(21.0))])),
            )]),
            1_000_000,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "double_risk")
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
        .find(|(n, _)| &**n == "ctx")
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
            .find(|(n, _)| &**n == "uid")
            .map(|(_, v)| v.clone()),
        Some(str_val(target))
    );

    // Different nested value → filtered out, no alert.
    let skipped = exec
        .execute_each(
            &event(vec![(
                "roles_obj",
                Value::Object(EngineHashMap::from_iter([(
                    "source".into(),
                    Value::Object(EngineHashMap::from_iter([(
                        "process".into(),
                        Value::Object(EngineHashMap::from_iter([("uid".into(), str_val("other"))])),
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
        field_values: EngineHashMap::from_iter([("roles_obj".into(), vec![nested_roles_value()])]),
    }];
    let alert = exec.execute_match(&matched).unwrap();

    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "uid")
            .map(|(_, v)| v.clone()),
        Some(str_val("d22b3fbcb9e77cb86834f6a18e2e0f68")),
        "match-rule nested path leaf must be extracted from tracked bind field"
    );
    assert_eq!(
        alert
            .yield_fields
            .iter()
            .find(|(n, _)| &**n == "sip")
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
        !alert.yield_fields.iter().any(|(n, _)| &**n == "risk_score"),
        "missing nested path into a float target must be omitted in match yield"
    );
}

#[test]
fn execute_match_yield_nested_path_inside_object_literal_full_pipeline() {
    // End-to-end: the WFL compiler tracks the root of a path nested inside an
    // `object { }` yield member, so the match context carries `roles_obj` and
    // the structured yield extracts it (wp-labs/warp-fusion#64).
    use crate::match_engine::cep::{CepStateMachine, StepResult};

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
        .find(|(n, _)| &**n == "ctx")
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
