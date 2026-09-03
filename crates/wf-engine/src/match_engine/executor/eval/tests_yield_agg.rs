//! eval_yield_expr 入口覆盖（一）（2026-09-04 自 tests.rs 拆出；`#[path]` 兄弟子模块）：
//! step 步值/L3 聚合（first/last/collect_list/collect_set、stddev/percentile、L3 嵌套算术、
//! qualified alias）+ object merge，以及 replace/mvcount/trim/blank 内建经 yield 入口求值。

use super::*;

#[test]
fn test_first_returns_first_value() {
    let ctx = make_test_event(vec![
        Value::Number(10.0),
        Value::Number(20.0),
        Value::Number(30.0),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "first".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Number(10.0)));
}

#[test]
fn test_last_returns_last_value() {
    let ctx = make_test_event(vec![
        Value::Number(10.0),
        Value::Number(20.0),
        Value::Number(30.0),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "last".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Number(30.0)));
}

#[test]
fn test_collect_list_returns_all_values() {
    let ctx = make_test_event(vec![
        Value::Number(10.0),
        Value::Number(20.0),
        Value::Number(30.0),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "collect_list".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(
        result,
        Some(Value::Array(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0),
        ]))
    );
}

#[test]
fn test_collect_set_returns_unique_values() {
    let ctx = make_test_event(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("a".into()),
        Value::Str("c".into()),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "collect_set".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
    };
    let result = eval_yield_expr(&expr, &ctx);
    if let Some(Value::Array(arr)) = result {
        assert_eq!(arr.len(), 3); // a, b, c (unique)
    } else {
        panic!("Expected array result");
    }
}

#[test]
fn test_collect_set_qualified_bind_field_missing_does_not_fallback_to_step_values() {
    let mut fields = EngineHashMap::default();
    fields.insert(
        "_step_0_values".into(),
        Value::Array(vec![Value::Str("10.0.0.1".into())]),
    );
    fields.insert("_step_0_source".into(), Value::Str("s".into()));
    fields.insert("_bind_s_count".into(), Value::Number(6.0));
    let ctx = Event { fields };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "collect_set".to_string(),
        args: vec![Expr::Field(FieldRef::Qualified(
            "s".to_string(),
            "event_id".to_string(),
        ))],
    };

    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Array(vec![])));
}

#[test]
fn test_merge_shallow_merges_objects_left_to_right() {
    let mut base = EngineHashMap::default();
    base.insert("severity".into(), Value::Number(3.0));
    base.insert("existing".into(), Value::Str("kept".into()));

    let mut fields = EngineHashMap::default();
    fields.insert("extension".into(), Value::Object(base));
    let ctx = Event { fields };

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "merge".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("missing_extension".to_string())),
            Expr::Field(FieldRef::Simple("extension".to_string())),
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
    };

    let result = eval_yield_expr(&expr, &ctx);
    let Some(Value::Object(object)) = result else {
        panic!("expected object, got {result:?}");
    };
    assert_eq!(object.get("existing"), Some(&Value::Str("kept".into())));
    assert_eq!(object.get("source"), Some(&Value::Str("wfl".into())));
    assert_eq!(object.get("severity"), Some(&Value::Number(10.0)));
}

#[test]
fn test_merge_fails_when_object_literal_value_is_missing() {
    let ctx = Event {
        fields: EngineHashMap::default(),
    };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "merge".to_string(),
        args: vec![
            Expr::Object(vec![ObjectItem {
                targets: vec!["source".to_string()],
                type_hint: None,
                value: Expr::Field(FieldRef::Simple("missing".to_string())),
            }]),
            Expr::Object(vec![ObjectItem {
                targets: vec!["severity".to_string()],
                type_hint: None,
                value: Expr::Number(10.0),
            }]),
        ],
    };

    assert_eq!(eval_expr_with_l3(&expr, &ctx, YieldMeta::default()), None);
}

#[test]
fn test_stddev_calculation() {
    let ctx = make_test_event(vec![
        Value::Number(2.0),
        Value::Number(4.0),
        Value::Number(4.0),
        Value::Number(4.0),
        Value::Number(5.0),
        Value::Number(5.0),
        Value::Number(7.0),
        Value::Number(9.0),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "stddev".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
    };
    let result = eval_yield_expr(&expr, &ctx);
    if let Some(Value::Number(stddev)) = result {
        // Population stddev of [2,4,4,4,5,5,7,9] = 2.0
        assert!((stddev - 2.0).abs() < 0.01, "Expected ~2.0, got {}", stddev);
    } else {
        panic!("Expected numeric result, got {:?}", result);
    }
}

#[test]
fn test_stddev_returns_zero_for_single_value() {
    let ctx = make_test_event(vec![Value::Number(5.0)]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "stddev".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Number(0.0)));
}

#[test]
fn test_percentile_calculation() {
    let ctx = make_test_event(vec![
        Value::Number(1.0),
        Value::Number(2.0),
        Value::Number(3.0),
        Value::Number(4.0),
    ]);
    // percentile(value, 50) should return median-like value.
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "percentile".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("value".to_string())),
            Expr::Number(50.0),
        ],
    };
    let result = eval_yield_expr(&expr, &ctx);
    if let Some(Value::Number(p)) = result {
        // sorted=[1,2,3,4], idx=(3*0.5).round=2, result=3
        assert!((p - 3.0).abs() < 0.01, "Expected ~3.0, got {}", p);
    } else {
        panic!("Expected numeric result, got {:?}", result);
    }
}

#[test]
fn test_percentile_zero_returns_min() {
    let ctx = make_test_event(vec![
        Value::Number(10.0),
        Value::Number(20.0),
        Value::Number(30.0),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "percentile".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("value".to_string())),
            Expr::Number(0.0),
        ],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Number(10.0)));
}

#[test]
fn test_percentile_one_returns_max() {
    let ctx = make_test_event(vec![
        Value::Number(10.0),
        Value::Number(20.0),
        Value::Number(30.0),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "percentile".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("value".to_string())),
            Expr::Number(100.0),
        ],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Number(30.0)));
}

#[test]
fn test_nested_l3_in_arithmetic() {
    let ctx = make_test_event(vec![
        Value::Number(2.0),
        Value::Number(4.0),
        Value::Number(4.0),
        Value::Number(4.0),
        Value::Number(5.0),
        Value::Number(5.0),
        Value::Number(7.0),
        Value::Number(9.0),
    ]);
    let expr = Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "stddev".to_string(),
            args: vec![Expr::Field(FieldRef::Qualified(
                "e".to_string(),
                "value".to_string(),
            ))],
        }),
        right: Box::new(Expr::Number(1.0)),
    };
    let result = eval_yield_expr(&expr, &ctx);
    if let Some(Value::Number(v)) = result {
        assert!((v - 3.0).abs() < 0.01, "Expected ~3.0, got {}", v);
    } else {
        panic!("Expected numeric result, got {:?}", result);
    }
}

#[test]
fn test_qualified_alias_selects_matching_step() {
    let mut fields = EngineHashMap::default();
    fields.insert(
        "_step_0_values".into(),
        Value::Array(vec![Value::Number(10.0)]),
    );
    fields.insert("_step_0_source".into(), Value::Str("a".into()));
    fields.insert(
        "_step_1_values".into(),
        Value::Array(vec![Value::Number(99.0)]),
    );
    fields.insert("_step_1_source".into(), Value::Str("b".into()));
    let ctx = Event { fields };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "first".to_string(),
        args: vec![Expr::Field(FieldRef::Qualified(
            "b".to_string(),
            "value".to_string(),
        ))],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Number(99.0)));
}

#[test]
fn test_qualified_alias_without_match_returns_none_for_first() {
    let mut fields = EngineHashMap::default();
    fields.insert(
        "_step_0_values".into(),
        Value::Array(vec![Value::Number(10.0)]),
    );
    fields.insert("_step_0_source".into(), Value::Str("a".into()));
    let ctx = Event { fields };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "first".to_string(),
        args: vec![Expr::Field(FieldRef::Qualified(
            "missing".to_string(),
            "value".to_string(),
        ))],
    };
    let result = eval_yield_expr(&expr, &ctx);
    // fallback: missing join data returns empty string instead of None
    assert_eq!(result, Some(Value::Str("".into())));
}

#[test]
fn test_replace_works_in_yield_eval() {
    let mut fields = EngineHashMap::default();
    fields.insert("msg".into(), Value::Str("failed_login_from_root".into()));
    let ctx = Event { fields };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "replace".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("fail.*root".to_string()),
            Expr::StringLit("suspicious".to_string()),
        ],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Str("suspicious".into())));
}

#[test]
fn test_mvcount_with_collect_set_nested_l3() {
    let ctx = make_test_event(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("a".into()),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvcount".to_string(),
        args: vec![Expr::FuncCall {
            qualifier: None,
            name: "collect_set".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        }],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Number(2.0)));
}

#[test]
fn test_trim_works_in_yield_eval() {
    let mut fields = EngineHashMap::default();
    fields.insert("msg".into(), Value::Str("  hello  ".into()));
    let ctx = Event { fields };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "trim".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Str("hello".into())));
}

#[test]
fn test_blank_functions_work_in_yield_eval() {
    let mut fields = EngineHashMap::default();
    fields.insert("empty".into(), Value::Str(String::new().into()));
    fields.insert("spaces".into(), Value::Str(" \t\n ".into()));
    fields.insert("host".into(), Value::Str("example.org".into()));
    fields.insert("fallback".into(), Value::Str("fallback".into()));
    fields.insert("n".into(), Value::Number(42.0));
    let ctx = Event { fields };

    let is_empty_expr = Expr::FuncCall {
        qualifier: None,
        name: "is_blank".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("empty".to_string()))],
    };
    let is_spaces_expr = Expr::FuncCall {
        qualifier: None,
        name: "is_blank".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("spaces".to_string()))],
    };
    let is_host_expr = Expr::FuncCall {
        qualifier: None,
        name: "is_blank".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("host".to_string()))],
    };
    let is_missing_expr = Expr::FuncCall {
        qualifier: None,
        name: "is_blank".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("missing".to_string()))],
    };
    let null_if_blank_expr = Expr::FuncCall {
        qualifier: None,
        name: "null_if_blank".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("spaces".to_string()))],
    };
    let null_if_host_expr = Expr::FuncCall {
        qualifier: None,
        name: "null_if_blank".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("host".to_string()))],
    };
    let default_blank_expr = Expr::FuncCall {
        qualifier: None,
        name: "default_if_blank".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("spaces".to_string())),
            Expr::Field(FieldRef::Simple("fallback".to_string())),
        ],
    };
    let default_host_expr = Expr::FuncCall {
        qualifier: None,
        name: "default_if_blank".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("host".to_string())),
            Expr::Field(FieldRef::Simple("fallback".to_string())),
        ],
    };
    let coalesce_blank_expr = Expr::FuncCall {
        qualifier: None,
        name: "coalesce".to_string(),
        args: vec![
            null_if_blank_expr.clone(),
            Expr::Field(FieldRef::Simple("fallback".to_string())),
        ],
    };
    let coalesce_direct_blank_expr = Expr::FuncCall {
        qualifier: None,
        name: "coalesce".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("spaces".to_string())),
            Expr::Field(FieldRef::Simple("host".to_string())),
        ],
    };
    let coalesce_all_blank_expr = Expr::FuncCall {
        qualifier: None,
        name: "coalesce".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("empty".to_string())),
            Expr::Field(FieldRef::Simple("spaces".to_string())),
            Expr::Field(FieldRef::Simple("missing".to_string())),
        ],
    };
    let invalid_type_expr = Expr::FuncCall {
        qualifier: None,
        name: "is_blank".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };

    assert_eq!(
        eval_yield_expr(&is_empty_expr, &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_yield_expr(&is_spaces_expr, &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_yield_expr(&is_host_expr, &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_yield_expr(&is_missing_expr, &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_yield_expr(&null_if_blank_expr, &ctx),
        Some(Value::Str(String::new().into()))
    );
    assert_eq!(
        eval_yield_expr(&null_if_host_expr, &ctx),
        Some(Value::Str("example.org".into()))
    );
    assert_eq!(
        eval_yield_expr(&default_blank_expr, &ctx),
        Some(Value::Str("fallback".into()))
    );
    assert_eq!(
        eval_yield_expr(&default_host_expr, &ctx),
        Some(Value::Str("example.org".into()))
    );
    assert_eq!(
        eval_yield_expr(&coalesce_blank_expr, &ctx),
        Some(Value::Str("fallback".into()))
    );
    assert_eq!(
        eval_yield_expr(&coalesce_direct_blank_expr, &ctx),
        Some(Value::Str("example.org".into()))
    );
    assert_eq!(
        eval_yield_expr(&coalesce_all_blank_expr, &ctx),
        Some(Value::Str(String::new().into()))
    );
    assert_eq!(
        eval_yield_expr(&invalid_type_expr, &ctx),
        Some(Value::Str(String::new().into()))
    );
}
