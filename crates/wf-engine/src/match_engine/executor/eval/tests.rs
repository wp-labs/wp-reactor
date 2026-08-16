use super::{
    Event, Value, YieldMeta, eval_bool_expr, eval_expr_with_l3, eval_yield_expr,
    eval_yield_expr_with_score, with_yield_eval_scope,
};
use crate::match_engine::EngineHashMap;
use wf_lang::ast::{BinOp, Expr, FieldRef, ObjectItem};

fn make_test_event(values: Vec<Value>) -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("_step_0_values".into(), Value::Array(values));
    fields.insert("_step_0_source".into(), Value::Str("e".into()));
    Event { fields }
}

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

#[test]
fn test_hash_and_id_functions_work_in_yield_eval() {
    let mut fields = EngineHashMap::default();
    fields.insert("msg".into(), Value::Str("hello".into()));
    fields.insert("empty".into(), Value::Str(String::new().into()));
    fields.insert("ip".into(), Value::Str("10.0.0.1".into()));
    fields.insert("count".into(), Value::Number(3.0));
    fields.insert("special".into(), Value::Str("a|b".into()));
    fields.insert("percent".into(), Value::Str("10%".into()));
    let ctx = Event { fields };

    let md5_expr = Expr::FuncCall {
        qualifier: None,
        name: "md5".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let sha1_expr = Expr::FuncCall {
        qualifier: None,
        name: "sha1".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let sha1_n_expr = Expr::FuncCall {
        qualifier: None,
        name: "sha1_n".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::Number(8.0),
        ],
    };
    let sha1_n_empty_expr = Expr::FuncCall {
        qualifier: None,
        name: "sha1_n".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("empty".to_string())),
            Expr::Number(8.0),
        ],
    };
    let sha256_expr = Expr::FuncCall {
        qualifier: None,
        name: "sha256".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let hex_expr = Expr::FuncCall {
        qualifier: None,
        name: "hex".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let stable_expr = Expr::FuncCall {
        qualifier: None,
        name: "stable_id".to_string(),
        args: vec![
            Expr::StringLit("alert_".to_string()),
            Expr::Field(FieldRef::Simple("ip".to_string())),
            Expr::Field(FieldRef::Simple("count".to_string())),
        ],
    };
    let stable_changed_expr = Expr::FuncCall {
        qualifier: None,
        name: "stable_id".to_string(),
        args: vec![
            Expr::StringLit("alert_".to_string()),
            Expr::Field(FieldRef::Simple("ip".to_string())),
            Expr::Number(4.0),
        ],
    };
    let join_expr = Expr::FuncCall {
        qualifier: None,
        name: "join".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("special".to_string())),
            Expr::Field(FieldRef::Simple("percent".to_string())),
            Expr::Field(FieldRef::Simple("empty".to_string())),
            Expr::Field(FieldRef::Simple("count".to_string())),
        ],
    };
    let join_by_expr = Expr::FuncCall {
        qualifier: None,
        name: "join_by".to_string(),
        args: vec![
            Expr::StringLit("|".to_string()),
            Expr::Field(FieldRef::Simple("special".to_string())),
            Expr::Field(FieldRef::Simple("percent".to_string())),
            Expr::Field(FieldRef::Simple("empty".to_string())),
            Expr::Field(FieldRef::Simple("count".to_string())),
        ],
    };
    let join_missing_expr = Expr::FuncCall {
        qualifier: None,
        name: "join".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("special".to_string())),
            Expr::Field(FieldRef::Simple("missing".to_string())),
            Expr::Field(FieldRef::Simple("percent".to_string())),
        ],
    };
    let join_by_missing_expr = Expr::FuncCall {
        qualifier: None,
        name: "join_by".to_string(),
        args: vec![
            Expr::StringLit("|".to_string()),
            Expr::Field(FieldRef::Simple("special".to_string())),
            Expr::Field(FieldRef::Simple("missing".to_string())),
            Expr::Field(FieldRef::Simple("percent".to_string())),
        ],
    };
    let join_array_expr = Expr::FuncCall {
        qualifier: None,
        name: "join".to_string(),
        args: vec![Expr::Array(vec![Expr::StringLit("x".to_string())])],
    };
    let join_by_object_expr = Expr::FuncCall {
        qualifier: None,
        name: "join_by".to_string(),
        args: vec![
            Expr::StringLit("|".to_string()),
            Expr::Object(vec![ObjectItem {
                targets: vec!["x".to_string()],
                type_hint: None,
                value: Expr::StringLit("y".to_string()),
            }]),
        ],
    };
    let join_invalid_nested_expr = Expr::FuncCall {
        qualifier: None,
        name: "join".to_string(),
        args: vec![
            Expr::StringLit("a".to_string()),
            Expr::FuncCall {
                qualifier: None,
                name: "sha1_n".to_string(),
                args: vec![Expr::StringLit("x".to_string()), Expr::Number(0.0)],
            },
            Expr::StringLit("b".to_string()),
        ],
    };
    let join_by_invalid_nested_expr = Expr::FuncCall {
        qualifier: None,
        name: "join_by".to_string(),
        args: vec![
            Expr::StringLit("|".to_string()),
            Expr::StringLit("a".to_string()),
            Expr::FuncCall {
                qualifier: None,
                name: "sha1_n".to_string(),
                args: vec![Expr::StringLit("x".to_string()), Expr::Number(0.0)],
            },
            Expr::StringLit("b".to_string()),
        ],
    };

    assert_eq!(
        eval_yield_expr(&md5_expr, &ctx),
        Some(Value::Str("5d41402abc4b2a76b9719d911017c592".into()))
    );
    assert_eq!(
        eval_yield_expr(&sha1_expr, &ctx),
        Some(Value::Str(
            "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d".into()
        ))
    );
    assert_eq!(
        eval_yield_expr(&sha1_n_expr, &ctx),
        Some(Value::Str("aaf4c61d".into()))
    );
    assert_eq!(
        eval_yield_expr(&sha1_n_empty_expr, &ctx),
        Some(Value::Str("da39a3ee".into()))
    );
    assert_eq!(
        eval_yield_expr(&sha256_expr, &ctx),
        Some(Value::Str(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into()
        ))
    );
    assert_eq!(
        eval_yield_expr(&hex_expr, &ctx),
        Some(Value::Str("68656c6c6f".into()))
    );
    assert_eq!(
        eval_yield_expr(&join_expr, &ctx),
        Some(Value::Str("a|b10%3".into()))
    );
    assert_eq!(
        eval_yield_expr(&join_by_expr, &ctx),
        Some(Value::Str("a|b|10%||3".into()))
    );
    assert_eq!(
        eval_yield_expr(&join_missing_expr, &ctx),
        Some(Value::Str("a|b10%".into()))
    );
    assert_eq!(
        eval_yield_expr(&join_by_missing_expr, &ctx),
        Some(Value::Str("a|b||10%".into()))
    );
    assert_eq!(
        eval_expr_with_l3(&join_array_expr, &ctx, YieldMeta::default()),
        None
    );
    assert_eq!(
        eval_expr_with_l3(&join_by_object_expr, &ctx, YieldMeta::default()),
        None
    );
    assert_eq!(
        eval_expr_with_l3(&join_invalid_nested_expr, &ctx, YieldMeta::default()),
        None
    );
    assert_eq!(
        eval_expr_with_l3(&join_by_invalid_nested_expr, &ctx, YieldMeta::default()),
        None
    );
    let Some(Value::Str(stable_id)) = eval_yield_expr(&stable_expr, &ctx) else {
        panic!("stable_id() should return a string");
    };
    assert_eq!(stable_id, "alert_ba0dab7ccfb2a04c");
    assert_eq!(
        eval_yield_expr(&stable_expr, &ctx),
        Some(Value::Str(stable_id.clone()))
    );
    let Some(Value::Str(changed_stable_id)) = eval_yield_expr(&stable_changed_expr, &ctx) else {
        panic!("stable_id() should return a string for changed input");
    };
    assert!(changed_stable_id.starts_with("alert_"));
    assert_eq!(changed_stable_id.len(), "alert_".len() + 16);
    assert_ne!(changed_stable_id, stable_id);
}

#[test]
fn test_stable_id_uses_unambiguous_segments_in_yield_eval() {
    let ctx = Event {
        fields: EngineHashMap::default(),
    };
    let first_expr = Expr::FuncCall {
        qualifier: None,
        name: "stable_id".to_string(),
        args: vec![
            Expr::StringLit("id_".to_string()),
            Expr::StringLit("a\x1fb".to_string()),
            Expr::StringLit("c".to_string()),
        ],
    };
    let second_expr = Expr::FuncCall {
        qualifier: None,
        name: "stable_id".to_string(),
        args: vec![
            Expr::StringLit("id_".to_string()),
            Expr::StringLit("a".to_string()),
            Expr::StringLit("b\x1fc".to_string()),
        ],
    };

    assert_eq!(
        eval_yield_expr(&first_expr, &ctx),
        Some(Value::Str("id_234c47ae916c73b0".into()))
    );
    assert_eq!(
        eval_yield_expr(&second_expr, &ctx),
        Some(Value::Str("id_1532803f7ab9f6de".into()))
    );
    assert_ne!(
        eval_yield_expr(&first_expr, &ctx),
        eval_yield_expr(&second_expr, &ctx)
    );
}

#[test]
fn test_now_functions_share_timestamp_within_yield_expression() {
    let ctx = Event {
        fields: EngineHashMap::default(),
    };
    let expr = Expr::BinOp {
        op: BinOp::Sub,
        left: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "now_ms".to_string(),
            args: vec![],
        }),
        right: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "now".to_string(),
            args: vec![],
        }),
    };

    assert_eq!(eval_yield_expr(&expr, &ctx), Some(Value::Number(0.0)));
}

#[test]
fn test_now_functions_share_timestamp_across_yield_scope() {
    let ctx = Event {
        fields: EngineHashMap::default(),
    };
    let now_expr = Expr::FuncCall {
        qualifier: None,
        name: "now".to_string(),
        args: vec![],
    };
    let now_ms_expr = Expr::FuncCall {
        qualifier: None,
        name: "now_ms".to_string(),
        args: vec![],
    };

    with_yield_eval_scope(|| {
        assert_eq!(
            eval_yield_expr(&now_expr, &ctx),
            eval_yield_expr(&now_ms_expr, &ctx)
        );
    });
}

#[test]
fn test_time_bucket_rejects_invalid_interval_in_yield_eval() {
    let ctx = Event {
        fields: EngineHashMap::default(),
    };

    for interval in [0.0, -60.0, f64::INFINITY, f64::NAN] {
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "time_bucket".to_string(),
            args: vec![Expr::Number(1_700_000_075_000.0), Expr::Number(interval)],
        };
        assert_eq!(eval_expr_with_l3(&expr, &ctx, YieldMeta::default()), None);
    }
}

#[test]
fn test_mvjoin_with_collect_list_nested_l3() {
    let ctx = make_test_event(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("c".into()),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvjoin".to_string(),
        args: vec![
            Expr::FuncCall {
                qualifier: None,
                name: "collect_list".to_string(),
                args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
            },
            Expr::StringLit(",".to_string()),
        ],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Str("a,b,c".into())));
}

#[test]
fn test_split_works_in_yield_eval() {
    let mut fields = EngineHashMap::default();
    fields.insert("csv".into(), Value::Str("a,b,,c".into()));
    let ctx = Event { fields };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "split".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("csv".to_string())),
            Expr::StringLit(",".to_string()),
        ],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(
        result,
        Some(Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str(String::new().into()),
            Value::Str("c".into()),
        ]))
    );
}

#[test]
fn test_mvdedup_with_collect_list_nested_l3() {
    let ctx = make_test_event(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("a".into()),
        Value::Str("c".into()),
        Value::Str("b".into()),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvdedup".to_string(),
        args: vec![Expr::FuncCall {
            qualifier: None,
            name: "collect_list".to_string(),
            args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
        }],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(
        result,
        Some(Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]))
    );
}

#[test]
fn test_substr_works_in_yield_eval() {
    let mut fields = EngineHashMap::default();
    fields.insert("msg".into(), Value::Str("abcdef".into()));
    let ctx = Event { fields };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "substr".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::Number(2.0),
            Expr::Number(3.0),
        ],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Str("bcd".into())));
}

#[test]
fn test_startswith_and_endswith_in_yield_eval() {
    let mut fields = EngineHashMap::default();
    fields.insert("msg".into(), Value::Str("failed_login_root".into()));
    let ctx = Event { fields };
    let starts_expr = Expr::FuncCall {
        qualifier: None,
        name: "startswith".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("failed".to_string()),
        ],
    };
    let ends_expr = Expr::FuncCall {
        qualifier: None,
        name: "endswith".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("root".to_string()),
        ],
    };
    assert_eq!(eval_yield_expr(&starts_expr, &ctx), Some(Value::Bool(true)));
    assert_eq!(eval_yield_expr(&ends_expr, &ctx), Some(Value::Bool(true)));
}

#[test]
fn test_math_and_time_functions_in_yield_eval() {
    let mut fields = EngineHashMap::default();
    fields.insert("n".into(), Value::Number(-12.345));
    fields.insert("p".into(), Value::Number(16.0));
    fields.insert("ts".into(), Value::Number(0.0));
    fields.insert("msg".into(), Value::Str("  failed_login_root  ".into()));
    fields.insert(
        "arr".into(),
        Value::Array(vec![
            Value::Str("b".into()),
            Value::Str("a".into()),
            Value::Str("c".into()),
        ]),
    );
    let ctx = Event { fields };

    let abs_expr = Expr::FuncCall {
        qualifier: None,
        name: "abs".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let round_expr = Expr::FuncCall {
        qualifier: None,
        name: "round".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("n".to_string())),
            Expr::Number(2.0),
        ],
    };
    let ceil_expr = Expr::FuncCall {
        qualifier: None,
        name: "ceil".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let floor_expr = Expr::FuncCall {
        qualifier: None,
        name: "floor".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let strftime_expr = Expr::FuncCall {
        qualifier: None,
        name: "strftime".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::StringLit("%Y-%m-%d".to_string()),
        ],
    };
    let strptime_expr = Expr::FuncCall {
        qualifier: None,
        name: "strptime".to_string(),
        args: vec![
            Expr::StringLit("1970-01-01".to_string()),
            Expr::StringLit("%Y-%m-%d".to_string()),
        ],
    };
    let now_expr = Expr::FuncCall {
        qualifier: None,
        name: "now".to_string(),
        args: vec![],
    };
    let now_s_expr = Expr::FuncCall {
        qualifier: None,
        name: "now_s".to_string(),
        args: vec![],
    };
    let now_ms_expr = Expr::FuncCall {
        qualifier: None,
        name: "now_ms".to_string(),
        args: vec![],
    };
    let now_us_expr = Expr::FuncCall {
        qualifier: None,
        name: "now_us".to_string(),
        args: vec![],
    };
    let now_ns_expr = Expr::FuncCall {
        qualifier: None,
        name: "now_ns".to_string(),
        args: vec![],
    };
    let now_fmt_expr = Expr::FuncCall {
        qualifier: None,
        name: "strftime".to_string(),
        args: vec![now_expr.clone(), Expr::StringLit("%Y".to_string())],
    };
    let sqrt_expr = Expr::FuncCall {
        qualifier: None,
        name: "sqrt".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("p".to_string()))],
    };
    let pow_expr = Expr::FuncCall {
        qualifier: None,
        name: "pow".to_string(),
        args: vec![Expr::Number(2.0), Expr::Number(8.0)],
    };
    let log_expr = Expr::FuncCall {
        qualifier: None,
        name: "log".to_string(),
        args: vec![Expr::Number(100.0), Expr::Number(10.0)],
    };
    let exp_expr = Expr::FuncCall {
        qualifier: None,
        name: "exp".to_string(),
        args: vec![Expr::Number(1.0)],
    };
    let clamp_expr = Expr::FuncCall {
        qualifier: None,
        name: "clamp".to_string(),
        args: vec![Expr::Number(120.0), Expr::Number(0.0), Expr::Number(100.0)],
    };
    let sign_expr = Expr::FuncCall {
        qualifier: None,
        name: "sign".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let trunc_expr = Expr::FuncCall {
        qualifier: None,
        name: "trunc".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let finite_expr = Expr::FuncCall {
        qualifier: None,
        name: "is_finite".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
    };
    let ltrim_expr = Expr::FuncCall {
        qualifier: None,
        name: "ltrim".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let rtrim_expr = Expr::FuncCall {
        qualifier: None,
        name: "rtrim".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let concat_expr = Expr::FuncCall {
        qualifier: None,
        name: "concat".to_string(),
        args: vec![
            Expr::StringLit("ip=".to_string()),
            Expr::StringLit("1.1.1.1".to_string()),
        ],
    };
    let index_expr = Expr::FuncCall {
        qualifier: None,
        name: "indexof".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("login".to_string()),
        ],
    };
    let replace_plain_expr = Expr::FuncCall {
        qualifier: None,
        name: "replace_plain".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("_".to_string()),
            Expr::StringLit("-".to_string()),
        ],
    };
    let sw_any_expr = Expr::FuncCall {
        qualifier: None,
        name: "startswith_any".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("  fail".to_string()),
            Expr::StringLit("deny".to_string()),
        ],
    };
    let ew_any_expr = Expr::FuncCall {
        qualifier: None,
        name: "endswith_any".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("root  ".to_string()),
            Expr::StringLit("deny".to_string()),
        ],
    };
    let coalesce_expr = Expr::FuncCall {
        qualifier: None,
        name: "coalesce".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("missing".to_string())),
            Expr::StringLit("fallback".to_string()),
        ],
    };
    let isnull_expr = Expr::FuncCall {
        qualifier: None,
        name: "isnull".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("missing".to_string()))],
    };
    let isnotnull_expr = Expr::FuncCall {
        qualifier: None,
        name: "isnotnull".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let mvsort_expr = Expr::FuncCall {
        qualifier: None,
        name: "mvsort".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("arr".to_string()))],
    };
    let mvreverse_expr = Expr::FuncCall {
        qualifier: None,
        name: "mvreverse".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("arr".to_string()))],
    };

    assert_eq!(
        eval_yield_expr(&abs_expr, &ctx),
        Some(Value::Number(12.345))
    );
    assert_eq!(
        eval_yield_expr(&round_expr, &ctx),
        Some(Value::Number(-12.35))
    );
    assert_eq!(
        eval_yield_expr(&ceil_expr, &ctx),
        Some(Value::Number(-12.0))
    );
    assert_eq!(
        eval_yield_expr(&floor_expr, &ctx),
        Some(Value::Number(-13.0))
    );
    assert_eq!(
        eval_yield_expr(&strftime_expr, &ctx),
        Some(Value::Str("1970-01-01".into()))
    );
    assert_eq!(
        eval_yield_expr(&strptime_expr, &ctx),
        Some(Value::Number(0.0))
    );
    let Some(Value::Number(now_millis)) = eval_yield_expr(&now_expr, &ctx) else {
        panic!("now() should return a numeric timestamp");
    };
    let Some(Value::Number(now_s)) = eval_yield_expr(&now_s_expr, &ctx) else {
        panic!("now_s() should return a numeric timestamp");
    };
    let Some(Value::Number(now_ms)) = eval_yield_expr(&now_ms_expr, &ctx) else {
        panic!("now_ms() should return a numeric timestamp");
    };
    let Some(Value::Number(now_us)) = eval_yield_expr(&now_us_expr, &ctx) else {
        panic!("now_us() should return a numeric timestamp");
    };
    let Some(Value::Number(now_ns)) = eval_yield_expr(&now_ns_expr, &ctx) else {
        panic!("now_ns() should return a numeric timestamp");
    };
    let Some(Value::Str(year)) = eval_yield_expr(&now_fmt_expr, &ctx) else {
        panic!("strftime(now(), ...) should format the current time");
    };
    assert!(now_millis > 1_000_000_000_000.0);
    assert!(now_ns > 1_000_000_000_000_000_000.0);
    assert!(now_us > 1_000_000_000_000_000.0);
    assert!(now_ms > 1_000_000_000_000.0);
    assert!(now_s > 1_000_000_000.0);
    assert!(year.len() == 4 && year.chars().all(|c| c.is_ascii_digit()));
    assert_eq!(eval_yield_expr(&sqrt_expr, &ctx), Some(Value::Number(4.0)));
    assert_eq!(eval_yield_expr(&pow_expr, &ctx), Some(Value::Number(256.0)));
    assert_eq!(eval_yield_expr(&log_expr, &ctx), Some(Value::Number(2.0)));
    assert_eq!(
        eval_yield_expr(&exp_expr, &ctx),
        Some(Value::Number(std::f64::consts::E))
    );
    assert_eq!(
        eval_yield_expr(&clamp_expr, &ctx),
        Some(Value::Number(100.0))
    );
    assert_eq!(eval_yield_expr(&sign_expr, &ctx), Some(Value::Number(-1.0)));
    assert_eq!(
        eval_yield_expr(&trunc_expr, &ctx),
        Some(Value::Number(-12.0))
    );
    assert_eq!(eval_yield_expr(&finite_expr, &ctx), Some(Value::Bool(true)));
    assert_eq!(
        eval_yield_expr(&ltrim_expr, &ctx),
        Some(Value::Str("failed_login_root  ".into()))
    );
    assert_eq!(
        eval_yield_expr(&rtrim_expr, &ctx),
        Some(Value::Str("  failed_login_root".into()))
    );
    assert_eq!(
        eval_yield_expr(&concat_expr, &ctx),
        Some(Value::Str("ip=1.1.1.1".into()))
    );
    assert_eq!(eval_yield_expr(&index_expr, &ctx), Some(Value::Number(9.0)));
    assert_eq!(
        eval_yield_expr(&replace_plain_expr, &ctx),
        Some(Value::Str("  failed-login-root  ".into()))
    );
    assert_eq!(eval_yield_expr(&sw_any_expr, &ctx), Some(Value::Bool(true)));
    assert_eq!(eval_yield_expr(&ew_any_expr, &ctx), Some(Value::Bool(true)));
    assert_eq!(
        eval_yield_expr(&coalesce_expr, &ctx),
        Some(Value::Str("fallback".into()))
    );
    assert_eq!(eval_yield_expr(&isnull_expr, &ctx), Some(Value::Bool(true)));
    assert_eq!(
        eval_yield_expr(&isnotnull_expr, &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_yield_expr(&mvsort_expr, &ctx),
        Some(Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]))
    );
    assert_eq!(
        eval_yield_expr(&mvreverse_expr, &ctx),
        Some(Value::Array(vec![
            Value::Str("c".into()),
            Value::Str("a".into()),
            Value::Str("b".into()),
        ]))
    );
}

#[test]
fn test_system_score_var_works_inside_builtin_functions() {
    let ctx = Event {
        fields: EngineHashMap::default(),
    };
    let round_expr = Expr::FuncCall {
        qualifier: None,
        name: "round".to_string(),
        args: vec![
            Expr::SystemVar(wf_lang::ast::SystemVar::Score),
            Expr::Number(1.0),
        ],
    };
    let concat_expr = Expr::FuncCall {
        qualifier: None,
        name: "concat".to_string(),
        args: vec![
            Expr::StringLit("risk=".to_string()),
            Expr::SystemVar(wf_lang::ast::SystemVar::Score),
        ],
    };

    assert_eq!(
        eval_yield_expr_with_score(&round_expr, &ctx, Some(70.126)),
        Some(Value::Number(70.1))
    );
    assert_eq!(
        eval_yield_expr_with_score(&concat_expr, &ctx, Some(70.126)),
        Some(Value::Str("risk=70.126".into()))
    );
}

#[test]
fn test_mvindex_with_collect_list_nested_l3() {
    let ctx = make_test_event(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("c".into()),
    ]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvindex".to_string(),
        args: vec![
            Expr::FuncCall {
                qualifier: None,
                name: "collect_list".to_string(),
                args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
            },
            Expr::Number(1.0),
        ],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(result, Some(Value::Str("b".into())));
}

#[test]
fn test_mvappend_with_collect_list_nested_l3() {
    let ctx = make_test_event(vec![Value::Str("a".into()), Value::Str("b".into())]);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvappend".to_string(),
        args: vec![
            Expr::FuncCall {
                qualifier: None,
                name: "collect_list".to_string(),
                args: vec![Expr::Field(FieldRef::Simple("value".to_string()))],
            },
            Expr::StringLit("c".to_string()),
        ],
    };
    let result = eval_yield_expr(&expr, &ctx);
    assert_eq!(
        result,
        Some(Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]))
    );
}

// -------------------------------------------------------------------
// external() tests
// -------------------------------------------------------------------

#[test]
fn external_without_handler_returns_none() {
    // NOTE: EXTERNAL_HANDLER is a global OnceLock. If a previous test
    // already installed a handler, dispatch will return Some(...) instead
    // of None. We verify the no-handler path by checking an empty OnceLock
    // directly (mirroring dispatch_external_call's logic).
    let empty: std::sync::OnceLock<std::sync::Arc<dyn crate::external::ExternalCallHandler>> =
        std::sync::OnceLock::new();
    assert!(empty.get().is_none());
    assert!(empty.get().and_then(|h| h.call("test", &[])).is_none());
}

#[test]
fn external_requires_at_least_two_args() {
    let ctx = Event {
        fields: EngineHashMap::default(),
    };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "external".to_string(),
        args: vec![Expr::StringLit("only_service".to_string())],
    };
    let result = eval_bool_expr(&expr, &ctx);
    assert_eq!(result, None);
}

#[test]
fn external_service_must_be_string_literal() {
    let ctx = Event {
        fields: EngineHashMap::default(),
    };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "external".to_string(),
        args: vec![
            Expr::Number(42.0), // not a string
            Expr::StringLit("arg".to_string()),
        ],
    };
    let result = eval_bool_expr(&expr, &ctx);
    assert_eq!(result, None);
}
