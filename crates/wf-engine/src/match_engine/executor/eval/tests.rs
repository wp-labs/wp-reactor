use super::builtins::{
    contains_system_var, eval_aggregate_func, eval_aggregate_over_numbers,
    eval_aggregate_over_values, eval_builtin_func_with_l3, eval_l3_func, eval_stat_func,
    is_stat_selector_func, materialize_system_vars, numeric_values, sum_numeric_values,
};
use super::utils;
use super::{
    Event, Value, YieldMeta, eval_bool_expr, eval_expr_with_l3, eval_yield_expr,
    eval_yield_expr_with_score, with_yield_eval_scope,
};
use super::{eval_entity_id, eval_score};
use crate::match_engine::EngineHashMap;
use sha2::{Digest, Sha256};
use wf_lang::ast::{BinOp, Expr, FieldRef, ObjectItem, SystemVar};
use wf_lang::wfu_meta::WfuMetaField;

fn lit(n: &str) -> Expr {
    Expr::StringLit(n.to_string())
}

fn field(name: &str) -> Expr {
    Expr::Field(FieldRef::Simple(name.to_string()))
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FuncCall {
        qualifier: None,
        name: name.to_string(),
        args,
    }
}

fn ctx_with(pairs: Vec<(&str, Value)>) -> Event {
    let mut fields = EngineHashMap::default();
    for (k, v) in pairs {
        fields.insert(k.into(), v);
    }
    Event { fields }
}

fn l3_ctx(expr: &Expr, ctx: &Event) -> Option<Value> {
    eval_expr_with_l3(expr, ctx, YieldMeta::default())
}

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

// ===========================================================================
// builtins.rs — string builtins (L3 eval path) + error branches
// ===========================================================================

#[test]
fn builtin_contains_startswith_endswith() {
    let ctx = ctx_with(vec![
        ("msg", Value::Str("failed_login_root".into())),
        ("n", Value::Number(5.0)),
    ]);
    assert_eq!(
        l3_ctx(&call("contains", vec![field("msg"), lit("login")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("contains", vec![field("msg"), lit("ok")]), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&call("startswith", vec![field("msg"), lit("failed")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("endswith", vec![field("msg"), lit("root")]), &ctx),
        Some(Value::Bool(true))
    );
    // wrong arg count
    assert_eq!(l3_ctx(&call("contains", vec![field("msg")]), &ctx), None);
    assert_eq!(
        l3_ctx(
            &call("contains", vec![field("msg"), lit("a"), lit("b")]),
            &ctx
        ),
        None
    );
    assert_eq!(l3_ctx(&call("startswith", vec![field("msg")]), &ctx), None);
    assert_eq!(l3_ctx(&call("endswith", vec![field("msg")]), &ctx), None);
    // wrong types
    assert_eq!(
        l3_ctx(&call("contains", vec![field("n"), lit("a")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("contains", vec![field("msg"), field("n")]), &ctx),
        None
    );
}

#[test]
fn builtin_substr_variants() {
    let ctx = ctx_with(vec![("msg", Value::Str("abcdef".into()))]);
    // start + length
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![field("msg"), Expr::Number(2.0), Expr::Number(3.0)]
            ),
            &ctx
        ),
        Some(Value::Str("bcd".into()))
    );
    // start only (to end)
    assert_eq!(
        l3_ctx(&call("substr", vec![field("msg"), Expr::Number(4.0)]), &ctx),
        Some(Value::Str("def".into()))
    );
    // negative start
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![field("msg"), Expr::Neg(Box::new(Expr::Number(2.0)))]
            ),
            &ctx
        ),
        Some(Value::Str("ef".into()))
    );
    // start == 0 → from beginning
    assert_eq!(
        l3_ctx(&call("substr", vec![field("msg"), Expr::Number(0.0)]), &ctx),
        Some(Value::Str("abcdef".into()))
    );
    // start beyond end → empty string
    assert_eq!(
        l3_ctx(
            &call("substr", vec![field("msg"), Expr::Number(100.0)]),
            &ctx
        ),
        Some(Value::Str(String::new().into()))
    );
    // very negative start clamps to 0
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![field("msg"), Expr::Neg(Box::new(Expr::Number(50.0)))]
            ),
            &ctx
        ),
        Some(Value::Str("abcdef".into()))
    );
    // length <= 0 → empty
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![field("msg"), Expr::Number(2.0), Expr::Number(0.0)]
            ),
            &ctx
        ),
        Some(Value::Str(String::new().into()))
    );
    // length past end is truncated
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![field("msg"), Expr::Number(5.0), Expr::Number(100.0)]
            ),
            &ctx
        ),
        Some(Value::Str("ef".into()))
    );
    // errors
    assert_eq!(l3_ctx(&call("substr", vec![field("msg")]), &ctx), None);
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![
                    field("msg"),
                    Expr::Number(1.0),
                    Expr::Number(1.0),
                    Expr::Number(1.0)
                ]
            ),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("substr", vec![Expr::Number(1.0), Expr::Number(1.0)]),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(&call("substr", vec![field("msg"), lit("x")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("substr", vec![field("msg"), Expr::Number(1.0), lit("x")]),
            &ctx
        ),
        None
    );
}

#[test]
fn builtin_replace_trim_case_len() {
    let ctx = ctx_with(vec![
        ("msg", Value::Str("  HeLLo\t".into())),
        ("n", Value::Number(42.0)),
        ("multibyte", Value::Str("你好".into())),
    ]);
    // replace (regex)
    assert_eq!(
        l3_ctx(
            &call(
                "replace",
                vec![lit("failed_login"), lit("fail.*"), lit("blocked")]
            ),
            &ctx
        ),
        Some(Value::Str("blocked".into()))
    );
    // invalid regex → None
    assert_eq!(
        l3_ctx(&call("replace", vec![lit("abc"), lit("("), lit("x")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("replace", vec![lit("a"), lit("b")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("replace", vec![Expr::Number(1.0), lit("b"), lit("c")]),
            &ctx
        ),
        None
    );
    // trim / lower / upper / ltrim / rtrim
    assert_eq!(
        l3_ctx(&call("trim", vec![field("msg")]), &ctx),
        Some(Value::Str("HeLLo".into()))
    );
    assert_eq!(
        l3_ctx(&call("lower", vec![field("msg")]), &ctx),
        Some(Value::Str("  hello\t".into()))
    );
    assert_eq!(
        l3_ctx(&call("upper", vec![field("msg")]), &ctx),
        Some(Value::Str("  HELLO\t".into()))
    );
    assert_eq!(
        l3_ctx(&call("ltrim", vec![field("msg")]), &ctx),
        Some(Value::Str("HeLLo\t".into()))
    );
    assert_eq!(
        l3_ctx(&call("rtrim", vec![field("msg")]), &ctx),
        Some(Value::Str("  HeLLo".into()))
    );
    assert_eq!(l3_ctx(&call("trim", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("lower", vec![field("n")]), &ctx), None);
    assert_eq!(l3_ctx(&call("upper", vec![field("n")]), &ctx), None);
    assert_eq!(l3_ctx(&call("ltrim", vec![Expr::Number(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("rtrim", vec![Expr::Number(1.0)]), &ctx), None);
    // len: byte length (multibyte aware per Rust str.len())
    assert_eq!(
        l3_ctx(&call("len", vec![lit("hello")]), &ctx),
        Some(Value::Number(5.0))
    );
    assert_eq!(
        l3_ctx(&call("len", vec![field("multibyte")]), &ctx),
        Some(Value::Number(6.0))
    );
    assert_eq!(l3_ctx(&call("len", vec![field("n")]), &ctx), None);
    assert_eq!(l3_ctx(&call("len", vec![]), &ctx), None);
}

#[test]
fn builtin_merge_branches() {
    let mut base = EngineHashMap::default();
    base.insert("severity".into(), Value::Number(3.0));
    let ctx = ctx_with(vec![
        ("extension", Value::Object(base)),
        ("scalar", Value::Number(7.0)),
    ]);
    let obj = Expr::Object(vec![ObjectItem {
        targets: vec!["source".to_string()],
        type_hint: None,
        value: lit("wfl"),
    }]);
    let result = l3_ctx(&call("merge", vec![field("extension"), obj.clone()]), &ctx);
    let Some(Value::Object(merged)) = result else {
        panic!("expected object");
    };
    assert_eq!(merged.get("severity"), Some(&Value::Number(3.0)));
    assert_eq!(merged.get("source"), Some(&Value::Str("wfl".into())));

    // missing field arg is skipped (treated as empty object)
    let result = l3_ctx(&call("merge", vec![field("missing"), obj.clone()]), &ctx);
    let Some(Value::Object(merged)) = result else {
        panic!("expected object");
    };
    assert_eq!(merged.get("source"), Some(&Value::Str("wfl".into())));

    // non-object arg → None
    assert_eq!(l3_ctx(&call("merge", vec![field("scalar")]), &ctx), None);
    // empty args → None
    assert_eq!(l3_ctx(&call("merge", vec![]), &ctx), None);
}

// ===========================================================================
// builtins.rs — array builtins (mv*) + error branches
// ===========================================================================

fn arr(values: Vec<Value>) -> Value {
    Value::Array(values)
}

#[test]
fn builtin_mvcount_mvjoin_mvdedup_mvsort_mvreverse() {
    let vals = arr(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("a".into()),
        Value::Str("c".into()),
    ]);
    let nums = arr(vec![
        Value::Number(3.0),
        Value::Number(1.0),
        Value::Number(2.0),
    ]);
    let ctx = ctx_with(vec![
        ("vals", vals),
        ("nums", nums),
        ("scalar", Value::Number(1.0)),
    ]);
    assert_eq!(
        l3_ctx(&call("mvcount", vec![field("vals")]), &ctx),
        Some(Value::Number(4.0))
    );
    assert_eq!(
        l3_ctx(&call("mvjoin", vec![field("vals"), lit("|")]), &ctx),
        Some(Value::Str("a|b|a|c".into()))
    );
    assert_eq!(
        l3_ctx(&call("mvdedup", vec![field("vals")]), &ctx),
        Some(arr(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]))
    );
    assert_eq!(
        l3_ctx(&call("mvsort", vec![field("nums")]), &ctx),
        Some(arr(vec![
            Value::Number(1.0),
            Value::Number(2.0),
            Value::Number(3.0),
        ]))
    );
    assert_eq!(
        l3_ctx(&call("mvreverse", vec![field("vals")]), &ctx),
        Some(arr(vec![
            Value::Str("c".into()),
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("a".into()),
        ]))
    );
    // non-array args → None
    assert_eq!(l3_ctx(&call("mvcount", vec![field("scalar")]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("mvjoin", vec![field("scalar"), lit(",")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("mvjoin", vec![field("vals"), field("scalar")]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("mvdedup", vec![field("scalar")]), &ctx), None);
    assert_eq!(l3_ctx(&call("mvsort", vec![field("scalar")]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("mvreverse", vec![field("scalar")]), &ctx),
        None
    );
    // wrong arg count
    assert_eq!(l3_ctx(&call("mvcount", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("mvjoin", vec![field("vals")]), &ctx), None);
    assert_eq!(l3_ctx(&call("mvdedup", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("mvsort", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("mvreverse", vec![]), &ctx), None);
}

#[test]
fn builtin_mvindex_single_and_range() {
    let vals = arr(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("c".into()),
        Value::Str("d".into()),
    ]);
    let ctx = ctx_with(vec![("vals", vals), ("scalar", Value::Number(1.0))]);
    // 2-arg: positive / negative / out of range
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("vals"), Expr::Number(0.0)]),
            &ctx
        ),
        Some(Value::Str("a".into()))
    );
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Neg(Box::new(Expr::Number(1.0)))]
            ),
            &ctx
        ),
        Some(Value::Str("d".into()))
    );
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("vals"), Expr::Number(10.0)]),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Neg(Box::new(Expr::Number(10.0)))]
            ),
            &ctx
        ),
        None
    );
    // 3-arg range
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Number(1.0), Expr::Number(2.0)]
            ),
            &ctx
        ),
        Some(arr(vec![Value::Str("b".into()), Value::Str("c".into())]))
    );
    // negative range bounds: start=-3 → idx 1, end=-1 → idx 3
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![
                    field("vals"),
                    Expr::Neg(Box::new(Expr::Number(3.0))),
                    Expr::Neg(Box::new(Expr::Number(1.0)))
                ]
            ),
            &ctx
        ),
        Some(arr(vec![
            Value::Str("b".into()),
            Value::Str("c".into()),
            Value::Str("d".into())
        ]))
    );
    // end < 0 → empty
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![
                    field("vals"),
                    Expr::Number(0.0),
                    Expr::Neg(Box::new(Expr::Number(10.0)))
                ]
            ),
            &ctx
        ),
        Some(arr(vec![]))
    );
    // start >= len → empty
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Number(10.0), Expr::Number(20.0)]
            ),
            &ctx
        ),
        Some(arr(vec![]))
    );
    // start > end → empty
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Number(3.0), Expr::Number(1.0)]
            ),
            &ctx
        ),
        Some(arr(vec![]))
    );
    // end clamped to len-1
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Number(2.0), Expr::Number(100.0)]
            ),
            &ctx
        ),
        Some(arr(vec![Value::Str("c".into()), Value::Str("d".into())]))
    );
    // empty array with range → empty
    let empty_ctx = ctx_with(vec![("empty", arr(vec![]))]);
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("empty"), Expr::Number(0.0), Expr::Number(1.0)]
            ),
            &empty_ctx
        ),
        Some(arr(vec![]))
    );
    // errors
    assert_eq!(l3_ctx(&call("mvindex", vec![field("vals")]), &ctx), None);
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![
                    field("vals"),
                    Expr::Number(1.0),
                    Expr::Number(2.0),
                    Expr::Number(3.0)
                ]
            ),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("scalar"), Expr::Number(0.0)]),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(&call("mvindex", vec![field("vals"), lit("x")]), &ctx),
        None
    );
}

#[test]
fn builtin_mvappend_split() {
    let ctx = ctx_with(vec![
        (
            "vals",
            arr(vec![Value::Str("a".into()), Value::Str("b".into())]),
        ),
        ("scalar", Value::Number(9.0)),
    ]);
    assert_eq!(
        l3_ctx(
            &call("mvappend", vec![field("vals"), lit("c"), field("scalar")]),
            &ctx
        ),
        Some(arr(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
            Value::Number(9.0),
        ]))
    );
    assert_eq!(l3_ctx(&call("mvappend", vec![]), &ctx), None);
    // split with separator
    assert_eq!(
        l3_ctx(&call("split", vec![lit("a,b,,c"), lit(",")]), &ctx),
        Some(arr(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str(String::new().into()),
            Value::Str("c".into()),
        ]))
    );
    // split with empty separator → per-char
    assert_eq!(
        l3_ctx(&call("split", vec![lit("ab"), lit("")]), &ctx),
        Some(arr(vec![Value::Str("a".into()), Value::Str("b".into())]))
    );
    assert_eq!(l3_ctx(&call("split", vec![lit("a")]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("split", vec![field("scalar"), lit(",")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("split", vec![lit("a"), field("scalar")]), &ctx),
        None
    );
}

// ===========================================================================
// builtins.rs — numeric builtins + error branches
// ===========================================================================

#[test]
fn builtin_math_funcs() {
    let ctx = ctx_with(vec![]);
    let n = |v: f64| Expr::Number(v);
    let neg = |v: f64| Expr::Neg(Box::new(Expr::Number(v)));

    assert_eq!(
        l3_ctx(&call("abs", vec![neg(5.0)]), &ctx),
        Some(Value::Number(5.0))
    );
    assert_eq!(
        l3_ctx(&call("abs", vec![n(2.5)]), &ctx),
        Some(Value::Number(2.5))
    );
    assert_eq!(l3_ctx(&call("abs", vec![lit("x")]), &ctx), None);
    assert_eq!(l3_ctx(&call("abs", vec![]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("round", vec![n(2.567)]), &ctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&call("round", vec![n(2.567), n(2.0)]), &ctx),
        Some(Value::Number(2.57))
    );
    assert_eq!(
        l3_ctx(&call("round", vec![n(1234.5), neg(2.0)]), &ctx),
        Some(Value::Number(1200.0))
    );
    assert_eq!(l3_ctx(&call("round", vec![lit("x")]), &ctx), None);
    assert_eq!(l3_ctx(&call("round", vec![n(1.0), lit("x")]), &ctx), None);
    // non-finite value / non-finite or huge precision → None
    assert_eq!(l3_ctx(&call("round", vec![n(f64::NAN)]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("round", vec![n(1.0), n(f64::NAN)]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("round", vec![n(1.0), n(1e300)]), &ctx), None);
    assert_eq!(l3_ctx(&call("round", vec![n(1.0), n(400.0)]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("ceil", vec![n(2.1)]), &ctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&call("ceil", vec![neg(2.1)]), &ctx),
        Some(Value::Number(-2.0))
    );
    assert_eq!(
        l3_ctx(&call("floor", vec![n(2.9)]), &ctx),
        Some(Value::Number(2.0))
    );
    assert_eq!(
        l3_ctx(&call("floor", vec![neg(2.1)]), &ctx),
        Some(Value::Number(-3.0))
    );
    assert_eq!(l3_ctx(&call("ceil", vec![lit("x")]), &ctx), None);
    assert_eq!(l3_ctx(&call("floor", vec![lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("sqrt", vec![n(16.0)]), &ctx),
        Some(Value::Number(4.0))
    );
    assert_eq!(l3_ctx(&call("sqrt", vec![neg(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("sqrt", vec![lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("pow", vec![n(2.0), n(8.0)]), &ctx),
        Some(Value::Number(256.0))
    );
    // non-finite result → None
    assert_eq!(l3_ctx(&call("pow", vec![n(0.0), neg(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("pow", vec![n(2.0)]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("log", vec![n(std::f64::consts::E)]), &ctx),
        Some(Value::Number(1.0))
    );
    assert_eq!(
        l3_ctx(&call("log", vec![n(100.0), n(10.0)]), &ctx),
        Some(Value::Number(2.0))
    );
    assert_eq!(l3_ctx(&call("log", vec![n(0.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![neg(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![n(100.0), n(0.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![n(100.0), n(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![n(100.0), neg(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![lit("x")]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![n(1.0), lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("exp", vec![n(0.0)]), &ctx),
        Some(Value::Number(1.0))
    );
    // overflow → None
    assert_eq!(l3_ctx(&call("exp", vec![n(1000.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("exp", vec![lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("clamp", vec![n(120.0), n(0.0), n(100.0)]), &ctx),
        Some(Value::Number(100.0))
    );
    assert_eq!(
        l3_ctx(&call("clamp", vec![n(-10.0), n(0.0), n(100.0)]), &ctx),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        l3_ctx(&call("clamp", vec![n(50.0), n(0.0), n(100.0)]), &ctx),
        Some(Value::Number(50.0))
    );
    // min > max → None
    assert_eq!(
        l3_ctx(&call("clamp", vec![n(50.0), n(10.0), n(5.0)]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("clamp", vec![n(1.0), n(2.0)]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("clamp", vec![lit("x"), n(1.0), n(2.0)]), &ctx),
        None
    );

    assert_eq!(
        l3_ctx(&call("sign", vec![neg(5.0)]), &ctx),
        Some(Value::Number(-1.0))
    );
    assert_eq!(
        l3_ctx(&call("sign", vec![n(5.0)]), &ctx),
        Some(Value::Number(1.0))
    );
    assert_eq!(
        l3_ctx(&call("sign", vec![n(0.0)]), &ctx),
        Some(Value::Number(1.0))
    );
    // non-finite → None
    assert_eq!(l3_ctx(&call("sign", vec![n(f64::NAN)]), &ctx), None);
    assert_eq!(l3_ctx(&call("sign", vec![lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("trunc", vec![n(2.9)]), &ctx),
        Some(Value::Number(2.0))
    );
    assert_eq!(
        l3_ctx(&call("trunc", vec![neg(2.9)]), &ctx),
        Some(Value::Number(-2.0))
    );
    assert_eq!(l3_ctx(&call("trunc", vec![lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("is_finite", vec![n(1.0)]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("is_finite", vec![n(f64::INFINITY)]), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&call("is_finite", vec![n(f64::NAN)]), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(l3_ctx(&call("is_finite", vec![lit("x")]), &ctx), None);
}

// ===========================================================================
// builtins.rs — fmt/concat/join/indexof/replace_plain/any + errors
// ===========================================================================

#[test]
fn builtin_fmt_concat_join() {
    let ctx = ctx_with(vec![
        ("a", Value::Str("x".into())),
        ("n", Value::Number(3.0)),
        ("arr", arr(vec![Value::Str("q".into())])),
    ]);
    // fmt
    assert_eq!(
        l3_ctx(
            &call("fmt", vec![lit("{}:{}!"), lit("a"), Expr::Number(3.0)]),
            &ctx
        ),
        Some(Value::Str("a:3!".into()))
    );
    // placeholder count mismatch → None
    assert_eq!(
        l3_ctx(&call("fmt", vec![lit("{}"), lit("a"), lit("b")]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("fmt", vec![]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("fmt", vec![Expr::Number(1.0), lit("a")]), &ctx),
        None
    );
    // concat
    assert_eq!(
        l3_ctx(&call("concat", vec![lit("ip="), lit("1.1.1.1")]), &ctx),
        Some(Value::Str("ip=1.1.1.1".into()))
    );
    assert_eq!(
        l3_ctx(&call("concat", vec![lit("n="), field("n")]), &ctx),
        Some(Value::Str("n=3".into()))
    );
    assert_eq!(l3_ctx(&call("concat", vec![]), &ctx), None);
    // join: scalar fields only, missing field → empty string
    assert_eq!(
        l3_ctx(&call("join", vec![field("a"), lit("!")]), &ctx),
        Some(Value::Str("x!".into()))
    );
    assert_eq!(
        l3_ctx(&call("join", vec![field("a"), field("missing")]), &ctx),
        Some(Value::Str("x".into()))
    );
    // join with array → None
    assert_eq!(l3_ctx(&call("join", vec![field("arr")]), &ctx), None);
    assert_eq!(l3_ctx(&call("join", vec![]), &ctx), None);
    // join_by
    assert_eq!(
        l3_ctx(&call("join_by", vec![lit("|"), field("a"), lit("y")]), &ctx),
        Some(Value::Str("x|y".into()))
    );
    assert_eq!(
        l3_ctx(
            &call("join_by", vec![lit("|"), field("a"), field("missing")]),
            &ctx
        ),
        Some(Value::Str("x|".into()))
    );
    assert_eq!(l3_ctx(&call("join_by", vec![lit("|")]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("join_by", vec![Expr::Number(1.0), lit("a")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("join_by", vec![lit("|"), field("arr")]), &ctx),
        None
    );
    // indexof
    assert_eq!(
        l3_ctx(
            &call("indexof", vec![lit("hello world"), lit("world")]),
            &ctx
        ),
        Some(Value::Number(6.0))
    );
    assert_eq!(
        l3_ctx(&call("indexof", vec![lit("hello"), lit("zzz")]), &ctx),
        Some(Value::Number(-1.0))
    );
    assert_eq!(l3_ctx(&call("indexof", vec![lit("a")]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("indexof", vec![Expr::Number(1.0), lit("a")]), &ctx),
        None
    );
    // replace_plain
    assert_eq!(
        l3_ctx(
            &call("replace_plain", vec![lit("a-b-a"), lit("a"), lit("x")]),
            &ctx
        ),
        Some(Value::Str("x-b-x".into()))
    );
    assert_eq!(
        l3_ctx(&call("replace_plain", vec![lit("a"), lit("b")]), &ctx),
        None
    );
    // startswith_any / endswith_any
    assert_eq!(
        l3_ctx(
            &call(
                "startswith_any",
                vec![lit("prefix123"), lit("nope"), lit("pre")]
            ),
            &ctx
        ),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(
            &call("startswith_any", vec![lit("abc"), lit("x"), lit("y")]),
            &ctx
        ),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(
            &call(
                "endswith_any",
                vec![lit("123suffix"), lit("nope"), lit("fix")]
            ),
            &ctx
        ),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(
            &call("endswith_any", vec![lit("abc"), lit("x"), lit("y")]),
            &ctx
        ),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&call("startswith_any", vec![lit("abc")]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("endswith_any", vec![lit("abc")]), &ctx), None);
}

// ===========================================================================
// builtins.rs — null/blank/coalesce + errors
// ===========================================================================

#[test]
fn builtin_null_blank_funcs() {
    let ctx = ctx_with(vec![
        ("empty", Value::Str(String::new().into())),
        ("spaces", Value::Str(" \t\n ".into())),
        ("host", Value::Str("example.org".into())),
        ("fallback", Value::Str("fb".into())),
        ("n", Value::Number(42.0)),
    ]);
    // coalesce: skips null and blank, returns first good value
    assert_eq!(
        l3_ctx(&call("coalesce", vec![field("missing"), lit("fb")]), &ctx),
        Some(Value::Str("fb".into()))
    );
    assert_eq!(
        l3_ctx(
            &call("coalesce", vec![field("spaces"), field("host")]),
            &ctx
        ),
        Some(Value::Str("example.org".into()))
    );
    assert_eq!(
        l3_ctx(
            &call(
                "coalesce",
                vec![field("empty"), field("spaces"), field("missing")]
            ),
            &ctx
        ),
        None
    );
    assert_eq!(l3_ctx(&call("coalesce", vec![]), &ctx), None);
    // isnull / isnotnull
    assert_eq!(
        l3_ctx(&call("isnull", vec![field("missing")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("isnull", vec![field("host")]), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&call("isnotnull", vec![field("host")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("isnotnull", vec![field("missing")]), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(l3_ctx(&call("isnull", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("isnotnull", vec![]), &ctx), None);
    // is_blank
    assert_eq!(
        l3_ctx(&call("is_blank", vec![field("empty")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("is_blank", vec![field("spaces")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("is_blank", vec![field("host")]), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&call("is_blank", vec![field("missing")]), &ctx),
        Some(Value::Bool(true))
    );
    // number → None
    assert_eq!(l3_ctx(&call("is_blank", vec![field("n")]), &ctx), None);
    assert_eq!(l3_ctx(&call("is_blank", vec![]), &ctx), None);
    // null_if_blank
    assert_eq!(
        l3_ctx(&call("null_if_blank", vec![field("spaces")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("null_if_blank", vec![field("host")]), &ctx),
        Some(Value::Str("example.org".into()))
    );
    assert_eq!(l3_ctx(&call("null_if_blank", vec![field("n")]), &ctx), None);
    assert_eq!(l3_ctx(&call("null_if_blank", vec![]), &ctx), None);
    // default_if_blank
    assert_eq!(
        l3_ctx(
            &call("default_if_blank", vec![field("spaces"), lit("fb")]),
            &ctx
        ),
        Some(Value::Str("fb".into()))
    );
    assert_eq!(
        l3_ctx(
            &call("default_if_blank", vec![field("missing"), lit("fb")]),
            &ctx
        ),
        Some(Value::Str("fb".into()))
    );
    assert_eq!(
        l3_ctx(
            &call("default_if_blank", vec![field("host"), lit("fb")]),
            &ctx
        ),
        Some(Value::Str("example.org".into()))
    );
    assert_eq!(
        l3_ctx(&call("default_if_blank", vec![field("n"), lit("fb")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("default_if_blank", vec![field("host")]), &ctx),
        None
    );
}

// ===========================================================================
// builtins.rs — hash/id functions + errors
// ===========================================================================

#[test]
fn builtin_hash_funcs() {
    let ctx = ctx_with(vec![
        ("msg", Value::Str("hello".into())),
        ("n", Value::Number(42.0)),
    ]);
    assert_eq!(
        l3_ctx(&call("md5", vec![lit("hello")]), &ctx),
        Some(Value::Str("5d41402abc4b2a76b9719d911017c592".into()))
    );
    assert_eq!(
        l3_ctx(&call("sha1", vec![lit("hello")]), &ctx),
        Some(Value::Str(
            "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d".into()
        ))
    );
    assert_eq!(
        l3_ctx(&call("sha1_n", vec![lit("hello"), Expr::Number(8.0)]), &ctx),
        Some(Value::Str("aaf4c61d".into()))
    );
    assert_eq!(
        l3_ctx(&call("sha256", vec![lit("hello")]), &ctx),
        Some(Value::Str(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into()
        ))
    );
    assert_eq!(
        l3_ctx(&call("hex", vec![lit("hello")]), &ctx),
        Some(Value::Str("68656c6c6f".into()))
    );
    // sha1_n validation: out of 1..=40, fractional, wrong type
    assert_eq!(
        l3_ctx(&call("sha1_n", vec![lit("x"), Expr::Number(0.0)]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("sha1_n", vec![lit("x"), Expr::Number(41.0)]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("sha1_n", vec![lit("x"), Expr::Number(2.5)]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("sha1_n", vec![lit("x"), lit("8")]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("sha1_n", vec![lit("x")]), &ctx), None);
    // wrong arg counts / wrong types
    assert_eq!(l3_ctx(&call("md5", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("md5", vec![field("n")]), &ctx), None);
    assert_eq!(l3_ctx(&call("sha1", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("sha256", vec![field("n")]), &ctx), None);
    assert_eq!(l3_ctx(&call("hex", vec![field("n")]), &ctx), None);

    // stable_id: typed, length-prefixed hash segments
    assert_eq!(
        l3_ctx(
            &call(
                "stable_id",
                vec![lit("alert_"), lit("10.0.0.1"), Expr::Number(3.0)]
            ),
            &ctx
        ),
        Some(Value::Str("alert_ba0dab7ccfb2a04c".into()))
    );
    assert_eq!(l3_ctx(&call("stable_id", vec![lit("p")]), &ctx), None);
    // array/object values are not hashable → None
    let arr_ctx = ctx_with(vec![("a", arr(vec![Value::Number(1.0)]))]);
    assert_eq!(
        l3_ctx(&call("stable_id", vec![lit("p"), field("a")]), &arr_ctx),
        None
    );
}

// ===========================================================================
// builtins.rs — time/regex builtins + errors
// ===========================================================================

#[test]
fn builtin_now_variants() {
    let ctx = ctx_with(vec![]);
    for (name, min_ts) in [
        ("now", 1_000_000_000_000.0),
        ("now_s", 1_000_000_000.0),
        ("now_ms", 1_000_000_000_000.0),
        ("now_us", 1_000_000_000_000_000.0),
        ("now_ns", 1_000_000_000_000_000_000.0),
    ] {
        let result = l3_ctx(&call(name, vec![]), &ctx);
        let Some(Value::Number(v)) = result else {
            panic!("{}() should return a number, got {:?}", name, result);
        };
        assert!(v > min_ts, "{}() timestamp too small: {}", name, v);
        // args are rejected
        assert_eq!(l3_ctx(&call(name, vec![Expr::Number(1.0)]), &ctx), None);
    }
    // now() and now_ms() share the same eval-time snapshot
    let expr = Expr::BinOp {
        op: BinOp::Sub,
        left: Box::new(call("now", vec![])),
        right: Box::new(call("now_ms", vec![])),
    };
    assert_eq!(l3_ctx(&expr, &ctx), Some(Value::Number(0.0)));
}

#[test]
fn builtin_strftime_strptime() {
    let ctx = ctx_with(vec![("ts", Value::Number(0.0)), ("n", Value::Number(7.0))]);
    // explicit format
    assert_eq!(
        l3_ctx(
            &call("strftime", vec![Expr::Number(0.0), lit("%Y-%m-%d")]),
            &ctx
        ),
        Some(Value::Str("1970-01-01".into()))
    );
    // default format from score meta (None → DEFAULT_OUTPUT_TIME_FORMAT)
    let expr = call("strftime", vec![Expr::Number(0.0)]);
    assert_eq!(
        eval_expr_with_l3(&expr, &ctx, YieldMeta::default()),
        Some(Value::Str("1970-01-01 00:00:00.000".into()))
    );
    // score.time_format override
    let expr = call("strftime", vec![Expr::Number(0.0)]);
    assert_eq!(
        eval_expr_with_l3(
            &expr,
            &ctx,
            YieldMeta {
                time_format: Some("%Y"),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Str("1970".into()))
    );
    // ts from a field
    assert_eq!(
        l3_ctx(&call("strftime", vec![field("ts"), lit("%Y")]), &ctx),
        Some(Value::Str("1970".into()))
    );
    // errors
    assert_eq!(
        l3_ctx(&call("strftime", vec![lit("x"), lit("%Y")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("strftime", vec![Expr::Number(0.0), Expr::Number(1.0)]),
            &ctx
        ),
        None
    );
    assert_eq!(l3_ctx(&call("strftime", vec![]), &ctx), None);
    assert_eq!(
        l3_ctx(
            &call("strftime", vec![Expr::Number(f64::NAN), lit("%Y")]),
            &ctx
        ),
        None
    );
    // strptime: naive date / naive datetime / offset datetime / failure
    assert_eq!(
        l3_ctx(
            &call("strptime", vec![lit("1970-01-01"), lit("%Y-%m-%d")]),
            &ctx
        ),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        l3_ctx(
            &call(
                "strptime",
                vec![lit("2024-03-11 00:00:00"), lit("%Y-%m-%d %H:%M:%S")]
            ),
            &ctx
        ),
        Some(Value::Number(1_710_115_200_000.0))
    );
    assert_eq!(
        l3_ctx(
            &call(
                "strptime",
                vec![lit("2024-03-11T00:00:00+08:00"), lit("%Y-%m-%dT%H:%M:%S%z")]
            ),
            &ctx
        ),
        Some(Value::Number(1_710_086_400_000.0))
    );
    assert_eq!(
        l3_ctx(
            &call("strptime", vec![lit("not-a-date"), lit("%Y-%m-%d")]),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(&call("strptime", vec![Expr::Number(1.0), lit("%Y")]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("strptime", vec![lit("x")]), &ctx), None);
}

#[test]
fn builtin_regex_time_funcs() {
    let ctx = ctx_with(vec![]);
    assert_eq!(
        l3_ctx(
            &call("regex_match", vec![lit("failed_login"), lit("fail.*")]),
            &ctx
        ),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(
            &call("regex_match", vec![lit("success"), lit("^fail")]),
            &ctx
        ),
        Some(Value::Bool(false))
    );
    // invalid pattern → None
    assert_eq!(
        l3_ctx(&call("regex_match", vec![lit("abc"), lit("(")]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("regex_match", vec![lit("abc")]), &ctx), None);
    assert_eq!(
        l3_ctx(
            &call("regex_match", vec![Expr::Number(1.0), lit("a")]),
            &ctx
        ),
        None
    );

    // time_diff in seconds (millis input)
    assert_eq!(
        l3_ctx(
            &call(
                "time_diff",
                vec![
                    Expr::Number(1_700_000_005_000.0),
                    Expr::Number(1_700_000_000_000.0)
                ]
            ),
            &ctx
        ),
        Some(Value::Number(5.0))
    );
    assert_eq!(
        l3_ctx(
            &call(
                "time_diff",
                vec![
                    Expr::Number(1_700_000_000_000.0),
                    Expr::Number(1_700_000_005_000.0)
                ]
            ),
            &ctx
        ),
        Some(Value::Number(5.0))
    );
    assert_eq!(
        l3_ctx(&call("time_diff", vec![Expr::Number(1.0)]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("time_diff", vec![lit("x"), Expr::Number(1.0)]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("time_diff", vec![Expr::Number(f64::NAN), Expr::Number(1.0)]),
            &ctx
        ),
        None
    );

    // time_bucket
    assert_eq!(
        l3_ctx(
            &call(
                "time_bucket",
                vec![Expr::Number(1_700_000_075_000.0), Expr::Number(60.0)]
            ),
            &ctx
        ),
        Some(Value::Number(1_700_000_040_000.0))
    );
    // bucket_end = bucket + interval
    assert_eq!(
        l3_ctx(
            &call(
                "bucket_end",
                vec![Expr::Number(1_700_000_075_000.0), Expr::Number(60.0)]
            ),
            &ctx
        ),
        Some(Value::Number(1_700_000_100_000.0))
    );
    // invalid intervals
    for bad in [0.0, -60.0, f64::INFINITY, f64::NAN] {
        assert_eq!(
            l3_ctx(
                &call(
                    "time_bucket",
                    vec![Expr::Number(1_700_000_075_000.0), Expr::Number(bad)]
                ),
                &ctx
            ),
            None
        );
        assert_eq!(
            l3_ctx(
                &call(
                    "bucket_end",
                    vec![Expr::Number(1_700_000_075_000.0), Expr::Number(bad)]
                ),
                &ctx
            ),
            None
        );
    }
    assert_eq!(
        l3_ctx(&call("time_bucket", vec![Expr::Number(1.0)]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("bucket_end", vec![Expr::Number(1.0)]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("time_bucket", vec![lit("x"), Expr::Number(60.0)]),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("bucket_end", vec![lit("x"), Expr::Number(60.0)]),
            &ctx
        ),
        None
    );
}

// ===========================================================================
// builtins.rs — unknown function, aggregate funcs, L3 funcs, stat selectors
// ===========================================================================

#[test]
fn builtin_unknown_name_returns_none() {
    let ctx = ctx_with(vec![]);
    assert_eq!(
        l3_ctx(&call("no_such_func", vec![Expr::Number(1.0)]), &ctx),
        None
    );
}

fn step_ctx(values: Vec<Value>) -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("_step_0_values".into(), Value::Array(values));
    fields.insert("_step_0_source".into(), Value::Str("e".into()));
    fields.insert("_step_0_label".into(), Value::Str("fail".into()));
    fields.insert("_step_0_measure".into(), Value::Number(6.0));
    Event { fields }
}

#[test]
fn builtin_l3_funcs() {
    let ctx = step_ctx(vec![
        Value::Number(10.0),
        Value::Number(20.0),
        Value::Number(30.0),
    ]);
    assert_eq!(
        l3_ctx(&call("collect_list", vec![field("e")]), &ctx),
        Some(arr(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0)
        ]))
    );
    assert_eq!(
        l3_ctx(&call("collect_set", vec![field("e")]), &ctx),
        Some(arr(vec![
            Value::Number(10.0),
            Value::Number(20.0),
            Value::Number(30.0)
        ]))
    );
    assert_eq!(
        l3_ctx(&call("first", vec![field("e")]), &ctx),
        Some(Value::Number(10.0))
    );
    assert_eq!(
        l3_ctx(&call("last", vec![field("e")]), &ctx),
        Some(Value::Number(30.0))
    );
    // stddev of [10,20,30] = sqrt(200/3) ≈ 8.1649658
    let Some(Value::Number(sd)) = l3_ctx(&call("stddev", vec![field("e")]), &ctx) else {
        panic!("stddev expected number");
    };
    assert!((sd - 8.16496580927726).abs() < 1e-9, "stddev = {}", sd);
    // percentile
    assert_eq!(
        l3_ctx(
            &call("percentile", vec![field("e"), Expr::Number(50.0)]),
            &ctx
        ),
        Some(Value::Number(20.0))
    );
    assert_eq!(
        l3_ctx(
            &call("percentile", vec![field("e"), Expr::Number(0.0)]),
            &ctx
        ),
        Some(Value::Number(10.0))
    );
    assert_eq!(
        l3_ctx(
            &call("percentile", vec![field("e"), Expr::Number(100.0)]),
            &ctx
        ),
        Some(Value::Number(30.0))
    );
    // wrong arg counts
    assert_eq!(
        l3_ctx(&call("collect_list", vec![field("e"), field("e")]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("percentile", vec![field("e")]), &ctx), None);
    assert_eq!(l3_ctx(&call("first", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("stddev", vec![]), &ctx), None);
    // non-numeric percentile arg → None
    assert_eq!(
        l3_ctx(&call("percentile", vec![field("e"), lit("x")]), &ctx),
        None
    );

    // empty series: first/last None, stddev 0, percentile 0, collect_list []
    let empty = step_ctx(vec![]);
    assert_eq!(l3_ctx(&call("first", vec![field("e")]), &empty), None);
    assert_eq!(l3_ctx(&call("last", vec![field("e")]), &empty), None);
    assert_eq!(
        l3_ctx(&call("stddev", vec![field("e")]), &empty),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        l3_ctx(
            &call("percentile", vec![field("e"), Expr::Number(50.0)]),
            &empty
        ),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        l3_ctx(&call("collect_list", vec![field("e")]), &empty),
        Some(arr(vec![]))
    );
    // single value: stddev → 0 (needs >= 2 numbers)
    let single = step_ctx(vec![Value::Number(7.0)]);
    assert_eq!(
        l3_ctx(&call("stddev", vec![field("e")]), &single),
        Some(Value::Number(0.0))
    );

    // dedup in collect_set
    let dup = step_ctx(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("a".into()),
    ]);
    assert_eq!(
        l3_ctx(&call("collect_set", vec![field("e")]), &dup),
        Some(arr(vec![Value::Str("a".into()), Value::Str("b".into())]))
    );

    // bind series path: Qualified(b, x) with _bind_b_field_x present
    let mut bf = EngineHashMap::default();
    bf.insert(
        "_bind_b_field_x".into(),
        arr(vec![Value::Number(1.0), Value::Number(2.0)]),
    );
    bf.insert("_bind_b_count".into(), Value::Number(2.0));
    let bctx = Event { fields: bf };
    let qualified = Expr::Field(FieldRef::Qualified("b".to_string(), "x".to_string()));
    assert_eq!(
        l3_ctx(&call("collect_list", vec![qualified.clone()]), &bctx),
        Some(arr(vec![Value::Number(1.0), Value::Number(2.0)]))
    );
    assert_eq!(
        l3_ctx(&call("first", vec![qualified.clone()]), &bctx),
        Some(Value::Number(1.0))
    );
    assert_eq!(
        l3_ctx(&call("last", vec![qualified]), &bctx),
        Some(Value::Number(2.0))
    );
}

#[test]
fn builtin_aggregate_funcs() {
    let ctx = step_ctx(vec![
        Value::Number(1.0),
        Value::Number(2.0),
        Value::Number(3.0),
    ]);
    // Simple field matching step source alias "e" → measure-based aggregates
    assert_eq!(
        l3_ctx(&call("sum", vec![field("e")]), &ctx),
        Some(Value::Number(6.0))
    );
    assert_eq!(
        l3_ctx(&call("count", vec![field("e")]), &ctx),
        Some(Value::Number(6.0))
    );
    assert_eq!(
        l3_ctx(&call("avg", vec![field("e")]), &ctx),
        Some(Value::Number(6.0))
    );
    assert_eq!(
        l3_ctx(&call("min", vec![field("e")]), &ctx),
        Some(Value::Number(6.0))
    );
    assert_eq!(
        l3_ctx(&call("max", vec![field("e")]), &ctx),
        Some(Value::Number(6.0))
    );

    // Qualified field → value-based aggregates over _step_0_field_x
    let mut fields = EngineHashMap::default();
    fields.insert(
        "_step_0_field_x".into(),
        arr(vec![
            Value::Number(1.0),
            Value::Number(2.0),
            Value::Number(3.0),
        ]),
    );
    fields.insert(
        "_step_0_values".into(),
        arr(vec![
            Value::Number(1.0),
            Value::Number(2.0),
            Value::Number(3.0),
        ]),
    );
    fields.insert("_step_0_source".into(), Value::Str("e".into()));
    let qctx = Event { fields };
    let qualified = Expr::Field(FieldRef::Qualified("e".to_string(), "x".to_string()));
    assert_eq!(
        l3_ctx(&call("count", vec![qualified.clone()]), &qctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&call("sum", vec![qualified.clone()]), &qctx),
        Some(Value::Number(6.0))
    );
    assert_eq!(
        l3_ctx(&call("avg", vec![qualified.clone()]), &qctx),
        Some(Value::Number(2.0))
    );
    assert_eq!(
        l3_ctx(&call("min", vec![qualified.clone()]), &qctx),
        Some(Value::Number(1.0))
    );
    assert_eq!(
        l3_ctx(&call("max", vec![qualified]), &qctx),
        Some(Value::Number(3.0))
    );

    // value-based over strings: min/max compare lexicographically, sum skips non-numeric
    let mut fields = EngineHashMap::default();
    fields.insert(
        "_step_0_field_x".into(),
        arr(vec![
            Value::Str("b".into()),
            Value::Str("a".into()),
            Value::Str("c".into()),
        ]),
    );
    fields.insert(
        "_step_0_values".into(),
        arr(vec![
            Value::Str("b".into()),
            Value::Str("a".into()),
            Value::Str("c".into()),
        ]),
    );
    fields.insert("_step_0_source".into(), Value::Str("e".into()));
    let sctx = Event { fields };
    let qualified = Expr::Field(FieldRef::Qualified("e".to_string(), "x".to_string()));
    assert_eq!(
        l3_ctx(&call("min", vec![qualified.clone()]), &sctx),
        Some(Value::Str("a".into()))
    );
    assert_eq!(
        l3_ctx(&call("max", vec![qualified.clone()]), &sctx),
        Some(Value::Str("c".into()))
    );
    assert_eq!(
        l3_ctx(&call("sum", vec![qualified.clone()]), &sctx),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        l3_ctx(&call("avg", vec![qualified.clone()]), &sctx),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        l3_ctx(&call("count", vec![qualified]), &sctx),
        Some(Value::Number(3.0))
    );

    // count via bind count (_bind_b_count), no steps
    let bctx = ctx_with(vec![("_bind_b_count", Value::Number(7.0))]);
    assert_eq!(
        l3_ctx(&call("count", vec![field("b")]), &bctx),
        Some(Value::Number(7.0))
    );

    // empty series aggregates: step exists but no values/measure → None
    // (step_ctx sets `_step_0_measure` = 6.0, so drop it for the empty case)
    let mut fields = EngineHashMap::default();
    fields.insert("_step_0_values".into(), arr(vec![]));
    fields.insert("_step_0_source".into(), Value::Str("e".into()));
    let empty = Event { fields };
    assert_eq!(l3_ctx(&call("sum", vec![field("e")]), &empty), None);
    assert_eq!(l3_ctx(&call("avg", vec![field("e")]), &empty), None);
    assert_eq!(l3_ctx(&call("min", vec![field("e")]), &empty), None);
    assert_eq!(l3_ctx(&call("max", vec![field("e")]), &empty), None);
    assert_eq!(l3_ctx(&call("count", vec![field("e")]), &empty), None);
    // no steps at all → aggregates over plain fields resolve to None
    let bare = ctx_with(vec![]);
    assert_eq!(l3_ctx(&call("sum", vec![field("x")]), &bare), None);
    // unknown aggregate name → None (count/sum/avg/min/max only)
    assert_eq!(l3_ctx(&call("median", vec![field("e")]), &ctx), None);
    // wrong arg count → None
    assert_eq!(
        l3_ctx(&call("sum", vec![field("e"), field("e")]), &ctx),
        None
    );
}

#[test]
fn builtin_aggregate_over_helpers() {
    // over numbers: count/sum/avg/min/max, empty avg/min/max → 0.0
    assert_eq!(
        eval_aggregate_over_numbers("count", &[1.0, 2.0, 3.0]),
        Some(Value::Number(6.0))
    );
    assert_eq!(
        eval_aggregate_over_numbers("sum", &[1.0, 2.0, 3.0]),
        Some(Value::Number(6.0))
    );
    assert_eq!(
        eval_aggregate_over_numbers("avg", &[1.0, 2.0, 3.0]),
        Some(Value::Number(2.0))
    );
    assert_eq!(
        eval_aggregate_over_numbers("min", &[3.0, 1.0, 2.0]),
        Some(Value::Number(1.0))
    );
    assert_eq!(
        eval_aggregate_over_numbers("max", &[3.0, 1.0, 2.0]),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        eval_aggregate_over_numbers("avg", &[]),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        eval_aggregate_over_numbers("min", &[]),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        eval_aggregate_over_numbers("max", &[]),
        Some(Value::Number(0.0))
    );
    assert_eq!(eval_aggregate_over_numbers("nope", &[1.0]), None);

    // over values: numeric coercion + string min/max via compare_sortable_values
    let values = vec![Value::Number(1.0), Value::Number(2.0), Value::Number(3.0)];
    assert_eq!(
        eval_aggregate_over_values("count", &values),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        eval_aggregate_over_values("sum", &values),
        Some(Value::Number(6.0))
    );
    assert_eq!(
        eval_aggregate_over_values("avg", &values),
        Some(Value::Number(2.0))
    );
    let strings = vec![Value::Str("b".into()), Value::Str("a".into())];
    assert_eq!(
        eval_aggregate_over_values("min", &strings),
        Some(Value::Str("a".into()))
    );
    assert_eq!(
        eval_aggregate_over_values("max", &strings),
        Some(Value::Str("b".into()))
    );
    assert_eq!(
        eval_aggregate_over_values("sum", &strings),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        eval_aggregate_over_values("avg", &strings),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        eval_aggregate_over_values("count", &[]),
        Some(Value::Number(0.0))
    );
    assert_eq!(eval_aggregate_over_values("nope", &values), None);

    assert_eq!(
        numeric_values(&[Value::Number(1.0), Value::Str("x".into())]),
        vec![1.0]
    );
    assert_eq!(
        sum_numeric_values(&[Value::Number(1.0), Value::Str("x".into())]),
        1.0
    );
}

#[test]
fn builtin_stat_selectors() {
    // is_stat_selector_func
    for name in [
        "window_event",
        "match_event",
        "match_distinct",
        "trigger",
        "final",
    ] {
        assert!(is_stat_selector_func(name));
    }
    assert!(!is_stat_selector_func("count"));

    // stat.count(window_event(x)) reads _bind_x_count
    let ctx = ctx_with(vec![
        ("_bind_x_count", Value::Number(5.0)),
        ("label", Value::Number(9.0)),
    ]);
    let count_expr = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "count".to_string(),
        args: vec![call("window_event", vec![field("x")])],
    };
    assert_eq!(l3_ctx(&count_expr, &ctx), Some(Value::Number(5.0)));
    // stat.count(match_event(label)) reads the label field directly
    let match_expr = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "count".to_string(),
        args: vec![call("match_event", vec![field("label")])],
    };
    assert_eq!(l3_ctx(&match_expr, &ctx), Some(Value::Number(9.0)));
    // stat.count(match_distinct(label)) same read path
    let distinct_expr = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "count".to_string(),
        args: vec![call("match_distinct", vec![field("label")])],
    };
    assert_eq!(l3_ctx(&distinct_expr, &ctx), Some(Value::Number(9.0)));
    // stat.value(trigger(label)) / stat.value(final(label))
    let trigger_expr = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "value".to_string(),
        args: vec![call("trigger", vec![field("label")])],
    };
    assert_eq!(l3_ctx(&trigger_expr, &ctx), Some(Value::Number(9.0)));
    let final_expr = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "value".to_string(),
        args: vec![call("final", vec![field("label")])],
    };
    assert_eq!(l3_ctx(&final_expr, &ctx), Some(Value::Number(9.0)));
    // unknown stat selector name → None
    let unknown_sel = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "count".to_string(),
        args: vec![call("no_selector", vec![field("x")])],
    };
    assert_eq!(l3_ctx(&unknown_sel, &ctx), None);
    // selector with wrong arg count → None
    let bad_args = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "count".to_string(),
        args: vec![call("window_event", vec![field("x"), field("y")])],
    };
    assert_eq!(l3_ctx(&bad_args, &ctx), None);
    // selector with a non-field arg → None
    let non_field = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "count".to_string(),
        args: vec![call("window_event", vec![Expr::Number(1.0)])],
    };
    assert_eq!(l3_ctx(&non_field, &ctx), None);
    // stat.count with wrong arg count → None
    let bad_count = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "count".to_string(),
        args: vec![
            call("window_event", vec![field("x")]),
            call("window_event", vec![field("y")]),
        ],
    };
    assert_eq!(l3_ctx(&bad_count, &ctx), None);
    // stat selector without qualifier → None
    assert_eq!(l3_ctx(&call("window_event", vec![field("x")]), &ctx), None);
    assert_eq!(l3_ctx(&call("final", vec![field("x")]), &ctx), None);
    // stat.value(trigger(x)) on missing field → None
    let missing = ctx_with(vec![]);
    let trigger_missing = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "value".to_string(),
        args: vec![call("trigger", vec![field("nope")])],
    };
    assert_eq!(l3_ctx(&trigger_missing, &missing), None);
    // non-numeric field → None
    let str_ctx = ctx_with(vec![("label", Value::Str("x".into()))]);
    let trigger_str = Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: "value".to_string(),
        args: vec![call("trigger", vec![field("label")])],
    };
    assert_eq!(l3_ctx(&trigger_str, &str_ctx), None);
}

// ===========================================================================
// executor/eval/utils.rs — helper coverage
// ===========================================================================

#[test]
fn utils_normalize_index_and_sort() {
    assert_eq!(utils::normalize_index(0, 4), Some(0));
    assert_eq!(utils::normalize_index(3, 4), Some(3));
    assert_eq!(utils::normalize_index(-1, 4), Some(3));
    assert_eq!(utils::normalize_index(4, 4), None);
    assert_eq!(utils::normalize_index(-5, 4), None);
    assert_eq!(utils::normalize_index(0, 0), None);

    use std::cmp::Ordering;
    assert_eq!(
        utils::compare_sortable_values(&Value::Number(1.0), &Value::Number(2.0)),
        Ordering::Less
    );
    assert_eq!(
        utils::compare_sortable_values(&Value::Str("b".into()), &Value::Str("a".into())),
        Ordering::Greater
    );
    assert_eq!(
        utils::compare_sortable_values(&Value::Bool(false), &Value::Bool(true)),
        Ordering::Less
    );
    // mixed types fall back to string comparison
    assert_eq!(
        utils::compare_sortable_values(&Value::Number(2.0), &Value::Str("10".into())),
        Ordering::Greater
    );
    assert_eq!(
        utils::compare_sortable_values(&Value::Array(vec![]), &Value::Number(1.0)),
        Ordering::Greater
    );
}

#[test]
fn utils_f64_helpers() {
    assert_eq!(utils::f64_to_i64_trunc(3.7), Some(3));
    assert_eq!(utils::f64_to_i64_trunc(-3.7), Some(-3));
    assert_eq!(utils::f64_to_i64_trunc(0.0), Some(0));
    assert_eq!(utils::f64_to_i64_trunc(f64::NAN), None);
    assert_eq!(utils::f64_to_i64_trunc(f64::INFINITY), None);
    assert_eq!(utils::f64_to_i64_trunc(1e300), None);
    assert_eq!(utils::f64_to_i64_trunc(-1e300), None);

    assert_eq!(utils::round_with_precision(2.567, 2), Some(2.57));
    assert_eq!(utils::round_with_precision(2.5, 0), Some(3.0));
    assert_eq!(utils::round_with_precision(1234.5, -2), Some(1200.0));
    assert_eq!(utils::round_with_precision(5.0, -1), Some(10.0));
    assert_eq!(utils::round_with_precision(f64::NAN, 2), None);
    assert_eq!(utils::round_with_precision(1.0, 400), None);
    assert_eq!(utils::round_with_precision(1.0, -400), None);
}

#[test]
fn utils_fmt_template() {
    let values = vec![Value::Str("a".into()), Value::Number(3.0)];
    assert_eq!(
        utils::apply_fmt_template("{}:{}!", &values),
        Some("a:3!".to_string())
    );
    assert_eq!(utils::apply_fmt_template("{}", &values), None);
    assert_eq!(utils::apply_fmt_template("no placeholders", &values), None);
    assert_eq!(utils::apply_fmt_template("", &[]), Some(String::new()));
}

#[test]
fn utils_time_helpers() {
    let dt = utils::timestamp_nanos_to_utc(0).unwrap();
    assert_eq!(dt.to_rfc3339(), "1970-01-01T00:00:00+00:00");
    let dt = utils::timestamp_nanos_to_utc(1_700_000_000_123_456_789).unwrap();
    assert_eq!(dt.timestamp(), 1_700_000_000);

    assert_eq!(
        utils::time_nanos_to_value(1_700_000_000_123_000_000),
        Value::Number(1_700_000_000_123.0)
    );
    match utils::time_nanos_to_expr(1_700_000_000_000_000_000) {
        Expr::Number(v) => assert_eq!(v, 1_700_000_000_000.0),
        other => panic!("expected Number expr, got {:?}", other),
    }

    // parse_time_to_timestamp_nanos: offset datetime / naive datetime / naive date / failure
    assert_eq!(
        utils::parse_time_to_timestamp_nanos("2024-03-11T00:00:00+08:00", "%Y-%m-%dT%H:%M:%S%z"),
        Some(1_710_086_400_000_000_000)
    );
    assert_eq!(
        utils::parse_time_to_timestamp_nanos("2024-03-11 00:00:00", "%Y-%m-%d %H:%M:%S"),
        Some(1_710_115_200_000_000_000)
    );
    assert_eq!(
        utils::parse_time_to_timestamp_nanos("1970-01-01", "%Y-%m-%d"),
        Some(0)
    );
    assert_eq!(
        utils::parse_time_to_timestamp_nanos("junk", "%Y-%m-%d"),
        None
    );

    assert!(utils::is_blank_str(""));
    assert!(utils::is_blank_str(" \t\n "));
    assert!(!utils::is_blank_str("x"));

    let ctx = ctx_with(vec![]);
    assert_eq!(
        utils::eval_single_string_arg_with_l3(&[lit("hi")], &ctx, YieldMeta::default()),
        Some("hi".to_string())
    );
    assert_eq!(
        utils::eval_single_string_arg_with_l3(&[], &ctx, YieldMeta::default()),
        None
    );
    assert_eq!(
        utils::eval_single_string_arg_with_l3(&[Expr::Number(1.0)], &ctx, YieldMeta::default()),
        None
    );
}

#[test]
fn utils_stable_id_hash_and_time() {
    // number / string / bool values hash to Some; array / object → None
    assert_eq!(
        utils::update_stable_id_hash(&mut Sha256::new(), &Value::Number(42.0)),
        Some(())
    );
    assert_eq!(
        utils::update_stable_id_hash(&mut Sha256::new(), &Value::Str("x".into())),
        Some(())
    );
    assert_eq!(
        utils::update_stable_id_hash(&mut Sha256::new(), &Value::Bool(true)),
        Some(())
    );
    assert_eq!(
        utils::update_stable_id_hash(&mut Sha256::new(), &Value::Array(vec![Value::Number(1.0)])),
        None
    );
    let mut obj = EngineHashMap::default();
    obj.insert("k".into(), Value::Number(1.0));
    assert_eq!(
        utils::update_stable_id_hash(&mut Sha256::new(), &Value::Object(obj)),
        None
    );

    // deterministic + type-tagged: number 42 and str "42" differ
    let mut h1 = Sha256::new();
    utils::update_stable_id_hash(&mut h1, &Value::Number(42.0)).unwrap();
    let mut h2 = Sha256::new();
    utils::update_stable_id_hash(&mut h2, &Value::Str("42".into())).unwrap();
    let (d1, d2) = (hex::encode(h1.finalize()), hex::encode(h2.finalize()));
    assert_ne!(d1, d2);

    assert!(utils::current_time_nanos().is_some());
    assert!(super::get_or_init_eval_time_nanos().is_some());
}

// ===========================================================================
// executor/eval/mod.rs — eval_score / eval_entity_id / eval_bool_expr / fallback
// ===========================================================================

#[test]
fn eval_score_clamps_and_rejects() {
    let ctx = ctx_with(vec![]);
    assert_eq!(eval_score(&Expr::Number(70.0), &ctx).unwrap(), 70.0);
    assert_eq!(eval_score(&Expr::Number(150.0), &ctx).unwrap(), 100.0);
    assert_eq!(eval_score(&Expr::Number(-5.0), &ctx).unwrap(), 0.0);
    assert_eq!(eval_score(&Expr::Number(33.3), &ctx).unwrap(), 33.3);
    // non-numeric value → Err
    assert!(eval_score(&lit("abc"), &ctx).is_err());
    // None → Err
    assert!(eval_score(&field("missing"), &ctx).is_err());
}

#[test]
fn eval_entity_id_stringifies() {
    let ctx = ctx_with(vec![]);
    assert_eq!(eval_entity_id(&lit("abc"), &ctx).unwrap(), "abc");
    assert_eq!(eval_entity_id(&Expr::Number(42.0), &ctx).unwrap(), "42");
    assert_eq!(eval_entity_id(&Expr::Bool(true), &ctx).unwrap(), "true");
    // None → eval_yield_expr falls back to an empty string
    assert_eq!(eval_entity_id(&field("missing"), &ctx).unwrap(), "");
}

#[test]
fn eval_bool_expr_strict_bool() {
    let ctx = ctx_with(vec![]);
    assert_eq!(eval_bool_expr(&Expr::Bool(true), &ctx), Some(true));
    assert_eq!(eval_bool_expr(&Expr::Bool(false), &ctx), Some(false));
    assert_eq!(eval_bool_expr(&Expr::Number(1.0), &ctx), None);
    assert_eq!(eval_bool_expr(&lit("x"), &ctx), None);
    assert_eq!(eval_bool_expr(&field("missing"), &ctx), None);
}

#[test]
fn eval_yield_expr_falls_back_to_empty_string() {
    let ctx = ctx_with(vec![]);
    assert_eq!(
        eval_yield_expr(&field("missing"), &ctx),
        Some(Value::Str(String::new().into()))
    );
    assert_eq!(
        eval_yield_expr(&Expr::Number(5.0), &ctx),
        Some(Value::Number(5.0))
    );
    // score-aware variant
    assert_eq!(
        eval_yield_expr_with_score(&Expr::SystemVar(SystemVar::Score), &ctx, Some(70.0)),
        Some(Value::Number(70.0))
    );
    assert_eq!(
        eval_yield_expr_with_score(&Expr::SystemVar(SystemVar::Score), &ctx, None),
        Some(Value::Str(String::new().into()))
    );
}

// ===========================================================================
// executor/eval/mod.rs — eval_expr_with_l3 expression branches
// ===========================================================================

#[test]
fn l3_expression_literals_and_fields() {
    let ctx = ctx_with(vec![("x", Value::Number(5.0))]);
    assert_eq!(l3_ctx(&Expr::Number(1.5), &ctx), Some(Value::Number(1.5)));
    assert_eq!(l3_ctx(&lit("hi"), &ctx), Some(Value::Str("hi".into())));
    assert_eq!(l3_ctx(&Expr::Bool(false), &ctx), Some(Value::Bool(false)));
    assert_eq!(l3_ctx(&field("x"), &ctx), Some(Value::Number(5.0)));
    assert_eq!(l3_ctx(&field("missing"), &ctx), None);
    // Neg
    assert_eq!(
        l3_ctx(&Expr::Neg(Box::new(Expr::Number(5.0))), &ctx),
        Some(Value::Number(-5.0))
    );
    assert_eq!(l3_ctx(&Expr::Neg(Box::new(lit("x"))), &ctx), None);
}

#[test]
fn l3_expression_system_and_wfu_vars() {
    let ctx = ctx_with(vec![]);
    // score system var
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::Score),
            &ctx,
            YieldMeta {
                score: Some(70.0),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(70.0))
    );
    assert_eq!(l3_ctx(&Expr::SystemVar(SystemVar::Score), &ctx), None);
    // event-first-time system var → millis
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::EventFirstTime),
            &ctx,
            YieldMeta {
                event_first_time_nanos: Some(1_700_000_000_123_000_000),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(1_700_000_000_123.0))
    );
    // emit time system var
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::EmitTime),
            &ctx,
            YieldMeta {
                emit_time_nanos: Some(1_700_000_000_000_000_000),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(1_700_000_000_000.0))
    );
    // wfu meta fields
    let meta = YieldMeta {
        wfx_id: Some("wx-1"),
        rule_name: Some("r"),
        score: Some(80.0),
        entity_type: Some("ip"),
        entity_id: Some("eid"),
        origin: Some("origin"),
        close_reason: Some("cr"),
        fired_at: Some("fa"),
        emit_time: Some("et"),
        summary: Some("sum"),
        ..YieldMeta::default()
    };
    let check = |f: WfuMetaField, expected: Value| {
        assert_eq!(
            eval_expr_with_l3(&Expr::WfuMeta(f), &ctx, meta),
            Some(expected)
        );
    };
    check(WfuMetaField::Id, Value::Str("wx-1".into()));
    check(WfuMetaField::RuleName, Value::Str("r".into()));
    check(WfuMetaField::Score, Value::Number(80.0));
    check(WfuMetaField::EntityType, Value::Str("ip".into()));
    check(WfuMetaField::EntityId, Value::Str("eid".into()));
    check(WfuMetaField::Origin, Value::Str("origin".into()));
    check(WfuMetaField::CloseReason, Value::Str("cr".into()));
    check(WfuMetaField::FiredAt, Value::Str("fa".into()));
    check(WfuMetaField::EmitTime, Value::Str("et".into()));
    check(WfuMetaField::Summary, Value::Str("sum".into()));
    // unresolvable wfu meta → None
    assert_eq!(
        eval_expr_with_l3(&Expr::WfuMeta(WfuMetaField::Id), &ctx, YieldMeta::default()),
        None
    );
}

#[test]
fn l3_expression_object_array_inlist() {
    let ctx = ctx_with(vec![]);
    // object literal: multiple targets get the same value
    let obj = Expr::Object(vec![ObjectItem {
        targets: vec!["a".to_string(), "b".to_string()],
        type_hint: None,
        value: Expr::Number(1.0),
    }]);
    let Some(Value::Object(map)) = l3_ctx(&obj, &ctx) else {
        panic!("expected object");
    };
    assert_eq!(map.get("a"), Some(&Value::Number(1.0)));
    assert_eq!(map.get("b"), Some(&Value::Number(1.0)));
    // array literal
    assert_eq!(
        l3_ctx(&Expr::Array(vec![Expr::Number(1.0), lit("x")]), &ctx),
        Some(arr(vec![Value::Number(1.0), Value::Str("x".into())]))
    );
    // in-list
    let in_list = |negated: bool| Expr::InList {
        expr: Box::new(Expr::Number(2.0)),
        list: vec![Expr::Number(1.0), Expr::Number(2.0), Expr::Number(3.0)],
        negated,
    };
    assert_eq!(l3_ctx(&in_list(false), &ctx), Some(Value::Bool(true)));
    assert_eq!(l3_ctx(&in_list(true), &ctx), Some(Value::Bool(false)));
    let not_found = Expr::InList {
        expr: Box::new(Expr::Number(9.0)),
        list: vec![Expr::Number(1.0)],
        negated: false,
    };
    assert_eq!(l3_ctx(&not_found, &ctx), Some(Value::Bool(false)));
    // missing target → None
    let missing = Expr::InList {
        expr: Box::new(field("nope")),
        list: vec![Expr::Number(1.0)],
        negated: false,
    };
    assert_eq!(l3_ctx(&missing, &ctx), None);
}

#[test]
fn l3_expression_arith_and_compare() {
    let ctx = ctx_with(vec![]);
    let binop = |op: BinOp, l: Expr, r: Expr| Expr::BinOp {
        op,
        left: Box::new(l),
        right: Box::new(r),
    };
    let num = |v: f64| Expr::Number(v);
    assert_eq!(
        l3_ctx(&binop(BinOp::Add, num(1.0), num(2.0)), &ctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Sub, num(5.0), num(2.0)), &ctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Mul, num(5.0), num(2.0)), &ctx),
        Some(Value::Number(10.0))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Div, num(6.0), num(2.0)), &ctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Mod, num(5.0), num(2.0)), &ctx),
        Some(Value::Number(1.0))
    );
    // div / mod by zero → None
    assert_eq!(l3_ctx(&binop(BinOp::Div, num(1.0), num(0.0)), &ctx), None);
    assert_eq!(l3_ctx(&binop(BinOp::Mod, num(1.0), num(0.0)), &ctx), None);
    // non-numeric operand → None
    assert_eq!(l3_ctx(&binop(BinOp::Add, num(1.0), lit("x")), &ctx), None);

    // comparisons: numbers / strings / bools / mismatch
    assert_eq!(
        l3_ctx(&binop(BinOp::Eq, num(1.0), num(1.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Ne, num(1.0), num(2.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Lt, num(1.0), num(2.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Gt, num(2.0), num(1.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Le, num(2.0), num(2.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Ge, num(2.0), num(2.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Lt, num(3.0), num(2.0)), &ctx),
        Some(Value::Bool(false))
    );
    // string comparison
    assert_eq!(
        l3_ctx(&binop(BinOp::Lt, lit("a"), lit("b")), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Gt, lit("b"), lit("a")), &ctx),
        Some(Value::Bool(true))
    );
    // bool comparison
    assert_eq!(
        l3_ctx(&binop(BinOp::Lt, Expr::Bool(false), Expr::Bool(true)), &ctx),
        Some(Value::Bool(true))
    );
    // mismatch: order comparisons → false, Ne → true (values_equal mismatch)
    assert_eq!(
        l3_ctx(&binop(BinOp::Lt, num(1.0), lit("a")), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Eq, num(1.0), lit("1")), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Ne, num(1.0), lit("1")), &ctx),
        Some(Value::Bool(true))
    );
}

#[test]
fn l3_expression_logic_ops() {
    let ctx = ctx_with(vec![]);
    let and = |l: Expr, r: Expr| Expr::BinOp {
        op: BinOp::And,
        left: Box::new(l),
        right: Box::new(r),
    };
    let or = |l: Expr, r: Expr| Expr::BinOp {
        op: BinOp::Or,
        left: Box::new(l),
        right: Box::new(r),
    };
    let t = Expr::Bool(true);
    let f = Expr::Bool(false);
    assert_eq!(
        l3_ctx(&and(t.clone(), t.clone()), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&and(f.clone(), t.clone()), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&and(t.clone(), f.clone()), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(l3_ctx(&and(t.clone(), field("missing")), &ctx), None);
    assert_eq!(
        l3_ctx(&and(field("missing"), f.clone()), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&or(t.clone(), f.clone()), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&or(f.clone(), t.clone()), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&or(f.clone(), f.clone()), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(l3_ctx(&or(f.clone(), field("missing")), &ctx), None);
}

#[test]
fn l3_expression_if_then_else() {
    let ctx = ctx_with(vec![]);
    let ite = |cond: Expr| Expr::IfThenElse {
        cond: Box::new(cond),
        then_expr: Box::new(Expr::Number(1.0)),
        else_expr: Box::new(Expr::Number(2.0)),
    };
    assert_eq!(
        l3_ctx(&ite(Expr::Bool(true)), &ctx),
        Some(Value::Number(1.0))
    );
    assert_eq!(
        l3_ctx(&ite(Expr::Bool(false)), &ctx),
        Some(Value::Number(2.0))
    );
    // non-bool condition → None
    assert_eq!(l3_ctx(&ite(Expr::Number(1.0)), &ctx), None);
}

#[test]
fn l3_materializes_system_vars_in_func_args() {
    let ctx = ctx_with(vec![]);
    // abs(@score) → materialize score to 70.0 → plain eval abs → 70
    let expr = call("abs", vec![Expr::SystemVar(SystemVar::Score)]);
    assert_eq!(
        eval_expr_with_l3(
            &expr,
            &ctx,
            YieldMeta {
                score: Some(70.0),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(70.0))
    );
    // concat with a wfu meta arg
    let expr = call("concat", vec![lit("id="), Expr::WfuMeta(WfuMetaField::Id)]);
    assert_eq!(
        eval_expr_with_l3(
            &expr,
            &ctx,
            YieldMeta {
                wfx_id: Some("wx"),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Str("id=wx".into()))
    );
    // unresolvable system var in func args → None
    let expr = call("abs", vec![Expr::SystemVar(SystemVar::Score)]);
    assert_eq!(l3_ctx(&expr, &ctx), None);
}

// ===========================================================================
// builtins.rs — contains_system_var / materialize_system_vars
// ===========================================================================

#[test]
fn contains_system_var_detection() {
    assert!(!contains_system_var(&Expr::Number(1.0)));
    assert!(!contains_system_var(&lit("x")));
    assert!(contains_system_var(&Expr::SystemVar(SystemVar::Score)));
    assert!(contains_system_var(&Expr::WfuMeta(WfuMetaField::Id)));
    assert!(contains_system_var(&Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(Expr::Number(1.0)),
        right: Box::new(Expr::SystemVar(SystemVar::Score)),
    }));
    assert!(contains_system_var(&Expr::Neg(Box::new(Expr::SystemVar(
        SystemVar::Score
    )))));
    assert!(contains_system_var(&call(
        "abs",
        vec![Expr::SystemVar(SystemVar::Score)]
    )));
    assert!(contains_system_var(&Expr::Object(vec![ObjectItem {
        targets: vec!["k".to_string()],
        type_hint: None,
        value: Expr::SystemVar(SystemVar::Score),
    }])));
    assert!(contains_system_var(&Expr::Array(vec![Expr::SystemVar(
        SystemVar::Score
    )])));
    assert!(contains_system_var(&Expr::InList {
        expr: Box::new(Expr::SystemVar(SystemVar::Score)),
        list: vec![Expr::Number(1.0)],
        negated: false,
    }));
    assert!(contains_system_var(&Expr::IfThenElse {
        cond: Box::new(Expr::Bool(true)),
        then_expr: Box::new(Expr::Number(1.0)),
        else_expr: Box::new(Expr::SystemVar(SystemVar::Score)),
    }));
}

#[test]
fn materialize_system_vars_rewrites() {
    let score = YieldMeta {
        score: Some(70.0),
        event_first_time_nanos: Some(1_700_000_000_000_000_000),
        event_last_time_nanos: Some(1_700_000_000_000_000_000),
        window_start_time_nanos: Some(1_700_000_000_000_000_000),
        window_end_time_nanos: Some(1_700_000_000_000_000_000),
        emit_time_nanos: Some(1_700_000_000_000_000_000),
        wfx_id: Some("wx"),
        ..YieldMeta::default()
    };
    assert_eq!(
        materialize_system_vars(&Expr::Number(1.0), score),
        Some(Expr::Number(1.0))
    );
    assert_eq!(materialize_system_vars(&lit("a"), score), Some(lit("a")));
    assert_eq!(
        materialize_system_vars(&Expr::Bool(true), score),
        Some(Expr::Bool(true))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::Score), score),
        Some(Expr::Number(70.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::EventFirstTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::EventLastTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::WindowStartTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::WindowEndTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::EmitTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    // wfu meta: string / number / bool values
    assert_eq!(
        materialize_system_vars(&Expr::WfuMeta(WfuMetaField::Id), score),
        Some(lit("wx"))
    );
    let score_meta = YieldMeta {
        score: Some(90.0),
        ..YieldMeta::default()
    };
    assert_eq!(
        materialize_system_vars(&Expr::WfuMeta(WfuMetaField::Score), score_meta),
        Some(Expr::Number(90.0))
    );
    // unresolvable wfu meta → None
    assert_eq!(
        materialize_system_vars(&Expr::WfuMeta(WfuMetaField::Id), YieldMeta::default()),
        None
    );
    // unsupported expr → None
    assert_eq!(
        materialize_system_vars(&Expr::PresetParam("p".to_string()), score),
        None
    );
    // structural rewrites
    let binop = Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(Expr::SystemVar(SystemVar::Score)),
        right: Box::new(Expr::Number(1.0)),
    };
    match materialize_system_vars(&binop, score) {
        Some(Expr::BinOp { left, right, .. }) => {
            assert_eq!(*left, Expr::Number(70.0));
            assert_eq!(*right, Expr::Number(1.0));
        }
        other => panic!("expected rewritten binop, got {:?}", other),
    }
    let neg = Expr::Neg(Box::new(Expr::SystemVar(SystemVar::Score)));
    assert_eq!(
        materialize_system_vars(&neg, score),
        Some(Expr::Neg(Box::new(Expr::Number(70.0))))
    );
    let call_expr = call("abs", vec![Expr::SystemVar(SystemVar::Score)]);
    match materialize_system_vars(&call_expr, score) {
        Some(Expr::FuncCall { name, args, .. }) => {
            assert_eq!(name, "abs");
            assert_eq!(args, vec![Expr::Number(70.0)]);
        }
        other => panic!("expected rewritten func call, got {:?}", other),
    }
    // unresolvable nested system var → whole tree None
    assert_eq!(
        materialize_system_vars(
            &call("abs", vec![Expr::SystemVar(SystemVar::Score)]),
            YieldMeta::default()
        ),
        None
    );
}

#[test]
fn builtin_dispatch_entry_points() {
    let ctx = ctx_with(vec![]);
    // direct builtin dispatch
    assert_eq!(
        eval_builtin_func_with_l3("abs", &[Expr::Number(-5.0)], &ctx, YieldMeta::default()),
        Some(Value::Number(5.0))
    );
    assert_eq!(
        eval_builtin_func_with_l3("unknown", &[], &ctx, YieldMeta::default()),
        None
    );
    // direct L3 func dispatch
    let step = step_ctx(vec![Value::Number(1.0), Value::Number(2.0)]);
    assert_eq!(
        eval_l3_func("collect_list", &[field("e")], &step, YieldMeta::default()),
        Some(arr(vec![Value::Number(1.0), Value::Number(2.0)]))
    );
    assert_eq!(
        eval_l3_func("nope", &[field("e")], &step, YieldMeta::default()),
        None
    );
    assert_eq!(
        eval_l3_func("collect_list", &[], &step, YieldMeta::default()),
        None
    );
    // direct aggregate dispatch
    assert_eq!(
        eval_aggregate_func("sum", &[field("e")], &step),
        Some(Value::Number(6.0))
    );
    assert_eq!(eval_aggregate_func("nope", &[field("e")], &step), None);
    assert_eq!(
        eval_aggregate_func("sum", &[Expr::Number(1.0)], &step),
        None
    );
    // direct stat dispatch
    let sctx = ctx_with(vec![("_bind_x_count", Value::Number(3.0))]);
    assert_eq!(
        eval_stat_func("count", &[call("window_event", vec![field("x")])], &sctx),
        Some(Value::Number(3.0))
    );
    // non-selector arg / unknown selector name → None
    assert_eq!(eval_stat_func("count", &[field("x")], &sctx), None);
    assert_eq!(
        eval_stat_func("nope", &[call("trigger", vec![field("x")])], &sctx),
        None
    );
}

/// review 2026-08-31：`contains_*` 递归检测必须穿透 match 表达式（issue #79
/// Issue 2）——否则 `fmt("{}", match x { ... => stat.value(final(t)) })` 里
/// 的 stat/时间/聚合/L3 函数藏在不同分支，判定为不含 → 走 L1 eval → 求值
/// None（静默错值）。
#[test]
fn contains_selector_checks_penetrate_match() {
    use super::{
        contains_aggregate_func, contains_eval_time_func, contains_l3_func, contains_stat_selector,
    };
    use wf_lang::ast::MatchArm;

    // `match x { "a" => <selector>, _ => 0 }`——selector 藏在分支值里。
    let arms = |value: Expr| vec![MatchArm {
        patterns: vec![lit("a")],
        value,
    }];
    let m = |value: Expr| Expr::Match {
        expr: Box::new(field("sev")),
        arms: arms(value),
        default: Some(Box::new(Expr::Number(0.0))),
    };

    let stat = m(Expr::FuncCall {
        qualifier: Some("stat".into()),
        name: "value".into(),
        args: vec![Expr::FuncCall {
            qualifier: None,
            name: "final".into(),
            args: vec![field("t")],
        }],
    });
    assert!(contains_stat_selector(&stat), "match 分支里的 stat.* 必须被检测");

    let time = m(Expr::FuncCall {
        qualifier: None,
        name: "now_ns".into(),
        args: vec![],
    });
    assert!(contains_eval_time_func(&time), "match 分支里的 now 系列必须被检测");

    let agg = m(Expr::FuncCall {
        qualifier: None,
        name: "sum".into(),
        args: vec![field("x")],
    });
    assert!(contains_aggregate_func(&agg), "match 分支里的聚合函数必须被检测");

    let l3 = m(Expr::FuncCall {
        qualifier: None,
        name: "collect_list".into(),
        args: vec![field("e")],
    });
    assert!(contains_l3_func(&l3), "match 分支里的 L3 函数必须被检测");
}

/// review 2026-08-31：match 表达式在 L3 求值器内可求值，且分支值里的时间
/// 函数（now_ns）走 L3 时间工具——验证 eval_expr_with_l3 的 Match 分支与
/// contains_eval_time_func 穿透的协同。
#[test]
fn eval_expr_with_l3_match_branch_time_func() {
    use wf_lang::ast::MatchArm;
    let ctx = ctx_with(vec![("sev", Value::Str("crit".into()))]);
    let expr = Expr::Match {
        expr: Box::new(field("sev")),
        arms: vec![MatchArm {
            patterns: vec![lit("crit")],
            value: Expr::FuncCall {
                qualifier: None,
                name: "now_ns".into(),
                args: vec![],
            },
        }],
        default: Some(Box::new(Expr::Number(0.0))),
    };
    let v = eval_expr_with_l3(&expr, &ctx, YieldMeta::default());
    let Some(Value::Number(ns)) = v else {
        panic!("now_ns in match branch should evaluate via L3, got {v:?}");
    };
    assert!(ns > 1_700_000_000_000_000_000.0, "now_ns 应为当前墙钟（ns）");
}
