//! L2 表达式求值——空值对象 / 哈希 ID / 字符串·多值内建与 external 分发（2026-09-04 自
//! expr.rs 拆出；`#[path]` 兄弟子模块，共享 import 在父文件 expr.rs，此处经
//! `use super::*` 复用）。主题：blank 系（is_blank / null_if_blank / default_if_blank /
//! coalesce）、merge 对象浅合并、hash 系（md5/sha1/sha1_n/sha256/hex/stable_id/join/
//! join_by）、strptime 时间解析、str·mv 族（replace/trim/substr/startswith/endswith/
//! mvcount/mvjoin/mvindex/mvappend/split/mvdedup）、external 全局 handler 分发。

use super::*;

#[test]
fn blank_functions_work() {
    use crate::match_engine::cep::{Event, eval_expr};

    let mut fields = EngineHashMap::default();
    fields.insert("empty".into(), Value::Str(String::new().into()));
    fields.insert("spaces".into(), Value::Str(" \t\n ".into()));
    fields.insert("host".into(), Value::Str("example.org".into()));
    fields.insert("fallback".into(), Value::Str("fallback".into()));
    fields.insert("n".into(), Value::Number(42.0));
    let event = Event { fields };

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

    assert_eq!(eval_expr(&is_empty_expr, &event), Some(Value::Bool(true)));
    assert_eq!(eval_expr(&is_spaces_expr, &event), Some(Value::Bool(true)));
    assert_eq!(eval_expr(&is_host_expr, &event), Some(Value::Bool(false)));
    assert_eq!(eval_expr(&is_missing_expr, &event), Some(Value::Bool(true)));
    assert_eq!(eval_expr(&null_if_blank_expr, &event), None);
    assert_eq!(
        eval_expr(&null_if_host_expr, &event),
        Some(Value::Str("example.org".into()))
    );
    assert_eq!(
        eval_expr(&default_blank_expr, &event),
        Some(Value::Str("fallback".into()))
    );
    assert_eq!(
        eval_expr(&default_host_expr, &event),
        Some(Value::Str("example.org".into()))
    );
    assert_eq!(
        eval_expr(&coalesce_blank_expr, &event),
        Some(Value::Str("fallback".into()))
    );
    assert_eq!(
        eval_expr(&coalesce_direct_blank_expr, &event),
        Some(Value::Str("example.org".into()))
    );
    assert_eq!(eval_expr(&coalesce_all_blank_expr, &event), None);
    assert_eq!(eval_expr(&invalid_type_expr, &event), None);
}

#[test]
fn merge_shallow_merges_objects_in_l2_eval() {
    use crate::match_engine::cep::{Event, eval_expr};

    let mut base = EngineHashMap::default();
    base.insert("severity".into(), Value::Number(3.0));
    base.insert("rule".into(), Value::Str("webshell".into()));

    let mut fields = EngineHashMap::default();
    fields.insert("extension".into(), Value::Object(base));
    let event = Event { fields };

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "merge".to_string(),
        args: vec![
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

    let Some(Value::Object(object)) = eval_expr(&expr, &event) else {
        panic!("expected object");
    };
    assert_eq!(object.get("rule"), Some(&Value::Str("webshell".into())));
    assert_eq!(object.get("source"), Some(&Value::Str("wfl".into())));
    assert_eq!(object.get("severity"), Some(&Value::Number(10.0)));
}

#[test]
fn merge_fails_when_object_literal_value_is_missing_in_l2_eval() {
    use crate::match_engine::cep::{Event, eval_expr};

    let event = Event {
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

    assert_eq!(eval_expr(&expr, &event), None);
}

#[test]
fn merge_treats_missing_field_arg_as_empty_object_in_l2_eval() {
    use crate::match_engine::cep::{Event, eval_expr};

    let event = Event {
        fields: EngineHashMap::default(),
    };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "merge".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("missing_extension".to_string())),
            Expr::Object(vec![ObjectItem {
                targets: vec!["source".to_string()],
                type_hint: None,
                value: Expr::StringLit("wfl".to_string()),
            }]),
        ],
    };

    let Some(Value::Object(object)) = eval_expr(&expr, &event) else {
        panic!("expected object");
    };
    assert_eq!(object.get("source"), Some(&Value::Str("wfl".into())));
}

#[test]
fn hash_and_id_functions_work() {
    use crate::match_engine::cep::{Event, eval_expr};

    let mut fields = EngineHashMap::default();
    fields.insert("msg".into(), Value::Str("hello".into()));
    fields.insert("empty".into(), Value::Str(String::new().into()));
    fields.insert("ip".into(), Value::Str("10.0.0.1".into()));
    fields.insert("count".into(), Value::Number(3.0));
    fields.insert("special".into(), Value::Str("a|b".into()));
    fields.insert("percent".into(), Value::Str("10%".into()));
    let event = Event { fields };

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
    let short_expr = Expr::FuncCall {
        qualifier: None,
        name: "substr".to_string(),
        args: vec![sha256_expr.clone(), Expr::Number(1.0), Expr::Number(16.0)],
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
    let invalid_expr = Expr::FuncCall {
        qualifier: None,
        name: "md5".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("count".to_string()))],
    };

    assert_eq!(
        eval_expr(&md5_expr, &event),
        Some(Value::Str("5d41402abc4b2a76b9719d911017c592".into()))
    );
    assert_eq!(
        eval_expr(&sha1_expr, &event),
        Some(Value::Str(
            "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d".into()
        ))
    );
    assert_eq!(
        eval_expr(&sha1_n_expr, &event),
        Some(Value::Str("aaf4c61d".into()))
    );
    assert_eq!(
        eval_expr(&sha1_n_empty_expr, &event),
        Some(Value::Str("da39a3ee".into()))
    );
    assert_eq!(
        eval_expr(&sha256_expr, &event),
        Some(Value::Str(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into()
        ))
    );
    assert_eq!(
        eval_expr(&hex_expr, &event),
        Some(Value::Str("68656c6c6f".into()))
    );
    assert_eq!(
        eval_expr(&short_expr, &event),
        Some(Value::Str("2cf24dba5fb0a30e".into()))
    );
    assert_eq!(
        eval_expr(&join_expr, &event),
        Some(Value::Str("a|b10%3".into()))
    );
    assert_eq!(
        eval_expr(&join_by_expr, &event),
        Some(Value::Str("a|b|10%||3".into()))
    );
    assert_eq!(
        eval_expr(&join_missing_expr, &event),
        Some(Value::Str("a|b10%".into()))
    );
    assert_eq!(
        eval_expr(&join_by_missing_expr, &event),
        Some(Value::Str("a|b||10%".into()))
    );
    assert_eq!(eval_expr(&join_array_expr, &event), None);
    assert_eq!(eval_expr(&join_by_object_expr, &event), None);
    assert_eq!(eval_expr(&join_invalid_nested_expr, &event), None);
    assert_eq!(eval_expr(&join_by_invalid_nested_expr, &event), None);
    let Some(Value::Str(stable_id)) = eval_expr(&stable_expr, &event) else {
        panic!("stable_id() should return a string");
    };
    assert_eq!(stable_id, "alert_ba0dab7ccfb2a04c");
    assert_eq!(
        eval_expr(&stable_expr, &event),
        Some(Value::Str(stable_id.clone()))
    );
    let Some(Value::Str(changed_stable_id)) = eval_expr(&stable_changed_expr, &event) else {
        panic!("stable_id() should return a string for changed input");
    };
    assert!(changed_stable_id.starts_with("alert_"));
    assert_eq!(changed_stable_id.len(), "alert_".len() + 16);
    assert_ne!(changed_stable_id, stable_id);
    assert_eq!(eval_expr(&invalid_expr, &event), None);
}

#[test]
fn stable_id_uses_unambiguous_segments() {
    use crate::match_engine::cep::{Event, eval_expr};

    let event = Event {
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
        eval_expr(&first_expr, &event),
        Some(Value::Str("id_234c47ae916c73b0".into()))
    );
    assert_eq!(
        eval_expr(&second_expr, &event),
        Some(Value::Str("id_1532803f7ab9f6de".into()))
    );
    assert_ne!(
        eval_expr(&first_expr, &event),
        eval_expr(&second_expr, &event)
    );
}

#[test]
fn strptime_parses_date() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "strptime".to_string(),
        args: vec![
            Expr::StringLit("1970-01-01".to_string()),
            Expr::StringLit("%Y-%m-%d".to_string()),
        ],
    };
    let event = Event {
        fields: EngineHashMap::default(),
    };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Number(0.0)));
}

#[test]
fn strptime_returns_epoch_milliseconds() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "strptime".to_string(),
        args: vec![
            Expr::StringLit("2024-03-11 00:00:00".to_string()),
            Expr::StringLit("%Y-%m-%d %H:%M:%S".to_string()),
        ],
    };
    let event = Event {
        fields: EngineHashMap::default(),
    };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Number(1_710_115_200_000.0))
    );
}

// ===========================================================================
// replace / trim / mvcount / mvjoin / mvindex / mvappend / split / mvdedup
// ===========================================================================

#[test]
fn replace_regex_substitution() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "replace".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("action".to_string())),
            Expr::StringLit("fail.*".to_string()),
            Expr::StringLit("blocked".to_string()),
        ],
    };
    let mut fields = EngineHashMap::default();
    fields.insert("action".into(), Value::Str("failed_login".into()));
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Str("blocked".into())));
}

#[test]
fn startswith_and_endswith_work() {
    use crate::match_engine::cep::{Event, eval_expr};

    let starts = Expr::FuncCall {
        qualifier: None,
        name: "startswith".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("failed".to_string()),
        ],
    };
    let ends = Expr::FuncCall {
        qualifier: None,
        name: "endswith".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::StringLit("root".to_string()),
        ],
    };
    let mut fields = EngineHashMap::default();
    fields.insert("msg".into(), Value::Str("failed_login_root".into()));
    let event = Event { fields };
    assert_eq!(eval_expr(&starts, &event), Some(Value::Bool(true)));
    assert_eq!(eval_expr(&ends, &event), Some(Value::Bool(true)));
}

#[test]
fn substr_supports_one_based_and_negative_start() {
    use crate::match_engine::cep::{Event, eval_expr};

    let mut fields = EngineHashMap::default();
    fields.insert("msg".into(), Value::Str("abcdef".into()));
    let event = Event { fields };

    let one_based = Expr::FuncCall {
        qualifier: None,
        name: "substr".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::Number(2.0),
            Expr::Number(3.0),
        ],
    };
    assert_eq!(
        eval_expr(&one_based, &event),
        Some(Value::Str("bcd".into()))
    );

    let negative = Expr::FuncCall {
        qualifier: None,
        name: "substr".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("msg".to_string())),
            Expr::Neg(Box::new(Expr::Number(2.0))),
        ],
    };
    assert_eq!(eval_expr(&negative, &event), Some(Value::Str("ef".into())));
}

#[test]
fn trim_removes_surrounding_whitespace() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "trim".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("msg".to_string()))],
    };
    let mut fields = EngineHashMap::default();
    fields.insert("msg".into(), Value::Str("  hello\t".into()));
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Str("hello".into())));
}

#[test]
fn mvcount_array_returns_length() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvcount".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("vals".to_string()))],
    };
    let mut fields = EngineHashMap::default();
    fields.insert(
        "vals".into(),
        Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]),
    );
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Number(3.0)));
}

#[test]
fn mvjoin_array_with_separator() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvjoin".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("vals".to_string())),
            Expr::StringLit("|".to_string()),
        ],
    };
    let mut fields = EngineHashMap::default();
    fields.insert(
        "vals".into(),
        Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]),
    );
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Str("a|b|c".into())));
}

#[test]
fn mvindex_single_and_range() {
    use crate::match_engine::cep::{Event, eval_expr};

    let mut fields = EngineHashMap::default();
    fields.insert(
        "vals".into(),
        Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
            Value::Str("d".into()),
        ]),
    );
    let event = Event { fields };

    let single = Expr::FuncCall {
        qualifier: None,
        name: "mvindex".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("vals".to_string())),
            Expr::Neg(Box::new(Expr::Number(1.0))),
        ],
    };
    assert_eq!(eval_expr(&single, &event), Some(Value::Str("d".into())));

    let range = Expr::FuncCall {
        qualifier: None,
        name: "mvindex".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("vals".to_string())),
            Expr::Number(1.0),
            Expr::Number(2.0),
        ],
    };
    assert_eq!(
        eval_expr(&range, &event),
        Some(Value::Array(vec![
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]))
    );
}

#[test]
fn mvappend_flattens_arrays_and_scalars() {
    use crate::match_engine::cep::{Event, eval_expr};

    let mut fields = EngineHashMap::default();
    fields.insert(
        "vals".into(),
        Value::Array(vec![Value::Str("a".into()), Value::Str("b".into())]),
    );
    let event = Event { fields };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvappend".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("vals".to_string())),
            Expr::StringLit("c".to_string()),
            Expr::FuncCall {
                qualifier: None,
                name: "split".to_string(),
                args: vec![
                    Expr::StringLit("d,e".to_string()),
                    Expr::StringLit(",".to_string()),
                ],
            },
        ],
    };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
            Value::Str("d".into()),
            Value::Str("e".into()),
        ]))
    );
}

#[test]
fn split_text_to_array() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "split".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("csv".to_string())),
            Expr::StringLit(",".to_string()),
        ],
    };
    let mut fields = EngineHashMap::default();
    fields.insert("csv".into(), Value::Str("a,b,,c".into()));
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str(String::new().into()),
            Value::Str("c".into()),
        ]))
    );
}

#[test]
fn mvdedup_removes_duplicates_keep_order() {
    use crate::match_engine::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "mvdedup".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("vals".to_string()))],
    };
    let mut fields = EngineHashMap::default();
    fields.insert(
        "vals".into(),
        Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("a".into()),
            Value::Str("c".into()),
            Value::Str("b".into()),
        ]),
    );
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]))
    );
}

// ===========================================================================
// external() — evaluated via eval_expr_ext -> eval_func_call
// (the match/close predicate path). Verifies the `external` arm in
// `eval_func_call` dispatches to the global ExternalCallHandler.
// ===========================================================================

#[test]
fn external_func_call_dispatches_to_handler() {
    use std::sync::Arc;

    use crate::external::{ExternalCallHandler, set_external_handler};
    use crate::match_engine::cep::eval_expr;

    struct PwdHandler;
    impl ExternalCallHandler for PwdHandler {
        fn call(&self, service: &str, args: &[Value]) -> Option<Value> {
            if service == "password_check"
                && let Some(Value::Str(s)) = args.first()
            {
                return Some(Value::Bool(matches!(
                    s.as_str(),
                    "welcome" | "apache" | "abcd1234" | "admin" | "123456" | "qweasdzxc"
                )));
            }
            None
        }
    }
    // Best-effort: ignores Err if another test already installed a handler.
    set_external_handler(Arc::new(PwdHandler));

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "external".to_string(),
        args: vec![
            Expr::StringLit("password_check".to_string()),
            Expr::Field(FieldRef::Simple("chars".to_string())),
        ],
    };

    // weak password -> handler returns true
    let hit = event(vec![("chars", Value::Str("welcome".into()))]);
    assert_eq!(eval_expr(&expr, &hit), Some(Value::Bool(true)));

    // non-weak password -> handler returns false
    let miss = event(vec![("chars", Value::Str("not-a-weak-password".into()))]);
    assert_eq!(eval_expr(&expr, &miss), Some(Value::Bool(false)));
}
