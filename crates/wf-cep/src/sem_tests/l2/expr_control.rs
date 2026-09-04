//! L2 表达式求值——逻辑控制流与谓词守卫（2026-09-04 自 expr.rs 拆出；`#[path]` 兄弟
//! 子模块，共享 import 在父文件 expr.rs，此处经 `use super::*` 复用）。主题：Not 逻辑
//! 否定（含德摩根等价、非布尔 → None）、IfThenElse 控制流（真/假分支、嵌套、字段条件）、
//! regex_match 守卫、cidr_match 守卫（命中 / 不命中 / 非法子网）。

use super::*;

// ===========================================================================
// Not (逻辑否定) evaluation
// ===========================================================================

#[test]
fn not_negates_bool_literal() {
    use crate::cep::{Event, eval_expr};

    let event = Event {
        fields: EngineHashMap::default(),
    };
    assert_eq!(
        eval_expr(&Expr::Not(Box::new(Expr::Bool(true))), &event),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&Expr::Not(Box::new(Expr::Bool(false))), &event),
        Some(Value::Bool(true))
    );
}

#[test]
fn not_negates_comparison() {
    use crate::cep::{Event, eval_expr};

    let expr = Expr::Not(Box::new(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
        right: Box::new(Expr::StringLit("failed".to_string())),
    }));
    let mut fields = EngineHashMap::default();
    fields.insert("action".into(), Value::Str("failed".into()));
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Bool(false)));

    let mut fields = EngineHashMap::default();
    fields.insert("action".into(), Value::Str("ok".into()));
    assert_eq!(eval_expr(&expr, &Event { fields }), Some(Value::Bool(true)));
}

#[test]
fn not_de_morgan_equivalence() {
    use crate::cep::{Event, eval_expr};

    // `not (a || b)` ≡ `(not a) && (not b)`。
    let a = Expr::Field(FieldRef::Simple("a".into()));
    let b = Expr::Field(FieldRef::Simple("b".into()));
    let not_or = Expr::Not(Box::new(Expr::BinOp {
        op: BinOp::Or,
        left: Box::new(a.clone()),
        right: Box::new(b.clone()),
    }));
    let demorgan = Expr::BinOp {
        op: BinOp::And,
        left: Box::new(Expr::Not(Box::new(a.clone()))),
        right: Box::new(Expr::Not(Box::new(b.clone()))),
    };
    for (av, bv) in [(true, true), (true, false), (false, true), (false, false)] {
        let mut fields = EngineHashMap::default();
        fields.insert("a".into(), Value::Bool(av));
        fields.insert("b".into(), Value::Bool(bv));
        let event = Event { fields };
        assert_eq!(
            eval_expr(&not_or, &event),
            eval_expr(&demorgan, &event),
            "not (a || b) 应等于 (not a) && (not b) @ a={av} b={bv}"
        );
    }
}

#[test]
fn not_non_bool_is_none() {
    use crate::cep::{Event, eval_expr};

    // `not 5`（数值）→ None：与 Neg 非数值 → None 一致，不做隐式非零判真。
    let event = Event {
        fields: EngineHashMap::default(),
    };
    assert_eq!(
        eval_expr(&Expr::Not(Box::new(Expr::Number(5.0))), &event),
        None
    );
}

// ===========================================================================
// IfThenElse expression evaluation
// ===========================================================================

#[test]
fn if_then_else_true_branch() {
    use crate::cep::{Event, eval_expr};

    let expr = Expr::IfThenElse {
        cond: Box::new(Expr::Bool(true)),
        then_expr: Box::new(Expr::Number(80.0)),
        else_expr: Box::new(Expr::Number(40.0)),
    };
    let event = Event {
        fields: EngineHashMap::default(),
    };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(80.0)));
}

#[test]
fn if_then_else_false_branch() {
    use crate::cep::{Event, eval_expr};

    let expr = Expr::IfThenElse {
        cond: Box::new(Expr::Bool(false)),
        then_expr: Box::new(Expr::Number(80.0)),
        else_expr: Box::new(Expr::Number(40.0)),
    };
    let event = Event {
        fields: EngineHashMap::default(),
    };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(40.0)));
}

#[test]
fn if_then_else_nested() {
    use crate::cep::{Event, eval_expr};

    // if true then (if false then 1 else 2) else 3
    let expr = Expr::IfThenElse {
        cond: Box::new(Expr::Bool(true)),
        then_expr: Box::new(Expr::IfThenElse {
            cond: Box::new(Expr::Bool(false)),
            then_expr: Box::new(Expr::Number(1.0)),
            else_expr: Box::new(Expr::Number(2.0)),
        }),
        else_expr: Box::new(Expr::Number(3.0)),
    };
    let event = Event {
        fields: EngineHashMap::default(),
    };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(2.0)));
}

#[test]
fn if_then_else_with_field_condition() {
    use crate::cep::{Event, eval_expr};

    // if action == "failed" then 80 else 40
    let expr = Expr::IfThenElse {
        cond: Box::new(Expr::BinOp {
            op: wf_lang::ast::BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
            right: Box::new(Expr::StringLit("failed".to_string())),
        }),
        then_expr: Box::new(Expr::Number(80.0)),
        else_expr: Box::new(Expr::Number(40.0)),
    };

    let mut fields = EngineHashMap::default();
    fields.insert("action".into(), Value::Str("failed".into()));
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Number(80.0)));

    let mut fields2 = EngineHashMap::default();
    fields2.insert("action".into(), Value::Str("success".into()));
    let event2 = Event { fields: fields2 };
    assert_eq!(eval_expr(&expr, &event2), Some(Value::Number(40.0)));
}

// ===========================================================================
// regex_match
// ===========================================================================

#[test]
fn regex_match_matches() {
    use crate::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "regex_match".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("action".to_string())),
            Expr::StringLit("fail.*".to_string()),
        ],
    };
    let mut fields = EngineHashMap::default();
    fields.insert("action".into(), Value::Str("failed_login".into()));
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Bool(true)));
}

#[test]
fn regex_match_no_match() {
    use crate::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "regex_match".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("action".to_string())),
            Expr::StringLit("^success$".to_string()),
        ],
    };
    let mut fields = EngineHashMap::default();
    fields.insert("action".into(), Value::Str("failed".into()));
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Bool(false)));
}

// ===========================================================================
// cidr_match（Sigma |cidr 等效）
// ===========================================================================

#[test]
fn cidr_match_guard_hit() {
    use crate::cep::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "cidr_match".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".to_string())),
            Expr::StringLit("10.0.0.0/8".to_string()),
        ],
    };
    let mut fields = EngineHashMap::default();
    fields.insert("sip".into(), Value::Str("10.23.45.67".into()));
    let event = Event { fields };
    assert_eq!(eval_expr(&expr, &event), Some(Value::Bool(true)));
}

#[test]
fn cidr_match_guard_miss_and_error() {
    use crate::cep::{Event, eval_expr};

    let base = |sip: &str| {
        let mut fields = EngineHashMap::default();
        fields.insert("sip".into(), Value::Str(sip.into()));
        Event { fields }
    };
    let expr = |ip: &str| Expr::FuncCall {
        qualifier: None,
        name: "cidr_match".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple(ip.to_string())),
            Expr::StringLit("172.16.0.0/12".to_string()),
        ],
    };
    // 命中。
    assert_eq!(
        eval_expr(&expr("sip"), &base("172.31.0.1")),
        Some(Value::Bool(true))
    );
    // 不命中。
    assert_eq!(
        eval_expr(&expr("sip"), &base("173.0.0.1")),
        Some(Value::Bool(false))
    );
    // 非法子网 → None。
    let bad = Expr::FuncCall {
        qualifier: None,
        name: "cidr_match".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".to_string())),
            Expr::StringLit("172.16.0.0/40".to_string()),
        ],
    };
    assert_eq!(eval_expr(&bad, &base("172.31.0.1")), None);
}
