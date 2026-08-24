use super::*;
use wf_lang::ast::{BinOp, ObjectItem};

// ===========================================================================
// Not (逻辑否定) evaluation
// ===========================================================================

#[test]
fn not_negates_bool_literal() {
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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

// ===========================================================================
// time_diff
// ===========================================================================

#[test]
fn time_diff_returns_seconds() {
    use crate::match_engine::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_diff".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("t1".to_string())),
            Expr::Field(FieldRef::Simple("t2".to_string())),
        ],
    };
    let mut fields = EngineHashMap::default();
    // 5 seconds apart in epoch milliseconds.
    fields.insert("t1".into(), Value::Number(1_700_000_005_000.0));
    fields.insert("t2".into(), Value::Number(1_700_000_000_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(5.0)));
}

#[test]
fn time_diff_absolute_value() {
    use crate::match_engine::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_diff".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("t1".to_string())),
            Expr::Field(FieldRef::Simple("t2".to_string())),
        ],
    };
    let mut fields = EngineHashMap::default();
    // Reversed order: t1 < t2.
    fields.insert("t1".into(), Value::Number(1_700_000_000_000.0));
    fields.insert("t2".into(), Value::Number(1_700_000_005_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(5.0)));
}

// ===========================================================================
// time_bucket
// ===========================================================================

#[test]
fn time_bucket_floors_to_interval() {
    use crate::match_engine::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_bucket".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::Number(60.0), // 60 second interval
        ],
    };
    let mut fields = EngineHashMap::default();
    // 75 seconds after an epoch millisecond timestamp.
    fields.insert("ts".into(), Value::Number(1_700_000_075_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(1_700_000_040_000.0)));
}

#[test]
fn time_bucket_exact_boundary() {
    use crate::match_engine::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "time_bucket".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::Number(300.0), // 5 minute interval
        ],
    };
    let mut fields = EngineHashMap::default();
    // Exact 5-minute bucket boundary in epoch milliseconds.
    fields.insert("ts".into(), Value::Number(1_700_000_100_000.0));
    let event = Event { fields };
    let result = eval_expr(&expr, &event);
    assert_eq!(result, Some(Value::Number(1_700_000_100_000.0)));
}

// ===========================================================================
// bucket_end（P2 join within 内建；Q8 形态上开桶）
// ===========================================================================

/// `bucket_end(t, 60s)` = 桶末 = `time_bucket(t) + interval`。
#[test]
fn bucket_end_returns_bucket_upper_edge() {
    use crate::match_engine::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "bucket_end".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::Number(60.0),
        ],
    };
    let mut fields = EngineHashMap::default();
    // ts = 75s（epoch ms）→ 60s 桶 [1_700_000_040_000, 1_700_000_100_000)，桶末 = 1_700_000_100_000
    fields.insert("ts".into(), Value::Number(1_700_000_075_000.0));
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Number(1_700_000_100_000.0))
    );
}

/// 恰在桶边界：t = 1_700_000_040_000（60s 桶界）→ 桶末 = 1_700_000_100_000（移入下桶）。
#[test]
fn bucket_end_at_exact_boundary_moves_to_next_bucket() {
    use crate::match_engine::match_engine::{Event, eval_expr};

    let expr = Expr::FuncCall {
        qualifier: None,
        name: "bucket_end".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::Number(60.0),
        ],
    };
    let mut fields = EngineHashMap::default();
    fields.insert("ts".into(), Value::Number(1_700_000_040_000.0));
    let event = Event { fields };
    assert_eq!(
        eval_expr(&expr, &event),
        Some(Value::Number(1_700_000_100_000.0))
    );
}

#[test]
fn time_bucket_rejects_non_positive_or_non_finite_interval() {
    use crate::match_engine::match_engine::{Event, eval_expr};

    let event = Event {
        fields: EngineHashMap::default(),
    };
    for interval in [0.0, -60.0, f64::INFINITY, f64::NAN] {
        let expr = Expr::FuncCall {
            qualifier: None,
            name: "time_bucket".to_string(),
            args: vec![Expr::Number(1_700_000_075_000.0), Expr::Number(interval)],
        };
        assert_eq!(eval_expr(&expr, &event), None);
    }
}

// ===========================================================================
// abs / round / ceil / floor / strftime / strptime
// ===========================================================================

#[test]
fn math_functions_work() {
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    let event = Event { fields };

    let abs_expr = Expr::FuncCall {
        qualifier: None,
        name: "abs".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("n".to_string()))],
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
    let round_expr = Expr::FuncCall {
        qualifier: None,
        name: "round".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("n".to_string())),
            Expr::Number(2.0),
        ],
    };
    let fmt_expr = Expr::FuncCall {
        qualifier: None,
        name: "strftime".to_string(),
        args: vec![
            Expr::Field(FieldRef::Simple("ts".to_string())),
            Expr::StringLit("%Y-%m-%d".to_string()),
        ],
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

    assert_eq!(eval_expr(&abs_expr, &event), Some(Value::Number(12.345)));
    assert_eq!(eval_expr(&ceil_expr, &event), Some(Value::Number(-12.0)));
    assert_eq!(eval_expr(&floor_expr, &event), Some(Value::Number(-13.0)));
    assert_eq!(eval_expr(&round_expr, &event), Some(Value::Number(-12.35)));
    assert_eq!(
        eval_expr(&fmt_expr, &event),
        Some(Value::Str("1970-01-01".into()))
    );
    assert_eq!(eval_expr(&sqrt_expr, &event), Some(Value::Number(4.0)));
    assert_eq!(eval_expr(&pow_expr, &event), Some(Value::Number(256.0)));
    assert_eq!(eval_expr(&log_expr, &event), Some(Value::Number(2.0)));
    assert_eq!(
        eval_expr(&exp_expr, &event),
        Some(Value::Number(std::f64::consts::E))
    );
    assert_eq!(eval_expr(&clamp_expr, &event), Some(Value::Number(100.0)));
    assert_eq!(eval_expr(&sign_expr, &event), Some(Value::Number(-1.0)));
    assert_eq!(eval_expr(&trunc_expr, &event), Some(Value::Number(-12.0)));
    assert_eq!(eval_expr(&finite_expr, &event), Some(Value::Bool(true)));
    assert_eq!(
        eval_expr(&ltrim_expr, &event),
        Some(Value::Str("failed_login_root  ".into()))
    );
    assert_eq!(
        eval_expr(&rtrim_expr, &event),
        Some(Value::Str("  failed_login_root".into()))
    );
    assert_eq!(
        eval_expr(&concat_expr, &event),
        Some(Value::Str("ip=1.1.1.1".into()))
    );
    assert_eq!(eval_expr(&index_expr, &event), Some(Value::Number(9.0)));
    assert_eq!(
        eval_expr(&replace_plain_expr, &event),
        Some(Value::Str("  failed-login-root  ".into()))
    );
    assert_eq!(eval_expr(&sw_any_expr, &event), Some(Value::Bool(true)));
    assert_eq!(eval_expr(&ew_any_expr, &event), Some(Value::Bool(true)));
    assert_eq!(
        eval_expr(&coalesce_expr, &event),
        Some(Value::Str("fallback".into()))
    );
    assert_eq!(eval_expr(&isnull_expr, &event), Some(Value::Bool(true)));
    assert_eq!(eval_expr(&isnotnull_expr, &event), Some(Value::Bool(true)));
    assert_eq!(
        eval_expr(&mvsort_expr, &event),
        Some(Value::Array(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]))
    );
    assert_eq!(
        eval_expr(&mvreverse_expr, &event),
        Some(Value::Array(vec![
            Value::Str("c".into()),
            Value::Str("a".into()),
            Value::Str("b".into()),
        ]))
    );
}

#[test]
fn now_functions_work() {
    use crate::match_engine::match_engine::{Event, eval_expr};

    let event = Event {
        fields: EngineHashMap::default(),
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
    let fmt_expr = Expr::FuncCall {
        qualifier: None,
        name: "strftime".to_string(),
        args: vec![now_expr.clone(), Expr::StringLit("%Y".to_string())],
    };
    let invalid_expr = Expr::FuncCall {
        qualifier: None,
        name: "now".to_string(),
        args: vec![Expr::Number(1.0)],
    };

    let Some(Value::Number(now_millis)) = eval_expr(&now_expr, &event) else {
        panic!("now() should return a numeric timestamp");
    };
    let Some(Value::Number(now_s)) = eval_expr(&now_s_expr, &event) else {
        panic!("now_s() should return a numeric timestamp");
    };
    let Some(Value::Number(now_ms)) = eval_expr(&now_ms_expr, &event) else {
        panic!("now_ms() should return a numeric timestamp");
    };
    let Some(Value::Number(now_us)) = eval_expr(&now_us_expr, &event) else {
        panic!("now_us() should return a numeric timestamp");
    };
    let Some(Value::Number(now_ns)) = eval_expr(&now_ns_expr, &event) else {
        panic!("now_ns() should return a numeric timestamp");
    };
    let Some(Value::Str(year)) = eval_expr(&fmt_expr, &event) else {
        panic!("strftime(now(), ...) should format the current time");
    };

    assert!(now_millis > 1_000_000_000_000.0);
    assert!(now_ns > 1_000_000_000_000_000_000.0);
    assert!(now_us > 1_000_000_000_000_000.0);
    assert!(now_ms > 1_000_000_000_000.0);
    assert!(now_s > 1_000_000_000.0);
    assert!(year.len() == 4 && year.chars().all(|c| c.is_ascii_digit()));
    assert_eq!(eval_expr(&invalid_expr, &event), None);
}

#[test]
fn now_functions_share_timestamp_within_expression() {
    use crate::match_engine::match_engine::{Event, eval_expr};

    let event = Event {
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

    assert_eq!(eval_expr(&expr, &event), Some(Value::Number(0.0)));
}

#[test]
fn blank_functions_work() {
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::{Event, eval_expr};

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
    use crate::match_engine::match_engine::eval_expr;

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
