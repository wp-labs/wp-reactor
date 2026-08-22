//! Round-2 coverage-fill tests for `executor/eval/mod.rs` — the interpreter
//! branches the happy-path battery in `tests.rs` and the builtins-focused
//! `coverage_extra.rs` leave cold: string/bool ordering in `compare_values`,
//! cross-type comparison rejection, the partial-None logic combinator rows,
//! arithmetic on non-numeric operands, division/modulo by zero, the
//! `contains_*` expression-shape walkers, and the `eval_bool_expr` /
//! `eval_yield_expr` fallback lanes.

use super::{Event, Value, YieldMeta, eval_bool_expr, eval_expr_with_l3, eval_yield_expr};
use crate::match_engine::EngineHashMap;
use wf_lang::ast::{BinOp, Expr, FieldRef, ObjectItem, SystemVar};

fn field(name: &str) -> Expr {
    Expr::Field(FieldRef::Simple(name.to_string()))
}

fn lit(s: &str) -> Expr {
    Expr::StringLit(s.to_string())
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

fn l3(expr: &Expr, ctx: &Event) -> Option<Value> {
    eval_expr_with_l3(expr, ctx, YieldMeta::default())
}

// ---------------------------------------------------------------------------
// compare_values — string/bool ordering and cross-type rejection
// ---------------------------------------------------------------------------

#[test]
fn compare_values_string_and_bool_ordering_ops() {
    let cmp = |op: BinOp, l: &str, r: &str| {
        l3(
            &Expr::BinOp {
                op,
                left: Box::new(lit(l)),
                right: Box::new(lit(r)),
            },
            &ctx_with(vec![]),
        )
    };
    assert_eq!(cmp(BinOp::Lt, "a", "b"), Some(Value::Bool(true)));
    assert_eq!(cmp(BinOp::Gt, "b", "a"), Some(Value::Bool(true)));
    assert_eq!(cmp(BinOp::Le, "a", "a"), Some(Value::Bool(true)));
    assert_eq!(cmp(BinOp::Ge, "a", "a"), Some(Value::Bool(true)));
    assert_eq!(cmp(BinOp::Lt, "b", "a"), Some(Value::Bool(false)));
    assert_eq!(cmp(BinOp::Gt, "a", "b"), Some(Value::Bool(false)));

    // Bool ordering: false < true.
    let bcmp = |op: BinOp, l: bool, r: bool| {
        l3(
            &Expr::BinOp {
                op,
                left: Box::new(Expr::Bool(l)),
                right: Box::new(Expr::Bool(r)),
            },
            &ctx_with(vec![]),
        )
    };
    assert_eq!(bcmp(BinOp::Lt, false, true), Some(Value::Bool(true)));
    assert_eq!(bcmp(BinOp::Gt, true, false), Some(Value::Bool(true)));
    assert_eq!(bcmp(BinOp::Le, true, true), Some(Value::Bool(true)));

    // Cross-type ordering (Str vs Number, Bool vs Str) → false.
    let cross = |op: BinOp| {
        l3(
            &Expr::BinOp {
                op,
                left: Box::new(lit("a")),
                right: Box::new(Expr::Number(1.0)),
            },
            &ctx_with(vec![]),
        )
    };
    assert_eq!(cross(BinOp::Lt), Some(Value::Bool(false)));
    assert_eq!(cross(BinOp::Gt), Some(Value::Bool(false)));
    assert_eq!(
        l3(
            &Expr::BinOp {
                op: BinOp::Lt,
                left: Box::new(Expr::Bool(true)),
                right: Box::new(lit("x")),
            },
            &ctx_with(vec![]),
        ),
        Some(Value::Bool(false))
    );
}

// ---------------------------------------------------------------------------
// eval_logic_and_with_l3 / eval_logic_or_with_l3 — partial-None rows
// ---------------------------------------------------------------------------

#[test]
fn logic_combinators_with_partial_none_operands() {
    let ctx = ctx_with(vec![("flag", Value::Bool(true))]);
    let and = |l: Expr, r: Expr| {
        l3(
            &Expr::BinOp {
                op: BinOp::And,
                left: Box::new(l),
                right: Box::new(r),
            },
            &ctx,
        )
    };
    let or = |l: Expr, r: Expr| {
        l3(
            &Expr::BinOp {
                op: BinOp::Or,
                left: Box::new(l),
                right: Box::new(r),
            },
            &ctx,
        )
    };

    let t = Expr::Bool(true);
    let f = Expr::Bool(false);
    let missing = field("ghost"); // evaluates to None

    // And: any explicit false wins; otherwise both must be true.
    assert_eq!(and(f.clone(), missing.clone()), Some(Value::Bool(false)));
    assert_eq!(and(missing.clone(), f.clone()), Some(Value::Bool(false)));
    assert_eq!(and(t.clone(), t.clone()), Some(Value::Bool(true)));
    assert_eq!(and(t.clone(), missing.clone()), None);
    assert_eq!(and(missing.clone(), t.clone()), None);
    assert_eq!(and(missing.clone(), missing.clone()), None);

    // Or: any explicit true wins; otherwise both must be false.
    assert_eq!(or(t.clone(), missing.clone()), Some(Value::Bool(true)));
    assert_eq!(or(missing.clone(), t.clone()), Some(Value::Bool(true)));
    assert_eq!(or(f.clone(), f.clone()), Some(Value::Bool(false)));
    assert_eq!(or(f.clone(), missing.clone()), None);
    assert_eq!(or(missing.clone(), f.clone()), None);
    assert_eq!(or(missing.clone(), missing.clone()), None);
}

// ---------------------------------------------------------------------------
// Arithmetic — non-numeric operands and division/modulo by zero
// ---------------------------------------------------------------------------

#[test]
fn arithmetic_rejects_non_numeric_operands_and_zero_divisor() {
    let ctx = ctx_with(vec![("s", Value::Str("x".into()))]);

    // coerce_to_f64 fails on a Str operand → the whole expression is None.
    let add_str = Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(field("s")),
        right: Box::new(Expr::Number(1.0)),
    };
    assert_eq!(l3(&add_str, &ctx), None);

    // Division / modulo by zero → None (not inf/nan).
    let div_zero = Expr::BinOp {
        op: BinOp::Div,
        left: Box::new(Expr::Number(10.0)),
        right: Box::new(Expr::Number(0.0)),
    };
    assert_eq!(l3(&div_zero, &ctx), None);
    let mod_zero = Expr::BinOp {
        op: BinOp::Mod,
        left: Box::new(Expr::Number(10.0)),
        right: Box::new(Expr::Number(0.0)),
    };
    assert_eq!(l3(&mod_zero, &ctx), None);

    // Non-zero divisors still compute.
    let div_ok = Expr::BinOp {
        op: BinOp::Div,
        left: Box::new(Expr::Number(10.0)),
        right: Box::new(Expr::Number(4.0)),
    };
    assert_eq!(l3(&div_ok, &ctx), Some(Value::Number(2.5)));
    let mod_ok = Expr::BinOp {
        op: BinOp::Mod,
        left: Box::new(Expr::Number(10.0)),
        right: Box::new(Expr::Number(3.0)),
    };
    assert_eq!(l3(&mod_ok, &ctx), Some(Value::Number(1.0)));
}

// ---------------------------------------------------------------------------
// contains_* walkers — every expression shape reaches the recursion arms
// ---------------------------------------------------------------------------

#[test]
fn contains_walkers_cover_binop_neg_object_array_inlist_and_ite() {
    let nested_l3 = Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(Expr::Neg(Box::new(call("first", vec![field("x")])))),
        right: Box::new(Expr::Object(vec![ObjectItem {
            targets: vec!["k".into()],
            type_hint: None,
            value: call("last", vec![field("y")]),
        }])),
    };
    // contains_l3_func: BinOp → Neg → FuncCall(first) and Object → FuncCall(last).
    assert!(super::contains_l3_func(&nested_l3));

    let ite_l3 = Expr::IfThenElse {
        cond: Box::new(call("collect_set", vec![field("a")])),
        then_expr: Box::new(Expr::Array(vec![call("percentile", vec![field("b")])])),
        else_expr: Box::new(Expr::InList {
            expr: Box::new(field("c")),
            list: vec![call("stddev", vec![field("d")])],
            negated: false,
        }),
    };
    assert!(super::contains_l3_func(&ite_l3));

    // contains_eval_time_func: FuncCall(now_ns) nested inside an object value.
    let obj_time = Expr::Object(vec![ObjectItem {
        targets: vec!["t".into()],
        type_hint: None,
        value: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(call("now_ns", vec![])),
            right: Box::new(Expr::Number(1.0)),
        },
    }]);
    assert!(super::contains_eval_time_func(&obj_time));
    assert!(super::contains_eval_time_func(&Expr::Neg(Box::new(call(
        "now_s",
        vec![]
    )))));

    // contains_aggregate_func: nested in InList / IfThenElse arms.
    let agg_ite = Expr::IfThenElse {
        cond: Box::new(Expr::Bool(true)),
        then_expr: Box::new(Expr::InList {
            expr: Box::new(field("a")),
            list: vec![call("sum", vec![field("b")])],
            negated: false,
        }),
        else_expr: Box::new(Expr::Array(vec![call("avg", vec![field("c")])])),
    };
    assert!(super::contains_aggregate_func(&agg_ite));

    // contains_stat_selector: a wrapper function around stat.count.
    let stat_wrap = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            lit("{}"),
            Expr::FuncCall {
                qualifier: Some("stat".into()),
                name: "count".into(),
                args: vec![field("x")],
            },
        ],
    };
    assert!(super::contains_stat_selector(&stat_wrap));
}

// ---------------------------------------------------------------------------
// eval_bool_expr / eval_yield_expr fallback lanes
// ---------------------------------------------------------------------------

#[test]
fn eval_bool_expr_rejects_non_bool_results() {
    let ctx = ctx_with(vec![("n", Value::Number(5.0))]);
    // Numeric expression → not a Bool → None.
    assert_eq!(eval_bool_expr(&Expr::Number(5.0), &ctx), None);
    // Literal bool works.
    assert_eq!(eval_bool_expr(&Expr::Bool(true), &ctx), Some(true));
    // Missing field → None.
    assert_eq!(eval_bool_expr(&field("ghost"), &ctx), None);
}

#[test]
fn eval_yield_expr_falls_back_to_empty_string() {
    let ctx = ctx_with(vec![]);
    // A missing field yields the empty-string fallback (not None).
    assert_eq!(
        eval_yield_expr(&field("ghost"), &ctx),
        Some(Value::Str("".into()))
    );
    // Present fields resolve normally.
    let ctx = ctx_with(vec![("s", Value::Str("x".into()))]);
    assert_eq!(
        eval_yield_expr(&field("s"), &ctx),
        Some(Value::Str("x".into()))
    );
    // A number literal stays a number (no string coercion on the eval path).
    assert_eq!(
        eval_yield_expr(&Expr::Number(3.0), &ctx),
        Some(Value::Number(3.0))
    );
}

// ---------------------------------------------------------------------------
// SystemVar lanes through the L3 interpreter
// ---------------------------------------------------------------------------

#[test]
fn system_vars_resolve_through_l3_interpreter() {
    let ctx = ctx_with(vec![]);
    let meta = YieldMeta {
        score: Some(90.0),
        event_first_time_nanos: Some(1_700_000_000_000_000_000),
        event_last_time_nanos: Some(1_700_000_000_001_000_000),
        window_start_time_nanos: Some(1_700_000_000_000_000_000),
        window_end_time_nanos: Some(1_700_000_000_060_000_000),
        emit_time_nanos: Some(1_700_000_000_002_000_000),
        ..YieldMeta::default()
    };
    assert_eq!(
        eval_expr_with_l3(&Expr::SystemVar(SystemVar::Score), &ctx, meta),
        Some(Value::Number(90.0))
    );
    // Time system vars convert to millis.
    let t = |v: Option<Value>| v;
    assert!(
        t(eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::EventFirstTime),
            &ctx,
            meta
        ))
        .is_some()
    );
    assert!(
        t(eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::EventLastTime),
            &ctx,
            meta
        ))
        .is_some()
    );
    assert!(
        t(eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::WindowStartTime),
            &ctx,
            meta
        ))
        .is_some()
    );
    assert!(
        t(eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::WindowEndTime),
            &ctx,
            meta
        ))
        .is_some()
    );
    assert!(
        t(eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::EmitTime),
            &ctx,
            meta
        ))
        .is_some()
    );
    // Absent meta → None.
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::Score),
            &ctx,
            YieldMeta::default()
        ),
        None
    );
}
