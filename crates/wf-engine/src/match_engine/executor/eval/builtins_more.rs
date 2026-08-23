//! Round-2 coverage-fill tests for `eval/builtins.rs` — the branches the
//! existing `tests.rs` battery and `coverage_extra.rs` do not reach:
//!
//! - `materialize_system_vars` structural rewrites: `Field`, `Object`,
//!   `Array`, `InList`, `IfThenElse`.
//! - `eval_l3_func` over **step field values** (`_step_{i}_field_{name}` via a
//!   qualified arg) and `eval_aggregate_func` over bind field series +
//!   close-stage preference (`prefer_close_steps`).
//! - `eval_join_arg_with_l3` `None` for a non-field arg (join/join_by), and
//!   `scalar_value_to_string` rejection of object values in `join_by`.
//! - `bucket_end` `checked_add` overflow → `None`.

use super::builtins::{
    contains_system_var, eval_aggregate_func, eval_l3_func, materialize_system_vars,
};
use super::{Event, Value, YieldMeta, eval_expr_with_l3};
use crate::match_engine::EngineHashMap;
use wf_lang::ast::{Expr, FieldRef, ObjectItem};

fn lit(s: &str) -> Expr {
    Expr::StringLit(s.to_string())
}

fn num_expr(n: f64) -> Expr {
    Expr::Number(n)
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

fn str_val(s: &str) -> Value {
    Value::Str(s.to_string().into())
}

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn l3_ctx(expr: &Expr, ctx: &Event) -> Option<Value> {
    eval_expr_with_l3(expr, ctx, YieldMeta::default())
}

#[test]
fn materialize_system_vars_structural_rewrites() {
    let score = YieldMeta {
        score: Some(70.0),
        wfx_id: Some("wx"),
        ..YieldMeta::default()
    };

    // Field passes through untouched.
    assert_eq!(
        materialize_system_vars(&field("x"), score),
        Some(field("x"))
    );
    // Field with a nested system var in an Object.
    let obj = Expr::Object(vec![
        ObjectItem {
            targets: vec!["a".to_string()],
            type_hint: None,
            value: Expr::SystemVar(wf_lang::ast::SystemVar::Score),
        },
        ObjectItem {
            targets: vec!["b".to_string()],
            type_hint: None,
            value: field("keep"),
        },
    ]);
    match materialize_system_vars(&obj, score) {
        Some(Expr::Object(items)) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0].value, Expr::Number(70.0));
            assert_eq!(items[1].value, field("keep"));
            assert_eq!(items[1].targets, vec!["b".to_string()]);
        }
        other => panic!("expected rewritten object, got {other:?}"),
    }
    // Array rewrite.
    let arr = Expr::Array(vec![
        Expr::SystemVar(wf_lang::ast::SystemVar::Score),
        num_expr(1.0),
    ]);
    assert_eq!(
        materialize_system_vars(&arr, score),
        Some(Expr::Array(vec![Expr::Number(70.0), num_expr(1.0)]))
    );
    // InList rewrite (negated preserved, list materialized).
    let in_list = Expr::InList {
        expr: Box::new(Expr::SystemVar(wf_lang::ast::SystemVar::Score)),
        list: vec![num_expr(1.0)],
        negated: true,
    };
    assert_eq!(
        materialize_system_vars(&in_list, score),
        Some(Expr::InList {
            expr: Box::new(Expr::Number(70.0)),
            list: vec![num_expr(1.0)],
            negated: true,
        })
    );
    // IfThenElse rewrite.
    let ite = Expr::IfThenElse {
        cond: Box::new(Expr::Bool(true)),
        then_expr: Box::new(Expr::SystemVar(wf_lang::ast::SystemVar::Score)),
        else_expr: Box::new(num_expr(0.0)),
    };
    match materialize_system_vars(&ite, score) {
        Some(Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        }) => {
            assert_eq!(*cond, Expr::Bool(true));
            assert_eq!(*then_expr, Expr::Number(70.0));
            assert_eq!(*else_expr, num_expr(0.0));
        }
        other => panic!("expected rewritten if-then-else, got {other:?}"),
    }
    // A nested unresolvable system var inside an Object → whole tree None.
    assert_eq!(
        materialize_system_vars(
            &Expr::Object(vec![ObjectItem {
                targets: vec!["a".to_string()],
                type_hint: None,
                value: Expr::SystemVar(wf_lang::ast::SystemVar::Score),
            }]),
            YieldMeta::default(),
        ),
        None
    );
}

#[test]
fn contains_system_var_negative_and_wrapper_shapes() {
    // `_ => false` leaves for plain literal shapes.
    assert!(!contains_system_var(&Expr::Field(FieldRef::Simple(
        "x".into()
    ))));
    assert!(!contains_system_var(&Expr::PresetParam("p".to_string())));
    // InList with the system var in the *list* side.
    assert!(contains_system_var(&Expr::InList {
        expr: Box::new(num_expr(1.0)),
        list: vec![Expr::SystemVar(wf_lang::ast::SystemVar::Score)],
        negated: false,
    }));
    // IfThenElse with the system var in the condition.
    assert!(contains_system_var(&Expr::IfThenElse {
        cond: Box::new(Expr::SystemVar(wf_lang::ast::SystemVar::Score)),
        then_expr: Box::new(num_expr(1.0)),
        else_expr: Box::new(num_expr(0.0)),
    }));
}

#[test]
fn l3_funcs_read_step_field_values_and_bind_series() {
    // Qualified step arg: `collect_list(e.x)` reads `_step_0_field_x`.
    // (The engine always materializes `_step_{i}_values` alongside the
    // per-field history — `resolve_step_indices` keys off it.)
    let step_fields = ctx_with(vec![
        (
            "_step_0_field_x",
            Value::Array(vec![num(1.0), num(2.0), num(2.0)]),
        ),
        ("_step_0_values", Value::Array(vec![num(1.0)])),
        ("_step_0_source", str_val("e")),
        ("_step_0_label", str_val("fail")),
    ]);
    let qualified = Expr::Field(FieldRef::Qualified("e".into(), "x".into()));
    assert_eq!(
        eval_l3_func(
            "collect_list",
            std::slice::from_ref(&qualified),
            &step_fields,
            YieldMeta::default()
        ),
        Some(Value::Array(vec![num(1.0), num(2.0), num(2.0)]))
    );
    // collect_set dedups across the step field series.
    assert_eq!(
        eval_l3_func(
            "collect_set",
            std::slice::from_ref(&qualified),
            &step_fields,
            YieldMeta::default()
        ),
        Some(Value::Array(vec![num(1.0), num(2.0)]))
    );
    assert_eq!(
        eval_l3_func(
            "first",
            std::slice::from_ref(&qualified),
            &step_fields,
            YieldMeta::default()
        ),
        Some(num(1.0))
    );
    assert_eq!(
        eval_l3_func(
            "last",
            std::slice::from_ref(&qualified),
            &step_fields,
            YieldMeta::default()
        ),
        Some(num(2.0))
    );
    // percentile over the step field series: p=100 → max.
    assert_eq!(
        eval_l3_func(
            "percentile",
            &[qualified.clone(), num_expr(100.0)],
            &step_fields,
            YieldMeta::default()
        ),
        Some(num(2.0))
    );

    // Bind series for l3: `collect_set(b.x)` over `_bind_b_field_x`.
    let bind = ctx_with(vec![
        (
            "_bind_b_field_x",
            Value::Array(vec![num(1.0), num(2.0), num(1.0)]),
        ),
        ("_bind_b_count", num(3.0)),
    ]);
    let bind_qualified = Expr::Field(FieldRef::Qualified("b".into(), "x".into()));
    assert_eq!(
        eval_l3_func(
            "collect_set",
            std::slice::from_ref(&bind_qualified),
            &bind,
            YieldMeta::default()
        ),
        Some(Value::Array(vec![num(1.0), num(2.0)]))
    );
    // When the bind series is present, step values are NOT consulted (missing
    // `_step_*` fields → empty fallback).
    assert_eq!(
        eval_l3_func("first", &[bind_qualified], &bind, YieldMeta::default()),
        Some(num(1.0))
    );
}

#[test]
fn aggregate_func_bind_series_and_close_stage_preference() {
    // Aggregate over a bind field series when no step series exists:
    // `sum(b.x)` → `_bind_b_field_x` = [1, 2, 3] → 6.
    let bind = ctx_with(vec![
        (
            "_bind_b_field_x",
            Value::Array(vec![num(1.0), num(2.0), num(3.0)]),
        ),
        ("_bind_b_count", num(3.0)),
    ]);
    let bind_qualified = Expr::Field(FieldRef::Qualified("b".into(), "x".into()));
    assert_eq!(
        eval_aggregate_func("sum", std::slice::from_ref(&bind_qualified), &bind),
        Some(num(6.0))
    );
    assert_eq!(
        eval_aggregate_func("count", std::slice::from_ref(&bind_qualified), &bind),
        Some(num(3.0))
    );
    assert_eq!(
        eval_aggregate_func("avg", &[bind_qualified], &bind),
        Some(num(2.0))
    );

    // Close-stage preference: two steps with the same source alias, one event
    // stage and one close stage. `sum(x)` must aggregate only the close-stage
    // step's measures (prefer_close_steps).
    let mixed = ctx_with(vec![
        ("_step_0_measure", num(10.0)),
        ("_step_0_values", Value::Array(vec![num(10.0)])),
        ("_step_0_source", str_val("e")),
        ("_step_0_stage", str_val("event")),
        ("_step_1_measure", num(30.0)),
        ("_step_1_values", Value::Array(vec![num(30.0)])),
        ("_step_1_source", str_val("e")),
        ("_step_1_stage", str_val("close")),
    ]);
    // Simple field arg: step_ref = "e" → by_source both steps → close only.
    assert_eq!(
        eval_aggregate_func("sum", &[field("e")], &mixed),
        Some(num(30.0))
    );
    assert_eq!(
        eval_aggregate_func("count", &[field("e")], &mixed),
        Some(num(30.0))
    );
    // Without any close-stage step, all matching steps contribute.
    let only_event = ctx_with(vec![
        ("_step_0_measure", num(10.0)),
        ("_step_0_values", Value::Array(vec![num(10.0)])),
        ("_step_0_source", str_val("e")),
        ("_step_0_stage", str_val("event")),
        ("_step_1_measure", num(30.0)),
        ("_step_1_values", Value::Array(vec![num(30.0)])),
        ("_step_1_source", str_val("e")),
        ("_step_1_stage", str_val("event")),
    ]);
    assert_eq!(
        eval_aggregate_func("sum", &[field("e")], &only_event),
        Some(num(40.0))
    );
}

#[test]
fn join_and_join_by_reject_non_scalar_or_failing_args() {
    let ctx = ctx_with(vec![("o", Value::Object(EngineHashMap::default()))]);
    // join with an object-valued arg → scalar_value_to_string rejects.
    assert_eq!(l3_ctx(&call("join", vec![field("o")]), &ctx), None);
    // join_by with an object-valued arg → None.
    assert_eq!(
        l3_ctx(&call("join_by", vec![lit("|"), field("o")]), &ctx),
        None
    );
    // join with a failing non-field arg (unknown function) → None (not the
    // missing-field empty-string fallback, which only applies to Field args).
    assert_eq!(
        l3_ctx(&call("join", vec![call("bogus_func", vec![])]), &ctx),
        None
    );
    // join_by likewise.
    assert_eq!(
        l3_ctx(
            &call("join_by", vec![lit("|"), call("bogus_func", vec![])]),
            &ctx
        ),
        None
    );
}

#[test]
fn bucket_end_checked_add_overflow_returns_none() {
    // t ≈ 9.2e18 ns (near i64::MAX) with a 9e9 s interval (9e18 ns): the
    // bucketed value plus the interval overflows i64 → None.
    let ctx = ctx_with(vec![]);
    assert_eq!(
        l3_ctx(
            &call(
                "bucket_end",
                vec![
                    num_expr(9_200_000_000_000_000_000.0),
                    num_expr(9_000_000_000.0)
                ]
            ),
            &ctx
        ),
        None
    );
    // time_bucket on the same input still works (no addition).
    assert!(
        l3_ctx(
            &call(
                "time_bucket",
                vec![
                    num_expr(9_200_000_000_000_000_000.0),
                    num_expr(9_000_000_000.0)
                ]
            ),
            &ctx
        )
        .is_some()
    );
}

#[test]
fn merge_accepts_two_object_literals() {
    // merge of two object literals with no fields involved.
    let obj = |k: &str, v: f64| {
        Expr::Object(vec![ObjectItem {
            targets: vec![k.to_string()],
            type_hint: None,
            value: num_expr(v),
        }])
    };
    assert_eq!(
        l3_ctx(
            &call("merge", vec![obj("a", 1.0), obj("b", 2.0)]),
            &ctx_with(vec![])
        ),
        Some(Value::Object(EngineHashMap::from_iter([
            ("a".into(), num(1.0)),
            ("b".into(), num(2.0)),
        ])))
    );
}
