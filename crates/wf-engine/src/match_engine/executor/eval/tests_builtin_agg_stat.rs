//! builtins.rs 直接求值（三）（2026-09-04 自 tests.rs 拆出）：未知函数返回 None、L3 函数族、
//! aggregate 函数族与 aggregate-over helpers、stat selectors。

use super::*;

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
