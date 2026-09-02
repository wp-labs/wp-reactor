//! M2 §11.6 coverage: the executor eval family must read its ctx exclusively
//! through the [`FieldSource`] name protocol (`field_value` / `field_names`),
//! never through the concrete `Event` map. Two non-`Event` sources prove it:
//!
//! - `RowSource` — resolves the same name→value mapping as an `Event` (full
//!   synthetic `_step_*` / `_bind_*` protocol included): every eval entry must
//!   produce byte-identical output to the `Event` ctx it mirrors. This is the
//!   M3 precondition — a composite ctx (columnar row + step history) that
//!   honours the name protocol serves identical results.
//! - `RowOnlySource` — carries only the real row's fields (no synthetic
//!   entries), modelling a columnar row before a composite wraps it: eval must
//!   behave exactly like an `Event` *without* those entries (L3/aggregate over
//!   empty step history), never panicking or silently consulting a map that
//!   isn't there.

use super::{
    Event, Value, YieldMeta, eval_bool_expr, eval_entity_id, eval_expr_with_l3, eval_score,
    eval_yield_expr, eval_yield_expr_with_meta,
};
use crate::match_engine::EngineHashMap;
use crate::match_engine::match_engine::FieldSource;
use wf_lang::ast::{BinOp, Expr, FieldRef};

fn field(name: &str) -> Expr {
    Expr::Field(FieldRef::Simple(name.to_string()))
}

fn qualified(alias: &str, name: &str) -> Expr {
    Expr::Field(FieldRef::Qualified(alias.to_string(), name.to_string()))
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FuncCall {
        qualifier: None,
        name: name.to_string(),
        args,
    }
}

/// `stat.<fn>(<selector>(<symbol>))` — e.g. `stat.count(match_event(fail))`.
fn stat_call(fn_name: &str, selector: &str, symbol: &str) -> Expr {
    Expr::FuncCall {
        qualifier: Some("stat".to_string()),
        name: fn_name.to_string(),
        args: vec![call(selector, vec![field(symbol)])],
    }
}

fn binop(op: BinOp, left: Expr, right: Expr) -> Expr {
    Expr::BinOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn ctx_with(pairs: Vec<(&str, Value)>) -> Event {
    let mut fields = EngineHashMap::default();
    for (k, v) in pairs {
        fields.insert(k.into(), v);
    }
    Event { fields }
}

/// A non-`Event` [`FieldSource`] over the same name→value mapping.
struct RowSource<'a> {
    map: &'a EngineHashMap<smol_str::SmolStr, Value>,
}

impl FieldSource for RowSource<'_> {
    fn field_value(&self, name: &str) -> Option<Value> {
        self.map.get(name).cloned()
    }

    fn field_names(&self) -> Vec<&str> {
        self.map.keys().map(|k| k.as_str()).collect()
    }

    fn to_event(&self) -> Event {
        Event {
            fields: self.map.clone(),
        }
    }
}

/// A row-only source: field reads identical to `Event`, but `field_names()`
/// never enumerates synthetic `_step_*` / `_bind_*` history entries — the
/// shape of a bare columnar row (M3). L3/aggregate functions must see exactly
/// what an `Event` without those entries sees.
struct RowOnlySource<'a> {
    map: &'a EngineHashMap<smol_str::SmolStr, Value>,
}

fn is_synthetic(name: &str) -> bool {
    name.starts_with("_step_") || name.starts_with("_bind_")
}

impl FieldSource for RowOnlySource<'_> {
    fn field_value(&self, name: &str) -> Option<Value> {
        self.map.get(name).cloned()
    }

    fn field_names(&self) -> Vec<&str> {
        self.map
            .keys()
            .map(|k| k.as_str())
            .filter(|k| !is_synthetic(k))
            .collect()
    }

    fn to_event(&self) -> Event {
        Event {
            fields: self.map.clone(),
        }
    }
}

fn l3(expr: &Expr, ctx: &dyn FieldSource) -> Option<Value> {
    eval_expr_with_l3(expr, ctx, YieldMeta::default())
}

fn step_ctx() -> Event {
    ctx_with(vec![
        ("sip", Value::Str("10.0.0.1".into())),
        ("risk", num(0.5)),
        ("fail", num(7.0)), // stat.count(match_event(fail)) / stat.value(trigger(fail))
        ("_step_0_values", Value::Array(vec![num(10.0), num(20.0)])),
        (
            "_step_0_field_x",
            Value::Array(vec![num(1.0), num(2.0), num(3.0)]),
        ),
        ("_step_0_source", Value::Str("e".into())),
        ("_step_0_label", Value::Str("fail".into())),
        ("_step_0_measure", num(10.0)),
        ("_step_0_stage", Value::Str("event".into())),
        ("_step_1_values", Value::Array(vec![num(30.0)])),
        ("_step_1_source", Value::Str("e".into())),
        ("_step_1_measure", num(30.0)),
        ("_step_1_stage", Value::Str("close".into())),
        ("_bind_b_count", num(3.0)),
        (
            "_bind_b_field_x",
            Value::Array(vec![num(1.0), num(2.0), num(3.0)]),
        ),
    ])
}

#[test]
fn m2_row_source_matches_event_byte_for_byte() {
    let ctx = step_ctx();
    let row = RowSource { map: &ctx.fields };

    // Every eval read lane: bare fields, arithmetic, L3 over the step series,
    // L3 with a qualified step-field argument, aggregates over bind series and
    // close-stage step measures, and the stat selectors.
    let exprs: Vec<(&str, Expr)> = vec![
        ("sip", field("sip")),
        (
            "arith",
            binop(BinOp::Add, field("risk"), Expr::Number(0.25)),
        ),
        ("collect_list", call("collect_list", vec![field("value")])),
        ("collect_set", call("collect_set", vec![field("value")])),
        ("first", call("first", vec![field("value")])),
        ("last", call("last", vec![field("value")])),
        ("stddev", call("stddev", vec![field("value")])),
        (
            "percentile",
            call("percentile", vec![field("value"), Expr::Number(50.0)]),
        ),
        ("first_q", call("first", vec![qualified("e", "x")])),
        ("sum_bind", call("sum", vec![qualified("b", "x")])),
        ("count_bind", call("count", vec![qualified("b", "x")])),
        ("sum_steps", call("sum", vec![field("e")])),
        ("count_steps", call("count", vec![field("e")])),
        ("stat_count", stat_call("count", "match_event", "fail")),
        ("stat_value", stat_call("value", "trigger", "fail")),
        ("stat_window", stat_call("count", "window_event", "b")),
        ("lower", call("lower", vec![field("sip")])),
        (
            "if",
            Expr::IfThenElse {
                cond: Box::new(stat_call("value", "trigger", "fail")),
                then_expr: Box::new(qualified("e", "x")),
                else_expr: Box::new(field("sip")),
            },
        ),
    ];

    for (label, expr) in &exprs {
        let via_event = l3(expr, &ctx);
        let via_row = l3(expr, &row);
        assert_eq!(
            via_event, via_row,
            "eval_expr_with_l3 diverged over FieldSource for [{label}]"
        );
    }

    // Sanity anchor on the shared lane (both sides == Event semantics): the
    // close-stage step measure is the only `e` source after preference.
    assert_eq!(l3(&exprs[11].1, &ctx), Some(num(30.0)));
    assert_eq!(l3(&exprs[8].1, &ctx), Some(num(1.0))); // first(e.x) over [1,2,3]
    assert_eq!(l3(&exprs[9].1, &ctx), Some(num(6.0))); // sum(b.x) over [1,2,3]
    assert_eq!(l3(&exprs[13].1, &ctx), Some(num(7.0))); // stat.count(match_event(fail))
    assert_eq!(l3(&exprs[15].1, &ctx), Some(num(3.0))); // stat.count(window_event(b))

    // Entry wrappers must agree too (score clamps, entity stringifies, bool
    // strictness, yield empty-string fallback).
    let score_expr = exprs[1].1.clone();
    assert_eq!(eval_score(&score_expr, &ctx), eval_score(&score_expr, &row));
    assert_eq!(eval_score(&score_expr, &ctx), Ok(0.75));

    let entity_expr = field("sip");
    assert_eq!(
        eval_entity_id(&entity_expr, &ctx),
        eval_entity_id(&entity_expr, &row)
    );
    assert_eq!(
        eval_entity_id(&entity_expr, &ctx).as_deref(),
        Ok("10.0.0.1")
    );

    let bool_expr = binop(BinOp::Ge, field("risk"), Expr::Number(0.5));
    assert_eq!(
        eval_bool_expr(&bool_expr, &ctx),
        eval_bool_expr(&bool_expr, &row)
    );
    assert_eq!(eval_bool_expr(&bool_expr, &ctx), Some(true));

    let meta = YieldMeta {
        score: Some(0.75),
        ..YieldMeta::default()
    };
    assert_eq!(
        eval_yield_expr_with_meta(&exprs[17].1, &ctx, meta),
        eval_yield_expr_with_meta(&exprs[17].1, &row, meta)
    );
    assert_eq!(
        eval_yield_expr(&entity_expr, &ctx),
        eval_yield_expr(&entity_expr, &row)
    );
}

#[test]
fn m2_row_only_source_equals_event_without_synthetic_history() {
    // Real fields only — no `_step_*`/`_bind_*` entries in either source.
    let ctx = ctx_with(vec![
        ("sip", Value::Str("10.0.0.2".into())),
        ("risk", num(0.5)),
    ]);
    let row = RowOnlySource { map: &ctx.fields };

    // Ordinary field reads resolve identically.
    assert_eq!(
        eval_yield_expr(&field("sip"), &ctx),
        eval_yield_expr(&field("sip"), &row)
    );

    // L3 over a bare row sees an empty step history — the same result an Event
    // with no `_step_*` entries produces (empty array / None / 0.0), never a
    // panic or a wrong-value read.
    for name in ["collect_list", "collect_set"] {
        let expr = call(name, vec![field("value")]);
        assert_eq!(l3(&expr, &ctx), l3(&expr, &row), "{name} diverged");
        assert_eq!(l3(&expr, &row), Some(Value::Array(vec![])), "{name} empty");
    }
    for name in ["first", "last"] {
        let expr = call(name, vec![field("value")]);
        assert_eq!(l3(&expr, &ctx), l3(&expr, &row), "{name} diverged");
        assert_eq!(l3(&expr, &row), None, "{name} on empty history");
    }
    assert_eq!(
        l3(&call("stddev", vec![field("value")]), &row),
        Some(num(0.0))
    );
    assert_eq!(
        l3(
            &call("percentile", vec![field("value"), Expr::Number(50.0)]),
            &row
        ),
        Some(num(0.0))
    );
    assert_eq!(l3(&call("sum", vec![field("e")]), &row), None);

    // Aggregate over a missing bind series → None, same as the Event side.
    let sum_b = call("sum", vec![qualified("b", "x")]);
    assert_eq!(l3(&sum_b, &ctx), l3(&sum_b, &row));
    assert_eq!(l3(&sum_b, &row), None);
}

#[test]
fn m2_field_names_only_enumerates_row_fields_for_row_only_source() {
    // The enumeration contract behind L3 `_step_*` discovery: a row-only
    // source reports no synthetic names; a full source reports them. Guards
    // the FieldSource-field_names handshake step_data relies on.
    let ctx = ctx_with(vec![
        ("_step_0_values", Value::Array(vec![num(1.0)])),
        ("sip", Value::Str("x".into())),
    ]);
    let row = RowOnlySource { map: &ctx.fields };
    let full = RowSource { map: &ctx.fields };

    assert!(full.field_names().contains(&"_step_0_values"));
    assert!(!row.field_names().contains(&"_step_0_values"));

    // A row-only source therefore aggregates nothing from steps …
    assert_eq!(
        l3(&call("collect_list", vec![field("value")]), &row),
        Some(Value::Array(vec![]))
    );
    // … while the same fields behind a full source do.
    assert_eq!(
        l3(&call("collect_list", vec![field("value")]), &full),
        Some(Value::Array(vec![num(1.0)]))
    );
}
