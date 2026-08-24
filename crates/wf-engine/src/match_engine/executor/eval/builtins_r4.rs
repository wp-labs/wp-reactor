//! Round-4 coverage-fill tests for `eval/builtins.rs`.
//!
//! `eval_builtin_func_with_l3` is only reached through `eval_expr_with_l3` when
//! one of the L3 routing conditions matches (external / strftime / now-family /
//! args containing an L3 / aggregate / eval-time / stat-selector call). Most of
//! the dispatch arms — `contains`, `startswith`, `endswith`, `merge`, `substr`,
//! `replace`, `trim`, `lower`, `upper`, `len`, `mv*`, `split`, the math
//! functions, `fmt`, `join*`, `coalesce`, `isnull`, hash funcs, `stable_id`,
//! `strptime`, `regex_match`, `time_*` — are therefore never hit by the
//! literal-arg happy-path battery. These tests call the dispatcher directly to
//! exercise every arm's happy path **and** its error/boundary branches.
//!
//! Also covers the `eval_l3_func` / `eval_aggregate_func` /
//! `eval_stat_func` / `parse_stat_selector` / `eval_aggregate_over_*` helpers.

use super::builtins::{
    eval_aggregate_func, eval_aggregate_over_numbers, eval_aggregate_over_values,
    eval_builtin_func_with_l3, eval_l3_func, eval_stat_func, is_stat_selector_func, numeric_values,
    sum_numeric_values,
};
use super::{Event, Value, YieldMeta};
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

fn qualified(alias: &str, name: &str) -> Expr {
    Expr::Field(FieldRef::Qualified(alias.to_string(), name.to_string()))
}

fn obj_expr(pairs: Vec<(&str, f64)>) -> Expr {
    Expr::Object(
        pairs
            .into_iter()
            .map(|(k, v)| ObjectItem {
                targets: vec![k.to_string()],
                type_hint: None,
                value: num_expr(v),
            })
            .collect(),
    )
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

fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

/// Direct dispatch into the L3 builtin interpreter.
fn eval(name: &str, args: &[Expr], ctx: &Event) -> Option<Value> {
    eval_builtin_func_with_l3(name, args, ctx, YieldMeta::default())
}

// ---------------------------------------------------------------------------
// String search: contains / startswith / endswith
// ---------------------------------------------------------------------------

#[test]
fn contains_startswith_endswith_happy_and_mismatch() {
    let ctx = ctx_with(vec![]);
    // contains: haystack contains needle.
    assert_eq!(
        eval("contains", &[lit("hello world"), lit("lo wo")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("contains", &[lit("abc"), lit("z")], &ctx),
        Some(Value::Bool(false))
    );
    // contains: non-string haystack / needle → None.
    assert_eq!(eval("contains", &[num_expr(1.0), lit("a")], &ctx), None);
    assert_eq!(eval("contains", &[lit("a"), num_expr(1.0)], &ctx), None);

    // startswith.
    assert_eq!(
        eval("startswith", &[lit("hello"), lit("he")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("startswith", &[lit("hello"), lit("lo")], &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(eval("startswith", &[num_expr(1.0), lit("a")], &ctx), None);
    assert_eq!(eval("startswith", &[lit("a"), num_expr(1.0)], &ctx), None);

    // endswith.
    assert_eq!(
        eval("endswith", &[lit("hello"), lit("lo")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("endswith", &[lit("hello"), lit("he")], &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(eval("endswith", &[num_expr(1.0), lit("a")], &ctx), None);
    assert_eq!(eval("endswith", &[lit("a"), num_expr(1.0)], &ctx), None);
}

// ---------------------------------------------------------------------------
// merge
// ---------------------------------------------------------------------------

#[test]
fn merge_object_field_skip_and_rejections() {
    // Two object literals merge key-wise.
    let ctx = ctx_with(vec![
        (
            "o",
            Value::Object(EngineHashMap::from_iter([("x".into(), num(1.0))])),
        ),
        ("n", num(7.0)),
    ]);
    assert_eq!(
        eval(
            "merge",
            &[obj_expr(vec![("a", 1.0)]), obj_expr(vec![("b", 2.0)])],
            &ctx
        ),
        Some(Value::Object(EngineHashMap::from_iter([
            ("a".into(), num(1.0)),
            ("b".into(), num(2.0)),
        ])))
    );
    // A field arg resolving to an object merges its entries.
    assert_eq!(
        eval("merge", &[field("o"), obj_expr(vec![("b", 2.0)])], &ctx),
        Some(Value::Object(EngineHashMap::from_iter([
            ("x".into(), num(1.0)),
            ("b".into(), num(2.0)),
        ])))
    );
    // A missing field arg is silently skipped (`None if Field`).
    assert_eq!(
        eval("merge", &[field("ghost"), obj_expr(vec![("b", 2.0)])], &ctx),
        Some(Value::Object(EngineHashMap::from_iter([(
            "b".into(),
            num(2.0)
        )])))
    );
    // A failing non-field arg → None.
    let bogus = Expr::FuncCall {
        qualifier: None,
        name: "bogus_fn".into(),
        args: vec![],
    };
    assert_eq!(
        eval("merge", &[bogus, obj_expr(vec![("b", 2.0)])], &ctx),
        None
    );
    // A non-object value (Number) → None.
    assert_eq!(
        eval("merge", &[field("n"), obj_expr(vec![("b", 2.0)])], &ctx),
        None
    );
}

// ---------------------------------------------------------------------------
// substr
// ---------------------------------------------------------------------------

#[test]
fn substr_positive_negative_zero_and_length_bounds() {
    let ctx = ctx_with(vec![]);
    // 2-arg: start is 1-based.
    assert_eq!(
        eval("substr", &[lit("hello"), num_expr(2.0)], &ctx),
        Some(str_val("ello"))
    );
    // 3-arg: start + length.
    assert_eq!(
        eval(
            "substr",
            &[lit("hello"), num_expr(1.0), num_expr(3.0)],
            &ctx
        ),
        Some(str_val("hel"))
    );
    // start == 0 → treated as 1.
    assert_eq!(
        eval("substr", &[lit("hello"), num_expr(0.0)], &ctx),
        Some(str_val("hello"))
    );
    // negative start: len + start.
    assert_eq!(
        eval("substr", &[lit("hello"), num_expr(-2.0)], &ctx),
        Some(str_val("lo"))
    );
    // very negative start clamped to 0.
    assert_eq!(
        eval("substr", &[lit("hello"), num_expr(-10.0)], &ctx),
        Some(str_val("hello"))
    );
    // start beyond the string → empty.
    assert_eq!(
        eval("substr", &[lit("ab"), num_expr(5.0)], &ctx),
        Some(str_val(""))
    );
    // length <= 0 → empty.
    assert_eq!(
        eval(
            "substr",
            &[lit("hello"), num_expr(1.0), num_expr(0.0)],
            &ctx
        ),
        Some(str_val(""))
    );
    // length clamped to the string end.
    assert_eq!(
        eval(
            "substr",
            &[lit("hello"), num_expr(1.0), num_expr(100.0)],
            &ctx
        ),
        Some(str_val("hello"))
    );
    // Unicode-aware: chars, not bytes.
    assert_eq!(
        eval(
            "substr",
            &[lit("你好世界"), num_expr(2.0), num_expr(2.0)],
            &ctx
        ),
        Some(str_val("好世"))
    );
    // Type mismatches → None.
    assert_eq!(eval("substr", &[num_expr(1.0), num_expr(1.0)], &ctx), None);
    assert_eq!(eval("substr", &[lit("hi"), lit("x")], &ctx), None);
    assert_eq!(
        eval("substr", &[lit("hi"), num_expr(1.0), lit("x")], &ctx),
        None
    );
}

// ---------------------------------------------------------------------------
// replace / replace_plain / trim / lower / upper / len
// ---------------------------------------------------------------------------

#[test]
fn replace_trim_case_and_len() {
    let ctx = ctx_with(vec![]);
    assert_eq!(
        eval("replace", &[lit("hello world"), lit("o"), lit("0")], &ctx),
        Some(str_val("hell0 w0rld"))
    );
    // invalid regex → None.
    assert_eq!(
        eval("replace", &[lit("abc"), lit("["), lit("x")], &ctx),
        None
    );
    assert_eq!(
        eval("replace", &[lit("a"), lit("b"), num_expr(1.0)], &ctx),
        None
    );

    assert_eq!(eval("trim", &[lit("  x  ")], &ctx), Some(str_val("x")));
    assert_eq!(eval("trim", &[num_expr(1.0)], &ctx), None);
    assert_eq!(eval("lower", &[lit("AbC")], &ctx), Some(str_val("abc")));
    assert_eq!(eval("lower", &[num_expr(1.0)], &ctx), None);
    assert_eq!(eval("upper", &[lit("aBc")], &ctx), Some(str_val("ABC")));
    assert_eq!(eval("upper", &[num_expr(1.0)], &ctx), None);
    assert_eq!(eval("len", &[lit("héllo")], &ctx), Some(num(6.0)));
    assert_eq!(eval("len", &[num_expr(1.0)], &ctx), None);
    assert_eq!(
        eval("replace_plain", &[lit("a-b-c"), lit("-"), lit("+")], &ctx),
        Some(str_val("a+b+c"))
    );
    assert_eq!(
        eval("replace_plain", &[lit("a"), lit("b"), num_expr(1.0)], &ctx),
        None
    );
}

// ---------------------------------------------------------------------------
// Multivalue: mvcount / mvjoin / mvindex / mvappend / split / mvdedup /
// mvsort / mvreverse
// ---------------------------------------------------------------------------

#[test]
fn mv_functions_bounds_and_shapes() {
    let ctx = ctx_with(vec![]);
    let three = Expr::Array(vec![num_expr(1.0), num_expr(2.0), num_expr(3.0)]);

    assert_eq!(
        eval("mvcount", &[Expr::Array(vec![])], &ctx),
        Some(num(0.0))
    );
    assert_eq!(
        eval(
            "mvcount",
            &[Expr::Array(vec![num_expr(1.0), num_expr(2.0)])],
            &ctx
        ),
        Some(num(2.0))
    );
    assert_eq!(eval("mvcount", &[lit("x")], &ctx), None);

    assert_eq!(
        eval(
            "mvjoin",
            &[Expr::Array(vec![num_expr(1.0), num_expr(2.0)]), lit("-")],
            &ctx
        ),
        Some(str_val("1-2"))
    );
    assert_eq!(eval("mvjoin", &[lit("x"), lit("-")], &ctx), None);
    assert_eq!(
        eval(
            "mvjoin",
            &[Expr::Array(vec![num_expr(1.0)]), num_expr(1.0)],
            &ctx
        ),
        None
    );

    // mvindex 2-arg: positive（0 基索引）、negative、out-of-range。
    assert_eq!(
        eval("mvindex", &[three.clone(), num_expr(1.0)], &ctx),
        Some(num(2.0)),
        "mvindex 0 基索引：index=1 → 第二个元素"
    );
    assert_eq!(
        eval("mvindex", &[three.clone(), num_expr(-1.0)], &ctx),
        Some(num(3.0))
    );
    assert_eq!(eval("mvindex", &[three.clone(), num_expr(9.0)], &ctx), None);
    assert_eq!(eval("mvindex", &[three.clone(), lit("x")], &ctx), None);
    // mvindex 3-arg slices.
    assert_eq!(
        eval(
            "mvindex",
            &[three.clone(), num_expr(1.0), num_expr(2.0)],
            &ctx
        ),
        Some(arr(vec![num(2.0), num(3.0)]))
    );
    // empty array → empty.
    assert_eq!(
        eval(
            "mvindex",
            &[Expr::Array(vec![]), num_expr(0.0), num_expr(1.0)],
            &ctx
        ),
        Some(arr(vec![]))
    );
    // negative end beyond the start → empty.
    assert_eq!(
        eval(
            "mvindex",
            &[three.clone(), num_expr(0.0), num_expr(-10.0)],
            &ctx
        ),
        Some(arr(vec![]))
    );
    // start beyond the end → empty.
    assert_eq!(
        eval(
            "mvindex",
            &[three.clone(), num_expr(10.0), num_expr(11.0)],
            &ctx
        ),
        Some(arr(vec![]))
    );
    // start > end → empty.
    assert_eq!(
        eval(
            "mvindex",
            &[three.clone(), num_expr(2.0), num_expr(1.0)],
            &ctx
        ),
        Some(arr(vec![]))
    );
    // negative start/end slice.
    assert_eq!(
        eval(
            "mvindex",
            &[three.clone(), num_expr(-2.0), num_expr(-1.0)],
            &ctx
        ),
        Some(arr(vec![num(2.0), num(3.0)]))
    );
    assert_eq!(eval("mvindex", &[lit("x"), num_expr(0.0)], &ctx), None);
    assert_eq!(
        eval("mvindex", &[three, num_expr(0.0), lit("x")], &ctx),
        None
    );

    // mvappend flattens arrays and keeps scalars.
    assert_eq!(
        eval(
            "mvappend",
            &[
                Expr::Array(vec![num_expr(1.0)]),
                num_expr(2.0),
                Expr::Array(vec![num_expr(3.0)])
            ],
            &ctx
        ),
        Some(arr(vec![num(1.0), num(2.0), num(3.0)]))
    );

    // split with a separator and with an empty separator (char split).
    assert_eq!(
        eval("split", &[lit("a,b,c"), lit(",")], &ctx),
        Some(arr(vec![str_val("a"), str_val("b"), str_val("c")]))
    );
    assert_eq!(
        eval("split", &[lit("ab"), lit("")], &ctx),
        Some(arr(vec![str_val("a"), str_val("b")]))
    );
    assert_eq!(eval("split", &[num_expr(1.0), lit(",")], &ctx), None);
    assert_eq!(eval("split", &[lit("a"), num_expr(1.0)], &ctx), None);

    // mvdedup preserves order.
    assert_eq!(
        eval(
            "mvdedup",
            &[Expr::Array(vec![
                num_expr(1.0),
                num_expr(1.0),
                num_expr(2.0)
            ])],
            &ctx
        ),
        Some(arr(vec![num(1.0), num(2.0)]))
    );
    assert_eq!(eval("mvdedup", &[lit("x")], &ctx), None);

    // mvsort / mvreverse.
    assert_eq!(
        eval(
            "mvsort",
            &[Expr::Array(vec![
                num_expr(3.0),
                num_expr(1.0),
                num_expr(2.0)
            ])],
            &ctx
        ),
        Some(arr(vec![num(1.0), num(2.0), num(3.0)]))
    );
    assert_eq!(
        eval(
            "mvreverse",
            &[Expr::Array(vec![num_expr(1.0), num_expr(2.0)])],
            &ctx
        ),
        Some(arr(vec![num(2.0), num(1.0)]))
    );
    assert_eq!(eval("mvsort", &[lit("x")], &ctx), None);
    assert_eq!(eval("mvreverse", &[lit("x")], &ctx), None);
}

// ---------------------------------------------------------------------------
// Math: abs / round / ceil / floor / sqrt / pow / log / exp / clamp / sign /
// trunc / is_finite
// ---------------------------------------------------------------------------

#[test]
fn math_functions_positive_negative_and_nonfinite() {
    let ctx = ctx_with(vec![]);
    assert_eq!(eval("abs", &[num_expr(-5.0)], &ctx), Some(num(5.0)));
    assert_eq!(eval("abs", &[lit("x")], &ctx), None);

    assert_eq!(eval("round", &[num_expr(3.7)], &ctx), Some(num(4.0)));
    assert_eq!(
        eval("round", &[num_expr(12.3456), num_expr(2.0)], &ctx),
        Some(num(12.35))
    );
    assert_eq!(eval("round", &[lit("x")], &ctx), None);
    assert_eq!(eval("round", &[num_expr(1.0), lit("x")], &ctx), None);
    // non-finite precision → None.
    assert_eq!(
        eval("round", &[num_expr(1.0), num_expr(f64::NAN)], &ctx),
        None
    );

    assert_eq!(eval("ceil", &[num_expr(1.2)], &ctx), Some(num(2.0)));
    assert_eq!(eval("ceil", &[lit("x")], &ctx), None);
    assert_eq!(eval("floor", &[num_expr(1.8)], &ctx), Some(num(1.0)));
    assert_eq!(eval("floor", &[lit("x")], &ctx), None);

    assert_eq!(eval("sqrt", &[num_expr(9.0)], &ctx), Some(num(3.0)));
    assert_eq!(eval("sqrt", &[num_expr(-1.0)], &ctx), None);
    assert_eq!(eval("sqrt", &[lit("x")], &ctx), None);

    assert_eq!(
        eval("pow", &[num_expr(2.0), num_expr(3.0)], &ctx),
        Some(num(8.0))
    );
    // 0^negative → inf → None.
    assert_eq!(eval("pow", &[num_expr(0.0), num_expr(-1.0)], &ctx), None);
    assert_eq!(eval("pow", &[lit("x"), num_expr(1.0)], &ctx), None);
    assert_eq!(eval("pow", &[num_expr(1.0), lit("x")], &ctx), None);

    assert_eq!(eval("log", &[num_expr(8.0)], &ctx), Some(num(8.0f64.ln())));
    assert_eq!(
        eval("log", &[num_expr(8.0), num_expr(2.0)], &ctx),
        Some(num(3.0))
    );
    assert_eq!(eval("log", &[num_expr(-1.0)], &ctx), None);
    assert_eq!(eval("log", &[num_expr(0.0)], &ctx), None);
    assert_eq!(eval("log", &[num_expr(8.0), num_expr(1.0)], &ctx), None);
    assert_eq!(eval("log", &[num_expr(8.0), num_expr(0.0)], &ctx), None);
    assert_eq!(eval("log", &[lit("x")], &ctx), None);

    assert!(eval("exp", &[num_expr(1.0)], &ctx).is_some());
    // exp(1000) overflows → None.
    assert_eq!(eval("exp", &[num_expr(1000.0)], &ctx), None);
    assert_eq!(eval("exp", &[lit("x")], &ctx), None);

    assert_eq!(
        eval(
            "clamp",
            &[num_expr(5.0), num_expr(0.0), num_expr(10.0)],
            &ctx
        ),
        Some(num(5.0))
    );
    assert_eq!(
        eval(
            "clamp",
            &[num_expr(50.0), num_expr(0.0), num_expr(10.0)],
            &ctx
        ),
        Some(num(10.0))
    );
    // min > max → None.
    assert_eq!(
        eval(
            "clamp",
            &[num_expr(5.0), num_expr(10.0), num_expr(0.0)],
            &ctx
        ),
        None
    );
    assert_eq!(
        eval("clamp", &[num_expr(5.0), lit("x"), num_expr(10.0)], &ctx),
        None
    );

    assert_eq!(eval("sign", &[num_expr(-3.0)], &ctx), Some(num(-1.0)));
    assert_eq!(eval("sign", &[num_expr(f64::NAN)], &ctx), None);
    assert_eq!(eval("sign", &[lit("x")], &ctx), None);

    assert_eq!(eval("trunc", &[num_expr(3.7)], &ctx), Some(num(3.0)));
    assert_eq!(eval("trunc", &[lit("x")], &ctx), None);

    assert_eq!(
        eval("is_finite", &[num_expr(3.0)], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("is_finite", &[num_expr(f64::NAN)], &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(eval("is_finite", &[lit("x")], &ctx), None);

    assert_eq!(eval("ltrim", &[lit("  x")], &ctx), Some(str_val("x")));
    assert_eq!(eval("ltrim", &[num_expr(1.0)], &ctx), None);
    assert_eq!(eval("rtrim", &[lit("x  ")], &ctx), Some(str_val("x")));
    assert_eq!(eval("rtrim", &[num_expr(1.0)], &ctx), None);
}

// ---------------------------------------------------------------------------
// fmt / concat / join / join_by / indexof / startswith_any / endswith_any
// ---------------------------------------------------------------------------

#[test]
fn format_and_join_functions() {
    let ctx = ctx_with(vec![
        ("o", Value::Object(EngineHashMap::default())),
        ("s", str_val("hello")),
    ]);
    assert_eq!(
        eval("fmt", &[lit("{}:{}"), lit("a"), num_expr(1.0)], &ctx),
        Some(str_val("a:1"))
    );
    // placeholder count mismatch → None.
    assert_eq!(eval("fmt", &[lit("{}"), lit("a"), lit("b")], &ctx), None);
    assert_eq!(eval("fmt", &[num_expr(1.0)], &ctx), None);

    assert_eq!(
        eval("concat", &[lit("a"), num_expr(1.0), lit("b")], &ctx),
        Some(str_val("a1b"))
    );

    assert_eq!(
        eval("join", &[lit("a"), num_expr(1.0)], &ctx),
        Some(str_val("a1"))
    );
    // missing field arg → empty string.
    assert_eq!(eval("join", &[field("ghost")], &ctx), Some(str_val("")));
    // object-valued arg → None (scalar_value_to_string rejects).
    assert_eq!(eval("join", &[field("o")], &ctx), None);
    // failing non-field arg → None (no empty-string fallback).
    let bogus = Expr::FuncCall {
        qualifier: None,
        name: "bogus_fn".into(),
        args: vec![],
    };
    assert_eq!(eval("join", std::slice::from_ref(&bogus), &ctx), None);

    assert_eq!(
        eval("join_by", &[lit("|"), lit("a"), num_expr(1.0)], &ctx),
        Some(str_val("a|1"))
    );
    assert_eq!(eval("join_by", &[lit("|"), field("o")], &ctx), None);
    assert_eq!(eval("join_by", &[lit("|"), bogus], &ctx), None);
    assert_eq!(eval("join_by", &[num_expr(1.0), lit("a")], &ctx), None);

    assert_eq!(
        eval("indexof", &[lit("abc"), lit("b")], &ctx),
        Some(num(1.0))
    );
    assert_eq!(
        eval("indexof", &[lit("abc"), lit("z")], &ctx),
        Some(num(-1.0))
    );
    assert_eq!(eval("indexof", &[num_expr(1.0), lit("a")], &ctx), None);
    assert_eq!(eval("indexof", &[lit("a"), num_expr(1.0)], &ctx), None);

    assert_eq!(
        eval(
            "startswith_any",
            &[lit("hello"), lit("he"), lit("xx")],
            &ctx
        ),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("startswith_any", &[lit("hello"), lit("xx")], &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval("endswith_any", &[lit("hello"), lit("lo"), lit("xx")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("endswith_any", &[lit("hello"), lit("xx")], &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval("startswith_any", &[num_expr(1.0), lit("a")], &ctx),
        None
    );
    assert_eq!(eval("endswith_any", &[lit("a"), num_expr(1.0)], &ctx), None);
}

// ---------------------------------------------------------------------------
// Null handling: coalesce / isnull / isnotnull / is_blank / null_if_blank /
// default_if_blank
// ---------------------------------------------------------------------------

#[test]
fn null_and_blank_handling() {
    let ctx = ctx_with(vec![("n", num(5.0))]);
    assert_eq!(
        eval("coalesce", &[lit(""), lit("x")], &ctx),
        Some(str_val("x"))
    );
    assert_eq!(eval("coalesce", &[lit("x")], &ctx), Some(str_val("x")));
    assert_eq!(eval("coalesce", &[lit(""), lit("  ")], &ctx), None);
    // Non-string values are returned as-is.
    assert_eq!(eval("coalesce", &[num_expr(1.0)], &ctx), Some(num(1.0)));

    assert_eq!(
        eval("isnull", &[field("ghost")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(eval("isnull", &[lit("x")], &ctx), Some(Value::Bool(false)));

    assert_eq!(
        eval("isnotnull", &[lit("x")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("isnotnull", &[field("ghost")], &ctx),
        Some(Value::Bool(false))
    );

    assert_eq!(
        eval("is_blank", &[lit("  ")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("is_blank", &[lit("x")], &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval("is_blank", &[field("ghost")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(eval("is_blank", &[field("n")], &ctx), None);

    assert_eq!(eval("null_if_blank", &[lit("")], &ctx), None);
    assert_eq!(eval("null_if_blank", &[lit("x")], &ctx), Some(str_val("x")));
    assert_eq!(eval("null_if_blank", &[field("n")], &ctx), None);

    assert_eq!(
        eval("default_if_blank", &[lit("x"), lit("d")], &ctx),
        Some(str_val("x"))
    );
    assert_eq!(
        eval("default_if_blank", &[lit(""), lit("d")], &ctx),
        Some(str_val("d"))
    );
    assert_eq!(
        eval("default_if_blank", &[field("ghost"), lit("d")], &ctx),
        Some(str_val("d"))
    );
    // first arg non-string → None.
    assert_eq!(
        eval("default_if_blank", &[field("n"), lit("d")], &ctx),
        None
    );
    // second arg non-string → None.
    assert_eq!(
        eval("default_if_blank", &[lit(""), num_expr(1.0)], &ctx),
        None
    );
}

// ---------------------------------------------------------------------------
// Hashing: md5 / sha1 / sha1_n / sha256 / hex / stable_id
// ---------------------------------------------------------------------------

#[test]
fn hash_functions_and_stable_id() {
    let ctx = ctx_with(vec![("o", Value::Object(EngineHashMap::default()))]);
    assert_eq!(
        eval("md5", &[lit("abc")], &ctx),
        Some(str_val("900150983cd24fb0d6963f7d28e17f72"))
    );
    assert_eq!(eval("md5", &[num_expr(1.0)], &ctx), None);

    assert_eq!(
        eval("sha1", &[lit("abc")], &ctx),
        Some(str_val("a9993e364706816aba3e25717850c26c9cd0d89d"))
    );
    assert_eq!(eval("sha1", &[num_expr(1.0)], &ctx), None);

    // sha1_n truncates to the first `len` hex chars.
    assert_eq!(
        eval("sha1_n", &[lit("abc"), num_expr(8.0)], &ctx),
        Some(str_val("a9993e36"))
    );
    // fractional / non-finite length → None.
    assert_eq!(eval("sha1_n", &[lit("abc"), num_expr(8.5)], &ctx), None);
    assert_eq!(
        eval("sha1_n", &[lit("abc"), num_expr(f64::NAN)], &ctx),
        None
    );
    // out-of-range length → None.
    assert_eq!(eval("sha1_n", &[lit("abc"), num_expr(0.0)], &ctx), None);
    assert_eq!(eval("sha1_n", &[lit("abc"), num_expr(41.0)], &ctx), None);
    assert_eq!(eval("sha1_n", &[num_expr(1.0), num_expr(8.0)], &ctx), None);

    assert_eq!(
        eval("sha256", &[lit("abc")], &ctx),
        Some(str_val(
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        ))
    );
    assert_eq!(eval("sha256", &[num_expr(1.0)], &ctx), None);

    assert_eq!(eval("hex", &[lit("ab")], &ctx), Some(str_val("6162")));
    assert_eq!(eval("hex", &[num_expr(1.0)], &ctx), None);

    // stable_id: prefix + 16 hex chars of the FNV of the args.
    let sid = eval("stable_id", &[lit("pfx"), lit("a"), num_expr(1.0)], &ctx);
    let Some(Value::Str(s)) = sid else {
        panic!("stable_id must return a string");
    };
    assert!(s.starts_with("pfx"));
    assert_eq!(s.len(), 3 + 16);
    // object arg → update_stable_id_hash fails → None.
    assert_eq!(eval("stable_id", &[lit("pfx"), field("o")], &ctx), None);
}

// ---------------------------------------------------------------------------
// Time: strptime / regex_match / time_diff / time_bucket / bucket_end
// ---------------------------------------------------------------------------

#[test]
fn time_and_regex_functions() {
    let ctx = ctx_with(vec![]);
    // strptime parses to epoch millis.
    assert_eq!(
        eval(
            "strptime",
            &[lit("2024-01-01 00:00:00"), lit("%Y-%m-%d %H:%M:%S")],
            &ctx
        ),
        Some(num(1704067200000.0))
    );
    // parse failure → None.
    assert_eq!(
        eval("strptime", &[lit("not-a-date"), lit("%Y")], &ctx),
        None
    );
    assert_eq!(eval("strptime", &[num_expr(1.0), lit("%Y")], &ctx), None);

    assert_eq!(
        eval("regex_match", &[lit("abc"), lit("^a")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("regex_match", &[lit("abc"), lit("^z")], &ctx),
        Some(Value::Bool(false))
    );
    // invalid pattern → None.
    assert_eq!(eval("regex_match", &[lit("abc"), lit("[")], &ctx), None);
    assert_eq!(eval("regex_match", &[num_expr(1.0), lit("a")], &ctx), None);

    // time_diff: seconds between two epoch values.
    assert_eq!(
        eval("time_diff", &[num_expr(5.0), num_expr(2.0)], &ctx),
        Some(num(3.0))
    );
    assert_eq!(
        eval("time_diff", &[num_expr(f64::NAN), num_expr(2.0)], &ctx),
        None
    );

    // time_bucket: 60s buckets over an epoch-nanos value.
    let ts = 1_700_000_000_000_000_000.0;
    assert_eq!(
        eval("time_bucket", &[num_expr(ts), num_expr(60.0)], &ctx),
        Some(num(1_699_999_980_000.0))
    );
    // non-positive interval → None.
    assert_eq!(
        eval("time_bucket", &[num_expr(ts), num_expr(0.0)], &ctx),
        None
    );

    // bucket_end = bucket + interval（60s = 60000ms，不是 600ms）。
    assert_eq!(
        eval("bucket_end", &[num_expr(ts), num_expr(60.0)], &ctx),
        Some(num(1_700_000_040_000.0))
    );
    assert_eq!(
        eval("bucket_end", &[num_expr(ts), num_expr(0.0)], &ctx),
        None
    );
}

// ---------------------------------------------------------------------------
// CIDR: cidr_match（Sigma |cidr 等效）
// ---------------------------------------------------------------------------

#[test]
fn cidr_match_v4_v6_and_errors() {
    let ctx = ctx_with(vec![]);
    // IPv4 私有网段命中。
    assert_eq!(
        eval("cidr_match", &[lit("10.1.2.3"), lit("10.0.0.0/8")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("cidr_match", &[lit("11.0.0.1"), lit("10.0.0.0/8")], &ctx),
        Some(Value::Bool(false))
    );
    // /32 精确匹配。
    assert_eq!(
        eval("cidr_match", &[lit("8.8.8.8"), lit("8.8.8.8/32")], &ctx),
        Some(Value::Bool(true))
    );
    // 默认路由 /0。
    assert_eq!(
        eval("cidr_match", &[lit("1.2.3.4"), lit("0.0.0.0/0")], &ctx),
        Some(Value::Bool(true))
    );
    // IPv6。
    assert_eq!(
        eval("cidr_match", &[lit("fe80::1"), lit("fe80::/10")], &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval("cidr_match", &[lit("fe80::1"), lit("::1/128")], &ctx),
        Some(Value::Bool(false))
    );
    // 版本不一致不匹配。
    assert_eq!(
        eval("cidr_match", &[lit("127.0.0.1"), lit("::1/128")], &ctx),
        Some(Value::Bool(false))
    );
    // 错误分支：非法 CIDR 子网 / 非字符串 / 非 IP。
    assert_eq!(eval("cidr_match", &[lit("1.2.3.4"), lit("bad")], &ctx), None);
    assert_eq!(eval("cidr_match", &[lit("1.2.3.4"), lit("10.0.0.0/33")], &ctx), None);
    assert_eq!(eval("cidr_match", &[num_expr(1.0), lit("10.0.0.0/8")], &ctx), None);
    assert_eq!(eval("cidr_match", &[lit("1.2.3.4"), num_expr(1.0)], &ctx), None);
    assert_eq!(eval("cidr_match", &[lit("1.2.3.4")], &ctx), None);
    // 非 IP 字符串（如 event.action）→ false。
    assert_eq!(
        eval("cidr_match", &[lit("not-an-ip"), lit("10.0.0.0/8")], &ctx),
        Some(Value::Bool(false))
    );
}

// ---------------------------------------------------------------------------
// stat selectors: eval_stat_func / parse_stat_selector / number_value
// ---------------------------------------------------------------------------

#[test]
fn stat_selector_and_stat_func_branches() {
    assert!(is_stat_selector_func("window_event"));
    assert!(is_stat_selector_func("match_event"));
    assert!(is_stat_selector_func("match_distinct"));
    assert!(is_stat_selector_func("trigger"));
    assert!(is_stat_selector_func("final"));
    assert!(!is_stat_selector_func("bogus"));

    let sel = |name: &str, sym: &str| Expr::FuncCall {
        qualifier: None,
        name: name.to_string(),
        args: vec![field(sym)],
    };

    // count(window_event(alias)) reads `_bind_{alias}_count`.
    let ctx = ctx_with(vec![("_bind_w_count", num(7.0))]);
    assert_eq!(
        eval_stat_func("count", &[sel("window_event", "w")], &ctx),
        Some(num(7.0))
    );
    // count(match_event(label)) reads the label field.
    let ctx = ctx_with(vec![("fail", num(3.0))]);
    assert_eq!(
        eval_stat_func("count", &[sel("match_event", "fail")], &ctx),
        Some(num(3.0))
    );
    assert_eq!(
        eval_stat_func("count", &[sel("match_distinct", "fail")], &ctx),
        Some(num(3.0))
    );
    // value(trigger/final(label)) reads the label field.
    assert_eq!(
        eval_stat_func("value", &[sel("trigger", "fail")], &ctx),
        Some(num(3.0))
    );
    assert_eq!(
        eval_stat_func("value", &[sel("final", "fail")], &ctx),
        Some(num(3.0))
    );
    // wrong arg count → None.
    assert_eq!(eval_stat_func("count", &[], &ctx), None);
    assert_eq!(
        eval_stat_func(
            "count",
            &[sel("window_event", "w"), sel("window_event", "w")],
            &ctx
        ),
        None
    );
    // non-func-call selector → None.
    assert_eq!(eval_stat_func("count", &[field("x")], &ctx), None);
    // unknown selector name → None.
    assert_eq!(eval_stat_func("count", &[sel("bogus", "x")], &ctx), None);
    // name/selector mismatch → None (count + trigger).
    assert_eq!(eval_stat_func("count", &[sel("trigger", "x")], &ctx), None);
    // unknown function name → None.
    assert_eq!(eval_stat_func("bogus", &[sel("trigger", "x")], &ctx), None);
    // non-numeric count field → None.
    let ctx = ctx_with(vec![("_bind_w_count", str_val("x"))]);
    assert_eq!(
        eval_stat_func("count", &[sel("window_event", "w")], &ctx),
        None
    );
}

// ---------------------------------------------------------------------------
// L3 functions: collect_set / collect_list / first / last / stddev / percentile
// ---------------------------------------------------------------------------

#[test]
fn l3_func_series_and_edge_cases() {
    let ctx = ctx_with(vec![
        ("_step_0_values", arr(vec![num(1.0), num(2.0), num(2.0)])),
        ("_step_0_source", str_val("e")),
    ]);
    let empty_ctx = ctx_with(vec![]);

    // empty args → None.
    assert_eq!(
        eval_l3_func("collect_set", &[], &empty_ctx, YieldMeta::default()),
        None
    );

    // collect_set dedups.
    assert_eq!(
        eval_l3_func("collect_set", &[field("e")], &ctx, YieldMeta::default()),
        Some(arr(vec![num(1.0), num(2.0)]))
    );
    // collect_list keeps everything.
    assert_eq!(
        eval_l3_func("collect_list", &[field("e")], &ctx, YieldMeta::default()),
        Some(arr(vec![num(1.0), num(2.0), num(2.0)]))
    );
    // first / last.
    assert_eq!(
        eval_l3_func("first", &[field("e")], &ctx, YieldMeta::default()),
        Some(num(1.0))
    );
    assert_eq!(
        eval_l3_func("last", &[field("e")], &ctx, YieldMeta::default()),
        Some(num(2.0))
    );
    // wrong arg count for these 1-arg funcs → None.
    assert_eq!(
        eval_l3_func(
            "collect_list",
            &[field("e"), field("e")],
            &ctx,
            YieldMeta::default()
        ),
        None
    );
    assert_eq!(
        eval_l3_func(
            "first",
            &[field("e"), field("e")],
            &ctx,
            YieldMeta::default()
        ),
        None
    );
    assert_eq!(
        eval_l3_func(
            "last",
            &[field("e"), field("e")],
            &ctx,
            YieldMeta::default()
        ),
        None
    );
    assert_eq!(
        eval_l3_func(
            "collect_set",
            &[field("e"), field("e")],
            &ctx,
            YieldMeta::default()
        ),
        None
    );

    // stddev with < 2 numeric values → 0.
    let single = ctx_with(vec![("_step_0_values", arr(vec![num(5.0)]))]);
    assert_eq!(
        eval_l3_func("stddev", &[field("e")], &single, YieldMeta::default()),
        Some(num(0.0))
    );
    // stddev with non-numeric values only → 0.
    let strs = ctx_with(vec![(
        "_step_0_values",
        arr(vec![str_val("a"), str_val("b")]),
    )]);
    assert_eq!(
        eval_l3_func("stddev", &[field("e")], &strs, YieldMeta::default()),
        Some(num(0.0))
    );
    // stddev happy path: [1, 2, 3] → sqrt(2/3).
    let three = ctx_with(vec![(
        "_step_0_values",
        arr(vec![num(1.0), num(2.0), num(3.0)]),
    )]);
    assert_eq!(
        eval_l3_func("stddev", &[field("e")], &three, YieldMeta::default()),
        Some(num((2.0f64 / 3.0f64).sqrt()))
    );
    // stddev arg count.
    assert_eq!(
        eval_l3_func(
            "stddev",
            &[field("e"), field("e")],
            &three,
            YieldMeta::default()
        ),
        None
    );

    // percentile p out of [0,100] clamps; p non-number → None.
    assert_eq!(
        eval_l3_func(
            "percentile",
            &[field("e"), num_expr(50.0)],
            &three,
            YieldMeta::default()
        ),
        Some(num(2.0))
    );
    assert_eq!(
        eval_l3_func("percentile", &[field("e")], &three, YieldMeta::default()),
        None
    );
    assert_eq!(
        eval_l3_func(
            "percentile",
            &[field("e"), lit("x")],
            &three,
            YieldMeta::default()
        ),
        None
    );
    // empty numeric series → 0.
    assert_eq!(
        eval_l3_func(
            "percentile",
            &[field("e"), num_expr(50.0)],
            &empty_ctx,
            YieldMeta::default()
        ),
        Some(num(0.0))
    );

    // unknown l3 name → None.
    assert_eq!(
        eval_l3_func("bogus", &[field("e")], &ctx, YieldMeta::default()),
        None
    );
}

// ---------------------------------------------------------------------------
// Aggregate funcs: eval_aggregate_func + over-numbers / over-values helpers
// ---------------------------------------------------------------------------

#[test]
fn aggregate_func_step_bind_and_missing_paths() {
    let steps = ctx_with(vec![
        ("_step_0_measure", num(10.0)),
        ("_step_0_values", arr(vec![num(10.0)])),
        ("_step_0_source", str_val("e")),
        ("_step_1_measure", num(20.0)),
        ("_step_1_values", arr(vec![num(20.0)])),
        ("_step_1_source", str_val("e")),
    ]);
    // Simple field arg → step measures.
    assert_eq!(
        eval_aggregate_func("sum", &[field("e")], &steps),
        Some(num(30.0))
    );
    assert_eq!(
        eval_aggregate_func("count", &[field("e")], &steps),
        Some(num(30.0))
    );
    // wrong arg count → None.
    assert_eq!(eval_aggregate_func("sum", &[], &steps), None);
    assert_eq!(
        eval_aggregate_func("sum", &[field("e"), field("e")], &steps),
        None
    );
    // non-field arg → None.
    let bogus = Expr::FuncCall {
        qualifier: None,
        name: "bogus_fn".into(),
        args: vec![],
    };
    assert_eq!(eval_aggregate_func("sum", &[bogus], &steps), None);

    // Qualified field with step values (by source) → over_values.
    assert_eq!(
        eval_aggregate_func(
            "sum",
            &[qualified("e", "x")],
            &ctx_with(vec![
                ("_step_0_values", arr(vec![num(1.0), num(2.0)])),
                ("_step_0_source", str_val("e")),
            ])
        ),
        Some(num(3.0))
    );
    // Qualified field with bind values only → over_values on bind series.
    let bind = ctx_with(vec![
        ("_bind_b_field_x", arr(vec![num(1.0), num(2.0), num(3.0)])),
        ("_bind_b_count", num(3.0)),
    ]);
    assert_eq!(
        eval_aggregate_func("max", &[qualified("b", "x")], &bind),
        Some(num(3.0))
    );
    // Qualified field with neither step nor bind series → None.
    assert_eq!(
        eval_aggregate_func("sum", &[qualified("b", "x")], &ctx_with(vec![])),
        None
    );

    // Simple field: count with a bind alias → bind count.
    assert_eq!(
        eval_aggregate_func("count", &[field("b")], &bind),
        Some(num(3.0))
    );
    // Simple field: step values fallback (no measures).
    assert_eq!(
        eval_aggregate_func(
            "avg",
            &[field("e")],
            &ctx_with(vec![
                ("_step_0_values", arr(vec![num(2.0), num(4.0)])),
                ("_step_0_source", str_val("e")),
            ])
        ),
        Some(num(3.0))
    );
    // Simple field with nothing → None.
    assert_eq!(
        eval_aggregate_func("sum", &[field("e")], &ctx_with(vec![])),
        None
    );
}

#[test]
fn aggregate_over_numbers_and_values_helpers() {
    // over numbers.
    assert_eq!(
        eval_aggregate_over_numbers("count", &[1.0, 2.0]),
        Some(num(3.0))
    );
    assert_eq!(
        eval_aggregate_over_numbers("sum", &[1.0, 2.0]),
        Some(num(3.0))
    );
    assert_eq!(
        eval_aggregate_over_numbers("avg", &[1.0, 3.0]),
        Some(num(2.0))
    );
    assert_eq!(eval_aggregate_over_numbers("avg", &[]), Some(num(0.0)));
    assert_eq!(
        eval_aggregate_over_numbers("min", &[3.0, 1.0]),
        Some(num(1.0))
    );
    assert_eq!(eval_aggregate_over_numbers("min", &[]), Some(num(0.0)));
    assert_eq!(
        eval_aggregate_over_numbers("max", &[3.0, 1.0]),
        Some(num(3.0))
    );
    assert_eq!(eval_aggregate_over_numbers("max", &[]), Some(num(0.0)));
    assert_eq!(eval_aggregate_over_numbers("bogus", &[1.0]), None);

    // over values.
    let vals = vec![num(1.0), num(2.0), num(3.0)];
    assert_eq!(eval_aggregate_over_values("count", &vals), Some(num(3.0)));
    assert_eq!(eval_aggregate_over_values("sum", &vals), Some(num(6.0)));
    assert_eq!(eval_aggregate_over_values("avg", &vals), Some(num(2.0)));
    assert_eq!(eval_aggregate_over_values("avg", &[]), Some(num(0.0)));
    // min/max use compare_sortable_values (numeric).
    assert_eq!(eval_aggregate_over_values("min", &vals), Some(num(1.0)));
    assert_eq!(eval_aggregate_over_values("max", &vals), Some(num(3.0)));
    // mixed types fall back to string ordering ("10" < "2" lexicographically).
    let mixed = vec![num(10.0), str_val("2")];
    assert_eq!(eval_aggregate_over_values("min", &mixed), Some(num(10.0)));
    assert_eq!(eval_aggregate_over_values("bogus", &vals), None);

    // numeric_values / sum_numeric_values filter non-numbers.
    assert_eq!(
        numeric_values(&[num(1.0), str_val("x"), num(2.0)]),
        vec![1.0, 2.0]
    );
    assert_eq!(sum_numeric_values(&[num(1.0), str_val("x"), num(2.0)]), 3.0);
}
