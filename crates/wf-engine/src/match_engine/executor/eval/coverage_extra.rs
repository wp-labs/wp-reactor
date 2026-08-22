//! Coverage-fill tests for `eval/builtins.rs` — the error / boundary branches
//! of the builtin function dispatch that the happy-path battery in `tests.rs`
//! does not reach: wrong argument counts, wrong value types (`_ => None`
//! arms), out-of-range indices, invalid inputs, and selector parsing.
//!
//! Everything goes through `eval_expr_with_l3` (the same interpreter entry the
//! executor's yield/score/where evaluation uses), with direct calls into the
//! stat/l3/aggregate helpers for the branches they own.

use super::builtins::{
    eval_aggregate_func, eval_aggregate_over_numbers, eval_aggregate_over_values, eval_l3_func,
    eval_stat_func, is_stat_selector_func, numeric_values, sum_numeric_values,
};
use super::{Event, Value, YieldMeta, eval_bool_expr, eval_expr_with_l3, eval_yield_expr};
use crate::match_engine::EngineHashMap;
use wf_lang::ast::{Expr, FieldRef};

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

fn l3_ctx(expr: &Expr, ctx: &Event) -> Option<Value> {
    eval_expr_with_l3(expr, ctx, YieldMeta::default())
}

/// Assert that `name` returns `None` for this exact arg list (arg-count guard
/// or a type-mismatch arm).
fn assert_none(name: &str, args: Vec<Expr>, ctx: &Event) {
    assert_eq!(
        l3_ctx(&call(name, args), ctx),
        None,
        "expected {name} to return None"
    );
}

#[test]
fn builtin_error_arg_count_guards() {
    let ctx = ctx_with(vec![
        ("s", str_val("abc")),
        ("n", num(3.0)),
        ("arr", arr_1_2_3()),
    ]);
    // Two-arg funcs with zero/one/three args. (`replace` excluded from the
    // 3-arg loop: a 3-arg call with valid strings succeeds.)
    for name in [
        "contains",
        "startswith",
        "endswith",
        "mvjoin",
        "split",
        "indexof",
        "pow",
        "regex_match",
        "time_diff",
        "time_bucket",
        "bucket_end",
        "strptime",
    ] {
        assert_none(name, vec![], &ctx);
        assert_none(name, vec![field("s")], &ctx);
        assert_none(name, vec![field("s"), field("s"), field("s")], &ctx);
    }
    // replace / replace_plain need exactly 3 args.
    assert_none("replace", vec![], &ctx);
    assert_none("replace", vec![field("s"), field("s")], &ctx);
    assert_none("replace_plain", vec![field("s"), field("s")], &ctx);
    // One-arg funcs with wrong counts.
    for name in [
        "trim",
        "lower",
        "upper",
        "len",
        "mvcount",
        "mvdedup",
        "abs",
        "ceil",
        "floor",
        "sqrt",
        "exp",
        "sign",
        "trunc",
        "is_finite",
        "ltrim",
        "rtrim",
        "mvsort",
        "mvreverse",
        "isnull",
        "isnotnull",
        "is_blank",
        "null_if_blank",
        "md5",
        "sha1",
        "sha256",
        "hex",
    ] {
        assert_none(name, vec![], &ctx);
        assert_none(name, vec![field("s"), field("s")], &ctx);
    }
    // merge / mvappend / concat / join / coalesce reject empty arg lists.
    for name in ["merge", "mvappend", "concat", "join", "coalesce", "fmt"] {
        assert_none(name, vec![], &ctx);
    }
    // join_by requires ≥2.
    assert_none("join_by", vec![], &ctx);
    assert_none("join_by", vec![field("s")], &ctx);
    // startswith_any / endswith_any require ≥2.
    assert_none("startswith_any", vec![field("s")], &ctx);
    assert_none("endswith_any", vec![field("s")], &ctx);
    // clamp needs exactly 3.
    assert_none("clamp", vec![field("n"), field("n")], &ctx);
    assert_none("clamp", vec![field("n")], &ctx);
    // sha1_n needs exactly 2.
    assert_none("sha1_n", vec![field("s")], &ctx);
    assert_none(
        "sha1_n",
        vec![field("s"), num_expr(8.0), num_expr(8.0)],
        &ctx,
    );
    // round: 1 or 2 args.
    assert_none("round", vec![], &ctx);
    assert_none("round", vec![field("n"), field("n"), field("n")], &ctx);
    // substr: 2 or 3 args.
    assert_none("substr", vec![field("s")], &ctx);
    assert_none(
        "substr",
        vec![field("s"), field("n"), field("n"), field("n")],
        &ctx,
    );
    // mvindex: 2 or 3 args.
    assert_none("mvindex", vec![field("arr")], &ctx);
    assert_none(
        "mvindex",
        vec![field("arr"), field("n"), field("n"), field("n")],
        &ctx,
    );
    // now family reject args.
    for name in ["now", "now_ms", "now_s", "now_us", "now_ns"] {
        assert_none(name, vec![field("n")], &ctx);
    }
    // stable_id needs ≥2.
    assert_none("stable_id", vec![field("s")], &ctx);
}

fn str_val(s: &str) -> Value {
    Value::Str(s.to_string().into())
}

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn arr_1_2_3() -> Value {
    Value::Array(vec![num(1.0), num(2.0), num(3.0)])
}

#[test]
fn builtin_error_type_mismatches() {
    let ctx = ctx_with(vec![
        ("s", str_val("abc")),
        ("n", num(3.0)),
        ("b", Value::Bool(true)),
        ("arr", arr_1_2_3()),
    ]);
    // String-typed args fed a number / bool → None.
    for name in [
        "contains",
        "startswith",
        "endswith",
        "trim",
        "lower",
        "upper",
        "ltrim",
        "rtrim",
        "split",
        "indexof",
        "replace",
        "replace_plain",
        "regex_match",
        "strptime",
    ] {
        assert_none(name, vec![field("n"), field("n")], &ctx);
        assert_none(name, vec![field("b")], &ctx);
    }
    // Number-typed args fed a string / bool → None.
    for name in [
        "abs",
        "ceil",
        "floor",
        "sqrt",
        "sign",
        "trunc",
        "is_finite",
        "exp",
    ] {
        assert_none(name, vec![field("s")], &ctx);
        assert_none(name, vec![field("b")], &ctx);
    }
    // len of a non-string → None.
    assert_none("len", vec![field("n")], &ctx);
    // mvcount of a non-array → None.
    assert_none("mvcount", vec![field("s")], &ctx);
    // mvjoin with a non-array / non-string separator → None.
    assert_none("mvjoin", vec![field("s"), field("s")], &ctx);
    assert_none("mvjoin", vec![field("arr"), field("n")], &ctx);
    // mvdedup / mvsort / mvreverse on non-array → None.
    for name in ["mvdedup", "mvsort", "mvreverse"] {
        assert_none(name, vec![field("s")], &ctx);
    }
    // sqrt of negative → None.
    assert_none("sqrt", vec![num_expr(-1.0)], &ctx);
    // log of non-positive → None.
    assert_none("log", vec![num_expr(0.0)], &ctx);
    assert_none("log", vec![num_expr(-5.0)], &ctx);
    // log with invalid base (0 / 1) → None.
    assert_none("log", vec![num_expr(2.0), num_expr(0.0)], &ctx);
    assert_none("log", vec![num_expr(2.0), num_expr(1.0)], &ctx);
    // clamp with min > max → None.
    assert_none(
        "clamp",
        vec![num_expr(5.0), num_expr(10.0), num_expr(1.0)],
        &ctx,
    );
    // pow overflow → None.
    assert_none("pow", vec![num_expr(1e300), num_expr(1e300)], &ctx);
    // exp overflow → None.
    assert_none("exp", vec![num_expr(1000.0)], &ctx);
    // round with a non-finite value → None.
    assert_none("round", vec![num_expr(f64::NAN)], &ctx);
    assert_none("round", vec![num_expr(f64::INFINITY)], &ctx);
    // round precision out of i64 range → None.
    assert_none("round", vec![num_expr(1.5), num_expr(1e300)], &ctx);
    // sign of NaN → None.
    assert_none("sign", vec![num_expr(f64::NAN)], &ctx);
    // replace with an invalid regex → None.
    assert_none("replace", vec![field("s"), lit("("), lit("x")], &ctx);
    // regex_match with an invalid regex → None.
    assert_none("regex_match", vec![field("s"), lit("[")], &ctx);
    // merge with a non-object literal → None.
    assert_none("merge", vec![num_expr(1.0)], &ctx);
    // mvindex with a non-array / non-number → None.
    assert_none("mvindex", vec![field("s"), field("n")], &ctx);
    assert_none("mvindex", vec![field("arr"), field("s")], &ctx);
    // fmt with a non-string template → None.
    assert_none("fmt", vec![field("n")], &ctx);
    // fmt with mismatched placeholder count → None.
    assert_none("fmt", vec![lit("{} {}"), field("s")], &ctx);
    // join_by with a non-string separator → None.
    assert_none("join_by", vec![field("n"), field("s")], &ctx);
    // default_if_blank with a non-string default (primary blank → default is
    // evaluated and is not a string → None).
    assert_none("default_if_blank", vec![lit(" "), field("n")], &ctx);
    // null_if_blank with a number → None.
    assert_none("null_if_blank", vec![field("n")], &ctx);
    // is_blank with a non-string non-missing value → None.
    assert_none("is_blank", vec![field("n")], &ctx);
    // stable_id with a non-string prefix → None.
    assert_none("stable_id", vec![field("n"), field("s")], &ctx);
    // stable_id with an array arg → None.
    assert_none("stable_id", vec![field("s"), field("arr")], &ctx);
    // sha1_n with a non-string / non-integer length / out-of-range length → None.
    assert_none("sha1_n", vec![field("n"), field("n")], &ctx);
    assert_none("sha1_n", vec![field("s"), field("s")], &ctx);
    assert_none("sha1_n", vec![field("s"), num_expr(2.5)], &ctx);
    assert_none("sha1_n", vec![field("s"), num_expr(0.0)], &ctx);
    assert_none("sha1_n", vec![field("s"), num_expr(41.0)], &ctx);
    // strftime with a non-numeric timestamp → None.
    assert_none("strftime", vec![field("s")], &ctx);
    assert_none("strftime", vec![field("n"), field("n")], &ctx);
    // strftime with an unparseable timestamp → None.
    assert_none("strftime", vec![num_expr(1e300)], &ctx);
    // strptime parse failure → None.
    assert_none("strptime", vec![lit("not a date"), lit("%Y")], &ctx);
    // time_diff with non-numeric args → None.
    assert_none("time_diff", vec![field("s"), field("n")], &ctx);
    // time_bucket with a non-numeric ts / interval / invalid interval → None.
    assert_none("time_bucket", vec![field("s"), field("n")], &ctx);
    assert_none("time_bucket", vec![field("n"), field("s")], &ctx);
    assert_none("time_bucket", vec![field("n"), num_expr(0.0)], &ctx);
    assert_none("time_bucket", vec![field("n"), num_expr(-1.0)], &ctx);
    // bucket_end with invalid interval → None.
    assert_none("bucket_end", vec![field("n"), num_expr(0.0)], &ctx);
    // time_bucket with an unparseable ts → None.
    assert_none("time_bucket", vec![num_expr(1e300), num_expr(60.0)], &ctx);
}

#[test]
fn builtin_edge_branches() {
    // substr negative / past-end / zero/negative length.
    let ctx = ctx_with(vec![("s", str_val("hello"))]);
    assert_eq!(
        l3_ctx(&call("substr", vec![field("s"), num_expr(-2.0)]), &ctx),
        Some(str_val("lo"))
    );
    assert_eq!(
        l3_ctx(&call("substr", vec![field("s"), num_expr(99.0)]), &ctx),
        Some(str_val(""))
    );
    assert_eq!(
        l3_ctx(
            &call("substr", vec![field("s"), num_expr(1.0), num_expr(0.0)]),
            &ctx
        ),
        Some(str_val(""))
    );
    assert_eq!(
        l3_ctx(
            &call("substr", vec![field("s"), num_expr(2.0), num_expr(10.0)]),
            &ctx
        ),
        Some(str_val("ello"))
    );
    // substr index 0 → start_idx clamps to 0 → the whole string.
    assert_eq!(
        l3_ctx(&call("substr", vec![field("s"), num_expr(0.0)]), &ctx),
        Some(str_val("hello"))
    );

    // split with an empty separator splits per char.
    assert_eq!(
        l3_ctx(&call("split", vec![lit("ab"), lit("")]), &ctx),
        Some(Value::Array(vec![str_val("a"), str_val("b")]))
    );

    // mvindex: out-of-range single index → None; empty range → empty array.
    let ctx = ctx_with(vec![("arr", arr_1_2_3())]);
    assert_none("mvindex", vec![field("arr"), num_expr(99.0)], &ctx);
    assert_none("mvindex", vec![field("arr"), num_expr(-99.0)], &ctx);
    assert_eq!(
        l3_ctx(&call("mvindex", vec![field("arr"), num_expr(2.0)]), &ctx),
        Some(num(3.0))
    );
    // Range variants.
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("arr"), num_expr(0.0), num_expr(1.0)]),
            &ctx
        ),
        Some(Value::Array(vec![num(1.0), num(2.0)]))
    );
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("arr"), num_expr(1.0), num_expr(-1.0)]),
            &ctx
        ),
        Some(Value::Array(vec![num(2.0), num(3.0)]))
    );
    // start >= len → empty.
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("arr"), num_expr(5.0), num_expr(6.0)]),
            &ctx
        ),
        Some(Value::Array(vec![]))
    );
    // end < 0 → empty.
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("arr"), num_expr(0.0), num_expr(-9.0)]),
            &ctx
        ),
        Some(Value::Array(vec![]))
    );
    // start > end → empty.
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("arr"), num_expr(2.0), num_expr(1.0)]),
            &ctx
        ),
        Some(Value::Array(vec![]))
    );
    // Empty array + range → empty.
    let ctx = ctx_with(vec![("e", Value::Array(vec![]))]);
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("e"), num_expr(0.0), num_expr(1.0)]),
            &ctx
        ),
        Some(Value::Array(vec![]))
    );

    // mvappend: scalar and array args interleave.
    let ctx = ctx_with(vec![("arr", arr_1_2_3())]);
    assert_eq!(
        l3_ctx(
            &call("mvappend", vec![num_expr(0.0), field("arr"), num_expr(9.0)]),
            &ctx
        ),
        Some(Value::Array(vec![
            num(0.0),
            num(1.0),
            num(2.0),
            num(3.0),
            num(9.0)
        ]))
    );

    // mvdedup with duplicates (incl. value equality across types).
    let ctx = ctx_with(vec![(
        "d",
        Value::Array(vec![num(1.0), num(1.0), str_val("a"), str_val("a")]),
    )]);
    assert_eq!(
        l3_ctx(&call("mvdedup", vec![field("d")]), &ctx),
        Some(Value::Array(vec![num(1.0), str_val("a")]))
    );

    // coalesce skips blank strings and None.
    let ctx = ctx_with(vec![("blank", str_val("  ")), ("v", str_val("x"))]);
    assert_eq!(
        l3_ctx(
            &call(
                "coalesce",
                vec![field("missing"), field("blank"), field("v")]
            ),
            &ctx
        ),
        Some(str_val("x"))
    );
    // coalesce all-none → None.
    let ctx = ctx_with(vec![]);
    assert_none("coalesce", vec![field("missing")], &ctx);

    // isnull / isnotnull on a missing field.
    let ctx = ctx_with(vec![]);
    assert_eq!(
        l3_ctx(&call("isnull", vec![field("missing")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("isnotnull", vec![field("missing")]), &ctx),
        Some(Value::Bool(false))
    );

    // is_blank: None → true; blank → true; non-blank → false.
    let ctx = ctx_with(vec![("blank", str_val(" \t ")), ("v", str_val("x"))]);
    assert_eq!(
        l3_ctx(&call("is_blank", vec![field("missing")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("is_blank", vec![field("blank")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("is_blank", vec![field("v")]), &ctx),
        Some(Value::Bool(false))
    );

    // null_if_blank: blank → None, non-blank → string.
    assert_none("null_if_blank", vec![field("blank")], &ctx);
    assert_eq!(
        l3_ctx(&call("null_if_blank", vec![field("v")]), &ctx),
        Some(str_val("x"))
    );

    // default_if_blank: blank → default; missing → default; present → value.
    assert_eq!(
        l3_ctx(
            &call("default_if_blank", vec![field("blank"), lit("d")]),
            &ctx
        ),
        Some(str_val("d"))
    );
    assert_eq!(
        l3_ctx(
            &call("default_if_blank", vec![field("missing"), lit("d")]),
            &ctx
        ),
        Some(str_val("d"))
    );
    assert_eq!(
        l3_ctx(&call("default_if_blank", vec![field("v"), lit("d")]), &ctx),
        Some(str_val("x"))
    );

    // indexof: hit and miss.
    let ctx = ctx_with(vec![("s", str_val("a-b-c"))]);
    assert_eq!(
        l3_ctx(&call("indexof", vec![field("s"), lit("b")]), &ctx),
        Some(num(2.0))
    );
    assert_eq!(
        l3_ctx(&call("indexof", vec![field("s"), lit("z")]), &ctx),
        Some(num(-1.0))
    );

    // startswith_any / endswith_any: hit, miss, and a failing arg type.
    let ctx = ctx_with(vec![("s", str_val("10.0.0.1"))]);
    assert_eq!(
        l3_ctx(
            &call("startswith_any", vec![field("s"), lit("8."), lit("10.")]),
            &ctx
        ),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("startswith_any", vec![field("s"), lit("8.")]), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(
            &call("endswith_any", vec![field("s"), lit(".1"), lit(".9")]),
            &ctx
        ),
        Some(Value::Bool(true))
    );
    assert_none("startswith_any", vec![field("s"), num_expr(1.0)], &ctx);

    // merge with a missing field arg (skip) and a non-object non-field arg.
    let ctx = ctx_with(vec![(
        "o",
        Value::Object(EngineHashMap::from_iter([("k".into(), num(1.0))])),
    )]);
    assert_eq!(
        l3_ctx(&call("merge", vec![field("missing"), field("o")]), &ctx),
        Some(Value::Object(EngineHashMap::from_iter([(
            "k".into(),
            num(1.0)
        )])))
    );

    // mvjoin renders non-string values via value_to_string.
    let ctx = ctx_with(vec![(
        "mixed",
        Value::Array(vec![num(1.5), Value::Bool(true)]),
    )]);
    assert_eq!(
        l3_ctx(&call("mvjoin", vec![field("mixed"), lit("|")]), &ctx),
        Some(str_val("1.5|true"))
    );

    // round with precision 0 (default) and 2.
    assert_eq!(
        l3_ctx(&call("round", vec![num_expr(1.234)]), &ctx_with(vec![])),
        Some(num(1.0))
    );
    assert_eq!(
        l3_ctx(
            &call("round", vec![num_expr(1.234), num_expr(2.0)]),
            &ctx_with(vec![])
        ),
        Some(num(1.23))
    );

    // hash funcs on empty string.
    assert_eq!(
        l3_ctx(&call("md5", vec![lit("")]), &ctx_with(vec![])),
        Some(str_val("d41d8cd98f00b204e9800998ecf8427e"))
    );
    // hex of bytes.
    assert_eq!(
        l3_ctx(&call("hex", vec![lit("ab")]), &ctx_with(vec![])),
        Some(str_val("6162"))
    );
}

#[test]
fn builtin_stat_selector_and_stat_func() {
    assert!(is_stat_selector_func("window_event"));
    assert!(is_stat_selector_func("match_event"));
    assert!(is_stat_selector_func("match_distinct"));
    assert!(is_stat_selector_func("trigger"));
    assert!(is_stat_selector_func("final"));
    assert!(!is_stat_selector_func("count"));
    assert!(!is_stat_selector_func(""));

    let select = |name: &str, sym: &str| Expr::FuncCall {
        qualifier: None,
        name: name.to_string(),
        args: vec![Expr::Field(FieldRef::Simple(sym.to_string()))],
    };
    // Wrong arg count → None.
    assert!(eval_stat_func("count", &[], &ctx_with(vec![])).is_none());
    assert!(eval_stat_func("count", &[field("a"), field("b")], &ctx_with(vec![])).is_none());
    // Non-selector first arg → None.
    assert!(eval_stat_func("count", &[field("x")], &ctx_with(vec![])).is_none());
    // Selector with a non-simple field → None.
    assert!(
        eval_stat_func(
            "count",
            &[Expr::FuncCall {
                qualifier: None,
                name: "window_event".into(),
                args: vec![Expr::Field(FieldRef::Qualified("w".into(), "x".into()))],
            }],
            &ctx_with(vec![])
        )
        .is_none()
    );
    // Unknown selector function name → None.
    assert!(
        eval_stat_func(
            "count",
            &[Expr::FuncCall {
                qualifier: None,
                name: "bogus".into(),
                args: vec![field("x")],
            }],
            &ctx_with(vec![])
        )
        .is_none()
    );
    // count(window_event(alias)) reads `_bind_<alias>_count`.
    let ctx = ctx_with(vec![("_bind_w_count", num(7.0))]);
    assert_eq!(
        eval_stat_func("count", &[select("window_event", "w")], &ctx),
        Some(num(7.0))
    );
    // count with a non-number bind count → None.
    let ctx = ctx_with(vec![("_bind_w_count", str_val("x"))]);
    assert!(eval_stat_func("count", &[select("window_event", "w")], &ctx).is_none());
    // count(match_event(label)) / count(match_distinct(label)) read the label.
    let ctx = ctx_with(vec![("login", num(4.0))]);
    assert_eq!(
        eval_stat_func("count", &[select("match_event", "login")], &ctx),
        Some(num(4.0))
    );
    assert_eq!(
        eval_stat_func("count", &[select("match_distinct", "login")], &ctx),
        Some(num(4.0))
    );
    // value(trigger/final(label)) read the label.
    assert_eq!(
        eval_stat_func("value", &[select("trigger", "login")], &ctx),
        Some(num(4.0))
    );
    assert_eq!(
        eval_stat_func("value", &[select("final", "login")], &ctx),
        Some(num(4.0))
    );
    // Unsupported name/selector combo → None.
    assert!(eval_stat_func("value", &[select("window_event", "w")], &ctx).is_none());
    // Non-number label value → None.
    let ctx = ctx_with(vec![("login", str_val("x"))]);
    assert!(eval_stat_func("value", &[select("final", "login")], &ctx).is_none());
}

fn step_ctx_with(values: Vec<Value>, source: &str, label: Option<&str>, stage: &str) -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("_step_0_values".into(), Value::Array(values));
    fields.insert("_step_0_source".into(), str_val(source));
    if let Some(l) = label {
        fields.insert("_step_0_label".into(), str_val(l));
    }
    fields.insert("_step_0_stage".into(), str_val(stage));
    Event { fields }
}

#[test]
fn builtin_l3_and_aggregate_error_branches() {
    // Empty args → None.
    assert!(eval_l3_func("collect_set", &[], &ctx_with(vec![]), YieldMeta::default()).is_none());
    // Wrong arg count for collect_set / collect_list / first / last / stddev.
    let ctx = step_ctx_with(vec![num(1.0), num(2.0)], "e", None, "event");
    let two = vec![field("x"), field("y")];
    assert!(eval_l3_func("collect_set", &two, &ctx, YieldMeta::default()).is_none());
    assert!(eval_l3_func("collect_list", &two, &ctx, YieldMeta::default()).is_none());
    assert!(eval_l3_func("first", &two, &ctx, YieldMeta::default()).is_none());
    assert!(eval_l3_func("last", &two, &ctx, YieldMeta::default()).is_none());
    assert!(eval_l3_func("stddev", &two, &ctx, YieldMeta::default()).is_none());
    // percentile needs 2 args.
    assert!(eval_l3_func("percentile", &[field("x")], &ctx, YieldMeta::default()).is_none());
    assert!(
        eval_l3_func(
            "percentile",
            &[field("x"), field("y"), field("z")],
            &ctx,
            YieldMeta::default()
        )
        .is_none()
    );
    // percentile with a non-number p → None.
    assert!(
        eval_l3_func(
            "percentile",
            &[field("x"), field("y")],
            &ctx,
            YieldMeta::default()
        )
        .is_none()
    );
    // percentile with empty values → 0.
    let empty = ctx_with(vec![]);
    assert_eq!(
        eval_l3_func(
            "percentile",
            &[field("x"), num_expr(50.0)],
            &empty,
            YieldMeta::default()
        ),
        Some(num(0.0))
    );
    // stddev with <2 numbers → 0.
    let single = step_ctx_with(vec![num(5.0)], "e", None, "event");
    assert_eq!(
        eval_l3_func("stddev", &[field("x")], &single, YieldMeta::default()),
        Some(num(0.0))
    );
    // Unknown l3 name → None.
    assert!(eval_l3_func("bogus", &[field("x")], &ctx, YieldMeta::default()).is_none());

    // eval_aggregate_func: wrong arg count → None.
    assert!(eval_aggregate_func("sum", &[], &ctx).is_none());
    // Non-field arg → None.
    assert!(eval_aggregate_func("sum", &[num_expr(1.0)], &ctx).is_none());
    // Simple field with no step data / bind data → None.
    assert!(eval_aggregate_func("sum", &[field("x")], &ctx_with(vec![])).is_none());
    // Qualified field with no data → None.
    assert!(
        eval_aggregate_func(
            "sum",
            &[Expr::Field(FieldRef::Qualified("w".into(), "x".into()))],
            &ctx_with(vec![])
        )
        .is_none()
    );
    // count(bind alias) reads `_bind_<alias>_count`.
    let bind_ctx = ctx_with(vec![("_bind_w_count", num(6.0))]);
    assert_eq!(
        eval_aggregate_func("count", &[field("w")], &bind_ctx),
        Some(num(6.0))
    );

    // eval_aggregate_over_numbers: all aggregates + unknown.
    assert_eq!(
        eval_aggregate_over_numbers("count", &[1.0, 2.0]),
        Some(num(3.0))
    );
    assert_eq!(
        eval_aggregate_over_numbers("sum", &[1.0, 2.0, 3.0]),
        Some(num(6.0))
    );
    assert_eq!(eval_aggregate_over_numbers("avg", &[]), Some(num(0.0)));
    assert_eq!(
        eval_aggregate_over_numbers("avg", &[2.0, 4.0]),
        Some(num(3.0))
    );
    assert_eq!(eval_aggregate_over_numbers("min", &[]), Some(num(0.0)));
    assert_eq!(
        eval_aggregate_over_numbers("min", &[5.0, 1.0, 3.0]),
        Some(num(1.0))
    );
    assert_eq!(eval_aggregate_over_numbers("max", &[]), Some(num(0.0)));
    assert_eq!(
        eval_aggregate_over_numbers("max", &[5.0, 1.0, 3.0]),
        Some(num(5.0))
    );
    assert_eq!(eval_aggregate_over_numbers("bogus", &[1.0]), None);

    // eval_aggregate_over_values: count/sum/avg/min/max + unknown; empty min/max.
    let vals = vec![num(1.0), num(2.0), str_val("x")];
    assert_eq!(eval_aggregate_over_values("count", &vals), Some(num(3.0)));
    assert_eq!(eval_aggregate_over_values("sum", &vals), Some(num(3.0)));
    assert_eq!(eval_aggregate_over_values("avg", &vals), Some(num(1.5)));
    assert_eq!(eval_aggregate_over_values("avg", &[]), Some(num(0.0)));
    assert_eq!(eval_aggregate_over_values("min", &vals), Some(num(1.0)));
    assert_eq!(eval_aggregate_over_values("max", &vals), Some(str_val("x")));
    assert_eq!(eval_aggregate_over_values("min", &[]), None);
    assert_eq!(eval_aggregate_over_values("max", &[]), None);
    assert_eq!(eval_aggregate_over_values("bogus", &vals), None);

    // numeric_values / sum_numeric_values filter out non-numbers.
    assert_eq!(numeric_values(&vals), vec![1.0, 2.0]);
    assert_eq!(sum_numeric_values(&vals), 3.0);
}

#[test]
fn eval_bool_expr_and_yield_fallback_branches() {
    // Non-bool expression → None.
    let ctx = ctx_with(vec![("n", num(3.0))]);
    assert!(eval_bool_expr(&field("n"), &ctx).is_none());
    assert!(eval_bool_expr(&num_expr(1.0), &ctx).is_none());
    // Bool literal works.
    assert_eq!(eval_bool_expr(&Expr::Bool(true), &ctx), Some(true));

    // eval_yield_expr: missing field falls back to empty string.
    let ctx = ctx_with(vec![]);
    assert_eq!(eval_yield_expr(&field("missing"), &ctx), Some(str_val("")));
}

#[test]
fn builtin_misc_math_and_time() {
    // abs of negative; is_finite on inf.
    assert_eq!(
        l3_ctx(&call("abs", vec![num_expr(-3.5)]), &ctx_with(vec![])),
        Some(num(3.5))
    );
    assert_eq!(
        l3_ctx(
            &call("is_finite", vec![num_expr(f64::INFINITY)]),
            &ctx_with(vec![])
        ),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&call("is_finite", vec![num_expr(1.0)]), &ctx_with(vec![])),
        Some(Value::Bool(true))
    );
    // log with 2 args.
    match l3_ctx(
        &call("log", vec![num_expr(8.0), num_expr(2.0)]),
        &ctx_with(vec![]),
    ) {
        Some(Value::Number(n)) => assert!((n - 3.0).abs() < 1e-9),
        other => panic!("expected log(8, 2) = 3, got {other:?}"),
    }
    // ceil / floor / trunc.
    assert_eq!(
        l3_ctx(&call("ceil", vec![num_expr(1.2)]), &ctx_with(vec![])),
        Some(num(2.0))
    );
    assert_eq!(
        l3_ctx(&call("floor", vec![num_expr(1.8)]), &ctx_with(vec![])),
        Some(num(1.0))
    );
    assert_eq!(
        l3_ctx(&call("trunc", vec![num_expr(1.8)]), &ctx_with(vec![])),
        Some(num(1.0))
    );

    // time_diff across an epoch.
    let t1 = 1_700_000_000_000_000_000f64;
    assert_eq!(
        l3_ctx(
            &call(
                "time_diff",
                vec![num_expr(t1), num_expr(t1 + 5_000_000_000.0)]
            ),
            &ctx_with(vec![])
        ),
        Some(num(5.0))
    );

    // time_bucket: buckets into a 60s interval. 1.8e18 ns = 30_000_000 × 60s;
    // the returned Value is epoch **millis** (time_nanos_to_value).
    let t = 1_800_000_000_000_000_000f64;
    let bucketed = l3_ctx(
        &call("time_bucket", vec![num_expr(t), num_expr(60.0)]),
        &ctx_with(vec![]),
    )
    .unwrap();
    assert_eq!(bucketed, num(1_800_000_000_000.0));
    // bucket_end = bucket + interval (in ms: +60_000).
    let end = l3_ctx(
        &call("bucket_end", vec![num_expr(t), num_expr(60.0)]),
        &ctx_with(vec![]),
    )
    .unwrap();
    assert_eq!(end, num(1_800_000_060_000.0));

    // strftime with explicit format and default format.
    let ts = 1_700_000_000_000_000_000f64;
    let formatted = l3_ctx(
        &call("strftime", vec![num_expr(ts), lit("%Y")]),
        &ctx_with(vec![]),
    )
    .unwrap();
    assert_eq!(formatted, str_val("2023"));
    let formatted = l3_ctx(&call("strftime", vec![num_expr(ts)]), &ctx_with(vec![])).unwrap();
    assert!(matches!(formatted, Value::Str(_)));

    // strptime round-trips an RFC3339-ish string.
    let parsed = l3_ctx(
        &call(
            "strptime",
            vec![lit("2023-11-14 22:14:20"), lit("%Y-%m-%d %H:%M:%S")],
        ),
        &ctx_with(vec![]),
    );
    assert!(parsed.is_some());
}
