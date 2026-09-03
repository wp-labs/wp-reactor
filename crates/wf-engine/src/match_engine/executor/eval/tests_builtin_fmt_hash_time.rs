//! builtins.rs 直接求值（二）（2026-09-04 自 tests.rs 拆出）：fmt/concat/join/indexof/
//! replace_plain、null/blank/coalesce、hash/id、now 变体、strftime/strptime、regex/time
//! 内建与错误分支。

use super::*;

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
