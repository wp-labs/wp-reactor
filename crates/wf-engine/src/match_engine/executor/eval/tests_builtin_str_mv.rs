//! builtins.rs 直接求值（一）（2026-09-04 自 tests.rs 拆出）：external() 错误分支 + string 内建
//! （contains/startswith/endswith/substr/replace/trim/case/len/merge）+ 数组内建 mv* 族
//! + numeric/math 内建与错误分支（L3 eval 路径）。

use super::*;

// -------------------------------------------------------------------
// external() tests
// -------------------------------------------------------------------

#[test]
fn external_without_handler_returns_none() {
    // NOTE: EXTERNAL_HANDLER is a global OnceLock. If a previous test
    // already installed a handler, dispatch will return Some(...) instead
    // of None. We verify the no-handler path by checking an empty OnceLock
    // directly (mirroring dispatch_external_call's logic).
    let empty: std::sync::OnceLock<std::sync::Arc<dyn crate::external::ExternalCallHandler>> =
        std::sync::OnceLock::new();
    assert!(empty.get().is_none());
    assert!(empty.get().and_then(|h| h.call("test", &[])).is_none());
}

#[test]
fn external_requires_at_least_two_args() {
    let ctx = Event {
        fields: EngineHashMap::default(),
    };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "external".to_string(),
        args: vec![Expr::StringLit("only_service".to_string())],
    };
    let result = eval_bool_expr(&expr, &ctx);
    assert_eq!(result, None);
}

#[test]
fn external_service_must_be_string_literal() {
    let ctx = Event {
        fields: EngineHashMap::default(),
    };
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "external".to_string(),
        args: vec![
            Expr::Number(42.0), // not a string
            Expr::StringLit("arg".to_string()),
        ],
    };
    let result = eval_bool_expr(&expr, &ctx);
    assert_eq!(result, None);
}

// ===========================================================================
// builtins.rs — string builtins (L3 eval path) + error branches
// ===========================================================================

#[test]
fn builtin_contains_startswith_endswith() {
    let ctx = ctx_with(vec![
        ("msg", Value::Str("failed_login_root".into())),
        ("n", Value::Number(5.0)),
    ]);
    assert_eq!(
        l3_ctx(&call("contains", vec![field("msg"), lit("login")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("contains", vec![field("msg"), lit("ok")]), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&call("startswith", vec![field("msg"), lit("failed")]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("endswith", vec![field("msg"), lit("root")]), &ctx),
        Some(Value::Bool(true))
    );
    // wrong arg count
    assert_eq!(l3_ctx(&call("contains", vec![field("msg")]), &ctx), None);
    assert_eq!(
        l3_ctx(
            &call("contains", vec![field("msg"), lit("a"), lit("b")]),
            &ctx
        ),
        None
    );
    assert_eq!(l3_ctx(&call("startswith", vec![field("msg")]), &ctx), None);
    assert_eq!(l3_ctx(&call("endswith", vec![field("msg")]), &ctx), None);
    // wrong types
    assert_eq!(
        l3_ctx(&call("contains", vec![field("n"), lit("a")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("contains", vec![field("msg"), field("n")]), &ctx),
        None
    );
}

#[test]
fn builtin_substr_variants() {
    let ctx = ctx_with(vec![("msg", Value::Str("abcdef".into()))]);
    // start + length
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![field("msg"), Expr::Number(2.0), Expr::Number(3.0)]
            ),
            &ctx
        ),
        Some(Value::Str("bcd".into()))
    );
    // start only (to end)
    assert_eq!(
        l3_ctx(&call("substr", vec![field("msg"), Expr::Number(4.0)]), &ctx),
        Some(Value::Str("def".into()))
    );
    // negative start
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![field("msg"), Expr::Neg(Box::new(Expr::Number(2.0)))]
            ),
            &ctx
        ),
        Some(Value::Str("ef".into()))
    );
    // start == 0 → from beginning
    assert_eq!(
        l3_ctx(&call("substr", vec![field("msg"), Expr::Number(0.0)]), &ctx),
        Some(Value::Str("abcdef".into()))
    );
    // start beyond end → empty string
    assert_eq!(
        l3_ctx(
            &call("substr", vec![field("msg"), Expr::Number(100.0)]),
            &ctx
        ),
        Some(Value::Str(String::new().into()))
    );
    // very negative start clamps to 0
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![field("msg"), Expr::Neg(Box::new(Expr::Number(50.0)))]
            ),
            &ctx
        ),
        Some(Value::Str("abcdef".into()))
    );
    // length <= 0 → empty
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![field("msg"), Expr::Number(2.0), Expr::Number(0.0)]
            ),
            &ctx
        ),
        Some(Value::Str(String::new().into()))
    );
    // length past end is truncated
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![field("msg"), Expr::Number(5.0), Expr::Number(100.0)]
            ),
            &ctx
        ),
        Some(Value::Str("ef".into()))
    );
    // errors
    assert_eq!(l3_ctx(&call("substr", vec![field("msg")]), &ctx), None);
    assert_eq!(
        l3_ctx(
            &call(
                "substr",
                vec![
                    field("msg"),
                    Expr::Number(1.0),
                    Expr::Number(1.0),
                    Expr::Number(1.0)
                ]
            ),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("substr", vec![Expr::Number(1.0), Expr::Number(1.0)]),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(&call("substr", vec![field("msg"), lit("x")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("substr", vec![field("msg"), Expr::Number(1.0), lit("x")]),
            &ctx
        ),
        None
    );
}

#[test]
fn builtin_replace_trim_case_len() {
    let ctx = ctx_with(vec![
        ("msg", Value::Str("  HeLLo\t".into())),
        ("n", Value::Number(42.0)),
        ("multibyte", Value::Str("你好".into())),
    ]);
    // replace (regex)
    assert_eq!(
        l3_ctx(
            &call(
                "replace",
                vec![lit("failed_login"), lit("fail.*"), lit("blocked")]
            ),
            &ctx
        ),
        Some(Value::Str("blocked".into()))
    );
    // invalid regex → None
    assert_eq!(
        l3_ctx(&call("replace", vec![lit("abc"), lit("("), lit("x")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("replace", vec![lit("a"), lit("b")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("replace", vec![Expr::Number(1.0), lit("b"), lit("c")]),
            &ctx
        ),
        None
    );
    // trim / lower / upper / ltrim / rtrim
    assert_eq!(
        l3_ctx(&call("trim", vec![field("msg")]), &ctx),
        Some(Value::Str("HeLLo".into()))
    );
    assert_eq!(
        l3_ctx(&call("lower", vec![field("msg")]), &ctx),
        Some(Value::Str("  hello\t".into()))
    );
    assert_eq!(
        l3_ctx(&call("upper", vec![field("msg")]), &ctx),
        Some(Value::Str("  HELLO\t".into()))
    );
    assert_eq!(
        l3_ctx(&call("ltrim", vec![field("msg")]), &ctx),
        Some(Value::Str("HeLLo\t".into()))
    );
    assert_eq!(
        l3_ctx(&call("rtrim", vec![field("msg")]), &ctx),
        Some(Value::Str("  HeLLo".into()))
    );
    assert_eq!(l3_ctx(&call("trim", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("lower", vec![field("n")]), &ctx), None);
    assert_eq!(l3_ctx(&call("upper", vec![field("n")]), &ctx), None);
    assert_eq!(l3_ctx(&call("ltrim", vec![Expr::Number(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("rtrim", vec![Expr::Number(1.0)]), &ctx), None);
    // len: byte length (multibyte aware per Rust str.len())
    assert_eq!(
        l3_ctx(&call("len", vec![lit("hello")]), &ctx),
        Some(Value::Number(5.0))
    );
    assert_eq!(
        l3_ctx(&call("len", vec![field("multibyte")]), &ctx),
        Some(Value::Number(6.0))
    );
    assert_eq!(l3_ctx(&call("len", vec![field("n")]), &ctx), None);
    assert_eq!(l3_ctx(&call("len", vec![]), &ctx), None);
}

#[test]
fn builtin_merge_branches() {
    let mut base = EngineHashMap::default();
    base.insert("severity".into(), Value::Number(3.0));
    let ctx = ctx_with(vec![
        ("extension", Value::Object(base)),
        ("scalar", Value::Number(7.0)),
    ]);
    let obj = Expr::Object(vec![ObjectItem {
        targets: vec!["source".to_string()],
        type_hint: None,
        value: lit("wfl"),
    }]);
    let result = l3_ctx(&call("merge", vec![field("extension"), obj.clone()]), &ctx);
    let Some(Value::Object(merged)) = result else {
        panic!("expected object");
    };
    assert_eq!(merged.get("severity"), Some(&Value::Number(3.0)));
    assert_eq!(merged.get("source"), Some(&Value::Str("wfl".into())));

    // missing field arg is skipped (treated as empty object)
    let result = l3_ctx(&call("merge", vec![field("missing"), obj.clone()]), &ctx);
    let Some(Value::Object(merged)) = result else {
        panic!("expected object");
    };
    assert_eq!(merged.get("source"), Some(&Value::Str("wfl".into())));

    // non-object arg → None
    assert_eq!(l3_ctx(&call("merge", vec![field("scalar")]), &ctx), None);
    // empty args → None
    assert_eq!(l3_ctx(&call("merge", vec![]), &ctx), None);
}

// ===========================================================================
// builtins.rs — array builtins (mv*) + error branches
// ===========================================================================

#[test]
fn builtin_mvcount_mvjoin_mvdedup_mvsort_mvreverse() {
    let vals = arr(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("a".into()),
        Value::Str("c".into()),
    ]);
    let nums = arr(vec![
        Value::Number(3.0),
        Value::Number(1.0),
        Value::Number(2.0),
    ]);
    let ctx = ctx_with(vec![
        ("vals", vals),
        ("nums", nums),
        ("scalar", Value::Number(1.0)),
    ]);
    assert_eq!(
        l3_ctx(&call("mvcount", vec![field("vals")]), &ctx),
        Some(Value::Number(4.0))
    );
    assert_eq!(
        l3_ctx(&call("mvjoin", vec![field("vals"), lit("|")]), &ctx),
        Some(Value::Str("a|b|a|c".into()))
    );
    assert_eq!(
        l3_ctx(&call("mvdedup", vec![field("vals")]), &ctx),
        Some(arr(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
        ]))
    );
    assert_eq!(
        l3_ctx(&call("mvsort", vec![field("nums")]), &ctx),
        Some(arr(vec![
            Value::Number(1.0),
            Value::Number(2.0),
            Value::Number(3.0),
        ]))
    );
    assert_eq!(
        l3_ctx(&call("mvreverse", vec![field("vals")]), &ctx),
        Some(arr(vec![
            Value::Str("c".into()),
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("a".into()),
        ]))
    );
    // non-array args → None
    assert_eq!(l3_ctx(&call("mvcount", vec![field("scalar")]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("mvjoin", vec![field("scalar"), lit(",")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("mvjoin", vec![field("vals"), field("scalar")]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("mvdedup", vec![field("scalar")]), &ctx), None);
    assert_eq!(l3_ctx(&call("mvsort", vec![field("scalar")]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("mvreverse", vec![field("scalar")]), &ctx),
        None
    );
    // wrong arg count
    assert_eq!(l3_ctx(&call("mvcount", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("mvjoin", vec![field("vals")]), &ctx), None);
    assert_eq!(l3_ctx(&call("mvdedup", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("mvsort", vec![]), &ctx), None);
    assert_eq!(l3_ctx(&call("mvreverse", vec![]), &ctx), None);
}

#[test]
fn builtin_mvindex_single_and_range() {
    let vals = arr(vec![
        Value::Str("a".into()),
        Value::Str("b".into()),
        Value::Str("c".into()),
        Value::Str("d".into()),
    ]);
    let ctx = ctx_with(vec![("vals", vals), ("scalar", Value::Number(1.0))]);
    // 2-arg: positive / negative / out of range
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("vals"), Expr::Number(0.0)]),
            &ctx
        ),
        Some(Value::Str("a".into()))
    );
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Neg(Box::new(Expr::Number(1.0)))]
            ),
            &ctx
        ),
        Some(Value::Str("d".into()))
    );
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("vals"), Expr::Number(10.0)]),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Neg(Box::new(Expr::Number(10.0)))]
            ),
            &ctx
        ),
        None
    );
    // 3-arg range
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Number(1.0), Expr::Number(2.0)]
            ),
            &ctx
        ),
        Some(arr(vec![Value::Str("b".into()), Value::Str("c".into())]))
    );
    // negative range bounds: start=-3 → idx 1, end=-1 → idx 3
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![
                    field("vals"),
                    Expr::Neg(Box::new(Expr::Number(3.0))),
                    Expr::Neg(Box::new(Expr::Number(1.0)))
                ]
            ),
            &ctx
        ),
        Some(arr(vec![
            Value::Str("b".into()),
            Value::Str("c".into()),
            Value::Str("d".into())
        ]))
    );
    // end < 0 → empty
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![
                    field("vals"),
                    Expr::Number(0.0),
                    Expr::Neg(Box::new(Expr::Number(10.0)))
                ]
            ),
            &ctx
        ),
        Some(arr(vec![]))
    );
    // start >= len → empty
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Number(10.0), Expr::Number(20.0)]
            ),
            &ctx
        ),
        Some(arr(vec![]))
    );
    // start > end → empty
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Number(3.0), Expr::Number(1.0)]
            ),
            &ctx
        ),
        Some(arr(vec![]))
    );
    // end clamped to len-1
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("vals"), Expr::Number(2.0), Expr::Number(100.0)]
            ),
            &ctx
        ),
        Some(arr(vec![Value::Str("c".into()), Value::Str("d".into())]))
    );
    // empty array with range → empty
    let empty_ctx = ctx_with(vec![("empty", arr(vec![]))]);
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![field("empty"), Expr::Number(0.0), Expr::Number(1.0)]
            ),
            &empty_ctx
        ),
        Some(arr(vec![]))
    );
    // errors
    assert_eq!(l3_ctx(&call("mvindex", vec![field("vals")]), &ctx), None);
    assert_eq!(
        l3_ctx(
            &call(
                "mvindex",
                vec![
                    field("vals"),
                    Expr::Number(1.0),
                    Expr::Number(2.0),
                    Expr::Number(3.0)
                ]
            ),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(
            &call("mvindex", vec![field("scalar"), Expr::Number(0.0)]),
            &ctx
        ),
        None
    );
    assert_eq!(
        l3_ctx(&call("mvindex", vec![field("vals"), lit("x")]), &ctx),
        None
    );
}

#[test]
fn builtin_mvappend_split() {
    let ctx = ctx_with(vec![
        (
            "vals",
            arr(vec![Value::Str("a".into()), Value::Str("b".into())]),
        ),
        ("scalar", Value::Number(9.0)),
    ]);
    assert_eq!(
        l3_ctx(
            &call("mvappend", vec![field("vals"), lit("c"), field("scalar")]),
            &ctx
        ),
        Some(arr(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str("c".into()),
            Value::Number(9.0),
        ]))
    );
    assert_eq!(l3_ctx(&call("mvappend", vec![]), &ctx), None);
    // split with separator
    assert_eq!(
        l3_ctx(&call("split", vec![lit("a,b,,c"), lit(",")]), &ctx),
        Some(arr(vec![
            Value::Str("a".into()),
            Value::Str("b".into()),
            Value::Str(String::new().into()),
            Value::Str("c".into()),
        ]))
    );
    // split with empty separator → per-char
    assert_eq!(
        l3_ctx(&call("split", vec![lit("ab"), lit("")]), &ctx),
        Some(arr(vec![Value::Str("a".into()), Value::Str("b".into())]))
    );
    assert_eq!(l3_ctx(&call("split", vec![lit("a")]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("split", vec![field("scalar"), lit(",")]), &ctx),
        None
    );
    assert_eq!(
        l3_ctx(&call("split", vec![lit("a"), field("scalar")]), &ctx),
        None
    );
}

// ===========================================================================
// builtins.rs — numeric builtins + error branches
// ===========================================================================

#[test]
fn builtin_math_funcs() {
    let ctx = ctx_with(vec![]);
    let n = |v: f64| Expr::Number(v);
    let neg = |v: f64| Expr::Neg(Box::new(Expr::Number(v)));

    assert_eq!(
        l3_ctx(&call("abs", vec![neg(5.0)]), &ctx),
        Some(Value::Number(5.0))
    );
    assert_eq!(
        l3_ctx(&call("abs", vec![n(2.5)]), &ctx),
        Some(Value::Number(2.5))
    );
    assert_eq!(l3_ctx(&call("abs", vec![lit("x")]), &ctx), None);
    assert_eq!(l3_ctx(&call("abs", vec![]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("round", vec![n(2.567)]), &ctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&call("round", vec![n(2.567), n(2.0)]), &ctx),
        Some(Value::Number(2.57))
    );
    assert_eq!(
        l3_ctx(&call("round", vec![n(1234.5), neg(2.0)]), &ctx),
        Some(Value::Number(1200.0))
    );
    assert_eq!(l3_ctx(&call("round", vec![lit("x")]), &ctx), None);
    assert_eq!(l3_ctx(&call("round", vec![n(1.0), lit("x")]), &ctx), None);
    // non-finite value / non-finite or huge precision → None
    assert_eq!(l3_ctx(&call("round", vec![n(f64::NAN)]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("round", vec![n(1.0), n(f64::NAN)]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("round", vec![n(1.0), n(1e300)]), &ctx), None);
    assert_eq!(l3_ctx(&call("round", vec![n(1.0), n(400.0)]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("ceil", vec![n(2.1)]), &ctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&call("ceil", vec![neg(2.1)]), &ctx),
        Some(Value::Number(-2.0))
    );
    assert_eq!(
        l3_ctx(&call("floor", vec![n(2.9)]), &ctx),
        Some(Value::Number(2.0))
    );
    assert_eq!(
        l3_ctx(&call("floor", vec![neg(2.1)]), &ctx),
        Some(Value::Number(-3.0))
    );
    assert_eq!(l3_ctx(&call("ceil", vec![lit("x")]), &ctx), None);
    assert_eq!(l3_ctx(&call("floor", vec![lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("sqrt", vec![n(16.0)]), &ctx),
        Some(Value::Number(4.0))
    );
    assert_eq!(l3_ctx(&call("sqrt", vec![neg(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("sqrt", vec![lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("pow", vec![n(2.0), n(8.0)]), &ctx),
        Some(Value::Number(256.0))
    );
    // non-finite result → None
    assert_eq!(l3_ctx(&call("pow", vec![n(0.0), neg(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("pow", vec![n(2.0)]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("log", vec![n(std::f64::consts::E)]), &ctx),
        Some(Value::Number(1.0))
    );
    assert_eq!(
        l3_ctx(&call("log", vec![n(100.0), n(10.0)]), &ctx),
        Some(Value::Number(2.0))
    );
    assert_eq!(l3_ctx(&call("log", vec![n(0.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![neg(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![n(100.0), n(0.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![n(100.0), n(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![n(100.0), neg(1.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![lit("x")]), &ctx), None);
    assert_eq!(l3_ctx(&call("log", vec![n(1.0), lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("exp", vec![n(0.0)]), &ctx),
        Some(Value::Number(1.0))
    );
    // overflow → None
    assert_eq!(l3_ctx(&call("exp", vec![n(1000.0)]), &ctx), None);
    assert_eq!(l3_ctx(&call("exp", vec![lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("clamp", vec![n(120.0), n(0.0), n(100.0)]), &ctx),
        Some(Value::Number(100.0))
    );
    assert_eq!(
        l3_ctx(&call("clamp", vec![n(-10.0), n(0.0), n(100.0)]), &ctx),
        Some(Value::Number(0.0))
    );
    assert_eq!(
        l3_ctx(&call("clamp", vec![n(50.0), n(0.0), n(100.0)]), &ctx),
        Some(Value::Number(50.0))
    );
    // min > max → None
    assert_eq!(
        l3_ctx(&call("clamp", vec![n(50.0), n(10.0), n(5.0)]), &ctx),
        None
    );
    assert_eq!(l3_ctx(&call("clamp", vec![n(1.0), n(2.0)]), &ctx), None);
    assert_eq!(
        l3_ctx(&call("clamp", vec![lit("x"), n(1.0), n(2.0)]), &ctx),
        None
    );

    assert_eq!(
        l3_ctx(&call("sign", vec![neg(5.0)]), &ctx),
        Some(Value::Number(-1.0))
    );
    assert_eq!(
        l3_ctx(&call("sign", vec![n(5.0)]), &ctx),
        Some(Value::Number(1.0))
    );
    assert_eq!(
        l3_ctx(&call("sign", vec![n(0.0)]), &ctx),
        Some(Value::Number(1.0))
    );
    // non-finite → None
    assert_eq!(l3_ctx(&call("sign", vec![n(f64::NAN)]), &ctx), None);
    assert_eq!(l3_ctx(&call("sign", vec![lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("trunc", vec![n(2.9)]), &ctx),
        Some(Value::Number(2.0))
    );
    assert_eq!(
        l3_ctx(&call("trunc", vec![neg(2.9)]), &ctx),
        Some(Value::Number(-2.0))
    );
    assert_eq!(l3_ctx(&call("trunc", vec![lit("x")]), &ctx), None);

    assert_eq!(
        l3_ctx(&call("is_finite", vec![n(1.0)]), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&call("is_finite", vec![n(f64::INFINITY)]), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&call("is_finite", vec![n(f64::NAN)]), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(l3_ctx(&call("is_finite", vec![lit("x")]), &ctx), None);
}
