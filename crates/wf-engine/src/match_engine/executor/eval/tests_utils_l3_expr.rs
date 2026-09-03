//! utils/入口语义与表达式分支（2026-09-04 自 tests.rs 拆出）：utils.rs helper 覆盖、
//! eval_score/eval_entity_id/eval_bool_expr/yield fallback、eval_expr_with_l3 表达式分支
//! （字面量/字段/系统与 wfu 变量/对象/数组/inlist/算术/比较/逻辑/if）、contains_system_var/
//! materialize 改写与 dispatch 入口。

use super::*;

// ===========================================================================
// executor/eval/utils.rs — helper coverage
// ===========================================================================

#[test]
fn utils_normalize_index_and_sort() {
    assert_eq!(utils::normalize_index(0, 4), Some(0));
    assert_eq!(utils::normalize_index(3, 4), Some(3));
    assert_eq!(utils::normalize_index(-1, 4), Some(3));
    assert_eq!(utils::normalize_index(4, 4), None);
    assert_eq!(utils::normalize_index(-5, 4), None);
    assert_eq!(utils::normalize_index(0, 0), None);

    use std::cmp::Ordering;
    assert_eq!(
        utils::compare_sortable_values(&Value::Number(1.0), &Value::Number(2.0)),
        Ordering::Less
    );
    assert_eq!(
        utils::compare_sortable_values(&Value::Str("b".into()), &Value::Str("a".into())),
        Ordering::Greater
    );
    assert_eq!(
        utils::compare_sortable_values(&Value::Bool(false), &Value::Bool(true)),
        Ordering::Less
    );
    // mixed types fall back to string comparison
    assert_eq!(
        utils::compare_sortable_values(&Value::Number(2.0), &Value::Str("10".into())),
        Ordering::Greater
    );
    assert_eq!(
        utils::compare_sortable_values(&Value::Array(vec![]), &Value::Number(1.0)),
        Ordering::Greater
    );
}

#[test]
fn utils_f64_helpers() {
    assert_eq!(utils::f64_to_i64_trunc(3.7), Some(3));
    assert_eq!(utils::f64_to_i64_trunc(-3.7), Some(-3));
    assert_eq!(utils::f64_to_i64_trunc(0.0), Some(0));
    assert_eq!(utils::f64_to_i64_trunc(f64::NAN), None);
    assert_eq!(utils::f64_to_i64_trunc(f64::INFINITY), None);
    assert_eq!(utils::f64_to_i64_trunc(1e300), None);
    assert_eq!(utils::f64_to_i64_trunc(-1e300), None);

    assert_eq!(utils::round_with_precision(2.567, 2), Some(2.57));
    assert_eq!(utils::round_with_precision(2.5, 0), Some(3.0));
    assert_eq!(utils::round_with_precision(1234.5, -2), Some(1200.0));
    assert_eq!(utils::round_with_precision(5.0, -1), Some(10.0));
    assert_eq!(utils::round_with_precision(f64::NAN, 2), None);
    assert_eq!(utils::round_with_precision(1.0, 400), None);
    assert_eq!(utils::round_with_precision(1.0, -400), None);
}

#[test]
fn utils_fmt_template() {
    let values = vec![Value::Str("a".into()), Value::Number(3.0)];
    assert_eq!(
        utils::apply_fmt_template("{}:{}!", &values),
        Some("a:3!".to_string())
    );
    assert_eq!(utils::apply_fmt_template("{}", &values), None);
    assert_eq!(utils::apply_fmt_template("no placeholders", &values), None);
    assert_eq!(utils::apply_fmt_template("", &[]), Some(String::new()));
}

#[test]
fn utils_time_helpers() {
    let dt = utils::timestamp_nanos_to_utc(0).unwrap();
    assert_eq!(dt.to_rfc3339(), "1970-01-01T00:00:00+00:00");
    let dt = utils::timestamp_nanos_to_utc(1_700_000_000_123_456_789).unwrap();
    assert_eq!(dt.timestamp(), 1_700_000_000);

    assert_eq!(
        utils::time_nanos_to_value(1_700_000_000_123_000_000),
        Value::Number(1_700_000_000_123.0)
    );
    match utils::time_nanos_to_expr(1_700_000_000_000_000_000) {
        Expr::Number(v) => assert_eq!(v, 1_700_000_000_000.0),
        other => panic!("expected Number expr, got {:?}", other),
    }

    // parse_time_to_timestamp_nanos: offset datetime / naive datetime / naive date / failure
    assert_eq!(
        utils::parse_time_to_timestamp_nanos("2024-03-11T00:00:00+08:00", "%Y-%m-%dT%H:%M:%S%z"),
        Some(1_710_086_400_000_000_000)
    );
    assert_eq!(
        utils::parse_time_to_timestamp_nanos("2024-03-11 00:00:00", "%Y-%m-%d %H:%M:%S"),
        Some(1_710_115_200_000_000_000)
    );
    assert_eq!(
        utils::parse_time_to_timestamp_nanos("1970-01-01", "%Y-%m-%d"),
        Some(0)
    );
    assert_eq!(
        utils::parse_time_to_timestamp_nanos("junk", "%Y-%m-%d"),
        None
    );

    assert!(utils::is_blank_str(""));
    assert!(utils::is_blank_str(" \t\n "));
    assert!(!utils::is_blank_str("x"));

    let ctx = ctx_with(vec![]);
    assert_eq!(
        utils::eval_single_string_arg_with_l3(&[lit("hi")], &ctx, YieldMeta::default()),
        Some("hi".to_string())
    );
    assert_eq!(
        utils::eval_single_string_arg_with_l3(&[], &ctx, YieldMeta::default()),
        None
    );
    assert_eq!(
        utils::eval_single_string_arg_with_l3(&[Expr::Number(1.0)], &ctx, YieldMeta::default()),
        None
    );
}

#[test]
fn utils_stable_id_hash_and_time() {
    // number / string / bool values hash to Some; array / object → None
    assert_eq!(
        utils::update_stable_id_hash(&mut Sha256::new(), &Value::Number(42.0)),
        Some(())
    );
    assert_eq!(
        utils::update_stable_id_hash(&mut Sha256::new(), &Value::Str("x".into())),
        Some(())
    );
    assert_eq!(
        utils::update_stable_id_hash(&mut Sha256::new(), &Value::Bool(true)),
        Some(())
    );
    assert_eq!(
        utils::update_stable_id_hash(&mut Sha256::new(), &Value::Array(vec![Value::Number(1.0)])),
        None
    );
    let mut obj = EngineHashMap::default();
    obj.insert("k".into(), Value::Number(1.0));
    assert_eq!(
        utils::update_stable_id_hash(&mut Sha256::new(), &Value::Object(obj)),
        None
    );

    // deterministic + type-tagged: number 42 and str "42" differ
    let mut h1 = Sha256::new();
    utils::update_stable_id_hash(&mut h1, &Value::Number(42.0)).unwrap();
    let mut h2 = Sha256::new();
    utils::update_stable_id_hash(&mut h2, &Value::Str("42".into())).unwrap();
    let (d1, d2) = (hex::encode(h1.finalize()), hex::encode(h2.finalize()));
    assert_ne!(d1, d2);

    assert!(utils::current_time_nanos().is_some());
    assert!(super::get_or_init_eval_time_nanos().is_some());
}

// ===========================================================================
// executor/eval/mod.rs — eval_score / eval_entity_id / eval_bool_expr / fallback
// ===========================================================================

#[test]
fn eval_score_clamps_and_rejects() {
    let ctx = ctx_with(vec![]);
    assert_eq!(eval_score(&Expr::Number(70.0), &ctx).unwrap(), 70.0);
    assert_eq!(eval_score(&Expr::Number(150.0), &ctx).unwrap(), 100.0);
    assert_eq!(eval_score(&Expr::Number(-5.0), &ctx).unwrap(), 0.0);
    assert_eq!(eval_score(&Expr::Number(33.3), &ctx).unwrap(), 33.3);
    // non-numeric value → Err
    assert!(eval_score(&lit("abc"), &ctx).is_err());
    // None → Err
    assert!(eval_score(&field("missing"), &ctx).is_err());
}

#[test]
fn eval_entity_id_stringifies() {
    let ctx = ctx_with(vec![]);
    assert_eq!(eval_entity_id(&lit("abc"), &ctx).unwrap(), "abc");
    assert_eq!(eval_entity_id(&Expr::Number(42.0), &ctx).unwrap(), "42");
    assert_eq!(eval_entity_id(&Expr::Bool(true), &ctx).unwrap(), "true");
    // None → eval_yield_expr falls back to an empty string
    assert_eq!(eval_entity_id(&field("missing"), &ctx).unwrap(), "");
}

#[test]
fn eval_bool_expr_strict_bool() {
    let ctx = ctx_with(vec![]);
    assert_eq!(eval_bool_expr(&Expr::Bool(true), &ctx), Some(true));
    assert_eq!(eval_bool_expr(&Expr::Bool(false), &ctx), Some(false));
    assert_eq!(eval_bool_expr(&Expr::Number(1.0), &ctx), None);
    assert_eq!(eval_bool_expr(&lit("x"), &ctx), None);
    assert_eq!(eval_bool_expr(&field("missing"), &ctx), None);
}

#[test]
fn eval_yield_expr_falls_back_to_empty_string() {
    let ctx = ctx_with(vec![]);
    assert_eq!(
        eval_yield_expr(&field("missing"), &ctx),
        Some(Value::Str(String::new().into()))
    );
    assert_eq!(
        eval_yield_expr(&Expr::Number(5.0), &ctx),
        Some(Value::Number(5.0))
    );
    // score-aware variant
    assert_eq!(
        eval_yield_expr_with_score(&Expr::SystemVar(SystemVar::Score), &ctx, Some(70.0)),
        Some(Value::Number(70.0))
    );
    assert_eq!(
        eval_yield_expr_with_score(&Expr::SystemVar(SystemVar::Score), &ctx, None),
        Some(Value::Str(String::new().into()))
    );
}

// ===========================================================================
// executor/eval/mod.rs — eval_expr_with_l3 expression branches
// ===========================================================================

#[test]
fn l3_expression_literals_and_fields() {
    let ctx = ctx_with(vec![("x", Value::Number(5.0))]);
    assert_eq!(l3_ctx(&Expr::Number(1.5), &ctx), Some(Value::Number(1.5)));
    assert_eq!(l3_ctx(&lit("hi"), &ctx), Some(Value::Str("hi".into())));
    assert_eq!(l3_ctx(&Expr::Bool(false), &ctx), Some(Value::Bool(false)));
    assert_eq!(l3_ctx(&field("x"), &ctx), Some(Value::Number(5.0)));
    assert_eq!(l3_ctx(&field("missing"), &ctx), None);
    // Neg
    assert_eq!(
        l3_ctx(&Expr::Neg(Box::new(Expr::Number(5.0))), &ctx),
        Some(Value::Number(-5.0))
    );
    assert_eq!(l3_ctx(&Expr::Neg(Box::new(lit("x"))), &ctx), None);
}

#[test]
fn l3_expression_system_and_wfu_vars() {
    let ctx = ctx_with(vec![]);
    // score system var
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::Score),
            &ctx,
            YieldMeta {
                score: Some(70.0),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(70.0))
    );
    assert_eq!(l3_ctx(&Expr::SystemVar(SystemVar::Score), &ctx), None);
    // event-first-time system var → millis
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::EventFirstTime),
            &ctx,
            YieldMeta {
                event_first_time_nanos: Some(1_700_000_000_123_000_000),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(1_700_000_000_123.0))
    );
    // emit time system var
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::EmitTime),
            &ctx,
            YieldMeta {
                emit_time_nanos: Some(1_700_000_000_000_000_000),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(1_700_000_000_000.0))
    );
    // first_match_time system var（issue #82）→ 引擎处理墙钟（毫秒）
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::FirstMatchTime),
            &ctx,
            YieldMeta {
                first_match_time_nanos: Some(1_700_000_000_123_000_000),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(1_700_000_000_123.0))
    );
    // event 与 evidence 槽独立（issue #82 方案 A）：@evidence_* 读自己的槽，
    // 不再是 @event_* 的别名。
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::EvidenceStartTime),
            &ctx,
            YieldMeta {
                event_first_time_nanos: Some(1_700_000_000_000_000_000),
                evidence_first_time_nanos: Some(1_700_000_000_500_000_000),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(1_700_000_000_500.0))
    );
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::EvidenceEndTime),
            &ctx,
            YieldMeta {
                event_last_time_nanos: Some(1_700_000_000_000_000_000),
                evidence_last_time_nanos: Some(1_700_000_000_600_000_000),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(1_700_000_000_600.0))
    );
    // 未注入 first_match 墙钟 → None（空值，不参与求值）
    assert_eq!(
        eval_expr_with_l3(
            &Expr::SystemVar(SystemVar::FirstMatchTime),
            &ctx,
            YieldMeta::default(),
        ),
        None
    );
    // wfu meta fields
    let meta = YieldMeta {
        wfx_id: Some("wx-1"),
        rule_name: Some("r"),
        score: Some(80.0),
        entity_type: Some("ip"),
        entity_id: Some("eid"),
        origin: Some("origin"),
        close_reason: Some("cr"),
        fired_at: Some("fa"),
        emit_time: Some("et"),
        summary: Some("sum"),
        ..YieldMeta::default()
    };
    let check = |f: WfuMetaField, expected: Value| {
        assert_eq!(
            eval_expr_with_l3(&Expr::WfuMeta(f), &ctx, meta),
            Some(expected)
        );
    };
    check(WfuMetaField::Id, Value::Str("wx-1".into()));
    check(WfuMetaField::RuleName, Value::Str("r".into()));
    check(WfuMetaField::Score, Value::Number(80.0));
    check(WfuMetaField::EntityType, Value::Str("ip".into()));
    check(WfuMetaField::EntityId, Value::Str("eid".into()));
    check(WfuMetaField::Origin, Value::Str("origin".into()));
    check(WfuMetaField::CloseReason, Value::Str("cr".into()));
    check(WfuMetaField::FiredAt, Value::Str("fa".into()));
    check(WfuMetaField::EmitTime, Value::Str("et".into()));
    check(WfuMetaField::Summary, Value::Str("sum".into()));
    // unresolvable wfu meta → None
    assert_eq!(
        eval_expr_with_l3(&Expr::WfuMeta(WfuMetaField::Id), &ctx, YieldMeta::default()),
        None
    );
}

#[test]
fn l3_expression_object_array_inlist() {
    let ctx = ctx_with(vec![]);
    // object literal: multiple targets get the same value
    let obj = Expr::Object(vec![ObjectItem {
        targets: vec!["a".to_string(), "b".to_string()],
        type_hint: None,
        value: Expr::Number(1.0),
    }]);
    let Some(Value::Object(map)) = l3_ctx(&obj, &ctx) else {
        panic!("expected object");
    };
    assert_eq!(map.get("a"), Some(&Value::Number(1.0)));
    assert_eq!(map.get("b"), Some(&Value::Number(1.0)));
    // array literal
    assert_eq!(
        l3_ctx(&Expr::Array(vec![Expr::Number(1.0), lit("x")]), &ctx),
        Some(arr(vec![Value::Number(1.0), Value::Str("x".into())]))
    );
    // in-list
    let in_list = |negated: bool| Expr::InList {
        expr: Box::new(Expr::Number(2.0)),
        list: vec![Expr::Number(1.0), Expr::Number(2.0), Expr::Number(3.0)],
        negated,
    };
    assert_eq!(l3_ctx(&in_list(false), &ctx), Some(Value::Bool(true)));
    assert_eq!(l3_ctx(&in_list(true), &ctx), Some(Value::Bool(false)));
    let not_found = Expr::InList {
        expr: Box::new(Expr::Number(9.0)),
        list: vec![Expr::Number(1.0)],
        negated: false,
    };
    assert_eq!(l3_ctx(&not_found, &ctx), Some(Value::Bool(false)));
    // missing target → None
    let missing = Expr::InList {
        expr: Box::new(field("nope")),
        list: vec![Expr::Number(1.0)],
        negated: false,
    };
    assert_eq!(l3_ctx(&missing, &ctx), None);
}

#[test]
fn l3_expression_arith_and_compare() {
    let ctx = ctx_with(vec![]);
    let binop = |op: BinOp, l: Expr, r: Expr| Expr::BinOp {
        op,
        left: Box::new(l),
        right: Box::new(r),
    };
    let num = |v: f64| Expr::Number(v);
    assert_eq!(
        l3_ctx(&binop(BinOp::Add, num(1.0), num(2.0)), &ctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Sub, num(5.0), num(2.0)), &ctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Mul, num(5.0), num(2.0)), &ctx),
        Some(Value::Number(10.0))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Div, num(6.0), num(2.0)), &ctx),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Mod, num(5.0), num(2.0)), &ctx),
        Some(Value::Number(1.0))
    );
    // div / mod by zero → None
    assert_eq!(l3_ctx(&binop(BinOp::Div, num(1.0), num(0.0)), &ctx), None);
    assert_eq!(l3_ctx(&binop(BinOp::Mod, num(1.0), num(0.0)), &ctx), None);
    // non-numeric operand → None
    assert_eq!(l3_ctx(&binop(BinOp::Add, num(1.0), lit("x")), &ctx), None);

    // comparisons: numbers / strings / bools / mismatch
    assert_eq!(
        l3_ctx(&binop(BinOp::Eq, num(1.0), num(1.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Ne, num(1.0), num(2.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Lt, num(1.0), num(2.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Gt, num(2.0), num(1.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Le, num(2.0), num(2.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Ge, num(2.0), num(2.0)), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Lt, num(3.0), num(2.0)), &ctx),
        Some(Value::Bool(false))
    );
    // string comparison
    assert_eq!(
        l3_ctx(&binop(BinOp::Lt, lit("a"), lit("b")), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Gt, lit("b"), lit("a")), &ctx),
        Some(Value::Bool(true))
    );
    // bool comparison
    assert_eq!(
        l3_ctx(&binop(BinOp::Lt, Expr::Bool(false), Expr::Bool(true)), &ctx),
        Some(Value::Bool(true))
    );
    // mismatch: order comparisons → false, Ne → true (values_equal mismatch)
    assert_eq!(
        l3_ctx(&binop(BinOp::Lt, num(1.0), lit("a")), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Eq, num(1.0), lit("1")), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&binop(BinOp::Ne, num(1.0), lit("1")), &ctx),
        Some(Value::Bool(true))
    );
}

#[test]
fn l3_expression_logic_ops() {
    let ctx = ctx_with(vec![]);
    let and = |l: Expr, r: Expr| Expr::BinOp {
        op: BinOp::And,
        left: Box::new(l),
        right: Box::new(r),
    };
    let or = |l: Expr, r: Expr| Expr::BinOp {
        op: BinOp::Or,
        left: Box::new(l),
        right: Box::new(r),
    };
    let t = Expr::Bool(true);
    let f = Expr::Bool(false);
    assert_eq!(
        l3_ctx(&and(t.clone(), t.clone()), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&and(f.clone(), t.clone()), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&and(t.clone(), f.clone()), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(l3_ctx(&and(t.clone(), field("missing")), &ctx), None);
    assert_eq!(
        l3_ctx(&and(field("missing"), f.clone()), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3_ctx(&or(t.clone(), f.clone()), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&or(f.clone(), t.clone()), &ctx),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3_ctx(&or(f.clone(), f.clone()), &ctx),
        Some(Value::Bool(false))
    );
    assert_eq!(l3_ctx(&or(f.clone(), field("missing")), &ctx), None);
}

#[test]
fn l3_expression_if_then_else() {
    let ctx = ctx_with(vec![]);
    let ite = |cond: Expr| Expr::IfThenElse {
        cond: Box::new(cond),
        then_expr: Box::new(Expr::Number(1.0)),
        else_expr: Box::new(Expr::Number(2.0)),
    };
    assert_eq!(
        l3_ctx(&ite(Expr::Bool(true)), &ctx),
        Some(Value::Number(1.0))
    );
    assert_eq!(
        l3_ctx(&ite(Expr::Bool(false)), &ctx),
        Some(Value::Number(2.0))
    );
    // non-bool condition → None
    assert_eq!(l3_ctx(&ite(Expr::Number(1.0)), &ctx), None);
}

#[test]
fn l3_materializes_system_vars_in_func_args() {
    let ctx = ctx_with(vec![]);
    // abs(@score) → materialize score to 70.0 → plain eval abs → 70
    let expr = call("abs", vec![Expr::SystemVar(SystemVar::Score)]);
    assert_eq!(
        eval_expr_with_l3(
            &expr,
            &ctx,
            YieldMeta {
                score: Some(70.0),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Number(70.0))
    );
    // concat with a wfu meta arg
    let expr = call("concat", vec![lit("id="), Expr::WfuMeta(WfuMetaField::Id)]);
    assert_eq!(
        eval_expr_with_l3(
            &expr,
            &ctx,
            YieldMeta {
                wfx_id: Some("wx"),
                ..YieldMeta::default()
            },
        ),
        Some(Value::Str("id=wx".into()))
    );
    // unresolvable system var in func args → None
    let expr = call("abs", vec![Expr::SystemVar(SystemVar::Score)]);
    assert_eq!(l3_ctx(&expr, &ctx), None);
}

// ===========================================================================
// builtins.rs — contains_system_var / materialize_system_vars
// ===========================================================================

#[test]
fn contains_system_var_detection() {
    assert!(!contains_system_var(&Expr::Number(1.0)));
    assert!(!contains_system_var(&lit("x")));
    assert!(contains_system_var(&Expr::SystemVar(SystemVar::Score)));
    assert!(contains_system_var(&Expr::WfuMeta(WfuMetaField::Id)));
    assert!(contains_system_var(&Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(Expr::Number(1.0)),
        right: Box::new(Expr::SystemVar(SystemVar::Score)),
    }));
    assert!(contains_system_var(&Expr::Neg(Box::new(Expr::SystemVar(
        SystemVar::Score
    )))));
    assert!(contains_system_var(&call(
        "abs",
        vec![Expr::SystemVar(SystemVar::Score)]
    )));
    assert!(contains_system_var(&Expr::Object(vec![ObjectItem {
        targets: vec!["k".to_string()],
        type_hint: None,
        value: Expr::SystemVar(SystemVar::Score),
    }])));
    assert!(contains_system_var(&Expr::Array(vec![Expr::SystemVar(
        SystemVar::Score
    )])));
    assert!(contains_system_var(&Expr::InList {
        expr: Box::new(Expr::SystemVar(SystemVar::Score)),
        list: vec![Expr::Number(1.0)],
        negated: false,
    }));
    assert!(contains_system_var(&Expr::IfThenElse {
        cond: Box::new(Expr::Bool(true)),
        then_expr: Box::new(Expr::Number(1.0)),
        else_expr: Box::new(Expr::SystemVar(SystemVar::Score)),
    }));
}

#[test]
fn materialize_system_vars_rewrites() {
    let score = YieldMeta {
        score: Some(70.0),
        event_first_time_nanos: Some(1_700_000_000_000_000_000),
        event_last_time_nanos: Some(1_700_000_000_000_000_000),
        window_start_time_nanos: Some(1_700_000_000_000_000_000),
        window_end_time_nanos: Some(1_700_000_000_000_000_000),
        emit_time_nanos: Some(1_700_000_000_000_000_000),
        // 与 event 槽取不同值：锁定 @evidence_* 独立读取自己的槽（非别名）。
        evidence_first_time_nanos: Some(1_700_000_000_500_000_000),
        evidence_last_time_nanos: Some(1_700_000_000_600_000_000),
        first_match_time_nanos: Some(1_700_000_000_000_000_000),
        wfx_id: Some("wx"),
        ..YieldMeta::default()
    };
    assert_eq!(
        materialize_system_vars(&Expr::Number(1.0), score),
        Some(Expr::Number(1.0))
    );
    assert_eq!(materialize_system_vars(&lit("a"), score), Some(lit("a")));
    assert_eq!(
        materialize_system_vars(&Expr::Bool(true), score),
        Some(Expr::Bool(true))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::Score), score),
        Some(Expr::Number(70.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::EventFirstTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::EventLastTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::WindowStartTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::WindowEndTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::EmitTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    // event 与 evidence 槽独立（issue #82 方案 A）——各自读自己的值。
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::EvidenceStartTime), score),
        Some(Expr::Number(1_700_000_000_500.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::EvidenceEndTime), score),
        Some(Expr::Number(1_700_000_000_600.0))
    );
    assert_eq!(
        materialize_system_vars(&Expr::SystemVar(SystemVar::FirstMatchTime), score),
        Some(Expr::Number(1_700_000_000_000.0))
    );
    // 未注入 first_match 墙钟（None）→ 不可物化（与 emit_time 同口径）。
    assert_eq!(
        materialize_system_vars(
            &Expr::SystemVar(SystemVar::FirstMatchTime),
            YieldMeta {
                emit_time_nanos: Some(1_700_000_000_000_000_000),
                ..YieldMeta::default()
            }
        ),
        None
    );
    // wfu meta: string / number / bool values
    assert_eq!(
        materialize_system_vars(&Expr::WfuMeta(WfuMetaField::Id), score),
        Some(lit("wx"))
    );
    let score_meta = YieldMeta {
        score: Some(90.0),
        ..YieldMeta::default()
    };
    assert_eq!(
        materialize_system_vars(&Expr::WfuMeta(WfuMetaField::Score), score_meta),
        Some(Expr::Number(90.0))
    );
    // unresolvable wfu meta → None
    assert_eq!(
        materialize_system_vars(&Expr::WfuMeta(WfuMetaField::Id), YieldMeta::default()),
        None
    );
    // unsupported expr → None
    assert_eq!(
        materialize_system_vars(&Expr::PresetParam("p".to_string()), score),
        None
    );
    // structural rewrites
    let binop = Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(Expr::SystemVar(SystemVar::Score)),
        right: Box::new(Expr::Number(1.0)),
    };
    match materialize_system_vars(&binop, score) {
        Some(Expr::BinOp { left, right, .. }) => {
            assert_eq!(*left, Expr::Number(70.0));
            assert_eq!(*right, Expr::Number(1.0));
        }
        other => panic!("expected rewritten binop, got {:?}", other),
    }
    let neg = Expr::Neg(Box::new(Expr::SystemVar(SystemVar::Score)));
    assert_eq!(
        materialize_system_vars(&neg, score),
        Some(Expr::Neg(Box::new(Expr::Number(70.0))))
    );
    let call_expr = call("abs", vec![Expr::SystemVar(SystemVar::Score)]);
    match materialize_system_vars(&call_expr, score) {
        Some(Expr::FuncCall { name, args, .. }) => {
            assert_eq!(name, "abs");
            assert_eq!(args, vec![Expr::Number(70.0)]);
        }
        other => panic!("expected rewritten func call, got {:?}", other),
    }
    // unresolvable nested system var → whole tree None
    assert_eq!(
        materialize_system_vars(
            &call("abs", vec![Expr::SystemVar(SystemVar::Score)]),
            YieldMeta::default()
        ),
        None
    );
}

#[test]
fn builtin_dispatch_entry_points() {
    let ctx = ctx_with(vec![]);
    // direct builtin dispatch
    assert_eq!(
        eval_builtin_func_with_l3("abs", &[Expr::Number(-5.0)], &ctx, YieldMeta::default()),
        Some(Value::Number(5.0))
    );
    assert_eq!(
        eval_builtin_func_with_l3("unknown", &[], &ctx, YieldMeta::default()),
        None
    );
    // direct L3 func dispatch
    let step = step_ctx(vec![Value::Number(1.0), Value::Number(2.0)]);
    assert_eq!(
        eval_l3_func("collect_list", &[field("e")], &step, YieldMeta::default()),
        Some(arr(vec![Value::Number(1.0), Value::Number(2.0)]))
    );
    assert_eq!(
        eval_l3_func("nope", &[field("e")], &step, YieldMeta::default()),
        None
    );
    assert_eq!(
        eval_l3_func("collect_list", &[], &step, YieldMeta::default()),
        None
    );
    // direct aggregate dispatch
    assert_eq!(
        eval_aggregate_func("sum", &[field("e")], &step),
        Some(Value::Number(6.0))
    );
    assert_eq!(eval_aggregate_func("nope", &[field("e")], &step), None);
    assert_eq!(
        eval_aggregate_func("sum", &[Expr::Number(1.0)], &step),
        None
    );
    // direct stat dispatch
    let sctx = ctx_with(vec![("_bind_x_count", Value::Number(3.0))]);
    assert_eq!(
        eval_stat_func("count", &[call("window_event", vec![field("x")])], &sctx),
        Some(Value::Number(3.0))
    );
    // non-selector arg / unknown selector name → None
    assert_eq!(eval_stat_func("count", &[field("x")], &sctx), None);
    assert_eq!(
        eval_stat_func("nope", &[call("trigger", vec![field("x")])], &sctx),
        None
    );
}

/// review 2026-08-31：`contains_*` 递归检测必须穿透 match 表达式（issue #79
/// Issue 2）——否则 `fmt("{}", match x { ... => stat.value(final(t)) })` 里
/// 的 stat/时间/聚合/L3 函数藏在不同分支，判定为不含 → 走 L1 eval → 求值
/// None（静默错值）。
#[test]
fn contains_selector_checks_penetrate_match() {
    use super::{
        contains_aggregate_func, contains_eval_time_func, contains_l3_func, contains_stat_selector,
    };
    use wf_lang::ast::MatchArm;

    // `match x { "a" => <selector>, _ => 0 }`——selector 藏在分支值里。
    let arms = |value: Expr| {
        vec![MatchArm {
            patterns: vec![lit("a")],
            value,
        }]
    };
    let m = |value: Expr| Expr::Match {
        expr: Box::new(field("sev")),
        arms: arms(value),
        default: Some(Box::new(Expr::Number(0.0))),
    };

    let stat = m(Expr::FuncCall {
        qualifier: Some("stat".into()),
        name: "value".into(),
        args: vec![Expr::FuncCall {
            qualifier: None,
            name: "final".into(),
            args: vec![field("t")],
        }],
    });
    assert!(
        contains_stat_selector(&stat),
        "match 分支里的 stat.* 必须被检测"
    );

    let time = m(Expr::FuncCall {
        qualifier: None,
        name: "now_ns".into(),
        args: vec![],
    });
    assert!(
        contains_eval_time_func(&time),
        "match 分支里的 now 系列必须被检测"
    );

    let agg = m(Expr::FuncCall {
        qualifier: None,
        name: "sum".into(),
        args: vec![field("x")],
    });
    assert!(
        contains_aggregate_func(&agg),
        "match 分支里的聚合函数必须被检测"
    );

    let l3 = m(Expr::FuncCall {
        qualifier: None,
        name: "collect_list".into(),
        args: vec![field("e")],
    });
    assert!(contains_l3_func(&l3), "match 分支里的 L3 函数必须被检测");
}

/// review 2026-08-31：match 表达式在 L3 求值器内可求值，且分支值里的时间
/// 函数（now_ns）走 L3 时间工具——验证 eval_expr_with_l3 的 Match 分支与
/// contains_eval_time_func 穿透的协同。
#[test]
fn eval_expr_with_l3_match_branch_time_func() {
    use wf_lang::ast::MatchArm;
    let ctx = ctx_with(vec![("sev", Value::Str("crit".into()))]);
    let expr = Expr::Match {
        expr: Box::new(field("sev")),
        arms: vec![MatchArm {
            patterns: vec![lit("crit")],
            value: Expr::FuncCall {
                qualifier: None,
                name: "now_ns".into(),
                args: vec![],
            },
        }],
        default: Some(Box::new(Expr::Number(0.0))),
    };
    let v = eval_expr_with_l3(&expr, &ctx, YieldMeta::default());
    let Some(Value::Number(ns)) = v else {
        panic!("now_ns in match branch should evaluate via L3, got {v:?}");
    };
    assert!(
        ns > 1_700_000_000_000_000_000.0,
        "now_ns 应为当前墙钟（ns）"
    );
}
