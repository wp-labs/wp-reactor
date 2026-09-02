//! Round-4 coverage-fill tests for `executor/eval/mod.rs` — the L3
//! interpreter branches the existing batteries leave cold:
//!
//! - `YieldMeta::resolve_wfu_meta` arms (all ten `WfuMetaField` variants)
//! - the eval-time TLS cache hit lane (`get_or_init_eval_time_nanos` second
//!   call inside one scope) and the `EvalTimeScope` enter/drop pairing
//! - `Object` / `Array` / `Neg`(non-number) / `InList` (negated + miss) /
//!   `IfThenElse` (non-bool cond) interpreter arms
//! - the `FuncCall` routing lanes: qualified + system-var rewrite, qualified
//!   fallback, aggregate / l3 dispatch, the L3 routing conditions into
//!   `eval_builtin_func_with_l3`, and the `materialize_system_vars` rewrite
//! - `eval_score` error paths (non-numeric value / None) and `eval_entity_id`
//!   None path

use super::builtins::materialize_system_vars;
use super::{
    Event, Value, YieldMeta, eval_bool_expr, eval_entity_id, eval_expr_with_l3, eval_score,
    eval_yield_expr, eval_yield_expr_with_score,
};
use crate::match_engine::EngineHashMap;
use wf_lang::ast::{BinOp, Expr, FieldRef, ObjectItem, SystemVar};
use wf_lang::wfu_meta::WfuMetaField;

fn lit(s: &str) -> Expr {
    Expr::StringLit(s.to_string())
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

fn l3(expr: &Expr, ctx: &Event, meta: YieldMeta) -> Option<Value> {
    eval_expr_with_l3(expr, ctx, meta)
}

fn full_meta() -> YieldMeta<'static> {
    YieldMeta {
        score: Some(88.0),
        wfx_id: Some("wx"),
        rule_name: Some("r"),
        entity_type: Some("ip"),
        entity_id: Some("1.2.3.4"),
        origin: Some("event"),
        close_reason: Some("timeout"),
        fired_at: Some("2024-01-01T00:00:00Z"),
        emit_time: Some("2024-01-01T00:00:01Z"),
        summary: Some("s"),
        event_first_time_nanos: Some(1_700_000_000_000_000_000),
        event_last_time_nanos: Some(1_700_000_000_001_000_000),
        evidence_first_time_nanos: Some(1_700_000_000_000_000_000),
        evidence_last_time_nanos: Some(1_700_000_000_001_000_000),
        window_start_time_nanos: Some(1_700_000_000_000_000_000),
        window_end_time_nanos: Some(1_700_000_000_060_000_000),
        emit_time_nanos: Some(1_700_000_000_002_000_000),
        first_match_time_nanos: None,
        time_format: Some("%Y-%m-%d"),
    }
}

// ---------------------------------------------------------------------------
// WfuMeta resolution — every variant of `resolve_wfu_meta`
// ---------------------------------------------------------------------------

#[test]
fn wfu_meta_fields_resolve_every_variant() {
    let ctx = ctx_with(vec![]);
    let meta = full_meta();
    for (field, expected) in [
        (WfuMetaField::Id, Value::Str("wx".into())),
        (WfuMetaField::RuleName, Value::Str("r".into())),
        (WfuMetaField::Score, Value::Number(88.0)),
        (WfuMetaField::EntityType, Value::Str("ip".into())),
        (WfuMetaField::EntityId, Value::Str("1.2.3.4".into())),
        (WfuMetaField::Origin, Value::Str("event".into())),
        (WfuMetaField::CloseReason, Value::Str("timeout".into())),
        (
            WfuMetaField::FiredAt,
            Value::Str("2024-01-01T00:00:00Z".into()),
        ),
        (
            WfuMetaField::EmitTime,
            Value::Str("2024-01-01T00:00:01Z".into()),
        ),
        (WfuMetaField::Summary, Value::Str("s".into())),
    ] {
        assert_eq!(
            l3(&Expr::WfuMeta(field), &ctx, meta),
            Some(expected),
            "wfu meta field {field:?}"
        );
    }
    // Absent meta → None.
    assert_eq!(
        l3(&Expr::WfuMeta(WfuMetaField::Id), &ctx, YieldMeta::default()),
        None
    );
    // `materialize_system_vars` rewrite of a WfuMeta (Str → StringLit).
    let rewritten = materialize_system_vars(&Expr::WfuMeta(WfuMetaField::Id), meta);
    assert_eq!(rewritten, Some(lit("wx")));
}

// ---------------------------------------------------------------------------
// Eval-time TLS: cache-hit lane + scope enter/drop
// ---------------------------------------------------------------------------

#[test]
fn eval_time_scope_caches_nanos_within_one_scope() {
    let t1 = super::get_or_init_eval_time_nanos();
    assert!(t1.is_some());
    // Outside any scope the cache is reset per call, but inside one scope the
    // second `now()` call hits the cached lane.
    let now_twice = super::with_yield_eval_scope(|| {
        let a = super::get_or_init_eval_time_nanos();
        let b = super::get_or_init_eval_time_nanos();
        (a, b)
    });
    assert_eq!(now_twice.0, now_twice.1);
}

// ---------------------------------------------------------------------------
// Interpreter shapes: Object / Array / Neg / InList / IfThenElse
// ---------------------------------------------------------------------------

#[test]
fn object_array_neg_inlist_and_ite_arms() {
    let ctx = ctx_with(vec![("s", str_val("x")), ("n", num(5.0))]);

    // Object with a multi-target item.
    let obj = Expr::Object(vec![ObjectItem {
        targets: vec!["a".to_string(), "b".to_string()],
        type_hint: None,
        value: field("n"),
    }]);
    assert_eq!(
        l3(&obj, &ctx, YieldMeta::default()),
        Some(Value::Object(EngineHashMap::from_iter([
            ("a".into(), num(5.0)),
            ("b".into(), num(5.0)),
        ])))
    );
    // Object whose value fails to evaluate → None.
    let bad_obj = Expr::Object(vec![ObjectItem {
        targets: vec!["a".to_string()],
        type_hint: None,
        value: field("ghost"),
    }]);
    assert_eq!(l3(&bad_obj, &ctx, YieldMeta::default()), None);

    // Array of expressions.
    assert_eq!(
        l3(
            &Expr::Array(vec![Expr::Number(1.0), lit("a")]),
            &ctx,
            YieldMeta::default()
        ),
        Some(Value::Array(vec![num(1.0), str_val("a")]))
    );
    // Array with a failing item → None.
    assert_eq!(
        l3(
            &Expr::Array(vec![Expr::Number(1.0), field("ghost")]),
            &ctx,
            YieldMeta::default()
        ),
        None
    );

    // Neg on a number; Neg on a non-number → None.
    assert_eq!(
        l3(
            &Expr::Neg(Box::new(Expr::Number(3.0))),
            &ctx,
            YieldMeta::default()
        ),
        Some(num(-3.0))
    );
    assert_eq!(
        l3(&Expr::Neg(Box::new(lit("x"))), &ctx, YieldMeta::default()),
        None
    );

    // InList: hit / miss / negated / failing list item.
    let in_list = |list: Vec<Expr>, negated: bool| Expr::InList {
        expr: Box::new(lit("b")),
        list,
        negated,
    };
    assert_eq!(
        l3(
            &in_list(vec![lit("a"), lit("b")], false),
            &ctx,
            YieldMeta::default()
        ),
        Some(Value::Bool(true))
    );
    assert_eq!(
        l3(&in_list(vec![lit("a")], false), &ctx, YieldMeta::default()),
        Some(Value::Bool(false))
    );
    assert_eq!(
        l3(&in_list(vec![lit("a")], true), &ctx, YieldMeta::default()),
        Some(Value::Bool(true))
    );
    // A list item that fails to evaluate is skipped (`.unwrap_or(false)`).
    assert_eq!(
        l3(
            &in_list(vec![field("ghost"), lit("b")], false),
            &ctx,
            YieldMeta::default()
        ),
        Some(Value::Bool(true))
    );

    // IfThenElse: true / false / non-bool cond → None.
    let ite = |cond: Expr| Expr::IfThenElse {
        cond: Box::new(cond),
        then_expr: Box::new(lit("T")),
        else_expr: Box::new(lit("F")),
    };
    assert_eq!(
        l3(&ite(Expr::Bool(true)), &ctx, YieldMeta::default()),
        Some(str_val("T"))
    );
    assert_eq!(
        l3(&ite(Expr::Bool(false)), &ctx, YieldMeta::default()),
        Some(str_val("F"))
    );
    assert_eq!(l3(&ite(field("n")), &ctx, YieldMeta::default()), None);
}

// ---------------------------------------------------------------------------
// FuncCall routing lanes in `eval_expr_with_l3`
// ---------------------------------------------------------------------------

#[test]
fn func_call_routing_qualified_system_var_and_builtins() {
    let ctx = ctx_with(vec![("f", num(7.0))]);
    let meta = full_meta();

    // Qualified call containing a system var → materialize + eval_expr.
    let qualified_sysvar = Expr::FuncCall {
        qualifier: Some("e".to_string()),
        name: "fmt".to_string(),
        args: vec![lit("{}"), Expr::SystemVar(SystemVar::Score)],
    };
    assert_eq!(l3(&qualified_sysvar, &ctx, meta), Some(str_val("88")));

    // Qualified call without system vars → eval_expr(expr, ctx).
    let qualified_plain = Expr::FuncCall {
        qualifier: Some("e".to_string()),
        name: "abs".to_string(),
        args: vec![num_expr(5.0)],
    };
    // (abs of a literal — match_engine eval supports it.)
    assert_eq!(l3(&qualified_plain, &ctx, meta), Some(num(5.0)));

    // L3 func dispatch (collect_set over `_step_0_values`).
    let l3_ctx = ctx_with(vec![
        ("_step_0_values", Value::Array(vec![num(1.0), num(1.0)])),
        ("_step_0_source", str_val("e")),
    ]);
    assert_eq!(
        l3(&call("collect_set", vec![field("e")]), &l3_ctx, meta),
        Some(Value::Array(vec![num(1.0)]))
    );
    // Aggregate func dispatch over the step series.
    assert_eq!(
        l3(&call("count", vec![field("e")]), &l3_ctx, meta),
        Some(num(2.0))
    );

    // Routing condition: an eval-time function arg pulls the whole call into
    // `eval_builtin_func_with_l3` (concat of now_ns()).
    let eval_time_wrapped = call("concat", vec![call("now_ns", vec![])]);
    assert!(l3(&eval_time_wrapped, &ctx, meta).is_some());

    // Routing condition: aggregate func nested in an arg.
    let agg_wrapped = call("fmt", vec![lit("{}"), call("count", vec![field("e")])]);
    assert_eq!(l3(&agg_wrapped, &l3_ctx, meta), Some(str_val("2")));

    // Routing condition: stat selector nested in an arg (fmt of stat.value).
    let stat_wrapped = call(
        "fmt",
        vec![
            lit("{}"),
            Expr::FuncCall {
                qualifier: Some("stat".into()),
                name: "value".into(),
                args: vec![call("final", vec![field("m")])],
            },
        ],
    );
    let stat_ctx = ctx_with(vec![("m", num(9.0))]);
    assert_eq!(l3(&stat_wrapped, &stat_ctx, meta), Some(str_val("9")));

    // Unqualified call with a system var → materialize + eval_expr.
    let sysvar_wrapped = call("fmt", vec![lit("{}"), Expr::SystemVar(SystemVar::Score)]);
    assert_eq!(l3(&sysvar_wrapped, &ctx, meta), Some(str_val("88")));

    // Unknown function with plain args → eval_expr fallback → None.
    assert_eq!(
        l3(
            &call("definitely_not_a_fn", vec![num_expr(1.0)]),
            &ctx,
            meta
        ),
        None
    );
}

fn num_expr(n: f64) -> Expr {
    Expr::Number(n)
}

// ---------------------------------------------------------------------------
// Comparison / arithmetic arms via the L3 interpreter
// ---------------------------------------------------------------------------

#[test]
fn compare_and_arithmetic_l3_arms() {
    let ctx = ctx_with(vec![]);
    let cmp = |op: BinOp, l: Expr, r: Expr| {
        l3(
            &Expr::BinOp {
                op,
                left: Box::new(l),
                right: Box::new(r),
            },
            &ctx,
            YieldMeta::default(),
        )
    };

    // Bool ordering.
    assert_eq!(
        cmp(BinOp::Lt, Expr::Bool(false), Expr::Bool(true)),
        Some(Value::Bool(true))
    );
    assert_eq!(
        cmp(BinOp::Gt, Expr::Bool(true), Expr::Bool(false)),
        Some(Value::Bool(true))
    );
    assert_eq!(
        cmp(BinOp::Le, Expr::Bool(true), Expr::Bool(true)),
        Some(Value::Bool(true))
    );
    assert_eq!(
        cmp(BinOp::Ge, Expr::Bool(false), Expr::Bool(false)),
        Some(Value::Bool(true))
    );
    // Cross-type ordering → false.
    assert_eq!(
        cmp(BinOp::Lt, lit("a"), Expr::Bool(true)),
        Some(Value::Bool(false))
    );
    // Missing operand → None.
    assert_eq!(cmp(BinOp::Eq, field("ghost"), Expr::Number(1.0)), None);

    // Arithmetic: sub/mul happy paths, non-numeric → None, div/mod by zero.
    assert_eq!(
        cmp(BinOp::Sub, Expr::Number(5.0), Expr::Number(3.0)),
        Some(num(2.0))
    );
    assert_eq!(
        cmp(BinOp::Mul, Expr::Number(5.0), Expr::Number(3.0)),
        Some(num(15.0))
    );
    assert_eq!(
        cmp(BinOp::Div, Expr::Number(5.0), Expr::Number(2.0)),
        Some(num(2.5))
    );
    assert_eq!(
        cmp(BinOp::Mod, Expr::Number(5.0), Expr::Number(2.0)),
        Some(num(1.0))
    );
    assert_eq!(cmp(BinOp::Div, Expr::Number(5.0), Expr::Number(0.0)), None);
    assert_eq!(cmp(BinOp::Mod, Expr::Number(5.0), Expr::Number(0.0)), None);
    assert_eq!(cmp(BinOp::Add, lit("a"), Expr::Number(1.0)), None);
}

// ---------------------------------------------------------------------------
// eval_score / eval_entity_id / eval_bool_expr / eval_yield_expr entries
// ---------------------------------------------------------------------------

#[test]
fn score_and_entity_id_error_paths() {
    let ctx = ctx_with(vec![("n", num(5.0)), ("s", str_val("x"))]);
    // Happy: numeric score clamps.
    assert_eq!(eval_score(&Expr::Number(150.0), &ctx), Ok(100.0));
    assert_eq!(eval_score(&Expr::Number(-3.0), &ctx), Ok(0.0));
    // Non-numeric score value → Err.
    assert!(eval_score(&field("s"), &ctx).is_err());
    // None score (missing field) → Err.
    assert!(eval_score(&field("ghost"), &ctx).is_err());

    // entity_id happy + None fallback（eval_yield_expr 对缺失字段回退空串 → Ok）。
    assert_eq!(eval_entity_id(&field("s"), &ctx), Ok("x".to_string()));
    assert_eq!(
        eval_entity_id(&field("ghost"), &ctx),
        Ok("".to_string()),
        "eval_yield_expr 缺失字段回退空串，eval_entity_id 不 err"
    );
}

#[test]
fn eval_bool_and_yield_entries() {
    let ctx = ctx_with(vec![("n", num(5.0))]);
    // eval_bool_expr rejects non-bool results.
    assert_eq!(eval_bool_expr(&Expr::Number(5.0), &ctx), None);
    assert_eq!(eval_bool_expr(&Expr::Bool(true), &ctx), Some(true));
    // eval_yield_expr falls back to empty string for missing fields.
    assert_eq!(eval_yield_expr(&field("ghost"), &ctx), Some(str_val("")));
    // eval_yield_expr_with_score is the score-carrying entry.
    assert_eq!(
        eval_yield_expr_with_score(&Expr::SystemVar(SystemVar::Score), &ctx, Some(42.0)),
        Some(num(42.0))
    );
}

// ---------------------------------------------------------------------------
// time_to_s / time_to_ms（issue #69）——time 值转 epoch 秒/毫秒
// ---------------------------------------------------------------------------

#[test]
fn time_to_ms_handles_nanos_and_millis_inputs() {
    let ctx = ctx_with(vec![]);
    // 输入字段（event_bridge 转纳秒）: 纳秒 → 毫秒。
    assert_eq!(
        l3(
            &call(
                "time_to_ms",
                vec![Expr::Number(1_786_501_399_000_000_000.0)]
            ),
            &ctx,
            YieldMeta::default()
        ),
        Some(num(1_786_501_399_000.0)),
        "纳秒输入 → 毫秒"
    );
    // 系统变量（已是毫秒）: 幂等。
    assert_eq!(
        l3(
            &call("time_to_ms", vec![Expr::Number(1_786_501_399_000.0)]),
            &ctx,
            YieldMeta::default()
        ),
        Some(num(1_786_501_399_000.0)),
        "毫秒输入幂等"
    );
    // 秒输入 → 毫秒。
    assert_eq!(
        l3(
            &call("time_to_ms", vec![Expr::Number(1_786_501_399.0)]),
            &ctx,
            YieldMeta::default()
        ),
        Some(num(1_786_501_399_000.0)),
        "秒输入 → 毫秒"
    );
}

#[test]
fn time_to_s_handles_nanos_and_millis_inputs() {
    let ctx = ctx_with(vec![]);
    // 纳秒 → 秒。
    assert_eq!(
        l3(
            &call("time_to_s", vec![Expr::Number(1_786_501_399_000_000_000.0)]),
            &ctx,
            YieldMeta::default()
        ),
        Some(num(1_786_501_399.0)),
        "纳秒输入 → 秒"
    );
    // 毫秒 → 秒。
    assert_eq!(
        l3(
            &call("time_to_s", vec![Expr::Number(1_786_501_399_000.0)]),
            &ctx,
            YieldMeta::default()
        ),
        Some(num(1_786_501_399.0)),
        "毫秒输入 → 秒"
    );
}

#[test]
fn time_to_ms_wraps_system_vars() {
    let ctx = ctx_with(vec![]);
    let meta = full_meta();
    // @event_first_time（纳秒源 1.7e18）→ time_to_ms → 毫秒。
    assert_eq!(
        l3(
            &call(
                "time_to_ms",
                vec![Expr::SystemVar(SystemVar::EventFirstTime)]
            ),
            &ctx,
            meta,
        ),
        Some(num(1_700_000_000_000.0)),
        "@event_first_time → 毫秒"
    );
    // @emit_time → time_to_s → 秒。
    assert_eq!(
        l3(
            &call("time_to_s", vec![Expr::SystemVar(SystemVar::EmitTime)]),
            &ctx,
            meta,
        ),
        Some(num(1_700_000_000.0)),
        "@emit_time → 秒"
    );
}

#[test]
fn time_to_ms_rejects_bad_args() {
    let ctx = ctx_with(vec![("s", str_val("2026-01-01"))]);
    // 非数值参数 → None。
    assert_eq!(
        l3(
            &call("time_to_ms", vec![field("s")]),
            &ctx,
            YieldMeta::default()
        ),
        None,
        "字符串参数 → None"
    );
    // 参数个数 != 1 → None。
    assert_eq!(
        l3(&call("time_to_ms", vec![]), &ctx, YieldMeta::default()),
        None
    );
    assert_eq!(
        l3(
            &call("time_to_ms", vec![Expr::Number(1.0), Expr::Number(2.0)]),
            &ctx,
            YieldMeta::default(),
        ),
        None
    );
}

#[test]
fn time_to_s_and_ms_cover_unit_matrix_and_edges() {
    let ctx = ctx_with(vec![]);
    let none = YieldMeta::default();
    // 单位矩阵：秒/毫秒/微秒/纳秒 → 秒与毫秒。
    for (input, expected_s, expected_ms) in [
        (1_786_501_399.0, 1_786_501_399.0, 1_786_501_399_000.0), // 秒
        (1_786_501_399_000.0, 1_786_501_399.0, 1_786_501_399_000.0), // 毫秒
        (
            1_786_501_399_000_000.0,
            1_786_501_399.0,
            1_786_501_399_000.0,
        ), // 微秒
        (
            1_786_501_399_000_000_000.0,
            1_786_501_399.0,
            1_786_501_399_000.0,
        ), // 纳秒
    ] {
        assert_eq!(
            l3(&call("time_to_s", vec![Expr::Number(input)]), &ctx, none),
            Some(num(expected_s)),
            "time_to_s({input})"
        );
        assert_eq!(
            l3(&call("time_to_ms", vec![Expr::Number(input)]), &ctx, none),
            Some(num(expected_ms)),
            "time_to_ms({input})"
        );
    }
    // 零：归一化幂等。
    assert_eq!(
        l3(&call("time_to_ms", vec![Expr::Number(0.0)]), &ctx, none),
        Some(num(0.0))
    );
    // 分数毫秒：纳秒 round 后截断到毫秒。
    assert_eq!(
        l3(
            &call("time_to_ms", vec![Expr::Number(1_786_501_399_000.5)]),
            &ctx,
            none,
        ),
        Some(num(1_786_501_399_000.0)),
        "分数毫秒 → round 截断"
    );
    // NaN / 非有限 → None。
    assert_eq!(
        l3(
            &call("time_to_ms", vec![Expr::Number(f64::NAN)]),
            &ctx,
            none,
        ),
        None
    );
}

#[test]
fn time_to_ms_routes_through_nested_l3_and_strptime() {
    let ctx = ctx_with(vec![]);
    let none = YieldMeta::default();
    // 嵌套：外层 fmt 含 time_to_ms → contains_eval_time_func 路由到 L3。
    assert_eq!(
        l3(
            &call(
                "fmt",
                vec![
                    lit("{}"),
                    call("time_to_ms", vec![Expr::Number(1_786_501_399_000.0)]),
                ],
            ),
            &ctx,
            none,
        ),
        Some(str_val("1786501399000")),
        "fmt 包装 time_to_ms（嵌套路由）"
    );
    // 组合：strptime 字符串 → time（毫秒）→ time_to_ms 幂等。
    assert_eq!(
        l3(
            &call(
                "time_to_ms",
                vec![call(
                    "strptime",
                    vec![lit("2026-08-12 02:23:19"), lit("%Y-%m-%d %H:%M:%S")],
                )],
            ),
            &ctx,
            none,
        ),
        Some(num(1_786_501_399_000.0)),
        "time_to_ms(strptime(...)) → 毫秒"
    );
}
