//! eval 模块的 crate 级测试面补充（只改测试代码）。
//!
//! 目标：
//! - `match_engine/eval/funcs.rs`（纯 eval 路径）：`count_char` 与各类参数个数/类型错误分支；
//! - `match_engine/eval/cmp.rs`：数值/字符串/布尔比较边界、`try_eval_expr_to_f64` 的
//!   Neg / 算术 BinOp / 除零 / 非算术操作分支（经 `CepStateMachine` 阈值求值）；
//! - `executor/eval`（L3 路径）：经 `RuleExecutor::execute_match` 驱动 score 钳位与错误、
//!   entity_id 回退、yield 表达式中的系统变量 / L3 聚合 / 时间函数路由。

use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, Measure};
use wf_lang::plan::{AggPlan, BranchPlan, YieldField};

use crate::match_engine::RuleExecutor;
use crate::match_engine::match_engine::{
    CepStateMachine, EngineHashMap, Event, MatchedContext, StepData, StepResult, Value, eval_expr,
};

use super::helpers::{
    branch, count_ge, event, num, simple_key, simple_plan, simple_rule_plan, step, str_val,
};

// ===========================================================================
// 纯路径辅助（与 tests/l2/expr.rs 一致）
// ===========================================================================

fn field(name: &str) -> Expr {
    Expr::Field(FieldRef::Simple(name.to_string()))
}

fn n(v: f64) -> Expr {
    Expr::Number(v)
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FuncCall {
        qualifier: None,
        name: name.to_string(),
        args,
    }
}

fn eval_ok(expr: &Expr) -> Value {
    let e = event(vec![]);
    eval_expr(expr, &e).unwrap_or_else(|| panic!("expected Some for {expr:?}"))
}

// ===========================================================================
// funcs.rs — count_char（纯 eval 路径，l2/expr.rs 未覆盖）
// ===========================================================================

#[test]
fn count_char_counts_occurrences() {
    assert_eq!(
        eval_ok(&call(
            "count_char",
            vec![str_lit("hello world"), str_lit("l")]
        )),
        num(3.0)
    );
    assert_eq!(
        eval_ok(&call("count_char", vec![str_lit("abc"), str_lit("z")])),
        num(0.0)
    );
    // 空 needle → 0（不 panic）
    assert_eq!(
        eval_ok(&call("count_char", vec![str_lit("abc"), str_lit("")])),
        num(0.0)
    );
    // 多字节按 char 计数
    assert_eq!(
        eval_ok(&call(
            "count_char",
            vec![str_lit("你好你好"), str_lit("你")]
        )),
        num(2.0)
    );
}

#[test]
fn count_char_error_branches() {
    let e = event(vec![("n", num(7.0))]);
    // 参数个数错误
    assert_eq!(eval_expr(&call("count_char", vec![str_lit("a")]), &e), None);
    assert_eq!(
        eval_expr(
            &call("count_char", vec![str_lit("a"), str_lit("b"), str_lit("c")]),
            &e
        ),
        None
    );
    // 非字符串参数
    assert_eq!(
        eval_expr(&call("count_char", vec![field("n"), str_lit("a")]), &e),
        None
    );
    assert_eq!(
        eval_expr(&call("count_char", vec![str_lit("a"), field("n")]), &e),
        None
    );
}

fn str_lit(s: &str) -> Expr {
    Expr::StringLit(s.to_string())
}

// ===========================================================================
// funcs.rs — 纯 eval 路径的常规错误分支（参数个数 / 类型错误 → None）
// ===========================================================================

#[test]
fn plain_path_error_branches_return_none() {
    let mut fields = EngineHashMap::default();
    fields.insert("s".into(), Value::Str("abc".into()));
    fields.insert("n".into(), Value::Number(5.0));
    fields.insert(
        "arr".into(),
        Value::Array(vec![Value::Str("a".into()), Value::Str("b".into())]),
    );
    let e = Event { fields };

    let cases: Vec<Expr> = vec![
        // 字符串类：参数个数错误
        call("contains", vec![field("s")]),
        call("startswith", vec![field("s")]),
        call("endswith", vec![field("s")]),
        call("substr", vec![field("s")]),
        call("replace", vec![field("s"), str_lit("a")]),
        call("trim", vec![]),
        call("lower", vec![]),
        call("upper", vec![]),
        call("len", vec![]),
        call("ltrim", vec![]),
        call("rtrim", vec![]),
        call("indexof", vec![field("s")]),
        call("replace_plain", vec![field("s"), str_lit("a")]),
        call("startswith_any", vec![field("s")]),
        call("endswith_any", vec![field("s")]),
        // 字符串类：类型错误
        call("contains", vec![field("n"), str_lit("a")]),
        call("startswith", vec![field("s"), field("n")]),
        call("len", vec![field("n")]),
        call("trim", vec![field("n")]),
        call("indexof", vec![field("n"), str_lit("a")]),
        // 数组类：参数个数错误
        call("mvcount", vec![]),
        call("mvjoin", vec![field("arr")]),
        call("mvindex", vec![field("arr")]),
        call("mvappend", vec![]),
        call("split", vec![str_lit("a")]),
        call("mvdedup", vec![]),
        call("mvsort", vec![]),
        call("mvreverse", vec![]),
        // 数组类：类型错误
        call("mvcount", vec![field("s")]),
        call("mvjoin", vec![field("s"), str_lit("|")]),
        call("mvjoin", vec![field("arr"), field("n")]),
        call("mvindex", vec![field("n"), n(0.0)]),
        call("mvdedup", vec![field("n")]),
        call("split", vec![field("n"), str_lit(",")]),
        call("split", vec![str_lit("a"), field("n")]),
        call("mvsort", vec![field("n")]),
        call("mvreverse", vec![field("n")]),
        // 数值类：参数个数错误 / 非法输入
        call("abs", vec![]),
        call("round", vec![n(1.0), n(1.0), n(1.0)]),
        call("ceil", vec![]),
        call("floor", vec![]),
        call("sqrt", vec![n(-1.0)]),
        call("sqrt", vec![str_lit("x")]),
        call("pow", vec![n(2.0)]),
        call("pow", vec![n(0.0), Expr::Neg(Box::new(n(1.0)))]),
        call("log", vec![n(0.0)]),
        call("log", vec![n(-1.0)]),
        call("log", vec![n(100.0), n(1.0)]),
        call("log", vec![n(100.0), n(0.0)]),
        call("exp", vec![]),
        call("clamp", vec![n(1.0), n(2.0)]),
        call("clamp", vec![n(50.0), n(10.0), n(5.0)]),
        call("clamp", vec![str_lit("x"), n(1.0), n(2.0)]),
        call("sign", vec![]),
        call("sign", vec![n(f64::NAN)]),
        call("trunc", vec![]),
        call("is_finite", vec![]),
        call("is_finite", vec![str_lit("x")]),
        // 合并/格式化类
        call("fmt", vec![str_lit("{}"), str_lit("a"), str_lit("b")]),
        call("fmt", vec![]),
        call("fmt", vec![n(1.0)]),
        call("concat", vec![]),
        call("join", vec![]),
        call("join_by", vec![str_lit("|")]),
        call("join_by", vec![n(1.0), str_lit("a")]),
        call("coalesce", vec![]),
        call("isnull", vec![]),
        call("isnotnull", vec![]),
        call("is_blank", vec![field("n")]),
        call("null_if_blank", vec![field("n")]),
        call("default_if_blank", vec![field("s")]),
        call("default_if_blank", vec![field("n"), str_lit("fb")]),
        // 哈希类
        call("md5", vec![]),
        call("md5", vec![field("n")]),
        call("sha1", vec![]),
        call("sha256", vec![field("n")]),
        call("hex", vec![field("n")]),
        call("sha1_n", vec![str_lit("x"), n(0.0)]),
        call("sha1_n", vec![str_lit("x"), n(41.0)]),
        call("sha1_n", vec![str_lit("x"), n(2.5)]),
        call("stable_id", vec![str_lit("p")]),
        call("stable_id", vec![str_lit("p"), field("arr")]),
        // 正则/时间类
        call("regex_match", vec![str_lit("abc"), str_lit("(")]),
        call("regex_match", vec![str_lit("abc")]),
        call("time_diff", vec![n(1.0)]),
        call("time_bucket", vec![n(1.0)]),
        call("time_bucket", vec![n(1.0), n(0.0)]),
        call("bucket_end", vec![n(1.0), n(-5.0)]),
        call("strptime", vec![str_lit("junk"), str_lit("%Y-%m-%d")]),
        call("strptime", vec![str_lit("x")]),
        // 未知函数
        call("no_such_builtin", vec![n(1.0)]),
    ];
    for expr in cases {
        assert_eq!(eval_expr(&expr, &e), None, "expected None for {expr:?}");
    }
}

// ===========================================================================
// cmp.rs — 比较边界（纯 eval 路径 BinOp）
// ===========================================================================

fn binop(op: BinOp, l: Expr, r: Expr) -> Expr {
    Expr::BinOp {
        op,
        left: Box::new(l),
        right: Box::new(r),
    }
}

#[test]
fn cmp_number_boundaries() {
    let e = event(vec![]);
    // epsilon 相等：0.1 + 0.2 == 0.3
    assert_eq!(
        eval_expr(
            &binop(BinOp::Eq, binop(BinOp::Add, n(0.1), n(0.2)), n(0.3)),
            &e
        ),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(
            &binop(BinOp::Ne, binop(BinOp::Add, n(0.1), n(0.2)), n(0.3)),
            &e
        ),
        Some(Value::Bool(false))
    );
    // 明显不等
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, n(1.0), n(2.0)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, n(1.0), n(2.0)), &e),
        Some(Value::Bool(true))
    );
    // 边界比较
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, n(1.0), n(2.0)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, n(2.0), n(2.0)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Gt, n(2.0), n(1.0)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Gt, n(1.0), n(1.0)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Le, n(2.0), n(2.0)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ge, n(2.0), n(2.0)), &e),
        Some(Value::Bool(true))
    );
    // 负值
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, n(-3.0), n(-2.0)), &e),
        Some(Value::Bool(true))
    );
    // NaN：所有比较为 false（含 Ne——`(NaN - x).abs() >= EPSILON` 为 false）
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, n(f64::NAN), n(f64::NAN)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, n(f64::NAN), n(1.0)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, n(f64::NAN), n(1.0)), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Gt, n(f64::NAN), n(1.0)), &e),
        Some(Value::Bool(false))
    );
}

#[test]
fn cmp_string_and_bool_boundaries() {
    let e = event(vec![]);
    // 字符串字典序
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, str_lit("a"), str_lit("a")), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, str_lit("a"), str_lit("b")), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, str_lit("a"), str_lit("b")), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Gt, str_lit("ab"), str_lit("a")), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Le, str_lit("a"), str_lit("a")), &e),
        Some(Value::Bool(true))
    );
    // 布尔：仅 Eq/Ne
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, Expr::Bool(true), Expr::Bool(true)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, Expr::Bool(true), Expr::Bool(false)), &e),
        Some(Value::Bool(true))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, Expr::Bool(false), Expr::Bool(true)), &e),
        Some(Value::Bool(false))
    );
    // 类型不匹配：顺序比较 false，Eq/Ne 均 false（cmp.rs 顶层 `_ => false`）
    assert_eq!(
        eval_expr(&binop(BinOp::Eq, n(1.0), str_lit("1")), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ne, n(1.0), str_lit("1")), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Lt, n(1.0), str_lit("a")), &e),
        Some(Value::Bool(false))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Ge, Expr::Bool(true), n(1.0)), &e),
        Some(Value::Bool(false))
    );
}

#[test]
fn cmp_arithmetic_overflow_and_div_zero() {
    let e = event(vec![]);
    // 除零 / 模零 → None
    assert_eq!(eval_expr(&binop(BinOp::Div, n(1.0), n(0.0)), &e), None);
    assert_eq!(eval_expr(&binop(BinOp::Mod, n(1.0), n(0.0)), &e), None);
    // 非数值操作数 → None
    assert_eq!(
        eval_expr(&binop(BinOp::Add, n(1.0), str_lit("x")), &e),
        None
    );
    // 减法/乘法/模
    assert_eq!(
        eval_expr(&binop(BinOp::Sub, n(5.0), n(2.0)), &e),
        Some(Value::Number(3.0))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Mul, n(5.0), n(2.0)), &e),
        Some(Value::Number(10.0))
    );
    assert_eq!(
        eval_expr(&binop(BinOp::Mod, n(5.0), n(2.0)), &e),
        Some(Value::Number(1.0))
    );
    // 大数值溢出为 inf 仍返回（f64 语义）
    assert!(matches!(
        eval_expr(&binop(BinOp::Mul, n(1e300), n(1e300)), &e),
        Some(Value::Number(v)) if v.is_infinite()
    ));
}

// ===========================================================================
// cmp.rs — 阈值表达式静态求值（try_eval_expr_to_f64）经 CepStateMachine 覆盖
// ===========================================================================

fn threshold_plan(threshold: Expr) -> wf_lang::plan::MatchPlan {
    simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "fail".to_string(),
            field: None,
            guard: None,
            agg: AggPlan {
                transforms: vec![],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold,
            },
        }])],
    )
}

#[test]
fn threshold_constant_arithmetic_expressions() {
    // count >= -(2) → 第一次事件即匹配
    let mut sm = CepStateMachine::new(
        "t_neg".to_string(),
        threshold_plan(Expr::Neg(Box::new(n(2.0)))),
        None,
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));

    // count >= (5 - 2) = 3
    let mut sm = CepStateMachine::new(
        "t_sub".to_string(),
        threshold_plan(binop(BinOp::Sub, n(5.0), n(2.0))),
        None,
    );
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));

    // count >= (2 * 2) = 4
    let mut sm = CepStateMachine::new(
        "t_mul".to_string(),
        threshold_plan(binop(BinOp::Mul, n(2.0), n(2.0))),
        None,
    );
    for _ in 0..3 {
        assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    }
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));

    // count >= (10 / 2) = 5
    let mut sm = CepStateMachine::new(
        "t_div".to_string(),
        threshold_plan(binop(BinOp::Div, n(10.0), n(2.0))),
        None,
    );
    for _ in 0..4 {
        assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    }
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));

    // count >= (7 % 3) = 1 → 第一次即匹配
    let mut sm = CepStateMachine::new(
        "t_mod".to_string(),
        threshold_plan(binop(BinOp::Mod, n(7.0), n(3.0))),
        None,
    );
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));
}

#[test]
fn threshold_unresolvable_expressions_never_match() {
    // 除零 / 模零 → try_eval_expr_to_f64 None → count 永不满足
    for (name, threshold) in [
        ("t_div0", binop(BinOp::Div, n(1.0), n(0.0))),
        ("t_mod0", binop(BinOp::Mod, n(1.0), n(0.0))),
        // 非算术 BinOp（比较）→ None
        ("t_cmpop", binop(BinOp::Eq, n(1.0), n(1.0))),
        // 逻辑 BinOp → None
        (
            "t_logic",
            Expr::BinOp {
                op: BinOp::And,
                left: Box::new(Expr::Bool(true)),
                right: Box::new(Expr::Bool(true)),
            },
        ),
    ] {
        let mut sm = CepStateMachine::new(name.to_string(), threshold_plan(threshold), None);
        let e = event(vec![("sip", str_val("10.0.0.1"))]);
        for _ in 0..5 {
            assert_eq!(
                sm.advance("fail", &e),
                StepResult::Accumulate,
                "{name} must never match"
            );
        }
    }
}

// ===========================================================================
// executor/eval L3 路径 — 经 RuleExecutor::execute_match 驱动
// ===========================================================================

fn matched_with_values(values: Vec<Value>) -> MatchedContext {
    MatchedContext {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("fail".to_string()),
            measure_value: values.len() as f64,
            event_first_time_nanos: Some(1_700_000_000_000_000_000),
            event_last_time_nanos: Some(1_700_000_000_000_000_000),
            collected_values: values,
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        event_time_nanos: 0,
        event_first_time_nanos: 1_700_000_000_000_000_000,
        event_last_time_nanos: 1_700_000_000_000_000_000,
        window_start_time_nanos: 1_700_000_000_000_000_000,
        window_end_time_nanos: 1_700_000_000_000_000_000,
        machine_id: String::new(),
        trigger_event: None,
    }
}

fn executor_with_yield(fields: Vec<YieldField>, score_expr: Expr) -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "r_yield",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
        ),
        score_expr,
        "ip",
        field("sip"),
    );
    plan.yield_plan.fields = fields;
    RuleExecutor::new(plan)
}

fn yield_value<'a>(alert: &'a crate::alert::OutputRecord, name: &str) -> Option<&'a Value> {
    alert
        .yield_fields
        .iter()
        .find(|(k, _)| k.as_ref() == name)
        .map(|(_, v)| v)
}

#[test]
fn execute_match_l3_yield_expressions() {
    let exec = executor_with_yield(
        vec![
            // L3 收集：first / mvcount(collect_set) / percentile 走 step values
            YieldField {
                name: "first".into(),
                value: call("first", vec![field("fail")]),
            },
            YieldField {
                name: "mvcount".into(),
                value: call("mvcount", vec![call("collect_set", vec![field("fail")])]),
            },
            YieldField {
                name: "p50".into(),
                value: call("percentile", vec![field("fail"), n(50.0)]),
            },
            YieldField {
                name: "agg_sum".into(),
                value: call("sum", vec![field("fail")]),
            },
            // 聚合嵌套触发 L3 路由：mvjoin(collect_list(...))
            YieldField {
                name: "joined".into(),
                value: call(
                    "mvjoin",
                    vec![call("collect_list", vec![field("fail")]), str_lit(",")],
                ),
            },
            // 系统变量：@score 直接求值
            YieldField {
                name: "sys_score".into(),
                value: Expr::SystemVar(wf_lang::ast::SystemVar::Score),
            },
            // WfuMeta 经 materialize 路径：concat(@__wfu_id) → 纯 eval
            YieldField {
                name: "wfu".into(),
                value: call(
                    "concat",
                    vec![
                        str_lit("id="),
                        Expr::WfuMeta(wf_lang::wfu_meta::WfuMetaField::Id),
                    ],
                ),
            },
            // eval-time 函数：fmt("{}", now()) 触发 L3 路由
            YieldField {
                name: "now".into(),
                value: call("fmt", vec![str_lit("{}"), call("now", vec![])]),
            },
            // strftime 默认格式（meta.time_format）
            YieldField {
                name: "fmt_ts".into(),
                value: call("strftime", vec![n(0.0)]),
            },
            // 缺失字段 → eval_yield_expr 回退空串
            YieldField {
                name: "missing".into(),
                value: field("absent_field"),
            },
        ],
        Expr::Number(70.0),
    );
    let matched = matched_with_values(vec![
        Value::Number(10.0),
        Value::Number(20.0),
        Value::Number(30.0),
    ]);
    let alert = exec.execute_match(&matched).unwrap();

    assert_eq!(yield_value(&alert, "first"), Some(&Value::Number(10.0)));
    assert_eq!(yield_value(&alert, "mvcount"), Some(&Value::Number(3.0)));
    assert_eq!(yield_value(&alert, "p50"), Some(&Value::Number(20.0)));
    // sum(fail) 走 step measure（collected_values.len() = 3）
    assert_eq!(yield_value(&alert, "agg_sum"), Some(&Value::Number(3.0)));
    assert_eq!(
        yield_value(&alert, "joined"),
        Some(&Value::Str("10,20,30".into()))
    );
    assert_eq!(yield_value(&alert, "sys_score"), Some(&Value::Number(70.0)));
    let Some(Value::Str(wfu)) = yield_value(&alert, "wfu") else {
        panic!("wfu yield expected string");
    };
    assert!(wfu.starts_with("id="), "wfu yield = {wfu}");
    let Some(Value::Str(now_str)) = yield_value(&alert, "now") else {
        panic!("now yield expected string from fmt({{}})");
    };
    let now_ts: f64 = now_str
        .parse()
        .expect("now yield should be a numeric string");
    assert!(now_ts > 1_000_000_000_000.0, "now yield = {now_str}");
    assert_eq!(
        yield_value(&alert, "fmt_ts"),
        Some(&Value::Str("1970-01-01 00:00:00.000".into()))
    );
    assert_eq!(
        yield_value(&alert, "missing"),
        Some(&Value::Str(String::new().into()))
    );
}

#[test]
fn execute_match_score_clamps_and_errors() {
    // 超上限 → 钳位到 100
    let exec = executor_with_yield(vec![], n(150.0));
    let alert = exec.execute_match(&matched_with_values(vec![])).unwrap();
    assert_eq!(alert.score, 100.0);
    // 低于下限 → 钳位到 0
    let exec = executor_with_yield(vec![], n(-5.0));
    let alert = exec.execute_match(&matched_with_values(vec![])).unwrap();
    assert_eq!(alert.score, 0.0);
    // 非数值 score 表达式 → Err
    let exec = executor_with_yield(vec![], str_lit("high"));
    assert!(exec.execute_match(&matched_with_values(vec![])).is_err());
    // 缺失字段的 score 表达式 → Err
    let exec = executor_with_yield(vec![], field("no_score_field"));
    assert!(exec.execute_match(&matched_with_values(vec![])).is_err());
    // 正常值
    let exec = executor_with_yield(vec![], binop(BinOp::Mul, n(7.0), n(10.0)));
    let alert = exec.execute_match(&matched_with_values(vec![])).unwrap();
    assert_eq!(alert.score, 70.0);
}

#[test]
fn execute_match_entity_id_fallback() {
    // entity_id 缺失字段 → eval_entity_id 经 eval_yield_expr 回退空串
    let plan = simple_rule_plan(
        "r_eid",
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
        ),
        n(50.0),
        "ip",
        field("absent_entity"),
    );
    let exec = RuleExecutor::new(plan);
    let alert = exec.execute_match(&matched_with_values(vec![])).unwrap();
    assert_eq!(alert.entity_id, "");
}
