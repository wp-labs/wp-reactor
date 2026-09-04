//! eval 覆盖 — executor/eval L3 路径（2026-09-04 P4-B2 拆分：纯 eval/cmp/阈值
//! 语义段 1-509 已随迁 `wf-cep::sem_tests::eval_coverage`；本文件保留
//! `RuleExecutor::execute_match` 驱动的 L3 yield / score 钳位 / entity_id 段）。

use wf_lang::ast::{BinOp, Expr, FieldRef};
use wf_lang::plan::YieldField;

use crate::match_engine::RuleExecutor;
use crate::match_engine::cep::{EngineHashMap, MatchedContext, StepData, Value};

use super::helpers::{branch, count_ge, simple_key, simple_plan, simple_rule_plan, step, str_val};

// ===========================================================================
// 表达式构造 helper（与随迁段同构，双份避免跨 crate 测试共享）
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

fn str_lit(s: &str) -> Expr {
    Expr::StringLit(s.to_string())
}

fn binop(op: BinOp, l: Expr, r: Expr) -> Expr {
    Expr::BinOp {
        op,
        left: Box::new(l),
        right: Box::new(r),
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
        first_match_time_nanos: None,
        evidence_first_time_nanos: 1_700_000_000_000_000_000,
        evidence_last_time_nanos: 1_700_000_000_000_000_000,
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
