//! StatsExecutor 单元测试（P1: 空键 fixed count/distinct/sum/avg/min/max）。

use std::collections::HashMap;

use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

use crate::match_engine::Value;
use crate::match_engine::executor::stats_exec::StatsExecutor;

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn str_val(s: &str) -> Value {
    Value::Str(s.into())
}

fn simple_plan(measures: Vec<StatsMeasurePlan>) -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
        keys: vec![],
        output_shape: StatsOutputShapePlan::Rows,
        measures,
        tracked_bind_fields: HashMap::new(),
    }
}

fn count_measure(label: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Count,
        field: None,
        arg: None,
    }
}

fn distinct_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::DistinctCount,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn sum_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Sum,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn avg_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Avg,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn row(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn extract(row: &HashMap<String, Value>, name: &str) -> Option<Value> {
    row.get(name).cloned()
}

#[test]
fn stats_count_and_distinct() {
    let plan = simple_plan(vec![
        count_measure("total_bids"),
        distinct_measure("total_bidders", "bidder"),
        distinct_measure("total_auctions", "auction"),
    ]);
    let mut exec = StatsExecutor::new(plan);

    exec.process_rows(
        &[
            row(&[("bidder", num(1.0)), ("auction", num(10.0))]),
            row(&[("bidder", num(1.0)), ("auction", num(11.0))]),
            row(&[("bidder", num(2.0)), ("auction", num(10.0))]),
        ],
        extract,
    );

    let values = exec.final_measure_values();
    assert_eq!(values[0], 3.0, "count = 3");
    assert_eq!(values[1], 2.0, "distinct bidder = {{1,2}}");
    assert_eq!(values[2], 2.0, "distinct auction = {{10,11}}");
}

#[test]
fn stats_sum_avg_min_max() {
    let plan = simple_plan(vec![
        sum_measure("sum_price", "price"),
        avg_measure("avg_price", "price"),
    ]);
    let mut exec = StatsExecutor::new(plan);

    exec.process_rows(
        &[
            row(&[("price", num(100.0))]),
            row(&[("price", num(200.0))]),
            row(&[("price", num(300.0))]),
        ],
        extract,
    );

    let values = exec.final_measure_values();
    assert_eq!(values[0], 600.0, "sum = 600");
    assert_eq!(values[1], 200.0, "avg = 600/3");
}

#[test]
fn stats_close_resets() {
    let plan = simple_plan(vec![count_measure("n")]);
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(&[row(&[]), row(&[])], extract);

    let closed = exec.close_window();
    assert_eq!(closed, vec![2.0]);

    // 清空后重新计数
    exec.process_rows(&[row(&[])], extract);
    assert_eq!(exec.final_measure_values(), vec![1.0]);
}

#[test]
fn stats_distinct_i64_precision() {
    // D7: bidder > 2^53 时不可 f64 化——DistinctKey::from_f64 整数域走 Int
    let plan = simple_plan(vec![distinct_measure("bidders", "bidder")]);
    let mut exec = StatsExecutor::new(plan);

    // 9.3e15 附近的两个整数（>2^53=9.007e15 的相邻值）
    let a = 9_007_199_254_740_993.0_f64;
    let b = 9_007_199_254_740_994.0_f64;
    exec.process_rows(
        &[row(&[("bidder", num(a))]), row(&[("bidder", num(b))])],
        extract,
    );
    assert_eq!(
        exec.final_measure_values()[0],
        2.0,
        "两个不同大整数应 distinct"
    );
}

#[test]
fn stats_where_filter_skips() {
    let mut m = count_measure("google_bids");
    m.where_expr = Some(Expr::Bool(false)); // 占位: where 由调用方预求值
    let plan = simple_plan(vec![m]);
    let mut exec = StatsExecutor::new(plan);

    // 注入 __where_ok=false 的行被跳过
    exec.process_rows(
        &[
            row(&[("__where_ok", Value::Bool(false))]),
            row(&[("__where_ok", Value::Bool(true))]),
        ],
        extract,
    );
    assert_eq!(exec.final_measure_values()[0], 1.0);
}
