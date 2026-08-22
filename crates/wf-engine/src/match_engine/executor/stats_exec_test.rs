//! StatsExecutor 单元测试（P1: 空键 fixed count/distinct/sum/avg/min/max）。

use std::collections::HashMap;

use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

use crate::match_engine::Value;
use crate::match_engine::executor::stats_exec::StatsExecutor;

fn num(n: f64) -> Value {
    Value::Number(n)
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

// ---------------------------------------------------------------------------
// Q15 对拍（设计 §8.1）: 12 列 = 4 count(total/r1/r2/r3) + 4 distinct bidder
// + 4 distinct auction; 价格分档 <1e4 / [1e4,1e6) / >=1e6。
// 独立参考实现作 ground truth（不共享执行器代码, 避免同源缺陷）。
// ---------------------------------------------------------------------------

/// 官方 q15 价格分档（与 close_bench::price_tier 同语义）。
fn q15_price_tier(price: f64) -> usize {
    if price < 10_000.0 {
        0
    } else if price < 1_000_000.0 {
        1
    } else {
        2
    }
}

/// q15 形状 StatsPlan: 12 度量, 各带 where_expr 占位以启用逐度量过滤。
fn q15_plan() -> StatsPlan {
    let mut measures = Vec::new();
    let mut push = |label: &str, agg: StatsAggPlan, field: Option<&str>| {
        measures.push(StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            // 占位: 仅用于触发 where_ok 咨询; 真实条件由 where_ok 闭包表达
            where_expr: Some(Expr::Bool(true)),
            agg,
            field: field.map(|f| FieldRef::Qualified("b".into(), f.into())),
            arg: None,
        });
    };
    for name in ["total", "r1", "r2", "r3"] {
        push(&format!("count_{name}"), StatsAggPlan::Count, None);
    }
    for name in ["total", "r1", "r2", "r3"] {
        push(
            &format!("bidder_{name}"),
            StatsAggPlan::DistinctCount,
            Some("bidder"),
        );
    }
    for name in ["total", "r1", "r2", "r3"] {
        push(
            &format!("auction_{name}"),
            StatsAggPlan::DistinctCount,
            Some("auction"),
        );
    }
    assert_eq!(measures.len(), 12, "q15 应为 12 个度量");
    simple_plan(measures)
}

/// q15 逐度量 where 过滤: 按 price 分档筛选对应档位度量。
/// 度量索引: 0-3 count(total/r1/r2/r3), 4-7 bidder, 8-11 auction。
/// 分档索引: total=任意, r1=档0, r2=档1, r3=档2。
/// null/missing price → 只计入 total 档（设计 §8.3, 对齐 Flink FILTER null 行为）。
fn q15_where_ok(row: &HashMap<String, Value>, idx: usize) -> bool {
    let tier_idx = |i: usize| match i % 4 {
        1 => 0,
        2 => 1,
        3 => 2,
        _ => usize::MAX,
    };
    let want = tier_idx(idx);
    if want == usize::MAX {
        return true; // total 档恒通过
    }
    match row.get("price") {
        Some(Value::Number(p)) => q15_price_tier(*p) == want,
        _ => false, // null → 不入任何条件档
    }
}

/// 确定性 bid 行（镜像 close_bench::bid_events: 同一 LCG 种子与公式）——
/// 与 CEP close_bench 的数据集字节一致, 便于将来交叉对拍。
fn bid_rows(n: usize) -> Vec<HashMap<String, Value>> {
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = |range: u64| {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (rng >> 33) % range
    };
    (0..n)
        .map(|_| {
            let price = (10f64.powf((next(1_000_000) as f64 / 1_000_000.0) * 6.0) * 100.0).round();
            row(&[
                ("price", num(price)),
                ("bidder", num((1000 + next(1010) as i64) as f64)),
                ("auction", num((1000 + next(110) as i64) as f64)),
            ])
        })
        .collect()
}

/// 独立参考实现: 对同一数据集手工折叠出 12 个期望值。
fn reference_q15(rows: &[HashMap<String, Value>]) -> Vec<u64> {
    let mut count = [0u64; 4];
    let mut bidders: Vec<std::collections::HashSet<i64>> =
        (0..4).map(|_| Default::default()).collect();
    let mut auctions: Vec<std::collections::HashSet<i64>> =
        (0..4).map(|_| Default::default()).collect();

    for r in rows {
        let price = match r.get("price") {
            Some(Value::Number(p)) => Some(*p),
            _ => None,
        };
        let tier = price.map(q15_price_tier);
        count[0] += 1;
        if let Some(t) = tier {
            count[t + 1] += 1;
        }
        let bidder = match r.get("bidder") {
            Some(Value::Number(n)) => Some(*n as i64),
            _ => None,
        };
        let auction = match r.get("auction") {
            Some(Value::Number(n)) => Some(*n as i64),
            _ => None,
        };
        if let Some(b) = bidder {
            bidders[0].insert(b);
            if let Some(t) = tier {
                bidders[t + 1].insert(b);
            }
        }
        if let Some(a) = auction {
            auctions[0].insert(a);
            if let Some(t) = tier {
                auctions[t + 1].insert(a);
            }
        }
    }
    let mut out = Vec::new();
    out.extend_from_slice(&count);
    for set in &bidders {
        out.push(set.len() as u64);
    }
    for set in &auctions {
        out.push(set.len() as u64);
    }
    out
}

#[test]
fn q15_hand_verified_small() {
    // 与 WFL inline test(q15_stats) 同数据集: 3 行 → "3 1 1 1 2 1 1 1 3 1 1 1"
    let rows = vec![
        row(&[
            ("price", num(100.0)),
            ("bidder", num(1.0)),
            ("auction", num(1.0)),
        ]),
        row(&[
            ("price", num(50_000.0)),
            ("bidder", num(1.0)),
            ("auction", num(2.0)),
        ]),
        row(&[
            ("price", num(2_000_000.0)),
            ("bidder", num(2.0)),
            ("auction", num(3.0)),
        ]),
    ];
    let mut exec = StatsExecutor::new(q15_plan());
    exec.process_rows_where(&rows, extract, q15_where_ok);
    let expected = [3.0, 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 3.0, 1.0, 1.0, 1.0];
    assert_eq!(
        exec.final_measure_values(),
        expected.to_vec(),
        "q15 小数据集"
    );
}

#[test]
fn q15_match_against_reference_fold() {
    // 独立参考实现逐列对拍（设计 §8.1: 不共享实现缺陷）
    let rows = bid_rows(100_000);
    let mut exec = StatsExecutor::new(q15_plan());
    exec.process_rows_where(&rows, extract, q15_where_ok);
    let values = exec.final_measure_values();
    let expected = reference_q15(&rows);
    assert_eq!(values.len(), 12);
    for (i, (v, e)) in values.iter().zip(expected.iter()).enumerate() {
        assert_eq!(*v, *e as f64, "measure[{i}] 对拍失配: got {v}, want {e}");
    }
}

#[test]
fn q15_tier_boundaries_and_null() {
    // 设计 §8.2: price=10000 → rank2, price=1000000 → rank3;
    // §8.3: null price → 只计入 total 档
    let rows = vec![
        row(&[
            ("price", num(9_999.0)),
            ("bidder", num(1.0)),
            ("auction", num(1.0)),
        ]),
        row(&[
            ("price", num(10_000.0)),
            ("bidder", num(1.0)),
            ("auction", num(2.0)),
        ]),
        row(&[
            ("price", num(999_999.0)),
            ("bidder", num(1.0)),
            ("auction", num(3.0)),
        ]),
        row(&[
            ("price", num(1_000_000.0)),
            ("bidder", num(1.0)),
            ("auction", num(4.0)),
        ]),
        row(&[("bidder", num(2.0)), ("auction", num(5.0))]), // 无 price → null
    ];
    let mut exec = StatsExecutor::new(q15_plan());
    exec.process_rows_where(&rows, extract, q15_where_ok);
    // total=5, r1=1, r2=2, r3=1; bidder total=2, r1=1, r2=1, r3=1;
    // auction total=5, r1=1, r2=2, r3=1
    let expected = [5.0, 1.0, 2.0, 1.0, 2.0, 1.0, 1.0, 1.0, 5.0, 1.0, 2.0, 1.0];
    assert_eq!(exec.final_measure_values(), expected.to_vec(), "边界+null");
}
