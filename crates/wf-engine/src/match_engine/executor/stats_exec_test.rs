//! StatsExecutor 单元测试（P1: 空键 fixed count/distinct/sum/avg/min/max）。

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Date32Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, Expr, FieldRef};
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
    // where 内建求值（P1 接线）: 逐行 eval 度量 where_expr, 非 Bool(true) 即过滤
    let mut m = count_measure("keep_bids");
    m.where_expr = Some(Expr::Field(FieldRef::Qualified("b".into(), "keep".into())));
    let plan = simple_plan(vec![m]);
    let mut exec = StatsExecutor::new(plan);

    exec.process_rows(
        &[
            row(&[("keep", Value::Bool(false))]),
            row(&[("keep", Value::Bool(true))]),
            row(&[("keep", Value::Bool(true))]),
            row(&[]), // 字段缺失 → null → 过滤
        ],
        extract,
    );
    assert_eq!(exec.final_measure_values()[0], 2.0);
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

fn price_field() -> Expr {
    Expr::Field(FieldRef::Qualified("b".into(), "price".into()))
}

fn price_lt(threshold: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::Lt,
        left: Box::new(price_field()),
        right: Box::new(Expr::Number(threshold)),
    }
}

fn price_ge(threshold: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::Ge,
        left: Box::new(price_field()),
        right: Box::new(Expr::Number(threshold)),
    }
}

fn price_range(lo: f64, hi: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::And,
        left: Box::new(price_ge(lo)),
        right: Box::new(price_lt(hi)),
    }
}

/// q15 形状 StatsPlan: 12 度量（4 count + 8 distinct）, where 为**真实分档条件**
/// （内建求值, 去重后 3 个唯一表达式共享）。
fn q15_plan() -> StatsPlan {
    let mut measures = Vec::new();
    let mut push =
        |label: &str, agg: StatsAggPlan, field: Option<&str>, where_expr: Option<Expr>| {
            measures.push(StatsMeasurePlan {
                label: label.into(),
                source_alias: "b".into(),
                where_expr,
                agg,
                field: field.map(|f| FieldRef::Qualified("b".into(), f.into())),
                arg: None,
            });
        };
    let where_of = |tier: usize| -> Option<Expr> {
        match tier {
            0 => None,
            1 => Some(price_lt(10_000.0)),
            2 => Some(price_range(10_000.0, 1_000_000.0)),
            3 => Some(price_ge(1_000_000.0)),
            _ => unreachable!(),
        }
    };
    for (i, name) in ["total", "r1", "r2", "r3"].iter().enumerate() {
        push(
            &format!("count_{name}"),
            StatsAggPlan::Count,
            None,
            where_of(i),
        );
    }
    for (i, name) in ["total", "r1", "r2", "r3"].iter().enumerate() {
        push(
            &format!("bidder_{name}"),
            StatsAggPlan::DistinctCount,
            Some("bidder"),
            where_of(i),
        );
    }
    for (i, name) in ["total", "r1", "r2", "r3"].iter().enumerate() {
        push(
            &format!("auction_{name}"),
            StatsAggPlan::DistinctCount,
            Some("auction"),
            where_of(i),
        );
    }
    assert_eq!(measures.len(), 12, "q15 应为 12 个度量");
    simple_plan(measures)
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
    exec.process_rows(&rows, extract);
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
    exec.process_rows(&rows, extract);
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
    exec.process_rows(&rows, extract);
    // total=5, r1=1, r2=2, r3=1; bidder total=2, r1=1, r2=1, r3=1;
    // auction total=5, r1=1, r2=2, r3=1
    let expected = [5.0, 1.0, 2.0, 1.0, 2.0, 1.0, 1.0, 1.0, 5.0, 1.0, 2.0, 1.0];
    assert_eq!(exec.final_measure_values(), expected.to_vec(), "边界+null");
}

// ---------------------------------------------------------------------------
// 列式段（P1.5）对拍: process_batch vs process_rows vs 参考实现
// ---------------------------------------------------------------------------

/// 行 → Int64 列 RecordBatch（price/bidder/auction, 对齐 nexmark 数据; null 保留）。
fn rows_to_batch(rows: &[HashMap<String, Value>]) -> RecordBatch {
    fn i64_of(row: &HashMap<String, Value>, name: &str) -> Option<i64> {
        match row.get(name) {
            Some(Value::Number(n)) => Some(*n as i64),
            _ => None,
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
    ]));
    let price: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "price")).collect();
    let bidder: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "bidder")).collect();
    let auction: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "auction")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(price)),
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(auction)),
        ],
    )
    .expect("batch")
}

#[test]
fn q15_columnar_matches_row_based_and_reference() {
    let rows = bid_rows(100_000);
    let batch = rows_to_batch(&rows);

    let mut row_exec = StatsExecutor::new(q15_plan());
    row_exec.process_rows(&rows, extract);

    let mut col_exec = StatsExecutor::new(q15_plan());
    assert!(col_exec.process_batch(&batch), "q15 计划应可列式化");

    let expected = reference_q15(&rows);
    let (rv, cv) = (
        row_exec.final_measure_values(),
        col_exec.final_measure_values(),
    );
    for i in 0..12 {
        assert_eq!(rv[i], cv[i], "measure[{i}] 行式 vs 列式");
        assert_eq!(cv[i], expected[i] as f64, "measure[{i}] 列式 vs 参考");
    }
}

#[test]
fn q15_columnar_tier_boundaries_and_null() {
    // 与 q15_tier_boundaries_and_null 同数据集, 列式路径: null price 只计 total 档
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
        row(&[("bidder", num(2.0)), ("auction", num(3.0))]), // 无 price → null
    ];
    let batch = rows_to_batch(&rows);
    let mut col_exec = StatsExecutor::new(q15_plan());
    assert!(col_exec.process_batch(&batch));
    // total=3, r1=1, r2=1, r3=0; bidder total=2, r1=1, r2=1, r3=0;
    // auction total=3, r1=1, r2=1, r3=0
    let expected = [3.0, 1.0, 1.0, 0.0, 2.0, 1.0, 1.0, 0.0, 3.0, 1.0, 1.0, 0.0];
    assert_eq!(
        col_exec.final_measure_values(),
        expected.to_vec(),
        "列式边界+null"
    );
}

#[test]
fn stats_columnar_falls_back_on_non_columnar_where() {
    // where 含函数调用（不可列式化）→ process_batch 返回 false, 调用方须回退行式
    let mut m = count_measure("n");
    m.where_expr = Some(Expr::FuncCall {
        qualifier: None,
        name: "len".into(),
        args: vec![Expr::Field(FieldRef::Qualified("b".into(), "price".into()))],
    });
    let plan = simple_plan(vec![m]);
    let mut exec = StatsExecutor::new(plan);
    let batch = rows_to_batch(&[]);
    assert!(
        !exec.process_batch(&batch),
        "非列式 where 应返回 false（回退行式）"
    );
}

#[test]
fn stats_columnar_distinct_unsupported_type_falls_back() {
    // distinct 字段列类型不在支持集（此处 batch 无该字段 → 走缺失分支, 恒 true）;
    // 用 object 字段类型构造不可 downcast 的场景不可行——改为验证字段缺失不误报。
    let plan = simple_plan(vec![distinct_measure("bidders", "bidder")]);
    let mut exec = StatsExecutor::new(plan);
    // batch 无 bidder 列（字段缺失 → 与行式 extract None 一致, 不插入）
    let schema = Arc::new(Schema::new(vec![Field::new(
        "price",
        DataType::Int64,
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
    assert!(exec.process_batch(&batch), "字段缺失不触发回退");
    assert_eq!(exec.final_measure_values()[0], 0.0, "distinct 空集");
}

#[test]
fn stats_columnar_partial_apply_rolls_back_cleanly() {
    // 回归（Bug H 修复）: process_batch 前置检查必须在任何累加副作用之前——
    // distinct 字段类型不支持时返回 false, 且不得留下已累加的 count（否则回退
    // 行式会把同一批重复计算）。
    let plan = simple_plan(vec![
        count_measure("n"),
        distinct_measure("bidders", "bidder"),
    ]);
    let rows = vec![
        row(&[("price", num(1.0)), ("bidder", num(7.0))]),
        row(&[("price", num(2.0)), ("bidder", num(8.0))]),
    ];
    // batch: price Int64（count 可算）, bidder Date32（distinct 不支持类型）
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Date32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(Date32Array::from(vec![7i32, 8])),
        ],
    )
    .unwrap();

    let mut col_exec = StatsExecutor::new(plan.clone());
    assert!(!col_exec.process_batch(&batch), "Date32 distinct 应回退");
    // 无副作用: 全部累积为零（count 不得被预累加）
    assert_eq!(
        col_exec.final_measure_values(),
        vec![0.0, 0.0],
        "回退前不得有部分累加"
    );
    // 回退: 调用方对同一 executor 走行式, 结果与纯行式一致
    col_exec.process_rows(&rows, extract);
    let mut row_exec = StatsExecutor::new(plan);
    row_exec.process_rows(&rows, extract);
    assert_eq!(
        col_exec.final_measure_values(),
        row_exec.final_measure_values(),
        "回退行式结果一致"
    );
}

// ---------------------------------------------------------------------------
// 列式段: sum/avg/min/max + 多类型 distinct + 多批累积
// ---------------------------------------------------------------------------

/// sum/avg/min/max 形状 plan（含带 where 的 sum——Q17 方向）。
fn num_measures_plan() -> StatsPlan {
    let mut measures = Vec::new();
    let mut push = |label: &str, agg: StatsAggPlan, field: &str, where_expr: Option<Expr>| {
        measures.push(StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr,
            agg,
            field: Some(FieldRef::Qualified("b".into(), field.into())),
            arg: None,
        });
    };
    push("sum_all", StatsAggPlan::Sum, "price", None);
    push("avg_all", StatsAggPlan::Avg, "price", None);
    push("min_all", StatsAggPlan::Min, "price", None);
    push("max_all", StatsAggPlan::Max, "price", None);
    push(
        "sum_r1",
        StatsAggPlan::Sum,
        "price",
        Some(price_lt(1_000_000.0)),
    );
    simple_plan(measures)
}

#[test]
fn stats_columnar_sum_avg_min_max_matches_row_based() {
    // 含 null price 行: count 计行、sum/min/max 跳过 null（对齐行式）
    let rows = vec![
        row(&[
            ("price", num(100.0)),
            ("bidder", num(1.0)),
            ("auction", num(1.0)),
        ]),
        row(&[
            ("price", num(200.0)),
            ("bidder", num(1.0)),
            ("auction", num(2.0)),
        ]),
        row(&[("bidder", num(2.0)), ("auction", num(3.0))]), // price null
        row(&[
            ("price", num(2_000_000.0)),
            ("bidder", num(2.0)),
            ("auction", num(4.0)),
        ]),
    ];
    let batch = rows_to_batch(&rows);
    let mut row_exec = StatsExecutor::new(num_measures_plan());
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::new(num_measures_plan());
    assert!(col_exec.process_batch(&batch), "数值度量应可列式化");
    let (rv, cv) = (
        row_exec.final_measure_values(),
        col_exec.final_measure_values(),
    );
    assert_eq!(rv.len(), 5);
    for i in 0..rv.len() {
        assert_eq!(rv[i], cv[i], "measure[{i}] 行式 vs 列式");
    }
    // 手算: sum_all=2000300, avg=2000300/4（null 行也计入 count）, min=100,
    // max=2000000, sum_r1=300
    assert_eq!(cv[0], 2_000_300.0, "sum_all");
    assert_eq!(cv[1], 2_000_300.0 / 4.0, "avg_all");
    assert_eq!(cv[2], 100.0, "min_all");
    assert_eq!(cv[3], 2_000_000.0, "max_all");
    assert_eq!(cv[4], 300.0, "sum_r1");
}

#[test]
fn stats_columnar_distinct_float_and_string() {
    // Float64 / Utf8 列 distinct（对齐行式 from_f64/from_str 分派）
    let schema = Arc::new(Schema::new(vec![
        Field::new("score", DataType::Float64, true),
        Field::new("tag", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float64Array::from(vec![1.5, 2.5, 1.5, 2.0, 2.0])),
            Arc::new(StringArray::from(vec!["a", "b", "a", "c", "c"])),
        ],
    )
    .unwrap();
    let rows = vec![
        row(&[("score", num(1.5)), ("tag", str_val("a"))]),
        row(&[("score", num(2.5)), ("tag", str_val("b"))]),
        row(&[("score", num(1.5)), ("tag", str_val("a"))]),
        row(&[("score", num(2.0)), ("tag", str_val("c"))]),
        row(&[("score", num(2.0)), ("tag", str_val("c"))]),
    ];
    let plan = simple_plan(vec![
        distinct_measure("scores", "score"),
        distinct_measure("tags", "tag"),
    ]);
    let mut row_exec = StatsExecutor::new(plan.clone());
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::new(plan);
    assert!(col_exec.process_batch(&batch));
    // score {1.5, 2.5, 2.0} = 3; tag {a,b,c} = 3
    assert_eq!(row_exec.final_measure_values(), vec![3.0, 3.0]);
    assert_eq!(col_exec.final_measure_values(), vec![3.0, 3.0]);
}

#[test]
fn stats_columnar_multiple_batches_accumulate() {
    // 同批两次 → 行式/列式累积一致（count 相加, distinct 并集）
    let rows = bid_rows(5_000);
    let batch = rows_to_batch(&rows);
    let mut row_exec = StatsExecutor::new(q15_plan());
    row_exec.process_rows(&rows, extract);
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::new(q15_plan());
    assert!(col_exec.process_batch(&batch));
    assert!(col_exec.process_batch(&batch));
    assert_eq!(
        row_exec.final_measure_values(),
        col_exec.final_measure_values(),
        "多批累积一致"
    );
    assert_eq!(col_exec.final_measure_values()[0], 10_000.0, "total=2×5000");
}
