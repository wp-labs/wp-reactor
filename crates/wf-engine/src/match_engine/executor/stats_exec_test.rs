//! StatsExecutor 单元测试（P1: 空键 fixed count/distinct/sum/avg/min/max）。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    BooleanArray, Date32Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

use crate::match_engine::Value;
use crate::match_engine::executor::stats_exec::{
    RowFieldLayout, RowFields, StatsAccum, StatsBucketAccs, StatsExecutor, TopEntry,
};
use crate::match_engine::executor::{
    accumulate_column_row, accumulate_soa, measure_values_soa, NumericSoALayout,
};

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

fn last_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Last,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn top_measure(label: &str, field: &str, n: u64) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Top,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: Some(n),
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

/// 行字段列数组按名取值（P5 紧凑化测试辅助; `names` = 提取列序）。
fn row_val(row: &std::sync::Arc<RowFields>, names: &[String], name: &str) -> Option<Value> {
    names
        .iter()
        .position(|n| n == name)
        .and_then(|i| row.value_at(i))
}

/// batch schema 字段名排序——与执行器 None 子集提取列序一致（行式/列式同序）。
fn sorted_schema_names(batch: &RecordBatch) -> Vec<String> {
    let mut ns: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    ns.sort();
    ns
}

/// rows_to_batch 的排序字段名（price/bidder/auction → auction, bidder, price）。
fn sorted_bid_names() -> Vec<String> {
    ["auction", "bidder", "price"]
        .into_iter()
        .map(String::from)
        .collect()
}

/// 全字段子集（等价 None 全列, 但带确定列序——last/top 度量值提取需要子集,
/// 生产经 spawn 恒有子集）。
fn full_bid_subset() -> Arc<HashSet<String>> {
    Arc::new(sorted_bid_names().into_iter().collect())
}

/// 输入分区分片归并（2026-08-24 q15）: 两片独立累加（各取一半行, 片间
/// distinct 值有重叠）→ `take_partial` + `merge_partial` → 最终度量值与单实例
/// 喂全部行**完全一致**（count 相加 / sum 相加 / min·max 取极值 / distinct 集
/// union 去重）。
#[test]
fn stats_input_shard_merge_matches_single() {
    let minmax = |agg: StatsAggPlan| StatsMeasurePlan {
        label: "m".into(),
        source_alias: "b".into(),
        where_expr: None,
        agg,
        field: Some(FieldRef::Qualified("b".into(), "price".into())),
        arg: None,
    };
    let plan = simple_plan(vec![
        count_measure("count"),
        distinct_measure("bidders", "bidder"),
        distinct_measure("auctions", "auction"),
        sum_measure("sum_price", "price"),
        minmax(StatsAggPlan::Min),
        minmax(StatsAggPlan::Max),
    ]);
    let rows: Vec<HashMap<String, Value>> = vec![
        row(&[
            ("bidder", num(1.0)),
            ("auction", num(10.0)),
            ("price", num(5.0)),
        ]),
        row(&[
            ("bidder", num(1.0)),
            ("auction", num(11.0)),
            ("price", num(7.0)),
        ]),
        row(&[
            ("bidder", num(2.0)),
            ("auction", num(10.0)),
            ("price", num(3.0)),
        ]),
        row(&[
            ("bidder", num(2.0)),
            ("auction", num(12.0)),
            ("price", num(9.0)),
        ]),
    ];

    // 单实例（基准）: 全部行。
    let mut single = StatsExecutor::new(plan.clone());
    single.process_rows(&rows, extract);
    let expect = single.final_measure_values();
    assert_eq!(expect[0], 4.0, "count");
    assert_eq!(expect[1], 2.0, "distinct bidder {{1,2}}");
    assert_eq!(expect[2], 3.0, "distinct auction {{10,11,12}}");
    assert_eq!(expect[3], 24.0, "sum price");
    assert_eq!(expect[4], 3.0, "min price");
    assert_eq!(expect[5], 9.0, "max price");

    // 两片（输入分区: 片 A = 行 0/2, 片 B = 行 1/3; bidder 1/2 跨片重复）。
    let mut a = StatsExecutor::new(plan.clone());
    a.process_rows(&[rows[0].clone(), rows[2].clone()], extract);
    let mut b = StatsExecutor::new(plan);
    b.process_rows(&[rows[1].clone(), rows[3].clone()], extract);
    let (buckets, count) = b.take_partial();
    a.merge_partial(buckets, count);
    assert_eq!(
        a.final_measure_values(),
        expect,
        "分片归并必须与单实例逐值一致（count/sum/min/max/distinct）"
    );

    // take_partial 后片 B 已重置: 再累加不叠加旧值。
    b.process_rows(&[rows[0].clone()], extract);
    assert_eq!(b.final_measure_values()[0], 1.0, "take_partial 重置窗口");
}

/// 输入分区分片归并——**列式路径**（2026-08-24 q15 生产路径）: 用
/// `process_batch_rows`（where mask + distinct 列式段）喂两片, `take_partial`
/// + `merge_partial` == 单实例喂全批逐值一致。
#[test]
fn stats_input_shard_merge_columnar_matches_single() {
    let plan = simple_plan(vec![
        count_measure("count"),
        distinct_measure("bidders", "bidder"),
        distinct_measure("auctions", "auction"),
        sum_measure("sum_price", "price"),
    ]);
    // 6 行覆盖 3 价格档（price 列驱动 where 过滤语义, 此处无 where）。
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                100, 50_000, 2_000_000, 50, 5_000, 999_999,
            ])),
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 3, 3])),
            Arc::new(Int64Array::from(vec![10, 11, 12, 10, 13, 11])),
        ],
    )
    .unwrap();

    // 单实例（基准）: 全批列式。
    let mut single = StatsExecutor::new(plan.clone());
    assert!(single.process_batch_rows(&batch, None), "列式前置必须通过");
    let expect = single.final_measure_values();
    assert_eq!(expect[0], 6.0, "count");
    assert_eq!(expect[1], 3.0, "distinct bidder {{1,2,3}}");
    assert_eq!(expect[2], 4.0, "distinct auction {{10,11,12,13}}");
    assert_eq!(expect[3], 3_055_149.0, "sum price");

    // 两片（输入行号分区: 偶行 vs 奇行; bidder 1/2/3 与 auction 跨片重叠）。
    let mut a = StatsExecutor::new(plan.clone());
    assert!(a.process_batch_rows(&batch, Some(&[0u32, 2, 4])));
    let mut b = StatsExecutor::new(plan);
    assert!(b.process_batch_rows(&batch, Some(&[1u32, 3, 5])));
    let (buckets, count) = b.take_partial();
    a.merge_partial(buckets, count);
    assert_eq!(
        a.final_measure_values(),
        expect,
        "列式分片归并必须与单实例逐值一致"
    );
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

/// 同 rows_to_batch, 但指定行（`null_price_rows` 集合中的行号）price 列为 null——
/// 列式 last 字段缺失语义的模拟（行式 = 缺 price 键）。
fn rows_to_batch_with_null_price(rows: &[HashMap<String, Value>]) -> RecordBatch {
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

// ---------------------------------------------------------------------------
// P2 复合键分组（group by）: 字段键 / 复合键 / bucket / tier / 列式对拍
// ---------------------------------------------------------------------------

use crate::match_engine::match_engine::ScopeKey;

/// 带键的 plan（keys 为桶键表达式）。
fn keyed_plan(keys: Vec<Expr>, measures: Vec<StatsMeasurePlan>) -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
        keys,
        output_shape: StatsOutputShapePlan::Rows,
        measures,
        tracked_bind_fields: HashMap::new(),
    }
}

fn field_key(alias: &str, name: &str) -> Expr {
    Expr::Field(FieldRef::Qualified(alias.into(), name.into()))
}

#[test]
fn stats_group_by_field_buckets() {
    // Q12 形状: group by (b.bidder) { count } —— 单字段键, 桶按 ScopeKey 升序输出
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let rows = vec![
        row(&[("bidder", num(1.0))]),
        row(&[("bidder", num(1.0))]),
        row(&[("bidder", num(2.0))]),
        row(&[("bidder", num(3.0))]),
    ];
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(&rows, extract);
    let buckets = exec.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 3, "3 个 bidder 桶");
    assert_eq!(buckets[0].0, ScopeKey::Int(1));
    assert_eq!(buckets[0].1, vec![2.0]);
    assert_eq!(buckets[1].0, ScopeKey::Int(2));
    assert_eq!(buckets[1].1, vec![1.0]);
    assert_eq!(buckets[2].0, ScopeKey::Int(3));
    assert_eq!(buckets[2].1, vec![1.0]);
}

#[test]
fn stats_group_by_compound_key_pairs() {
    // 复合键 (b.bidder, b.auction) → ScopeKey::Pair; 桶按 Pair 字典序升序
    let plan = keyed_plan(
        vec![field_key("b", "bidder"), field_key("b", "auction")],
        vec![count_measure("n")],
    );
    let rows = vec![
        row(&[("bidder", num(1.0)), ("auction", num(10.0))]),
        row(&[("bidder", num(1.0)), ("auction", num(10.0))]),
        row(&[("bidder", num(1.0)), ("auction", num(11.0))]),
        row(&[("bidder", num(2.0)), ("auction", num(10.0))]),
    ];
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(&rows, extract);
    let buckets = exec.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 3);
    // 桶序: (1,10) → (1,11) → (2,10)
    assert_eq!(
        buckets[0].0,
        ScopeKey::Pair(Box::new(ScopeKey::Int(1)), Box::new(ScopeKey::Int(10)))
    );
    assert_eq!(buckets[0].1, vec![2.0]);
    assert_eq!(
        buckets[1].0,
        ScopeKey::Pair(Box::new(ScopeKey::Int(1)), Box::new(ScopeKey::Int(11)))
    );
    assert_eq!(buckets[1].1, vec![1.0]);
    assert_eq!(
        buckets[2].0,
        ScopeKey::Pair(Box::new(ScopeKey::Int(2)), Box::new(ScopeKey::Int(10)))
    );
}

#[test]
fn stats_group_by_bucket_day() {
    // bucket(b.dateTime, 'day'): 时间按天取整为桶键（Q16/Q17 形状）
    let day = 86_400_000_000_000i64;
    let bucket_key = Expr::FuncCall {
        qualifier: None,
        name: "bucket".into(),
        args: vec![field_key("b", "dateTime"), Expr::StringLit("day".into())],
    };
    let plan = keyed_plan(vec![bucket_key], vec![count_measure("n")]);
    let rows = vec![
        row(&[("dateTime", num(day as f64 + 1.0))]),
        row(&[("dateTime", num(day as f64 + 1000.0))]),
        row(&[("dateTime", num(2.0 * day as f64 + 1.0))]),
    ];
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(&rows, extract);
    let buckets = exec.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 2, "两个 day 桶");
    assert_eq!(buckets[0].0, ScopeKey::Int(day));
    assert_eq!(buckets[0].1, vec![2.0]);
    assert_eq!(buckets[1].0, ScopeKey::Int(2 * day));
    assert_eq!(buckets[1].1, vec![1.0]);
}

#[test]
fn stats_group_by_tier_bucket_index() {
    // tier(b.price, 10000, 1000000): 区间桶索引（Q16/Q17 分档键）
    let tier_key = Expr::FuncCall {
        qualifier: None,
        name: "tier".into(),
        args: vec![
            field_key("b", "price"),
            Expr::Number(10_000.0),
            Expr::Number(1_000_000.0),
        ],
    };
    let plan = keyed_plan(vec![tier_key], vec![count_measure("n")]);
    let rows = vec![
        row(&[("price", num(100.0))]),       // tier 0
        row(&[("price", num(50_000.0))]),    // tier 1
        row(&[("price", num(2_000_000.0))]), // tier 2
        row(&[("price", num(5_000.0))]),     // tier 0
    ];
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(&rows, extract);
    let buckets = exec.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 3);
    assert_eq!(buckets[0].0, ScopeKey::Int(0));
    assert_eq!(buckets[0].1, vec![2.0]);
    assert_eq!(buckets[1].0, ScopeKey::Int(1));
    assert_eq!(buckets[1].1, vec![1.0]);
    assert_eq!(buckets[2].0, ScopeKey::Int(2));
    assert_eq!(buckets[2].1, vec![1.0]);
}

#[test]
fn stats_group_by_null_key_skips_row() {
    // 键缺失/null → 行跳过（对齐 CEP key 语义）
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let rows = vec![
        row(&[("bidder", num(1.0))]),
        row(&[]), // 键缺失
    ];
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(&rows, extract);
    assert_eq!(exec.final_measure_values()[0], 1.0, "键缺失行跳过");
}

#[test]
fn stats_group_by_columnar_matches_row_based() {
    // 带 key 批处理（列式桶键 + mask）vs 行式: 逐桶对拍
    let plan = keyed_plan(vec![field_key("b", "bidder")], vec![count_measure("n")]);
    let rows = vec![
        row(&[("bidder", num(7.0))]),
        row(&[("bidder", num(7.0))]),
        row(&[("bidder", num(8.0))]),
        row(&[("bidder", num(9.0))]),
    ];
    let batch = rows_to_batch(&rows); // price/bidder/auction 列
    let mut row_exec = StatsExecutor::new(plan.clone());
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::new(plan);
    assert!(col_exec.process_batch(&batch), "字段键应可列式化");
    assert_eq!(
        row_exec.final_measure_values_by_bucket(),
        col_exec.final_measure_values_by_bucket(),
        "行式/列式逐桶一致"
    );
}

#[test]
fn stats_group_by_function_key_falls_back_row() {
    // 桶键含函数（bucket）→ process_batch 返回 false（回退行式）, 语义等价
    let day = 86_400_000_000_000i64;
    let bucket_key = Expr::FuncCall {
        qualifier: None,
        name: "bucket".into(),
        args: vec![field_key("b", "dateTime"), Expr::StringLit("day".into())],
    };
    let plan = keyed_plan(vec![bucket_key], vec![count_measure("n")]);
    let rows = vec![row(&[("dateTime", num(day as f64 + 1.0))])];
    // batch 无 dateTime 列（rows_to_batch 只有 price/bidder/auction）→ 不可列式 → false
    let batch = rows_to_batch(&rows);
    let mut exec = StatsExecutor::new(plan);
    assert!(
        !exec.process_batch(&batch),
        "函数桶键应回退行式（调用方负责）"
    );
    // 行式仍正确
    exec.process_rows(&rows, extract);
    assert_eq!(exec.final_measure_values_by_bucket()[0].1, vec![1.0]);
}

// ---------------------------------------------------------------------------
// P5+ 复合键扁平哈希（列式无 Box 分配路径）
// ---------------------------------------------------------------------------

#[test]
fn stats_composite_key_hash_flat_matches_tree() {
    // 复合键优化契约: 列式叶数组哈希 == 行式完整树哈希——同一逻辑键两条查找
    // 路径（comps_hash / scope_key_hash）必落同桶。字节级同构: N-1 个 Pair tag
    // 前缀 + 每叶 tag/payload + 0x1f 分隔。
    use crate::match_engine::executor::stats_exec::{
        comps_hash, scope_key_from_comps, scope_key_hash,
    };
    let cases: Vec<Vec<ScopeKey>> = vec![
        vec![ScopeKey::Int(42)],
        vec![ScopeKey::Int(7), ScopeKey::Int(8)],
        vec![ScopeKey::Int(1), ScopeKey::Int(2), ScopeKey::Int(3)],
        vec![
            ScopeKey::Int(1),
            ScopeKey::Int(2),
            ScopeKey::Int(3),
            ScopeKey::Int(4),
        ],
        vec![ScopeKey::Float(1.5f64.to_bits()), ScopeKey::Int(9)],
        vec![ScopeKey::Str("cat".into()), ScopeKey::Int(10)],
        vec![
            ScopeKey::Str("a".into()),
            ScopeKey::Float((-0.0f64).to_bits()),
            ScopeKey::Int(3),
        ],
    ];
    for comps in cases {
        let tree = scope_key_from_comps(&comps); // 左深 Pair 链
        assert_eq!(
            comps_hash(&comps),
            scope_key_hash(&tree),
            "comps={comps:?} tree={tree:?}"
        );
    }
}

#[test]
fn stats_composite_key_mixed_types_columnar_matches_row_based() {
    // 复合键 (bidder: Int64, auction: Utf8) 混合类型——列式扁平键 vs 行式树键
    // 逐桶对拍（两路径哈希同值 + 碰撞链完整比较消歧）。
    let plan = keyed_plan(
        vec![field_key("b", "bidder"), field_key("b", "auction")],
        vec![count_measure("n")],
    );
    let rows = vec![
        row(&[
            ("bidder", num(1.0)),
            ("auction", str_val("a")),
            ("price", num(10.0)),
        ]),
        row(&[
            ("bidder", num(1.0)),
            ("auction", str_val("a")),
            ("price", num(20.0)),
        ]),
        row(&[
            ("bidder", num(1.0)),
            ("auction", str_val("b")),
            ("price", num(30.0)),
        ]),
        row(&[
            ("bidder", num(2.0)),
            ("auction", str_val("a")),
            ("price", num(40.0)),
        ]),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(StringArray::from(vec!["a", "a", "b", "a"])),
        ],
    )
    .unwrap();
    let mut row_exec = StatsExecutor::new(plan.clone());
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::new(plan);
    assert!(col_exec.process_batch(&batch), "字段键应可列式化");
    assert_eq!(
        row_exec.final_measure_values_by_bucket(),
        col_exec.final_measure_values_by_bucket(),
        "混合类型复合键行式/列式逐桶一致"
    );
}

#[test]
fn stats_composite_key_three_field_columnar_matches_row_based() {
    // 3 键 (price, bidder, auction) 左深 Pair(Pair(p,b),a)——列式扁平键 vs 行式
    // 树键逐桶对拍（>2 键的 comps_match 递归边界）。
    let plan = keyed_plan(
        vec![
            field_key("b", "price"),
            field_key("b", "bidder"),
            field_key("b", "auction"),
        ],
        vec![count_measure("n")],
    );
    let rows = vec![
        row(&[
            ("price", num(10.0)),
            ("bidder", num(1.0)),
            ("auction", num(100.0)),
        ]),
        row(&[
            ("price", num(10.0)),
            ("bidder", num(1.0)),
            ("auction", num(100.0)),
        ]),
        row(&[
            ("price", num(10.0)),
            ("bidder", num(2.0)),
            ("auction", num(100.0)),
        ]),
        row(&[
            ("price", num(20.0)),
            ("bidder", num(1.0)),
            ("auction", num(100.0)),
        ]),
    ];
    let batch = rows_to_batch(&rows);
    let mut row_exec = StatsExecutor::new(plan.clone());
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::new(plan);
    assert!(col_exec.process_batch(&batch), "字段键应可列式化");
    assert_eq!(
        row_exec.final_measure_values_by_bucket(),
        col_exec.final_measure_values_by_bucket(),
        "3 键行式/列式逐桶一致"
    );
    let buckets = col_exec.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 3);
    assert_eq!(
        buckets[0].0,
        ScopeKey::Pair(
            Box::new(ScopeKey::Pair(
                Box::new(ScopeKey::Int(10)),
                Box::new(ScopeKey::Int(1))
            )),
            Box::new(ScopeKey::Int(100)),
        )
    );
    assert_eq!(buckets[0].1, vec![2.0]);
}

#[test]
fn stats_composite_key_mixed_paths_same_bucket() {
    // 同一 executor 先列式批、再行式行——两查找路径（扁平键 vs 树键）必须落
    // **同一桶**（哈希同值 + 链内完整比较消歧）; 计数跨路径累加, 不产生重复桶。
    let plan = keyed_plan(
        vec![field_key("b", "bidder"), field_key("b", "auction")],
        vec![count_measure("n")],
    );
    let rows = vec![
        row(&[
            ("bidder", num(1.0)),
            ("auction", num(10.0)),
            ("price", num(1.0)),
        ]),
        row(&[
            ("bidder", num(1.0)),
            ("auction", num(10.0)),
            ("price", num(2.0)),
        ]),
        row(&[
            ("bidder", num(2.0)),
            ("auction", num(10.0)),
            ("price", num(3.0)),
        ]),
    ];
    let batch = rows_to_batch(&rows);
    let mut exec = StatsExecutor::new(plan);
    assert!(exec.process_batch(&batch), "列式路径");
    exec.process_rows(&rows, extract); // 行式路径同一桶
    let buckets = exec.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 2, "不产生重复桶");
    assert_eq!(buckets[0].1, vec![4.0], "(1,10): 列式 2 + 行式 2");
    assert_eq!(buckets[1].1, vec![2.0], "(2,10): 列式 1 + 行式 1");
}

// ---------------------------------------------------------------------------
// P2 分片行子集（Blocker 1 回归）: process_batch_rows(batch, Some(rows)) 只归并
// 行域内的行——否则每片处理全批, 每个键被 N 片各算一遍, close 重复输出 N 倍。
// ---------------------------------------------------------------------------

#[test]
fn stats_columnar_row_subset_empty_key_counts_domain_only() {
    // 空键 + 行域: 归并只对行域生效（domain mask 与 where mask 逐位 AND）
    let mut m = count_measure("cheap");
    m.where_expr = Some(price_lt(300.0));
    let plan = simple_plan(vec![
        m,
        sum_measure("sum_price", "price"),
        distinct_measure("bidders", "bidder"),
    ]);
    let rows = vec![
        row(&[("price", num(100.0)), ("bidder", num(1.0))]), // 域内, 便宜
        row(&[("price", num(200.0)), ("bidder", num(2.0))]), // 域外
        row(&[("price", num(300.0)), ("bidder", num(1.0))]), // 域内, 不便宜
        row(&[("price", num(400.0)), ("bidder", num(3.0))]), // 域外
    ];
    let batch = rows_to_batch(&rows);
    let mut exec = StatsExecutor::new(plan.clone());
    assert!(exec.process_batch_rows(&batch, Some(&[0, 2])));
    // 行域 {0,2}: cheap=1（仅行 0）, sum=400, distinct bidder={1}
    assert_eq!(exec.final_measure_values(), vec![1.0, 400.0, 1.0]);
    // 对照: 全批
    let mut full = StatsExecutor::new(plan);
    assert!(full.process_batch_rows(&batch, None));
    assert_eq!(full.final_measure_values(), vec![2.0, 1000.0, 3.0]);
}

#[test]
fn stats_columnar_row_subset_keyed_disjoint_partition() {
    // 分片核心回归: 带 key + 行域 → 每片只归并自己的行（不得重复整批）。
    // 模拟 2 片: 片 0 行 {0,1}（key 7）, 片 1 行 {2,3}（key 8/9）; 并集 = 全批。
    let keys = || vec![field_key("b", "bidder")];
    let plan = keyed_plan(keys(), vec![count_measure("n")]);
    let rows = vec![
        row(&[("bidder", num(7.0))]),
        row(&[("bidder", num(7.0))]),
        row(&[("bidder", num(8.0))]),
        row(&[("bidder", num(9.0))]),
    ];
    let batch = rows_to_batch(&rows);
    let mut shard0 = StatsExecutor::new(plan.clone());
    assert!(shard0.process_batch_rows(&batch, Some(&[0, 1])));
    let mut shard1 = StatsExecutor::new(plan);
    assert!(shard1.process_batch_rows(&batch, Some(&[2, 3])));
    // 片 0 只有 key 7; 片 1 只有 8/9 —— 无跨片重复
    let b0 = shard0.final_measure_values_by_bucket();
    assert_eq!(b0, vec![(ScopeKey::Int(7), vec![2.0])]);
    let b1 = shard1.final_measure_values_by_bucket();
    assert_eq!(
        b1,
        vec![(ScopeKey::Int(8), vec![1.0]), (ScopeKey::Int(9), vec![1.0]),]
    );
    // 并集 = 全批结果（无丢无重）
    let mut full = StatsExecutor::new(keyed_plan(keys(), vec![count_measure("n")]));
    assert!(full.process_batch_rows(&batch, None));
    let mut union = b0;
    union.extend(b1);
    assert_eq!(full.final_measure_values_by_bucket(), union);
}

#[test]
fn stats_columnar_keyed_precision_matches_empty_key_native() {
    // D7/D8 回归（review 发现）: 带 key 列式路径曾经 `column_value` 把 Int64
    // 转 `Value::Number(f64)`——≥2^53 的 id 被舍入（2^53 与 2^53+1 的 f64 相同）:
    // distinct 从 2 塌缩到 1、sum 从 2^54+1 变成 2^54, 与空键列式原生路径发散。
    // 修复后带 key 走原生列值（column_i128/column_distinct_key）。
    let schema = Arc::new(Schema::new(vec![
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                9_007_199_254_740_992i64, // 2^53
                9_007_199_254_740_993i64, // 2^53+1（f64 会舍入到 2^53）
            ])),
            Arc::new(Int64Array::from(vec![1i64, 1])),
            Arc::new(Int64Array::from(vec![100i64, 200])),
        ],
    )
    .unwrap();

    // 带 key: group by (auction); 度量 = distinct(bidder) + sum(bidder)
    let keyed_plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![
            distinct_measure("bidders", "bidder"),
            sum_measure("sum_bidder", "bidder"),
        ],
    );
    let mut keyed = StatsExecutor::new(keyed_plan);
    assert!(keyed.process_batch(&batch), "字段键应可列式化");
    let buckets = keyed.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 1, "单 auction 桶");
    assert_eq!(buckets[0].0, ScopeKey::Int(1));
    // distinct 原生 = 2（f64 路径会塌缩到 1）
    assert_eq!(buckets[0].1[0], 2.0, "≥2^53 的 distinct 不得 f64 化");
    // sum 精确断言（i128 累加器域）: 2^53 + (2^53+1) = 2^54+1
    let accs = keyed.window.find_bucket(&ScopeKey::Int(1)).unwrap();
    let StatsBucketAccs::Classic(accs) = accs else {
        panic!("distinct+sum 计划恒 Classic");
    };
    assert_eq!(
        accs[1].numeric().sum,
        9_007_199_254_740_992i128 + 9_007_199_254_740_993i128,
        "≥2^53 的 sum 不得 f64 舍入"
    );

    // 空键同批对照: 两列式路径同精度
    let mut empty = StatsExecutor::new(simple_plan(vec![
        distinct_measure("bidders", "bidder"),
        sum_measure("sum_bidder", "bidder"),
    ]));
    assert!(empty.process_batch(&batch));
    let ev = empty.final_measure_values();
    assert_eq!(ev[0], 2.0);
    assert_eq!(ev[1], buckets[0].1[1], "带 key 与空键列式 sum 同精度");
}

#[test]
fn stats_columnar_keyed_timestamp_distinct_native() {
    // review 发现: 旧 `column_value` 不含 Timestamp 分派——带 key 列式对
    // Timestamp 字段 distinct 全跳过（静默 0）; 修复后原生 i64（对齐
    // insert_distinct_column 的 from_i64, D7 口径）。
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let t0 = 1_700_000_000_000_000_000i64;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 1, 1])),
            Arc::new(TimestampNanosecondArray::from(vec![t0, t0 + 1, t0])),
        ],
    )
    .unwrap();
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![distinct_measure("days", "dateTime")],
    );
    let mut exec = StatsExecutor::new(plan);
    assert!(exec.process_batch(&batch));
    let buckets = exec.final_measure_values_by_bucket();
    assert_eq!(
        buckets[0].1,
        vec![2.0],
        "Timestamp 原生 distinct = 2（修复前为 0）"
    );
}

// ---------------------------------------------------------------------------
// P4 last/top 扩展度量（Q18/Q19）: last 保留最近合格行, top 保留 key DESC top-N;
// rich close（close_window_by_bucket_rows）按条目携带行字段供 yield 注入。
// ---------------------------------------------------------------------------

#[test]
fn stats_last_keeps_last_row_and_injects_fields() {
    // Q18 形状: group by (auction), last(price) —— 最近合格行的价格 + 行字段
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![last_measure("last_price", "price")],
    );
    let rows = vec![
        row(&[
            ("auction", num(1.0)),
            ("price", num(100.0)),
            ("bidder", num(7.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(200.0)),
            ("bidder", num(8.0)),
        ]),
        row(&[
            ("auction", num(2.0)),
            ("price", num(300.0)),
            ("bidder", num(9.0)),
        ]),
    ];
    let mut exec = StatsExecutor::with_row_fields(plan, Some(full_bid_subset()));
    exec.process_rows(&rows, extract);
    let buckets = exec.close_window_by_bucket_rows();
    assert_eq!(buckets.len(), 2, "2 个 auction 桶");
    // 桶序 ScopeKey 升序: auction 1 → 2
    assert_eq!(buckets[0].key, ScopeKey::Int(1));
    assert_eq!(buckets[0].measures[0].len(), 1, "last 单条目");
    let e = &buckets[0].measures[0][0];
    assert_eq!(e.measure_value, 200.0, "最后一条 bid 的价格");
    let rf = e.row_fields.as_ref().expect("last 携带行字段");
    let names = sorted_bid_names();
    assert_eq!(row_val(rf, &names, "price"), Some(num(200.0)));
    assert_eq!(row_val(rf, &names, "bidder"), Some(num(8.0)));
    assert_eq!(buckets[1].key, ScopeKey::Int(2));
    assert_eq!(buckets[1].measures[0][0].measure_value, 300.0);
}

#[test]
fn stats_top_keeps_top_n_desc() {
    // Q19 形状: group by (auction), top(2, price) —— key DESC 前 2 条, 各带行字段
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![top_measure("top_price", "price", 2)],
    );
    let rows = vec![
        row(&[
            ("auction", num(1.0)),
            ("price", num(100.0)),
            ("bidder", num(1.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(300.0)),
            ("bidder", num(2.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(200.0)),
            ("bidder", num(3.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(250.0)),
            ("bidder", num(4.0)),
        ]),
    ];
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(&rows, extract);
    let buckets = exec.close_window_by_bucket_rows();
    assert_eq!(buckets.len(), 1);
    let entries = &buckets[0].measures[0];
    assert_eq!(entries.len(), 2, "top-2");
    assert_eq!(entries[0].measure_value, 300.0, "rank1 = 最高价");
    assert_eq!(entries[1].measure_value, 250.0, "rank2");
    let names = sorted_bid_names();
    assert_eq!(
        row_val(entries[0].row_fields.as_ref().unwrap(), &names, "bidder"),
        Some(num(2.0))
    );
    assert_eq!(
        row_val(entries[1].row_fields.as_ref().unwrap(), &names, "bidder"),
        Some(num(4.0))
    );
}

#[test]
fn stats_top_tie_earlier_arrival_wins() {
    // 同 key 平局: 先到者保留在前（流有序下的确定性 tie-break）
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![top_measure("top_price", "price", 2)],
    );
    let rows = vec![
        row(&[
            ("auction", num(1.0)),
            ("price", num(100.0)),
            ("bidder", num(1.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(100.0)),
            ("bidder", num(2.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(50.0)),
            ("bidder", num(3.0)),
        ]),
    ];
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(&rows, extract);
    let entries = &exec.close_window_by_bucket_rows()[0].measures[0];
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].measure_value, 100.0);
    let names = sorted_bid_names();
    assert_eq!(
        row_val(entries[0].row_fields.as_ref().unwrap(), &names, "bidder"),
        Some(num(1.0)),
        "同价先到者 rank1"
    );
    assert_eq!(
        row_val(entries[1].row_fields.as_ref().unwrap(), &names, "bidder"),
        Some(num(2.0)),
        "同价后到者 rank2"
    );
}

#[test]
fn stats_last_top_where_filter_applies() {
    // where 过滤: last/top 只统计合格行
    let mut m = last_measure("last_high", "price");
    m.where_expr = Some(price_ge(150.0));
    let plan = keyed_plan(vec![field_key("b", "auction")], vec![m]);
    let rows = vec![
        row(&[
            ("auction", num(1.0)),
            ("price", num(100.0)),
            ("bidder", num(1.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(200.0)),
            ("bidder", num(2.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(150.0)),
            ("bidder", num(3.0)),
        ]),
    ];
    let mut exec = StatsExecutor::with_row_fields(plan, Some(full_bid_subset()));
    exec.process_rows(&rows, extract);
    let e = &exec.close_window_by_bucket_rows()[0].measures[0][0];
    assert_eq!(e.measure_value, 150.0, "最后合格行（price>=150）");
    let names = sorted_bid_names();
    assert_eq!(
        row_val(e.row_fields.as_ref().unwrap(), &names, "bidder"),
        Some(num(3.0))
    );
}

#[test]
fn stats_last_top_columnar_matches_row_based() {
    // 列式（带 key 逐行）vs 行式: 逐桶逐条目（值 + 行字段）一致
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![
            last_measure("last_price", "price"),
            top_measure("top_price", "price", 2),
        ],
    );
    let rows = vec![
        row(&[
            ("auction", num(1.0)),
            ("price", num(100.0)),
            ("bidder", num(1.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(300.0)),
            ("bidder", num(2.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(200.0)),
            ("bidder", num(3.0)),
        ]),
        row(&[
            ("auction", num(2.0)),
            ("price", num(50.0)),
            ("bidder", num(4.0)),
        ]),
    ];
    let batch = rows_to_batch(&rows);
    let mut row_exec = StatsExecutor::new(plan.clone());
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::new(plan);
    assert!(col_exec.process_batch(&batch), "字段键应可列式化");
    let (rb, cb) = (
        row_exec.close_window_by_bucket_rows(),
        col_exec.close_window_by_bucket_rows(),
    );
    assert_eq!(rb.len(), cb.len());
    for (r, c) in rb.iter().zip(cb.iter()) {
        assert_eq!(r.key, c.key);
        assert_eq!(r.measures.len(), c.measures.len());
        for (rm, cm) in r.measures.iter().zip(c.measures.iter()) {
            assert_eq!(rm.len(), cm.len(), "条目数一致");
            for (re, ce) in rm.iter().zip(cm.iter()) {
                assert_eq!(re.measure_value, ce.measure_value);
                assert_eq!(
                    re.row_fields.is_some(),
                    ce.row_fields.is_some(),
                    "行字段一致"
                );
                if let (Some(rf), Some(cf)) = (&re.row_fields, &ce.row_fields) {
                    let rv: Vec<Option<Value>> = rf.iter_values().collect();
                    let cv: Vec<Option<Value>> = cf.iter_values().collect();
                    assert_eq!(rv, cv, "行字段一致");
                }
            }
        }
    }
}

#[test]
fn stats_last_scalar_accessor_numeric() {
    // 标量访问器 final_measure_values_by_bucket 对 last 返回字段数值
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![last_measure("last_price", "price")],
    );
    let rows = vec![
        row(&[("auction", num(1.0)), ("price", num(100.0))]),
        row(&[("auction", num(1.0)), ("price", num(250.0))]),
    ];
    let mut exec = StatsExecutor::with_row_fields(plan, Some(full_bid_subset()));
    exec.process_rows(&rows, extract);
    assert_eq!(
        exec.final_measure_values_by_bucket(),
        vec![(ScopeKey::Int(1), vec![250.0])]
    );
}

#[test]
fn stats_top_full_cutoff_replaces_tail() {
    // top-N 满后: 高于门槛的 key 替换尾部, 低于/等于门槛的跳过（快速淘汰路径）——
    // 行序: 100, 200, 50, 150 → top-2 = [200, 150]
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![top_measure("top_price", "price", 2)],
    );
    let rows = vec![
        row(&[
            ("auction", num(1.0)),
            ("price", num(100.0)),
            ("bidder", num(1.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(200.0)),
            ("bidder", num(2.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(50.0)),
            ("bidder", num(3.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(150.0)),
            ("bidder", num(4.0)),
        ]),
    ];
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(&rows, extract);
    let entries = &exec.close_window_by_bucket_rows()[0].measures[0];
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].measure_value, 200.0);
    assert_eq!(entries[1].measure_value, 150.0, "150 替换 100");
    let names = sorted_bid_names();
    assert_eq!(
        row_val(entries[1].row_fields.as_ref().unwrap(), &names, "bidder"),
        Some(num(4.0))
    );
}

#[test]
fn stats_last_missing_field_keeps_row() {
    // 字段缺失语义（P4 review 补充）: last 的字段缺失仍保留整行（yield 可能读
    // 其它字段）; 度量值回退 0.0。行式/列式两条路径一致。
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![last_measure("last_price", "price")],
    );
    let rows = vec![
        row(&[
            ("auction", num(1.0)),
            ("price", num(100.0)),
            ("bidder", num(7.0)),
        ]),
        // 最后一条缺 price 字段（列式 = price 列 null）
        row(&[("auction", num(1.0)), ("bidder", num(8.0))]),
    ];
    // 行式路径
    let mut row_exec = StatsExecutor::new(plan.clone());
    row_exec.process_rows(&rows, extract);
    let buckets = row_exec.close_window_by_bucket_rows();
    let e = &buckets[0].measures[0][0];
    assert_eq!(e.measure_value, 0.0, "字段缺失 → 度量值 0.0");
    let rf = e.row_fields.as_ref().expect("last 保留整行");
    // 行式 None 子集列序 = 本行排序键（最后一条缺 price → [auction, bidder]）
    let row_names = ["auction", "bidder"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    assert_eq!(
        row_val(rf, &row_names, "bidder"),
        Some(num(8.0)),
        "字段缺失仍保留行字段"
    );
    // 列式路径: price 列 null 对应行
    let batch = rows_to_batch_with_null_price(&rows);
    let mut col_exec = StatsExecutor::new(plan);
    assert!(col_exec.process_batch(&batch), "应可列式化");
    let cb = col_exec.close_window_by_bucket_rows();
    let ce = &cb[0].measures[0][0];
    assert_eq!(ce.measure_value, 0.0);
    let col_names = sorted_schema_names(&batch); // [auction, bidder, price]
    assert_eq!(
        row_val(ce.row_fields.as_ref().unwrap(), &col_names, "bidder"),
        Some(num(8.0))
    );
}

#[test]
fn stats_top_zero_keeps_no_entries() {
    // top(0, ...) 边界（P4 review 补充）: 不保留任何条目, close 时该度量空条目
    // （任务层 n_records=0 → 不产出; 而非以前虚假的 scalar(0.0)）。
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![top_measure("top_price", "price", 0)],
    );
    let rows = vec![
        row(&[("auction", num(1.0)), ("price", num(300.0))]),
        row(&[("auction", num(1.0)), ("price", num(100.0))]),
    ];
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(&rows, extract);
    let buckets = exec.close_window_by_bucket_rows();
    assert_eq!(buckets.len(), 1, "桶仍存在（有事件）");
    assert!(
        buckets[0].measures[0].is_empty(),
        "top(0) 无条目——不产出而非 0.0"
    );
}

#[test]
fn stats_top_precheck_skips_below_cutoff_rows() {
    // 快速淘汰预检（q19 优化）: top 已满后大量低于门槛的行被预检挡下——
    // 不构建行字段、不改变条目; 行式/列式同语义（列式预检用 measure_field_idx
    // 原生列读, 行式预检用 value_to_f64——两实现独立, 结果必须一致）。
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![top_measure("top_price", "price", 2)],
    );
    // 键 1: 300/200 进 top-2; 之后 50 行低 bid（150..101 递减, 全低于门槛 200）
    // → 全部被预检淘汰。键 2: 1 行（占位, 验证桶隔离）。
    let mut rows = vec![
        row(&[
            ("auction", num(1.0)),
            ("price", num(300.0)),
            ("bidder", num(1.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(200.0)),
            ("bidder", num(2.0)),
        ]),
    ];
    for p in (101..150).rev() {
        rows.push(row(&[
            ("auction", num(1.0)),
            ("price", num(p as f64)),
            ("bidder", num(p as f64)),
        ]));
    }
    rows.push(row(&[
        ("auction", num(2.0)),
        ("price", num(50.0)),
        ("bidder", num(9.0)),
    ]));

    let batch = rows_to_batch(&rows);
    let mut row_exec = StatsExecutor::new(plan.clone());
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::new(plan);
    assert!(col_exec.process_batch(&batch), "字段键应可列式化");
    for (name, mut exec) in [("行式", row_exec), ("列式", col_exec)] {
        let buckets = exec.close_window_by_bucket_rows();
        assert_eq!(buckets.len(), 2, "{name}: 两个键的桶都在");
        let top = &buckets[0]; // 键 1（ScopeKey 升序 → Int(1) 在前）
        assert_eq!(top.key, ScopeKey::Int(1));
        assert_eq!(top.measures[0].len(), 2, "{name}: 预检淘汰后仍 2 条目");
        assert_eq!(top.measures[0][0].measure_value, 300.0, "{name}: rank1 300");
        assert_eq!(top.measures[0][1].measure_value, 200.0, "{name}: rank2 200");
        // 行字段仍携带原始 bidder（淘汰行不污染）。
        let row = top.measures[0][0]
            .row_fields
            .as_ref()
            .expect("条目带行字段");
        assert!(
            row.iter_values().any(|v| v == Some(num(1.0))),
            "{name}: rank1 bidder=1"
        );
    }
}

#[test]
fn stats_top_precheck_random_stream_matches_reference() {
    // 强验证（预检正确性）: 随机流 × 高淘汰压力（top-5, 20 auction × ~200 bid,
    // ~97.5% 行被预检挡下）——close 结果与**独立参考实现**（每键全量收集 →
    // 按 (price DESC, 到达序 ASC) 排序 → 取前 N）逐位一致。若预检误淘汰
    // 或误放行, 条目内容必然偏离参考。
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![top_measure("top_price", "price", 5)],
    );
    let mut rng: u64 = 0x1234_5678_9abc_def0;
    let next = |rng: &mut u64| {
        *rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *rng >> 33
    };
    let mut rows = Vec::new();
    for _ in 0..4000usize {
        let auction = next(&mut rng) % 20;
        let price = next(&mut rng) % 1000; // 大量低值 → 高淘汰率
        let bidder = next(&mut rng) % 100;
        rows.push(row(&[
            ("auction", num(auction as f64)),
            ("price", num(price as f64)),
            ("bidder", num(bidder as f64)),
        ]));
    }
    // 参考: 每键全量收集 (price, bidder, 到达序) → 降序取前 5（同价先到者前）。
    let mut reference: HashMap<u64, Vec<(f64, f64, usize)>> = HashMap::new();
    for (i, r) in rows.iter().enumerate() {
        let auction = match r.get("auction") {
            Some(Value::Number(n)) => *n as u64,
            _ => unreachable!(),
        };
        let price = match r.get("price") {
            Some(Value::Number(n)) => *n,
            _ => unreachable!(),
        };
        let bidder = match r.get("bidder") {
            Some(Value::Number(n)) => *n,
            _ => unreachable!(),
        };
        reference
            .entry(auction)
            .or_default()
            .push((price, bidder, i));
    }
    let names = sorted_bid_names();
    for (name, mut exec) in [
        ("行式", {
            let mut e = StatsExecutor::new(plan.clone());
            e.process_rows(&rows, extract);
            e
        }),
        ("列式", {
            let batch = rows_to_batch(&rows);
            let mut e = StatsExecutor::new(plan.clone());
            assert!(e.process_batch(&batch), "字段键应可列式化");
            e
        }),
    ] {
        let buckets = exec.close_window_by_bucket_rows();
        assert_eq!(buckets.len(), reference.len(), "{name}: 键数一致");
        for b in &buckets {
            let auction = match &b.key {
                ScopeKey::Int(v) => *v as u64,
                _ => panic!("{name}: 期望 Int 键"),
            };
            let mut ref_entries = reference[&auction].clone();
            ref_entries.sort_by(|a, c| {
                c.0.partial_cmp(&a.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.2.cmp(&c.2)) // 同价先到者前
            });
            ref_entries.truncate(5);
            let entries = &b.measures[0];
            assert_eq!(
                entries.len(),
                ref_entries.len(),
                "{name}: auction {auction} 条目数"
            );
            for (k, (re, e)) in ref_entries.iter().zip(entries.iter()).enumerate() {
                assert_eq!(
                    e.measure_value, re.0,
                    "{name}: auction {auction} rank {k} price"
                );
                assert_eq!(
                    row_val(
                        e.row_fields.as_ref().expect("条目带行字段"),
                        &names,
                        "bidder"
                    ),
                    Some(num(re.1)),
                    "{name}: auction {auction} rank {k} bidder"
                );
            }
        }
    }
}

#[test]
fn stats_row_fields_compact_and_shared() {
    // P5 紧凑化结构验证: (1) 行字段列数组长度 = 子集大小（非整行 8 字段）;
    // (2) 同桶多个 last 度量 Arc 共享同一列数组（内存 1 份）。
    let subset: Arc<HashSet<String>> = Arc::new(
        ["price".to_string(), "bidder".to_string()]
            .into_iter()
            .collect(),
    );
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![
            last_measure("last_price", "price"),
            last_measure("last_bidder", "bidder"),
        ],
    );
    let rows = vec![row(&[
        ("auction", num(1.0)),
        ("price", num(100.0)),
        ("bidder", num(7.0)),
    ])];
    let mut exec = StatsExecutor::with_row_fields(plan, Some(subset));
    exec.process_rows(&rows, extract);
    let buckets = exec.close_window_by_bucket_rows();
    assert_eq!(buckets.len(), 1);
    let m0 = &buckets[0].measures[0][0].row_fields;
    let m1 = &buckets[0].measures[1][0].row_fields;
    let (r0, r1) = (
        m0.as_ref().expect("last 行字段"),
        m1.as_ref().expect("last 行字段"),
    );
    assert_eq!(
        r0.iter_values().count(),
        2,
        "列数组长度 = 子集大小, 而非整行"
    );
    assert!(
        std::sync::Arc::ptr_eq(r0, r1),
        "同桶多 last 度量共享同一列数组"
    );
    // 子集列序 = 排序 [bidder, price]
    let names = ["bidder", "price"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    assert_eq!(row_val(r0, &names, "price"), Some(num(100.0)));
    assert_eq!(row_val(r0, &names, "bidder"), Some(num(7.0)));
    assert!(row_val(r0, &names, "auction").is_none(), "子集外不入列");
}

#[test]
fn stats_row_fields_subset_both_paths_match() {
    // row_fields 子集（P4 review 修复）: 行式回退与列式路径都只保留子集字段——
    // Q18/Q19 内存关键（整行 8 字段 vs 子集）。行式此前保留整行（修复点）。
    let subset: std::sync::Arc<HashSet<String>> = std::sync::Arc::new(
        ["price".to_string(), "bidder".to_string()]
            .into_iter()
            .collect(),
    );
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![last_measure("last_price", "price")],
    );
    let rows = vec![
        row(&[
            ("auction", num(1.0)),
            ("price", num(100.0)),
            ("bidder", num(7.0)),
        ]),
        row(&[
            ("auction", num(1.0)),
            ("price", num(200.0)),
            ("bidder", num(8.0)),
        ]),
    ];
    let batch = rows_to_batch(&rows);
    let mut row_exec = StatsExecutor::with_row_fields(plan.clone(), Some(subset.clone()));
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::with_row_fields(plan, Some(subset));
    assert!(col_exec.process_batch(&batch), "应可列式化");
    let (rb, cb) = (
        row_exec.close_window_by_bucket_rows(),
        col_exec.close_window_by_bucket_rows(),
    );
    assert_eq!(rb.len(), cb.len());
    for (r, c) in rb.iter().zip(cb.iter()) {
        assert_eq!(r.key, c.key);
        for (rm, cm) in r.measures.iter().zip(c.measures.iter()) {
            assert_eq!(rm.len(), cm.len());
            for (re, ce) in rm.iter().zip(cm.iter()) {
                assert_eq!(re.measure_value, ce.measure_value);
                assert_eq!(re.row_fields.is_some(), ce.row_fields.is_some());
                if let (Some(rf), Some(cf)) = (&re.row_fields, &ce.row_fields) {
                    let rv: Vec<Option<Value>> = rf.iter_values().collect();
                    let cv: Vec<Option<Value>> = cf.iter_values().collect();
                    assert_eq!(rv, cv);
                }
            }
        }
    }
    // 子集生效: 行字段不含 auction（不在子集内, 且非桶键注入目标）
    let rf = &rb[0].measures[0][0]
        .row_fields
        .as_ref()
        .expect("last 行字段");
    // 子集 {price, bidder} 排序列序 = [bidder, price]
    let names = ["bidder", "price"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    assert!(row_val(rf, &names, "price").is_some());
    assert!(row_val(rf, &names, "bidder").is_some());
    assert!(row_val(rf, &names, "auction").is_none(), "子集外字段不入行");
}

// ---------------------------------------------------------------------------
// 状态内存 guard（2026-08-25）: `limits.max_memory` → 超限拒收新键桶
// ---------------------------------------------------------------------------

/// Q19 形状（键 = auction, 度量 = top(3, price)）。
fn q19_like_plan() -> StatsPlan {
    keyed_plan(
        vec![field_key("b", "auction")],
        vec![top_measure("top_price", "price", 3)],
    )
}

fn auction_price_rows(pairs: &[(f64, f64)]) -> Vec<HashMap<String, Value>> {
    pairs
        .iter()
        .map(|(a, p)| row(&[("auction", num(*a)), ("price", num(*p))]))
        .collect()
}

#[test]
fn stats_memory_guard_rejects_new_buckets_over_limit() {
    // top(3) 桶预算 = 512 + 1×128 + 3×160 = 1120B。限额 1200 → 只放行 1 桶。
    let plan = q19_like_plan();
    let mut exec = StatsExecutor::new(plan);
    exec.set_memory_limit("guard_test", Some(1200));

    // 10 个不同 auction 键, 每键 1 行（先到者进桶）。
    let rows = auction_price_rows(&[
        (1.0, 100.0),
        (2.0, 200.0),
        (3.0, 300.0),
        (4.0, 400.0),
        (5.0, 500.0),
        (6.0, 600.0),
        (7.0, 700.0),
        (8.0, 800.0),
        (9.0, 900.0),
        (10.0, 1000.0),
    ]);
    exec.process_rows(&rows, extract);

    assert_eq!(
        exec.window.over_limit_new_buckets(),
        9,
        "10 个新键, 限额只放 1 个 → 拒收 9"
    );
    assert!(
        exec.window.estimated_bytes() <= 1200,
        "估算必须在限额内（有界）: {}",
        exec.window.estimated_bytes()
    );
    // 放行的键累积成功, 拒收的键无桶。
    let buckets = exec.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 1, "只应存在 1 个桶");
    assert_eq!(buckets[0].0, ScopeKey::Int(1));
}

#[test]
fn stats_memory_guard_existing_bucket_keeps_accumulating() {
    // 已存在的桶不受拒收影响（同键后续行继续累积）。
    let plan = q19_like_plan();
    let mut exec = StatsExecutor::new(plan);
    exec.set_memory_limit("guard_test", Some(1200));

    exec.process_rows(&auction_price_rows(&[(1.0, 100.0), (2.0, 200.0)]), extract);
    assert_eq!(exec.window.over_limit_new_buckets(), 1, "键 2 被拒");

    // 键 1 再进 2 行 → 桶计数/条目继续累积。
    exec.process_rows(&auction_price_rows(&[(1.0, 90.0), (1.0, 80.0)]), extract);
    assert_eq!(exec.window.over_limit_new_buckets(), 1, "同键不新增拒收");
    let buckets = exec.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 1);
}

#[test]
fn stats_memory_guard_no_limit_accepts_all() {
    // 未设限额（默认 None）→ 全部键放行, 拒收计数 0（不设防 = 原行为）。
    let plan = q19_like_plan();
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(
        &auction_price_rows(&[(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]),
        extract,
    );
    assert_eq!(exec.window.over_limit_new_buckets(), 0);
    assert!(
        exec.window.estimated_bytes() > 0,
        "估算恒记账（可观测）, 无限额不拒收"
    );
    let buckets = exec.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 3);
}

#[test]
fn stats_memory_guard_resets_on_close() {
    let plan = q19_like_plan();
    let mut exec = StatsExecutor::new(plan);
    exec.set_memory_limit("guard_test", Some(1200));
    exec.process_rows(&auction_price_rows(&[(1.0, 100.0), (2.0, 200.0)]), extract);
    assert!(exec.window.estimated_bytes() > 0);
    assert_eq!(exec.window.over_limit_new_buckets(), 1);

    // close（take_buckets + reset_window）→ 账本清零; 拒收计数保留（指标用）。
    let _ = exec.close_window_by_bucket_rows();
    assert_eq!(exec.window.estimated_bytes(), 0, "close 后状态清零");
    assert_eq!(
        exec.window.over_limit_new_buckets(),
        1,
        "拒收计数跨窗口保留"
    );

    // 新窗口仍受 guard 保护（限额配置跨窗口保留）。
    exec.process_rows(&auction_price_rows(&[(3.0, 300.0), (4.0, 400.0)]), extract);
    assert_eq!(exec.window.over_limit_new_buckets(), 2, "新窗口继续拒收");
}

#[test]
fn stats_memory_guard_empty_key_unaffected() {
    // 空键规则: Empty 桶预建, 不参与限额（guard 只针对键空间膨胀）。
    let plan = simple_plan(vec![count_measure("n")]);
    let mut exec = StatsExecutor::new(plan);
    exec.set_memory_limit("guard_test", Some(1)); // 极小限额也不影响空键桶
    let rows = vec![
        row(&[("price", num(1.0))]),
        row(&[("price", num(2.0))]),
        row(&[("price", num(3.0))]),
    ];
    exec.process_rows(&rows, extract);
    assert_eq!(exec.window.over_limit_new_buckets(), 0);
    let values = exec.final_measure_values();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0], 3.0, "空键 count 不受 guard 影响");
}

#[test]
fn stats_memory_guard_columnar_path_rejects_too() {
    // 列式路径（process_batch）与行式同受 guard 约束。
    let plan = q19_like_plan();
    let mut exec = StatsExecutor::new(plan);
    exec.set_memory_limit("guard_test", Some(1200));

    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as _,
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])) as _,
        ],
    )
    .unwrap();
    assert!(exec.process_batch(&batch), "列式前置满足");
    assert_eq!(exec.window.over_limit_new_buckets(), 4, "5 键限 1 → 拒 4");
    assert!(exec.window.estimated_bytes() <= 1200);
}

#[test]
fn stats_memory_guard_event_count_counts_only_accumulated_rows() {
    // F1: 列式 keyed 路径的 event_count 只计归并成功行（被拒行不计）——
    // 与行式路径一致（对拍契约）; 全被拒窗口 event_count == 0 → 空窗 guard。
    let plan = q19_like_plan();
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as _,
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])) as _,
        ],
    )
    .unwrap();

    // 列式: 5 键限 1 → 只归并 1 行, event_count == 1（不是 5）。
    let mut col_exec = StatsExecutor::new(plan.clone());
    col_exec.set_memory_limit("guard_test", Some(1200));
    assert!(col_exec.process_batch(&batch), "列式前置满足");
    assert_eq!(col_exec.window.over_limit_new_buckets(), 4);
    assert_eq!(
        col_exec.window.event_count, 1,
        "被拒 4 行不计入 event_count"
    );

    // 行式对拍: 同一输入 → 同样只归并 1 行。
    let mut row_exec = StatsExecutor::new(plan);
    row_exec.set_memory_limit("guard_test", Some(1200));
    let rows = auction_price_rows(&[
        (1.0, 100.0),
        (2.0, 200.0),
        (3.0, 300.0),
        (4.0, 400.0),
        (5.0, 500.0),
    ]);
    row_exec.process_rows(&rows, extract);
    assert_eq!(row_exec.window.over_limit_new_buckets(), 4);
    assert_eq!(row_exec.window.event_count, 1, "行式同口径");
}

#[test]
fn stats_memory_guard_over_limit_counts_rows_not_keys() {
    // F4: 拒收计数按行（每次新桶尝试）而非按新键——被拒键不建桶, 后续同键行
    // 仍尝试建桶 → 每次 +1。这是有意取舍（每键记账需无界集合, 违背有界承诺）。
    let plan = q19_like_plan();
    let mut exec = StatsExecutor::new(plan);
    exec.set_memory_limit("guard_test", Some(1200)); // 只放 1 桶

    // 键 1 放行; 键 2 首次被拒; 键 2 再来 2 行仍被拒; 键 3 被拒。
    exec.process_rows(
        &auction_price_rows(&[
            (1.0, 100.0),
            (2.0, 200.0),
            (2.0, 210.0),
            (2.0, 220.0),
            (3.0, 300.0),
        ]),
        extract,
    );
    assert_eq!(
        exec.window.over_limit_new_buckets(),
        4,
        "键 2 被拒 3 行 + 键 3 被拒 1 行 = 4（按行, 非按新键 2）"
    );
    assert_eq!(exec.window.event_count, 1, "只归并键 1 的 1 行");
}

#[test]
fn stats_memory_guard_merge_partial_rejects_over_limit() {
    // F2（engine 侧）: 协调片 merge_partial 时新键同样过 guard——分片各自限额
    // 内放行的键, 合并到协调片后可能超限被拒（协调片 own 预算）。
    let plan = q19_like_plan();
    let mut exec = StatsExecutor::new(plan);
    exec.set_memory_limit("guard_test", Some(1200)); // 只放 1 桶

    // 协调片已有键 1（占满预算 1120B）。
    exec.process_rows(&auction_price_rows(&[(1.0, 100.0)]), extract);
    assert_eq!(exec.window.over_limit_new_buckets(), 0);

    // 分片 partial 带来键 2（分片侧限额内放行）——协调片合并时超限被拒。
    let partial: Vec<(ScopeKey, StatsBucketAccs)> = vec![(
        ScopeKey::Int(2),
        StatsBucketAccs::Classic(vec![StatsAccum::Top(vec![TopEntry {
            key: 200.0,
            row: {
                let layout = std::sync::Arc::new(RowFieldLayout::all_other(&["price".to_string()]));
                let mut rf = RowFields::empty(layout);
                rf.set(0, Some(Value::Number(200.0)));
                rf
            },
        }])]),
    )];
    exec.merge_partial(partial, 1);
    assert_eq!(
        exec.window.over_limit_new_buckets(),
        1,
        "协调片合并新键超限 → 拒收计数 +1"
    );
    assert_eq!(exec.window.event_count, 2, "partial 的 event_count 仍累计");
    assert_eq!(
        exec.final_measure_values_by_bucket().len(),
        1,
        "只有键 1 桶"
    );
}
#[test]
fn stats_memory_guard_q18_shape_budget_not_overcounted() {
    // 2026-08-26 q18 预算口径回归：度量专用累加器后 allowance = 432B/键
    // （旧全功能累加器口径 1664B/键，含 last 160B/度量死预算）。43.2MB 预算下
    // 新口径阈值 10 万键、旧口径仅 ~2.6 万键——喂 5 万唯一键：新口径全收
    // （不丢键），旧口径会拒收 ~2.4 万。若 allowance 口径回退（变高），本测试红。
    let plan = keyed_plan(
        vec![field_key("b", "bidder"), field_key("b", "auction")],
        vec![
            last_measure("last_price", "price"),
            last_measure("last_channel", "channel"),
            last_measure("last_url", "url"),
            last_measure("last_dateTime", "dateTime"),
        ],
    );
    let mut exec = StatsExecutor::new(plan);
    // 43.2MB: 新口径 432B → 阈值 10 万键; 旧口径 1664B → 阈值 2.6 万键。
    exec.set_memory_limit("guard_q18_shape", Some(43_200_000));
    const N: usize = 50_000;
    let rows: Vec<HashMap<String, Value>> = (0..N)
        .map(|i| {
            row(&[
                ("bidder", num(1000.0 + (i % 1010) as f64)),
                ("auction", num(i as f64)), // auction 唯一 → (bidder,auction) 唯一
                ("price", num(100.0)),
                ("channel", str_val("Google")),
                (
                    "url",
                    str_val("https://www.nexmark.com/a/b/c/item.htm?query=1"),
                ),
                ("dateTime", num(1_700_000_000_000_000_000.0 + i as f64)),
            ])
        })
        .collect();
    exec.process_rows(&rows, extract);
    assert_eq!(
        exec.window.over_limit_new_buckets(),
        0,
        "新口径 5 万键全收（旧口径 ~2.6 万键即开始拒收）"
    );
    assert_eq!(exec.window.event_count, N as u64, "全部行归并");
    assert_eq!(
        exec.window.buckets.values().map(|c| c.len()).sum::<usize>(),
        N,
        "桶数 = 键数, 无丢键"
    );
}

/// 快速验证：q18 形状列式路径的 RowFields layout 是否紧凑（2026-08-26）。
#[test]
fn q18_columnar_layout_is_compact() {
    let plan = keyed_plan(
        vec![field_key("b", "auction")],
        vec![
            last_measure("last_price", "price"),
            last_measure("last_channel", "channel"),
        ],
    );
    let subset: std::sync::Arc<std::collections::HashSet<String>> = std::sync::Arc::new(
        ["auction", "price", "channel"]
            .into_iter()
            .map(String::from)
            .collect(),
    );
    let mut exec = StatsExecutor::with_row_fields(plan, Some(subset));
    // 列式批（bid_events 形状：auction/price Int64 + channel Utf8）。
    let batch = arrow::record_batch::RecordBatch::try_new(
        std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("auction", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("price", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("channel", arrow::datatypes::DataType::Utf8, false),
        ])),
        vec![
            std::sync::Arc::new(arrow::array::Int64Array::from(vec![1, 1, 2])),
            std::sync::Arc::new(arrow::array::Int64Array::from(vec![100, 200, 300])),
            std::sync::Arc::new(arrow::array::StringArray::from(vec!["G", "G", "B"])),
        ],
    )
    .expect("batch");
    assert!(exec.process_batch(&batch), "列式前置应满足");
    let buckets = exec.close_window_by_bucket_rows();
    assert_eq!(buckets.len(), 2, "2 个 auction 桶");
    // 行字段 layout：auction/price 数字槽 + channel 字符串槽。
    let layout = buckets[0].measures[0][0]
        .row_fields
        .as_ref()
        .expect("last 携带行字段")
        .layout();
    assert_eq!(layout.n_numeric(), 2, "auction/price 数字槽");
    assert_eq!(layout.n_strings(), 1, "channel 字符串槽");
}

// ---------------------------------------------------------------------------
// SoA 桶专项（2026-08-27 q17 优化）
// ---------------------------------------------------------------------------
//
// 覆盖: 内部状态精确值 / null 语义 / 行式-列式对拍 / 底层累积对拍（SoA vs
// Classic）/ 分片合并 / close 三路径一致 / 空键整列归并段 / 窗口重置 / 形态
// 门控 / guard 记账口径。

fn min_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Min,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn max_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Max,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

/// q17 形状计划: 带 auction 键 + 8 度量（total/r1/r2/r3 分档 count + min/max/avg/sum）。
fn q17_shape_plan() -> StatsPlan {
    let mk = |label: &str, agg: StatsAggPlan, field: Option<&str>, w: Option<Expr>| {
        StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr: w,
            agg,
            field: field.map(|f| FieldRef::Qualified("b".into(), f.into())),
            arg: None,
        }
    };
    keyed_plan(
        vec![field_key("b", "auction")],
        vec![
            mk("total", StatsAggPlan::Count, None, None),
            mk("r1", StatsAggPlan::Count, None, Some(price_lt(10_000.0))),
            mk(
                "r2",
                StatsAggPlan::Count,
                None,
                Some(price_range(10_000.0, 1_000_000.0)),
            ),
            mk("r3", StatsAggPlan::Count, None, Some(price_ge(1_000_000.0))),
            mk("minp", StatsAggPlan::Min, Some("price"), None),
            mk("maxp", StatsAggPlan::Max, Some("price"), None),
            mk("avgp", StatsAggPlan::Avg, Some("price"), None),
            mk("sump", StatsAggPlan::Sum, Some("price"), None),
        ],
    )
}

/// q17 计划度量序: 0 total, 1 r1, 2 r2, 3 r3, 4 minp, 5 maxp, 6 avgp, 7 sump。
/// SoA 槽映射: sum_slot avgp→0 sump→1; min_slot minp→0; max_slot maxp→0。
#[test]
fn stats_soa_internal_values_match_expected() {
    let plan = q17_shape_plan();
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(
        &auction_price_rows(&[
            (1.0, 5_000.0),     // r1
            (1.0, 15_000.0),    // r2
            (1.0, 2_000_000.0), // r3
            (1.0, 5_000.0),     // r1
            (1.0, 15_000.0),    // r2
            (2.0, 3_000.0),     // r1
            (2.0, 3_000.0),     // r1
        ]),
        extract,
    );

    let StatsBucketAccs::Numeric(a1) = exec.window.find_bucket(&ScopeKey::Int(1)).unwrap() else {
        panic!("纯数值计划应走 SoA 桶");
    };
    // counts（索引 = 度量 idx; 无条件度量 total/min/max/avg/sum 全计 5）
    assert_eq!(&*a1.counts, &[5, 2, 2, 1, 5, 5, 5, 5], "auction 1 计数");
    // sums: avgp→slot 0, sump→slot 1; 两度量同字段共享同一 price 累加
    assert_eq!(&*a1.sums, &[2_040_000, 2_040_000], "auction 1 sum");
    assert_eq!(a1.mins[0], Some(5_000), "auction 1 min");
    assert_eq!(a1.maxs[0], Some(2_000_000), "auction 1 max");

    let StatsBucketAccs::Numeric(a2) = exec.window.find_bucket(&ScopeKey::Int(2)).unwrap() else {
        panic!("纯数值计划应走 SoA 桶");
    };
    assert_eq!(&*a2.counts, &[2, 2, 0, 0, 2, 2, 2, 2], "auction 2 计数");
    assert_eq!(&*a2.sums, &[6_000, 6_000], "auction 2 sum");
    assert_eq!(a2.mins[0], Some(3_000));
    assert_eq!(a2.maxs[0], Some(3_000));

    // 最终输出（avg = sum/count）
    let vals = exec.final_measure_values_by_bucket();
    assert_eq!(vals.len(), 2);
    let v1 = &vals[0];
    assert_eq!(v1.0, ScopeKey::Int(1));
    assert_eq!(v1.1, vec![5.0, 2.0, 2.0, 1.0, 5_000.0, 2_000_000.0, 408_000.0, 2_040_000.0]);
    let v2 = &vals[1];
    assert_eq!(v2.0, ScopeKey::Int(2));
    assert_eq!(v2.1, vec![2.0, 2.0, 0.0, 0.0, 3_000.0, 3_000.0, 3_000.0, 6_000.0]);
}

/// null price: count 仍 +1（where 对 null 不过——r1 不计数）, sum/min/max 不更新。
#[test]
fn stats_soa_null_price_count_only() {
    let plan = q17_shape_plan();
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(
        &[
            row(&[("auction", num(1.0)), ("price", num(100.0))]),
            row(&[("auction", num(1.0))]), // price 缺失 → null
            row(&[("auction", num(1.0)), ("price", num(300.0))]),
        ],
        extract,
    );
    let StatsBucketAccs::Numeric(soa) = exec.window.find_bucket(&ScopeKey::Int(1)).unwrap() else {
        panic!("纯数值计划应走 SoA 桶");
    };
    assert_eq!(soa.counts[0], 3, "total 计 3（含 null 行）");
    assert_eq!(soa.counts[1], 2, "r1 只计 2（null 行 where 不过）");
    assert_eq!(soa.sums[1], 400, "sum 只累非 null: 100+300");
    assert_eq!(soa.mins[0], Some(100));
    assert_eq!(soa.maxs[0], Some(300));

    // 列式路径同口径（null 由数组 mask 标记）
    let mut col = StatsExecutor::new(q17_shape_plan());
    let batch = rows_to_batch_with_null_price(&[
        row(&[("auction", num(1.0)), ("price", num(100.0))]),
        row(&[("auction", num(1.0))]),
        row(&[("auction", num(1.0)), ("price", num(300.0))]),
    ]);
    assert!(col.process_batch(&batch), "列式前置应满足");
    assert_eq!(
        exec.final_measure_values_by_bucket(),
        col.final_measure_values_by_bucket(),
        "行式与列式 null 语义一致"
    );
}

/// 带 key 纯数值计划: 行式（process_rows）与列式（process_batch）最终值一致。
#[test]
fn stats_soa_row_and_columnar_agree() {
    let rows = auction_price_rows(&[
        (1.0, 100.0),
        (1.0, 2_000_000.0),
        (2.0, 50_000.0),
        (3.0, 15_000.0),
        (1.0, 7.0),
    ]);
    let plan = q17_shape_plan();
    let mut row_exec = StatsExecutor::new(plan.clone());
    row_exec.process_rows(&rows, extract);
    let mut col_exec = StatsExecutor::new(plan);
    assert!(col_exec.process_batch(&rows_to_batch(&rows)), "列式前置应满足");
    assert_eq!(
        row_exec.final_measure_values_by_bucket(),
        col_exec.final_measure_values_by_bucket()
    );
}

/// 底层累积函数对拍: 同一批/掩码喂 accumulate_soa（SoA 桶）与
/// accumulate_column_row（Classic 桶）, 最终值逐度量一致——隔离验证两条热路径。
#[test]
fn stats_soa_classic_accumulate_agree() {
    let rows = auction_price_rows(&[
        (1.0, 5_000.0),
        (1.0, 15_000.0),
        (1.0, 2_000_000.0),
        (2.0, 3_000.0),
        (2.0, 3_000.0),
    ]);
    let plan = q17_shape_plan();
    let batch = rows_to_batch(&rows);
    let n = batch.num_rows();
    // 批级预解析（同生产）
    let price_col = batch.schema().index_of("price").unwrap();
    let measure_field_cols: Vec<Option<usize>> = plan
        .measures
        .iter()
        .map(|m| m.field.as_ref().map(|_| price_col))
        .collect();
    // 3 个唯一 where 的批级 mask
    let price = batch
        .column(price_col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let masks = vec![
        BooleanArray::from((0..n).map(|i| price.value(i) < 10_000).collect::<Vec<_>>()),
        BooleanArray::from(
            (0..n)
                .map(|i| price.value(i) >= 10_000 && price.value(i) < 1_000_000)
                .collect::<Vec<_>>(),
        ),
        BooleanArray::from((0..n).map(|i| price.value(i) >= 1_000_000).collect::<Vec<_>>()),
    ];
    let measure_where: Vec<Option<usize>> = vec![None, Some(0), Some(1), Some(2), None, None, None, None];
    let measure_field_idx: Vec<Option<usize>> = vec![None; plan.measures.len()];
    let row_layout = Arc::new(RowFieldLayout::all_other(&[]));

    let layout = NumericSoALayout::build(&plan);
    let mut soa = layout.zeros();
    let mut classic: Vec<StatsAccum> = plan
        .measures
        .iter()
        .map(|m| StatsAccum::for_measure(&m.agg))
        .collect();
    for row in 0..n {
        accumulate_soa(
            &mut soa,
            &layout,
            &measure_where,
            &measure_field_cols,
            &batch,
            &masks,
            row,
        );
        accumulate_column_row(
            &mut classic,
            &plan,
            &measure_where,
            &measure_field_idx,
            None,
            None,
            &batch,
            &masks,
            row,
            &row_layout,
            &measure_field_cols,
        );
    }
    let soa_vals = measure_values_soa(&plan, &soa, &layout);
    let classic_vals: Vec<f64> = plan
        .measures
        .iter()
        .zip(classic.iter())
        .map(|(m, acc)| match m.agg {
            StatsAggPlan::Count => acc.numeric().count as f64,
            StatsAggPlan::Sum => acc.numeric().sum as f64,
            StatsAggPlan::Avg => {
                let n = acc.numeric().count;
                if n == 0 {
                    0.0
                } else {
                    acc.numeric().sum as f64 / n as f64
                }
            }
            StatsAggPlan::Min => acc.numeric().min.unwrap_or(0) as f64,
            StatsAggPlan::Max => acc.numeric().max.unwrap_or(0) as f64,
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(soa_vals, classic_vals, "SoA 与 Classic 累积逐度量一致");
}

/// 分片合并（merge_partial）: 两片 SoA 桶合并 = 手工和（计数相加/sum 相加/极值）。
#[test]
fn stats_soa_merge_partial_combines() {
    let plan = q17_shape_plan();
    let mut coord = StatsExecutor::new(plan.clone());
    let mut shard = StatsExecutor::new(plan);
    coord.process_rows(&auction_price_rows(&[(1.0, 100.0), (1.0, 200.0)]), extract);
    shard.process_rows(
        &auction_price_rows(&[(1.0, 300.0), (2.0, 50.0), (1.0, 400.0)]),
        extract,
    );
    let (partial, cnt) = shard.take_partial();
    coord.merge_partial(partial, cnt);

    let vals = coord.final_measure_values_by_bucket();
    assert_eq!(vals.len(), 2);
    // auction 1: total 4, sum 1000, min 100, max 400, avg 250
    let v1 = vals.iter().find(|(k, _)| *k == ScopeKey::Int(1)).unwrap();
    assert_eq!(v1.1, vec![4.0, 4.0, 0.0, 0.0, 100.0, 400.0, 250.0, 1_000.0]);
    // auction 2: total 1, sum 50, min 50, max 50
    let v2 = vals.iter().find(|(k, _)| *k == ScopeKey::Int(2)).unwrap();
    assert_eq!(v2.1, vec![1.0, 1.0, 0.0, 0.0, 50.0, 50.0, 50.0, 50.0]);
}

/// close 三路径输出一致: final_measure_values_by_bucket / close_window_by_bucket_rows
/// / close_window_by_bucket——同一数据三种读取口径逐值相等。
#[test]
fn stats_soa_close_paths_emit_same() {
    let rows = auction_price_rows(&[(1.0, 100.0), (1.0, 300.0), (2.0, 200.0)]);

    let mut e1 = StatsExecutor::new(q17_shape_plan());
    e1.process_rows(&rows, extract);
    let by_bucket: Vec<(ScopeKey, Vec<f64>)> = e1.final_measure_values_by_bucket();

    let mut e2 = StatsExecutor::new(q17_shape_plan());
    e2.process_rows(&rows, extract);
    let rich = e2.close_window_by_bucket_rows();
    let rich_vals: Vec<(ScopeKey, Vec<f64>)> = rich
        .into_iter()
        .map(|b| {
            (
                b.key,
                b.measures
                    .iter()
                    .map(|entries| entries[0].measure_value)
                    .collect(),
            )
        })
        .collect();

    let mut e3 = StatsExecutor::new(q17_shape_plan());
    e3.process_rows(&rows, extract);
    let scalar = e3.close_window_by_bucket();

    assert_eq!(by_bucket, rich_vals, "by_bucket 与 rich close 一致");
    assert_eq!(by_bucket, scalar, "by_bucket 与标量 close 一致");
}

/// 空键纯数值计划: 段 1d 整列归并（count/sum/min/max 列式）与行式最终值一致。
#[test]
fn stats_soa_empty_key_columnar_matches_row() {
    let plan = simple_plan(vec![
        count_measure("n"),
        sum_measure("s", "price"),
        avg_measure("a", "price"),
        min_measure("m", "price"),
        max_measure("x", "price"),
    ]);
    let rows = auction_price_rows(&[(1.0, 100.0), (1.0, 300.0), (2.0, 200.0)]);

    let mut col = StatsExecutor::new(plan.clone());
    assert!(col.process_batch(&rows_to_batch(&rows)), "列式前置应满足");
    let mut row = StatsExecutor::new(plan);
    row.process_rows(&rows, extract);
    assert_eq!(col.final_measure_values(), row.final_measure_values());
    // 手工期望: n=3, s=600, a=200, m=100, x=300
    assert_eq!(col.final_measure_values(), vec![3.0, 600.0, 200.0, 100.0, 300.0]);
}

/// 窗口 close 后重置: 桶从零开始（新窗口不残留旧计数）。
#[test]
fn stats_soa_reset_rebuilds_zeros() {
    let mut exec = StatsExecutor::new(q17_shape_plan());
    exec.process_rows(&auction_price_rows(&[(1.0, 100.0)]), extract);
    assert_eq!(exec.window.event_count, 1);
    exec.close_window_by_bucket();
    assert_eq!(exec.window.event_count, 0, "reset 清空事件计数");

    exec.process_rows(&auction_price_rows(&[(1.0, 200.0)]), extract);
    let StatsBucketAccs::Numeric(soa) = exec.window.find_bucket(&ScopeKey::Int(1)).unwrap() else {
        panic!("纯数值计划应走 SoA 桶");
    };
    assert_eq!(soa.counts[0], 1, "新窗口从零开始");
    assert_eq!(soa.sums[1], 200, "新窗口 sum 不残留旧值");
    assert_eq!(soa.mins[0], Some(200));
}

/// 形态门控: 含 distinct 的计划恒走 Classic 桶（SoA 仅纯数值计划）。
#[test]
fn stats_soa_mixed_plan_stays_classic() {
    let plan = simple_plan(vec![
        count_measure("n"),
        distinct_measure("bidders", "bidder"),
    ]);
    let mut exec = StatsExecutor::new(plan);
    exec.process_rows(
        &[
            row(&[("bidder", num(1.0))]),
            row(&[("bidder", num(1.0))]),
            row(&[("bidder", num(2.0))]),
        ],
        extract,
    );
    let bucket = exec.window.find_bucket(&ScopeKey::Empty).unwrap();
    assert!(
        matches!(bucket, StatsBucketAccs::Classic(_)),
        "含 distinct 的计划应走 Classic"
    );
    assert_eq!(exec.final_measure_values(), vec![3.0, 2.0]);
}

/// SoA guard 记账口径: q17 形状 SoA allowance = 256 + 8×8 + 2×16 + 1×16 + 1×16
/// = 384B——限额 384 恰好放 1 桶, 第 2 键拒收。
#[test]
fn stats_soa_guard_allowance_soa_budget() {
    let mut exec = StatsExecutor::new(q17_shape_plan());
    exec.set_memory_limit("soa_guard", Some(384));
    exec.process_rows(
        &auction_price_rows(&[(1.0, 100.0), (2.0, 200.0)]),
        extract,
    );
    assert_eq!(exec.window.over_limit_new_buckets(), 1, "第 2 键超 384B 拒收");
    assert_eq!(exec.window.event_count, 1, "只归并键 1");
    let vals = exec.final_measure_values_by_bucket();
    assert_eq!(vals.len(), 1, "只有键 1 桶");
}

/// SoA 空窗 avg 输出 0.0（count==0 防除零）。
#[test]
fn stats_soa_empty_window_avg_zero() {
    let plan = simple_plan(vec![avg_measure("a", "price"), sum_measure("s", "price")]);
    let exec = StatsExecutor::new(plan);
    let vals = exec.final_measure_values();
    assert_eq!(vals, vec![0.0, 0.0], "空窗 avg=0.0（非 NaN）");
}
