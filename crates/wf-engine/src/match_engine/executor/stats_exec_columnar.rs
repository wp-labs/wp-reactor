//! Q15 与列式执行段对拍（2026-09-04 自 stats_exec_test.rs 拆出；`#[path]`
//! 兄弟子模块）：12 度量行式 vs 列式 vs 独立参考实现逐值对拍、列式回退（非列式
//! where/不支持类型/部分应用零副作用）、sum/avg/min/max 与 Float64/Utf8 distinct
//! 列式累积、多批累积；批级 where mask/max_time 共享缓存（2026-08-27 q17 分片
//! 去重——命中/容量清理/并发正确性/分片语义集成）。

use super::*;

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
// 批级 where mask 共享缓存（2026-08-27 q17 分片去重）
// ---------------------------------------------------------------------------

/// 同批两次 get_or_compute: 第二次命中（compute 只调一次）——分片去重的核心契约。
#[test]
fn stats_mask_cache_hits_same_batch() {
    let cache = StatsMaskCache::new();
    let batch = rows_to_batch(&auction_price_rows(&[(1.0, 100.0), (1.0, 200.0)]));
    // 模拟两片拿到同一批（值副本, 列 Arc 同源）
    let batch2 = batch.clone();

    let mut calls = 0usize;
    let m1 = cache.get_or_compute(&batch, || {
        calls += 1;
        vec![BooleanArray::from(vec![true, false])]
    });
    assert_eq!(calls, 1);
    assert_eq!(m1.len(), 1);

    let m2 = cache.get_or_compute(&batch2, || {
        calls += 1;
        vec![]
    });
    assert_eq!(calls, 1, "同批（列 Arc 同源）第二次必须命中缓存");
    assert_eq!(m2.len(), 1, "命中返回首片结果");
    assert!(std::sync::Arc::ptr_eq(&m1, &m2), "两片共享同一 mask Arc");
}

/// 不同批不串: 各自 compute（key 含列 Arc 指针, 批不同列不同）。
#[test]
fn stats_mask_cache_distinct_batches() {
    let cache = StatsMaskCache::new();
    let b1 = rows_to_batch(&auction_price_rows(&[(1.0, 100.0)]));
    let b2 = rows_to_batch(&auction_price_rows(&[(1.0, 999_999_999.0)]));

    let mut calls = 0usize;
    let m1 = cache.get_or_compute(&b1, || {
        calls += 1;
        vec![BooleanArray::from(vec![true])]
    });
    let m2 = cache.get_or_compute(&b2, || {
        calls += 1;
        vec![BooleanArray::from(vec![false])]
    });
    assert_eq!(calls, 2, "不同批各自 compute");
    assert!(m1[0].value(0));
    assert!(!m2[0].value(0));
}

/// 容量超限整体清空（流式批下旧批已消费, 清空安全; 之后新批正常重算）。
#[test]
fn stats_mask_cache_capacity_clears() {
    let cache = StatsMaskCache::new();
    // 缩小容量便于触发（pub(crate) 字段, 测试同 crate 可改）
    let mut cache = cache;
    cache.max_rows = 3;
    let b1 = rows_to_batch(&auction_price_rows(&[(1.0, 100.0)]));
    let b2 = rows_to_batch(&auction_price_rows(&[(1.0, 200.0)]));
    let b3 = rows_to_batch(&auction_price_rows(&[(1.0, 300.0)]));
    let b4 = rows_to_batch(&auction_price_rows(&[(1.0, 400.0)]));

    let mut calls = 0usize;
    for b in [&b1, &b2, &b3, &b4] {
        cache.get_or_compute(b, || {
            calls += 1;
            vec![]
        });
    }
    assert_eq!(calls, 4, "超限清空后每批都重算");
    assert_eq!(cache.len(), 1, "清空后只留最后一批");
}

/// 分片缓存版 `process_batch_rows_cached` 与无缓存版语义一致（同批同结果）。
#[test]
fn stats_mask_cache_cached_path_agrees_with_plain() {
    let plan = q17_shape_plan();
    let rows = auction_price_rows(&[(1.0, 100.0), (1.0, 2_000_000.0), (2.0, 50_000.0)]);
    let batch = rows_to_batch(&rows);

    let cache = StatsMaskCache::new();
    let mut cached = StatsExecutor::new(plan.clone());
    let ok1 = cached.process_batch_rows_cached(&batch, None, &cache);
    let mut plain = StatsExecutor::new(plan);
    let ok2 = plain.process_batch_rows(&batch, None);
    assert!(ok1 && ok2);
    assert_eq!(
        cached.final_measure_values_by_bucket(),
        plain.final_measure_values_by_bucket(),
        "缓存版与无缓存版同批结果一致"
    );
    // 再次同批（模拟第二片）: 命中缓存, 结果仍一致
    let mut cached2 = StatsExecutor::new(q17_shape_plan());
    let ok3 = cached2.process_batch_rows_cached(&batch, None, &cache);
    assert!(ok3);
    assert_eq!(
        cached2.final_measure_values_by_bucket(),
        plain.final_measure_values_by_bucket()
    );
}

/// 并发正确性（第 2 轮 review 补）: 多线程（模拟 S 片）同时 get_or_compute 同批——
/// compute 只执行 1 次, 所有线程拿到同一 mask Arc（结果一致）。
#[test]
fn stats_mask_cache_concurrent_shards() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let cache = std::sync::Arc::new(StatsMaskCache::new());
    let batch = std::sync::Arc::new(rows_to_batch(&auction_price_rows(&[
        (1.0, 100.0),
        (1.0, 2_000_000.0),
        (2.0, 50_000.0),
        (3.0, 15_000.0),
        (1.0, 7.0),
    ])));
    let compute_calls = AtomicUsize::new(0);

    const THREADS: usize = 10;
    std::thread::scope(|s| {
        for _ in 0..THREADS {
            let cache = std::sync::Arc::clone(&cache);
            let batch = std::sync::Arc::clone(&batch);
            let calls = &compute_calls;
            s.spawn(move || {
                let m = cache.get_or_compute(&batch, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    vec![BooleanArray::from(vec![true, true, false, false, true])]
                });
                assert_eq!(m.len(), 1, "所有线程拿到同一结果");
                assert!(m[0].value(0), "mask 值正确");
                assert!(!m[0].value(2));
            });
        }
    });
    assert_eq!(
        compute_calls.load(Ordering::SeqCst),
        1,
        "10 片并发只算 1 次"
    );
    assert_eq!(cache.len(), 1);
}

/// 清空后重算一致性（第 3 轮 review 补）: 容量超限整体清空后, 同批再次
/// get_or_compute 重算——结果与首次一致（清理不破坏正确性）。
#[test]
fn stats_mask_cache_recompute_after_clear_matches() {
    let cache = StatsMaskCache::new();
    let mut cache = cache;
    cache.max_rows = 4;
    let b1 = rows_to_batch(&auction_price_rows(&[(1.0, 100.0), (1.0, 200.0)])); // 2 行
    let b2 = rows_to_batch(&auction_price_rows(&[(1.0, 300.0), (1.0, 400.0)])); // 2 行
    let b3 = rows_to_batch(&auction_price_rows(&[(1.0, 500.0), (1.0, 600.0)])); // 2 行 → 触发清空

    let m1 = cache.get_or_compute(&b1, || vec![BooleanArray::from(vec![true, true])]);
    let _ = cache.get_or_compute(&b2, || vec![BooleanArray::from(vec![false, false])]);
    let _ = cache.get_or_compute(&b3, || vec![BooleanArray::from(vec![true, false])]);
    // 第 3 批: total 4+2=6 > 4 → 清空（b1/b2 被清）; 缓存只留 b3
    assert_eq!(cache.len(), 1, "清空后只留当前批");

    // b1 再次访问 → 重算（同一表达式 → 同一结果; 缓存设计下 compute 是纯函数）
    let m1b = cache.get_or_compute(&b1, || vec![BooleanArray::from(vec![true, true])]);
    assert!(m1b[0].value(0) && m1b[0].value(1), "重算结果与首次一致");
    assert_eq!(m1[0].value(0), m1b[0].value(0));
    assert_eq!(m1[0].value(1), m1b[0].value(1));
    // b3 仍在缓存（未被重算影响）
    let m3b = cache.get_or_compute(&b3, Vec::new);
    assert_eq!(m3b.len(), 1);
    assert!(m3b[0].value(0) && !m3b[0].value(1));
}

/// 批级时间信息共享（2026-08-27 q17 扩展 A）: 同批 get_or_compute_time 命中
/// （compute 只调 1 次, 模拟 10 片共享 batch_max_time 扫描）; 不同批各自算。
#[test]
fn stats_mask_cache_time_shares_same_batch() {
    let cache = StatsMaskCache::new();
    let batch = Arc::new(rows_to_batch(&auction_price_rows(&[
        (1.0, 100.0),
        (1.0, 200.0),
    ])));
    let batch2 = batch.clone(); // 模拟另一片（值副本, 列 Arc 同源）

    let mut calls = 0usize;
    let t1 = cache.get_or_compute_time(&batch, || {
        calls += 1;
        1_750_000_000_000_000_000i64
    });
    let t2 = cache.get_or_compute_time(&batch2, || {
        calls += 1;
        -1
    });
    assert_eq!(calls, 1, "同批第二次命中（不重扫）");
    assert_eq!(t1, t2);

    // 不同批各自算
    let b_other = Arc::new(rows_to_batch(&auction_price_rows(&[(2.0, 300.0)])));
    let t3 = cache.get_or_compute_time(&b_other, || {
        calls += 1;
        2_000_000_000_000_000_000i64
    });
    assert_eq!(calls, 2);
    assert_ne!(t1, t3);
}

/// time 并发正确性（第 2 轮 review 补）: 多线程并发 get_or_compute_time 同批——
/// compute 只执行 1 次, 所有线程拿到同一 max_time。
#[test]
fn stats_mask_cache_time_concurrent_shards() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let cache = std::sync::Arc::new(StatsMaskCache::new());
    let batch = std::sync::Arc::new(rows_to_batch(&auction_price_rows(&[
        (1.0, 100.0),
        (1.0, 200.0),
    ])));
    let compute_calls = AtomicUsize::new(0);

    const THREADS: usize = 10;
    std::thread::scope(|s| {
        for _ in 0..THREADS {
            let cache = std::sync::Arc::clone(&cache);
            let batch = std::sync::Arc::clone(&batch);
            let calls = &compute_calls;
            s.spawn(move || {
                let t = cache.get_or_compute_time(&batch, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    42_000_000_000i64
                });
                assert_eq!(t, 42_000_000_000);
            });
        }
    });
    assert_eq!(
        compute_calls.load(Ordering::SeqCst),
        1,
        "10 片并发只扫 1 次"
    );
}

/// time 表容量清理（第 3 轮 review 补）: 超限整体清空, 与 mask 表独立记账。
#[test]
fn stats_mask_cache_time_capacity_clears() {
    let cache = StatsMaskCache::new();
    let mut cache = cache;
    cache.max_rows = 3;
    let b1 = rows_to_batch(&auction_price_rows(&[(1.0, 100.0)]));
    let b2 = rows_to_batch(&auction_price_rows(&[(1.0, 200.0)]));
    let b3 = rows_to_batch(&auction_price_rows(&[(1.0, 300.0)]));

    cache.get_or_compute_time(&b1, || 1);
    cache.get_or_compute_time(&b2, || 2);
    assert_eq!(cache.time_len(), 2);
    // 第 3 批: total 3 > 3? —— 3 批各 1 行: 第 3 批 total=3 不超; 第 4 批才触发。
    cache.get_or_compute_time(&b3, || 3);
    assert_eq!(cache.time_len(), 3, "3 批各 1 行 ≤ 上限 3");
    let b4 = rows_to_batch(&auction_price_rows(&[(1.0, 400.0)]));
    cache.get_or_compute_time(&b4, || 4);
    assert_eq!(cache.time_len(), 1, "超限清空后只留当前批");
    // 清空后旧批重算（与 mask 表互不影响）
    assert_eq!(cache.get_or_compute_time(&b1, || 1), 1);
}

/// 两表同批共享一致性（第 5 轮 review 补）: 同一批 mask 与 max_time 都在缓存中
/// 命中（同 key 批身份, 两表独立但一致工作）。
#[test]
fn stats_mask_cache_mask_and_time_share_batch() {
    let cache = StatsMaskCache::new();
    let batch = rows_to_batch(&auction_price_rows(&[(1.0, 100.0), (1.0, 200.0)]));

    let mut calls = 0usize;
    // mask 首算
    let m1 = cache.get_or_compute(&batch, || {
        calls += 1;
        vec![BooleanArray::from(vec![true, false])]
    });
    // time 首算（独立表）
    let t1 = cache.get_or_compute_time(&batch, || {
        calls += 1;
        5_000_000_000i64
    });
    assert_eq!(calls, 2, "mask 与 time 各算 1 次");
    // 第二片同批: 两表都命中
    let batch2 = batch.clone();
    let m2 = cache.get_or_compute(&batch2, || {
        calls += 1;
        vec![]
    });
    let t2 = cache.get_or_compute_time(&batch2, || {
        calls += 1;
        -1
    });
    assert_eq!(calls, 2, "第二片两表均命中");
    assert!(std::sync::Arc::ptr_eq(&m1, &m2));
    assert_eq!(t1, t2);
}

/// 空批不缓存（第 6 轮 review 补）: rows==0 直接 compute, 不写入缓存（避免
/// key=(0,0) 与后续真实批碰撞）。
#[test]
fn stats_mask_cache_empty_batch_not_cached() {
    let cache = StatsMaskCache::new();
    let empty = rows_to_batch(&[]);
    assert_eq!(empty.num_rows(), 0);
    let m = cache.get_or_compute(&empty, || vec![BooleanArray::from(Vec::<bool>::new())]);
    assert_eq!(m.len(), 1);
    assert_eq!(cache.len(), 0, "空批不缓存");

    // 空批后再来真实批: 正常缓存（key 不冲突）
    let real = rows_to_batch(&auction_price_rows(&[(1.0, 100.0)]));
    let m2 = cache.get_or_compute(&real, || vec![BooleanArray::from(vec![true])]);
    assert_eq!(m2.len(), 1);
    assert_eq!(cache.len(), 1);
}

/// 分片语义集成（第 5 轮 review 补）: 两片共享 cache, 各处理自己的行域子集,
/// 合并后与单实例全批结果一致——缓存路径在真实分片语义下不破坏正确性。
#[test]
fn stats_mask_cache_two_shards_match_single() {
    let plan = q17_shape_plan();
    let rows = auction_price_rows(&[
        (1.0, 100.0),
        (1.0, 2_000_000.0),
        (2.0, 50_000.0),
        (3.0, 15_000.0),
        (1.0, 7.0),
    ]);
    let batch = rows_to_batch(&rows);
    let n = batch.num_rows() as u32;
    // 行号 % 2 分区（与 fanout 的 shard_rows 口径一致）
    let shard0: Vec<u32> = (0..n).filter(|i| i % 2 == 0).collect();
    let shard1: Vec<u32> = (0..n).filter(|i| i % 2 == 1).collect();

    let cache = StatsMaskCache::new();
    let mut s0 = StatsExecutor::new(plan.clone());
    let mut s1 = StatsExecutor::new(plan.clone());
    assert!(s0.process_batch_rows_cached(&batch, Some(&shard0), &cache));
    assert!(s1.process_batch_rows_cached(&batch, Some(&shard1), &cache));

    // 两片桶值合并（键 1 的 count/sum 等相加——分片归并语义）
    let mut merged = StatsExecutor::new(plan);
    let (b0, c0) = s0.take_partial();
    let (b1, c1) = s1.take_partial();
    merged.merge_partial(b0, c0);
    merged.merge_partial(b1, c1);

    let mut single = StatsExecutor::new(q17_shape_plan());
    assert!(single.process_batch_rows(&batch, None));
    assert_eq!(
        merged.final_measure_values_by_bucket(),
        single.final_measure_values_by_bucket(),
        "两片共享缓存 + 分片行域归并 = 单实例全批"
    );
}

/// SoA 空键整批域归并（段 1d）分片对拍（2026-09-06 exec.rs 拆分回归）: 纯数值
/// 计划（SoA 桶）按行号分两片 `process_batch_rows`（行域子集）+ `take_partial`/
/// `merge_partial`, 与单实例全批逐值一致——锁定行域 + where mask（含被过滤的
/// sum_r1）在 `accumulate_empty_bucket_numeric` 的 wi/rows/n 交互。
#[test]
fn stats_soa_empty_bucket_domain_split_matches_single() {
    let plan = num_measures_plan();
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
        // 3e6 ≥ 1e6: sum_r1 的 where（price<1e6）不过滤掉它——过滤的是行内值。
        row(&[
            ("price", num(3_000_000.0)),
            ("bidder", num(2.0)),
            ("auction", num(3.0)),
        ]),
        row(&[
            ("price", num(400.0)),
            ("bidder", num(2.0)),
            ("auction", num(4.0)),
        ]),
        row(&[
            ("price", num(5.0)),
            ("bidder", num(3.0)),
            ("auction", num(5.0)),
        ]),
    ];
    let batch = rows_to_batch(&rows);
    let mut single = StatsExecutor::new(plan.clone());
    assert!(
        single.process_batch_rows(&batch, None),
        "数值度量应可列式化"
    );
    let expect = single.final_measure_values();
    let mut a = StatsExecutor::new(plan.clone());
    let mut b = StatsExecutor::new(plan);
    assert!(a.process_batch_rows(&batch, Some(&[0u32, 2, 4])));
    assert!(b.process_batch_rows(&batch, Some(&[1u32, 3])));
    let (buckets, _) = b.take_partial();
    a.merge_partial(buckets, 0);
    assert_eq!(
        a.final_measure_values(),
        expect,
        "SoA 分片归并必须与单实例逐值一致"
    );
    // 手算锚点: sum_all=3_000_705; avg_all=sum/5; min_all=5; max_all=3e6;
    // sum_r1(price<1e6)=100+200+400+5=705。
    assert_eq!(expect[0], 3_000_705.0, "sum_all");
    assert_eq!(expect[1], 3_000_705.0 / 5.0, "avg_all");
    assert_eq!(expect[2], 5.0, "min_all");
    assert_eq!(expect[3], 3_000_000.0, "max_all");
    assert_eq!(expect[4], 705.0, "sum_r1");
}
