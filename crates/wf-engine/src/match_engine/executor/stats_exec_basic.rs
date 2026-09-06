//! P1 空键 accumulator 族 + 输入分区分片归并（2026-09-04 自 stats_exec_test.rs
//! 拆出；`#[path]` 兄弟子模块）：count/distinct/sum/avg/min/max 行式基础行为、
//! where 过滤、close 重置、≥2^53 大整数 distinct 精度；两片独立累加后
//! `take_partial` + `merge_partial` 与单实例喂全部行逐值一致（行式/列式两路径）。

use super::*;

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

/// 行式 SoA/Classic 桶等值（2026-09-06 exec.rs 拆分回归）: 同批行喂给纯数值
/// 计划（SoA 桶）与「1 个 distinct + 同批数值度量」计划（Classic 桶）, 数值
/// 度量值必须一致——锁定 `process_rows` 两分支（现为 row_acc
/// `accumulate_row_map_soa`/`accumulate_row_map_classic`）的同语义契约, 以及
/// 相同 where 表达式去重后共享同一 where_ok 位（q15 型多度量共享条件）。
#[test]
fn stats_row_soa_bucket_matches_classic_with_deduped_wheres() {
    let mk =
        |label: &str, agg: StatsAggPlan, field: Option<&str>, w: Option<Expr>| StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr: w,
            agg,
            field: field.map(|f| FieldRef::Qualified("b".into(), f.into())),
            arg: None,
        };
    // sum_lt/min_lt 共享同一 where（price<5000）→ 去重后 1 个唯一表达式;
    // max_ge 用第二个唯一表达式——锁定 measure_where/where_ok 索引对齐。
    let soa_plan = simple_plan(vec![
        mk("n", StatsAggPlan::Count, None, None),
        mk(
            "sum_lt",
            StatsAggPlan::Sum,
            Some("price"),
            Some(price_lt(5000.0)),
        ),
        mk(
            "min_lt",
            StatsAggPlan::Min,
            Some("price"),
            Some(price_lt(5000.0)),
        ),
        mk(
            "max_ge",
            StatsAggPlan::Max,
            Some("price"),
            Some(price_ge(5000.0)),
        ),
        mk("avg", StatsAggPlan::Avg, Some("price"), None),
    ]);
    let mut classic_measures = vec![distinct_measure("bids", "auction")]; // distinct → 强制 Classic 桶
    classic_measures.extend(soa_plan.measures.clone());
    let classic_plan = simple_plan(classic_measures);

    let rows = vec![
        row(&[("price", num(3000.0)), ("auction", num(11.0))]),
        row(&[("price", num(8000.0)), ("auction", num(12.0))]),
        row(&[("price", num(5000.0)), ("auction", num(13.0))]),
    ];
    let mut soa = StatsExecutor::new(soa_plan);
    soa.process_rows(&rows, extract);
    let sv = soa.final_measure_values();
    let mut classic = StatsExecutor::new(classic_plan);
    classic.process_rows(&rows, extract);
    let cv = classic.final_measure_values();
    assert_eq!(cv.len(), sv.len() + 1);
    assert_eq!(cv[0], 3.0, "distinct auction 数 = 3");
    for i in 0..sv.len() {
        assert_eq!(sv[i], cv[i + 1], "measure[{i}] SoA vs Classic");
    }
    // 手算: n=3; sum_lt=3000（8000/5000 不过 price<5000）; min_lt=3000;
    // max_ge=8000（5000 不过 >=5000 的 where——按值过滤, 非按行）; avg=16000/3。
    assert_eq!(sv[0], 3.0, "count");
    assert_eq!(sv[1], 3000.0, "sum_lt");
    assert_eq!(sv[2], 3000.0, "min_lt");
    assert_eq!(sv[3], 8000.0, "max_ge");
    assert_eq!(sv[4], 16_000.0 / 3.0, "avg");
}
