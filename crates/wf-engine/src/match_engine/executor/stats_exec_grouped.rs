//! 分组桶（group by，2026-09-04 自 stats_exec_test.rs 拆出；`#[path]` 兄弟
//! 子模块）：字段键/复合键/bucket/tier 函数键行式-列式逐桶对拍、null 键跳过；
//! 复合键扁平哈希（comps_hash）与树哈希（scope_key_hash）同桶契约；分片行子集
//! （process_batch_rows 只归并行域）；≥2^53 与 Timestamp 原生列值精度回归。

use super::*;

// ---------------------------------------------------------------------------
// P2 复合键分组（group by）: 字段键 / 复合键 / bucket / tier / 列式对拍
// ---------------------------------------------------------------------------

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
