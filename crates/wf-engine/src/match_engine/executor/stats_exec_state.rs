//! 窗口状态内存 guard 与 SoA 桶专项（2026-09-04 自 stats_exec_test.rs 拆出；
//! `#[path]` 兄弟子模块）：`limits.max_memory` 超限拒收新键桶（行式/列式/
//! merge_partial/账本口径/q18 预算回归）；SoA（q17 优化）内部精确值/null 语义/
//! 行式-列式对拍/Classic 底层累积对拍/分片合并/close 三路径/窗口重置/形态门控/
//! guard 记账。

use super::*;

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

#[test]
fn stats_memory_guard_rejects_new_buckets_over_limit() {
    // top(3) 桶预算 = 512 + 1×128 + 3×160 = 1120B。限额 1200 → 只放行 1 桶。
    let plan = q19_like_plan();
    let mut exec = StatsExecutor::new(plan);
    exec.set_memory_limit("guard_test", Some(2640));

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
        exec.window.estimated_bytes() <= 2640,
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
    exec.set_memory_limit("guard_test", Some(2640));

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
    exec.set_memory_limit("guard_test", Some(2640));
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
    exec.set_memory_limit("guard_test", Some(2640));

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
    assert!(exec.window.estimated_bytes() <= 2640);
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
    col_exec.set_memory_limit("guard_test", Some(2640));
    assert!(col_exec.process_batch(&batch), "列式前置满足");
    assert_eq!(col_exec.window.over_limit_new_buckets(), 4);
    assert_eq!(
        col_exec.window.event_count, 1,
        "被拒 4 行不计入 event_count"
    );

    // 行式对拍: 同一输入 → 同样只归并 1 行。
    let mut row_exec = StatsExecutor::new(plan);
    row_exec.set_memory_limit("guard_test", Some(2640));
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
    exec.set_memory_limit("guard_test", Some(2640)); // 只放 1 桶

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
    exec.set_memory_limit("guard_test", Some(2640)); // 只放 1 桶

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
    exec.set_memory_limit("guard_q18_shape", Some(95_000_000));
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
    assert_eq!(
        v1.1,
        vec![
            5.0,
            2.0,
            2.0,
            1.0,
            5_000.0,
            2_000_000.0,
            408_000.0,
            2_040_000.0
        ]
    );
    let v2 = &vals[1];
    assert_eq!(v2.0, ScopeKey::Int(2));
    assert_eq!(
        v2.1,
        vec![2.0, 2.0, 0.0, 0.0, 3_000.0, 3_000.0, 3_000.0, 6_000.0]
    );
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
    assert!(
        col_exec.process_batch(&rows_to_batch(&rows)),
        "列式前置应满足"
    );
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
        BooleanArray::from(
            (0..n)
                .map(|i| price.value(i) >= 1_000_000)
                .collect::<Vec<_>>(),
        ),
    ];
    let measure_where: Vec<Option<usize>> =
        vec![None, Some(0), Some(1), Some(2), None, None, None, None];
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
    assert_eq!(
        col.final_measure_values(),
        vec![3.0, 600.0, 200.0, 100.0, 300.0]
    );
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
    exec.process_rows(&auction_price_rows(&[(1.0, 100.0), (2.0, 200.0)]), extract);
    assert_eq!(
        exec.window.over_limit_new_buckets(),
        1,
        "第 2 键超 384B 拒收"
    );
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
