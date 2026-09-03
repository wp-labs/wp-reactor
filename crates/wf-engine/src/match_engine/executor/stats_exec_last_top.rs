//! P4 last/top 扩展度量与 row fields（2026-09-04 自 stats_exec_test.rs 拆出；
//! `#[path]` 兄弟子模块）：last 保留最近合格行、top 保 key DESC top-N（先到者
//! 平局/满额替换/预检淘汰/独立参考随机流对拍）；rich close 条目行字段注入与
//! 子集紧凑化（行式/列式一致）；q18/q19 close 键字段注入回归（键来源 =
//! stats_plan.keys）。

use super::*;

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
fn stats_q18_shape_subset_excluding_keys_row_fields_readable() {
    // 生产形态（spawn.rs stats_row_fields P5 优化）: 行字段子集**排除桶键字段**
    // （bidder/auction——注释声称 close 时键字段从 scope_key 单独注入）。
    // 验证排除后 close 条目的行字段仍携带非键字段（price/bidder）。
    let plan = keyed_plan(
        vec![field_key("b", "bidder"), field_key("b", "auction")],
        vec![
            last_measure("last_price", "price"),
            last_measure("last_bidder", "bidder"),
        ],
    );
    let rows = vec![row(&[
        ("bidder", num(7.0)),
        ("auction", num(1.0)),
        ("price", num(250.0)),
    ])];
    // 子集 = 非键字段（spawn.rs 对 q18 移除 bidder/auction 后的形态）
    let subset: Arc<HashSet<String>> = Arc::new(["price".into()].into_iter().collect());
    // 行式路径
    let mut row_exec = StatsExecutor::with_row_fields(plan.clone(), Some(subset.clone()));
    row_exec.process_rows(&rows, extract);
    let r_buckets = row_exec.close_window_by_bucket_rows();
    let r_rf = r_buckets[0].measures[0][0]
        .row_fields
        .as_ref()
        .expect("行式: last 携带行字段");
    assert_eq!(
        row_val(r_rf, &["price".to_string()], "price"),
        Some(num(250.0)),
        "行式: 排除键字段后非键字段仍可读"
    );
    // 列式路径（生产 q18 走列式 process_batch）
    let batch = rows_to_batch(&rows);
    let mut col_exec = StatsExecutor::with_row_fields(plan, Some(subset));
    assert!(col_exec.process_batch(&batch), "应可列式化");
    let c_buckets = col_exec.close_window_by_bucket_rows();
    let c_rf = c_buckets[0].measures[0][0]
        .row_fields
        .as_ref()
        .expect("列式: last 携带行字段");
    assert_eq!(
        row_val(c_rf, &["price".to_string()], "price"),
        Some(num(250.0)),
        "列式: 排除键字段后非键字段仍可读"
    );
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
// q18/q19 close 键字段注入回归（2026-08-30 修复）
// ---------------------------------------------------------------------------
// `execute_stats_close_batch_columnar` 的键来源：修复前用 `match_plan.keys`
// （stats 规则为空），修复后用 `stats_plan.keys`（group by）。本用例端到端验证
// q18/q19 生产形态——行字段子集排除键字段（spawn.rs stats_row_fields）——close
// 直装时 entity/yield 里的**键字段**（b.auction / b.bidder）仍能解析出值。
#[test]
fn stats_close_columnar_resolves_key_fields_in_entity_and_yield() {
    use wf_lang::ast::{CloseMode, MatchMode};
    use wf_lang::plan::{
        BindPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, YieldField, YieldPlan,
    };

    use crate::alert::AlertColumnBuilder;
    use crate::match_engine::RuleExecutor;

    // 键字段的 Qualified FieldRef（entity/yield 里以 Expr::Field 引用）。
    let auction_ref = FieldRef::Qualified("b".into(), "auction".into());
    let bidder_ref = FieldRef::Qualified("b".into(), "bidder".into());
    let stats_plan = keyed_plan(
        vec![field_key("b", "bidder"), field_key("b", "auction")],
        vec![last_measure("last_price", "price")],
    );
    // q18/q19 形态：match_plan.keys = []（stats 规则），键在 stats_plan.keys。
    let rule_plan = RulePlan {
        conv_window: None,
        name: "q18_last_bid_stats".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(86400)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::And,
            match_mode: MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: HashSet::new(),
            tracked_bind_fields: HashMap::new(),
            tracked_plain_fields: HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(stats_plan),
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(auction_ref.clone()),
        },
        yield_plan: YieldPlan {
            target: "nexmark_alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(auction_ref.clone()),
                },
                YieldField {
                    name: "detail".into(),
                    value: Expr::Field(bidder_ref.clone()),
                },
                YieldField {
                    name: "alert_type".into(),
                    value: Expr::StringLit("q18_last_stats".into()),
                },
                YieldField {
                    name: "request_count".into(),
                    value: Expr::Number(1.0),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };
    let exec = RuleExecutor::new(rule_plan);
    assert!(exec.close_plan_columnar_safe(), "q18 形态必须列式安全");

    // 生产形态：行字段子集**排除键字段**（bidder/auction），仅留 price。
    let subset: Arc<HashSet<String>> = Arc::new(["price".into()].into_iter().collect());
    let mut stats = StatsExecutor::with_row_fields(
        keyed_plan(
            vec![field_key("b", "bidder"), field_key("b", "auction")],
            vec![last_measure("last_price", "price")],
        ),
        Some(subset),
    );
    let batch = rows_to_batch(&[row(&[
        ("bidder", num(7.0)),
        ("auction", num(1.0)),
        ("price", num(250.0)),
    ])]);
    assert!(stats.process_batch(&batch), "列式前置应满足");
    let buckets = stats.close_window_by_bucket_rows();
    assert_eq!(buckets.len(), 1, "(bidder=7, auction=1) 单桶");
    let labels: Vec<String> = stats
        .plan
        .measures
        .iter()
        .map(|m| m.label.clone())
        .collect();
    let row_names = stats.row_field_names().cloned();

    let mut builder = AlertColumnBuilder::new(std::sync::Arc::from("nexmark_alerts"));
    let outcome = exec.execute_stats_close_batch_columnar(
        &buckets,
        &labels,
        row_names.as_ref(),
        &mut builder,
        1_700_000_000_000,
        1_700_000_000_000 + 86_400_000_000_000,
    );
    assert_eq!(outcome.appended, 1, "1 行 close 输出");
    assert_eq!(outcome.failed, 0);
    let batch = builder.finish();
    let rows: Vec<_> = batch
        .iter_data_records()
        .collect::<crate::error::CoreResult<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    let field_of = |name: &str| {
        rows[0]
            .items
            .iter()
            .find(|f| f.get_name() == name)
            .map(|f| f.get_value().to_string())
            .unwrap_or_default()
    };
    // 修复前：键字段从空 match_plan.keys 解析 → entity/yield 键字段全 None。
    assert_eq!(
        field_of("__wfu_entity_id"),
        "1",
        "entity = b.auction 键字段必须解析（修复前为 None/空）"
    );
    assert_eq!(field_of("id"), "1", "yield id = b.auction 键字段必须解析");
    assert_eq!(
        field_of("detail"),
        "7",
        "yield detail = b.bidder 键字段必须解析"
    );
}
