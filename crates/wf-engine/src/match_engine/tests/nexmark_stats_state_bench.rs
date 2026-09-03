//! nexmark_hotpath_bench 拆出的兄弟子模块（2026-09-04）：stats 路径与 Q18/Q19
//! 状态/输出链归因度量——Q19 stats group by top(10, price) / q4b stats avg(final) 的
//! 行式↔列式基准、Q19 close 输出链分解（逐条目 CloseOutput 构建+drop / 列式 close
//! 全链基线 / fmt 增量 / prepare 物化）、Q18 每键状态内存分账（size_of 栈上 / 真实
//! 每键求和 / bucket_allowance 预算口径 vs 真实）。共享 harness/import 在父模块
//! nexmark_hotpath_bench.rs，此处经 `use super::*` 复用；切片内独占构造随迁。

use super::*;

// ---------------------------------------------------------------------------
// Bench 9：Q19 stats group by auction + top(10, price)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q19_stats_group_topn() {
    let rows = bid_rows(N);
    let batch = bid_batch(N);
    let row_fields: Arc<HashSet<String>> = Arc::new(
        ["auction".into(), "bidder".into(), "price".into()]
            .into_iter()
            .collect(),
    );

    // 行式：group by + per-key top(10)
    let mut exec = StatsExecutor::with_row_fields(q19_stats_plan(), Some(row_fields.clone()));
    let t0 = Instant::now();
    exec.process_rows(&rows, |row, name| row.get(name).cloned());
    let row_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q19 stats rows top10", row_ns, row_ns);

    // 列式：group by + per-key top(10)
    let mut exec2 = StatsExecutor::with_row_fields(q19_stats_plan(), Some(row_fields));
    let t1 = Instant::now();
    assert!(
        exec2.process_batch(&batch),
        "列式前置应满足（Int64 auction/price）"
    );
    let col_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q19 stats batch top10", col_ns, row_ns);
}

/// Q4b：stats `stats<1d:fixed> group by (f.category) { f | avg(f.final) }`——
/// 消费 q4a 中间窗 auction_finals（id/category/final/dateTime）。group 键域
/// 极小（category 0..4），avg 累加——测 stats executor 净成本（2026-08-26
/// q4 归因：q4a staging 列式化后剩余差异主嫌疑）。
fn q4b_stats_plan() -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(86400)), // 1d
        keys: vec![Expr::Field(FieldRef::Qualified(
            "f".into(),
            "category".into(),
        ))],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![StatsMeasurePlan {
            label: "avg_final".into(),
            source_alias: "f".into(),
            where_expr: None,
            agg: StatsAggPlan::Avg,
            field: Some(FieldRef::Qualified("f".into(), "final".into())),
            arg: None,
        }],
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "f".to_string(),
                HashSet::from(["category".to_string(), "final".to_string()]),
            );
            m
        },
    }
}

/// Q4b stats 消费成本（2026-08-26 q4 归因）：1.67M auction_finals 行 →
/// stats group by category avg（键域 5）。行式/列式双路径。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine q4b_stats_group_avg -- --ignored --nocapture"]
fn q4b_stats_group_avg() {
    // auction_finals 形状行（id/category/final；category 域 5，final 连续值）。
    let rows: Vec<HashMap<String, Value>> = (0..N)
        .map(|i| {
            let mut m = HashMap::new();
            m.insert("id".to_string(), num(i as f64));
            m.insert("category".to_string(), num((i % 5) as f64));
            m.insert("final".to_string(), num(10.0 + (i % 997) as f64));
            m
        })
        .collect();
    let batch = {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("category", DataType::Int64, false),
            Field::new("final", DataType::Int64, false),
        ]);
        let ids: Vec<i64> = (0..N as i64).collect();
        let cats: Vec<i64> = (0..N as i64).map(|i| i % 5).collect();
        let finals: Vec<i64> = (0..N as i64).map(|i| 10 + i % 997).collect();
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(cats)),
                Arc::new(Int64Array::from(finals)),
            ],
        )
        .expect("batch")
    };

    // 行式：group by category + avg(final)
    let mut exec = StatsExecutor::with_row_fields(
        q4b_stats_plan(),
        Some(Arc::new(
            ["category".to_string(), "final".to_string()]
                .into_iter()
                .collect(),
        )),
    );
    let t0 = Instant::now();
    exec.process_rows(&rows, |row, name| row.get(name).cloned());
    let row_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4b stats rows avg", row_ns, row_ns);

    // 列式：group by category + avg(final)
    let mut exec2 = StatsExecutor::with_row_fields(q4b_stats_plan(), None);
    let t1 = Instant::now();
    assert!(
        exec2.process_batch(&batch),
        "列式前置应满足（Int64 category/final）"
    );
    let col_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4b stats batch avg", col_ns, row_ns);
}

// ---------------------------------------------------------------------------
// Bench 13：Q19 close 输出链分解（2026-08-25 daemon 采样定位可压点的数据度量）
// ---------------------------------------------------------------------------
//
// 背景：q19 30m diag 墙表主墙 = full 档（+172 ns/evt，61.5%），daemon `sample`
// 定位热点链为 close_current_window(49%) → execute_close_direct_batch_columnar
// (39%)，其内部分项：build_eval_context 10%、commit_close_rows_batch 8% 、
// memmove 8% + malloc 7%（落列/字符串分配）、fmt detail 求值链 5%、逐条目
// CloseOutput 结构构建+析构 ≈13%（build_stats_close_output 5.2 + CloseOutput
// drop 4.2 + Value drop 3.9）。本基准把这三处可压点固化为可复现基线：
//
//   entry_build_drop : 逐条目 CloseOutput 构建 + drop（复刻 build_stats_close_output
//                      的分配形状：scope_key + StepData + field_values 3 键注入）
//                       —— 结构开销（采样 ≈13% 的点）
//   chain_full       : 列式 close 全链 execute_close_direct_batch_columnar（q19 形状
//                      top-10 条目，detail = fmt("{} {}", bidder, price)）——现状基线
//   chain_no_fmt     : 同上但 detail 为常量 → fmt 增量 = full − no_fmt
//   fmt_blackbox     : 黑盒 format!（字符串分配下界参考）
//
// 对照口径：生产 30M 实测 full 档输出链 ≈ 573 ns/alert（172 ns/evt × 30M ÷ 9M
// alert/档）。本基准 N = 50 万条目（≈ 5 万桶 × top-10，对齐 10m 窗桶量级）。

/// q19 列式 close 的 RulePlan：常量 score + entity(Field b.auction) + yield
/// id=Field(b.auction) / alert_type=Lit / detail=fmt 或 Lit / request_count=Number
/// —— 通过 `close_plan_columnar_safe` 门控（与 stats_task 生产路径同形状）。
fn q19_close_columnar_rule(fmt_detail: bool) -> RulePlan {
    let mut plan = simple_rule_plan(
        "q19_auction_top10_stats",
        simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.yield_plan.target = "nexmark_alerts".into();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q19_top10_stats".into()),
        },
        YieldField {
            name: "detail".into(),
            value: if fmt_detail {
                Expr::FuncCall {
                    qualifier: None,
                    name: "fmt".into(),
                    args: vec![
                        Expr::StringLit("{} {}".into()),
                        b_field("bidder"),
                        b_field("price"),
                    ],
                }
            } else {
                Expr::StringLit("q19_top10_stats".into())
            },
        },
        YieldField {
            name: "request_count".into(),
            value: Expr::Number(1.0),
        },
    ];
    plan
}

/// q19 top-10 条目 CloseOutput（复刻 stats_task::build_stats_close_output 的
/// 分配形状：scope_key=[auction]、close_step_data=[top_price + field_values
/// {auction,bidder,price}]——键字段 + row_fields 列数组展开注入）。
fn q19_close_entry(
    rule: &str,
    auction: i64,
    bidder: i64,
    price: i64,
    window_start: i64,
    window_end: i64,
) -> CloseOutput {
    let mut field_values = EngineHashMap::default();
    field_values.insert("auction".into(), vec![Value::Number(auction as f64)]);
    field_values.insert("bidder".into(), vec![Value::Number(bidder as f64)]);
    field_values.insert("price".into(), vec![Value::Number(price as f64)]);
    CloseOutput {
        rule_name: rule.to_string(),
        scope_key: vec![Value::Number(auction as f64)],
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
        close_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some("top_price".into()),
            measure_value: price as f64,
            event_first_time_nanos: Some(window_start),
            event_last_time_nanos: Some(window_end),
            collected_values: vec![],
            field_values,
        }],
        bind_data: vec![],
        watermark_nanos: window_end,
        machine_id: String::new(),
        event_first_time_nanos: window_start,
        event_last_time_nanos: window_end,
        first_match_time_nanos: None,
        evidence_first_time_nanos: window_start,
        evidence_last_time_nanos: window_end,
        window_start_time_nanos: window_start,
        window_end_time_nanos: window_end,
        last_event_nanos: window_end,
        row_fields: None,
        row_field_names: None,
    }
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q19_close_output_chain() {
    use crate::alert::AlertColumnBuilder;

    const W_START: i64 = 1_750_000_000_000_000_000;
    const W_END: i64 = W_START + 600_000_000_000; // 10m 窗
    let rule = "q19_auction_top10_stats";
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    // 桶序：每 10 条目同 auction（top-10 rank 形状）；bidder 近 1000 人域、
    // price 对数均匀（与 bid_events 同数据域）。
    let auctions: Vec<i64> = (0..N).map(|i| AUCTION_BASE + (i / 10) as i64).collect();
    let bidders: Vec<i64> = (0..N)
        .map(|_| BIDDER_BASE + (next_u64(&mut rng) % BIDDER_DOMAIN) as i64)
        .collect();
    let prices: Vec<i64> = (0..N).map(|_| next_price(&mut rng) as i64).collect();

    // ① 逐条目 CloseOutput 构建 + drop（结构分配/析构，采样 ≈13% 的点）
    let t0 = Instant::now();
    let mut guard = 0u64;
    for i in 0..N {
        let co = q19_close_entry(rule, auctions[i], bidders[i], prices[i], W_START, W_END);
        guard = guard.wrapping_add(std::hint::black_box(co).scope_key.len() as u64);
    }
    let entry_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    std::hint::black_box(guard);
    report("q19 entry 构建+drop", entry_ns, entry_ns);

    // 预构造条目（不计时，供 close 链复用）
    let closes: Vec<CloseOutput> = (0..N)
        .map(|i| q19_close_entry(rule, auctions[i], bidders[i], prices[i], W_START, W_END))
        .collect();

    // ② 列式 close 全链（现状基线，detail = fmt）——含 yield 求值 / fmt / wfx_id / 落列
    let exec_full = RuleExecutor::new(q19_close_columnar_rule(true));
    assert!(
        exec_full.close_plan_columnar_safe(),
        "q19 形状必须过列式 close 门控"
    );
    let mut builder = AlertColumnBuilder::new(Arc::from("nexmark_alerts"));
    let t0 = Instant::now();
    let stats = exec_full.execute_close_direct_batch_columnar(&closes, &mut builder, W_END);
    let full_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    assert_eq!(stats.appended, N, "输出行数 = 条目数");
    report("q19 close链 full(fmt)", full_ns, full_ns);

    // ③ 同上但 detail 常量 → fmt 增量 = full − no_fmt
    let exec_nofmt = RuleExecutor::new(q19_close_columnar_rule(false));
    assert!(exec_nofmt.close_plan_columnar_safe());
    let mut builder2 = AlertColumnBuilder::new(Arc::from("nexmark_alerts"));
    let t0 = Instant::now();
    let stats2 = exec_nofmt.execute_close_direct_batch_columnar(&closes, &mut builder2, W_END);
    let nofmt_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    assert_eq!(stats2.appended, N);
    report("q19 close链 no-fmt", nofmt_ns, full_ns);

    // ④ 黑盒 format!（fmt 字符串分配下界参考）
    let t0 = Instant::now();
    let mut len_acc = 0usize;
    for i in 0..N {
        let s = format!("{} {}", bidders[i], prices[i]);
        len_acc = len_acc.wrapping_add(std::hint::black_box(&s).len());
    }
    let fmt_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    std::hint::black_box(len_acc);
    report("q19 fmt黑盒 format!", fmt_ns, full_ns);

    // ⑤ 列式 cell 准备段（close_batch_prepare：引用字段物化 + 编译 + eval_vec）
    //    ——层 1 新增成本的单独归因（fmt 增量 = 准备 + 逐行 cell 读取）。
    let t0 = Instant::now();
    let prepared = exec_full.close_batch_prepare(&closes);
    let prep_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    std::hint::black_box(&prepared);
    report("q19 close prepare(物化)", prep_ns, full_ns);

    // 归因（对齐 2026-08-25 采样占比；chain 计时不含 entry 构建——closes 预构造）
    let fmt_delta = full_ns - nofmt_ns;
    let exec_net = full_ns - entry_ns; // executor 侧净成本：yield 求值 + wfx_id + 落列
    eprintln!(
        "[hotpath] q19 归因: fmt增量={:.1}ns/entry({:.0}% of full) | entry结构={:.1}ns({:.0}%) | executor净成本={:.1}ns({:.0}%)",
        fmt_delta,
        fmt_delta / full_ns * 100.0,
        entry_ns,
        entry_ns / full_ns * 100.0,
        exec_net,
        exec_net / full_ns * 100.0
    );
}

// ---------------------------------------------------------------------------
// Bench 13：Q18 每键状态分账 —— 「键数 × 每键状态」（2026-08-26）
// ---------------------------------------------------------------------------
//
// 背景：q18 = `stats<1d:fixed> group by (bidder, auction)` + 4×last，30M 数据
// 键数 ≈ 2300 万（(bidder,auction) 组合几乎每行唯一——数据特征决定，不可减）。
// 每键状态 = 唯一可压项。本 bench 量化每键构成：
//   1. size_of 栈上（StatsAccum / RowFields / ScopeKey）
//   2. 真实每键内存求和（ScopeKey 堆 + 累加器 + 共享 RowFields 堆 + HashMap 槽）
//   3. `bucket_allowance` 预算口径 vs 真实 → 高估倍数（guard 拒收阈值失真度）
//
// 预期发现（2026-08-26 代码审查）：4 个 last 度量各占一个全功能 `StatsAccum`
// （count/sum/min/max/distinct/top 死字段 ~80% 浪费），真实每键 ≈ 1KB；预算
// 口径 last 按 160B/度量固定计 → 每桶 1664B，高估 ~1.5×。16GB 预算 → 拒收
// 阈值 ~1000 万键 < 30M 数据真实键数 2300 万 → **guard 早拒（语义丢失）**。

/// Q18 形态 stats plan：`stats<1d:fixed> group by (b.bidder, b.auction)` +
/// 4×last（price/channel/url/dateTime），与 q18.wfl 对齐。
fn q18_stats_last_plan() -> StatsPlan {
    fn last(label: &str, field: &str) -> StatsMeasurePlan {
        StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Last,
            field: Some(FieldRef::Qualified("b".into(), field.into())),
            arg: None,
        }
    }
    StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(86400)),
        keys: vec![
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        ],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![
            last("last_price", "price"),
            last("last_channel", "channel"),
            last("last_url", "url"),
            last("last_dateTime", "dateTime"),
        ],
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "b".to_string(),
                HashSet::from([
                    "auction".to_string(),
                    "bidder".to_string(),
                    "price".to_string(),
                    "channel".to_string(),
                    "url".to_string(),
                    "dateTime".to_string(),
                ]),
            );
            m
        },
    }
}

/// Q18 形态列式 batch（auction/bidder/price/dateTime Int64 + channel/url Utf8）。
/// 键域：bidder 1010（真实域）；auction 域放大到 2_000_000 → (bidder,auction)
/// 组合 ≈ 每行唯一（对齐 30M 数据「键数≈行数」的真实形态——域小会严重低估
/// 键数，测不到每键真实成本）。
fn q18_last_batch(n: usize) -> RecordBatch {
    const BIDDER_BASE: i64 = 1000;
    const BIDDER_DOMAIN: u64 = 1010;
    const AUCTION_BASE: i64 = 1000;
    const AUCTION_DOMAIN: u64 = 2_000_000;
    let schema = Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
    ]);
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let auctions: Vec<i64> = (0..n)
        .map(|_| AUCTION_BASE + (next_u64(&mut rng) % AUCTION_DOMAIN) as i64)
        .collect();
    let bidders: Vec<i64> = (0..n)
        .map(|_| BIDDER_BASE + (next_u64(&mut rng) % BIDDER_DOMAIN) as i64)
        .collect();
    let prices: Vec<i64> = (0..n).map(|_| next_price(&mut rng) as i64).collect();
    let channels: Vec<String> = (0..n).map(|_| "Google".to_string()).collect();
    let urls: Vec<String> = (0..n).map(|_| nexmark_url().to_string()).collect();
    let times: Vec<i64> = (0..n).map(|i| NOW + i as i64 * EVENT_STEP_NS).collect();
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(auctions)),
            Arc::new(Int64Array::from(bidders)),
            Arc::new(Int64Array::from(prices)),
            Arc::new(StringArray::from(channels)),
            Arc::new(StringArray::from(urls)),
            Arc::new(Int64Array::from(times)),
        ],
    )
    .expect("q18 batch")
}

/// ScopeKey 堆内存（Box 子节点；Str 长串堆分配忽略——q18 键为数字）。
fn scope_key_heap_bytes(k: &ScopeKey) -> usize {
    match k {
        ScopeKey::Pair(a, b) => {
            // 每个 Box 子节点 = 1 个 ScopeKey 的栈上大小（enum 24B，含 tag）
            size_of::<ScopeKey>() * 2 + scope_key_heap_bytes(a) + scope_key_heap_bytes(b)
        }
        ScopeKey::Str(s) if s.len() > 22 => s.len(),
        _ => 0,
    }
}

/// RowFields 堆内存（Box 数组元素 + null_mask；layout Arc 全局共享不计）。
fn row_fields_heap_bytes(rf: &RowFields) -> usize {
    let l = rf.layout();
    l.n_numeric() * 8
        + l.n_strings() * 24 // SmolStr 24B 内联
        + l.n_others() * size_of::<Option<Value>>()
        + l.n_fields().div_ceil(64) * 8 // null_mask
}

/// Q18 每键状态分账（release-only）。
///
/// 运行：cargo test --release -p wf-engine q18_stats_last_key_state -- --ignored --nocapture
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine q18_stats_last_key_state -- --ignored --nocapture"]
fn q18_stats_last_key_state() {
    eprintln!("[q18-state] === size_of（栈上，不含堆）===");
    eprintln!(
        "[q18-state] size_of::<StatsAccum>()     = {} B",
        size_of::<StatsAccum>()
    );
    eprintln!(
        "[q18-state] size_of::<RowFields>()      = {} B",
        size_of::<RowFields>()
    );
    eprintln!(
        "[q18-state] size_of::<RowFieldLayout>() = {} B",
        size_of::<RowFieldLayout>()
    );
    eprintln!(
        "[q18-state] size_of::<ScopeKey>()       = {} B",
        size_of::<ScopeKey>()
    );

    let row_fields: Arc<HashSet<String>> = Arc::new(
        ["auction", "bidder", "price", "channel", "url", "dateTime"]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    );
    let mut exec = StatsExecutor::with_row_fields(q18_stats_last_plan(), Some(row_fields));
    let batch = q18_last_batch(N);
    assert!(
        exec.process_batch(&batch),
        "列式前置应满足（Int64 键/值 + Utf8 字符串）"
    );
    let n_buckets: usize = exec.window.buckets.values().map(|c| c.len()).sum();
    let n_chains = exec.window.buckets.len();
    let estimated = exec.window.estimated_bytes();
    let allowance = if n_chains > 0 {
        estimated / n_chains as u64
    } else {
        0
    };

    // 真实每键内存求和：ScopeKey 栈+堆 / accs / 共享 RowFields 堆 / HashMap 槽估算
    let mut real_sum = 0usize;
    let mut last_shared = 0usize;
    for chain in exec.window.buckets.values() {
        for b in chain {
            real_sum += size_of_val(b); // StatsBucket 栈（scope_key + accs 载体头）
            real_sum += scope_key_heap_bytes(&b.scope_key);
            // q18 计划（last/top）恒 Classic；SoA 桶不在此路径。
            let StatsBucketAccs::Classic(accs) = &b.accs else {
                unreachable!("q18 last 计划不走 SoA");
            };
            real_sum += accs.len() * size_of::<StatsAccum>();
            let shared = accs.iter().filter(|a| a.last().is_some()).count();
            if shared > 0 {
                last_shared += 1;
                let rf = accs
                    .iter()
                    .find_map(|a| a.last().as_ref())
                    .expect("is_some");
                real_sum += 16 /* Arc 头 */ + row_fields_heap_bytes(rf);
            }
        }
    }
    // HashMap<u64, Vec<StatsBucket>> 槽位（foldhash 控制字 + entry + Vec 头）估算
    let slot_est = n_buckets * 40;
    let real_per_key = (real_sum + slot_est) as f64 / n_buckets as f64;

    eprintln!(
        "[q18-state] === 运行形态（N={} 列式，键域 bidder 1010 × auction 2M ≈ 每行唯一）===",
        N
    );
    eprintln!("[q18-state] 键数 n_buckets            = {}", n_buckets);
    eprintln!("[q18-state] 哈希链数 n_chains          = {}", n_chains);
    eprintln!(
        "[q18-state] 状态估算 estimated_bytes   = {} MB",
        estimated / 1024 / 1024
    );
    eprintln!("[q18-state] 预算/键（allowance 口径）  = {} B", allowance);
    eprintln!(
        "[q18-state] 真实/键（求和 + 槽估算）    = {:.0} B",
        real_per_key
    );
    eprintln!(
        "[q18-state] 预算高估倍数              = {:.2}×（guard 拒收阈值被低估）",
        allowance as f64 / real_per_key
    );
    eprintln!(
        "[q18-state] 共享 last_row 桶数/总桶   = {}/{}（多 last 度量 Arc 共享已生效）",
        last_shared, n_buckets
    );

    // 推算 30M 真实数据（键数 ≈ 2300 万）：16GB 预算下的拒收阈值
    let keys_30m = 23_000_000u64;
    let cap_by_budget = 16_000_000_000u64 / allowance;
    let real_30m_gb = keys_30m as f64 * real_per_key / 1e9;
    eprintln!("[q18-state] === 推算 30M 数据（键数≈{}）===", keys_30m);
    eprintln!(
        "[q18-state] 16GB 预算可容纳键数        = {}（{} 万）{}",
        cap_by_budget,
        cap_by_budget / 10_000,
        if cap_by_budget < keys_30m {
            "⚠ 早于 2300 万拒收 → 新键语义丢失"
        } else {
            ""
        }
    );
    eprintln!(
        "[q18-state] 30M 真实状态内存估算        = {:.1} GB（按当前每键 {:.0}B 求和）",
        real_30m_gb, real_per_key
    );
    eprintln!(
        "[q18-state] 紧凑化后预期（Last 变体 16B/度量 + 共享行字段）: 每键 ≈ {:.0} B, 30M ≈ {:.1} GB, 预算/键 ≈ {} B → 拒收阈值 {} 万键",
        256.0 + 4.0 * 16.0 + 104.0,
        (256.0 + 4.0 * 16.0 + 104.0) * 23_000_000.0 / 1e9,
        256 + 4 * 16 + 112,
        16_000_000_000u64 / (256 + 4 * 16 + 112) / 10_000
    );
}
