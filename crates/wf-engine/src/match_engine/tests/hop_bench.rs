//! HOP + top_ties 新增算子的性能基准（release 跑，`--ignored` 形态）。
//!
//! 背景（2026-08-23）：HOP 滑动窗口（`match<key:hop(size, slide)>`）与 conv
//! `top_ties(N)`（RANK 并列全输出）是新算子。HOP 每事件扇入 `size/slide` 个覆盖
//! 窗口（Q5：hop(10s, 2s) = 5× 实例放大）；top_ties 在 sort 后需额外比较排序键
//! 判定并列。本基准量化两者相对既有路径（fixed 窗口 / top 截断）的增量成本。
//!
//! 运行：
//!   cargo test --release -p wf-engine hop_bench -- --ignored --nocapture
//!
//! 测量对象（Q5 真实形状：auction 键 + `and close` count）：
//!   hop_vs_fixed_advance : 同数据流下 hop(10s,2s) vs fixed(10s) 每事件
//!                          advance 成本（含每事件 scan_expired，复刻 rule_task）
//!   hop_scan_cost        : hop 滑动边界的过期扫描成本（无界预算收口）
//!   top_ties_vs_top      : Q5/Q7 收口批（10k CloseOutput）sort+top(1) vs
//!                          sort+top_ties(1)，有/无并列两种分布
//!
//! 2026-08-24 优化后基线（Apple M3 Max，release，N=200k 事件、100µs/事件）：
//!   hop_vs_fixed_advance : hop(10,2)=831 ns/evt（1.20M eps）| hop(10,10)=247
//!     ns/evt（4.05M eps）| fixed(10)=220 ns/evt（4.55M eps）。
//!     → 每窗口成本 146 ns/window（(831−247)/4）；hop(10,10)≈fixed（+12%）
//!       验证单窗口路径无额外开销。
//!   hop_scan_cost        : 873 ns/evt（含 advance + slide 边界扫描），
//!     200k 事件收口 18k 窗口，扫描摊还成本可忽略。
//!   top_ties_vs_top      : 无并列 +0%、高并列 +2%（2026-08-24 优化后：
//!     `apply_chain` 对 `sort | top_ties` 相邻对共享一次 key 预提取，双倍 eval
//!     消除——此前 +46~55%）。
//!
//! 2026-08-23 基线（改前，存档）：hop(10,2)=942 ns/evt（1.06M eps）；每窗口
//!   成本 ≈ 181 ns/window；top_ties 增量 +36~52%（双倍 eval key）。
//! 2026-08-24 优化：① `advance_window` 实例取用改 entry（contains+remove+insert
//!   三次哈希 → contains+entry 两次，remove/insert 不再破坏 HashMap 缓存局部性），
//!   每窗口成本 172→146 ns（−15%）；② conv `sort | top_ties` 合并共享 key_rows。

use std::time::Instant;

use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, Measure};
use wf_lang::plan::{
    AggPlan, BranchPlan, ConvChainPlan, ConvOpPlan, ConvPlan, ExprPlan, MatchPlan, SortKeyPlan,
    StepPlan, WindowSpec,
};

use crate::match_engine::apply_conv;
use crate::match_engine::match_engine::{
    CepStateMachine, CloseOutput, CloseReason, EngineHashMap, Event, StepData, Value,
};

/// Q5 引用域：auction ≈ 热点域（HotAuctionBatch）± lead。
const AUCTION_DOMAIN: i64 = 2000;
const N: i64 = 200_000;
/// nexmark 固定事件间隔 100µs。
const TICK_NS: i64 = 100_000;

/// bench 规模可由环境变量放大（sample profile 用）：WF_HOP_BENCH_N=2000000
fn bench_n() -> i64 {
    std::env::var("WF_HOP_BENCH_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(N)
}

fn event(auction: f64) -> Event {
    let mut fields: EngineHashMap<smol_str::SmolStr, Value> = EngineHashMap::default();
    fields.insert("auction".into(), Value::Number(auction));
    fields.insert("bidder".into(), Value::Number(1.0));
    Event { fields }
}

/// Q5 形状 plan：auction 键 + `on event` + `and close` count。
fn q5_plan(window_spec: WindowSpec) -> MatchPlan {
    let branch = || BranchPlan {
        label: None,
        source: "b".to_string(),
        field: None,
        guard: None,
        agg: AggPlan {
            transforms: vec![],
            measure: Measure::Count,
            cmp: CmpOp::Ge,
            threshold: Expr::Number(1.0),
        },
    };
    MatchPlan {
        keys: vec![FieldRef::Simple("auction".to_string())],
        key_map: None,
        key_join: None,
        window_spec,
        event_steps: vec![StepPlan {
            branches: vec![branch()],
        }],
        close_steps: vec![StepPlan {
            branches: vec![branch()],
        }],
        close_mode: CloseMode::And,
        tracked_bind_aliases: std::collections::HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: std::collections::HashSet::new(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

/// 跑同一事件流：每事件 scan（rule_task 生产行为）+ advance。
/// 返回 ns/事件。
fn run_advance_scan(window_spec: WindowSpec, n: i64) -> f64 {
    let mut sm = CepStateMachine::new("q5_hop".to_string(), q5_plan(window_spec), None);
    // 预热
    for i in 0..10_000 {
        let t = i * TICK_NS;
        let e = event(((i % AUCTION_DOMAIN) + 1000) as f64);
        sm.advance_at("b", &e, t);
    }
    let start = Instant::now();
    for i in 0..n {
        let t = i * TICK_NS;
        let e = event(((i % AUCTION_DOMAIN) + 1000) as f64);
        sm.scan_expired_at_skip_non_alerting_unbounded(t);
        sm.advance_at("b", &e, t);
    }
    start.elapsed().as_nanos() as f64 / n as f64
}

#[test]
#[ignore]
fn bench_hop_vs_fixed_advance() {
    // Q5 形状：hop(10s, 2s) 每事件 5 窗口 vs fixed(10s) 每事件 1 桶。
    // hop(10s, 10s) 是 size=slide 退化（≡ fixed，每事件 1 窗口）——作为
    // "单窗口"参照，hop(10,2) − hop(10,10) = 额外 4 个窗口的开销。
    let n = bench_n();
    let hop5 = run_advance_scan(
        WindowSpec::Hop {
            size: std::time::Duration::from_secs(10),
            slide: std::time::Duration::from_secs(2),
        },
        n,
    );
    let hop1 = run_advance_scan(
        WindowSpec::Hop {
            size: std::time::Duration::from_secs(10),
            slide: std::time::Duration::from_secs(10),
        },
        n,
    );
    let fixed = run_advance_scan(WindowSpec::Fixed(std::time::Duration::from_secs(10)), n);
    eprintln!(
        "hop_vs_fixed_advance ({} 事件): hop(10,2)={:.0} ns/evt ({:.2}M eps) | hop(10,10)={:.0} ns/evt ({:.2}M eps) | fixed={:.0} ns/evt ({:.2}M eps)",
        n,
        hop5,
        1e9 / hop5 / 1e6,
        hop1,
        1e9 / hop1 / 1e6,
        fixed,
        1e9 / fixed / 1e6
    );
    eprintln!(
        "  → 每窗口成本 {:.0} ns/window（(hop5−hop1)/4）；hop(10,10)≈fixed 验证单窗口等价 {:.0}%",
        (hop5 - hop1) / 4.0,
        (hop1 / fixed - 1.0) * 100.0
    );
    // 无硬断言（性能回归由 bench 报告人工判定）；仅防极端退化。
    assert!(hop5 / fixed < 10.0, "hop 每事件 5 窗口不应超过 fixed 10×");
}

/// 只跑 hop 扫描路径：事件流 + 每 slide 边界 scan（oracle 口径）。
#[test]
#[ignore]
fn bench_hop_scan_cost() {
    let mut sm = CepStateMachine::new(
        "q5_hop_scan".to_string(),
        q5_plan(WindowSpec::Hop {
            size: std::time::Duration::from_secs(10),
            slide: std::time::Duration::from_secs(2),
        }),
        None,
    );
    let mut total_closes = 0usize;
    let start = Instant::now();
    let mut last_bucket: i64 = i64::MIN;
    let mut last_scan = 0i64;
    for i in 0..N {
        let t = i * TICK_NS;
        let e = event(((i % AUCTION_DOMAIN) + 1000) as f64);
        // slide 边界扫描（oracle fixed_bucket_nanos 口径）
        let bucket = t.div_euclid(2_000_000_000);
        if bucket != last_bucket {
            last_bucket = bucket;
            last_scan += 1;
            total_closes += sm.scan_expired_at_skip_non_alerting_unbounded(t).len();
        }
        sm.advance_at("b", &e, t);
    }
    let elapsed = start.elapsed().as_nanos() as f64;
    eprintln!(
        "hop_scan_cost ({} 事件, {} 次 slide 边界扫描, {} 窗口收口): {:.0} ns/evt, 扫描摊还 {:.0} ns/evt",
        N,
        last_scan,
        total_closes,
        elapsed / N as f64,
        elapsed / N as f64
    );
    assert!(total_closes > 0, "hop 窗口应在扫描中收口");
}

fn labeled_step(label: &str, value: f64) -> StepData {
    StepData {
        satisfied_branch_index: 0,
        label: Some(label.to_string()),
        measure_value: value,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: Vec::new(),
        field_values: EngineHashMap::default(),
    }
}

fn make_close(scope: Vec<Value>, m: f64) -> CloseOutput {
    CloseOutput {
        rule_name: "q5".to_string(),
        scope_key: scope,
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
        close_step_data: vec![labeled_step("n", m)],
        bind_data: vec![],
        watermark_nanos: 0,
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: String::new(),
        last_event_nanos: 0,
        row_fields: None,
        row_field_names: None,
    }
}

/// 收口批：Q5 窗口规模 ~2k auction，构造 10k 条 CloseOutput（m = count）。
/// tie = true：高并列（5 个值循环，并列条目多）；tie = false：无并列（唯一值）。
fn conv_batch(n: usize, tie: bool) -> Vec<CloseOutput> {
    let m = if tie {
        // 高并列：count 聚集在少量值（大量并列最高 count 条目）
        |i: usize| ((i % 5) as f64) * 10.0
    } else {
        // 无并列：唯一 count
        |i: usize| i as f64
    };
    (0..n)
        .map(|i| make_close(vec![Value::Number((i + 1000) as f64)], m(i)))
        .collect()
}

/// 收口批：按位运算构造并列分布——tie_every = 并列块大小（1 = 无并列）。
fn conv_batch_with_ties(n: usize, tie_every: usize) -> Vec<CloseOutput> {
    (0..n)
        .map(|i| {
            make_close(
                vec![Value::Number((i + 1000) as f64)],
                (i / tie_every) as f64,
            )
        })
        .collect()
}

fn run_conv(ops: Vec<ConvOpPlan>, batch: Vec<CloseOutput>) -> f64 {
    let plan = ConvPlan {
        chains: vec![ConvChainPlan { ops }],
    };
    let keys = [FieldRef::Simple("auction".to_string())];
    // 预热
    apply_conv(&plan, &keys, batch.clone());
    let start = Instant::now();
    let iters = 200;
    for _ in 0..iters {
        apply_conv(&plan, &keys, batch.clone());
    }
    start.elapsed().as_nanos() as f64 / iters as f64
}

#[test]
#[ignore]
fn bench_conv_top_ties_vs_top() {
    const BATCH: usize = 10_000;
    let sort_key = SortKeyPlan {
        expr: ExprPlan::Field(FieldRef::Simple("n".into())),
        descending: true,
    };
    let sort = ConvOpPlan::Sort(vec![sort_key.clone()]);
    let top = ConvOpPlan::Top(1);
    let top_ties = ConvOpPlan::TopTies {
        n: 1,
        sort_keys: vec![sort_key.clone()],
    };

    // 无并列分布：top_ties 的并列判定需比较到尾。
    let plain = conv_batch(BATCH, false);
    let t_top = run_conv(vec![sort.clone(), top.clone()], plain.clone());
    let t_ties = run_conv(vec![sort.clone(), top_ties.clone()], plain.clone());
    eprintln!(
        "top_ties_vs_top (10k 批, 无并列): top={:.0} µs  top_ties={:.0} µs  增量={:.0}%",
        t_top / 1000.0,
        t_ties / 1000.0,
        (t_ties - t_top) / t_top * 100.0
    );

    // 高并列分布：并列判定在前几条即命中截断（代价更低）。
    let tied = conv_batch(BATCH, true);
    let t_top_t = run_conv(vec![sort.clone(), top], tied.clone());
    let t_ties_t = run_conv(vec![sort, top_ties], tied.clone());
    eprintln!(
        "top_ties_vs_top (10k 批, 高并列): top={:.0} µs  top_ties={:.0} µs  增量={:.0}%",
        t_top_t / 1000.0,
        t_ties_t / 1000.0,
        (t_ties_t - t_top_t) / t_top_t * 100.0
    );

    // 并列块大小扫描：并列判定循环长度 = 并列条目数（tie_every 块大小直接
    // 决定 top_ties 的额外比较数——无并列只比 1 条，全并列比到尾部）。
    for tie_every in [1usize, 10, 100, 1000] {
        let batch = conv_batch_with_ties(BATCH, tie_every);
        let t_t = run_conv(
            vec![ConvOpPlan::Sort(vec![sort_key.clone()]), ConvOpPlan::Top(1)],
            batch.clone(),
        );
        let t_tt = run_conv(
            vec![
                ConvOpPlan::Sort(vec![sort_key.clone()]),
                ConvOpPlan::TopTies {
                    n: 1,
                    sort_keys: vec![sort_key.clone()],
                },
            ],
            batch,
        );
        eprintln!(
            "top_ties tie_every={tie_every:>4}: top={:.0} µs  top_ties={:.0} µs  增量={:+.0}%",
            t_t / 1000.0,
            t_tt / 1000.0,
            (t_tt - t_t) / t_t * 100.0
        );
    }
}
