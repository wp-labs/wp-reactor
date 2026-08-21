//! Q15 close 累积路径的逐分量微基准——fixed+`and close` 规则每事件
//! `accumulate_close_steps` 的 ns/事件归因（数据版）。
//!
//! 背景（2026-08-22）：q15_bidding_stats（`match<:30m:fixed>` + `and close` 12
//! 个 measure：4 count + 8 distinct）在 10M 数据上 EPS=37k（~25µs/事件）、CPU
//! 单核——引擎对 fixed+close 规则在**每事件**全量累积 close steps。本基准量化
//! 各分量占比，为优化（惰性/定期累积、distinct 免 ValueKey 包装、免 raw 值收集）
//! 提供基线。
//!
//! 运行：
//!   cargo test --release -p wf-engine close_bench -- --ignored --nocapture
//!
//! 测量对象（q15 真实形状：close 12 branch = 4 count（field=None）+ 8 distinct
//! （bidder/auction × 4 价格档），9 个价格分档 guard）：
//!   baseline        : `accumulate_close_steps` 完整路径（逐事件，同生产 advance）
//!   guards          : 9 个 guard 的 eval_expr_ext（解释器）
//!   distinct_valkey : 8 个 distinct：field 提取 + ValueKey::from_value +
//!                     EngineHashSet 插入（当前实现）
//!   distinct_i64    : 同上但原生 i64 key（优化方向：免 ValueKey 包装）
//!   collect_clone   : `update_measure` 的 raw 值收集（8× Value.clone + Vec push）
//!   amort_slice_10s : 惰性累积（每 100k 事件/slice 全量一次）的每事件摊还
//!                     = baseline_ns / 100_000（优化方向：close 状态定期快照）

use std::collections::HashSet;
use std::time::Instant;

use wf_lang::ast::{
    BinOp, CloseMode, CmpOp, Expr, FieldRef, FieldSelector, MatchMode, Measure, Transform,
};
use wf_lang::plan::{AggPlan, BranchPlan, MatchPlan, StepPlan, WindowSpec};

use crate::match_engine::EngineHashSet;
use crate::match_engine::match_engine::{
    EngineHashMap, Event, FieldSource, RollingStats, StepState, Value, ValueKey,
    accumulate_close_steps, eval_expr_ext,
};

use super::helpers::{event, num};

const N: usize = 500_000;
/// q15 引用域：bidder ≈ 最近 1000 人 ± lead，auction ≈ 最近 100 个 ± lead。
const BIDDER_BASE: i64 = 1000;
const BIDDER_DOMAIN: i64 = 1010;
const AUCTION_BASE: i64 = 1000;
const AUCTION_DOMAIN: i64 = 110;
/// 惰性累积假设的 slice 大小（100µs/事件 → 100k 事件 = 10s，对齐官方窗口粒度）。
const SLICE_EVENTS: f64 = 100_000.0;

fn v_f64(v: Option<Value>) -> f64 {
    match v {
        Some(Value::Number(n)) => n,
        _ => 0.0,
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
    // lo <= price && price < hi（官方 q15 档位语义）
    Expr::BinOp {
        op: BinOp::And,
        left: Box::new(price_ge(lo)),
        right: Box::new(price_lt(hi)),
    }
}

fn agg(distinct: bool) -> AggPlan {
    AggPlan {
        transforms: if distinct {
            vec![Transform::Distinct]
        } else {
            vec![]
        },
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        threshold: Expr::Number(1.0),
    }
}

/// q15 `q15_bidding_stats` 的 12 个 close branch（与 .wfl 完全同构）。
fn q15_close_steps() -> Vec<StepPlan> {
    let branch =
        |label: &str, field: Option<&str>, guard: Option<Expr>, distinct: bool| BranchPlan {
            label: Some(label.to_string()),
            source: "b".to_string(),
            field: field.map(|f| FieldSelector::Dot(f.to_string())),
            guard,
            agg: agg(distinct),
        };
    // 4 count（field=None）
    let count_branches = [
        branch("total", None, None, false),
        branch("r1", None, Some(price_lt(10_000.0)), false),
        branch("r2", None, Some(price_range(10_000.0, 1_000_000.0)), false),
        branch("r3", None, Some(price_ge(1_000_000.0)), false),
    ];
    // 8 distinct（bidder/auction × 4 档；total 档无 guard）
    let distinct_branches = [
        branch("total_bidder", Some("bidder"), None, true),
        branch("r1_bidder", Some("bidder"), Some(price_lt(10_000.0)), true),
        branch(
            "r2_bidder",
            Some("bidder"),
            Some(price_range(10_000.0, 1_000_000.0)),
            true,
        ),
        branch(
            "r3_bidder",
            Some("bidder"),
            Some(price_ge(1_000_000.0)),
            true,
        ),
        branch("total_auction", Some("auction"), None, true),
        branch(
            "r1_auction",
            Some("auction"),
            Some(price_lt(10_000.0)),
            true,
        ),
        branch(
            "r2_auction",
            Some("auction"),
            Some(price_range(10_000.0, 1_000_000.0)),
            true,
        ),
        branch(
            "r3_auction",
            Some("auction"),
            Some(price_ge(1_000_000.0)),
            true,
        ),
    ];
    count_branches
        .into_iter()
        .chain(distinct_branches)
        .map(|b| StepPlan { branches: vec![b] })
        .collect()
}

fn q15_plan() -> MatchPlan {
    MatchPlan {
        keys: vec![],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
        // q15 的 on-event 步骤：`on event { b | count >= 1; }`。
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "b".to_string(),
                field: None,
                guard: None,
                agg: agg(false),
            }],
        }],
        close_steps: q15_close_steps(),
        close_mode: CloseMode::And,
        match_mode: MatchMode::Seq,
        accu: false,
        seq: None,
        tracked_bind_aliases: HashSet::from(["b".to_string()]),
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "b".to_string(),
                HashSet::from([
                    "price".to_string(),
                    "bidder".to_string(),
                    "auction".to_string(),
                ]),
            );
            m
        },
        tracked_plain_fields: HashSet::new(),
        needs_field_history: false,
    }
}

/// 对数均匀价格（官方 nextPrice = 10^(6u)×100 ∈ [100, 1e8)）+ 引用域内的
/// bidder/auction（确定性 LCG，失败可复现；bidder/auction 域小 → distinct
/// 集合在真实规模，插入含碰撞路径）。
fn bid_events(n: usize) -> Vec<Event> {
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
            event(vec![
                ("price", num(price)),
                (
                    "bidder",
                    num((BIDDER_BASE + next(BIDDER_DOMAIN as u64) as i64) as f64),
                ),
                (
                    "auction",
                    num((AUCTION_BASE + next(AUCTION_DOMAIN as u64) as i64) as f64),
                ),
            ])
        })
        .collect()
}

/// 价格分档（官方 q15 阈值）：<1e4 / [1e4, 1e6) / >=1e6。
fn price_tier(price: f64) -> usize {
    if price < 10_000.0 {
        0
    } else if price < 1_000_000.0 {
        1
    } else {
        2
    }
}

struct Report {
    name: &'static str,
    per_ns: f64,
}

impl Report {
    fn line(&self, baseline_ns: f64) {
        eprintln!(
            "[close-bench] {:<20} {:>8.1} ns/evt  ({:>6.2}M evt/s)  = {:>6.1}% of baseline",
            self.name,
            self.per_ns,
            1e3 / self.per_ns,
            self.per_ns / baseline_ns * 100.0
        );
    }
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine close_bench -- --ignored --nocapture"]
fn q15_close_accumulate_components() {
    let plan = q15_plan();
    assert_eq!(plan.close_steps.len(), 12, "q15 应有 12 个 close step");
    let events = bid_events(N);
    let now = 1_700_000_000_000_000_000i64;

    // ---- engine_full：CepStateMachine 完整 advance 路径（实例管理 + 事件步骤 + close
    // 累积 + 窗口推进；生产真实路径，用于与纯 accumulate_close_steps 对比归因） ----
    let mut sm = crate::match_engine::match_engine::CepStateMachine::new(
        "q15_bench".to_string(),
        q15_plan(),
        None,
    );
    let start = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        std::hint::black_box(sm.advance_at("b", ev, now + i as i64));
    }
    let full = Report {
        name: "engine_full(advance)",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    full.line(full.per_ns);
    let full_ns = full.per_ns;

    // ---- baseline：accumulate_close_steps 完整路径（逐事件，同生产） ----
    let mut step_states: Vec<StepState> = plan
        .close_steps
        .iter()
        .map(|s| StepState::new(s.branches.len()))
        .collect();
    let mut baselines = EngineHashMap::<String, RollingStats>::default();
    let start = Instant::now();
    for ev in &events {
        accumulate_close_steps(
            "b",
            ev,
            now,
            &plan,
            &mut step_states,
            None,
            &mut baselines,
            0,
            None,
        );
    }
    let baseline_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    let baseline = Report {
        name: "baseline(完整累积)",
        per_ns: baseline_ns,
    };
    baseline.line(baseline_ns);

    // ---- close 累积占完整 advance 路径的比例（核心归因） ----
    eprintln!(
        "[close-bench] accumulate_close_steps 占完整 advance = {:.1}% （其余 = 实例/事件步骤/窗口推进）",
        baseline_ns / full_ns * 100.0
    );

    // ---- guards：9 个价格分档 guard 的 eval_expr_ext（解释器） ----
    let guards: Vec<Expr> = [
        price_lt(10_000.0),
        price_range(10_000.0, 1_000_000.0),
        price_ge(1_000_000.0),
    ]
    .into_iter()
    .cycle()
    .take(9)
    .collect();
    let mut baselines = EngineHashMap::<String, RollingStats>::default();
    let start = Instant::now();
    let mut hit = 0u32;
    for ev in &events {
        for g in &guards {
            hit += u32::from(matches!(
                std::hint::black_box(eval_expr_ext(g, ev, None, &mut baselines)),
                Some(Value::Bool(true))
            ));
        }
    }
    assert!(hit > 0, "价格分档 guard 应命中");
    let g = Report {
        name: "guards(9×解释器)",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    g.line(baseline_ns);

    // ---- distinct_valkey：8 个 distinct（field 提取 + ValueKey + EngineHashSet） ----
    let mut sets: Vec<EngineHashSet<ValueKey>> = (0..8).map(|_| Default::default()).collect();
    let mut inserted = 0usize;
    let start = Instant::now();
    for ev in &events {
        let tier = price_tier(v_f64(ev.field_value("price")));
        let b = ev.field_value("bidder").unwrap();
        let a = ev.field_value("auction").unwrap();
        for (idx, val) in [(0usize, &b), (1 + tier, &b), (4, &a), (5 + tier, &a)] {
            if sets[idx].insert(ValueKey::from_value(val)) {
                inserted += 1;
            }
        }
    }
    assert!(inserted > 0);
    let d = Report {
        name: "distinct_valkey",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    d.line(baseline_ns);

    // ---- distinct_i64：原生 i64 key（优化方向：免 ValueKey 包装 + 直接域内哈希） ----
    let mut sets64: Vec<EngineHashSet<i64>> = (0..8).map(|_| Default::default()).collect();
    let mut inserted = 0usize;
    let start = Instant::now();
    for ev in &events {
        let tier = price_tier(v_f64(ev.field_value("price")));
        let b = v_f64(ev.field_value("bidder")) as i64;
        let a = v_f64(ev.field_value("auction")) as i64;
        for (idx, val) in [(0usize, b), (1 + tier, b), (4, a), (5 + tier, a)] {
            if sets64[idx].insert(val) {
                inserted += 1;
            }
        }
    }
    assert!(inserted > 0);
    let d64 = Report {
        name: "distinct_i64",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    d64.line(baseline_ns);

    // ---- collect_clone：update_measure 的 raw 值收集（8× field + Value.clone + push） ----
    let mut heaps: Vec<Vec<Value>> = (0..8).map(|_| Vec::new()).collect();
    let start = Instant::now();
    for ev in &events {
        let b = ev.field_value("bidder");
        let a = ev.field_value("auction");
        for (h, v) in heaps.iter_mut().zip([&b, &b, &b, &b, &a, &a, &a, &a]) {
            if let Some(v) = v {
                h.push(v.clone());
            }
        }
    }
    let c = Report {
        name: "collect_clone(8×)",
        per_ns: start.elapsed().as_secs_f64() * 1e9 / N as f64,
    };
    c.line(baseline_ns);

    // ---- 惰性累积摊还：每 slice 全量一次 → 每事件成本 = baseline / SLICE_EVENTS ----
    let amort = Report {
        name: "amort_slice_10s",
        per_ns: baseline_ns / SLICE_EVENTS,
    };
    amort.line(baseline_ns);

    eprintln!();
    eprintln!(
        "[close-bench] baseline = {:.1} ns/evt → 惰性累积(每{:.0}k事件一次)上限 = {:.2} ns/evt ({:.0}×)",
        baseline_ns,
        SLICE_EVENTS / 1e3,
        baseline_ns / SLICE_EVENTS,
        SLICE_EVENTS
    );
}
