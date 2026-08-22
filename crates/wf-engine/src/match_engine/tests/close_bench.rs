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
//!
//! 生产外围路径（2026-08-22 追加——状态机 advance 之外，rule_task 每行循环）：
//!   masks_build     : `RuleExecutor::branch_guard_masks` 列式 guard mask（batch 级，摊到行）
//!
//! 已否决的优化（2026-08-22 A/B 实测，勿重试）：
//!   - `needs_collected_values`（update_measure 跳过 raw 值收集）：q15 每事件仅 4
//!     branch 带 field 值，push_capped 只省 ~7ns，但新增参数破坏 accumulate_close_steps
//!     内联 → 实测 -7~-13ns/evt 负优化（baseline 550 vs collect_on 542）。
//!   - distinct 集合换 `foldhash::fast::FixedState`：固定种子反而比 RandomState 慢
//!     ~49%（distinct_valkey_fixed 92ns vs valkey 62ns）；RandomState 的
//!     `GlobalSeed::get` 是原子读已足够快，且固定种子失去 foldhash 的每-hasher
//!     seed 混合。`distinct_i64`（原生 i64 key）仍是有效方向（35ns vs 62ns）。
//!   scan_per_row    : `scan_expired_at_with_conv_skip_non_alerting` 每行过期扫描
//!   deferred_row    : ColumnarEvent + advance_at_with_masks（列式 guard，deferred 路径）
//!   eager_row       : 每行 Event 物化（HashMap 3 字段）+ advance_at（解释器 guard，eager 路径）
//!   prod_row_full   : masks 摊还 + scan + advance_with_masks（复刻 rule_task deferred 行）
//!
//! stats 执行器对照（2026-08-22 追加——P1 行式 StatsExecutor vs CEP 同数据同机）：
//!   q15_stats_executor_profile：同 bid_events(N) 数据, 12 度量（4 count + 8 distinct）
//!     stats 行式全量 : process_rows（where **内建求值**: 每行 1 次 ctx + 去重后
//!                      唯一条件共享——q15 9 度量 where → 3 唯一表达式）
//!     分量: count_only / where9（1× build + 9 eval, 未共享）/ where3（1× build + 3 eval
//!           共享分档参考）/ distinct（4× DistinctKey 插入/行）
//!     列式段（P1.5）为下一步：count/sum 整列归并 + where 列式 mask, distinct 每行哈希
//!     不可回避——本基准的行式基线即为列式化的优化依据。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{
    BinOp, CloseMode, CmpOp, Expr, FieldRef, FieldSelector, MatchMode, Measure, Transform,
};
use wf_lang::plan::{AggPlan, BindPlan, BranchPlan, MatchPlan, StepPlan, WindowSpec};

use crate::match_engine::EngineHashSet;
use crate::match_engine::RuleExecutor;
use crate::match_engine::event_bridge::ColumnarEvent;
use crate::match_engine::executor::StatsExecutor;
use crate::match_engine::match_engine::{
    CepStateMachine, EngineHashMap, Event, FieldSource, RollingStats, StepState, Value, ValueKey,
    accumulate_close_steps, eval_expr_ext,
};

use super::helpers::{event, num, simple_rule_plan};

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

    // ===================================================================
    // 生产外围路径（rule_task 每行循环）：状态机 advance 之外的每行开销
    // ===================================================================
    let prod = q15_production_path(&full.per_ns);
    eprintln!(
        "[close-bench] 状态机 advance {:.0}ns vs 生产每行 {:.0}ns → 外围增量 = {:.0} ns/evt ({:.0}%)",
        full.per_ns,
        prod,
        prod - full.per_ns,
        (prod - full.per_ns) / full.per_ns * 100.0
    );
}

/// q15 的 RulePlan（bind=b / bid_events + q15 match_plan），供 RuleExecutor 路径。
fn q15_rule_plan() -> wf_lang::plan::RulePlan {
    let mut plan = simple_rule_plan(
        "q15_bench",
        q15_plan(),
        Expr::Number(10.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds = vec![BindPlan {
        alias: "b".into(),
        window: "bid_events".into(),
        filter: None,
    }];
    plan
}

/// q15 形状的 Arrow 批（与 nexmark_pk 7 列一致；dateTime 列供事件时间）。
fn q15_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
    ]));
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = |range: u64| {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (rng >> 33) % range
    };
    let mut auction = Vec::with_capacity(n);
    let mut bidder = Vec::with_capacity(n);
    let mut price = Vec::with_capacity(n);
    let mut date_time = Vec::with_capacity(n);
    for _ in 0..n {
        auction.push(1000 + (next(110) as i64));
        bidder.push(1000 + (next(1010) as i64));
        price.push((10f64.powf((next(1_000_000) as f64 / 1_000_000.0) * 6.0) * 100.0) as i64);
        date_time.push(1_700_000_000_000_000_000i64 + next(1_000_000) as i64);
    }
    let channel = vec!["mobile"; n];
    let url = vec!["https://www.nexmark.com/aaaa/bbbb/cccc/item.htm?query=1"; n];
    let extra = vec!["x"; n];
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)) as ArrayRef,
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(price)),
            Arc::new(StringArray::from(channel)),
            Arc::new(StringArray::from(url)),
            Arc::new(Int64Array::from(date_time)),
            Arc::new(StringArray::from(extra)),
        ],
    )
    .unwrap()
}

/// 复刻 rule_task 每行循环的生产路径，返回完整生产每行 ns。
fn q15_production_path(engine_full_ns: &f64) -> f64 {
    const ROWS: usize = 200_000;
    let exec = RuleExecutor::new(q15_rule_plan());
    let batch = q15_batch(ROWS);
    let nanos = 1_700_000_000_000_000_000i64;

    // ---- masks_build：列式 guard mask（batch 级一次，摊到每行） ----
    let start = Instant::now();
    let masks = exec.branch_guard_masks(&batch);
    let masks_ns = start.elapsed().as_secs_f64() * 1e9 / ROWS as f64;
    let masks_report = Report {
        name: "masks_build(摊还)",
        per_ns: masks_ns,
    };
    masks_report.line(*engine_full_ns);

    // ---- scan_per_row：每行过期扫描（fixed 30m 单实例，watermark 递增） ----
    let mut sm = CepStateMachine::new("q15_bench".to_string(), q15_plan(), None);
    let start = Instant::now();
    for i in 0..ROWS {
        std::hint::black_box(
            sm.scan_expired_at_with_conv_skip_non_alerting(nanos + i as i64, None),
        );
    }
    let scan_ns = start.elapsed().as_secs_f64() * 1e9 / ROWS as f64;
    let scan_report = Report {
        name: "scan_per_row",
        per_ns: scan_ns,
    };
    scan_report.line(*engine_full_ns);

    // ---- deferred_row：ColumnarEvent + advance_at_with_masks（列式 guard） ----
    let mut sm = CepStateMachine::new("q15_bench".to_string(), q15_plan(), None);
    let start = Instant::now();
    for i in 0..ROWS {
        let ev = ColumnarEvent::new(&batch, i);
        std::hint::black_box(sm.advance_at_with_masks(
            "b",
            &ev,
            nanos + i as i64,
            None,
            i,
            Some(&masks),
        ));
    }
    let deferred_ns = start.elapsed().as_secs_f64() * 1e9 / ROWS as f64;
    let deferred_report = Report {
        name: "deferred_row(列式)",
        per_ns: deferred_ns,
    };
    deferred_report.line(*engine_full_ns);

    // ---- eager_row：每行 Event 物化 + advance_at（解释器 guard） ----
    let mut sm = CepStateMachine::new("q15_bench".to_string(), q15_plan(), None);
    let start = Instant::now();
    for i in 0..ROWS {
        let ev = ColumnarEvent::new(&batch, i);
        let owned = ev.to_event();
        std::hint::black_box(sm.advance_at("b", &owned, nanos + i as i64));
    }
    let eager_ns = start.elapsed().as_secs_f64() * 1e9 / ROWS as f64;
    let eager_report = Report {
        name: "eager_row(物化)",
        per_ns: eager_ns,
    };
    eager_report.line(*engine_full_ns);

    // ---- prod_row_full：masks 摊还 + scan + advance_with_masks（完整生产行） ----
    let mut sm = CepStateMachine::new("q15_bench".to_string(), q15_plan(), None);
    let start = Instant::now();
    for i in 0..ROWS {
        let ev = ColumnarEvent::new(&batch, i);
        sm.scan_expired_at_with_conv_skip_non_alerting(nanos + i as i64, None);
        std::hint::black_box(sm.advance_at_with_masks(
            "b",
            &ev,
            nanos + i as i64,
            None,
            i,
            Some(&masks),
        ));
    }
    let prod_ns = start.elapsed().as_secs_f64() * 1e9 / ROWS as f64;
    let prod_report = Report {
        name: "prod_row_full(生产)",
        per_ns: prod_ns,
    };
    prod_report.line(*engine_full_ns);

    prod_ns
}

// ---------------------------------------------------------------------------
// stats 执行器 Q15 profile（P1 行式基线, 列式段 P1.5 的优化依据）
// ---------------------------------------------------------------------------

/// q15 形状的 stats StatsPlan（12 度量: 4 count + 8 distinct, 8 个带价格分档
/// where）——与 `q15_close_steps` 同构（同档位阈值/字段域）, 同数据可横向对拍。
fn q15_stats_plan() -> wf_lang::plan::StatsPlan {
    use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan};
    let m = |label: &str, agg: StatsAggPlan, field: Option<&str>, where_expr: Option<Expr>| {
        StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr,
            agg,
            field: field.map(|f| FieldRef::Qualified("b".into(), f.into())),
            arg: None,
        }
    };
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
        keys: vec![],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![
            m("total", StatsAggPlan::Count, None, None),
            m("r1", StatsAggPlan::Count, None, Some(price_lt(10_000.0))),
            m(
                "r2",
                StatsAggPlan::Count,
                None,
                Some(price_range(10_000.0, 1_000_000.0)),
            ),
            m("r3", StatsAggPlan::Count, None, Some(price_ge(1_000_000.0))),
            m(
                "total_bidder",
                StatsAggPlan::DistinctCount,
                Some("bidder"),
                None,
            ),
            m(
                "r1_bidder",
                StatsAggPlan::DistinctCount,
                Some("bidder"),
                Some(price_lt(10_000.0)),
            ),
            m(
                "r2_bidder",
                StatsAggPlan::DistinctCount,
                Some("bidder"),
                Some(price_range(10_000.0, 1_000_000.0)),
            ),
            m(
                "r3_bidder",
                StatsAggPlan::DistinctCount,
                Some("bidder"),
                Some(price_ge(1_000_000.0)),
            ),
            m(
                "total_auction",
                StatsAggPlan::DistinctCount,
                Some("auction"),
                None,
            ),
            m(
                "r1_auction",
                StatsAggPlan::DistinctCount,
                Some("auction"),
                Some(price_lt(10_000.0)),
            ),
            m(
                "r2_auction",
                StatsAggPlan::DistinctCount,
                Some("auction"),
                Some(price_range(10_000.0, 1_000_000.0)),
            ),
            m(
                "r3_auction",
                StatsAggPlan::DistinctCount,
                Some("auction"),
                Some(price_ge(1_000_000.0)),
            ),
        ],
        tracked_bind_fields: HashMap::new(),
    }
}

fn extract_field(row: &HashMap<String, Value>, name: &str) -> Option<Value> {
    row.get(name).cloned()
}

fn rows_from_events(events: &[Event]) -> Vec<HashMap<String, Value>> {
    events
        .iter()
        .map(|ev| {
            ev.fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect()
        })
        .collect()
}

/// Q15 stats 执行器 profile：行式全量 + 分量（count/where/distinct）, 与 CEP
/// 同数据同机对照（engine_full advance + accumulate_close_steps）。
///
/// 运行：
///   cargo test --release -p wf-engine close_bench -- --ignored --nocapture
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine close_bench -- --ignored --nocapture"]
fn q15_stats_executor_profile() {
    use crate::match_engine::executor::DistinctKey;
    let events = bid_events(N);
    let rows = rows_from_events(&events);
    let now = 1_700_000_000_000_000_000i64;

    eprintln!(
        "[close-bench] ===== Q15 stats vs CEP profile（N={}, 同数据同机）=====",
        N
    );

    // ---- CEP 对照（与 q15_close_accumulate_components 同路径, 同一次运行）----
    let mut sm = CepStateMachine::new("q15_bench".to_string(), q15_plan(), None);
    let start = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        std::hint::black_box(sm.advance_at("b", ev, now + i as i64));
    }
    let cep_full_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    Report {
        name: "CEP engine_full",
        per_ns: cep_full_ns,
    }
    .line(cep_full_ns);

    let plan = q15_plan();
    let mut step_states: Vec<StepState> = plan
        .close_steps
        .iter()
        .map(|s| StepState::new(s.branches.len()))
        .collect();
    let mut baselines0 = EngineHashMap::<String, RollingStats>::default();
    let start = Instant::now();
    for ev in &events {
        accumulate_close_steps(
            "b",
            ev,
            now,
            &plan,
            &mut step_states,
            None,
            &mut baselines0,
            0,
            None,
        );
    }
    let cep_accum_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    Report {
        name: "CEP accumulate_close",
        per_ns: cep_accum_ns,
    }
    .line(cep_full_ns);

    // ---- stats 行式全量（P1: process_rows 内建 where 求值——每行 1 次 ctx
    // 构建 + 去重后唯一条件求值, 同条件度量共享结果）----
    let stats_plan = q15_stats_plan();
    let exprs: Vec<Option<Expr>> = stats_plan
        .measures
        .iter()
        .map(|m| m.where_expr.clone())
        .collect();
    // 收集 9 个带 where 表达式（owned）, 供分量 2 使用。
    let with_where: Vec<Expr> = exprs.iter().flatten().cloned().collect();
    assert_eq!(
        with_where.len(),
        9,
        "q15 9 个带 where 度量（r1/r2/r3 × count/bidder/auction）"
    );
    let start = Instant::now();
    let mut stats_exec = StatsExecutor::new(stats_plan);
    stats_exec.process_rows(&rows, extract_field);
    let stats_full_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    let values = stats_exec.final_measure_values();
    assert_eq!(values[0], N as f64, "total count 应为 N");
    Report {
        name: "stats 行式全量",
        per_ns: stats_full_ns,
    }
    .line(cep_full_ns);
    eprintln!(
        "[close-bench]    → vs CEP engine_full {:.2}× ; vs accumulate_close 的 {:.0}%",
        cep_full_ns / stats_full_ns,
        stats_full_ns / cep_accum_ns * 100.0
    );

    // ---- 分量 1: count_only（4 count 无 where 无 distinct——行式纯归并下限）----
    let count_plan = {
        let mut p = q15_stats_plan();
        p.measures.retain(|m| {
            matches!(m.agg, wf_lang::plan::StatsAggPlan::Count) && m.where_expr.is_none()
        });
        p
    };
    assert_eq!(
        count_plan.measures.len(),
        1,
        "q15 仅 total 一个无条件 count"
    );
    let start = Instant::now();
    let mut exec = StatsExecutor::new(count_plan);
    exec.process_rows(&rows, extract_field);
    let count_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    Report {
        name: "  分量 count(×1)",
        per_ns: count_ns,
    }
    .line(cep_full_ns);

    // ---- 分量 2: where9（9 个带 where 度量 × Event build + eval/行, 当前实现）----
    let baselines = std::cell::RefCell::new(EngineHashMap::<String, RollingStats>::default());
    let start = Instant::now();
    let mut hits = 0u64;
    for row in &rows {
        let ctx = Event {
            fields: row
                .iter()
                .map(|(k, v)| (k.as_str().into(), v.clone()))
                .collect(),
        };
        for e in &with_where {
            hits += u64::from(matches!(
                std::hint::black_box(eval_expr_ext(e, &ctx, None, &mut baselines.borrow_mut())),
                Some(Value::Bool(true))
            ));
        }
    }
    assert!(hits > 0);
    let where9_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    Report {
        name: "  分量 where9(当前)",
        per_ns: where9_ns,
    }
    .line(cep_full_ns);

    // ---- 分量 3: where_shared（1× Event build + 3 eval, 共享分档——优化参考）----
    let shared: Vec<Expr> = [
        price_lt(10_000.0),
        price_range(10_000.0, 1_000_000.0),
        price_ge(1_000_000.0),
    ]
    .into_iter()
    .collect();
    let baselines = std::cell::RefCell::new(EngineHashMap::<String, RollingStats>::default());
    let start = Instant::now();
    let mut hits = 0u64;
    for row in &rows {
        let ctx = Event {
            fields: row
                .iter()
                .map(|(k, v)| (k.as_str().into(), v.clone()))
                .collect(),
        };
        for e in &shared {
            hits += u64::from(matches!(
                std::hint::black_box(eval_expr_ext(e, &ctx, None, &mut baselines.borrow_mut())),
                Some(Value::Bool(true))
            ));
        }
    }
    assert!(hits > 0);
    let where3_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    Report {
        name: "  分量 where3(共享)",
        per_ns: where3_ns,
    }
    .line(cep_full_ns);

    // ---- 分量 4: distinct_8（4× DistinctKey 插入/行, 8 集合）----
    let mut sets: Vec<std::collections::HashSet<DistinctKey>> =
        (0..8).map(|_| Default::default()).collect();
    let mut inserted = 0u64;
    let start = Instant::now();
    for row in &rows {
        let price = match row.get("price") {
            Some(Value::Number(p)) => *p,
            _ => 0.0,
        };
        let tier = price_tier(price);
        let b = DistinctKey::from_f64(match row.get("bidder") {
            Some(Value::Number(n)) => *n,
            _ => 0.0,
        });
        let a = DistinctKey::from_f64(match row.get("auction") {
            Some(Value::Number(n)) => *n,
            _ => 0.0,
        });
        for (idx, key) in [(0usize, &b), (1 + tier, &b), (4, &a), (5 + tier, &a)] {
            if sets[idx].insert(key.clone()) {
                inserted += 1;
            }
        }
    }
    assert!(inserted > 0);
    let distinct_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    Report {
        name: "  分量 distinct(×4)",
        per_ns: distinct_ns,
    }
    .line(cep_full_ns);

    // ---- 汇总 ----
    eprintln!(
        "[close-bench] ---- 汇总（P1 行式基线; 列式段 P1.5 目标: count/where 整列化, distinct 不可回避）----"
    );
    eprintln!(
        "[close-bench]   行式全量（内建共享 where）应 ≈ count={count_ns:.0} + where3={where3_ns:.0} + distinct={distinct_ns:.0} = {:.0} ns/evt（对照实测 stats_full_ns）",
        count_ns + where3_ns + distinct_ns
    );
    eprintln!(
        "[close-bench]   列式化后可消灭 ≈ count/where 全部 + where 摊还 → 理论下限 ≈ distinct {distinct_ns:.0} ns/evt（{:.0}M/s）",
        1e3 / distinct_ns
    );
}
