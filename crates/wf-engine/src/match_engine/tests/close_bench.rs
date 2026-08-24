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
//!   - scan_per_row    : `scan_expired_at_with_conv_skip_non_alerting` 每行过期扫描
//!   - deferred_row    : ColumnarEvent + advance_at_with_masks（列式 guard，deferred 路径）
//!   - eager_row       : 每行 Event 物化（HashMap 3 字段）+ advance_at（解释器 guard，eager 路径）
//!   - prod_row_full   : masks 摊还 + scan + advance_with_masks（复刻 rule_task deferred 行）
//!
//! stats 执行器对照（2026-08-22 追加——P1 行式 StatsExecutor vs CEP 同数据同机）：
//!   q15_stats_executor_profile：同 bid_events(N) 数据, 12 度量（4 count + 8 distinct）
//!     stats 行式全量 : process_rows（where **内建求值**: 每行 1 次 ctx + 去重后
//!                      唯一条件共享——q15 9 度量 where → 3 唯一表达式）
//!     stats 列式全量 : process_batch（P1.5: where 列式 mask + count 整列归并 +
//!                      distinct 行式段按 mask true 行读原生列值插入）
//!     分量: count_only / where9（1× build + 9 eval, 未共享）/ where3（1× build + 3 eval
//!           共享分档参考）/ distinct（4× DistinctKey 插入/行）
//!     实测: 行式 450 ns/evt → 列式 115 ns/evt（3.9×）; 列式相对 CEP 生产路径 ~3×,
//!     理论下限 ≈ distinct 87 ns/evt（distinct 每行哈希不可回避）。
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{
    BinOp, CloseMode, CmpOp, Expr, FieldRef, FieldSelector, MatchMode, Measure, Transform,
};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, MatchPlan, StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan,
    StatsPlan, StepPlan, WindowSpec,
};

use crate::match_engine::EngineHashSet;
use crate::match_engine::RuleExecutor;
use crate::match_engine::StatsAccum;
use crate::match_engine::event_bridge::{
    ColumnarEvent, batch_event_time_nanos_at, batch_time_col_index,
};
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
        trigger_event_needed: false,
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

/// rows → Int64 列 RecordBatch（price/bidder/auction; 与行式 rows 数值字节一致,
/// 保证列式/行式对拍同一分档——round() 的整数 f64 → i64 无损）。
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
    .unwrap()
}

/// Q15 stats 执行器 profile：行式全量 + 分量（count/where/distinct）, 与 CEP
/// 同数据同机对照（engine_full advance + accumulate_close_steps）。
///
/// 运行：
///   cargo test --release -p wf-engine close_bench -- --ignored --nocapture
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine close_bench -- --ignored --nocapture"]
fn q15_stats_executor_profile() {
    use crate::match_engine::DistinctKey;
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

    // ---- stats 列式全量（P1.5: where 列式 mask + count 整列归并 + distinct 行式段）----
    let batch = rows_to_batch(&rows);
    let start = Instant::now();
    let mut col_exec = StatsExecutor::new(q15_stats_plan());
    assert!(col_exec.process_batch(&batch), "q15 计划应可列式化");
    let stats_col_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    let col_values = col_exec.final_measure_values();
    assert_eq!(col_values, values, "列式/行式 12 值应一致");
    Report {
        name: "stats 列式全量",
        per_ns: stats_col_ns,
    }
    .line(cep_full_ns);
    eprintln!(
        "[close-bench]    → 列式 vs 行式 {:.2}× ; vs CEP engine_full {:.2}×",
        stats_full_ns / stats_col_ns,
        cep_full_ns / stats_col_ns
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
    eprintln!("[close-bench] ---- 汇总（列式段 P1.5 已落地）----");
    eprintln!(
        "[close-bench]   行式 {stats_full_ns:.0} ns/evt → 列式 {stats_col_ns:.0} ns/evt（{:.2}×）; 理论下限 ≈ distinct {distinct_ns:.0} ns/evt（{:.0}M/s）",
        stats_full_ns / stats_col_ns,
        1e3 / distinct_ns
    );
    eprintln!(
        "[close-bench]   列式剩余开销 ≈ {:.0} ns/evt = where mask 摊还 + 度量循环 + 列解析（distinct 每行哈希不可回避）",
        stats_col_ns - distinct_ns
    );
}

/// 构造 N 行 price/bidder/auction/event_time 批（event_time 线性 0..span, 100µs/evt
/// 对齐 nexmark 速率; 窗口边界落在批内, 复刻 Q12/Q18/Q19 批跨边界形态）。
fn time_batch(n: usize, span_ns: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let step = span_ns / n as i64;
    let ts: Vec<i64> = (0..n as i64).map(|i| i * step).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64; n])),
            Arc::new(Int64Array::from(vec![1i64; n])),
            Arc::new(Int64Array::from(vec![1i64; n])),
            Arc::new(TimestampNanosecondArray::from(ts)),
        ],
    )
    .unwrap()
}

/// 窗口切段边界扫描（复刻 stats_task::process_batch_from 的段循环——扫时间列,
/// 每段起点行索引; 不含归并, 测量切段本身的最小成本）。
fn segment_starts(batch: &RecordBatch, time_col: usize, dur_nanos: i64) -> Vec<usize> {
    let n = batch.num_rows();
    let mut starts = vec![0usize];
    let mut window_end: Option<i64> = None;
    for i in 0..n {
        let t = batch_event_time_nanos_at(batch, time_col, i);
        let bucket = (t / dur_nanos) * dur_nanos;
        match window_end {
            None => window_end = Some(bucket + dur_nanos),
            Some(end) if t >= end => {
                window_end = Some(bucket + dur_nanos);
                starts.push(i);
            }
            Some(_) => {}
        }
    }
    starts
}

/// 切段 + 每段列式归并（复刻 process_batch_from 的归并侧: 段行子集喂
/// process_batch_rows）。
fn process_segmented(
    exec: &mut StatsExecutor,
    batch: &RecordBatch,
    time_col: usize,
    dur_nanos: i64,
) {
    let starts = segment_starts(batch, time_col, dur_nanos);
    let n = batch.num_rows();
    for (k, &s) in starts.iter().enumerate() {
        let e = if k + 1 < starts.len() {
            starts[k + 1]
        } else {
            n
        };
        let seg: Vec<u32> = (s..e).map(|i| i as u32).collect();
        let ok = exec.process_batch_rows(batch, Some(&seg));
        assert!(ok, "count-only 计划应可列式化");
    }
}

/// stats 窗口切段热路径 profile（P4 追加）: `process_batch_from` 对**每个 batch**
/// 扫时间列切段（批跨窗口边界时逐段归并, Q12 10s 窗 / Q18/Q19 30m 窗）——
/// 量化切段开销相对整批归并基线, 防止切段逻辑性能回归。
///
/// 运行:
///   cargo test --release -p wf-engine close_bench -- --ignored --nocapture
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine close_bench -- --ignored --nocapture"]
fn stats_window_segmentation_profile() {
    let n = 500_000usize;
    let dur_nanos = 10 * 1_000_000_000i64; // 10s 窗
    let span_ns = dur_nanos * 2; // 批跨 2 个窗口（1 边界）
    let batch = time_batch(n, span_ns);
    let time_col = batch_time_col_index(&batch, Some("event_time")).expect("event_time col");
    let starts = segment_starts(&batch, time_col, dur_nanos);
    assert!(
        starts.len() >= 2,
        "批应跨至少 1 个窗口边界, 实际段数={}",
        starts.len()
    );

    // keyed count 计划（group by bidder, 近似 Q12 真实形态——每行哈希; 整批归并
    // 本身 O(n), 对照公平; 空键 sum 会被向量化到 ~0.4 ns/evt, 虚高切段占比）。
    let count_plan = wf_lang::plan::StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(10)),
        keys: vec![Expr::Field(FieldRef::Qualified(
            "b".into(),
            "bidder".into(),
        ))],
        output_shape: wf_lang::plan::StatsOutputShapePlan::Rows,
        measures: vec![wf_lang::plan::StatsMeasurePlan {
            label: "n".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Count,
            field: None,
            arg: None,
        }],
        tracked_bind_fields: HashMap::new(),
    };

    eprintln!(
        "[close-bench] ===== stats 窗口切段热路径 profile（N={}, 10s 窗, 段数={}, {} 边界）=====",
        n,
        starts.len(),
        starts.len() - 1
    );

    // ---- 分量 1: segment_scan（仅时间列扫描切段, 无归并）----
    let iters = 20;
    let start = Instant::now();
    let mut total_starts = 0usize;
    for _ in 0..iters {
        let s = segment_starts(&batch, time_col, dur_nanos);
        total_starts += s.len();
        std::hint::black_box(&s);
    }
    assert!(total_starts > 0);
    let scan_ns = start.elapsed().as_secs_f64() * 1e9 / (n as f64 * iters as f64);
    Report {
        name: "切段 segment_scan(仅扫描)",
        per_ns: scan_ns,
    }
    .line(scan_ns);

    // ---- 分量 2: segmented_merge（切段 + 每段列式归并, 生产完整路径）----
    let start = Instant::now();
    let mut seg_exec = StatsExecutor::new(count_plan.clone());
    process_segmented(&mut seg_exec, &batch, time_col, dur_nanos);
    let seg_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    let seg_total = seg_exec.final_measure_values();
    assert_eq!(seg_total[0], n as f64, "切段归并 count = N");
    Report {
        name: "切段+归并 segmented_merge",
        per_ns: seg_ns,
    }
    .line(seg_ns);

    // ---- 分量 3: whole_batch（对照基线: 无切段, 整批一次归并）----
    let start = Instant::now();
    let mut whole_exec = StatsExecutor::new(count_plan.clone());
    assert!(whole_exec.process_batch(&batch));
    let whole_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    let whole_total = whole_exec.final_measure_values();
    assert_eq!(whole_total[0], n as f64, "keyed count 总和 = N");
    Report {
        name: "整批归并 whole_batch(对照)",
        per_ns: whole_ns,
    }
    .line(whole_ns);

    eprintln!(
        "[close-bench]   切段热路径: 扫描 {scan_ns:.1} ns/evt, 切段+归并 {seg_ns:.1} vs 整批归并 {whole_ns:.1}（切段附加 {:.1} ns/evt = {:.1}%）",
        seg_ns - whole_ns,
        (seg_ns - whole_ns) / whole_ns * 100.0
    );

    // ---- 边界密集对照: 同一批跨 10 个窗口（9 边界）——段数放大每段的
    // domain_mask/view 前置（每段 O(n) 构建）, 量化切段附加对段数的敏感度。
    let dense_batch = time_batch(n, dur_nanos * 10);
    let dense_starts = segment_starts(&dense_batch, time_col, dur_nanos);
    let start = Instant::now();
    let mut dense_exec = StatsExecutor::new(count_plan.clone());
    process_segmented(&mut dense_exec, &dense_batch, time_col, dur_nanos);
    let dense_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    assert_eq!(dense_exec.final_measure_values()[0], n as f64);
    Report {
        name: "切段+归并 边界密集(10 段)",
        per_ns: dense_ns,
    }
    .line(seg_ns);
    eprintln!(
        "[close-bench]   边界密集({} 段) vs 单边界: {:.1} vs {:.1} ns/evt（段数放大附加 {:.1}×）",
        dense_starts.len(),
        dense_ns,
        seg_ns,
        (dense_ns - whole_ns) / (seg_ns - whole_ns)
    );
}

/// last/top 热路径批（Q18/Q19 形态）: 10k 个 auction 桶 × 10 万 bidder 值伪随机
/// 交错 → (bidder, auction) 组合 ~10 万桶, 每桶 ~5 行（对齐 Q18 真实分布:
/// 27.6M bids / 5.29M 组合 ≈ 5.2 行/桶, 唯一率 ~19%）——避免全唯一键的
/// 高桶密度缓存放大。price 伪随机, 每桶多条 → last 替换 + top 有界插入路径。
fn last_top_batch(n: usize) -> RecordBatch {
    // schema 对齐真实 Q18 bid 表: price/bidder/auction/event_time + 4 个 last 度量
    // 字段（channel/url/dateTime——行字段提取子集的关键成员）。
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("channel", DataType::Int64, true),
        Field::new("url", DataType::Int64, true),
        Field::new("dateTime", DataType::Int64, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let step = 30 * 60 * 1_000_000_000i64 / n as i64; // 30m 跨度
    let ts: Vec<i64> = (0..n as i64).map(|i| i * step).collect();
    let price: Vec<i64> = (0..n as i64)
        .map(|i| (i * 7919 % 1_000_000) + 100)
        .collect();
    let channel: Vec<i64> = (0..n as i64).map(|i| (i * 104_729) % 100_000).collect();
    let url: Vec<i64> = (0..n as i64).map(|i| i * 15_485_863 % 100_000).collect();
    let date_time: Vec<i64> = (0..n as i64).map(|i| i % 1_000_000).collect();
    // 104729 与 100000 互质 → (i*104729)%100000 与 i%10000 的组合周期 = 100000,
    // 每组合 5 行（n=500k）——~10 万桶, 对齐 Q18 唯一率
    let bidder: Vec<i64> = (0..n as i64)
        .map(|i| ((i * 104_729) % 100_000) + 1)
        .collect();
    let auction: Vec<i64> = (0..n as i64).map(|i| (i % 10_000) + 1).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(price)),
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(auction)),
            Arc::new(Int64Array::from(channel)),
            Arc::new(Int64Array::from(url)),
            Arc::new(Int64Array::from(date_time)),
            Arc::new(TimestampNanosecondArray::from(ts)),
        ],
    )
    .unwrap()
}

fn keyed_stats_plan(keys: Vec<Expr>, measures: Vec<StatsMeasurePlan>) -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
        keys,
        output_shape: StatsOutputShapePlan::Rows,
        measures,
        tracked_bind_fields: HashMap::new(),
    }
}

fn last_measure(field: &str, label: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Last,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn top_measure(field: &str, label: &str, n: u64) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Top,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: Some(n),
    }
}

/// stats last/top 热路径 profile（P5 紧凑化）: 带 key 列式（Q18/Q19 形态）——
/// 每事件行字段列数组提取 + apply_last_top（last 保留 / top 有界插入）。对比
/// 同形态 keyed count-only 基线, 量化 last/top 边际成本。
///
/// 运行:
///   cargo test --release -p wf-engine close_bench -- --ignored --nocapture
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine close_bench -- --ignored --nocapture"]
fn stats_last_top_keyed_profile() {
    let n = 500_000usize;
    let batch = last_top_batch(n);
    // 行字段提取子集 = 生产 spawn 形态（yield ∪ 度量字段, **排除桶键**——
    // bidder/auction 已由 scope_key 注入 field_values）。Q18 = 4 字段。
    let subset: Arc<HashSet<String>> = Arc::new(
        ["price", "channel", "url", "dateTime"]
            .into_iter()
            .map(String::from)
            .collect(),
    );
    let auction_key = Expr::Field(FieldRef::Qualified("b".into(), "auction".into()));
    let bidder_key = Expr::Field(FieldRef::Qualified("b".into(), "bidder".into()));

    // 基线: keyed count-only（每行 1 次桶哈希, 无行字段提取）
    let count_plan = keyed_stats_plan(
        vec![auction_key.clone()],
        vec![StatsMeasurePlan {
            label: "n".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Count,
            field: None,
            arg: None,
        }],
    );
    let start = Instant::now();
    let mut exec = StatsExecutor::new(count_plan);
    assert!(exec.process_batch_rows(&batch, None));
    let count_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    Report {
        name: "keyed count(基线)",
        per_ns: count_ns,
    }
    .line(count_ns);

    // 复合键 count-only（隔离: 复合键查找本身 vs 单键基线）
    let comp_key_count = keyed_stats_plan(
        vec![bidder_key.clone(), auction_key.clone()],
        vec![StatsMeasurePlan {
            label: "n".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Count,
            field: None,
            arg: None,
        }],
    );
    let start = Instant::now();
    let mut exec = StatsExecutor::new(comp_key_count);
    assert!(exec.process_batch_rows(&batch, None));
    let comp_count_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    Report {
        name: "复合键 count",
        per_ns: comp_count_ns,
    }
    .line(count_ns);

    // Q18 形态: 复合键 (bidder, auction) + 4 last 度量
    let q18_plan = keyed_stats_plan(
        vec![bidder_key.clone(), auction_key.clone()],
        vec![
            last_measure("price", "last_price"),
            last_measure("channel", "last_channel"),
            last_measure("url", "last_url"),
            last_measure("dateTime", "last_dt"),
        ],
    );
    let start = Instant::now();
    let mut exec = StatsExecutor::with_row_fields(q18_plan.clone(), Some(subset.clone()));
    assert!(exec.process_batch_rows(&batch, None));
    let q18_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    Report {
        name: "Q18 复合键 4×last",
        per_ns: q18_ns,
    }
    .line(count_ns);

    // Q18 对照: 不排除桶键（6 字段全提取）——量化 spawn「桶键不入行」收益
    let full_subset: Arc<HashSet<String>> = Arc::new(
        ["price", "channel", "url", "dateTime", "bidder", "auction"]
            .into_iter()
            .map(String::from)
            .collect(),
    );
    let start = Instant::now();
    let mut exec = StatsExecutor::with_row_fields(q18_plan.clone(), Some(full_subset));
    assert!(exec.process_batch_rows(&batch, None));
    let q18_full_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    Report {
        name: "Q18 不排键(6 字段)",
        per_ns: q18_full_ns,
    }
    .line(count_ns);

    // 单键 + 4 last（隔离: 复合键 Pair 盒装与 last 提取分开计）
    let last4_plan = keyed_stats_plan(
        vec![auction_key.clone()],
        vec![
            last_measure("price", "last_price"),
            last_measure("channel", "last_channel"),
            last_measure("url", "last_url"),
            last_measure("dateTime", "last_dt"),
        ],
    );
    let start = Instant::now();
    let mut exec = StatsExecutor::with_row_fields(last4_plan, Some(subset.clone()));
    assert!(exec.process_batch_rows(&batch, None));
    let last4_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    Report {
        name: "单键 4×last",
        per_ns: last4_ns,
    }
    .line(count_ns);

    // Q19 形态: 单键 auction + top(10, price)
    let q19_plan = keyed_stats_plan(
        vec![auction_key],
        vec![top_measure("price", "top_price", 10)],
    );
    let start = Instant::now();
    let mut exec = StatsExecutor::with_row_fields(q19_plan, Some(subset.clone()));
    assert!(exec.process_batch_rows(&batch, None));
    let q19_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;
    Report {
        name: "Q19 形态 top(10)",
        per_ns: q19_ns,
    }
    .line(count_ns);

    eprintln!("[close-bench] ---- last/top 热路径汇总（P5 紧凑列数组）----");
    eprintln!(
        "[close-bench]   复合键查找 = {:.1} ns/evt; 单键 4×last 边际 = {:.1}; Q18(4 字段) = {:.1}; Q18(不排键 6 字段) = {:.1}（桶键不入行收益 = {:.1}）; Q19 top10 边际 = {:.1}",
        comp_count_ns - count_ns,
        last4_ns - count_ns,
        q18_ns,
        q18_full_ns,
        q18_full_ns - q18_ns,
        q19_ns - count_ns
    );
}

// ---------------------------------------------------------------------------
// q15 EOS 归并（`StatsExecutor::merge_partial`）逐分量微基准（2026-08-24）
// ---------------------------------------------------------------------------
//
// 背景：q15 输入分区分片后，协调片 EOS 归并 ~883ms 串行（9 片 × 8 distinct 集
// union ≈ 68M 次 insert）。本基准量化当前 `merge_accum`（`os.clone()` + 无
// reserve 的 `extend`）的成本构成，并对比候选优化：
//   move        : merge_partial 改用 owned `into_iter`——协调片 None 时直接
//                 move 整个 set（免 68M 元素克隆）; extend 也 move 元素免 clone
//   move+reserve: extend 前 `reserve(o.len())` 预扩容，免多轮 rehash
//   move+小并大  : 小集插大集（union by size），rehash 次数按小集容量增长
//
// 运行：
//   cargo test --release -p wf-engine close_bench q15_merge_partial -- --ignored --nocapture

/// 生产 merge_accum 逻辑的内联副本（bench 隔离: 不依赖 executor 内部）。
/// 与生产 `merge_partial` 一致——借用 `&StatsAccum`（每度量不额外 clone）:
/// None 时整集 `os.clone()`（协调片首次归并）, 否则逐元素 `iter().cloned()`。
fn merge_accum_cur(t: &mut StatsAccum, o: &StatsAccum) {
    t.count += o.count;
    t.sum_i128 += o.sum_i128;
    t.min = match (t.min, o.min) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    t.max = match (t.max, o.max) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    match (&mut t.distinct_set, &o.distinct_set) {
        (Some(ts), Some(os)) => ts.extend(os.iter().cloned()),
        (None, Some(os)) => t.distinct_set = Some(os.clone()),
        _ => {}
    }
}

/// 候选: owned move（免克隆; 协调片 None 时直接吞 set）。
fn merge_accum_move(t: &mut StatsAccum, o: StatsAccum) {
    t.count += o.count;
    t.sum_i128 += o.sum_i128;
    t.min = match (t.min, o.min) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    t.max = match (t.max, o.max) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    match (&mut t.distinct_set, o.distinct_set) {
        (Some(ts), Some(os)) => ts.extend(os),
        (None, Some(os)) => t.distinct_set = Some(os),
        _ => {}
    }
}

/// 候选: owned move + extend 前 reserve 预扩容。
fn merge_accum_move_reserve(t: &mut StatsAccum, o: StatsAccum) {
    t.count += o.count;
    t.sum_i128 += o.sum_i128;
    t.min = match (t.min, o.min) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    t.max = match (t.max, o.max) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    match (&mut t.distinct_set, o.distinct_set) {
        (Some(ts), Some(os)) => {
            ts.reserve(os.len());
            ts.extend(os);
        }
        (None, Some(os)) => t.distinct_set = Some(os),
        _ => {}
    }
}

/// 候选: owned move + 小并大（union by size——小集插大集, 扩容按小集增长）。
fn merge_accum_move_small_into_big(t: &mut StatsAccum, o: StatsAccum) {
    t.count += o.count;
    t.sum_i128 += o.sum_i128;
    t.min = match (t.min, o.min) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    t.max = match (t.max, o.max) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    match (&mut t.distinct_set, o.distinct_set) {
        (Some(ts), Some(mut os)) => {
            if ts.len() < os.len() {
                std::mem::swap(ts, &mut os);
            }
            ts.extend(os);
        }
        (None, Some(os)) => t.distinct_set = Some(os),
        _ => {}
    }
}

/// 构造一个 `StatsAccum`：count + sum + min/max 固定值, distinct_set 填 `n` 个
/// 确定性 i64 键（LCG, 域 = [0, domain)）。
fn shard_accum(n: usize, domain: u64, seed: u64) -> StatsAccum {
    use crate::match_engine::{DistinctKey, StatsAccum};
    let mut acc = StatsAccum {
        count: 1,
        sum_i128: 7,
        min: Some(1),
        max: Some(9),
        distinct_set: Some(EngineHashSet::default()),
        last_row: None,
        top_entries: None,
    };
    let mut rng = seed;
    let mut next = |range: u64| {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (rng >> 33) % range
    };
    let set = acc.distinct_set.as_mut().unwrap();
    set.reserve(n);
    for _ in 0..n {
        set.insert(DistinctKey::Int((next(domain) + 1) as i64));
    }
    acc
}

/// 运行一轮：协调片已含自己 1/N 数据, 依次 merge 其余 partial。
/// 两个变体的**调用侧成本一致**：每轮从模板 clone 一份 partial（模拟从
/// channel 收到的 owned 包——生产里 `merge_partial(buckets, count)` 直接消费
/// owned, 无额外成本）; `borrowed` 仅决定 merge 函数内部用借用还是 move。
fn run_merge(
    borrowed: bool,
    merge_borrowed: fn(&mut StatsAccum, &StatsAccum),
    merge_owned: fn(&mut StatsAccum, StatsAccum),
    per_shard_distinct: usize,
    domain: u64,
) -> f64 {
    run_merge_shards(
        borrowed,
        merge_borrowed,
        merge_owned,
        9,
        per_shard_distinct,
        domain,
    )
}

/// 与 [`run_merge`] 同构, 但分片数可配置（分片数敏感度: 协调片 + N-1 partial）。
fn run_merge_shards(
    borrowed: bool,
    merge_borrowed: fn(&mut StatsAccum, &StatsAccum),
    merge_owned: fn(&mut StatsAccum, StatsAccum),
    shards: usize,
    per_shard_distinct: usize,
    domain: u64,
) -> f64 {
    const N_MEASURES: usize = 12; // 4 count + 8 distinct（q15 形状）

    // 协调片自己 1/N 数据（distinct 度量索引 4..12）。
    let coord: Vec<StatsAccum> = (0..N_MEASURES)
        .map(|m| {
            if m >= 4 {
                shard_accum(per_shard_distinct, domain, 0x1000 + m as u64)
            } else {
                StatsAccum::default()
            }
        })
        .collect();
    // N-1 个 partial（各 12 个 StatsAccum, distinct 度量带 own 数据）。
    let partials: Vec<Vec<StatsAccum>> = (1..shards)
        .map(|s| {
            (0..N_MEASURES)
                .map(|m| {
                    if m >= 4 {
                        shard_accum(
                            per_shard_distinct,
                            domain,
                            0x2000 + s as u64 * 16 + m as u64,
                        )
                    } else {
                        StatsAccum::default()
                    }
                })
                .collect()
        })
        .collect();

    // 预热一轮（含 alloc）。
    {
        let mut c = coord.clone();
        let mut p = partials.clone();
        if borrowed {
            for src in &p {
                for (t, o) in c.iter_mut().zip(src.iter()) {
                    merge_borrowed(t, o);
                }
            }
        } else {
            for src in p.drain(..) {
                for (t, o) in c.iter_mut().zip(src.into_iter()) {
                    merge_owned(t, o);
                }
            }
        }
    }

    let start = Instant::now();
    let mut c = coord.clone();
    let mut p = partials.clone();
    if borrowed {
        for src in &p {
            for (t, o) in c.iter_mut().zip(src.iter()) {
                merge_borrowed(t, o);
            }
        }
    } else {
        for src in p.drain(..) {
            for (t, o) in c.iter_mut().zip(src.into_iter()) {
                merge_owned(t, o);
            }
        }
    }
    std::hint::black_box(&c);
    start.elapsed().as_secs_f64() * 1e9
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine close_bench q15_merge_partial -- --ignored --nocapture"]
fn q15_merge_partial_profile() {
    eprintln!(
        "[close-bench] ===== q15 EOS 归并 profile（merge_partial 9 片 × 8 distinct 集）====="
    );

    // 规模: q15 30M 行分 9 片, 每片每 distinct 集 ~1M 元素（域 8M 高度重叠,
    // 近似生产 68M 次 insert）。小规模做敏感性检查。
    for (label, per_shard, domain) in [
        ("250k/集", 250_000usize, 2_000_000u64),
        ("1M/集(生产)", 1_000_000usize, 8_000_000u64),
    ] {
        eprintln!(
            "[close-bench] -- 规模 {label}: 每片每 distinct 集 {per_shard} 元素（域 {domain}）--"
        );
        let mut base_ns = 1.0;
        for (name, borrowed, fb, fo) in [
            (
                "cur(生产: clone+extend)",
                true,
                merge_accum_cur as fn(&mut StatsAccum, &StatsAccum),
                merge_accum_move as fn(&mut StatsAccum, StatsAccum),
            ),
            (
                "move(免 clone)",
                false,
                merge_accum_cur as fn(&mut StatsAccum, &StatsAccum),
                merge_accum_move as fn(&mut StatsAccum, StatsAccum),
            ),
            (
                "move+reserve",
                false,
                merge_accum_cur as fn(&mut StatsAccum, &StatsAccum),
                merge_accum_move_reserve as fn(&mut StatsAccum, StatsAccum),
            ),
            (
                "move+小并大",
                false,
                merge_accum_cur as fn(&mut StatsAccum, &StatsAccum),
                merge_accum_move_small_into_big as fn(&mut StatsAccum, StatsAccum),
            ),
        ] {
            let ns = run_merge(borrowed, fb, fo, per_shard, domain);
            if name.starts_with("cur") {
                base_ns = ns;
            }
            eprintln!(
                "[close-bench]   {:<22} {:>8.1} ms  ({:>6.1}% of cur)",
                name,
                ns / 1e6,
                ns / base_ns * 100.0
            );
        }
    }

    // ---- 分片数敏感度（配置层决策）: 域固定（8M）, 总行数固定（30M）, 分片
    // 越多 → 每片 distinct 越少但**重复 insert 总数越多**（域重叠被反复插入）;
    // 分片越少 → 单核瓶颈越大。量化归并成本 vs 分片数的关系, 供 rule_parallelism
    // 配置取舍（q15 归并是协调片单核尾部, 每多一片多一轮全量 extend）。
    eprintln!(
        "[close-bench] -- 分片数敏感度: 域 8M/度量, 每片 distinct 反比于分片数（总 distinct 域不变）--"
    );
    for shards in [4usize, 9, 16] {
        // 每片 distinct ≈ 域 / 片数（30M 行充分采样, 片间域重叠按均匀覆盖近似）;
        // 片数越多, 单片 merge 量越小但归并轮数越多——量化墙钟关系。
        let per_shard = 8_000_000usize / shards;
        let ns = run_merge_shards(
            true,
            merge_accum_cur as fn(&mut StatsAccum, &StatsAccum),
            merge_accum_move as fn(&mut StatsAccum, StatsAccum),
            shards,
            per_shard,
            8_000_000u64,
        );
        eprintln!(
            "[close-bench]   分片 {:>2}: 每片 {:<8} 元素/集 → merge 串行 {:>7.1} ms（每片 {:.0} ns/元素）",
            shards,
            per_shard,
            ns / 1e6,
            ns / (per_shard as f64 * 8.0 * (shards - 1) as f64)
        );
    }
}
