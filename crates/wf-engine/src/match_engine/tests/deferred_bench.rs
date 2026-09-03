//! P3 deferred join（`emit at`）热路径微基准。
//!
//! 与 P2 eager interval 对比，量化 deferred 的增量成本：
//!   pending       : `deferred_pending_for`（驱动行挂起：clone + lets + 界/触发点求值）
//!   eval-maxrow   : `execute_deferred_join`（Q9：maxrow+tie+label 注入 + alert 构建）
//!   eval-exists   : `execute_deferred_join`（Q8：纯存在）
//!   eval-cand{8,64}: 候选行数对 reduce/过滤的影响
//!   eager baseline: P2 `execute_joins` interval（同环境直接对比）
//!
//! 运行：
//!   cargo test --release -p wf-engine deferred_bench -- --ignored --nocapture
//!
//! 与 interval_bench 一致，lookup 替身模拟「索引已按 key 过滤」的 O(1) 读路径——
//! 对比的是 join/挂起/到期评估逻辑本身。
use std::sync::Arc;

use std::collections::HashSet;
use std::time::{Duration, Instant};

use wf_lang::ast::{
    Bound, BoundVal, Expr, FieldRef, JoinMode, ReduceClause, ReduceMeasure, TieSpec, WithinSpec,
};
use wf_lang::plan::{EachPlan, JoinCondPlan, JoinPlan, LetPlan, RulePlan, YieldField};

use crate::match_engine::RuleExecutor;
use crate::match_engine::cep::{EngineHashMap, Event, Value, WindowLookup};
use crate::match_engine::executor::execute_joins;
use crate::match_engine::{DeferredLeft, FieldSource, JoinRow};

const N: usize = 1_000_000;
const NOW: i64 = 1_750_000_000_000_000_000;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// 右窗候选行：`(ts_nanos, auction, bidder, price, dateTime)`。
fn timed_bid(ts: i64, auction: f64, bidder: f64, price: f64) -> (i64, JoinRow) {
    let mut fields = EngineHashMap::default();
    fields.insert("auction".into(), Value::Number(auction));
    fields.insert("bidder".into(), Value::Number(bidder));
    fields.insert("price".into(), Value::Number(price));
    fields.insert("dateTime".into(), Value::Number(ts as f64));
    (ts, JoinRow::Event(Arc::new(Event { fields })))
}

/// 模拟「索引已按 key 过滤」的 lookup（等价真实 buffer hash index O(1) 路径）。
struct BidLookup(Vec<(i64, JoinRow)>);
impl WindowLookup for BidLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(self.0.iter().map(|(_, r)| r.clone()).collect())
    }
    fn asof_candidates(&self, _w: &str, _kf: &str, _k: &Value) -> Option<Vec<(i64, JoinRow)>> {
        Some(self.0.clone())
    }
}

/// Q9 形状：`within [a.dateTime, a.expires] ... reduce maxrow(price) tie(dateTime asc)
/// on a.id == bid_events.auction as winner emit at a.expires`（each 驱动）。
fn deferred_plan(reduce: bool) -> RulePlan {
    let mut plan = super::helpers::simple_rule_plan(
        "q9_bench",
        super::helpers::simple_plan(vec![], vec![]),
        Expr::Number(30.0),
        "digit",
        Expr::Field(FieldRef::Simple("id".into())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "a".into(),
        filter: None,
    });
    plan.joins = vec![JoinPlan {
        right_window: "bid_events".to_string(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("a".into(), "id".into()),
            right: FieldRef::Qualified("bid_events".into(), "auction".into()),
        }],
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                    "a".into(),
                    "dateTime".into(),
                ))),
            },
            hi: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                    "a".into(),
                    "expires".into(),
                ))),
            },
        }),
        reduce: reduce.then(|| ReduceClause {
            measure: ReduceMeasure::Maxrow {
                field: FieldRef::Simple("price".into()),
                tie: Some(TieSpec {
                    field: FieldRef::Simple("dateTime".into()),
                    desc: false,
                }),
            },
            label: Some("winner".into()),
        }),
        emit_at: Some(Expr::Field(FieldRef::Qualified(
            "a".into(),
            "expires".into(),
        ))),
    }];
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Simple("id".into())),
        },
        YieldField {
            name: "winner_bidder".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "winner".into(),
                segments: vec![wf_lang::ast::PathSegment::Field("bidder".into())],
            }),
        },
    ];
    plan
}

/// 驱动 auction 事件：id=5, dateTime=NOW, expires=NOW+60s（无 lets）。
fn auction_event() -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("id".into(), Value::Number(5.0));
    fields.insert("dateTime".into(), Value::Number(NOW as f64));
    fields.insert(
        "expires".into(),
        Value::Number((NOW + 60_000_000_000) as f64),
    );
    Event { fields }
}

/// 带 lets 的驱动事件（挂起含 apply_lets 成本）。
fn auction_event_with_let() -> (RulePlan, Event) {
    let mut plan = deferred_plan(true);
    plan.lets = vec![LetPlan {
        name: "buf".into(),
        expr: Expr::BinOp {
            op: wf_lang::ast::BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Simple("id".into()))),
            right: Box::new(Expr::Number(1000.0)),
        },
    }];
    let mut event = auction_event();
    event.fields.insert("extra".into(), Value::Number(7.0));
    (plan, event)
}

fn report(name: &str, per_ns: f64, baseline_ns: f64) {
    let mps = 1e9 / per_ns / 1e6;
    eprintln!(
        "[deferred-bench] {:<20} {:>8.1} ns/op   ({:>5.1}M ops/s)  = {:>5.1}% of baseline",
        name,
        per_ns,
        mps,
        per_ns / baseline_ns * 100.0
    );
}

// ---------------------------------------------------------------------------
// Release-only 热路径分解基准
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine deferred_bench -- --ignored --nocapture"]
fn deferred_join_hot_paths() {
    // 4 行候选：2 行在 [dateTime, expires] 内、2 行在外
    let rows = vec![
        timed_bid(NOW - 20_000_000_000, 5.0, 1.0, 10.0),
        timed_bid(NOW + 10_000_000_000, 5.0, 2.0, 20.0),
        timed_bid(NOW + 40_000_000_000, 5.0, 3.0, 30.0),
        timed_bid(NOW + 70_000_000_000, 5.0, 4.0, 40.0), // 超 expires
    ];
    let lookup = BidLookup(rows.clone());
    let event = auction_event();

    // ---- 1. 挂起（deferred_pending_for）----
    let exec = RuleExecutor::new(deferred_plan(true));
    let start = Instant::now();
    for _ in 0..N {
        let p = exec
            .deferred_pending_for(0, &DeferredLeft::Event(event.clone()), NOW)
            .expect("pending");
        std::hint::black_box(&p);
    }
    let pending_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("pending(hang)", pending_ns, pending_ns);

    // 挂起 + lets（apply_lets 成本）
    let (plan_let, event_let) = auction_event_with_let();
    let exec_let = RuleExecutor::new(plan_let);
    let start = Instant::now();
    for _ in 0..N {
        let p = exec_let
            .deferred_pending_for(0, &DeferredLeft::Event(event_let.clone()), NOW)
            .expect("pending");
        std::hint::black_box(&p);
    }
    let pending_let_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("pending+lets", pending_let_ns, pending_ns);

    // ---- 2. 到期评估（Q9：maxrow + tie + label 注入 + alert 构建）----
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(event.clone()), NOW)
        .unwrap();
    let start = Instant::now();
    for _ in 0..N {
        let rec = exec
            .execute_deferred_join(0, &pending, &lookup, NOW + 100_000_000_000)
            .unwrap()
            .expect("eval hits");
        std::hint::black_box(&rec);
    }
    let eval_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("eval-maxrow+tie", eval_ns, eval_ns);

    // ---- 3. 纯存在（Q8 形状，无 reduce/label）----
    let exec8 = RuleExecutor::new(deferred_plan(false));
    let pending8 = exec8
        .deferred_pending_for(0, &DeferredLeft::Event(event.clone()), NOW)
        .unwrap();
    let start = Instant::now();
    for _ in 0..N {
        let rec = exec8
            .execute_deferred_join(0, &pending8, &lookup, NOW + 100_000_000_000)
            .unwrap()
            .expect("eval hits");
        std::hint::black_box(&rec);
    }
    let eval8_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("eval-exists", eval8_ns, eval_ns);

    // ---- 4. 候选行数对 reduce/过滤的影响 ----
    for n_cand in [8usize, 64] {
        let span = 120_000_000_000i64; // [NOW-60s, NOW+60s]，1/6 在 [NOW, NOW+60s] 内
        let rows: Vec<(i64, JoinRow)> = (0..n_cand)
            .map(|i| {
                let ts = NOW - 60_000_000_000 + span * (i as i64) / (n_cand as i64 - 1);
                timed_bid(ts, 5.0, i as f64, (i as f64) * 10.0)
            })
            .collect();
        let lookup = BidLookup(rows);
        let start = Instant::now();
        for _ in 0..(N / 4) {
            let rec = exec
                .execute_deferred_join(0, &pending, &lookup, NOW + 100_000_000_000)
                .unwrap()
                .expect("eval hits");
            std::hint::black_box(&rec);
        }
        let per = start.elapsed().as_secs_f64() * 1e9 / (N / 4) as f64;
        report(&format!("eval-cand{}", n_cand), per, eval_ns);
    }

    // ---- 5. P2 eager interval 对比（同环境 execute_joins）----
    let eager_join = JoinPlan {
        right_window: "bid_events".to_string(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("aid".into()),
            right: FieldRef::Simple("auction".into()),
        }],
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::from_secs(0),
                    neg: false,
                },
            },
            hi: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::from_secs(60),
                    neg: false,
                },
            },
        }),
        reduce: None,
        emit_at: None,
    };
    let mut eager_ctx = Event {
        fields: EngineHashMap::default(),
    };
    eager_ctx.fields.insert("aid".into(), Value::Number(5.0));
    let start = Instant::now();
    for _ in 0..N {
        let mut c = eager_ctx.clone();
        execute_joins(std::slice::from_ref(&eager_join), &mut c, &lookup, NOW);
        std::hint::black_box(&c);
    }
    let eager_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("eager-interval", eager_ns, eager_ns);
}
/// Q4a 形状：auction_finals yield 4 字段（id/category/final/dateTime=a.expires），
/// deferred reduce maxrow(price) tie(dateTime asc) within [a.dateTime, a.expires]
/// on a.id == bid_events.auction。候选数 = auction 生命周期内的 bid 数（q4a
/// 30M 全流 ~16.5 bid/auction；生命周期内候选是评估成本主变量）。
fn q4a_deferred_plan() -> RulePlan {
    let mut plan = deferred_plan(true);
    plan.name = "q4a_auction_finals".into();
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Simple("id".into())),
        },
        YieldField {
            name: "category".into(),
            value: Expr::Field(FieldRef::Simple("category".into())),
        },
        YieldField {
            name: "final".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "winner".into(),
                segments: vec![wf_lang::ast::PathSegment::Field("price".into())],
            }),
        },
        YieldField {
            name: "dateTime".into(),
            value: Expr::Field(FieldRef::Qualified("a".into(), "expires".into())),
        },
    ];
    plan
}

/// q4a 驱动 auction 事件（含 category）。
fn q4a_auction_event() -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("id".into(), Value::Number(5.0));
    fields.insert("category".into(), Value::Number(3.0));
    fields.insert("dateTime".into(), Value::Number(NOW as f64));
    fields.insert(
        "expires".into(),
        Value::Number((NOW + 60_000_000_000) as f64),
    );
    Event { fields }
}

/// q4a 到期评估成本随候选数（auction 生命周期内 bid 数）扫描（2026-08-26
/// q4 归因：q4a 与 q9 deferred 部分同构，候选数分布是评估成本主变量——
/// eval-cand8 的 1330ns 是合成形状，q4a 实际候选数待定）。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine q4a_deferred_eval_candidate_scan -- --ignored --nocapture"]
fn q4a_deferred_eval_candidate_scan() {
    let exec = RuleExecutor::new(q4a_deferred_plan());
    let event = q4a_auction_event();
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(event.clone()), NOW)
        .expect("pending");
    let base_rows: Vec<(i64, JoinRow)> = (0..32usize)
        .map(|i| {
            timed_bid(
                NOW + 1_000_000_000 + (i as i64) * 1_000_000_000,
                5.0,
                i as f64,
                (i as f64) * 10.0,
            )
        })
        .collect();
    for &n_cand in &[1usize, 4, 8, 16, 32] {
        let rows = base_rows[..n_cand].to_vec();
        let lookup = BidLookup(rows);
        let start = Instant::now();
        for _ in 0..N {
            let rec = exec
                .execute_deferred_join(0, &pending, &lookup, NOW + 100_000_000_000)
                .unwrap()
                .expect("eval hits");
            std::hint::black_box(&rec);
        }
        let per = start.elapsed().as_secs_f64() * 1e9 / N as f64;
        report(&format!("eval-q4a-cand{n_cand}"), per, per);
    }

    // ---- 中间窗轻量化对比（2026-08-26 q4a）：evaluate + build_each_alert_pipe
    // （跳过 wfx_id/fired_at/summary 构建）vs 全量 execute_deferred_join ----
    let rows4 = base_rows[..4].to_vec();
    let lookup4 = BidLookup(rows4);
    let start = Instant::now();
    for _ in 0..N {
        let out_ctx = exec
            .evaluate_deferred_join(0, &pending, &lookup4)
            .unwrap()
            .expect("eval hits");
        let rec = exec
            .build_each_alert_pipe(&out_ctx, pending.expiry_nanos)
            .unwrap()
            .expect("light build");
        std::hint::black_box(&rec);
    }
    let light_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("eval-q4a-light-cand4", light_ns, light_ns);
    eprintln!(
        "[deferred-bench] 轻量/全量(cand4) = {:.1}x（中间窗跳过告警字段构建）",
        1702.9 / light_ns.max(1.0)
    );
}
/// q4a 评估成本分解（2026-08-26）：asof/filter/recheck/reduce/enrich 各段独立
/// 计时（in_interval/row_matches_conds/enrich_join_row/select_reduce_row 可独立
/// 调用；asof_candidates 用 BidLookup 的 Vec clone 近似——真实引擎是索引查询）。
/// 结论：定位评估 ~1.2µs 固定成本的大头段，指导下一轮优化。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine q4a_eval_cost_decomposition -- --ignored --nocapture"]
fn q4a_eval_cost_decomposition() {
    use crate::match_engine::executor::{
        enrich_join_row, enrich_join_row_bare, in_interval, row_matches_conds, select_reduce_row,
    };

    let plan = q4a_deferred_plan();
    let exec = RuleExecutor::new(plan.clone());
    let join = &plan.joins[0];
    let event = q4a_auction_event();
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(event.clone()), NOW)
        .unwrap();

    // 候选：key=5.0 的 4 条（与 eval-q4a-cand4 同构）。
    let rows: Vec<(i64, JoinRow)> = (0..4)
        .map(|i| {
            timed_bid(
                NOW + 1_000_000_000 + (i as i64) * 1_000_000_000,
                5.0,
                i as f64,
                (i as f64) * 10.0,
            )
        })
        .collect();

    // ① asof_candidates：窗口查询 + 候选物化（BidLookup = Vec clone 近似）。
    let lookup = BidLookup(rows.clone());
    let start = Instant::now();
    for _ in 0..N {
        let cand = lookup
            .asof_candidates("bid_events", &pending.key_field, &pending.key)
            .unwrap();
        std::hint::black_box(&cand);
    }
    let asof_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4a-①asof(cand4)", asof_ns, asof_ns);

    // ② filter：in_interval × 候选。
    let start = Instant::now();
    for _ in 0..N {
        let mut hit = 0usize;
        for (ts, _) in &rows {
            if in_interval(
                *ts,
                pending.lo_ns,
                pending.hi_ns,
                pending.lo_open,
                pending.hi_open,
            ) {
                hit += 1;
            }
        }
        std::hint::black_box(hit);
    }
    let filter_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4a-②filter×4", filter_ns, filter_ns);

    // ③ 条件复核 row_matches_conds × 候选（2026-08-26 skip 后已省——此段展示原成本）。
    let start = Instant::now();
    for _ in 0..N {
        let mut hit = 0usize;
        for (_, row) in &rows {
            if row_matches_conds(row, &join.conds, &pending.left) {
                hit += 1;
            }
        }
        std::hint::black_box(hit);
    }
    let recheck_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4a-③recheck×4", recheck_ns, recheck_ns);

    // ④ select_reduce_row：maxrow(price) tie(dateTime asc) 扫描 4 候选。
    let start = Instant::now();
    for _ in 0..N {
        let winner = select_reduce_row(rows.clone(), &join.reduce.as_ref().unwrap().measure);
        std::hint::black_box(&winner);
    }
    let reduce_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4a-④reduce×4", reduce_ns, reduce_ns);

    // ⑤a left 物化（evaluate 的 out_ctx = pending.left.to_event() 成本；
    // 2026-09-02 列式化后仅到期评估时物化一次）。
    let start = Instant::now();
    for _ in 0..N {
        let ctx = pending.left.to_event();
        std::hint::black_box(&ctx);
    }
    let clone_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4a-⑤a-left-to_event", clone_ns, clone_ns);

    // ⑤ enrich_join_row（全量：qualified + bare；eager 路径契约）。
    let winner =
        select_reduce_row(rows.clone(), &join.reduce.as_ref().unwrap().measure).expect("winner");
    let start = Instant::now();
    for _ in 0..N {
        let mut ctx = pending.left.to_event();
        enrich_join_row(&mut ctx, join, &winner);
        std::hint::black_box(&ctx);
    }
    let enrich_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4a-⑤enrich", enrich_ns, enrich_ns);

    // ⑤b enrich_join_row_bare（deferred 路径 2026-08-26：只裸名，省 qualified 死数据）。
    let start = Instant::now();
    for _ in 0..N {
        let mut ctx = pending.left.to_event();
        enrich_join_row_bare(&mut ctx, &winner);
        std::hint::black_box(&ctx);
    }
    let enrich_bare_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4a-⑤b-enrich-bare", enrich_bare_ns, enrich_ns);

    // ⑥ 轻量 build（build_each_alert_pipe，含 evaluate 的 out_ctx 输入）。
    let start = Instant::now();
    for _ in 0..N {
        let rec = exec
            .build_each_alert_pipe(&pending.left.to_event(), pending.expiry_nanos)
            .unwrap()
            .expect("light build");
        std::hint::black_box(&rec);
    }
    let build_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4a-⑥build(light)", build_ns, build_ns);

    eprintln!(
        "[deferred-bench] 分解合计 ≈ {:.0}ns（asof {:.0} + filter {:.0} + recheck {:.0} + reduce {:.0} + enrich {:.0} + build {:.0}）",
        asof_ns + filter_ns + recheck_ns + reduce_ns + enrich_ns + build_ns,
        asof_ns,
        filter_ns,
        recheck_ns,
        reduce_ns,
        enrich_ns,
        build_ns
    );
}

// ---------------------------------------------------------------------------
// 常规（debug 可跑）宽松回归测试
// ---------------------------------------------------------------------------

#[test]
fn deferred_join_overhead_bounded() {
    // 宽松上限：到期评估 ≤ eager interval × 8 + 50ns 容差（相对式，跨机器稳定）；
    // 挂起 ≤ 8µs/事件。挂起绝对值在 debug 下随机器抖动大（实测 1.3–2.2µs/事件，
    // 慢机/频率调制可更高），2µs 会在正常路径上误报——放宽到 8µs 仍远低于灾难量级
    // （意外整窗扫描 ≈ eval 路径 ~12µs、逐事件克隆大结构），且对 debug 方差留足余量；
    // release 真实成本见 deferred_join_hot_paths（~127ns/op）。
    // 防止挂起/评估引入灾难性开销（如意外的整窗扫描、每次克隆大结构）。
    let n = 20_000;
    let rows = vec![
        timed_bid(NOW - 20_000_000_000, 5.0, 1.0, 10.0),
        timed_bid(NOW + 10_000_000_000, 5.0, 2.0, 20.0),
        timed_bid(NOW + 40_000_000_000, 5.0, 3.0, 30.0),
        timed_bid(NOW + 70_000_000_000, 5.0, 4.0, 40.0),
    ];
    let lookup = BidLookup(rows);
    let event = auction_event();
    let exec = RuleExecutor::new(deferred_plan(true));

    // 挂起
    let start = Instant::now();
    for _ in 0..n {
        let p = exec
            .deferred_pending_for(0, &DeferredLeft::Event(event.clone()), NOW)
            .expect("pending");
        std::hint::black_box(&p);
    }
    let pending_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;

    // 到期评估
    let pending = exec
        .deferred_pending_for(0, &DeferredLeft::Event(event.clone()), NOW)
        .unwrap();
    let start = Instant::now();
    for _ in 0..n {
        let rec = exec
            .execute_deferred_join(0, &pending, &lookup, NOW + 100_000_000_000)
            .unwrap()
            .expect("eval hits");
        std::hint::black_box(&rec);
    }
    let eval_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;

    // eager interval 基线（同环境）
    let eager_join = JoinPlan {
        right_window: "bid_events".to_string(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("aid".into()),
            right: FieldRef::Simple("auction".into()),
        }],
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::from_secs(0),
                    neg: false,
                },
            },
            hi: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::from_secs(60),
                    neg: false,
                },
            },
        }),
        reduce: None,
        emit_at: None,
    };
    let mut eager_ctx = Event {
        fields: EngineHashMap::default(),
    };
    eager_ctx.fields.insert("aid".into(), Value::Number(5.0));
    let start = Instant::now();
    for _ in 0..n {
        let mut c = eager_ctx.clone();
        execute_joins(std::slice::from_ref(&eager_join), &mut c, &lookup, NOW);
        std::hint::black_box(&c);
    }
    let eager_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;

    eprintln!(
        "[deferred-bench] debug sanity: pending {:.1} ns/event, eval {:.1} ns, eager {:.1} ns",
        pending_ns, eval_ns, eager_ns
    );
    assert!(
        eval_ns <= eager_ns * 8.0 + 50.0,
        "deferred eval must stay within 8x of eager interval: eager={eager_ns:.1}ns eval={eval_ns:.1}ns"
    );
    assert!(
        pending_ns < 8_000.0,
        "deferred pending must stay under 8us/event: {pending_ns:.1}ns"
    );
}

// ---------------------------------------------------------------------------
// scan_deferred 全量扫描 O(n) vs 有序前缀 O(due)（2026-08-25，q4 100M 归因）
// ---------------------------------------------------------------------------
//
// 背景：q4a（deferred join）100M EPS 从 30M 的 7.6M 掉到 0.27M（28×），RSS
// 22GB。归因：pending 无序 Vec 全量累积（100M ≈ 33M 挂起），scan_deferred
// 每 batch 全量遍历取到期项——O(挂起数 × batch 数) = O(n²)。本基准量化两种
// 扫描策略在真实量级的成本：
//   scan-full  : 当前实现（全量扫 + 保序重建，每 batch 一次）
//   scan-prefix: 候选修复（按 expiry 有序，只取到期前缀 O(due)）
//
// 运行：cargo test --release -p wf-engine deferred_bench scan_deferred -- --ignored --nocapture

/// 模拟 scan_deferred 的当前实现：全量扫 + 到期移出 + 保序重建。
/// `with_event` = 模拟真实 DeferredPending（含 Event 字段）的拷贝成本。
fn scan_full(pending: &mut Vec<(i64, i64)>, wm: i64) -> usize {
    let mut due = 0usize;
    let mut keep = Vec::with_capacity(pending.len());
    for &(expiry, _lo) in pending.iter() {
        if expiry <= wm {
            due += 1;
        } else {
            keep.push((expiry, _lo));
        }
    }
    *pending = keep;
    due
}

// 候选修复：按 expiry 升序，二分取到期前缀（O(due) + O(log n)）。
// 维护有序：插入时二分定位（挂起路径一次 O(log n)，远低于扫描节约）。
fn scan_prefix(pending: &mut Vec<(i64, i64)>, wm: i64) -> usize {
    let split = pending.partition_point(|&(expiry, _)| expiry <= wm);
    let due = split;
    pending.drain(..split);
    due
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine deferred_bench scan_deferred -- --ignored --nocapture"]
fn deferred_scan_strategy_bench() {
    // 挂起量级：30M ≈ 10M、100M ≈ 33M。
    for &n_pending in &[1_000_000usize, 10_000_000] {
        // 到期比例：每 batch 到期一小部分（100M 数据 2740 batch，33M 挂起尾部
        // 到期——每 batch 到期 ~1/挂起总数比例；取 0.1% 模拟）。
        let expiry_step = 100i64;
        let pending_init: Vec<(i64, i64)> = (0..n_pending)
            .map(|i| (i as i64 * expiry_step, i as i64 * 7))
            .collect();
        let batch_ns = n_pending as i64 * expiry_step / 1000; // wm 推进到 ~0.1% 到期
        let rounds = 100usize;

        // 当前实现（全量扫）
        let mut p = pending_init.clone();
        let mut wm = 0i64;
        let t0 = Instant::now();
        let mut due_total = 0usize;
        for _ in 0..rounds {
            wm += batch_ns;
            due_total += scan_full(&mut p, wm);
        }
        std::hint::black_box(&due_total);
        let full_ns = t0.elapsed().as_secs_f64() * 1e9 / rounds as f64;

        // 真实 DeferredPending 含 Event（HashMap），拷贝成本高——用大结构模拟。
        // 每个 pending 附带一个 4 字段 HashMap（q4a 左行 auction: id/category/
        // dateTime/expires）。
        let big_pending_init: Vec<(i64, i64, std::collections::HashMap<String, i64>)> = (0
            ..n_pending)
            .map(|i| {
                let mut m = std::collections::HashMap::new();
                m.insert("id".into(), i as i64);
                m.insert("category".into(), i as i64);
                m.insert("dateTime".into(), i as i64);
                m.insert("expires".into(), i as i64 * expiry_step);
                (i as i64 * expiry_step, i as i64 * 7, m)
            })
            .collect();
        let mut bp = big_pending_init.clone();
        let mut wmb = 0i64;
        let t0b = Instant::now();
        let mut due_totalb = 0usize;
        for _ in 0..rounds {
            wmb += batch_ns;
            let mut keep = Vec::with_capacity(bp.len());
            for (expiry, lo, ev) in bp.drain(..) {
                if expiry <= wmb {
                    due_totalb += 1;
                } else {
                    keep.push((expiry, lo, ev));
                }
            }
            bp = keep;
        }
        std::hint::black_box(&due_totalb);
        let full_big_ns = t0b.elapsed().as_secs_f64() * 1e9 / rounds as f64;

        // 候选修复（前缀扫，且插入有序）
        // 插入有序：每轮在尾部追加新挂起（q4 驱动 auction 持续到达），二分插入。
        let mut p2: Vec<(i64, i64)> = Vec::new();
        let mut wm2 = 0i64;
        let mut idx = 0usize;
        let t1 = Instant::now();
        for _ in 0..rounds {
            // 模拟一个 batch 内新增的挂起（有序插入）
            let new_n = n_pending / rounds;
            for _ in 0..new_n {
                let e = (idx as i64) * expiry_step;
                let pos = p2.partition_point(|&(x, _)| x <= e);
                p2.insert(pos, (e, idx as i64 * 7));
                idx += 1;
            }
            wm2 += batch_ns;
            std::hint::black_box(scan_prefix(&mut p2, wm2));
        }
        let prefix_ns = t1.elapsed().as_secs_f64() * 1e9 / rounds as f64;

        eprintln!(
            "[deferred-bench] pending={:>2}M: scan-full {:>9.1} µs/batch; scan-full(带Event) {:>9.1} µs/batch; scan-prefix {:>9.1} µs/batch → {:.1}×（带Event vs prefix）",
            n_pending / 1_000_000,
            full_ns / 1e3,
            full_big_ns / 1e3,
            prefix_ns / 1e3,
            full_big_ns / prefix_ns
        );
    }
}

// ---------------------------------------------------------------------------
// join_index 写锁竞争：单锁 vs 分片（2026-08-25，q4 100M 断崖候选验证）
// ---------------------------------------------------------------------------
//
// 背景：q4a（deferred join）100M EPS 0.27M（30M 7.6M，28× 断崖），30s 采样
// 21969/21969 全部停在 `join_lookup_timestamped → lock_shared_slow`（读锁等
// 写锁）。写锁持有者 = bid 窗口 append 的 `index_batch`（每 batch ~36.5k 行
// 哈希插入）。本基准直接模拟该竞争：
//   1 写者线程：持续做 batch 哈希插入（36.5k 行/批，key 均匀落在既有键上）
//   R=4 读者线程：持续按随机 key 查找（asof 场景：读 rows + 汇总）
// 对比 单锁 `RwLock<HashMap>`（当前生产形态） vs 64 分片（候选修复），在
// 两种索引规模（30M ≈ 3M 键、100M ≈ 10M 键）下的读者吞吐与写者单批持锁时长。
//
// 运行：cargo test --release -p wf-engine deferred_bench index_contention -- --ignored --nocapture

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::thread;

use parking_lot::RwLock as PLRwLock;

const BATCH_ROWS: usize = 36_500;
const SHARDS: usize = 64;
const CONTEND_READERS: usize = 4;

type IndexRow = (i64, i64); // (ts_nanos, seq)

fn pick_key(state: &mut u64, n_keys: u64) -> i64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    ((*state >> 33) % n_keys) as i64
}

/// 索引替身公共接口：写者插 batch、读者按 key 查找。
trait ContendedIndex: Sync {
    fn insert_batch(&self, batch: &[(i64, i64, i64)]); // (key, ts, seq)
    fn lookup(&self, key: i64) -> usize; // 返回该键行数（模拟 asof 读）
}

/// 单锁形态（当前生产：`join_index` 一把 RwLock 包整表）。
struct SingleLockIndex {
    map: PLRwLock<HashMap<i64, Vec<IndexRow>>>,
}
impl ContendedIndex for SingleLockIndex {
    fn insert_batch(&self, batch: &[(i64, i64, i64)]) {
        let mut map = self.map.write();
        for &(key, ts, seq) in batch {
            map.entry(key).or_default().push((ts, seq));
        }
    }
    fn lookup(&self, key: i64) -> usize {
        let map = self.map.read();
        map.get(&key).map(|r| r.len()).unwrap_or(0)
    }
}

/// 分片形态（候选修复）：按 key 散列分 SHARDS 片，每片独立 RwLock。
struct ShardedIndex {
    shards: Vec<PLRwLock<HashMap<i64, Vec<IndexRow>>>>,
    mask: usize,
}
impl ShardedIndex {
    fn shard_of(key: i64, mask: usize) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) & mask
    }
}
impl ContendedIndex for ShardedIndex {
    fn insert_batch(&self, batch: &[(i64, i64, i64)]) {
        // 先按片分组（一遍散列），再逐片持写锁插入：每片临界区只有
        // 36.5k/64 ≈ 570 行，读者最多等自己那片。
        let mut buckets: Vec<Vec<(i64, i64, i64)>> = vec![Vec::new(); SHARDS];
        for &row in batch {
            buckets[Self::shard_of(row.0, self.mask)].push(row);
        }
        for (s, rows) in buckets.into_iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            let mut map = self.shards[s].write();
            for (key, ts, seq) in rows {
                map.entry(key).or_default().push((ts, seq));
            }
        }
    }
    fn lookup(&self, key: i64) -> usize {
        let map = self.shards[Self::shard_of(key, self.mask)].read();
        map.get(&key).map(|r| r.len()).unwrap_or(0)
    }
}

/// 预置 n_keys 个键、每键 3 行（模拟已累积的 bid 索引；真实量级下 map 已是
/// 该规模，插入/查找都落在既有键上）。
fn prefill<I: ContendedIndex>(index: &I, n_keys: u64) {
    let mut batch = Vec::with_capacity(BATCH_ROWS);
    for key in 0..n_keys {
        for i in 0..3i64 {
            batch.push((key as i64, key as i64 * 10 + i, 0));
        }
        if batch.len() >= BATCH_ROWS {
            index.insert_batch(&batch);
            batch.clear();
        }
    }
    if !batch.is_empty() {
        index.insert_batch(&batch);
    }
}

/// 跑一轮竞争：1 写者（主线程）持续 batch 插入 + CONTEND_READERS 个读者线程
/// 持续查找，持续 `duration`。返回 (读者 ops/s, 写者平均单批持锁 µs, 最大单批 µs)。
fn contention_case<I: ContendedIndex + Sync>(
    name: &str,
    index: &I,
    n_keys: u64,
    duration: Duration,
    run_writer: bool,
) -> (f64, f64, f64) {
    let stop = Arc::new(AtomicBool::new(false));
    let reader_ops = Arc::new(AtomicU64::new(0));
    thread::scope(|scope| {
        for r in 0..CONTEND_READERS {
            let stop = Arc::clone(&stop);
            let ops = Arc::clone(&reader_ops);
            scope.spawn(move || {
                let mut state = 0x9E37_79B9_7F4A_7C15u64 ^ ((r as u64) * 0x9E37_79B9_7F4A_7C15);
                while !stop.load(AtomicOrdering::Relaxed) {
                    let key = pick_key(&mut state, n_keys);
                    std::hint::black_box(index.lookup(key));
                    ops.fetch_add(1, AtomicOrdering::Relaxed);
                }
            });
        }
        let start = Instant::now();
        let mut batches = 0u64;
        let mut hold_total = Duration::ZERO;
        let mut hold_max = Duration::ZERO;
        if run_writer {
            let mut state = 0xC0FF_EE00_CAFE_F00Du64;
            let mut batch: Vec<(i64, i64, i64)> = Vec::with_capacity(BATCH_ROWS);
            loop {
                batch.clear();
                for i in 0..BATCH_ROWS {
                    let key = pick_key(&mut state, n_keys);
                    batch.push((key, i as i64, batches as i64));
                }
                let t0 = Instant::now();
                index.insert_batch(&batch);
                let d = t0.elapsed();
                hold_total += d;
                hold_max = hold_max.max(d);
                batches += 1;
                if start.elapsed() >= duration {
                    break;
                }
            }
        } else {
            thread::sleep(duration);
        }
        stop.store(true, AtomicOrdering::Relaxed);
        let elapsed = start.elapsed().as_secs_f64().max(1e-9);
        let ops = reader_ops.load(AtomicOrdering::Relaxed);
        let reader_mops = ops as f64 / elapsed / 1e6;
        let avg_hold = hold_total.as_secs_f64() * 1e6 / batches.max(1) as f64;
        let max_hold = hold_max.as_secs_f64() * 1e6;
        eprintln!(
            "[deferred-bench] {:<14} keys={:>2}M 读者 {:>6.2}M ops/s; 写者 {}批 平均持锁 {:>8.1} µs/批 最大 {:>8.1} µs/批",
            name,
            n_keys / 1_000_000,
            reader_mops,
            batches,
            avg_hold,
            max_hold
        );
        (reader_mops, avg_hold, max_hold)
    })
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine deferred_bench index_contention -- --ignored --nocapture"]
fn deferred_index_contention_bench() {
    let duration = Duration::from_millis(2_000);
    for &n_keys_m in &[3u64, 10u64] {
        let n_keys = n_keys_m * 1_000_000;

        // 单锁：无写者基线（读者天花板）
        let single = SingleLockIndex {
            map: PLRwLock::new(HashMap::new()),
        };
        prefill(&single, n_keys);
        let _ = contention_case("single-noW", &single, n_keys, duration, false);
        // 单锁：有写者（当前生产形态）
        let (single_mops, _, _) = contention_case(
            &format!("single+W-{}", n_keys_m),
            &single,
            n_keys,
            duration,
            true,
        );

        // 分片：有写者（候选修复）
        let sharded = ShardedIndex {
            shards: (0..SHARDS).map(|_| PLRwLock::new(HashMap::new())).collect(),
            mask: SHARDS - 1,
        };
        prefill(&sharded, n_keys);
        let (shard_mops, _, _) = contention_case(
            &format!("shard+W-{}", n_keys_m),
            &sharded,
            n_keys,
            duration,
            true,
        );
        eprintln!(
            "[deferred-bench] 分片/单锁 读者吞吐比 = {:.2}×（keys={}M）",
            shard_mops / single_mops.max(1e-9),
            n_keys_m
        );
    }
}

// ---------------------------------------------------------------------------
// 索引驱逐：全量扫描 vs 增量（batch_keys registry）（2026-08-25，q4 100M 主因）
// ---------------------------------------------------------------------------
//
// 背景：gen-nexmark 事件跨度 = count×100µs → 30M = 50min（< over=1h，无驱逐）、
// 100M = 2h46m（> over=1h，bid 窗 time 驱逐合法触发）。旧 `remove_batch` 每
// 驱逐一批就**全索引扫描**（全片 retain + max_ts 重算，O(全行数)）——100M 时
// 33M 行 × 数千批 → evictor 线程独占一核（采样 21870/21870 在
// `remove_batch_from_index`），EPS 0.27M。新实现按 `batch_keys[seq]` 只清该批
// 贡献的 key。本基准量化两种实现在真实量级的每批驱逐成本：
//   remove-full : 旧实现（全键扫描）
//   remove-incr : 新实现（只动本批 ~6k 受影响 key）
//
// 运行：cargo test --release -p wf-engine deferred_bench remove_batch -- --ignored --nocapture

/// 旧实现形状：全键遍历 retain + max_ts 重算（无 batch_keys）。
fn remove_batch_full(map: &mut HashMap<i64, Vec<(i64, u64)>>, seq: u64) -> usize {
    let mut removed = 0usize;
    for kr in map.values_mut() {
        let before = kr.len();
        kr.retain(|(_, s)| *s != seq);
        removed += before - kr.len();
    }
    removed
}

/// 新实现形状：只动 batch_keys[seq] 里的 key。
fn remove_batch_incr(map: &mut HashMap<i64, Vec<(i64, u64)>>, keys: &[i64], seq: u64) -> usize {
    let mut removed = 0usize;
    for &key in keys {
        if let Some(rows) = map.get_mut(&key) {
            let before = rows.len();
            rows.retain(|(_, s)| *s != seq);
            removed += before - rows.len();
            if rows.is_empty() {
                map.remove(&key);
            }
        }
    }
    removed
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine deferred_bench remove_batch -- --ignored --nocapture"]
fn deferred_remove_batch_strategy_bench() {
    // 100M 量级：~33M 索引行、~1.7M 去重键、每批 ~6k 受影响键（36.5k 行/批 ÷
    // ~6 行每 auction）。30M 量级：~10M 行、~570k 键、~6k 键/批。
    for &(n_keys, rows_per_key) in &[(570_000usize, 17usize), (1_700_000, 19)] {
        let total_rows = n_keys * rows_per_key;
        let keys_per_batch = 6_000usize;
        let seqs_per_key = rows_per_key; // 每 key 的行来自不同批
        let mut map: HashMap<i64, Vec<(i64, u64)>> = HashMap::new();
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        for k in 0..n_keys as i64 {
            let rows: Vec<(i64, u64)> = (0..rows_per_key)
                .map(|i| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    (
                        k * 100 + i as i64,
                        (k as u64 * seqs_per_key as u64 + i as u64),
                    )
                })
                .collect();
            map.insert(k, rows);
        }
        // 模拟一批的受影响 key 集（均匀采样 6k 键）。
        let batch_keys: Vec<i64> = (0..keys_per_batch)
            .map(|i| ((state.wrapping_mul(97).wrapping_add(i as u64)) % n_keys as u64) as i64)
            .collect();
        let seq = u64::MAX - 7; // 一个不影响既有行的 seq（纯 retain 成本）

        // 旧实现：全键扫描（每批）
        let mut map_full = map.clone();
        let t0 = Instant::now();
        let rounds = 50usize;
        for _ in 0..rounds {
            remove_batch_full(&mut map_full, seq);
        }
        let full_ns = t0.elapsed().as_secs_f64() * 1e9 / rounds as f64;

        // 新实现：增量（每批）
        let mut map_incr = map.clone();
        let t1 = Instant::now();
        for _ in 0..rounds {
            remove_batch_incr(&mut map_incr, &batch_keys, seq);
        }
        let incr_ns = t1.elapsed().as_secs_f64() * 1e9 / rounds as f64;

        // 一致性：两实现的「移除量」相同（本 seq 无既有行 → 0）。
        assert_eq!(remove_batch_full(&mut map_full.clone(), seq), 0);
        assert_eq!(
            remove_batch_incr(&mut map_incr.clone(), &batch_keys, seq),
            0
        );

        eprintln!(
            "[deferred-bench] 驱逐 keys={:>3}万 行={:>5}万: remove-full {:>10.1} µs/批; remove-incr {:>9.1} µs/批 → {:.0}×",
            n_keys / 10_000,
            total_rows / 10_000,
            full_ns / 1e3,
            incr_ns / 1e3,
            full_ns / incr_ns.max(1.0)
        );
    }
}

// ---------------------------------------------------------------------------
// 挂起插入路径：单调 expires vs 随机 expires（2026-08-25 q4 100M 断崖候选）
// ---------------------------------------------------------------------------
//
// q4a 的 `pending.insert`（有序维护）在 expires **非单调**（= dateTime + 随机
// 有效期）时每次插中间 → Vec shift O(n)。本基准量化：
//   mono: expires 单调（追加尾部 O(1)）
//   rand: expires 带 ±50% 抖动（insert 中间 O(n) shift）

fn insert_ordered(pending: &mut Vec<(i64, i64)>, e: i64, v: i64) {
    let pos = pending.partition_point(|&(x, _)| x <= e);
    pending.insert(pos, (e, v));
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine deferred_bench insert -- --ignored --nocapture"]
fn deferred_insert_path_bench() {
    for &n_pending in &[1_000_000usize, 10_000_000] {
        // 单调：expires = i×step（数据时间正序）——insert 尾部 O(1)
        let mut mono: Vec<(i64, i64)> = Vec::new();
        let t0m = Instant::now();
        for i in 0..n_pending {
            insert_ordered(&mut mono, i as i64 * 100, i as i64);
        }
        let mono_ns = t0m.elapsed().as_secs_f64() * 1e9 / n_pending as f64;

        // 随机：expires = i×100 ± 50（q4 expires = dateTime + 随机有效期 → 非单调）
        let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = |range: u64| {
            rng = rng
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (rng >> 33) % range
        };
        let mut rnd: Vec<(i64, i64)> = Vec::new();
        let t0r = Instant::now();
        for i in 0..n_pending {
            let e = i as i64 * 100 + (next(101) as i64 - 50) * 2;
            insert_ordered(&mut rnd, e, i as i64);
        }
        let rnd_ns = t0r.elapsed().as_secs_f64() * 1e9 / n_pending as f64;

        eprintln!(
            "[deferred-bench] 挂起插入 pending={:>2}M: 单调expires {:>7.1} ns/次; 随机expires {:>10.1} ns/次 → {:.0}×（O(n) shift 代价）",
            n_pending / 1_000_000,
            mono_ns,
            rnd_ns,
            rnd_ns / mono_ns.max(1.0)
        );
    }
}
