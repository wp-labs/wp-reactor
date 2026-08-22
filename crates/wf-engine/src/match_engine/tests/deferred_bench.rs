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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use wf_lang::ast::{
    Bound, BoundVal, Expr, FieldRef, JoinMode, ReduceClause, ReduceMeasure, TieSpec, WithinSpec,
};
use wf_lang::plan::{EachPlan, JoinCondPlan, JoinPlan, LetPlan, RulePlan, YieldField};

use crate::match_engine::JoinRow;
use crate::match_engine::RuleExecutor;
use crate::match_engine::executor::execute_joins;
use crate::match_engine::match_engine::{EngineHashMap, Event, Value, WindowLookup};

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
        let p = exec.deferred_pending_for(0, &event, NOW).expect("pending");
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
            .deferred_pending_for(0, &event_let, NOW)
            .expect("pending");
        std::hint::black_box(&p);
    }
    let pending_let_ns = start.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("pending+lets", pending_let_ns, pending_ns);

    // ---- 2. 到期评估（Q9：maxrow + tie + label 注入 + alert 构建）----
    let pending = exec.deferred_pending_for(0, &event, NOW).unwrap();
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
    let pending8 = exec8.deferred_pending_for(0, &event, NOW).unwrap();
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

// ---------------------------------------------------------------------------
// 常规（debug 可跑）宽松回归测试
// ---------------------------------------------------------------------------

#[test]
fn deferred_join_overhead_bounded() {
    // 宽松上限：到期评估 ≤ eager interval × 8 + 50ns 容差；挂起 ≤ 2µs/事件。
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
        let p = exec.deferred_pending_for(0, &event, NOW).expect("pending");
        std::hint::black_box(&p);
    }
    let pending_ns = start.elapsed().as_secs_f64() * 1e9 / n as f64;

    // 到期评估
    let pending = exec.deferred_pending_for(0, &event, NOW).unwrap();
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
        pending_ns < 2_000.0,
        "deferred pending must stay under 2us/event: {pending_ns:.1}ns"
    );
}
