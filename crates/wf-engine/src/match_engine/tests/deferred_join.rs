//! P3 deferred join（`emit at`）执行测试：挂起求值、到期 reduce（Q9 maxrow+tie）、
//! 纯存在（Q8）、空集不输出、label 注入（`winner.bidder` → Path）、origin=deferred。

use std::collections::HashSet;
use std::sync::Arc;

use wf_lang::ast::{
    Bound, BoundVal, Expr, FieldRef, JoinMode, PathSegment, ReduceMeasure, TieSpec, WithinSpec,
};
use wf_lang::plan::{EachPlan, JoinCondPlan, JoinPlan, RulePlan, YieldField};

use crate::alert::AlertOrigin;
use crate::match_engine::match_engine::{Event, Value, WindowLookup};
use crate::match_engine::{JoinRow, RuleExecutor};

use super::helpers::{event, num, str_val};

const T: i64 = 1_700_000_000_000_000_000;

/// `within [a.dateTime, a.expires]` 行内字段界（Q9）。
fn within_expires() -> WithinSpec {
    WithinSpec {
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
    }
}

/// Q9 形状：`join bid_events reduce maxrow(price) tie(dateTime asc)
/// within [a.dateTime, a.expires] on a.id == bid_events.auction as winner
/// emit at a.expires`（each 驱动形态）。
fn q9_rule_plan() -> RulePlan {
    let mut plan = super::helpers::simple_rule_plan(
        "q9_deferred",
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
        within: Some(within_expires()),
        reduce: Some(wf_lang::ast::ReduceClause {
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
                segments: vec![PathSegment::Field("bidder".into())],
            }),
        },
    ];
    plan
}

/// 右窗候选（asof_candidates 直接返回）。
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

fn bid(ts: i64, auction: f64, bidder: f64, price: f64) -> (i64, JoinRow) {
    let mut fields = crate::match_engine::EngineHashMap::default();
    fields.insert("auction".into(), num(auction));
    fields.insert("bidder".into(), num(bidder));
    fields.insert("price".into(), num(price));
    fields.insert("dateTime".into(), num(ts as f64));
    (ts, JoinRow::Event(Arc::new(Event { fields })))
}

/// 驱动 auction 事件：id=5, dateTime=T, expires=T+60s。
fn auction_event() -> Event {
    event(vec![
        ("id", num(5.0)),
        ("dateTime", num(T as f64)),
        ("expires", num((T + 60_000_000_000) as f64)),
    ])
}

#[test]
fn deferred_pending_for_evaluates_key_bounds_expiry() {
    let exec = RuleExecutor::new(q9_rule_plan());
    let pending = exec
        .deferred_pending_for(0, &auction_event(), T)
        .expect("pending");
    assert_eq!(pending.key_field, "auction");
    assert_eq!(pending.key, num(5.0));
    assert_eq!(pending.lo_ns, T);
    assert_eq!(pending.hi_ns, T + 60_000_000_000);
    assert_eq!(pending.expiry_nanos, T + 60_000_000_000);
    assert!(!pending.lo_open && !pending.hi_open);
}

#[test]
fn execute_deferred_join_q9_maxrow_tie_and_label() {
    let exec = RuleExecutor::new(q9_rule_plan());
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();

    // bids：100（ts=T+10s）、200（ts=T+20s）、200（ts=T+30s）——同价平手取 dateTime 最早
    let lookup = BidLookup(vec![
        bid(T + 10_000_000_000, 5.0, 1.0, 100.0),
        bid(T + 20_000_000_000, 5.0, 2.0, 200.0),
        bid(T + 30_000_000_000, 5.0, 3.0, 200.0),
        bid(T + 40_000_000_000, 9.0, 4.0, 999.0), // 其他 auction，不匹配
    ]);

    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("q9 must output the winning bid");

    assert_eq!(rec.origin, AlertOrigin::Deferred);
    // expiry = T+60s → fired_at 2023-11-14T22:14:20Z
    assert_eq!(&*rec.fired_at, "2023-11-14T22:14:20.000Z");
    assert!((rec.score - 30.0).abs() < f64::EPSILON);
    // 胜者 = price 200 且 dateTime 最早（bidder=2）
    let winner_bidder: &Value = rec
        .yield_fields
        .iter()
        .find(|(name, _)| &**name == "winner_bidder")
        .map(|(_, v)| v)
        .expect("winner_bidder yield field");
    assert_eq!(winner_bidder, &num(2.0));
}

#[test]
fn execute_deferred_join_empty_set_no_output() {
    let exec = RuleExecutor::new(q9_rule_plan());
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
    // 无匹配 bid（auction=5 无行，auction=9 有行）
    let lookup = BidLookup(vec![bid(T + 10_000_000_000, 9.0, 4.0, 999.0)]);
    let out = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap();
    assert!(
        out.is_none(),
        "no matching bid → no deferred output (Q9 无 bid 不输出)"
    );
}

#[test]
fn execute_deferred_join_pure_existence_q8_shape() {
    // Q8 形态：无 reduce，纯存在——区间内有匹配则输出（取最早行富化），miss 不输出。
    let mut plan = q9_rule_plan();
    plan.joins[0].reduce = None;
    plan.joins[0].emit_at = Some(Expr::Field(FieldRef::Qualified(
        "a".into(),
        "expires".into(),
    )));
    plan.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::Field(FieldRef::Simple("id".into())),
    }];
    let exec = RuleExecutor::new(plan);
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();

    // 命中
    let hit = BidLookup(vec![bid(T + 10_000_000_000, 5.0, 1.0, 100.0)]);
    let rec = exec
        .execute_deferred_join(0, &pending, &hit, T + 100_000_000_000)
        .unwrap()
        .expect("existence hit outputs");
    assert_eq!(rec.origin, AlertOrigin::Deferred);

    // miss
    let miss = BidLookup(vec![bid(T + 10_000_000_000, 9.0, 1.0, 100.0)]);
    assert!(
        exec.execute_deferred_join(0, &pending, &miss, T + 100_000_000_000)
            .unwrap()
            .is_none()
    );
}

#[test]
fn deferred_minrow_last_and_top_select() {
    // reduce 选择器覆盖：minrow / last / top(2, price)（无 label 时取首行）
    let bids = vec![
        bid(T + 10_000_000_000, 5.0, 1.0, 100.0),
        bid(T + 20_000_000_000, 5.0, 2.0, 200.0),
        bid(T + 30_000_000_000, 5.0, 3.0, 300.0),
    ];
    let lookup = BidLookup(bids.clone());

    // minrow(price) → 100（bidder=1）
    let mut plan = q9_rule_plan();
    plan.joins[0].reduce = Some(wf_lang::ast::ReduceClause {
        measure: ReduceMeasure::Minrow {
            field: FieldRef::Simple("price".into()),
            tie: None,
        },
        label: Some("winner".into()),
    });
    let exec = RuleExecutor::new(plan);
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("minrow outputs");
    let winner: &Value = rec
        .yield_fields
        .iter()
        .find(|(name, _)| &**name == "winner_bidder")
        .map(|(_, v)| v)
        .unwrap();
    assert_eq!(winner, &num(1.0));

    // last(price) → ts 最新（bidder=3）
    let mut plan = q9_rule_plan();
    plan.joins[0].reduce = Some(wf_lang::ast::ReduceClause {
        measure: ReduceMeasure::Last {
            field: FieldRef::Simple("price".into()),
        },
        label: Some("winner".into()),
    });
    let exec = RuleExecutor::new(plan);
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("last outputs");
    let winner: &Value = rec
        .yield_fields
        .iter()
        .find(|(name, _)| &**name == "winner_bidder")
        .map(|(_, v)| v)
        .unwrap();
    assert_eq!(winner, &num(3.0));

    // top(2, price) → 按 price 降序首行 = 300（bidder=3）
    let mut plan = q9_rule_plan();
    plan.joins[0].reduce = Some(wf_lang::ast::ReduceClause {
        measure: ReduceMeasure::Top {
            n: 2,
            field: FieldRef::Simple("price".into()),
        },
        label: Some("winner".into()),
    });
    let exec = RuleExecutor::new(plan);
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("top outputs");
    let winner: &Value = rec
        .yield_fields
        .iter()
        .find(|(name, _)| &**name == "winner_bidder")
        .map(|(_, v)| v)
        .unwrap();
    assert_eq!(winner, &num(3.0));
}

/// review 修复：minrow + tie asc——同主键时 tie 字段小者胜
///（等价 SQL `ORDER BY price ASC, dateTime ASC`）。
#[test]
fn deferred_minrow_with_tie_asc_picks_smallest_tie() {
    let bids = vec![
        bid(T + 30_000_000_000, 5.0, 3.0, 100.0), // 同价 100，dateTime 晚
        bid(T + 10_000_000_000, 5.0, 1.0, 100.0), // 同价 100，dateTime 早 → 胜
        bid(T + 20_000_000_000, 5.0, 2.0, 50.0),  // 更低价 → 主键胜
    ];
    let lookup = BidLookup(bids);
    let mut plan = q9_rule_plan();
    plan.joins[0].reduce = Some(wf_lang::ast::ReduceClause {
        measure: ReduceMeasure::Minrow {
            field: FieldRef::Simple("price".into()),
            tie: Some(TieSpec {
                field: FieldRef::Simple("dateTime".into()),
                desc: false,
            }),
        },
        label: Some("winner".into()),
    });
    let exec = RuleExecutor::new(plan);
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
    let rec = exec
        .execute_deferred_join(0, &pending, &lookup, T + 100_000_000_000)
        .unwrap()
        .expect("minrow+tie outputs");
    let winner: &Value = rec
        .yield_fields
        .iter()
        .find(|(name, _)| &**name == "winner_bidder")
        .map(|(_, v)| v)
        .unwrap();
    assert_eq!(winner, &num(2.0), "minrow: price 50 胜出");
}

/// review 补充：post-join `where` 抑制 deferred 输出。
#[test]
fn deferred_where_suppresses_output() {
    let mut plan = q9_rule_plan();
    plan.r#where = Some(Expr::BinOp {
        op: wf_lang::ast::BinOp::Gt,
        left: Box::new(Expr::Field(FieldRef::Simple("price".into()))),
        right: Box::new(Expr::Number(150.0)),
    });
    let exec = RuleExecutor::new(plan);
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();

    // price=100 < 150 → where false → 抑制
    let low = BidLookup(vec![bid(T + 10_000_000_000, 5.0, 1.0, 100.0)]);
    assert!(
        exec.execute_deferred_join(0, &pending, &low, T + 100_000_000_000)
            .unwrap()
            .is_none()
    );

    // price=200 > 150 → 输出
    let high = BidLookup(vec![bid(T + 10_000_000_000, 5.0, 1.0, 200.0)]);
    assert!(
        exec.execute_deferred_join(0, &pending, &high, T + 100_000_000_000)
            .unwrap()
            .is_some()
    );
}

/// review 补充：deferred 规则 + `let` 绑定——触发点/界/yield 可引用裸名绑定。
#[test]
fn deferred_respects_let_bindings() {
    let mut plan = q9_rule_plan();
    // `let buf = a.id + 1000`：expiry 用绑定字段（证明 lets 注入到挂起 ctx）
    plan.lets = vec![wf_lang::plan::LetPlan {
        name: "buf".into(),
        expr: Expr::BinOp {
            op: wf_lang::ast::BinOp::Add,
            left: Box::new(Expr::Field(FieldRef::Simple("id".into()))),
            right: Box::new(Expr::Number(1000.0)),
        },
    }];
    plan.joins[0].emit_at = Some(Expr::Field(FieldRef::Simple("buf".into())));
    let exec = RuleExecutor::new(plan);
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();
    // expiry = let buf = id(5) + 1000 = 1005（epoch 秒 → 归一化为 1005e9 纳秒）
    assert_eq!(pending.expiry_nanos, 1005_000_000_000);
}

/// review 补充：上开区间界（`[lo, <hi]`）排除边界行。
#[test]
fn deferred_open_upper_bound_excludes_boundary() {
    let mut plan = q9_rule_plan();
    let within = WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                "a".into(),
                "dateTime".into(),
            ))),
        },
        hi: Bound {
            open: true,
            val: BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                "a".into(),
                "expires".into(),
            ))),
        },
    };
    plan.joins[0].within = Some(within);
    plan.joins[0].reduce = None; // 纯存在
    plan.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::Field(FieldRef::Simple("id".into())),
    }];
    let exec = RuleExecutor::new(plan);
    let pending = exec.deferred_pending_for(0, &auction_event(), T).unwrap();

    // ts == expires（边界）→ 上开排除 → 无输出
    let boundary = BidLookup(vec![bid(T + 60_000_000_000, 5.0, 1.0, 100.0)]);
    assert!(
        exec.execute_deferred_join(0, &pending, &boundary, T + 100_000_000_000)
            .unwrap()
            .is_none()
    );
    // ts < expires → 命中
    let inside = BidLookup(vec![bid(T + 59_000_000_000, 5.0, 1.0, 100.0)]);
    assert!(
        exec.execute_deferred_join(0, &pending, &inside, T + 100_000_000_000)
            .unwrap()
            .is_some()
    );
}

/// review 补充：多条件 deferred join——首条件键查 + 全部条件复核。
#[test]
fn deferred_multi_condition_rechecks_all_conds() {
    let mut plan = q9_rule_plan();
    plan.joins[0].conds.push(JoinCondPlan {
        left: FieldRef::Qualified("a".into(), "seller".into()),
        right: FieldRef::Qualified("bid_events".into(), "channel".into()),
    });
    plan.joins[0].reduce = None; // 纯存在
    plan.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::Field(FieldRef::Simple("id".into())),
    }];
    // auction 事件带 seller 字段
    let mut auction = auction_event();
    auction.fields.insert("seller".into(), num(7.0));
    let exec = RuleExecutor::new(plan);
    let pending = exec.deferred_pending_for(0, &auction, T).unwrap();

    // channel 匹配 → 输出
    let hit = BidLookup(vec![bid_with_channel(
        T + 10_000_000_000,
        5.0,
        1.0,
        100.0,
        7.0,
    )]);
    assert!(
        exec.execute_deferred_join(0, &pending, &hit, T + 100_000_000_000)
            .unwrap()
            .is_some()
    );
    // channel 不匹配 → 复核拒绝 → 无输出
    let miss = BidLookup(vec![bid_with_channel(
        T + 10_000_000_000,
        5.0,
        1.0,
        100.0,
        9.0,
    )]);
    assert!(
        exec.execute_deferred_join(0, &pending, &miss, T + 100_000_000_000)
            .unwrap()
            .is_none()
    );
}

/// 带 `channel` 字段的 bid 行（多条件复核用）。
fn bid_with_channel(
    ts: i64,
    auction: f64,
    bidder: f64,
    price: f64,
    channel: f64,
) -> (i64, JoinRow) {
    let mut fields = crate::match_engine::EngineHashMap::default();
    fields.insert("auction".into(), num(auction));
    fields.insert("bidder".into(), num(bidder));
    fields.insert("price".into(), num(price));
    fields.insert("dateTime".into(), num(ts as f64));
    fields.insert("channel".into(), num(channel));
    (ts, JoinRow::Event(Arc::new(Event { fields })))
}
