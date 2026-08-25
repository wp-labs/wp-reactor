//! Q1~Q22 运行热路径性能测试补充（nexmark_hotpath_bench）。
//!
//! 目标：为 NEXMark Q1~Q22 的运行热路径补齐微基准，量化各查询形态的
//! ns/事件 成本并归因，找出性能数据不合理（偏离预期形态 / 与同类查询量级
//! 不匹配）的位置。覆盖矩阵与 30M 实测基线见
//! `docs/archive/nexmark-hotpath-bench.md`（本文件为数据版，文档为分析版，
//! 已随历史调查记录归档）。
//!
//! 运行（release-only，与 close_bench/deferred_bench 同款）：
//!   cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture
//!
//! 覆盖形态（Q1/Q2/Q3/Q8/Q9/Q10/Q15 已有既有 bench，见 docs 覆盖矩阵）：
//!   q4_q6_join_then_key_advance : join-then-key（bid → auction join → 键分组）
//!                                 固定 10m avg（Q4）/ 滑动 10m avg（Q6）状态机推进
//!   q5_q7_window_conv_top       : fixed 10s 窗口 + conv sort(-m)|top(1) 归并（Q5/Q7）
//!   q11_session_advance         : session(10s) 窗口状态推进/过期（Q11，RSS 17.3GB 最高之一）
//!   q12_fixed_window_count      : fixed 10s count 窗口（Q12）
//!   q13_match_snapshot_join     : match<bidder:10m> + snapshot join person 富化（Q13）
//!   q14_each_strftime_count_char: bind filter + strftime/count_char 字符串函数（Q14）
//!   q16_q17_keyed_close         : 键分片 close 12 measure（Q16）/ 8 measure（Q17）
//!   q18_composite_key_close     : (bidder,auction) 复合键 close count（Q18，RSS 18.5GB）
//!   q19_stats_group_topn        : stats group by auction + top(10, price)（Q19）
//!   q20_each_snapshot_join_where: on each + snapshot join + where 过滤（Q20）
//!   q21_string_bind_filter      : bind filter channel_id != "" 字符串比较（Q21）
//!   q22_each_split              : let split(url) + mvindex + concat 字符串投影（Q22）
//!
//! 数据域对齐（NEXMark 官方）：bidder ≈ 最近 1000 人、auction ≈ 最近 100 个、
//! 价格对数均匀 ∈ [100, 1e8)；事件时间步长 = 30m 数据 / 27.6M bid ≈ 65.2µs/事件。
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{
    BinOp, CloseMode, CmpOp, Expr, FieldRef, FieldSelector, JoinMode, MatchMode, Measure, Transform,
};
use wf_lang::plan::{
    AggPlan, BranchPlan, ConvChainPlan, ConvOpPlan, ConvPlan, EachPlan, JoinCondPlan, JoinKeyPlan,
    JoinPlan, LetPlan, MatchPlan, RulePlan, SortKeyPlan, StatsAggPlan, StatsMeasurePlan,
    StatsOutputShapePlan, StatsPlan, StepPlan, WindowSpec, YieldField,
};

use crate::match_engine::executor::StatsExecutor;
use crate::match_engine::match_engine::{
    BindData, CepStateMachine, CloseOutput, CloseReason, EngineHashMap, Event, MatchedContext,
    StepData, Value, WindowLookup,
};
use crate::match_engine::{JoinRow, RuleExecutor, apply_conv};

use super::helpers::{event, num, simple_rule_plan, str_val};

// ---------------------------------------------------------------------------
// 常量与数据生成（确定性 LCG，失败可复现）
// ---------------------------------------------------------------------------

const N: usize = 500_000;
/// NEXMark 30m 数据 27.6M bid → 每事件事件时间步长 ≈ 65.2µs。
const EVENT_STEP_NS: i64 = 65_217;
const NOW: i64 = 1_750_000_000_000_000_000;

/// q15/q16 引用域：bidder ≈ 最近 1000 人 ± lead，auction ≈ 最近 100 个 ± lead。
const BIDDER_BASE: i64 = 1000;
const BIDDER_DOMAIN: u64 = 1010;
const AUCTION_BASE: i64 = 1000;
const AUCTION_DOMAIN: u64 = 110;

fn next_u64(rng: &mut u64) -> u64 {
    *rng = rng
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *rng >> 33
}

/// 官方 nextPrice = 10^(6u)×100 ∈ [100, 1e8)（对数均匀，单位分）。
fn next_price(rng: &mut u64) -> f64 {
    (10f64.powf((next_u64(rng) % 1_000_000) as f64 / 1_000_000.0 * 6.0) * 100.0).round()
}

fn bid_event(auction: f64, bidder: f64, price: f64, channel: &str, url: &str, ts: i64) -> Event {
    event(vec![
        ("auction", num(auction)),
        ("bidder", num(bidder)),
        ("price", num(price)),
        ("channel", str_val(channel)),
        ("url", str_val(url)),
        ("dateTime", num(ts as f64)),
        ("channel_id", str_val("1")),
        ("extra", str_val("x")),
    ])
}

/// 官方 url 形态：3 段目录 + query（split 后 ≥ 6 段，mvindex(3/4/5) 非越界）。
fn nexmark_url() -> &'static str {
    "https://www.nexmark.com/aaaaa/bbbbb/ccccc/item.htm?query=1"
}

fn bid_events(n: usize) -> Vec<Event> {
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..n)
        .map(|i| {
            let price = next_price(&mut rng);
            let bidder = (BIDDER_BASE + (next_u64(&mut rng) % BIDDER_DOMAIN) as i64) as f64;
            let auction = (AUCTION_BASE + (next_u64(&mut rng) % AUCTION_DOMAIN) as i64) as f64;
            bid_event(
                auction,
                bidder,
                price,
                "Google",
                nexmark_url(),
                NOW + i as i64 * EVENT_STEP_NS,
            )
        })
        .collect()
}

/// 行式 rows（stats process_rows 用；键域同 bid_events）。
fn bid_rows(n: usize) -> Vec<HashMap<String, Value>> {
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..n)
        .map(|_| {
            let mut m = HashMap::new();
            m.insert(
                "auction".to_string(),
                num((AUCTION_BASE + (next_u64(&mut rng) % AUCTION_DOMAIN) as i64) as f64),
            );
            m.insert(
                "bidder".to_string(),
                num((BIDDER_BASE + (next_u64(&mut rng) % BIDDER_DOMAIN) as i64) as f64),
            );
            m.insert("price".to_string(), num(next_price(&mut rng)));
            m
        })
        .collect()
}

/// 列式 batch（stats process_batch 用；schema = auction/bidder/price Int64）。
fn bid_batch(n: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
    ]);
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let auctions: Vec<i64> = (0..n)
        .map(|_| AUCTION_BASE + (next_u64(&mut rng) % AUCTION_DOMAIN) as i64)
        .collect();
    let bidders: Vec<i64> = (0..n)
        .map(|_| BIDDER_BASE + (next_u64(&mut rng) % BIDDER_DOMAIN) as i64)
        .collect();
    let prices: Vec<i64> = (0..n).map(|_| next_price(&mut rng) as i64).collect();
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(auctions)),
            Arc::new(Int64Array::from(bidders)),
            Arc::new(Int64Array::from(prices)),
        ],
    )
    .expect("batch")
}

// ---------------------------------------------------------------------------
// 报告
// ---------------------------------------------------------------------------

fn report(name: &str, per_ns: f64, baseline_ns: f64) {
    let mps = 1e9 / per_ns / 1e6;
    eprintln!(
        "[hotpath] {:<28} {:>9.1} ns/evt ({:>7.2}M evt/s) = {:>6.1}% of baseline",
        name,
        per_ns,
        mps,
        per_ns / baseline_ns * 100.0
    );
}

// ---------------------------------------------------------------------------
// 表达 helpers
// ---------------------------------------------------------------------------

fn b_field(name: &str) -> Expr {
    Expr::Field(FieldRef::Qualified("b".into(), name.into()))
}

fn price_lt(threshold: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::Lt,
        left: Box::new(b_field("price")),
        right: Box::new(Expr::Number(threshold)),
    }
}

fn price_ge(threshold: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::Ge,
        left: Box::new(b_field("price")),
        right: Box::new(Expr::Number(threshold)),
    }
}

fn price_range(lo: f64, hi: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::And,
        left: Box::new(price_ge(lo)),
        right: Box::new(price_lt(hi)),
    }
}

fn count_ge(n: f64) -> AggPlan {
    AggPlan {
        transforms: vec![],
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        threshold: Expr::Number(n),
    }
}

fn agg(measure: Measure, cmp: CmpOp, threshold: f64) -> AggPlan {
    AggPlan {
        transforms: vec![],
        measure,
        cmp,
        threshold: Expr::Number(threshold),
    }
}

// ---------------------------------------------------------------------------
// plan 构造（对齐真实 wfl 形状）
// ---------------------------------------------------------------------------

/// Q4/Q6 共享 join-then-key：`match<category:...>` 键来自 auction join 右窗。
fn q4_q6_plan(fixed: bool) -> MatchPlan {
    MatchPlan {
        keys: vec![FieldRef::Simple("category".into())],
        key_map: None,
        key_join: Some(JoinKeyPlan {
            join_idx: 0,
            right_window: "auction_events".into(),
            left_field: FieldRef::Qualified("b".into(), "auction".into()),
            right_key_field: "id".into(),
            right_field: "category".into(),
            key_name: "category".into(),
        }),
        window_spec: if fixed {
            WindowSpec::Fixed(Duration::from_secs(600))
        } else {
            WindowSpec::Sliding(Duration::from_secs(600))
        },
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "b".into(),
                field: if fixed {
                    None
                } else {
                    // Q6: `on event { b.price | avg >= 200; }`
                    Some(FieldSelector::Dot("price".into()))
                },
                guard: None,
                agg: if fixed {
                    count_ge(1.0)
                } else {
                    // Q6: `b.price | avg >= 200`
                    AggPlan {
                        transforms: vec![],
                        measure: Measure::Avg,
                        cmp: CmpOp::Ge,
                        threshold: Expr::Number(200.0),
                    }
                },
            }],
        }],
        close_steps: if fixed {
            // Q4: `and close { w: b.price | avg >= 10; }`
            vec![StepPlan {
                branches: vec![BranchPlan {
                    label: Some("w".into()),
                    source: "b".into(),
                    field: Some(FieldSelector::Dot("price".into())),
                    guard: None,
                    agg: agg(Measure::Avg, CmpOp::Ge, 10.0),
                }],
            }]
        } else {
            vec![]
        },
        close_mode: if fixed { CloseMode::And } else { CloseMode::Or },
        tracked_bind_aliases: HashSet::from(["b".to_string()]),
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert("b".to_string(), HashSet::from(["price".to_string()]));
            m
        },
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    }
}

/// Q4/Q6 的 RulePlan（joins 携带 auction_events snapshot）。
// 注：join-then-key 的 lookup 由 state machine 直接消费，bench 不需要 RulePlan。
/// Q5/Q7 的 MatchPlan：fixed 10s 窗口 + close count/max。
fn q5_q7_plan(max_measure: bool) -> MatchPlan {
    MatchPlan {
        keys: vec![FieldRef::Simple("auction".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "b".into(),
                field: None,
                guard: None,
                agg: count_ge(1.0),
            }],
        }],
        close_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some(if max_measure { "m".into() } else { "n".into() }),
                source: "b".into(),
                field: if max_measure {
                    Some(FieldSelector::Dot("price".into()))
                } else {
                    None
                },
                guard: None,
                agg: if max_measure {
                    agg(Measure::Max, CmpOp::Ge, 1.0)
                } else {
                    count_ge(1.0)
                },
            }],
        }],
        close_mode: CloseMode::And,
        tracked_bind_aliases: HashSet::from(["b".to_string()]),
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            if max_measure {
                m.insert("b".to_string(), HashSet::from(["price".to_string()]));
            }
            m
        },
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: max_measure,
        trigger_event_needed: false,
    }
}

/// Q11 session(10s) / Q12 fixed(10s) count 窗口（真实 wfl 均带 `and close`）。
fn q11_q12_plan(session: bool) -> MatchPlan {
    MatchPlan {
        keys: vec![FieldRef::Simple("bidder".into())],
        key_map: None,
        key_join: None,
        window_spec: if session {
            WindowSpec::Session(Duration::from_secs(10))
        } else {
            WindowSpec::Fixed(Duration::from_secs(10))
        },
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "b".into(),
                field: None,
                guard: None,
                agg: count_ge(1.0),
            }],
        }],
        close_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("n".into()),
                source: "b".into(),
                field: None,
                guard: None,
                agg: count_ge(1.0),
            }],
        }],
        close_mode: CloseMode::And,
        tracked_bind_aliases: HashSet::from(["b".to_string()]),
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert("b".to_string(), HashSet::from(["bidder".to_string()]));
            m
        },
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    }
}

/// Q13：`match<bidder:10m>` + snapshot join person。
fn q13_rule() -> RulePlan {
    let mut plan = simple_rule_plan(
        "q13_bench",
        MatchPlan {
            keys: vec![FieldRef::Simple("bidder".into())],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(600)),
            event_steps: vec![StepPlan {
                branches: vec![BranchPlan {
                    label: None,
                    source: "b".into(),
                    field: None,
                    guard: None,
                    agg: count_ge(1.0),
                }],
            }],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: HashSet::from(["b".to_string()]),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: HashSet::new(),
            seq: None,
            match_mode: MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        },
        Expr::Number(10.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.joins = vec![JoinPlan {
        right_window: "person_events".to_string(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "bidder".into()),
            right: FieldRef::Qualified("person_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
    }];
    plan
}

/// Q14：on each + bind filter（价格区间）+ strftime/count_char 字符串 detail。
fn q14_rule() -> RulePlan {
    let mut plan = simple_rule_plan(
        "q14_bench",
        super::helpers::simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: Some(Expr::BinOp {
            op: BinOp::And,
            left: Box::new(Expr::BinOp {
                op: BinOp::Gt,
                left: Box::new(Expr::BinOp {
                    op: BinOp::Mul,
                    left: Box::new(Expr::Number(0.908)),
                    right: Box::new(b_field("price")),
                }),
                right: Box::new(Expr::Number(1_000_000.0)),
            }),
            right: Box::new(Expr::BinOp {
                op: BinOp::Lt,
                left: Box::new(Expr::BinOp {
                    op: BinOp::Mul,
                    left: Box::new(Expr::Number(0.908)),
                    right: Box::new(b_field("price")),
                }),
                right: Box::new(Expr::Number(50_000_000.0)),
            }),
        }),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("{} c={}".into()),
                Expr::IfThenElse {
                    cond: Box::new(Expr::InList {
                        expr: Box::new(Expr::FuncCall {
                            qualifier: None,
                            name: "strftime".into(),
                            args: vec![b_field("dateTime"), Expr::StringLit("%H".into())],
                        }),
                        list: vec![
                            Expr::StringLit("00".into()),
                            Expr::StringLit("01".into()),
                            Expr::StringLit("02".into()),
                        ],
                        negated: false,
                    }),
                    then_expr: Box::new(Expr::StringLit("nightTime".into())),
                    else_expr: Box::new(Expr::StringLit("dayTime".into())),
                },
                Expr::FuncCall {
                    qualifier: None,
                    name: "count_char".into(),
                    args: vec![b_field("extra"), Expr::StringLit("c".into())],
                },
            ],
        },
    }];
    plan
}

/// Q16：`match<channel:30m:fixed>` + 12 close measure（4 count 档 + 8 distinct）。
fn q16_plan() -> MatchPlan {
    let mk = |label: &str, field: Option<&str>, guard: Option<Expr>, distinct: bool| BranchPlan {
        label: Some(label.to_string()),
        source: "b".to_string(),
        field: field.map(|f| FieldSelector::Dot(f.to_string())),
        guard,
        agg: if distinct {
            AggPlan {
                transforms: vec![Transform::Distinct],
                measure: Measure::Count,
                cmp: CmpOp::Ge,
                threshold: Expr::Number(1.0),
            }
        } else {
            count_ge(1.0)
        },
    };
    let count_branches = [
        mk("total", None, None, false),
        mk("r1", None, Some(price_lt(10_000.0)), false),
        mk("r2", None, Some(price_range(10_000.0, 1_000_000.0)), false),
        mk("r3", None, Some(price_ge(1_000_000.0)), false),
    ];
    let distinct_branches = [
        mk("total_bidder", Some("bidder"), None, true),
        mk("r1_bidder", Some("bidder"), Some(price_lt(10_000.0)), true),
        mk(
            "r2_bidder",
            Some("bidder"),
            Some(price_range(10_000.0, 1_000_000.0)),
            true,
        ),
        mk(
            "r3_bidder",
            Some("bidder"),
            Some(price_ge(1_000_000.0)),
            true,
        ),
        mk("total_auction", Some("auction"), None, true),
        mk(
            "r1_auction",
            Some("auction"),
            Some(price_lt(10_000.0)),
            true,
        ),
        mk(
            "r2_auction",
            Some("auction"),
            Some(price_range(10_000.0, 1_000_000.0)),
            true,
        ),
        mk(
            "r3_auction",
            Some("auction"),
            Some(price_ge(1_000_000.0)),
            true,
        ),
    ];
    MatchPlan {
        keys: vec![FieldRef::Simple("channel".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(1800)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "b".into(),
                field: None,
                guard: None,
                agg: count_ge(1.0),
            }],
        }],
        close_steps: count_branches
            .into_iter()
            .chain(distinct_branches)
            .map(|b| StepPlan { branches: vec![b] })
            .collect(),
        close_mode: CloseMode::And,
        tracked_bind_aliases: HashSet::from(["b".to_string()]),
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "b".to_string(),
                HashSet::from([
                    "price".to_string(),
                    "bidder".to_string(),
                    "auction".to_string(),
                    "channel".to_string(),
                ]),
            );
            m
        },
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    }
}

/// Q17：`match<auction:30m:fixed>` + 8 close measure（4 count 档 + min/max/avg/sum）。
fn q17_plan() -> MatchPlan {
    let mk = |label: &str, field: Option<&str>, guard: Option<Expr>| BranchPlan {
        label: Some(label.to_string()),
        source: "b".to_string(),
        field: field.map(|f| FieldSelector::Dot(f.to_string())),
        guard,
        agg: count_ge(1.0),
    };
    let agg_branch = |label: &str, measure: Measure| BranchPlan {
        label: Some(label.to_string()),
        source: "b".to_string(),
        field: Some(FieldSelector::Dot("price".to_string())),
        guard: None,
        agg: agg(measure, CmpOp::Ge, 1.0),
    };
    let count_branches = [
        mk("total", None, None),
        mk("r1", None, Some(price_lt(10_000.0))),
        mk("r2", None, Some(price_range(10_000.0, 1_000_000.0))),
        mk("r3", None, Some(price_ge(1_000_000.0))),
    ];
    let stat_branches = [
        agg_branch("minp", Measure::Min),
        agg_branch("maxp", Measure::Max),
        agg_branch("avgp", Measure::Avg),
        agg_branch("sump", Measure::Sum),
    ];
    MatchPlan {
        keys: vec![FieldRef::Simple("auction".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(1800)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "b".into(),
                field: None,
                guard: None,
                agg: count_ge(1.0),
            }],
        }],
        close_steps: count_branches
            .into_iter()
            .chain(stat_branches)
            .map(|b| StepPlan { branches: vec![b] })
            .collect(),
        close_mode: CloseMode::And,
        tracked_bind_aliases: HashSet::from(["b".to_string()]),
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "b".to_string(),
                HashSet::from(["price".to_string(), "auction".to_string()]),
            );
            m
        },
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    }
}

/// Q18：`match<bidder,auction:30m:fixed>` 复合键 + close count。
fn q18_plan() -> MatchPlan {
    MatchPlan {
        keys: vec![
            FieldRef::Simple("bidder".into()),
            FieldRef::Simple("auction".into()),
        ],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(1800)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "b".into(),
                field: None,
                guard: None,
                agg: count_ge(1.0),
            }],
        }],
        close_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("n".into()),
                source: "b".into(),
                field: None,
                guard: None,
                agg: count_ge(1.0),
            }],
        }],
        close_mode: CloseMode::And,
        tracked_bind_aliases: HashSet::from(["b".to_string()]),
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "b".to_string(),
                HashSet::from(["bidder".to_string(), "auction".to_string()]),
            );
            m
        },
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    }
}

/// Q19：stats `stats<30m:fixed> group by (b.auction) { b | top(10, b.price) }`。
fn q19_stats_plan() -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(1800)),
        keys: vec![Expr::Field(FieldRef::Qualified(
            "b".into(),
            "auction".into(),
        ))],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![StatsMeasurePlan {
            label: "top_price".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Top,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: Some(10),
        }],
        tracked_bind_fields: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "b".to_string(),
                HashSet::from(["auction".to_string(), "price".to_string()]),
            );
            m
        },
    }
}

/// Q20：on each + snapshot join auction + where category == 10。
fn q20_rule() -> RulePlan {
    let mut plan = simple_rule_plan(
        "q20_bench",
        super::helpers::simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    plan.joins = vec![JoinPlan {
        right_window: "auction_events".to_string(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "auction".into()),
            right: FieldRef::Qualified("auction_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Qualified(
            "auction_events".into(),
            "category".into(),
        ))),
        right: Box::new(Expr::Number(10.0)),
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::StringLit("bid + category-10 auction".into()),
        },
    ];
    plan
}

/// Q21：on each + bind filter channel_id != ""。
fn q21_rule() -> RulePlan {
    let mut plan = simple_rule_plan(
        "q21_bench",
        super::helpers::simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: Some(Expr::BinOp {
            op: BinOp::Ne,
            left: Box::new(b_field("channel_id")),
            right: Box::new(Expr::StringLit("".into())),
        }),
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "detail".into(),
            value: b_field("channel_id"),
        },
    ];
    plan
}

/// Q22：on each + `let parts = split(b.url, "/")` + concat(mvindex(parts,3..5))。
fn q22_rule() -> RulePlan {
    let mut plan = simple_rule_plan(
        "q22_bench",
        super::helpers::simple_plan(vec![], vec![]),
        Expr::Number(10.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.lets = vec![LetPlan {
        name: "parts".into(),
        expr: Expr::FuncCall {
            qualifier: None,
            name: "split".into(),
            args: vec![b_field("url"), Expr::StringLit("/".into())],
        },
    }];
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    let mvindex = |idx: f64| Expr::FuncCall {
        qualifier: None,
        name: "mvindex".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("parts".into())),
            Expr::Number(idx),
        ],
    };
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "concat".into(),
                args: vec![
                    mvindex(3.0),
                    Expr::StringLit("/".into()),
                    mvindex(4.0),
                    Expr::StringLit("/".into()),
                    mvindex(5.0),
                ],
            },
        },
    ];
    plan
}

// ---------------------------------------------------------------------------
// Lookup 替身：索引 O(1) 路径（模拟窗口 hash index）
// ---------------------------------------------------------------------------

/// auction_events 窗口替身：id → (category, seller)。join_lookup 按 id O(1)。
struct AuctionLookup {
    map: HashMap<i64, (f64, f64)>,
}

impl AuctionLookup {
    fn new(domain: u64) -> Self {
        let mut map = HashMap::with_capacity(domain as usize);
        for id in 0..domain as i64 {
            map.insert(
                AUCTION_BASE + id,
                (10.0 + (id % 5) as f64, 1000.0 + (id % 100) as f64),
            );
        }
        Self { map }
    }

    fn row_for(&self, id: i64) -> JoinRow {
        let (cat, seller) = self.map.get(&id).copied().unwrap_or((10.0, 0.0));
        let mut fields = EngineHashMap::default();
        fields.insert("id".into(), Value::Number(id as f64));
        fields.insert("category".into(), Value::Number(cat));
        fields.insert("seller".into(), Value::Number(seller));
        JoinRow::Event(Arc::new(Event { fields }))
    }
}

impl WindowLookup for AuctionLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(self.map.keys().map(|id| self.row_for(*id)).collect())
    }
    fn join_lookup(&self, _w: &str, key_field: &str, key: &Value) -> Option<Vec<JoinRow>> {
        let k = match key {
            Value::Number(n) => *n as i64,
            _ => return Some(vec![]),
        };
        let mut fields = EngineHashMap::default();
        if key_field == "id" {
            let (cat, seller) = match self.map.get(&k) {
                Some((c, s)) => (*c, *s),
                None => return Some(vec![]),
            };
            fields.insert("id".into(), Value::Number(k as f64));
            fields.insert("category".into(), Value::Number(cat));
            fields.insert("seller".into(), Value::Number(seller));
        } else {
            return Some(vec![]);
        }
        Some(vec![JoinRow::Event(Arc::new(Event { fields }))])
    }
}

/// person_events 窗口替身：id → person（含 state/city）。
/// 行预缓存（模拟真实窗口索引——join_lookup 返回已存在行，Arc clone 便宜；
/// 不每事件新建 JoinRow）。
struct PersonLookup {
    rows: HashMap<i64, JoinRow>,
}

impl PersonLookup {
    fn new(domain: u64) -> Self {
        let states = ["OR", "ID", "CA", "AZ", "WY", "WA"];
        // 索引构建模拟（与真实窗口 join 索引同成本量级）；建完只留行缓存。
        let map: HashMap<i64, (String, String)> = {
            let mut m = HashMap::with_capacity(domain as usize);
            for id in 0..domain as i64 {
                m.insert(
                    BIDDER_BASE + id,
                    (
                        states[(id % states.len() as i64) as usize].to_string(),
                        "city".to_string(),
                    ),
                );
            }
            m
        };
        let rows = map
            .iter()
            .map(|(id, (state, city))| {
                let mut fields = EngineHashMap::default();
                fields.insert("id".into(), Value::Number(*id as f64));
                fields.insert("state".into(), Value::Str(state.clone().into()));
                fields.insert("city".into(), Value::Str(city.clone().into()));
                (*id, JoinRow::Event(Arc::new(Event { fields })))
            })
            .collect();
        Self { rows }
    }
}

impl WindowLookup for PersonLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(self.rows.values().cloned().collect())
    }
    fn join_lookup(&self, _w: &str, key_field: &str, key: &Value) -> Option<Vec<JoinRow>> {
        let k = match key {
            Value::Number(n) => *n as i64,
            _ => return Some(vec![]),
        };
        if key_field != "id" {
            return Some(vec![]);
        }
        match self.rows.get(&k) {
            Some(row) => Some(vec![row.clone()]),
            None => Some(vec![]),
        }
    }
}

// ---------------------------------------------------------------------------
// CloseOutput 构造（conv bench 用）
// ---------------------------------------------------------------------------

fn close_output(rule: &str, scope_key: Vec<Value>, label: &str, measure: f64) -> CloseOutput {
    CloseOutput {
        rule_name: rule.to_string(),
        scope_key,
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
        close_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some(label.to_string()),
            measure_value: measure,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        watermark_nanos: 0,
        machine_id: "".into(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        last_event_nanos: 0,
    }
}

/// Q5/Q7 的 conv plan：`sort(-n|-m) | top(1)`。
fn conv_top1(sort_label: &str) -> ConvPlan {
    ConvPlan {
        chains: vec![ConvChainPlan {
            ops: vec![
                ConvOpPlan::Sort(vec![SortKeyPlan {
                    expr: Expr::Field(FieldRef::Simple(sort_label.into())),
                    descending: true,
                }]),
                ConvOpPlan::Top(1),
            ],
        }],
    }
}

/// MatchedContext 构造（Q13 match + join 富化 bench 用）。
fn simple_matched(rule: &str, scope_key: Vec<Value>, event: &Event, ts: i64) -> MatchedContext {
    MatchedContext {
        rule_name: rule.to_string(),
        scope_key,
        step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: None,
            measure_value: 1.0,
            event_first_time_nanos: Some(ts),
            event_last_time_nanos: Some(ts),
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![BindData {
            alias: "b".into(),
            count: 1,
            field_values: EngineHashMap::default(),
        }],
        event_time_nanos: ts,
        event_first_time_nanos: ts,
        event_last_time_nanos: ts,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: "".into(),
        trigger_event: Some(Arc::new(event.clone())),
    }
}

// ---------------------------------------------------------------------------
// Bench 1：Q4/Q6 join-then-key（固定 10m avg close / 滑动 10m avg on-event）
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q4_q6_join_then_key_advance() {
    let events = bid_events(N);
    let lookup = AuctionLookup::new(AUCTION_DOMAIN);

    // Q4：fixed 10m + close avg —— 每事件 advance 全量累积 close steps
    let mut sm = CepStateMachine::new("q4_bench".to_string(), q4_q6_plan(true), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at_with("b", ev, ts, Some(&lookup)));
    }
    let q4_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q4 join-then-key+close", q4_ns, q4_ns);

    // Q6：sliding 10m + on-event avg —— 每事件状态机推进 + rolling avg
    let mut sm6 = CepStateMachine::new("q6_bench".to_string(), q4_q6_plan(false), None);
    let t1 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm6.advance_at_with("b", ev, ts, Some(&lookup)));
    }
    let q6_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q6 join-then-key+sliding", q6_ns, q6_ns);
}

/// 批级 join-then-key（2026-08-23）：同一批列式行，路径 A 逐事件内部解析
/// （`advance_at_with_masks`）vs 路径 B 批级预解析（`precompute_join_then_keys` +
/// `advance_at_with_masks_key`）。前 K 行收集 StepResult 逐位对拍（防语义发散），
/// 全量计时报告加速比。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q4_q6_join_then_key_batch_precompute() {
    use crate::match_engine::event_bridge::ColumnarEvent;
    use crate::match_engine::precompute_join_then_keys;

    const K: usize = 10_000; // 对拍抽样行
    let batch = bid_batch(N);
    let lookup = AuctionLookup::new(AUCTION_DOMAIN);
    let row_domain: Vec<usize> = (0..N).collect();

    for (label, fixed) in [("q4 fixed10m", true), ("q6 sliding10m", false)] {
        let plan = q4_q6_plan(fixed);
        let kjp = plan.key_join.as_ref().unwrap();
        let keys = precompute_join_then_keys(&batch, &row_domain, kjp, &lookup);
        assert_eq!(keys.len(), N, "{label}: 每行一个预解析 key");

        // 正确性对拍：前 K 行 StepResult 序列逐位一致（同 rule_name）。
        let mut sm_a = CepStateMachine::new("q".into(), plan.clone(), None);
        let mut sm_b = CepStateMachine::new("q".into(), plan.clone(), None);
        for (i, key) in keys.iter().enumerate().take(K) {
            let ev = ColumnarEvent::new(&batch, i);
            let ts = NOW + i as i64 * EVENT_STEP_NS;
            let ra = sm_a.advance_at_with_masks("b", &ev, ts, Some(&lookup), i, None);
            let rb =
                sm_b.advance_at_with_masks_key("b", &ev, ts, Some(&lookup), i, None, Some(key));
            assert_eq!(
                ra, rb,
                "{label} row {i}: 批级预解析 vs 内部解析结果必须一致"
            );
        }
        assert_eq!(
            sm_a.instance_count(),
            sm_b.instance_count(),
            "{label}: 实例数一致"
        );

        // 计时：路径 A（内部解析，基线）。
        let mut sm = CepStateMachine::new("q".into(), plan.clone(), None);
        let t0 = Instant::now();
        for i in 0..N {
            let ev = ColumnarEvent::new(&batch, i);
            let ts = NOW + i as i64 * EVENT_STEP_NS;
            let _ = std::hint::black_box(sm.advance_at_with_masks(
                "b",
                &ev,
                ts,
                Some(&lookup),
                i,
                None,
            ));
        }
        let row_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;

        // 计时：路径 B（批级预解析）。
        let mut sm2 = CepStateMachine::new("q".into(), plan, None);
        let t1 = Instant::now();
        for (i, key) in keys.iter().enumerate().take(N) {
            let ev = ColumnarEvent::new(&batch, i);
            let ts = NOW + i as i64 * EVENT_STEP_NS;
            let _ = std::hint::black_box(sm2.advance_at_with_masks_key(
                "b",
                &ev,
                ts,
                Some(&lookup),
                i,
                None,
                Some(key),
            ));
        }
        let batch_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;

        report(&format!("{label} 批级预解析"), batch_ns, row_ns);
        report(&format!("{label} 行式(内部解析)"), row_ns, row_ns);
    }
}

// ---------------------------------------------------------------------------
// Bench 2：Q5/Q7 fixed 10s 窗口 advance + conv sort/top(1) 归并
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q5_q7_window_conv_top() {
    let events = bid_events(N);

    // Q5：fixed 10s count + close count
    let mut sm = CepStateMachine::new("q5_bench".to_string(), q5_q7_plan(false), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let q5_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q5 fixed10s count", q5_ns, q5_ns);

    // Q7：fixed 10s max + close max
    let mut sm7 = CepStateMachine::new("q7_bench".to_string(), q5_q7_plan(true), None);
    let t1 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm7.advance_at("b", ev, ts));
    }
    let q7_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q7 fixed10s max", q7_ns, q7_ns);

    // conv 归并：一批收口 CloseOutput（~1000 行，auction 键域）→ sort(-n)|top(1)
    let plan = conv_top1("n");
    let keys = vec![FieldRef::Simple("auction".into())];
    let mut outputs: Vec<CloseOutput> = Vec::with_capacity(2000);
    let mut rng: u64 = 0x1234_5678_9ABC_DEF0;
    for _ in 0..2000 {
        let auction = (AUCTION_BASE + (next_u64(&mut rng) % AUCTION_DOMAIN) as i64) as f64;
        let count = (next_u64(&mut rng) % 500) as f64;
        outputs.push(close_output(
            "q5_bench",
            vec![Value::Number(auction)],
            "n",
            count,
        ));
    }
    let t2 = Instant::now();
    for _ in 0..100 {
        let out = std::hint::black_box(apply_conv(&plan, &keys, outputs.clone()));
        std::hint::black_box(out.len());
    }
    let conv_ns = t2.elapsed().as_secs_f64() * 1e9 / (100.0 * outputs.len() as f64);
    report("q5/q7 conv sort+top1", conv_ns, conv_ns);
}

// ---------------------------------------------------------------------------
// Bench 3：Q11 session(10s) 状态推进（RSS 17.3GB 查询之一）
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q11_session_advance() {
    let events = bid_events(N);
    let mut sm = CepStateMachine::new("q11_bench".to_string(), q11_q12_plan(true), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let q11_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q11 session(10s)", q11_ns, q11_ns);
}

// ---------------------------------------------------------------------------
// Bench 4：Q12 fixed(10s) count 窗口
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q12_fixed_window_count() {
    let events = bid_events(N);
    let mut sm = CepStateMachine::new("q12_bench".to_string(), q11_q12_plan(false), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let q12_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q12 fixed10s count", q12_ns, q12_ns);
}

// ---------------------------------------------------------------------------
// Bench 5：Q13 match<bidder:10m> + snapshot join 富化
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q13_match_snapshot_join() {
    let events = bid_events(N);
    let lookup = PersonLookup::new(BIDDER_DOMAIN);
    let exec = RuleExecutor::new(q13_rule());

    // advance：状态机推进（每事件命中 → 构造 MatchedContext 的成本在 exec）
    let mut sm = CepStateMachine::new("q13_bench".to_string(), q13_rule().match_plan.clone(), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let adv_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q13 advance", adv_ns, adv_ns);

    // execute_match_with_joins：join 富化 + alert 构建（每事件命中即输出）
    // MatchedContext 一次构造复用（生产由 state machine 构造，构造成本计入
    // advance bench；这里只测富化 + 输出路径）。
    let matched = simple_matched("q13_bench", vec![num(1005.0)], &events[0], NOW);
    let t1 = Instant::now();
    for _ in events.iter().take(N / 10) {
        let _ = std::hint::black_box(exec.execute_match_with_joins(&matched, &lookup));
    }
    let exec_ns = t1.elapsed().as_secs_f64() * 1e9 / (N as f64 / 10.0);
    report("q13 match+join emit", exec_ns, adv_ns);

    // 分量 1：build_eval_context（不含 join）
    let rule = q13_rule();
    let step_plans: Vec<&StepPlan> = rule.match_plan.event_steps.iter().collect();
    let needed = crate::match_engine::executor::CloseCtxFields::All;
    let t2 = Instant::now();
    for _ in 0..(N / 10) {
        let ctx = crate::match_engine::executor::build_eval_context(
            &rule.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
            matched.trigger_event.as_deref(),
            &needed,
        );
        std::hint::black_box(ctx);
    }
    let ctx_ns = t2.elapsed().as_secs_f64() * 1e9 / (N as f64 / 10.0);
    report("q13 build ctx", ctx_ns, exec_ns);

    // 分量 2：build_eval_context + execute_joins（富化，不含 alert 构建）
    let t3 = Instant::now();
    for _ in 0..(N / 10) {
        let mut ctx = crate::match_engine::executor::build_eval_context(
            &rule.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
            matched.trigger_event.as_deref(),
            &needed,
        );
        let ok = crate::match_engine::executor::execute_joins(&rule.joins, &mut ctx, &lookup, NOW);
        std::hint::black_box((ctx, ok));
    }
    let ctxjoin_ns = t3.elapsed().as_secs_f64() * 1e9 / (N as f64 / 10.0);
    report("q13 ctx+join (富化)", ctxjoin_ns, exec_ns);
}

// ---------------------------------------------------------------------------
// Bench 5b：Q6 每事件 emit 路径归因（match + join-then-key，live_joins 空，
//          score 常量 + entity/yield 读左窗字段）——q6 26M EMIT 的瓶颈侧。
// ---------------------------------------------------------------------------

/// Q6 形状 RulePlan：`match<seller:10m> avg>=200` + auction snapshot join
/// （键来自 join 右窗 → join 存活但输出全左窗限定 → live_joins 空）。
fn q6_rule() -> RulePlan {
    let mut plan = simple_rule_plan(
        "q6_bench",
        q4_q6_plan(false),
        Expr::Number(20.0),
        "digit",
        Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
    );
    plan.binds[0].alias = "b".into();
    plan.binds[0].window = "bid_events".into();
    plan.joins = vec![JoinPlan {
        right_window: "auction_events".to_string(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "auction".into()),
            right: FieldRef::Qualified("auction_events".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q6_avg200".into()),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::StringLit("avg bid >= 200".into()),
        },
        YieldField {
            name: "request_count".into(),
            value: Expr::Number(1.0),
        },
    ];
    plan
}

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q6_match_emit() {
    use crate::match_engine::executor::CloseCtxFields;
    use crate::match_engine::executor::build_eval_context;

    let events = bid_events(N);
    let lookup = AuctionLookup::new(AUCTION_DOMAIN);
    let rule = q6_rule();
    let exec = RuleExecutor::new(rule.clone());
    assert!(
        exec.live_joins().is_empty(),
        "q6 输出全左窗限定（yield 读 b.auction）→ join 必须判死，否则富化是纯浪费"
    );

    // 每事件 emit：execute_match_with_joins（live_joins 空 → execute_joins 空转）。
    let matched = simple_matched("q6_bench", vec![num(20.0)], &events[0], NOW);
    let t1 = Instant::now();
    for _ in 0..N {
        let _ = std::hint::black_box(exec.execute_match_with_joins(&matched, &lookup));
    }
    let exec_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q6 match emit(全路径)", exec_ns, exec_ns);

    // 分量 1：build_eval_context（Named 窄化——q6 编译产物只读 b.auction/seller）。
    let step_plans: Vec<&StepPlan> = rule.match_plan.event_steps.iter().collect();
    let needed = CloseCtxFields::Named(HashSet::from(["auction".to_string()]));
    let t2 = Instant::now();
    for _ in 0..N {
        let ctx = build_eval_context(
            &rule.match_plan.keys,
            &matched.scope_key,
            &matched.step_data,
            &matched.bind_data,
            &step_plans,
            matched.trigger_event.as_deref(),
            &needed,
        );
        std::hint::black_box(ctx);
    }
    let ctx_ns = t2.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q6 build ctx(窄化)", ctx_ns, exec_ns);

    // 分量 2：build_match_alert（ctx 复用，只测 alert 构建）。
    let ctx = build_eval_context(
        &rule.match_plan.keys,
        &matched.scope_key,
        &matched.step_data,
        &matched.bind_data,
        &step_plans,
        matched.trigger_event.as_deref(),
        &needed,
    );
    let t3 = Instant::now();
    for _ in 0..N {
        let _ = std::hint::black_box(exec.build_match_alert(&matched, &ctx, NOW).unwrap());
    }
    let alert_ns = t3.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q6 build_match_alert", alert_ns, exec_ns);
}

// ---------------------------------------------------------------------------
// Bench 6：Q14 on each + bind filter + strftime/count_char
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q14_each_strftime_count_char() {
    let events = bid_events(N);
    let exec = RuleExecutor::new(q14_rule());
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(exec.execute_each(ev, ts));
    }
    let q14_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q14 filter+strftime", q14_ns, q14_ns);
}

/// Q14 同数据（与 `bid_events` 同一 LCG 序列）：auction/bidder/price 用列，
/// dateTime/extra 支撑 strftime/count_char；分帧构建 RecordBatch 模拟 wfgen
/// 8MiB 帧（~5-6 万行/批）输入形态。
fn q14_bid_batch(start: usize, n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("dateTime", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
    ]));
    // 与 `bid_events` 完全同一 LCG 序列（price/bidder/auction 每事件 3 次）。
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    for _ in 0..start {
        next_price(&mut rng);
        next_u64(&mut rng);
        next_u64(&mut rng);
    }
    let mut auction = Vec::with_capacity(n);
    let mut bidder = Vec::with_capacity(n);
    let mut price = Vec::with_capacity(n);
    let mut date_time = Vec::with_capacity(n);
    for i in 0..n {
        price.push(next_price(&mut rng) as i64);
        bidder.push(BIDDER_BASE + (next_u64(&mut rng) % BIDDER_DOMAIN) as i64);
        auction.push(AUCTION_BASE + (next_u64(&mut rng) % AUCTION_DOMAIN) as i64);
        date_time.push(NOW + (start + i) as i64 * EVENT_STEP_NS);
    }
    let channel = vec!["Google"; n];
    let url = vec![nexmark_url(); n];
    let extra = vec!["x"; n];
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)),
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(price)),
            Arc::new(arrow::array::StringArray::from(channel)),
            Arc::new(arrow::array::StringArray::from(url)),
            Arc::new(Int64Array::from(date_time)),
            Arc::new(arrow::array::StringArray::from(extra)),
        ],
    )
    .expect("batch")
}

struct NoLookup;
impl WindowLookup for NoLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        None
    }
    fn join_lookup(&self, _w: &str, _kf: &str, _k: &Value) -> Option<Vec<JoinRow>> {
        None
    }
}

/// Q14 列式路径（F6 扩展：each 列式 filter + 递归输出函数）：多帧（同 wfgen
/// 8MiB 帧）+ ALERT_BATCH_SIZE 分段调用，同生产 `emit_each_direct_batch_columnar`；
/// 与行式批路径**同数据同分段对拍**（stats + 输出行逐位一致）并测加速比。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q14_each_strftime_count_char_columnar() {
    use crate::alert::AlertColumnBuilder;
    use crate::match_engine::event_bridge::ColumnarEvent;
    use crate::match_engine::executor::EachDirectBatchStats;

    const SEG: usize = 4096; // 生产 ALERT_BATCH_SIZE
    const FRAME: usize = 65_536; // wfgen 默认 8MiB 帧 ≈ 5-6 万行/批

    let exec = RuleExecutor::new(q14_rule());
    assert!(
        exec.each_plan_columnar_safe(),
        "q14 each filter + 递归输出函数必须列式放行"
    );

    // 列式：多帧分段调用（同生产 emit_each_direct_batch_columnar——帧级
    // each_batch_prepare 一次 + 各段复用，避免逐段对整帧重算）。
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let mut stats_col = EachDirectBatchStats::default();
    let t0 = Instant::now();
    for start in (0..N).step_by(FRAME) {
        let n = FRAME.min(N - start);
        let batch = q14_bid_batch(start, n);
        let prepared = exec.each_batch_prepare(&batch);
        let col_events: Vec<ColumnarEvent<'_>> =
            (0..n).map(|r| ColumnarEvent::new(&batch, r)).collect();
        let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
            .iter()
            .enumerate()
            .map(|(i, ev)| (ev, NOW + (start + i) as i64 * EVENT_STEP_NS))
            .collect();
        for seg in col_rows.chunks(SEG) {
            let s = exec.execute_each_direct_batch_columnar_with(
                seg,
                NOW,
                &prepared,
                &mut builder,
                &mut appended,
            );
            stats_col.appended += s.appended;
            stats_col.rejected += s.rejected;
            stats_col.failed += s.failed;
        }
    }
    let col_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    let col_output = builder.finish();

    // 行式参照（Event 版批路径，同数据同分段）。
    let events = bid_events(N);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NOW + i as i64 * EVENT_STEP_NS))
        .collect();
    let mut b2 = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx2 = Vec::new();
    let mut stats_row = EachDirectBatchStats::default();
    let t0 = Instant::now();
    for seg in rows.chunks(SEG) {
        let s = exec.execute_each_direct_batch(seg, &NoLookup, &[], NOW, &mut b2, &mut idx2);
        stats_row.appended += s.appended;
        stats_row.rejected += s.rejected;
        stats_row.failed += s.failed;
    }
    let row_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    let row_output = b2.finish();

    // 对拍：stats + 输出行逐位一致（防列式路径回归）。O(n) zip 对比。
    assert_eq!(stats_col, stats_row, "列式/行式 stats 必须一致");
    assert_eq!(col_output.len(), row_output.len(), "输出行数一致");
    let rows_a = col_output.iter_data_records();
    let rows_b = row_output.iter_data_records();
    for (row, (ra, rb)) in rows_a.zip(rows_b).enumerate() {
        let (ra, rb) = (ra.unwrap(), rb.unwrap());
        assert_eq!(ra.items.len(), rb.items.len(), "row {row} field count");
        for (fa, fb) in ra.items.iter().zip(rb.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(fa.get_value(), fb.get_value(), "row {row} field value");
        }
    }
    eprintln!(
        "[hotpath] q14 对拍通过：rejected={} appended={}（N={N}）",
        stats_row.rejected, stats_row.appended
    );

    report("q14 each+strftime 列式", col_ns, row_ns);
    report("q14 each+strftime 行式", row_ns, row_ns);
}

// ---------------------------------------------------------------------------
// Bench 7：Q16/Q17 键分片 close 累积（channel 12 measure / auction 8 measure）
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q16_q17_keyed_close() {
    let events = bid_events(N);

    // Q16：channel 键 fixed 30m + 12 close measure（4 count 档 + 8 distinct）
    let mut sm = CepStateMachine::new("q16_bench".to_string(), q16_plan(), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let q16_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q16 channel-keyed close", q16_ns, q16_ns);

    // Q17：auction 键 fixed 30m + 8 close measure（4 count 档 + min/max/avg/sum）
    let mut sm17 = CepStateMachine::new("q17_bench".to_string(), q17_plan(), None);
    let t1 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm17.advance_at("b", ev, ts));
    }
    let q17_ns = t1.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q17 auction-keyed close", q17_ns, q17_ns);
}

// ---------------------------------------------------------------------------
// Bench 8：Q18 (bidder,auction) 复合键 close count
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q18_composite_key_close() {
    let events = bid_events(N);
    let mut sm = CepStateMachine::new("q18_bench".to_string(), q18_plan(), None);
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(sm.advance_at("b", ev, ts));
    }
    let q18_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q18 composite-key close", q18_ns, q18_ns);
}

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

// ---------------------------------------------------------------------------
// Bench 10：Q20 on each + snapshot join + where
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q20_each_snapshot_join_where() {
    let events = bid_events(N);
    let lookup = AuctionLookup::new(AUCTION_DOMAIN);
    let exec = RuleExecutor::new(q20_rule());
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(exec.execute_each_with_joins(ev, ts, &lookup, &[], ts));
    }
    let q20_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q20 each+join+where", q20_ns, q20_ns);
}

/// Q20 列式 join 富化（F6，2026-08-23）：批级 join_lookup + 列式右窗读，与行式
/// 批路径（`execute_each_direct_batch`）**同批对拍**（stats + 输出行逐位一致）
/// 并测量加速比。分段 256 行模拟生产 `ALERT_BATCH_SIZE` 调用形态。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q20_each_snapshot_join_where_columnar() {
    use crate::alert::AlertColumnBuilder;
    use crate::match_engine::event_bridge::{ColumnarEvent, materialize_rows};
    use crate::match_engine::executor::EachDirectBatchStats;

    const SEG: usize = 256; // 生产 ALERT_BATCH_SIZE 分段
    let batch = bid_batch(N);
    let lookup = AuctionLookup::new(AUCTION_DOMAIN);
    let exec = RuleExecutor::new(q20_rule());
    assert!(
        exec.each_join_columnar_ready() && exec.each_plan_columnar_safe(),
        "q20 形状必须列式 join 支持（F6）"
    );

    // 列式 join 路径（分段调用，同生产 emit_each_direct_batch_columnar_join）。
    let col_events: Vec<ColumnarEvent<'_>> =
        (0..N).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent<'_>, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NOW + i as i64 * EVENT_STEP_NS))
        .collect();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let mut stats_col = EachDirectBatchStats::default();
    let t0 = Instant::now();
    for seg in col_rows.chunks(SEG) {
        let s = exec.execute_each_direct_batch_columnar_join(
            seg,
            &lookup,
            NOW,
            &mut builder,
            &mut appended,
        );
        stats_col.appended += s.appended;
        stats_col.rejected += s.rejected;
        stats_col.failed += s.failed;
    }
    let col_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    let col_output = builder.finish();

    // 行式参照（Event 版批路径，同批对拍）。
    let all: Vec<u32> = (0..N as u32).collect();
    let events = materialize_rows(&batch, &all);
    let rows: Vec<(&Event, i64)> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NOW + i as i64 * EVENT_STEP_NS))
        .collect();
    let mut b2 = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut idx2 = Vec::new();
    let mut stats_row = EachDirectBatchStats::default();
    let t0 = Instant::now();
    for seg in rows.chunks(SEG) {
        let s = exec.execute_each_direct_batch(seg, &lookup, &[], NOW, &mut b2, &mut idx2);
        stats_row.appended += s.appended;
        stats_row.rejected += s.rejected;
        stats_row.failed += s.failed;
    }
    let row_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    let row_output = b2.finish();

    // 对拍：stats + 输出行逐位一致（防列式路径回归）。O(n) zip 对比——
    // `nth(row)` 每次重扫是 O(n²)，50 万行输出会挂起。
    assert_eq!(stats_col, stats_row, "列式/行式 stats 必须一致");
    assert_eq!(col_output.len(), row_output.len(), "输出行数一致");
    let rows_a = col_output.iter_data_records();
    let rows_b = row_output.iter_data_records();
    for (row, (ra, rb)) in rows_a.zip(rows_b).enumerate() {
        let (ra, rb) = (ra.unwrap(), rb.unwrap());
        assert_eq!(ra.items.len(), rb.items.len(), "row {row} field count");
        for (fa, fb) in ra.items.iter().zip(rb.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(fa.get_value(), fb.get_value(), "row {row} field value");
        }
    }

    report("q20 each+join+where 列式(F6)", col_ns, row_ns);
    report("q20 each+join+where 行式", row_ns, row_ns);
}

// ---------------------------------------------------------------------------
// Bench 11：Q21 bind filter channel_id != ""
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q21_string_bind_filter() {
    let events = bid_events(N);
    let exec = RuleExecutor::new(q21_rule());
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(exec.execute_each(ev, ts));
    }
    let q21_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q21 str bind filter", q21_ns, q21_ns);
}

// ---------------------------------------------------------------------------
// Bench 12：Q22 let split + mvindex + concat 字符串投影
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q22_each_split() {
    let events = bid_events(N);
    let exec = RuleExecutor::new(q22_rule());
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(exec.execute_each(ev, ts));
    }
    let q22_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 each+split", q22_ns, q22_ns);
}
