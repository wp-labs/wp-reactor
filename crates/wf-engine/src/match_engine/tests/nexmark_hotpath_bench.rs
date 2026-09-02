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
//!   q19_close_output_chain      : Q19 close 输出链分解（2026-08-25 采样定位的可压点
//!                                 数据度量——逐条目 CloseOutput 结构开销 / fmt detail /
//!                                 列式 close 全链基线，见 `q19_close_output_chain`）
//!
//! 数据域对齐（NEXMark 官方）：bidder ≈ 最近 1000 人、auction ≈ 最近 100 个、
//! 价格对数均匀 ∈ [100, 1e8)；事件时间步长 = 30m 数据 / 27.6M bid ≈ 65.2µs/事件。
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, StringArray};
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
use wf_lang::{BaseType, FieldType};

use crate::match_engine::executor::{
    RowFieldLayout, RowFields, StatsAccum, StatsBucketAccs, StatsExecutor,
};
use crate::match_engine::match_engine::{
    BindData, CepStateMachine, CloseOutput, CloseReason, EngineHashMap, Event, MatchedContext,
    ScopeKey, StepData, Value, WindowLookup,
};
use crate::match_engine::{JoinRow, RuleExecutor, TriggerEvent, apply_conv};

use super::helpers::{event, num, simple_plan, simple_rule_plan, str_val};

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

/// Q14（对齐真实 `nexmark_pk/models/queries/q14.wfl` 形状）：on each + 价格区间
/// filter + 4 个 yield 字段；detail = `fmt("{} c={}", 嵌套 3 档 CASE
/// nightTime/dayTime/otherTime（10/9 项 InList）, count_char(extra,"c"))`。
/// 价格区间在真实规则里是 bind filter；bench 直接驱动 executor（不经过
/// rule_task 的 bind mask），故按 each filter 建模——输出链成本形状一致。
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
    let in_hours = |hours: &[&str]| Expr::InList {
        expr: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "strftime".into(),
            args: vec![b_field("dateTime"), Expr::StringLit("%H".into())],
        }),
        list: hours.iter().map(|h| Expr::StringLit((*h).into())).collect(),
        negated: false,
    };
    let bid_time_type = Expr::IfThenElse {
        cond: Box::new(in_hours(&[
            "00", "01", "02", "03", "04", "05", "06", "20", "21", "22", "23",
        ])),
        then_expr: Box::new(Expr::StringLit("nightTime".into())),
        else_expr: Box::new(Expr::IfThenElse {
            cond: Box::new(in_hours(&[
                "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
            ])),
            then_expr: Box::new(Expr::StringLit("dayTime".into())),
            else_expr: Box::new(Expr::StringLit("otherTime".into())),
        }),
    };
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: b_field("auction"),
        },
        YieldField {
            name: "alert_type".into(),
            value: Expr::StringLit("q14_calc".into()),
        },
        YieldField {
            name: "detail".into(),
            value: Expr::FuncCall {
                qualifier: None,
                name: "fmt".into(),
                args: vec![
                    Expr::StringLit("{} c={}".into()),
                    bid_time_type,
                    Expr::FuncCall {
                        qualifier: None,
                        name: "count_char".into(),
                        args: vec![b_field("extra"), Expr::StringLit("c".into())],
                    },
                ],
            },
        },
        YieldField {
            name: "request_count".into(),
            value: Expr::Number(1.0),
        },
    ];
    plan
}

/// q14 bench executor：真实 yield 字段类型（同 `q14.wfl` 输出目标）。
fn q14_exec() -> RuleExecutor {
    RuleExecutor::new_with_yield_field_types(
        q14_rule(),
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Float)),
            ("alert_type".into(), FieldType::Base(BaseType::Chars)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
            ("request_count".into(), FieldType::Base(BaseType::Float)),
        ]),
    )
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
        row_fields: None,
        row_field_names: None,
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
        trigger_event: Some(TriggerEvent::from_event(Arc::new(event.clone()))),
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
            matched.trigger_event.as_ref(),
            &needed,
            None,
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
            matched.trigger_event.as_ref(),
            &needed,
            None,
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
            matched.trigger_event.as_ref(),
            &needed,
            None,
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
        matched.trigger_event.as_ref(),
        &needed,
        None,
    );
    let t3 = Instant::now();
    for _ in 0..N {
        let _ = std::hint::black_box(exec.build_match_alert(&matched, &ctx, NOW).unwrap());
    }
    let alert_ns = t3.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q6 build_match_alert", alert_ns, exec_ns);
}
/// q6 列式批 emit（2026-08-26 对账）：生产 q6 过 `match_plan_columnar_safe`
/// gate → `execute_match_direct_batch_columnar`（列式批，零 OutputRecord 物化），
/// 而 `q6_match_emit` 测的是行式 `execute_match_with_joins`（484ns，非生产形态）。
/// 本 bench 用生产分段（ALERT_BATCH_SIZE 级 chunk）测列式批成本——对账
/// diag 实测 1576ns/evt 的构成。
#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine q6_match_emit_columnar -- --ignored --nocapture"]
fn q6_match_emit_columnar() {
    use crate::alert::AlertColumnBuilder;

    let events = bid_events(N);
    let rule = q6_rule();
    let exec = RuleExecutor::new(rule.clone());
    assert!(
        exec.match_plan_columnar_safe(),
        "q6 形状必须过列式 gate（生产走 execute_match_direct_batch_columnar）"
    );

    // 每事件命中 1 个 MatchedContext（q6 avg>=200 高频，近似生产 advance 命中）。
    let matched: Vec<MatchedContext> = events
        .iter()
        .enumerate()
        .map(|(i, ev)| {
            simple_matched(
                "q6_bench",
                vec![num(20.0)],
                ev,
                NOW + i as i64 * EVENT_STEP_NS,
            )
        })
        .collect();
    let refs: Vec<&MatchedContext> = matched.iter().collect();

    // 批级列式装载（生产分段形态）。
    const SEG: usize = 256;
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended_out = Vec::new();
    let t0 = Instant::now();
    for _ in 0..4 {
        for seg in refs.chunks(SEG) {
            let stats =
                exec.execute_match_direct_batch_columnar(seg, NOW, &mut builder, &mut appended_out);
            std::hint::black_box(&stats);
        }
    }
    let col_ns = t0.elapsed().as_secs_f64() * 1e9 / (N as f64 * 4.0);
    report("q6 match emit(列式批)", col_ns, col_ns);
}

// ---------------------------------------------------------------------------
// Bench 6：Q14 on each + bind filter + strftime/count_char
// ---------------------------------------------------------------------------

#[test]
#[ignore = "release-only benchmark: cargo test --release -p wf-engine nexmark_hotpath_bench -- --ignored --nocapture"]
fn q14_each_strftime_count_char() {
    let events = bid_events(N);
    let exec = q14_exec();
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

    let exec = q14_exec();
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
    // 行式（既有）：逐事件解释（let 逐行 apply_lets + split/mvindex/concat）。
    let events = bid_events(N);
    let exec = RuleExecutor::new(q22_rule());
    let t0 = Instant::now();
    for (i, ev) in events.iter().enumerate() {
        let ts = NOW + i as i64 * EVENT_STEP_NS;
        let _ = std::hint::black_box(exec.execute_each(ev, ts));
    }
    let q22_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 each+split 行式", q22_ns, q22_ns);

    // 列式（层 2，2026-08-25）：let 内联 + SplitIndex/Concat 融合——同一规则
    // 走 each 列式批路径（内联 `let parts = split(...)`），同批对拍 + 测加速比。
    use crate::alert::AlertColumnBuilder;
    use crate::match_engine::event_bridge::{ColumnarEvent, materialize_rows};
    use arrow::array::StringArray;

    let schema = Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("url", DataType::Utf8, false),
    ]);
    let auctions: Vec<i64> = (0..N).map(|i| AUCTION_BASE + i as i64).collect();
    let urls: Vec<String> = (0..N).map(|_| nexmark_url().to_string()).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(auctions)),
            Arc::new(StringArray::from(urls)),
        ],
    )
    .expect("batch");
    let exec_col = RuleExecutor::new_with_yield_field_types(
        q22_rule(),
        HashMap::from([
            ("id".into(), FieldType::Base(BaseType::Digit)),
            ("detail".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );
    assert!(
        exec_col.each_plan_columnar_safe(),
        "q22 let+split+mvindex+concat 必须过 each 列式门控（层 2）"
    );

    let col_events: Vec<ColumnarEvent> = (0..N).map(|r| ColumnarEvent::new(&batch, r)).collect();
    let col_rows: Vec<(&ColumnarEvent, i64)> = col_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NOW + i as i64 * EVENT_STEP_NS))
        .collect();
    let mut b_col = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_col = Vec::new();
    let t0 = Instant::now();
    let stats_col =
        exec_col.execute_each_direct_batch_columnar(&col_rows, NOW, &mut b_col, &mut app_col);
    let col_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    assert_eq!(stats_col.appended, N, "列式输出行数 = N");
    report("q22 each+split 列式", col_ns, q22_ns);

    // 行式批路径同批对拍（层 2 防回归：内联展开与 apply_lets 逐位一致）。
    let all: Vec<u32> = (0..N as u32).collect();
    let row_events = materialize_rows(&batch, &all);
    let rows: Vec<(&Event, i64)> = row_events
        .iter()
        .enumerate()
        .map(|(i, ev)| (ev, NOW + i as i64 * EVENT_STEP_NS))
        .collect();
    let mut b_row = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut app_row = Vec::new();
    let stats_row =
        exec_col.execute_each_direct_batch(&rows, &NoLookup, &[], NOW, &mut b_row, &mut app_row);
    assert_eq!(stats_row.appended, N, "行式输出行数 = N");
    assert_eq!(
        b_col.finish().len(),
        b_row.finish().len(),
        "列式/行式输出行数一致"
    );

    // ---- split 内部拆解（2026-08-26 q22 内存归因）：全分割 collect vs 惰性 nth ----
    // 生产 `split_index_vec` 每行 `text.split(sep).collect::<Vec<_>>()` 再索引——
    // url 3 段目录 + query（split 后 ≥6 段）全分割建 Vec 是纯浪费。量化惰性
    // `split(sep).nth(k)`（只扫描到第 k 段）的加速空间。
    let sep = "/";
    let mut sum = 0usize;
    let t0 = Instant::now();
    for _ in 0..N {
        let parts: Vec<&str> = nexmark_url().split(sep).collect();
        let k = normalize_idx(3, parts.len());
        if let Some(k) = k {
            sum += parts[k].len();
        }
    }
    let collect_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 split 全分割 collect", collect_ns, collect_ns);

    let t0 = Instant::now();
    for _ in 0..N {
        let picked = nexmark_url().split(sep).nth(3);
        if let Some(p) = picked {
            sum += p.len();
        }
    }
    let nth_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 split 惰性 nth(3)", nth_ns, collect_ns);

    // ---- concat 内部拆解（2026-08-26 q22 内存归因 2）：String::new 无预分配 +
    // 逐参 value_to_string 转换 vs 预分配 + 直接 push_str。q22 detail =
    // concat(3 段 + 2 个 "/")，每行 5 参数。----
    let segs: Vec<&str> = nexmark_url().split(sep).collect();
    let mut sum2 = 0usize;
    let t0 = Instant::now();
    for _ in 0..N {
        let mut s = String::new();
        s.push_str(segs[3]);
        s.push_str(sep);
        s.push_str(segs[4]);
        s.push_str(sep);
        s.push_str(segs[5]);
        sum2 += s.len();
    }
    let cat_naive_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 concat 无预分配", cat_naive_ns, cat_naive_ns);

    let t0 = Instant::now();
    for _ in 0..N {
        let cap = segs[3].len() + 1 + segs[4].len() + 1 + segs[5].len();
        let mut s = String::with_capacity(cap);
        s.push_str(segs[3]);
        s.push_str(sep);
        s.push_str(segs[4]);
        s.push_str(sep);
        s.push_str(segs[5]);
        sum2 += s.len();
    }
    let cat_cap_ns = t0.elapsed().as_secs_f64() * 1e9 / N as f64;
    report("q22 concat 预分配", cat_cap_ns, cat_naive_ns);
    assert!(sum > 0 && sum2 > 0);
}

/// mvindex 负索引/越界归一（与 `normalize_index_simple` 同语义的 bench 内联版）。
fn normalize_idx(index: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let normalized = if index < 0 { len + index } else { index };
    if normalized < 0 || normalized >= len {
        None
    } else {
        Some(normalized as usize)
    }
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
