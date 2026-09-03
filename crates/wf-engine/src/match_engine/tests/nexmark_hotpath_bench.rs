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
//! 本文件为共享基座：doc + import + 数据生成/report/表达与 plan 构造/Lookup
//! 替身/CloseOutput 构造等跨 bench harness。bench 本体已按主题拆入兄弟 `#[path]`
//! 子模块（同目录文件，2026-09-04；机制见 refactor handoff 坑 #24，子模块经
//! `use super::*` 复用本文件绑定；独占 helper 随切片迁走）：
//! - `nexmark_state_advance_bench`：Q4/Q6/Q5/Q7/Q11/Q12/Q13/Q16/Q17/Q18 状态机推进与命中 emit
//! - `nexmark_each_emit_bench`：Q14/Q20/Q21/Q22 on each 直通输出（行式 ↔ 列式对拍）
//! - `nexmark_stats_state_bench`：Q19/q4b stats 基准 + Q19 close 输出链 / Q18 每键状态归因
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

use crate::match_engine::cep::{
    BindData, CepStateMachine, CloseOutput, CloseReason, EngineHashMap, Event, MatchedContext,
    ScopeKey, StepData, Value, WindowLookup,
};
use crate::match_engine::executor::{
    RowFieldLayout, RowFields, StatsAccum, StatsBucketAccs, StatsExecutor,
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
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
            key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
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
        first_match_time_nanos: None,
        evidence_first_time_nanos: ts,
        evidence_last_time_nanos: ts,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        machine_id: "".into(),
        trigger_event: Some(TriggerEvent::from_event(Arc::new(event.clone()))),
    }
}

// ---- 兄弟子模块（2026-09-04 按主题拆分；#[path] 相对本文件目录，机制同 core_coverage / compile_tests）----
#[path = "nexmark_each_emit_bench.rs"]
mod nexmark_each_emit_bench;
#[path = "nexmark_state_advance_bench.rs"]
mod nexmark_state_advance_bench;
#[path = "nexmark_stats_state_bench.rs"]
mod nexmark_stats_state_bench;
