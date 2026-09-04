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
//! 文件结构（2026-09-04 按主题拆分）：本文件为共享基座——doc + import + q15 计划/
//! 数据生成/report 构造等跨 bench harness。bench 本体拆入兄弟 `#[path]` 子模块
//! （同目录文件，机制见 refactor handoff 坑 #24，子模块经 `use super::*` 复用本
//! 文件绑定；独占 helper 随切片迁走）：
//! - `close_bench_q15_cep`：Q15 CEP close 累积逐分量归因 + 生产外围行循环对照
//! - `close_bench_stats_hotpath`：stats 执行器热路径（Q15 行式/列式 vs CEP 对照、
//!   窗口切段、last/top 紧凑化）
//! - `close_bench_stats_merge`：q15 EOS 归并（`StatsExecutor::merge_partial`）候选
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
use crate::match_engine::cep::{
    CepStateMachine, EngineHashMap, Event, FieldSource, RollingStats, StepState, Value, ValueKey,
    accumulate_close_steps, eval_expr_ext,
};
use crate::match_engine::event_bridge::{
    ColumnarEvent, batch_event_time_nanos_at, batch_time_col_index,
};
use crate::match_engine::executor::StatsExecutor;

use super::helpers::{event, num, simple_rule_plan};

const N: usize = 500_000;
/// q15 引用域：bidder ≈ 最近 1000 人 ± lead，auction ≈ 最近 100 个 ± lead。
const BIDDER_BASE: i64 = 1000;
const BIDDER_DOMAIN: i64 = 1010;
const AUCTION_BASE: i64 = 1000;
const AUCTION_DOMAIN: i64 = 110;
/// 惰性累积假设的 slice 大小（100µs/事件 → 100k 事件 = 10s，对齐官方窗口粒度）。
const SLICE_EVENTS: f64 = 100_000.0;

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
        key_exprs: Vec::new(),
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

// ---- 兄弟子模块（2026-09-04 按主题拆分；#[path] 相对本文件目录，机制同 core_coverage / nexmark_hotpath_bench）----
#[path = "close_bench_q15_cep.rs"]
mod close_bench_q15_cep;
#[path = "close_bench_stats_hotpath.rs"]
mod close_bench_stats_hotpath;
#[path = "close_bench_stats_merge.rs"]
mod close_bench_stats_merge;
