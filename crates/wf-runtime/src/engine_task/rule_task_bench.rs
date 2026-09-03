//! q13a 中间窗生产路径微基准（2026-08-25，数据驱动定位用）。
//!
//! 背景：q13a（`on each b` → yield 中间窗 bid_mod，含 `mod_key = auction % 10000`
//! BinOp）分片后仍是 100M q13 瓶颈——10 核总吞吐仅 ~692k/s（每行 ~14µs），
//! 远超合理值（q13b 的 row path join 才 ~2.5µs/行）。本基准在同一进程内直接
//! 测 q13a 生产路径的**逐段成本**（非猜测）：
//!
//!   ① per-record 求值（`execute_each_with_joins` → OutputRecord）——中间窗
//!      each 无批量路径，走每行 OutputRecord；
//!   ② 中间窗装载（`PipeBatchStager::push_record`，含 `record_window_fields`
//!      的字段查找）；
//!   ③ 对照：批量路径（`execute_each_direct_batch`，sink 形态）——量化
//!      「intermediate 无批量路径」的代价；
//!   ④ 对照：无 staging 的裸 on-each（`execute_each_direct`）。
//!
//! 运行：
//!   cargo test --release -p wf-runtime q13a_pipe_bench -- --ignored --nocapture
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{
    BindPlan, EachPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, YieldField, YieldPlan,
};

use wf_engine::match_engine::event_bridge::batch_to_events;
use wf_engine::match_engine::{RuleExecutor, StatsBucketAccs, WindowLookup};

use super::{OutputRecord, PipeBatchStager};
use crate::engine_task::tests::empty_tracked_bind_fields;

// row_loop 子模块基准的 r4 harness（私有 use 绑定，子模块 `use super::*` 继承）。
use super::rule_task_r4::{
    Spec, machine_rule, make_batch, make_task, make_window, metrics, minimal_plan,
    run_with_dispatch, test_schema,
};

const N: usize = 100_000;
const NANOS: i64 = 1_750_000_000_000_000_000;

/// bid_events 形状批（q13a 读 auction/bidder/price/dateTime/channel/url）。
fn bid_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("channel", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
    ]));
    let auction: Vec<i64> = (0..n as i64).map(|i| i * 7).collect();
    let bidder: Vec<i64> = (0..n as i64).map(|i| i % 100_000).collect();
    let price: Vec<i64> = (0..n as i64).map(|i| (i * 37) % 1_000_000).collect();
    let date_time: Vec<i64> = (0..n as i64).map(|i| NANOS + i).collect();
    let channel: Vec<&str> = vec!["G"; n];
    let url: Vec<&str> = vec!["https://x/y/z"; n];
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)) as ArrayRef,
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(price)),
            Arc::new(arrow::array::TimestampNanosecondArray::from(date_time)),
            Arc::new(arrow::array::StringArray::from(channel)),
            Arc::new(arrow::array::StringArray::from(url)),
        ],
    )
    .unwrap()
}

/// q13a 形状的 RulePlan（`on each b` + entity(digit, b.bidder) + yield
/// bid_mod（5 个 Field + 1 个 mod BinOp））——executor 与边缘对拍共用。
fn q13a_plan_rule_plan() -> RulePlan {
    RulePlan {
        conv_window: None,
        name: "q13a_bench".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
            key_map: None,
            key_join: None,
            window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::ZERO),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "b".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
        },
        yield_plan: YieldPlan {
            target: "bid_mod".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
                },
                YieldField {
                    name: "bidder".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
                },
                YieldField {
                    name: "auction".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
                },
                YieldField {
                    name: "price".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
                },
                YieldField {
                    name: "dateTime".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "dateTime".into())),
                },
                YieldField {
                    name: "mod_key".into(),
                    value: Expr::BinOp {
                        op: wf_lang::ast::BinOp::Mod,
                        left: Box::new(Expr::Field(FieldRef::Qualified(
                            "b".into(),
                            "auction".into(),
                        ))),
                        right: Box::new(Expr::Number(10000.0)),
                    },
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    }
}

/// q13a 形状的 RuleExecutor：`on each b` + entity(digit, b.bidder) + yield
/// bid_mod（5 个 Field + 1 个 mod BinOp）。
fn q13a_plan_rule() -> RuleExecutor {
    RuleExecutor::new(q13a_plan_rule_plan())
}

/// 空 WindowLookup（q13a 无 join，不查询窗口）。
struct NoLookup;
impl WindowLookup for NoLookup {
    fn snapshot_field_values(
        &self,
        _w: &str,
        _f: &str,
    ) -> Option<std::collections::HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<wf_engine::match_engine::JoinRow>> {
        None
    }
}

/// bid_mod 中间窗 schema（q13a yield 目标：id/bidder/auction/price/dateTime/mod_key）。
fn bid_mod_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("mod_key", DataType::Int64, true),
    ]))
}

fn report(name: &str, per_ns: f64, baseline_ns: f64) {
    let mps = 1e9 / per_ns / 1e6;
    eprintln!(
        "[q13a-pipe-bench] {:<34} {:>9.1} ns/row  ({:>7.2}M rows/s)  = {:>6.1}% of baseline",
        name,
        per_ns,
        mps,
        per_ns / baseline_ns * 100.0
    );
}

// ---------------------------------------------------------------------------
// 基准子模块拆件（2026-09-04）：共享 harness / import / helper 留本文件顶层，
// 子模块以 `use super::*` 继承；主题：rule_task_bench_pipe（中间窗装载/字节对拍）·
// rule_task_bench_join（q13b 生产路径/并发锁）· rule_task_bench_alloc（pipe+q18
// 分配足迹）· rule_task_bench_row_loop（行循环吞吐回归）。
// ---------------------------------------------------------------------------
#[cfg(test)]
#[path = "rule_task_bench_pipe.rs"]
mod rule_task_bench_pipe;

#[cfg(test)]
#[path = "rule_task_bench_join.rs"]
mod rule_task_bench_join;

#[cfg(test)]
#[path = "rule_task_bench_alloc.rs"]
mod rule_task_bench_alloc;

#[cfg(test)]
#[path = "rule_task_bench_row_loop.rs"]
mod rule_task_bench_row_loop;
