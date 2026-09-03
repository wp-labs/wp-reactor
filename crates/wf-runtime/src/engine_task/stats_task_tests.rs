//! StatsTask 接线测试（P1 步骤④c）: push 路径端到端——批次投递 → 列式归并 →
//! 固定窗口 close → alert。本文件为共享 harness（任务构造器/批次/断言辅助）并
//! 按主题分派到兄弟测试子模块（#[path]）:
//! - `stats_task_q15`: Q15 12 度量与 CEP 对拍 + 输入分片归并（StatsPartial）;
//! - `stats_task_windows`: 基础空键/Q12 复合键窗口语义（close/flush/跳变/切段/大 ts/快路径）;
//! - `stats_task_ranked`: Q18/Q19 last/top + perf-diag cuts + evictor 自愈 + 列式分块 close。

#![allow(clippy::await_holding_lock)] // perf-diag 门控测试跨 await 持全局锁（PERF_CUT_SERIAL）
use std::sync::Arc;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use arrow::array::{StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use tokio::sync::mpsc;

use wf_engine::match_engine::{RuleExecutor, StatsExecutor};
use wf_engine::window::{RulePush, Window, WindowParams};
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{
    BindPlan, EntityPlan, ScorePlan, StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan,
    StatsPlan, WindowSpec, YieldField, YieldPlan,
};

use super::stats_task::StatsTask;
use super::task_types::StatsTaskConfig;
use super::tests::{field_str, make_test_fanout, take_alert};
use crate::engine_task::StatsPartial;

#[path = "stats_task_q15.rs"]
mod stats_task_q15;

#[path = "stats_task_windows.rs"]
mod stats_task_windows;

#[path = "stats_task_ranked.rs"]
mod stats_task_ranked;

/// 取回一个 AlertBatch 并展开为全部 record（批量 emit: 一个 close 的多个桶合成
/// 一批, `take_alert` 只取首条——带 key 多桶断言须用本函数）。
fn take_alerts(
    rx: &mut mpsc::Receiver<crate::alert_task::AlertBatch>,
) -> Vec<std::sync::Arc<wp_model_core::model::DataRecord>> {
    let batch = rx.try_recv().expect("expected an alert batch");
    match batch {
        crate::alert_task::AlertBatch::Rows(rows) => rows.as_ref().clone(),
        crate::alert_task::AlertBatch::Columns(cols) => cols
            .iter_data_records()
            .collect::<Result<Vec<_>, _>>()
            .expect("columnar row view conversion")
            .into_iter()
            .map(std::sync::Arc::new)
            .collect(),
    }
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

/// 每行独立时间戳的批次（跨窗口边界切段测试用）。
fn make_ts_batch(pairs: &[(&str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(StringArray::from(
                pairs.iter().map(|(s, _)| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(
                pairs.iter().map(|(_, t)| Some(*t)).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// `stat.value(final(label))` 表达式（与编译后的 yield 同构）。
fn stat_value(label: &str) -> Expr {
    let final_sel = Expr::FuncCall {
        qualifier: None,
        name: "final".into(),
        args: vec![Expr::Field(FieldRef::Simple(label.into()))],
    };
    Expr::FuncCall {
        qualifier: Some("stat".into()),
        name: "value".into(),
        args: vec![final_sel],
    }
}

/// 3 度量 stats 计划（10s fixed 空键）:
/// - total: count（无条件）
/// - r1: count where sip == "10.0.0.1"
/// - uniq: distinct_count(sip)
fn make_stats_plan() -> StatsPlan {
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
        window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
        keys: vec![],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![
            m("total", StatsAggPlan::Count, None, None),
            m(
                "r1",
                StatsAggPlan::Count,
                None,
                Some(Expr::BinOp {
                    op: wf_lang::ast::BinOp::Eq,
                    left: Box::new(Expr::Field(FieldRef::Qualified("b".into(), "sip".into()))),
                    right: Box::new(Expr::StringLit("10.0.0.1".into())),
                }),
            ),
            m("uniq", StatsAggPlan::DistinctCount, Some("sip"), None),
        ],
        tracked_bind_fields: HashMap::new(),
    }
}

fn make_stats_rule_plan() -> wf_lang::plan::RulePlan {
    let detail = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("{} {} {}".into()),
            stat_value("total"),
            stat_value("r1"),
            stat_value("uniq"),
        ],
    };
    wf_lang::plan::RulePlan {
        name: "stats_rule".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: vec![],
        match_plan: wf_lang::plan::MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::And,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(make_stats_plan()),
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Number(1.0),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "detail".into(),
                value: detail,
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    }
}

async fn push_batch(task: &mut StatsTask, batch: RecordBatch, seq: u64) {
    let push = RulePush {
        window_name: "bid_events".into(),
        events: None,
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: None,
        seq,
    };
    task.process_push(push).await;
}
