//! StatsTask 接线测试（P1 步骤④c）: push 路径端到端——批次投递 → 列式归并 →
//! 固定窗口 close → alert; 窗口语义（单窗口/flush/跨窗口跳变）; ack floor;
//! 非列式 where 回退行式。

use std::collections::HashMap;
use std::sync::Arc;
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

fn make_window(name: &str, schema: &SchemaRef) -> Arc<Window> {
    Arc::new(Window::new(
        WindowParams {
            name: name.into(),
            schema: schema.clone(),
            time_col_index: Some(1), // event_time 是第二列
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        super::tests::test_window_config(usize::MAX),
    ))
}

fn make_batch(sips: &[&str], ts: i64) -> RecordBatch {
    let n = sips.len();
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(StringArray::from(
                sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![ts; n])),
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

/// 构建 StatsTask（push 路径, seq 从 1 开始）+ alert 接收 + progress slot。
fn make_stats_task() -> (
    StatsTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<AtomicU64>,
) {
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let win = make_window("bid_events", &test_schema());
    let progress = Arc::new(AtomicU64::new(0));
    let config = StatsTaskConfig {
        stats: StatsExecutor::new(make_stats_plan()),
        executor: RuleExecutor::new(make_stats_rule_plan()),
        window_sources: vec![super::task_types::WindowSource {
            window_name: "bid_events".into(),
            window: Arc::clone(&win),
            notify: Arc::new(tokio::sync::Notify::new()),
            aliases: vec!["b".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        router: Arc::new(wf_engine::window::Router::new(
            wf_engine::window::WindowRegistry::build(vec![]).unwrap(),
        )),
        metrics: None,
        time_field: Some("event_time".into()),
        intermediate_targets: std::collections::HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        progress: std::collections::HashMap::from([(
            "bid_events".to_string(),
            Arc::clone(&progress),
        )]),
        shard_index: None,
        shard_count: 1,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx, progress)
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

#[tokio::test]
async fn stats_push_closes_window_on_watermark_and_emits_alert() {
    let (mut task, mut alert_rx, progress) = make_stats_task();

    // batch1: 3 行, ts=5s（窗口 [0,10s) 内）→ 不 close, 无 alert
    let b1 = make_batch(&["10.0.0.1", "10.0.0.1", "10.0.0.2"], 5_000_000_000);
    push_batch(&mut task, b1, 1).await;
    assert!(alert_rx.try_recv().is_err(), "窗口未关闭不应产出");
    // ack: seq=1 → slot=2
    assert_eq!(progress.load(std::sync::atomic::Ordering::Relaxed), 2);

    // batch2: 3 行, ts=15s（>= 窗口边界 10s）→ close 窗口 1, 开窗口 2
    let b2 = make_batch(&["10.0.0.1", "10.0.0.3", "10.0.0.3"], 15_000_000_000);
    push_batch(&mut task, b2, 2).await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "stats_rule");
    assert_eq!(
        field_str(&alert, "detail"),
        "3 2 2",
        "total=3, r1=2, uniq=2"
    );
    // ack: seq=2 → slot=3
    assert_eq!(progress.load(std::sync::atomic::Ordering::Relaxed), 3);

    // flush: 关闭残留窗口 2（batch2 数据: total=3, r1=1, uniq=2）
    task.flush().await;
    let alert2 = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert2, "detail"), "3 1 2", "窗口 2 数据");
}

#[tokio::test]
async fn stats_push_single_window_flush_emits() {
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    // 单批 10s 内 → 不 close; flush 收尾
    let b1 = make_batch(&["10.0.0.1", "10.0.0.1"], 5_000_000_000);
    push_batch(&mut task, b1, 1).await;
    assert!(alert_rx.try_recv().is_err());
    task.flush().await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "detail"), "2 2 1");
}

#[tokio::test]
async fn stats_push_multiple_window_jump_emits_only_populated() {
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    // batch1: 5s（窗口 1）
    push_batch(&mut task, make_batch(&["10.0.0.1"], 5_000_000_000), 1).await;
    // batch2: 直接跳 35s（窗口 4）→ 窗口 1 close（窗口 2/3 空无产出）
    push_batch(
        &mut task,
        make_batch(&["10.0.0.2", "10.0.0.2"], 35_000_000_000),
        2,
    )
    .await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&alert, "detail"),
        "1 1 1",
        "窗口 1: total=1, r1=1, uniq=1"
    );
    // flush 关窗口 4
    task.flush().await;
    let alert2 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&alert2, "detail"),
        "2 0 1",
        "窗口 4: total=2, r1=0, uniq=1"
    );
}

#[tokio::test]
async fn stats_push_columnar_fallback_row_path() {
    // 非列式 where（含函数调用）→ process_batch 返回 false → 回退行式, 语义等价
    // 用第 4 个度量（where 含 len(sip) > 4——不可列式化）验证不崩且 close 正确
    let plan = {
        let mut p = make_stats_plan();
        p.measures.push(StatsMeasurePlan {
            label: "long_sip".into(),
            source_alias: "b".into(),
            where_expr: Some(Expr::BinOp {
                op: wf_lang::ast::BinOp::Gt,
                left: Box::new(Expr::FuncCall {
                    qualifier: None,
                    name: "len".into(),
                    args: vec![Expr::Field(FieldRef::Qualified("b".into(), "sip".into()))],
                }),
                right: Box::new(Expr::Number(4.0)),
            }),
            agg: StatsAggPlan::Count,
            field: None,
            arg: None,
        });
        p
    };
    let (mut task, mut alert_rx, _progress) = make_stats_task_with_plan(plan);
    push_batch(
        &mut task,
        make_batch(&["10.0.0.1", "10.0.0.2"], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    let alert = take_alert(&mut alert_rx);
    // len("10.0.0.1")=8 >4 → 计数; 两个都满足
    assert_eq!(
        field_str(&alert, "detail"),
        "2 1 2 2",
        "含不可列式 where 的回退路径"
    );
}

fn make_stats_task_with_plan(
    plan: StatsPlan,
) -> (
    StatsTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<AtomicU64>,
) {
    let mut rp = make_stats_rule_plan();
    rp.stats_plan = Some(plan.clone());
    rp.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("{} {} {} {}".into()),
                stat_value("total"),
                stat_value("r1"),
                stat_value("uniq"),
                stat_value("long_sip"),
            ],
        },
    }];
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let win = make_window("bid_events", &test_schema());
    let progress = Arc::new(AtomicU64::new(0));
    let config = StatsTaskConfig {
        stats: StatsExecutor::new(plan),
        executor: RuleExecutor::new(rp),
        window_sources: vec![super::task_types::WindowSource {
            window_name: "bid_events".into(),
            window: Arc::clone(&win),
            notify: Arc::new(tokio::sync::Notify::new()),
            aliases: vec!["b".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        router: Arc::new(wf_engine::window::Router::new(
            wf_engine::window::WindowRegistry::build(vec![]).unwrap(),
        )),
        metrics: None,
        time_field: Some("event_time".into()),
        intermediate_targets: std::collections::HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        progress: std::collections::HashMap::from([(
            "bid_events".to_string(),
            Arc::clone(&progress),
        )]),
        shard_index: None,
        shard_count: 1,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx, progress)
}
