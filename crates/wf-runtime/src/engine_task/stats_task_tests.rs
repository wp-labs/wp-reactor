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

// ---------------------------------------------------------------------------
// Q15 12 度量验证（stats 执行器真实任务路径, 与 CEP 锚点对拍）
// ---------------------------------------------------------------------------

fn q15_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

fn make_q15_batch(rows: &[(i64, i64, i64)], ts: i64) -> RecordBatch {
    use arrow::array::Int64Array;
    let n = rows.len();
    RecordBatch::try_new(
        q15_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| Some(r.0)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| Some(r.1)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| Some(r.2)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![ts; n])),
        ],
    )
    .unwrap()
}

/// Q15 12 度量 plan（价格分档 where + count/distinct_count）。
fn q15_plan() -> StatsPlan {
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
    let price = |op: wf_lang::ast::BinOp, v: f64| Expr::BinOp {
        op,
        left: Box::new(Expr::Field(FieldRef::Qualified("b".into(), "price".into()))),
        right: Box::new(Expr::Number(v)),
    };
    let lt = |v: f64| price(wf_lang::ast::BinOp::Lt, v);
    let ge = |v: f64| price(wf_lang::ast::BinOp::Ge, v);
    let range = |lo: f64, hi: f64| Expr::BinOp {
        op: wf_lang::ast::BinOp::And,
        left: Box::new(ge(lo)),
        right: Box::new(lt(hi)),
    };
    let tier_where = |tier: usize| -> Option<Expr> {
        match tier {
            0 => None,
            1 => Some(lt(10_000.0)),
            2 => Some(range(10_000.0, 1_000_000.0)),
            3 => Some(ge(1_000_000.0)),
            _ => unreachable!(),
        }
    };
    let mut measures = Vec::new();
    for (i, name) in ["total", "r1", "r2", "r3"].iter().enumerate() {
        measures.push(m(
            &format!("count_{name}"),
            StatsAggPlan::Count,
            None,
            tier_where(i),
        ));
    }
    for (i, name) in ["total", "r1", "r2", "r3"].iter().enumerate() {
        measures.push(m(
            &format!("bidder_{name}"),
            StatsAggPlan::DistinctCount,
            Some("bidder"),
            tier_where(i),
        ));
    }
    for (i, name) in ["total", "r1", "r2", "r3"].iter().enumerate() {
        measures.push(m(
            &format!("auction_{name}"),
            StatsAggPlan::DistinctCount,
            Some("auction"),
            tier_where(i),
        ));
    }
    StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(1800)),
        keys: vec![],
        output_shape: StatsOutputShapePlan::Rows,
        measures,
        tracked_bind_fields: HashMap::new(),
    }
}

fn make_q15_task() -> (StatsTask, mpsc::Receiver<crate::alert_task::AlertBatch>) {
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let schema = q15_schema();
    let win = Arc::new(Window::new(
        WindowParams {
            name: "bid_events".into(),
            schema: schema.clone(),
            time_col_index: Some(3), // event_time 第 4 列
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        super::tests::test_window_config(usize::MAX),
    ));
    // 12 值 detail（与 CEP 版 q15 yield 同构）
    let mut fmt_args = vec![Expr::StringLit(
        "{} {} {} {} {} {} {} {} {} {} {} {}".into(),
    )];
    for label in [
        "count_total",
        "count_r1",
        "count_r2",
        "count_r3",
        "bidder_total",
        "bidder_r1",
        "bidder_r2",
        "bidder_r3",
        "auction_total",
        "auction_r1",
        "auction_r2",
        "auction_r3",
    ] {
        fmt_args.push(stat_value(label));
    }
    let rp = wf_lang::plan::RulePlan {
        name: "q15_stats".into(),
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
            window_spec: WindowSpec::Fixed(Duration::from_secs(1800)),
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
        stats_plan: Some(q15_plan()),
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
                value: Expr::FuncCall {
                    qualifier: None,
                    name: "fmt".into(),
                    args: fmt_args,
                },
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    };
    let config = StatsTaskConfig {
        stats: StatsExecutor::new(q15_plan()),
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
        timeout_scan_interval: Duration::from_secs(1),
        intermediate_targets: std::collections::HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        progress: std::collections::HashMap::new(),
        shard_index: None,
        shard_count: 1,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx)
}

#[tokio::test]
async fn q15_stats_task_12_measures_matches_cep_anchor() {
    // Q15 真实任务路径（StatsTask 列式归并 → 固定窗口 close → alert）:
    // 6 行覆盖 3 价格档, 期望 12 值（与 CEP 版 q15 锚点同构, 独立手算）。
    let (mut task, mut alert_rx) = make_q15_task();
    let rows = [
        (100, 1, 10),       // tier0
        (50_000, 1, 11),    // tier1
        (2_000_000, 2, 12), // tier2
        (50, 2, 10),        // tier0
        (5_000, 3, 13),     // tier0
        (999_999, 3, 11),   // tier1
    ];
    let batch = make_q15_batch(&rows, 5_000_000_000);
    push_batch(&mut task, batch, 1).await;
    // 未到 30m 边界 → 无产出
    assert!(alert_rx.try_recv().is_err(), "窗口未关闭不应产出");
    task.flush().await;
    let alert = take_alert(&mut alert_rx);
    // total=6, r1=3, r2=2, r3=1; bidder total=3, r1=3, r2=2, r3=1;
    // auction total=4, r1=2, r2=1, r3=1
    assert_eq!(
        field_str(&alert, "detail"),
        "6 3 2 1 3 3 2 1 4 2 1 1",
        "Q15 stats 12 度量（真实任务路径）"
    );
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
        timeout_scan_interval: Duration::from_secs(1),
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
        timeout_scan_interval: Duration::from_secs(1),
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

#[tokio::test]
async fn stats_scan_timeouts_closes_tail_window() {
    // 墙钟兜底（对齐 CEP scan_timeouts）: 数据跨度未达窗口边界时, 周期性扫描用
    // wall elapsed 推进 watermark 关闭尾部窗口; 关闭后清空状态, 空窗口不得循环产出。
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    // ts=9.999s: watermark 接近但未达 10s 边界 → 事件推进不 close
    push_batch(&mut task, make_batch(&["10.0.0.1"], 9_999_000_000), 1).await;
    assert!(alert_rx.try_recv().is_err(), "事件推进未到边界不应产出");

    // 等待 20ms 后 scan → effective watermark 越过边界 → 关闭尾部窗口
    tokio::time::sleep(Duration::from_millis(20)).await;
    task.scan_timeouts().await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "detail"), "1 1 1", "尾部窗口产出");

    // 再次 scan: 窗口已清空 → 无产出（不得每 tick 关闭空窗口循环 emit）
    tokio::time::sleep(Duration::from_millis(20)).await;
    task.scan_timeouts().await;
    assert!(alert_rx.try_recv().is_err(), "空窗口不得循环产出");
}

// ---------------------------------------------------------------------------
// P2 复合键分组: StatsTask 每桶一条 alert + 键字段注入 yield
// ---------------------------------------------------------------------------

/// Q12 形状任务: group by (b.bidder) { count } —— 每桶一条 alert, detail 含 bidder。
fn make_q12_task() -> (StatsTask, mpsc::Receiver<crate::alert_task::AlertBatch>) {
    make_q12_task_sharded(None, 1)
}

/// 分片版 Q12 任务（P2）: `shard_index` 决定本片拉/收的行子集。
fn make_q12_task_sharded(
    shard_index: Option<usize>,
    shard_count: usize,
) -> (StatsTask, mpsc::Receiver<crate::alert_task::AlertBatch>) {
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let schema = test_schema(); // sip + event_time
    let win = Arc::new(Window::new(
        WindowParams {
            name: "bid_events".into(),
            schema: schema.clone(),
            time_col_index: Some(1),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        super::tests::test_window_config(usize::MAX),
    ));
    let plan = StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
        keys: vec![Expr::Field(FieldRef::Qualified("b".into(), "sip".into()))],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![StatsMeasurePlan {
            label: "bid_count".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Count,
            field: None,
            arg: None,
        }],
        tracked_bind_fields: HashMap::new(),
    };
    // detail = fmt("{} {}", b.sip, stat.value(final(bid_count)))
    let rp = wf_lang::plan::RulePlan {
        name: "q12_stats".into(),
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
        stats_plan: Some(plan.clone()),
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
                value: Expr::FuncCall {
                    qualifier: None,
                    name: "fmt".into(),
                    args: vec![
                        Expr::StringLit("{} {}".into()),
                        Expr::Field(FieldRef::Qualified("b".into(), "sip".into())),
                        stat_value("bid_count"),
                    ],
                },
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    };
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
        timeout_scan_interval: Duration::from_secs(1),
        intermediate_targets: std::collections::HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        progress: std::collections::HashMap::new(),
        shard_index,
        shard_count,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx)
}

#[tokio::test]
async fn q12_stats_task_per_bucket_alert_with_key_injected() {
    // group by (b.sip): 每桶一条 alert（同一 close 批量合成一批）, detail 含分组
    // 键值 + 计数; 桶序 = ScopeKey 升序: 10.0.0.1 → 10.0.0.2
    let (mut task, mut alert_rx) = make_q12_task();
    push_batch(
        &mut task,
        make_batch(&["10.0.0.1", "10.0.0.1", "10.0.0.2"], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 2, "2 桶合成一批");
    assert_eq!(field_str(&alerts[0], "detail"), "10.0.0.1 2", "桶 10.0.0.1");
    assert_eq!(field_str(&alerts[1], "detail"), "10.0.0.2 1", "桶 10.0.0.2");
    // 无更多桶
    assert!(alert_rx.try_recv().is_err(), "只有 2 桶");
}

#[tokio::test]
async fn stats_sharded_task_processes_only_own_rows() {
    // P2 分片回归（Blocker 1）: 带 key 任务每片只归并自己的 shard_rows 子集——
    // 否则每片处理全批, 每个键被 N 片各算一遍, close 重复输出 N 倍
    // （Q16 实测 EMIT 10 倍）。模拟 2 片: 片 0 拥有行 {0,1}（key A）,
    // 片 1 拥有行 {2,3}（key B）; 各自产出且互不重复。
    let (mut shard0, mut rx0) = make_q12_task_sharded(Some(0), 2);
    let (mut shard1, mut rx1) = make_q12_task_sharded(Some(1), 2);
    let batch = make_batch(&["A", "A", "B", "B"], 5_000_000_000);
    // 分片广播: RulePush.shard_rows 携带本片行子集（fanout 按键分区）
    for (task, rows) in [(&mut shard0, vec![0u32, 1]), (&mut shard1, vec![2u32, 3])] {
        let push = RulePush {
            window_name: "bid_events".into(),
            events: None,
            batch: Some(Arc::new(batch.clone())),
            materialize_fields: None,
            shard_rows: Some(Arc::new(rows)),
            seq: 1,
        };
        task.process_push(push).await;
    }
    shard0.flush().await;
    shard1.flush().await;
    // 片 0 只产出 key A（不得重复全批的 B）; 片 1 只产出 key B
    let a0 = take_alert(&mut rx0);
    assert_eq!(field_str(&a0, "detail"), "A 2", "片 0 只归并自己的行");
    assert!(rx0.try_recv().is_err(), "片 0 不得产出 B（重复 bug）");
    let a1 = take_alert(&mut rx1);
    assert_eq!(field_str(&a1, "detail"), "B 2", "片 1 只归并自己的行");
    assert!(rx1.try_recv().is_err(), "片 1 不得产出 A（重复 bug）");
}

#[tokio::test]
async fn stats_task_segments_keyed_batch_across_window_boundary() {
    // 回归（批跨窗口边界, Q12 根因）: 同一批的行按事件时间归属各自窗口。
    // 旧整批归并: batch max=11s → 先推进到窗口 2, 4 行全进窗口 2 → 单条 "A 4";
    // 新切段: 窗口 1 [0,10) A=2, 窗口 2 [10,20) A=2 → 两条 "A 2"。
    let (mut task, mut alert_rx) = make_q12_task();
    let batch = make_ts_batch(&[
        ("A", 9_000_000_000),
        ("A", 9_500_000_000),
        ("A", 10_500_000_000),
        ("A", 11_000_000_000),
    ]);
    push_batch(&mut task, batch, 1).await;
    task.flush().await;
    let a1 = take_alert(&mut alert_rx);
    assert_eq!(field_str(&a1, "detail"), "A 2", "窗口 1 [0,10): A=2");
    let a2 = take_alert(&mut alert_rx);
    assert_eq!(field_str(&a2, "detail"), "A 2", "窗口 2 [10,20): A=2");
    assert!(
        alert_rx.try_recv().is_err(),
        "只有 2 窗（不得整批归并成 A 4）"
    );
}

#[tokio::test]
async fn stats_task_segments_empty_key_batch_across_window_boundary() {
    // 空键同样按窗口切段: 窗口 1 收 9s/9.5s 两行, 窗口 2 收 10.5s 一行。
    // 旧整批归并: 窗口 1 空（无产出）, 3 行全进窗口 2 → "3 1 2" 单条。
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    let batch = make_ts_batch(&[
        ("10.0.0.1", 9_000_000_000),
        ("10.0.0.1", 9_500_000_000),
        ("10.0.0.2", 10_500_000_000),
    ]);
    push_batch(&mut task, batch, 1).await;
    task.flush().await;
    let a1 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a1, "detail"),
        "2 2 1",
        "窗口 1 [0,10): total=2 r1=2 uniq=1"
    );
    let a2 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a2, "detail"),
        "1 0 1",
        "窗口 2 [10,20): total=1 r1=0 uniq=1"
    );
    assert!(alert_rx.try_recv().is_err(), "只有 2 窗");
}

#[tokio::test]
async fn stats_task_empty_key_jump_emits_no_zero_windows() {
    // 空键单批跨多窗跳变（5s → 35s, 10s 窗）: 窗口 1 收 5s 行, 窗口 4 收 35s 行,
    // 空窗口 2/3 不得产出。旧整批归并: 先按 max=35s 推进 → 窗口 1 空窗 close
    // 产出全 0 alert（空键预建 Empty 桶）, 两行全进窗口 4。
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    let batch = make_ts_batch(&[("10.0.0.1", 5_000_000_000), ("10.0.0.2", 35_000_000_000)]);
    push_batch(&mut task, batch, 1).await;
    task.flush().await;
    let a1 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a1, "detail"),
        "1 1 1",
        "窗口 1 [0,10): total=1 r1=1 uniq=1"
    );
    let a2 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a2, "detail"),
        "1 0 1",
        "窗口 4 [30,40): total=1 r1=0 uniq=1"
    );
    assert!(
        alert_rx.try_recv().is_err(),
        "只有 2 窗——空窗/跳变不得产出全 0 alert"
    );
}
