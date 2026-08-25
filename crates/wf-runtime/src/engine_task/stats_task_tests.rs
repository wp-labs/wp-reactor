//! StatsTask 接线测试（P1 步骤④c）: push 路径端到端——批次投递 → 列式归并 →
//! 固定窗口 close → alert; 窗口语义（单窗口/flush/跨窗口跳变）; ack floor;
//! 非列式 where 回退行式。
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

/// Q18/Q19 形状任务（P4 last/top）: 用 price/bidder/auction/event_time schema。
/// 返回 (task, alert_rx); detail 由调用方给 Expr 决定。
fn make_ranked_task(
    keys: Vec<Expr>,
    measures: Vec<StatsMeasurePlan>,
    detail: Expr,
) -> (StatsTask, mpsc::Receiver<crate::alert_task::AlertBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let win = Arc::new(Window::new(
        WindowParams {
            name: "bid_events".into(),
            schema: schema.clone(),
            time_col_index: Some(3),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        super::tests::test_window_config(usize::MAX),
    ));
    let plan = StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
        keys,
        output_shape: StatsOutputShapePlan::Rows,
        measures: measures.clone(),
        tracked_bind_fields: HashMap::new(),
    };
    let rp = wf_lang::plan::RulePlan {
        name: "ranked_stats".into(),
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
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(plan.clone()),
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
                },
                YieldField {
                    name: "detail".into(),
                    value: detail,
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    };
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    // last/top 行字段提取子集（P5: 生产经 spawn 恒有; 测试用全 schema 字段）——
    // 无子集时列数组列序不定, 任务层注入需要列名。
    let row_subset: Option<std::sync::Arc<std::collections::HashSet<String>>> =
        Some(std::sync::Arc::new(
            ["price", "bidder", "auction", "event_time"]
                .into_iter()
                .map(String::from)
                .collect(),
        ));
    let config = StatsTaskConfig {
        stats: StatsExecutor::with_row_fields(plan.clone(), row_subset),
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
        merge_rx: None,
        merge_tx: None,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx)
}

/// 带 price/bidder/auction 的批次（q18/q19 任务测试用）。
fn make_bid_batch(rows: &[(i64, i64, i64)], ts: i64) -> RecordBatch {
    use arrow::array::Int64Array;
    let n = rows.len();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("auction", DataType::Int64, true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ])),
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
    let rp = q15_rule_plan();
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
        merge_rx: None,
        merge_tx: None,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx)
}

/// q15 stats 规则计划（12 度量 + fmt detail; 与 make_q15_task 共享, 分片测试复用）。
fn q15_rule_plan() -> wf_lang::plan::RulePlan {
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
    wf_lang::plan::RulePlan {
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
            trigger_event_needed: false,
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
    }
}

#[tokio::test]
async fn q15_stats_task_12_measures_matches_cep_anchor() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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

/// 输入分区分片（2026-08-24 q15）: 2 片（协调片 shard 0 + 发送片 shard 1）按
/// 行号奇偶分区各归并一半行; flush 时发送片发 raw partial、协调片收齐归并后
/// 统一 emit——输出与单实例锚点**字节一致**, 且非协调片不产出。
#[tokio::test]
async fn q15_input_shard_merge_emits_single_equivalent() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (alert_tx0, mut alert_rx0) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (alert_tx1, mut alert_rx1) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (merge_tx, merge_rx) = mpsc::channel::<StatsPartial>(8);

    let mk = |shard_idx: usize,
              alert_tx: mpsc::Sender<crate::alert_task::AlertBatch>,
              merge_rx: Option<mpsc::Receiver<StatsPartial>>,
              merge_tx: Option<mpsc::Sender<StatsPartial>>|
     -> StatsTask {
        let config = StatsTaskConfig {
            stats: StatsExecutor::new(q15_plan()),
            executor: RuleExecutor::new(q15_rule_plan()),
            window_sources: vec![],
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
            shard_index: Some(shard_idx),
            shard_count: 2,
            merge_rx,
            merge_tx,
        };
        let (task, _cancel) = StatsTask::new(config);
        task
    };
    let mut coord = mk(0, alert_tx0, Some(merge_rx), None);
    let mut shard1 = mk(1, alert_tx1, None, Some(merge_tx));

    let rows = [
        (100, 1, 10),       // tier0
        (50_000, 1, 11),    // tier1
        (2_000_000, 2, 12), // tier2
        (50, 2, 10),        // tier0
        (5_000, 3, 13),     // tier0
        (999_999, 3, 11),   // tier1
    ];
    let batch = make_q15_batch(&rows, 5_000_000_000);
    // 输入分区（行号奇偶）: 片 0 = 行 0/2/4, 片 1 = 行 1/3/5。
    let push = |shard_rows: Arc<Vec<u32>>| RulePush {
        window_name: "bid_events".into(),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: Some(shard_rows),
        seq: 1,
    };
    coord.process_push(push(Arc::new(vec![0u32, 2, 4]))).await;
    shard1.process_push(push(Arc::new(vec![1u32, 3, 5]))).await;
    assert!(alert_rx0.try_recv().is_err(), "窗口未关闭不应产出");
    assert!(alert_rx1.try_recv().is_err(), "非协调片不应产出");

    // 并发 flush: 发送片发 raw partial, 协调片收齐归并后统一 emit。
    tokio::join!(coord.flush(), shard1.flush());

    let alert = take_alert(&mut alert_rx0);
    assert_eq!(
        field_str(&alert, "detail"),
        "6 3 2 1 3 3 2 1 4 2 1 1",
        "2 片输入分区归并输出必须与单实例锚点一致"
    );
    assert!(alert_rx1.try_recv().is_err(), "非协调片不得 emit");
}

/// 构造一个输入分片 stats 任务（make_stats_plan 10s 窗口形状）:
/// `shard_idx` + 归并通道角色 + alert 发送端。window_sources 留空（push 路径
/// 不经窗口 log; process_push 直接用 push 的 window_name）。
fn make_stats_shard_task(
    shard_idx: usize,
    alert_tx: mpsc::Sender<crate::alert_task::AlertBatch>,
    merge_rx: Option<mpsc::Receiver<StatsPartial>>,
    merge_tx: Option<mpsc::Sender<StatsPartial>>,
) -> StatsTask {
    let config = StatsTaskConfig {
        stats: StatsExecutor::new(make_stats_plan()),
        executor: RuleExecutor::new(make_stats_rule_plan()),
        window_sources: vec![],
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
        shard_index: Some(shard_idx),
        shard_count: 2,
        merge_rx,
        merge_tx,
    };
    let (task, _cancel) = StatsTask::new(config);
    task
}

/// 多窗口 advance 归并（2026-08-24）: 批次 2 越过 10s 窗口边界 → 协调片
/// mid-stream close 窗口 1（收齐发送片 partial 归并后 emit）; flush 收窗口 2。
/// 两窗口输出与单实例（全批）逐字节一致。
#[tokio::test]
async fn q15_input_shard_merge_multi_window_matches_single() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (alert_tx0, mut alert_rx0) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (alert_tx1, mut alert_rx1) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (merge_tx, merge_rx) = mpsc::channel::<StatsPartial>(8);
    let mut coord = make_stats_shard_task(0, alert_tx0, Some(merge_rx), None);
    let mut shard1 = make_stats_shard_task(1, alert_tx1, None, Some(merge_tx));

    let push = |batch: RecordBatch, shard_rows: Arc<Vec<u32>>| RulePush {
        window_name: "bid_events".into(),
        events: None,
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: Some(shard_rows),
        seq: 1,
    };
    // 窗口 1 [0,10s): 4 行（sip a/b/c/d, ts=5s）。行号分区: 片 0 = 行 0/2,
    // 片 1 = 行 1/3。
    let b1 = make_ts_batch(&[
        ("a", 5_000_000_000),
        ("b", 5_000_000_000),
        ("c", 5_000_000_000),
        ("d", 5_000_000_000),
    ]);
    tokio::join!(
        coord.process_push(push(b1.clone(), Arc::new(vec![0u32, 2]))),
        shard1.process_push(push(b1, Arc::new(vec![1u32, 3])))
    );

    // 窗口 2 [10s,20s): 越过边界 → close 窗口 1（协调片 close 时收齐发送片
    // partial——必须并发执行, 顺序 await 会死锁: 协调片 close 阻塞在 recv,
    // 发送片 push 还没执行）。
    let b2 = make_ts_batch(&[
        ("a", 15_000_000_000),
        ("b", 15_000_000_000),
        ("e", 15_000_000_000),
        ("f", 15_000_000_000),
    ]);
    tokio::join!(
        coord.process_push(push(b2.clone(), Arc::new(vec![0u32, 2]))),
        shard1.process_push(push(b2, Arc::new(vec![1u32, 3])))
    );

    // 协调片 mid-stream close 窗口 1: total=4, r1=4（全是 10.0.0.1? 不——sip
    // 是 a/b/c/d, where sip=="10.0.0.1" 全 false）→ r1=0; uniq=4。
    let alert = take_alert(&mut alert_rx0);
    assert_eq!(
        field_str(&alert, "detail"),
        "4 0 4",
        "窗口 1 归并（total/r1/uniq）"
    );
    assert!(alert_rx1.try_recv().is_err(), "非协调片不得 emit");

    // flush 收窗口 2: total=4, r1=0, uniq=4（a/b/e/f）。
    tokio::join!(coord.flush(), shard1.flush());
    let alert = take_alert(&mut alert_rx0);
    assert_eq!(field_str(&alert, "detail"), "4 0 4", "窗口 2 归并");
}

/// 非协调片 0 事件（无窗口）时 flush 发 sentinel——协调片不 panic 不死锁,
/// 输出 = 协调片自己的数据（空 partial 合并无效果）。
#[tokio::test]
async fn q15_input_shard_empty_shard_flush_sentinel_no_deadlock() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (alert_tx0, mut alert_rx0) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (alert_tx1, _alert_rx1) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (merge_tx, merge_rx) = mpsc::channel::<StatsPartial>(8);
    let mut coord = make_stats_shard_task(0, alert_tx0, Some(merge_rx), None);
    let mut shard1 = make_stats_shard_task(1, alert_tx1, None, Some(merge_tx));

    // 只有协调片有数据; 发送片 0 事件（不 push）。
    let b1 = make_ts_batch(&[("a", 5_000_000_000), ("b", 5_000_000_000)]);
    let push = |batch: RecordBatch| RulePush {
        window_name: "bid_events".into(),
        events: None,
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: Some(Arc::new(vec![0u32, 1])),
        seq: 1,
    };
    coord.process_push(push(b1)).await;

    // 并发 flush: 发送片无窗口 → 发 (MIN, MIN) 空 sentinel; 协调片收齐后
    // merge（空）→ 输出 = 自己的 2 行。
    tokio::join!(coord.flush(), shard1.flush());
    let alert = take_alert(&mut alert_rx0);
    assert_eq!(
        field_str(&alert, "detail"),
        "2 0 2",
        "空片 sentinel 合并无效果"
    );
}

/// 协调片 recv None（某片已退出 / tx drop）不 panic——warn 后放弃该窗口
/// 剩余合并, 输出 = 已收到的 partial + 自己（2026-08-24 review 修复）。
#[tokio::test]
async fn q15_input_shard_partial_sender_exit_no_panic() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (alert_tx0, mut alert_rx0) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (merge_tx, merge_rx) = mpsc::channel::<StatsPartial>(8);
    let mut coord = make_stats_shard_task(0, alert_tx0, Some(merge_rx), None);

    // 发送片先退出（tx drop）——协调片 flush 时 recv None。
    drop(merge_tx);

    let b1 = make_ts_batch(&[("a", 5_000_000_000), ("b", 5_000_000_000)]);
    let push = RulePush {
        window_name: "bid_events".into(),
        events: None,
        batch: Some(Arc::new(b1)),
        materialize_fields: None,
        shard_rows: Some(Arc::new(vec![0u32, 1])),
        seq: 1,
    };
    coord.process_push(push).await;

    coord.flush().await; // 不得 panic
    let alert = take_alert(&mut alert_rx0);
    assert_eq!(
        field_str(&alert, "detail"),
        "2 0 2",
        "片退出后仅协调片自己的数据"
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
        merge_rx: None,
        merge_tx: None,
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
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
        merge_rx: None,
        merge_tx: None,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx, progress)
}

#[tokio::test]
async fn stats_scan_timeouts_closes_tail_window() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
            trigger_event_needed: false,
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
        merge_rx: None,
        merge_tx: None,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx)
}

#[tokio::test]
async fn q12_stats_task_per_bucket_alert_with_key_injected() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
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

// ---------------------------------------------------------------------------
// P4 last/top 任务接线（Q18/Q19）: rich close 每桶多条目 + 行字段注入 yield
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// perf-diag cuts（2026-08-25 补 stats 缺口）: 与 rule_task 对齐——cut_rules
// 归并直通（空窗无输出）, cut_output 输出直通（归并正常但 alert 被切）。
// ---------------------------------------------------------------------------

/// q19 形状任务（top + fmt detail, 门控通过走列式 close）。
fn make_q19_cut_task() -> (StatsTask, mpsc::Receiver<crate::alert_task::AlertBatch>) {
    let detail = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("{} {}".into()),
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
        ],
    };
    make_ranked_task(
        vec![Expr::Field(FieldRef::Qualified(
            "b".into(),
            "auction".into(),
        ))],
        vec![StatsMeasurePlan {
            label: "top_price".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Top,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: Some(2),
        }],
        detail,
    )
}

#[tokio::test]
async fn stats_task_perf_cut_rules_floor_no_emit() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // floor 档（cut_rules）: 归并直通 → 窗口无事件 → 空窗不产出（无 EMIT）。
    // 恢复后同一数据正常归并 + 输出。全局门控跨 await 持锁（PERF_CUT_SERIAL）——
    // 否则并行测试期间其它 stats 测试被切（2026-08-25 实测干扰）。
    crate::perf_diag::set_perf_cuts(true, false, false);
    let (mut task, mut alert_rx) = make_q19_cut_task();
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    assert!(alert_rx.try_recv().is_err(), "cut_rules: 无归并 → 空窗无输出");
    crate::perf_diag::reset_perf_diag();

    // 恢复: 同数据重推 → 正常产出（窗口已重置, 重新开窗）。
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1)], 5_000_000_000),
        2,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 2, "恢复后 top-2 两条");
}

#[tokio::test]
async fn stats_task_perf_cut_output_keeps_accumulate_no_emit() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // full 档的对照: cut_output 只切输出链——归并照常（窗口 close 状态正确
    // 重置）, alert 不投递。恢复后正常输出。全局门控跨 await 持锁。
    crate::perf_diag::set_perf_cuts(false, true, false);
    let (mut task, mut alert_rx) = make_q19_cut_task();
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    assert!(alert_rx.try_recv().is_err(), "cut_output: 输出链直通");
    crate::perf_diag::reset_perf_diag();

    // 恢复: 新窗口数据正常产出（前窗已被 cut_output 正确 close 重置——若泄漏
    // 会污染本窗输出）。
    push_batch(
        &mut task,
        make_bid_batch(&[(150, 1, 1), (250, 2, 1)], 11_000_000_000),
        2,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 2, "恢复后 top-2 两条（前窗无泄漏）");
}

#[tokio::test]
async fn q18_stats_task_last_bid_fields_injected() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // Q18 形状: group by (bidder, auction), last(price) —— 每键一条 alert,
    // detail 读最后一条 bid 的 price（行字段经 field_values 注入 yield 的 b.price）。
    let detail = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("{} {}".into()),
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
        ],
    };
    let (mut task, mut alert_rx) = make_ranked_task(
        vec![
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        ],
        vec![StatsMeasurePlan {
            label: "last_price".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Last,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: None,
        }],
        detail,
    );
    // 同一 (bidder=5, auction=1): 两条 bid, price 100 → 200; last = 200
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 5, 1), (200, 5, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 1, "每 (bidder,auction) 一条");
    assert_eq!(
        field_str(&alerts[0], "detail"),
        "5 200",
        "last bid 的 price"
    );
    assert_eq!(field_str(&alerts[0], "id"), "1", "entity = auction");
}

#[tokio::test]
async fn q19_stats_task_top_n_emits_per_entry() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // Q19 形状: group by (auction), top(2, price) —— 每 auction 2 条 alert（rank 序）,
    // detail 读每条目的 bidder + price。
    let detail = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("{} {}".into()),
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
        ],
    };
    let (mut task, mut alert_rx) = make_ranked_task(
        vec![Expr::Field(FieldRef::Qualified(
            "b".into(),
            "auction".into(),
        ))],
        vec![StatsMeasurePlan {
            label: "top_price".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Top,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: Some(2),
        }],
        detail,
    );
    // auction=1 三条 bid: 100, 300, 200 → top-2 = 300(bidder 2), 200(bidder 3)
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1), (200, 3, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 2, "top-2 → 每 auction 2 条 alert");
    assert_eq!(
        field_str(&alerts[0], "detail"),
        "2 300",
        "rank1: bidder 2 price 300"
    );
    assert_eq!(
        field_str(&alerts[1], "detail"),
        "3 200",
        "rank2: bidder 3 price 200"
    );
    assert!(alert_rx.try_recv().is_err(), "只有 2 条");
}

#[tokio::test]
async fn stats_top_zero_emits_nothing() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // top(0, ...) 边界（P4 review 修复）: 无条目 → 整桶不产出（此前虚假产出
    // scalar(0.0) 记录）。
    let detail = Expr::StringLit("x".into());
    let (mut task, mut alert_rx) = make_ranked_task(
        vec![Expr::Field(FieldRef::Qualified(
            "b".into(),
            "auction".into(),
        ))],
        vec![StatsMeasurePlan {
            label: "top_price".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Top,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: Some(0),
        }],
        detail,
    );
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "top(0) 无条目 → 整桶不产出, 无 alert"
    );
}

// ---------------------------------------------------------------------------
// 窗口判定边界（大 case 定位回归教训: 30m 窗 + 非零 BASE 的桶归属必须在测试
// 锁定, 不靠 30M 对拍暴露）:
// 1. 桶起点 = (t/dur)*dur（epoch 对齐, 与事件时间起始值无关）;
// 2. 两窗口各自归属（Q18/Q19 形态）;
// 3. 数据未越边界不产出（10m 数据 EMIT=0 是语义而非 bug）;
// 4. 尾部窗口仅 flush/墙钟关闭。
// ---------------------------------------------------------------------------

const NEXMARK_BASE_NS: i64 = 1_767_225_600_000_000_000; // 2026-01-01T00:00:00Z

#[tokio::test]
async fn stats_task_window_bucket_epoch_aligned_large_ts() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // 真实 nexmark BASE_NS 级大时间戳: ts = BASE+25s, 10s 窗 → 窗口
    // [BASE+20s, BASE+30s), fired_at = BASE+30s —— 桶起点 = (t/dur)*dur,
    // 不是「首事件时间 + dur」（BASE 非 0 时两者的差会暴露）。
    let (mut task, mut alert_rx, _p) = make_stats_task();
    push_batch(
        &mut task,
        make_ts_batch(&[("10.0.0.1", NEXMARK_BASE_NS + 25_000_000_000)]),
        1,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 1);
    assert_eq!(
        field_str(&alerts[0], "__wfu_fired_at"),
        "2026-01-01T00:00:30.000Z",
        "窗口 end = bucket(BASE+25s)+10s = BASE+30s"
    );
}

#[tokio::test]
async fn stats_task_two_windows_large_ts_buckets() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // Q18/Q19 形态: 大时间戳下两窗口各自归属。同一批: BASE+25s（窗 1
    // [BASE+20s,+30s)）与 BASE+32s（窗 2 [BASE+30s,+40s)）; 窗 1 在事件推进中
    // close（32s 越过 30s 边界）, 窗 2 由 flush 收尾。
    let (mut task, mut alert_rx, _p) = make_stats_task();
    push_batch(
        &mut task,
        make_ts_batch(&[
            ("10.0.0.1", NEXMARK_BASE_NS + 25_000_000_000),
            ("10.0.0.2", NEXMARK_BASE_NS + 32_000_000_000),
        ]),
        1,
    )
    .await;
    task.flush().await;
    let a1 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a1, "__wfu_fired_at"),
        "2026-01-01T00:00:30.000Z",
        "窗口 1 end"
    );
    assert_eq!(
        field_str(&a1, "detail"),
        "1 1 1",
        "窗口 1: total=1 r1=1 uniq=1"
    );
    let a2 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a2, "__wfu_fired_at"),
        "2026-01-01T00:00:40.000Z",
        "窗口 2 end"
    );
    assert_eq!(
        field_str(&a2, "detail"),
        "1 0 1",
        "窗口 2: total=1 r1=0 uniq=1"
    );
    assert!(alert_rx.try_recv().is_err(), "只有 2 窗");
}

#[tokio::test]
async fn stats_task_window_not_closed_until_watermark_crosses() {
        let _g = crate::perf_diag::PERF_CUT_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // 数据 max < window_end → 窗口未到点不产出（10m 数据 span < 30m 窗时
    // EMIT=0 的语义: 窗口只在事件时间越过边界 close, 否则等 flush/墙钟兜底）。
    let (mut task, mut alert_rx, _p) = make_stats_task();
    push_batch(&mut task, make_batch(&["10.0.0.1"], 9_000_000_000), 1).await;
    assert!(alert_rx.try_recv().is_err(), "9s 未越 10s 边界不产出");
    // 越过 10s 边界 → 窗口 1 close
    push_batch(&mut task, make_batch(&["10.0.0.2"], 15_000_000_000), 2).await;
    let a1 = take_alert(&mut alert_rx);
    assert_eq!(field_str(&a1, "detail"), "1 1 1", "窗口 1 [0,10)");
    // 尾部窗口 2 由 flush 关闭
    task.flush().await;
    let a2 = take_alert(&mut alert_rx);
    assert_eq!(field_str(&a2, "detail"), "1 0 1", "窗口 2 [10,20)");
    assert!(alert_rx.try_recv().is_err(), "只有 2 窗");
}
