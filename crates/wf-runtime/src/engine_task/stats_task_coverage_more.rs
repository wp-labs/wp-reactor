//! stats_task.rs 第二轮深度补测（注册于 stats_task.rs 内）。
//!
//! 覆盖点（第一轮 `stats_task_coverage` 之外）:
//! - 主循环: `run_stats_task` 的 push / pull 两种数据路径 + cancel / EOS /
//!   通道关闭退出分支。
//! - `process_batch_from` 非固定窗口（sliding/session）整批退化归并分支。
//! - `emit_close_record`: 中间流目标丢弃 / 未达标（event_ok=false）无输出。
//! - `dispatch_columns`: 通道 Full（回退阻塞投递）/ Closed（丢弃）/ 无 sink。
use std::sync::Arc;

use super::*;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use arrow::array::{Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use tokio_util::sync::CancellationToken;
use wf_engine::alert::{AlertOrigin, OutputRecord};
use wf_engine::match_engine::{RuleExecutor, StatsExecutor, Value};
use wf_engine::pipe::PipeRegistry;
use wf_engine::window::{Router, Window, WindowParams, WindowRegistry};
use wf_lang::ast::Expr;
use wf_lang::plan::{
    BindPlan, EntityPlan, MatchPlan, ScorePlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan,
    WindowSpec, YieldField, YieldPlan,
};

use crate::alert_task::{AlertBatch, SinkFanout};
use crate::engine_task::window_lookup::RegistryLookup;

use super::super::tests::test_window_config;

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

fn stats_plan() -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
        keys: vec![],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![StatsMeasurePlan {
            label: "cnt".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Count,
            field: None,
            arg: None,
        }],
        tracked_bind_fields: HashMap::new(),
    }
}

fn stats_rule_plan() -> wf_lang::plan::RulePlan {
    wf_lang::plan::RulePlan {
        name: "stats_more_rule".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: vec![],
        match_plan: MatchPlan {
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
            tracked_bind_aliases: HashSet::new(),
            tracked_bind_fields: HashMap::new(),
            tracked_plain_fields: HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(stats_plan()),
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "b".into(),
                "auction".into(),
            )),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "id".into(),
                value: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                    "b".into(),
                    "auction".into(),
                )),
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

fn make_config(
    window_sources: Vec<WindowSource>,
    push_rx: Option<mpsc::Receiver<RulePush>>,
    eos_tx: &watch::Sender<u64>,
    intermediate_targets: HashSet<String>,
) -> (StatsTaskConfig, CancellationToken) {
    let config = StatsTaskConfig {
        stats: StatsExecutor::with_row_fields(stats_plan(), None),
        executor: RuleExecutor::new(stats_rule_plan()),
        window_sources,
        sink_fanout: SinkFanout::closed(),
        cancel: CancellationToken::new(),
        router: Arc::new(Router::new(
            WindowRegistry::build(vec![]).expect("registry"),
        )),
        metrics: None,
        time_field: Some("event_time".into()),
        timeout_scan_interval: Duration::from_secs(60),
        intermediate_targets,
        pipe_registry: Arc::new(PipeRegistry::new()),
        eos_flush: eos_tx.subscribe(),
        push_rx,
        progress: HashMap::new(),
        shard_index: None,
        shard_count: 1,
        merge_rx: None,
        merge_tx: None,
    };
    let cancel = config.cancel.clone();
    (config, cancel)
}

fn time_batch(times: &[i64]) -> arrow::record_batch::RecordBatch {
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "event_time",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        true,
    )]));
    arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampNanosecondArray::from(times.to_vec()))],
    )
    .expect("batch")
}

fn make_window() -> (Arc<Window>, Arc<tokio::sync::Notify>) {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let mut cfg = test_window_config(usize::MAX);
    cfg.name = "bid_events".to_string();
    let win = Window::new(
        WindowParams {
            name: "bid_events".into(),
            schema,
            time_col_index: Some(1),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        cfg,
    );
    (Arc::new(win), Arc::new(tokio::sync::Notify::new()))
}

/// 与 `make_window` 的 schema（auction + event_time）匹配的批次。
fn window_batch(times: &[i64]) -> arrow::record_batch::RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
        ],
    )
    .expect("batch")
}

fn record_with(target: &str) -> OutputRecord {
    OutputRecord {
        wfx_id: "id".into(),
        rule_name: "stats_more_rule".into(),
        score: 10.0,
        entity_type: "digit".into(),
        entity_id: "7".to_string(),
        origin: AlertOrigin::Event,
        fired_at: "2026-01-01T00:00:00Z".to_string(),
        emit_time: "2026-01-01T00:00:00Z".into(),
        matched_rows: Vec::new(),
        summary: "".into(),
        yield_target: target.into(),
        yield_fields: vec![(Arc::from("id"), Value::Number(7.0))],
        yield_field_types: Vec::new().into(),
        event_time_nanos: 100,
        machine_id: Arc::from(""),
        scope_key: "".into(),
    }
}

// ---------------------------------------------------------------------------
// run_stats_task 主循环
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_stats_task_push_loop_exits_on_cancel() {
    let (tx, rx) = mpsc::channel::<RulePush>(8);
    let (eos_tx, _) = watch::channel(0u64);
    let (config, cancel) = make_config(vec![], Some(rx), &eos_tx, HashSet::new());
    let run = tokio::spawn(run_stats_task(config));

    tx.send(RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: Some(Arc::new(time_batch(&[1_000, 2_000]))),
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    })
    .await
    .expect("send");

    cancel.cancel();
    run.await
        .expect("task panicked")
        .expect("run_stats_task push loop exits cleanly on cancel");
}

#[tokio::test]
async fn run_stats_task_push_loop_eos_flush_keeps_running() {
    let (tx, rx) = mpsc::channel::<RulePush>(8);
    let (eos_tx, _) = watch::channel(0u64);
    let (config, cancel) = make_config(vec![], Some(rx), &eos_tx, HashSet::new());
    let run = tokio::spawn(run_stats_task(config));

    // EOS → flush 尾部窗口后继续运行（不退出）。
    eos_tx.send(1).expect("eos");
    // EOS 之后仍可消费 push。
    tx.send(RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: Some(Arc::new(time_batch(&[1_000]))),
        materialize_fields: None,
        seq: 2,
        shard_rows: None,
    })
    .await
    .expect("send");

    cancel.cancel();
    run.await
        .expect("task panicked")
        .expect("push loop survives EOS until cancel");
}

#[tokio::test]
async fn run_stats_task_push_loop_channel_close_exits() {
    let (tx, rx) = mpsc::channel::<RulePush>(8);
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(vec![], Some(rx), &eos_tx, HashSet::new());
    let run = tokio::spawn(run_stats_task(config));

    drop(tx); // 所有生产者退出 → 通道关闭 → drain + flush + 退出
    run.await
        .expect("task panicked")
        .expect("push loop exits when the channel closes");
}

#[tokio::test]
async fn run_stats_task_pull_loop_exits_on_cancel() {
    let (eos_tx, _) = watch::channel(0u64);
    let (config, cancel) = make_config(vec![], None, &eos_tx, HashSet::new());
    let run = tokio::spawn(run_stats_task(config));
    cancel.cancel();
    run.await
        .expect("task panicked")
        .expect("pull loop exits cleanly on cancel");
}

#[tokio::test]
async fn run_stats_task_pull_loop_with_window_exits_on_cancel() {
    let (win, notify) = make_window();
    let sources = vec![WindowSource {
        window_name: "bid_events".into(),
        window: win,
        notify,
        aliases: vec!["b".into()],
    }];
    let (eos_tx, _) = watch::channel(0u64);
    let (config, cancel) = make_config(sources, None, &eos_tx, HashSet::new());
    let run = tokio::spawn(run_stats_task(config));
    cancel.cancel();
    run.await
        .expect("task panicked")
        .expect("pull loop with window exits cleanly on cancel");
}

// ---------------------------------------------------------------------------
// process_batch_from — 非固定窗口退化
// ---------------------------------------------------------------------------

#[tokio::test]
async fn process_batch_from_non_fixed_window_accumulates_whole_batch() {
    let (mut task, _cancel) = {
        let (eos_tx, _) = watch::channel(0u64);
        let (config, cancel) = make_config(vec![], None, &eos_tx, HashSet::new());
        (StatsTask::new(config).0, cancel)
    };
    // 计划改为 sliding → window_dur_nanos None → 单段整批归并，不推进窗口。
    let mut plan = stats_plan();
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(10));
    task.stats = StatsExecutor::with_row_fields(plan, None);
    task.process_batch_from("bid_events", &time_batch(&[1_000, 2_000, 3_000]), None)
        .await;
    assert_eq!(task.window_start, None);
    assert_eq!(task.last_watermark, 3_000);
}

// ---------------------------------------------------------------------------
// dispatch_columns 分支
// ---------------------------------------------------------------------------

fn column_batch() -> wf_engine::alert::AlertColumnBatch {
    let mut builder = wf_engine::alert::AlertColumnBuilder::new(Arc::from("alerts"));
    builder
        .append_record(&record_with("alerts"))
        .expect("append");
    builder.finish()
}

#[tokio::test]
async fn dispatch_columns_full_channel_backpressure() {
    let (tx, mut rx) = mpsc::channel::<AlertBatch>(1);
    tx.send(AlertBatch::Rows(Arc::new(vec![])))
        .await
        .expect("prefill");
    let drainer = tokio::spawn(async move {
        for _ in 0..2 {
            assert!(rx.recv().await.is_some(), "drainer expected a batch");
        }
    });
    let mut cache = HashMap::new();
    let groups = Arc::new(vec![(0usize, Arc::new(vec![tx]))]);
    cache.insert("alerts".to_string(), groups);
    let fanout = SinkFanout::from_resolved(cache);
    let (mut task, _cancel) = {
        let (eos_tx, _) = watch::channel(0u64);
        let (config, _cancel) = make_config(vec![], None, &eos_tx, HashSet::new());
        StatsTask::new(config)
    };
    task.sink_fanout = fanout;
    task.dispatch_columns("alerts", column_batch()).await;
    drainer.await.expect("drainer finished");
}

#[tokio::test]
async fn dispatch_columns_closed_channel_drops() {
    let (tx, rx) = mpsc::channel::<AlertBatch>(4);
    drop(rx);
    let mut cache = HashMap::new();
    let groups = Arc::new(vec![(0usize, Arc::new(vec![tx]))]);
    cache.insert("alerts".to_string(), groups);
    let fanout = SinkFanout::from_resolved(cache);
    let (mut task, _cancel) = {
        let (eos_tx, _) = watch::channel(0u64);
        let (config, _cancel) = make_config(vec![], None, &eos_tx, HashSet::new());
        StatsTask::new(config)
    };
    task.sink_fanout = fanout;
    // Closed → 丢弃分支，不 panic。
    task.dispatch_columns("alerts", column_batch()).await;
}

#[tokio::test]
async fn dispatch_columns_no_sink_warns() {
    let (task, _cancel) = {
        let (eos_tx, _) = watch::channel(0u64);
        let (config, _cancel) = make_config(vec![], None, &eos_tx, HashSet::new());
        StatsTask::new(config)
    };
    // 无 sink → warn + 计数，不 panic。
    task.dispatch_columns("alerts", column_batch()).await;
}

// ---------------------------------------------------------------------------
// emit_close_record 分支
// ---------------------------------------------------------------------------

#[tokio::test]
async fn emit_close_record_intermediate_target_dropped() {
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) =
        make_config(vec![], None, &eos_tx, HashSet::from(["alerts".to_string()]));
    let (task, _cancel) = StatsTask::new(config);
    let close = build_stats_close_output(
        "stats_more_rule",
        &[1.0],
        &["cnt".to_string()],
        &[],
        None,
        100,
        110,
        &ScopeKey::Empty,
        &[],
    );
    let lookup = RegistryLookup::new(&task.router);
    let mut builders: HashMap<Arc<str>, AlertColumnBuilder> = HashMap::new();
    task.emit_close_record(&close, &lookup, &mut builders).await;
    // 中间流目标 → 丢弃（不进入 builders）。
    assert!(builders.is_empty());
}

#[tokio::test]
async fn emit_close_record_unqualified_returns_none() {
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(vec![], None, &eos_tx, HashSet::new());
    let (task, _cancel) = StatsTask::new(config);
    let mut close = build_stats_close_output(
        "stats_more_rule",
        &[1.0],
        &["cnt".to_string()],
        &[],
        None,
        100,
        110,
        &ScopeKey::Empty,
        &[],
    );
    // And 模式 + event_ok=false → 未达标 → Ok(None)。
    close.event_ok = false;
    let lookup = RegistryLookup::new(&task.router);
    let mut builders: HashMap<Arc<str>, AlertColumnBuilder> = HashMap::new();
    task.emit_close_record(&close, &lookup, &mut builders).await;
    assert!(builders.is_empty());
}

// ---------------------------------------------------------------------------
// 进度槽 ack（pull 路径）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pull_and_process_acks_read_position() {
    let (win, notify) = make_window();
    let sources = vec![WindowSource {
        window_name: "bid_events".into(),
        window: Arc::clone(&win),
        notify,
        aliases: vec!["b".into()],
    }];
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(sources, None, &eos_tx, HashSet::new());
    let (mut task, _cancel) = StatsTask::new(config);
    let slot = Arc::new(AtomicU64::new(0));
    task.progress
        .insert("bid_events".to_string(), Arc::clone(&slot));
    win.append(window_batch(&[1_000, 2_000])).expect("append");
    task.pull_and_process().await;
    // 单次 append → 一个 batch, next_seq = 1。
    assert_eq!(slot.load(std::sync::atomic::Ordering::Relaxed), 1);
}
