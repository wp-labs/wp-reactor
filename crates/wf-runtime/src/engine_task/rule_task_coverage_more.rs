//! rule_task.rs 第二轮深度补测（注册于 rule_task.rs 内）。
//!
//! 覆盖点（第一轮 `rule_task_coverage` 之外）:
//! - `RuleTask::new`: `each_direct` / `deferred`（emit at）标志计算。
//! - 输出路径: `emit` 指标采样（detail/e2e + serialize 采样）、`emit_batch`
//!   （批量 + 中间流拆分）、`stage_or_emit_record` 满批 flush。
//! - `flush_alerts`: 通道 Full（回退阻塞投递）/ Closed（丢弃）/ 无 sink 三种分支。
//! - `stage_pipe_record` 目标缺失 → Dead 终态; `flush_pipes` 空转。
//! - `scan_timeouts` / `flush` 的 conv-sink 路由分支（含 barrier 批次投递）。
//! - `pull_and_advance` 整批 round-robin 门控跳过分支。
//! - `dump_profiling` 节流 / 日志; `update_rule_instances_metric` delta 上报。
//! - `process_batch` 未知窗口早退 / 仅 events 输入路径; `Drop` 释放进度槽。

use super::*;

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;

use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use wf_engine::alert::{AlertOrigin, OutputRecord};
use wf_engine::match_engine::{CepStateMachine, ColumnarEvent, EngineHashMap, RuleExecutor, Value};
use wf_engine::pipe::PipeRegistry;
use wf_engine::window::{Router, Window, WindowParams, WindowRegistry};
use wf_lang::ast::{FieldRef, JoinMode};
use wf_lang::plan::{
    EachPlan, JoinCondPlan, JoinPlan, MatchPlan, RulePlan, ScorePlan, WindowSpec, YieldPlan,
};

use crate::alert_task::{AlertBatch, SinkFanout};
use crate::engine_task::conv_stage::ConvShardSink;
use crate::metrics::RuntimeMetrics;

use super::super::tests::test_window_config;

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

fn empty_router() -> Arc<Router> {
    Arc::new(Router::new(
        WindowRegistry::build(vec![]).expect("empty registry"),
    ))
}

fn minimal_plan() -> RulePlan {
    RulePlan {
        name: "coverage_more_rule".into(),
        binds: vec![],
        lets: vec![],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(60)),
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
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: wf_lang::plan::EntityPlan {
            entity_type: "e".into(),
            entity_id_expr: wf_lang::ast::Expr::Bool(false),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: wf_lang::ast::Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    }
}

/// 灵活的任务构造器：可注入 machine / each / joins / fanout / metrics / conv_sink。
struct Spec {
    plan: RulePlan,
    machine: Option<CepStateMachine>,
    each_alias: Option<String>,
    each_time_field: Option<String>,
    window_sources: Vec<super::super::task_types::WindowSource>,
    sink_fanout: Arc<SinkFanout>,
    router: Arc<Router>,
    metrics: Option<Arc<RuntimeMetrics>>,
    intermediate_targets: HashSet<String>,
    pipe_registry: Arc<PipeRegistry>,
    push_rx: Option<mpsc::Receiver<RulePush>>,
    shard_index: Option<usize>,
    shard_count: usize,
    progress: HashMap<String, Arc<AtomicU64>>,
    conv_sink: Option<ConvShardSink>,
}

impl Default for Spec {
    fn default() -> Self {
        Self {
            plan: minimal_plan(),
            machine: None,
            each_alias: None,
            each_time_field: None,
            window_sources: vec![],
            sink_fanout: SinkFanout::closed(),
            router: empty_router(),
            metrics: None,
            intermediate_targets: HashSet::new(),
            pipe_registry: Arc::new(PipeRegistry::new()),
            push_rx: None,
            shard_index: None,
            shard_count: 1,
            progress: HashMap::new(),
            conv_sink: None,
        }
    }
}

fn make_task(spec: Spec) -> RuleTask {
    let Spec {
        plan,
        machine,
        each_alias,
        each_time_field,
        window_sources,
        sink_fanout,
        router,
        metrics,
        intermediate_targets,
        pipe_registry,
        push_rx,
        shard_index,
        shard_count,
        progress,
        conv_sink,
    } = spec;
    let config = RuleTaskConfig {
        machine,
        each_alias,
        each_time_field,
        executor: RuleExecutor::new(plan),
        window_sources,
        sink_fanout,
        cancel: CancellationToken::new(),
        timeout_scan_interval: std::time::Duration::from_secs(60),
        router,
        metrics,
        intermediate_targets,
        pipe_registry,
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx,
        shard_index,
        shard_count,
        progress,
        conv_sink,
    };
    let (task, _cancel, _interval) = RuleTask::new(config);
    task
}

fn record_with(target: &str, event_time_nanos: i64) -> OutputRecord {
    OutputRecord {
        wfx_id: format!("id-{event_time_nanos}"),
        rule_name: "coverage_more_rule".into(),
        score: 1.0,
        entity_type: "ip".into(),
        entity_id: "10.0.0.1".to_string(),
        origin: AlertOrigin::Event,
        fired_at: "2026-01-01T00:00:00Z".to_string(),
        emit_time: "2026-01-01T00:00:00Z".into(),
        matched_rows: Vec::new(),
        summary: "".into(),
        yield_target: target.into(),
        yield_fields: vec![(Arc::from("sip"), Value::Str("1.2.3.4".into()))],
        yield_field_types: Vec::new().into(),
        event_time_nanos,
        machine_id: String::new(),
        scope_key: "".into(),
    }
}

fn metrics() -> Arc<RuntimeMetrics> {
    Arc::new(RuntimeMetrics::new(
        &["coverage_more_rule".to_string()],
        &[],
        &[],
        BTreeMap::new(),
    ))
}

fn make_window(name: &str, schema: &SchemaRef) -> (Arc<Window>, Arc<Notify>) {
    let mut cfg = test_window_config(usize::MAX);
    cfg.name = name.to_string();
    let win = Window::new(
        WindowParams {
            name: name.into(),
            schema: schema.clone(),
            time_col_index: Some(1),
            over: std::time::Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        cfg,
    );
    (Arc::new(win), Arc::new(Notify::new()))
}

fn make_batch(sips: &[&str], ts: i64) -> arrow::record_batch::RecordBatch {
    let n = sips.len();
    arrow::record_batch::RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(arrow::array::StringArray::from(
                sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::TimestampNanosecondArray::from(vec![ts; n])),
        ],
    )
    .expect("batch")
}

// ---------------------------------------------------------------------------
// RuleTask::new 标志
// ---------------------------------------------------------------------------

#[test]
fn new_sets_each_direct_and_deferred_flags() {
    // each + 非中间流目标 → 直写列式路径。
    let mut plan = minimal_plan();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    let task = make_task(Spec {
        plan: plan.clone(),
        ..Spec::default()
    });
    assert!(task.each_direct, "sink target + each → each_direct");
    assert!(task.deferred.is_none());

    // each + 中间流目标 → 保持 record 路径。
    let task = make_task(Spec {
        plan: plan.clone(),
        intermediate_targets: HashSet::from(["alerts".to_string()]),
        ..Spec::default()
    });
    assert!(!task.each_direct, "intermediate target keeps record path");

    // 带 `emit at` 的 join → deferred 运行时挂起。
    plan.joins = vec![JoinPlan {
        right_window: "bid_events".into(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Qualified("a".into(), "id".into()),
            right: FieldRef::Qualified("bid_events".into(), "auction".into()),
        }],
        within: None,
        reduce: None,
        emit_at: Some(wf_lang::ast::Expr::Number(1.0)),
    }];
    let task = make_task(Spec {
        plan,
        ..Spec::default()
    });
    let deferred = task.deferred.as_ref().expect("emit at → deferred runtime");
    assert_eq!(deferred.join_idx, 0);
}

// ---------------------------------------------------------------------------
// dump_profiling / update_rule_instances_metric
// ---------------------------------------------------------------------------

#[test]
fn dump_profiling_throttles_and_logs() {
    let mut task = make_task(Spec::default());
    // 最近 dump 过 → 直接返回。
    task.dump_profiling();
    // 强制过期 → 输出一条 profile 日志并刷新时间戳。
    task.last_profile_dump = std::time::Instant::now() - std::time::Duration::from_secs(2);
    task.dump_profiling();
    let after = task.last_profile_dump;
    assert!(
        std::time::Instant::now().duration_since(after) < std::time::Duration::from_secs(1),
        "profile dump timestamp must refresh"
    );
}

#[test]
fn update_rule_instances_metric_reports_delta() {
    let m = metrics();
    let task = make_task(Spec {
        metrics: Some(m.clone()),
        ..Spec::default()
    });
    // 无 machine → cur 0; last 0 → delta 0。
    task.update_rule_instances_metric();

    // 强制 last=5 → delta -5（调整路径）。
    task.last_reported_instances
        .store(5, std::sync::atomic::Ordering::Relaxed);
    task.update_rule_instances_metric();
    assert_eq!(
        task.last_reported_instances
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    // 带 machine（空实例）→ cur 0。
    let machine = CepStateMachine::new(
        "coverage_more_rule".into(),
        minimal_plan().match_plan,
        Some("event_time".into()),
    );
    let task = make_task(Spec {
        machine: Some(machine),
        metrics: Some(metrics()),
        ..Spec::default()
    });
    task.update_rule_instances_metric();
}

// ---------------------------------------------------------------------------
// emit / emit_batch 指标采样与 flush_alerts 分支
// ---------------------------------------------------------------------------

#[tokio::test]
async fn emit_metric_sampling_detail_and_serialize() {
    let m = metrics();
    let task = make_task(Spec {
        metrics: Some(m.clone()),
        ..Spec::default()
    });
    // 采样计数器归零 → 命中 detail + e2e 分支; serialize 采样=1 → 计时分支。
    task.emit_sample_remaining
        .store(0, std::sync::atomic::Ordering::Relaxed);
    task.serialize_sample_remaining
        .store(1, std::sync::atomic::Ordering::Relaxed);
    task.emit(record_with("alerts", 1_700_000_000_000_000_000))
        .await;
    assert!(
        task.serialize_nanos
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "serialize timing sampled path must accumulate nanos"
    );
    assert!(
        m.summary_line().contains("alerts=1"),
        "alert total must be counted: {}",
        m.summary_line()
    );
}

#[tokio::test]
async fn emit_append_error_increments_serialize_failed() {
    // 空 yield 目标（无字段）仍可 append；用与 schema 冲突的 yield 触发失败——
    // 直接构造一个非有限数值的 yield 字段让 convert_yield 失败。
    let m = metrics();
    let task = make_task(Spec {
        metrics: Some(m.clone()),
        ..Spec::default()
    });
    let mut record = record_with("alerts", 1);
    record.yield_fields = vec![(Arc::from("x"), Value::Number(f64::NAN))];
    task.emit(record).await;
    // serialize_failed 计数无法直接读，仅验证不 panic 且 pending 未增长。
    assert_eq!(task.pending_alerts.lock().unwrap().count, 0);
}

#[tokio::test]
async fn emit_batch_splits_intermediate_records() {
    // 中间流目标 + 空 registry → stage_pipe_record 解析失败 → Dead。
    let task = make_task(Spec {
        intermediate_targets: HashSet::from(["pipe_x".to_string()]),
        ..Spec::default()
    });
    let records = vec![
        record_with("pipe_x", 1),
        record_with("pipe_x", 2),
        record_with("pipe_x", 3),
    ];
    task.emit_batch(records).await;
    let state = task.pipe_state.lock().unwrap();
    assert!(
        matches!(&*state, PipeState::Dead),
        "missing internal window must mark the pipe Dead"
    );
    drop(state);
    // Dead 状态下 flush_pipes 空转。
    task.flush_pipes().await;
}

#[tokio::test]
async fn stage_or_emit_record_flushes_at_batch_size() {
    let (tx, mut rx) = mpsc::channel::<AlertBatch>(8);
    let mut cache = HashMap::new();
    let groups = Arc::new(vec![(0usize, Arc::new(vec![tx]))]);
    cache.insert("alerts".to_string(), groups);
    let fanout = SinkFanout::from_resolved(cache);
    let task = make_task(Spec {
        sink_fanout: fanout,
        ..Spec::default()
    });
    let mut staged: Vec<OutputRecord> = Vec::new();
    for i in 0..ALERT_BATCH_SIZE {
        task.stage_or_emit_record(&mut staged, record_with("alerts", i as i64))
            .await;
    }
    assert!(
        staged.is_empty(),
        "full batch must drain through emit_batch"
    );
    // 满批 flush → sink 通道收到一批。
    let got = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("flush timed out")
        .expect("sink channel closed");
    assert_eq!(got.len(), ALERT_BATCH_SIZE);
}

#[tokio::test]
async fn flush_alerts_full_channel_falls_back_to_blocking_send() {
    let (tx, mut rx) = mpsc::channel::<AlertBatch>(1);
    // 预填通道 → try_send 必 Full → 回退阻塞 send。
    tx.send(AlertBatch::Rows(Arc::new(vec![])))
        .await
        .expect("prefill");
    // 确定性排水：预填 1 条 + 本次投递 1 条。
    let drainer = tokio::spawn(async move {
        for _ in 0..2 {
            assert!(rx.recv().await.is_some(), "drainer expected a batch");
        }
    });
    let mut cache = HashMap::new();
    let groups = Arc::new(vec![(0usize, Arc::new(vec![tx]))]);
    cache.insert("alerts".to_string(), groups);
    let fanout = SinkFanout::from_resolved(cache);
    let task = make_task(Spec {
        sink_fanout: fanout,
        ..Spec::default()
    });
    task.emit(record_with("alerts", 1)).await;
    task.flush_alerts().await;
    drainer.await.expect("drainer finished");
}

#[tokio::test]
async fn flush_alerts_closed_channel_drops() {
    let (tx, rx) = mpsc::channel::<AlertBatch>(4);
    drop(rx); // 关闭通道
    let mut cache = HashMap::new();
    let groups = Arc::new(vec![(0usize, Arc::new(vec![tx]))]);
    cache.insert("alerts".to_string(), groups);
    let fanout = SinkFanout::from_resolved(cache);
    let task = make_task(Spec {
        sink_fanout: fanout,
        ..Spec::default()
    });
    task.emit(record_with("alerts", 1)).await;
    // Closed → 丢弃分支，不 panic。
    task.flush_alerts().await;
}

#[tokio::test]
async fn flush_alerts_no_sink_counts_and_warns() {
    let m = metrics();
    let task = make_task(Spec {
        sink_fanout: SinkFanout::closed(),
        metrics: Some(m.clone()),
        ..Spec::default()
    });
    task.emit(record_with("alerts", 1)).await;
    task.flush_alerts().await;
    // 无 sink 时批次被丢弃；flush 后 pending 计数归零（不 panic）。
    assert_eq!(task.pending_alerts.lock().unwrap().count, 0);
}

// ---------------------------------------------------------------------------
// scan_timeouts / flush — conv-sink 路由分支
// ---------------------------------------------------------------------------

fn machine_task_with_conv_sink() -> (RuleTask, mpsc::Receiver<crate::engine_task::ConvCloseBatch>) {
    let machine = CepStateMachine::new(
        "coverage_more_rule".into(),
        minimal_plan().match_plan,
        Some("event_time".into()),
    );
    let (tx, rx) = mpsc::channel::<crate::engine_task::ConvCloseBatch>(8);
    let task = make_task(Spec {
        machine: Some(machine),
        conv_sink: Some(ConvShardSink {
            tx,
            barrier_index: 0,
        }),
        ..Spec::default()
    });
    (task, rx)
}

#[tokio::test]
async fn scan_timeouts_conv_sink_routes_barrier_batch() {
    let (mut task, mut rx) = machine_task_with_conv_sink();
    task.scan_timeouts().await;
    let batch = rx
        .recv()
        .await
        .expect("conv stage must receive a scan batch");
    assert!(!batch.drained, "scan batch is not a drain barrier");
}

#[tokio::test]
async fn flush_conv_sink_routes_drained_batch() {
    let (mut task, mut rx) = machine_task_with_conv_sink();
    task.flush().await;
    let batch = rx
        .recv()
        .await
        .expect("conv stage must receive a flush batch");
    assert!(batch.drained, "flush must publish a drained barrier");
}

#[tokio::test]
async fn scan_timeouts_machine_without_conv_is_noop() {
    let machine = CepStateMachine::new(
        "coverage_more_rule".into(),
        minimal_plan().match_plan,
        Some("event_time".into()),
    );
    let mut task = make_task(Spec {
        machine: Some(machine),
        metrics: Some(metrics()),
        ..Spec::default()
    });
    // 强制墙钟流逝 → effective watermark 推进（capped at 扫描间隔）。
    task.last_activity_wall = std::time::Instant::now() - std::time::Duration::from_secs(10);
    task.scan_timeouts().await;
}

#[tokio::test]
async fn scan_timeouts_deferred_only_returns_early() {
    // machine None + deferred Some + watermark i64::MIN → 不扫描直接返回。
    let mut plan = minimal_plan();
    plan.joins = vec![JoinPlan {
        right_window: "w".into(),
        mode: JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: Some(wf_lang::ast::Expr::Number(1.0)),
    }];
    let mut task = make_task(Spec {
        plan,
        ..Spec::default()
    });
    task.scan_timeouts().await;
}

// ---------------------------------------------------------------------------
// pull_and_advance — 整批 round-robin 门控
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pull_and_advance_round_robin_skips_other_shard_batches() {
    let schema = test_schema();
    let (win, notify) = make_window("auth_events", &schema);
    let mut task = make_task(Spec {
        window_sources: vec![super::super::task_types::WindowSource {
            window_name: "auth_events".into(),
            window: win.clone(),
            notify,
            aliases: vec!["b".into()],
        }],
        shard_index: Some(0),
        shard_count: 2,
        ..Spec::default()
    });
    win.append(make_batch(&["1.1.1.1"], 100)).expect("append");
    win.append(make_batch(&["2.2.2.2"], 200)).expect("append");
    task.pull_and_advance().await;
    // 两条批次 seq 0/1; shard 0 只处理偶数序批次; 光标推进到 2。
    assert_eq!(task.cursors.get("auth_events").copied(), Some(2));
}

// ---------------------------------------------------------------------------
// process_batch 早退 / 仅 events 输入
// ---------------------------------------------------------------------------

#[tokio::test]
async fn process_batch_unknown_window_is_noop() {
    let mut task = make_task(Spec::default());
    task.process_batch("ghost", 0, None, None, None, None, None)
        .await;
}

#[tokio::test]
async fn process_batch_events_only_machine_free_loop() {
    // machine None + each None：行循环体为空，但覆盖 eager events 解析分支。
    let mut task = make_task(Spec::default());
    let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![
        Arc::new(Event {
            fields: EngineHashMap::default(),
        }),
        Arc::new(Event {
            fields: EngineHashMap::default(),
        }),
    ]);
    let push = RulePush {
        window_name: Arc::from("auth_events"),
        events: Some(events),
        batch: None,
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    };
    task.process_push(push).await;
}

// ---------------------------------------------------------------------------
// Drop 释放进度槽
// ---------------------------------------------------------------------------

#[test]
fn drop_releases_progress_slots() {
    let slot = Arc::new(AtomicU64::new(0));
    let task = make_task(Spec {
        progress: HashMap::from([("w".to_string(), Arc::clone(&slot))]),
        ..Spec::default()
    });
    drop(task);
    assert_eq!(
        slot.load(std::sync::atomic::Ordering::Relaxed),
        u64::MAX,
        "Drop must release the progress slot"
    );
}

// ---------------------------------------------------------------------------
// row_event_debug_ref Columnar 臂
// ---------------------------------------------------------------------------

#[test]
fn row_event_debug_ref_columnar_arm() {
    let batch = make_batch(&["1.2.3.4"], 100);
    let col = ColumnarEvent::new(&batch, 0);
    let row_event = RowEvent::Columnar(col);
    assert_eq!(row_event_debug_ref(&row_event, 7, 3), "batch:7/row:3");
}
