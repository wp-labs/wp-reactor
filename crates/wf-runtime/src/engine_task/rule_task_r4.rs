//! rule_task.rs 第四轮补测（注册于 rule_task.rs 内, `#[path]` 方式）。
//!
//! 覆盖点（第三轮 `rule_task_coverage_more` 之外）:
//! - `RowEvent::Columnar` FieldSource 三臂（field_value / field_names / to_event）。
//! - L2 延迟物化路径（`defer_materialize`）: DeferredRows 构建 + 命中行
//!   ColumnarEvent 视图 + `advance_at_with_masks`。
//! - Q1 on-each 列式快路径（`columnar_each`）: 无 machine 直写批次 emit。
//! - eager 机器路径（debug 开）: `advance_at_with_progress` + Accumulate/
//!   Advance/Matched 细节日志分支。
//! - on-each direct 批量化路径（debug 关）与 per-event 路径（debug 开）,
//!   含采样器（detail / serialize）重置分支。
//! - conv-sink 通道关闭（process_batch / scan_timeouts / flush）丢弃日志。
//! - `emit` / `emit_batch` 的采样、空集、should_flush 分支;
//!   `flush_alerts` 通道关闭（带 metrics）计数。
//! - deferred-only 规则 `scan_timeouts` watermark 推进后扫描。
//! - `PipeBatchStager` 事件时间字段为 Null 列 + Timestamp 列非数值分支;
//!   `value_to_json` Object 成功路径。
use std::sync::Arc;

use super::*;

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;

use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use wf_engine::alert::{AlertOrigin, OutputRecord};
use wf_engine::match_engine::{
    CepStateMachine, ColumnarEvent, EngineHashMap, Event, RuleExecutor, Value,
};
use wf_engine::pipe::PipeRegistry;
use wf_engine::window::{Router, RulePush, Window, WindowParams, WindowRegistry};
use wf_lang::ast::{FieldRef, JoinMode};
use wf_lang::plan::{
    EachPlan, EntityPlan, JoinPlan, MatchPlan, RulePlan, ScorePlan, WindowSpec, YieldPlan,
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
        name: "r4_rule".into(),
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
        entity_plan: EntityPlan {
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

fn metrics() -> Arc<RuntimeMetrics> {
    Arc::new(RuntimeMetrics::new(
        &["r4_rule".to_string()],
        &[],
        &[],
        BTreeMap::new(),
    ))
}

/// 单步 count>=3 规则（tests.rs 的 make_task 形状）: 机器路径的安全计划。
/// 返回 (RulePlan, machine)。
fn machine_rule() -> (RulePlan, CepStateMachine) {
    use wf_lang::ast::{CmpOp, Measure};
    use wf_lang::plan::{AggPlan, BranchPlan, StepPlan};
    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(300)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("fail".into()),
                source: "fail".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: wf_lang::ast::Expr::Number(3.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: wf_lang::ast::CloseMode::Or,
        tracked_bind_aliases: HashSet::from(["fail".to_string()]),
        tracked_bind_fields: HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };
    let plan = RulePlan {
        name: "r4_machine".into(),
        binds: vec![wf_lang::plan::BindPlan {
            alias: "fail".into(),
            window: "auth_events".into(),
            filter: None,
        }],
        lets: vec![],
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: wf_lang::ast::Expr::Field(FieldRef::Qualified(
                "fail".into(),
                "sip".into(),
            )),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: wf_lang::ast::Expr::Number(70.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    };
    let machine = CepStateMachine::new("r4_machine".into(), match_plan, Some("event_time".into()));
    (plan, machine)
}

fn record_with(target: &str, event_time_nanos: i64) -> OutputRecord {
    OutputRecord {
        wfx_id: format!("id-{event_time_nanos}"),
        rule_name: "r4_rule".into(),
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
        machine_id: Arc::from(""),
        scope_key: "".into(),
    }
}

/// 在指定 tracing dispatch 下运行 async 闭包（当前线程 runtime）。
/// 用于隔离 debug 开/关: `Dispatch::none()` 关闭所有级别（defer/columnar 路径
/// 需要 `!debug_enabled`）, 而 `debug_dispatch()` 打开（细节日志路径）。
fn run_with_dispatch<F, Fut>(dispatch: tracing::Dispatch, f: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");
        tracing::dispatcher::with_default(&dispatch, || rt.block_on(f()));
    })
    .join()
    .expect("worker thread");
}

fn debug_dispatch() -> tracing::Dispatch {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    let layer = tracing_subscriber::fmt::layer()
        .with_test_writer()
        .with_filter(tracing_subscriber::EnvFilter::try_new("debug").expect("debug filter"));
    tracing::Dispatch::new(tracing_subscriber::registry().with(layer))
}

fn no_debug_dispatch() -> tracing::Dispatch {
    tracing::Dispatch::none()
}

// ---------------------------------------------------------------------------
// RowEvent::Columnar FieldSource 三臂
// ---------------------------------------------------------------------------

#[test]
fn row_event_columnar_field_source_arms() {
    let batch = make_batch(&["1.1.1.1", "2.2.2.2"], 100);
    let ev = RowEvent::Columnar(ColumnarEvent::new(&batch, 0));
    assert_eq!(ev.field_value("sip"), Some(Value::Str("1.1.1.1".into())));
    let names = ev.field_names();
    assert!(names.contains(&"sip"));
    let materialized = ev.to_event();
    assert_eq!(
        materialized.fields.get("sip"),
        Some(&Value::Str("1.1.1.1".into()))
    );
}

// ---------------------------------------------------------------------------
// L2 延迟物化路径（debug 关 + machine + batch）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn process_batch_deferred_materialization_path() {
    let schema = test_schema();
    let (win, notify) = make_window("auth_events", &schema);
    let (plan, machine) = machine_rule();
    let mut task = make_task(Spec {
        plan,
        machine: Some(machine),
        window_sources: vec![super::super::task_types::WindowSource {
            window_name: "auth_events".into(),
            window: win,
            notify,
            aliases: vec!["fail".into()],
        }],
        metrics: Some(metrics()),
        ..Spec::default()
    });
    let batch = make_batch(&["1.1.1.1", "1.1.1.1", "1.1.1.1"], 100);
    run_with_dispatch(no_debug_dispatch(), move || async move {
        // defer_materialize = batch + machine + !debug + columnar-safe filters。
        // DeferredRows 构建（times/hit/hit_indices/build_field_index）与
        // 命中行 ColumnarEvent 视图（advance_at_with_masks）路径。
        task.process_batch("auth_events", 0, None, None, Some(&batch), None, None)
            .await;
        task.flush().await;
    });
}

// ---------------------------------------------------------------------------
// Q1 on-each 列式快路径（debug 关 + 无 machine + each）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn process_batch_columnar_each_path() {
    let schema = test_schema();
    let (win, notify) = make_window("auth_events", &schema);
    let mut plan = minimal_plan();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    // columnar 门控需要 entity 为 Field / StringLit（Bool 会被拒绝）。
    plan.entity_plan.entity_id_expr =
        wf_lang::ast::Expr::Field(FieldRef::Qualified("b".into(), "sip".into()));
    let mut task = make_task(Spec {
        plan,
        each_alias: Some("b".into()),
        each_time_field: Some("event_time".into()),
        window_sources: vec![super::super::task_types::WindowSource {
            window_name: "auth_events".into(),
            window: win,
            notify,
            aliases: vec!["b".into()],
        }],
        metrics: Some(metrics()),
        ..Spec::default()
    });
    let batch = make_batch(&["1.1.1.1", "2.2.2.2"], 200);
    run_with_dispatch(no_debug_dispatch(), move || async move {
        // columnar_each = !debug + machine None + each_direct + events None +
        // batch Some + each_plan_columnar_safe → 整批列式直写 emit。
        task.process_batch("auth_events", 0, None, None, Some(&batch), None, None)
            .await;
    });
}

// ---------------------------------------------------------------------------
// eager 机器路径（debug 开）→ advance_at_with_progress + 细节日志
// ---------------------------------------------------------------------------

#[tokio::test]
async fn process_batch_eager_machine_debug_detail_paths() {
    let schema = test_schema();
    let (win, notify) = make_window("auth_events", &schema);
    let (plan, machine) = machine_rule();
    let mut task = make_task(Spec {
        plan,
        machine: Some(machine),
        window_sources: vec![super::super::task_types::WindowSource {
            window_name: "auth_events".into(),
            window: win,
            notify,
            aliases: vec!["fail".into()],
        }],
        metrics: Some(metrics()),
        ..Spec::default()
    });
    // eager events（relay push: batch=None, events=Some）→ debug 细节日志路径。
    let mut e1 = EngineHashMap::default();
    e1.insert("sip".into(), Value::Str("1.1.1.1".into()));
    let mut e2 = EngineHashMap::default();
    e2.insert("sip".into(), Value::Str("1.1.1.1".into()));
    let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![
        Arc::new(Event { fields: e1 }),
        Arc::new(Event { fields: e2 }),
    ]);
    run_with_dispatch(debug_dispatch(), move || async move {
        let push = RulePush {
            window_name: Arc::from("auth_events"),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            seq: 1,
            shard_rows: None,
        };
        task.process_push(push).await;
        task.flush().await;
    });
}

// ---------------------------------------------------------------------------
// on-each direct 路径: debug 关（批量化 emit_each_direct_batch）/
//                     debug 开（per-event emit_each_direct + 采样重置）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn process_batch_each_direct_batched_debug_off() {
    let schema = test_schema();
    let (win, notify) = make_window("auth_events", &schema);
    let mut plan = minimal_plan();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    // entity 用字段表达式（Bool 会在 build_each_direct 求值时失败→行被跳过）。
    plan.entity_plan.entity_id_expr =
        wf_lang::ast::Expr::Field(FieldRef::Qualified("b".into(), "sip".into()));
    let mut task = make_task(Spec {
        plan,
        each_alias: Some("b".into()),
        each_time_field: Some("event_time".into()),
        window_sources: vec![super::super::task_types::WindowSource {
            window_name: "auth_events".into(),
            window: win,
            notify,
            aliases: vec!["b".into()],
        }],
        metrics: Some(metrics()),
        ..Spec::default()
    });
    // events 携带 + batch 为空 → columnar_each 关闭, 走 eager each-direct
    // 批量化收集（each_direct_rows）→ 批次尾 emit_each_direct_batch。
    let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(Event {
        fields: EngineHashMap::default(),
    })]);
    run_with_dispatch(no_debug_dispatch(), move || async move {
        let push = RulePush {
            window_name: Arc::from("auth_events"),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            seq: 1,
            shard_rows: None,
        };
        task.process_push(push).await;
        task.flush().await;
    });
}

#[tokio::test]
async fn process_batch_each_direct_per_event_debug_on_samplers() {
    let schema = test_schema();
    let (win, notify) = make_window("auth_events", &schema);
    let mut plan = minimal_plan();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    plan.entity_plan.entity_id_expr =
        wf_lang::ast::Expr::Field(FieldRef::Qualified("b".into(), "sip".into()));
    let mut task = make_task(Spec {
        plan,
        each_alias: Some("b".into()),
        each_time_field: Some("event_time".into()),
        window_sources: vec![super::super::task_types::WindowSource {
            window_name: "auth_events".into(),
            window: win,
            notify,
            aliases: vec!["b".into()],
        }],
        metrics: Some(metrics()),
        ..Spec::default()
    });
    run_with_dispatch(debug_dispatch(), move || async move {
        // 65 批 × 1 事件 → emit_each_direct 65 次: 越过 emit_sample_remaining
        // 与 serialize_sample_remaining 的 64 采样间隔, 触发采样重置分支
        // （detail/e2e + serialize timing）。
        for i in 0..65u64 {
            let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(Event {
                fields: EngineHashMap::default(),
            })]);
            let push = RulePush {
                window_name: Arc::from("auth_events"),
                events: Some(events),
                batch: None,
                materialize_fields: None,
                seq: i,
                shard_rows: None,
            };
            task.process_push(push).await;
        }
        task.flush().await;
    });
}

// ---------------------------------------------------------------------------
// conv-sink 通道关闭（process_batch / scan_timeouts / flush 丢弃日志）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn conv_sink_closed_channel_drop_paths() {
    let (plan, machine) = machine_rule();
    let (tx, rx) = mpsc::channel::<crate::engine_task::ConvCloseBatch>(8);
    drop(rx); // 通道关闭 → send 失败 → 丢弃日志分支。
    let mut task = make_task(Spec {
        plan,
        machine: Some(machine),
        conv_sink: Some(ConvShardSink {
            tx,
            barrier_index: 0,
        }),
        ..Spec::default()
    });
    // process_batch 内 conv 关闭（1440）; scan_timeouts（1779）; flush（1928）。
    task.scan_timeouts().await;
    task.flush().await;
}

// ---------------------------------------------------------------------------
// deferred-only 规则: scan_timeouts watermark 推进后扫描 + flush 到期
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deferred_only_scan_timeouts_after_watermark_advances() {
    let schema = test_schema();
    let (win, notify) = make_window("auth_events", &schema);
    let mut plan = minimal_plan();
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    // deferred join（emit at）: 需要 within + 键条件 + 事件字段才能挂起。
    let within = wf_lang::ast::WithinSpec {
        lo: wf_lang::ast::Bound {
            open: false,
            val: wf_lang::ast::BoundVal::Expr(wf_lang::ast::Expr::Number(0.0)),
        },
        hi: wf_lang::ast::Bound {
            open: false,
            val: wf_lang::ast::BoundVal::Expr(wf_lang::ast::Expr::Number(5_000.0)),
        },
    };
    plan.joins = vec![JoinPlan {
        right_window: "w".into(),
        mode: JoinMode::Inner,
        conds: vec![wf_lang::plan::JoinCondPlan {
            left: FieldRef::Simple("id".into()),
            right: FieldRef::Simple("id".into()),
        }],
        within: Some(within),
        reduce: None,
        emit_at: Some(wf_lang::ast::Expr::Number(3_000.0)),
    }];
    let mut task = make_task(Spec {
        plan,
        each_alias: Some("b".into()),
        each_time_field: Some("event_time".into()),
        window_sources: vec![super::super::task_types::WindowSource {
            window_name: "auth_events".into(),
            window: win,
            notify,
            aliases: vec!["b".into()],
        }],
        metrics: Some(metrics()),
        ..Spec::default()
    });
    // 事件带键字段与时间字段 → deferred_pending_for 成功 → watermark 推进。
    let mut fields = EngineHashMap::default();
    fields.insert("id".into(), Value::Number(1.0));
    fields.insert("event_time".into(), Value::Number(1_000.0));
    let events: Arc<Vec<Arc<Event>>> = Arc::new(vec![Arc::new(Event { fields })]);
    run_with_dispatch(no_debug_dispatch(), move || async move {
        let push = RulePush {
            window_name: Arc::from("auth_events"),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            seq: 1,
            shard_rows: None,
        };
        task.process_push(push).await;
        // watermark 已推进（> i64::MIN）→ scan_timeouts 触发 scan_deferred。
        task.scan_timeouts().await;
        // flush → i64::MAX 全部到期 → execute_deferred_join。
        task.flush().await;
    });
}

// ---------------------------------------------------------------------------
// emit / emit_batch 采样与 flush 分支（直接调用私有方法）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn emit_batch_sampling_empty_and_flush() {
    let task = make_task(Spec {
        metrics: Some(metrics()),
        ..Spec::default()
    });
    // 空集 → n == 0 早退。
    task.emit_batch(Vec::new()).await;

    // 4096 条 → pending.count 满 → flush_alerts。65 条间隔触发 detail 采样。
    let records: Vec<OutputRecord> = (0..ALERT_BATCH_SIZE)
        .map(|i| record_with("alerts", i as i64))
        .collect();
    task.emit_batch(records).await;

    // 再来 1 条 → 采样 detail 分支 + should_flush 再次触发。
    task.emit(record_with("alerts", 999_999)).await;
}

#[tokio::test]
async fn emit_metric_sampler_reset_branch() {
    let task = make_task(Spec {
        metrics: Some(metrics()),
        ..Spec::default()
    });
    // 65 次 emit → emit_sample_remaining 越过 64 → detail 采样重置分支。
    for i in 0..65u64 {
        task.emit(record_with("alerts", i as i64)).await;
    }
}

#[tokio::test]
async fn flush_alerts_closed_channel_with_metrics() {
    let (tx, rx) = mpsc::channel::<AlertBatch>(4);
    drop(rx);
    let mut cache = HashMap::new();
    let groups = Arc::new(vec![(0usize, Arc::new(vec![tx]))]);
    cache.insert("alerts".to_string(), groups);
    let fanout = SinkFanout::from_resolved(cache);
    let task = make_task(Spec {
        sink_fanout: fanout,
        metrics: Some(metrics()),
        ..Spec::default()
    });
    // Closed 通道 → inc_alert_channel_send_failed + 丢弃日志。
    task.emit(record_with("alerts", 1)).await;
    task.flush_alerts().await;
}

// ---------------------------------------------------------------------------
// PipeBatchStager 补充分支 + value_to_json Object
// ---------------------------------------------------------------------------

#[test]
fn pipe_stager_event_time_field_null_column_and_timestamp_non_number() {
    // 事件时间字段为 Date32（→ Null 列）: push_record 命中 Null 分支。
    // 列实际含 null（无值填充），schema 必须声明可空。
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new(PIPE_EVENT_TIME_FIELD, DataType::Date32, true),
        ArrowField::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
    let record = record_with("t", 1_000);
    stager
        .push_record(&record)
        .expect("stage with Null event-time col");
    let (_, events, _) = stager.take_events().unwrap().expect("rows staged");
    assert_eq!(events.len(), 1);
    // event_time 列为 Timestamp 但值为非数值 → `_ => None` 分支。
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new(PIPE_EVENT_TIME_FIELD, DataType::Date32, true),
        ArrowField::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let mut stager = PipeBatchStager::new("t".into(), schema, Some(1));
    let mut record = record_with("t", 1_000);
    record.yield_fields = vec![(Arc::from("event_time"), Value::Str("not-a-number".into()))];
    stager
        .push_record(&record)
        .expect("stage with non-number time");
    let (_, events, _) = stager.take_events().unwrap().expect("rows staged");
    assert_eq!(events[0].fields.get("event_time"), None);
}

#[test]
fn value_to_json_object_success_path() {
    let value = Value::Object(
        [
            ("a".into(), Value::Number(1.0)),
            ("b".into(), Value::Bool(true)),
        ]
        .into_iter()
        .collect(),
    );
    let json = value_to_json(&value).expect("object serializes");
    assert_eq!(json, serde_json::json!({"a": 1.0, "b": true}));
    // 嵌套数组。
    let value = Value::Array(vec![Value::Number(2.0), Value::Str("x".into())]);
    let json = value_to_json(&value).expect("array serializes");
    assert_eq!(json, serde_json::json!([2.0, "x"]));
}

// ---------------------------------------------------------------------------
// scan_deferred 早退（deferred None）经 scan_timeouts 空转
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scan_timeouts_machine_none_deferred_none_is_noop() {
    let mut task = make_task(Spec::default());
    task.scan_timeouts().await;
    task.flush().await;
}

// ---------------------------------------------------------------------------
// emit_each_direct Err 分支（each + 异常字段值）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn emit_each_direct_serialize_failure_path() {
    // 通过直接调用 emit_each_direct 不可行（需内部字段）; 改用 emit 追加一个
    // 结构化非有限数值 → AlertColumnBuilder::append_record 失败 → 计数分支。
    let task = make_task(Spec {
        metrics: Some(metrics()),
        ..Spec::default()
    });
    let mut record = record_with("alerts", 1);
    record.yield_fields = vec![(
        Arc::from("sip"),
        Value::Object(
            [("score".into(), Value::Number(f64::NAN))]
                .into_iter()
                .collect(),
        ),
    )];
    task.emit(record).await;
    task.flush_alerts().await;
}
