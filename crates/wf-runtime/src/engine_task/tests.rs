use super::*;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use tokio::sync::{Notify, mpsc};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
use wf_engine::match_engine::{CepStateMachine, RuleExecutor, batch_to_events};
use wf_engine::window::{
    Router, RulePush, Window, WindowDef, WindowParams, WindowRegistry, content_bytes,
};
use wf_lang::ast::{BinOp, CloseMode, CmpOp, Expr, FieldRef, Measure, ObjectItem};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EachPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
    WindowSpec, YieldField, YieldPlan,
};

use crate::alert_task::SinkFanout;
use crate::tracing_init::DomainFormat;

// -- helpers ------------------------------------------------------------

/// Install a tracing subscriber that prints to the test harness.
///
/// `cargo test` captures output by default; pass `--nocapture` to see it:
/// ```sh
/// cargo test -p wf-runtime -- engine_task::tests --nocapture
/// ```
/// Safe to call multiple times -- subsequent calls are no-ops.
pub fn init_tracing() {
    let _ = tracing_subscriber::registry()
        .with(
            fmt::layer()
                .event_format(DomainFormat::new())
                .with_test_writer()
                .with_filter(EnvFilter::try_new("debug").unwrap()),
        )
        .try_init();
}

pub fn empty_tracked_bind_fields() -> std::collections::HashMap<String, HashSet<String>> {
    std::collections::HashMap::new()
}

/// Extract the first record from the next alert batch (tests deliver batches).
pub fn take_alert(
    rx: &mut mpsc::Receiver<crate::alert_task::AlertBatch>,
) -> Arc<wp_model_core::model::DataRecord> {
    let batch = rx.try_recv().expect("expected an alert batch");
    first_record(&batch)
}

/// Async variant of [`take_alert`] for `recv().await` based assertions.
pub async fn take_alert_recv(
    rx: &mut mpsc::Receiver<crate::alert_task::AlertBatch>,
) -> Arc<wp_model_core::model::DataRecord> {
    let batch = rx.recv().await.expect("expected an alert batch");
    first_record(&batch)
}

/// First record of a batch in either payload form (columns go through the
/// row view, which is field-identical to `to_data_record`).
fn first_record(batch: &crate::alert_task::AlertBatch) -> Arc<wp_model_core::model::DataRecord> {
    match batch {
        crate::alert_task::AlertBatch::Rows(rows) => {
            Arc::clone(rows.first().expect("alert batch must not be empty"))
        }
        crate::alert_task::AlertBatch::Columns(cols) => Arc::new(
            cols.iter_data_records()
                .next()
                .expect("alert batch must not be empty")
                .expect("columnar row view conversion"),
        ),
    }
}

pub fn make_test_fanout(tx: mpsc::Sender<crate::alert_task::AlertBatch>) -> Arc<SinkFanout> {
    let mut cache = std::collections::HashMap::new();
    // One sink (ptr=0) with a single writer channel (batches); the cache type is
    // inferred from `SinkFanout::from_resolved`.
    let groups = Arc::new(vec![(0usize, Arc::new(vec![tx.clone()]))]);
    cache.insert("alerts".to_string(), Arc::clone(&groups));
    cache.insert("network_alerts".to_string(), groups);
    // nexmark_pk 输出窗口（q8/q9 等 yield nexmark_alerts）——fanout 无此 key
    // 时 flush_alerts 的 resolve 为空 → 输出被丢弃（测试假失败）。
    let nexmark_groups = Arc::new(vec![(0usize, Arc::new(vec![tx]))]);
    cache.insert("nexmark_alerts".to_string(), nexmark_groups);
    SinkFanout::from_resolved(cache)
}

/// Extract a `__wfu_*` field's string form from a sink `DataRecord`.
pub fn field_str(record: &wp_model_core::model::DataRecord, name: &str) -> String {
    record
        .field(name)
        .map(|f| f.get_value().to_string())
        .unwrap_or_default()
}

/// Extract a `__wfu_*` float field from a sink `DataRecord`.
fn field_f64(record: &wp_model_core::model::DataRecord, name: &str) -> f64 {
    record
        .field(name)
        .map(|f| f.get_value().to_string().parse::<f64>().unwrap_or(f64::NAN))
        .unwrap_or(f64::NAN)
}

pub fn empty_tracked_plain_fields() -> HashSet<String> {
    HashSet::new()
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

fn filtered_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new("action", DataType::Utf8, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

fn internal_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "__wf_pipe_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("sip", DataType::Utf8, true),
        Field::new("ev_count", DataType::Int64, true),
    ]))
}

fn intermediate_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("sip", DataType::Utf8, true),
        Field::new("__wfu_score", DataType::Float64, true),
        Field::new("__wfu_rule_name", DataType::Utf8, true),
        Field::new("__wfu_entity_type", DataType::Utf8, true),
        Field::new("__wfu_entity_id", DataType::Utf8, true),
        Field::new("risk_context", DataType::Utf8, true),
        Field::new("tags", DataType::Utf8, true),
    ]))
}

fn intermediate_score_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("sip", DataType::Utf8, true),
        Field::new("risk_score", DataType::Float64, true),
        Field::new("__wfu_score", DataType::Float64, true),
        Field::new("__wfu_rule_name", DataType::Utf8, true),
        Field::new("__wfu_entity_type", DataType::Utf8, true),
        Field::new("__wfu_entity_id", DataType::Utf8, true),
    ]))
}

fn scored_source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new("risk_score", DataType::Float64, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

pub fn test_window_config(max_bytes: usize) -> WindowConfig {
    WindowConfig {
        name: "auth_events".into(),
        mode: DistMode::Local,
        max_window_bytes: max_bytes.into(),
        over_cap: Duration::from_secs(3600).into(),
        evict_policy: EvictPolicy::TimeFirst,
        watermark: Duration::from_secs(0).into(),
        allowed_lateness: Duration::from_secs(3600).into(),
        late_policy: LatePolicy::Drop,
        table: None,
    }
}

fn make_window(name: &str, schema: &SchemaRef, max_bytes: usize) -> (Arc<Window>, Arc<Notify>) {
    let win = Window::new(
        WindowParams {
            name: name.into(),
            schema: schema.clone(),
            time_col_index: Some(1), // event_time is the second column
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_window_config(max_bytes),
    );
    (Arc::new(win), Arc::new(Notify::new()))
}

fn make_batch(schema: &SchemaRef, sips: &[&str], ts: i64) -> RecordBatch {
    let n = sips.len();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![ts; n])),
        ],
    )
    .unwrap()
}

fn make_filtered_batch(
    schema: &SchemaRef,
    sips: &[&str],
    actions: &[&str],
    ts: i64,
) -> RecordBatch {
    let n = sips.len();
    assert_eq!(n, actions.len());
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                actions.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![ts; n])),
        ],
    )
    .unwrap()
}

fn make_scored_batch(schema: &SchemaRef, sips: &[&str], scores: &[f64], ts: i64) -> RecordBatch {
    let n = sips.len();
    assert_eq!(n, scores.len());
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::Float64Array::from(scores.to_vec())),
            Arc::new(TimestampNanosecondArray::from(vec![ts; n])),
        ],
    )
    .unwrap()
}

fn make_window_def(
    name: &str,
    schema: &SchemaRef,
    streams: &[&str],
    time_col: Option<usize>,
) -> WindowDef {
    let mut cfg = test_window_config(usize::MAX);
    cfg.name = name.to_string();
    WindowDef {
        params: WindowParams {
            name: name.to_string(),
            schema: schema.clone(),
            time_col_index: time_col,
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: streams.iter().map(|s| (*s).to_string()).collect(),
        config: cfg,
    }
}

/// Build a single-step count>=3 rule and return (task, alert_rx, window_arc, notify_arc).
fn make_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    make_task_with_window_bytes(usize::MAX)
}

/// Build a RuleTask for the following WFL rule:
///
/// ```wfl
/// rule test_rule {
///   events {
///     fail : auth_events           // stream "syslog"
///   }
///   match<sip:5m> {
///     on event {
///       fail | count >= 3;
///     }
///   } -> score(70.0)
///   entity(ip, fail.sip)
///   yield alerts ()
/// }
/// ```
///
/// `max_bytes` controls the window's `max_window_bytes` for memory-pressure tests.
fn make_task_inner(
    filter: Option<Expr>,
    branch_guard: Option<Expr>,
    max_bytes: usize,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    let schema = test_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, max_bytes);

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("fail".into()),
                source: "fail".into(),
                field: None,
                guard: branch_guard,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(3.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: std::collections::HashSet::from(["x".to_string()]),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };

    let rule_plan = RulePlan {
        conv_window: None,
        name: "test_rule".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: "auth_events".into(),
            filter,
        }],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("fail".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(70.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let machine = CepStateMachine::new("test_rule".into(), match_plan, Some("event_time".into()));
    let executor = RuleExecutor::new(rule_plan);

    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);

    // Empty registry for tests (no joins or has() usage).
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));

    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: Some(machine),
        each_alias: None,
        each_time_field: None,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&win_arc),
            notify: Arc::clone(&notify_arc),
            aliases: vec!["fail".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router,
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };

    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
}

fn make_task_with_window_bytes(
    max_bytes: usize,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    make_task_inner(None, None, max_bytes)
}

fn make_filter_task(
    filter: Expr,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    make_task_inner(Some(filter), None, usize::MAX)
}

fn make_branch_guard_task(
    branch_guard: Expr,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    make_task_inner(None, Some(branch_guard), usize::MAX)
}

fn make_pipeline_stage_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    make_pipeline_stage_task_opts(true)
}

/// `include_target_window: false` builds the stage task without the pipe
/// target window (and an empty pipe registry), exercising the
/// `PipeState::Uninit -> Dead` degradation path.
fn make_pipeline_stage_task_opts(
    include_target_window: bool,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let src_schema = test_schema();
    let internal = internal_schema();
    let source_name = "auth_events";
    let target_name = "__wf_pipe_pipe_s1_w1";
    let mut window_defs = vec![make_window_def(
        source_name,
        &src_schema,
        &["syslog"],
        Some(1),
    )];
    if include_target_window {
        window_defs.push(make_window_def(
            target_name,
            &internal,
            &[target_name],
            Some(0),
        ));
    }
    let registry = WindowRegistry::build(window_defs).unwrap();
    let router = Arc::new(Router::new(registry));

    let source_window = router.registry().get_window(source_name).unwrap();
    let source_notify = router.registry().get_notifier(source_name).unwrap();

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("ev_count".into()),
                source: "fail".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: std::collections::HashSet::from(["x".to_string()]),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };
    let rule_plan = RulePlan {
        conv_window: None,
        name: "__wf_pipe_pipe_s1".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: source_name.into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "pipeline".into(),
            entity_id_expr: Expr::Field(FieldRef::Simple("sip".into())),
        },
        yield_plan: YieldPlan {
            target: target_name.into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "sip".into(),
                    value: Expr::Field(FieldRef::Simple("sip".into())),
                },
                YieldField {
                    name: "ev_count".into(),
                    value: Expr::Field(FieldRef::Simple("ev_count".into())),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(0.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let machine = CepStateMachine::new(
        "__wf_pipe_pipe_s1".into(),
        match_plan,
        Some("event_time".into()),
    );
    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: Some(machine),
        each_alias: None,
        each_time_field: None,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: source_name.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["fail".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::from([target_name.into()]),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, router)
}

fn make_each_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    make_each_task_with_bind_filter(None)
}

/// make_each_task 的参数化：自定义 bind filter（gap-4 非列式 bind filter 对拍
/// 用——columnar_each 命中循环逐行解释 vs 行式 event_matches_alias）。
fn make_each_task_with_bind_filter(
    bind_filter: Option<Expr>,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    let schema = test_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, usize::MAX);
    let rule_plan = RulePlan {
        conv_window: None,
        name: "each_rule".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: "auth_events".into(),
            filter: bind_filter,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(1)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: true,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            // 无 each filter——被测的是 bind filter（gap-4）；保留 each filter
            // 会与测试数据冲突（原 make_each_task 的 sip==10.0.0.1 过滤器）。
            filter: None,
        }),
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "x".into(),
                value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("e".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&win_arc),
            notify: Arc::clone(&notify_arc),
            aliases: vec!["e".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router,
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
}

fn make_filtered_match_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    let schema = filtered_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, usize::MAX);

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
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
                    threshold: Expr::Number(2.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: std::collections::HashSet::from(["x".to_string()]),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };

    let rule_plan = RulePlan {
        conv_window: None,
        name: "filtered_match".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: "auth_events".into(),
            filter: Some(Expr::BinOp {
                op: BinOp::Eq,
                left: Box::new(Expr::Field(FieldRef::Simple("action".into()))),
                right: Box::new(Expr::StringLit("failed".into())),
            }),
        }],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("fail".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let machine = CepStateMachine::new(
        "filtered_match".into(),
        match_plan,
        Some("event_time".into()),
    );
    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: Some(machine),
        each_alias: None,
        each_time_field: None,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&win_arc),
            notify: Arc::clone(&notify_arc),
            aliases: vec!["fail".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router,
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
}

fn make_filtered_close_config() -> (
    task_types::RuleTaskConfig,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    let schema = filtered_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, usize::MAX);

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
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
                    threshold: Expr::Number(3.0),
                },
            }],
        }],
        close_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("close_count".into()),
                source: "fail".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_mode: CloseMode::And,
        tracked_bind_aliases: std::collections::HashSet::new(),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };

    let rule_plan = RulePlan {
        conv_window: None,
        name: "filtered_close".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: "auth_events".into(),
            filter: Some(Expr::BinOp {
                op: BinOp::Eq,
                left: Box::new(Expr::Field(FieldRef::Simple("action".into()))),
                right: Box::new(Expr::StringLit("failed".into())),
            }),
        }],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("fail".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(70.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let machine = CepStateMachine::new(
        "filtered_close".into(),
        match_plan,
        Some("event_time".into()),
    );
    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: Some(machine),
        each_alias: None,
        each_time_field: None,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&win_arc),
            notify: Arc::clone(&notify_arc),
            aliases: vec!["fail".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router,
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    (config, alert_rx, win_arc, notify_arc)
}

fn make_filtered_close_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    let (config, alert_rx, win_arc, notify_arc) = make_filtered_close_config();
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
}

fn make_filtered_each_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    let schema = filtered_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, usize::MAX);
    let rule_plan = RulePlan {
        conv_window: None,
        name: "filtered_each".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: "auth_events".into(),
            filter: Some(Expr::BinOp {
                op: BinOp::Eq,
                left: Box::new(Expr::Field(FieldRef::Simple("action".into()))),
                right: Box::new(Expr::StringLit("failed".into())),
            }),
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(1)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: true,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("e".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&win_arc),
            notify: Arc::clone(&notify_arc),
            aliases: vec!["e".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router,
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
}

fn make_intermediate_each_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let src_schema = test_schema();
    let mid_schema = intermediate_schema();
    let source_name = "auth_events";
    let target_name = "enriched_events";
    let registry = WindowRegistry::build(vec![
        make_window_def(source_name, &src_schema, &["syslog"], Some(1)),
        make_window_def(target_name, &mid_schema, &[], Some(0)),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));

    let source_window = router.registry().get_window(source_name).unwrap();
    let source_notify = router.registry().get_notifier(source_name).unwrap();

    let rule_plan = RulePlan {
        conv_window: None,
        name: "intermediate_each".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: source_name.into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(1)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: true,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: target_name.into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "sip".into(),
                    value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                },
                YieldField {
                    name: "risk_context".into(),
                    value: Expr::Object(vec![
                        ObjectItem {
                            targets: vec!["score".into()],
                            type_hint: None,
                            value: Expr::SystemVar(wf_lang::ast::SystemVar::Score),
                        },
                        ObjectItem {
                            targets: vec!["source".into()],
                            type_hint: None,
                            value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                        },
                    ]),
                },
                YieldField {
                    name: "tags".into(),
                    value: Expr::Array(vec![
                        Expr::StringLit("intermediate".into()),
                        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                    ]),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(7.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("e".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: source_name.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["e".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::from([target_name.into()]),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, router)
}

fn make_intermediate_each_task_with_explicit_time() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let src_schema = test_schema();
    let mid_schema = intermediate_schema();
    let source_name = "auth_events";
    let target_name = "enriched_events";
    let registry = WindowRegistry::build(vec![
        make_window_def(source_name, &src_schema, &["syslog"], Some(1)),
        make_window_def(target_name, &mid_schema, &[], Some(0)),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));

    let source_window = router.registry().get_window(source_name).unwrap();
    let source_notify = router.registry().get_notifier(source_name).unwrap();

    let rule_plan = RulePlan {
        conv_window: None,
        name: "intermediate_each_explicit_time".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: source_name.into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(1)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: true,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: target_name.into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "event_time".into(),
                    value: Expr::Number(10_000_000_000.0),
                },
                YieldField {
                    name: "sip".into(),
                    value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(7.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("e".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: source_name.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["e".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::from([target_name.into()]),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, router)
}

fn make_intermediate_score_tasks() -> (
    rule_task::RuleTask,
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let src_schema = scored_source_schema();
    let mid_schema = intermediate_score_schema();
    let source_name = "auth_events";
    let target_name = "semantic_events";
    let registry = WindowRegistry::build(vec![
        make_window_def(source_name, &src_schema, &["syslog"], Some(2)),
        make_window_def(target_name, &mid_schema, &[], Some(0)),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));

    let source_window = router.registry().get_window(source_name).unwrap();
    let source_notify = router.registry().get_notifier(source_name).unwrap();
    let intermediate_window = router.registry().get_window(target_name).unwrap();
    let intermediate_notify = router.registry().get_notifier(target_name).unwrap();

    let upstream_plan = RulePlan {
        conv_window: None,
        name: "semantic_project".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: source_name.into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(1)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: true,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: target_name.into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "event_time".into(),
                    value: Expr::Field(FieldRef::Qualified("e".into(), "event_time".into())),
                },
                YieldField {
                    name: "sip".into(),
                    value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                },
                YieldField {
                    name: "risk_score".into(),
                    value: Expr::Field(FieldRef::Qualified("e".into(), "risk_score".into())),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Field(FieldRef::Qualified("e".into(), "risk_score".into())),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let upstream_executor = RuleExecutor::new(upstream_plan);
    let (upstream_alert_tx, _upstream_alert_rx) =
        mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let upstream_config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("e".into()),
        each_time_field: Some("event_time".into()),
        executor: upstream_executor,
        window_sources: vec![task_types::WindowSource {
            window_name: source_name.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["e".into()],
        }],
        sink_fanout: make_test_fanout(upstream_alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::from([target_name.into()]),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (upstream_task, _cancel, _interval) = rule_task::RuleTask::new(upstream_config);

    let downstream_match = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(1)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "x".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "x".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_mode: CloseMode::And,
        tracked_bind_aliases: std::collections::HashSet::from(["x".to_string()]),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };

    let downstream_plan = RulePlan {
        conv_window: None,
        name: "window_risk".into(),
        binds: vec![BindPlan {
            alias: "x".into(),
            window: target_name.into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: downstream_match.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("x".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "avg_score".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "avg".into(),
                        args: vec![Expr::Field(FieldRef::Qualified(
                            "x".into(),
                            "__wfu_score".into(),
                        ))],
                    },
                },
                YieldField {
                    name: "avg_risk".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "avg".into(),
                        args: vec![Expr::Field(FieldRef::Qualified(
                            "x".into(),
                            "risk_score".into(),
                        ))],
                    },
                },
                YieldField {
                    name: "event_count".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "count".into(),
                        args: vec![Expr::Field(FieldRef::Simple("x".into()))],
                    },
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::FuncCall {
                qualifier: None,
                name: "avg".into(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "x".into(),
                    "__wfu_score".into(),
                ))],
            },
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let downstream_executor = RuleExecutor::new(downstream_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let downstream_machine = CepStateMachine::new(
        "window_risk".into(),
        downstream_match,
        Some("event_time".into()),
    );
    let downstream_config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: Some(downstream_machine),
        each_alias: None,
        each_time_field: None,
        executor: downstream_executor,
        window_sources: vec![task_types::WindowSource {
            window_name: target_name.into(),
            window: intermediate_window,
            notify: intermediate_notify,
            aliases: vec!["x".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (downstream_task, _cancel, _interval) = rule_task::RuleTask::new(downstream_config);

    (upstream_task, downstream_task, alert_rx, router)
}

fn make_intermediate_score_band_tasks() -> (
    rule_task::RuleTask,
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let src_schema = scored_source_schema();
    let mid_schema = intermediate_score_schema();
    let source_name = "auth_events";
    let target_name = "semantic_events";
    let registry = WindowRegistry::build(vec![
        make_window_def(source_name, &src_schema, &["syslog"], Some(2)),
        make_window_def(target_name, &mid_schema, &[], Some(0)),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));

    let source_window = router.registry().get_window(source_name).unwrap();
    let source_notify = router.registry().get_notifier(source_name).unwrap();
    let intermediate_window = router.registry().get_window(target_name).unwrap();
    let intermediate_notify = router.registry().get_notifier(target_name).unwrap();

    let upstream_plan = RulePlan {
        conv_window: None,
        name: "semantic_project".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: source_name.into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(1)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: true,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: target_name.into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "event_time".into(),
                    value: Expr::Field(FieldRef::Qualified("e".into(), "event_time".into())),
                },
                YieldField {
                    name: "sip".into(),
                    value: Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
                },
                YieldField {
                    name: "risk_score".into(),
                    value: Expr::Field(FieldRef::Qualified("e".into(), "risk_score".into())),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Field(FieldRef::Qualified("e".into(), "risk_score".into())),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let upstream_executor = RuleExecutor::new(upstream_plan);
    let (upstream_alert_tx, _upstream_alert_rx) =
        mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let upstream_config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("e".into()),
        each_time_field: Some("event_time".into()),
        executor: upstream_executor,
        window_sources: vec![task_types::WindowSource {
            window_name: source_name.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["e".into()],
        }],
        sink_fanout: make_test_fanout(upstream_alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::from([target_name.into()]),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (upstream_task, _cancel, _interval) = rule_task::RuleTask::new(upstream_config);

    let downstream_match = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(1)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "x".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "x".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_mode: CloseMode::And,
        tracked_bind_aliases: std::collections::HashSet::from(["x".to_string()]),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };

    let downstream_plan = RulePlan {
        conv_window: None,
        name: "window_risk".into(),
        binds: vec![
            BindPlan {
                alias: "x".into(),
                window: target_name.into(),
                filter: None,
            },
            BindPlan {
                alias: "hi".into(),
                window: target_name.into(),
                filter: Some(Expr::BinOp {
                    op: BinOp::Ge,
                    left: Box::new(Expr::Field(FieldRef::Simple("risk_score".into()))),
                    right: Box::new(Expr::Number(85.0)),
                }),
            },
            BindPlan {
                alias: "elevated".into(),
                window: target_name.into(),
                filter: Some(Expr::BinOp {
                    op: BinOp::Ge,
                    left: Box::new(Expr::Field(FieldRef::Simple("risk_score".into()))),
                    right: Box::new(Expr::Number(70.0)),
                }),
            },
        ],
        lets: Vec::new(),
        match_plan: downstream_match.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("x".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "event_count".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "count".into(),
                        args: vec![Expr::Field(FieldRef::Simple("x".into()))],
                    },
                },
                YieldField {
                    name: "source_avg".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "avg".into(),
                        args: vec![Expr::Field(FieldRef::Qualified(
                            "x".into(),
                            "risk_score".into(),
                        ))],
                    },
                },
                YieldField {
                    name: "high_event_count".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "count".into(),
                        args: vec![Expr::Field(FieldRef::Simple("hi".into()))],
                    },
                },
                YieldField {
                    name: "elevated_event_count".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "count".into(),
                        args: vec![Expr::Field(FieldRef::Simple("elevated".into()))],
                    },
                },
                YieldField {
                    name: "status".into(),
                    value: Expr::IfThenElse {
                        cond: Box::new(Expr::BinOp {
                            op: BinOp::And,
                            left: Box::new(Expr::BinOp {
                                op: BinOp::Ge,
                                left: Box::new(Expr::FuncCall {
                                    qualifier: None,
                                    name: "count".into(),
                                    args: vec![Expr::Field(FieldRef::Simple("hi".into()))],
                                }),
                                right: Box::new(Expr::Number(1.0)),
                            }),
                            right: Box::new(Expr::BinOp {
                                op: BinOp::Ge,
                                left: Box::new(Expr::FuncCall {
                                    qualifier: None,
                                    name: "count".into(),
                                    args: vec![Expr::Field(FieldRef::Simple("elevated".into()))],
                                }),
                                right: Box::new(Expr::Number(2.0)),
                            }),
                        }),
                        then_expr: Box::new(Expr::StringLit("high".into())),
                        else_expr: Box::new(Expr::StringLit("low".into())),
                    },
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::FuncCall {
                qualifier: None,
                name: "avg".into(),
                args: vec![Expr::Field(FieldRef::Qualified(
                    "x".into(),
                    "__wfu_score".into(),
                ))],
            },
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let downstream_executor = RuleExecutor::new(downstream_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let downstream_machine = CepStateMachine::new(
        "window_risk".into(),
        downstream_match,
        Some("event_time".into()),
    );
    let downstream_config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: Some(downstream_machine),
        each_alias: None,
        each_time_field: None,
        executor: downstream_executor,
        window_sources: vec![task_types::WindowSource {
            window_name: target_name.into(),
            window: intermediate_window,
            notify: intermediate_notify,
            aliases: vec!["x".into(), "hi".into(), "elevated".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (downstream_task, _cancel, _interval) = rule_task::RuleTask::new(downstream_config);

    (upstream_task, downstream_task, alert_rx, router)
}

fn make_filtered_bind_alias_match_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    let schema = scored_source_schema();
    let source_name = "auth_events";
    let registry = WindowRegistry::build(vec![make_window_def(
        source_name,
        &schema,
        &["syslog"],
        Some(2),
    )])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let window = router.registry().get_window(source_name).unwrap();
    let notify = router.registry().get_notifier(source_name).unwrap();

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "x".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(2.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: std::collections::HashSet::from(["x".to_string()]),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };

    let rule_plan = RulePlan {
        conv_window: None,
        name: "bind_alias_match".into(),
        binds: vec![
            BindPlan {
                alias: "x".into(),
                window: source_name.into(),
                filter: None,
            },
            BindPlan {
                alias: "hi".into(),
                window: source_name.into(),
                filter: Some(Expr::BinOp {
                    op: BinOp::Ge,
                    left: Box::new(Expr::Field(FieldRef::Simple("risk_score".into()))),
                    right: Box::new(Expr::Number(85.0)),
                }),
            },
            BindPlan {
                alias: "elevated".into(),
                window: source_name.into(),
                filter: Some(Expr::BinOp {
                    op: BinOp::Ge,
                    left: Box::new(Expr::Field(FieldRef::Simple("risk_score".into()))),
                    right: Box::new(Expr::Number(70.0)),
                }),
            },
        ],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("x".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "source_avg".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "avg".into(),
                        args: vec![Expr::Field(FieldRef::Qualified(
                            "x".into(),
                            "risk_score".into(),
                        ))],
                    },
                },
                YieldField {
                    name: "high_event_count".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "count".into(),
                        args: vec![Expr::Field(FieldRef::Simple("hi".into()))],
                    },
                },
                YieldField {
                    name: "elevated_avg".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "avg".into(),
                        args: vec![Expr::Field(FieldRef::Qualified(
                            "elevated".into(),
                            "risk_score".into(),
                        ))],
                    },
                },
                YieldField {
                    name: "last_high_sip".into(),
                    value: Expr::FuncCall {
                        qualifier: None,
                        name: "last".into(),
                        args: vec![Expr::Field(FieldRef::Qualified("hi".into(), "sip".into()))],
                    },
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::FuncCall {
                qualifier: None,
                name: "count".into(),
                args: vec![Expr::Field(FieldRef::Simple("hi".into()))],
            },
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let machine = CepStateMachine::new(
        "bind_alias_match".into(),
        match_plan,
        Some("event_time".into()),
    );
    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: Some(machine),
        each_alias: None,
        each_time_field: None,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: source_name.into(),
            window: Arc::clone(&window),
            notify: Arc::clone(&notify),
            aliases: vec!["x".into(), "hi".into(), "elevated".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router,
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, window, notify)
}

fn make_window_has_match_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let schema = test_schema();
    let source_name = "auth_events";
    let lookup_name = "threat_list";
    let registry = WindowRegistry::build(vec![
        make_window_def(source_name, &schema, &["syslog"], Some(1)),
        make_window_def(lookup_name, &schema, &["feed"], Some(1)),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));

    let source_window = router.registry().get_window(source_name).unwrap();
    let source_notify = router.registry().get_notifier(source_name).unwrap();

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
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
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: std::collections::HashSet::new(),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };

    let rule_plan = RulePlan {
        conv_window: None,
        name: "window_has_match".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: source_name.into(),
            filter: Some(Expr::FuncCall {
                qualifier: Some(lookup_name.into()),
                name: "has".into(),
                args: vec![Expr::Field(FieldRef::Simple("sip".into()))],
            }),
        }],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("fail".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let machine = CepStateMachine::new(
        "window_has_match".into(),
        match_plan,
        Some("event_time".into()),
    );
    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: Some(machine),
        each_alias: None,
        each_time_field: None,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: source_name.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["fail".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, router)
}

// -- test cases ---------------------------------------------------------

#[tokio::test]
async fn pull_empty_window() {
    init_tracing();
    let (mut task, mut alert_rx, _win, _notify) = make_task();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "empty window should produce no alerts"
    );
}

#[tokio::test]
async fn pull_advances_cursor() {
    init_tracing();
    let schema = test_schema();
    let (mut task, _alert_rx, win, _notify) = make_task();

    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.2"], ts);
    win.append(batch).unwrap();

    task.pull_and_advance().await;
    let cursor = task.cursors["auth_events"];
    assert_eq!(
        cursor, 1,
        "cursor should advance to 1 after reading one batch"
    );

    task.pull_and_advance().await;
    let cursor2 = task.cursors["auth_events"];
    assert_eq!(cursor2, 1, "cursor should remain 1 with no new data");
}

#[tokio::test]
async fn pull_triggers_alert() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, win, _notify) = make_task();

    let ts_nanos = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts_nanos);
    win.append(batch).unwrap();

    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "test_rule");
    assert_eq!(field_str(&alert, "__wfu_entity_type"), "ip");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!((field_f64(&alert, "__wfu_score") - 70.0).abs() < f64::EPSILON);
    assert!(!field_str(&alert, "__wfu_fired_at").is_empty());
}

#[tokio::test]
async fn push_triggers_alert() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, _win, _notify) = make_task();

    let ts_nanos = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts_nanos);

    // Feed the same parsed events the router would broadcast into the rule's
    // push channel, and advance the state machine through the push path.
    let push = RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::new(
            batch_to_events(&batch)
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>(),
        )),
        batch: None,
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    };
    task.process_push(push).await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "test_rule");
    assert_eq!(field_str(&alert, "__wfu_entity_type"), "ip");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!((field_f64(&alert, "__wfu_score") - 70.0).abs() < f64::EPSILON);
    assert!(!field_str(&alert, "__wfu_fired_at").is_empty());
}

#[tokio::test]
async fn push_columnar_sharded_defers_runs_all_rows() {
    // 列式 sharded deferred push：events=None + batch + shard_rows（本 shard 行子集）。
    // 规则任务只对 shard_rows 内的行跑 bind filter(无=全行命中）+状态机（count>=3 触发）。
    // 此处 shard_rows 含全部 3 行 → 全命中 → 触发 alert。
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, _win, _notify) = make_task();

    let ts_nanos = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts_nanos);

    let push = RulePush {
        window_name: "auth_events".into(),
        events: None, // deferred: 规则任务按 batch 列式物化命中行
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: Some(Arc::new(vec![0, 1, 2])), // 本 shard 拥有全部行
        seq: u64::MAX,
    };
    task.process_push(push).await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!((field_f64(&alert, "__wfu_score") - 70.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn push_sharded_only_processes_shard_rows_subset() {
    // 列式 sharded：shard_rows 只含行 0,2（两个 10.0.0.1）；行 1（10.0.0.2）不属于本
    // shard。规则只应对 shard_rows 内行推进状态机 → count=2 <3 不触发 → 无 alert，
    // 证明只扫 shard 子集（若误扫全批会让 10.0.0.1 count=2 仍不触发，故同时把行 1 也
    // 设为 10.0.0.1 以区分「子集处理」的额外断言在尾部补）。
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, _win, _notify) = make_task();

    let ts_nanos = 1_700_000_000_000_000_000i64;
    // 行 0: 10.0.0.1, 行 1: 10.0.0.1, 行 2: 10.0.0.1 —— 全同 key；
    // 若规则误扫整批（3 行）会触发 count=3；shard_rows 只给 [0,1] → 只扫 2 行 → count=2 不触发。
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts_nanos);

    let push = RulePush {
        window_name: "auth_events".into(),
        events: None,
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: Some(Arc::new(vec![0, 1])), // 本 shard 只有 2 行
        seq: u64::MAX,
    };
    task.process_push(push).await;

    // count=2 (<3) → 不触发。
    let tr = alert_rx.try_recv();
    assert!(
        matches!(tr, Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
        "shard_rows 子集行 count=2 不应触发 alert"
    );
}

fn drain_alert_entity_ids(rx: &mut mpsc::Receiver<crate::alert_task::AlertBatch>) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        let records: Vec<wp_model_core::model::DataRecord> = match &batch {
            crate::alert_task::AlertBatch::Rows(rows) => {
                rows.iter().map(|r| r.as_ref().clone()).collect()
            }
            crate::alert_task::AlertBatch::Columns(cols) => cols
                .iter_data_records()
                .collect::<Result<Vec<_>, _>>()
                .expect("columnar row view conversion"),
        };
        for record in &records {
            ids.push(field_str(record, "__wfu_entity_id"));
        }
    }
    ids
}

#[tokio::test]
async fn columnar_bind_filter_matches_interpreted_path() {
    init_tracing();
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    // 3× "10.0.0.1" (count>=3 fires once) + 1× "10.0.0.2" (filtered out).
    let batch = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.1"],
        ts,
    );
    assert!(wf_lang::columnar::expr_is_columnar(&filter));
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );

    // Columnar path: the push carries the raw batch → bind filter is a mask.
    let (mut task, mut alert_rx, _win, _notify) = make_filter_task(filter.clone());
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::clone(&events)),
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let columnar_ids = drain_alert_entity_ids(&mut alert_rx);

    // Interpreted path: no raw batch → per-event `event_matches_alias`.
    let (mut task2, mut alert_rx2, _win2, _notify2) = make_filter_task(filter);
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let interpreted_ids = drain_alert_entity_ids(&mut alert_rx2);

    assert_eq!(columnar_ids, interpreted_ids);
    // Only sip == "10.0.0.1" passes the filter; 3 of them reach count>=3 → one fire.
    assert_eq!(columnar_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn columnar_branch_guard_matches_interpreted_path() {
    init_tracing();
    let guard = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.1"],
        ts,
    );
    assert!(wf_lang::columnar::expr_is_columnar(&guard));
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );

    // Columnar branch guard: push carries the raw batch → guard is a mask.
    let (mut task, mut alert_rx, _win, _notify) = make_branch_guard_task(guard.clone());
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::clone(&events)),
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let columnar_ids = drain_alert_entity_ids(&mut alert_rx);

    // Interpreted branch guard: no raw batch → per-event guard in the state machine.
    let (mut task2, mut alert_rx2, _win2, _notify2) = make_branch_guard_task(guard);
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let interpreted_ids = drain_alert_entity_ids(&mut alert_rx2);

    assert_eq!(columnar_ids, interpreted_ids);
    assert_eq!(columnar_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn deferred_materialization_matches_eager_path() {
    init_tracing();
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.1"],
        ts,
    );

    // Deferred: no pre-parsed events → the rule task materializes from the raw batch.
    let (mut task, mut alert_rx, _win, _notify) = make_filter_task(filter.clone());
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let deferred_ids = drain_alert_entity_ids(&mut alert_rx);

    // Eager: pre-parsed events (as `route_parse` would broadcast).
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let (mut task2, mut alert_rx2, _win2, _notify2) = make_filter_task(filter);
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let eager_ids = drain_alert_entity_ids(&mut alert_rx2);

    assert_eq!(deferred_ids, eager_ids);
    assert_eq!(deferred_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn each_noncolumnar_bind_filter_columnar_hit_matches_row_path() {
    // gap-4（2026-09-02）：非列式 bind filter 的 each 规则——columnar_each
    // 命中循环逐行 `event_matches_alias`（ColumnarEvent 视图直读列）vs 行式
    // eager 路径（Event 物化 + 同函数解释），输出必须一致（filter 拒绝的行
    // 不再被 hit.fill(true) 静默放行）。
    init_tracing();
    let bind_filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "upper".into(),
            args: vec![Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))],
        }),
        right: Box::new(Expr::StringLit("ABC".into())),
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["abc", "AB", "xyz", "abc"], ts);

    // 列式路径：push raw batch（columnar_each → 命中循环逐行解释）。
    let (mut task, mut alert_rx, _win, _notify) =
        make_each_task_with_bind_filter(Some(bind_filter.clone()));
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let columnar_ids = drain_alert_entity_ids(&mut alert_rx);

    // 行式路径：push materialized events（eager，无 batch → event_matches_alias
    // 解释于 Event）。
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let (mut task2, mut alert_rx2, _win2, _notify2) =
        make_each_task_with_bind_filter(Some(bind_filter));
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let row_ids = drain_alert_entity_ids(&mut alert_rx2);

    // upper(sip)=="ABC" → 行 0/3（"abc"）过；行 1（"AB"）、行 2（"xyz"）拒。
    assert_eq!(columnar_ids, row_ids, "列式命中循环必须与行式 filter 一致");
    assert_eq!(columnar_ids, vec!["abc".to_string(), "abc".to_string()]);
}

#[tokio::test]
async fn events_and_batch_both_present_prefers_columnar_path() {
    // 2026-08-22：defer_materialize 放宽——raw batch 存在且 bind filter 列式时，
    // 即使 relay/push 同时携带物化 events 也走列式（deferred）路径；events 仅作
    // emit 路径 trigger 投影。断言与纯 eager 输出一致（filter 仍生效：只放行
    // sip=10.0.0.1）。
    init_tracing();
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.1"],
        ts,
    );
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );

    // events + batch 同时存在：放宽后应走列式（deferred），filter 列式生效。
    let (mut task, mut alert_rx, _win, _notify) = make_filter_task(filter);
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::clone(&events)),
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let both_ids = drain_alert_entity_ids(&mut alert_rx);
    assert_eq!(both_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn non_columnar_filter_with_batch_falls_back_to_eager() {
    // 非列式 bind filter（含谓词函数调用）→ 即使 batch 存在也不 defer：
    // eager 路径解释执行 filter（拒绝的行不得漏进状态机——deferred 的
    // missing-mask 兜底会全放行，必须避免）。
    init_tracing();
    // `sip contains "0.0"` —— contains 非列式（列式安全门外），走解释器。
    let filter = Expr::FuncCall {
        qualifier: None,
        name: "contains".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".into())),
            Expr::StringLit("0.0".into()),
        ],
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    // contains "0.0"：10.0.0.1×3 命中（count=3 收口 fire）；9.9.9.9 不含 "0.0" 被拒。
    let batch = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.1", "10.0.0.1", "9.9.9.9"],
        ts,
    );
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let (mut task, mut alert_rx, _win, _notify) = make_filter_task(filter);

    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::clone(&events)),
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let both_ids = drain_alert_entity_ids(&mut alert_rx);
    assert_eq!(both_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn deferred_materialization_scans_every_row_for_intra_batch_expiry() {
    init_tracing();
    // `sip == "10.0.0.1"` is a columnar bind filter, so the deferred path
    // skips materializing the rejected "10.0.0.2" row. The rejected row's
    // event time (400s) must still drive the watermark/expiry scan: the
    // 300s sliding window instance created at T=0 must expire at T=400s,
    // before the next accepted row starts a fresh instance (count=1).
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let schema = test_schema();
    let sips = ["10.0.0.1", "10.0.0.1", "10.0.0.2", "10.0.0.1"];
    let times = [0i64, 100_000_000_000, 400_000_000_000, 400_000_000_000];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
        ],
    )
    .unwrap();
    assert!(wf_lang::columnar::expr_is_columnar(&filter));

    // Deferred: no pre-parsed events → only the bind-filter hit rows are
    // materialized; the rejected row is still scanned for expiry.
    let (mut task, mut alert_rx, _win, _notify) = make_filter_task(filter.clone());
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let deferred_ids = drain_alert_entity_ids(&mut alert_rx);

    // Eager: pre-parsed events (full materialization).
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let (mut task2, mut alert_rx2, _win2, _notify2) = make_filter_task(filter);
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let eager_ids = drain_alert_entity_ids(&mut alert_rx2);

    // The 300s window expires the T=0 instance at the rejected row's T=400s,
    // so the final accepted row starts over at count=1 — no `count>=3` fire.
    assert_eq!(deferred_ids, eager_ids);
    assert!(deferred_ids.is_empty());
}

#[tokio::test]
async fn deferred_materialization_preserves_close_emission_for_rejected_rows() {
    init_tracing();
    // Regression: the deferred path used to `continue` past the close-emission
    // block for bind-filter-rejected rows, dropping expired-instance closes.
    // A columnar bind filter (`action == "failed"`) + a close step: accepted
    // rows complete the event step, then a later rejected row's event time
    // expires the instance and must still emit the close.
    let schema = filtered_schema();
    let sips = ["10.0.0.1", "10.0.0.1", "10.0.0.1", "10.0.0.1"];
    let actions = ["failed", "failed", "failed", "login"];
    let times = [0i64, 100_000_000_000, 200_000_000_000, 400_000_000_000];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                actions.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
        ],
    )
    .unwrap();

    // Deferred: no pre-parsed events; the rejected "login" row (T=400s) must
    // still drive the expiry scan and emit the instance's close.
    let (mut task, mut alert_rx, _win, _notify) = make_filtered_close_task();
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let deferred_ids = drain_alert_entity_ids(&mut alert_rx);

    // Eager: pre-parsed events (full materialization).
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let (mut task2, mut alert_rx2, _win2, _notify2) = make_filtered_close_task();
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let eager_ids = drain_alert_entity_ids(&mut alert_rx2);

    // Both paths emit exactly one close for the expired instance.
    assert_eq!(deferred_ids, eager_ids);
    assert_eq!(deferred_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn sharded_rule_produces_same_alerts_as_single_worker() {
    init_tracing();
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    // 6 events: 3 for "10.0.0.1", 3 for "10.0.0.2" → each triggers count>=3.
    let batch = make_batch(
        &schema,
        &[
            "10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.2",
        ],
        ts,
    );
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );

    // Single worker: feed the whole batch.
    let (mut single, mut single_rx, _w, _n) = make_task();
    single
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(Arc::clone(&events)),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let mut single_ids = drain_alert_entity_ids(&mut single_rx);

    // Sharded: partition via the router fan-out (2 shards), then two machines.
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));
    let (s0_tx, mut s0_rx) = mpsc::channel(8);
    let (s1_tx, mut s1_rx) = mpsc::channel(8);
    let keys: Arc<[FieldRef]> = Arc::from(vec![FieldRef::Simple("sip".into())].into_boxed_slice());
    router
        .fanout()
        .register_sharded("auth_events", vec![s0_tx, s1_tx], keys);
    router.fanout().broadcast("auth_events", &events, 0).await;

    let (mut t0, mut rx0, _w0, _n0) = make_task();
    let (mut t1, mut rx1, _w1, _n1) = make_task();
    while let Ok(push) = s0_rx.try_recv() {
        t0.process_push(push).await;
    }
    while let Ok(push) = s1_rx.try_recv() {
        t1.process_push(push).await;
    }
    let mut sharded_ids = drain_alert_entity_ids(&mut rx0);
    sharded_ids.extend(drain_alert_entity_ids(&mut rx1));

    single_ids.sort();
    sharded_ids.sort();
    assert_eq!(
        single_ids, sharded_ids,
        "sharded rule must produce identical alerts to the single worker"
    );
}

#[tokio::test]
async fn pull_keeps_normalized_nanos_event_time() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, win, _notify) = make_task();

    let ts_nanos = 1_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts_nanos);
    win.append(batch).unwrap();

    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert!(!field_str(&alert, "__wfu_fired_at").is_empty());
}

#[tokio::test]
async fn flush_emits_close_alert_for_completed_and_close_rule() {
    init_tracing();
    let schema = filtered_schema();
    let (mut task, mut alert_rx, win, _notify) = make_filtered_close_task();

    let ts = 1_700_000_000_000_000_000i64;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["10.0.0.1", "10.0.0.1", "10.0.0.1"])),
            Arc::new(StringArray::from(vec!["failed", "failed", "failed"])),
            Arc::new(TimestampNanosecondArray::from(vec![ts, ts + 1, ts + 2])),
        ],
    )
    .unwrap();
    win.append(batch).unwrap();

    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "and-close rule should not emit before close/flush"
    );

    task.flush().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "filtered_close");
    assert_eq!(field_str(&alert, "__wfu_entity_type"), "ip");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert_eq!(field_str(&alert, "__wfu_origin"), "close:flush");
}

#[tokio::test]
async fn shutdown_drain_pulls_tail_before_flush() {
    // e2e_datagen_brute_force CI flake regression: at full shutdown the rule
    // task must keep pulling until the window actor reports drained, so the
    // final flush runs against a complete machine. Without it the flush
    // closes at a stale machine watermark (the alert's fired_at falls on the
    // pre-tail watermark) and tail-triggered alerts are lost.
    init_tracing();
    let (mut task, mut alert_rx, win, _notify) = make_filtered_close_task();
    // The window actor is still committing its queued tail at shutdown.
    win.set_actor_drained(false);
    let ts = 1_700_000_000_000_000_000i64;

    // Tail committed to the window but NOT yet pulled by the rule task (what
    // the actor's cancel-drain commits before setting the drained flag):
    // 3 rows push count>=3, the 4th row (ts+60s) is the tail's last event.
    let batch = RecordBatch::try_new(
        filtered_schema(),
        vec![
            Arc::new(StringArray::from(vec![
                "10.0.0.1", "10.0.0.1", "10.0.0.1", "10.0.0.1",
            ])),
            Arc::new(StringArray::from(vec![
                "failed", "failed", "failed", "failed",
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![
                ts,
                ts + 1,
                ts + 2,
                ts + 60_000_000_000,
            ])),
        ],
    )
    .unwrap();
    win.append(batch).unwrap();

    // The drain must block while the actor is still draining…
    let mut drain = Box::pin(task.wait_shutdown_drain());
    tokio::select! {
        _ = &mut drain => panic!(
            "wait_shutdown_drain must block while the window actor is still draining"
        ),
        _ = tokio::time::sleep(Duration::from_millis(30)) => {}
    }
    // …and once the actor reports drained, it completes and the flush sees
    // the tail.
    win.set_actor_drained(true);
    drain.await;
    task.flush().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "filtered_close");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert_eq!(field_str(&alert, "__wfu_origin"), "close:flush");
    // The close must fire at the tail's watermark (4th row, ts+60s) — not a
    // stale pre-tail watermark (ts+2).
    let fired = chrono::DateTime::parse_from_rfc3339(&field_str(&alert, "__wfu_fired_at"))
        .unwrap_or_else(|e| panic!("parse fired_at: {e}"));
    assert_eq!(
        fired.timestamp_nanos_opt().expect("fired_at nanos"),
        ts + 60_000_000_000,
        "flush must run after the tail was pulled (fired_at = tail watermark)"
    );
}

#[tokio::test]
async fn shutdown_drain_times_out_when_actor_stuck() {
    // Safety net: a window actor that never reports drained must not hang the
    // shutdown forever — wait_shutdown_drain bails at the timeout and the
    // flush proceeds with the state it has.
    init_tracing();
    let (mut task, _alert_rx, win, _notify) = make_filtered_close_task();
    win.set_actor_drained(false);
    tokio::time::timeout(
        std::time::Duration::from_secs(7),
        task.wait_shutdown_drain(),
    )
    .await
    .expect("shutdown drain must not hang on a stuck actor (bounded by SHUTDOWN_DRAIN_TIMEOUT)");
}

#[tokio::test]
async fn rule_cancel_without_root_cancel_skips_drain_wait() {
    // Hot-reload shape: only the rule token fires, the window actors keep
    // running (never report drained). The shutdown drain wait must be skipped
    // or every reload would stall ~SHUTDOWN_DRAIN_TIMEOUT.
    init_tracing();
    let (mut config, _alert_rx, win, _notify) = make_filtered_close_config();
    win.set_actor_drained(false); // actors keep running → never drained
    let rule_cancel = tokio_util::sync::CancellationToken::new();
    config.cancel = rule_cancel.clone();
    let root_cancel = tokio_util::sync::CancellationToken::new();

    rule_cancel.cancel();
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        super::run_rule_task(config, root_cancel),
    )
    .await
    .expect("rule-only cancel must exit promptly without waiting for the actors")
    .expect("run_rule_task ok");
}

#[tokio::test]
async fn full_shutdown_with_real_actor_processes_mailbox_tail() {
    // 完整竞态的端到端回归：真实 window actor 在关停时 mailbox 里还押着尾部
    // 批次。规则任务必须等 actor 的 drained 标志（边等边拉），最终 flush 才
    // 会收口每个 key 的告警——无修复时它在陈旧 machine watermark 上 flush，
    // 未提交的尾部直接丢失（e2e_datagen_brute_force CI flake 同型）。
    init_tracing();
    let (mut config, mut alert_rx, win, notify) = make_filtered_close_config();

    // 在配置同一窗口上起一个真实 actor（规则任务从同一 Arc 拉取）。
    let (mailbox_tx, mailbox_rx) =
        mpsc::channel::<wf_engine::window::WindowMsg>(wf_engine::window::WINDOW_CHANNEL_DEPTH);
    let actor_cancel = tokio_util::sync::CancellationToken::new();
    let actor_win = Arc::clone(&win);
    let actor_cancel2 = actor_cancel.clone();
    let actor_notify = Arc::clone(&notify);
    let actor = tokio::spawn(async move {
        wf_engine::window::run_window_actor(
            Arc::from("auth_events"),
            actor_win,
            Arc::new(wf_engine::window::EvictionGate::new(usize::MAX)),
            wf_engine::window::RuleFanout::new(),
            actor_notify,
            mailbox_rx,
            actor_cancel2,
            None,
        )
        .await;
    });

    // 规则任务（pull 模式）读同一窗口。
    let root_cancel = tokio_util::sync::CancellationToken::new();
    let rule_cancel = tokio_util::sync::CancellationToken::new();
    config.cancel = rule_cancel.clone();
    let run = tokio::spawn(super::run_rule_task(config, root_cancel.clone()));

    // 等 actor 与规则任务就绪，然后把 5 个 3 行批次（每 key 一个）打进
    // actor 的 mailbox。count>=3 命中、无 close 事件 → 告警全部由关停
    // flush 收口（close:flush）。
    tokio::time::sleep(Duration::from_millis(30)).await;
    let ts = 1_700_000_000_000_000_000i64;
    let keys = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5"];
    for (i, key) in keys.iter().enumerate() {
        let batch = RecordBatch::try_new(
            filtered_schema(),
            vec![
                Arc::new(StringArray::from(vec![*key, *key, *key])),
                Arc::new(StringArray::from(vec!["failed", "failed", "failed"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    ts + (i * 3) as i64 * 1_000_000_000,
                    ts + (i * 3 + 1) as i64 * 1_000_000_000,
                    ts + (i * 3 + 2) as i64 * 1_000_000_000,
                ])),
            ],
        )
        .unwrap();
        mailbox_tx
            .send(wf_engine::window::WindowMsg::Append {
                source: Arc::from("ingress"),
                seq: i as u64,
                batch,
                events: None,
                byte_size: 128,
                permits: Vec::new(),
                shard_rows: None,
            })
            .await
            .unwrap();
    }

    // actor 可能还押着尾部时立刻关停（真实 reactor 中 root cancel 会传播
    // 到 rule_cancel——child token；测试里两个 token 独立，需都 cancel）。
    root_cancel.cancel();
    rule_cancel.cancel();
    actor_cancel.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("rule task must finish promptly")
        .expect("rule task joined without panic")
        .expect("run_rule_task ok");
    actor.await.expect("actor joins");

    // 每个 key 都通过 close:flush 收口——drain 等到了尾部。
    let mut ids = drain_alert_entity_ids(&mut alert_rx);
    ids.sort();
    let expected: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    assert_eq!(
        ids, expected,
        "all tail keys must be flushed after the drain"
    );
}

#[tokio::test]
async fn pull_multiple_keys_isolated() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, win, _notify) = make_task();

    let ts = 1_700_000_000_000_000_000i64;
    let batch1 = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.1", "10.0.0.2", "10.0.0.2"],
        ts,
    );
    win.append(batch1).unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "neither key should trigger at count=2"
    );

    let batch2 = make_batch(&schema, &["10.0.0.1"], ts + 1_000_000_000);
    win.append(batch2).unwrap();
    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");

    assert!(
        alert_rx.try_recv().is_err(),
        "sip=10.0.0.2 should not trigger"
    );
}

#[tokio::test]
async fn pull_detects_gap() {
    init_tracing();
    let schema = test_schema();
    let batch_size = {
        let tmp = make_batch(&schema, &["10.0.0.1"], 1_000_000_000);
        content_bytes(&tmp)
    };
    let (mut task, _alert_rx, win, _notify) = make_task_with_window_bytes(batch_size);

    let ts = 1_700_000_000_000_000_000i64;

    task.cursors.insert("auth_events".into(), 0);

    let batch0 = make_batch(&schema, &["10.0.0.1"], ts);
    win.append(batch0).unwrap();

    let batch1 = make_batch(&schema, &["10.0.0.2"], ts + 1_000_000_000);
    win.append(batch1).unwrap();

    assert_eq!(
        win.batch_count(),
        1,
        "only 1 batch should remain after eviction"
    );

    task.pull_and_advance().await;

    let cursor = task.cursors["auth_events"];
    assert_eq!(
        cursor, 2,
        "cursor should advance to 2 (past the surviving batch)"
    );
}

/// Build a keyed (`sip`) match rule (count>=1 fires once per key) with the
/// pull-model window sharding registered, and return `shard_count` independent
/// pull `RuleTask`s that all share ONE window log. Used to test P2 zero
/// re-partition: each shard must process only its stored `shard_rows` subset
/// and the union of all shards must cover every key exactly once.
fn make_sharded_match_tasks(
    shard_count: usize,
) -> (
    Vec<rule_task::RuleTask>,
    Vec<mpsc::Receiver<crate::alert_task::AlertBatch>>,
    Arc<Window>,
    Arc<Router>,
) {
    let schema = test_schema(); // sip(col0), event_time(col1)
    let registry = WindowRegistry::build(vec![make_window_def(
        "auth_events",
        &schema,
        &["syslog"],
        Some(1),
    )])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let window = router.registry().get_window("auth_events").unwrap();
    let notify = router.registry().get_notifier("auth_events").unwrap();
    // Register the key partition so `pull_and_advance` treats the window as
    // key-sharded (reads its `shard_rows` subset instead of the whole batch).
    router.fanout().register_window_sharding(
        "auth_events",
        Arc::from(vec![FieldRef::Simple("sip".into())].into_boxed_slice()),
        shard_count,
    );

    let mut tasks = Vec::new();
    let mut rxs = Vec::new();
    for shard_index in 0..shard_count {
        let match_plan = MatchPlan {
            keys: vec![FieldRef::Simple("sip".into())],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
            event_steps: vec![StepPlan {
                branches: vec![BranchPlan {
                    label: Some("x".into()),
                    source: "x".into(),
                    field: None,
                    guard: None,
                    agg: AggPlan {
                        transforms: vec![],
                        measure: Measure::Count,
                        cmp: CmpOp::Ge,
                        threshold: Expr::Number(1.0),
                    },
                }],
            }],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: true,
            trigger_event_needed: false,
        };
        let rule_plan = RulePlan {
            conv_window: None,
            name: "sharded_match".into(),
            binds: vec![BindPlan {
                alias: "x".into(),
                window: "auth_events".into(),
                filter: None,
            }],
            lets: Vec::new(),
            match_plan: match_plan.clone(),
            each_plan: None,
            stats_plan: None,
            joins: vec![],
            r#where: None,
            entity_plan: EntityPlan {
                entity_type: "ip".into(),
                entity_id_expr: Expr::Field(FieldRef::Qualified("x".into(), "sip".into())),
            },
            yield_plan: YieldPlan {
                target: "alerts".into(),
                version: None,
                fields: vec![],
            },
            score_plan: ScorePlan {
                expr: Expr::Number(1.0),
            },
            pattern_origin: None,
            conv_plan: None,
            limits_plan: None,
        };
        let machine = CepStateMachine::new(
            "sharded_match".into(),
            match_plan,
            Some("event_time".into()),
        );
        let executor = RuleExecutor::new(rule_plan);

        let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
        let mut progress = std::collections::HashMap::new();
        if let Some(slot) = router
            .registry()
            .progress("auth_events")
            .map(|p| p.register())
        {
            progress.insert("auth_events".to_string(), slot);
        }
        let config = task_types::RuleTaskConfig {
            progress,
            conv_sink: None,
            machine: Some(machine),
            each_alias: None,
            each_time_field: None,
            executor,
            window_sources: vec![task_types::WindowSource {
                window_name: "auth_events".into(),
                window: Arc::clone(&window),
                notify: Arc::clone(&notify),
                aliases: vec!["x".into()],
            }],
            sink_fanout: make_test_fanout(alert_tx),
            cancel: tokio_util::sync::CancellationToken::new(),
            timeout_scan_interval: Duration::from_secs(60),
            router: Arc::clone(&router),
            metrics: None,
            intermediate_targets: HashSet::new(),
            pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
            eos_flush: tokio::sync::watch::channel(0u64).1,
            push_rx: None,
            shard_index: Some(shard_index),
            shard_count,
            key_partitioned: true,
        };
        let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
        tasks.push(task);
        rxs.push(alert_rx);
    }
    (tasks, rxs, window, router)
}

#[tokio::test]
async fn pull_sharded_match_zero_repartition() {
    // P2 零重复分片端到端验证：6 个 key 按行号 % 2 分片（row i → shard i%2），
    // 每个 shard 只处理自己 shard_rows 子集 → 只对自己 key 触发；跨所有 shard
    // 的并集 == 全部 key 各一次（不丢、不重、不跨 shard 重复触发）。
    init_tracing();
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let sips = ["s0", "s1", "s2", "s3", "s4", "s5"];
    let batch = make_batch(&schema, &sips, ts);
    let shard_rows: Vec<Vec<u32>> = (0..2)
        .map(|sh| {
            (0..sips.len() as u32)
                .filter(|&r| r as usize % 2 == sh)
                .collect()
        })
        .collect();

    let (mut tasks, mut rxs, win, _router) = make_sharded_match_tasks(2);
    let size = content_bytes(&batch);
    win.append_with_watermark_sized(batch, size, Some(Arc::new(shard_rows)))
        .unwrap();

    for t in tasks.iter_mut() {
        t.pull_and_advance().await;
    }
    let ids0: HashSet<String> = drain_alert_entity_ids(&mut rxs[0]).into_iter().collect();
    let ids1: HashSet<String> = drain_alert_entity_ids(&mut rxs[1]).into_iter().collect();

    let expect0: HashSet<String> = ["s0", "s2", "s4"].iter().map(|s| s.to_string()).collect();
    let expect1: HashSet<String> = ["s1", "s3", "s5"].iter().map(|s| s.to_string()).collect();
    assert_eq!(ids0, expect0, "shard 0 must fire only its own keys");
    assert_eq!(ids1, expect1, "shard 1 must fire only its own keys");

    // Union across shards == every key exactly once.
    let union: HashSet<String> = ids0.union(&ids1).cloned().collect();
    assert_eq!(
        union.len(),
        sips.len(),
        "every key must fire exactly once across all shards (zero re-partition)"
    );
    assert!(
        union.iter().all(|s| sips.contains(&s.as_str())),
        "only real keys should fire"
    );
}

/// 2026-08-29 q1/q20 all 模式分片误拉回归：bid_events 被其它 match 规则注册 key
/// 分片后，on-each round-robin 任务（q20 形态）若用全局 `window_is_sharded` 判定
/// 拉取模式，会误把**别的规则**的 key 划分（`shard_rows`）当自己的行子集拉取——
/// 每 shard 处理被划分走的部分行（`columnar_each` 因 `shard_rows.is_some()` 失效
/// → 行式路径）→ 偶发丢行（all 模式 q20 196517→189k~193k、q1 重复处理 10×）。
/// 修复：任务携带**自己**的 `key_partitioned` 标志，round-robin 规则恒拉全批
/// （`shard_rows=None`）→ 走列式路径。
///
/// 本用例：窗口已注册 key 分片（模拟其它 match 规则），round-robin 任务
/// `key_partitioned=false` → 必须拉全批（shard 0 处理 batch_seq=0 的整批 6 行），
/// 而不是 `shard_rows[0]`（3 行）。
#[tokio::test]
async fn round_robin_pulls_whole_batch_despite_foreign_window_sharding() {
    init_tracing();
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let sips = ["s0", "s1", "s2", "s3", "s4", "s5"];
    let batch = make_batch(&schema, &sips, ts);
    // 模拟「别的 match 规则」注册的分片划分（6 行 → shard 0/1 各 3 行，按行号奇偶）。
    let shard_rows: Vec<Vec<u32>> = (0..2)
        .map(|sh| {
            (0..sips.len() as u32)
                .filter(|&r| r as usize % 2 == sh)
                .collect()
        })
        .collect();

    let registry = WindowRegistry::build(vec![make_window_def(
        "auth_events",
        &schema,
        &["syslog"],
        Some(1),
    )])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let window = router.registry().get_window("auth_events").unwrap();
    let notify = router.registry().get_notifier("auth_events").unwrap();
    // 全局注册 key 分片（模拟其它 match 规则）——旧代码 `window_is_sharded` 会因此
    // 把 round-robin 任务误判为 key-partitioned。
    router.fanout().register_window_sharding(
        "auth_events",
        Arc::from(vec![FieldRef::Simple("sip".into())].into_boxed_slice()),
        2,
    );

    // q1 形态 on-each 直通任务：每行一条输出（entity = sip）。
    let rule_plan = RulePlan {
        conv_window: None,
        name: "rr_whole_batch".into(),
        binds: vec![BindPlan {
            alias: "x".into(),
            window: "auth_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Fixed(std::time::Duration::ZERO),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "x".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("x".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };
    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, mut alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("x".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&window),
            notify: Arc::clone(&notify),
            aliases: vec!["x".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        // round-robin shard 0/2：batch_seq=0 归本 shard。key_partitioned=false →
        // 拉全批（shard_rows=None），不被上面的全局分片注册影响。
        shard_index: Some(0),
        shard_count: 2,
        key_partitioned: false,
    };
    let (mut task, _cancel, _interval) = rule_task::RuleTask::new(config);

    let size = content_bytes(&batch);
    window
        .append_with_watermark_sized(batch, size, Some(Arc::new(shard_rows)))
        .unwrap();
    task.pull_and_advance().await;

    // round-robin：batch_seq=0 → shard 0 处理整批 6 行 → 6 条输出。
    // 若误用全局分片（shard_rows[0] = 3 行）→ 只输出 3 条（回归锚点）。
    let ids: HashSet<String> = drain_alert_entity_ids(&mut alert_rx).into_iter().collect();
    assert_eq!(
        ids.len(),
        sips.len(),
        "round-robin shard must process the WHOLE batch (not the foreign key partition)"
    );
    assert!(
        ids.iter().all(|s| sips.contains(&s.as_str())),
        "all batch rows must be emitted"
    );
}

/// 2026-08-29 key_partitioned 修复副产物回归：round-robin（whole-batch 分片）规则
/// 的 ack 必须是**处理位置**（本 shard 份额内最后处理批次 + 1），而不是读位置
/// （`new_cursor` = 全部批次）。旧代码 ack 读位置会让 `min_acked` 追平
/// `next_seq` → 窗口驱逐无未读保护 → 删掉**其它 shard 尚未处理**的批次（cursor
/// gap 静默丢数据，q13a 分片隐患同类）。修复后 `key_partitioned=false` 走处理
/// 位置 ack。
///
/// 本用例：2 shard round-robin，append 4 批（seq 0-3，各 1 行）。shard 0 的
/// 份额 = 批 0、2 → 处理后 ack=3（批 0 后处理批 2 → last+1 = 3），而非读位置 4。
#[tokio::test]
async fn round_robin_shard_acks_processed_not_read_position() {
    init_tracing();
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;

    let registry = WindowRegistry::build(vec![make_window_def(
        "auth_events",
        &schema,
        &["syslog"],
        Some(1),
    )])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let window = router.registry().get_window("auth_events").unwrap();
    let notify = router.registry().get_notifier("auth_events").unwrap();
    // 模拟其它 match 规则注册 key 分片：旧代码 `window_is_sharded` 会因此把
    // round-robin 规则误判为 key-partitioned → ack 读位置（4）+ 处理全部 4 批；
    // 新代码 key_partitioned=false → ack 处理位置（3）+ 只处理份额内批 0、2。
    router.fanout().register_window_sharding(
        "auth_events",
        Arc::from(vec![FieldRef::Simple("sip".into())].into_boxed_slice()),
        2,
    );
    // q1 形态 on-each 直通任务（同 round_robin_pulls_whole_batch_despite_foreign_window_sharding）。
    let rule_plan = RulePlan {
        conv_window: None,
        name: "rr_ack".into(),
        binds: vec![BindPlan {
            alias: "x".into(),
            window: "auth_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Fixed(std::time::Duration::ZERO),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: HashSet::new(),
            tracked_bind_fields: empty_tracked_bind_fields(),
            tracked_plain_fields: empty_tracked_plain_fields(),
            seq: None,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "x".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("x".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };
    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, _alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let mut progress = std::collections::HashMap::new();
    if let Some(slot) = router
        .registry()
        .progress("auth_events")
        .map(|p| p.register())
    {
        progress.insert("auth_events".to_string(), slot);
    }
    let config = task_types::RuleTaskConfig {
        progress,
        conv_sink: None,
        machine: None,
        each_alias: Some("x".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&window),
            notify: Arc::clone(&notify),
            aliases: vec!["x".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: Some(0),
        shard_count: 2,
        key_partitioned: false,
    };
    let (mut task, _cancel, _interval) = rule_task::RuleTask::new(config);

    for b in 0..4u32 {
        let batch = make_batch(&schema, &["10.0.0.1"], ts + b as i64);
        let size = content_bytes(&batch);
        window
            .append_with_watermark_sized(batch, size, None)
            .unwrap();
    }
    task.pull_and_advance().await;

    let floor = router
        .registry()
        .progress("auth_events")
        .expect("progress table exists")
        .min_acked();
    assert_eq!(
        floor, 3,
        "round-robin shard must ack its PROCESSED position (批 0、2 → 3)，not the read position (4)"
    );
}

#[tokio::test]
async fn pull_sharded_advances_ack_floor() {
    // pull 后必须把消费进度写进 WindowProgress slot（min_acked 跟上 cursor）。
    // 内存驱逐仍依赖这个地板；时间驱逐不再依赖它（见下方断言）。
    init_tracing();
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let (mut tasks, _rxs, win, router) = make_sharded_match_tasks(1);

    for b in 0..3u32 {
        let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.2"], ts + b as i64);
        let size = content_bytes(&batch);
        win.append_with_watermark_sized(batch, size, None).unwrap();
    }
    tasks[0].pull_and_advance().await;

    let floor = router
        .registry()
        .progress("auth_events")
        .expect("progress table exists")
        .min_acked();
    assert_eq!(floor, 3, "ack floor must equal batches processed + 1");

    // 时间驱逐现在纯按事件时间，忽略 ack floor：now 推进到 over 之后，即使
    // batch 已 ack（floor=3），过期批次仍被驱逐（慢规则会在这里观察到 pull gap）。
    assert_eq!(win.batch_count(), 3, "sanity: 3 batches buffered");
    win.evict_expired(ts + 3_600_000_000_000 + 1_000);
    assert_eq!(
        win.batch_count(),
        0,
        "time eviction drops expired batches regardless of ack floor"
    );
}

#[tokio::test]
async fn flush_closes_active_instances() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, win, _notify) = make_task();

    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1"], ts);
    win.append(batch).unwrap();
    task.pull_and_advance().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "count=2 should not trigger alert"
    );

    task.flush().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "flush of incomplete instance should not produce alert"
    );
}

#[tokio::test]
async fn pipeline_stage_output_writes_internal_window_instead_of_alert_channel() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, router) = make_pipeline_stage_task();
    let ts = 1_700_000_000_123_000_000i64;
    // Pure relay (P1c): register a downstream rule subscriber; no window storage.
    let (down_tx, mut down_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    router.fanout().register("__wf_pipe_pipe_s1_w1", down_tx);

    let batch = make_batch(&schema, &["10.0.0.8"], ts);
    let source = router.registry().get_window("auth_events").unwrap();
    source.append(batch).unwrap();
    task.pull_and_advance().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "internal pipeline stage must not emit sink alerts"
    );

    // 2026-08-23 行为变更：pipe flush 同时 **append 目标窗口**（供 pull 模式的
    // 下游消费方读取——q4a→auction_finals→q4b(stats) 双规则链；纯 relay 只广播
    // 会让 pull 消费方静默饿死）+ 广播（供 push 消费方）。
    assert!(
        !router
            .registry()
            .snapshot("__wf_pipe_pipe_s1_w1")
            .unwrap_or_default()
            .is_empty(),
        "pipe flush must append the internal window (pull consumers read it)"
    );
    let push = down_rx
        .try_recv()
        .expect("downstream rule received pipeline events");
    let rows = push.events.expect("push carries events");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].fields.get("sip"),
        Some(&wf_engine::match_engine::Value::Str("10.0.0.8".into()))
    );
    assert_eq!(
        rows[0].fields.get("ev_count"),
        Some(&wf_engine::match_engine::Value::Number(1.0))
    );
    assert_eq!(
        rows[0].fields.get("__wf_pipe_ts"),
        Some(&wf_engine::match_engine::Value::Number(ts as f64))
    );
}

#[tokio::test]
async fn intermediate_target_writes_window_instead_of_alert_channel() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, router) = make_intermediate_each_task();
    let ts = 4_000_000_000_000_000_000i64;
    // Pure relay (P1c): register a downstream rule subscriber; no window storage.
    let (down_tx, mut down_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    router.fanout().register("enriched_events", down_tx);

    let batch = make_batch(&schema, &["10.0.0.8"], ts);
    let source = router.registry().get_window("auth_events").unwrap();
    source.append(batch).unwrap();
    task.pull_and_advance().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "intermediate targets must not emit sink alerts"
    );

    // 2026-08-23 行为变更：pipe flush append 目标窗口（pull 消费方读取）+
    // 广播（push 消费方）。
    assert!(
        !router
            .registry()
            .snapshot("enriched_events")
            .unwrap_or_default()
            .is_empty(),
        "intermediate pipe flush must append the target window"
    );
    let push = down_rx
        .try_recv()
        .expect("downstream rule received intermediate events");
    let rows = push.events.expect("push carries events");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].fields.get("sip"),
        Some(&wf_engine::match_engine::Value::Str("10.0.0.8".into()))
    );
    assert_eq!(
        rows[0].fields.get("__wfu_score"),
        Some(&wf_engine::match_engine::Value::Number(7.0))
    );
    assert_eq!(
        rows[0].fields.get("__wfu_rule_name"),
        Some(&wf_engine::match_engine::Value::Str(
            "intermediate_each".into()
        ))
    );
    assert_eq!(
        rows[0].fields.get("event_time"),
        Some(&wf_engine::match_engine::Value::Number(ts as f64))
    );
    assert_eq!(
        rows[0].fields.get("risk_context"),
        Some(&wf_engine::match_engine::Value::Str(
            r#"{"score":7.0,"source":"10.0.0.8"}"#.into()
        ))
    );
    assert_eq!(
        rows[0].fields.get("tags"),
        Some(&wf_engine::match_engine::Value::Str(
            r#"["intermediate","10.0.0.8"]"#.into()
        ))
    );
}

#[tokio::test]
async fn intermediate_target_preserves_explicit_time_field() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, router) = make_intermediate_each_task_with_explicit_time();
    let ts = 4_000_000_000_000_000i64;
    // Pure relay (P1c): register a downstream rule subscriber; no window storage.
    let (down_tx, mut down_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    router.fanout().register("enriched_events", down_tx);

    let batch = make_batch(&schema, &["10.0.0.8"], ts);
    let source = router.registry().get_window("auth_events").unwrap();
    source.append(batch).unwrap();
    task.pull_and_advance().await;

    assert!(alert_rx.try_recv().is_err());

    // Pure relay: no window storage; the broadcast event preserves the explicit
    // time field as epoch nanos.
    // 2026-08-23 行为变更：pipe flush 同时 append 目标窗口（pull 消费方）。
    assert!(
        !router
            .registry()
            .snapshot("enriched_events")
            .unwrap_or_default()
            .is_empty(),
        "intermediate pipe flush must append the target window"
    );
    let push = down_rx
        .try_recv()
        .expect("downstream rule received intermediate events");
    let event = &push.events.as_ref().unwrap()[0];
    assert_eq!(
        event.fields.get("event_time"),
        Some(&wf_engine::match_engine::Value::Number(
            10_000_000_000_000_000.0
        ))
    );
}

#[tokio::test]
async fn downstream_close_aggregates_intermediate_float_fields() {
    init_tracing();
    let schema = scored_source_schema();
    let (mut upstream_task, mut downstream_task, mut alert_rx, router) =
        make_intermediate_score_tasks();
    let ts = 4_000_000_000_000_000i64;

    let batch = make_scored_batch(&schema, &["10.0.0.8", "10.0.0.8"], &[10.0, 30.0], ts);
    let source = router.registry().get_window("auth_events").unwrap();
    source.append(batch).unwrap();
    // 推进 watermark 到 fixed 1s 桶 [ts, ts+1s) 完整（w_end ≤ wm）——2026-08-23
    // close_all 对齐 oracle/Flink：尾部未完整窗口（w_end > 最终事件时间）不输出。
    source
        .append(make_scored_batch(
            &schema,
            &["10.0.0.1"],
            &[1.0],
            ts + 2_000_000_000,
        ))
        .unwrap();

    // Pure relay (P1c): the intermediate pipe is not stored in a window; the
    // downstream rule consumes the broadcast via push.
    let (down_tx, mut down_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    router.fanout().register("semantic_events", down_tx);

    upstream_task.pull_and_advance().await;
    while let Ok(push) = down_rx.try_recv() {
        downstream_task.process_push(push).await;
    }
    downstream_task.flush().await;

    let alert = take_alert_recv(&mut alert_rx).await;
    assert!((field_f64(&alert, "__wfu_score") - 20.0).abs() < f64::EPSILON);
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.8");
    assert_eq!(field_f64(&alert, "avg_score"), 20.0);
    assert_eq!(field_f64(&alert, "avg_risk"), 20.0);
    assert_eq!(field_f64(&alert, "event_count"), 2.0);
}

#[tokio::test]
async fn downstream_close_counts_filtered_bind_aliases() {
    init_tracing();
    let schema = scored_source_schema();
    let (mut upstream_task, mut downstream_task, mut alert_rx, router) =
        make_intermediate_score_band_tasks();
    let ts = 4_000_000_000_000_000i64;

    let batch = make_scored_batch(&schema, &["10.0.0.9", "10.0.0.9"], &[90.0, 70.0], ts);
    let source = router.registry().get_window("auth_events").unwrap();
    source.append(batch).unwrap();
    // 推进 watermark 到 fixed 1s 桶 [ts, ts+1s) 完整（w_end ≤ wm）——2026-08-23
    // close_all 对齐 oracle/Flink：尾部未完整窗口（w_end > 最终事件时间）不输出。
    source
        .append(make_scored_batch(
            &schema,
            &["10.0.0.1"],
            &[1.0],
            ts + 2_000_000_000,
        ))
        .unwrap();

    // Pure relay (P1c): the intermediate pipe is not stored in a window; the
    // downstream rule consumes the broadcast via push.
    let (down_tx, mut down_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    router.fanout().register("semantic_events", down_tx);

    upstream_task.pull_and_advance().await;
    while let Ok(push) = down_rx.try_recv() {
        downstream_task.process_push(push).await;
    }
    downstream_task.flush().await;

    let alert = take_alert_recv(&mut alert_rx).await;
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.9");
    assert_eq!(field_f64(&alert, "event_count"), 2.0);
    assert_eq!(field_f64(&alert, "source_avg"), 80.0);
    assert_eq!(field_f64(&alert, "high_event_count"), 1.0);
    assert_eq!(field_f64(&alert, "elevated_event_count"), 2.0);
    assert_eq!(field_str(&alert, "status"), "high");
}

#[tokio::test]
async fn match_event_path_counts_filtered_bind_aliases() {
    init_tracing();
    let schema = scored_source_schema();
    let (mut task, mut alert_rx, win, _notify) = make_filtered_bind_alias_match_task();
    let ts = 4_000_000_000_000_000i64;
    let batch = make_scored_batch(&schema, &["10.0.0.7", "10.0.0.7"], &[90.0, 70.0], ts);
    win.append(batch).unwrap();

    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.7");
    assert_eq!(field_f64(&alert, "__wfu_score"), 1.0);
    assert_eq!(field_f64(&alert, "high_event_count"), 1.0);
    assert_eq!(field_f64(&alert, "elevated_avg"), 80.0);
    assert_eq!(field_str(&alert, "last_high_sip"), "10.0.0.7");
}

#[tokio::test]
async fn on_each_emits_one_alert_per_matching_row() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, win, _notify) = make_each_task();
    let ts_nanos = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.2"], ts_nanos);
    win.append(batch).unwrap();

    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "each_rule");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert_eq!(field_str(&alert, "__wfu_origin"), "event");
    assert!(!field_str(&alert, "__wfu_fired_at").is_empty());
    assert_eq!(field_str(&alert, "x"), "10.0.0.1");
    assert!(
        alert_rx.try_recv().is_err(),
        "non-matching rows must not emit alerts"
    );
}

#[tokio::test]
async fn match_respects_events_bind_filter() {
    init_tracing();
    let schema = filtered_schema();
    let (mut task, mut alert_rx, win, _notify) = make_filtered_match_task();
    let ts = 4_000_000_000_000_000i64;

    let batch1 = make_filtered_batch(
        &schema,
        &["10.0.0.1", "10.0.0.1"],
        &["failed", "success"],
        ts,
    );
    win.append(batch1).unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "non-matching bind-filter rows must not count toward the match"
    );

    let batch2 = make_filtered_batch(&schema, &["10.0.0.1"], &["failed"], ts + 1);
    win.append(batch2).unwrap();
    task.pull_and_advance().await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "filtered_match");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
}

#[tokio::test]
async fn match_bind_filter_supports_window_has_lookup() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, router) = make_window_has_match_task();
    let ts = 4_000_000_000_000_000i64;

    let lookup_batch = make_batch(&schema, &["10.0.0.1"], ts - 1);
    let lookup = router.registry().get_window("threat_list").unwrap();
    lookup.append(lookup_batch).unwrap();

    let source_batch = make_batch(&schema, &["10.0.0.1", "10.0.0.2"], ts);
    let source = router.registry().get_window("auth_events").unwrap();
    source.append(source_batch).unwrap();

    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "window_has_match");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!(
        alert_rx.try_recv().is_err(),
        "rows rejected by window.has bind filter must not match"
    );
}

#[tokio::test]
async fn on_each_respects_events_bind_filter() {
    init_tracing();
    let schema = filtered_schema();
    let (mut task, mut alert_rx, win, _notify) = make_filtered_each_task();
    let ts = 4_000_000_000_000_000i64;
    let batch = make_filtered_batch(
        &schema,
        &["10.0.0.1", "10.0.0.1"],
        &["failed", "success"],
        ts,
    );
    win.append(batch).unwrap();

    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "filtered_each");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!(
        alert_rx.try_recv().is_err(),
        "rows rejected by bind filter must not emit alerts"
    );
}

// -- port_scan regression test ---------------------------------------------

/// Schema matching the conn_events window from network.wfs.
fn conn_events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new("dip", DataType::Utf8, true),
        Field::new("dport", DataType::Int64, true),
        Field::new("bytes_out", DataType::Int64, true),
        Field::new("action", DataType::Utf8, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

/// Build a window with the conn_events schema.
fn make_conn_events_window(max_bytes: usize) -> (Arc<Window>, Arc<Notify>) {
    let schema = conn_events_schema();
    let mut cfg = test_window_config(max_bytes);
    cfg.name = "conn_events".to_string();
    let win = Window::new(
        WindowParams {
            name: "conn_events".into(),
            schema: schema.clone(),
            time_col_index: Some(5), // event_time is the 6th column (0-based: 5)
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        cfg,
    );
    let win_arc = Arc::new(win);
    let notify_arc = Arc::new(Notify::new());
    (win_arc, notify_arc)
}

/// Build a RecordBatch matching port_scan data: same sip, varying dport, action=syn.
fn make_port_scan_batch(sip: &str, dports: &[i64], ts_base: i64) -> RecordBatch {
    let n = dports.len();
    let schema = conn_events_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![sip; n])),
            Arc::new(StringArray::from(vec!["10.0.0.2"; n])),
            Arc::new(Int64Array::from(dports.to_vec())),
            Arc::new(Int64Array::from(vec![0i64; n])),
            Arc::new(StringArray::from(vec!["syn"; n])),
            Arc::new(TimestampNanosecondArray::from(
                (0..n as i64).map(|i| ts_base + i).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Regression test: port_scan rule with close steps and tracked_bind_aliases.
///
/// Verifies that events flow from the window through the rule_task and
/// produce a close alert when the window expires.
#[tokio::test]
async fn port_scan_rule_triggers_close_alert() {
    init_tracing();
    let (win_arc, notify_arc) = make_conn_events_window(usize::MAX);

    // port_scan-like MatchPlan
    let match_plan = MatchPlan {
        keys: vec![FieldRef::Qualified("c".into(), "sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(10)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "c".into(),
                field: None, // count(c) — aggregate the event itself
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(2.0),
                },
            }],
        }],
        close_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "c".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(2.0),
                },
            }],
        }],
        close_mode: CloseMode::And,
        tracked_bind_aliases: std::collections::HashSet::from(["c".to_string()]),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };

    let rule_plan = RulePlan {
        conv_window: None,
        name: "port_scan".into(),
        binds: vec![BindPlan {
            alias: "c".into(),
            window: "conn_events".into(),
            filter: Some(Expr::BinOp {
                left: Box::new(Expr::Field(FieldRef::Qualified(
                    "c".into(),
                    "action".into(),
                ))),
                op: BinOp::Eq,
                right: Box::new(Expr::StringLit("syn".into())),
            }),
        }],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("c".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "network_alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "sip".into(),
                value: Expr::Field(FieldRef::Qualified("c".into(), "sip".into())),
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(80.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let machine = CepStateMachine::new("port_scan".into(), match_plan, Some("event_time".into()));
    let executor = RuleExecutor::new(rule_plan);

    let (alert_tx, mut alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));

    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: Some(machine),
        each_alias: None,
        each_time_field: None,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "conn_events".into(),
            window: Arc::clone(&win_arc),
            notify: Arc::clone(&notify_arc),
            aliases: vec!["c".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router,
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };

    let (mut task, _cancel, _interval) = rule_task::RuleTask::new(config);

    // Feed batch: 5 events with same sip, action=syn
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_port_scan_batch("10.0.0.1", &[80, 443, 22, 8080, 3306], ts);
    win_arc.append(batch).unwrap();

    task.pull_and_advance().await;

    // No matched alert (close mode is AND)
    assert!(
        alert_rx.try_recv().is_err(),
        "AND mode should not emit on-event match"
    );

    // Feed second batch with later timestamps to trigger expiry (ts + 11s > created_at + 10s)
    let nanos_per_sec: i64 = 1_000_000_000;
    let batch2 = make_port_scan_batch("10.0.0.1", &[21, 25, 53], ts + 11 * nanos_per_sec);
    win_arc.append(batch2).unwrap();

    task.pull_and_advance().await;

    // Should have a close alert now
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "port_scan");
    assert_eq!(field_str(&alert, "__wfu_entity_type"), "ip");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
}

#[tokio::test]
async fn pure_relay_broadcasts_to_sharded_downstream() {
    init_tracing();
    let schema = test_schema();
    let (mut task, _alert_rx, router) = make_pipeline_stage_task();
    // Two shards keyed by sip (P2a sharding on the intermediate pipe).
    let (shard_a_tx, mut shard_a_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    let (shard_b_tx, mut shard_b_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    router.fanout().register_sharded(
        "__wf_pipe_pipe_s1_w1",
        vec![shard_a_tx, shard_b_tx],
        std::sync::Arc::from([FieldRef::Simple("sip".into())]),
    );

    let ts = 1_700_000_000_123_000_000i64;
    // Two events with the SAME key → the pure-relay broadcast must keep them on
    // the same shard (deterministic key hash), even though nothing is stored.
    let batch = make_batch(&schema, &["10.0.0.8", "10.0.0.8"], ts);
    let source = router.registry().get_window("auth_events").unwrap();
    source.append(batch).unwrap();
    task.pull_and_advance().await;

    // Pure relay: nothing stored in the intermediate window.
    // 2026-08-23 行为变更：pipe flush 同时 append 目标窗口（pull 消费方读取
    // 分片行子集）——shard_rows 由 fanout 预计算，append 带分区。
    assert!(
        !router
            .registry()
            .snapshot("__wf_pipe_pipe_s1_w1")
            .unwrap_or_default()
            .is_empty(),
        "sharded pipe flush must append the internal window"
    );

    let a: Vec<_> = std::iter::from_fn(|| shard_a_rx.try_recv().ok()).collect();
    let b: Vec<_> = std::iter::from_fn(|| shard_b_rx.try_recv().ok()).collect();
    let (full, empty) = if a.len() > b.len() { (a, b) } else { (b, a) };
    // Rule-side channelization: rows of one input batch relay as a single
    // pushed batch (all same-key events together, in emit order).
    assert!(
        full.len() == 1 && full[0].events.as_ref().unwrap().len() == 2,
        "same-key events must land together on the same shard (one batched push), got {} pushes",
        full.len()
    );
    assert!(
        empty.is_empty(),
        "the other shard must stay empty for the same key"
    );
    // Pure relay carries the real window-batch seq (append 返回的真实 seq，
    // 非 u64::MAX sentinel)——2026-08-23 q13：此前固定 u64::MAX 使下游
    // push 规则的 ack 不反映真实消费进度（acked_lag 恒 0，bench 完成判定
    // 提前 SIGTERM）。首批 append seq 从 0 起。
    assert_eq!(full[0].seq, 0, "relay pushes carry the real append seq");
    assert_eq!(
        full[0].events.as_ref().unwrap()[0].fields.get("sip"),
        Some(&wf_engine::match_engine::Value::Str("10.0.0.8".into()))
    );
    assert_eq!(
        full[0].events.as_ref().unwrap()[1].fields.get("sip"),
        Some(&wf_engine::match_engine::Value::Str("10.0.0.8".into()))
    );

    // Flush boundary: rows of a SECOND input batch relay as their own push
    // (per-input-batch flush), in order, on the same shard.
    let batch2 = make_batch(&schema, &["10.0.0.8"], ts + 1_000_000);
    source.append(batch2).unwrap();
    task.pull_and_advance().await;
    // Re-drain both shards; the new row must appear as one extra push.
    let a2: Vec<_> = std::iter::from_fn(|| shard_a_rx.try_recv().ok()).collect();
    let b2: Vec<_> = std::iter::from_fn(|| shard_b_rx.try_recv().ok()).collect();
    assert_eq!(
        a2.len() + b2.len(),
        1,
        "second input batch relays as exactly one more push"
    );
    let (second, _) = if !a2.is_empty() {
        (&a2[0], ())
    } else {
        (&b2[0], ())
    };
    assert_eq!(
        second.events.as_ref().unwrap()[0].fields.get("sip"),
        Some(&wf_engine::match_engine::Value::Str("10.0.0.8".into()))
    );
}

/// An input batch that produces no intermediate rows must not broadcast:
/// flushing an empty stager is a no-op on the pipe channel.
#[tokio::test]
async fn pipe_relay_empty_input_batch_sends_nothing() {
    init_tracing();
    let schema = test_schema();
    let (mut task, _alert_rx, router) = make_pipeline_stage_task();
    let (down_tx, mut down_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    router.fanout().register("__wf_pipe_pipe_s1_w1", down_tx);

    let source = router.registry().get_window("auth_events").unwrap();
    let ts = 1_700_000_000_123_000_000i64;
    source.append(make_batch(&schema, &[], ts)).unwrap();
    task.pull_and_advance().await;

    assert!(
        down_rx.try_recv().is_err(),
        "empty flush must not broadcast anything"
    );
}

/// A pipe target with no window and no pipe-registry entry degrades to
/// `PipeState::Dead`: rows are dropped with a warning, the task keeps
/// running (no panic, no hang), and nothing reaches sink or pipe channel.
#[tokio::test]
async fn pipe_missing_target_degrades_to_dead_without_panic() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, router) = make_pipeline_stage_task_opts(false);
    let (down_tx, mut down_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    router.fanout().register("__wf_pipe_pipe_s1_w1", down_tx);

    let source = router.registry().get_window("auth_events").unwrap();
    let ts = 1_700_000_000_123_000_000i64;
    source
        .append(make_batch(&schema, &["10.0.0.8", "10.0.0.9"], ts))
        .unwrap();
    // Uninit -> resolve fails -> Dead; must complete instead of hanging.
    task.pull_and_advance().await;
    assert!(down_rx.try_recv().is_err(), "dead pipe must not broadcast");
    assert!(
        alert_rx.try_recv().is_err(),
        "intermediate emit must not fall through to the sink"
    );

    // Second batch exercises the Dead fast path (silent drop).
    source
        .append(make_batch(&schema, &["10.0.0.8"], ts + 1_000_000))
        .unwrap();
    task.pull_and_advance().await;
    assert!(down_rx.try_recv().is_err());
}

/// Backpressure: while the downstream subscriber channel is full, the
/// end-of-batch pipe flush blocks the rule task; once a slot frees up the
/// pending flush completes and delivers its batch in order.
#[tokio::test]
async fn pipe_flush_backpressures_until_downstream_drains() {
    init_tracing();
    let schema = test_schema();
    let (mut task, _alert_rx, router) = make_pipeline_stage_task();
    let (down_tx, mut down_rx) = mpsc::channel::<wf_engine::window::RulePush>(1);
    router.fanout().register("__wf_pipe_pipe_s1_w1", down_tx);

    let source = router.registry().get_window("auth_events").unwrap();
    let ts = 1_700_000_000_123_000_000i64;
    // Batch 1: one staged flush fills (and stays in) the single-slot channel.
    source
        .append(make_batch(&schema, &["10.0.0.8", "10.0.0.8"], ts))
        .unwrap();
    task.pull_and_advance().await;

    // Batch 2: flush blocks on the full channel (backpressure on emit).
    source
        .append(make_batch(&schema, &["10.0.0.9"], ts + 1_000_000))
        .unwrap();
    let mut pending = std::pin::pin!(task.pull_and_advance());
    let blocked = tokio::time::timeout(std::time::Duration::from_millis(150), &mut pending).await;
    assert!(
        blocked.is_err(),
        "pipe flush must block while the subscriber channel is full"
    );

    // Drain the first push; the blocked flush must then complete and
    // deliver batch 2 in order.
    let first = down_rx.recv().await.expect("first push");
    assert_eq!(first.events.as_ref().unwrap().len(), 2);
    pending.await;
    let second = down_rx
        .recv()
        .await
        .expect("second push after backpressure");
    assert_eq!(second.events.as_ref().unwrap().len(), 1);
    assert_eq!(
        second.events.as_ref().unwrap()[0].fields.get("sip"),
        Some(&wf_engine::match_engine::Value::Str("10.0.0.9".into()))
    );
}

// ---------------------------------------------------------------------------
// P2c regression: conv-sink barrier watermark
// ---------------------------------------------------------------------------

/// Build a RuleTask for a raw-conv-mode shard whose closes (and barrier
/// watermarks) land in the returned conv channel.
fn make_conv_sink_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::engine_task::ConvCloseBatch>,
) {
    let schema = test_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, usize::MAX);
    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(60)),
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
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    };
    let rule_plan = RulePlan {
        conv_window: None,
        name: "conv_sink_rule".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: "auth_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("fail".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(70.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };
    let mut machine = CepStateMachine::new(
        "conv_sink_rule".into(),
        match_plan,
        Some("event_time".into()),
    );
    machine.set_raw_conv_mode();
    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, _alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (conv_tx, conv_rx) = mpsc::channel::<crate::engine_task::ConvCloseBatch>(4);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: Some(crate::engine_task::ConvShardSink {
            tx: conv_tx,
            barrier_index: 0,
        }),
        machine: Some(machine),
        each_alias: None,
        each_time_field: None,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: win_arc,
            notify: notify_arc,
            aliases: vec!["fail".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router: Arc::new(Router::new(WindowRegistry::build(vec![]).unwrap())),
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, conv_rx)
}

#[tokio::test]
async fn conv_sink_process_batch_barrier_tracks_event_time() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut conv_rx) = make_conv_sink_task();
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1"], ts);
    let push = RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::new(
            batch_to_events(&batch)
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>(),
        )),
        batch: None,
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    };
    task.process_push(push).await;
    let b = conv_rx
        .try_recv()
        .expect("conv stage should receive a barrier batch");
    // Regression: the barrier must be the scan (event-time) watermark, not the
    // machine's cached watermark (which only advances during `advance`, after
    // the scan).
    assert_eq!(
        b.watermark, ts,
        "barrier watermark must equal the event time (scan watermark)"
    );
}

#[tokio::test]
async fn conv_sink_scan_timeouts_advances_barrier_by_wall_clock() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut conv_rx) = make_conv_sink_task();
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1"], ts);
    let push = RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::new(
            batch_to_events(&batch)
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>(),
        )),
        batch: None,
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    };
    task.process_push(push).await;
    let _ = conv_rx.try_recv(); // drain the process_batch barrier batch

    // Simulate an idle shard: wall clock advances with no new events.
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.scan_timeouts().await;

    let b = conv_rx
        .try_recv()
        .expect("scan_timeouts should send a barrier batch");
    // Regression: an idle shard's barrier must advance with wall-clock (the
    // effective scan watermark), otherwise the conv stage never seals buckets
    // for the whole rule (starvation).
    assert!(
        b.watermark > ts,
        "idle shard barrier must advance by wall-clock, got {} (stale machine watermark)",
        b.watermark
    );
}

// ---------------------------------------------------------------------------
// P2c: conv stage end-to-end emit (regression for drop-on-full delivery path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn conv_stage_emits_sealed_close_to_sink() {
    init_tracing();
    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(60)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("count".into()),
                source: "fail".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: CloseMode::And,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    };
    let rule_plan = RulePlan {
        conv_window: None,
        name: "conv_stage_rule".into(),
        binds: vec![],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Simple("sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(70.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };
    let executor = RuleExecutor::new(rule_plan);

    let (alert_tx, mut alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (conv_tx, conv_rx) = mpsc::channel::<crate::engine_task::ConvCloseBatch>(4);
    let barrier: Arc<Vec<std::sync::atomic::AtomicI64>> =
        Arc::new(vec![std::sync::atomic::AtomicI64::new(i64::MIN)]);
    let config = crate::engine_task::ConvStageConfig {
        executor,
        conv_plan: None,
        keys: Arc::new([FieldRef::Simple("sip".into())]),
        over: Duration::from_secs(60),
        bucket_align: Duration::from_secs(60),
        limits: None,
        shared_limits: None,
        barrier,
        sink_fanout: make_test_fanout(alert_tx),
        router: Arc::new(Router::new(WindowRegistry::build(vec![]).unwrap())),
        metrics: None,
        rx: conv_rx,
        cancel: tokio_util::sync::CancellationToken::new(),
        eos: tokio::sync::watch::channel(0u64).1,
        timeout_scan_interval: Duration::from_secs(60),
    };
    let _stage = tokio::spawn(async move { crate::engine_task::run_conv_stage_task(config).await });

    // A qualified close in bucket 0; `drained` lifts the barrier so the bucket
    // is sealed and the close is emitted to the sink.
    let close = wf_engine::match_engine::CloseOutput {
        rule_name: "conv_stage_rule".into(),
        scope_key: vec![wf_engine::match_engine::Value::Str("10.0.0.1".into())],
        close_reason: wf_engine::match_engine::CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![wf_engine::match_engine::StepData {
            satisfied_branch_index: 0,
            label: Some("count".into()),
            measure_value: 1.0,
            event_first_time_nanos: Some(0),
            event_last_time_nanos: Some(0),
            collected_values: vec![],
            field_values: Default::default(),
        }],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 0,
        machine_id: "m".into(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 60_000_000_000,
        first_match_time_nanos: None,
        last_event_nanos: 0,
        row_fields: None,
        row_field_names: None,
    };
    conv_tx
        .send(crate::engine_task::ConvCloseBatch {
            closes: vec![close],
            watermark: 0,
            drained: true,
            barrier_index: 0,
        })
        .await
        .unwrap();
    // Drop the sender so the stage drains and exits after sealing.
    drop(conv_tx);

    let alert = take_alert_recv(&mut alert_rx).await;
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "conv_stage_rule");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!((field_f64(&alert, "__wfu_score") - 70.0).abs() < f64::EPSILON);
}

// ---------------------------------------------------------------------------
// P1① / P2③ / P2④ — conv stage regression tests
// ---------------------------------------------------------------------------

/// Build the RuleExecutor used by the conv-stage tests (yields `alerts`,
/// entity `ip`, score 70).
fn conv_stage_test_executor() -> RuleExecutor {
    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Fixed(Duration::from_secs(60)),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("count".into()),
                source: "fail".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: CloseMode::And,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    };
    let rule_plan = RulePlan {
        conv_window: None,
        name: "conv_stage_rule".into(),
        binds: vec![],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Simple("sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(70.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };
    RuleExecutor::new(rule_plan)
}

/// A qualified close for bucket 0.
fn conv_stage_test_close() -> wf_engine::match_engine::CloseOutput {
    wf_engine::match_engine::CloseOutput {
        rule_name: "conv_stage_rule".into(),
        scope_key: vec![wf_engine::match_engine::Value::Str("10.0.0.1".into())],
        close_reason: wf_engine::match_engine::CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![wf_engine::match_engine::StepData {
            satisfied_branch_index: 0,
            label: Some("count".into()),
            measure_value: 1.0,
            event_first_time_nanos: Some(0),
            event_last_time_nanos: Some(0),
            collected_values: vec![],
            field_values: Default::default(),
        }],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 0,
        machine_id: "m".into(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 60_000_000_000,
        first_match_time_nanos: None,
        last_event_nanos: 0,
        row_fields: None,
        row_field_names: None,
    }
}

#[allow(clippy::type_complexity)]
fn make_conv_stage_config(
    limits: Option<wf_lang::plan::LimitsPlan>,
    shared_limits: Option<std::sync::Arc<wf_engine::match_engine::SharedLimits>>,
    barrier: Arc<Vec<std::sync::atomic::AtomicI64>>,
    cancel: tokio_util::sync::CancellationToken,
) -> (
    crate::engine_task::ConvStageConfig,
    mpsc::Sender<crate::engine_task::ConvCloseBatch>,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
) {
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (conv_tx, conv_rx) = mpsc::channel::<crate::engine_task::ConvCloseBatch>(4);
    let config = crate::engine_task::ConvStageConfig {
        executor: conv_stage_test_executor(),
        conv_plan: None,
        keys: Arc::new([FieldRef::Simple("sip".into())]),
        over: Duration::from_secs(60),
        bucket_align: Duration::from_secs(60),
        limits,
        shared_limits,
        barrier,
        sink_fanout: make_test_fanout(alert_tx),
        router: Arc::new(Router::new(WindowRegistry::build(vec![]).unwrap())),
        metrics: None,
        rx: conv_rx,
        cancel,
        eos: tokio::sync::watch::channel(0u64).1,
        timeout_scan_interval: Duration::from_secs(60),
    };
    (config, conv_tx, alert_rx)
}

/// P2c hop 扩展（2026-08-24）：conv stage 的桶键按 `bucket_align`（hop = slide）
/// 对齐，封口长度仍用 `over`（hop = size）。
///
/// 判别性设计：close window_start = 6s / 16s（hop 收口事件 window_start =
/// k*slide，2s 对齐 → 桶 6s / 16s；若误用 `over`（10s）对齐 → 桶 0s / 10s）。
/// barrier 水位 = 20s：正确逻辑只封 6s 桶（6s+10s ≤ 20s），16s 桶未封
///（16s+10s > 20s）→ 只输出 1 条；错误对齐会封两个桶（0s+10s、10s+10s 均
/// ≤ 20s）→ 输出 2 条。
#[tokio::test]
async fn conv_stage_hop_bucket_aligns_to_slide_seals_by_size() {
    init_tracing();
    let (alert_tx, mut alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (conv_tx, conv_rx) = mpsc::channel::<crate::engine_task::ConvCloseBatch>(4);
    let barrier: Arc<Vec<std::sync::atomic::AtomicI64>> =
        Arc::new(vec![std::sync::atomic::AtomicI64::new(i64::MIN)]);
    let config = crate::engine_task::ConvStageConfig {
        executor: conv_stage_test_executor(),
        conv_plan: None,
        keys: Arc::new([FieldRef::Simple("sip".into())]),
        over: Duration::from_secs(10),        // hop size：封口长度
        bucket_align: Duration::from_secs(2), // hop slide：桶对齐
        limits: None,
        shared_limits: None,
        barrier: Arc::clone(&barrier),
        sink_fanout: make_test_fanout(alert_tx),
        router: Arc::new(Router::new(WindowRegistry::build(vec![]).unwrap())),
        metrics: None,
        rx: conv_rx,
        cancel: tokio_util::sync::CancellationToken::new(),
        eos: tokio::sync::watch::channel(0u64).1,
        timeout_scan_interval: Duration::from_secs(60),
    };
    let _stage = tokio::spawn(async move { crate::engine_task::run_conv_stage_task(config).await });

    let mut close_6 = conv_stage_test_close();
    close_6.window_start_time_nanos = 6_000_000_000; // 桶 = 6s（2s 对齐）
    close_6.scope_key = vec![wf_engine::match_engine::Value::Str("a".into())];
    let mut close_16 = conv_stage_test_close();
    close_16.window_start_time_nanos = 16_000_000_000; // 桶 = 16s
    close_16.scope_key = vec![wf_engine::match_engine::Value::Str("b".into())];
    conv_tx
        .send(crate::engine_task::ConvCloseBatch {
            closes: vec![close_6, close_16],
            watermark: 20_000_000_000,
            drained: false,
            barrier_index: 0,
        })
        .await
        .unwrap();

    // barrier=20s：只 6s 桶封口（6+10=16 ≤ 20）；16s 桶（16+10=26 > 20）不封。
    let alert = take_alert_recv(&mut alert_rx).await;
    assert_eq!(
        field_str(&alert, "__wfu_entity_id"),
        "a",
        "只有 6s 桶（2s 对齐）应封口输出"
    );
    // 16s 桶未封口：关停 stage 时被丢弃（partial 不输出），无第二条。
    drop(conv_tx);
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while alert_rx.try_recv().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .ok();
    assert!(
        alert_rx.try_recv().is_err(),
        "未封口的 16s 桶不得输出（hop 分片语义：桶级全局聚合）"
    );
}

/// P2c hop 分片（2026-08-24）：**跨分片**全局聚合 + `top_ties` 语义。
///
/// 两个分片各自收口自己那部分 auction 的 close（同 bucket、不同 count），
/// 路由到 conv stage 后：按 slide 对齐分桶 → barrier 等齐 → 桶封口时全局
/// `apply_conv(sort(-count) | top_ties(1))` —— 必须取**跨分片**最高 count，
/// 而非片内 top（片内 top 会错：分片 0 只有 count=5）。
///
/// 判别性：分片 0 发 a(count=5)，分片 1 发 b(count=9)（同桶 6s）→ 输出必须是
/// b（count=9）；若 conv stage 误按片聚合会输出 a（错误）。另发 c(count=7,
/// 桶 16s) 验证封口长度（16+10=26 > barrier 20 → 不封，不输出）。
#[tokio::test]
async fn conv_stage_hop_shards_aggregate_globally_top_ties() {
    init_tracing();
    let (alert_tx, mut alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (conv_tx, conv_rx) = mpsc::channel::<crate::engine_task::ConvCloseBatch>(4);
    let barrier: Arc<Vec<std::sync::atomic::AtomicI64>> = Arc::new(vec![
        std::sync::atomic::AtomicI64::new(i64::MIN),
        std::sync::atomic::AtomicI64::new(i64::MIN),
    ]);
    let sort_key = wf_lang::plan::SortKeyPlan {
        expr: wf_lang::plan::ExprPlan::Field(wf_lang::ast::FieldRef::Simple("count".into())),
        descending: true,
    };
    let config = crate::engine_task::ConvStageConfig {
        executor: conv_stage_test_executor(),
        conv_plan: Some(wf_lang::plan::ConvPlan {
            chains: vec![wf_lang::plan::ConvChainPlan {
                ops: vec![
                    wf_lang::plan::ConvOpPlan::Sort(vec![sort_key.clone()]),
                    wf_lang::plan::ConvOpPlan::TopTies {
                        n: 1,
                        sort_keys: vec![sort_key],
                    },
                ],
            }],
        }),
        keys: Arc::new([FieldRef::Simple("sip".into())]),
        over: Duration::from_secs(10),        // hop size
        bucket_align: Duration::from_secs(2), // hop slide
        limits: None,
        shared_limits: None,
        barrier: Arc::clone(&barrier),
        sink_fanout: make_test_fanout(alert_tx),
        router: Arc::new(Router::new(WindowRegistry::build(vec![]).unwrap())),
        metrics: None,
        rx: conv_rx,
        cancel: tokio_util::sync::CancellationToken::new(),
        eos: tokio::sync::watch::channel(0u64).1,
        timeout_scan_interval: Duration::from_secs(60),
    };
    let _stage = tokio::spawn(async move { crate::engine_task::run_conv_stage_task(config).await });

    // 分片 0：a(count=5, 桶 6s)，水位 20s。
    let mut close_a = conv_stage_test_close();
    close_a.window_start_time_nanos = 6_000_000_000;
    close_a.scope_key = vec![wf_engine::match_engine::Value::Str("a".into())];
    close_a.event_step_data[0].measure_value = 5.0;
    conv_tx
        .send(crate::engine_task::ConvCloseBatch {
            closes: vec![close_a],
            watermark: 20_000_000_000,
            drained: false,
            barrier_index: 0,
        })
        .await
        .unwrap();

    // 分片 1：b(count=9, 同桶 6s) + c(count=7, 桶 16s)，水位 20s。
    let mut close_b = conv_stage_test_close();
    close_b.window_start_time_nanos = 6_000_000_000;
    close_b.scope_key = vec![wf_engine::match_engine::Value::Str("b".into())];
    close_b.event_step_data[0].measure_value = 9.0;
    let mut close_c = conv_stage_test_close();
    close_c.window_start_time_nanos = 16_000_000_000;
    close_c.scope_key = vec![wf_engine::match_engine::Value::Str("c".into())];
    close_c.event_step_data[0].measure_value = 7.0;
    conv_tx
        .send(crate::engine_task::ConvCloseBatch {
            closes: vec![close_b, close_c],
            watermark: 20_000_000_000,
            drained: false,
            barrier_index: 1,
        })
        .await
        .unwrap();

    // 两分片水位都到 20s：桶 6s 封口（6+10 ≤ 20），全局 top_ties → b（count=9）。
    let alert = take_alert_recv(&mut alert_rx).await;
    assert_eq!(
        field_str(&alert, "__wfu_entity_id"),
        "b",
        "跨分片全局聚合必须取 count 最高者（片内 top 会错选 a）"
    );
    // 桶 16s 未封（16+10=26 > 20），关停时丢弃，不输出。
    drop(conv_tx);
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while alert_rx.try_recv().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .ok();
    assert!(alert_rx.try_recv().is_err(), "未封口的 16s 桶不得输出");
}

// P1①: conv-stage throttle over-limit must dispatch on_exceed — FailRule
// latches the shared rule (matching the inline close path), and later batches
// are not emitted.
#[tokio::test]
async fn conv_stage_throttle_failrule_latches_shared() {
    init_tracing();
    let shared = wf_engine::match_engine::SharedLimits::new();
    let limits = wf_lang::plan::LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(wf_lang::plan::RateSpec {
            count: 1,
            per: Duration::from_secs(60),
        }),
        on_exceed: wf_lang::plan::ExceedAction::FailRule,
        disk_provider: None,
        max_disk_bytes: None,
    };
    let barrier: Arc<Vec<std::sync::atomic::AtomicI64>> =
        Arc::new(vec![std::sync::atomic::AtomicI64::new(i64::MIN)]);
    let cancel = tokio_util::sync::CancellationToken::new();
    let (config, conv_tx, mut alert_rx) = make_conv_stage_config(
        Some(limits),
        Some(std::sync::Arc::clone(&shared)),
        barrier,
        cancel.clone(),
    );
    let _stage = tokio::spawn(async move { crate::engine_task::run_conv_stage_task(config).await });

    // Two qualified closes at the same watermark: the 1st is within the shared
    // budget (count=1), the 2nd is throttled → FailRule must latch.
    let close = conv_stage_test_close();
    conv_tx
        .send(crate::engine_task::ConvCloseBatch {
            closes: vec![close.clone(), close],
            watermark: 0,
            drained: true,
            barrier_index: 0,
        })
        .await
        .unwrap();
    drop(conv_tx);

    let alert = take_alert_recv(&mut alert_rx).await;
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "conv_stage_rule");
    assert!(alert_rx.try_recv().is_err(), "2nd close must be throttled");
    assert!(
        shared.is_failed(),
        "FailRule must latch the shared rule (not silently degrade to Throttle)"
    );
    cancel.cancel();
}

// N3: after a FailRule latch fires mid-bucket, the REST of the bucket must be
// suppressed too — a later close whose watermark falls into a fresh throttle
// window would otherwise pass try_acquire_throttle and emit after the latch.
#[tokio::test]
async fn conv_stage_failrule_latch_suppresses_rest_of_bucket() {
    init_tracing();
    let shared = wf_engine::match_engine::SharedLimits::new();
    let limits = wf_lang::plan::LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(wf_lang::plan::RateSpec {
            count: 1,
            per: Duration::from_secs(60),
        }),
        on_exceed: wf_lang::plan::ExceedAction::FailRule,
        disk_provider: None,
        max_disk_bytes: None,
    };
    let barrier: Arc<Vec<std::sync::atomic::AtomicI64>> =
        Arc::new(vec![std::sync::atomic::AtomicI64::new(i64::MIN)]);
    let cancel = tokio_util::sync::CancellationToken::new();
    let (config, conv_tx, mut alert_rx) = make_conv_stage_config(
        Some(limits),
        Some(std::sync::Arc::clone(&shared)),
        barrier,
        cancel.clone(),
    );
    let _stage = tokio::spawn(async move { crate::engine_task::run_conv_stage_task(config).await });

    // close1 @wm=0: within budget → emits. close2 @wm=0: throttled → FailRule
    // latches. close3 @wm=61s: FRESH throttle window — without the mid-bucket
    // break it would acquire the new window's budget and emit after the latch.
    let mut close_fresh_window = conv_stage_test_close();
    close_fresh_window.watermark_nanos = 61_000_000_000;
    conv_tx
        .send(crate::engine_task::ConvCloseBatch {
            closes: vec![
                conv_stage_test_close(),
                conv_stage_test_close(),
                close_fresh_window,
            ],
            watermark: 61_000_000_000,
            drained: true,
            barrier_index: 0,
        })
        .await
        .unwrap();
    drop(conv_tx);

    let alert = take_alert_recv(&mut alert_rx).await;
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "conv_stage_rule");
    assert!(
        alert_rx.try_recv().is_err(),
        "close3 (fresh throttle window) must be suppressed by the FailRule latch"
    );
    assert!(shared.is_failed());
    cancel.cancel();
}

// P2③: one ConvCloseBatch per process_batch (max event-time watermark), not
// one per event.
#[tokio::test]
async fn conv_sink_sends_one_batch_per_process_batch() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut conv_rx) = make_conv_sink_task();
    let ts = 1_700_000_000_000_000_000i64;
    // 3 events in one pushed batch.
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.2", "10.0.0.1"], ts);
    let push = RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::new(
            batch_to_events(&batch)
                .into_iter()
                .map(Arc::new)
                .collect::<Vec<_>>(),
        )),
        batch: None,
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    };
    task.process_push(push).await;

    let b = conv_rx
        .try_recv()
        .expect("process_batch must send exactly one ConvCloseBatch");
    assert!(
        conv_rx.try_recv().is_err(),
        "per-batch aggregation must send ONE batch, not one per event"
    );
    assert!(
        b.watermark >= ts,
        "barrier watermark must be the max event-time in the batch, got {}",
        b.watermark
    );
}

// P2④: unsealed (partial) buckets are DROPPED on cancel — never emitted as
// wrong top(N)/sort results.
#[tokio::test]
async fn conv_stage_cancel_drops_unsealed_buckets() {
    init_tracing();
    // Barrier stuck at 0: bucket 0 needs min watermark >= 60s to seal.
    let barrier: Arc<Vec<std::sync::atomic::AtomicI64>> =
        Arc::new(vec![std::sync::atomic::AtomicI64::new(0)]);
    let cancel = tokio_util::sync::CancellationToken::new();
    let (config, conv_tx, mut alert_rx) =
        make_conv_stage_config(None, None, barrier, cancel.clone());
    let _stage = tokio::spawn(async move { crate::engine_task::run_conv_stage_task(config).await });

    // A qualified close in bucket 0, NOT drained (barrier stays 0 → never seals).
    conv_tx
        .send(crate::engine_task::ConvCloseBatch {
            closes: vec![conv_stage_test_close()],
            watermark: 0,
            drained: false,
            barrier_index: 0,
        })
        .await
        .unwrap();
    // Give the stage a beat to receive the batch, then cancel.
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    drop(conv_tx);

    assert!(
        alert_rx.try_recv().is_err(),
        "cancel must DROP unsealed (partial) buckets, not emit them"
    );
}

/// **广播载荷按订阅类型裁剪**（2026-08-25 q13 分片内存修复的核心不变量）。
///
/// 为何必须单独钉死：现有 q13 链用例（`deferred_integration_tests` 的
/// round-robin 场景）只断言**输出正确性**——把 `round_robin_only` 条件写反后
/// 链路依然跑通、用例依然通过，而每批多物化 36.5k 个 `Event`（≈18MB/批）会随
/// 分片积压把 30M 的 RSS 从 9.9GB 推回 28.8GB（`53aca64` 修复的正是这个）。
/// 所以这里断言的是**载荷形状**，不是业务结果。
///
/// - RoundRobin-only 订阅（stateless each 分片，列式安全）→ **batch-only**：
///   `events == None` 且 `batch == Some`（下游从 raw batch 列式读）。
/// - Single 订阅（row-path 中间窗契约）→ 保留 `events`：已由
///   `intermediate_target_writes_window_instead_of_alert_channel` 等三个用例覆盖。
#[tokio::test]
async fn intermediate_broadcast_is_batch_only_for_round_robin_subscribers() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, router) = make_intermediate_each_task();
    let ts = 4_000_000_000_000_000_000i64;

    // 关键差异：round-robin 订阅（生产分片路径），而非 register()（Single）。
    let (down_tx, mut down_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    router
        .fanout()
        .register_round_robin("enriched_events", vec![down_tx]);

    let batch = make_batch(&schema, &["10.0.0.8"], ts);
    router
        .registry()
        .get_window("auth_events")
        .unwrap()
        .append(batch)
        .unwrap();
    task.pull_and_advance().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "intermediate targets must not emit sink alerts"
    );

    let push = down_rx
        .try_recv()
        .expect("round-robin 订阅者必须收到投递（裁剪不等于不投递）");
    assert!(
        push.events.is_none(),
        "RoundRobin-only 订阅必须裁剪为 batch-only：events 物化是分片积压内存主因"
    );
    let batch = push
        .batch
        .as_ref()
        .expect("batch-only 投递必须携带 raw batch，否则下游无数据可读");
    assert_eq!(
        batch.num_rows(),
        1,
        "投递内容仍须完整（1 行输入 → 1 行中间窗）"
    );
    // 载荷可用性：下游按列名读得到 yield 字段（列式消费路径的前提）。
    assert!(
        batch.schema().index_of("sip").is_ok(),
        "中间窗 batch 必须含 yield 列 sip"
    );
}
