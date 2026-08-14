use super::*;

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
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
fn init_tracing() {
    let _ = tracing_subscriber::registry()
        .with(
            fmt::layer()
                .event_format(DomainFormat::new())
                .with_test_writer()
                .with_filter(EnvFilter::try_new("debug").unwrap()),
        )
        .try_init();
}

fn empty_tracked_bind_fields() -> std::collections::HashMap<String, HashSet<String>> {
    std::collections::HashMap::new()
}

/// A test sink fanout that resolves the `"alerts"` and `"network_alerts"` yield
/// targets (the sink targets used across the tests) to `tx`.

/// Extract the first record from the next alert batch (tests deliver batches).
fn take_alert(rx: &mut mpsc::Receiver<crate::alert_task::AlertBatch>) -> Arc<wp_model_core::model::DataRecord> {
    let batch = rx.try_recv().expect("expected an alert batch");
    batch.first().cloned().expect("alert batch must not be empty")
}

/// Async variant of [`take_alert`] for `recv().await` based assertions.
async fn take_alert_recv(
    rx: &mut mpsc::Receiver<crate::alert_task::AlertBatch>,
) -> Arc<wp_model_core::model::DataRecord> {
    let batch = rx.recv().await.expect("expected an alert batch");
    batch.first().cloned().expect("alert batch must not be empty")
}

fn make_test_fanout(tx: mpsc::Sender<crate::alert_task::AlertBatch>) -> Arc<SinkFanout> {
    let mut cache = std::collections::HashMap::new();
    // One sink (ptr=0) with a single writer channel (batches); the cache type is
    // inferred from `SinkFanout::from_resolved`.
    let groups = Arc::new(vec![(0usize, Arc::new(vec![tx]))]);
    cache.insert("alerts".to_string(), Arc::clone(&groups));
    cache.insert("network_alerts".to_string(), groups);
    SinkFanout::from_resolved(cache)
}

/// Extract a `__wfu_*` field's string form from a sink `DataRecord`.
fn field_str(record: &wp_model_core::model::DataRecord, name: &str) -> String {
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

fn empty_tracked_plain_fields() -> HashSet<String> {
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

fn test_window_config(max_bytes: usize) -> WindowConfig {
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

fn make_window(
    name: &str,
    schema: &SchemaRef,
    max_bytes: usize,
) -> (Arc<RwLock<Window>>, Arc<Notify>) {
    let win = Window::new(
        WindowParams {
            name: name.into(),
            schema: schema.clone(),
            time_col_index: Some(1), // event_time is the second column
            over: Duration::from_secs(3600),
            materialize_fields: None,
        },
        test_window_config(max_bytes),
    );
    (Arc::new(RwLock::new(win)), Arc::new(Notify::new()))
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
        },
        streams: streams.iter().map(|s| (*s).to_string()).collect(),
        config: cfg,
    }
}

/// Build a single-step count>=3 rule and return (task, alert_rx, window_arc, notify_arc).
fn make_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<RwLock<Window>>,
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
fn make_task_with_window_bytes(
    max_bytes: usize,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<RwLock<Window>>,
    Arc<Notify>,
) {
    let schema = test_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, max_bytes);

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
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
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: std::collections::HashSet::from(["x".to_string()]),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
            needs_field_history: true,
    };

    let rule_plan = RulePlan {
        name: "test_rule".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: "auth_events".into(),
            filter: None,
        }],
        match_plan: match_plan.clone(),
        each_plan: None,
        joins: vec![],
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
    };

    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
}

fn make_pipeline_stage_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let src_schema = test_schema();
    let internal = internal_schema();
    let source_name = "auth_events";
    let target_name = "__wf_pipe_pipe_s1_w1";
    let registry = WindowRegistry::build(vec![
        make_window_def(source_name, &src_schema, &["syslog"], Some(1)),
        make_window_def(target_name, &internal, &[target_name], Some(0)),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));

    let source_window = router.registry().get_window(source_name).unwrap();
    let source_notify = router.registry().get_notifier(source_name).unwrap();

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
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
    };
    let rule_plan = RulePlan {
        name: "__wf_pipe_pipe_s1".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: source_name.into(),
            filter: None,
        }],
        match_plan: match_plan.clone(),
        each_plan: None,
        joins: vec![],
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
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, router)
}

fn make_each_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<RwLock<Window>>,
    Arc<Notify>,
) {
    let schema = test_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, usize::MAX);
    let rule_plan = RulePlan {
        name: "each_rule".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: "auth_events".into(),
            filter: None,
        }],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
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
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: Some(Expr::BinOp {
                op: BinOp::Eq,
                left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
                right: Box::new(Expr::StringLit("10.0.0.1".into())),
            }),
        }),
        joins: vec![],
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
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
}

fn make_filtered_match_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<RwLock<Window>>,
    Arc<Notify>,
) {
    let schema = filtered_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, usize::MAX);

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
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
    };

    let rule_plan = RulePlan {
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
        match_plan: match_plan.clone(),
        each_plan: None,
        joins: vec![],
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
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
}

fn make_filtered_close_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<RwLock<Window>>,
    Arc<Notify>,
) {
    let schema = filtered_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, usize::MAX);

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
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
    };

    let rule_plan = RulePlan {
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
        match_plan: match_plan.clone(),
        each_plan: None,
        joins: vec![],
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
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
}

fn make_filtered_each_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<RwLock<Window>>,
    Arc<Notify>,
) {
    let schema = filtered_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, usize::MAX);
    let rule_plan = RulePlan {
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
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
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
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: None,
        }),
        joins: vec![],
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
        name: "intermediate_each".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: source_name.into(),
            filter: None,
        }],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
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
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: None,
        }),
        joins: vec![],
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
        name: "intermediate_each_explicit_time".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: source_name.into(),
            filter: None,
        }],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
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
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: None,
        }),
        joins: vec![],
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
        name: "semantic_project".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: source_name.into(),
            filter: None,
        }],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
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
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: None,
        }),
        joins: vec![],
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
    let (upstream_alert_tx, _upstream_alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let upstream_config = task_types::RuleTaskConfig {
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
    };
    let (upstream_task, _cancel, _interval) = rule_task::RuleTask::new(upstream_config);

    let downstream_match = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
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
    };

    let downstream_plan = RulePlan {
        name: "window_risk".into(),
        binds: vec![BindPlan {
            alias: "x".into(),
            window: target_name.into(),
            filter: None,
        }],
        match_plan: downstream_match.clone(),
        each_plan: None,
        joins: vec![],
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
        name: "semantic_project".into(),
        binds: vec![BindPlan {
            alias: "e".into(),
            window: source_name.into(),
            filter: None,
        }],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
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
        },
        each_plan: Some(EachPlan {
            alias: "e".into(),
            filter: None,
        }),
        joins: vec![],
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
    let (upstream_alert_tx, _upstream_alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let upstream_config = task_types::RuleTaskConfig {
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
    };
    let (upstream_task, _cancel, _interval) = rule_task::RuleTask::new(upstream_config);

    let downstream_match = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_map: None,
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
    };

    let downstream_plan = RulePlan {
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
        match_plan: downstream_match.clone(),
        each_plan: None,
        joins: vec![],
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
    };
    let (downstream_task, _cancel, _interval) = rule_task::RuleTask::new(downstream_config);

    (upstream_task, downstream_task, alert_rx, router)
}

fn make_filtered_bind_alias_match_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<RwLock<Window>>,
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
    };

    let rule_plan = RulePlan {
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
        match_plan: match_plan.clone(),
        each_plan: None,
        joins: vec![],
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
    };

    let rule_plan = RulePlan {
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
        match_plan: match_plan.clone(),
        each_plan: None,
        joins: vec![],
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
    win.write().unwrap().append(batch).unwrap();

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
    win.write().unwrap().append(batch).unwrap();

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
        events: Arc::new(batch_to_events(&batch).into_iter().map(Arc::new).collect::<Vec<_>>()),
    };
    task.process_push(push).await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "test_rule");
    assert_eq!(field_str(&alert, "__wfu_entity_type"), "ip");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!((field_f64(&alert, "__wfu_score") - 70.0).abs() < f64::EPSILON);
    assert!(!field_str(&alert, "__wfu_fired_at").is_empty());
}

fn drain_alert_entity_ids(
    rx: &mut mpsc::Receiver<crate::alert_task::AlertBatch>,
) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        for record in batch.iter() {
            ids.push(field_str(record, "__wfu_entity_id"));
        }
    }
    ids
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
    let events = Arc::new(batch_to_events(&batch).into_iter().map(Arc::new).collect::<Vec<_>>());

    // Single worker: feed the whole batch.
    let (mut single, mut single_rx, _w, _n) = make_task();
    single
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Arc::clone(&events),
        })
        .await;
    let mut single_ids = drain_alert_entity_ids(&mut single_rx);

    // Sharded: partition via the router fan-out (2 shards), then two machines.
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));
    let (s0_tx, mut s0_rx) = mpsc::channel(8);
    let (s1_tx, mut s1_rx) = mpsc::channel(8);
    let keys: Arc<[FieldRef]> =
        Arc::from(vec![FieldRef::Simple("sip".into())].into_boxed_slice());
    router
        .fanout()
        .register_sharded("auth_events", vec![s0_tx, s1_tx], keys);
    router.fanout().broadcast("auth_events", &events).await;

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
    win.write().unwrap().append(batch).unwrap();

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
    win.write().unwrap().append(batch).unwrap();

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
    win.write().unwrap().append(batch1).unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "neither key should trigger at count=2"
    );

    let batch2 = make_batch(&schema, &["10.0.0.1"], ts + 1_000_000_000);
    win.write().unwrap().append(batch2).unwrap();
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
    win.write().unwrap().append(batch0).unwrap();

    let batch1 = make_batch(&schema, &["10.0.0.2"], ts + 1_000_000_000);
    win.write().unwrap().append(batch1).unwrap();

    assert_eq!(
        win.read().unwrap().batch_count(),
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

#[tokio::test]
async fn flush_closes_active_instances() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, win, _notify) = make_task();

    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1"], ts);
    win.write().unwrap().append(batch).unwrap();
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
    source.write().unwrap().append(batch).unwrap();
    task.pull_and_advance().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "internal pipeline stage must not emit sink alerts"
    );

    // Pure relay: the internal pipe is NOT stored — events broadcast downstream.
    assert!(
        router
            .registry()
            .snapshot("__wf_pipe_pipe_s1_w1")
            .unwrap_or_default()
            .is_empty(),
        "pure relay: internal pipe must not be stored in a window"
    );
    let push = down_rx.try_recv().expect("downstream rule received pipeline events");
    let rows = push.events;
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
    source.write().unwrap().append(batch).unwrap();
    task.pull_and_advance().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "intermediate targets must not emit sink alerts"
    );

    // Pure relay: no window storage; events broadcast to downstream rules.
    assert!(
        router
            .registry()
            .snapshot("enriched_events")
            .unwrap_or_default()
            .is_empty(),
        "pure relay: intermediate pipe must not be stored in a window"
    );
    let push = down_rx.try_recv().expect("downstream rule received intermediate events");
    let rows = push.events;
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

#[test]
fn pipeline_batch_rejects_non_finite_number_inside_structured_value() {
    let schema = intermediate_schema();
    let fields = vec![(
        "risk_context".to_string(),
        wf_engine::match_engine::Value::Object(
            [(
                "score".into(),
                wf_engine::match_engine::Value::Number(f64::NAN),
            )]
            .into_iter()
            .collect(),
        ),
    )];

    let err = rule_task::build_pipeline_batch(schema, None, 0, &fields)
        .expect_err("non-finite structured number should fail");
    assert!(
        err.to_string()
            .contains("structured numeric value must be finite")
    );
}

#[test]
fn pipeline_batch_preserves_time_yield_as_epoch_nanos() {
    let schema = intermediate_schema();
    let ts = 1_700_000_000_123_000_000i64;
    let fields = vec![
        (
            "event_time".to_string(),
            wf_engine::match_engine::Value::Number(1_700_000_000_123.0),
        ),
        (
            "sip".to_string(),
            wf_engine::match_engine::Value::Str("10.0.0.8".into()),
        ),
    ];

    let batch = rule_task::build_pipeline_batch(schema, None, 0, &fields).expect("pipeline batch");
    let ts_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("event_time should be timestamp nanos");
    assert_eq!(ts_col.value(0), ts);
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
    source.write().unwrap().append(batch).unwrap();
    task.pull_and_advance().await;

    assert!(alert_rx.try_recv().is_err());

    // Pure relay: no window storage; the broadcast event preserves the explicit
    // time field as epoch nanos.
    assert!(
        router
            .registry()
            .snapshot("enriched_events")
            .unwrap_or_default()
            .is_empty(),
        "pure relay: intermediate pipe must not be stored in a window"
    );
    let push = down_rx.try_recv().expect("downstream rule received intermediate events");
    let event = &push.events[0];
    assert_eq!(
        event.fields.get("event_time"),
        Some(&wf_engine::match_engine::Value::Number(10_000_000_000_000_000.0))
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
    source.write().unwrap().append(batch).unwrap();

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
    assert_eq!(
        field_f64(&alert, "avg_score"),
        20.0
    );
    assert_eq!(
        field_f64(&alert, "avg_risk"),
        20.0
    );
    assert_eq!(
        field_f64(&alert, "event_count"),
        2.0
    );
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
    source.write().unwrap().append(batch).unwrap();

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
    assert_eq!(
        field_f64(&alert, "event_count"),
        2.0
    );
    assert_eq!(
        field_f64(&alert, "source_avg"),
        80.0
    );
    assert_eq!(
        field_f64(&alert, "high_event_count"),
        1.0
    );
    assert_eq!(
        field_f64(&alert, "elevated_event_count"),
        2.0
    );
    assert_eq!(
        field_str(&alert, "status"),
        "high"
    );
}

#[tokio::test]
async fn match_event_path_counts_filtered_bind_aliases() {
    init_tracing();
    let schema = scored_source_schema();
    let (mut task, mut alert_rx, win, _notify) = make_filtered_bind_alias_match_task();
    let ts = 4_000_000_000_000_000i64;
    let batch = make_scored_batch(&schema, &["10.0.0.7", "10.0.0.7"], &[90.0, 70.0], ts);
    win.write().unwrap().append(batch).unwrap();

    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.7");
    assert_eq!(field_f64(&alert, "__wfu_score"), 1.0);
    assert_eq!(
        field_f64(&alert, "high_event_count"),
        1.0
    );
    assert_eq!(
        field_f64(&alert, "elevated_avg"),
        80.0
    );
    assert_eq!(
        field_str(&alert, "last_high_sip"),
        "10.0.0.7"
    );
}

#[tokio::test]
async fn on_each_emits_one_alert_per_matching_row() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, win, _notify) = make_each_task();
    let ts_nanos = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.2"], ts_nanos);
    win.write().unwrap().append(batch).unwrap();

    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "each_rule");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert_eq!(field_str(&alert, "__wfu_origin"), "event");
    assert!(!field_str(&alert, "__wfu_fired_at").is_empty());
    assert_eq!(
        field_str(&alert, "x"),
        "10.0.0.1"
    );
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
    win.write().unwrap().append(batch1).unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "non-matching bind-filter rows must not count toward the match"
    );

    let batch2 = make_filtered_batch(&schema, &["10.0.0.1"], &["failed"], ts + 1);
    win.write().unwrap().append(batch2).unwrap();
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
    lookup.write().unwrap().append(lookup_batch).unwrap();

    let source_batch = make_batch(&schema, &["10.0.0.1", "10.0.0.2"], ts);
    let source = router.registry().get_window("auth_events").unwrap();
    source.write().unwrap().append(source_batch).unwrap();

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
    win.write().unwrap().append(batch).unwrap();

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
fn make_conn_events_window(max_bytes: usize) -> (Arc<RwLock<Window>>, Arc<Notify>) {
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
        },
        cfg,
    );
    let win_arc = Arc::new(RwLock::new(win));
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
    };

    let rule_plan = RulePlan {
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
        match_plan: match_plan.clone(),
        each_plan: None,
        joins: vec![],
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
    };

    let (mut task, _cancel, _interval) = rule_task::RuleTask::new(config);

    // Feed batch: 5 events with same sip, action=syn
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_port_scan_batch("10.0.0.1", &[80, 443, 22, 8080, 3306], ts);
    win_arc.write().unwrap().append(batch).unwrap();

    task.pull_and_advance().await;

    // No matched alert (close mode is AND)
    assert!(
        alert_rx.try_recv().is_err(),
        "AND mode should not emit on-event match"
    );

    // Feed second batch with later timestamps to trigger expiry (ts + 11s > created_at + 10s)
    let nanos_per_sec: i64 = 1_000_000_000;
    let batch2 = make_port_scan_batch("10.0.0.1", &[21, 25, 53], ts + 11 * nanos_per_sec);
    win_arc.write().unwrap().append(batch2).unwrap();

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
    source.write().unwrap().append(batch).unwrap();
    task.pull_and_advance().await;

    // Pure relay: nothing stored in the intermediate window.
    assert!(
        router
            .registry()
            .snapshot("__wf_pipe_pipe_s1_w1")
            .unwrap_or_default()
            .is_empty(),
        "pure relay: sharded intermediate pipe must not be stored"
    );

    let a: Vec<_> = std::iter::from_fn(|| shard_a_rx.try_recv().ok()).collect();
    let b: Vec<_> = std::iter::from_fn(|| shard_b_rx.try_recv().ok()).collect();
    let (full, empty) = if a.len() > b.len() { (a, b) } else { (b, a) };
    assert_eq!(full.len(), 2, "same-key events must land on the same shard");
    assert!(empty.is_empty(), "the other shard must stay empty for the same key");
    assert_eq!(
        full[0].events[0].fields.get("sip"),
        Some(&wf_engine::match_engine::Value::Str("10.0.0.8".into()))
    );
}
