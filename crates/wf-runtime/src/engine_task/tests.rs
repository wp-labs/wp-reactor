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
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
            key_exprs: Vec::new(),
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

/// Drain all pending alert batches and collect `__wfu_entity_id` values
/// (used by multiple sibling test modules — shared harness item).
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

// -- test cases ---

// 测试已按主题拆为 #[path] 兄弟子模块（见各子文件顶部 //! 说明）：
#[path = "engine_task_tests_core_paths.rs"]
mod engine_task_tests_core_paths;

#[path = "engine_task_tests_sharded_pull.rs"]
mod engine_task_tests_sharded_pull;

#[path = "engine_task_tests_intermediate_relay.rs"]
mod engine_task_tests_intermediate_relay;

#[path = "engine_task_tests_downstream_close.rs"]
mod engine_task_tests_downstream_close;

#[path = "engine_task_tests_bind_each.rs"]
mod engine_task_tests_bind_each;

#[path = "engine_task_tests_port_scan.rs"]
mod engine_task_tests_port_scan;

#[path = "engine_task_tests_conv_stage.rs"]
mod engine_task_tests_conv_stage;
