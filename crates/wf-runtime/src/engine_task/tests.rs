use super::*;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use arrow::array::{StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use tokio::sync::{Notify, mpsc};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

use wf_config::{DistMode, EvictPolicy, LatePolicy, WindowConfig};
use wf_core::rule::{CepStateMachine, RuleExecutor};
use wf_core::window::{Router, Window, WindowParams, WindowRegistry};
use wf_lang::ast::{CmpOp, Expr, FieldRef, Measure};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
    WindowSpec, YieldPlan,
};

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

/// Build a single-step count>=3 rule and return (task, alert_rx, window_arc, notify_arc).
fn make_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<wf_core::alert::OutputRecord>,
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
    mpsc::Receiver<wf_core::alert::OutputRecord>,
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
    };

    let rule_plan = RulePlan {
        name: "test_rule".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: "auth_events".into(),
            filter: None,
        }],
        match_plan: match_plan.clone(),
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

    let machine = CepStateMachine::new("test_rule".into(), match_plan, None);
    let executor = RuleExecutor::new(rule_plan);

    let (alert_tx, alert_rx) = mpsc::channel(64);

    // Empty registry for tests (no joins or has() usage).
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));

    let config = task_types::RuleTaskConfig {
        machine,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&win_arc),
            notify: Arc::clone(&notify_arc),
            stream_names: vec!["syslog".into()],
        }],
        stream_aliases: HashMap::from([("syslog".into(), vec!["fail".into()])]),
        alert_tx,
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router,
    };

    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
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

    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts);
    win.write().unwrap().append(batch).unwrap();

    task.pull_and_advance().await;

    let alert = alert_rx.try_recv().expect("should have produced an alert");
    assert_eq!(alert.rule_name, "test_rule");
    assert_eq!(alert.entity_type, "ip");
    assert_eq!(alert.entity_id, "10.0.0.1");
    assert!((alert.score - 70.0).abs() < f64::EPSILON);
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

    let alert = alert_rx
        .try_recv()
        .expect("sip=10.0.0.1 should trigger at count=3");
    assert_eq!(alert.entity_id, "10.0.0.1");

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
        tmp.get_array_memory_size()
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
