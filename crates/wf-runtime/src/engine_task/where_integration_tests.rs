//! Post-join `where` integration tests (wf-runtime): join enrichment + strict
//! where filter across the real RuleTask + window path (q3/q20 pattern).

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use wf_engine::match_engine::{CepStateMachine, RuleExecutor};
use wf_engine::window::{Router, Window, WindowDef, WindowParams, WindowRegistry};
use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, JoinMode, MatchMode, Measure};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EntityPlan, JoinCondPlan, JoinPlan, MatchPlan, RulePlan,
    ScorePlan, StepPlan, WindowSpec, YieldPlan,
};

use super::tests::{empty_tracked_bind_fields, empty_tracked_plain_fields, make_test_fanout};
use crate::engine_task::{rule_task, task_types};

/// auth-like driver schema: sip + event_time.
fn driver_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

/// person-like join-target schema: id + state + event_time.
fn person_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

fn driver_batch(sips: &[&str], ts: i64) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(sips.to_vec())),
        Arc::new(TimestampNanosecondArray::from(vec![ts; sips.len()])),
    ];
    RecordBatch::try_new(driver_schema(), cols).unwrap()
}

fn person_batch(ids: &[&str], states: &[&str], ts: i64) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(ids.to_vec())),
        Arc::new(StringArray::from(states.to_vec())),
        Arc::new(TimestampNanosecondArray::from(vec![ts; ids.len()])),
    ];
    RecordBatch::try_new(person_schema(), cols).unwrap()
}

fn window_def(name: &str, schema: &Arc<Schema>) -> WindowDef {
    let mut cfg = super::tests::test_window_config(usize::MAX);
    cfg.name = name.to_string();
    WindowDef {
        params: WindowParams {
            name: name.to_string(),
            schema: schema.clone(),
            time_col_index: Some(schema.index_of("event_time").unwrap()),
            over: std::time::Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec![name.to_string()],
        config: cfg,
    }
}

fn where_state_in_or() -> Expr {
    Expr::InList {
        expr: Box::new(Expr::Field(FieldRef::Qualified(
            "person_events".to_string(),
            "state".to_string(),
        ))),
        list: vec![Expr::StringLit("OR".into()), Expr::StringLit("CA".into())],
        negated: false,
    }
}

/// Build a task for:
/// ```wfl
/// rule r {
///   events { fail : auth_events }
///   match<sip:5m> { on event { fail | count >= 1; } } -> score(1.0)
///   join person_events snapshot on fail.sip == person_events.id
///   where person_events.state in ("OR","CA")
///   entity(ip, fail.sip)
///   yield alerts ()
/// }
/// ```
fn make_join_where_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let driver = "auth_events";
    let person = "person_events";
    let registry = WindowRegistry::build(vec![
        window_def(driver, &driver_schema()),
        window_def(person, &person_schema()),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let source_window = router.registry().get_window(driver).unwrap();
    let source_notify = router.registry().get_notifier(driver).unwrap();

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
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: true,
    };

    let rule_plan = RulePlan {
        conv_window: None,
        name: "join_where".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: driver.into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: match_plan.clone(),
        each_plan: None,
        stats_plan: None,
        joins: vec![JoinPlan {
            right_window: person.into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Simple("sip".into()),
                right: FieldRef::Simple("id".into()),
            }],
        }],
        r#where: Some(where_state_in_or()),
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

    let machine = CepStateMachine::new("join_where".into(), match_plan, Some("event_time".into()));
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
            window_name: driver.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["fail".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: std::time::Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, router)
}

fn person_window(router: &Router) -> Arc<Window> {
    router.registry().get_window("person_events").unwrap()
}

#[tokio::test]
async fn match_join_where_hit_emits() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_join_where_task();
    let ts = 4_000_000_000_000_000i64;

    // person row first (state = OR), then the driver event.
    person_window(&router)
        .append(person_batch(&["10.0.0.1"], &["OR"], ts - 1))
        .unwrap();
    router
        .registry()
        .get_window("auth_events")
        .unwrap()
        .append(driver_batch(&["10.0.0.1"], ts))
        .unwrap();

    task.pull_and_advance().await;

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_rule_name"),
        "join_where"
    );
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "10.0.0.1"
    );
}

#[tokio::test]
async fn match_join_where_false_suppresses() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_join_where_task();
    let ts = 4_000_000_000_000_000i64;

    // person row state = ID (not in OR/CA) → where false → no alert.
    person_window(&router)
        .append(person_batch(&["10.0.0.1"], &["ID"], ts - 1))
        .unwrap();
    router
        .registry()
        .get_window("auth_events")
        .unwrap()
        .append(driver_batch(&["10.0.0.1"], ts))
        .unwrap();

    task.pull_and_advance().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "where false (state=ID) must suppress the alert"
    );
}

#[tokio::test]
async fn match_join_where_miss_suppresses() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_join_where_task();
    let ts = 4_000_000_000_000_000i64;

    // No person row → join miss → joined field absent → strict where → no alert.
    router
        .registry()
        .get_window("auth_events")
        .unwrap()
        .append(driver_batch(&["10.0.0.1"], ts))
        .unwrap();

    task.pull_and_advance().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "join miss must suppress (INNER JOIN semantics)"
    );
}
