//! Post-join `where` integration tests (wf-runtime): join enrichment + strict
//! where filter across the real RuleTask + window path (q3/q20 pattern).
use std::sync::Arc;

use std::collections::HashSet;

use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use wf_engine::match_engine::{CepStateMachine, RuleExecutor};
use wf_engine::window::{Router, Window, WindowDef, WindowParams, WindowRegistry};
use wf_lang::ast::{
    Bound, BoundVal, CloseMode, CmpOp, Expr, FieldRef, JoinMode, MatchMode, Measure, WithinSpec,
};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EachPlan, EntityPlan, JoinCondPlan, JoinPlan, MatchPlan,
    RulePlan, ScorePlan, StepPlan, WindowSpec, YieldField, YieldPlan,
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
        trigger_event_needed: false,
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
            within: None,
            reduce: None,
            emit_at: None,
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
        key_partitioned: false,
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

// ---------------------------------------------------------------------------
// P0b：2026-08-29 snapshot join 竞态 gate 回归（q6/q20/q3 多规则同跑丢行根因）
// ---------------------------------------------------------------------------
// 背景：并行 parse + 跨窗口 actor 独立 commit 使 join 目标窗（person_events /
// auction_events）的提交可能滞后于驱动窗消费——即时 join 读目标窗时，已 append
// 但未 commit 的行不可见 → 静默 miss（all 模式实测 q6 872913→788k~845k、q20
// 196517→189430、q3 6060→4795；单规则隔离全对，竞争放大）。
// 修复：`process_batch` 处理驱动批前，等目标窗 `committed_frontier_ns` 追平本批
// max 事件时间（match 规则回退驱动窗时间列名触发）。以下用例构造「目标行在
// process_batch 等待期间才提交」——无 gate 时 join 立即 miss（无输出），有 gate
// 时等 frontier 追平后命中（有输出）。

/// match 路径（q3/q6 形态：machine + snapshot join + where，each_time_field
/// = None → 驱动窗时间列名触发 gate）回归：目标窗提交滞后时等待后 join 命中。
#[tokio::test]
async fn match_join_waits_for_target_commit_frontier() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_join_where_task();
    let ts = 4_000_000_000_000_000i64;

    // 驱动批先提交（auth 10.0.0.1 @ ts）；person 行**尚未**提交——模拟目标窗
    // actor 滞后（跨窗口独立 commit）：即时 join 此刻读不到 person → miss。
    router
        .registry()
        .get_window("auth_events")
        .unwrap()
        .append(driver_batch(&["10.0.0.1"], ts))
        .unwrap();

    // 处理在后台跑：gate 应等待 person_events frontier 追平 ts（而非立即 join）。
    let handle = tokio::spawn(async move {
        task.pull_and_advance().await;
    });

    // gate 轮询窗口（20ms 轮询、~60ms 停滞兜底）内提交 person 行：其事件时间
    // 必须覆盖 `batch_max + 跨批前视余量`（250ms）——person 行落在 ts+300ms
    // （> 余量）→ frontier 追平目标上界 → gate 放行。
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    person_window(&router)
        .append(person_batch(&["10.0.0.1"], &["OR"], ts + 300_000_000))
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(10), handle)
        .await
        .expect("task must finish promptly")
        .expect("pull_and_advance ok");

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "10.0.0.1",
        "等待目标窗前沿追平后 join 必须命中（无 gate 时此处 join miss → where 抑制 → 无输出）"
    );
}

/// on-each 路径（q20 形态：each + snapshot join + where）回归：目标窗提交滞后时
/// 等待后 join 命中（q20 p=10 196517→189430 的精确回归锚点）。
#[tokio::test]
async fn each_join_waits_for_target_commit_frontier() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_each_join_where_task();
    let ts = 4_000_000_000_000_000i64;

    router
        .registry()
        .get_window("auth_events")
        .unwrap()
        .append(driver_batch(&["10.0.0.1"], ts))
        .unwrap();

    let handle = tokio::spawn(async move {
        task.pull_and_advance().await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    person_window(&router)
        .append(person_batch(&["10.0.0.1"], &["OR"], ts + 300_000_000))
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(10), handle)
        .await
        .expect("task must finish promptly")
        .expect("pull_and_advance ok");

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "10.0.0.1",
        "on-each join 同样必须等目标窗前沿追平（q20 回归）"
    );
}

/// 构造 on-each + snapshot join + where 任务（q20 形态，机器为空）：
/// ```wfl
/// rule r {
///   events { fail : auth_events }
///   on each fail
///   join person_events snapshot on fail.sip == person_events.id
///   where person_events.state in ("OR","CA")
///   entity(ip, fail.sip)
///   yield alerts ()
/// }
/// ```
fn make_each_join_where_task() -> (
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

    let rule_plan = RulePlan {
        conv_window: None,
        name: "each_join_where".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: driver.into(),
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
            match_mode: MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "fail".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![JoinPlan {
            right_window: person.into(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Simple("sip".into()),
                right: FieldRef::Simple("id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
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

    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("fail".into()),
        each_time_field: Some("event_time".into()),
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
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, router)
}

// ---------------------------------------------------------------------------
// P2：eager interval join（within 回看时间谓词，缺省 inner）端到端
// ---------------------------------------------------------------------------

/// P2 任务：`join person_events within [10s, 0s] on fail.sip == person_events.id`
///（缺省 inner）——区间内命中富化输出，miss 丢事件。yield 带 join 富化字段证明注入。
fn make_interval_join_task() -> (
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
        trigger_event_needed: false,
    };

    // `within [10s, 0s]`——回看 10s（`within 10s` 糖的常量界等价形态）
    let within = WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: std::time::Duration::from_secs(10),
                neg: true,
            },
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: std::time::Duration::ZERO,
                neg: false,
            },
        },
    };

    let rule_plan = RulePlan {
        conv_window: None,
        name: "interval_join".into(),
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
            mode: JoinMode::Inner,
            conds: vec![JoinCondPlan {
                left: FieldRef::Simple("sip".into()),
                right: FieldRef::Simple("id".into()),
            }],
            within: Some(within),
            reduce: None,
            emit_at: None,
        }],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "ip".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("fail".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "state".into(),
                value: Expr::Field(FieldRef::Qualified("person_events".into(), "state".into())),
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let machine = CepStateMachine::new(
        "interval_join".into(),
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
        key_partitioned: false,
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, router)
}

#[tokio::test]
async fn interval_inner_hit_emits_and_enriches() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_interval_join_task();
    let ts = 4_000_000_000_000_000i64;

    // person 行在 ts-1s（回看 10s 窗口内）→ 命中 → 富化 state 并输出
    person_window(&router)
        .append(person_batch(&["10.0.0.1"], &["OR"], ts - 1_000_000_000))
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
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "10.0.0.1"
    );
    assert_eq!(
        super::tests::field_str(&alert, "state"),
        "OR",
        "interval join hit must enrich the joined field"
    );
}

#[tokio::test]
async fn interval_inner_miss_drops_alert() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_interval_join_task();
    let ts = 4_000_000_000_000_000i64;

    // person 行在 ts-20s（回看 10s 窗口之外）→ interval miss → 缺省 inner 丢事件
    person_window(&router)
        .append(person_batch(&["10.0.0.1"], &["OR"], ts - 20_000_000_000))
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
        "interval inner miss must drop the event (no alert)"
    );
}
