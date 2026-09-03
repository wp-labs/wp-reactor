//! bind filter / each / on-each / match / has() 语义测试（2026-09-04 自 engine_task/tests.rs 拆出）：
//! - on-each 每命中行一条输出；
//! - match/each 的 events bind filter：被拒行不得计入状态机/不得输出；
//! - window.has() 查找窗 bind filter。

use super::*;

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

fn make_each_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    make_each_task_with_bind_filter(None)
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
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
