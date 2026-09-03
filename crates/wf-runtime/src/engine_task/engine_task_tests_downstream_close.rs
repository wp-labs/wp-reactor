//! 下游中间窗 close 聚合测试（2026-09-04 自 engine_task/tests.rs 拆出）：
//! - 下游 fixed 窗 close 聚合中间窗浮点字段（avg/__wfu_score/event_count）；
//! - 多 bind 别名（filtered hi/elevated）计数与 IfThenElse status；
//! - match event 路径对 filtered bind aliases 的计数与 last() 取值。

use super::*;

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
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
