//! port_scan close 回归测试（2026-09-04 自 engine_task/tests.rs 拆出）：
//! - close step + tracked_bind_alias 规则：AND close 模式到期（时间驱逐）输出 close 告警；
//! - 独占 helper：conn_events schema / make_conn_events_window / make_port_scan_batch。

use super::*;

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
        key_exprs: Vec::new(),
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
