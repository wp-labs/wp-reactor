//! 中间目标窗与 relay/pipe 输出测试（2026-09-04 自 engine_task/tests.rs 拆出）：
//! - 中间目标（intermediate target / pipeline stage）写内部窗而非 sink alert；
//! - 显式时间字段在 relay 载荷中保留；round-robin 订阅裁剪为 batch-only 广播；
//! - pure relay 同 key 同 shard、表达式派生 key 跨批稳定分片、空批不广播、
//!   缺目标降级 Dead 不 panic、满通道 flush 背压。

use super::*;

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

/// issue #80 e2e：表达式派生 key（`concat("net-", sip)` 的 let 展开）在真实
/// relay push 链路上按逐行求值结果分片——同派生值不跨片、跨批稳定落同一片
/// （spawn 装配产物的 spec 同构：keys=[Simple(k)] + key_exprs=[Some(expr)]）。
#[tokio::test]
async fn expr_key_sharded_relay_routes_derived_key_together() {
    init_tracing();
    use wf_lang::ast::Expr;
    let (mut task, _alert_rx, router) = make_pipeline_stage_task();
    let (shard_a_tx, mut shard_a_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    let (shard_b_tx, mut shard_b_rx) = mpsc::channel::<wf_engine::window::RulePush>(8);
    let expr = Expr::FuncCall {
        qualifier: None,
        name: "concat".into(),
        args: vec![
            Expr::StringLit("net-".into()),
            Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
        ],
    };
    router.fanout().register_sharded_with_exprs(
        "__wf_pipe_pipe_s1_w1",
        vec![shard_a_tx, shard_b_tx],
        wf_engine::window::ShardKeySpec {
            keys: std::sync::Arc::from([FieldRef::Simple("k".into())]),
            key_exprs: std::sync::Arc::from([Some(expr)]),
        },
    );

    let ts = 1_700_000_000_123_000_000i64;
    let schema = test_schema();
    let source = router.registry().get_window("auth_events").unwrap();
    // 两个不同派生 key：可能同片或异片，但每个 key 只能出现在一个片内。
    let batch = make_batch(&schema, &["10.0.0.8", "10.0.0.9"], ts);
    source.append(batch).unwrap();
    task.pull_and_advance().await;
    let drain = |rx: &mut mpsc::Receiver<wf_engine::window::RulePush>| -> Vec<String> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .flat_map(|p| {
                // 分片 push 携带全量 events + shard_rows 子集（消费方按子集取行）：
                // 该片实际拥有的行 = shard_rows 索引（无 shard_rows 的行式 push
                // = 整批归属）。events 与 batch 行序一致。
                let owned: Vec<usize> = match p.shard_rows {
                    Some(rows) => rows.iter().map(|&r| r as usize).collect(),
                    None => (0..p.events.as_ref().map(|e| e.len()).unwrap_or(0)).collect(),
                };
                let evs = p.events.expect("relay push carries events");
                owned
                    .into_iter()
                    .filter_map(|i| match evs.get(i).and_then(|e| e.fields.get("sip")) {
                        Some(wf_engine::match_engine::Value::Str(s)) => Some(s.to_string()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    };
    let mut a = drain(&mut shard_a_rx);
    let mut b = drain(&mut shard_b_rx);
    let sip_in_8 = |list: &[String]| list.iter().any(|s| s == "10.0.0.8");
    // 同派生 key 不跨片：8/9 各自只出现在一片。
    assert!(!(sip_in_8(&a) && sip_in_8(&b)), "10.0.0.8 不得跨片");
    assert!(
        !(a.iter().any(|s| s == "10.0.0.9") && b.iter().any(|s| s == "10.0.0.9")),
        "10.0.0.9 不得跨片"
    );
    // 全部两行都送达（不丢行）。
    let mut all = a.clone();
    all.extend(b.clone());
    assert_eq!(all.len(), 2, "两行都必须被送达");
    let key_shard: usize = if sip_in_8(&a) { 0 } else { 1 };

    // 跨批稳定：第二批 10.0.0.8 必须仍落在同一片。
    let batch2 = make_batch(&schema, &["10.0.0.8"], ts + 1_000_000);
    source.append(batch2).unwrap();
    task.pull_and_advance().await;
    a.extend(drain(&mut shard_a_rx));
    b.extend(drain(&mut shard_b_rx));
    let landed = if key_shard == 0 { a } else { b };
    assert!(sip_in_8(&landed), "同派生 key 跨批必须稳定落同一片");
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
