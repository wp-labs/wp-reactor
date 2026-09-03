//! conv sink / conv stage 测试（2026-09-04 自 engine_task/tests.rs 拆出）：
//! - conv sink：process_batch barrier 水位 = 事件时间扫描水位（非 machine 缓存水位）、
//!   scan_timeouts 按墙钟推进 idle shard barrier、每批只发一个 ConvCloseBatch；
//! - conv stage：密封桶对齐/封口长度（hop = slide/size）、跨分片全局聚合 top_ties、
//!   阈值 failrule 共享锁存（含同桶剩余抑制）、cancel 丢弃未密封桶。

use super::*;

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
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
        key_exprs: Vec::new(),
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
