//! 分片规则与 pull 消费进度测试（2026-09-04 自 engine_task/tests.rs 拆出）：
//! - 分片规则与单 worker 告警一致（push 经 router fanout 按 key 分片）；
//! - P2 零重复分片：pull 每 shard 只处理自己 `shard_rows` 子集，并集全 key 恰一次；
//! - round-robin（whole-batch）任务不被其它规则注册的 key 分片误导：拉全批、
//!   ack 处理位置（非读位置），防窗口驱逐删其它 shard 未处理批次；
//! - pull 多 key 隔离与内存驱逐导致的 cursor gap 检测。

use super::*;

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
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );

    // Single worker: feed the whole batch.
    let (mut single, mut single_rx, _w, _n) = make_task();
    single
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(Arc::clone(&events)),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let mut single_ids = drain_alert_entity_ids(&mut single_rx);

    // Sharded: partition via the router fan-out (2 shards), then two machines.
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));
    let (s0_tx, mut s0_rx) = mpsc::channel(8);
    let (s1_tx, mut s1_rx) = mpsc::channel(8);
    let keys: Arc<[FieldRef]> = Arc::from(vec![FieldRef::Simple("sip".into())].into_boxed_slice());
    router
        .fanout()
        .register_sharded("auth_events", vec![s0_tx, s1_tx], keys);
    router.fanout().broadcast("auth_events", &events, 0).await;

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
    win.append(batch1).unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "neither key should trigger at count=2"
    );

    let batch2 = make_batch(&schema, &["10.0.0.1"], ts + 1_000_000_000);
    win.append(batch2).unwrap();
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
    win.append(batch0).unwrap();

    let batch1 = make_batch(&schema, &["10.0.0.2"], ts + 1_000_000_000);
    win.append(batch1).unwrap();

    assert_eq!(
        win.batch_count(),
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

/// Build a keyed (`sip`) match rule (count>=1 fires once per key) with the
/// pull-model window sharding registered, and return `shard_count` independent
/// pull `RuleTask`s that all share ONE window log. Used to test P2 zero
/// re-partition: each shard must process only its stored `shard_rows` subset
/// and the union of all shards must cover every key exactly once.
fn make_sharded_match_tasks(
    shard_count: usize,
) -> (
    Vec<rule_task::RuleTask>,
    Vec<mpsc::Receiver<crate::alert_task::AlertBatch>>,
    Arc<Window>,
    Arc<Router>,
) {
    let schema = test_schema(); // sip(col0), event_time(col1)
    let registry = WindowRegistry::build(vec![make_window_def(
        "auth_events",
        &schema,
        &["syslog"],
        Some(1),
    )])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let window = router.registry().get_window("auth_events").unwrap();
    let notify = router.registry().get_notifier("auth_events").unwrap();
    // Register the key partition so `pull_and_advance` treats the window as
    // key-sharded (reads its `shard_rows` subset instead of the whole batch).
    router.fanout().register_window_sharding(
        "auth_events",
        Arc::from(vec![FieldRef::Simple("sip".into())].into_boxed_slice()),
        shard_count,
    );

    let mut tasks = Vec::new();
    let mut rxs = Vec::new();
    for shard_index in 0..shard_count {
        let match_plan = MatchPlan {
            keys: vec![FieldRef::Simple("sip".into())],
            key_exprs: Vec::new(),
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
            event_steps: vec![StepPlan {
                branches: vec![BranchPlan {
                    label: Some("x".into()),
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
            close_steps: vec![],
            close_mode: CloseMode::Or,
            tracked_bind_aliases: HashSet::new(),
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
            name: "sharded_match".into(),
            binds: vec![BindPlan {
                alias: "x".into(),
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
                entity_id_expr: Expr::Field(FieldRef::Qualified("x".into(), "sip".into())),
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
            "sharded_match".into(),
            match_plan,
            Some("event_time".into()),
        );
        let executor = RuleExecutor::new(rule_plan);

        let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
        let mut progress = std::collections::HashMap::new();
        if let Some(slot) = router
            .registry()
            .progress("auth_events")
            .map(|p| p.register())
        {
            progress.insert("auth_events".to_string(), slot);
        }
        let config = task_types::RuleTaskConfig {
            progress,
            conv_sink: None,
            machine: Some(machine),
            each_alias: None,
            each_time_field: None,
            executor,
            window_sources: vec![task_types::WindowSource {
                window_name: "auth_events".into(),
                window: Arc::clone(&window),
                notify: Arc::clone(&notify),
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
            shard_index: Some(shard_index),
            shard_count,
            key_partitioned: true,
        };
        let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
        tasks.push(task);
        rxs.push(alert_rx);
    }
    (tasks, rxs, window, router)
}

#[tokio::test]
async fn pull_sharded_match_zero_repartition() {
    // P2 零重复分片端到端验证：6 个 key 按行号 % 2 分片（row i → shard i%2），
    // 每个 shard 只处理自己 shard_rows 子集 → 只对自己 key 触发；跨所有 shard
    // 的并集 == 全部 key 各一次（不丢、不重、不跨 shard 重复触发）。
    init_tracing();
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let sips = ["s0", "s1", "s2", "s3", "s4", "s5"];
    let batch = make_batch(&schema, &sips, ts);
    let shard_rows: Vec<Vec<u32>> = (0..2)
        .map(|sh| {
            (0..sips.len() as u32)
                .filter(|&r| r as usize % 2 == sh)
                .collect()
        })
        .collect();

    let (mut tasks, mut rxs, win, _router) = make_sharded_match_tasks(2);
    let size = content_bytes(&batch);
    win.append_with_watermark_sized(batch, size, Some(Arc::new(shard_rows)))
        .unwrap();

    for t in tasks.iter_mut() {
        t.pull_and_advance().await;
    }
    let ids0: HashSet<String> = drain_alert_entity_ids(&mut rxs[0]).into_iter().collect();
    let ids1: HashSet<String> = drain_alert_entity_ids(&mut rxs[1]).into_iter().collect();

    let expect0: HashSet<String> = ["s0", "s2", "s4"].iter().map(|s| s.to_string()).collect();
    let expect1: HashSet<String> = ["s1", "s3", "s5"].iter().map(|s| s.to_string()).collect();
    assert_eq!(ids0, expect0, "shard 0 must fire only its own keys");
    assert_eq!(ids1, expect1, "shard 1 must fire only its own keys");

    // Union across shards == every key exactly once.
    let union: HashSet<String> = ids0.union(&ids1).cloned().collect();
    assert_eq!(
        union.len(),
        sips.len(),
        "every key must fire exactly once across all shards (zero re-partition)"
    );
    assert!(
        union.iter().all(|s| sips.contains(&s.as_str())),
        "only real keys should fire"
    );
}

/// 2026-08-29 q1/q20 all 模式分片误拉回归：bid_events 被其它 match 规则注册 key
/// 分片后，on-each round-robin 任务（q20 形态）若用全局 `window_is_sharded` 判定
/// 拉取模式，会误把**别的规则**的 key 划分（`shard_rows`）当自己的行子集拉取——
/// 每 shard 处理被划分走的部分行（`columnar_each` 因 `shard_rows.is_some()` 失效
/// → 行式路径）→ 偶发丢行（all 模式 q20 196517→189k~193k、q1 重复处理 10×）。
/// 修复：任务携带**自己**的 `key_partitioned` 标志，round-robin 规则恒拉全批
/// （`shard_rows=None`）→ 走列式路径。
///
/// 本用例：窗口已注册 key 分片（模拟其它 match 规则），round-robin 任务
/// `key_partitioned=false` → 必须拉全批（shard 0 处理 batch_seq=0 的整批 6 行），
/// 而不是 `shard_rows[0]`（3 行）。
#[tokio::test]
async fn round_robin_pulls_whole_batch_despite_foreign_window_sharding() {
    init_tracing();
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let sips = ["s0", "s1", "s2", "s3", "s4", "s5"];
    let batch = make_batch(&schema, &sips, ts);
    // 模拟「别的 match 规则」注册的分片划分（6 行 → shard 0/1 各 3 行，按行号奇偶）。
    let shard_rows: Vec<Vec<u32>> = (0..2)
        .map(|sh| {
            (0..sips.len() as u32)
                .filter(|&r| r as usize % 2 == sh)
                .collect()
        })
        .collect();

    let registry = WindowRegistry::build(vec![make_window_def(
        "auth_events",
        &schema,
        &["syslog"],
        Some(1),
    )])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let window = router.registry().get_window("auth_events").unwrap();
    let notify = router.registry().get_notifier("auth_events").unwrap();
    // 全局注册 key 分片（模拟其它 match 规则）——旧代码 `window_is_sharded` 会因此
    // 把 round-robin 任务误判为 key-partitioned。
    router.fanout().register_window_sharding(
        "auth_events",
        Arc::from(vec![FieldRef::Simple("sip".into())].into_boxed_slice()),
        2,
    );

    // q1 形态 on-each 直通任务：每行一条输出（entity = sip）。
    let rule_plan = RulePlan {
        conv_window: None,
        name: "rr_whole_batch".into(),
        binds: vec![BindPlan {
            alias: "x".into(),
            window: "auth_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
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
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "x".into(),
            filter: None,
        }),
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
    let (alert_tx, mut alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("x".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&window),
            notify: Arc::clone(&notify),
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
        // round-robin shard 0/2：batch_seq=0 归本 shard。key_partitioned=false →
        // 拉全批（shard_rows=None），不被上面的全局分片注册影响。
        shard_index: Some(0),
        shard_count: 2,
        key_partitioned: false,
    };
    let (mut task, _cancel, _interval) = rule_task::RuleTask::new(config);

    let size = content_bytes(&batch);
    window
        .append_with_watermark_sized(batch, size, Some(Arc::new(shard_rows)))
        .unwrap();
    task.pull_and_advance().await;

    // round-robin：batch_seq=0 → shard 0 处理整批 6 行 → 6 条输出。
    // 若误用全局分片（shard_rows[0] = 3 行）→ 只输出 3 条（回归锚点）。
    let ids: HashSet<String> = drain_alert_entity_ids(&mut alert_rx).into_iter().collect();
    assert_eq!(
        ids.len(),
        sips.len(),
        "round-robin shard must process the WHOLE batch (not the foreign key partition)"
    );
    assert!(
        ids.iter().all(|s| sips.contains(&s.as_str())),
        "all batch rows must be emitted"
    );
}

/// 2026-08-29 key_partitioned 修复副产物回归：round-robin（whole-batch 分片）规则
/// 的 ack 必须是**处理位置**（本 shard 份额内最后处理批次 + 1），而不是读位置
/// （`new_cursor` = 全部批次）。旧代码 ack 读位置会让 `min_acked` 追平
/// `next_seq` → 窗口驱逐无未读保护 → 删掉**其它 shard 尚未处理**的批次（cursor
/// gap 静默丢数据，q13a 分片隐患同类）。修复后 `key_partitioned=false` 走处理
/// 位置 ack。
///
/// 本用例：2 shard round-robin，append 4 批（seq 0-3，各 1 行）。shard 0 的
/// 份额 = 批 0、2 → 处理后 ack=3（批 0 后处理批 2 → last+1 = 3），而非读位置 4。
#[tokio::test]
async fn round_robin_shard_acks_processed_not_read_position() {
    init_tracing();
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;

    let registry = WindowRegistry::build(vec![make_window_def(
        "auth_events",
        &schema,
        &["syslog"],
        Some(1),
    )])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let window = router.registry().get_window("auth_events").unwrap();
    let notify = router.registry().get_notifier("auth_events").unwrap();
    // 模拟其它 match 规则注册 key 分片：旧代码 `window_is_sharded` 会因此把
    // round-robin 规则误判为 key-partitioned → ack 读位置（4）+ 处理全部 4 批；
    // 新代码 key_partitioned=false → ack 处理位置（3）+ 只处理份额内批 0、2。
    router.fanout().register_window_sharding(
        "auth_events",
        Arc::from(vec![FieldRef::Simple("sip".into())].into_boxed_slice()),
        2,
    );
    // q1 形态 on-each 直通任务（同 round_robin_pulls_whole_batch_despite_foreign_window_sharding）。
    let rule_plan = RulePlan {
        conv_window: None,
        name: "rr_ack".into(),
        binds: vec![BindPlan {
            alias: "x".into(),
            window: "auth_events".into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
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
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: Some(EachPlan {
            alias: "x".into(),
            filter: None,
        }),
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
    let (alert_tx, _alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let mut progress = std::collections::HashMap::new();
    if let Some(slot) = router
        .registry()
        .progress("auth_events")
        .map(|p| p.register())
    {
        progress.insert("auth_events".to_string(), slot);
    }
    let config = task_types::RuleTaskConfig {
        progress,
        conv_sink: None,
        machine: None,
        each_alias: Some("x".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&window),
            notify: Arc::clone(&notify),
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
        shard_index: Some(0),
        shard_count: 2,
        key_partitioned: false,
    };
    let (mut task, _cancel, _interval) = rule_task::RuleTask::new(config);

    for b in 0..4u32 {
        let batch = make_batch(&schema, &["10.0.0.1"], ts + b as i64);
        let size = content_bytes(&batch);
        window
            .append_with_watermark_sized(batch, size, None)
            .unwrap();
    }
    task.pull_and_advance().await;

    let floor = router
        .registry()
        .progress("auth_events")
        .expect("progress table exists")
        .min_acked();
    assert_eq!(
        floor, 3,
        "round-robin shard must ack its PROCESSED position (批 0、2 → 3)，not the read position (4)"
    );
}

#[tokio::test]
async fn pull_sharded_advances_ack_floor() {
    // pull 后必须把消费进度写进 WindowProgress slot（min_acked 跟上 cursor）。
    // 内存驱逐仍依赖这个地板；时间驱逐不再依赖它（见下方断言）。
    init_tracing();
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let (mut tasks, _rxs, win, router) = make_sharded_match_tasks(1);

    for b in 0..3u32 {
        let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.2"], ts + b as i64);
        let size = content_bytes(&batch);
        win.append_with_watermark_sized(batch, size, None).unwrap();
    }
    tasks[0].pull_and_advance().await;

    let floor = router
        .registry()
        .progress("auth_events")
        .expect("progress table exists")
        .min_acked();
    assert_eq!(floor, 3, "ack floor must equal batches processed + 1");

    // 时间驱逐现在纯按事件时间，忽略 ack floor：now 推进到 over 之后，即使
    // batch 已 ack（floor=3），过期批次仍被驱逐（慢规则会在这里观察到 pull gap）。
    assert_eq!(win.batch_count(), 3, "sanity: 3 batches buffered");
    win.evict_expired(ts + 3_600_000_000_000 + 1_000);
    assert_eq!(
        win.batch_count(),
        0,
        "time eviction drops expired batches regardless of ack floor"
    );
}
