//! Q13 双规则链（q13a 写中间窗 bid_mod → q13b join side_input 静态表）中间管道
//! 契约：中间窗积压触发内存驱逐时不得丢未读（消费者 ack floor 保护、宁可超内存），
//! 已读被回收; 分片 push round-robin 消费恰一次投递、无漏无重、3 路并行完整输出。

use super::*;

/// q13 双规则链复现（2026-08-25 100M RSS 27GB + memory_evicted_total=1479）：
///
/// 链：q13a（on each b → yield 中间窗 bid_mod）→ q13b（on each m + join
/// side_input 静态表）。复现目标：中间窗积压时（写入快、消费慢）——
/// ① 中间窗的内存驱逐**不得丢未读数据**（消费者 ack floor 保护，输出完整）；
/// ② 已读数据被驱逐回收（内存有界）。
///
/// 生产路径：task_b 通过 `register_progress` 注册 bid_mod 消费者槽（spawn.rs），
/// 测试模拟该配置；负向对照（无槽位）验证引擎对"未注册消费者"中间窗的驱逐行为
/// ——若生产中间窗漏注册，就会丢未读（memory_evicted_total 归因方向）。
#[tokio::test]
async fn q13_dual_chain_intermediate_window_pressure() {
    crate::engine_task::tests::init_tracing();
    let schemas = nexmark_schemas();
    let file = wf_lang::parse_wfl(Q13_WFL).expect("parse q13.wfl");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile q13.wfl");
    assert_eq!(
        plans.len(),
        2,
        "q13.wfl → 2 个 plan（q13a_bid_mod + q13b_side_input_join）"
    );
    let mut plans = plans.into_iter();
    let mut plan_a = plans.next().unwrap();
    let plan_b = plans.next().unwrap();
    plan_a.name = "q13a_bid_mod".into();

    // 中间窗预算 = 3 个中间 batch（每 bid batch 写 1 个中间 batch）——写入超过
    // 预算触发内存驱逐。probe 用 3 行 batch（与实际驱动批同构）。
    let probe = q13c_bid_mod_batch(&[(1, 1, 100, T), (1, 2, 200, T), (1, 3, 300, T)]);
    let one_batch_bytes = wf_engine::window::content_bytes(&probe);
    let bid_mod_budget = one_batch_bytes * 3;

    let mut registry = WindowRegistry::build(vec![
        q13c_window_def("bid_events", &q13c_bid_schema(), usize::MAX),
        q13c_window_def("bid_mod", &q13c_bid_mod_schema(), bid_mod_budget),
    ])
    .unwrap();
    // side_input 静态表：mod_key 1 → "v1"，2 → "v2"
    let mut pw = ProviderWindow::new("side_input".into(), "SELECT * FROM side_input".into(), None);
    pw.load(vec![
        {
            let mut m = HashMap::new();
            m.insert("key".to_string(), Value::Number(1.0));
            m.insert("value".to_string(), Value::Str("v1".into()));
            m
        },
        {
            let mut m = HashMap::new();
            m.insert("key".to_string(), Value::Number(2.0));
            m.insert("value".to_string(), Value::Str("v2".into()));
            m
        },
    ]);
    registry
        .register_provider("side_input".to_string(), pw)
        .unwrap();
    let router = Arc::new(Router::new(registry));

    // task_a：q13a（驱动 bid_events → yield 中间窗 bid_mod）
    let src_a = router.registry().get_window("bid_events").unwrap();
    let notify_a = router.registry().get_notifier("bid_events").unwrap();
    let executor_a = RuleExecutor::new(plan_a);
    let (alert_tx_a, _alert_rx_a) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let mut intermediate = HashSet::new();
    intermediate.insert("bid_mod".to_string());
    let config_a = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("b".into()),
        each_time_field: Some("dateTime".into()),
        executor: executor_a,
        window_sources: vec![task_types::WindowSource {
            window_name: "bid_events".into(),
            window: src_a,
            notify: notify_a,
            aliases: vec!["b".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx_a),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: std::time::Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: intermediate,
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (mut task_a, _cancel_a, _interval_a) = rule_task::RuleTask::new(config_a);

    // task_b：q13b（驱动 bid_mod + join side_input），注册 bid_mod 消费者槽
    let src_b = router.registry().get_window("bid_mod").unwrap();
    let notify_b = router.registry().get_notifier("bid_mod").unwrap();
    let executor_b = RuleExecutor::new(plan_b);
    let (alert_tx_b, mut alert_rx_b) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let progress_b = {
        let slot = router.registry().progress("bid_mod").unwrap().register();
        let mut m = HashMap::new();
        m.insert("bid_mod".to_string(), slot);
        m
    };
    let config_b = task_types::RuleTaskConfig {
        progress: progress_b,
        conv_sink: None,
        machine: None,
        each_alias: Some("m".into()),
        each_time_field: Some("dateTime".into()),
        executor: executor_b,
        window_sources: vec![task_types::WindowSource {
            window_name: "bid_mod".into(),
            window: src_b,
            notify: notify_b,
            aliases: vec!["m".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx_b),
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
    let (mut task_b, _cancel_b, _interval_b) = rule_task::RuleTask::new(config_b);

    // 驱动：5 个 bid batch（每批 3 行，auction 恒 1 → mod_key=1 → 命中 side_input）
    let bid_win = router.registry().get_window("bid_events").unwrap();
    for i in 0..5i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[
                (1, i * 3, 100 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 1, 101 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 2, 102 + i * 3, T + i * 1_000_000_000),
            ]))
            .unwrap();
    }

    // task_a 处理全部 5 个 bid batch → 写中间窗 5 个 batch（超 3 预算）
    task_a.pull_and_advance().await;
    let bm = router.registry().get_window("bid_mod").unwrap();
    assert_eq!(
        bm.batch_count(),
        5,
        "task_b 未读前中间窗不得驱逐（超预算保留，宁可内存）"
    );
    assert_eq!(bm.total_rows(), 15, "中间窗全量 15 行");

    // task_b 消费中间窗 + join 输出：全部 15 行必须富化输出（不丢未读）
    task_b.pull_and_advance().await;
    let mut values: Vec<String> = Vec::new();
    while let Ok(batch) = alert_rx_b.try_recv() {
        match batch {
            crate::alert_task::AlertBatch::Rows(rows) => {
                for r in rows.iter() {
                    values.push(crate::engine_task::tests::field_str(r, "detail"));
                }
            }
            crate::alert_task::AlertBatch::Columns(cols) => {
                for r in cols.iter_data_records().flatten() {
                    values.push(crate::engine_task::tests::field_str(&r, "detail"));
                }
            }
        }
    }
    assert_eq!(
        values.len(),
        15,
        "消费者 ack 保护：中间窗驱逐不得丢未读（输出完整 15/15）"
    );
    assert!(
        values.iter().all(|v| v == "v1"),
        "全部命中 side_input mod_key=1 → 富化 value=v1"
    );

    // task_a 继续写入 → 已读部分（前 3 batch）被驱逐回收，未读（后 2）保留
    for i in 5..8i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[
                (1, i * 3, 100 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 1, 101 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 2, 102 + i * 3, T + i * 1_000_000_000),
            ]))
            .unwrap();
    }
    task_a.pull_and_advance().await;
    assert!(
        bm.batch_count() <= 6,
        "已读 batch 被驱逐回收（内存有界），当前 {}",
        bm.batch_count()
    );

    // 剩余 9 行（5-7 批 + 未消费的）也能完整输出
    task_b.pull_and_advance().await;
    let mut more = 0usize;
    while let Ok(batch) = alert_rx_b.try_recv() {
        more += match batch {
            crate::alert_task::AlertBatch::Rows(rows) => rows.len(),
            crate::alert_task::AlertBatch::Columns(cols) => {
                cols.iter_data_records().flatten().count()
            }
        };
    }
    assert_eq!(more, 9, "已读驱逐后剩余未读仍完整输出 9/9");
}

/// q13 双规则链**分片 push 消费**（2026-08-25 q13 单 worker 串行根治）：
///
/// q13b（bind 中间窗 bid_mod 的 stateless each + snapshot join）以
/// `shard_count=3` 走 **push round-robin** 分片——每个中间窗批次**恰一次**
/// 投递到唯一 shard 通道（无 pull 共享游标、无重复、无漏投），3 路并行消费。
///
/// 这是 spawn.rs `consumes_intermediate` 分片放宽的机制级守护（生产 q13b 由
/// 此从单 worker ~400k EPS 提升到 10 路并行）：
/// ① round-robin 分布正确（每 shard 收到自己的批次，按序处理）；
/// ② 输出完整（15/15，无漏无重——push 恰一次投递 + ack 真实 seq）；
/// ③ 未读批次不被驱逐（超预算宁可内存，同单 worker 契约）；
/// ④ 已读批次被回收（内存有界）。
#[tokio::test]
async fn q13_dual_chain_sharded_push_consumption_complete() {
    crate::engine_task::tests::init_tracing();
    let schemas = nexmark_schemas();
    let file = wf_lang::parse_wfl(Q13_WFL).expect("parse q13.wfl");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile q13.wfl");
    assert_eq!(
        plans.len(),
        2,
        "q13.wfl → 2 个 plan（q13a_bid_mod + q13b_side_input_join）"
    );
    let mut plans = plans.into_iter();
    let mut plan_a = plans.next().unwrap();
    let plan_b = plans.next().unwrap();
    plan_a.name = "q13a_bid_mod".into();

    // 中间窗预算 = 3 个中间 batch（与单 worker 测试同口径）——写入超过预算
    // 触发内存驱逐；未读保护必须顶住。
    let probe = q13c_bid_mod_batch(&[(1, 1, 100, T), (1, 2, 200, T), (1, 3, 300, T)]);
    let one_batch_bytes = wf_engine::window::content_bytes(&probe);
    let bid_mod_budget = one_batch_bytes * 3;

    let mut registry = WindowRegistry::build(vec![
        q13c_window_def("bid_events", &q13c_bid_schema(), usize::MAX),
        q13c_window_def("bid_mod", &q13c_bid_mod_schema(), bid_mod_budget),
    ])
    .unwrap();
    // side_input 静态表：mod_key 1 → "v1"，2 → "v2"
    let mut pw = ProviderWindow::new("side_input".into(), "SELECT * FROM side_input".into(), None);
    pw.load(vec![
        {
            let mut m = HashMap::new();
            m.insert("key".to_string(), Value::Number(1.0));
            m.insert("value".to_string(), Value::Str("v1".into()));
            m
        },
        {
            let mut m = HashMap::new();
            m.insert("key".to_string(), Value::Number(2.0));
            m.insert("value".to_string(), Value::Str("v2".into()));
            m
        },
    ]);
    registry
        .register_provider("side_input".to_string(), pw)
        .unwrap();
    let router = Arc::new(Router::new(registry));

    // task_a：q13a 单 worker（驱动 bid_events → yield 中间窗 bid_mod）
    let src_a = router.registry().get_window("bid_events").unwrap();
    let notify_a = router.registry().get_notifier("bid_events").unwrap();
    let executor_a = RuleExecutor::new(plan_a);
    let (alert_tx_a, _alert_rx_a) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let mut intermediate = HashSet::new();
    intermediate.insert("bid_mod".to_string());
    let config_a = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("b".into()),
        each_time_field: Some("dateTime".into()),
        executor: executor_a,
        window_sources: vec![task_types::WindowSource {
            window_name: "bid_events".into(),
            window: src_a,
            notify: notify_a,
            aliases: vec!["b".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx_a),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: std::time::Duration::from_secs(60),
        router: Arc::clone(&router),
        metrics: None,
        intermediate_targets: intermediate,
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    let (mut task_a, _cancel_a, _interval_a) = rule_task::RuleTask::new(config_a);

    // task_b × 3 shards：push round-robin 消费 bid_mod（每 shard 独立通道 +
    // 独立 alert 输出通道 + 独立 progress 槽）。
    const SHARDS: usize = 3;
    let executor_b = RuleExecutor::new(plan_b);
    let mut shard_txs: Vec<mpsc::Sender<wf_engine::window::RulePush>> = Vec::new();
    let mut shard_alert_rxs: Vec<mpsc::Receiver<crate::alert_task::AlertBatch>> = Vec::new();
    let mut shard_tasks = Vec::new();
    let mut shard_rxs: Vec<Option<mpsc::Receiver<wf_engine::window::RulePush>>> = Vec::new();
    for shard_idx in 0..SHARDS {
        let (push_tx, push_rx) = mpsc::channel::<wf_engine::window::RulePush>(16);
        shard_txs.push(push_tx);
        let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
        shard_alert_rxs.push(alert_rx);
        let progress_b = {
            let slot = router.registry().progress("bid_mod").unwrap().register();
            let mut m = HashMap::new();
            m.insert("bid_mod".to_string(), slot);
            m
        };
        let config_b = task_types::RuleTaskConfig {
            progress: progress_b,
            conv_sink: None,
            machine: None,
            each_alias: Some("m".into()),
            each_time_field: Some("dateTime".into()),
            executor: executor_b.clone(),
            window_sources: vec![task_types::WindowSource {
                window_name: "bid_mod".into(),
                window: router.registry().get_window("bid_mod").unwrap(),
                notify: router.registry().get_notifier("bid_mod").unwrap(),
                aliases: vec!["m".into()],
            }],
            sink_fanout: make_test_fanout(alert_tx),
            cancel: tokio_util::sync::CancellationToken::new(),
            timeout_scan_interval: std::time::Duration::from_secs(60),
            router: Arc::clone(&router),
            metrics: None,
            intermediate_targets: HashSet::new(),
            pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
            eos_flush: tokio::sync::watch::channel(0u64).1,
            push_rx: Some(push_rx),
            shard_index: Some(shard_idx),
            shard_count: SHARDS,
            key_partitioned: false,
        };
        let (mut task_b, _cancel_b, _interval_b) = rule_task::RuleTask::new(config_b);
        // RuleTask 持有 push_rx（run_rule_task 里 take）；测试自己保留接收端
        // 以便驱动 drain_push_channel。
        shard_rxs.push(Some(task_b.push_rx.take().unwrap()));
        shard_tasks.push(task_b);
    }
    // 生产 spawn 路径：fanout round-robin 订阅（flush_pipes 广播 → 恰一次投递）
    router.fanout().register_round_robin("bid_mod", shard_txs);

    // 驱动 5 个 bid batch（每批 3 行，auction 恒 1 → mod_key=1 → 命中 v1）
    let bid_win = router.registry().get_window("bid_events").unwrap();
    for i in 0..5i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[
                (1, i * 3, 100 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 1, 101 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 2, 102 + i * 3, T + i * 1_000_000_000),
            ]))
            .unwrap();
    }

    // task_a 处理全部 5 个 bid batch → 写中间窗 5 批 + round-robin 广播
    task_a.pull_and_advance().await;
    let bm = router.registry().get_window("bid_mod").unwrap();
    assert_eq!(
        bm.batch_count(),
        5,
        "task_b 未读前中间窗不得驱逐（超预算保留，宁可内存）"
    );
    assert_eq!(bm.total_rows(), 15, "中间窗全量 15 行");

    // 各 shard 消化自己的批次：round-robin 分布 = 批 0,3 → shard0；1,4 → shard1；
    // 2 → shard2（每批 3 行）→ 6/6/3。输出必须完整 15/15（恰一次投递）。
    let mut per_shard_rows = vec![0usize; SHARDS];
    for (i, task) in shard_tasks.iter_mut().enumerate() {
        let rx = shard_rxs[i].as_mut().unwrap();
        task.drain_push_channel(rx).await;
        let rx = &mut shard_alert_rxs[i];
        let mut values: Vec<String> = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            match batch {
                crate::alert_task::AlertBatch::Rows(rows) => {
                    for r in rows.iter() {
                        values.push(crate::engine_task::tests::field_str(r, "detail"));
                    }
                }
                crate::alert_task::AlertBatch::Columns(cols) => {
                    for r in cols.iter_data_records().flatten() {
                        values.push(crate::engine_task::tests::field_str(&r, "detail"));
                    }
                }
            }
        }
        per_shard_rows[i] = values.len();
        assert!(
            values.iter().all(|v| v == "v1"),
            "shard {i} 全部命中 side_input mod_key=1 → 富化 value=v1"
        );
    }
    assert_eq!(
        per_shard_rows,
        vec![6, 6, 3],
        "round-robin 分布：批 0,3→shard0(6 行)；批 1,4→shard1(6 行)；批 2→shard2(3 行)"
    );
    assert_eq!(
        per_shard_rows.iter().sum::<usize>(),
        15,
        "消费者 ack 保护：push 恰一次投递，输出完整 15/15（无漏无重）"
    );

    // 已读（3 shard 全部消化）后继续写入 → 超预算部分被驱逐回收，内存有界
    for i in 5..8i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[
                (1, i * 3, 100 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 1, 101 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 2, 102 + i * 3, T + i * 1_000_000_000),
            ]))
            .unwrap();
    }
    task_a.pull_and_advance().await;
    assert!(
        bm.batch_count() <= 6,
        "已读 batch 被驱逐回收（内存有界），当前 {}",
        bm.batch_count()
    );

    // 剩余 9 行（批 5-7）完整输出
    let mut more = 0usize;
    for (i, task) in shard_tasks.iter_mut().enumerate() {
        let rx = shard_rxs[i].as_mut().unwrap();
        task.drain_push_channel(rx).await;
        let rx = &mut shard_alert_rxs[i];
        while let Ok(batch) = rx.try_recv() {
            more += match batch {
                crate::alert_task::AlertBatch::Rows(rows) => rows.len(),
                crate::alert_task::AlertBatch::Columns(cols) => {
                    cols.iter_data_records().flatten().count()
                }
            };
        }
    }
    assert_eq!(more, 9, "已读驱逐后剩余未读仍完整输出 9/9（3 shard 合计）");
}
