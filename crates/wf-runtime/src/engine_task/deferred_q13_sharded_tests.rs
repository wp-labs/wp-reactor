//! Q13 分片 push/pull 与 ack 语义守护：大斜率背压（紧通道广播）卡尾复现、分片
//! 生产者乱序广播、pull 分片 ack=处理位置（读位置 ack 会致 min_acked 追平丢保护）、
//! q13a 编译计划走 pipe 列式路径、未注册消费者中间窗驱逐丢数据（生产漏注册归因）。

use super::*;

/// q13 双规则链**分片 push 大斜率复现**（2026-08-25 生产 666 批 × 10 shard
/// 卡尾）：生产 q13b 分片后只处理了 ~1 批/shard（emitted ~53k/shard）就停，
/// 但 ack 却推进到 657/666——ack 不反映处理。本测试用 10 shard + 紧通道
/// （cap=2，强制广播背压）+ 70 批驱动，统计总输出：若输出 < 210 行即为
/// 复现（批被 ack 未处理），定位静默吞批点。
#[tokio::test]
async fn q13_dual_chain_sharded_push_high_slope_repro() {
    crate::engine_task::tests::init_tracing();
    let schemas = nexmark_schemas();
    let file = wf_lang::parse_wfl(Q13_WFL).expect("parse q13.wfl");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile q13.wfl");
    assert_eq!(plans.len(), 2);
    let mut plans = plans.into_iter();
    let mut plan_a = plans.next().unwrap();
    let plan_b = plans.next().unwrap();
    plan_a.name = "q13a_bid_mod".into();

    let probe = q13c_bid_mod_batch(&[(1, 1, 100, T), (1, 2, 200, T), (1, 3, 300, T)]);
    let one_batch_bytes = wf_engine::window::content_bytes(&probe);

    let mut registry = WindowRegistry::build(vec![
        q13c_window_def("bid_events", &q13c_bid_schema(), usize::MAX),
        q13c_window_def("bid_mod", &q13c_bid_mod_schema(), one_batch_bytes * 70 * 2),
    ])
    .unwrap();
    let mut pw = ProviderWindow::new("side_input".into(), "SELECT * FROM side_input".into(), None);
    pw.load(vec![{
        let mut m = HashMap::new();
        m.insert("key".to_string(), Value::Number(1.0));
        m.insert("value".to_string(), Value::Str("v1".into()));
        m
    }]);
    registry
        .register_provider("side_input".to_string(), pw)
        .unwrap();
    let router = Arc::new(Router::new(registry));

    // task_a：q13a 单 worker
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
    let (task_a, _cancel_a, _interval_a) = rule_task::RuleTask::new(config_a);

    // task_b × 10 shards：紧通道 cap=2
    const SHARDS: usize = 10;
    let executor_b = RuleExecutor::new(plan_b);
    let mut shard_txs: Vec<mpsc::Sender<wf_engine::window::RulePush>> = Vec::new();
    let mut shard_alert_rxs: Vec<mpsc::Receiver<crate::alert_task::AlertBatch>> = Vec::new();
    let mut shard_tasks = Vec::new();
    let mut shard_rxs: Vec<Option<mpsc::Receiver<wf_engine::window::RulePush>>> = Vec::new();
    for shard_idx in 0..SHARDS {
        let (push_tx, push_rx) = mpsc::channel::<wf_engine::window::RulePush>(2);
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
        shard_rxs.push(Some(task_b.push_rx.take().unwrap()));
        shard_tasks.push(task_b);
    }
    router.fanout().register_round_robin("bid_mod", shard_txs);

    // 驱动 70 个 bid batch（3 行/批）。task_a 处理时广播会被满通道背压阻塞
    // ——必须与 shard 消费并发（生产即如此）。task_a 放 spawn，主循环边
    // drain shard 边等 task_a 收口。
    let bid_win = router.registry().get_window("bid_events").unwrap();
    for i in 0..70i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[
                (1, i * 3, 100 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 1, 101 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 2, 102 + i * 3, T + i * 1_000_000_000),
            ]))
            .unwrap();
    }
    let mut task_a = task_a;
    let producer = tokio::spawn(async move { task_a.pull_and_advance().await });
    let mut pending = 0usize;
    loop {
        let mut drained = 0usize;
        for (i, task) in shard_tasks.iter_mut().enumerate() {
            let rx = shard_rxs[i].as_mut().unwrap();
            task.drain_push_channel(rx).await;
            let rx = &mut shard_alert_rxs[i];
            while let Ok(batch) = rx.try_recv() {
                drained += match batch {
                    crate::alert_task::AlertBatch::Rows(rows) => rows.len(),
                    crate::alert_task::AlertBatch::Columns(cols) => {
                        cols.iter_data_records().flatten().count()
                    }
                };
            }
        }
        pending += drained;
        if producer.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    producer.await.unwrap();
    // task_a 收口后可能还有残余广播：再 drain 一轮
    for (i, task) in shard_tasks.iter_mut().enumerate() {
        let rx = shard_rxs[i].as_mut().unwrap();
        task.drain_push_channel(rx).await;
        let rx = &mut shard_alert_rxs[i];
        while let Ok(batch) = rx.try_recv() {
            pending += match batch {
                crate::alert_task::AlertBatch::Rows(rows) => rows.len(),
                crate::alert_task::AlertBatch::Columns(cols) => {
                    cols.iter_data_records().flatten().count()
                }
            };
        }
    }

    let bm = router.registry().get_window("bid_mod").unwrap();
    eprintln!(
        "[repro] bid_mod batches={} rows={}",
        bm.batch_count(),
        bm.total_rows()
    );
    eprintln!("[repro] total output = {pending} (expect 210)");
    assert_eq!(
        pending, 210,
        "分片 push 大斜率：全部 210 行必须输出（ack 不得先于处理）"
    );
}

/// q13 双规则链**生产者分片 + 消费者分片**（2026-08-25 q13a 分片放开守护）：
///
/// 生产：2 个 q13a shard（round-robin pull 驱动 bid_events）→ bid_mod 批次
/// **并发乱序** append + 广播（shard0 处理偶数批、shard1 处理奇数批）。
/// 消费：3 个 q13b shard（push round-robin）→ 每批恰一次投递。
///
/// 断言：① 输出完整（60/60，乱序广播不丢不重）；② 消费 ack 单调
/// （fetch_max——乱序到达的 seq 不回退 ack floor）。
#[tokio::test]
async fn q13_dual_chain_sharded_producer_and_consumer() {
    crate::engine_task::tests::init_tracing();
    let schemas = nexmark_schemas();
    let file = wf_lang::parse_wfl(Q13_WFL).expect("parse q13.wfl");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile q13.wfl");
    assert_eq!(plans.len(), 2);
    let mut plans = plans.into_iter();
    let mut plan_a = plans.next().unwrap();
    let plan_b = plans.next().unwrap();
    plan_a.name = "q13a_bid_mod".into();

    let probe = q13c_bid_mod_batch(&[(1, 1, 100, T), (1, 2, 200, T), (1, 3, 300, T)]);
    let one_batch_bytes = wf_engine::window::content_bytes(&probe);

    let mut registry = WindowRegistry::build(vec![
        q13c_window_def("bid_events", &q13c_bid_schema(), usize::MAX),
        q13c_window_def("bid_mod", &q13c_bid_mod_schema(), one_batch_bytes * 30),
    ])
    .unwrap();
    let mut pw = ProviderWindow::new("side_input".into(), "SELECT * FROM side_input".into(), None);
    pw.load(vec![{
        let mut m = HashMap::new();
        m.insert("key".to_string(), Value::Number(1.0));
        m.insert("value".to_string(), Value::Str("v1".into()));
        m
    }]);
    registry
        .register_provider("side_input".to_string(), pw)
        .unwrap();
    let router = Arc::new(Router::new(registry));

    // q13a × 2 shards（round-robin pull 分片，模拟 spawn 的 pull 门控）
    const PRODUCER_SHARDS: usize = 2;
    let mut producer_tasks = Vec::new();
    for shard_idx in 0..PRODUCER_SHARDS {
        let src_a = router.registry().get_window("bid_events").unwrap();
        let notify_a = router.registry().get_notifier("bid_events").unwrap();
        let executor_a = RuleExecutor::new(plan_a.clone());
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
            shard_index: Some(shard_idx),
            shard_count: PRODUCER_SHARDS,
            key_partitioned: false,
        };
        let (task_a, _cancel_a, _interval_a) = rule_task::RuleTask::new(config_a);
        producer_tasks.push(task_a);
    }

    // q13b × 3 shards（push round-robin 消费 bid_mod）
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
        shard_rxs.push(Some(task_b.push_rx.take().unwrap()));
        shard_tasks.push(task_b);
    }
    router.fanout().register_round_robin("bid_mod", shard_txs);

    // 驱动 20 个 bid batch（3 行/批 = 60 行）
    let bid_win = router.registry().get_window("bid_events").unwrap();
    for i in 0..20i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[
                (1, i * 3, 100 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 1, 101 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 2, 102 + i * 3, T + i * 1_000_000_000),
            ]))
            .unwrap();
    }
    // 两个生产者 shard 各自 pull：round-robin 门控（batch_seq % 2 == shard）
    // 只处理自己的批次 → bid_mod 乱序 append + 广播。
    for task in producer_tasks.iter_mut() {
        task.pull_and_advance().await;
    }

    // 消费者分片消化：总输出必须 60/60（乱序广播不丢不重）
    let mut total = 0usize;
    for (i, task) in shard_tasks.iter_mut().enumerate() {
        let rx = shard_rxs[i].as_mut().unwrap();
        task.drain_push_channel(rx).await;
        let rx = &mut shard_alert_rxs[i];
        while let Ok(batch) = rx.try_recv() {
            total += match batch {
                crate::alert_task::AlertBatch::Rows(rows) => rows.len(),
                crate::alert_task::AlertBatch::Columns(cols) => {
                    cols.iter_data_records().flatten().count()
                }
            };
        }
    }
    eprintln!("[producer-shard] total output = {total} (expect 60)");
    assert_eq!(
        total, 60,
        "生产者分片乱序广播：全部 60 行必须输出（每批恰一次投递）"
    );
}

/// q13a 分片 pull 的 ack 语义守护（2026-08-25 隐患修复）：分片（whole-batch
/// round-robin）下 ack 必须是**处理位置**（本 shard 份额内最后处理批次+1），
/// 而非**读位置**（`new_cursor`=全部批次）。读位置 ack 会让 `min_acked` 追平
/// `next_seq` → `bid_events` 驱逐无未读保护 → cap/时间驱逐可能删掉其他 shard
/// 尚未处理的批次（cursor gap 静默丢数据）。
///
/// 场景：2 shard 分片 pull bid_events，4 批（next_seq=4）。shard0 处理批 0,2
/// （ack=3）、shard1 处理批 1,3（ack=4）。断言：min_acked=3（≠4，未处理份额
/// 受保护）、max_acked=4（完成信号 = next_seq，哨兵排空判定不受影响）。
#[tokio::test]
async fn q13_sharded_pull_acks_processed_not_read_position() {
    crate::engine_task::tests::init_tracing();
    let schemas = nexmark_schemas();
    let file = wf_lang::parse_wfl(Q13_WFL).expect("parse q13.wfl");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile q13.wfl");
    let mut plans = plans.into_iter();
    let mut plan_a = plans.next().unwrap();
    plan_a.name = "q13a_bid_mod".into();

    let registry = WindowRegistry::build(vec![q13c_window_def(
        "bid_events",
        &q13c_bid_schema(),
        usize::MAX,
    )])
    .unwrap();
    let router = Arc::new(Router::new(registry));

    // 2 个 q13a shard，各自注册 bid_events 消费 slot
    const SHARDS: usize = 2;
    let mut tasks = Vec::new();
    let mut slots = Vec::new();
    for shard_idx in 0..SHARDS {
        let src = router.registry().get_window("bid_events").unwrap();
        let notify = router.registry().get_notifier("bid_events").unwrap();
        let executor_a = RuleExecutor::new(plan_a.clone());
        let (alert_tx_a, _alert_rx_a) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
        let mut intermediate = HashSet::new();
        intermediate.insert("bid_mod".to_string());
        let slot = router.registry().progress("bid_events").unwrap().register();
        let mut progress = HashMap::new();
        progress.insert("bid_events".to_string(), slot.clone());
        slots.push(slot);
        let config_a = task_types::RuleTaskConfig {
            progress,
            conv_sink: None,
            machine: None,
            each_alias: Some("b".into()),
            each_time_field: Some("dateTime".into()),
            executor: executor_a,
            window_sources: vec![task_types::WindowSource {
                window_name: "bid_events".into(),
                window: src,
                notify,
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
            shard_index: Some(shard_idx),
            shard_count: SHARDS,
            key_partitioned: false,
        };
        let (task_a, _cancel_a, _interval_a) = rule_task::RuleTask::new(config_a);
        tasks.push(task_a);
    }

    // 驱动 4 批
    let bid_win = router.registry().get_window("bid_events").unwrap();
    for i in 0..4i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[
                (1, i * 3, 100 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 1, 101 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 2, 102 + i * 3, T + i * 1_000_000_000),
            ]))
            .unwrap();
    }
    let progress = router.registry().progress("bid_events").unwrap();
    let next = bid_win.next_seq();
    assert_eq!(next, 4);

    // shard0 先 pull：处理批 0,2（round-robin 门控）→ ack 处理位置 3
    tasks[0].pull_and_advance().await;
    assert_eq!(
        slots[0].load(std::sync::atomic::Ordering::Acquire),
        3,
        "shard0 ack = 处理位置（批 0,2 → 3），不是读位置 4"
    );
    assert_eq!(
        progress.min_acked(),
        0,
        "shard1 未 pull → min_acked=0：全部批次受驱逐保护（未读）"
    );

    // shard1 pull：处理批 1,3 → ack 4
    tasks[1].pull_and_advance().await;
    assert_eq!(slots[1].load(std::sync::atomic::Ordering::Acquire), 4);
    assert_eq!(
        progress.min_acked(),
        3,
        "min = 最慢 shard 处理位置（shard0=3）——未处理份额不驱逐"
    );
    assert_eq!(
        progress.max_acked(),
        4,
        "max = 完成信号 = next_seq：全部批次已被其归属 shard 处理"
    );
}

/// 真实编译的 q13a 计划必须走 **pipe 列式装载** 快路径（2026-08-25 q13a
/// 列式化守护）：`each_pipe_columnar_safe` 对生产 q13a（5 Field + `%` BinOp
/// yield，yield 中间窗 bid_mod）返回 true——否则列式化对生产规则不生效。
#[test]
fn q13a_compiled_plan_takes_pipe_columnar_path() {
    let schemas = nexmark_schemas();
    let file = wf_lang::parse_wfl(Q13_WFL).expect("parse q13.wfl");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile q13.wfl");
    let plan = plans.into_iter().next().expect("q13a first");
    let executor = RuleExecutor::new(plan);
    assert!(
        executor.each_pipe_columnar_safe(),
        "真实 q13a 计划必须通过 pipe 列式门控（projection + mod BinOp）"
    );
    assert_eq!(executor.live_joins().len(), 0, "q13a 无 join");
}

/// q13 双规则链**无消费者槽位**对照：中间窗无 ack 保护时驱逐自由删未读 →
/// 下游输出丢失（引擎依赖消费者注册——生产 `register_progress` 已注册；
/// 此对照证明该依赖是丢数据的守卫，任何漏注册都是正确性事故）。
#[tokio::test]
async fn q13_dual_chain_intermediate_window_unregistered_consumer_loses() {
    crate::engine_task::tests::init_tracing();
    let schemas = nexmark_schemas();
    let file = wf_lang::parse_wfl(Q13_WFL).expect("parse q13.wfl");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile q13.wfl");
    let mut plans = plans.into_iter();
    let mut plan_a = plans.next().unwrap();
    let _plan_b = plans.next().unwrap();
    plan_a.name = "q13a_bid_mod".into();

    let probe = q13c_bid_mod_batch(&[(1, 1, 100, T)]);
    let one_batch_bytes = wf_engine::window::content_bytes(&probe);
    let registry = WindowRegistry::build(vec![
        q13c_window_def("bid_events", &q13c_bid_schema(), usize::MAX),
        q13c_window_def("bid_mod", &q13c_bid_mod_schema(), one_batch_bytes * 2),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));
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

    // 无消费者槽位：写 5 个 batch 超 2 预算 → append_inner 驱逐自由（min_acked=MAX）
    let bid_win = router.registry().get_window("bid_events").unwrap();
    for i in 0..5i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[(1, i, 100 + i, T + i * 1_000_000_000)]))
            .unwrap();
    }
    task_a.pull_and_advance().await;
    let bm = router.registry().get_window("bid_mod").unwrap();
    assert!(
        bm.batch_count() < 5,
        "无消费者槽位：中间窗驱逐自由删（min_acked=u64::MAX）——若生产漏注册即丢数据"
    );
}
