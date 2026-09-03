//! 真实 wfl 编译集成收口：q9.wfl 源码 → parse/compile（checker+compiler）→ RulePlan
//! → rule_task 执行全链路（编译产物与手写 plan 差异的排查盲区）; 分片 worker flush 必须
//! 用驱动窗口全局尾部评估剩余挂起（自身 watermark 停在最后处理批次的时刻会漏尾部）。

use super::*;

/// 真实 q9.wfl 编译 → rule_task 执行：挂起 → watermark 过 expiry → 输出胜者。
#[tokio::test]
async fn deferred_q9_real_wfl_compiled_plan_runs() {
    crate::engine_task::tests::init_tracing();
    let schemas = nexmark_schemas();

    // 1) 真实 wfl → parse + compile（checker + compiler 全链路）
    let file = wf_lang::parse_wfl(Q9_WFL).expect("parse q9.wfl");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile q9.wfl");
    assert_eq!(plans.len(), 1, "q9.wfl → 1 个 plan");
    let plan = plans.into_iter().next().unwrap();
    // 编译产物断言：deferred 形态完整落入 JoinPlan（手写 plan 测试覆盖不到这里）
    assert!(plan.each_plan.is_some());
    let join = &plan.joins[0];
    assert!(
        join.emit_at.is_some(),
        "emit_at must survive compilation (deferred 标记)"
    );
    assert!(join.within.is_some(), "within 区间必须编译进 plan");
    assert_eq!(
        join.reduce.as_ref().and_then(|r| r.label.as_deref()),
        Some("winner"),
        "reduce `as winner` label 必须编译进 plan"
    );

    // 2) 编译产物 → rule_task（each_time_field = schema time = dateTime，同 daemon）
    let driver = "auction_events";
    let registry = WindowRegistry::build(vec![
        q9c_window_def(driver, &q9c_auction_schema()),
        q9c_window_def("bid_events", &q9c_bid_schema()),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let source_window = router.registry().get_window(driver).unwrap();
    let source_notify = router.registry().get_notifier(driver).unwrap();

    let executor = RuleExecutor::new(plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("a".into()),
        each_time_field: Some("dateTime".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: driver.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["a".into()],
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
    let (mut task, _cancel, _interval) = rule_task::RuleTask::new(config);
    let mut alert_rx = alert_rx;

    // 3) 事件：auction5(T, expires=T+60s) → bid 100@T+10s / 200@T+20s
    //    → auction6(T+61s) 推 watermark 过 T+60s → auction5 到期输出胜者
    router
        .registry()
        .get_window("auction_events")
        .unwrap()
        .append_with_watermark(q9c_auction_batch(&[(5, 42, T, T + 60_000_000_000)]))
        .unwrap();
    router
        .registry()
        .get_window("bid_events")
        .unwrap()
        .append_with_watermark(q9c_bid_batch(&[
            (5, 1, 100, T + 10_000_000_000),
            (5, 2, 200, T + 20_000_000_000),
        ]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "未到期 — 不输出");

    router
        .registry()
        .get_window("auction_events")
        .unwrap()
        .append_with_watermark(q9c_auction_batch(&[(
            6,
            43,
            T + 61_000_000_000,
            T + 121_000_000_000,
        )]))
        .unwrap();
    // 目标窗口追平（bid 6 随 auction 6 到达，max_event_time 推过 T+60s）
    router
        .registry()
        .get_window("bid_events")
        .unwrap()
        .append_with_watermark(q9c_bid_batch(&[(6, 3, 300, T + 61_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "编译产物的 deferred join 必须输出 auction 5 的胜者"
    );
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "detail"),
        "winner 2",
        "maxrow(price) 胜者 = price 200（bidder=2），label 注入 detail"
    );
}

/// 分片尾部评估（2026-08-24 q4/q9 分片修复）：round-robin 分片后 worker 自身
/// watermark 停在**最后处理批次**的时刻，而驱动窗口的全局尾部（其他 worker 拿到
/// 的更晚批次）可能更靠后——flush（EOS）必须用**驱动窗口全局最终事件时间**评估
/// 剩余挂起（expiry ≤ 全局末尾），否则尾部 pending 永不评估。
///
/// q4 30M 实测：修复前丢 869 条（1,671,690 vs 1,672,559）；修复后 identical。
#[tokio::test]
async fn deferred_flush_uses_global_window_tail_for_sharded_workers() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // bid 先到（auction=5，price 200）。
    bid_window(&router)
        .append(bid_batch(&[(5, 2, 200, T + 20_000_000_000)]))
        .unwrap();
    // auction=5（expiry = T+60s）到达并处理：挂起创建，任务自身 watermark = T。
    auction_window(&router)
        .append(auction_batch(&[(5, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // 模拟「其他 worker 拿到更晚批次」：驱动窗口 append auction=9（dateTime
    // T+120s），但**本任务不处理它**（不调 pull_and_advance）→ 窗口全局尾部推进
    // 到 T+120s，任务自身 watermark 仍是 T。用 append_with_watermark：普通
    // append 不更新窗口 max_event_time（watermark.rs L128），flush 读不到全局尾部。
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            9,
            T + 120_000_000_000,
            T + 180_000_000_000,
        )]))
        .unwrap();

    // flush：必须用窗口全局尾部（T+120s ≥ expiry T+60s）评估 auction=5 的挂起
    // ——修复前用自身 watermark（T）会漏评估。
    task.flush().await;

    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "flush 必须用驱动窗口全局尾部评估尾部挂起（分片 worker 自身 watermark 不足）"
    );
}
