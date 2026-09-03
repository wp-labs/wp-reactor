//! ranked（Q18/Q19 last/top）形状任务接线: rich close 每桶多条目 + 行字段注入
//! yield; perf-diag 门控消融（cut_rules/cut_output/cut_alert）; pull 模式 actor
//! 超预算 evictor 自愈; 列式 close 分块 flush 与一次性全量逐字节一致。

use super::*;
/// Q18/Q19 形状任务（P4 last/top）: 用 price/bidder/auction/event_time schema。
/// 返回 (task, alert_rx); detail 由调用方给 Expr 决定。
fn make_ranked_task(
    keys: Vec<Expr>,
    measures: Vec<StatsMeasurePlan>,
    detail: Expr,
) -> (StatsTask, mpsc::Receiver<crate::alert_task::AlertBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let win = Arc::new(Window::new(
        WindowParams {
            name: "bid_events".into(),
            schema: schema.clone(),
            time_col_index: Some(3),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        crate::engine_task::tests::test_window_config(usize::MAX),
    ));
    let notify = Arc::new(tokio::sync::Notify::new());
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = ranked_task_config(keys, measures, detail, win, notify, HashMap::new());
    let config = StatsTaskConfig {
        sink_fanout: make_test_fanout(alert_tx),
        ..config
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx)
}

/// 构造 ranked（last/top）stats 任务的完整配置。窗口/notify/progress 由调用方给
/// ——生产接线（registry 窗口 + actor 通知 + 分片 progress）与测试裸 Window 共用。
fn ranked_task_config(
    keys: Vec<Expr>,
    measures: Vec<StatsMeasurePlan>,
    detail: Expr,
    win: Arc<Window>,
    notify: Arc<tokio::sync::Notify>,
    progress: HashMap<String, Arc<AtomicU64>>,
) -> StatsTaskConfig {
    let _schema = win.schema().clone();
    let plan = StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
        keys,
        output_shape: StatsOutputShapePlan::Rows,
        measures: measures.clone(),
        tracked_bind_fields: HashMap::new(),
    };
    let rp = wf_lang::plan::RulePlan {
        name: "ranked_stats".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: vec![],
        match_plan: wf_lang::plan::MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::And,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(plan.clone()),
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
                },
                YieldField {
                    name: "detail".into(),
                    value: detail,
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    };
    // last/top 行字段提取子集（P5: 生产经 spawn 恒有; 测试用全 schema 字段）——
    // 无子集时列数组列序不定, 任务层注入需要列名。
    let row_subset: Option<std::sync::Arc<std::collections::HashSet<String>>> =
        Some(std::sync::Arc::new(
            ["price", "bidder", "auction", "event_time"]
                .into_iter()
                .map(String::from)
                .collect(),
        ));
    StatsTaskConfig {
        stats: StatsExecutor::with_row_fields(plan.clone(), row_subset),
        executor: RuleExecutor::new(rp),
        window_sources: vec![crate::engine_task::task_types::WindowSource {
            window_name: "bid_events".into(),
            window: win,
            notify,
            aliases: vec!["b".into()],
        }],
        sink_fanout: crate::engine_task::tests::make_test_fanout(mpsc::channel(1).0),
        cancel: tokio_util::sync::CancellationToken::new(),
        router: Arc::new(wf_engine::window::Router::new(
            wf_engine::window::WindowRegistry::build(vec![]).unwrap(),
        )),
        metrics: None,
        time_field: Some("event_time".into()),
        timeout_scan_interval: Duration::from_secs(1),
        intermediate_targets: std::collections::HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        progress,
        shard_index: None,
        shard_count: 1,
        merge_rx: None,
        merge_tx: None,
        mask_cache: None,
    }
}

/// 带 price/bidder/auction 的批次（q18/q19 任务测试用）。
fn make_bid_batch(rows: &[(i64, i64, i64)], ts: i64) -> RecordBatch {
    use arrow::array::Int64Array;
    let n = rows.len();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int64, true),
            Field::new("bidder", DataType::Int64, true),
            Field::new("auction", DataType::Int64, true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| Some(r.0)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| Some(r.1)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| Some(r.2)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![ts; n])),
        ],
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// P4 last/top 任务接线（Q18/Q19）: rich close 每桶多条目 + 行字段注入 yield
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// perf-diag cuts（2026-08-25 补 stats 缺口）: 与 rule_task 对齐——cut_rules
// 归并直通（空窗无输出）, cut_output 输出直通（归并正常但 alert 被切）。
// ---------------------------------------------------------------------------

/// q19 形状任务（top + fmt detail, 门控通过走列式 close）。
fn make_q19_cut_task() -> (StatsTask, mpsc::Receiver<crate::alert_task::AlertBatch>) {
    let detail = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("{} {}".into()),
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
        ],
    };
    make_ranked_task(
        vec![Expr::Field(FieldRef::Qualified(
            "b".into(),
            "auction".into(),
        ))],
        vec![StatsMeasurePlan {
            label: "top_price".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Top,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: Some(2),
        }],
        detail,
    )
}

#[tokio::test]
async fn stats_task_perf_cut_rules_no_emit() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // cut_rules 单切（rules 档的 output 侧对照）: 归并直通 → 窗口无事件 →
    // 空窗不产出（无 EMIT）。恢复后同一数据正常归并 + 输出。全局门控跨
    // await 持锁（PERF_CUT_SERIAL）——否则并行测试期间其它 stats 测试被切。
    crate::perf_diag::set_perf_cuts(true, false, false, false, false);
    let (mut task, mut alert_rx) = make_q19_cut_task();
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "cut_rules: 无归并 → 空窗无输出"
    );
    crate::perf_diag::reset_perf_diag();

    // 恢复: 同数据重推 → 正常产出（窗口已重置, 重新开窗）。
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1)], 5_000_000_000),
        2,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 2, "恢复后 top-2 两条");
}

#[tokio::test]
async fn stats_task_perf_cut_output_keeps_accumulate_no_emit() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // full 档的对照: cut_output 只切输出链——归并照常（窗口 close 状态正确
    // 重置）, alert 不投递。恢复后正常输出。全局门控跨 await 持锁。
    crate::perf_diag::set_perf_cuts(false, true, false, false, false);
    let (mut task, mut alert_rx) = make_q19_cut_task();
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    assert!(alert_rx.try_recv().is_err(), "cut_output: 输出链直通");
    crate::perf_diag::reset_perf_diag();

    // 恢复: 新窗口数据正常产出（前窗已被 cut_output 正确 close 重置——若泄漏
    // 会污染本窗输出）。
    push_batch(
        &mut task,
        make_bid_batch(&[(150, 1, 1), (250, 2, 1)], 11_000_000_000),
        2,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 2, "恢复后 top-2 两条（前窗无泄漏）");
}

#[tokio::test]
async fn stats_task_perf_cut_alert_keeps_accumulate_no_emit() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // 输出链消融（q19 同款列式 close + top 度量）: 只切 alert 构建——归并照常
    // （窗口 close 状态正确重置）, CloseOutput 构造照常, alert 不投递。恢复后
    // 正常输出。全局门控跨 await 持锁（PERF_CUT_SERIAL）。
    crate::perf_diag::set_perf_cut_alert_for_test(true);
    let (mut task, mut alert_rx) = make_q19_cut_task();
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    assert!(alert_rx.try_recv().is_err(), "cut_alert: alert 构建被切");
    crate::perf_diag::set_perf_cut_alert_for_test(false);

    // 恢复: 新窗口数据正常产出（前窗已被 cut_alert 正确 close 重置——若泄漏
    // 会污染本窗输出）。
    push_batch(
        &mut task,
        make_bid_batch(&[(150, 1, 1), (250, 2, 1)], 11_000_000_000),
        2,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 2, "恢复后 top-2 两条（前窗无泄漏）");
}

#[tokio::test]
async fn q18_stats_task_last_bid_fields_injected() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // Q18 形状: group by (bidder, auction), last(price) —— 每键一条 alert,
    // detail 读最后一条 bid 的 price（行字段经 field_values 注入 yield 的 b.price）。
    let detail = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("{} {}".into()),
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
        ],
    };
    let (mut task, mut alert_rx) = make_ranked_task(
        vec![
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        ],
        vec![StatsMeasurePlan {
            label: "last_price".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Last,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: None,
        }],
        detail,
    );
    // 同一 (bidder=5, auction=1): 两条 bid, price 100 → 200; last = 200
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 5, 1), (200, 5, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 1, "每 (bidder,auction) 一条");
    assert_eq!(
        field_str(&alerts[0], "detail"),
        "5 200",
        "last bid 的 price"
    );
    assert_eq!(field_str(&alerts[0], "id"), "1", "entity = auction");
}

#[tokio::test]
async fn q19_stats_task_top_n_emits_per_entry() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // Q19 形状: group by (auction), top(2, price) —— 每 auction 2 条 alert（rank 序）,
    // detail 读每条目的 bidder + price。
    let detail = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("{} {}".into()),
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
        ],
    };
    let (mut task, mut alert_rx) = make_ranked_task(
        vec![Expr::Field(FieldRef::Qualified(
            "b".into(),
            "auction".into(),
        ))],
        vec![StatsMeasurePlan {
            label: "top_price".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Top,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: Some(2),
        }],
        detail,
    );
    // auction=1 三条 bid: 100, 300, 200 → top-2 = 300(bidder 2), 200(bidder 3)
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1), (200, 3, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 2, "top-2 → 每 auction 2 条 alert");
    assert_eq!(
        field_str(&alerts[0], "detail"),
        "2 300",
        "rank1: bidder 2 price 300"
    );
    assert_eq!(
        field_str(&alerts[1], "detail"),
        "3 200",
        "rank2: bidder 3 price 200"
    );
    assert!(alert_rx.try_recv().is_err(), "只有 2 条");
}

#[tokio::test]
async fn stats_top_zero_emits_nothing() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // top(0, ...) 边界（P4 review 修复）: 无条目 → 整桶不产出（此前虚假产出
    // scalar(0.0) 记录）。
    let detail = Expr::StringLit("x".into());
    let (mut task, mut alert_rx) = make_ranked_task(
        vec![Expr::Field(FieldRef::Qualified(
            "b".into(),
            "auction".into(),
        ))],
        vec![StatsMeasurePlan {
            label: "top_price".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Top,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: Some(0),
        }],
        detail,
    );
    push_batch(
        &mut task,
        make_bid_batch(&[(100, 1, 1), (300, 2, 1)], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "top(0) 无条目 → 整桶不产出, 无 alert"
    );
}

/// 复现 perf-diag 墙梯（q19 30m rules 档）冻结的**统计层**形态：真实 `StatsTask`
/// （pull 模式）+ window actor + 小 mailbox 预算 + 小全局内存 cap + 周期 evictor。
///
/// 墙梯把同一批数据重发多次（每档一次），归并任务吃进第一批时若 append 持续
/// 涌入且窗口超全局 cap，actor 会在 `commit_append` 的 `gate.freed` park——测试
/// 验证**自愈**：任务追平后系统必须恢复（2026-08-25 线上：q19 30m 墙梯 rules 档
/// CPU 0% 永久冻结 2min+，window 层已证自愈，本测试锁定 stats 层）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stats_pull_actor_evictor_over_budget_recovers() {
    use tokio_util::sync::CancellationToken;
    use wf_engine::window::{
        EvictionGate, Evictor, WINDOW_CHANNEL_DEPTH, WindowDef, WindowMsg, WindowRegistry,
        acquire_window_budget, run_window_actor,
    };

    const ROWS_PER_BATCH: u64 = 100_000;
    const N_BATCHES: u64 = 12;
    const BATCH_BYTES: usize = 4 * 1024 * 1024; // 4MB/批
    const MAILBOX_BUDGET: usize = BATCH_BYTES * 2; // 在途 2 批
    const GLOBAL_CAP: usize = BATCH_BYTES; // 全局 cap = 1 批（每批都要先撞 cap）

    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let reg = Arc::new(
        WindowRegistry::build(vec![WindowDef {
            params: WindowParams {
                name: "bid_events".into(),
                schema: schema.clone(),
                time_col_index: Some(3),
                over: Duration::from_secs(3600),
                materialize_fields: None,
                defer_materialization: false,
            },
            streams: vec![],
            config: crate::engine_task::tests::test_window_config(usize::MAX),
        }])
        .unwrap(),
    );
    let win = reg.get_window("bid_events").unwrap();
    let notify = reg.get_notifier("bid_events").unwrap();
    let slot = reg
        .progress("bid_events")
        .unwrap()
        .register_row_partitioned();

    // actor + mailbox（小预算）
    let (tx, rx) = mpsc::channel::<WindowMsg>(WINDOW_CHANNEL_DEPTH);
    let budget = Arc::new(tokio::sync::Semaphore::new(MAILBOX_BUDGET));
    let gate = Arc::new(EvictionGate::new(GLOBAL_CAP));
    let name: Arc<str> = Arc::from("bid_events");
    let fanout = wf_engine::window::RuleFanout::new();
    let notify_a = Arc::clone(&notify);
    let win_a = Arc::clone(&win);
    let gate_a = Arc::clone(&gate);
    let cancel = CancellationToken::new();
    tokio::spawn(async move {
        run_window_actor(name, win_a, gate_a, fanout, notify_a, rx, cancel, None).await;
    });

    // 周期 evictor
    let gate_e = Arc::clone(&gate);
    let reg_e = Arc::clone(&reg);
    let evictor_task = tokio::spawn(async move {
        let evictor = Evictor::new(Arc::clone(&gate_e));
        loop {
            evictor.run_once(&reg_e, 0);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    // q19 形状 stats 任务（pull 模式）: group by auction, top(2, price)
    let detail = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("{} {}".into()),
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
        ],
    };
    let keys = vec![Expr::Field(FieldRef::Qualified(
        "b".into(),
        "auction".into(),
    ))];
    let measures = vec![StatsMeasurePlan {
        label: "top_price".into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Top,
        field: Some(FieldRef::Qualified("b".into(), "price".into())),
        arg: Some(2),
    }];
    // 真实接线：registry 窗口 + actor 通知 + 分片 progress（make_ranked_task 用裸 Window）
    let notify_task = Arc::clone(&notify);
    let config = ranked_task_config(
        keys,
        measures,
        detail,
        win.clone(),
        notify_task,
        HashMap::from([("bid_events".to_string(), slot.clone())]),
    );
    let task = {
        let (t, _c) = StatsTask::new(config);
        t
    };

    // 消费者拉循环（镜像 run_stats_pull_loop：注册通知 → pull → 处理 → ack）——
    // **必须先于生产者 spawn**（生产者会在 budget 上阻塞, 消费者要早就在跑）
    let notify_c = Arc::clone(&notify);
    let slot_pull = slot.clone();
    let mut pull = tokio::spawn(async move {
        let mut task = task;
        loop {
            let notified = notify_c.notified();
            tokio::pin!(notified);
            task.pull_and_process().await;
            if slot_pull.load(std::sync::atomic::Ordering::Acquire) >= N_BATCHES {
                break;
            }
            tokio::select! {
                _ = &mut notified => {}
                // 兜底轮询（镜像 timeout_tick 的周期性唤醒）
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
    });

    // 生产者：墙梯式重发同一批数据（相同事件时间）——镜像 dispatch_parsed
    let ts = 10_000_000_000i64;
    for seq in 0..N_BATCHES {
        let acquired = tokio::time::timeout(
            Duration::from_secs(10),
            acquire_window_budget(&budget, MAILBOX_BUDGET, BATCH_BYTES),
        )
        .await;
        let permits = match acquired {
            Ok(p) => p,
            Err(_) => {
                panic!(
                    "producer blocked on mailbox budget at seq={seq}: acked={} gate_bytes={} cap={GLOBAL_CAP} win_batches={} win_rows={} win_bytes={}",
                    slot.load(std::sync::atomic::Ordering::Acquire),
                    gate.current_bytes
                        .load(std::sync::atomic::Ordering::Relaxed),
                    win.batch_count(),
                    win.total_rows(),
                    win.memory_usage(),
                );
            }
        };
        tx.send(WindowMsg::Append {
            source: Arc::from("ingress"),
            seq,
            batch: big_bid_batch(&schema, ROWS_PER_BATCH, ts),
            events: None,
            byte_size: BATCH_BYTES,
            permits,
            shard_rows: None,
        })
        .await
        .unwrap();
    }

    let result = tokio::time::timeout(Duration::from_secs(15), &mut pull).await;
    evictor_task.abort();
    let acked = slot.load(std::sync::atomic::Ordering::Acquire);
    if result.is_err() {
        if pull.is_finished() {
            // 消费者任务提前结束（panic?）——把 JoinError 亮出来
            let join = pull.await;
            panic!(
                "pull task ended unexpectedly: {join:?} (acked={acked} gate_bytes={} cap={GLOBAL_CAP} win_batches={} win_rows={})",
                gate.current_bytes
                    .load(std::sync::atomic::Ordering::Relaxed),
                win.batch_count(),
                win.total_rows(),
            );
        }
        panic!(
            "死锁：stats 任务未在超时内追平全部批（acked={acked}/{} gate_bytes={} cap={GLOBAL_CAP} win_batches={} win_rows={})",
            N_BATCHES,
            gate.current_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            win.batch_count(),
            win.total_rows(),
        );
    }
    // 外层 expect 解超时（L1909 已处理, 此处不可达）; 内层 expect 解
    // JoinError——pull 任务 panic 时暴露（不留静默吞错）。
    result
        .expect("pull task timed out")
        .expect("pull task panicked");
    assert_eq!(acked, N_BATCHES, "全部批次被消费并 ack");
}

/// 构造 100k 行 bid 批（auction 0..1000 循环, price 递增——top-2 有真实竞争）。
fn big_bid_batch(schema: &SchemaRef, rows: u64, ts: i64) -> RecordBatch {
    use arrow::array::{Int64Array, TimestampNanosecondArray};
    let n = rows as usize;
    let price: Vec<i64> = (0..n as i64).collect();
    let bidder: Vec<i64> = (0..n as i64).map(|i| i % 100).collect();
    let auction: Vec<i64> = (0..n as i64).map(|i| i % 1000).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(price)),
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(auction)),
            Arc::new(TimestampNanosecondArray::from(vec![ts; n])),
        ],
    )
    .unwrap()
}

/// 列式 close 分块 flush 与一次性全量输出逐字节一致（2026-08-26 q18 100M 修复
/// 的保护测试）。q19 形状（top(2) + fmt detail, 门控通过走 columnar close）:
/// 6 auction × 2 bid → 每桶 top-2 → 12 条输出。chunk=2 强制 6 次分块 flush;
/// 对照 chunk=1_000_000（不分块）跑同数据——分块不得丢行/重复/乱序。
#[tokio::test]
async fn stats_task_columnar_close_chunked_matches_full() {
    let detail = Expr::FuncCall {
        qualifier: None,
        name: "fmt".into(),
        args: vec![
            Expr::StringLit("{} {}".into()),
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "price".into())),
        ],
    };
    let keys = vec![Expr::Field(FieldRef::Qualified(
        "b".into(),
        "auction".into(),
    ))];
    let measures = vec![StatsMeasurePlan {
        label: "top_price".into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Top,
        field: Some(FieldRef::Qualified("b".into(), "price".into())),
        arg: Some(2),
    }];

    let run = |chunk: usize| {
        let detail = detail.clone();
        let keys = keys.clone();
        let measures = measures.clone();
        async move {
            crate::engine_task::stats_task::set_emit_chunk_for_test(chunk);
            let (mut task, mut alert_rx) = make_ranked_task(keys, measures, detail);
            let rows: Vec<(i64, i64, i64)> = (0..6)
                .flat_map(|a| vec![(a * 100 + 10, 1, a), (a * 100 + 20, 2, a)])
                .collect();
            push_batch(&mut task, make_bid_batch(&rows, 5_000_000_000), 1).await;
            task.flush().await;
            // 分块 flush 每块独立投递一个 AlertBatch——drain 全部（take_alerts
            // 只读首个 batch, 分块下会漏后面的块）。
            let mut out = Vec::new();
            while let Ok(batch) = alert_rx.try_recv() {
                match batch {
                    crate::alert_task::AlertBatch::Rows(rows) => out.extend(rows.as_ref().clone()),
                    crate::alert_task::AlertBatch::Columns(cols) => {
                        out.extend(
                            cols.iter_data_records()
                                .collect::<Result<Vec<_>, _>>()
                                .expect("columnar row view conversion")
                                .into_iter()
                                .map(std::sync::Arc::new),
                        );
                    }
                }
            }
            out
        }
    };

    let chunked = run(2).await; // 12 条 > chunk 2 → 6 次分块 flush
    crate::engine_task::stats_task::set_emit_chunk_for_test(1_000_000);
    let full = run(1_000_000).await; // 一次性（原实现路径）
    crate::engine_task::stats_task::set_emit_chunk_for_test(1_000_000); // 恢复全局

    assert_eq!(
        chunked.len(),
        full.len(),
        "分块与全量输出条数一致（不丢不重）"
    );
    // 无序比较: 流式分批打破批间全局排序（对拍只比每规则 EMIT 计数）, 逐条
    // zip 会因批间顺序差异错位——按 (entity_id, detail) 排序后逐条比对。
    let mut keyed_c: Vec<(String, String)> = chunked
        .iter()
        .map(|r| (field_str(r, "__wfu_entity_id"), field_str(r, "detail")))
        .collect();
    let mut keyed_f: Vec<(String, String)> = full
        .iter()
        .map(|r| (field_str(r, "__wfu_entity_id"), field_str(r, "detail")))
        .collect();
    keyed_c.sort();
    keyed_f.sort();
    assert_eq!(keyed_c, keyed_f, "分块与全量输出内容一致（无序比较）");
}
