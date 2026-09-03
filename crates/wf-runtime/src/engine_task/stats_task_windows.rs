//! 基础 stats 任务（空键 3 度量 + Q12 复合键分组）push 路径窗口语义: 单窗口/
//! flush/跨窗口跳变/批内跨窗切段、ack floor、超时扫描关尾窗、大时间戳 epoch 对齐
//! 桶归属、非列式 where 回退行式、单段快路径与跨窗回退。

use super::*;
fn make_window(name: &str, schema: &SchemaRef) -> Arc<Window> {
    Arc::new(Window::new(
        WindowParams {
            name: name.into(),
            schema: schema.clone(),
            time_col_index: Some(1), // event_time 是第二列
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        crate::engine_task::tests::test_window_config(usize::MAX),
    ))
}

fn make_batch(sips: &[&str], ts: i64) -> RecordBatch {
    let n = sips.len();
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(StringArray::from(
                sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![ts; n])),
        ],
    )
    .unwrap()
}

/// 构建 StatsTask（push 路径, seq 从 1 开始）+ alert 接收 + progress slot。
fn make_stats_task() -> (
    StatsTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<AtomicU64>,
) {
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let win = make_window("bid_events", &test_schema());
    let progress = Arc::new(AtomicU64::new(0));
    let config = StatsTaskConfig {
        stats: StatsExecutor::new(make_stats_plan()),
        executor: RuleExecutor::new(make_stats_rule_plan()),
        window_sources: vec![crate::engine_task::task_types::WindowSource {
            window_name: "bid_events".into(),
            window: Arc::clone(&win),
            notify: Arc::new(tokio::sync::Notify::new()),
            aliases: vec!["b".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
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
        progress: std::collections::HashMap::from([(
            "bid_events".to_string(),
            Arc::clone(&progress),
        )]),
        shard_index: None,
        shard_count: 1,
        merge_rx: None,
        merge_tx: None,
        mask_cache: None,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx, progress)
}

#[tokio::test]
async fn stats_push_closes_window_on_watermark_and_emits_alert() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let (mut task, mut alert_rx, progress) = make_stats_task();

    // batch1: 3 行, ts=5s（窗口 [0,10s) 内）→ 不 close, 无 alert
    let b1 = make_batch(&["10.0.0.1", "10.0.0.1", "10.0.0.2"], 5_000_000_000);
    push_batch(&mut task, b1, 1).await;
    assert!(alert_rx.try_recv().is_err(), "窗口未关闭不应产出");
    // ack: seq=1 → slot=2
    assert_eq!(progress.load(std::sync::atomic::Ordering::Relaxed), 2);

    // batch2: 3 行, ts=15s（>= 窗口边界 10s）→ close 窗口 1, 开窗口 2
    let b2 = make_batch(&["10.0.0.1", "10.0.0.3", "10.0.0.3"], 15_000_000_000);
    push_batch(&mut task, b2, 2).await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "stats_rule");
    assert_eq!(
        field_str(&alert, "detail"),
        "3 2 2",
        "total=3, r1=2, uniq=2"
    );
    // ack: seq=2 → slot=3
    assert_eq!(progress.load(std::sync::atomic::Ordering::Relaxed), 3);

    // flush: 关闭残留窗口 2（batch2 数据: total=3, r1=1, uniq=2）
    task.flush().await;
    let alert2 = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert2, "detail"), "3 1 2", "窗口 2 数据");
}

#[tokio::test]
async fn stats_push_single_window_flush_emits() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    // 单批 10s 内 → 不 close; flush 收尾
    let b1 = make_batch(&["10.0.0.1", "10.0.0.1"], 5_000_000_000);
    push_batch(&mut task, b1, 1).await;
    assert!(alert_rx.try_recv().is_err());
    task.flush().await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "detail"), "2 2 1");
}

#[tokio::test]
async fn stats_push_multiple_window_jump_emits_only_populated() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    // batch1: 5s（窗口 1）
    push_batch(&mut task, make_batch(&["10.0.0.1"], 5_000_000_000), 1).await;
    // batch2: 直接跳 35s（窗口 4）→ 窗口 1 close（窗口 2/3 空无产出）
    push_batch(
        &mut task,
        make_batch(&["10.0.0.2", "10.0.0.2"], 35_000_000_000),
        2,
    )
    .await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&alert, "detail"),
        "1 1 1",
        "窗口 1: total=1, r1=1, uniq=1"
    );
    // flush 关窗口 4
    task.flush().await;
    let alert2 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&alert2, "detail"),
        "2 0 1",
        "窗口 4: total=2, r1=0, uniq=1"
    );
}

#[tokio::test]
async fn stats_push_columnar_fallback_row_path() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // 非列式 where（含函数调用）→ process_batch 返回 false → 回退行式, 语义等价
    // 用第 4 个度量（where 含 len(sip) > 4——不可列式化）验证不崩且 close 正确
    let plan = {
        let mut p = make_stats_plan();
        p.measures.push(StatsMeasurePlan {
            label: "long_sip".into(),
            source_alias: "b".into(),
            where_expr: Some(Expr::BinOp {
                op: wf_lang::ast::BinOp::Gt,
                left: Box::new(Expr::FuncCall {
                    qualifier: None,
                    name: "len".into(),
                    args: vec![Expr::Field(FieldRef::Qualified("b".into(), "sip".into()))],
                }),
                right: Box::new(Expr::Number(4.0)),
            }),
            agg: StatsAggPlan::Count,
            field: None,
            arg: None,
        });
        p
    };
    let (mut task, mut alert_rx, _progress) = make_stats_task_with_plan(plan);
    push_batch(
        &mut task,
        make_batch(&["10.0.0.1", "10.0.0.2"], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    let alert = take_alert(&mut alert_rx);
    // len("10.0.0.1")=8 >4 → 计数; 两个都满足
    assert_eq!(
        field_str(&alert, "detail"),
        "2 1 2 2",
        "含不可列式 where 的回退路径"
    );
}

fn make_stats_task_with_plan(
    plan: StatsPlan,
) -> (
    StatsTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<AtomicU64>,
) {
    let mut rp = make_stats_rule_plan();
    rp.stats_plan = Some(plan.clone());
    rp.yield_plan.fields = vec![YieldField {
        name: "detail".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("{} {} {} {}".into()),
                stat_value("total"),
                stat_value("r1"),
                stat_value("uniq"),
                stat_value("long_sip"),
            ],
        },
    }];
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let win = make_window("bid_events", &test_schema());
    let progress = Arc::new(AtomicU64::new(0));
    let config = StatsTaskConfig {
        stats: StatsExecutor::new(plan),
        executor: RuleExecutor::new(rp),
        window_sources: vec![crate::engine_task::task_types::WindowSource {
            window_name: "bid_events".into(),
            window: Arc::clone(&win),
            notify: Arc::new(tokio::sync::Notify::new()),
            aliases: vec!["b".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
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
        progress: std::collections::HashMap::from([(
            "bid_events".to_string(),
            Arc::clone(&progress),
        )]),
        shard_index: None,
        shard_count: 1,
        merge_rx: None,
        merge_tx: None,
        mask_cache: None,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx, progress)
}

#[tokio::test]
async fn stats_scan_timeouts_closes_tail_window() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // 墙钟兜底（对齐 CEP scan_timeouts）: 数据跨度未达窗口边界时, 周期性扫描用
    // wall elapsed 推进 watermark 关闭尾部窗口; 关闭后清空状态, 空窗口不得循环产出。
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    // ts=9.999s: watermark 接近但未达 10s 边界 → 事件推进不 close
    push_batch(&mut task, make_batch(&["10.0.0.1"], 9_999_000_000), 1).await;
    assert!(alert_rx.try_recv().is_err(), "事件推进未到边界不应产出");

    // 等待 20ms 后 scan → effective watermark 越过边界 → 关闭尾部窗口
    tokio::time::sleep(Duration::from_millis(20)).await;
    task.scan_timeouts().await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "detail"), "1 1 1", "尾部窗口产出");

    // 再次 scan: 窗口已清空 → 无产出（不得每 tick 关闭空窗口循环 emit）
    tokio::time::sleep(Duration::from_millis(20)).await;
    task.scan_timeouts().await;
    assert!(alert_rx.try_recv().is_err(), "空窗口不得循环产出");
}

// ---------------------------------------------------------------------------
// P2 复合键分组: StatsTask 每桶一条 alert + 键字段注入 yield
// ---------------------------------------------------------------------------

/// Q12 形状任务: group by (b.bidder) { count } —— 每桶一条 alert, detail 含 bidder。
fn make_q12_task() -> (StatsTask, mpsc::Receiver<crate::alert_task::AlertBatch>) {
    make_q12_task_sharded(None, 1)
}

/// 分片版 Q12 任务（P2）: `shard_index` 决定本片拉/收的行子集。
fn make_q12_task_sharded(
    shard_index: Option<usize>,
    shard_count: usize,
) -> (StatsTask, mpsc::Receiver<crate::alert_task::AlertBatch>) {
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let schema = test_schema(); // sip + event_time
    let win = Arc::new(Window::new(
        WindowParams {
            name: "bid_events".into(),
            schema: schema.clone(),
            time_col_index: Some(1),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        crate::engine_task::tests::test_window_config(usize::MAX),
    ));
    let plan = StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
        keys: vec![Expr::Field(FieldRef::Qualified("b".into(), "sip".into()))],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![StatsMeasurePlan {
            label: "bid_count".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Count,
            field: None,
            arg: None,
        }],
        tracked_bind_fields: HashMap::new(),
    };
    // detail = fmt("{} {}", b.sip, stat.value(final(bid_count)))
    let rp = wf_lang::plan::RulePlan {
        name: "q12_stats".into(),
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
            entity_id_expr: Expr::Number(1.0),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "detail".into(),
                value: Expr::FuncCall {
                    qualifier: None,
                    name: "fmt".into(),
                    args: vec![
                        Expr::StringLit("{} {}".into()),
                        Expr::Field(FieldRef::Qualified("b".into(), "sip".into())),
                        stat_value("bid_count"),
                    ],
                },
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    };
    let config = StatsTaskConfig {
        stats: StatsExecutor::new(plan),
        executor: RuleExecutor::new(rp),
        window_sources: vec![crate::engine_task::task_types::WindowSource {
            window_name: "bid_events".into(),
            window: Arc::clone(&win),
            notify: Arc::new(tokio::sync::Notify::new()),
            aliases: vec!["b".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
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
        progress: std::collections::HashMap::new(),
        shard_index,
        shard_count,
        merge_rx: None,
        merge_tx: None,
        mask_cache: None,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx)
}

#[tokio::test]
async fn q12_stats_task_per_bucket_alert_with_key_injected() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // group by (b.sip): 每桶一条 alert（同一 close 批量合成一批）, detail 含分组
    // 键值 + 计数; 桶序 = ScopeKey 升序: 10.0.0.1 → 10.0.0.2
    let (mut task, mut alert_rx) = make_q12_task();
    push_batch(
        &mut task,
        make_batch(&["10.0.0.1", "10.0.0.1", "10.0.0.2"], 5_000_000_000),
        1,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 2, "2 桶合成一批");
    assert_eq!(field_str(&alerts[0], "detail"), "10.0.0.1 2", "桶 10.0.0.1");
    assert_eq!(field_str(&alerts[1], "detail"), "10.0.0.2 1", "桶 10.0.0.2");
    // 无更多桶
    assert!(alert_rx.try_recv().is_err(), "只有 2 桶");
}

#[tokio::test]
async fn stats_sharded_task_processes_only_own_rows() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // P2 分片回归（Blocker 1）: 带 key 任务每片只归并自己的 shard_rows 子集——
    // 否则每片处理全批, 每个键被 N 片各算一遍, close 重复输出 N 倍
    // （Q16 实测 EMIT 10 倍）。模拟 2 片: 片 0 拥有行 {0,1}（key A）,
    // 片 1 拥有行 {2,3}（key B）; 各自产出且互不重复。
    let (mut shard0, mut rx0) = make_q12_task_sharded(Some(0), 2);
    let (mut shard1, mut rx1) = make_q12_task_sharded(Some(1), 2);
    let batch = make_batch(&["A", "A", "B", "B"], 5_000_000_000);
    // 分片广播: RulePush.shard_rows 携带本片行子集（fanout 按键分区）
    for (task, rows) in [(&mut shard0, vec![0u32, 1]), (&mut shard1, vec![2u32, 3])] {
        let push = RulePush {
            window_name: "bid_events".into(),
            events: None,
            batch: Some(Arc::new(batch.clone())),
            materialize_fields: None,
            shard_rows: Some(Arc::new(rows)),
            seq: 1,
        };
        task.process_push(push).await;
    }
    shard0.flush().await;
    shard1.flush().await;
    // 片 0 只产出 key A（不得重复全批的 B）; 片 1 只产出 key B
    let a0 = take_alert(&mut rx0);
    assert_eq!(field_str(&a0, "detail"), "A 2", "片 0 只归并自己的行");
    assert!(rx0.try_recv().is_err(), "片 0 不得产出 B（重复 bug）");
    let a1 = take_alert(&mut rx1);
    assert_eq!(field_str(&a1, "detail"), "B 2", "片 1 只归并自己的行");
    assert!(rx1.try_recv().is_err(), "片 1 不得产出 A（重复 bug）");
}

#[tokio::test]
async fn stats_task_segments_keyed_batch_across_window_boundary() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // 回归（批跨窗口边界, Q12 根因）: 同一批的行按事件时间归属各自窗口。
    // 旧整批归并: batch max=11s → 先推进到窗口 2, 4 行全进窗口 2 → 单条 "A 4";
    // 新切段: 窗口 1 [0,10) A=2, 窗口 2 [10,20) A=2 → 两条 "A 2"。
    let (mut task, mut alert_rx) = make_q12_task();
    let batch = make_ts_batch(&[
        ("A", 9_000_000_000),
        ("A", 9_500_000_000),
        ("A", 10_500_000_000),
        ("A", 11_000_000_000),
    ]);
    push_batch(&mut task, batch, 1).await;
    task.flush().await;
    let a1 = take_alert(&mut alert_rx);
    assert_eq!(field_str(&a1, "detail"), "A 2", "窗口 1 [0,10): A=2");
    let a2 = take_alert(&mut alert_rx);
    assert_eq!(field_str(&a2, "detail"), "A 2", "窗口 2 [10,20): A=2");
    assert!(
        alert_rx.try_recv().is_err(),
        "只有 2 窗（不得整批归并成 A 4）"
    );
}

#[tokio::test]
async fn stats_task_segments_empty_key_batch_across_window_boundary() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // 空键同样按窗口切段: 窗口 1 收 9s/9.5s 两行, 窗口 2 收 10.5s 一行。
    // 旧整批归并: 窗口 1 空（无产出）, 3 行全进窗口 2 → "3 1 2" 单条。
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    let batch = make_ts_batch(&[
        ("10.0.0.1", 9_000_000_000),
        ("10.0.0.1", 9_500_000_000),
        ("10.0.0.2", 10_500_000_000),
    ]);
    push_batch(&mut task, batch, 1).await;
    task.flush().await;
    let a1 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a1, "detail"),
        "2 2 1",
        "窗口 1 [0,10): total=2 r1=2 uniq=1"
    );
    let a2 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a2, "detail"),
        "1 0 1",
        "窗口 2 [10,20): total=1 r1=0 uniq=1"
    );
    assert!(alert_rx.try_recv().is_err(), "只有 2 窗");
}

#[tokio::test]
async fn stats_task_empty_key_jump_emits_no_zero_windows() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // 空键单批跨多窗跳变（5s → 35s, 10s 窗）: 窗口 1 收 5s 行, 窗口 4 收 35s 行,
    // 空窗口 2/3 不得产出。旧整批归并: 先按 max=35s 推进 → 窗口 1 空窗 close
    // 产出全 0 alert（空键预建 Empty 桶）, 两行全进窗口 4。
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    let batch = make_ts_batch(&[("10.0.0.1", 5_000_000_000), ("10.0.0.2", 35_000_000_000)]);
    push_batch(&mut task, batch, 1).await;
    task.flush().await;
    let a1 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a1, "detail"),
        "1 1 1",
        "窗口 1 [0,10): total=1 r1=1 uniq=1"
    );
    let a2 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a2, "detail"),
        "1 0 1",
        "窗口 4 [30,40): total=1 r1=0 uniq=1"
    );
    assert!(
        alert_rx.try_recv().is_err(),
        "只有 2 窗——空窗/跳变不得产出全 0 alert"
    );
}

// ---------------------------------------------------------------------------
// 窗口判定边界（大 case 定位回归教训: 30m 窗 + 非零 BASE 的桶归属必须在测试
// 锁定, 不靠 30M 对拍暴露）:
// 1. 桶起点 = (t/dur)*dur（epoch 对齐, 与事件时间起始值无关）;
// 2. 两窗口各自归属（Q18/Q19 形态）;
// 3. 数据未越边界不产出（10m 数据 EMIT=0 是语义而非 bug）;
// 4. 尾部窗口仅 flush/墙钟关闭。
// ---------------------------------------------------------------------------

const NEXMARK_BASE_NS: i64 = 1_767_225_600_000_000_000; // 2026-01-01T00:00:00Z

#[tokio::test]
async fn stats_task_window_bucket_epoch_aligned_large_ts() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // 真实 nexmark BASE_NS 级大时间戳: ts = BASE+25s, 10s 窗 → 窗口
    // [BASE+20s, BASE+30s), fired_at = BASE+30s —— 桶起点 = (t/dur)*dur,
    // 不是「首事件时间 + dur」（BASE 非 0 时两者的差会暴露）。
    let (mut task, mut alert_rx, _p) = make_stats_task();
    push_batch(
        &mut task,
        make_ts_batch(&[("10.0.0.1", NEXMARK_BASE_NS + 25_000_000_000)]),
        1,
    )
    .await;
    task.flush().await;
    let alerts = take_alerts(&mut alert_rx);
    assert_eq!(alerts.len(), 1);
    assert_eq!(
        field_str(&alerts[0], "__wfu_fired_at"),
        "2026-01-01T00:00:30.000Z",
        "窗口 end = bucket(BASE+25s)+10s = BASE+30s"
    );
}

#[tokio::test]
async fn stats_task_two_windows_large_ts_buckets() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // Q18/Q19 形态: 大时间戳下两窗口各自归属。同一批: BASE+25s（窗 1
    // [BASE+20s,+30s)）与 BASE+32s（窗 2 [BASE+30s,+40s)）; 窗 1 在事件推进中
    // close（32s 越过 30s 边界）, 窗 2 由 flush 收尾。
    let (mut task, mut alert_rx, _p) = make_stats_task();
    push_batch(
        &mut task,
        make_ts_batch(&[
            ("10.0.0.1", NEXMARK_BASE_NS + 25_000_000_000),
            ("10.0.0.2", NEXMARK_BASE_NS + 32_000_000_000),
        ]),
        1,
    )
    .await;
    task.flush().await;
    let a1 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a1, "__wfu_fired_at"),
        "2026-01-01T00:00:30.000Z",
        "窗口 1 end"
    );
    assert_eq!(
        field_str(&a1, "detail"),
        "1 1 1",
        "窗口 1: total=1 r1=1 uniq=1"
    );
    let a2 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&a2, "__wfu_fired_at"),
        "2026-01-01T00:00:40.000Z",
        "窗口 2 end"
    );
    assert_eq!(
        field_str(&a2, "detail"),
        "1 0 1",
        "窗口 2: total=1 r1=0 uniq=1"
    );
    assert!(alert_rx.try_recv().is_err(), "只有 2 窗");
}

#[tokio::test]
async fn stats_task_window_not_closed_until_watermark_crosses() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // 数据 max < window_end → 窗口未到点不产出（10m 数据 span < 30m 窗时
    // EMIT=0 的语义: 窗口只在事件时间越过边界 close, 否则等 flush/墙钟兜底）。
    let (mut task, mut alert_rx, _p) = make_stats_task();
    push_batch(&mut task, make_batch(&["10.0.0.1"], 9_000_000_000), 1).await;
    assert!(alert_rx.try_recv().is_err(), "9s 未越 10s 边界不产出");
    // 越过 10s 边界 → 窗口 1 close
    push_batch(&mut task, make_batch(&["10.0.0.2"], 15_000_000_000), 2).await;
    let a1 = take_alert(&mut alert_rx);
    assert_eq!(field_str(&a1, "detail"), "1 1 1", "窗口 1 [0,10)");
    // 尾部窗口 2 由 flush 关闭
    task.flush().await;
    let a2 = take_alert(&mut alert_rx);
    assert_eq!(field_str(&a2, "detail"), "1 0 1", "窗口 2 [10,20)");
    assert!(alert_rx.try_recv().is_err(), "只有 2 窗");
}

/// 单段快路径（2026-08-27 q17 优化 B）: 窗口已建 + 批内 max_time < window_end
/// → 整批单段直接归并（跳过逐行段扫 + domain 构造）。组合共享缓存（优化 A:
/// batch_max_time 分片共享）——结果语义与逐行段扫完全一致。
#[tokio::test]
async fn stats_push_single_segment_fast_path_with_shared_cache() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    // 注入分片共享缓存（A + B 组合: max_time 缓存 + 单段快路径）
    task.mask_cache = Some(std::sync::Arc::new(
        wf_engine::match_engine::StatsMaskCache::new(),
    ));

    // b1: 2 行 ts=5s → 首窗开窗 [0,10s)（window_end=None 分支, 快路径不触发——
    // 开窗逻辑路径）
    let b1 = make_batch(&["10.0.0.1", "10.0.0.1"], 5_000_000_000);
    push_batch(&mut task, b1, 1).await;
    // b2: 3 行 ts ∈ {6s, 6.5s, 7s} → max=7s < window_end 10s → 单段快路径触发
    // （跳过逐行段扫）; max_time 走共享缓存（compute 一次, 第二次命中）
    let b2 = make_ts_batch(&[
        ("10.0.0.1", 6_000_000_000),
        ("10.0.0.2", 6_500_000_000),
        ("10.0.0.2", 7_000_000_000),
    ]);
    push_batch(&mut task, b2, 2).await;
    assert!(alert_rx.try_recv().is_err(), "仍在窗口 [0,10s) 内不 close");

    task.flush().await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&alert, "detail"),
        "5 3 2",
        "total=5（2+3）, r1=3, uniq=2——快路径归并正确"
    );
}

/// 快路径不误伤多段窗口: 批跨窗口边界（max_time >= window_end）时快路径不触发,
/// 走逐行段扫——跨窗数据正确归属（对照 stats_push_closes_window 的跨窗语义）。
#[tokio::test]
async fn stats_push_fast_path_skips_when_batch_crosses_window() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let (mut task, mut alert_rx, _progress) = make_stats_task();
    task.mask_cache = Some(std::sync::Arc::new(
        wf_engine::match_engine::StatsMaskCache::new(),
    ));

    // b1: 2 行 ts=5s → 窗口 [0,10s)
    let b1 = make_batch(&["10.0.0.1", "10.0.0.1"], 5_000_000_000);
    push_batch(&mut task, b1, 1).await;
    // b2: 3 行 ts ∈ {9.9s, 10.1s, 10.2s} → max=10.2s >= 10s → 快路径不触发,
    // 逐行段扫: 9.9s 行归窗口 1, 10.1/10.2s 行触发 close 归窗口 2
    let b2 = make_ts_batch(&[
        ("10.0.0.1", 9_900_000_000),
        ("10.0.0.2", 10_100_000_000),
        ("10.0.0.3", 10_200_000_000),
    ]);
    push_batch(&mut task, b2, 2).await;
    // 窗口 1 close: total=3（2+1）, r1=3, uniq=1
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "detail"), "3 3 1", "窗口 1: 含 9.9s 行");

    task.flush().await;
    let alert2 = take_alert(&mut alert_rx);
    assert_eq!(
        field_str(&alert2, "detail"),
        "2 0 2",
        "窗口 2: 10.1/10.2s 两行（r1=0 非 10.0.0.1, uniq=2）"
    );
}
