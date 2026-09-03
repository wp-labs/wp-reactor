//! Q15 统计任务真实路径接线（P1 步骤④c 补）: 12 度量（count/distinct_count 分档
//! where）真实 StatsTask 路径与 CEP 锚点对拍; 输入分区分片归并（StatsPartial）——
//! 协调片收齐 partial 统一 emit、多窗口 advance 归并、空片 sentinel、发送片退出不 panic。

use super::*;
// ---------------------------------------------------------------------------
// Q15 12 度量验证（stats 执行器真实任务路径, 与 CEP 锚点对拍）
// ---------------------------------------------------------------------------

fn q15_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

fn make_q15_batch(rows: &[(i64, i64, i64)], ts: i64) -> RecordBatch {
    use arrow::array::Int64Array;
    let n = rows.len();
    RecordBatch::try_new(
        q15_schema(),
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

/// Q15 12 度量 plan（价格分档 where + count/distinct_count）。
fn q15_plan() -> StatsPlan {
    let m = |label: &str, agg: StatsAggPlan, field: Option<&str>, where_expr: Option<Expr>| {
        StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr,
            agg,
            field: field.map(|f| FieldRef::Qualified("b".into(), f.into())),
            arg: None,
        }
    };
    let price = |op: wf_lang::ast::BinOp, v: f64| Expr::BinOp {
        op,
        left: Box::new(Expr::Field(FieldRef::Qualified("b".into(), "price".into()))),
        right: Box::new(Expr::Number(v)),
    };
    let lt = |v: f64| price(wf_lang::ast::BinOp::Lt, v);
    let ge = |v: f64| price(wf_lang::ast::BinOp::Ge, v);
    let range = |lo: f64, hi: f64| Expr::BinOp {
        op: wf_lang::ast::BinOp::And,
        left: Box::new(ge(lo)),
        right: Box::new(lt(hi)),
    };
    let tier_where = |tier: usize| -> Option<Expr> {
        match tier {
            0 => None,
            1 => Some(lt(10_000.0)),
            2 => Some(range(10_000.0, 1_000_000.0)),
            3 => Some(ge(1_000_000.0)),
            _ => unreachable!(),
        }
    };
    let mut measures = Vec::new();
    for (i, name) in ["total", "r1", "r2", "r3"].iter().enumerate() {
        measures.push(m(
            &format!("count_{name}"),
            StatsAggPlan::Count,
            None,
            tier_where(i),
        ));
    }
    for (i, name) in ["total", "r1", "r2", "r3"].iter().enumerate() {
        measures.push(m(
            &format!("bidder_{name}"),
            StatsAggPlan::DistinctCount,
            Some("bidder"),
            tier_where(i),
        ));
    }
    for (i, name) in ["total", "r1", "r2", "r3"].iter().enumerate() {
        measures.push(m(
            &format!("auction_{name}"),
            StatsAggPlan::DistinctCount,
            Some("auction"),
            tier_where(i),
        ));
    }
    StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(1800)),
        keys: vec![],
        output_shape: StatsOutputShapePlan::Rows,
        measures,
        tracked_bind_fields: HashMap::new(),
    }
}

fn make_q15_task() -> (StatsTask, mpsc::Receiver<crate::alert_task::AlertBatch>) {
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let schema = q15_schema();
    let win = Arc::new(Window::new(
        WindowParams {
            name: "bid_events".into(),
            schema: schema.clone(),
            time_col_index: Some(3), // event_time 第 4 列
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        crate::engine_task::tests::test_window_config(usize::MAX),
    ));
    let rp = q15_rule_plan();
    let config = StatsTaskConfig {
        stats: StatsExecutor::new(q15_plan()),
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
        shard_index: None,
        shard_count: 1,
        merge_rx: None,
        merge_tx: None,
        mask_cache: None,
    };
    let (task, _cancel) = StatsTask::new(config);
    (task, alert_rx)
}

/// q15 stats 规则计划（12 度量 + fmt detail; 与 make_q15_task 共享, 分片测试复用）。
fn q15_rule_plan() -> wf_lang::plan::RulePlan {
    // 12 值 detail（与 CEP 版 q15 yield 同构）
    let mut fmt_args = vec![Expr::StringLit(
        "{} {} {} {} {} {} {} {} {} {} {} {}".into(),
    )];
    for label in [
        "count_total",
        "count_r1",
        "count_r2",
        "count_r3",
        "bidder_total",
        "bidder_r1",
        "bidder_r2",
        "bidder_r3",
        "auction_total",
        "auction_r1",
        "auction_r2",
        "auction_r3",
    ] {
        fmt_args.push(stat_value(label));
    }
    wf_lang::plan::RulePlan {
        name: "q15_stats".into(),
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
            window_spec: WindowSpec::Fixed(Duration::from_secs(1800)),
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
        stats_plan: Some(q15_plan()),
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
                    args: fmt_args,
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
    }
}

#[tokio::test]
async fn q15_stats_task_12_measures_matches_cep_anchor() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    // Q15 真实任务路径（StatsTask 列式归并 → 固定窗口 close → alert）:
    // 6 行覆盖 3 价格档, 期望 12 值（与 CEP 版 q15 锚点同构, 独立手算）。
    let (mut task, mut alert_rx) = make_q15_task();
    let rows = [
        (100, 1, 10),       // tier0
        (50_000, 1, 11),    // tier1
        (2_000_000, 2, 12), // tier2
        (50, 2, 10),        // tier0
        (5_000, 3, 13),     // tier0
        (999_999, 3, 11),   // tier1
    ];
    let batch = make_q15_batch(&rows, 5_000_000_000);
    push_batch(&mut task, batch, 1).await;
    // 未到 30m 边界 → 无产出
    assert!(alert_rx.try_recv().is_err(), "窗口未关闭不应产出");
    task.flush().await;
    let alert = take_alert(&mut alert_rx);
    // total=6, r1=3, r2=2, r3=1; bidder total=3, r1=3, r2=2, r3=1;
    // auction total=4, r1=2, r2=1, r3=1
    assert_eq!(
        field_str(&alert, "detail"),
        "6 3 2 1 3 3 2 1 4 2 1 1",
        "Q15 stats 12 度量（真实任务路径）"
    );
}

/// 输入分区分片（2026-08-24 q15）: 2 片（协调片 shard 0 + 发送片 shard 1）按
/// 行号奇偶分区各归并一半行; flush 时发送片发 raw partial、协调片收齐归并后
/// 统一 emit——输出与单实例锚点**字节一致**, 且非协调片不产出。
#[tokio::test]
async fn q15_input_shard_merge_emits_single_equivalent() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let (alert_tx0, mut alert_rx0) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (alert_tx1, mut alert_rx1) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (merge_tx, merge_rx) = mpsc::channel::<StatsPartial>(8);

    let mk = |shard_idx: usize,
              alert_tx: mpsc::Sender<crate::alert_task::AlertBatch>,
              merge_rx: Option<mpsc::Receiver<StatsPartial>>,
              merge_tx: Option<mpsc::Sender<StatsPartial>>|
     -> StatsTask {
        let config = StatsTaskConfig {
            stats: StatsExecutor::new(q15_plan()),
            executor: RuleExecutor::new(q15_rule_plan()),
            window_sources: vec![],
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
            shard_index: Some(shard_idx),
            shard_count: 2,
            merge_rx,
            merge_tx,
            mask_cache: None,
        };
        let (task, _cancel) = StatsTask::new(config);
        task
    };
    let mut coord = mk(0, alert_tx0, Some(merge_rx), None);
    let mut shard1 = mk(1, alert_tx1, None, Some(merge_tx));

    let rows = [
        (100, 1, 10),       // tier0
        (50_000, 1, 11),    // tier1
        (2_000_000, 2, 12), // tier2
        (50, 2, 10),        // tier0
        (5_000, 3, 13),     // tier0
        (999_999, 3, 11),   // tier1
    ];
    let batch = make_q15_batch(&rows, 5_000_000_000);
    // 输入分区（行号奇偶）: 片 0 = 行 0/2/4, 片 1 = 行 1/3/5。
    let push = |shard_rows: Arc<Vec<u32>>| RulePush {
        window_name: "bid_events".into(),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: Some(shard_rows),
        seq: 1,
    };
    coord.process_push(push(Arc::new(vec![0u32, 2, 4]))).await;
    shard1.process_push(push(Arc::new(vec![1u32, 3, 5]))).await;
    assert!(alert_rx0.try_recv().is_err(), "窗口未关闭不应产出");
    assert!(alert_rx1.try_recv().is_err(), "非协调片不应产出");

    // 并发 flush: 发送片发 raw partial, 协调片收齐归并后统一 emit。
    tokio::join!(coord.flush(), shard1.flush());

    let alert = take_alert(&mut alert_rx0);
    assert_eq!(
        field_str(&alert, "detail"),
        "6 3 2 1 3 3 2 1 4 2 1 1",
        "2 片输入分区归并输出必须与单实例锚点一致"
    );
    assert!(alert_rx1.try_recv().is_err(), "非协调片不得 emit");
}

/// 构造一个输入分片 stats 任务（make_stats_plan 10s 窗口形状）:
/// `shard_idx` + 归并通道角色 + alert 发送端。window_sources 留空（push 路径
/// 不经窗口 log; process_push 直接用 push 的 window_name）。
fn make_stats_shard_task(
    shard_idx: usize,
    alert_tx: mpsc::Sender<crate::alert_task::AlertBatch>,
    merge_rx: Option<mpsc::Receiver<StatsPartial>>,
    merge_tx: Option<mpsc::Sender<StatsPartial>>,
) -> StatsTask {
    let config = StatsTaskConfig {
        stats: StatsExecutor::new(make_stats_plan()),
        executor: RuleExecutor::new(make_stats_rule_plan()),
        window_sources: vec![],
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
        shard_index: Some(shard_idx),
        shard_count: 2,
        merge_rx,
        merge_tx,
        mask_cache: None,
    };
    let (task, _cancel) = StatsTask::new(config);
    task
}

/// 多窗口 advance 归并（2026-08-24）: 批次 2 越过 10s 窗口边界 → 协调片
/// mid-stream close 窗口 1（收齐发送片 partial 归并后 emit）; flush 收窗口 2。
/// 两窗口输出与单实例（全批）逐字节一致。
#[tokio::test]
async fn q15_input_shard_merge_multi_window_matches_single() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let (alert_tx0, mut alert_rx0) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (alert_tx1, mut alert_rx1) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (merge_tx, merge_rx) = mpsc::channel::<StatsPartial>(8);
    let mut coord = make_stats_shard_task(0, alert_tx0, Some(merge_rx), None);
    let mut shard1 = make_stats_shard_task(1, alert_tx1, None, Some(merge_tx));

    let push = |batch: RecordBatch, shard_rows: Arc<Vec<u32>>| RulePush {
        window_name: "bid_events".into(),
        events: None,
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: Some(shard_rows),
        seq: 1,
    };
    // 窗口 1 [0,10s): 4 行（sip a/b/c/d, ts=5s）。行号分区: 片 0 = 行 0/2,
    // 片 1 = 行 1/3。
    let b1 = make_ts_batch(&[
        ("a", 5_000_000_000),
        ("b", 5_000_000_000),
        ("c", 5_000_000_000),
        ("d", 5_000_000_000),
    ]);
    tokio::join!(
        coord.process_push(push(b1.clone(), Arc::new(vec![0u32, 2]))),
        shard1.process_push(push(b1, Arc::new(vec![1u32, 3])))
    );

    // 窗口 2 [10s,20s): 越过边界 → close 窗口 1（协调片 close 时收齐发送片
    // partial——必须并发执行, 顺序 await 会死锁: 协调片 close 阻塞在 recv,
    // 发送片 push 还没执行）。
    let b2 = make_ts_batch(&[
        ("a", 15_000_000_000),
        ("b", 15_000_000_000),
        ("e", 15_000_000_000),
        ("f", 15_000_000_000),
    ]);
    tokio::join!(
        coord.process_push(push(b2.clone(), Arc::new(vec![0u32, 2]))),
        shard1.process_push(push(b2, Arc::new(vec![1u32, 3])))
    );

    // 协调片 mid-stream close 窗口 1: total=4, r1=4（全是 10.0.0.1? 不——sip
    // 是 a/b/c/d, where sip=="10.0.0.1" 全 false）→ r1=0; uniq=4。
    let alert = take_alert(&mut alert_rx0);
    assert_eq!(
        field_str(&alert, "detail"),
        "4 0 4",
        "窗口 1 归并（total/r1/uniq）"
    );
    assert!(alert_rx1.try_recv().is_err(), "非协调片不得 emit");

    // flush 收窗口 2: total=4, r1=0, uniq=4（a/b/e/f）。
    tokio::join!(coord.flush(), shard1.flush());
    let alert = take_alert(&mut alert_rx0);
    assert_eq!(field_str(&alert, "detail"), "4 0 4", "窗口 2 归并");
}

/// 非协调片 0 事件（无窗口）时 flush 发 sentinel——协调片不 panic 不死锁,
/// 输出 = 协调片自己的数据（空 partial 合并无效果）。
#[tokio::test]
async fn q15_input_shard_empty_shard_flush_sentinel_no_deadlock() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let (alert_tx0, mut alert_rx0) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (alert_tx1, _alert_rx1) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (merge_tx, merge_rx) = mpsc::channel::<StatsPartial>(8);
    let mut coord = make_stats_shard_task(0, alert_tx0, Some(merge_rx), None);
    let mut shard1 = make_stats_shard_task(1, alert_tx1, None, Some(merge_tx));

    // 只有协调片有数据; 发送片 0 事件（不 push）。
    let b1 = make_ts_batch(&[("a", 5_000_000_000), ("b", 5_000_000_000)]);
    let push = |batch: RecordBatch| RulePush {
        window_name: "bid_events".into(),
        events: None,
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: Some(Arc::new(vec![0u32, 1])),
        seq: 1,
    };
    coord.process_push(push(b1)).await;

    // 并发 flush: 发送片无窗口 → 发 (MIN, MIN) 空 sentinel; 协调片收齐后
    // merge（空）→ 输出 = 自己的 2 行。
    tokio::join!(coord.flush(), shard1.flush());
    let alert = take_alert(&mut alert_rx0);
    assert_eq!(
        field_str(&alert, "detail"),
        "2 0 2",
        "空片 sentinel 合并无效果"
    );
}

/// 协调片 recv None（某片已退出 / tx drop）不 panic——warn 后放弃该窗口
/// 剩余合并, 输出 = 已收到的 partial + 自己（2026-08-24 review 修复）。
#[tokio::test]
async fn q15_input_shard_partial_sender_exit_no_panic() {
    let _g = crate::perf_diag::PERF_CUT_SERIAL
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let (alert_tx0, mut alert_rx0) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (merge_tx, merge_rx) = mpsc::channel::<StatsPartial>(8);
    let mut coord = make_stats_shard_task(0, alert_tx0, Some(merge_rx), None);

    // 发送片先退出（tx drop）——协调片 flush 时 recv None。
    drop(merge_tx);

    let b1 = make_ts_batch(&[("a", 5_000_000_000), ("b", 5_000_000_000)]);
    let push = RulePush {
        window_name: "bid_events".into(),
        events: None,
        batch: Some(Arc::new(b1)),
        materialize_fields: None,
        shard_rows: Some(Arc::new(vec![0u32, 1])),
        seq: 1,
    };
    coord.process_push(push).await;

    coord.flush().await; // 不得 panic
    let alert = take_alert(&mut alert_rx0);
    assert_eq!(
        field_str(&alert, "detail"),
        "2 0 2",
        "片退出后仅协调片自己的数据"
    );
}
