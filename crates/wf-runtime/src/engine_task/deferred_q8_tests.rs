//! Q8 形状（person 驱动 + auction 右窗，`within [p.dateTime, bucket_end(10s)]`
//! 上开桶 + `emit at bucket_end(...)`）纯存在 deferred join：watermark 命中输出、
//! join-key 冲突回退扫描、边界 auction 排除、EOS retry（miss 恢复/真 miss 静默/
//! 窗口补全前保留）、flush 幂等不重不漏、未到期尾部不输出。

use super::*;

fn person_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

/// Q8 右窗：auction（仅 join 所需字段 seller/dateTime/event_time）。
fn q8_auction_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("seller", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

fn person_batch(rows: &[(i64, i64)]) -> RecordBatch {
    // (id, dateTime)，event_time = dateTime
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(person_schema(), cols).unwrap()
}

fn q8_auction_batch(rows: &[(i64, i64)]) -> RecordBatch {
    // (seller, dateTime)，event_time = dateTime
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(q8_auction_schema(), cols).unwrap()
}

// ---------------------------------------------------------------------------
// Q8 形状：person 驱动 + auction 右窗，`within [p.dateTime, <bucket_end(10s)]`
// 上开桶 + `emit at bucket_end(p.dateTime, 10s)`（纯存在 deferred）。
// T = 1.7e18 ns 整除 10s → bucket_end(T) = T + 10s。
// ---------------------------------------------------------------------------

fn make_q8_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let driver = "person_events";
    let registry = WindowRegistry::build(vec![
        window_def(driver, &person_schema()),
        window_def("auction_events", &q8_auction_schema()),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let source_window = router.registry().get_window(driver).unwrap();
    let source_notify = router.registry().get_notifier(driver).unwrap();

    let bucket_end = |arg: Expr| Expr::FuncCall {
        qualifier: None,
        name: "bucket_end".to_string(),
        args: vec![arg, Expr::Number(10.0)],
    };
    let rule_plan = RulePlan {
        conv_window: None,
        name: "q8_deferred_e2e".into(),
        binds: vec![BindPlan {
            alias: "p".into(),
            window: driver.into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
            key_map: None,
            key_join: None,
            window_spec: wf_lang::plan::WindowSpec::Fixed(std::time::Duration::ZERO),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::Or,
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
            alias: "p".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![JoinPlan {
            right_window: "auction_events".to_string(),
            mode: JoinMode::Inner,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("p".into(), "id".into()),
                right: FieldRef::Qualified("auction_events".into(), "seller".into()),
            }],
            within: Some(WithinSpec {
                lo: Bound {
                    open: false,
                    val: BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                        "p".into(),
                        "dateTime".into(),
                    ))),
                },
                hi: Bound {
                    open: true, // 上开桶 [B, B+10s)
                    val: BoundVal::Expr(bucket_end(Expr::Field(FieldRef::Qualified(
                        "p".into(),
                        "dateTime".into(),
                    )))),
                },
            }),
            reduce: None,
            emit_at: Some(bucket_end(Expr::Field(FieldRef::Qualified(
                "p".into(),
                "dateTime".into(),
            )))),
        }],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Simple("id".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "id".into(),
                value: Expr::Field(FieldRef::Simple("id".into())),
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(10.0),
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
        each_alias: Some("p".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: driver.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["p".into()],
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
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, router)
}

fn q8_person_window(router: &Router) -> Arc<Window> {
    router.registry().get_window("person_events").unwrap()
}

fn q8_auction_window(router: &Router) -> Arc<Window> {
    router.registry().get_window("auction_events").unwrap()
}

/// 桶内 seller==id 的 auction → watermark 过桶末 → 输出（注册且创建拍卖）。
#[tokio::test]
async fn deferred_q8_hit_outputs_when_watermark_passes_bucket_end() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // person 5 注册（T，10s 桶界上）→ 挂起（expiry = T+10s），watermark = T
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(5, T)]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "未到期 — 不输出");

    // auction seller=5 在桶内（T+5s）入右窗；另一个 auction（seller=99）在
    // T+11s 入右窗 → 目标窗口 max_event_time 推过 T+10s（2026-08-25 评估
    // gate：目标 max_event_time ≥ expiry 才评估，生产流中 auction 持续
    // append 天然追平，单测需显式补）
    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[(5, T + 5_000_000_000)]))
        .unwrap();
    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[(99, T + 11_000_000_000)]))
        .unwrap();

    // 第二个 person（T+11s，下个桶）推进 watermark ≥ T+10s → person 5 到期
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "桶内 seller==5 的 auction → person 5 输出"
    );
}

/// 回归（2026-08-29 q8 多规则 7565→1 根因 + 2026-08-30 多 key 索引）：
/// 旧实现 join 索引**首键独占**——另一规则先用不同 key（id）注册 auction_events
/// 后，q8（join 键 seller）的索引查询会得到「索引命中但空」的假空（不触发扫描
/// 回退）→ 静默全 miss；后改为 key_field 不匹配回退扫描（2026-08-29），再改为
/// **每 key 字段各建索引**（2026-08-30，多 key 支持）。本测试覆盖两条路径：
/// ① 只注册 id → q8 的 seller 无索引走扫描回退仍正确；② 补注册 seller → q8
/// 走 O(1) 索引仍正确（两种路径结果一致）。
#[tokio::test]
async fn deferred_q8_join_key_conflict_falls_back_to_scan() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // 模拟 q20（join auction_events on b.auction == auction_events.id）先注册
    // id 索引——q8 的 join 键是 seller。
    q8_auction_window(&router).set_join_key("id".into());
    assert!(q8_auction_window(&router).has_join_key("id"));
    assert!(!q8_auction_window(&router).has_join_key("seller"));

    // 与 deferred_q8_hit_outputs_when_watermark_passes_bucket_end 相同的场景。
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(5, T)]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "未到期 — 不输出");

    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[(5, T + 5_000_000_000)]))
        .unwrap();
    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[(99, T + 11_000_000_000)]))
        .unwrap();
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "id 索引占用时 q8（seller 无索引）走扫描回退仍命中"
    );

    // 2026-08-30 多 key：补注册 seller → q8 走 O(1) 索引，同场景输出不变。
    q8_auction_window(&router).set_join_key("seller".into());
    assert!(q8_auction_window(&router).has_join_key("seller"));
    let (mut task2, mut alert_rx2, router2) = make_q8_task();
    q8_auction_window(&router2).set_join_key("id".into());
    q8_auction_window(&router2).set_join_key("seller".into());
    q8_person_window(&router2)
        .append_with_watermark(person_batch(&[(5, T)]))
        .unwrap();
    task2.pull_and_advance().await;
    q8_auction_window(&router2)
        .append_with_watermark(q8_auction_batch(&[(5, T + 5_000_000_000)]))
        .unwrap();
    q8_auction_window(&router2)
        .append_with_watermark(q8_auction_batch(&[(99, T + 11_000_000_000)]))
        .unwrap();
    q8_person_window(&router2)
        .append_with_watermark(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task2.pull_and_advance().await;
    let alert2 = crate::engine_task::tests::take_alert(&mut alert_rx2);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert2, "__wfu_entity_id"),
        "5",
        "多 key 索引（seller 有索引）时 q8 走 O(1) 仍命中"
    );
}

/// 恰在桶边界（T+10s）的 auction → 上开排除（归下桶，权威 TUMBLE [B, B+10s)）。
#[tokio::test]
async fn deferred_q8_boundary_auction_excluded() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    q8_person_window(&router)
        .append(person_batch(&[(5, T)]))
        .unwrap();
    // auction 恰在桶边界 T+10s
    q8_auction_window(&router)
        .append(q8_auction_batch(&[(5, T + 10_000_000_000)]))
        .unwrap();
    q8_person_window(&router)
        .append(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // person 5 到期但桶内无其 auction（边界行归下桶）→ 不输出；person 6 未到期
    assert!(
        alert_rx.try_recv().is_err(),
        "上开桶排除边界 auction → person 5 不输出"
    );
}

/// 桶内无该 seller 的 auction → 到期不输出（没创建拍卖）。
#[tokio::test]
async fn deferred_q8_no_auction_no_output() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    q8_person_window(&router)
        .append(person_batch(&[(5, T)]))
        .unwrap();
    // auction seller=9（不同 seller）在桶内
    q8_auction_window(&router)
        .append(q8_auction_batch(&[(9, T + 5_000_000_000)]))
        .unwrap();
    q8_person_window(&router)
        .append(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "桶内无该 seller 的 auction → 不输出"
    );
}

/// EOS 重试（2026-08-23 q8 修复）：到期评估时 join 目标窗口 append 滞后
/// （auction 尚未 ingest）→ 实例进 `missed`；EOS flush 时目标完整，重试命中
/// 补输出——q8 引擎 33k vs oracle 82k 的修复路径。
#[tokio::test]
async fn deferred_q8_eos_retry_recovers_miss_from_late_join_target() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // person 5 注册（T，桶界 T+10s）→ 挂起；watermark = T
    q8_person_window(&router)
        .append(person_batch(&[(5, T)]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "未到期 — 不输出");

    // person 6（T+11s）推进 watermark 过 T+10s → person 5 到期评估，但此时
    // auction 窗口为空（append 滞后）→ miss 进 `missed`（非 EOS 扫描收集）
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "auction 窗口仍为空 → 到期 miss，等 EOS 重试"
    );

    // auction（seller=5，桶内 T+5s）迟到进入右窗——模拟 append 滞后
    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[(5, T + 5_000_000_000)]))
        .unwrap();

    // EOS flush：scan_deferred(i64::MAX) + 重试 missed → 命中补输出
    task.flush().await;

    let alert = crate::engine_task::tests::take_alert(&mut alert_rx);
    assert_eq!(
        crate::engine_task::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "EOS 重试必须补出桶内 seller==5 的 person"
    );
}

/// EOS 重试对真 miss 不误报：auction 窗口补齐后仍无匹配 → 不输出。
#[tokio::test]
async fn deferred_q8_eos_retry_true_miss_stays_silent() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // person 5 挂起，到期时 auction 窗口为空 → miss
    q8_person_window(&router)
        .append(person_batch(&[(5, T)]))
        .unwrap();
    task.pull_and_advance().await;
    q8_person_window(&router)
        .append(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // 迟到的 auction seller=9 ≠ 5 → EOS 重试仍 miss
    q8_auction_window(&router)
        .append(q8_auction_batch(&[(9, T + 5_000_000_000)]))
        .unwrap();
    task.flush().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "EOS 重试后仍无 seller==5 的 auction → 真 miss，不输出"
    );
}

/// keep-running EOS 竞态复现（2026-08-23 补充）：EOS flush 发生时窗口 actors
/// 可能还在排空 mailbox → join 目标窗口**不完整**。若重试 miss 被直接 drop，
/// 之后窗口补全也丢失输出（shutdown 路径因 LIFO 排序无此问题；keep-running
/// 的 daemon 场景是真实隐患）。修复：重试仍 miss 的实例保留回 `missed`，
/// 等窗口确认完整后的下一次 flush 再判定真 miss。
#[tokio::test]
async fn deferred_q8_eos_retry_preserves_miss_until_window_complete() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // person 5（T，桶 [T, T+10s)）挂起；person 6（T+11s）推水位过桶末 →
    // person 5 到期评估，auction 窗口为空 → miss 收集进 missed
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(5, T)]))
        .unwrap();
    task.pull_and_advance().await;
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // 模拟 EOS flush，但 auction 窗口**仍未 append**（actors 排空滞后）——
    // 重试基于不完整窗口 → 假 miss
    task.flush().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "窗口不完整时 EOS flush 不输出（假 miss）"
    );

    // actors 排空后窗口补全：auction（seller=5）入桶
    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[(5, T + 5_000_000_000)]))
        .unwrap();
    // 窗口补全后再 flush（shutdown 或下一输入 EOS）→ 必须补出 person 5
    task.flush().await;
    assert_eq!(
        drain_alert_entity_ids(&mut alert_rx),
        vec!["5"],
        "窗口补全后的 flush 必须补出 person 5（重试 miss 不得被提前丢弃）"
    );
}

/// watermark 扫描**命中**（非 miss）→ flush 不得重复输出：missed 只收集 miss
/// 实例，已命中实例不会进入重试路径。
#[tokio::test]
async fn deferred_q8_watermark_hit_not_duplicated_by_flush() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // auction seller=5 先入右窗（桶内 T+5s）；person 5 注册（T）→ 挂起；
    // person 6（T+11s）推 watermark 过 T+10s → person 5 到期**命中**
    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[(5, T + 5_000_000_000)]))
        .unwrap();
    // 目标窗口追平（另一个 auction @T+11s 推 max_event_time 过 T+10s）
    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[(99, T + 11_000_000_000)]))
        .unwrap();
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(5, T)]))
        .unwrap();
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    assert_eq!(
        drain_alert_entity_ids(&mut alert_rx),
        vec!["5"],
        "watermark 扫描命中输出 person 5"
    );

    // EOS flush：missed 为空 → 不得重复输出已命中实例
    task.flush().await;
    assert!(
        drain_alert_entity_ids(&mut alert_rx).is_empty(),
        "flush 不得重复输出已命中实例"
    );
}

/// flush 幂等：EOS 重试命中后再次 flush（pending 已收口、missed 已 take）
/// 不产生重复输出。
#[tokio::test]
async fn deferred_q8_flush_twice_idempotent() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // person 5 到期时 auction 窗口为空 → miss；auction（seller=5）迟到入桶
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(5, T)]))
        .unwrap();
    task.pull_and_advance().await;
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[(5, T + 5_000_000_000)]))
        .unwrap();

    task.flush().await;
    assert_eq!(
        drain_alert_entity_ids(&mut alert_rx),
        vec!["5"],
        "EOS 重试补出 person 5"
    );

    // 第二次 flush：无新增（missed 被 take、pending 已收口）
    task.flush().await;
    assert!(
        drain_alert_entity_ids(&mut alert_rx).is_empty(),
        "第二次 flush 幂等，不重复输出"
    );
}

/// 多个实例在不同 watermark 扫描 miss → EOS 重试各自恰好补出一次
/// （不丢不重；混入的真 miss 保持静默）。
#[tokio::test]
async fn deferred_q8_multiple_missed_recovered_exactly_once() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // person 5（T，桶 [T, T+10s)）、person 7（T+21s，桶 [T+21s, T+31s)）
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(5, T), (7, T + 21_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    // person 6（T+11s）推 watermark 过 person 5 桶末 → person 5 miss（窗空）
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    // person 8（T+32s）推 watermark 过 person 6/person 7 桶末：person 6 到期
    // miss（真 miss，无其 auction）、person 7 到期 miss（窗仍空）
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(8, T + 32_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "全部 miss，flush 前无输出");

    // 两个迟到 auction 各自入桶（seller 5 桶内 T+5s；seller 7 桶内 T+25s）
    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[
            (5, T + 5_000_000_000),
            (7, T + 25_000_000_000),
        ]))
        .unwrap();

    task.flush().await;
    let mut ids = drain_alert_entity_ids(&mut alert_rx);
    ids.sort();
    assert_eq!(
        ids,
        vec!["5", "7"],
        "EOS 重试各自恰好补出一次；person 6 真 miss 保持静默"
    );
}

/// miss 实例从 pending 移除后**不进后续 watermark 扫描**（不提前输出、不重复），
/// 只由 EOS 重试补出——期间多次水位推进必须保持静默。
#[tokio::test]
async fn deferred_q8_miss_not_reevaluated_until_flush() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // person 5（T）→ person 6（T+11s）推水位过桶末 → person 5 miss（窗空）
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(5, T)]))
        .unwrap();
    task.pull_and_advance().await;
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "miss 后无输出");

    // auction（seller=5）入桶；再推两轮水位（person 9/10）——person 5 在
    // missed 中，后续扫描不得重新评估它
    q8_auction_window(&router)
        .append_with_watermark(q8_auction_batch(&[(5, T + 5_000_000_000)]))
        .unwrap();
    q8_person_window(&router)
        .append_with_watermark(person_batch(&[
            (9, T + 41_000_000_000),
            (10, T + 51_000_000_000),
        ]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "miss 实例只由 EOS 重试补出，后续 watermark 扫描不得提前输出"
    );

    task.flush().await;
    assert_eq!(
        drain_alert_entity_ids(&mut alert_rx),
        vec!["5"],
        "flush 重试恰好补出一次"
    );
}

/// 尾部未到期实例（expiry > 最终事件时间 watermark）即使桶内有 auction 也
/// **不输出**——事件时间域窗口未完成（oracle/Flink 语义）。i64::MAX 强评会
/// 多出尾部桶（Q8 实证 82446 → 83274，+828），flush 按最终水位收口后必须静默。
#[tokio::test]
async fn deferred_q8_unexpired_tail_with_auction_not_emitted_at_flush() {
    crate::engine_task::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // person 5（T，桶 [T, T+10s)）挂起；auction seller=5 桶内 T+5s 入右窗
    q8_person_window(&router)
        .append(person_batch(&[(5, T)]))
        .unwrap();
    q8_auction_window(&router)
        .append(q8_auction_batch(&[(5, T + 5_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    // 无后续 person 事件 → 最终 watermark = T < T+10s：窗口未完成
    task.flush().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "尾部未到期（expiry > 最终 watermark）即使桶内有 auction 也不输出"
    );
}
