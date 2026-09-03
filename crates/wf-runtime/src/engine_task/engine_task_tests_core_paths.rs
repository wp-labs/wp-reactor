//! 规则任务核心数据通路与生命周期测试（2026-09-04 自 engine_task/tests.rs 拆出，`#[path]` 兄弟子模块，
//! `use super::*` 继承共享 harness）：
//! - pull/push 直驱基础：空窗不产出、cursor 推进、count>=3 触发告警；
//! - push 分片行子集：deferred 列式 push 只扫本 shard `shard_rows`，subset 不触发；
//! - 列式/延迟物化与行式 interpreted 路径对拍（bind filter / branch guard / each）；
//! - 事件时间归一化、flush close 语义与关机 drain / rule cancel 生命周期；
//! - 独占 helper make_filter_task / make_branch_guard_task / make_filtered_close_task
//!   （+ make_filtered_close_config）随迁；`crate::engine_task::run_rule_task` 下沉后改绝对路径。

use super::*;

fn make_filter_task(
    filter: Expr,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    make_task_inner(Some(filter), None, usize::MAX)
}

fn make_branch_guard_task(
    branch_guard: Expr,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    make_task_inner(None, Some(branch_guard), usize::MAX)
}

fn make_filtered_close_config() -> (
    task_types::RuleTaskConfig,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    let schema = filtered_schema();
    let (win_arc, notify_arc) = make_window("auth_events", &schema, usize::MAX);

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Simple("sip".into())],
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
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
                    threshold: Expr::Number(3.0),
                },
            }],
        }],
        close_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: Some("close_count".into()),
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
        close_mode: CloseMode::And,
        tracked_bind_aliases: std::collections::HashSet::new(),
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
        name: "filtered_close".into(),
        binds: vec![BindPlan {
            alias: "fail".into(),
            window: "auth_events".into(),
            filter: Some(Expr::BinOp {
                op: BinOp::Eq,
                left: Box::new(Expr::Field(FieldRef::Simple("action".into()))),
                right: Box::new(Expr::StringLit("failed".into())),
            }),
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

    let machine = CepStateMachine::new(
        "filtered_close".into(),
        match_plan,
        Some("event_time".into()),
    );
    let executor = RuleExecutor::new(rule_plan);
    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: Some(machine),
        each_alias: None,
        each_time_field: None,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&win_arc),
            notify: Arc::clone(&notify_arc),
            aliases: vec!["fail".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: Duration::from_secs(60),
        router,
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
    };
    (config, alert_rx, win_arc, notify_arc)
}

fn make_filtered_close_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
) {
    let (config, alert_rx, win_arc, notify_arc) = make_filtered_close_config();
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc)
}

#[tokio::test]
async fn pull_empty_window() {
    init_tracing();
    let (mut task, mut alert_rx, _win, _notify) = make_task();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "empty window should produce no alerts"
    );
}

#[tokio::test]
async fn pull_advances_cursor() {
    init_tracing();
    let schema = test_schema();
    let (mut task, _alert_rx, win, _notify) = make_task();

    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.2"], ts);
    win.append(batch).unwrap();

    task.pull_and_advance().await;
    let cursor = task.cursors["auth_events"];
    assert_eq!(
        cursor, 1,
        "cursor should advance to 1 after reading one batch"
    );

    task.pull_and_advance().await;
    let cursor2 = task.cursors["auth_events"];
    assert_eq!(cursor2, 1, "cursor should remain 1 with no new data");
}

#[tokio::test]
async fn pull_triggers_alert() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, win, _notify) = make_task();

    let ts_nanos = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts_nanos);
    win.append(batch).unwrap();

    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "test_rule");
    assert_eq!(field_str(&alert, "__wfu_entity_type"), "ip");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!((field_f64(&alert, "__wfu_score") - 70.0).abs() < f64::EPSILON);
    assert!(!field_str(&alert, "__wfu_fired_at").is_empty());
}

#[tokio::test]
async fn push_triggers_alert() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, _win, _notify) = make_task();

    let ts_nanos = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts_nanos);

    // Feed the same parsed events the router would broadcast into the rule's
    // push channel, and advance the state machine through the push path.
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

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "test_rule");
    assert_eq!(field_str(&alert, "__wfu_entity_type"), "ip");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!((field_f64(&alert, "__wfu_score") - 70.0).abs() < f64::EPSILON);
    assert!(!field_str(&alert, "__wfu_fired_at").is_empty());
}

#[tokio::test]
async fn push_columnar_sharded_defers_runs_all_rows() {
    // 列式 sharded deferred push：events=None + batch + shard_rows（本 shard 行子集）。
    // 规则任务只对 shard_rows 内的行跑 bind filter(无=全行命中）+状态机（count>=3 触发）。
    // 此处 shard_rows 含全部 3 行 → 全命中 → 触发 alert。
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, _win, _notify) = make_task();

    let ts_nanos = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts_nanos);

    let push = RulePush {
        window_name: "auth_events".into(),
        events: None, // deferred: 规则任务按 batch 列式物化命中行
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: Some(Arc::new(vec![0, 1, 2])), // 本 shard 拥有全部行
        seq: u64::MAX,
    };
    task.process_push(push).await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!((field_f64(&alert, "__wfu_score") - 70.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn push_sharded_only_processes_shard_rows_subset() {
    // 列式 sharded：shard_rows 只含行 0,2（两个 10.0.0.1）；行 1（10.0.0.2）不属于本
    // shard。规则只应对 shard_rows 内行推进状态机 → count=2 <3 不触发 → 无 alert，
    // 证明只扫 shard 子集（若误扫全批会让 10.0.0.1 count=2 仍不触发，故同时把行 1 也
    // 设为 10.0.0.1 以区分「子集处理」的额外断言在尾部补）。
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, _win, _notify) = make_task();

    let ts_nanos = 1_700_000_000_000_000_000i64;
    // 行 0: 10.0.0.1, 行 1: 10.0.0.1, 行 2: 10.0.0.1 —— 全同 key；
    // 若规则误扫整批（3 行）会触发 count=3；shard_rows 只给 [0,1] → 只扫 2 行 → count=2 不触发。
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts_nanos);

    let push = RulePush {
        window_name: "auth_events".into(),
        events: None,
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: Some(Arc::new(vec![0, 1])), // 本 shard 只有 2 行
        seq: u64::MAX,
    };
    task.process_push(push).await;

    // count=2 (<3) → 不触发。
    let tr = alert_rx.try_recv();
    assert!(
        matches!(tr, Err(tokio::sync::mpsc::error::TryRecvError::Empty)),
        "shard_rows 子集行 count=2 不应触发 alert"
    );
}

#[tokio::test]
async fn columnar_bind_filter_matches_interpreted_path() {
    init_tracing();
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    // 3× "10.0.0.1" (count>=3 fires once) + 1× "10.0.0.2" (filtered out).
    let batch = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.1"],
        ts,
    );
    assert!(wf_lang::columnar::expr_is_columnar(&filter));
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );

    // Columnar path: the push carries the raw batch → bind filter is a mask.
    // 用 await 收单条（带超时）而非 drain+try_recv：emit 与断言之间无
    // 确定性同步点，立即排空在全量并发下会偶发 Empty（2026-09 实测 flake）。
    let (mut task, mut alert_rx, _win, _notify) = make_filter_task(filter.clone());
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::clone(&events)),
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let columnar = take_alert_recv_timeout(&mut alert_rx).await;

    // Interpreted path: no raw batch → per-event `event_matches_alias`.
    let (mut task2, mut alert_rx2, _win2, _notify2) = make_filter_task(filter);
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let interpreted = take_alert_recv_timeout(&mut alert_rx2).await;

    assert_eq!(
        field_str(&columnar, "__wfu_entity_id"),
        field_str(&interpreted, "__wfu_entity_id"),
        "列式与解释路径实体一致"
    );
    // Only sip == "10.0.0.1" passes the filter; 3 of them reach count>=3 → one fire.
    assert_eq!(field_str(&columnar, "__wfu_entity_id"), "10.0.0.1");
    assert_eq!(field_str(&interpreted, "__wfu_entity_id"), "10.0.0.1");
}

/// `take_alert_recv` 带 5s 超时版：测试收不到输出时快速失败而非挂起。
async fn take_alert_recv_timeout(
    rx: &mut mpsc::Receiver<crate::alert_task::AlertBatch>,
) -> Arc<wp_model_core::model::DataRecord> {
    let batch = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("5s 内未收到 alert（emit 未发生或投递竞态）")
        .expect("alert channel closed");
    first_record(&batch)
}

#[tokio::test]
async fn columnar_branch_guard_matches_interpreted_path() {
    init_tracing();
    let guard = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.1"],
        ts,
    );
    assert!(wf_lang::columnar::expr_is_columnar(&guard));
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );

    // Columnar branch guard: push carries the raw batch → guard is a mask.
    let (mut task, mut alert_rx, _win, _notify) = make_branch_guard_task(guard.clone());
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::clone(&events)),
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let columnar_ids = drain_alert_entity_ids(&mut alert_rx);

    // Interpreted branch guard: no raw batch → per-event guard in the state machine.
    let (mut task2, mut alert_rx2, _win2, _notify2) = make_branch_guard_task(guard);
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let interpreted_ids = drain_alert_entity_ids(&mut alert_rx2);

    assert_eq!(columnar_ids, interpreted_ids);
    assert_eq!(columnar_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn deferred_materialization_matches_eager_path() {
    init_tracing();
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.1"],
        ts,
    );

    // Deferred: no pre-parsed events → the rule task materializes from the raw batch.
    let (mut task, mut alert_rx, _win, _notify) = make_filter_task(filter.clone());
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let deferred_ids = drain_alert_entity_ids(&mut alert_rx);

    // Eager: pre-parsed events (as `route_parse` would broadcast).
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let (mut task2, mut alert_rx2, _win2, _notify2) = make_filter_task(filter);
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let eager_ids = drain_alert_entity_ids(&mut alert_rx2);

    assert_eq!(deferred_ids, eager_ids);
    assert_eq!(deferred_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn each_noncolumnar_bind_filter_columnar_hit_matches_row_path() {
    // gap-4（2026-09-02）：非列式 bind filter 的 each 规则——columnar_each
    // 命中循环逐行 `event_matches_alias`（ColumnarEvent 视图直读列）vs 行式
    // eager 路径（Event 物化 + 同函数解释），输出必须一致（filter 拒绝的行
    // 不再被 hit.fill(true) 静默放行）。
    init_tracing();
    let bind_filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::FuncCall {
            qualifier: None,
            name: "upper".into(),
            args: vec![Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))],
        }),
        right: Box::new(Expr::StringLit("ABC".into())),
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["abc", "AB", "xyz", "abc"], ts);

    // 列式路径：push raw batch（columnar_each → 命中循环逐行解释）。
    let (mut task, mut alert_rx, _win, _notify) =
        make_each_task_with_bind_filter(Some(bind_filter.clone()));
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let columnar_ids = drain_alert_entity_ids(&mut alert_rx);

    // 行式路径：push materialized events（eager，无 batch → event_matches_alias
    // 解释于 Event）。
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let (mut task2, mut alert_rx2, _win2, _notify2) =
        make_each_task_with_bind_filter(Some(bind_filter));
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let row_ids = drain_alert_entity_ids(&mut alert_rx2);

    // upper(sip)=="ABC" → 行 0/3（"abc"）过；行 1（"AB"）、行 2（"xyz"）拒。
    assert_eq!(columnar_ids, row_ids, "列式命中循环必须与行式 filter 一致");
    assert_eq!(columnar_ids, vec!["abc".to_string(), "abc".to_string()]);
}

#[tokio::test]
async fn events_and_batch_both_present_prefers_columnar_path() {
    // 2026-08-22：defer_materialize 放宽——raw batch 存在且 bind filter 列式时，
    // 即使 relay/push 同时携带物化 events 也走列式（deferred）路径；events 仅作
    // emit 路径 trigger 投影。断言与纯 eager 输出一致（filter 仍生效：只放行
    // sip=10.0.0.1）。
    init_tracing();
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.1"],
        ts,
    );
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );

    // events + batch 同时存在：放宽后应走列式（deferred），filter 列式生效。
    let (mut task, mut alert_rx, _win, _notify) = make_filter_task(filter);
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::clone(&events)),
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let both_ids = drain_alert_entity_ids(&mut alert_rx);
    assert_eq!(both_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn non_columnar_filter_with_batch_falls_back_to_eager() {
    // 非列式 bind filter（含谓词函数调用）→ 即使 batch 存在也不 defer：
    // eager 路径解释执行 filter（拒绝的行不得漏进状态机——deferred 的
    // missing-mask 兜底会全放行，必须避免）。
    init_tracing();
    // `sip contains "0.0"` —— contains 非列式（列式安全门外），走解释器。
    let filter = Expr::FuncCall {
        qualifier: None,
        name: "contains".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".into())),
            Expr::StringLit("0.0".into()),
        ],
    };
    let schema = test_schema();
    let ts = 1_700_000_000_000_000_000i64;
    // contains "0.0"：10.0.0.1×3 命中（count=3 收口 fire）；9.9.9.9 不含 "0.0" 被拒。
    let batch = make_batch(
        &schema,
        &["10.0.0.1", "10.0.0.1", "10.0.0.1", "9.9.9.9"],
        ts,
    );
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let (mut task, mut alert_rx, _win, _notify) = make_filter_task(filter);

    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: Some(Arc::clone(&events)),
        batch: Some(Arc::new(batch)),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let both_ids = drain_alert_entity_ids(&mut alert_rx);
    assert_eq!(both_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn deferred_materialization_scans_every_row_for_intra_batch_expiry() {
    init_tracing();
    // `sip == "10.0.0.1"` is a columnar bind filter, so the deferred path
    // skips materializing the rejected "10.0.0.2" row. The rejected row's
    // event time (400s) must still drive the watermark/expiry scan: the
    // 300s sliding window instance created at T=0 must expire at T=400s,
    // before the next accepted row starts a fresh instance (count=1).
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("sip".into()))),
        right: Box::new(Expr::StringLit("10.0.0.1".into())),
    };
    let schema = test_schema();
    let sips = ["10.0.0.1", "10.0.0.1", "10.0.0.2", "10.0.0.1"];
    let times = [0i64, 100_000_000_000, 400_000_000_000, 400_000_000_000];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
        ],
    )
    .unwrap();
    assert!(wf_lang::columnar::expr_is_columnar(&filter));

    // Deferred: no pre-parsed events → only the bind-filter hit rows are
    // materialized; the rejected row is still scanned for expiry.
    let (mut task, mut alert_rx, _win, _notify) = make_filter_task(filter.clone());
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let deferred_ids = drain_alert_entity_ids(&mut alert_rx);

    // Eager: pre-parsed events (full materialization).
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let (mut task2, mut alert_rx2, _win2, _notify2) = make_filter_task(filter);
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let eager_ids = drain_alert_entity_ids(&mut alert_rx2);

    // The 300s window expires the T=0 instance at the rejected row's T=400s,
    // so the final accepted row starts over at count=1 — no `count>=3` fire.
    assert_eq!(deferred_ids, eager_ids);
    assert!(deferred_ids.is_empty());
}

#[tokio::test]
async fn deferred_materialization_preserves_close_emission_for_rejected_rows() {
    init_tracing();
    // Regression: the deferred path used to `continue` past the close-emission
    // block for bind-filter-rejected rows, dropping expired-instance closes.
    // A columnar bind filter (`action == "failed"`) + a close step: accepted
    // rows complete the event step, then a later rejected row's event time
    // expires the instance and must still emit the close.
    let schema = filtered_schema();
    let sips = ["10.0.0.1", "10.0.0.1", "10.0.0.1", "10.0.0.1"];
    let actions = ["failed", "failed", "failed", "login"];
    let times = [0i64, 100_000_000_000, 200_000_000_000, 400_000_000_000];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                sips.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                actions.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
        ],
    )
    .unwrap();

    // Deferred: no pre-parsed events; the rejected "login" row (T=400s) must
    // still drive the expiry scan and emit the instance's close.
    let (mut task, mut alert_rx, _win, _notify) = make_filtered_close_task();
    task.process_push(RulePush {
        window_name: "auth_events".into(),
        events: None,
        batch: Some(Arc::new(batch.clone())),
        materialize_fields: None,
        shard_rows: None,
        seq: u64::MAX,
    })
    .await;
    let deferred_ids = drain_alert_entity_ids(&mut alert_rx);

    // Eager: pre-parsed events (full materialization).
    let events = Arc::new(
        batch_to_events(&batch)
            .into_iter()
            .map(Arc::new)
            .collect::<Vec<_>>(),
    );
    let (mut task2, mut alert_rx2, _win2, _notify2) = make_filtered_close_task();
    task2
        .process_push(RulePush {
            window_name: "auth_events".into(),
            events: Some(events),
            batch: None,
            materialize_fields: None,
            shard_rows: None,
            seq: u64::MAX,
        })
        .await;
    let eager_ids = drain_alert_entity_ids(&mut alert_rx2);

    // Both paths emit exactly one close for the expired instance.
    assert_eq!(deferred_ids, eager_ids);
    assert_eq!(deferred_ids, vec!["10.0.0.1".to_string()]);
}

#[tokio::test]
async fn pull_keeps_normalized_nanos_event_time() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, win, _notify) = make_task();

    let ts_nanos = 1_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1", "10.0.0.1"], ts_nanos);
    win.append(batch).unwrap();

    task.pull_and_advance().await;

    let alert = take_alert(&mut alert_rx);
    assert!(!field_str(&alert, "__wfu_fired_at").is_empty());
}

#[tokio::test]
async fn flush_emits_close_alert_for_completed_and_close_rule() {
    init_tracing();
    let schema = filtered_schema();
    let (mut task, mut alert_rx, win, _notify) = make_filtered_close_task();

    let ts = 1_700_000_000_000_000_000i64;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["10.0.0.1", "10.0.0.1", "10.0.0.1"])),
            Arc::new(StringArray::from(vec!["failed", "failed", "failed"])),
            Arc::new(TimestampNanosecondArray::from(vec![ts, ts + 1, ts + 2])),
        ],
    )
    .unwrap();
    win.append(batch).unwrap();

    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "and-close rule should not emit before close/flush"
    );

    task.flush().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "filtered_close");
    assert_eq!(field_str(&alert, "__wfu_entity_type"), "ip");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert_eq!(field_str(&alert, "__wfu_origin"), "close:flush");
}

#[tokio::test]
async fn shutdown_drain_pulls_tail_before_flush() {
    // e2e_datagen_brute_force CI flake regression: at full shutdown the rule
    // task must keep pulling until the window actor reports drained, so the
    // final flush runs against a complete machine. Without it the flush
    // closes at a stale machine watermark (the alert's fired_at falls on the
    // pre-tail watermark) and tail-triggered alerts are lost.
    init_tracing();
    let (mut task, mut alert_rx, win, _notify) = make_filtered_close_task();
    // The window actor is still committing its queued tail at shutdown.
    win.set_actor_drained(false);
    let ts = 1_700_000_000_000_000_000i64;

    // Tail committed to the window but NOT yet pulled by the rule task (what
    // the actor's cancel-drain commits before setting the drained flag):
    // 3 rows push count>=3, the 4th row (ts+60s) is the tail's last event.
    let batch = RecordBatch::try_new(
        filtered_schema(),
        vec![
            Arc::new(StringArray::from(vec![
                "10.0.0.1", "10.0.0.1", "10.0.0.1", "10.0.0.1",
            ])),
            Arc::new(StringArray::from(vec![
                "failed", "failed", "failed", "failed",
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![
                ts,
                ts + 1,
                ts + 2,
                ts + 60_000_000_000,
            ])),
        ],
    )
    .unwrap();
    win.append(batch).unwrap();

    // The drain must block while the actor is still draining…
    let mut drain = Box::pin(task.wait_shutdown_drain());
    tokio::select! {
        _ = &mut drain => panic!(
            "wait_shutdown_drain must block while the window actor is still draining"
        ),
        _ = tokio::time::sleep(Duration::from_millis(30)) => {}
    }
    // …and once the actor reports drained, it completes and the flush sees
    // the tail.
    win.set_actor_drained(true);
    drain.await;
    task.flush().await;

    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "filtered_close");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert_eq!(field_str(&alert, "__wfu_origin"), "close:flush");
    // The close must fire at the tail's watermark (4th row, ts+60s) — not a
    // stale pre-tail watermark (ts+2).
    let fired = chrono::DateTime::parse_from_rfc3339(&field_str(&alert, "__wfu_fired_at"))
        .unwrap_or_else(|e| panic!("parse fired_at: {e}"));
    assert_eq!(
        fired.timestamp_nanos_opt().expect("fired_at nanos"),
        ts + 60_000_000_000,
        "flush must run after the tail was pulled (fired_at = tail watermark)"
    );
}

#[tokio::test]
async fn shutdown_drain_times_out_when_actor_stuck() {
    // Safety net: a window actor that never reports drained must not hang the
    // shutdown forever — wait_shutdown_drain bails at the timeout and the
    // flush proceeds with the state it has.
    init_tracing();
    let (mut task, _alert_rx, win, _notify) = make_filtered_close_task();
    win.set_actor_drained(false);
    tokio::time::timeout(
        std::time::Duration::from_secs(7),
        task.wait_shutdown_drain(),
    )
    .await
    .expect("shutdown drain must not hang on a stuck actor (bounded by SHUTDOWN_DRAIN_TIMEOUT)");
}

#[tokio::test]
async fn rule_cancel_without_root_cancel_skips_drain_wait() {
    // Hot-reload shape: only the rule token fires, the window actors keep
    // running (never report drained). The shutdown drain wait must be skipped
    // or every reload would stall ~SHUTDOWN_DRAIN_TIMEOUT.
    init_tracing();
    let (mut config, _alert_rx, win, _notify) = make_filtered_close_config();
    win.set_actor_drained(false); // actors keep running → never drained
    let rule_cancel = tokio_util::sync::CancellationToken::new();
    config.cancel = rule_cancel.clone();
    let root_cancel = tokio_util::sync::CancellationToken::new();

    rule_cancel.cancel();
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        crate::engine_task::run_rule_task(config, root_cancel),
    )
    .await
    .expect("rule-only cancel must exit promptly without waiting for the actors")
    .expect("run_rule_task ok");
}

#[tokio::test]
async fn full_shutdown_with_real_actor_processes_mailbox_tail() {
    // 完整竞态的端到端回归：真实 window actor 在关停时 mailbox 里还押着尾部
    // 批次。规则任务必须等 actor 的 drained 标志（边等边拉），最终 flush 才
    // 会收口每个 key 的告警——无修复时它在陈旧 machine watermark 上 flush，
    // 未提交的尾部直接丢失（e2e_datagen_brute_force CI flake 同型）。
    init_tracing();
    let (mut config, mut alert_rx, win, notify) = make_filtered_close_config();

    // 在配置同一窗口上起一个真实 actor（规则任务从同一 Arc 拉取）。
    let (mailbox_tx, mailbox_rx) =
        mpsc::channel::<wf_engine::window::WindowMsg>(wf_engine::window::WINDOW_CHANNEL_DEPTH);
    let actor_cancel = tokio_util::sync::CancellationToken::new();
    let actor_win = Arc::clone(&win);
    let actor_cancel2 = actor_cancel.clone();
    let actor_notify = Arc::clone(&notify);
    let actor = tokio::spawn(async move {
        wf_engine::window::run_window_actor(
            Arc::from("auth_events"),
            actor_win,
            Arc::new(wf_engine::window::EvictionGate::new(usize::MAX)),
            wf_engine::window::RuleFanout::new(),
            actor_notify,
            mailbox_rx,
            actor_cancel2,
            None,
        )
        .await;
    });

    // 规则任务（pull 模式）读同一窗口。
    let root_cancel = tokio_util::sync::CancellationToken::new();
    let rule_cancel = tokio_util::sync::CancellationToken::new();
    config.cancel = rule_cancel.clone();
    let run = tokio::spawn(crate::engine_task::run_rule_task(
        config,
        root_cancel.clone(),
    ));

    // 等 actor 与规则任务就绪，然后把 5 个 3 行批次（每 key 一个）打进
    // actor 的 mailbox。count>=3 命中、无 close 事件 → 告警全部由关停
    // flush 收口（close:flush）。
    tokio::time::sleep(Duration::from_millis(30)).await;
    let ts = 1_700_000_000_000_000_000i64;
    let keys = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5"];
    for (i, key) in keys.iter().enumerate() {
        let batch = RecordBatch::try_new(
            filtered_schema(),
            vec![
                Arc::new(StringArray::from(vec![*key, *key, *key])),
                Arc::new(StringArray::from(vec!["failed", "failed", "failed"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    ts + (i * 3) as i64 * 1_000_000_000,
                    ts + (i * 3 + 1) as i64 * 1_000_000_000,
                    ts + (i * 3 + 2) as i64 * 1_000_000_000,
                ])),
            ],
        )
        .unwrap();
        mailbox_tx
            .send(wf_engine::window::WindowMsg::Append {
                source: Arc::from("ingress"),
                seq: i as u64,
                batch,
                events: None,
                byte_size: 128,
                permits: Vec::new(),
                shard_rows: None,
            })
            .await
            .unwrap();
    }

    // actor 可能还押着尾部时立刻关停（真实 reactor 中 root cancel 会传播
    // 到 rule_cancel——child token；测试里两个 token 独立，需都 cancel）。
    root_cancel.cancel();
    rule_cancel.cancel();
    actor_cancel.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("rule task must finish promptly")
        .expect("rule task joined without panic")
        .expect("run_rule_task ok");
    actor.await.expect("actor joins");

    // 每个 key 都通过 close:flush 收口——drain 等到了尾部。
    let mut ids = drain_alert_entity_ids(&mut alert_rx);
    ids.sort();
    let expected: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    assert_eq!(
        ids, expected,
        "all tail keys must be flushed after the drain"
    );
}

#[tokio::test]
async fn flush_closes_active_instances() {
    init_tracing();
    let schema = test_schema();
    let (mut task, mut alert_rx, win, _notify) = make_task();

    let ts = 1_700_000_000_000_000_000i64;
    let batch = make_batch(&schema, &["10.0.0.1", "10.0.0.1"], ts);
    win.append(batch).unwrap();
    task.pull_and_advance().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "count=2 should not trigger alert"
    );

    task.flush().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "flush of incomplete instance should not produce alert"
    );
}
