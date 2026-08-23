//! HOP 滑动窗口 rule-task 级集成测试（wf-runtime）：hop(size, slide) 经
//! rule_task 机器路径逐窗口扇出，slide 边界无界预算收口扫描，conv-sink 路由
//! 与 flush 收口。引擎级逐窗口语义见 wf-engine `tests/l3/hop.rs`。

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::{Notify, mpsc};

use wf_engine::match_engine::{CepStateMachine, RuleExecutor};
use wf_engine::window::{Router, Window, WindowParams, WindowRegistry};
use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure};
use wf_lang::plan::{
    AggPlan, BindPlan, BranchPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StepPlan,
    WindowSpec, YieldField, YieldPlan,
};

use super::tests::{
    empty_tracked_bind_fields, empty_tracked_plain_fields, field_str, init_tracing,
    make_test_fanout, take_alert, test_window_config,
};
use crate::engine_task::conv_stage::{ConvCloseBatch, ConvShardSink};
use crate::engine_task::{rule_task, task_types};

/// 与 tests.rs 相同的全局时间基点（1.7e18 ns 可被 2s/10s 整除 → epoch 对齐）。
const T: i64 = 1_700_000_000_000_000_000;

/// Drain 当前缓冲的全部 alert 批次并摊平为记录（发射路径按 ALERT_BATCH_SIZE
/// 打包成 Columns 批次，`take_alert` 只能取每条批次的第 1 条记录）。
fn drain_alerts(
    rx: &mut mpsc::Receiver<crate::alert_task::AlertBatch>,
) -> Vec<wp_model_core::model::DataRecord> {
    let mut records = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        match batch {
            crate::alert_task::AlertBatch::Rows(rows) => {
                records.extend(rows.iter().map(|r| (**r).clone()));
            }
            crate::alert_task::AlertBatch::Columns(cols) => {
                records.extend(
                    cols.iter_data_records()
                        .map(|r| r.expect("columnar row view")),
                );
            }
        }
    }
    records
}

fn driver_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

fn driver_batch(sips: &[&str], ts: i64) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(sips.to_vec())),
        Arc::new(TimestampNanosecondArray::from(vec![ts; sips.len()])),
    ];
    RecordBatch::try_new(driver_schema(), cols).unwrap()
}

fn make_window(name: &str) -> (Arc<Window>, Arc<Notify>) {
    let mut cfg = test_window_config(usize::MAX);
    cfg.name = name.to_string();
    let win = Window::new(
        WindowParams {
            name: name.into(),
            schema: driver_schema(),
            time_col_index: Some(1), // event_time
            over: std::time::Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        cfg,
    );
    (Arc::new(win), Arc::new(Notify::new()))
}

/// 构建 hop(size, slide) count>=2 AND close 规则的 rule task。
///
/// 等价 wfl（close 阶段输出）：
/// ```wfl
/// rule hop_rule {
///   events { c : auth_events }
///   match<c:hop(size, slide)> {
///     on event { c | count >= 2; }
///     and close { c | count >= 2; }
///   } -> score(80.0)
///   entity(ip, c.sip)
///   yield alerts (sip = c.sip)
/// }
/// ```
///
/// `conv` 为真时把 raw close 路由到 conv 阶段（`ConvShardSink`）而非 inline
/// 执行——对应 rule_task 的 `conv_sink.is_some()` 分支。
#[allow(clippy::type_complexity)]
fn make_hop_task(
    size_secs: u64,
    slide_secs: u64,
    conv: bool,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Window>,
    Arc<Notify>,
    mpsc::Receiver<ConvCloseBatch>,
) {
    let (win_arc, notify_arc) = make_window("auth_events");

    let match_plan = MatchPlan {
        keys: vec![FieldRef::Qualified("c".into(), "sip".into())],
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Hop {
            size: std::time::Duration::from_secs(size_secs),
            slide: std::time::Duration::from_secs(slide_secs),
        },
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "c".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(2.0),
                },
            }],
        }],
        close_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "c".into(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(2.0),
                },
            }],
        }],
        close_mode: CloseMode::And,
        tracked_bind_aliases: HashSet::from(["c".to_string()]),
        tracked_bind_fields: empty_tracked_bind_fields(),
        tracked_plain_fields: empty_tracked_plain_fields(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: true,
        trigger_event_needed: false,
    };

    let rule_plan = RulePlan {
        conv_window: None,
        name: "hop_rule".into(),
        binds: vec![BindPlan {
            alias: "c".into(),
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
            entity_id_expr: Expr::Field(FieldRef::Qualified("c".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "sip".into(),
                value: Expr::Field(FieldRef::Qualified("c".into(), "sip".into())),
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(80.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
    };

    let machine = CepStateMachine::new("hop_rule".into(), match_plan, Some("event_time".into()));
    let executor = RuleExecutor::new(rule_plan);

    let (alert_tx, alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let (conv_tx, conv_rx) = mpsc::channel::<ConvCloseBatch>(64);

    let registry = WindowRegistry::build(vec![]).unwrap();
    let router = Arc::new(Router::new(registry));

    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: conv.then_some(ConvShardSink {
            tx: conv_tx,
            barrier_index: 0,
        }),
        machine: Some(machine),
        each_alias: None,
        each_time_field: None,
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: "auth_events".into(),
            window: Arc::clone(&win_arc),
            notify: Arc::clone(&notify_arc),
            aliases: vec!["c".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx),
        cancel: tokio_util::sync::CancellationToken::new(),
        timeout_scan_interval: std::time::Duration::from_secs(60),
        router,
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(wf_engine::pipe::PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
    };

    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, win_arc, notify_arc, conv_rx)
}

// ---------------------------------------------------------------------------
// hop(10s, 2s)：t=T 的 2 个事件扇出到 k∈{849999996..=850000000} 共 5 个窗口
// （窗口 k 区间 [k*2s, k*2s+10s)，上界开）。k=849999996 上界 T+2s，k=849999997
// 上界 T+4s……每 slide 边界恰一个窗口到期收口。
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hop_rule_task_closes_each_window_at_slide_boundary() {
    init_tracing();
    let (mut task, mut alert_rx, win, _notify, _conv_rx) = make_hop_task(10, 2, false);

    // t=T：2 个事件 → 5 个覆盖窗口各 count=2。And close 模式 + 未到期 → 无输出。
    win.append(driver_batch(&["10.0.0.1", "10.0.0.1"], T))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "no expiry yet");

    // t=T+3s：watermark 越过 k=849999996 上界（T+2s）→ 恰 1 个窗口收口输出。
    win.append(driver_batch(&["10.0.0.1"], T + 3_000_000_000))
        .unwrap();
    task.pull_and_advance().await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_rule_name"), "hop_rule");
    assert_eq!(field_str(&alert, "__wfu_entity_type"), "ip");
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert_eq!(field_str(&alert, "sip"), "10.0.0.1");

    // t=T+5s：k=849999997 上界 T+4s 越过 → 又一个窗口收口，且每边界恰 1 条。
    win.append(driver_batch(&["10.0.0.1"], T + 5_000_000_000))
        .unwrap();
    task.pull_and_advance().await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!(
        alert_rx.try_recv().is_err(),
        "exactly one window per slide boundary"
    );

    // t=T+6s：k=849999998 上界 T+6s 恰等于 wm → 到期收口（count=4）→ 又 1 条。
    // 完整窗口（w_end ≤ wm）经由正常到期路径输出——与 flush 的「未完整窗口
    // 不发射」互补（2026-08-23 close_all 对齐 oracle/Flink）。
    win.append(driver_batch(&["10.0.0.1"], T + 6_000_000_000))
        .unwrap();
    task.pull_and_advance().await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");

    // flush：剩余窗口 k∈{849999999..=850000003} 全部未完整（w_end=T+8s..T+16s
    // > wm=T+6s）→ 0 条。close_all 只收口完整窗口，未完整窗口释放实例但不
    // 发射（oracle/Flink 事件时间到末尾即止）。
    task.flush().await;
    let flushed = drain_alerts(&mut alert_rx);
    assert!(flushed.is_empty(), "flush 不发射尾部未完整窗口");
}

#[tokio::test]
async fn hop_rule_task_square_window_behaves_like_fixed() {
    init_tracing();
    // hop(10s, 10s)：size/slide = 1 → 每事件恰 1 个 epoch 对齐窗口（等价 fixed）。
    let (mut task, mut alert_rx, win, _notify, _conv_rx) = make_hop_task(10, 10, false);

    win.append(driver_batch(&["10.0.0.1", "10.0.0.1"], T))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "no expiry yet");

    // t=T+12s：窗口 [T, T+10s) 上界越过 → 恰 1 条收口。
    win.append(driver_batch(&["10.0.0.1"], T + 12_000_000_000))
        .unwrap();
    task.pull_and_advance().await;
    let alert = take_alert(&mut alert_rx);
    assert_eq!(field_str(&alert, "__wfu_entity_id"), "10.0.0.1");
    assert!(
        alert_rx.try_recv().is_err(),
        "square hop == fixed: single window"
    );
}

#[tokio::test]
async fn hop_rule_task_multiple_keys_close_independently() {
    init_tracing();
    let (mut task, mut alert_rx, win, _notify, _conv_rx) = make_hop_task(10, 2, false);

    // 两个 key 同时到达 → 每 key 5 窗口。
    win.append(driver_batch(
        &["10.0.0.1", "10.0.0.2", "10.0.0.1", "10.0.0.2"],
        T,
    ))
    .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "no expiry yet");

    // t=T+3s：每 key 恰 1 个窗口收口 → 2 条。
    win.append(driver_batch(&["10.0.0.1"], T + 3_000_000_000))
        .unwrap();
    task.pull_and_advance().await;
    let mut ids: Vec<String> = drain_alerts(&mut alert_rx)
        .iter()
        .map(|r| field_str(r, "__wfu_entity_id"))
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["10.0.0.1".to_string(), "10.0.0.2".to_string()],
        "每 key 每边界恰 1 个窗口收口"
    );
}

#[tokio::test]
async fn hop_rule_task_conv_sink_routes_unbounded_scan_closes() {
    init_tracing();
    let (mut task, mut alert_rx, win, _notify, mut conv_rx) = make_hop_task(10, 2, true);

    // t=T：无窗口到期 → 批次以空 closes + watermark=T 送达 conv 阶段。
    win.append(driver_batch(&["10.0.0.1", "10.0.0.1"], T))
        .unwrap();
    task.pull_and_advance().await;
    let batch = conv_rx.try_recv().expect("conv close batch sent per batch");
    assert!(batch.closes.is_empty(), "no expiry yet");
    assert_eq!(batch.watermark, T);
    assert!(!batch.drained);
    assert_eq!(batch.barrier_index, 0);
    // 路由路径不做 inline 输出。
    assert!(
        alert_rx.try_recv().is_err(),
        "routed closes skip inline emit"
    );

    // t=T+3s：k=849999996 到期 → 1 条 close 路由到 conv 阶段，barrier=T+3s。
    win.append(driver_batch(&["10.0.0.1"], T + 3_000_000_000))
        .unwrap();
    task.pull_and_advance().await;
    let batch = conv_rx.try_recv().expect("hop close routed");
    assert_eq!(batch.closes.len(), 1, "exactly one window expired");
    assert_eq!(batch.watermark, T + 3_000_000_000, "barrier = event time");
    assert!(!batch.drained);
    assert!(
        alert_rx.try_recv().is_err(),
        "routed closes skip inline emit"
    );

    // t=T+6s：k=849999997（上界 T+4s）与 849999998（上界 T+6s 恰等于 wm）到期
    // → 2 条 close 路由到 conv。
    win.append(driver_batch(&["10.0.0.1"], T + 6_000_000_000))
        .unwrap();
    task.pull_and_advance().await;
    let batch = conv_rx.try_recv().expect("hop close routed at boundary");
    assert_eq!(
        batch.closes.len(),
        2,
        "two windows expired at/under boundary"
    );
    assert_eq!(batch.watermark, T + 6_000_000_000, "barrier = event time");
    assert!(!batch.drained);

    // flush → drained 批次收口剩余窗口。2026-08-23 close_all 对齐 oracle/Flink：
    // 尾部未完整窗口（w_end > 最终事件时间）不发射 → drained closes 为空。
    task.flush().await;
    let drained = conv_rx.try_recv().expect("drained batch on flush");
    assert!(drained.drained, "flush marks the conv barrier drained");
    assert!(
        drained.closes.is_empty(),
        "尾部未完整窗口不在 flush 收口发射（q5 语义）"
    );
}
