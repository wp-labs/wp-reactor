//! P3 deferred join（`emit at`）端到端测试（wf-runtime）：Q9 形状——auction 驱动流
//! 挂起 → bid 注入右窗 → 事件时间 watermark 推进到 expiry → 到期评估输出胜者；
//! 无 bid 不输出；EOS flush 触发剩余挂起实例。

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use wf_engine::match_engine::RuleExecutor;
use wf_engine::window::{Router, Window, WindowDef, WindowParams, WindowRegistry};
use wf_lang::ast::{
    Bound, BoundVal, Expr, FieldRef, JoinMode, PathSegment, ReduceMeasure, TieSpec, WithinSpec,
};
use wf_lang::plan::{
    BindPlan, EachPlan, EntityPlan, JoinCondPlan, JoinPlan, MatchPlan, RulePlan, ScorePlan,
    YieldField, YieldPlan,
};

use super::tests::{empty_tracked_bind_fields, empty_tracked_plain_fields, make_test_fanout};
use crate::engine_task::{rule_task, task_types};

const T: i64 = 1_700_000_000_000_000_000;

fn auction_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new(
            "expires",
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

fn bid_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
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

fn window_def(name: &str, schema: &Arc<Schema>) -> WindowDef {
    let mut cfg = super::tests::test_window_config(usize::MAX);
    cfg.name = name.to_string();
    WindowDef {
        params: WindowParams {
            name: name.to_string(),
            schema: schema.clone(),
            time_col_index: Some(schema.index_of("event_time").unwrap()),
            over: std::time::Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec![name.to_string()],
        config: cfg,
    }
}

fn auction_batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
    // (id, dateTime, expires)，event_time = dateTime
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(auction_schema(), cols).unwrap()
}

fn bid_batch(rows: &[(i64, i64, i64, i64)]) -> RecordBatch {
    // (auction, bidder, price, dateTime)，event_time = dateTime
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(bid_schema(), cols).unwrap()
}

/// Q9 形状 deferred 规则任务：
/// ```wfl
/// events { a : auction_events }
/// on each a
/// join bid_events reduce maxrow(price) tie(dateTime asc)
///   within [a.dateTime, a.expires] on a.id == bid_events.auction as winner
///   emit at a.expires
/// entity(digit, a.id)
/// yield alerts (id = a.id, winner_bidder = winner.bidder)
/// ```
fn make_deferred_join_task() -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let driver = "auction_events";
    let registry = WindowRegistry::build(vec![
        window_def(driver, &auction_schema()),
        window_def("bid_events", &bid_schema()),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let source_window = router.registry().get_window(driver).unwrap();
    let source_notify = router.registry().get_notifier(driver).unwrap();

    let within = WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                "a".into(),
                "dateTime".into(),
            ))),
        },
        hi: Bound {
            open: false,
            val: BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                "a".into(),
                "expires".into(),
            ))),
        },
    };
    let rule_plan = RulePlan {
        conv_window: None,
        name: "q9_deferred_e2e".into(),
        binds: vec![BindPlan {
            alias: "a".into(),
            window: driver.into(),
            filter: None,
        }],
        lets: Vec::new(),
        match_plan: MatchPlan {
            keys: vec![],
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
        },
        each_plan: Some(EachPlan {
            alias: "a".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![JoinPlan {
            right_window: "bid_events".to_string(),
            mode: JoinMode::Inner,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("a".into(), "id".into()),
                right: FieldRef::Qualified("bid_events".into(), "auction".into()),
            }],
            within: Some(within),
            reduce: Some(wf_lang::ast::ReduceClause {
                measure: ReduceMeasure::Maxrow {
                    field: FieldRef::Simple("price".into()),
                    tie: Some(TieSpec {
                        field: FieldRef::Simple("dateTime".into()),
                        desc: false,
                    }),
                },
                label: Some("winner".into()),
            }),
            emit_at: Some(Expr::Field(FieldRef::Qualified(
                "a".into(),
                "expires".into(),
            ))),
        }],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Simple("id".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(FieldRef::Simple("id".into())),
                },
                YieldField {
                    name: "winner_bidder".into(),
                    value: Expr::Field(FieldRef::Path {
                        alias: "winner".into(),
                        segments: vec![PathSegment::Field("bidder".into())],
                    }),
                },
            ],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(30.0),
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
        // deferred（emit at）规则是 each 形态：无状态机（rule_task 的 each 分支挂起）
        machine: None,
        each_alias: Some("a".into()),
        each_time_field: Some("event_time".into()),
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
    };
    let (task, _cancel, _interval) = rule_task::RuleTask::new(config);
    (task, alert_rx, router)
}

fn auction_window(router: &Router) -> Arc<Window> {
    router.registry().get_window("auction_events").unwrap()
}

fn bid_window(router: &Router) -> Arc<Window> {
    router.registry().get_window("bid_events").unwrap()
}

#[tokio::test]
async fn deferred_q9_hit_outputs_winner_when_watermark_passes_expiry() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // bid 先到（auction=5，price 100/200，dateTime T+10s / T+20s）
    bid_window(&router)
        .append(bid_batch(&[
            (5, 1, 100, T + 10_000_000_000),
            (5, 2, 200, T + 20_000_000_000),
        ]))
        .unwrap();
    // auction 到达：挂起（expiry = T+60s），watermark = T
    auction_window(&router)
        .append(auction_batch(&[(5, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    // 未到期 → 无输出
    assert!(alert_rx.try_recv().is_err(), "not due yet — no output");

    // 第二个 auction（ts=T+61s）推进 watermark ≥ expiry → 第一个到期输出胜者
    auction_window(&router)
        .append(auction_batch(&[(
            6,
            T + 61_000_000_000,
            T + 121_000_000_000,
        )]))
        .unwrap();
    task.pull_and_advance().await;

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_origin"),
        "deferred",
        "deferred join output must carry origin=deferred"
    );
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "winner for auction 5"
    );
    // 胜者 = price 200 的 bid（bidder=2）
    assert_eq!(
        super::tests::field_str(&alert, "winner_bidder"),
        "2",
        "maxrow(price) must pick the highest bid"
    );
}

#[tokio::test]
async fn deferred_q9_no_bid_no_output() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // auction=7 无任何 bid
    auction_window(&router)
        .append(auction_batch(&[(7, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // 推进 watermark 超过 expiry（flush 到期）
    task.flush().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "no bid in [dateTime, expires] → no deferred output"
    );
}

#[tokio::test]
async fn deferred_q9_flush_triggers_remaining_pending() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    bid_window(&router)
        .append(bid_batch(&[(8, 3, 50, T + 10_000_000_000)]))
        .unwrap();
    auction_window(&router)
        .append(auction_batch(&[(8, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // 无后续事件推进 watermark → EOS flush 触发剩余挂起实例
    task.flush().await;

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(super::tests::field_str(&alert, "__wfu_entity_id"), "8");
    assert_eq!(super::tests::field_str(&alert, "winner_bidder"), "3");
}
