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
            trigger_event_needed: false,
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
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_q8_task();

    // person 5 注册（T，10s 桶界上）→ 挂起（expiry = T+10s），watermark = T
    q8_person_window(&router)
        .append(person_batch(&[(5, T)]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "未到期 — 不输出");

    // auction seller=5 在桶内（T+5s）入右窗
    q8_auction_window(&router)
        .append(q8_auction_batch(&[(5, T + 5_000_000_000)]))
        .unwrap();

    // 第二个 person（T+11s，下个桶）推进 watermark ≥ T+10s → person 5 到期
    q8_person_window(&router)
        .append(person_batch(&[(6, T + 11_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "桶内 seller==5 的 auction → person 5 输出"
    );
}

/// 恰在桶边界（T+10s）的 auction → 上开排除（归下桶，权威 TUMBLE [B, B+10s)）。
#[tokio::test]
async fn deferred_q8_boundary_auction_excluded() {
    super::tests::init_tracing();
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
    super::tests::init_tracing();
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

// ---------------------------------------------------------------------------
// 真实 wfl 编译集成测试：q9.wfl（文件内容）→ parse/compile（checker+compiler）
// → RulePlan → rule_task 执行。覆盖「编译层 → 执行层」全链路——现有 e2e 均为
// 手写 plan，若编译产物与手写 plan 有差异（emit_at/within/reduce 形态），
// 手写 plan 测不出来（Q9 引擎 0 输出的排查盲区）。
// ---------------------------------------------------------------------------

/// 与 `wf-examples/performance/nexmark_pk/models/queries/q9.wfl` 同步的规则源码。
const Q9_WFL: &str = r#"
rule q9_winning_bid {
    events { a : auction_events }
    on each a -> score(30.0)
    join bid_events reduce maxrow(price) tie(dateTime asc)
        within [a.dateTime, a.expires]
        on a.id == bid_events.auction as winner
        emit at a.expires
    entity(digit, a.id)
    yield nexmark_alerts (id = a.id, alert_type = "q9_win", detail = fmt("winner {}", winner.bidder), request_count = 1)
}
"#;

fn nexmark_schemas() -> Vec<wf_lang::WindowSchema> {
    use wf_lang::{BaseType, FieldDef, FieldType};
    let f = |name: &str, ft: FieldType| FieldDef {
        name: name.to_string(),
        field_type: ft,
    };
    let d = || FieldType::Base(BaseType::Digit);
    let t = || FieldType::Base(BaseType::Time);
    let c = || FieldType::Base(BaseType::Chars);
    vec![
        wf_lang::WindowSchema {
            name: "auction_events".to_string(),
            streams: vec!["auction".to_string()],
            time_field: Some("dateTime".to_string()),
            over: std::time::Duration::from_secs(600),
            fields: vec![
                f("id", d()),
                f("seller", d()),
                f("dateTime", t()),
                f("expires", t()),
            ],
        },
        wf_lang::WindowSchema {
            name: "bid_events".to_string(),
            streams: vec!["bid".to_string()],
            time_field: Some("dateTime".to_string()),
            over: std::time::Duration::from_secs(600),
            fields: vec![
                f("auction", d()),
                f("bidder", d()),
                f("price", d()),
                f("dateTime", t()),
            ],
        },
        wf_lang::WindowSchema {
            name: "nexmark_alerts".to_string(),
            streams: vec![],
            time_field: None,
            over: std::time::Duration::ZERO,
            fields: vec![
                f("id", d()),
                f("alert_type", c()),
                f("detail", c()),
                f("request_count", d()),
            ],
        },
    ]
}

/// 窗口 arrow schema：时间列 = dateTime（真实 nexmark.wfs auction/bid 的 time 字段）。
fn q9c_auction_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("seller", DataType::Int64, true),
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
    ]))
}

fn q9c_bid_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

fn q9c_window_def(name: &str, schema: &Arc<Schema>) -> WindowDef {
    let mut cfg = super::tests::test_window_config(usize::MAX);
    cfg.name = name.to_string();
    WindowDef {
        params: WindowParams {
            name: name.to_string(),
            schema: schema.clone(),
            time_col_index: Some(schema.index_of("dateTime").unwrap()),
            over: std::time::Duration::from_secs(600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec![name.to_string()],
        config: cfg,
    }
}

fn q9c_auction_batch(rows: &[(i64, i64, i64, i64)]) -> RecordBatch {
    // (id, seller, dateTime, expires)
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(q9c_auction_schema(), cols).unwrap()
}

fn q9c_bid_batch(rows: &[(i64, i64, i64, i64)]) -> RecordBatch {
    // (auction, bidder, price, dateTime)
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
    ];
    RecordBatch::try_new(q9c_bid_schema(), cols).unwrap()
}

/// 真实 q9.wfl 编译 → rule_task 执行：挂起 → watermark 过 expiry → 输出胜者。
#[tokio::test]
async fn deferred_q9_real_wfl_compiled_plan_runs() {
    super::tests::init_tracing();
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
    };
    let (mut task, _cancel, _interval) = rule_task::RuleTask::new(config);
    let mut alert_rx = alert_rx;

    // 3) 事件：auction5(T, expires=T+60s) → bid 100@T+10s / 200@T+20s
    //    → auction6(T+61s) 推 watermark 过 T+60s → auction5 到期输出胜者
    router
        .registry()
        .get_window("auction_events")
        .unwrap()
        .append(q9c_auction_batch(&[(5, 42, T, T + 60_000_000_000)]))
        .unwrap();
    router
        .registry()
        .get_window("bid_events")
        .unwrap()
        .append(q9c_bid_batch(&[
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
        .append(q9c_auction_batch(&[(
            6,
            43,
            T + 61_000_000_000,
            T + 121_000_000_000,
        )]))
        .unwrap();
    task.pull_and_advance().await;

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "编译产物的 deferred join 必须输出 auction 5 的胜者"
    );
    assert_eq!(
        super::tests::field_str(&alert, "detail"),
        "winner 2",
        "maxrow(price) 胜者 = price 200（bidder=2），label 注入 detail"
    );
}
