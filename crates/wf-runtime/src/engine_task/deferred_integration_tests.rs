//! P3 deferred join（`emit at`）端到端测试（wf-runtime）：Q9 形状——auction 驱动流
//! 挂起 → bid 注入右窗 → 事件时间 watermark 推进到 expiry → 到期评估输出胜者；
//! 无 bid 不输出；EOS flush 触发剩余挂起实例。
use std::sync::Arc;

use std::collections::HashSet;
use std::collections::HashMap;

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use wf_engine::match_engine::{RuleExecutor, Value};
use wf_engine::window::{
    ProviderWindow, Router, Window, WindowDef, WindowParams, WindowRegistry,
};
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
    window_def_with_over(name, schema, std::time::Duration::from_secs(3600))
}

/// `window_def` 的变体：可指定 `over`（时间驱逐窗口）。deferred join 目标窗
/// 用小 over 可复现生产 q4a/q9（`bid_events over=30m`）的「评估时右行已越过
/// 时间驱逐线」场景——D4 保留 pin 必须保住它们。
fn window_def_with_over(
    name: &str,
    schema: &Arc<Schema>,
    over: std::time::Duration,
) -> WindowDef {
    let mut cfg = super::tests::test_window_config(usize::MAX);
    cfg.name = name.to_string();
    WindowDef {
        params: WindowParams {
            name: name.to_string(),
            schema: schema.clone(),
            time_col_index: Some(schema.index_of("event_time").unwrap()),
            over,
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
    make_deferred_join_task_with_over(std::time::Duration::from_secs(3600))
}

/// `make_deferred_join_task` 的变体：bid 目标窗用小 `over`（复现 q4a/q9 生产
/// `bid_events over=30m` 的时间驱逐场景）。
fn make_deferred_join_task_with_over(bid_over: std::time::Duration) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let driver = "auction_events";
    let registry = WindowRegistry::build(vec![
        window_def(driver, &auction_schema()),
        window_def_with_over("bid_events", &bid_schema(), bid_over),
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

/// 时间驱逐 × D4 保留 pin 闭环（30M q4 over=30m 欠发的机制验证）：
///
/// auction 时长 > bid 窗 `over` 时，到期评估需要的右行早已越过时间驱逐线
/// （生产：`bid_events over=30m` + 驱逐 tick）。deferred 规则发布保留 pin
/// （= 存活挂起实例的 min(lo_ns)），时间驱逐与内存驱逐都不得删 `[lo, expiry]`
/// 内的行（2026-08-25 D4 闭环：`evict_expired_impl` 尊重 pin）。
///
/// 本用例验证**正路径**：pin 保住越过驱逐线的右行 → 评估命中输出。
/// （无 pin 侧由 wf-engine `evict_expired_respects_retention_pin` 覆盖。）
#[tokio::test]
async fn deferred_q9_time_eviction_pin_keeps_in_range_bids() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) =
        make_deferred_join_task_with_over(std::time::Duration::from_secs(10));

    // bid 先到（auction=5，price 100 @ T+5s）——落在 auction 5 的 [lo=T, expiry=T+30s] 内
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(5, 1, 100, T + 5_000_000_000)]))
        .unwrap();
    // auction=5：时长 30s > over 10s → 到期评估时右行已越过驱逐线
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 30_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    // 挂起中（expiry=T+30s，watermark=T）；pin 已发布（lo_min=T）

    // 事件时间推进到 T+25s：cutoff = T+15s > bid @ T+5s → 时间驱逐线已覆盖右行。
    // pin（挂起实例 lo=T）必须挡住驱逐：batch(max=T+5s) ≥ pin(=T) → 保留。
    bid_window(&router).evict_expired(T + 25_000_000_000);
    assert_eq!(
        bid_window(&router).total_rows(),
        1,
        "pin 必须保住挂起实例需要的右行（时间驱逐不得删）"
    );

    // 驱动 watermark + 目标窗都追平 expiry：auction=6 @ T+31s、bid=6 @ T+31s
    auction_window(&router)
        .append_with_watermark(auction_batch(&[
            (6, T + 31_000_000_000, T + 61_000_000_000),
        ]))
        .unwrap();
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(6, 3, 300, T + 31_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // auction 5 到期评估：右行在 → 命中输出
    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "pin 保住右行 → 到期评估命中"
    );
}

/// 内存机制回归：**lo_min 缓存必须随 pending drain 推进**（2026-08-25 修复）。
///
/// 旧实现把 lo_min 缓存为**历史最小** lo（插入时 min，drain 不更新）——任何
/// shard 只要 pending 非空，pin 就发布历史第一个实例的 lo（≈流起点）→ 时间
/// 驱逐全被挡（30M q4 over=30m：pin_floor=起点+1ms、evict=0、RSS 9.2GB = 整窗
/// 保留，探针实锤）。修复：scan drain 到期前缀后标 dirty，publish 重算当前
/// pending 的 min lo → pin 随评估前沿推进 → over 窗口外的旧行可驱逐。
#[tokio::test]
async fn deferred_q9_pin_floor_advances_with_pending_drain() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) =
        make_deferred_join_task_with_over(std::time::Duration::from_secs(10));

    // 三个短时长实例：1/2/3 号 auction，expiry = lo + 1s（随事件流推进评估）
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 1_000_000_000)]))
        .unwrap();
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(5, 1, 100, T + 500_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    auction_window(&router)
        .append_with_watermark(auction_batch(&[
            (6, T + 2_000_000_000, T + 3_000_000_000),
        ]))
        .unwrap();
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(6, 3, 300, T + 2_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(super::tests::field_str(&alert, "__wfu_entity_id"), "5");

    auction_window(&router)
        .append_with_watermark(auction_batch(&[
            (7, T + 4_000_000_000, T + 5_000_000_000),
        ]))
        .unwrap();
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(7, 7, 7, T + 4_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(super::tests::field_str(&alert, "__wfu_entity_id"), "6");

    // 三个实例都已评估（pending 只剩 7 号，min lo = T+4s）。
    // 修复前：lo_min 缓存 = 历史最小 = T（1 号实例的 lo）→ pin = T。
    // 修复后：drain 标 dirty → publish 重算 → pin = T+4s。
    let pin_floor = bid_window(&router).retention_floor_ns();
    assert!(
        pin_floor >= T + 4_000_000_000,
        "pin 必须随 pending drain 推进（当前 pending min lo = T+4s），实际 pin_floor={pin_floor}"
    );

    // 时间驱逐：now=T+20s、over=10s → cutoff=T+10s。三条 bid 都在 cutoff 前。
    // 修复前 pin=T → 全被 pin 住 → 不驱逐（BUG：整窗保留）。
    // 修复后 pin=T+4s → T+0.5s / T+2s 两条 < pin → 驱逐；T+4s 那条 = pin
    //（auction 7 的 lo，挂起实例区间起点）→ 合法保留（正确性）。
    bid_window(&router).evict_expired(T + 20_000_000_000);
    assert_eq!(
        bid_window(&router).total_rows(),
        1,
        "pin 推进后 over 窗口外的旧右行必须可驱逐（仅剩当前挂起实例区间内的行）"
    );
}

/// flush 收口**不受健全前沿限制**：目标窗一直不提交（frontier 卡 i64::MIN）时，
/// 运行期 gate 挂起全部实例（不假 miss），flush（gate=false）仍按最终水位收口
/// 评估——否则尾部/静态目标场景会全部丢到 flush 之外。
#[tokio::test]
async fn deferred_q9_flush_unblocks_evaluation_when_frontier_never_advances() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();
    let src_a: Arc<str> = Arc::from("ingress#1");
    let bw = bid_window(&router);
    let schema = bw.schema().clone();

    // auction 5 挂起（expiry=T+30s）；目标窗**没有任何提交**（per-source 空 →
    // frontier 回退 max_event_time = i64::MIN → 运行期 gate 挂起）。
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 30_000_000_000)]))
        .unwrap();
    // 驱动 wm 追平 expiry（auction 6 @ T+31s）——即使驱动已过 expiry，
    // frontier=i64::MIN → gate=i64::MIN → 不评估（不假 miss）。
    auction_window(&router)
        .append_with_watermark(auction_batch(&[
            (6, T + 31_000_000_000, T + 61_000_000_000),
        ]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "目标无提交 → 运行期保持挂起（不假 miss）"
    );

    // 目标窗随后提交右行（跨源延迟送达）
    bw.append_with_watermark_sized_from(
        bid_batch(&[(5, 1, 100, T + 5_000_000_000)]),
        0,
        None,
        Arc::clone(&src_a),
    )
    .unwrap();

    // flush 收口：gate=false → 不受 frontier 限制 → 评估命中补输出
    task.flush().await;
    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "flush 收口必须绕过 frontier gate（右行已提交 → 命中）"
    );
}

/// 跨源提交乱序 × deferred 评估 gate（30M q4 over=30m -860 的机制回归）：
///
/// ingress `instances=8` + parse 并行派发下，窗口 actor 只保证 **source 内**
/// seq 有序，跨 source 提交顺序自由——全局 `max_event_time` 会被任一 source 的
/// 远未来 batch 提前推高。修复前 gate 用它 → 右行未落地就评估 → 假 miss →
/// 行随后被 over 时间驱逐 → flush 重试无法恢复（-860）。修复后 gate 用
/// `min(驱动 wm, 健全提交前沿 = 各源已提交 max 的 min)` → 右行真正落地才评估。
#[tokio::test]
async fn deferred_q9_cross_source_reorder_holds_evaluation_until_committed() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();
    let src_a: Arc<str> = Arc::from("ingress#1");
    let src_b: Arc<str> = Arc::from("ingress#2");
    let bw = bid_window(&router);
    let schema = bw.schema().clone();

    // auction 5 挂起（lo=T, expiry=T+30s）
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 30_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    // 跨源乱序：source A 先提交远未来 bid（全局 max → T+100s），
    // source B 只提交到 T+2s（无关 auction 98）→ 健全前沿 = T+2s。
    bw.append_with_watermark_sized_from(
        bid_batch(&[(99, 9, 9, T + 100_000_000_000)]),
        0,
        None,
        Arc::clone(&src_a),
    )
    .unwrap();
    bw.append_with_watermark_sized_from(
        bid_batch(&[(98, 8, 8, T + 2_000_000_000)]),
        0,
        None,
        Arc::clone(&src_b),
    )
    .unwrap();

    // 驱动 wm 追平 expiry（auction 6 @ T+31s）
    auction_window(&router)
        .append_with_watermark(auction_batch(&[
            (6, T + 31_000_000_000, T + 61_000_000_000),
        ]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "修复前：eff_wm=全局 max=T+100s ≥ expiry → 提前评估假 miss；\
         修复后：eff_wm=min(驱动 T+31s, 前沿 T+2s)=T+2s < expiry → 保持挂起"
    );

    // source B 提交 auction 5 的右行（@T+5s）→ 前沿 = T+5s < expiry → 仍挂起
    bw.append_with_watermark_sized_from(
        bid_batch(&[(5, 1, 100, T + 5_000_000_000)]),
        0,
        None,
        Arc::clone(&src_b),
    )
    .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "前沿（T+5s）未过 expiry（T+30s）前保持挂起"
    );

    // source B 提交到 T+40s → 前沿 = T+40s ≥ expiry；auction 7 @ T+45s 触发
    // 下一次扫描：eff_wm = min(驱动 T+45s, 前沿 T+40s) = T+40s ≥ T+30s →
    // 评估命中（右行已提交）
    bw.append_with_watermark_sized_from(
        bid_batch(&[(97, 7, 7, T + 40_000_000_000)]),
        0,
        None,
        Arc::clone(&src_b),
    )
    .unwrap();
    auction_window(&router)
        .append_with_watermark(auction_batch(&[
            (7, T + 45_000_000_000, T + 75_000_000_000),
        ]))
        .unwrap();
    task.pull_and_advance().await;
    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "右行提交后评估命中"
    );
    assert_eq!(
        super::tests::field_str(&alert, "winner_bidder"),
        "1",
        "maxrow(price) 胜者 = bidder 1"
    );
}

#[tokio::test]
async fn deferred_q9_hit_outputs_winner_when_watermark_passes_expiry() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // bid 先到（auction=5，price 100/200，dateTime T+10s / T+20s）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[
            (5, 1, 100, T + 10_000_000_000),
            (5, 2, 200, T + 20_000_000_000),
        ]))
        .unwrap();
    // auction 到达：挂起（expiry = T+60s），watermark = T
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    // 未到期 → 无输出
    assert!(alert_rx.try_recv().is_err(), "not due yet — no output");

    // 第二个 auction（ts=T+61s）推进 watermark ≥ expiry → 第一个到期输出胜者
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            6,
            T + 61_000_000_000,
            T + 121_000_000_000,
        )]))
        .unwrap();
    // join 目标窗口同步追平（2026-08-25 评估 gate：目标 max_event_time ≥ expiry
    // 才评估——生产流中 bid/auction 交错 append，目标天然追平；单测需显式补）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(6, 3, 300, T + 61_000_000_000)]))
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

/// 乱序驱动（2026-08-25 q4 100M 回归）：auction 到达顺序与 expires 顺序
/// **相反**（二分插入保持 pending 按 expiry 有序）——到期扫描只取前缀，
/// 输出顺序仍按到期时间正确。回归防网：pending 改有序前缀前，全量扫不
/// 依赖顺序（等价）；改后若插入有序性被破坏会漏输出。
#[tokio::test]
async fn deferred_q9_out_of_order_driver_emits_by_expiry_order() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // 3 个 auction 乱序到达（expires 顺序：auction 11 最先到期）：
    //   auction=11: dateTime=T,     expires=T+30s
    //   auction=13: dateTime=T+2s,  expires=T+90s
    //   auction=12: dateTime=T+1s,  expires=T+60s
    // 每个 auction 各一个 bid（在各自区间内），保证到期评估命中。
    bid_window(&router)
        .append_with_watermark(bid_batch(&[
            (11, 1, 100, T + 5_000_000_000),
            (12, 2, 200, T + 10_000_000_000),
            (13, 3, 300, T + 15_000_000_000),
        ]))
        .unwrap();
    auction_window(&router)
        .append_with_watermark(auction_batch(&[
            (11, T, T + 30_000_000_000),
            (13, T + 2_000_000_000, T + 90_000_000_000),
            (12, T + 1_000_000_000, T + 60_000_000_000),
        ]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(alert_rx.try_recv().is_err(), "全部未到期，无输出");

    // 推进 watermark 到 T+31s：只有 auction 11 到期 → 输出 1 条（id=11）
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(14, T + 31_000_000_000, T + 91_000_000_000)]))
        .unwrap();
    // 目标窗口追平（bid 14 随 auction 14 到达，max_event_time 推过 T+30s）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(14, 4, 400, T + 31_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    task.flush().await;
    let a1 = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&a1, "__wfu_entity_id"),
        "11",
        "最先到期（T+30s）的 auction 先输出"
    );

    // 推进到 T+61s：auction 12 到期（T+60s），auction 13 未到期（T+90s）
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(15, T + 61_000_000_000, T + 121_000_000_000)]))
        .unwrap();
    // 目标窗口追平（bid 15 随 auction 15 到达，max_event_time 推过 T+60s）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(15, 5, 500, T + 61_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    task.flush().await;
    let a2 = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&a2, "__wfu_entity_id"),
        "12",
        "第二个到期（T+60s）的 auction 输出"
    );
    // 此时已无其它到期（auction 13 expires=T+90s > T+61s）
    assert!(alert_rx.try_recv().is_err(), "auction 13 未到期，不应输出");
}

/// EOS/关闭 flush 只收口**已到期**实例：尾部 expiry > 最终事件时间 watermark 的
/// 实例窗口未完成（事件时间域），不输出——与 oracle 一致（oracle/mod.rs EOS
/// 水位注释：按 slice 边界强扫会多出尾部桶，Q8 实证 82446 → 83274 +828）。
#[tokio::test]
async fn deferred_q9_flush_does_not_emit_unexpired_tail() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // auction=8（T，expires=T+60s）窗口内有 bid，但无后续事件推进 watermark
    // → 最终 watermark = T < expiry：尾部未到期，flush 不输出
    bid_window(&router)
        .append(bid_batch(&[(8, 3, 50, T + 10_000_000_000)]))
        .unwrap();
    auction_window(&router)
        .append(auction_batch(&[(8, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    task.flush().await;

    assert!(
        alert_rx.try_recv().is_err(),
        "尾部未到期实例（expiry > 最终 watermark）flush 不输出"
    );
}

/// 已到期实例（expiry ≤ 最终 watermark）由 EOS 重试补出：窗口内 bid 存在、
/// 但评估发生在 watermark 过 expiry 之后（missed → EOS 重试命中）。
#[tokio::test]
async fn deferred_q9_eos_retry_recovers_due_instance() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // auction=8 挂起（T，expires=T+60s）；此时 bid 窗口为空 → 到期评估 miss
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(8, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    // auction=9（T+61s）推进 watermark 过 expiry → auction 8 到期，bid 为空 → miss
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            9,
            T + 61_000_000_000,
            T + 121_000_000_000,
        )]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "bid 窗口为空 → 到期 miss，等 EOS 重试"
    );

    // bid 迟到进入右窗（append 滞后）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(8, 3, 50, T + 10_000_000_000)]))
        .unwrap();
    task.flush().await;

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(super::tests::field_str(&alert, "__wfu_entity_id"), "8");
    assert_eq!(super::tests::field_str(&alert, "winner_bidder"), "3");
}

/// 2026-08-25 q4 100M 欠发根治：运行期评估 gate——join 目标窗口 append 位置未
/// 过 expiry 时实例**保持挂起**（不评估、不 miss），目标追平后随下一次扫描命中
/// 输出（无需 flush/EOS 重试）。修复前目标未追平就评估 → 运行期 miss → missed
/// 积压（RSS 随总量增长）+ 100M 下 EOS 重试时早段右行已被 over 驱逐 → 欠发
/// ~63%（oracle 5.58M vs 2.07M）。
#[tokio::test]
async fn deferred_q9_target_lag_holds_evaluation_until_target_catches_up() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_deferred_join_task();

    // auction 5（T，expires=T+60s）挂起；bid 5 在窗内（T+10s）；auction 6
    // （T+61s）推 watermark 过 expiry——但 bid 窗口 max_event_time 还停在
    // T+10s（目标 append 滞后）→ 评估 gate 把实例保持挂起（旧行为：立即
    // 评估 → 窗口缺后续 bid → miss 进 missed）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(5, 1, 100, T + 10_000_000_000)]))
        .unwrap();
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(5, T, T + 60_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(
            6,
            T + 61_000_000_000,
            T + 121_000_000_000,
        )]))
        .unwrap();
    task.pull_and_advance().await;
    assert!(
        alert_rx.try_recv().is_err(),
        "目标窗口未追平（max_event_time=T+10s < expiry T+60s）→ 实例保持挂起，不评估"
    );

    // 目标窗口追平：bid 6 随 auction 6 到达 → bid 窗口 max_event_time 推过
    // T+60s；auction 7 随后的驱动事件触发批次尾扫描 → auction 5 命中输出
    // （无需 flush/EOS 重试）
    bid_window(&router)
        .append_with_watermark(bid_batch(&[(6, 2, 200, T + 61_000_000_000)]))
        .unwrap();
    auction_window(&router)
        .append_with_watermark(auction_batch(&[(7, T + 62_000_000_000, T + 122_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;
    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(super::tests::field_str(&alert, "__wfu_entity_id"), "5");

    // 运行期已命中（missed 为空）→ flush 收口不重复输出
    task.flush().await;
    assert!(
        drain_alert_entity_ids(&mut alert_rx).is_empty(),
        "运行期已命中，flush 不重复输出"
    );
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

/// EOS 重试（2026-08-23 q8 修复）：到期评估时 join 目标窗口 append 滞后
/// （auction 尚未 ingest）→ 实例进 `missed`；EOS flush 时目标完整，重试命中
/// 补输出——q8 引擎 33k vs oracle 82k 的修复路径。
#[tokio::test]
async fn deferred_q8_eos_retry_recovers_miss_from_late_join_target() {
    super::tests::init_tracing();
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

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "EOS 重试必须补出桶内 seller==5 的 person"
    );
}

/// EOS 重试对真 miss 不误报：auction 窗口补齐后仍无匹配 → 不输出。
#[tokio::test]
async fn deferred_q8_eos_retry_true_miss_stays_silent() {
    super::tests::init_tracing();
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
    super::tests::init_tracing();
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

/// 排空 alert 通道并按 `__wfu_entity_id` 收集（精确计数断言：不重不丢）。
fn drain_alert_entity_ids(rx: &mut mpsc::Receiver<crate::alert_task::AlertBatch>) -> Vec<String> {
    use crate::alert_task::AlertBatch;
    let mut ids = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        match batch {
            AlertBatch::Rows(rows) => {
                for r in rows.iter() {
                    ids.push(super::tests::field_str(r, "__wfu_entity_id"));
                }
            }
            AlertBatch::Columns(cols) => {
                for r in cols.iter_data_records().flatten() {
                    ids.push(super::tests::field_str(&r, "__wfu_entity_id"));
                }
            }
        }
    }
    ids
}

/// watermark 扫描**命中**（非 miss）→ flush 不得重复输出：missed 只收集 miss
/// 实例，已命中实例不会进入重试路径。
#[tokio::test]
async fn deferred_q8_watermark_hit_not_duplicated_by_flush() {
    super::tests::init_tracing();
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
    super::tests::init_tracing();
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
    super::tests::init_tracing();
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
    super::tests::init_tracing();
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
    super::tests::init_tracing();
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

/// 与 `wf-examples/performance/nexmark_pk/models/queries/q13.wfl` 同步的双规则链源码
/// （q13a 写中间窗 bid_mod → q13b join side_input 静态表）。
/// 100M 实测 RSS 27GB + `memory_evicted_total=1479`（2026-08-25）——复现测试用。
const Q13_WFL: &str = r#"
rule q13a_bid_mod {
    events { b : bid_events }
    on each b -> score(10.0)
    entity(digit, b.bidder)
    yield bid_mod (
        id = b.bidder,
        bidder = b.bidder,
        auction = b.auction,
        price = b.price,
        dateTime = b.dateTime,
        mod_key = b.auction % 10000
    )
}
rule q13b_side_input_join {
    events { m : bid_mod }
    on each m -> score(10.0)
    join side_input snapshot on m.mod_key == side_input.key
    entity(digit, m.bidder)
    yield nexmark_alerts (
        id = m.bidder,
        alert_type = "q13_sidejoin",
        detail = fmt("{}", side_input.value),
        request_count = 1
    )
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
        // q13 中间窗（q13a yield → q13b bind）
        wf_lang::WindowSchema {
            name: "bid_mod".to_string(),
            streams: vec![],
            time_field: Some("dateTime".to_string()),
            over: std::time::Duration::ZERO,
            fields: vec![
                f("id", d()),
                f("bidder", d()),
                f("auction", d()),
                f("price", d()),
                f("dateTime", t()),
                f("mod_key", d()),
            ],
        },
        // q13 有界侧输入静态表（provider）
        wf_lang::WindowSchema {
            name: "side_input".to_string(),
            streams: vec![],
            time_field: None,
            over: std::time::Duration::ZERO,
            fields: vec![f("key", d()), f("value", c())],
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

/// q13 双规则链测试的窗口定义：可指定字节预算（中间窗用小预算触发驱逐）。
/// 中间窗 `bid_mod` over=0（无时间驱逐，模拟中间窗无 over 配置——只靠内存驱逐）。
fn q13c_window_def(name: &str, schema: &Arc<Schema>, max_bytes: usize) -> WindowDef {
    let mut cfg = super::tests::test_window_config(max_bytes);
    cfg.name = name.to_string();
    let time_idx = if name == "bid_mod" { 4 } else { 3 }; // dateTime 列位置
    WindowDef {
        params: WindowParams {
            name: name.to_string(),
            schema: schema.clone(),
            time_col_index: Some(time_idx),
            over: std::time::Duration::ZERO,
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec![name.to_string()],
        config: cfg,
    }
}

fn q13c_bid_schema() -> Arc<Schema> {
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

fn q13c_bid_mod_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Int64, true),
        Field::new(
            "dateTime",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("mod_key", DataType::Int64, true),
    ]))
}

fn q13c_bid_batch(rows: &[(i64, i64, i64, i64)]) -> RecordBatch {
    // (auction, bidder, price, dateTime)
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(q13c_bid_schema(), cols).unwrap()
}

fn q13c_bid_mod_batch(rows: &[(i64, i64, i64, i64)]) -> RecordBatch {
    // (id, bidder, auction, price, dateTime, mod_key) — mod_key = auction % 10000
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())),
        Arc::new(TimestampNanosecondArray::from(
            rows.iter().map(|r| r.3).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.1 % 10000).collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(q13c_bid_mod_schema(), cols).unwrap()
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

/// q13 双规则链复现（2026-08-25 100M RSS 27GB + memory_evicted_total=1479）：
///
/// 链：q13a（on each b → yield 中间窗 bid_mod）→ q13b（on each m + join
/// side_input 静态表）。复现目标：中间窗积压时（写入快、消费慢）——
/// ① 中间窗的内存驱逐**不得丢未读数据**（消费者 ack floor 保护，输出完整）；
/// ② 已读数据被驱逐回收（内存有界）。
///
/// 生产路径：task_b 通过 `register_progress` 注册 bid_mod 消费者槽（spawn.rs），
/// 测试模拟该配置；负向对照（无槽位）验证引擎对"未注册消费者"中间窗的驱逐行为
/// ——若生产中间窗漏注册，就会丢未读（memory_evicted_total 归因方向）。
#[tokio::test]
async fn q13_dual_chain_intermediate_window_pressure() {
    super::tests::init_tracing();
    let schemas = nexmark_schemas();
    let file = wf_lang::parse_wfl(Q13_WFL).expect("parse q13.wfl");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile q13.wfl");
    assert_eq!(plans.len(), 2, "q13.wfl → 2 个 plan（q13a_bid_mod + q13b_side_input_join）");
    let mut plans = plans.into_iter();
    let mut plan_a = plans.next().unwrap();
    let plan_b = plans.next().unwrap();
    plan_a.name = "q13a_bid_mod".into();

    // 中间窗预算 = 3 个中间 batch（每 bid batch 写 1 个中间 batch）——写入超过
    // 预算触发内存驱逐。probe 用 3 行 batch（与实际驱动批同构）。
    let probe = q13c_bid_mod_batch(&[(1, 1, 100, T), (1, 2, 200, T), (1, 3, 300, T)]);
    let one_batch_bytes = wf_engine::window::content_bytes(&probe);
    let bid_mod_budget = one_batch_bytes * 3;

    let mut registry = WindowRegistry::build(vec![
        q13c_window_def("bid_events", &q13c_bid_schema(), usize::MAX),
        q13c_window_def("bid_mod", &q13c_bid_mod_schema(), bid_mod_budget),
    ])
    .unwrap();
    // side_input 静态表：mod_key 1 → "v1"，2 → "v2"
    let mut pw = ProviderWindow::new(
        "side_input".into(),
        "SELECT * FROM side_input".into(),
        None,
    );
    pw.load(vec![
        {
            let mut m = HashMap::new();
            m.insert("key".to_string(), Value::Number(1.0));
            m.insert("value".to_string(), Value::Str("v1".into()));
            m
        },
        {
            let mut m = HashMap::new();
            m.insert("key".to_string(), Value::Number(2.0));
            m.insert("value".to_string(), Value::Str("v2".into()));
            m
        },
    ]);
    registry
        .register_provider("side_input".to_string(), pw)
        .unwrap();
    let router = Arc::new(Router::new(registry));

    // task_a：q13a（驱动 bid_events → yield 中间窗 bid_mod）
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
    };
    let (mut task_a, _cancel_a, _interval_a) = rule_task::RuleTask::new(config_a);

    // task_b：q13b（驱动 bid_mod + join side_input），注册 bid_mod 消费者槽
    let src_b = router.registry().get_window("bid_mod").unwrap();
    let notify_b = router.registry().get_notifier("bid_mod").unwrap();
    let executor_b = RuleExecutor::new(plan_b);
    let (alert_tx_b, mut alert_rx_b) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
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
        executor: executor_b,
        window_sources: vec![task_types::WindowSource {
            window_name: "bid_mod".into(),
            window: src_b,
            notify: notify_b,
            aliases: vec!["m".into()],
        }],
        sink_fanout: make_test_fanout(alert_tx_b),
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
    let (mut task_b, _cancel_b, _interval_b) = rule_task::RuleTask::new(config_b);

    // 驱动：5 个 bid batch（每批 3 行，auction 恒 1 → mod_key=1 → 命中 side_input）
    let bid_win = router.registry().get_window("bid_events").unwrap();
    for i in 0..5i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[
                (1, i * 3, 100 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 1, 101 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 2, 102 + i * 3, T + i * 1_000_000_000),
            ]))
            .unwrap();
    }

    // task_a 处理全部 5 个 bid batch → 写中间窗 5 个 batch（超 3 预算）
    task_a.pull_and_advance().await;
    let bm = router.registry().get_window("bid_mod").unwrap();
    assert_eq!(
        bm.batch_count(),
        5,
        "task_b 未读前中间窗不得驱逐（超预算保留，宁可内存）"
    );
    assert_eq!(bm.total_rows(), 15, "中间窗全量 15 行");

    // task_b 消费中间窗 + join 输出：全部 15 行必须富化输出（不丢未读）
    task_b.pull_and_advance().await;
    let mut values: Vec<String> = Vec::new();
    while let Ok(batch) = alert_rx_b.try_recv() {
        match batch {
            crate::alert_task::AlertBatch::Rows(rows) => {
                for r in rows.iter() {
                    values.push(super::tests::field_str(&r, "detail"));
                }
            }
            crate::alert_task::AlertBatch::Columns(cols) => {
                for r in cols.iter_data_records().flatten() {
                    values.push(super::tests::field_str(&r, "detail"));
                }
            }
        }
    }
    assert_eq!(
        values.len(),
        15,
        "消费者 ack 保护：中间窗驱逐不得丢未读（输出完整 15/15）"
    );
    assert!(
        values.iter().all(|v| v == "v1"),
        "全部命中 side_input mod_key=1 → 富化 value=v1"
    );

    // task_a 继续写入 → 已读部分（前 3 batch）被驱逐回收，未读（后 2）保留
    for i in 5..8i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[
                (1, i * 3, 100 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 1, 101 + i * 3, T + i * 1_000_000_000),
                (1, i * 3 + 2, 102 + i * 3, T + i * 1_000_000_000),
            ]))
            .unwrap();
    }
    task_a.pull_and_advance().await;
    assert!(
        bm.batch_count() <= 6,
        "已读 batch 被驱逐回收（内存有界），当前 {}",
        bm.batch_count()
    );

    // 剩余 9 行（5-7 批 + 未消费的）也能完整输出
    task_b.pull_and_advance().await;
    let mut more = 0usize;
    while let Ok(batch) = alert_rx_b.try_recv() {
        more += match batch {
            crate::alert_task::AlertBatch::Rows(rows) => rows.len(),
            crate::alert_task::AlertBatch::Columns(cols) => {
                cols.iter_data_records().flatten().count()
            }
        };
    }
    assert_eq!(more, 9, "已读驱逐后剩余未读仍完整输出 9/9");
}

/// q13 双规则链**无消费者槽位**对照：中间窗无 ack 保护时驱逐自由删未读 →
/// 下游输出丢失（引擎依赖消费者注册——生产 `register_progress` 已注册；
/// 此对照证明该依赖是丢数据的守卫，任何漏注册都是正确性事故）。
#[tokio::test]
async fn q13_dual_chain_intermediate_window_unregistered_consumer_loses() {
    super::tests::init_tracing();
    let schemas = nexmark_schemas();
    let file = wf_lang::parse_wfl(Q13_WFL).expect("parse q13.wfl");
    let plans = wf_lang::compile_wfl(&file, &schemas).expect("compile q13.wfl");
    let mut plans = plans.into_iter();
    let mut plan_a = plans.next().unwrap();
    let _plan_b = plans.next().unwrap();
    plan_a.name = "q13a_bid_mod".into();

    let probe = q13c_bid_mod_batch(&[(1, 1, 100, T)]);
    let one_batch_bytes = wf_engine::window::content_bytes(&probe);
    let mut registry = WindowRegistry::build(vec![
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
    };
    let (mut task_a, _cancel_a, _interval_a) = rule_task::RuleTask::new(config_a);

    // 无消费者槽位：写 5 个 batch 超 2 预算 → append_inner 驱逐自由（min_acked=MAX）
    let bid_win = router.registry().get_window("bid_events").unwrap();
    for i in 0..5i64 {
        bid_win
            .append_with_watermark(q13c_bid_batch(&[(
                1,
                i,
                100 + i,
                T + i * 1_000_000_000,
            )]))
            .unwrap();
    }
    task_a.pull_and_advance().await;
    let bm = router.registry().get_window("bid_mod").unwrap();
    assert!(
        bm.batch_count() < 5,
        "无消费者槽位：中间窗驱逐自由删（min_acked=u64::MAX）——若生产漏注册即丢数据"
    );
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

/// 分片尾部评估（2026-08-24 q4/q9 分片修复）：round-robin 分片后 worker 自身
/// watermark 停在**最后处理批次**的时刻，而驱动窗口的全局尾部（其他 worker 拿到
/// 的更晚批次）可能更靠后——flush（EOS）必须用**驱动窗口全局最终事件时间**评估
/// 剩余挂起（expiry ≤ 全局末尾），否则尾部 pending 永不评估。
///
/// q4 30M 实测：修复前丢 869 条（1,671,690 vs 1,672,559）；修复后 identical。
#[tokio::test]
async fn deferred_flush_uses_global_window_tail_for_sharded_workers() {
    super::tests::init_tracing();
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

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "flush 必须用驱动窗口全局尾部评估尾部挂起（分片 worker 自身 watermark 不足）"
    );
}
