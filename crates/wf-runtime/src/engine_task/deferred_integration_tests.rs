//! P3 deferred join（`emit at`）端到端测试（wf-runtime）共享 harness：Q9 形状——
//! auction 驱动流挂起 → bid 注入右窗 → 事件时间 watermark 推进到 expiry → 到期评估输出
//! 胜者; 无 bid 不输出; EOS flush 触发剩余挂起实例。本文件保留共享 schema/batch/
//! 任务构造器、drain 断言与 WFL 常量+编译 schema，按主题分派到兄弟测试子模块（#[path]）:
//! - `deferred_q9_tests`: Q9 评估触发语义（watermark/驱逐 pin/flush/EOS retry/乱序/target lag）;
//! - `deferred_q8_tests`: Q8 存在 deferred join（bucket_end/幂等/EOS retry/miss 恢复/join-key 回退）;
//! - `deferred_q13_tests`: Q13 双链中间窗压力 × 分片 push 消费完整;
//! - `deferred_q13_sharded_tests`: Q13 分片高斜率/生产消费/pull ack/列式管道/未注册消费;
//! - `deferred_wfl_tests`: 真实 q9.wfl 编译计划执行 + 分片 flush 全局窗尾。

use std::sync::Arc;

use std::collections::HashMap;
use std::collections::HashSet;

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use wf_engine::match_engine::{RuleExecutor, Value};
use wf_engine::window::{ProviderWindow, Router, Window, WindowDef, WindowParams, WindowRegistry};
use wf_lang::ast::{
    Bound, BoundVal, Expr, FieldRef, JoinMode, PathSegment, ReduceMeasure, TieSpec, WithinSpec,
};
use wf_lang::plan::{
    BindPlan, EachPlan, EntityPlan, JoinCondPlan, JoinPlan, MatchPlan, RulePlan, ScorePlan,
    YieldField, YieldPlan,
};

use super::tests::{empty_tracked_bind_fields, empty_tracked_plain_fields, make_test_fanout};
use crate::engine_task::{rule_task, task_types};

#[path = "deferred_q9_tests.rs"]
mod deferred_q9_tests;

#[path = "deferred_q8_tests.rs"]
mod deferred_q8_tests;

#[path = "deferred_q13_tests.rs"]
mod deferred_q13_tests;

#[path = "deferred_q13_sharded_tests.rs"]
mod deferred_q13_sharded_tests;

#[path = "deferred_wfl_tests.rs"]
mod deferred_wfl_tests;

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
fn window_def_with_over(name: &str, schema: &Arc<Schema>, over: std::time::Duration) -> WindowDef {
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
fn make_deferred_join_task_with_over(
    bid_over: std::time::Duration,
) -> (
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
        key_partitioned: false,
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
    RecordBatch::try_new(q13c_bid_schema(), cols).unwrap()
}

fn q13c_bid_mod_batch(rows: &[(i64, i64, i64, i64)]) -> RecordBatch {
    // (id, bidder, auction, price, dateTime, mod_key) — mod_key = auction % 10000
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
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
