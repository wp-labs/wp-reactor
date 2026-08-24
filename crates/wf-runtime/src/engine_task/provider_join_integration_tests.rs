//! P4 side input（provider 窗口）端到端测试（wf-runtime）：Q13 精确化形状——
//! bid 驱动流 ⋈ provider person 静态表（knowdb 加载），snapshot join 富化输出。
//! 回归锚点：此前 `RegistryLookup::join_lookup` 对 provider 窗口 `get_window`
//! 返回 None → join 静默 miss（事件不富化），本测试钉死「命中富化 / 未命中丢弃」。
use std::sync::Arc;

use std::collections::{HashMap, HashSet};

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

use wf_engine::match_engine::{RuleExecutor, Value};
use wf_engine::window::{ProviderWindow, Router, Window, WindowDef, WindowParams, WindowRegistry};
use wf_lang::ast::{Expr, FieldRef, JoinMode};
use wf_lang::plan::{
    BindPlan, EachPlan, EntityPlan, JoinCondPlan, JoinPlan, MatchPlan, RulePlan, ScorePlan,
    YieldField, YieldPlan,
};

use super::tests::{empty_tracked_bind_fields, empty_tracked_plain_fields, make_test_fanout};
use crate::engine_task::{rule_task, task_types};

const T: i64 = 1_700_000_000_000_000_000;

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

/// Q13 精确化形状（side input）：bid ⋈ provider person 静态表 snapshot join。
/// ```wfl
/// events { b : bid_events }
/// on each b
/// join person_table snapshot on b.bidder == person_table.id
/// entity(digit, b.bidder)
/// yield alerts (id = b.bidder, state = person_table.state)
/// ```
/// `mode` 可配置（snapshot / anti）——anti 是白名单排除（2026-08-24 放开静态表
/// anti：纯键存在性否定不依赖时间）。
fn make_provider_join_task(
    mode: JoinMode,
) -> (
    rule_task::RuleTask,
    mpsc::Receiver<crate::alert_task::AlertBatch>,
    Arc<Router>,
) {
    let driver = "bid_events";
    let mut registry = WindowRegistry::build(vec![window_def(driver, &bid_schema())]).unwrap();

    // knowdb 加载的 person 静态表：id → state
    let mut pw = ProviderWindow::new(
        "person_table".into(),
        "SELECT * FROM person_table".into(),
        None,
    );
    pw.load(vec![
        {
            let mut m = HashMap::new();
            m.insert("id".to_string(), Value::Number(5.0));
            m.insert("state".to_string(), Value::Str("CA".into()));
            m
        },
        {
            let mut m = HashMap::new();
            m.insert("id".to_string(), Value::Number(7.0));
            m.insert("state".to_string(), Value::Str("ID".into()));
            m
        },
    ]);
    registry
        .register_provider("person_table".to_string(), pw)
        .unwrap();
    let router = Arc::new(Router::new(registry));
    let source_window = router.registry().get_window(driver).unwrap();
    let source_notify = router.registry().get_notifier(driver).unwrap();

    let rule_plan = RulePlan {
        conv_window: None,
        name: "q13_provider_e2e".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
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
            alias: "b".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![JoinPlan {
            right_window: "person_table".to_string(),
            mode,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("b".into(), "bidder".into()),
                right: FieldRef::Qualified("person_table".into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Simple("bidder".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![
                YieldField {
                    name: "id".into(),
                    value: Expr::Field(FieldRef::Simple("bidder".into())),
                },
                YieldField {
                    name: "state".into(),
                    value: Expr::Field(FieldRef::Qualified("person_table".into(), "state".into())),
                },
            ],
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
        each_alias: Some("b".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: driver.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["b".into()],
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

fn bid_window(router: &Router) -> Arc<Window> {
    router.registry().get_window("bid_events").unwrap()
}

/// 命中 provider 行 → 富化输出（bidder 5 在 person_table 中 state=CA）。
#[tokio::test]
async fn provider_join_hit_enriches_from_static_table() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_provider_join_task(JoinMode::Snapshot);

    bid_window(&router)
        .append(bid_batch(&[(1, 5, 100, T + 10_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "5",
        "entity = bidder"
    );
    assert_eq!(
        super::tests::field_str(&alert, "state"),
        "CA",
        "snapshot join must enrich `person_table.state` from the provider table"
    );
}

/// 未命中 provider 行 → snapshot miss 语义：不富化但保留事件（snapshot 非 inner）。
#[tokio::test]
async fn provider_join_miss_keeps_event_without_enrichment() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_provider_join_task(JoinMode::Snapshot);

    // bidder=999 不在 person_table 中
    bid_window(&router)
        .append(bid_batch(&[(2, 999, 100, T + 10_000_000_000)]))
        .unwrap();
    task.pull_and_advance().await;

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "999",
        "snapshot miss keeps the event"
    );
    assert_eq!(
        super::tests::field_str(&alert, "state"),
        "",
        "unmatched bid has no enriched state"
    );
}

/// provider anti（2026-08-24 放开）：命中静态表 → 丢弃（白名单排除）；未命中
/// → 保留。person_table 有 id 5/7（state CA/ID）。
#[tokio::test]
async fn provider_anti_drops_whitelisted_keeps_others() {
    super::tests::init_tracing();
    let (mut task, mut alert_rx, router) = make_provider_join_task(JoinMode::Anti);

    // bidder=5（白名单）→ 丢弃；bidder=999（不在白名单）→ 保留。
    bid_window(&router)
        .append(bid_batch(&[
            (1, 5, 100, T + 10_000_000_000),
            (2, 999, 100, T + 11_000_000_000),
        ]))
        .unwrap();
    task.pull_and_advance().await;

    let alert = super::tests::take_alert(&mut alert_rx);
    assert_eq!(
        super::tests::field_str(&alert, "__wfu_entity_id"),
        "999",
        "anti join keeps non-whitelisted bidder"
    );
    assert_eq!(
        super::tests::field_str(&alert, "state"),
        "",
        "anti hit has no enrichment (event dropped instead)"
    );
}

/// D4 扩展（2026-08-24）：snapshot join 目标窗口（**buffer 窗口**，区别于 provider
/// 静态表）在规则任务构造时取得保留 pin——全保留（`i64::MIN`）直到任务结束。
///
/// 回归锚点：snapshot 语义 = join 时刻的完整状态，驱动事件可引用任意老的实体行
///（q3 join person / q6·q20 join auction）。此前这类窗口的字节上限一旦成为约束，
/// 驱逐会静默丢掉快照还需要的行（历史根因：q3 差 50% / q6 差 36% / q20 差 21%）。
#[test]
fn snapshot_join_buffer_target_gets_retention_pin() {
    let driver = "bid_events";
    let target = "auction_events";
    let registry = WindowRegistry::build(vec![
        window_def(driver, &bid_schema()),
        window_def(target, &auction_schema()),
    ])
    .unwrap();
    let router = Arc::new(Router::new(registry));
    let source_window = router.registry().get_window(driver).unwrap();
    let source_notify = router.registry().get_notifier(driver).unwrap();

    // Q20 形状：bid 驱动 ⋈ auction 实体表 snapshot join。
    let rule_plan = RulePlan {
        conv_window: None,
        name: "snap_pin_e2e".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
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
            alias: "b".into(),
            filter: None,
        }),
        stats_plan: None,
        joins: vec![JoinPlan {
            right_window: target.to_string(),
            mode: JoinMode::Snapshot,
            conds: vec![JoinCondPlan {
                left: FieldRef::Qualified("b".into(), "auction".into()),
                right: FieldRef::Qualified(target.into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(FieldRef::Simple("auction".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "id".into(),
                value: Expr::Field(FieldRef::Simple("auction".into())),
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
    let (alert_tx, _alert_rx) = mpsc::channel::<crate::alert_task::AlertBatch>(64);
    let config = task_types::RuleTaskConfig {
        progress: std::collections::HashMap::new(),
        conv_sink: None,
        machine: None,
        each_alias: Some("b".into()),
        each_time_field: Some("event_time".into()),
        executor,
        window_sources: vec![task_types::WindowSource {
            window_name: driver.into(),
            window: source_window,
            notify: source_notify,
            aliases: vec!["b".into()],
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

    let task = rule_task::RuleTask::new(config);
    let target_win = router.registry().get_window(target).unwrap();
    assert_eq!(
        target_win.retention_floor_ns(),
        i64::MIN,
        "snapshot join 目标窗口必须全保留（快照需要完整状态）"
    );

    // 任务结束（drop）→ pin 自动释放 → 窗口恢复可驱逐。
    drop(task);
    assert_eq!(
        target_win.retention_floor_ns(),
        i64::MAX,
        "任务 drop 后 pin 必须自动释放（Weak 死）"
    );
}

/// auction 实体表 schema（join 目标用）：id + event_time。
fn auction_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}
