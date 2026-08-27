//! stats_task.rs 第四轮补测（注册于 stats_task.rs 内, `#[path]` 方式——
//! 需要访问 `StatsTask` 私有字段）。
//!
//! 覆盖点（经 `pub(super)` 入口驱动）:
//! - `process_batch_from` 已建窗口内后续批次（`Some(_) => {}` 分支）与跨窗口
//!   分段归并。
//! - 行式回退整批路径（`accumulate_segment` 的 `None => batch_to_events`）。
//! - `scan_timeouts` 未到边界早退 + 墙钟兜底关闭尾部窗口。
//! - `flush` 空窗口早退; `close_current_window` 空窗 guard。
//! - `build_stats_close_output` 非 Field 键过滤分支。

use super::*;

use std::time::Duration;

use arrow::array::{Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use tokio_util::sync::CancellationToken;
use wf_engine::match_engine::{RuleExecutor, StatsExecutor, StatsWindowState};
use wf_engine::pipe::PipeRegistry;
use wf_engine::window::{Router, Window, WindowParams, WindowRegistry};
use wf_lang::ast::Expr;
use wf_lang::plan::{
    BindPlan, EntityPlan, MatchPlan, ScorePlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan,
    WindowSpec, YieldField, YieldPlan,
};

use super::super::tests::test_window_config;

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

fn stats_plan() -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
        keys: vec![],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![StatsMeasurePlan {
            label: "cnt".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Count,
            field: None,
            arg: None,
        }],
        tracked_bind_fields: HashMap::new(),
    }
}

fn stats_rule_plan() -> wf_lang::plan::RulePlan {
    wf_lang::plan::RulePlan {
        name: "stats_r4_rule".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        }],
        lets: vec![],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Fixed(Duration::from_secs(10)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: wf_lang::ast::CloseMode::And,
            match_mode: wf_lang::ast::MatchMode::Seq,
            accu: false,
            seq: None,
            tracked_bind_aliases: HashSet::new(),
            tracked_bind_fields: HashMap::new(),
            tracked_plain_fields: HashSet::new(),
            needs_field_history: false,
            trigger_event_needed: false,
        },
        each_plan: None,
        stats_plan: Some(stats_plan()),
        joins: vec![],
        r#where: None,
        entity_plan: EntityPlan {
            entity_type: "digit".into(),
            entity_id_expr: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                "b".into(),
                "auction".into(),
            )),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "id".into(),
                value: Expr::Field(wf_lang::ast::FieldRef::Qualified(
                    "b".into(),
                    "auction".into(),
                )),
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

fn make_config(
    window_sources: Vec<WindowSource>,
    push_rx: Option<mpsc::Receiver<RulePush>>,
    eos_tx: &watch::Sender<u64>,
    metrics: Option<Arc<crate::metrics::RuntimeMetrics>>,
) -> (StatsTaskConfig, CancellationToken) {
    let config = StatsTaskConfig {
        stats: StatsExecutor::with_row_fields(stats_plan(), None),
        executor: RuleExecutor::new(stats_rule_plan()),
        window_sources,
        sink_fanout: crate::alert_task::SinkFanout::closed(),
        cancel: CancellationToken::new(),
        router: Arc::new(Router::new(
            WindowRegistry::build(vec![]).expect("registry"),
        )),
        metrics,
        time_field: Some("event_time".into()),
        timeout_scan_interval: Duration::from_secs(60),
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(PipeRegistry::new()),
        eos_flush: eos_tx.subscribe(),
        push_rx,
        progress: HashMap::new(),
        shard_index: None,
        shard_count: 1,
        merge_rx: None,
        merge_tx: None,
    };
    let cancel = config.cancel.clone();
    (config, cancel)
}

fn time_batch(times: &[i64]) -> arrow::record_batch::RecordBatch {
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "event_time",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        true,
    )]));
    arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampNanosecondArray::from(times.to_vec()))],
    )
    .expect("batch")
}

fn make_window() -> (Arc<Window>, Arc<tokio::sync::Notify>) {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let mut cfg = test_window_config(usize::MAX);
    cfg.name = "bid_events".to_string();
    let win = Window::new(
        WindowParams {
            name: "bid_events".into(),
            schema,
            time_col_index: Some(1),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        cfg,
    );
    (Arc::new(win), Arc::new(tokio::sync::Notify::new()))
}

fn window_batch(times: &[i64]) -> arrow::record_batch::RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("auction", DataType::Int64, true),
        ArrowField::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    // auction 列长度必须与 times 对齐（旧代码固定 2 行，3 个 times 的调用
    // 触发 "all columns must have the same length"）。
    let auctions: Vec<i64> = (0..times.len() as i64).collect();
    arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auctions)),
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
        ],
    )
    .expect("batch")
}

// ---------------------------------------------------------------------------
// process_batch_from — 窗口内/跨窗口分段
// ---------------------------------------------------------------------------

/// 已建窗口（首批 t=5s）后, 后续批次首行落在窗口内（`Some(_) => {}`）再越界。
/// 两次 `pull_and_process`（每次一个 append batch）驱动。
#[tokio::test]
async fn pull_and_process_same_window_then_cross_window_segments() {
    let (win, notify) = make_window();
    let sources = vec![WindowSource {
        window_name: "bid_events".into(),
        window: Arc::clone(&win),
        notify,
        aliases: vec!["b".into()],
    }];
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(sources, None, &eos_tx, None);
    let (mut task, _cancel) = StatsTask::new(config);

    // 首批: t=5s → 窗口 [0,10s)。
    win.append(window_batch(&[5_000_000_000])).expect("append");
    task.pull_and_process().await;
    assert_eq!(task.last_watermark, 5_000_000_000);

    // 第二批: t=6s（窗口内）+ t=15s（跨窗口 → 关闭 [0,10) 并推进）。本次
    // 覆盖 `Some(_) => {}`（t=6s 段）与 `Some(end) if first_t >= end`（t=15s）。
    win.append(window_batch(&[6_000_000_000, 15_000_000_000]))
        .expect("append");
    task.pull_and_process().await;
    assert_eq!(task.last_watermark, 15_000_000_000);
    // 15s 推进后窗口应为 [10,20s)。
    assert_eq!(task.window_start, Some(10_000_000_000));
    assert_eq!(task.window_end, Some(20_000_000_000));

    // flush 收尾当前窗口。
    task.flush().await;
    assert_eq!(task.window_start, None);
    assert_eq!(task.window_end, None);
}

/// 非 fixed（sliding）计划 → `window_dur_nanos` None → 整批退化归并
/// （`accumulate_segment` 的 `None => batch_to_events` 行式回退由 stats 前置
/// 不满足触发——这里用 sliding 保证退化单段）。
#[tokio::test]
async fn process_batch_from_sliding_accumulates_whole_batch() {
    let (win, notify) = make_window();
    let sources = vec![WindowSource {
        window_name: "bid_events".into(),
        window: Arc::clone(&win),
        notify,
        aliases: vec!["b".into()],
    }];
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(sources, None, &eos_tx, None);
    let (mut task, _cancel) = StatsTask::new(config);

    // sliding → window_dur_nanos None → 单段整批归并。非 Field 键使列式前置
    // 不满足 → 回退行式整批（`None => batch_to_events` 分支）。
    let mut plan = stats_plan();
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(10));
    plan.keys = vec![Expr::Number(1.0)];
    task.stats = StatsExecutor::with_row_fields(plan, None);

    win.append(window_batch(&[1_000, 2_000, 3_000]))
        .expect("append");
    task.pull_and_process().await;
    // sliding → 不建窗口。
    assert_eq!(task.window_start, None);
    assert_eq!(task.last_watermark, 3_000);
}

// ---------------------------------------------------------------------------
// scan_timeouts — 早退 / 墙钟兜底关闭
// ---------------------------------------------------------------------------

/// 窗口刚建立、墙钟未推进 → `effective_watermark < end` 早退。
#[tokio::test]
async fn scan_timeouts_early_return_before_boundary() {
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(vec![], None, &eos_tx, None);
    let (mut task, _cancel) = StatsTask::new(config);
    // 通过真实批次建立窗口: push 一个 t=5s 的批次。
    task.process_push(RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: Some(Arc::new(time_batch(&[5_000_000_000]))),
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    })
    .await;
    assert_eq!(task.window_start, Some(0));
    assert_eq!(task.window_end, Some(10_000_000_000));

    task.scan_timeouts().await;
    // 未到边界: 窗口保持不变。
    assert_eq!(task.window_start, Some(0));
    assert_eq!(task.window_end, Some(10_000_000_000));
}

/// 墙钟推进（小扫描间隔 + 睡眠）→ watermark 越过边界 → 关闭尾部窗口。
#[tokio::test]
async fn scan_timeouts_closes_tail_window_on_wall_clock_advance() {
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(vec![], None, &eos_tx, None);
    let (mut task, _cancel) = StatsTask::new(config);
    // 让扫描间隔小到墙钟流逝可以推进: 覆盖 scan_timeouts 的 config 需要
    // 重建 timeout_scan_interval —— 用默认 60s 无法在测试内推进, 这里改为
    // 把批次时间放在 end 前 1ns, 扫描间隔 1ms, 睡 2ms 后墙钟推进。
    task.timeout_scan_interval = Duration::from_millis(1);
    task.process_push(RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: Some(Arc::new(time_batch(&[9_999_999_999]))),
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    })
    .await;
    assert_eq!(task.window_end, Some(10_000_000_000));
    assert!(task.stats.window.event_count > 0);

    tokio::time::sleep(Duration::from_millis(5)).await;
    task.scan_timeouts().await;
    // 关闭后清空窗口状态。
    assert_eq!(task.window_start, None);
    assert_eq!(task.window_end, None);
}

// ---------------------------------------------------------------------------
// flush — 空窗口早退 / 无数据 close guard
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flush_without_window_is_noop() {
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(vec![], None, &eos_tx, None);
    let (mut task, _cancel) = StatsTask::new(config);
    task.flush().await;
    assert_eq!(task.window_start, None);
}

/// `close_current_window` 空窗 guard（`event_count == 0` 直接返回不产出）。
/// 构造: 通过 push 批次建立窗口后, 手动清空 stats 累加器再 flush。
#[tokio::test]
async fn close_current_window_empty_bucket_guard() {
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(vec![], None, &eos_tx, None);
    let (mut task, _cancel) = StatsTask::new(config);
    task.process_push(RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: Some(Arc::new(time_batch(&[1_000_000_000]))),
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    })
    .await;
    assert!(task.window_start.is_some());
    // 清空累加器 → close 时 event_count == 0 → guard 返回, 不产出。
    task.stats.window = StatsWindowState::default();
    task.flush().await;
    assert_eq!(task.window_start, None);
}

// ---------------------------------------------------------------------------
// build_stats_close_output — 非 Field 键
// ---------------------------------------------------------------------------

/// keys 含非 Field 表达式 → `filter_map` 的 `_ => None` 分支（键字段不进
/// field_values）。
#[tokio::test]
async fn close_with_non_field_key_skips_key_field_injection() {
    let (win, notify) = make_window();
    let sources = vec![WindowSource {
        window_name: "bid_events".into(),
        window: Arc::clone(&win),
        notify,
        aliases: vec!["b".into()],
    }];
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(sources, None, &eos_tx, None);
    let (mut task, _cancel) = StatsTask::new(config);

    // 计划带一个非 Field 键（Number）。
    let mut plan = stats_plan();
    plan.keys = vec![Expr::Number(1.0)];
    task.stats = StatsExecutor::with_row_fields(plan, None);

    win.append(window_batch(&[1_000_000_000])).expect("append");
    task.pull_and_process().await;
    // 越过 10s 边界 → close。非 Field 键被 filter_map 丢弃, 不 panic。
    win.append(window_batch(&[11_000_000_000])).expect("append");
    task.pull_and_process().await;
    task.flush().await;
    assert_eq!(task.window_start, None);
}

// ---------------------------------------------------------------------------
// 进度槽 ack（pull 路径）——已有 coverage_more 覆盖, 此处不再重复
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 状态内存 guard 告警（metrics + 每窗 wf_warn）
// ---------------------------------------------------------------------------

/// 从快照记录中取 `rule.stats_over_limit_total` 的 label 值（快照是 drain 语义——
/// 每次调用 `snapshot()` 即消费并清零计数, 断言用一次取一次）。
fn over_limit_reported(metrics: &crate::metrics::RuntimeMetrics, rule: &str) -> u64 {
    metrics
        .snapshot()
        .to_records()
        .iter()
        .find(|r| {
            r.fields
                .iter()
                .any(|(k, v)| k == "name" && v == "stats_over_limit_total")
                && r.fields.iter().any(|(k, v)| k == "label" && v == rule)
        })
        .and_then(|r| {
            r.fields
                .iter()
                .find(|(k, _)| k == "value")
                .map(|(_, v)| v.parse::<u64>().unwrap_or(0))
        })
        .unwrap_or(0)
}

/// 带键 count 计划 + 极小限额（count 桶预算 = (256+80)×2.2 校准 = 739B,
/// 2026-08-27 q18 实测估算低估 2.2×; 限额 739 → 只放行 1 桶, 其余键全拒收）。
fn guard_plan_and_limit(task: &mut StatsTask) {
    let mut plan = stats_plan();
    plan.keys = vec![Expr::Field(wf_lang::ast::FieldRef::Qualified(
        "b".into(),
        "auction".into(),
    ))];
    task.stats = StatsExecutor::with_row_fields(plan, None);
    task.stats.set_memory_limit("stats_r4_rule", Some(739));
}

/// 状态内存 guard 告警: close 时按窗口增量上报 metrics（delta 记账, 不重复）。
/// 两个窗口各拒收 9 个新键 → 两次 close 各上报 9（不是第二次报累计 18）。
#[tokio::test]
async fn close_current_window_reports_over_limit_delta() {
    let metrics = Arc::new(crate::metrics::RuntimeMetrics::new(
        &["stats_r4_rule".to_string()],
        &[],
        &[],
        std::collections::BTreeMap::new(),
    ));
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(vec![], None, &eos_tx, Some(Arc::clone(&metrics)));
    let (mut task, _cancel) = StatsTask::new(config);
    guard_plan_and_limit(&mut task);

    // 窗口 1: 10 个不同 auction 键（window_batch 的 auction = 0..len）→ 拒 9。
    task.process_push(RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: Some(Arc::new(window_batch(&[1_000_000_000; 10]))),
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    })
    .await;
    assert_eq!(
        task.stats.window.over_limit_new_buckets(),
        9,
        "窗口 1 拒收 9"
    );

    task.flush().await; // close 窗口 1 → 上报增量 9
    assert_eq!(
        over_limit_reported(&metrics, "stats_r4_rule"),
        9,
        "窗口 1 拒收 9 个新键应上报"
    );

    // 窗口 2（越界推进新窗口）: 新 10 键 → 再拒 9。计数跨窗口保留 → 增量 9。
    task.process_push(RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: Some(Arc::new(window_batch(&[11_000_000_000; 10]))),
        materialize_fields: None,
        seq: 2,
        shard_rows: None,
    })
    .await;
    assert_eq!(task.stats.window.over_limit_new_buckets(), 18, "累计 18");
    task.flush().await;
    assert_eq!(
        over_limit_reported(&metrics, "stats_r4_rule"),
        9,
        "窗口 2 增量 9（delta 记账, 不是累计 18）"
    );
}

/// 输入分区分片非协调片（merge_tx）: close 发 partial 的同时**上报自己的拒收
/// 增量**——被拒键不在 partial 里（take_partial 只导出已建桶）, 协调片看不到;
/// 不报则分片拒收永远丢失。各片上报自己的部分（互不重叠, 汇总 = 总拒收）。
#[tokio::test]
async fn shard_non_coordinator_reports_own_over_limit() {
    let metrics = Arc::new(crate::metrics::RuntimeMetrics::new(
        &["stats_r4_rule".to_string()],
        &[],
        &[],
        std::collections::BTreeMap::new(),
    ));
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(vec![], None, &eos_tx, Some(Arc::clone(&metrics)));
    let (mut task, _cancel) = StatsTask::new(config);
    guard_plan_and_limit(&mut task);

    // 模拟分片非协调片: merge_tx 已设置 → flush/close 走 take_partial 早退。
    let (tx, mut rx) = tokio::sync::mpsc::channel::<StatsPartial>(1);
    task.merge_tx = Some(tx);

    task.process_push(RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: Some(Arc::new(window_batch(&[1_000_000_000; 10]))),
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    })
    .await;
    assert_eq!(
        task.stats.window.over_limit_new_buckets(),
        9,
        "guard 照常拒收"
    );

    task.flush().await; // 发 partial + 上报分片自己的拒收增量
    let partial = rx.try_recv().expect("partial 已发送");
    assert_eq!(partial.1 - partial.0, 10_000_000_000, "窗口区间正确");
    assert_eq!(partial.2.len(), 1, "partial 只含已建桶（键 0）");
    assert_eq!(
        over_limit_reported(&metrics, "stats_r4_rule"),
        9,
        "分片上报自己的拒收 9（协调片看不到被拒键）"
    );
}

/// 全被拒窗口（event_count == 0）: 空窗 guard 不产出, 但拒收计数仍需上报——
/// 不延迟到下一有数据窗口（metrics 实时性; F3）。
#[tokio::test]
async fn close_current_window_all_rejected_window_still_reports() {
    let metrics = Arc::new(crate::metrics::RuntimeMetrics::new(
        &["stats_r4_rule".to_string()],
        &[],
        &[],
        std::collections::BTreeMap::new(),
    ));
    let (eos_tx, _) = watch::channel(0u64);
    let (config, _cancel) = make_config(vec![], None, &eos_tx, Some(Arc::clone(&metrics)));
    let (mut task, _cancel) = StatsTask::new(config);
    guard_plan_and_limit(&mut task);
    // 限额 < 桶预算（校准后 count 桶 739B）→ 全部新键被拒（每键 allowance > 100）。
    task.stats.set_memory_limit("stats_r4_rule", Some(100));

    task.process_push(RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: Some(Arc::new(window_batch(&[1_000_000_000; 10]))),
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    })
    .await;
    assert_eq!(task.stats.window.over_limit_new_buckets(), 10, "10 行全拒");
    assert_eq!(task.stats.window.event_count, 0, "全被拒 → 无归并行");

    task.flush().await; // 空窗 guard: 不产出但上报拒收
    assert_eq!(
        over_limit_reported(&metrics, "stats_r4_rule"),
        10,
        "全被拒窗口仍上报拒收增量"
    );
}
