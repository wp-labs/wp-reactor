//! stats_task.rs 覆盖测试（注册于 stats_task.rs 内）。
//!
//! 覆盖点:
//! - `batch_max_time`: 无时间字段 / 无时间列 / 正常扫描最大值。
//! - `scope_key_to_values`: 全部 ScopeKey 变体（含嵌套 Pair）。
//! - `build_stats_close_output`: 键字段注入 / last-top 行字段展开 / 空键。
//! - `StatsTask::new` 初始化状态; `scan_timeouts` / `flush` 无窗口早退;
//!   `process_push` ack。

use super::*;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use arrow::array::{StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use tokio_util::sync::CancellationToken;
use wf_engine::match_engine::{RuleExecutor, StatsExecutor};
use wf_engine::pipe::PipeRegistry;
use wf_engine::window::WindowRegistry;
use wf_lang::ast::Expr;
use wf_lang::plan::{
    BindPlan, EntityPlan, MatchPlan, ScorePlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan,
    WindowSpec, YieldField, YieldPlan,
};

// ---------------------------------------------------------------------------
// batch_max_time
// ---------------------------------------------------------------------------

fn time_batch(times: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "event_time",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        true,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampNanosecondArray::from(times.to_vec()))],
    )
    .expect("batch")
}

#[test]
fn batch_max_time_without_time_field_returns_min() {
    let batch = time_batch(&[1, 2, 3]);
    assert_eq!(batch_max_time(&batch, None), i64::MIN);
}

#[test]
fn batch_max_time_without_time_column_returns_min() {
    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "sip",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a", "b"]))])
        .expect("batch");
    assert_eq!(batch_max_time(&batch, Some("event_time")), i64::MIN);
}

#[test]
fn batch_max_time_scans_max() {
    let batch = time_batch(&[1_000, 9_000, 5_000]);
    assert_eq!(batch_max_time(&batch, Some("event_time")), 9_000);
    assert_eq!(batch_max_time(&batch, Some("missing_col")), i64::MIN);
}

// ---------------------------------------------------------------------------
// scope_key_to_values
// ---------------------------------------------------------------------------

#[test]
fn scope_key_to_values_all_variants() {
    assert!(scope_key_to_values(&ScopeKey::Empty).is_empty());

    assert_eq!(
        scope_key_to_values(&ScopeKey::Int(7)),
        vec![Value::Number(7.0)]
    );

    let bits = 1.25f64.to_bits();
    assert_eq!(
        scope_key_to_values(&ScopeKey::Float(bits)),
        vec![Value::Number(1.25)]
    );

    assert_eq!(
        scope_key_to_values(&ScopeKey::Str("abc".into())),
        vec![Value::Str("abc".into())]
    );

    // Nested Pair flattens in prefix order.
    let pair = ScopeKey::Pair(
        Box::new(ScopeKey::Int(1)),
        Box::new(ScopeKey::Pair(
            Box::new(ScopeKey::Str("x".into())),
            Box::new(ScopeKey::Int(2)),
        )),
    );
    assert_eq!(
        scope_key_to_values(&pair),
        vec![
            Value::Number(1.0),
            Value::Str("x".into()),
            Value::Number(2.0)
        ]
    );
}

// ---------------------------------------------------------------------------
// build_stats_close_output
// ---------------------------------------------------------------------------

#[test]
fn build_stats_close_output_injects_key_fields() {
    let key_fields = vec!["bidder".to_string(), "auction".to_string()];
    let close = build_stats_close_output(
        "stats_rule",
        &[10.0, 20.0],
        &["cnt".to_string(), "sum".to_string()],
        &[],
        None,
        100,
        110,
        &ScopeKey::Pair(Box::new(ScopeKey::Int(7)), Box::new(ScopeKey::Int(8))),
        &key_fields,
    );
    assert_eq!(close.rule_name, "stats_rule");
    assert_eq!(close.close_reason, CloseReason::Timeout);
    assert_eq!(close.close_step_data.len(), 2);
    // Key fields land in the first StepData's field_values.
    let first = &close.close_step_data[0].field_values;
    assert_eq!(first.get("bidder"), Some(&vec![Value::Number(7.0)]));
    assert_eq!(first.get("auction"), Some(&vec![Value::Number(8.0)]));
    // Second StepData carries no key injection.
    assert!(close.close_step_data[1].field_values.is_empty());
    assert_eq!(close.close_step_data[0].measure_value, 10.0);
    assert_eq!(close.close_step_data[1].measure_value, 20.0);
    assert_eq!(close.close_step_data[0].label.as_deref(), Some("cnt"));
}

#[test]
fn build_stats_close_output_empty_keys_no_injection() {
    let close = build_stats_close_output(
        "stats_rule",
        &[1.0],
        &["cnt".to_string()],
        &[],
        None,
        100,
        110,
        &ScopeKey::Empty,
        &[],
    );
    assert!(close.close_step_data[0].field_values.is_empty());
    assert!(close.scope_key.is_empty());
}

#[test]
fn build_stats_close_output_expands_row_fields() {
    // last/top row-field columns: one per measure, expanded by row_names order.
    let row_names = vec!["price".to_string(), "channel".to_string()];
    let row_a: Arc<[Option<Value>]> =
        Arc::new([Some(Value::Number(99.0)), Some(Value::Str("web".into()))]);
    let row_b: Arc<[Option<Value>]> = Arc::new([None, Some(Value::Str("app".into()))]);
    let row_fields: Vec<Option<&Arc<[Option<Value>]>>> = vec![Some(&row_a), Some(&row_b)];

    let close = build_stats_close_output(
        "stats_rule",
        &[1.0, 2.0],
        &["last_price".to_string(), "last_channel".to_string()],
        &row_fields,
        Some(&row_names),
        100,
        110,
        &ScopeKey::Int(5),
        &[],
    );
    let fv = &close.close_step_data[0].field_values;
    assert_eq!(fv.get("price"), Some(&vec![Value::Number(99.0)]));
    assert_eq!(fv.get("channel"), Some(&vec![Value::Str("web".into())]));
    let fv1 = &close.close_step_data[1].field_values;
    // row_b has no price → the key is not injected (empty slot skipped).
    assert_eq!(fv1.get("price"), None);
    assert_eq!(fv1.get("channel"), Some(&vec![Value::Str("app".into())]));
}

// ---------------------------------------------------------------------------
// StatsTask 轻量路径
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
        name: "stats_rule".into(),
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

fn make_stats_task() -> (StatsTask, CancellationToken) {
    let config = StatsTaskConfig {
        stats: StatsExecutor::with_row_fields(stats_plan(), None),
        executor: RuleExecutor::new(stats_rule_plan()),
        window_sources: vec![],
        sink_fanout: SinkFanout::closed(),
        cancel: CancellationToken::new(),
        router: Arc::new(Router::new(
            WindowRegistry::build(vec![]).expect("registry"),
        )),
        metrics: None,
        time_field: Some("event_time".into()),
        timeout_scan_interval: Duration::from_secs(60),
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        progress: HashMap::new(),
        shard_index: None,
        shard_count: 1,
        merge_rx: None,
        merge_tx: None,
    };
    StatsTask::new(config)
}

#[test]
fn stats_task_new_initial_state() {
    let (task, _cancel) = make_stats_task();
    assert_eq!(task.rule_name(), "stats_rule");
    assert!(task.task_id.starts_with("stats_rule#"));
    assert_eq!(task.window_start, None);
    assert_eq!(task.window_end, None);
    assert_eq!(task.last_watermark, i64::MIN);
}

#[test]
fn window_dur_nanos_fixed_vs_non_fixed() {
    let (task, _cancel) = make_stats_task();
    assert_eq!(task.window_dur_nanos(), Some(10_000_000_000));

    // Sliding spec → None.
    let mut plan = stats_plan();
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(10));
    let (mut task2, _cancel) = make_stats_task();
    task2.stats = StatsExecutor::with_row_fields(plan, None);
    assert_eq!(task2.window_dur_nanos(), None);
}

#[tokio::test]
async fn scan_timeouts_without_window_returns() {
    let (mut task, _cancel) = make_stats_task();
    task.scan_timeouts().await;
}

#[tokio::test]
async fn flush_without_window_returns() {
    let (mut task, _cancel) = make_stats_task();
    task.flush().await;
}

#[tokio::test]
async fn process_push_acks_progress_slot() {
    let (mut task, _cancel) = make_stats_task();
    let slot = Arc::new(AtomicU64::new(0));
    task.progress
        .insert("bid_events".to_string(), Arc::clone(&slot));

    let push = RulePush {
        window_name: Arc::from("bid_events"),
        events: None,
        batch: None,
        materialize_fields: None,
        seq: 3,
        shard_rows: None,
    };
    task.process_push(push).await;
    assert_eq!(slot.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn drain_push_channel_drains_buffered() {
    let (mut task, _cancel) = make_stats_task();
    let slot = Arc::new(AtomicU64::new(0));
    task.progress.insert("w".to_string(), Arc::clone(&slot));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<RulePush>(8);
    tx.send(RulePush {
        window_name: Arc::from("w"),
        events: None,
        batch: None,
        materialize_fields: None,
        seq: 1,
        shard_rows: None,
    })
    .await
    .expect("send");
    tx.send(RulePush {
        window_name: Arc::from("w"),
        events: None,
        batch: None,
        materialize_fields: None,
        seq: 9,
        shard_rows: None,
    })
    .await
    .expect("send");
    drop(tx);

    task.drain_push_channel(&mut rx).await;
    assert_eq!(slot.load(Ordering::Relaxed), 10);
}

#[test]
fn rule_plan_fields_are_consistent() {
    // Smoke-check the helper plan compiles into an executor without panicking.
    let executor = RuleExecutor::new(stats_rule_plan());
    assert_eq!(executor.plan().name, "stats_rule");
}
