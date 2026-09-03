//! rule_task.rs 纯函数与轻量路径覆盖测试（注册于 rule_task.rs 内）。
//!
//! 覆盖点:
//! - `value_to_json` / `value_to_json_string`: 各 Value 变体 + 非有限数错误。
//! - `event_time_nanos` / `record_window_fields` / `record_wfu_intermediate_meta_value`。
//! - `output_kind` / `event_debug_ref` / `value_debug_string` / `debug_scope_key`。
//! - `resolve_pipe_shape`: pipe 优先 / 空 schema 回退窗口 / 未知目标。
//! - `alias_accepts`: 列式 mask 命中与未命中。
//! - `RuleTask` 轻量路径: `scan_timeouts` / `flush` 无 machine 早退,
//!   `process_push` ack progress slot。
use std::sync::Arc;

use super::*;

use std::sync::atomic::AtomicU64;

use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};
use smol_str::SmolStr;
use tokio_util::sync::CancellationToken;
use wf_engine::alert::{AlertOrigin, OutputRecord};
use wf_engine::match_engine::{EngineHashMap, RuleExecutor, Value};
use wf_engine::pipe::{Pipe, PipeRegistry};
use wf_engine::window::{Router, WindowRegistry};
use wf_lang::wfu_meta::WfuIntermediateMetaField;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        ArrowField::new("sip", DataType::Utf8, true),
        ArrowField::new(
            "event_time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
}

fn empty_router() -> Arc<Router> {
    Arc::new(Router::new(
        WindowRegistry::build(vec![]).expect("empty registry"),
    ))
}

// ---------------------------------------------------------------------------
// value_to_json
// ---------------------------------------------------------------------------

#[test]
fn value_to_json_variants() {
    let number = value_to_json(&Value::Number(3.5)).expect("number");
    assert_eq!(number, serde_json::json!(3.5));

    let string = value_to_json(&Value::Str("hello".into())).expect("str");
    assert_eq!(string, serde_json::json!("hello"));

    let bool = value_to_json(&Value::Bool(true)).expect("bool");
    assert_eq!(bool, serde_json::json!(true));

    let array = value_to_json(&Value::Array(vec![
        Value::Number(1.0),
        Value::Str("x".into()),
    ]))
    .expect("array");
    assert_eq!(array, serde_json::json!([1.0, "x"]));

    let mut object = EngineHashMap::default();
    object.insert(SmolStr::new("b"), Value::Number(2.0));
    object.insert(SmolStr::new("a"), Value::Bool(false));
    let object = value_to_json(&Value::Object(object)).expect("object");
    // Keys are sorted for deterministic output.
    assert_eq!(object, serde_json::json!({"a": false, "b": 2.0}));

    // Nested object inside an array.
    let mut nested = EngineHashMap::default();
    nested.insert(SmolStr::new("k"), Value::Str("v".into()));
    let nested = value_to_json(&Value::Array(vec![Value::Object(nested)])).expect("nested");
    assert_eq!(nested, serde_json::json!([{"k": "v"}]));
}

#[test]
fn value_to_json_non_finite_number_errors() {
    let err = value_to_json(&Value::Number(f64::NAN)).expect_err("NaN must fail");
    assert!(err.to_string().contains("finite"), "got: {err:?}");
    let err = value_to_json(&Value::Number(f64::INFINITY)).expect_err("inf must fail");
    assert!(err.to_string().contains("finite"), "got: {err:?}");
}

#[test]
fn value_to_json_string_structured() {
    let mut object = EngineHashMap::default();
    object.insert(SmolStr::new("a"), Value::Number(1.0));
    let json = value_to_json_string(&Value::Object(object)).expect("serialize");
    assert_eq!(json, r#"{"a":1.0}"#);
}

// ---------------------------------------------------------------------------
// event_time_nanos / record meta helpers
// ---------------------------------------------------------------------------

#[test]
fn event_time_nanos_missing_field_returns_zero() {
    let mut fields = EngineHashMap::default();
    fields.insert(SmolStr::new("sip"), Value::Str("1.2.3.4".into()));
    let event = Event { fields };
    assert_eq!(event_time_nanos(&event, Some("event_time")), 0);

    let mut fields = EngineHashMap::default();
    fields.insert(
        SmolStr::new("event_time"),
        Value::Number(1_700_000_000_000_000_000.0),
    );
    let event = Event { fields };
    assert_eq!(
        event_time_nanos(&event, Some("event_time")),
        1_700_000_000_000_000_000
    );
    // Non-number value → 0.
    let mut fields = EngineHashMap::default();
    fields.insert(SmolStr::new("event_time"), Value::Str("later".into()));
    let event = Event { fields };
    assert_eq!(event_time_nanos(&event, Some("event_time")), 0);
}

fn output_record(target: &str, yield_fields: Vec<(Arc<str>, Value)>) -> OutputRecord {
    OutputRecord {
        wfx_id: "abc123".into(),
        rule_name: Arc::from("test_rule"),
        score: 70.0,
        entity_type: Arc::from("ip"),
        entity_id: "1.2.3.4".into(),
        origin: AlertOrigin::Event,
        fired_at: "2026-01-01T00:00:00Z".into(),
        emit_time: Arc::from("2026-01-01T00:00:00Z"),
        matched_rows: vec![],
        summary: Arc::from("summary"),
        yield_target: Arc::from(target),
        yield_fields,
        yield_field_types: Arc::new([]),
        event_time_nanos: 1_700_000_000_000_000_000,
        machine_id: Arc::from(""),
        scope_key: Arc::from("k=v"),
    }
}

#[test]
fn record_window_fields_appends_missing_meta() {
    let record = output_record(
        "alerts",
        vec![(Arc::from("sip"), Value::Str("1.2.3.4".into()))],
    );
    let fields = record_window_fields(&record);

    let names: HashSet<&str> = fields.iter().map(|(n, _)| &**n).collect();
    assert!(names.contains("sip"));
    // Missing __wfu_* meta fields are appended.
    assert!(names.contains("__wfu_rule_name"));
    assert!(names.contains("__wfu_score"));
    assert!(names.contains("__wfu_entity_type"));
    assert!(names.contains("__wfu_entity_id"));

    // Existing meta fields are NOT duplicated.
    let record = output_record(
        "alerts",
        vec![
            (Arc::from("__wfu_rule_name"), Value::Str("custom".into())),
            (Arc::from("sip"), Value::Str("1.2.3.4".into())),
        ],
    );
    let fields = record_window_fields(&record);
    let rule_names: Vec<_> = fields
        .iter()
        .filter(|(n, _)| &**n == "__wfu_rule_name")
        .collect();
    assert_eq!(rule_names.len(), 1);
}

#[test]
fn record_wfu_intermediate_meta_value_variants() {
    let record = output_record("alerts", vec![]);
    assert_eq!(
        record_wfu_intermediate_meta_value(&record, WfuIntermediateMetaField::RuleName),
        Value::Str("test_rule".into())
    );
    assert_eq!(
        record_wfu_intermediate_meta_value(&record, WfuIntermediateMetaField::Score),
        Value::Number(70.0)
    );
    assert_eq!(
        record_wfu_intermediate_meta_value(&record, WfuIntermediateMetaField::EntityType),
        Value::Str("ip".into())
    );
    assert_eq!(
        record_wfu_intermediate_meta_value(&record, WfuIntermediateMetaField::EntityId),
        Value::Str("1.2.3.4".into())
    );
}

#[test]
fn output_kind_split() {
    let record = output_record("alerts", vec![]);
    let mut intermediate = HashSet::new();
    assert_eq!(output_kind(&record, &intermediate), "alert");

    let record = output_record("__wf_pipe_x", vec![]);
    intermediate.insert("__wf_pipe_x".to_string());
    assert_eq!(output_kind(&record, &intermediate), "intermediate");
}

// ---------------------------------------------------------------------------
// debug helpers
// ---------------------------------------------------------------------------

#[test]
fn event_debug_ref_priority_and_fallback() {
    fn event_with(key: Option<(&str, Value)>) -> Event {
        let mut fields = EngineHashMap::default();
        if let Some((name, value)) = key {
            fields.insert(SmolStr::new(name), value);
        }
        Event { fields }
    }

    assert_eq!(
        event_debug_ref(
            &event_with(Some(("event_id", Value::Str("e1".into())))),
            7,
            3
        ),
        "e1"
    );
    assert_eq!(
        event_debug_ref(&event_with(Some((WFU_ID, Value::Str("w1".into())))), 7, 3),
        "w1"
    );
    assert_eq!(
        event_debug_ref(&event_with(Some(("id", Value::Str("i1".into())))), 7, 3),
        "i1"
    );
    assert_eq!(event_debug_ref(&event_with(None), 7, 3), "batch:7/row:3");
}

#[test]
fn value_debug_string_variants() {
    assert_eq!(value_debug_string(&Value::Number(1.5)), "1.5");
    assert_eq!(value_debug_string(&Value::Str("s".into())), "s");
    assert_eq!(value_debug_string(&Value::Bool(true)), "true");
    assert_eq!(
        value_debug_string(&Value::Array(vec![Value::Number(1.0)])),
        "<structured>"
    );
    assert_eq!(
        value_debug_string(&Value::Object(EngineHashMap::default())),
        "<structured>"
    );
}

#[test]
fn debug_scope_key_joins_values() {
    let key = [
        Value::Number(1.0),
        Value::Str("a".into()),
        Value::Bool(false),
        Value::Array(vec![]),
    ];
    assert_eq!(debug_scope_key(&key), "1,a,false,<structured>");
    assert_eq!(debug_scope_key(&[]), "");
}

#[test]
fn log_output_helpers_do_not_panic() {
    let record = output_record("alerts", vec![]);
    log_output_emitted("execute_close", "close", "alert", &record, &[]);
    log_output_suppressed("test_rule", "execute_close", None);
    log_output_suppressed("test_rule", "execute_close", Some(&[Value::Number(1.0)]));
}

// ---------------------------------------------------------------------------
// resolve_pipe_shape
// ---------------------------------------------------------------------------

#[test]
fn resolve_pipe_shape_prefers_registered_pipe() {
    let registry = PipeRegistry::new();
    let pipe_schema: SchemaRef = Arc::new(Schema::new(vec![ArrowField::new(
        "sip",
        DataType::Utf8,
        true,
    )]));
    registry.register(Pipe {
        name: "alerts".into(),
        schema: Arc::clone(&pipe_schema),
        over: std::time::Duration::ZERO,
        time_col_index: None,
    });
    let router = empty_router();

    let (schema, time_col) = resolve_pipe_shape(&Arc::new(registry), &router, &Arc::from("alerts"))
        .expect("pipe resolves");
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(time_col, None);
}

#[test]
fn resolve_pipe_shape_empty_schema_falls_back_to_window() {
    let registry = PipeRegistry::new();
    registry.register(Pipe {
        name: "alerts".into(),
        schema: Arc::new(Schema::empty()),
        over: std::time::Duration::ZERO,
        time_col_index: None,
    });

    // Window registered with the real schema + time column.
    let def = wf_engine::window::WindowDef {
        params: wf_engine::window::WindowParams {
            name: "alerts".into(),
            schema: test_schema(),
            time_col_index: Some(1),
            over: std::time::Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec![],
        config: wf_config::WindowConfig {
            name: "alerts".into(),
            mode: wf_config::DistMode::Local,
            max_window_bytes: (64 * 1024 * 1024).into(),
            over_cap: std::time::Duration::from_secs(3600).into(),
            evict_policy: wf_config::EvictPolicy::TimeFirst,
            watermark: std::time::Duration::ZERO.into(),
            allowed_lateness: std::time::Duration::from_secs(3600).into(),
            late_policy: wf_config::LatePolicy::Drop,
            table: None,
        },
    };
    let router = Arc::new(Router::new(
        WindowRegistry::build(vec![def]).expect("registry"),
    ));

    let (schema, time_col) = resolve_pipe_shape(&Arc::new(registry), &router, &Arc::from("alerts"))
        .expect("window fallback resolves");
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(time_col, Some(1));
}

#[test]
fn resolve_pipe_shape_unknown_target_returns_none() {
    let registry = PipeRegistry::new();
    let router = empty_router();
    assert!(resolve_pipe_shape(&Arc::new(registry), &router, &Arc::from("ghost")).is_none());
}

// ---------------------------------------------------------------------------
// alias_accepts — 列式 mask 路径
// ---------------------------------------------------------------------------

#[test]
fn alias_accepts_uses_columnar_mask() {
    use arrow::array::BooleanArray;
    let executor = RuleExecutor::new(minimal_plan());
    let router = empty_router();
    let lookup = RegistryLookup::new(&router);
    let event = Event {
        fields: EngineHashMap::default(),
    };

    let masks: HashMap<String, Option<BooleanArray>> = HashMap::from([(
        "a".to_string(),
        Some(BooleanArray::from(vec![true, false, true])),
    )]);
    assert!(alias_accepts(&executor, &masks, "a", 0, &event, &lookup));
    assert!(!alias_accepts(&executor, &masks, "a", 1, &event, &lookup));

    // Alias without a mask → falls through to the interpreted path; the
    // plan has no bind filter for this alias so the row is accepted.
    let empty: HashMap<String, Option<BooleanArray>> = HashMap::new();
    assert!(alias_accepts(
        &executor, &empty, "missing", 0, &event, &lookup
    ));
}

fn minimal_plan() -> wf_lang::plan::RulePlan {
    wf_lang::plan::RulePlan {
        name: "coverage_rule".into(),
        binds: vec![],
        lets: vec![],
        match_plan: wf_lang::plan::MatchPlan {
            keys: vec![],
            key_exprs: Vec::new(),
            key_map: None,
            key_join: None,
            window_spec: wf_lang::plan::WindowSpec::Sliding(std::time::Duration::from_secs(60)),
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
        stats_plan: None,
        joins: vec![],
        r#where: None,
        entity_plan: wf_lang::plan::EntityPlan {
            entity_type: "e".into(),
            entity_id_expr: wf_lang::ast::Expr::Bool(false),
        },
        yield_plan: wf_lang::plan::YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![],
        },
        score_plan: wf_lang::plan::ScorePlan {
            expr: wf_lang::ast::Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    }
}

// ---------------------------------------------------------------------------
// RuleTask 轻量路径
// ---------------------------------------------------------------------------

fn make_bare_task() -> RuleTask {
    let config = RuleTaskConfig {
        machine: None,
        each_alias: None,
        each_time_field: None,
        executor: RuleExecutor::new(minimal_plan()),
        window_sources: vec![],
        sink_fanout: SinkFanout::closed(),
        cancel: CancellationToken::new(),
        timeout_scan_interval: std::time::Duration::from_secs(60),
        router: empty_router(),
        metrics: None,
        intermediate_targets: HashSet::new(),
        pipe_registry: Arc::new(PipeRegistry::new()),
        eos_flush: tokio::sync::watch::channel(0u64).1,
        push_rx: None,
        shard_index: None,
        shard_count: 1,
        key_partitioned: false,
        progress: HashMap::new(),
        conv_sink: None,
    };
    let (task, _cancel, _interval) = RuleTask::new(config);
    task
}

#[tokio::test]
async fn scan_timeouts_without_machine_is_noop() {
    let mut task = make_bare_task();
    // machine None + deferred None → immediate return.
    task.scan_timeouts().await;
}

#[tokio::test]
async fn flush_without_machine_is_noop() {
    let mut task = make_bare_task();
    task.flush().await;
}

#[tokio::test]
async fn process_push_acks_progress_slot() {
    let mut task = make_bare_task();
    let slot = Arc::new(AtomicU64::new(0));
    task.progress
        .insert("auth_events".to_string(), Arc::clone(&slot));

    let push = RulePush {
        window_name: Arc::from("auth_events"),
        events: None,
        batch: None,
        materialize_fields: None,
        seq: 5,
        shard_rows: None,
    };
    task.process_push(push).await;
    // Acked seq + 1 (saturating).
    assert_eq!(slot.load(Ordering::Relaxed), 6);

    // u64::MAX seq must not wrap.
    let push = RulePush {
        window_name: Arc::from("auth_events"),
        events: None,
        batch: None,
        materialize_fields: None,
        seq: u64::MAX,
        shard_rows: None,
    };
    task.process_push(push).await;
    assert_eq!(slot.load(Ordering::Relaxed), u64::MAX);
}

#[tokio::test]
async fn drain_push_channel_processes_buffered_pushes() {
    let mut task = make_bare_task();
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
        seq: 2,
        shard_rows: None,
    })
    .await
    .expect("send");
    drop(tx);

    task.drain_push_channel(&mut rx).await;
    assert_eq!(slot.load(Ordering::Relaxed), 3);
}

#[test]
fn wall_nanos_is_positive() {
    assert!(wall_nanos() > 0);
}

#[test]
fn task_new_ids_and_flags() {
    let task = make_bare_task();
    assert!(task.task_id.starts_with("coverage_rule#"));
    assert_eq!(task.pushed_seq, 0);
    assert_eq!(task.shard_count, 1);
    assert!(task.shard_index.is_none());
}
