//! spawn.rs 覆盖测试（注册于 spawn.rs 内, 可访问私有辅助函数）。
//!
//! 覆盖点:
//! - 纯函数: `resolve_source_path` / `source_data_format` / `source_stream_tag` /
//!   `resolve_connector_kind` / `register_builtin_external_sources` /
//!   `collect_expr_field_names` / `stats_row_fields`（无 last/top → None）。
//! - `resolve_window_sources`: 注册表缺失窗口时的跳过分支。
//! - 任务组组装: `spawn_metrics_task`（disabled / enabled ± metrics）、
//!   `spawn_window_actors`（空注册表 / 单窗口）、`spawn_evictor_task`。
//! - `spawn_receiver_task`: 无可用 source 的错误路径。

use super::*;

use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};
use tokio_util::sync::CancellationToken;

use wf_config::{ConfigVarContext, DistMode, EvictPolicy, FusionConfig, LatePolicy, WindowConfig};
use wf_engine::window::WindowDef;
use wf_lang::ast::{BinOp, Expr, FieldRef};
use wf_lang::plan::{
    BindPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StatsAggPlan, StatsMeasurePlan,
    StatsOutputShapePlan, StatsPlan, WindowSpec, YieldField, YieldPlan,
};

// ---------------------------------------------------------------------------
// 配置加载辅助（最小 wfusion.toml + 外部 windows.toml）
// ---------------------------------------------------------------------------

const WINDOWS_TOML: &str = r#"[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"
"#;

fn load_config(dir: &tempfile::TempDir, extra: &str) -> FusionConfig {
    let windows_path = dir.path().join("windows.toml");
    std::fs::write(&windows_path, WINDOWS_TOML).expect("write windows.toml");
    let config_path = dir.path().join("wfusion.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
sinks = "sinks"
windows = "windows.toml"
{extra}
[runtime]
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/*.wfl"

[[sources]]
type = "file"
enable = true
path = "data/events.ndjson"
"#
        ),
    )
    .expect("write wfusion.toml");
    FusionConfig::load_with_context(&config_path, &ConfigVarContext::new(), Some(dir.path()))
        .expect("load fusion config")
}

fn empty_router() -> Arc<Router> {
    let registry = WindowRegistry::build(vec![]).expect("empty registry");
    Arc::new(Router::new(registry))
}

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

// ---------------------------------------------------------------------------
// 纯函数
// ---------------------------------------------------------------------------

#[test]
fn resolve_source_path_absolute_and_relative() {
    let base = std::path::Path::new("/tmp/wf-base");
    assert_eq!(
        resolve_source_path(base, "/abs/data.ndjson"),
        std::path::PathBuf::from("/abs/data.ndjson")
    );
    assert_eq!(
        resolve_source_path(base, "data/events.ndjson"),
        std::path::PathBuf::from("/tmp/wf-base/data/events.ndjson")
    );
}

#[test]
fn source_data_format_precedence_and_default() {
    let mut source = wf_config::SourceConfig::default();
    // No format params → default "ndjson".
    assert_eq!(source_data_format(&source), "ndjson");

    source.params.insert("format".into(), "csv".into());
    assert_eq!(source_data_format(&source), "csv");

    // `data_format` wins over `format`.
    source
        .params
        .insert("data_format".into(), "arrow_ipc".into());
    assert_eq!(source_data_format(&source), "arrow_ipc");
}

#[test]
fn source_stream_tag_defaults_empty() {
    let source = wf_config::SourceConfig::default();
    assert_eq!(source_stream_tag(&source), "");
    let mut source = wf_config::SourceConfig::default();
    source.params.insert("stream_tag".into(), "syslog".into());
    assert_eq!(source_stream_tag(&source), "syslog");
}

#[test]
fn resolve_connector_kind_known_and_unknown() {
    register_builtin_external_sources();
    let defs = wp_core_connectors::registry::registered_source_defs();
    if let Some(def) = defs.first() {
        assert_eq!(resolve_connector_kind(&def.id), Some(def.kind.clone()));
    }
    assert_eq!(resolve_connector_kind("no_such_connector_id"), None);
}

#[test]
fn register_builtin_external_sources_is_idempotent() {
    // The `Once` guard makes repeat calls no-ops.
    register_builtin_external_sources();
    register_builtin_external_sources();
    // A second registration must not panic.
}

#[test]
fn collect_expr_field_names_walks_expressions() {
    let mut out = HashSet::new();
    let expr = Expr::BinOp {
        left: Box::new(Expr::Field(FieldRef::Qualified("b".into(), "price".into()))),
        op: BinOp::Gt,
        right: Box::new(Expr::Neg(Box::new(Expr::Field(FieldRef::Simple(
            "min_price".into(),
        ))))),
    };
    let func = Expr::FuncCall {
        qualifier: None,
        name: "coalesce".into(),
        args: vec![
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Number(0.0),
        ],
    };
    // `not <expr>` 子树里的字段同样要收集（issue #22 回归）。
    let not_expr = Expr::Not(Box::new(Expr::Field(FieldRef::Simple("is_private".into()))));
    collect_expr_field_names(&expr, &mut out);
    collect_expr_field_names(&func, &mut out);
    collect_expr_field_names(&not_expr, &mut out);
    assert!(out.contains("price"));
    assert!(out.contains("min_price"));
    assert!(out.contains("bidder"));
    assert!(out.contains("is_private"));
    // Non-field leaves contribute nothing.
    collect_expr_field_names(&Expr::Number(1.0), &mut out);
    assert_eq!(out.len(), 4);
}

#[test]
fn stats_row_fields_none_without_row_measures() {
    let plan = minimal_rule_plan("r_none");
    let stats_plan = StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(60)),
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
    };
    // Only last/top require row-field extraction.
    assert!(stats_row_fields(&plan, &stats_plan).is_none());
}

fn minimal_rule_plan(name: &str) -> RulePlan {
    RulePlan {
        name: name.into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "w1".into(),
            filter: None,
        }],
        lets: vec![],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(60)),
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
        entity_plan: EntityPlan {
            entity_type: "e".into(),
            entity_id_expr: Expr::Bool(false),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "id".into(),
                value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
            }],
        },
        score_plan: ScorePlan {
            expr: Expr::Number(1.0),
        },
        pattern_origin: None,
        conv_plan: None,
        limits_plan: None,
        conv_window: None,
    }
}

// ---------------------------------------------------------------------------
// resolve_window_sources
// ---------------------------------------------------------------------------

#[test]
fn resolve_window_sources_skips_missing_windows() {
    let schema = test_schema();
    let def = WindowDef {
        params: wf_engine::window::WindowParams {
            name: "w1".into(),
            schema,
            time_col_index: Some(1),
            over: std::time::Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec!["syslog".into()],
        config: test_window_config(),
    };
    let registry = WindowRegistry::build(vec![def]).expect("build registry");

    let mut aliases = HashMap::new();
    aliases.insert("w1".to_string(), vec!["a".to_string()]);
    aliases.insert("missing_window".to_string(), vec!["b".to_string()]);

    let sources = resolve_window_sources(&aliases, &registry);
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].window_name, "w1");
    assert_eq!(sources[0].aliases, vec!["a".to_string()]);
}

fn test_window_config() -> WindowConfig {
    WindowConfig {
        name: "w1".into(),
        mode: DistMode::Local,
        max_window_bytes: (64 * 1024 * 1024).into(),
        over_cap: std::time::Duration::from_secs(3600).into(),
        evict_policy: EvictPolicy::TimeFirst,
        watermark: std::time::Duration::ZERO.into(),
        allowed_lateness: std::time::Duration::from_secs(3600).into(),
        late_policy: LatePolicy::Drop,
        table: None,
    }
}

// ---------------------------------------------------------------------------
// 任务组组装
// ---------------------------------------------------------------------------

#[tokio::test]
async fn spawn_metrics_task_disabled_returns_empty_group() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = load_config(&dir, "");
    assert!(!config.metrics.enabled);

    let router = empty_router();
    let cancel = CancellationToken::new();
    let group = spawn_metrics_task(&config, &router, cancel.clone(), None, None)
        .await
        .expect("disabled metrics returns Ok");
    // Empty group joins immediately.
    group.wait(cancel).await.expect("join ok");
}

#[tokio::test]
async fn spawn_metrics_task_enabled_without_metrics_returns_empty_group() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = load_config(&dir, "");
    config.metrics.enabled = true;

    let router = empty_router();
    let cancel = CancellationToken::new();
    let group = spawn_metrics_task(&config, &router, cancel.clone(), None, None)
        .await
        .expect("enabled metrics returns Ok");
    group.wait(cancel).await.expect("join ok");
}

#[tokio::test]
async fn spawn_metrics_task_enabled_with_metrics_spawns_task() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = load_config(&dir, "");
    config.metrics.enabled = true;
    config.metrics.report_interval = "1s".parse().expect("1s duration");

    let router = empty_router();
    let cancel = CancellationToken::new();
    let metrics = Arc::new(RuntimeMetrics::new(&[], &[], &[], BTreeMap::new()));
    let group = spawn_metrics_task(&config, &router, cancel.clone(), Some(metrics), None)
        .await
        .expect("spawn metrics task");
    cancel.cancel();
    group
        .wait(cancel)
        .await
        .expect("metrics task exits on cancel");
}

#[tokio::test]
async fn spawn_window_actors_empty_registry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = load_config(&dir, "");
    let router = empty_router();
    let gate = Arc::new(EvictionGate::new(1024 * 1024));
    let cancel = CancellationToken::new();
    let group = spawn_window_actors(&config, &router, gate, cancel.clone(), None);
    // No windows → no actor tasks; joins immediately.
    group.wait(cancel).await.expect("join ok");
}

#[tokio::test]
async fn spawn_window_actors_with_one_window() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = load_config(&dir, "");
    let schema = test_schema();
    let def = WindowDef {
        params: wf_engine::window::WindowParams {
            name: "w1".into(),
            schema,
            time_col_index: Some(1),
            over: std::time::Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec!["syslog".into()],
        config: test_window_config(),
    };
    let registry = WindowRegistry::build(vec![def]).expect("build registry");
    let router = Arc::new(Router::new(registry));
    let gate = Arc::new(EvictionGate::new(1024 * 1024));
    let cancel = CancellationToken::new();
    let group = spawn_window_actors(&config, &router, gate, cancel.clone(), None);
    cancel.cancel();
    group.wait(cancel).await.expect("actor exits on cancel");
}

#[tokio::test]
async fn spawn_evictor_task_builds_group() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = load_config(&dir, "");
    let router = empty_router();
    let gate = Arc::new(EvictionGate::new(1024 * 1024));
    let cancel = CancellationToken::new();
    let group = spawn_evictor_task(&config, &router, gate, cancel.clone(), None);
    cancel.cancel();
    group.wait(cancel).await.expect("evictor exits on cancel");
}

#[tokio::test]
async fn spawn_receiver_task_file_source_ok() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Empty ndjson: replay reaches EOF immediately.
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    std::fs::write(data_dir.join("events.ndjson"), "").expect("write ndjson");
    let config = load_config(&dir, "");

    let router = empty_router();
    let cancel = CancellationToken::new();
    let base_dir = dir.path();
    let group = spawn_receiver_task(&config, router, cancel.clone(), None, &[], base_dir)
        .await
        .expect("spawn receiver task");
    cancel.cancel();
    // The replay task observes the cancel token and exits.
    group.wait(cancel).await.expect("receiver group joins");
}

// ---------------------------------------------------------------------------
// metrics_record_to_data_record
// ---------------------------------------------------------------------------

#[test]
fn metrics_record_to_data_record_maps_fields() {
    let record = MetricsRecord {
        fields: vec![
            ("stage".to_string(), "receiver".to_string()),
            ("name".to_string(), "connections_total".to_string()),
            ("value".to_string(), "3".to_string()),
        ],
    };
    let data = metrics_record_to_data_record(&record);
    assert_eq!(
        data.field("stage").map(|f| f.get_value().to_string()),
        Some("receiver".to_string())
    );
    assert_eq!(
        data.field("value").map(|f| f.get_value().to_string()),
        Some("3".to_string())
    );
}
