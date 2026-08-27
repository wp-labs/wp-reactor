//! spawn.rs 第二轮深度补测（注册于 spawn.rs 内）。
//!
//! 覆盖点（第一轮 `spawn_coverage` 之外）:
//! - `spawn_alert_task`: 真实 dispatcher（业务 + error + monitor sink 分支）。
//! - `spawn_rule_tasks`: stats（单实例 / 按键分片）、each（单实例 / round-robin
//!   分片 / deferred 单 worker）、match（单实例 / 分片含 limits / 分片 conv 阶段）。
//! - `spawn_receiver_task`: unsupported format / 全部禁用 / 未知 source kind 错误路径。
//! - `stats_row_fields` 带 last/top 且剔除桶键字段; `source_param_to_json` 更多类型。

use super::*;

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use wf_config::{ConfigVarContext, FusionConfig, WindowConfig};
use wf_engine::match_engine::RuleExecutor;
use wf_engine::sink::SinkDispatcher;
use wf_engine::window::WindowDef;
use wf_lang::ast::{CloseMode, Expr, FieldRef, MatchMode};
use wf_lang::plan::{
    BindPlan, EachPlan, EntityPlan, LimitsPlan, MatchPlan, RulePlan, ScorePlan, StatsAggPlan,
    StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec, YieldField, YieldPlan,
};

use crate::alert_task::SinkFanout;
use crate::lifecycle::types::{RunRule, RunRuleKind};
use crate::sink_build::{SinkFactoryRegistry, build_sink_dispatcher};
use wp_core_connectors::sinks::file_factory::FileFactory;

// ---------------------------------------------------------------------------
// 配置辅助
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

fn load_config(dir: &tempfile::TempDir, top_toml: &str, sources_toml: &str) -> FusionConfig {
    let windows_path = dir.path().join("windows.toml");
    std::fs::write(&windows_path, WINDOWS_TOML).expect("write windows.toml");
    let config_path = dir.path().join("wfusion.toml");
    std::fs::write(
        &config_path,
        format!(
            r#"
sinks = "sinks"
windows = "windows.toml"
{top_toml}
[runtime]
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/*.wfl"

{sources_toml}
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

fn test_window_config(name: &str) -> WindowConfig {
    WindowConfig {
        name: name.into(),
        mode: wf_config::DistMode::Local,
        max_window_bytes: (64 * 1024 * 1024).into(),
        over_cap: std::time::Duration::from_secs(3600).into(),
        evict_policy: wf_config::EvictPolicy::TimeFirst,
        watermark: std::time::Duration::ZERO.into(),
        allowed_lateness: std::time::Duration::from_secs(3600).into(),
        late_policy: wf_config::LatePolicy::Drop,
        table: None,
    }
}

fn router_with_window(name: &str) -> Arc<Router> {
    let def = WindowDef {
        params: wf_engine::window::WindowParams {
            name: name.into(),
            schema: test_schema(),
            time_col_index: Some(1),
            over: std::time::Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec!["syslog".into()],
        config: test_window_config(name),
    };
    let registry = WindowRegistry::build(vec![def]).expect("build registry");
    Arc::new(Router::new(registry))
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
            keys: vec![FieldRef::Simple("sip".into())],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(60)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: CloseMode::And,
            match_mode: MatchMode::Seq,
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
            entity_id_expr: Expr::Field(FieldRef::Qualified("b".into(), "sip".into())),
        },
        yield_plan: YieldPlan {
            target: "alerts".into(),
            version: None,
            fields: vec![YieldField {
                name: "sip".into(),
                value: Expr::Field(FieldRef::Qualified("b".into(), "sip".into())),
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

fn make_rule(kind: RunRuleKind, plan: RulePlan) -> RunRule {
    RunRule {
        kind,
        executor: RuleExecutor::new(plan),
        window_aliases: HashMap::from([("w1".to_string(), vec!["b".to_string()])]),
    }
}

/// 跑 `spawn_rule_tasks` 并取消回收；断言所有任务干净退出。
async fn run_group(rules: Vec<RunRule>, router: Arc<Router>, shard_count: usize) {
    let (eos_tx, _) = watch::channel(0u64);
    let cancel = CancellationToken::new();
    let group = spawn_rule_tasks(
        rules,
        &router,
        &HashSet::new(),
        Arc::new(wf_engine::pipe::PipeRegistry::new()),
        SinkFanout::closed(),
        cancel.clone(),
        cancel.clone(),
        None,
        eos_tx,
        shard_count,
    );
    cancel.cancel();
    group.wait(cancel).await.expect("rule tasks join cleanly");
}

// ---------------------------------------------------------------------------
// spawn_alert_task — 真实 dispatcher
// ---------------------------------------------------------------------------

/// 写入一个含业务 + error + monitor sink 的分层 sink 配置。
fn write_sink_layout(root: &std::path::Path) {
    let connector = root.join("connectors/sink.d/file_json.toml");
    std::fs::create_dir_all(connector.parent().unwrap()).expect("dir");
    std::fs::write(
        &connector,
        r#"
[[connectors]]
id = "file_json"
type = "file"
allow_override = ["file"]

[connectors.params]
fmt = "json"
file = "default.jsonl"
"#,
    )
    .expect("write connector");
    let sinks_dir = root.join("sinks");
    std::fs::create_dir_all(&sinks_dir).expect("sinks dir");
    std::fs::write(sinks_dir.join("defaults.toml"), "tags = [\"env:dev\"]\n").expect("defaults");
    let business = root.join("sinks/business.d/catch_all.toml");
    std::fs::create_dir_all(business.parent().unwrap()).expect("dir");
    std::fs::write(
        &business,
        r#"
[sink_group]
name = "catch_all"
windows = ["*"]

[[sink_group.sinks]]
connect = "file_json"
name = "all_alerts"

[sink_group.sinks.params]
file = "all.jsonl"
"#,
    )
    .expect("write business");
    let infra = sinks_dir.join("infra.d");
    std::fs::create_dir_all(&infra).expect("dir");
    std::fs::write(
        infra.join("error.toml"),
        r#"
[sink_group]
name = "__error"

[[sink_group.sinks]]
connect = "file_json"

[sink_group.sinks.params]
file = "error.jsonl"
"#,
    )
    .expect("write error");
    std::fs::write(
        infra.join("monitor.toml"),
        r#"
[sink_group]
name = "__monitor"

[[sink_group.sinks]]
connect = "file_json"

[sink_group.sinks.params]
file = "monitor.jsonl"
"#,
    )
    .expect("write monitor");
}

async fn build_dispatcher(root: &std::path::Path) -> Arc<SinkDispatcher> {
    let sinks_dir = root.join("sinks");
    let ctx = ConfigVarContext::new();
    let bundle = wf_config::sink::load_sink_config_with_context(&sinks_dir, &ctx, Some(root))
        .expect("load sink bundle");
    let mut registry = SinkFactoryRegistry::new();
    registry.register(Arc::new(FileFactory));
    registry.import_from_global_registry();
    Arc::new(
        build_sink_dispatcher(&bundle, &registry, root, &[])
            .await
            .expect("build dispatcher"),
    )
}

#[tokio::test]
async fn spawn_alert_task_covers_error_and_monitor_branches() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_sink_layout(dir.path());
    let dispatcher = build_dispatcher(dir.path()).await;
    assert!(dispatcher.has_monitor_sinks(), "monitor sink configured");

    let cancel = CancellationToken::new();
    let (fanout, group) = spawn_alert_task(dispatcher, None, cancel.clone());
    // 业务 sink 解析命中。
    assert!(
        !fanout.resolve("alerts").is_empty(),
        "alerts routes to the business sink"
    );
    cancel.cancel();
    // 释放 fanout（其持有 sink 通道发送端）→ 消费者 drain 立即结束。
    drop(fanout);
    group
        .wait(cancel)
        .await
        .expect("alert consumers exit on cancel");
}

// ---------------------------------------------------------------------------
// spawn_rule_tasks — 三种形态
// ---------------------------------------------------------------------------

#[tokio::test]
async fn spawn_rule_tasks_stats_unsharded() {
    let mut plan = minimal_rule_plan("stats_rule");
    plan.stats_plan = Some(StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(10)),
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
    });
    let rule = make_rule(
        RunRuleKind::Stats {
            stats_plan: plan.stats_plan.clone().expect("stats"),
            time_field: Some("event_time".into()),
        },
        plan,
    );
    run_group(vec![rule], router_with_window("w1"), 1).await;
}

#[tokio::test]
async fn spawn_rule_tasks_stats_sharded() {
    let mut plan = minimal_rule_plan("stats_sharded");
    plan.stats_plan = Some(StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(10)),
        // 简单字段键 → 可按键分片。
        keys: vec![Expr::Field(FieldRef::Qualified("b".into(), "sip".into()))],
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
    });
    let stats_plan = plan.stats_plan.clone().expect("stats");
    let rule = make_rule(
        RunRuleKind::Stats {
            stats_plan,
            time_field: Some("event_time".into()),
        },
        plan,
    );
    run_group(vec![rule], router_with_window("w1"), 2).await;
}

#[tokio::test]
async fn spawn_rule_tasks_each_unsharded() {
    let mut plan = minimal_rule_plan("each_rule");
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    let rule = make_rule(
        RunRuleKind::Each {
            alias: "b".into(),
            time_field: Some("event_time".into()),
        },
        plan,
    );
    run_group(vec![rule], router_with_window("w1"), 1).await;
}

#[tokio::test]
async fn spawn_rule_tasks_each_round_robin_sharded() {
    let mut plan = minimal_rule_plan("each_rr");
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    let rule = make_rule(
        RunRuleKind::Each {
            alias: "b".into(),
            time_field: Some("event_time".into()),
        },
        plan,
    );
    run_group(vec![rule], router_with_window("w1"), 2).await;
}

#[tokio::test]
async fn spawn_rule_tasks_each_deferred_single_worker() {
    let mut plan = minimal_rule_plan("each_deferred");
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter: None,
    });
    // 带 `emit at` 的 join → 强制单 worker（与 round-robin 分片冲突）。
    plan.joins = vec![wf_lang::plan::JoinPlan {
        right_window: "w2".into(),
        mode: wf_lang::ast::JoinMode::Inner,
        conds: vec![wf_lang::plan::JoinCondPlan {
            left: FieldRef::Qualified("b".into(), "sip".into()),
            right: FieldRef::Qualified("w2".into(), "sip".into()),
        }],
        within: None,
        reduce: None,
        emit_at: Some(Expr::Number(1.0)),
    }];
    let rule = make_rule(
        RunRuleKind::Each {
            alias: "b".into(),
            time_field: Some("event_time".into()),
        },
        plan,
    );
    run_group(vec![rule], router_with_window("w1"), 2).await;
}

#[tokio::test]
async fn spawn_rule_tasks_match_unsharded() {
    let plan = minimal_rule_plan("match_rule");
    let rule = make_rule(
        RunRuleKind::Match {
            match_plan: plan.match_plan.clone(),
            time_field: Some("event_time".into()),
            limits: None,
        },
        plan,
    );
    run_group(vec![rule], router_with_window("w1"), 1).await;
}

#[tokio::test]
async fn spawn_rule_tasks_match_sharded_with_limits() {
    let mut plan = minimal_rule_plan("match_limits");
    plan.limits_plan = Some(LimitsPlan {
        max_memory_bytes: Some(1 << 20),
        max_instances: Some(100),
        max_throttle: None,
        on_exceed: wf_lang::plan::ExceedAction::DropOldest,
    });
    let limits = plan.limits_plan.clone();
    let rule = make_rule(
        RunRuleKind::Match {
            match_plan: plan.match_plan.clone(),
            time_field: Some("event_time".into()),
            limits,
        },
        plan,
    );
    run_group(vec![rule], router_with_window("w1"), 2).await;
}

#[tokio::test]
async fn spawn_rule_tasks_match_sharded_conv_stage() {
    let mut plan = minimal_rule_plan("match_conv");
    // conv_window → shardable conv 规则：spawn 聚合阶段任务 + 分片规则任务。
    plan.conv_window = Some(wf_lang::plan::ConvWindowPlan {
        over: std::time::Duration::from_secs(60),
        slide: None,
        keys: vec![FieldRef::Simple("sip".into())],
    });
    let rule = make_rule(
        RunRuleKind::Match {
            match_plan: plan.match_plan.clone(),
            time_field: Some("event_time".into()),
            limits: None,
        },
        plan,
    );
    run_group(vec![rule], router_with_window("w1"), 2).await;
}

// ---------------------------------------------------------------------------
// spawn_receiver_task 错误路径
// ---------------------------------------------------------------------------

#[tokio::test]
async fn spawn_receiver_task_unsupported_format_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("dir");
    std::fs::write(data_dir.join("events.ndjson"), "").expect("seed");
    let config = load_config(
        &dir,
        "",
        r#"
[[sources]]
type = "file"
enable = true
path = "data/events.ndjson"
data_format = "parquet"
"#,
    );
    let cancel = CancellationToken::new();
    let group = spawn_receiver_task(&config, empty_router(), cancel, None, &[], dir.path())
        .await
        .expect("spawn receiver");
    // 任务内部立即失败 → group.wait 返回错误（外层包装为 shutdown 上下文）。
    let err = group
        .wait(CancellationToken::new())
        .await
        .expect_err("must fail");
    assert!(err.to_string().contains("task failed"), "got: {err}");
}

#[tokio::test]
async fn spawn_receiver_task_all_sources_disabled_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = load_config(
        &dir,
        "",
        r#"
[[sources]]
type = "file"
enable = true
path = "data/events.ndjson"
"#,
    );
    // 加载期校验要求至少一个启用源；加载后禁用 → 命中 spawned==0 分支。
    config.sources[0].enabled = false;
    let cancel = CancellationToken::new();
    match spawn_receiver_task(&config, empty_router(), cancel, None, &[], dir.path()).await {
        Err(err) => {
            assert!(err.to_string().contains("no enabled sources"), "got: {err}");
        }
        Ok(_) => panic!("no enabled sources must fail"),
    }
}

#[tokio::test]
async fn spawn_receiver_task_unknown_source_kind_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = load_config(
        &dir,
        "",
        r#"
[[sources]]
type = "no_such_kind"
enable = true
"#,
    );
    let cancel = CancellationToken::new();
    match spawn_receiver_task(&config, empty_router(), cancel, None, &[], dir.path()).await {
        Err(err) => {
            assert!(
                err.to_string().contains("no factory registered"),
                "got: {err}"
            );
        }
        Ok(_) => panic!("unknown kind must fail"),
    }
}

// ---------------------------------------------------------------------------
// stats_row_fields / source_param_to_json
// ---------------------------------------------------------------------------

#[test]
fn stats_row_fields_extracts_subset_and_excludes_key_fields() {
    // last 度量 → 提取 yield/entity/score/度量字段子集，剔除桶键字段。
    let mut plan = minimal_rule_plan("stats_rows");
    plan.stats_plan = Some(StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(60)),
        keys: vec![
            Expr::Field(FieldRef::Qualified("b".into(), "bidder".into())),
            Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        ],
        output_shape: StatsOutputShapePlan::Rows,
        measures: vec![
            StatsMeasurePlan {
                label: "last_price".into(),
                source_alias: "b".into(),
                where_expr: None,
                agg: StatsAggPlan::Last,
                field: Some(FieldRef::Qualified("b".into(), "price".into())),
                arg: None,
            },
            StatsMeasurePlan {
                label: "cnt".into(),
                source_alias: "b".into(),
                where_expr: None,
                agg: StatsAggPlan::Count,
                field: None,
                arg: None,
            },
        ],
        tracked_bind_fields: HashMap::new(),
    });
    let stats_plan = plan.stats_plan.clone().expect("stats");
    let fields = stats_row_fields(&plan, &stats_plan).expect("row measures present");
    // yield 引用了 b.sip → 在子集中。
    assert!(
        fields.contains("sip"),
        "yield field must be included: {fields:?}"
    );
    // 度量字段 price 在子集中。
    assert!(
        fields.contains("price"),
        "measure field must be included: {fields:?}"
    );
    // 桶键字段不入行。
    assert!(
        !fields.contains("bidder"),
        "key field must be excluded: {fields:?}"
    );
    assert!(
        !fields.contains("auction"),
        "key field must be excluded: {fields:?}"
    );
}

#[test]
fn source_param_to_json_extra_types() {
    assert_eq!(source_param_to_json("false"), serde_json::json!(false));
    assert_eq!(source_param_to_json("1.5"), serde_json::json!(1.5));
    assert_eq!(
        source_param_to_json("some string"),
        serde_json::json!("some string")
    );
    // 前导/尾随空格会被 trim 后解析。
    assert_eq!(source_param_to_json("  42  "), serde_json::json!(42));
}
