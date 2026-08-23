//! spawn.rs 第四轮补测（注册于 spawn.rs 内, `#[path]` 方式）。
//!
//! 覆盖点（第二轮 `spawn_coverage_more` 之外）:
//! - `spawn_window_actors` 带 metrics 的 append 报告闭包（Some 分支）。
//! - `spawn_rule_tasks`: stats 键含非 Field 表达式（field_keys 过滤 None 分支）;
//!   `WFUSION_WINDOW_DISPATCH=push` 下 stats / each / match 的分片与非分片
//!   push 通道注册分支（含 `register_sharded` / `register_round_robin`）。
//! - `spawn_receiver_task` 的 file 源 csv / arrow_framed / arrow_ipc 分支与
//!   connector id 解析回退（未知 connector → 回退 source_type）。
//! - `spawn_metrics_task` 带 monitor sink 的 monitor 消费者分支。

use super::*;

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use arrow::datatypes::{DataType, Field as ArrowField, Schema, SchemaRef};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use wf_config::{ConfigVarContext, DistMode, EvictPolicy, FusionConfig, LatePolicy, WindowConfig};
use wf_engine::match_engine::RuleExecutor;
use wf_engine::sink::SinkDispatcher;
use wf_engine::window::WindowDef;
use wf_lang::ast::{CloseMode, Expr, FieldRef, MatchMode};
use wf_lang::plan::{
    BindPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, StatsAggPlan, StatsMeasurePlan,
    StatsOutputShapePlan, StatsPlan, WindowSpec, YieldField, YieldPlan,
};

use crate::alert_task::SinkFanout;
use crate::lifecycle::types::{RunRule, RunRuleKind};
use crate::metrics::RuntimeMetrics;
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

fn window_def() -> WindowDef {
    WindowDef {
        params: wf_engine::window::WindowParams {
            name: "w".into(),
            schema: test_schema(),
            time_col_index: Some(1),
            over: std::time::Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec!["events".to_string()],
        config: WindowConfig {
            name: "w".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: std::time::Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: std::time::Duration::from_secs(0).into(),
            allowed_lateness: std::time::Duration::from_secs(3600).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        },
    }
}

fn metrics() -> Arc<RuntimeMetrics> {
    Arc::new(RuntimeMetrics::new(
        &["r4_rule".to_string()],
        &["w".to_string()],
        &[],
        BTreeMap::new(),
    ))
}

fn base_plan(name: &str) -> RulePlan {
    RulePlan {
        name: name.into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "w".into(),
            filter: None,
        }],
        lets: vec![],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(60)),
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
                name: "id".into(),
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

fn router_with_window() -> Arc<Router> {
    let registry = WindowRegistry::build(vec![window_def()]).expect("registry");
    Arc::new(Router::new(registry))
}

fn run_rule(kind: RunRuleKind) -> RunRule {
    let plan = base_plan("r4_rule");
    RunRule {
        kind,
        executor: RuleExecutor::new(plan),
        window_aliases: HashMap::from([("w".to_string(), vec!["b".to_string()])]),
    }
}

/// 序列化 WFUSION_WINDOW_DISPATCH 的读写（并行测试间的全局 env 竞争）。
static ENV_LOCK: Mutex<()> = Mutex::new(());

// ---------------------------------------------------------------------------
// spawn_window_actors — metrics 报告闭包
// ---------------------------------------------------------------------------

#[tokio::test]
async fn spawn_window_actors_with_metrics_report() {
    let dir = tempfile::tempdir().unwrap();
    let config = load_config(&dir, "");
    let router = router_with_window();
    let gate = Arc::new(EvictionGate::new(usize::MAX));
    let cancel = CancellationToken::new();
    let group = spawn_window_actors(&config, &router, gate, cancel.clone(), Some(metrics()));
    cancel.cancel();
    // 让 actor 退出, 避免测试结束时的悬挂任务。
    group
        .wait(cancel)
        .await
        .expect("window actors join cleanly");
}

// ---------------------------------------------------------------------------
// spawn_rule_tasks — 非 Field 键 + push 模式各分支
// ---------------------------------------------------------------------------

#[tokio::test]
async fn spawn_rule_tasks_stats_non_field_key_shard_filter() {
    let router = router_with_window();
    let stats_plan = StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(10)),
        keys: vec![Expr::Number(1.0)], // 非 Field 键 → filter_map None 分支
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
    let rule = run_rule(RunRuleKind::Stats {
        stats_plan,
        time_field: Some("event_time".into()),
    });
    let cancel = CancellationToken::new();
    let (eos_tx, _) = watch::channel(0u64);
    let group = spawn_rule_tasks(
        vec![rule],
        &router,
        &HashSet::new(),
        Arc::new(wf_engine::pipe::PipeRegistry::new()),
        SinkFanout::closed(),
        cancel.clone(),
        None,
        eos_tx,
        2,
    );
    cancel.cancel();
    group.wait(cancel).await.expect("rule tasks join cleanly");
}

#[tokio::test]
#[allow(clippy::await_holding_lock)] // 测试环境变量守卫（std Mutex）须跨越整个 await 测试体
async fn spawn_rule_tasks_push_mode_all_kinds() {
    let _guard = ENV_LOCK.lock().unwrap();
    unsafe {
        std::env::set_var("WFUSION_WINDOW_DISPATCH", "push");
    }
    let restore = PushEnvGuard;
    let router = router_with_window();
    let cancel = CancellationToken::new();
    let (eos_tx, _) = watch::channel(0u64);

    // stats（分片）: keys 全 Field + shard_count>1 → 分片 push 分支 + register_sharded。
    let stats_plan = StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(10)),
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
    };
    let stats_rule = run_rule(RunRuleKind::Stats {
        stats_plan,
        time_field: Some("event_time".into()),
    });

    // each（分片）: 终端目标 + shard_count>1 → round-robin 分片 push。
    let each_rule = run_rule(RunRuleKind::Each {
        alias: "b".into(),
        time_field: Some("event_time".into()),
    });

    // match（分片）: 带 key + shard_count>1 → 分片 push + register_sharded。
    let mut plan = base_plan("r4_match");
    plan.match_plan.keys = vec![FieldRef::Qualified("b".into(), "sip".into())];
    let match_rule = RunRule {
        kind: RunRuleKind::Match {
            match_plan: plan.match_plan.clone(),
            time_field: Some("event_time".into()),
            limits: None,
        },
        executor: RuleExecutor::new(plan),
        window_aliases: HashMap::from([("w".to_string(), vec!["b".to_string()])]),
    };

    // 非分片 stats（空键）: 非分片 push + register。
    let plain_stats = StatsPlan {
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
    };
    let plain_stats_rule = run_rule(RunRuleKind::Stats {
        stats_plan: plain_stats,
        time_field: Some("event_time".into()),
    });

    let group = spawn_rule_tasks(
        vec![stats_rule, each_rule, match_rule, plain_stats_rule],
        &router,
        &HashSet::new(),
        Arc::new(wf_engine::pipe::PipeRegistry::new()),
        SinkFanout::closed(),
        cancel.clone(),
        None,
        eos_tx,
        2,
    );
    cancel.cancel();
    group.wait(cancel).await.expect("rule tasks join cleanly");
    drop(restore);
}

struct PushEnvGuard;
impl Drop for PushEnvGuard {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("WFUSION_WINDOW_DISPATCH");
        }
    }
}

// ---------------------------------------------------------------------------
// spawn_receiver_task — file 源格式分支 + connector 回退
// ---------------------------------------------------------------------------

#[tokio::test]
async fn spawn_receiver_task_csv_format_branch() {
    let dir = tempfile::tempdir().unwrap();
    let config = load_config(
        &dir,
        r#"
[[sources]]
type = "file"
enable = true
path = "data/events.csv"
data_format = "csv"
"#,
    );
    let router = router_with_window();
    let cancel = CancellationToken::new();
    let schemas = vec![wf_lang::WindowSchema {
        name: "w".to_string(),
        streams: vec!["events".to_string()],
        time_field: Some("event_time".to_string()),
        over: std::time::Duration::from_secs(3600),
        fields: vec![
            wf_lang::FieldDef {
                name: "sip".to_string(),
                field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Chars),
            },
            wf_lang::FieldDef {
                name: "event_time".to_string(),
                field_type: wf_lang::FieldType::Base(wf_lang::BaseType::Time),
            },
        ],
    }];
    // 文件不存在 → replay 失败（任务返回 Err, 但格式分支已执行）。
    let group = spawn_receiver_task(
        &config,
        Arc::clone(&router),
        cancel.clone(),
        None,
        &schemas,
        dir.path(),
    )
    .await
    .expect("receiver spawn succeeds");
    cancel.cancel();
    let _ = group.wait(cancel).await;
}

#[tokio::test]
async fn spawn_receiver_task_arrow_formats_and_connector_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let config = load_config(
        &dir,
        r#"
[[sources]]
type = "file"
enable = true
path = "data/a.frame"
data_format = "arrow_framed"
stream_tag = "events"

[[sources]]
type = "file"
enable = true
path = "data/b.arrow"
data_format = "arrow_ipc"
stream_tag = "events"

[[sources]]
type = "kafka"
enable = true
connect = "definitely_not_a_connector"
"#,
    );
    let router = router_with_window();
    let cancel = CancellationToken::new();
    let schemas: Vec<wf_lang::WindowSchema> = vec![];
    // 前两个 file 源（arrow_framed / arrow_ipc 分支）已 spawn; 第三个源
    // 的 connector 解析回退到 kind="kafka", 无 factory → 报错（覆盖
    // resolve_connector_kind 回退 + 未知 kind 错误路径）。
    let res = spawn_receiver_task(
        &config,
        Arc::clone(&router),
        cancel.clone(),
        None,
        &schemas,
        dir.path(),
    )
    .await;
    let err = match res {
        Err(e) => e,
        Ok(_) => panic!("unknown connector kind must fail at spawn"),
    };
    assert!(
        err.to_string().contains("no factory registered"),
        "got: {err}"
    );
}

// ---------------------------------------------------------------------------
// spawn_metrics_task — monitor sink 消费者
// ---------------------------------------------------------------------------

/// 写入一个含 monitor sink 的分层 sink 配置。
fn write_monitor_sink_layout(root: &std::path::Path) {
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
    let infra = sinks_dir.join("infra.d");
    std::fs::create_dir_all(&infra).expect("dir");
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
    // build_sink_dispatcher 要求至少一个 fallback sink（infra.d/default.toml）
    // ——只有 monitor 组会报 "no sinks configured"（spawn_r4 实测）。
    std::fs::write(
        infra.join("default.toml"),
        r#"
[sink_group]
name = "__default"

[[sink_group.sinks]]
connect = "file_json"

[sink_group.sinks.params]
file = "default.jsonl"
"#,
    )
    .expect("write default");
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
async fn spawn_metrics_task_with_monitor_sink() {
    let dir = tempfile::tempdir().unwrap();
    write_monitor_sink_layout(dir.path());
    let dispatcher = build_dispatcher(dir.path()).await;
    assert!(dispatcher.has_monitor_sinks(), "monitor sink configured");
    // metrics 启用 + dispatcher 带 monitor sink → mon_send + run_monitor_consumer。
    // prometheus_listen = 0 端口避免与其它测试端口冲突。
    let config = load_config(
        &dir,
        "[metrics]\nenabled = true\nreport_interval = \"30s\"\nprometheus_listen = \"127.0.0.1:0\"\n",
    );
    let router = router_with_window();
    let cancel = CancellationToken::new();
    let group = spawn_metrics_task(
        &config,
        &router,
        cancel.clone(),
        Some(metrics()),
        Some(dispatcher),
    )
    .await
    .expect("metrics spawn");
    cancel.cancel();
    group
        .wait(cancel)
        .await
        .expect("metrics tasks join cleanly");
}
