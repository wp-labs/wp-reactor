//! bootstrap.rs 第四轮补测（注册于 bootstrap.rs 内, `#[path]` 方式）。
//!
//! 覆盖点（第二轮 `bootstrap_coverage_more` 之外）:
//! - `load_and_compile` 的 over vs over_cap 校验失败路径。
//! - `init_knowledge_redis_if_configured`: 文件缺失 / TOML 非法 / 无 redis 配置 /
//!   有 redis 配置（init 失败回退 warn）各分支。
//! - `load_knowledge_into_windows`: 文件读取失败 / TOML 解析失败 /
//!   PG 配置下 PG init 失败回退 CSV 的成功与告警分支。
//! - `load_from_postgres`: PG provider 初始化失败的错误路径。
//! - `configure_join_indexes`: join 无条件时的空转。

use super::*;

use std::collections::{HashMap, HashSet};
use std::path::Path;

use tempfile::TempDir;

use wf_config::{ConfigVarContext, FusionConfigLoader};
use wf_engine::window::WindowDef;

// ---------------------------------------------------------------------------
// 最小工程 fixture（schema + rule + sinks + windows.toml）
// ---------------------------------------------------------------------------

const SECURITY_SCHEMA: &str = r#"
window auth_events {
    stream_tag = "syslog"
    time = event_time
    over = 5m

    fields {
        sip: ip
        username: chars
        action: chars
        event_time: time
    }
}

window security_alerts {
    over = 0
    fields {
        sip: ip
        fail_count: digit
        message: chars
    }
}
"#;

const BRUTE_FORCE_RULE: &str = r#"
rule brute_force_then_scan {
  events {
    fail : auth_events && action == "failed"
  }

  match<sip:5m> {
    on event {
      fail | count >= ${FAIL_THRESHOLD:3};
    }
    and close {
      fail | count >= 1;
    }
  } -> score(70.0)

  entity(ip, fail.sip)

  yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
    message = fmt("{} brute force detected", fail.sip)
  )
}
"#;

const WINDOWS_TOML: &str = r#"[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.auth_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "1m"

[window.security_alerts]
mode = "local"
max_window_bytes = "64MB"
over_cap = "1h"
"#;

fn write_sink_layout(root: &Path) {
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
    let sinks = root.join("sinks");
    std::fs::create_dir_all(sinks.join("business.d")).expect("dir");
    std::fs::write(sinks.join("defaults.toml"), "tags = [\"env:dev\"]\n").expect("defaults");
    std::fs::write(
        sinks.join("business.d/catch_all.toml"),
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
}

fn write_fixture(root: &TempDir, windows_extra: &str) {
    std::fs::create_dir_all(root.path().join("schemas")).expect("schemas dir");
    std::fs::create_dir_all(root.path().join("rules")).expect("rules dir");
    std::fs::create_dir_all(root.path().join("models")).expect("models dir");
    std::fs::write(root.path().join("schemas/security.wfs"), SECURITY_SCHEMA).expect("schema");
    std::fs::write(root.path().join("rules/brute_force.wfl"), BRUTE_FORCE_RULE).expect("rule");
    std::fs::write(root.path().join("seed.ndjson"), "").expect("seed");
    write_sink_layout(root.path());
    std::fs::write(
        root.path().join("models/windows.toml"),
        format!("{WINDOWS_TOML}\n{windows_extra}"),
    )
    .expect("windows");
    std::fs::write(
        root.path().join("wfusion.toml"),
        r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "file"
name = "seed"
path = "seed.ndjson"
stream_tag = "syslog"
data_format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/*.wfl"

[vars]
FAIL_THRESHOLD = "3"
"#,
    )
    .expect("wfusion.toml");
}

fn load_config(root: &TempDir) -> FusionConfig {
    let cfg_path = root.path().join("wfusion.toml");
    let ctx = ConfigVarContext::new();
    let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(root.path()));
    loader.load().expect("load config")
}

// ---------------------------------------------------------------------------
// load_and_compile — over vs over_cap 校验失败
// ---------------------------------------------------------------------------

#[tokio::test]
async fn load_and_compile_over_exceeds_over_cap_errors() {
    let dir = TempDir::new().expect("tempdir");
    // auth_events over = 5m（schema）, over_cap = 1m（WINDOWS_TOML）→ 校验失败。
    // 不能经 windows_extra 再定义 [window.auth_events]（TOML 重复键）；
    // over_cap 已在常量里改为 1m。
    write_fixture(&dir, "");
    let config = load_config(&dir);
    let res = load_and_compile(&config, dir.path()).await;
    let err = match res {
        Err(e) => e,
        Ok(_) => panic!("over > over_cap must fail validation"),
    };
    assert!(err.to_string().contains("over vs over_cap"), "got: {err}");
}

// ---------------------------------------------------------------------------
// init_knowledge_redis_if_configured — 各分支
// ---------------------------------------------------------------------------

#[test]
fn init_knowledge_redis_missing_file_warns() {
    let dir = TempDir::new().expect("tempdir");
    let missing = dir.path().join("no-such-knowdb.toml");
    // 不 panic（读失败 → warn 后返回）。
    init_knowledge_redis_if_configured(&missing, dir.path());
}

#[test]
fn init_knowledge_redis_invalid_toml_warns() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("knowdb.toml");
    std::fs::write(&path, "this is ] not [ toml").expect("write");
    init_knowledge_redis_if_configured(&path, dir.path());
}

#[test]
fn init_knowledge_redis_without_provider_returns() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("knowdb.toml");
    std::fs::write(&path, "base_dir = \"data\"\n[[tables]]\nname = \"t\"\n").expect("write");
    init_knowledge_redis_if_configured(&path, dir.path());
}

#[test]
fn init_knowledge_redis_configured_init_attempt() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("knowdb.toml");
    // 配置了 [provider.redis] → 走 init（无 Redis 后端 → 非致命 warn 返回）。
    std::fs::write(
        &path,
        r#"
[provider.redis]
uri = "redis://127.0.0.1:6399"
[[tables]]
name = "t"
"#,
    )
    .expect("write");
    init_knowledge_redis_if_configured(&path, dir.path());
}

// ---------------------------------------------------------------------------
// load_knowledge_into_windows — 错误路径 + PG 回退
// ---------------------------------------------------------------------------

#[test]
fn load_knowledge_missing_file_errors() {
    let dir = TempDir::new().expect("tempdir");
    let missing = dir.path().join("no-knowdb.toml");
    let mut registry = WindowRegistry::build(vec![]).expect("registry");
    let err = load_knowledge_into_windows(&missing, dir.path(), &mut registry)
        .expect_err("missing knowdb.toml must fail");
    assert!(err.to_string().contains("read "), "got: {err}");
}

#[test]
fn load_knowledge_invalid_toml_errors() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("knowdb.toml");
    std::fs::write(&path, "not [ valid").expect("write");
    let mut registry = WindowRegistry::build(vec![]).expect("registry");
    let err = load_knowledge_into_windows(&path, dir.path(), &mut registry)
        .expect_err("invalid knowdb.toml must fail");
    assert!(err.to_string().contains("parse "), "got: {err}");
}

#[test]
fn load_knowledge_pg_failure_falls_back_to_csv() {
    let dir = TempDir::new().expect("tempdir");
    let data_dir = dir.path().join("data/countries");
    std::fs::create_dir_all(&data_dir).expect("dir");
    std::fs::write(
        data_dir.join("data.csv"),
        "code,name\ncn,China\nus,United States\n",
    )
    .expect("csv");
    let path = dir.path().join("knowdb.toml");
    // provider.kind = postgres + 无效 connection_uri → PG init 失败 → 告警回退 CSV。
    std::fs::write(
        &path,
        r#"
base_dir = "data"
[provider]
kind = "postgres"
connection_uri = "postgresql://nobody:nowhere@127.0.0.1:1/nope?connect_timeout=1"
pool_size = 1
[[tables]]
name = "countries"
dir = "countries"
data_file = "data.csv"
"#,
    )
    .expect("knowdb");
    let mut registry = WindowRegistry::build(vec![]).expect("registry");
    load_knowledge_into_windows(&path, dir.path(), &mut registry)
        .expect("PG failure falls back to CSV");
    assert!(
        registry.get_provider("countries").is_some(),
        "CSV fallback registers the provider window"
    );
}

#[test]
fn load_knowledge_empty_csv_is_skipped() {
    let dir = TempDir::new().expect("tempdir");
    let data_dir = dir.path().join("data/empty_t");
    std::fs::create_dir_all(&data_dir).expect("dir");
    std::fs::write(data_dir.join("data.csv"), "code,name\n").expect("csv");
    let path = dir.path().join("knowdb.toml");
    std::fs::write(
        &path,
        r#"
base_dir = "data"
[[tables]]
name = "empty_t"
dir = "empty_t"
data_file = "data.csv"
"#,
    )
    .expect("knowdb");
    let mut registry = WindowRegistry::build(vec![]).expect("registry");
    load_knowledge_into_windows(&path, dir.path(), &mut registry)
        .expect("empty CSV loads without rows");
    assert!(registry.get_provider("empty_t").is_none());
}

// ---------------------------------------------------------------------------
// load_from_postgres — PG init 失败
// ---------------------------------------------------------------------------

#[test]
fn load_from_postgres_init_failure_errors() {
    let config: toml::Value = toml::from_str(
        r#"
[provider]
kind = "postgres"
connection_uri = "postgresql://nobody:nowhere@127.0.0.1:1/nope?connect_timeout=1"
pool_size = 1
[[tables]]
name = "t"
"#,
    )
    .expect("toml");
    let tables = config.get("tables").and_then(|t| t.as_array()).unwrap();
    let mut registry = WindowRegistry::build(vec![]).expect("registry");
    let err = load_from_postgres(&config, tables, &mut registry)
        .expect_err("bogus PG URI must fail init");
    assert!(err.to_string().contains("init PG provider"), "got: {err}");
}

// ---------------------------------------------------------------------------
// configure_join_indexes — join 无条件 / 空条件
// ---------------------------------------------------------------------------

#[test]
fn configure_join_indexes_empty_conds_is_noop() {
    let def = WindowDef {
        params: wf_engine::window::WindowParams {
            name: "w".into(),
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            time_col_index: None,
            over: Duration::ZERO,
            materialize_fields: None,
            defer_materialization: false,
        },
        streams: vec!["s".to_string()],
        config: wf_config::WindowConfig {
            name: "w".into(),
            mode: wf_config::DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: wf_config::EvictPolicy::TimeFirst,
            watermark: Duration::ZERO.into(),
            allowed_lateness: Duration::from_secs(3600).into(),
            late_policy: wf_config::LatePolicy::Drop,
            table: None,
        },
    };
    let registry = WindowRegistry::build(vec![def]).expect("registry");
    let router = Router::new(registry);
    // join 带空 conds → 不设置 join key（空转, 不 panic）。
    let mut plan = base_plan();
    plan.joins = vec![wf_lang::plan::JoinPlan {
        right_window: "w".into(),
        mode: wf_lang::ast::JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    configure_join_indexes(&router, &[plan]);
}

fn base_plan() -> wf_lang::plan::RulePlan {
    use wf_lang::ast::{CloseMode, Expr, MatchMode};
    use wf_lang::plan::{
        BindPlan, EntityPlan, MatchPlan, RulePlan, ScorePlan, WindowSpec, YieldPlan,
    };
    RulePlan {
        name: "r4".into(),
        binds: vec![BindPlan {
            alias: "b".into(),
            window: "auth_events".into(),
            filter: None,
        }],
        lets: vec![],
        match_plan: MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
            window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
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
            target: "security_alerts".into(),
            version: None,
            fields: vec![],
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
