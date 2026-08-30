//! bootstrap.rs 第二轮深度补测（注册于 bootstrap.rs 内）。
//!
//! 覆盖点（第一轮 `bootstrap_coverage` 之外）:
//! - `load_and_compile` 完整链路: 最小工程成功 / provider 窗口（knowdb CSV 加载）/
//!   无 sink 配置启动失败 / 无 knowdb.toml 时跳过 provider 加载。

use super::*;

use std::path::Path;

use tempfile::TempDir;

use wf_config::{ConfigVarContext, FusionConfigLoader};

// ---------------------------------------------------------------------------
// 最小工程 fixture（schema + rule + sinks + windows.toml + seed）
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
over_cap = "30m"

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

fn load_config(root: &TempDir) -> (wf_config::RawFusionConfigTree, wf_config::FusionConfig) {
    let cfg_path = root.path().join("wfusion.toml");
    let ctx = ConfigVarContext::new();
    let loader = FusionConfigLoader::new(&cfg_path, &[], &ctx, Some(root.path()));
    (
        loader.load_raw().expect("load raw"),
        loader.load().expect("load config"),
    )
}

// ---------------------------------------------------------------------------
// load_and_compile
// ---------------------------------------------------------------------------

#[tokio::test]
async fn load_and_compile_minimal_success() {
    let dir = TempDir::new().expect("tempdir");
    write_fixture(&dir, "");

    let (_raw, config) = load_config(&dir);
    let data = load_and_compile(&config, dir.path())
        .await
        .expect("load and compile");

    assert_eq!(data.rules.len(), 1, "one compiled rule");
    assert_eq!(data.rules[0].executor.plan().name, "brute_force_then_scan");
    assert!(
        data.schema_count >= 2,
        "auth_events + security_alerts schemas"
    );
    assert!(data.intermediate_targets.is_empty());
    assert!(
        data.router.registry().get_window("auth_events").is_some(),
        "buffer window registered"
    );
    assert!(
        !data.dispatcher.resolve_sinks("security_alerts").is_empty(),
        "sink dispatcher routes the yield target"
    );
}

#[tokio::test]
async fn load_and_compile_skips_provider_without_knowdb() {
    let dir = TempDir::new().expect("tempdir");
    // windows.toml 声明了一个 table 窗口，但没有 knowdb.toml → 跳过加载。
    write_fixture(
        &dir,
        "[window.countries]\nmode = \"local\"\nover_cap = \"1h\"\ntable = \"countries\"\n",
    );

    let (_raw, config) = load_config(&dir);
    let data = load_and_compile(&config, dir.path())
        .await
        .expect("load and compile without knowdb");
    assert!(
        data.router.registry().get_provider("countries").is_none(),
        "no knowdb.toml → provider window not registered"
    );
}

#[tokio::test]
async fn load_and_compile_loads_knowledge_provider_window() {
    let dir = TempDir::new().expect("tempdir");
    write_fixture(
        &dir,
        "[window.countries]\nmode = \"local\"\nover_cap = \"1h\"\ntable = \"countries\"\n",
    );

    let data_dir = dir.path().join("data/countries");
    std::fs::create_dir_all(&data_dir).expect("dir");
    std::fs::write(
        data_dir.join("data.csv"),
        "code,name\ncn,China\nus,United States\n",
    )
    .expect("csv");
    std::fs::write(
        dir.path().join("knowdb.toml"),
        r#"
        base_dir = "data"
        [[tables]]
        name = "countries"
        enabled = true
        dir = "countries"
        data_file = "data.csv"
        "#,
    )
    .expect("knowdb");

    let (_raw, config) = load_config(&dir);
    let data = load_and_compile(&config, dir.path())
        .await
        .expect("load and compile with knowdb");
    let provider = data
        .router
        .registry()
        .get_provider("countries")
        .expect("provider window registered from CSV");
    let snapshot = provider.read().expect("lock").snapshot();
    assert_eq!(snapshot.len(), 2, "both CSV rows loaded");
}

#[tokio::test]
async fn load_and_compile_loads_knowledge_provider_window_from_models_schemas() {
    let dir = TempDir::new().expect("tempdir");
    write_fixture(
        &dir,
        "[window.countries]\nmode = \"local\"\nover_cap = \"1h\"\ntable = \"countries\"\n",
    );

    let data_dir = dir.path().join("data/countries");
    std::fs::create_dir_all(&data_dir).expect("dir");
    std::fs::write(
        data_dir.join("data.csv"),
        "code,name\ncn,China\nus,United States\n",
    )
    .expect("csv");
    // knowdb.toml 放 models/schemas/（nexmark_pk 2026-08-30 迁移位置）；
    // base_dir 相对本文件目录 → "../../data" 指回工程根 data/。
    let schemas_dir = dir.path().join("models/schemas");
    std::fs::create_dir_all(&schemas_dir).expect("dir");
    std::fs::write(
        schemas_dir.join("knowdb.toml"),
        r#"
        base_dir = "../../data"
        [[tables]]
        name = "countries"
        enabled = true
        dir = "countries"
        data_file = "data.csv"
        "#,
    )
    .expect("knowdb");

    let (_raw, config) = load_config(&dir);
    let data = load_and_compile(&config, dir.path())
        .await
        .expect("load and compile with knowdb in models/schemas");
    let provider = data
        .router
        .registry()
        .get_provider("countries")
        .expect("provider window registered from CSV in models/schemas");
    let snapshot = provider.read().expect("lock").snapshot();
    assert_eq!(snapshot.len(), 2, "both CSV rows loaded");
}

#[tokio::test]
async fn load_and_compile_without_sinks_errors() {
    let dir = TempDir::new().expect("tempdir");
    // 无 sink 布局（空 sinks 目录）→ build_sink_dispatcher 启动守卫失败。
    std::fs::create_dir_all(dir.path().join("schemas")).expect("schemas dir");
    std::fs::create_dir_all(dir.path().join("rules")).expect("rules dir");
    std::fs::create_dir_all(dir.path().join("models")).expect("models dir");
    std::fs::create_dir_all(dir.path().join("sinks")).expect("sinks dir");
    std::fs::write(dir.path().join("schemas/security.wfs"), SECURITY_SCHEMA).expect("schema");
    std::fs::write(dir.path().join("rules/brute_force.wfl"), BRUTE_FORCE_RULE).expect("rule");
    std::fs::write(dir.path().join("seed.ndjson"), "").expect("seed");
    std::fs::write(dir.path().join("models/windows.toml"), WINDOWS_TOML).expect("windows");
    std::fs::write(
        dir.path().join("wfusion.toml"),
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
"#,
    )
    .expect("wfusion.toml");

    let (_raw, config) = load_config(&dir);
    match load_and_compile(&config, dir.path()).await {
        Err(err) => {
            assert!(
                err.to_string().contains("no sinks configured"),
                "got: {err}"
            );
        }
        Ok(_) => panic!("no sinks must fail bootstrap"),
    }
}
