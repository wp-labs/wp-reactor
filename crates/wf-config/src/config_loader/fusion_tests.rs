use super::*;
use crate::source::SourceConfig;
use crate::types::{ByteSize, DistMode, EvictPolicy, HumanDuration, LatePolicy};
use std::path::{Path, PathBuf};
use std::time::Duration;

fn make_temp_dir(name: &str) -> PathBuf {
    let unique = format!(
        "wf-config-fusion-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos()
    );
    let dir = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&dir).expect("failed to create temp dir");
    dir
}

fn write_file(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("failed to create parent dir");
    }
    std::fs::write(path, content).expect("failed to write test file");
}

const WINDOWS_TOML: &str = r#"
[window_defaults]
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

[window.fw_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"
watermark = "10s"
allowed_lateness = "30s"
late_policy = "drop"

[window.ip_blocklist]
mode = "replicated"
max_window_bytes = "64MB"
over_cap = "48h"
"#;

fn try_load_with_windows(main_toml: &str, windows_toml: &str) -> ConfigResult<FusionConfig> {
    let root = make_temp_dir("cfg");
    let config_path = root.join("wfusion.toml");
    let windows_path = root.join("models").join("windows.toml");
    write_file(&config_path, main_toml);
    write_file(&windows_path, windows_toml);
    FusionConfigLoader::new(&config_path, &[], &ConfigVarContext::new(), Some(&root)).load()
}

fn load_with_windows(main_toml: &str, windows_toml: &str) -> FusionConfig {
    try_load_with_windows(main_toml, windows_toml).unwrap()
}

const FULL_TOML: &str = r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "tcp"
key = "ingress"

listen = "tcp://127.0.0.1:9800"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"
"#;

#[test]
fn load_full_toml() {
    let cfg: FusionConfig = load_with_windows(FULL_TOML, WINDOWS_TOML);

    // mode
    assert_eq!(cfg.mode, FusionMode::Daemon);

    // runtime
    assert_eq!(cfg.runtime.executor_parallelism, 2);
    assert_eq!(
        cfg.runtime.rule_exec_timeout.as_duration(),
        Duration::from_secs(30),
    );
    assert_eq!(cfg.runtime.schemas, "schemas/*.wfs");
    assert_eq!(cfg.runtime.rules, "rules/*.wfl");

    // window_defaults
    assert_eq!(
        cfg.window_defaults.evict_interval,
        "30s".parse::<HumanDuration>().unwrap(),
    );
    assert_eq!(cfg.window_defaults.evict_policy, EvictPolicy::TimeFirst);
    assert_eq!(cfg.window_defaults.late_policy, LatePolicy::Drop);

    // windows (sorted by name)
    assert_eq!(cfg.windows.len(), 3);
    assert_eq!(cfg.windows[0].name, "auth_events");
    assert_eq!(cfg.windows[0].mode, DistMode::Local);
    assert_eq!(
        cfg.windows[0].over_cap.as_duration(),
        Duration::from_secs(30 * 60),
    );
    // auth_events inherits watermark from defaults
    assert_eq!(
        cfg.windows[0].watermark,
        "5s".parse::<HumanDuration>().unwrap(),
    );

    assert_eq!(cfg.windows[1].name, "fw_events");
    assert_eq!(
        cfg.windows[1].watermark,
        "10s".parse::<HumanDuration>().unwrap(),
    );
    assert_eq!(
        cfg.windows[1].allowed_lateness,
        "30s".parse::<HumanDuration>().unwrap(),
    );

    assert_eq!(cfg.windows[2].name, "ip_blocklist");
    assert_eq!(cfg.windows[2].mode, DistMode::Replicated);
    assert_eq!(
        cfg.windows[2].max_window_bytes,
        "64MB".parse::<ByteSize>().unwrap(),
    );

    // sinks
    assert_eq!(cfg.sinks, "sinks");
    assert!(!cfg.metrics.enabled);
    assert_eq!(
        cfg.metrics.report_interval.as_duration(),
        Duration::from_secs(2)
    );
    assert_eq!(cfg.metrics.prometheus_listen, "127.0.0.1:9901");
    assert_eq!(cfg.sources.len(), 1);
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("tcp") => {
            assert_eq!(name.as_deref(), Some("ingress"));
            assert_eq!(
                params.get("listen").unwrap().as_str(),
                "tcp://127.0.0.1:9800"
            );
            assert!(enabled);
        }
        _ => {}
    }
}

#[test]
fn reject_invalid_tcp_source_listen() {
    let toml = FULL_TOML.replace("tcp://127.0.0.1:9800", "http://bad");
    assert!(try_load_with_windows(&toml, WINDOWS_TOML).is_err());
}

#[test]
fn reject_zero_parallelism() {
    let toml = FULL_TOML.replace("executor_parallelism = 2", "executor_parallelism = 0");
    assert!(try_load_with_windows(&toml, WINDOWS_TOML).is_err());
}

#[test]
fn reject_partitioned_no_key() {
    let windows = WINDOWS_TOML.replace(
        "[window.auth_events]\nmode = \"local\"",
        "[window.auth_events]\nmode = \"partitioned\"",
    );
    assert!(try_load_with_windows(FULL_TOML, &windows).is_err());
}

#[test]
fn reject_unknown_mode() {
    let windows = WINDOWS_TOML.replace(
        "[window.auth_events]\nmode = \"local\"",
        "[window.auth_events]\nmode = \"distributed\"",
    );
    assert!(try_load_with_windows(FULL_TOML, &windows).is_err());
}

#[test]
fn reject_window_exceeds_total() {
    // Set max_total_bytes very small so a window exceeds it.
    let windows = WINDOWS_TOML.replace("max_total_bytes = \"2GB\"", "max_total_bytes = \"32MB\"");
    assert!(try_load_with_windows(FULL_TOML, &windows).is_err());
}

#[test]
fn missing_sources_fails() {
    let toml = r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "sinks"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"
"#;
    assert!(try_load_with_windows(toml, WINDOWS_TOML).is_err());
}

#[test]
fn batch_mode_accepts_file_source() {
    let toml = FULL_TOML
            .replace("mode = \"daemon\"", "mode = \"batch\"")
            .replace(
                "[[sources]]\ntype = \"tcp\"\nkey = \"ingress\"\n\nlisten = \"tcp://127.0.0.1:9800\"\n",
                "[[sources]]\ntype = \"file\"\nkey = \"seed_file\"\n\npath = \"data/auth_events.ndjson\"\nstream = \"syslog\"\nformat = \"ndjson\"\n",
            );
    let cfg: FusionConfig = load_with_windows(&toml, WINDOWS_TOML);
    assert_eq!(cfg.mode, FusionMode::Batch);
    assert_eq!(cfg.sources.len(), 1);
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(name.as_deref(), Some("seed_file"));
            assert_eq!(
                params.get("path").unwrap().as_str(),
                "data/auth_events.ndjson"
            );
            assert_eq!(params.get("stream").unwrap().as_str(), "syslog");
            assert_eq!(params.get("format").unwrap().as_str(), "ndjson");
        }
        _ => {}
    }
}

#[test]
fn batch_mode_rejects_tcp_source() {
    let toml = FULL_TOML.replace("mode = \"daemon\"", "mode = \"batch\"");
    assert!(try_load_with_windows(&toml, WINDOWS_TOML).is_err());
}

#[test]
fn daemon_mode_accepts_file_source() {
    let toml = FULL_TOML.replace(
            "[[sources]]\ntype = \"tcp\"\nname = \"ingress\"\nlisten = \"tcp://127.0.0.1:9800\"\n",
            "[[sources]]\ntype = \"file\"\nname = \"seed_file\"\npath = \"data/auth_events.ndjson\"\nstream = \"syslog\"\nformat = \"ndjson\"\n",
        );
    let cfg: FusionConfig = load_with_windows(&toml, WINDOWS_TOML);
    assert_eq!(cfg.mode, FusionMode::Daemon);
    assert_eq!(cfg.sources.len(), 1);
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(name.as_deref(), Some("seed_file"));
            assert_eq!(
                params.get("path").unwrap().as_str(),
                "data/auth_events.ndjson"
            );
            assert_eq!(params.get("stream").unwrap().as_str(), "syslog");
            assert_eq!(params.get("format").unwrap().as_str(), "ndjson");
        }
        _ => {}
    }
}

#[test]
fn load_with_vars() {
    let toml = format!(
        r#"{}
[vars]
FAIL_THRESHOLD = "5"
SCAN_THRESHOLD = "10"
"#,
        FULL_TOML
    );
    let cfg: FusionConfig = load_with_windows(&toml, WINDOWS_TOML);
    assert_eq!(cfg.vars["FAIL_THRESHOLD"], "5");
    assert_eq!(cfg.vars["SCAN_THRESHOLD"], "10");
}

#[test]
fn config_strings_expand_from_vars() {
    let toml = r#"
mode = "batch"
windows = "models/windows.toml"
sinks = "${CASE_PATH}/sinks"
work_root = "${CASE_PATH}"

[[sources]]

type = "file"
name = "seed_${ENV}"
path = "${CASE_PATH}/data/input.ndjson"
stream = "${STREAM_NAME}"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "${CASE_PATH}/models/schemas/*.wfs"
rules = "${CASE_PATH}/models/rules/*.wfl"

[vars]
CASE_PATH = "/tmp/case-a"
ENV = "dev"
STREAM_NAME = "netflow"
"#;
    let cfg: FusionConfig = load_with_windows(toml, WINDOWS_TOML);
    assert_eq!(cfg.sinks, "/tmp/case-a/sinks");
    assert_eq!(cfg.work_root.as_deref(), Some("/tmp/case-a"));
    assert_eq!(cfg.runtime.schemas, "/tmp/case-a/models/schemas/*.wfs");
    assert_eq!(cfg.runtime.rules, "/tmp/case-a/models/rules/*.wfl");
    assert_eq!(cfg.vars["CASE_PATH"], "/tmp/case-a");
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(name.as_deref(), Some("seed_dev"));
            assert_eq!(
                params.get("path").unwrap().as_str(),
                "/tmp/case-a/data/input.ndjson"
            );
            assert_eq!(params.get("stream").unwrap().as_str(), "netflow");
        }
        _ => {}
    }
}

#[test]
fn config_strings_expand_from_environment() {
    let toml = r#"
mode = "batch"
windows = "models/windows.toml"
sinks = "${WF_CONFIG_TEST_CASE_PATH}/sinks"

[[sources]]
type = "file"

path = "${WF_CONFIG_TEST_CASE_PATH}/data/input.ndjson"
stream = "netflow"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "${WF_CONFIG_TEST_CASE_PATH}/models/schemas/*.wfs"
rules = "${WF_CONFIG_TEST_CASE_PATH}/models/rules/*.wfl"
"#;
    unsafe {
        std::env::set_var("WF_CONFIG_TEST_CASE_PATH", "/tmp/case-env");
    }
    let cfg: FusionConfig = load_with_windows(toml, WINDOWS_TOML);
    unsafe {
        std::env::remove_var("WF_CONFIG_TEST_CASE_PATH");
    }

    assert_eq!(cfg.sinks, "/tmp/case-env/sinks");
    assert_eq!(cfg.runtime.schemas, "/tmp/case-env/models/schemas/*.wfs");
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(
                params.get("path").unwrap().as_str(),
                "/tmp/case-env/data/input.ndjson"
            );
        }
        _ => {}
    }
}

#[test]
fn explicit_vars_override_file_vars_and_expose_builtins() {
    let root = make_temp_dir("context-vars");
    let config_path = root.join("conf/wfusion.toml");
    let windows_path = root.join("workspace/models/windows.toml");
    let work_dir = root.join("workspace");
    std::fs::create_dir_all(&work_dir).expect("failed to create work dir");
    write_file(
        &config_path,
        r#"
mode = "batch"
windows = "models/windows.toml"
sinks = "${CASE_PATH}/sinks"
work_root = "${WORK_DIR}/out"

[[sources]]

type = "file"
name = "seed"
path = "${CONFIG_DIR}/data/input.ndjson"
stream = "netflow"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "${CONFIG_DIR}/models/schemas/*.wfs"
rules = "${WORK_DIR}/rules/*.wfl"

[vars]
CASE_PATH = "/tmp/from-file"
"#,
    );
    write_file(&windows_path, WINDOWS_TOML);

    let mut explicit_vars = HashMap::new();
    explicit_vars.insert("CASE_PATH".to_string(), "/tmp/from-cli".to_string());
    let ctx = ConfigVarContext::from_explicit_vars(explicit_vars);
    let cfg = FusionConfig::load_with_context(&config_path, &ctx, Some(&work_dir)).unwrap();

    assert_eq!(cfg.sinks, "/tmp/from-cli/sinks");
    assert_eq!(
        cfg.work_root.as_deref(),
        Some(work_dir.join("out").to_string_lossy().as_ref())
    );
    assert_eq!(
        cfg.runtime.schemas,
        config_path
            .parent()
            .expect("config dir")
            .join("models/schemas/*.wfs")
            .to_string_lossy()
    );
    assert_eq!(
        cfg.runtime.rules,
        work_dir.join("rules/*.wfl").to_string_lossy()
    );
    assert_eq!(cfg.vars["CASE_PATH"], "/tmp/from-cli");
    assert_eq!(
        cfg.vars["CONFIG_DIR"],
        config_path.parent().unwrap().to_string_lossy()
    );
    assert_eq!(cfg.vars["WORK_DIR"], work_dir.to_string_lossy());

    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(
                params.get("path").unwrap().as_str(),
                config_path
                    .parent()
                    .unwrap()
                    .join("data/input.ndjson")
                    .to_string_lossy()
            );
        }
        _ => {}
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_with_overlays_merges_tables_and_replaces_arrays() {
    let root = make_temp_dir("overlay-merge");
    let base_path = root.join("conf/base.toml");
    let overlay_path = root.join("conf/overlay.toml");
    let windows_path = root.join("conf/models/windows.toml");
    write_file(
        &base_path,
        r#"
mode = "daemon"
windows = "models/windows.toml"
sinks = "base_sinks"

[[sources]]
type = "tcp"
key = "ingress"

listen = "tcp://127.0.0.1:9800"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"
"#,
    );
    write_file(
        &overlay_path,
        r#"
mode = "batch"

[[sources]]

type = "file"
name = "seed_file"
path = "data/seed.ndjson"
stream = "syslog"
format = "ndjson"

[runtime]
rules = "rules/overlay/*.wfl"
"#,
    );
    write_file(
        &windows_path,
        r#"
[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.base_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"

[window.overlay_events]
mode = "replicated"
max_window_bytes = "64MB"
over_cap = "48h"
"#,
    );

    let cfg = FusionConfig::load_with_overlays(
        &base_path,
        &[overlay_path],
        &ConfigVarContext::new(),
        Some(&root.join("conf")),
    )
    .expect("load with overlays");
    assert_eq!(cfg.mode, FusionMode::Batch);
    assert_eq!(cfg.sinks, "base_sinks");
    assert_eq!(cfg.runtime.schemas, "schemas/base/*.wfs");
    assert_eq!(cfg.runtime.rules, "rules/overlay/*.wfl");
    assert_eq!(cfg.windows.len(), 2);
    assert!(cfg.windows.iter().any(|w| w.name == "base_events"));
    assert!(cfg.windows.iter().any(|w| w.name == "overlay_events"));
    assert_eq!(cfg.sources.len(), 1);
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(params.get("path").unwrap().as_str(), "data/seed.ndjson");
            assert_eq!(params.get("stream").unwrap().as_str(), "syslog");
        }
        _ => {}
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_with_overlays_allows_overlay_vars_to_override_base_vars() {
    let root = make_temp_dir("overlay-vars");
    let base_path = root.join("conf/base.toml");
    let overlay_path = root.join("conf/overlay.toml");
    let windows_path = root.join("models/windows.toml");
    write_file(
        &base_path,
        r#"
mode = "batch"
windows = "models/windows.toml"
sinks = "${CASE_PATH}/sinks"

[[sources]]
type = "file"

path = "${CASE_PATH}/data/base.ndjson"
stream = "syslog"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "${CASE_PATH}/schemas/*.wfs"
rules = "${CASE_PATH}/rules/*.wfl"

[vars]
CASE_PATH = "/tmp/base"
"#,
    );
    write_file(
        &overlay_path,
        r#"
[vars]
CASE_PATH = "/tmp/overlay"
"#,
    );
    write_file(&windows_path, WINDOWS_TOML);

    let cfg = FusionConfig::load_with_overlays(
        &base_path,
        &[overlay_path],
        &ConfigVarContext::new(),
        Some(&root),
    )
    .expect("load with overlays");
    assert_eq!(cfg.sinks, "/tmp/overlay/sinks");
    assert_eq!(cfg.runtime.schemas, "/tmp/overlay/schemas/*.wfs");
    assert_eq!(cfg.vars["CASE_PATH"], "/tmp/overlay");
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(
                params.get("path").unwrap().as_str(),
                "/tmp/overlay/data/base.ndjson"
            );
        }
        _ => {}
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_with_overlays_rebases_overlay_relative_paths_against_base_config_dir() {
    let root = make_temp_dir("overlay-rebase-config-dir");
    let base_path = root.join("conf/base.toml");
    let overlay_path = root.join("env/dev/overlay.toml");
    let windows_path = root.join("conf/models/windows.toml");
    write_file(
        &base_path,
        r#"
mode = "batch"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "file"

path = "data/base.ndjson"
stream = "syslog"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"
"#,
    );
    write_file(
        &overlay_path,
        r#"
sinks = "../sinks/dev"
work_root = "../out/dev"

[[sources]]
type = "file"

path = "../data/dev.ndjson"
stream = "syslog"
format = "ndjson"

[runtime]
schemas = "../schemas/dev/*.wfs"
rules = "../rules/dev/*.wfl"

[logging]
file = "../logs/dev.log"
"#,
    );
    write_file(&windows_path, WINDOWS_TOML);

    let cfg = FusionConfig::load_with_overlays(
        &base_path,
        &[overlay_path],
        &ConfigVarContext::new(),
        Some(&root.join("conf")),
    )
    .expect("load with overlays");
    assert_eq!(cfg.sinks, "../env/sinks/dev");
    assert_eq!(cfg.work_root.as_deref(), Some("../env/out/dev"));
    assert_eq!(cfg.runtime.schemas, "../env/schemas/dev/*.wfs");
    assert_eq!(cfg.runtime.rules, "../env/rules/dev/*.wfl");
    assert_eq!(
        cfg.logging
            .file
            .as_ref()
            .map(|p| p.to_string_lossy().to_string()),
        Some("../env/logs/dev.log".to_string())
    );
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(
                params.get("path").unwrap().as_str(),
                "../env/data/dev.ndjson"
            );
        }
        _ => {}
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_with_overlays_rebases_overlay_relative_paths_against_work_dir_when_provided() {
    let root = make_temp_dir("overlay-rebase-work-dir");
    let work_dir = root.join("workspace");
    let base_path = work_dir.join("conf/base.toml");
    let overlay_path = work_dir.join("env/dev/overlay.toml");
    let windows_path = work_dir.join("models/windows.toml");
    std::fs::create_dir_all(&work_dir).expect("failed to create work dir");
    write_file(
        &base_path,
        r#"
mode = "batch"
windows = "models/windows.toml"
sinks = "conf/sinks"

[[sources]]
type = "file"

path = "conf/data/base.ndjson"
stream = "syslog"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "conf/schemas/base/*.wfs"
rules = "conf/rules/base/*.wfl"
"#,
    );
    write_file(
        &overlay_path,
        r#"
sinks = "../sinks/dev"

[[sources]]
type = "file"

path = "../data/dev.ndjson"
stream = "syslog"
format = "ndjson"

[runtime]
rules = "../rules/dev/*.wfl"
"#,
    );
    write_file(&windows_path, WINDOWS_TOML);

    let ctx = ConfigVarContext::new();
    let cfg = FusionConfig::load_with_overlays(&base_path, &[overlay_path], &ctx, Some(&work_dir))
        .expect("load with overlays");
    assert_eq!(cfg.sinks, "env/sinks/dev");
    assert_eq!(cfg.runtime.rules, "env/rules/dev/*.wfl");
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(params.get("path").unwrap().as_str(), "env/data/dev.ndjson");
        }
        _ => {}
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn reject_cyclic_config_vars() {
    let toml = format!(
            r#"{}
[vars]
A = "${{B}}"
B = "${{A}}"
"#,
            FULL_TOML.replace("mode = \"daemon\"", "mode = \"batch\"").replace(
                "[[sources]]\ntype = \"tcp\"\nkey = \"ingress\"\n\nlisten = \"tcp://127.0.0.1:9800\"\n",
                "[[sources]]\ntype = \"file\"\nkey = \"seed_file\"\n\npath = \"data/auth_events.ndjson\"\nstream = \"syslog\"\nformat = \"ndjson\"\n",
            )
        );
    let err = toml.parse::<FusionConfig>().unwrap_err();
    assert!(
        err.to_string().contains("cyclic variable reference"),
        "error should mention cycle: {err}",
    );
}

#[test]
fn reject_invalid_var_name_hyphen() {
    let toml = format!(
        r#"{}
[vars]
my-var = "value"
"#,
        FULL_TOML
    );
    let err = try_load_with_windows(&toml, WINDOWS_TOML).unwrap_err();
    assert!(
        err.to_string().contains("my-var"),
        "error should mention the bad key: {err}",
    );
}

#[test]
fn reject_invalid_var_name_digit_start() {
    let toml = format!(
        r#"{}
[vars]
1BAD = "value"
"#,
        FULL_TOML
    );
    let err = try_load_with_windows(&toml, WINDOWS_TOML).unwrap_err();
    assert!(
        err.to_string().contains("1BAD"),
        "error should mention the bad key: {err}",
    );
}

#[test]
fn accept_underscore_var_name() {
    let toml = format!(
        r#"{}
[vars]
_PRIVATE = "ok"
MAX_COUNT_2 = "99"
"#,
        FULL_TOML
    );
    let cfg: FusionConfig = load_with_windows(&toml, WINDOWS_TOML);
    assert_eq!(cfg.vars["_PRIVATE"], "ok");
    assert_eq!(cfg.vars["MAX_COUNT_2"], "99");
}

#[test]
fn load_with_metrics_block() {
    let toml = format!(
        r#"{}
[metrics]
enabled = true
report_interval = "5s"
prometheus_listen = "127.0.0.1:19001"

[metrics.topn]
enabled = true
max = 50
queue_capacity = 8192
"#,
        FULL_TOML
    );
    let cfg: FusionConfig = load_with_windows(&toml, WINDOWS_TOML);
    assert!(cfg.metrics.enabled);
    assert_eq!(
        cfg.metrics.report_interval.as_duration(),
        Duration::from_secs(5)
    );
    assert_eq!(cfg.metrics.prometheus_listen, "127.0.0.1:19001");
    assert!(cfg.metrics.topn.enabled);
    assert_eq!(cfg.metrics.topn.max, 50);
    assert_eq!(cfg.metrics.topn.queue_capacity, 8192);
}

#[test]
fn reject_invalid_metrics_listen() {
    let toml = format!(
        r#"{}
[metrics]
enabled = true
prometheus_listen = "not-a-socket"
"#,
        FULL_TOML
    );
    assert!(try_load_with_windows(&toml, WINDOWS_TOML).is_err());
}

#[test]
fn sources_dir_expands_scoped_and_explicit_vars() {
    let root = make_temp_dir("sources-dir-vars");
    let config_path = root.join("conf/wfusion.toml");
    let work_dir = root.join("workspace");
    let source_path = work_dir.join("sources.d/seed.toml");
    let windows_path = work_dir.join("models/windows.toml");
    std::fs::create_dir_all(&work_dir).expect("failed to create work dir");
    write_file(
        &config_path,
        r#"
mode = "batch"
windows = "models/windows.toml"
sinks = "${CASE_PATH}/sinks"
sources_dir = "sources.d"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "${CONFIG_DIR}/models/schemas/*.wfs"
rules = "${WORK_DIR}/rules/*.wfl"

[vars]
CASE_PATH = "/tmp/from-file"
"#,
    );
    write_file(
        &source_path,
        r#"
type = "file"
key = "seed_${ENV}"
path = "${CONFIG_DIR}/data/${NAME}.ndjson"
stream = "${STREAM_NAME}"
format = "ndjson"
"#,
    );
    write_file(&windows_path, WINDOWS_TOML);

    let mut explicit_vars = HashMap::new();
    explicit_vars.insert("CASE_PATH".to_string(), "/tmp/from-cli".to_string());
    explicit_vars.insert("ENV".to_string(), "dev".to_string());
    explicit_vars.insert("NAME".to_string(), "input".to_string());
    explicit_vars.insert("STREAM_NAME".to_string(), "netflow".to_string());
    let ctx = ConfigVarContext::from_explicit_vars(explicit_vars);
    let cfg = FusionConfig::load_with_context(&config_path, &ctx, Some(&work_dir))
        .expect("load config with sources_dir");

    assert_eq!(cfg.sinks, "/tmp/from-cli/sinks");
    assert_eq!(
        cfg.runtime.schemas,
        config_path
            .parent()
            .expect("config dir")
            .join("models/schemas/*.wfs")
            .to_string_lossy()
    );
    assert_eq!(
        cfg.runtime.rules,
        work_dir.join("rules/*.wfl").to_string_lossy()
    );
    assert_eq!(cfg.sources.len(), 1);
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(name.as_deref(), Some("seed_dev"));
            assert_eq!(
                params.get("path").unwrap().as_str(),
                source_path
                    .parent()
                    .unwrap()
                    .join("data/input.ndjson")
                    .to_string_lossy()
            );
            assert_eq!(params.get("stream").unwrap().as_str(), "netflow");
            assert_eq!(params.get("format").unwrap().as_str(), "ndjson");
        }
        other => panic!("unexpected source config: {other:?}"),
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn missing_sources_dir_fails() {
    let root = make_temp_dir("missing-sources-dir");
    let config_path = root.join("conf/wfusion.toml");
    let windows_path = root.join("models/windows.toml");
    write_file(
        &config_path,
        r#"
mode = "batch"
windows = "models/windows.toml"
sinks = "sinks"
sources_dir = "missing"

[[sources]]
type = "file"
key = "seed_file"
path = "data/auth_events.ndjson"
stream = "syslog"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/*.wfl"
"#,
    );
    write_file(&windows_path, WINDOWS_TOML);

    let err = FusionConfigLoader::new(&config_path, &[], &ConfigVarContext::new(), Some(&root))
        .load()
        .unwrap_err();
    assert!(
        err.to_string().contains("sources_dir does not exist"),
        "error should mention missing sources_dir: {err}",
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_explicit_sources() {
    let toml = format!(
        r#"{}
[[sources]]
type = "file"
key = "seed_file"

path = "data/auth_events.ndjson"
stream = "syslog"
format = "ndjson"
"#,
        FULL_TOML
    );
    let cfg: FusionConfig = load_with_windows(&toml, WINDOWS_TOML);
    assert_eq!(cfg.sources.len(), 2);
    match &cfg.sources[0] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("tcp") => {
            assert_eq!(name.as_deref(), Some("ingress"));
            assert_eq!(
                params.get("listen").unwrap().as_str(),
                "tcp://127.0.0.1:9800"
            );
        }
        _ => {}
    }
    match &cfg.sources[1] {
        SourceConfig {
            source_type,
            params,
            enabled,
            name,
            ..
        } if source_type.as_deref() == Some("file") => {
            assert_eq!(name.as_deref(), Some("seed_file"));
            assert_eq!(
                params.get("path").unwrap().as_str(),
                "data/auth_events.ndjson"
            );
            assert_eq!(params.get("stream").unwrap().as_str(), "syslog");
            assert_eq!(params.get("format").unwrap().as_str(), "ndjson");
        }
        _ => {}
    }
}

// -----------------------------------------------------------------------
// windows.toml external file tests
// -----------------------------------------------------------------------

#[test]
fn load_windows_from_external_file() {
    let root = make_temp_dir("windows-ext");
    let config_path = root.join("wfusion.toml");
    let windows_path = root.join("models").join("windows.toml");
    std::fs::create_dir_all(windows_path.parent().unwrap()).unwrap();

    // wfusion.toml: no inline window config, just a reference
    let main_toml = r#"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "file"
key = "seed"
path = "seed.ndjson"
stream = "events"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/*.wfl"
"#;
    write_file(&config_path, main_toml);

    // models/windows.toml
    let windows_toml = r#"
[window_defaults]
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
"#;
    write_file(&windows_path, windows_toml);

    let cfg = FusionConfigLoader::new(&config_path, &[], &ConfigVarContext::new(), Some(&root))
        .load()
        .unwrap();

    assert_eq!(
        cfg.window_defaults.evict_interval,
        "30s".parse::<HumanDuration>().unwrap()
    );
    assert_eq!(cfg.windows.len(), 1);
    assert_eq!(cfg.windows[0].name, "auth_events");
}

#[test]
fn missing_windows_defaults_fails() {
    let root = make_temp_dir("windows-missing");
    let config_path = root.join("wfusion.toml");
    let windows_path = root.join("models").join("windows.toml");
    std::fs::create_dir_all(windows_path.parent().unwrap()).unwrap();

    // wfusion.toml: references windows file, but file has no window_defaults
    let main_toml = r#"
windows = "models/windows.toml"
sinks = "sinks"

[[sources]]
type = "file"
key = "seed"
path = "seed.ndjson"
stream = "events"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules = "rules/*.wfl"
"#;
    write_file(&config_path, main_toml);

    // models/windows.toml: only has [window.*], no [window_defaults]
    let windows_toml = r#"
[window.auth_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"
"#;
    write_file(&windows_path, windows_toml);

    let err = FusionConfigLoader::new(&config_path, &[], &ConfigVarContext::new(), Some(&root))
        .load()
        .unwrap_err();
    // A windows.toml without [window_defaults] must fail. The serde error
    // (missing field `window_defaults`) is buried in the error source
    // chain, so walk it to confirm the field name surfaces somewhere.
    let mut msg = err.display_chain();
    if let Some(src) = err.source_ref() {
        msg.push(' ');
        msg.push_str(&src.to_string());
    }
    assert!(
        msg.contains("window_defaults"),
        "error should mention window_defaults: {msg}",
    );
}
