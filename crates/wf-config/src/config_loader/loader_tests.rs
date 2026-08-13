use super::*;
use std::collections::HashMap;

fn make_temp_dir(name: &str) -> PathBuf {
    let unique = format!(
        "wf-config-loader-{}-{}-{}",
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

// External window config shared by loader tests. `FusionConfigRaw` no longer
// accepts inline `[window_defaults]` / `[window.*]` in wfusion.toml; window
// config must live in an external file referenced via `windows = "..."`.
const WINDOWS_TOML: &str = r#"[window_defaults]
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
"#;

#[test]
fn raw_loader_tracks_field_origins_after_overlay_merge() {
    let root = make_temp_dir("origin-merge");
    let base_path = root.join("conf/base.toml");
    let overlay_path = root.join("env/dev/overlay.toml");
    write_file(
        &base_path,
        r#"
mode = "daemon"
sinks = "sinks"
windows = "models/windows.toml"

[[sources]]
type = "tcp"
name = "ingress"
listen = "tcp://127.0.0.1:9800"

[runtime]
parse_parallelism = 2
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
path = "../data/seed.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
rules = "../rules/dev/*.wfl"
"#,
    );
    write_file(&root.join("models/windows.toml"), WINDOWS_TOML);

    let raw = FusionConfigLoader::new(
        &base_path,
        std::slice::from_ref(&overlay_path),
        &ConfigVarContext::new(),
        None,
    )
    .load_raw()
    .expect("load raw");

    assert_eq!(raw.origin_for("mode"), Some(overlay_path.as_path()));
    assert_eq!(raw.origin_for("runtime.schemas"), Some(base_path.as_path()));
    assert_eq!(
        raw.origin_for("runtime.rules"),
        Some(overlay_path.as_path())
    );
    // Window config now lives in the external `windows.toml`, which
    // `load_raw()` does not pull into the merged raw tree, so window-field
    // origins are not tracked here. (The external file's own origins are
    // only resolved later by `FusionConfig`, not by `load_raw`.)
    assert_eq!(
        raw.origin_for("sources[0].path"),
        Some(overlay_path.as_path())
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn raw_loader_tracks_rebased_overlay_path_values() {
    let root = make_temp_dir("origin-rebase");
    let windows_path = root.join("models/windows.toml");
    write_file(&windows_path, WINDOWS_TOML);
    let base_path = root.join("conf/base.toml");
    let overlay_path = root.join("env/dev/overlay.toml");
    write_file(
        &base_path,
        r#"
mode = "batch"
sinks = "sinks"
windows = "models/windows.toml"

[[sources]]
type = "file"
path = "data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"
"#,
    );
    write_file(
        &overlay_path,
        r#"
sinks = "../sinks/dev"

[runtime]
rules = "../rules/dev/*.wfl"
"#,
    );

    let raw = FusionConfigLoader::new(
        &base_path,
        std::slice::from_ref(&overlay_path),
        &ConfigVarContext::new(),
        None,
    )
    .load_raw()
    .expect("load raw");
    let sinks = raw
        .value()
        .get("sinks")
        .and_then(TomlValue::as_str)
        .expect("sinks string");
    let rules = raw
        .value()
        .get("runtime")
        .and_then(|v| v.get("rules"))
        .and_then(TomlValue::as_str)
        .expect("rules string");

    assert_eq!(sinks, "../env/sinks/dev");
    assert_eq!(rules, "../env/rules/dev/*.wfl");
    assert_eq!(raw.origin_for("sinks"), Some(overlay_path.as_path()));
    assert_eq!(
        raw.origin_for("runtime.rules"),
        Some(overlay_path.as_path())
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn raw_loader_diff_reports_changed_values_and_origins() {
    let root = make_temp_dir("origin-diff");
    let windows_path = root.join("models/windows.toml");
    write_file(&windows_path, WINDOWS_TOML);
    let base_path = root.join("conf/base.toml");
    let old_overlay = root.join("env/dev/old.toml");
    let new_overlay = root.join("env/dev/new.toml");
    write_file(
        &base_path,
        r#"
mode = "daemon"
sinks = "sinks"
windows = "models/windows.toml"

[[sources]]
type = "tcp"
name = "ingress"
listen = "tcp://127.0.0.1:9800"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"
"#,
    );
    write_file(
        &old_overlay,
        r#"
mode = "batch"

[runtime]
rules = "../rules/v1/*.wfl"
"#,
    );
    write_file(
        &new_overlay,
        r#"
mode = "batch"

[runtime]
rules = "../rules/v2/*.wfl"

[vars]
CASE_PATH = "/tmp/case-a"
"#,
    );

    let old_raw = FusionConfigLoader::new(
        &base_path,
        std::slice::from_ref(&old_overlay),
        &ConfigVarContext::new(),
        None,
    )
    .load_raw()
    .expect("load old raw");
    let new_raw = FusionConfigLoader::new(
        &base_path,
        std::slice::from_ref(&new_overlay),
        &ConfigVarContext::new(),
        None,
    )
    .load_raw()
    .expect("load new raw");

    let changes = old_raw.diff(&new_raw);
    assert_eq!(changes.len(), 2);

    let rules_change = changes
        .iter()
        .find(|c| c.path == "runtime.rules")
        .expect("runtime.rules change");
    assert_eq!(
        rules_change.old_value,
        Some(TomlValue::String("../env/rules/v1/*.wfl".to_string()))
    );
    assert_eq!(
        rules_change.new_value,
        Some(TomlValue::String("../env/rules/v2/*.wfl".to_string()))
    );
    assert_eq!(
        rules_change.old_origin.as_deref(),
        Some(old_overlay.as_path())
    );
    assert_eq!(
        rules_change.new_origin.as_deref(),
        Some(new_overlay.as_path())
    );

    let vars_change = changes
        .iter()
        .find(|c| c.path == "vars")
        .expect("vars change");
    assert_eq!(vars_change.old_value, None);
    let mut expected_vars = toml::map::Map::new();
    expected_vars.insert(
        "CASE_PATH".to_string(),
        TomlValue::String("/tmp/case-a".to_string()),
    );
    assert_eq!(vars_change.new_value, Some(TomlValue::Table(expected_vars)));
    assert_eq!(vars_change.old_origin, None);
    assert_eq!(
        vars_change.new_origin.as_deref(),
        Some(new_overlay.as_path())
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_expanded_toml_renders_overlay_and_vars_in_final_output() {
    let root = make_temp_dir("expanded-render");
    let windows_path = root.join("models/windows.toml");
    write_file(&windows_path, WINDOWS_TOML);
    let base_path = root.join("conf/base.toml");
    let overlay_path = root.join("env/dev/overlay.toml");
    write_file(
        &base_path,
        &r#"
mode = "batch"
sinks = "${CASE_PATH}/sinks"
windows = "@WINDOWS_PATH@"

[[sources]]
type = "file"
path = "${CASE_PATH}/data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "${CASE_PATH}/schemas/base/*.wfs"
rules = "${CASE_PATH}/rules/base/*.wfl"

[vars]
CASE_PATH = "/tmp/base"
"#
        .replace("@WINDOWS_PATH@", &windows_path.display().to_string()),
    );
    write_file(
        &overlay_path,
        r#"
[runtime]
rules = "../rules/dev/*.wfl"

[vars]
CASE_PATH = "/tmp/overlay"
"#,
    );

    let expanded = FusionConfigLoader::new(
        &base_path,
        std::slice::from_ref(&overlay_path),
        &ConfigVarContext::new(),
        None,
    )
    .load_expanded_toml()
    .expect("load expanded toml");

    assert!(expanded.contains("sinks = \"/tmp/overlay/sinks\""));
    assert!(expanded.contains("path = \"/tmp/overlay/data/base.ndjson\""));
    assert!(expanded.contains("schemas = \"/tmp/overlay/schemas/base/*.wfs\""));
    assert!(expanded.contains("rules = \"../env/rules/dev/*.wfl\""));
    assert!(expanded.contains("CASE_PATH = \"/tmp/overlay\""));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn origin_entries_are_sorted_by_path() {
    let root = make_temp_dir("origin-entries");
    let windows_path = root.join("models/windows.toml");
    write_file(&windows_path, WINDOWS_TOML);
    let base_path = root.join("conf/base.toml");
    write_file(
        &base_path,
        r#"
mode = "batch"
sinks = "sinks"
windows = "models/windows.toml"

[[sources]]
type = "file"
path = "data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"
"#,
    );

    let raw = FusionConfigLoader::new(&base_path, &[], &ConfigVarContext::new(), None)
        .load_raw()
        .expect("load raw");
    let entries = raw.origin_entries();

    assert!(!entries.is_empty());
    let paths: Vec<&str> = entries.iter().map(|(path, _)| path.as_str()).collect();
    let mut sorted = paths.clone();
    sorted.sort_unstable();
    assert_eq!(paths, sorted);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_expanded_raw_keeps_origins_and_expands_values() {
    let root = make_temp_dir("expanded-raw");
    let windows_path = root.join("models/windows.toml");
    write_file(&windows_path, WINDOWS_TOML);
    let base_path = root.join("conf/base.toml");
    write_file(
        &base_path,
        &r#"
mode = "batch"
sinks = "${CASE_PATH}/sinks"
windows = "@WINDOWS_PATH@"

[[sources]]
type = "file"
path = "${CASE_PATH}/data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "${CASE_PATH}/schemas/base/*.wfs"
rules = "${CASE_PATH}/rules/base/*.wfl"

[vars]
CASE_PATH = "/tmp/base"
"#
        .replace("@WINDOWS_PATH@", &windows_path.display().to_string()),
    );

    let expanded = FusionConfigLoader::new(&base_path, &[], &ConfigVarContext::new(), None)
        .load_expanded_raw()
        .expect("load expanded raw");

    let sinks = expanded
        .value()
        .get("sinks")
        .and_then(TomlValue::as_str)
        .expect("sinks string");
    assert_eq!(sinks, "/tmp/base/sinks");
    assert_eq!(expanded.origin_for("sinks"), Some(base_path.as_path()));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_expanded_raw_aggregates_array_field_sources() {
    let root = make_temp_dir("expanded-raw-array");
    let windows_path = root.join("models/windows.toml");
    write_file(&windows_path, WINDOWS_TOML);
    let base_path = root.join("conf/base.toml");
    write_file(
        &base_path,
        &r#"
mode = "batch"
sinks = "${CASE_PATH}/sinks"
windows = "@WINDOWS_PATH@"

[[sources]]
type = "file"
path = "${CASE_PATH}/data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "${CASE_PATH}/schemas/base/*.wfs"
rules = "${CASE_PATH}/rules/base/*.wfl"
"#
        .replace("@WINDOWS_PATH@", &windows_path.display().to_string()),
    );

    let mut explicit = HashMap::new();
    explicit.insert("CASE_PATH".to_string(), "/tmp/from-cli".to_string());
    let expanded = FusionConfigLoader::new(
        &base_path,
        &[],
        &ConfigVarContext::from_explicit_vars(explicit),
        None,
    )
    .load_expanded_raw()
    .expect("load expanded raw");

    let origin = expanded
        .origin_for("sources")
        .map(|path| path.to_string_lossy().to_string())
        .expect("sources origin");
    assert!(origin.starts_with("<mixed:"));
    assert!(origin.contains(&format!("file:{}", base_path.display())));
    assert!(origin.contains("cli:CASE_PATH"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_effective_vars_reports_final_value_sources() {
    let root = make_temp_dir("effective-vars");
    let windows_path = root.join("models/windows.toml");
    write_file(&windows_path, WINDOWS_TOML);
    let base_path = root.join("conf/base.toml");
    write_file(
        &base_path,
        r#"
mode = "batch"
sinks = "${CASE_PATH}/sinks"
work_root = "$HOME"
windows = "models/windows.toml"

[[sources]]
type = "file"
path = "data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"

[vars]
CASE_PATH = "${HOME}/case"
MIXED = "${CASE_PATH}/tail"
"#,
    );

    let mut explicit = HashMap::new();
    explicit.insert("CASE_PATH".to_string(), "/tmp/from-cli".to_string());
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).expect("failed to create workspace");
    let ctx = ConfigVarContext::from_explicit_vars(explicit);
    let vars = FusionConfigLoader::new(&base_path, &[], &ctx, Some(&workspace))
        .load_effective_vars()
        .expect("load effective vars");
    let home = std::env::var("HOME").expect("HOME env var");

    assert!(vars.iter().any(|entry| {
        entry.key == "CASE_PATH"
            && entry.value == "/tmp/from-cli"
            && entry.source == "<cli:CASE_PATH>"
    }));
    assert!(
        vars.iter()
            .any(|entry| { entry.key == "CONFIG_DIR" && entry.source == "<builtin:CONFIG_DIR>" })
    );
    assert!(
        vars.iter()
            .any(|entry| { entry.key == "WORK_DIR" && entry.source == "<builtin:WORK_DIR>" })
    );
    assert!(vars.iter().any(|entry| {
        entry.key == "HOME" && entry.value == home && entry.source == "<env:HOME>"
    }));
    assert!(vars.iter().any(|entry| {
        entry.key == "MIXED"
            && entry.source == format!("<mixed:file:{},cli:CASE_PATH>", base_path.display())
    }));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn load_effective_vars_does_not_leak_env_from_overridden_file_var() {
    let root = make_temp_dir("effective-vars-overridden");
    let windows_path = root.join("models/windows.toml");
    write_file(&windows_path, WINDOWS_TOML);
    let base_path = root.join("conf/base.toml");
    write_file(
        &base_path,
        r#"
mode = "batch"
sinks = "${CASE_PATH}/sinks"
windows = "models/windows.toml"

[[sources]]
type = "file"
path = "${CASE_PATH}/data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
parse_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"

[vars]
CASE_PATH = "${HOME}/case"
"#,
    );

    let mut explicit = HashMap::new();
    explicit.insert("CASE_PATH".to_string(), "/tmp/from-cli".to_string());
    let vars = FusionConfigLoader::new(
        &base_path,
        &[],
        &ConfigVarContext::from_explicit_vars(explicit),
        None,
    )
    .load_effective_vars()
    .expect("load effective vars");

    assert!(vars.iter().any(|entry| {
        entry.key == "CASE_PATH"
            && entry.value == "/tmp/from-cli"
            && entry.source == "<cli:CASE_PATH>"
    }));
    assert!(!vars.iter().any(|entry| entry.key == "HOME"));

    let _ = std::fs::remove_dir_all(root);
}
