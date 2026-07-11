use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use toml::Value as TomlValue;
use wp_connector_api::ConnectorDef;

use super::build::{build_fixed_group, build_flex_group};
use super::connector::load_connector_defs_with_context;
use super::defaults::{DefaultsBody, load_defaults_with_context};
use super::group::{FixedGroup, FlexGroup};
use super::route::RouteFile;
use crate::error::{ConfigReason, ConfigResult};
use crate::vars::inject_loader_scoped_vars;
use crate::vars::{ConfigVarContext, expand_value};
use orion_error::conversion::{SourceErr, SourceRawErr};
use orion_error::runtime::OperationContext;

// ---------------------------------------------------------------------------
// SinkConfigBundle — aggregated result of loading all sink config files
// ---------------------------------------------------------------------------

/// The complete sink configuration loaded from a `sinks/` directory.
#[derive(::moju_derive::MoJu, Debug)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct SinkConfigBundle {
    /// Connector definitions loaded from `sink.d/`.
    pub connectors: BTreeMap<String, ConnectorDef>,
    /// Global defaults from `defaults.toml`.
    pub defaults: DefaultsBody,
    /// Business routing groups loaded from `business.d/`.
    pub business: Vec<FlexGroup>,
    /// Infrastructure default group from `infra.d/default.toml`.
    pub infra_default: Option<FixedGroup>,
    /// Infrastructure error group from `infra.d/error.toml`.
    pub infra_error: Option<FixedGroup>,
    /// Infrastructure monitor group from `infra.d/monitor.toml` (optional).
    pub infra_monitor: Option<FixedGroup>,
}

// ---------------------------------------------------------------------------
// Directory loading
// ---------------------------------------------------------------------------

/// Load the complete sink configuration from a `sinks/` root directory.
///
/// Expected directory layout:
/// ```text
/// sinks/
/// ├── sink.d/               # connector definitions (*.toml)
/// ├── business.d/           # business routing groups (*.toml)
/// ├── infra.d/              # infrastructure groups (*.toml)
/// │   ├── default.toml
/// │   └── error.toml
/// └── defaults.toml         # global defaults
/// ```
///
/// Connector definitions are loaded from the nearest ancestor containing
/// `connectors/sink.d`.
pub fn load_sink_config(sink_root: &Path) -> ConfigResult<SinkConfigBundle> {
    load_sink_config_with_context(sink_root, &ConfigVarContext::new(), None)
}

pub fn load_sink_config_with_context(
    sink_root: &Path,
    ctx: &ConfigVarContext,
    work_dir: Option<&Path>,
) -> ConfigResult<SinkConfigBundle> {
    if !sink_root.is_dir() {
        return ConfigReason::Sink.fail(format!(
            "sink config directory does not exist: {}",
            sink_root.display()
        ));
    }

    // 1. Load connectors from sink.d/
    let connectors = if let Some(connector_dir) = find_connector_dir(sink_root) {
        load_connector_defs_with_context(&connector_dir, ctx, work_dir)?
    } else {
        BTreeMap::new()
    };

    // 2. Load defaults
    let defaults = load_defaults_with_context(sink_root, ctx, work_dir)?;

    // 3. Load business groups from business.d/
    let business = load_business_groups(
        &sink_root.join("business.d"),
        &connectors,
        &defaults,
        ctx,
        work_dir,
    )?;

    // 4. Load infra groups from infra.d/
    let infra_default = load_infra_group(
        &sink_root.join("infra.d").join("default.toml"),
        &connectors,
        &defaults,
        ctx,
        work_dir,
    )?;
    let infra_error = load_infra_group(
        &sink_root.join("infra.d").join("error.toml"),
        &connectors,
        &defaults,
        ctx,
        work_dir,
    )?;
    let infra_monitor = load_infra_group(
        &sink_root.join("infra.d").join("monitor.toml"),
        &connectors,
        &defaults,
        ctx,
        work_dir,
    )?;

    Ok(SinkConfigBundle {
        connectors,
        defaults,
        business,
        infra_default,
        infra_error,
        infra_monitor,
    })
}

fn find_connector_dir(sink_root: &Path) -> Option<PathBuf> {
    let base = if sink_root.is_absolute() {
        sink_root.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(sink_root)
    };
    let mut current = if base.is_dir() {
        base
    } else {
        base.parent()?.to_path_buf()
    };

    for _ in 0..32 {
        let candidate = current.join("connectors").join("sink.d");
        if candidate.is_dir() {
            return Some(candidate);
        }
        if !current.pop() {
            break;
        }
    }

    None
}

/// Load all business routing groups from `*.toml` files in a directory.
fn load_business_groups(
    dir: &Path,
    connectors: &BTreeMap<String, ConnectorDef>,
    defaults: &DefaultsBody,
    ctx: &ConfigVarContext,
    work_dir: Option<&Path>,
) -> ConfigResult<Vec<FlexGroup>> {
    let mut groups = Vec::new();

    if !dir.is_dir() {
        return Ok(groups);
    }

    let pattern = dir.join("*.toml");
    let pattern_str = pattern.to_string_lossy();

    let mut entries: Vec<_> = glob::glob(&pattern_str)
        .source_raw_err(
            ConfigReason::Sink,
            format!("invalid glob {}", pattern.display()),
        )?
        .filter_map(|e| e.ok())
        .collect();
    entries.sort();

    for path in entries {
        let content = std::fs::read_to_string(&path).source_err(
            ConfigReason::Sink,
            format!("failed to read {}", path.display()),
        )?;
        let value: TomlValue = toml::from_str(&content).source_raw_err(
            ConfigReason::Sink,
            format!("failed to parse {}", path.display()),
        )?;
        let scoped = inject_loader_scoped_vars(&value, &path, work_dir);
        let mut expanded = expand_sink_value(&scoped, ctx, &path)?;
        if let Some(table) = expanded.as_table_mut() {
            table.remove("vars");
        }
        let expanded_toml = toml::to_string(&expanded).source_raw_err(
            ConfigReason::Sink,
            format!("failed to serialize expanded {}", path.display()),
        )?;
        let file: RouteFile = toml::from_str(&expanded_toml).source_raw_err(
            ConfigReason::Sink,
            format!("failed to parse {}", path.display()),
        )?;

        let group = build_flex_group(&file.sink_group, connectors, defaults)
            .source_err(ConfigReason::Sink, format!("error in {}", path.display()))?;
        groups.push(group);
    }

    Ok(groups)
}

/// Load a single infra group from a TOML file (returns None if file doesn't exist).
fn load_infra_group(
    path: &Path,
    connectors: &BTreeMap<String, ConnectorDef>,
    defaults: &DefaultsBody,
    ctx: &ConfigVarContext,
    work_dir: Option<&Path>,
) -> ConfigResult<Option<FixedGroup>> {
    if !path.exists() {
        return Ok(None);
    }

    let content = std::fs::read_to_string(path).source_err(
        ConfigReason::Sink,
        format!("failed to read {}", path.display()),
    )?;
    let value: TomlValue = toml::from_str(&content).source_raw_err(
        ConfigReason::Sink,
        format!("failed to parse {}", path.display()),
    )?;
    let scoped = inject_loader_scoped_vars(&value, path, work_dir);
    let mut expanded = expand_sink_value(&scoped, ctx, path)?;
    if let Some(table) = expanded.as_table_mut() {
        table.remove("vars");
    }
    let expanded_toml = toml::to_string(&expanded).source_raw_err(
        ConfigReason::Sink,
        format!("failed to serialize expanded {}", path.display()),
    )?;
    let file: RouteFile = toml::from_str(&expanded_toml).source_raw_err(
        ConfigReason::Sink,
        format!("failed to parse {}", path.display()),
    )?;

    let group = build_fixed_group(&file.sink_group, connectors, defaults)
        .source_err(ConfigReason::Sink, format!("error in {}", path.display()))?;
    Ok(Some(group))
}

fn expand_sink_value(
    value: &TomlValue,
    ctx: &ConfigVarContext,
    path: &Path,
) -> ConfigResult<TomlValue> {
    expand_value(value, ctx)
        .source_err(ConfigReason::Sink, "expand sink config variables")
        .map_err(|err| {
            err.with_context(
                OperationContext::doing("load sink config").with_field("path", path.display()),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_temp_dir(name: &str) -> PathBuf {
        let unique = format!(
            "wf-config-{}-{}-{}",
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

    fn sample_connector_toml() -> &'static str {
        r#"
[[connectors]]
id = "file_json"
type = "file"
allow_override = ["file"]

[connectors.params]
fmt = "json"
file = "default.jsonl"
"#
    }

    fn sample_connector_toml_with_env_base() -> &'static str {
        r#"
[[connectors]]
id = "file_json"
type = "file"
allow_override = ["base", "file"]

[connectors.params]
fmt = "json"
base = "${WF_CONFIG_SINK_ENV_CASE_PATH}/data/out_dat"
file = "${WF_CONFIG_SINK_ENV_OUT_FILE:default.jsonl}"
"#
    }

    fn sample_business_toml() -> &'static str {
        r#"
[sink_group]
name = "catch_all"
windows = ["*"]

[[sink_group.sinks]]
connect = "file_json"

[sink_group.sinks.params]
file = "all.jsonl"
"#
    }

    fn sample_business_toml_with_env_override() -> &'static str {
        r#"
[sink_group]
name = "catch_all"
windows = ["*"]

[[sink_group.sinks]]
connect = "file_json"

[sink_group.sinks.params]
file = "${WF_CONFIG_SINK_ENV_OUT_FILE:alerts.jsonl}"
"#
    }

    fn sample_business_toml_with_wf_meta_disabled() -> &'static str {
        r#"
[sink_group]
name = "catch_all"
windows = ["*"]
wf_meta_disable = ["__wfu_rule_name"]

[[sink_group.sinks]]
connect = "file_json"

[sink_group.sinks.params]
file = "all.jsonl"
"#
    }

    fn sample_connector_toml_with_file_vars() -> &'static str {
        r#"
[vars]
CASE_PATH = "/tmp/from-file"

[[connectors]]
id = "file_json"
type = "file"
allow_override = ["base", "file"]

[connectors.params]
fmt = "json"
base = "${CASE_PATH}/data/out_dat"
file = "default.jsonl"
"#
    }

    fn sample_business_toml_with_file_vars() -> &'static str {
        r#"
[vars]
OUT_FILE = "alerts-from-file.jsonl"

[sink_group]
name = "catch_all"
windows = ["*"]

[[sink_group.sinks]]
connect = "file_json"

[sink_group.sinks.params]
file = "${OUT_FILE}"
"#
    }

    #[test]
    fn loads_connectors_from_ancestor_connectors_dir() {
        let root = make_temp_dir("ancestor-connectors");
        let sink_root = root.join("examples/sinks");

        write_file(
            &root.join("examples/connectors/sink.d/file_json.toml"),
            sample_connector_toml(),
        );
        write_file(
            &sink_root.join("business.d/catch_all.toml"),
            sample_business_toml(),
        );

        let bundle = load_sink_config(&sink_root).expect("load sink config");
        assert!(bundle.connectors.contains_key("file_json"));
        assert_eq!(bundle.business.len(), 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn loads_wf_meta_disable_from_route_file() {
        let root = make_temp_dir("sink-wf-meta-disable");
        let sink_root = root.join("examples/sinks");

        write_file(
            &root.join("examples/connectors/sink.d/file_json.toml"),
            sample_connector_toml(),
        );
        write_file(
            &sink_root.join("business.d/catch_all.toml"),
            sample_business_toml_with_wf_meta_disabled(),
        );

        let bundle = load_sink_config(&sink_root).expect("load sink config");

        assert_eq!(bundle.business.len(), 1);
        assert_eq!(
            bundle.business[0].wf_meta_disable,
            vec!["__wfu_rule_name".to_string()]
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn returns_none_when_no_ancestor_connectors_dir_exists() {
        let root = make_temp_dir("no-connectors");
        let sink_root = root.join("examples/sinks");

        std::fs::create_dir_all(&sink_root).expect("failed to create sink root");
        assert_eq!(find_connector_dir(&sink_root), None);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn expands_env_vars_in_sink_connector_and_route_files() {
        let root = make_temp_dir("sink-env-expand");
        let sink_root = root.join("examples/sinks");

        unsafe {
            std::env::set_var("WF_CONFIG_SINK_ENV_CASE_PATH", "/tmp/case-root");
            std::env::set_var("WF_CONFIG_SINK_ENV_OUT_FILE", "alerts-from-env.jsonl");
        }

        write_file(
            &root.join("examples/connectors/sink.d/file_json.toml"),
            sample_connector_toml_with_env_base(),
        );
        write_file(
            &sink_root.join("business.d/catch_all.toml"),
            sample_business_toml_with_env_override(),
        );

        let bundle = load_sink_config(&sink_root).expect("load sink config");
        let sink = &bundle.business[0].sinks[0].spec;
        assert_eq!(
            sink.params.get("base"),
            Some(&serde_json::Value::String(
                "/tmp/case-root/data/out_dat".into()
            ))
        );
        assert_eq!(
            sink.params.get("file"),
            Some(&serde_json::Value::String("alerts-from-env.jsonl".into()))
        );

        unsafe {
            std::env::remove_var("WF_CONFIG_SINK_ENV_CASE_PATH");
            std::env::remove_var("WF_CONFIG_SINK_ENV_OUT_FILE");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn explicit_vars_override_sink_file_vars() {
        let root = make_temp_dir("sink-explicit-vars");
        let sink_root = root.join("examples/sinks");

        write_file(
            &root.join("examples/connectors/sink.d/file_json.toml"),
            sample_connector_toml_with_file_vars(),
        );
        write_file(
            &sink_root.join("business.d/catch_all.toml"),
            sample_business_toml_with_file_vars(),
        );

        let mut explicit_vars = std::collections::HashMap::new();
        explicit_vars.insert("CASE_PATH".to_string(), "/tmp/from-cli".to_string());
        explicit_vars.insert("OUT_FILE".to_string(), "alerts-from-cli.jsonl".to_string());
        let ctx = ConfigVarContext::from_explicit_vars(explicit_vars);
        let bundle =
            load_sink_config_with_context(&sink_root, &ctx, None).expect("load sink config");
        let sink = &bundle.business[0].sinks[0].spec;
        assert_eq!(
            sink.params.get("base"),
            Some(&serde_json::Value::String(
                "/tmp/from-cli/data/out_dat".into()
            ))
        );
        assert_eq!(
            sink.params.get("file"),
            Some(&serde_json::Value::String("alerts-from-cli.jsonl".into()))
        );

        let _ = std::fs::remove_dir_all(root);
    }
}
