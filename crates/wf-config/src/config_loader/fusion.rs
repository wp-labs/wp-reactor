use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::admin_api::AdminApiConf;
use crate::project_remote::ProjectRemoteConf;
use orion_error::conversion::{ConvErr, SourceRawErr};
use serde::{Deserialize, Serialize};

use crate::config_loader::FusionConfigLoader;
use crate::config_loader::runtime::RuntimeConfig;
use crate::config_loader::validate;
use crate::error::{ConfigReason, ConfigResult};
use crate::logging_metrics::logging::LoggingConfig;
use crate::logging_metrics::metrics::MetricsConfig;
use crate::output::OutputConfig;
use crate::source::SourceConfig;
use crate::vars::inject_loader_scoped_vars;
use crate::vars::{ConfigVarContext, expand_value};
use crate::window::{WindowConfig, WindowDefaults, WindowOverride};
use toml::Value as TomlValue;

#[derive(
    ::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default,
)]
#[serde(rename_all = "snake_case")]
#[moju(kind = "state", domain = "Config", module = "Config.ConfigLoader")]
pub enum FusionMode {
    #[default]
    Daemon,
    Batch,
}

// ---------------------------------------------------------------------------
// Raw TOML structure (intermediate representation)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct FusionConfigRaw {
    #[serde(default)]
    mode: FusionMode,
    runtime: RuntimeConfig,
    /// External window config file (e.g. "models/windows.toml").
    /// When set, `[window_defaults]` and `[window.*]` are loaded from this file.
    /// Inline `[window_defaults]` / `[window.*]` sections are NOT accepted in wfusion.toml.
    /// When absent, engine startup fails with a clear error.
    #[serde(default)]
    windows: Option<String>,
    /// Path to the sinks/ directory for connector-based sink routing.
    sinks: String,
    /// Optional working root for sink file-path resolution.
    #[serde(default)]
    work_root: Option<String>,
    #[serde(default)]
    logging: LoggingConfig,
    #[serde(default)]
    metrics: MetricsConfig,
    #[serde(default)]
    output: OutputConfig,
    /// User-defined variables for WFL `$VAR` / `${VAR:default}` preprocessing.
    #[serde(default)]
    vars: HashMap<String, String>,
    /// Inline data input sources (from `[[sources]]` in wfusion.toml).
    #[serde(default)]
    sources: Vec<SourceConfig>,
    /// Optional directory of source config files (`sources.d/*.toml`).
    #[serde(default)]
    sources_dir: Option<String>,
    #[serde(default)]
    admin_api: AdminApiConf,
    #[serde(default)]
    project_remote: ProjectRemoteConf,
}

// ---------------------------------------------------------------------------
// FusionConfig (resolved, validated)
// ---------------------------------------------------------------------------

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigLoader")]
pub struct FusionConfig {
    pub mode: FusionMode,
    pub runtime: RuntimeConfig,
    pub window_defaults: WindowDefaults,
    pub windows: Vec<WindowConfig>,
    /// Path to the sinks/ directory for connector-based sink routing.
    pub sinks: String,
    /// Optional working root for sink file-path resolution.
    pub work_root: Option<String>,
    pub logging: LoggingConfig,
    pub metrics: MetricsConfig,
    pub output: OutputConfig,
    /// User-defined variables for WFL `$VAR` / `${VAR:default}` preprocessing.
    pub vars: HashMap<String, String>,
    /// Resolved input source list.
    pub sources: Vec<SourceConfig>,
    pub admin_api: AdminApiConf,
    pub project_remote: ProjectRemoteConf,
}

impl FusionConfig {
    /// Read and parse a `wfusion.toml` file.
    pub fn load(path: impl AsRef<Path>) -> ConfigResult<Self> {
        Self::load_with_context(path, &ConfigVarContext::new(), None)
    }

    /// Read and parse a base `wfusion.toml` file plus overlay files.
    pub fn load_with_overlays(
        path: impl AsRef<Path>,
        overlay_paths: &[PathBuf],
        ctx: &ConfigVarContext,
        work_dir: Option<&Path>,
    ) -> ConfigResult<Self> {
        FusionConfigLoader::new(path.as_ref(), overlay_paths, ctx, work_dir).load()
    }

    /// Read and parse a `wfusion.toml` file with an explicit variable context.
    pub fn load_with_context(
        path: impl AsRef<Path>,
        ctx: &ConfigVarContext,
        work_dir: Option<&Path>,
    ) -> ConfigResult<Self> {
        Self::load_with_overlays(path, &[], ctx, work_dir)
    }

    pub(crate) fn from_toml_with_context(
        toml_str: &str,
        ctx: &ConfigVarContext,
    ) -> ConfigResult<Self> {
        let value: TomlValue =
            toml::from_str(toml_str).source_raw_err(ConfigReason::Parse, "parse fusion TOML")?;
        Self::from_value_with_context(&value, ctx, None, None)
    }

    pub(crate) fn from_value_with_context(
        value: &TomlValue,
        ctx: &ConfigVarContext,
        source_path: Option<&Path>,
        work_dir: Option<&Path>,
    ) -> ConfigResult<Self> {
        let scoped = match source_path {
            Some(path) => {
                inject_loader_scoped_vars(value, path, work_dir.or_else(|| path.parent()))
            }
            None => value.clone(),
        };
        let expanded = expand_value(&scoped, ctx).conv_err()?;
        reject_invalid_source_fields(&expanded)?;
        let expanded_toml = toml::to_string(&expanded)
            .source_raw_err(ConfigReason::Parse, "serialize expanded fusion TOML")?;
        let mut raw: FusionConfigRaw = toml::from_str(&expanded_toml)
            .source_raw_err(ConfigReason::Parse, "parse expanded fusion TOML")?;
        raw.vars = ctx.materialize_vars(&raw.vars);

        // Load window config from external file (if specified).
        let (window_defaults, windows) = if let Some(ref windows_rel) = raw.windows {
            let windows_root = match work_dir {
                Some(wd) => wd.join(windows_rel),
                None => PathBuf::from(windows_rel),
            };
            let file_content = std::fs::read_to_string(&windows_root).source_raw_err(
                ConfigReason::Parse,
                format!("read windows file: {}", windows_root.display()),
            )?;
            let file: WindowFileRaw = toml::from_str(&file_content).source_raw_err(
                ConfigReason::Parse,
                format!("parse windows TOML: {}", windows_root.display()),
            )?;
            let mut ws: Vec<WindowConfig> = file
                .window
                .into_iter()
                .map(|(name, ovr)| ovr.resolve(name, &file.window_defaults))
                .collect::<ConfigResult<_>>()?;
            ws.sort_by(|a, b| a.name.cmp(&b.name));
            (file.window_defaults, ws)
        } else {
            return ConfigReason::Parse.fail(
                "`windows` field is required — set windows = \"models/windows.toml\" in wfusion.toml",
            );
        };

        // Load sources from directory if configured
        let mut sources = raw.sources;
        if let Some(ref dir) = raw.sources_dir {
            let sources_root = if let Some(wd) = work_dir {
                wd.join(dir)
            } else {
                PathBuf::from(dir)
            };
            if !sources_root.is_dir() {
                return ConfigReason::Parse.fail(format!(
                    "sources_dir does not exist or is not a directory: {}",
                    sources_root.display()
                ));
            }
            let dir_sources = load_sources_from_dir(&sources_root, source_path, work_dir, ctx)?;
            sources.extend(dir_sources);
        }

        let config = FusionConfig {
            mode: raw.mode,
            runtime: raw.runtime,
            window_defaults,
            windows,
            sinks: raw.sinks,
            work_root: raw.work_root,
            logging: raw.logging,
            metrics: raw.metrics,
            output: raw.output,
            vars: raw.vars,
            sources,
            admin_api: raw.admin_api,
            project_remote: raw.project_remote,
        };

        validate::validate(&config)?;

        Ok(config)
    }
}

fn reject_invalid_source_fields(value: &TomlValue) -> ConfigResult<()> {
    let Some(sources) = value.get("sources").and_then(TomlValue::as_array) else {
        return Ok(());
    };
    for (idx, source) in sources.iter().enumerate() {
        let Some(table) = source.as_table() else {
            continue;
        };
        if table.contains_key("enabled") {
            return ConfigReason::Parse.fail(format!(
                "sources[{idx}] uses `enable`, not `enabled`; replace `enabled = ...` with `enable = ...`"
            ));
        }
        if let Some(vars) = table.get("vars")
            && !vars.is_table()
        {
            return ConfigReason::Parse.fail(format!(
                "sources[{idx}] field `vars` is reserved for config loading and cannot be used as a source parameter"
            ));
        }
    }
    Ok(())
}

/// Load `SourceConfig` entries from `*.toml` files in a directory.
///
/// Each file must deserialize as a single `SourceConfig` (no `[[sources]]` wrapper).
/// Files go through the same scoped-var injection and variable expansion pipeline
/// as the main `wfusion.toml` to ensure `${WORK_DIR}`, `${CONFIG_DIR}`, CLI vars,
/// and default vars are expanded consistently.
fn load_sources_from_dir(
    dir: &Path,
    _source_path: Option<&Path>,
    work_dir: Option<&Path>,
    ctx: &ConfigVarContext,
) -> ConfigResult<Vec<SourceConfig>> {
    let mut sources = Vec::new();
    let entries = std::fs::read_dir(dir).source_raw_err(
        ConfigReason::Parse,
        format!("read sources dir: {}", dir.display()),
    )?;

    let mut paths: Vec<_> = entries
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "toml"))
        .map(|e| e.path())
        .collect();
    paths.sort();

    for path in paths {
        let content = std::fs::read_to_string(&path).source_raw_err(
            ConfigReason::Parse,
            format!("read source file: {}", path.display()),
        )?;
        // Parse → inject scoped vars → expand → re-parse, same pipeline as main config
        let value: TomlValue = toml::from_str(&content).source_raw_err(
            ConfigReason::Parse,
            format!("parse source file TOML: {}", path.display()),
        )?;
        let scoped = inject_loader_scoped_vars(&value, &path, work_dir.or_else(|| path.parent()));
        let mut expanded = expand_value(&scoped, ctx).conv_err()?;
        // Strip the injected `vars` key so `SourceConfig` flatten deserialization
        // doesn't choke on the nested table.
        if let Some(table) = expanded.as_table_mut() {
            table.remove("vars");
        }
        let expanded_toml = toml::to_string(&expanded).source_raw_err(
            ConfigReason::Parse,
            format!("serialize expanded source file: {}", path.display()),
        )?;
        let source: SourceConfig = toml::from_str(&expanded_toml).source_raw_err(
            ConfigReason::Parse,
            format!("parse expanded source file: {}", path.display()),
        )?;
        sources.push(source);
    }

    Ok(sources)
}

impl FromStr for FusionConfig {
    type Err = crate::ConfigError;

    /// Parse a TOML string into a resolved, validated [`FusionConfig`].
    fn from_str(toml_str: &str) -> ConfigResult<Self> {
        Self::from_toml_with_context(toml_str, &ConfigVarContext::new())
    }
}

/// Minimal struct to deserialize a standalone windows.toml file.
#[derive(Debug, Deserialize)]
struct WindowFileRaw {
    window_defaults: WindowDefaults,
    #[serde(default)]
    window: HashMap<String, WindowOverride>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "fusion_tests.rs"]
mod fusion_tests;
