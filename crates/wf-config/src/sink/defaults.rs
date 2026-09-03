use std::path::Path;

use serde::Deserialize;

use super::expect::GroupExpectSpec;
use crate::error::{ConfigReason, ConfigResult};
use crate::vars::inject_loader_scoped_vars;
use crate::vars::{ConfigVarContext, expand_value};
use orion_error::conversion::{SourceErr, SourceRawErr};
use orion_error::runtime::OperationContext;
use toml::Value as TomlValue;

// ---------------------------------------------------------------------------
// DefaultsBody — global defaults loaded from defaults.toml
// ---------------------------------------------------------------------------

/// Global default tags and expect settings loaded from `defaults.toml`.
///
/// ```toml
/// tags = ["env:dev"]
/// ```
#[derive(Debug, Clone, Default, Deserialize, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct DefaultsBody {
    /// Default tags applied to all groups/sinks (lowest priority).
    #[serde(default)]
    pub tags: Vec<String>,
    /// Default expect settings.
    pub expect: Option<GroupExpectSpec>,
}

/// Load `defaults.toml` from the sink root directory.
///
/// Returns `DefaultsBody::default()` if the file doesn't exist.
pub fn load_defaults(sink_root: &Path) -> ConfigResult<DefaultsBody> {
    load_defaults_with_context(sink_root, &ConfigVarContext::new(), None)
}

pub fn load_defaults_with_context(
    sink_root: &Path,
    ctx: &ConfigVarContext,
    work_dir: Option<&Path>,
) -> ConfigResult<DefaultsBody> {
    let path = sink_root.join("defaults.toml");
    if !path.exists() {
        return Ok(DefaultsBody::default());
    }
    let content = std::fs::read_to_string(&path).source_err(
        ConfigReason::Sink,
        format!("failed to read {}", path.display()),
    )?;
    let value: TomlValue = toml::from_str(&content).source_raw_err(
        ConfigReason::Sink,
        format!("failed to parse {}", path.display()),
    )?;
    let scoped = inject_loader_scoped_vars(&value, &path, work_dir);
    let mut expanded = expand_value(&scoped, ctx)
        .source_err(ConfigReason::Sink, "expand sink defaults variables")
        .map_err(|err| {
            err.with_context(
                OperationContext::doing("load sink defaults").with_field("path", path.display()),
            )
        })?;
    if let Some(table) = expanded.as_table_mut() {
        table.remove("vars");
    }
    let expanded_toml = toml::to_string(&expanded).source_raw_err(
        ConfigReason::Sink,
        format!("failed to serialize expanded {}", path.display()),
    )?;
    let body: DefaultsBody = toml::from_str(&expanded_toml).source_raw_err(
        ConfigReason::Sink,
        format!("failed to parse {}", path.display()),
    )?;
    Ok(body)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_defaults() {
        let toml_str = r#"
tags = ["env:dev", "region:us"]
"#;
        let body: DefaultsBody = toml::from_str(toml_str).unwrap();
        assert_eq!(body.tags, vec!["env:dev", "region:us"]);
    }

    #[test]
    fn empty_defaults() {
        let body: DefaultsBody = toml::from_str("").unwrap();
        assert!(body.tags.is_empty());
        assert!(body.expect.is_none());
    }
}
