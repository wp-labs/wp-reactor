use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;
use toml::Value as TomlValue;

use wp_connector_api::{ConnectorDef, ConnectorScope};

use super::types::ParamMap;
use crate::error::{ConfigReason, ConfigResult};
use crate::vars::inject_loader_scoped_vars;
use crate::vars::{ConfigVarContext, expand_value};
use orion_error::conversion::{SourceErr, SourceRawErr};
use orion_error::runtime::OperationContext;

// ---------------------------------------------------------------------------
// TOML file container for connector definitions
// ---------------------------------------------------------------------------

/// A `.toml` file containing one or more connector definitions.
///
/// ```toml
/// [[connectors]]
/// id = "file_json"
/// type = "file"
/// allow_override = ["base", "file", "sync"]
///
/// [connectors.params]
/// fmt = "json"
/// base = "alerts"
/// file = "default.jsonl"
/// sync = false
/// ```
#[derive(::moju_derive::MoJu, Debug, Deserialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct ConnectorTomlFile {
    pub connectors: Vec<ConnectorDefRaw>,
}

/// Raw TOML representation of a connector definition.
#[derive(::moju_derive::MoJu, Debug, Deserialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct ConnectorDefRaw {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub allow_override: Vec<String>,
    #[serde(default)]
    pub params: Option<toml::value::Table>,
}

impl ConnectorDefRaw {
    /// Convert to the canonical `ConnectorDef` from wp-connector-api.
    pub fn into_connector_def(self, origin: Option<String>) -> ConnectorDef {
        let default_params: ParamMap = self
            .params
            .map(parammap_from_toml_table)
            .unwrap_or_default();

        ConnectorDef {
            id: self.id,
            kind: self.kind,
            scope: ConnectorScope::Sink,
            allow_override: self.allow_override,
            default_params,
            origin,
        }
    }
}

fn parammap_from_toml_table(table: toml::value::Table) -> ParamMap {
    fn conv(value: toml::Value) -> serde_json::Value {
        match value {
            toml::Value::String(s) => serde_json::Value::String(s),
            toml::Value::Integer(i) => serde_json::Value::Number(i.into()),
            toml::Value::Float(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            toml::Value::Boolean(b) => serde_json::Value::Bool(b),
            toml::Value::Datetime(dt) => serde_json::Value::String(dt.to_string()),
            toml::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(conv).collect())
            }
            toml::Value::Table(entries) => serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, conv(value)))
                    .collect(),
            ),
        }
    }

    table
        .into_iter()
        .map(|(key, value)| (key, conv(value)))
        .collect()
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

/// Load all connector definitions from `*.toml` files in `dir`.
///
/// Returns an error if the directory doesn't exist or if any connector ID
/// appears more than once.
pub fn load_connector_defs(dir: &Path) -> ConfigResult<BTreeMap<String, ConnectorDef>> {
    load_connector_defs_with_context(dir, &ConfigVarContext::new(), None)
}

pub fn load_connector_defs_with_context(
    dir: &Path,
    ctx: &ConfigVarContext,
    work_dir: Option<&Path>,
) -> ConfigResult<BTreeMap<String, ConnectorDef>> {
    let mut result = BTreeMap::new();

    if !dir.is_dir() {
        return Ok(result);
    }

    let pattern = dir.join("*.toml");
    let pattern_str = pattern.to_string_lossy();

    for entry in glob::glob(&pattern_str).source_raw_err(
        ConfigReason::Sink,
        format!("invalid glob {}", pattern.display()),
    )? {
        let path = entry.source_raw_err(
            ConfigReason::Sink,
            format!("failed to read glob entry {}", pattern.display()),
        )?;
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
            .source_err(ConfigReason::Sink, "expand sink connector variables")
            .map_err(|err| {
                err.with_context(
                    OperationContext::doing("load sink connector")
                        .with_field("path", path.display()),
                )
            })?;
        if let Some(table) = expanded.as_table_mut() {
            table.remove("vars");
        }
        let expanded_toml = toml::to_string(&expanded).source_raw_err(
            ConfigReason::Sink,
            format!("failed to serialize expanded {}", path.display()),
        )?;
        let file: ConnectorTomlFile = toml::from_str(&expanded_toml).source_raw_err(
            ConfigReason::Sink,
            format!("failed to parse {}", path.display()),
        )?;

        let origin = path.display().to_string();
        for raw in file.connectors {
            let id = raw.id.clone();
            let def = raw.into_connector_def(Some(origin.clone()));
            if result.insert(id.clone(), def).is_some() {
                return ConfigReason::Sink.fail(format!(
                    "duplicate connector id {:?} in {}",
                    id,
                    path.display()
                ));
            }
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_connector_toml() {
        let toml_str = r#"
[[connectors]]
id = "file_json"
type = "file"
allow_override = ["base", "file", "sync"]

[connectors.params]
fmt = "json"
base = "alerts"
file = "default.jsonl"
sync = false
"#;
        let file: ConnectorTomlFile = toml::from_str(toml_str).unwrap();
        assert_eq!(file.connectors.len(), 1);
        assert_eq!(file.connectors[0].id, "file_json");
        assert_eq!(file.connectors[0].kind, "file");
        assert_eq!(
            file.connectors[0].allow_override,
            vec!["base", "file", "sync"]
        );

        let def = file
            .connectors
            .into_iter()
            .next()
            .unwrap()
            .into_connector_def(None);
        assert_eq!(def.id, "file_json");
        assert_eq!(def.kind, "file");
        assert_eq!(def.scope, ConnectorScope::Sink);
        assert_eq!(
            def.default_params.get("base"),
            Some(&serde_json::Value::String("alerts".into()))
        );
        assert_eq!(
            def.default_params.get("file"),
            Some(&serde_json::Value::String("default.jsonl".into()))
        );
    }

    #[test]
    fn load_from_missing_dir() {
        let result = load_connector_defs(Path::new("/nonexistent_dir_abc123"));
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
