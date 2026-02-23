use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

pub use wp_connector_api::{ConnectorDef, ConnectorScope};
use wp_connector_api::parammap_from_toml_table;

use super::types::ParamMap;

// ---------------------------------------------------------------------------
// TOML file container for connector definitions
// ---------------------------------------------------------------------------

/// A `.toml` file containing one or more connector definitions.
///
/// ```toml
/// [[connectors]]
/// id = "file_json"
/// type = "file"
/// allow_override = ["path"]
///
/// [connectors.params]
/// path = "alerts/default.jsonl"
/// ```
#[derive(Debug, Deserialize)]
pub struct ConnectorTomlFile {
    pub connectors: Vec<ConnectorDefRaw>,
}

/// Raw TOML representation of a connector definition.
#[derive(Debug, Deserialize)]
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
            .map(|t| parammap_from_toml_table(t))
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

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

/// Load all connector definitions from `*.toml` files in `dir`.
///
/// Returns an error if the directory doesn't exist or if any connector ID
/// appears more than once.
pub fn load_connector_defs(dir: &Path) -> anyhow::Result<BTreeMap<String, ConnectorDef>> {
    let mut result = BTreeMap::new();

    if !dir.is_dir() {
        return Ok(result);
    }

    let pattern = dir.join("*.toml");
    let pattern_str = pattern.to_string_lossy();

    for entry in glob::glob(&pattern_str)? {
        let path = entry?;
        let content = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", path.display()))?;
        let file: ConnectorTomlFile = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse {}: {e}", path.display()))?;

        let origin = path.display().to_string();
        for raw in file.connectors {
            let id = raw.id.clone();
            let def = raw.into_connector_def(Some(origin.clone()));
            if result.insert(id.clone(), def).is_some() {
                anyhow::bail!("duplicate connector id {:?} in {}", id, path.display());
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
allow_override = ["path"]

[connectors.params]
path = "alerts/default.jsonl"
"#;
        let file: ConnectorTomlFile = toml::from_str(toml_str).unwrap();
        assert_eq!(file.connectors.len(), 1);
        assert_eq!(file.connectors[0].id, "file_json");
        assert_eq!(file.connectors[0].kind, "file");
        assert_eq!(file.connectors[0].allow_override, vec!["path"]);

        let def = file.connectors.into_iter().next().unwrap().into_connector_def(None);
        assert_eq!(def.id, "file_json");
        assert_eq!(def.kind, "file");
        assert_eq!(def.scope, ConnectorScope::Sink);
        assert_eq!(
            def.default_params.get("path"),
            Some(&serde_json::Value::String("alerts/default.jsonl".into()))
        );
    }

    #[test]
    fn load_from_missing_dir() {
        let result = load_connector_defs(Path::new("/nonexistent_dir_abc123"));
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
