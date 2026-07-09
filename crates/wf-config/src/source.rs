use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use toml::Value as TomlValue;

/// A single `[[sources]]` entry.
///
/// Supports two formats:
///
/// 1. Legacy flat format:
/// ```toml
/// type = "file"
/// key = "netflow_file"
/// enable = true
/// path = "data/events.ndjson"
/// stream = "netflow"
/// ```
///
/// 2. Standard connector format:
/// ```toml
/// connect = "kafka_src"
/// key = "kafka_1"
/// stream = "nginx_access"
/// brokers = "localhost:9092"
/// topic = "wp_nginx_logs"
/// ```
///
/// When `connect` is set, `type` is optional — the kind is resolved from the
/// connector registry at runtime via [`SourceConfig::resolve_kind`].
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq, Serialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.SourceConfig")]
pub struct SourceConfig {
    #[serde(default, alias = "key")]
    pub name: Option<String>,
    /// Legacy: direct source kind (e.g. `"file"`, `"tcp"`, `"kafka"`).
    #[serde(rename = "type", default)]
    pub source_type: Option<String>,
    /// Standard: connector id (e.g. `"kafka_src"`, `"file_src"`).
    /// Resolved to a kind via the connector registry.
    #[serde(default)]
    pub connect: Option<String>,
    #[serde(default = "default_true", rename = "enable")]
    pub enabled: bool,
    /// All other fields (flat or under `[sources.params]`) are captured here.
    #[serde(default, flatten)]
    pub params: BTreeMap<String, String>,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            name: None,
            source_type: None,
            connect: None,
            enabled: true,
            params: BTreeMap::new(),
        }
    }
}

impl SourceConfig {
    pub fn effective_name(&self, index: usize) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| format!("{}_{}", self.kind(), index + 1))
    }

    /// Return the effective source kind.
    ///
    /// Priority: `source_type` > `connect` derived kind > `"unknown"`.
    /// For standard connector ids (`<kind>_src` → `<kind>`), the kind is
    /// derived directly without needing the connector registry.
    pub fn kind(&self) -> &str {
        if let Some(t) = self.source_type.as_deref() {
            return t;
        }
        if let Some(conn) = self.connect.as_deref() {
            if let Some(kind) = conn.strip_suffix("_src") {
                return kind;
            }
            // Return the connector id itself as fallback
            return conn;
        }
        "unknown"
    }

    /// Resolve `connect` → kind via a lookup function, storing the result in
    /// `source_type`. Returns the resolved kind.
    ///
    /// The lookup function receives a connector id (e.g. `"kafka_src"`) and
    /// should return the corresponding kind (e.g. `"kafka"`), or `None` if
    /// the connector is not found.
    pub fn resolve_kind(&mut self, lookup: impl Fn(&str) -> Option<String>) -> Option<&str> {
        if self.source_type.is_none()
            && let Some(ref conn) = self.connect
            && let Some(kind) = lookup(conn)
        {
            self.source_type = Some(kind);
        }
        self.source_type.as_deref()
    }
}

fn default_true() -> bool {
    true
}

impl<'de> Deserialize<'de> for SourceConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawSourceConfig {
            #[serde(default, alias = "key")]
            name: Option<String>,
            #[serde(rename = "type", default)]
            source_type: Option<String>,
            #[serde(default)]
            connect: Option<String>,
            #[serde(default = "default_true", rename = "enable")]
            enabled: bool,
            #[serde(default, alias = "params_override")]
            params: BTreeMap<String, TomlValue>,
            #[serde(default, flatten)]
            flat_params: BTreeMap<String, TomlValue>,
        }

        let mut raw = RawSourceConfig::deserialize(deserializer)?;
        if raw.flat_params.contains_key("enabled") {
            return Err(D::Error::custom(
                "source uses `enable`, not `enabled`; replace `enabled = ...` with `enable = ...`",
            ));
        }
        if let Some(value) = raw.flat_params.remove("vars")
            && !matches!(value, TomlValue::Table(_))
        {
            return Err(D::Error::custom(
                "`vars` is a reserved source field and cannot be used as a source parameter",
            ));
        }

        let mut params = BTreeMap::new();
        for (key, value) in std::mem::take(&mut raw.params) {
            let value = param_value_to_string("params", &key, value)?;
            params.insert(key, value);
        }
        for (key, value) in raw.flat_params {
            let value = param_value_to_string("source", &key, value)?;
            params.insert(key, value);
        }

        Ok(SourceConfig {
            name: raw.name,
            source_type: raw.source_type,
            connect: raw.connect,
            enabled: raw.enabled,
            params,
        })
    }
}

fn param_value_to_string<E>(scope: &str, key: &str, value: TomlValue) -> Result<String, E>
where
    E: DeError,
{
    match value {
        TomlValue::String(value) => Ok(value),
        TomlValue::Integer(value) => Ok(value.to_string()),
        TomlValue::Float(value) => Ok(value.to_string()),
        TomlValue::Boolean(value) => Ok(value.to_string()),
        TomlValue::Datetime(value) => Ok(value.to_string()),
        TomlValue::Array(_) | TomlValue::Table(_) => Err(E::custom(format!(
            "{scope} field {key:?} must be a scalar value"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_legacy_flat_format() {
        let toml = r#"
type = "file"
key = "netflow_file"
path = "data/events.ndjson"
stream = "netflow"
format = "ndjson"
"#;
        let s: SourceConfig = toml::from_str(toml).unwrap();
        assert_eq!(s.kind(), "file");
        assert_eq!(s.name.as_deref(), Some("netflow_file"));
        assert_eq!(s.params.get("path").unwrap(), "data/events.ndjson");
        assert!(s.connect.is_none());
    }

    #[test]
    fn parse_connector_format_with_flat_params() {
        let toml = r#"
key = "kafka_1"
connect = "kafka_src"
stream = "nginx_access"
brokers = "localhost:9092"
topic = "wp_nginx_logs"
group_id = "wfusion"
"#;
        let s: SourceConfig = toml::from_str(toml).unwrap();
        assert_eq!(s.name.as_deref(), Some("kafka_1"));
        assert_eq!(s.connect.as_deref(), Some("kafka_src"));
        assert!(s.source_type.is_none());
        assert_eq!(s.params.get("brokers").unwrap(), "localhost:9092");
        assert_eq!(s.params.get("topic").unwrap(), "wp_nginx_logs");
        assert_eq!(s.params.get("stream").unwrap(), "nginx_access");
    }

    #[test]
    fn parse_connector_format_with_nested_params() {
        let toml = r#"
key = "kafka_1"
connect = "kafka_src"

[params]
stream = "nginx_access"
brokers = "localhost:9092"
topic = "wp_nginx_logs"
group_id = "wfusion"
"#;
        let s: SourceConfig = toml::from_str(toml).unwrap();
        assert_eq!(s.name.as_deref(), Some("kafka_1"));
        assert_eq!(s.connect.as_deref(), Some("kafka_src"));
        assert_eq!(s.params.get("brokers").unwrap(), "localhost:9092");
        assert_eq!(s.params.get("topic").unwrap(), "wp_nginx_logs");
        assert_eq!(s.params.get("stream").unwrap(), "nginx_access");
    }

    #[test]
    fn parse_connector_params_override_alias() {
        let toml = r#"
key = "file_1"
connect = "file_src"

[params_override]
path = "data/events.ndjson"
stream = "events"
"#;
        let s: SourceConfig = toml::from_str(toml).unwrap();
        assert_eq!(s.params.get("path").unwrap(), "data/events.ndjson");
        assert_eq!(s.params.get("stream").unwrap(), "events");
    }

    #[test]
    fn flat_params_override_nested_params() {
        let toml = r#"
key = "file_1"
connect = "file_src"
path = "data/flat.ndjson"

[params]
path = "data/nested.ndjson"
stream = "events"
"#;
        let s: SourceConfig = toml::from_str(toml).unwrap();
        assert_eq!(s.params.get("path").unwrap(), "data/flat.ndjson");
        assert_eq!(s.params.get("stream").unwrap(), "events");
    }

    #[test]
    fn parse_scalar_source_params_as_strings() {
        let toml = r#"
key = "tcp_1"
connect = "tcp_src"
port = 9800
tls = false
ratio = 1.5
"#;
        let s: SourceConfig = toml::from_str(toml).unwrap();
        assert_eq!(s.params.get("port").unwrap(), "9800");
        assert_eq!(s.params.get("tls").unwrap(), "false");
        assert_eq!(s.params.get("ratio").unwrap(), "1.5");
    }

    #[test]
    fn resolve_kind_from_connect() {
        let mut s = SourceConfig {
            name: Some("kafka_1".into()),
            source_type: None,
            connect: Some("kafka_src".into()),
            ..Default::default()
        };
        // kind() derives from connect before explicit resolution
        assert_eq!(s.kind(), "kafka");
        // Explicit resolution also works (e.g. for non-standard connector ids)
        let kind = s.resolve_kind(|id| {
            if id == "kafka_src" {
                Some("kafka".into())
            } else {
                None
            }
        });
        assert_eq!(kind, Some("kafka"));
        assert_eq!(s.source_type.as_deref(), Some("kafka"));
    }

    #[test]
    fn resolve_kind_keeps_existing_type() {
        let mut s = SourceConfig {
            source_type: Some("tcp".into()),
            ..Default::default()
        };
        let kind = s.resolve_kind(|_| panic!("should not be called"));
        assert_eq!(kind, Some("tcp"));
    }

    #[test]
    fn kind_derived_from_connect_file_src() {
        let s = SourceConfig {
            connect: Some("file_src".into()),
            ..Default::default()
        };
        assert_eq!(s.kind(), "file");
    }

    #[test]
    fn kind_derived_from_connect_tcp_src() {
        let s = SourceConfig {
            connect: Some("tcp_src".into()),
            ..Default::default()
        };
        assert_eq!(s.kind(), "tcp");
    }

    #[test]
    fn kind_returns_connect_when_no_src_suffix() {
        let s = SourceConfig {
            connect: Some("custom_connector".into()),
            ..Default::default()
        };
        assert_eq!(s.kind(), "custom_connector");
    }

    #[test]
    fn parse_tcp_legacy_format() {
        let toml = r#"
type = "tcp"
listen = "tcp://0.0.0.0:9800"
"#;
        let s: SourceConfig = toml::from_str(toml).unwrap();
        assert_eq!(s.kind(), "tcp");
        assert_eq!(s.params.get("listen").unwrap(), "tcp://0.0.0.0:9800");
    }

    #[test]
    fn source_enable_defaults_to_true() {
        let toml = r#"
type = "file"
path = "data/events.ndjson"
stream = "events"
"#;
        let s: SourceConfig = toml::from_str(toml).unwrap();
        assert!(s.enabled);
    }

    #[test]
    fn parse_source_enable_false() {
        let toml = r#"
type = "file"
enable = false
path = "data/events.ndjson"
stream = "events"
"#;
        let s: SourceConfig = toml::from_str(toml).unwrap();
        assert!(!s.enabled);
    }

    #[test]
    fn parse_source_rejects_enabled_field() {
        let toml = r#"
type = "file"
enabled = false
path = "data/events.ndjson"
stream = "events"
"#;
        let err = toml::from_str::<SourceConfig>(toml).unwrap_err();
        assert!(err.to_string().contains("source uses `enable`"));
    }

    #[test]
    fn parse_source_ignores_loader_vars_table() {
        let toml = r#"
type = "file"
path = "data/events.ndjson"
stream = "events"

[vars]
WORK_DIR = "/tmp/work"
"#;
        let s: SourceConfig = toml::from_str(toml).unwrap();
        assert!(!s.params.contains_key("vars"));
    }

    #[test]
    fn parse_source_rejects_vars_scalar_param() {
        let toml = r#"
type = "file"
vars = "not allowed"
path = "data/events.ndjson"
stream = "events"
"#;
        let err = toml::from_str::<SourceConfig>(toml).unwrap_err();
        assert!(
            err.to_string()
                .contains("`vars` is a reserved source field")
        );
    }

    #[test]
    fn parse_source_rejects_vars_array_param() {
        let toml = r#"
type = "file"
vars = ["not", "allowed"]
path = "data/events.ndjson"
stream = "events"
"#;
        let err = toml::from_str::<SourceConfig>(toml).unwrap_err();
        assert!(
            err.to_string()
                .contains("`vars` is a reserved source field")
        );
    }

    #[test]
    fn serialize_source_uses_enable_key() {
        let s = SourceConfig {
            source_type: Some("file".into()),
            enabled: false,
            ..Default::default()
        };
        let out = toml::to_string(&s).unwrap();
        assert!(out.contains("enable = false"));
        assert!(!out.contains("enabled"));
    }
}
