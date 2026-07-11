use serde::Deserialize;

use super::expect::{GroupExpectSpec, SinkExpectOverride};
use super::types::{ParamMap, StringOrArray};

// ---------------------------------------------------------------------------
// RouteFile — top-level TOML structure for business/infra route files
// ---------------------------------------------------------------------------

/// Top-level TOML structure for a route file.
///
/// ```toml
/// version = "1.0"
///
/// [sink_group]
/// name = "security_output"
/// windows = ["security_*"]
///
/// [[sink_group.sinks]]
/// connect = "file_json"
/// ```
#[derive(::moju_derive::MoJu, Debug, Deserialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct RouteFile {
    #[allow(dead_code)]
    pub version: Option<String>,
    pub sink_group: RouteGroup,
}

// ---------------------------------------------------------------------------
// RouteGroup — a single sink routing group
// ---------------------------------------------------------------------------

/// A sink group that routes alerts based on yield-target window name matching.
#[derive(::moju_derive::MoJu, Debug, Deserialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct RouteGroup {
    /// Group name (unique across all groups).
    pub name: String,
    /// Max parallel writers; default 1, upper limit 10.
    #[serde(default)]
    pub parallel: Option<usize>,
    /// Window name patterns for routing (replaces OML/rule matching).
    pub windows: Option<StringOrArray>,
    /// Tags to attach to all alerts in this group.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    /// WarpFusion-managed `__wfu_*` metadata fields to disable in this group's output.
    #[serde(default)]
    pub wf_meta_disable: Vec<String>,
    /// Group-level expect specification.
    pub expect: Option<GroupExpectSpec>,
    /// Sinks within this group.
    pub sinks: Vec<RouteSink>,
}

// ---------------------------------------------------------------------------
// RouteSink — a single sink within a group
// ---------------------------------------------------------------------------

/// A single sink definition within a route group.
#[derive(::moju_derive::MoJu, Debug, Deserialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.SinkConfig")]
pub struct RouteSink {
    /// Connector ID reference (must exist in connector definitions).
    pub connect: String,
    /// Sink instance name; defaults to `"[index]"` if not specified.
    pub name: Option<String>,
    /// Output field projection/order for this sink.
    pub fields: Option<StringOrArray>,
    /// Parameter overrides (merged with connector defaults, subject to allow_override).
    #[serde(default)]
    pub params: ParamMap,
    /// Tags to attach to alerts for this specific sink.
    pub tags: Option<Vec<String>>,
    /// Per-sink expect overrides.
    pub expect: Option<SinkExpectOverride>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_route_file() {
        let toml_str = r#"
version = "1.0"

[sink_group]
name = "security_output"
windows = ["security_*"]
wf_meta_disable = ["__wfu_rule_name"]

[[sink_group.sinks]]
connect = "file_json"
name = "sec_file"

[sink_group.sinks.params]
file = "security_alerts.jsonl"
"#;
        let file: RouteFile = toml::from_str(toml_str).unwrap();
        assert_eq!(file.sink_group.name, "security_output");
        assert_eq!(
            file.sink_group.wf_meta_disable,
            vec!["__wfu_rule_name".to_string()]
        );
        let windows = file.sink_group.windows.as_ref().unwrap();
        assert_eq!(windows.0, vec!["security_*"]);
        assert_eq!(file.sink_group.sinks.len(), 1);
        assert_eq!(file.sink_group.sinks[0].connect, "file_json");
        assert!(file.sink_group.sinks[0].fields.is_none());
    }

    #[test]
    fn parse_infra_group() {
        let toml_str = r#"
[sink_group]
name = "__default"

[[sink_group.sinks]]
connect = "file_json"

[sink_group.sinks.params]
file = "unrouted.jsonl"
"#;
        let file: RouteFile = toml::from_str(toml_str).unwrap();
        assert_eq!(file.sink_group.name, "__default");
        assert!(file.sink_group.wf_meta_disable.is_empty());
        assert!(file.sink_group.windows.is_none());
    }

    #[test]
    fn parse_single_window_string() {
        let toml_str = r#"
[sink_group]
name = "single"
windows = "security_*"

[[sink_group.sinks]]
connect = "file_json"
fields = ["a", "b"]
"#;
        let file: RouteFile = toml::from_str(toml_str).unwrap();
        let windows = file.sink_group.windows.as_ref().unwrap();
        assert_eq!(windows.0, vec!["security_*"]);
        assert_eq!(
            file.sink_group.sinks[0]
                .fields
                .as_ref()
                .map(|f| f.0.clone()),
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }
}
