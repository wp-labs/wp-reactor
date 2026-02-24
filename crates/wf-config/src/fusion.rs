use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use serde::Deserialize;

use crate::alert::AlertConfig;
use crate::logging::LoggingConfig;
use crate::runtime::RuntimeConfig;
use crate::server::ServerConfig;
use crate::validate;
use crate::window::{WindowConfig, WindowDefaults, WindowOverride};

// ---------------------------------------------------------------------------
// Raw TOML structure (intermediate representation)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct FusionConfigRaw {
    server: ServerConfig,
    runtime: RuntimeConfig,
    window_defaults: WindowDefaults,
    #[serde(default)]
    window: HashMap<String, WindowOverride>,
    #[serde(default)]
    alert: AlertConfig,
    /// Path to the sinks/ directory for connector-based sink routing.
    /// When present, the new sink system is used instead of alert.sinks.
    #[serde(default)]
    sinks: Option<String>,
    #[serde(default)]
    logging: LoggingConfig,
    /// User-defined variables for WFL `$VAR` / `${VAR:default}` preprocessing.
    #[serde(default)]
    vars: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// FusionConfig (resolved, validated)
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct FusionConfig {
    pub server: ServerConfig,
    pub runtime: RuntimeConfig,
    pub window_defaults: WindowDefaults,
    pub windows: Vec<WindowConfig>,
    pub alert: AlertConfig,
    /// Path to the sinks/ directory for connector-based sink routing.
    /// When present, the new sink system is used instead of `alert.sinks`.
    pub sinks: Option<String>,
    pub logging: LoggingConfig,
    /// User-defined variables for WFL `$VAR` / `${VAR:default}` preprocessing.
    pub vars: HashMap<String, String>,
}

impl FusionConfig {
    /// Read and parse a `wfusion.toml` file.
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", path.as_ref().display()))?;
        content.parse()
    }
}

impl FromStr for FusionConfig {
    type Err = anyhow::Error;

    /// Parse a TOML string into a resolved, validated [`FusionConfig`].
    fn from_str(toml_str: &str) -> anyhow::Result<Self> {
        let raw: FusionConfigRaw = toml::from_str(toml_str)?;

        // Resolve window overrides against defaults.
        let mut windows = Vec::with_capacity(raw.window.len());
        for (name, ovr) in raw.window {
            let wc = ovr.resolve(name, &raw.window_defaults)?;
            windows.push(wc);
        }
        // Sort by name for deterministic ordering.
        windows.sort_by(|a, b| a.name.cmp(&b.name));

        let config = FusionConfig {
            server: raw.server,
            runtime: raw.runtime,
            window_defaults: raw.window_defaults,
            windows,
            alert: raw.alert,
            sinks: raw.sinks,
            logging: raw.logging,
            vars: raw.vars,
        };

        validate::validate(&config)?;

        Ok(config)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ByteSize, DistMode, EvictPolicy, HumanDuration, LatePolicy};
    use std::time::Duration;

    const FULL_TOML: &str = r#"
[server]
listen = "tcp://127.0.0.1:9800"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"

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

[alert]
sinks = ["file:///var/log/wf-alerts.jsonl"]
"#;

    #[test]
    fn load_full_toml() {
        let cfg: FusionConfig = FULL_TOML.parse().unwrap();

        // server
        assert_eq!(cfg.server.listen, "tcp://127.0.0.1:9800");

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

        // alert
        assert_eq!(cfg.alert.sinks, vec!["file:///var/log/wf-alerts.jsonl"],);
    }

    #[test]
    fn reject_invalid_listen() {
        let toml = FULL_TOML.replace("tcp://127.0.0.1:9800", "http://bad");
        assert!(toml.parse::<FusionConfig>().is_err());
    }

    #[test]
    fn reject_zero_parallelism() {
        let toml = FULL_TOML.replace("executor_parallelism = 2", "executor_parallelism = 0");
        assert!(toml.parse::<FusionConfig>().is_err());
    }

    #[test]
    fn reject_partitioned_no_key() {
        let toml = FULL_TOML.replace(
            "[window.auth_events]\nmode = \"local\"",
            "[window.auth_events]\nmode = \"partitioned\"",
        );
        assert!(toml.parse::<FusionConfig>().is_err());
    }

    #[test]
    fn reject_unknown_mode() {
        let toml = FULL_TOML.replace(
            "[window.auth_events]\nmode = \"local\"",
            "[window.auth_events]\nmode = \"distributed\"",
        );
        assert!(toml.parse::<FusionConfig>().is_err());
    }

    #[test]
    fn reject_window_exceeds_total() {
        // Set max_total_bytes very small so a window exceeds it.
        let toml = FULL_TOML.replace("max_total_bytes = \"2GB\"", "max_total_bytes = \"32MB\"");
        assert!(toml.parse::<FusionConfig>().is_err());
    }

    #[test]
    fn missing_server_fails() {
        let toml = r#"
[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/*.wfs"
rules   = "rules/*.wfl"

[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[alert]
sinks = ["file:///var/log/wf-alerts.jsonl"]
"#;
        assert!(toml.parse::<FusionConfig>().is_err());
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
        let cfg: FusionConfig = toml.parse().unwrap();
        assert_eq!(cfg.vars.len(), 2);
        assert_eq!(cfg.vars["FAIL_THRESHOLD"], "5");
        assert_eq!(cfg.vars["SCAN_THRESHOLD"], "10");
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
        let err = toml.parse::<FusionConfig>().unwrap_err();
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
        let err = toml.parse::<FusionConfig>().unwrap_err();
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
        let cfg: FusionConfig = toml.parse().unwrap();
        assert_eq!(cfg.vars["_PRIVATE"], "ok");
        assert_eq!(cfg.vars["MAX_COUNT_2"], "99");
    }
}
