use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

/// Logging configuration. All fields have defaults so the entire `[logging]`
/// section may be omitted from `wfusion.toml`.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct LoggingConfig {
    /// Global log level filter (e.g. `"info"`, `"debug"`).
    pub level: String,
    /// Per-module level overrides, e.g. `{ "wf_runtime::receiver" = "debug" }`.
    pub modules: HashMap<String, String>,
    /// Optional file path for log output. Relative paths are resolved against
    /// the config file's parent directory.
    pub file: Option<PathBuf>,
    /// Output format: `plain` (human-readable) or `json` (structured).
    pub format: LogFormat,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            modules: HashMap::new(),
            file: None,
            format: LogFormat::Plain,
        }
    }
}

/// Log output format.
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[moju(kind = "state", domain = "Config", module = "Config.ConfigIo")]
pub enum LogFormat {
    Plain,
    Json,
}
