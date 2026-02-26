use serde::Deserialize;

use crate::types::HumanDuration;

/// Runtime metrics settings.
///
/// When disabled, runtime metrics collection/export is skipped entirely.
#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_report_interval")]
    pub report_interval: HumanDuration,
    #[serde(default = "default_prometheus_listen")]
    pub prometheus_listen: String,
    #[serde(default)]
    pub topn: MetricsTopNConfig,
}

/// Optional Top-N diagnostics settings.
#[derive(Debug, Clone, Deserialize)]
pub struct MetricsTopNConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_topn_max")]
    pub max: usize,
    #[serde(default = "default_topn_queue_capacity")]
    pub queue_capacity: usize,
}

fn default_report_interval() -> HumanDuration {
    "2s".parse().expect("hardcoded duration must parse")
}

fn default_prometheus_listen() -> String {
    "127.0.0.1:9901".to_string()
}

fn default_topn_max() -> usize {
    20
}

fn default_topn_queue_capacity() -> usize {
    4096
}

impl Default for MetricsTopNConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max: default_topn_max(),
            queue_capacity: default_topn_queue_capacity(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            report_interval: default_report_interval(),
            prometheus_listen: default_prometheus_listen(),
            topn: MetricsTopNConfig::default(),
        }
    }
}
