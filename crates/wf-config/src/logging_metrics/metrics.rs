use serde::Deserialize;

use crate::types::HumanDuration;

/// Runtime metrics settings.
///
/// When disabled, runtime metrics collection/export is skipped entirely.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq, Deserialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_report_interval")]
    pub report_interval: HumanDuration,
    #[serde(default = "default_prometheus_listen")]
    pub prometheus_listen: String,
    /// Emit the periodic metrics summary (snapshot + interval table) to the
    /// `res` tracing domain on each tick. Default `true` preserves prior
    /// behaviour; set `false` to silence console log output while keeping
    /// prometheus export and Top-N collection running.
    #[serde(default = "default_console_output")]
    pub console_output: bool,
    #[serde(default)]
    pub topn: MetricsTopNConfig,
}

/// Optional Top-N diagnostics settings.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq, Deserialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct MetricsTopNConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_topn_max")]
    pub max: usize,
    #[serde(default = "default_topn_queue_capacity")]
    pub queue_capacity: usize,
}

fn default_report_interval() -> HumanDuration {
    // 100ms：短跑诊断（perf-diag sentinel 轮次）不被 ~1s 粒度钉死——2026-08-23
    // 实测 300k 单流 floor 被 1s 指标粒度钉成 26 万 EPS 假象，改 100ms 后为 970 万。
    "100ms".parse().expect("hardcoded duration must parse")
}

fn default_prometheus_listen() -> String {
    "127.0.0.1:9901".to_string()
}

fn default_console_output() -> bool {
    true
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
            console_output: default_console_output(),
            topn: MetricsTopNConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_output_false_is_parsed() {
        // Regression for #61: `console_output = false` must be recognized,
        // not silently dropped by serde.
        let cfg: MetricsConfig = toml::from_str("console_output = false\n").unwrap();
        assert!(!cfg.console_output, "console_output=false must parse");
    }

    #[test]
    fn console_output_defaults_true_when_omitted() {
        let cfg: MetricsConfig = toml::from_str("").unwrap();
        assert!(
            cfg.console_output,
            "omitted console_output must default to true"
        );
    }

    #[test]
    fn full_metrics_section_parses_with_console_output() {
        let toml_src = r#"
            enabled = true
            report_interval = "1s"
            prometheus_listen = "127.0.0.1:9901"
            console_output = false
        "#;
        let cfg: MetricsConfig = toml::from_str(toml_src).unwrap();
        assert!(cfg.enabled);
        assert!(!cfg.console_output);
    }

    #[test]
    fn default_report_interval_is_100ms() {
        // perf-diag 防假象协议：短跑不被 ~1s 指标粒度钉死（设计 §4.5）。
        let cfg = MetricsConfig::default();
        assert_eq!(
            cfg.report_interval.as_duration(),
            std::time::Duration::from_millis(100)
        );
        let parsed: MetricsConfig = toml::from_str("enabled = true\n").unwrap();
        assert_eq!(
            parsed.report_interval.as_duration(),
            std::time::Duration::from_millis(100),
            "omitted report_interval must default to 100ms"
        );
    }
}
