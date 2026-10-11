use serde::Deserialize;

use crate::types::HumanDuration;

/// Runtime metrics settings.
///
/// When disabled, runtime metrics collection/export is skipped entirely.
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq, Eq, Deserialize)]
#[jumo(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
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
    /// Monitor-output filtering (`[metrics.monitor]`). Absent → every record is
    /// forwarded to the monitor sinks (pre-filtering behaviour preserved).
    #[serde(default)]
    pub monitor: MetricsMonitorConfig,
}

/// Monitor-output filter settings (`[metrics.monitor]`).
///
/// Selects which [`MetricsRecord`](crate::logging_metrics) records are forwarded
/// to the monitor sinks by matching the record's `stage` / `name` / `label`
/// metadata against glob rules. Filtering never touches rule processing or alert
/// output — it only trims the monitor fan-out.
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[jumo(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct MetricsMonitorConfig {
    /// Whether a record matched by no rule is forwarded (`include`) or dropped
    /// (`exclude`). Defaults to `include` so an unconfigured filter is a no-op.
    pub default: MonitorDefault,
    /// Rules that force inclusion (lower priority than `exclude`).
    pub include: Vec<MetricsMonitorRule>,
    /// Rules that force exclusion (highest priority).
    pub exclude: Vec<MetricsMonitorRule>,
}

/// Default disposition for records matched by no `[metrics.monitor]` rule.
#[derive(::jumo_derive::Jumo, Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[jumo(kind = "state", domain = "Config", module = "Config.ConfigIo")]
pub enum MonitorDefault {
    /// Forward records not matched by any rule.
    Include,
    /// Drop records not matched by any rule.
    Exclude,
}

/// One `[metrics.monitor]` include/exclude rule.
///
/// `stage` / `name` are glob patterns (default `"*"` = match anything).
/// `target` matches the record's `label`:
/// * omitted → matches only **global** records (those without a `label` field);
/// * `"*"` → matches any record, labelled or not;
/// * otherwise → matches records whose `label` matches the glob.
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(default)]
#[jumo(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct MetricsMonitorRule {
    /// Stage glob, e.g. `"rule"`, `"receiver"`, `"*"`.
    pub stage: Option<String>,
    /// Metric-name glob, e.g. `"flush_seconds_*"`, `"decode_seconds_*"`, `"*"`.
    pub name: Option<String>,
    /// Target (`label`) glob; `None` = global metrics only, `"*"` = any target.
    pub target: Option<String>,
}

/// Optional Top-N diagnostics settings.
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq, Eq, Deserialize)]
#[jumo(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
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

impl Default for MetricsMonitorConfig {
    fn default() -> Self {
        Self {
            default: MonitorDefault::Include,
            include: Vec::new(),
            exclude: Vec::new(),
        }
    }
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
            monitor: MetricsMonitorConfig::default(),
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

    #[test]
    fn monitor_defaults_to_include_without_rules() {
        let cfg = MetricsConfig::default();
        assert_eq!(cfg.monitor.default, MonitorDefault::Include);
        assert!(cfg.monitor.include.is_empty());
        assert!(cfg.monitor.exclude.is_empty());

        let parsed: MetricsConfig = toml::from_str("enabled = true\n").unwrap();
        assert_eq!(parsed.monitor.default, MonitorDefault::Include);
        assert!(parsed.monitor.exclude.is_empty());
    }

    #[test]
    fn monitor_parses_issue_115_shape() {
        let toml_src = r#"
            enabled = true
            report_interval = "30s"

            [monitor]
            default = "include"

            [[monitor.exclude]]
            stage = "rule"
            name = "flush_seconds_*"
            target = "*"

            [[monitor.exclude]]
            stage = "receiver"
            name = "decode_seconds_*"
        "#;
        let cfg: MetricsConfig = toml::from_str(toml_src).unwrap();
        assert_eq!(cfg.monitor.default, MonitorDefault::Include);
        assert_eq!(cfg.monitor.exclude.len(), 2);
        let first = &cfg.monitor.exclude[0];
        assert_eq!(first.stage.as_deref(), Some("rule"));
        assert_eq!(first.name.as_deref(), Some("flush_seconds_*"));
        assert_eq!(first.target.as_deref(), Some("*"));
        let second = &cfg.monitor.exclude[1];
        assert_eq!(second.target, None, "omitted target must be None");
    }

    #[test]
    fn monitor_default_exclude_parses() {
        let toml_src = r#"
            [monitor]
            default = "exclude"

            [[monitor.include]]
            stage = "alert"
            name = "emitted_total"
        "#;
        let cfg: MetricsConfig = toml::from_str(toml_src).unwrap();
        assert_eq!(cfg.monitor.default, MonitorDefault::Exclude);
        assert_eq!(cfg.monitor.include.len(), 1);
    }

    #[test]
    fn monitor_rejects_unknown_default_value() {
        let err = toml::from_str::<MetricsConfig>("[monitor]\ndefault = \"nope\"\n").unwrap_err();
        assert!(
            err.to_string().contains("nope"),
            "unknown default must be rejected: {err}"
        );
    }

    #[test]
    fn monitor_rule_omitted_fields_default_to_none() {
        let cfg: MetricsConfig = toml::from_str("[[monitor.exclude]]\nname = \"x\"\n").unwrap();
        let rule = &cfg.monitor.exclude[0];
        assert_eq!(rule.stage, None);
        assert_eq!(rule.name.as_deref(), Some("x"));
        assert_eq!(rule.target, None);
    }

    #[test]
    fn monitor_mixed_include_and_exclude_parse() {
        let toml_src = r#"
            [monitor]
            default = "exclude"

            [[monitor.include]]
            stage = "alert"
            name = "emitted_total"

            [[monitor.exclude]]
            stage = "alert"
            name = "emitted_total"
            target = "noisy_*"
        "#;
        let cfg: MetricsConfig = toml::from_str(toml_src).unwrap();
        assert_eq!(cfg.monitor.default, MonitorDefault::Exclude);
        assert_eq!(cfg.monitor.include.len(), 1);
        assert_eq!(cfg.monitor.exclude.len(), 1);
        assert_eq!(cfg.monitor.exclude[0].target.as_deref(), Some("noisy_*"));
    }
}
