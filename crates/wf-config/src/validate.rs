use std::collections::HashMap;
use std::time::Duration;

use crate::fusion::FusionConfig;
use crate::window::WindowConfig;

/// Internal validation, called automatically during `FusionConfig::from_str` / `load`.
pub(crate) fn validate(config: &FusionConfig) -> anyhow::Result<()> {
    // server.listen must start with tcp://
    if !config.server.listen.starts_with("tcp://") {
        anyhow::bail!(
            "server.listen must start with \"tcp://\", got {:?}",
            config.server.listen,
        );
    }

    // runtime.executor_parallelism > 0
    if config.runtime.executor_parallelism == 0 {
        anyhow::bail!("runtime.executor_parallelism must be > 0");
    }

    // Each window's max_window_bytes ≤ window_defaults.max_total_bytes
    let max_total = config.window_defaults.max_total_bytes.as_bytes();
    for w in &config.windows {
        if w.max_window_bytes.as_bytes() > max_total {
            anyhow::bail!(
                "window {:?}: max_window_bytes ({}) exceeds window_defaults.max_total_bytes ({})",
                w.name,
                w.max_window_bytes,
                config.window_defaults.max_total_bytes,
            );
        }
    }

    // vars keys must be valid WFL identifiers: [A-Za-z_][A-Za-z0-9_]*
    for key in config.vars.keys() {
        if !is_valid_var_name(key) {
            anyhow::bail!(
                "vars: invalid variable name {:?} — must match [A-Za-z_][A-Za-z0-9_]*",
                key,
            );
        }
    }

    // alert.sinks must be non-empty and each URI must be parseable
    if config.alert.sinks.is_empty() {
        anyhow::bail!("alert.sinks must contain at least one sink URI");
    }
    for (i, uri) in config.alert.sinks.iter().enumerate() {
        crate::alert::parse_sink_uri(uri)
            .map_err(|e| anyhow::anyhow!("alert.sinks[{}]: {}", i, e))?;
    }

    Ok(())
}

/// A valid variable name starts with ASCII letter or underscore, followed by
/// ASCII alphanumerics or underscores.
fn is_valid_var_name(name: &str) -> bool {
    let mut chars = name.bytes();
    match chars.next() {
        Some(b) if b.is_ascii_alphabetic() || b == b'_' => {}
        _ => return false,
    }
    chars.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Cross-file validation: check that every window's `.wfs` `over` duration does not exceed
/// the `over_cap` configured in `fusion.toml`.
///
/// Call this after loading both the config and the `.wfs` schema files.
///
/// - `windows`: resolved window configs from `FusionConfig`.
/// - `window_overs`: map of window name → `over` duration parsed from `.wfs` files.
pub fn validate_over_vs_over_cap(
    windows: &[WindowConfig],
    window_overs: &HashMap<String, Duration>,
) -> anyhow::Result<()> {
    for (name, over) in window_overs {
        let wc = windows.iter().find(|w| w.name == *name).ok_or_else(|| {
            anyhow::anyhow!(
                "window {name:?} found in .wfs schema but not in fusion.toml [window.{name}]"
            )
        })?;
        let cap: Duration = wc.over_cap.into();
        if *over > cap {
            anyhow::bail!("window {name:?}: over ({over:?}) exceeds over_cap ({cap:?})",);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ByteSize, DistMode, EvictPolicy, HumanDuration, LatePolicy};

    fn sample_window(name: &str, over_cap_secs: u64) -> WindowConfig {
        WindowConfig {
            name: name.to_string(),
            mode: DistMode::Local,
            max_window_bytes: ByteSize::from(256 * 1024 * 1024),
            over_cap: HumanDuration::from(Duration::from_secs(over_cap_secs)),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: HumanDuration::from(Duration::from_secs(5)),
            allowed_lateness: HumanDuration::from(Duration::from_secs(0)),
            late_policy: LatePolicy::Drop,
        }
    }

    #[test]
    fn over_vs_over_cap_accept() {
        let windows = vec![sample_window("auth_events", 1800)]; // 30m
        let mut overs = HashMap::new();
        overs.insert("auth_events".into(), Duration::from_secs(300)); // 5m ≤ 30m
        assert!(validate_over_vs_over_cap(&windows, &overs).is_ok());
    }

    #[test]
    fn over_vs_over_cap_reject() {
        let windows = vec![sample_window("auth_events", 1800)]; // 30m
        let mut overs = HashMap::new();
        overs.insert("auth_events".into(), Duration::from_secs(3600)); // 60m > 30m
        let err = validate_over_vs_over_cap(&windows, &overs).unwrap_err();
        assert!(err.to_string().contains("auth_events"));
    }

    #[test]
    fn over_vs_over_cap_missing_window() {
        let windows = vec![sample_window("auth_events", 1800)];
        let mut overs = HashMap::new();
        overs.insert("unknown_window".into(), Duration::from_secs(300));
        assert!(validate_over_vs_over_cap(&windows, &overs).is_err());
    }

    #[test]
    fn reject_empty_alert_sinks() {
        use crate::FusionConfig;
        let toml = MINIMAL_TOML.replace(
            r#"sinks = ["file:///tmp/alerts.jsonl"]"#,
            "sinks = []",
        );
        let err = toml.parse::<FusionConfig>().unwrap_err();
        assert!(
            err.to_string().contains("at least one sink"),
            "expected empty-sinks error, got: {err}",
        );
    }

    #[test]
    fn reject_unknown_sink_scheme() {
        use crate::FusionConfig;
        let toml = MINIMAL_TOML.replace(
            r#"sinks = ["file:///tmp/alerts.jsonl"]"#,
            r#"sinks = ["http://localhost:9200"]"#,
        );
        let err = toml.parse::<FusionConfig>().unwrap_err();
        assert!(
            err.to_string().contains("alert.sinks[0]"),
            "expected indexed error, got: {err}",
        );
    }

    /// Minimal valid TOML for validation tests.
    const MINIMAL_TOML: &str = r#"
[server]
listen = "tcp://127.0.0.1:9800"

[runtime]
executor_parallelism = 1
rule_exec_timeout = "30s"
schemas = "*.wfs"
rules   = "*.wfl"

[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[alert]
sinks = ["file:///tmp/alerts.jsonl"]
"#;
}
