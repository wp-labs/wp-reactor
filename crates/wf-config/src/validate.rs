use std::collections::HashMap;
use std::net::ToSocketAddrs;
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

    // sinks path must be non-empty
    if config.sinks.is_empty() {
        anyhow::bail!("sinks must be a non-empty path to the sinks/ directory");
    }

    // metrics config sanity
    if config.metrics.report_interval.as_duration().is_zero() {
        anyhow::bail!("metrics.report_interval must be > 0");
    }
    if config.metrics.topn.max == 0 {
        anyhow::bail!("metrics.topn.max must be > 0");
    }
    if config.metrics.topn.queue_capacity == 0 {
        anyhow::bail!("metrics.topn.queue_capacity must be > 0");
    }
    if config.metrics.enabled {
        if config.metrics.prometheus_listen.trim().is_empty() {
            anyhow::bail!("metrics.prometheus_listen must be non-empty when metrics.enabled=true");
        }
        // Must be host:port (no scheme).
        if config
            .metrics
            .prometheus_listen
            .to_socket_addrs()
            .map_err(|e| anyhow::anyhow!("metrics.prometheus_listen invalid: {e}"))?
            .next()
            .is_none()
        {
            anyhow::bail!("metrics.prometheus_listen resolved to no socket address");
        }
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
/// the `over_cap` configured in `wfusion.toml`.
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
                "window {name:?} found in .wfs schema but not in wfusion.toml [window.{name}]"
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
}
