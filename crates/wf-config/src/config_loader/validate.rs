use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::time::Duration;

use orion_error::conversion::SourceErr;

use crate::config_loader::fusion::{FusionConfig, FusionMode};
use crate::error::{ConfigReason, ConfigResult};
use crate::output::OutputTimeZone;
use crate::window::WindowConfig;

/// Internal validation, called automatically during `FusionConfig::from_str` / `load`.
pub(crate) fn validate(config: &FusionConfig) -> ConfigResult<()> {
    // runtime.parse_parallelism > 0
    if config.runtime.parse_parallelism == 0 {
        return ConfigReason::Validation.fail("runtime.parse_parallelism must be > 0");
    }
    validate_output_config(config)?;

    // Each window's max_window_bytes ≤ window_defaults.max_total_bytes
    let max_total = config.window_defaults.max_total_bytes.as_bytes();
    for w in &config.windows {
        if w.max_window_bytes.as_bytes() > max_total {
            return ConfigReason::Validation.fail(format!(
                "window {:?}: max_window_bytes ({}) exceeds window_defaults.max_total_bytes ({})",
                w.name, w.max_window_bytes, config.window_defaults.max_total_bytes,
            ));
        }
    }

    // vars keys must be valid WFL identifiers: [A-Za-z_][A-Za-z0-9_]*
    for key in config.vars.keys() {
        if !is_valid_var_name(key) {
            return ConfigReason::Validation.fail(format!(
                "vars: invalid variable name {:?} - must match [A-Za-z_][A-Za-z0-9_]*",
                key,
            ));
        }
    }

    // sinks path must be non-empty
    if config.sinks.is_empty() {
        return ConfigReason::Validation
            .fail("sinks must be a non-empty path to the sinks/ directory");
    }

    // sources validation
    if config.sources.is_empty() {
        return ConfigReason::Validation.fail("at least one source is required");
    }
    let mut names = std::collections::HashSet::new();
    let mut enabled_count = 0usize;
    let mut enabled_file = 0usize;
    let mut enabled_non_file = 0usize;
    for (idx, source) in config.sources.iter().enumerate() {
        let name = source.effective_name(idx);
        if !names.insert(name.clone()) {
            return ConfigReason::Validation.fail(format!("duplicate source name: {name:?}"));
        }
        if !source.enabled {
            continue;
        }
        match source.kind() {
            "tcp" => {
                enabled_count += 1;
                enabled_non_file += 1;
                // Legacy inline format: requires listen = "tcp://..."
                if source.connect.is_none() {
                    let listen = source
                        .params
                        .get("listen")
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    if !listen.starts_with("tcp://") {
                        return ConfigReason::Validation.fail(format!(
                            "sources[{idx}] ({name}): tcp listen must start with \"tcp://\", got {:?}",
                            listen
                        ));
                    }
                }
                // connector-based format (addr + port): validated by SourceFactory::validate_spec
            }
            "file" => {
                enabled_count += 1;
                enabled_file += 1;
                let path = source.params.get("path").map(|s| s.as_str()).unwrap_or("");
                if path.trim().is_empty() {
                    return ConfigReason::Validation.fail(format!(
                        "sources[{idx}] ({name}): file path must be non-empty"
                    ));
                }
                let fmt = source_data_format(source);
                let stream_tag = source
                    .params
                    .get("stream_tag")
                    .map(|s| s.as_str())
                    .unwrap_or("");
                if fmt == "arrow_ipc" && stream_tag.trim().is_empty() {
                    return ConfigReason::Validation.fail(format!(
                        "sources[{idx}] ({name}): file stream_tag must be non-empty"
                    ));
                }
            }
            _ => {
                // External source types (e.g., kafka via wp-connectors registry).
                // Parameter-level validation is delegated to each factory's validate_spec.
                enabled_count += 1;
                enabled_non_file += 1;
                let fmt = source_data_format(source);
                let stream_tag = source
                    .params
                    .get("stream_tag")
                    .map(|s| s.as_str())
                    .unwrap_or("");
                if stream_tag.trim().is_empty() && !matches!(fmt, "ndjson" | "arrow_framed" | "csv")
                {
                    return ConfigReason::Validation.fail(format!(
                        "sources[{idx}] ({name}): external source stream_tag must be non-empty"
                    ));
                }
            }
        }
    }
    match config.mode {
        FusionMode::Daemon => {
            if enabled_count == 0 {
                return ConfigReason::Validation
                    .fail("daemon mode requires at least one enabled source");
            }
        }
        FusionMode::Batch => {
            if enabled_file == 0 {
                return ConfigReason::Validation
                    .fail("batch mode requires at least one enabled file source");
            }
            if enabled_non_file > 0 {
                return ConfigReason::Validation
                    .fail("batch mode does not allow enabled non-file sources");
            }
        }
    }

    // metrics config sanity
    if config.metrics.report_interval.as_duration().is_zero() {
        return ConfigReason::Validation.fail("metrics.report_interval must be > 0");
    }
    if config.metrics.topn.max == 0 {
        return ConfigReason::Validation.fail("metrics.topn.max must be > 0");
    }
    if config.metrics.topn.queue_capacity == 0 {
        return ConfigReason::Validation.fail("metrics.topn.queue_capacity must be > 0");
    }
    if config.metrics.enabled {
        if config.metrics.prometheus_listen.trim().is_empty() {
            return ConfigReason::Validation
                .fail("metrics.prometheus_listen must be non-empty when metrics.enabled=true");
        }
        // Must be host:port (no scheme).
        if config
            .metrics
            .prometheus_listen
            .to_socket_addrs()
            .source_err(
                ConfigReason::Validation,
                "metrics.prometheus_listen invalid",
            )?
            .next()
            .is_none()
        {
            return ConfigReason::Validation
                .fail("metrics.prometheus_listen resolved to no socket address");
        }
    }

    Ok(())
}

fn validate_output_config(config: &FusionConfig) -> ConfigResult<()> {
    if config.output.time_format.trim().is_empty() {
        return ConfigReason::Validation.fail("output.time_format must be non-empty");
    }
    for item in chrono::format::strftime::StrftimeItems::new(&config.output.time_format) {
        if matches!(item, chrono::format::Item::Error) {
            return ConfigReason::Validation.fail(format!(
                "output.time_format contains an invalid strftime specifier: {:?}",
                config.output.time_format
            ));
        }
    }
    match config.output.time_zone {
        OutputTimeZone::Utc => {}
    }
    Ok(())
}

fn source_data_format(source: &crate::SourceConfig) -> &str {
    source
        .params
        .get("data_format")
        .or_else(|| source.params.get("format"))
        .map(|s| s.as_str())
        .unwrap_or("ndjson")
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
) -> ConfigResult<()> {
    for (name, over) in window_overs {
        let Some(wc) = windows.iter().find(|w| w.name == *name) else {
            return ConfigReason::Validation.fail(format!(
                "window {name:?} found in .wfs schema but not in wfusion.toml [window.{name}]"
            ));
        };
        let cap: Duration = wc.over_cap.into();
        if *over > cap {
            return ConfigReason::Validation.fail(format!(
                "window {name:?}: over ({over:?}) exceeds over_cap ({cap:?})",
            ));
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
    use crate::SourceConfig;
    use crate::admin_api::AdminApiConf;
    use crate::config_loader::fusion::{FusionConfig, FusionMode};
    use crate::output::OutputConfig;
    use crate::types::{ByteSize, DistMode, EvictPolicy, HumanDuration, LatePolicy};
    use crate::window::WindowConfig;
    use std::collections::{BTreeMap, HashMap};

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
            table: None,
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

    // -----------------------------------------------------------------------
    // source validation tests
    // -----------------------------------------------------------------------

    /// Build a minimal daemon config with the given sources.
    fn make_config(mode: FusionMode, sources: Vec<SourceConfig>) -> FusionConfig {
        let window = WindowConfig {
            name: "w".into(),
            mode: DistMode::Local,
            max_window_bytes: ByteSize::from(256 * 1024 * 1024),
            over_cap: HumanDuration::from(Duration::from_secs(1800)),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: HumanDuration::from(Duration::from_secs(1)),
            allowed_lateness: HumanDuration::from(Duration::from_secs(0)),
            late_policy: LatePolicy::Drop,
            table: None,
        };
        FusionConfig {
            mode,
            runtime: crate::config_loader::runtime::RuntimeConfig {
                parse_parallelism: 2,
                rule_parallelism: 1,
                rule_exec_timeout: "30s".parse().unwrap(),
                max_ingest_rate: None,
                schemas: "schemas/*.wfs".into(),
                rules: "rules/*.wfl".into(),
            },
            window_defaults: crate::window::WindowDefaults {
                evict_interval: "30s".parse().unwrap(),
                max_window_bytes: ByteSize::from(256 * 1024 * 1024),
                max_total_bytes: ByteSize::from(2 * 1024 * 1024 * 1024),
                evict_policy: EvictPolicy::TimeFirst,
                watermark: "1s".parse().unwrap(),
                allowed_lateness: "0s".parse().unwrap(),
                late_policy: LatePolicy::Drop,
            },
            windows: vec![window],
            sinks: "sinks".into(),
            work_root: None,
            logging: Default::default(),
            metrics: Default::default(),
            output: OutputConfig::default(),
            vars: HashMap::new(),
            sources,
            admin_api: AdminApiConf::default(),
            project_remote: Default::default(),
        }
    }

    fn src_enabled(kind: &str) -> SourceConfig {
        let mut params = BTreeMap::new();
        if kind != "tcp" && kind != "file" {
            params.insert("stream_tag".into(), "events".into());
        }
        SourceConfig {
            name: None,
            connect: None,
            source_type: Some(kind.into()),
            enabled: true,
            params,
        }
    }

    fn src_disabled(kind: &str) -> SourceConfig {
        SourceConfig {
            name: None,
            connect: None,
            source_type: Some(kind.into()),
            enabled: false,
            params: BTreeMap::new(),
        }
    }

    #[test]
    fn daemon_mode_accepts_external_source() {
        let cfg = make_config(FusionMode::Daemon, vec![src_enabled("kafka")]);
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn daemon_mode_accepts_unknown_external_source() {
        // Any external source type (registered via wp_core_connectors) counts.
        let cfg = make_config(FusionMode::Daemon, vec![src_enabled("postgres")]);
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn daemon_mode_rejects_all_disabled() {
        let cfg = make_config(FusionMode::Daemon, vec![src_disabled("kafka")]);
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("daemon mode requires"));
    }

    #[test]
    fn daemon_mode_requires_at_least_one_source() {
        let cfg = make_config(FusionMode::Daemon, vec![]);
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("at least one source"));
    }

    #[test]
    fn batch_mode_requires_file_source() {
        // Batch mode only works with file sources; external sources don't count.
        let cfg = make_config(FusionMode::Batch, vec![src_enabled("kafka")]);
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("batch mode requires"));
    }

    #[test]
    fn batch_mode_rejects_file_plus_external_source() {
        let mut file = SourceConfig {
            name: None,
            connect: None,
            source_type: Some("file".into()),
            enabled: true,
            params: BTreeMap::new(),
        };
        file.params
            .insert("path".into(), "data/events.ndjson".into());
        file.params.insert("stream_tag".into(), "events".into());
        file.params.insert("format".into(), "ndjson".into());
        let cfg = make_config(FusionMode::Batch, vec![file, src_enabled("kafka")]);
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("batch mode does not allow"));
    }

    #[test]
    fn daemon_mode_ignores_disabled_source_params() {
        let cfg = make_config(
            FusionMode::Daemon,
            vec![
                src_disabled("tcp"),
                src_disabled("file"),
                src_enabled("kafka"),
            ],
        );
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn batch_mode_accepts_enabled_file_plus_disabled_external_source() {
        let mut file = SourceConfig {
            name: None,
            connect: None,
            source_type: Some("file".into()),
            enabled: true,
            params: BTreeMap::new(),
        };
        file.params
            .insert("path".into(), "data/events.ndjson".into());
        file.params.insert("stream_tag".into(), "events".into());
        file.params.insert("data_format".into(), "ndjson".into());

        let cfg = make_config(FusionMode::Batch, vec![file, src_disabled("kafka")]);
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn batch_mode_rejects_all_disabled_file_sources() {
        let cfg = make_config(FusionMode::Batch, vec![src_disabled("file")]);
        let err = validate(&cfg).unwrap_err();
        assert!(
            err.to_string()
                .contains("batch mode requires at least one enabled file source")
        );
    }

    #[test]
    fn external_arrow_ipc_source_requires_stream() {
        let cfg = make_config(
            FusionMode::Daemon,
            vec![SourceConfig {
                name: None,
                connect: None,
                source_type: Some("syslog".into()),
                enabled: true,
                params: {
                    let mut m = BTreeMap::new();
                    m.insert("data_format".into(), "arrow_ipc".into());
                    m
                },
            }],
        );
        let err = validate(&cfg).unwrap_err();
        assert!(err.to_string().contains("external source stream"));
    }

    #[test]
    fn daemon_mode_accepts_ndjson_external_source_without_fixed_stream() {
        let cfg = make_config(
            FusionMode::Daemon,
            vec![SourceConfig {
                name: None,
                connect: None,
                source_type: Some("syslog".into()),
                enabled: true,
                params: {
                    let mut m = BTreeMap::new();
                    m.insert("data_format".into(), "ndjson".into());
                    m.insert("stream_tag_field".into(), "wp_oml_name".into());
                    m
                },
            }],
        );
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn daemon_mode_accepts_arrow_framed_external_source_without_fixed_stream() {
        let cfg = make_config(
            FusionMode::Daemon,
            vec![SourceConfig {
                name: None,
                connect: None,
                source_type: Some("tcp".into()),
                enabled: true,
                params: {
                    let mut m = BTreeMap::new();
                    m.insert("listen".into(), "tcp://127.0.0.1:9800".into());
                    m.insert("data_format".into(), "arrow_framed".into());
                    m
                },
            }],
        );
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn batch_mode_accepts_file_source() {
        let cfg = make_config(
            FusionMode::Batch,
            vec![SourceConfig {
                name: None,
                connect: None,
                source_type: Some("file".into()),
                enabled: true,
                params: {
                    let mut m = BTreeMap::new();
                    m.insert("path".into(), "data/events.ndjson".into());
                    m.insert("stream_tag".into(), "events".into());
                    m.insert("format".into(), "ndjson".into());
                    m
                },
            }],
        );
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn batch_mode_accepts_ndjson_file_source_without_fixed_stream() {
        let cfg = make_config(
            FusionMode::Batch,
            vec![SourceConfig {
                name: None,
                connect: None,
                source_type: Some("file".into()),
                enabled: true,
                params: {
                    let mut m = BTreeMap::new();
                    m.insert("path".into(), "data/events.ndjson".into());
                    m.insert("data_format".into(), "ndjson".into());
                    m
                },
            }],
        );
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn batch_mode_accepts_csv_file_source_without_fixed_stream() {
        let cfg = make_config(
            FusionMode::Batch,
            vec![SourceConfig {
                name: None,
                connect: None,
                source_type: Some("file".into()),
                enabled: true,
                params: {
                    let mut m = BTreeMap::new();
                    m.insert("path".into(), "data/events.csv".into());
                    m.insert("data_format".into(), "csv".into());
                    m.insert("stream_tag_field".into(), "wp_oml_name".into());
                    m
                },
            }],
        );
        assert!(validate(&cfg).is_ok());
    }

    #[test]
    fn batch_mode_rejects_arrow_ipc_file_source_without_fixed_stream() {
        let cfg = make_config(
            FusionMode::Batch,
            vec![SourceConfig {
                name: None,
                connect: None,
                source_type: Some("file".into()),
                enabled: true,
                params: {
                    let mut m = BTreeMap::new();
                    m.insert("path".into(), "data/events.arrow".into());
                    m.insert("data_format".into(), "arrow_ipc".into());
                    m
                },
            }],
        );
        let err = validate(&cfg).unwrap_err();
        assert!(
            err.to_string()
                .contains("file stream_tag must be non-empty")
        );
    }
}
