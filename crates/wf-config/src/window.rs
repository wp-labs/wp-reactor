use serde::{Deserialize, Serialize};

use crate::error::{ConfigReason, ConfigResult};
use crate::types::{ByteSize, DistMode, EvictPolicy, HumanDuration, LatePolicy};

// ---------------------------------------------------------------------------
// WindowDefaults — deserialized from [window_defaults]
// ---------------------------------------------------------------------------

#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[jumo(kind = "struct", domain = "Config", module = "Config.WindowConfig")]
pub struct WindowDefaults {
    pub evict_interval: HumanDuration,
    pub max_window_bytes: ByteSize,
    pub max_total_bytes: ByteSize,
    pub evict_policy: EvictPolicy,
    pub watermark: HumanDuration,
    pub allowed_lateness: HumanDuration,
    pub late_policy: LatePolicy,
}

// ---------------------------------------------------------------------------
// WindowOverride — deserialized from [window.<name>]
// ---------------------------------------------------------------------------

#[derive(::jumo_derive::Jumo, Debug, Clone, Deserialize)]
#[jumo(kind = "struct", domain = "Config", module = "Config.WindowConfig")]
pub struct WindowOverride {
    pub mode: String,
    pub partition_key: Option<String>,
    pub max_window_bytes: Option<ByteSize>,
    pub over_cap: HumanDuration,
    pub evict_policy: Option<EvictPolicy>,
    pub watermark: Option<HumanDuration>,
    pub allowed_lateness: Option<HumanDuration>,
    pub late_policy: Option<LatePolicy>,
    /// knowdb table name for provider windows (None = buffer window).
    #[serde(default)]
    pub table: Option<String>,
}

// ---------------------------------------------------------------------------
// WindowConfig — fully resolved window configuration
// ---------------------------------------------------------------------------

#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq, Eq)]
#[jumo(kind = "struct", domain = "Config", module = "Config.WindowConfig")]
pub struct WindowConfig {
    pub name: String,
    pub mode: DistMode,
    pub max_window_bytes: ByteSize,
    pub over_cap: HumanDuration,
    pub evict_policy: EvictPolicy,
    pub watermark: HumanDuration,
    pub allowed_lateness: HumanDuration,
    pub late_policy: LatePolicy,
    /// knowdb table for provider windows.
    pub table: Option<String>,
}

impl WindowOverride {
    /// Resolve this override against `defaults`, producing a fully populated [`WindowConfig`].
    pub fn resolve(self, name: String, defaults: &WindowDefaults) -> ConfigResult<WindowConfig> {
        let mode = resolve_mode(&self.mode, self.partition_key)?;

        Ok(WindowConfig {
            name,
            mode,
            max_window_bytes: self.max_window_bytes.unwrap_or(defaults.max_window_bytes),
            over_cap: self.over_cap,
            evict_policy: self.evict_policy.unwrap_or(defaults.evict_policy),
            watermark: self.watermark.unwrap_or(defaults.watermark),
            allowed_lateness: self.allowed_lateness.unwrap_or(defaults.allowed_lateness),
            late_policy: self.late_policy.unwrap_or(defaults.late_policy),
            table: self.table,
        })
    }
}

fn resolve_mode(mode: &str, partition_key: Option<String>) -> ConfigResult<DistMode> {
    match mode {
        "local" => Ok(DistMode::Local),
        "replicated" => Ok(DistMode::Replicated),
        "partitioned" => {
            let Some(key) = partition_key else {
                return ConfigReason::Validation
                    .fail("mode \"partitioned\" requires a partition_key");
            };
            Ok(DistMode::Partitioned { key })
        }
        other => ConfigReason::Validation.fail(format!("unknown window mode: {other:?}")),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_defaults() -> WindowDefaults {
        WindowDefaults {
            evict_interval: "30s".parse().unwrap(),
            max_window_bytes: "256MB".parse().unwrap(),
            max_total_bytes: "2GB".parse().unwrap(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: "5s".parse().unwrap(),
            allowed_lateness: "0s".parse().unwrap(),
            late_policy: LatePolicy::Drop,
        }
    }

    #[test]
    fn resolve_mode_local() {
        assert_eq!(resolve_mode("local", None).unwrap(), DistMode::Local);
    }

    #[test]
    fn resolve_mode_replicated() {
        assert_eq!(
            resolve_mode("replicated", None).unwrap(),
            DistMode::Replicated,
        );
    }

    #[test]
    fn resolve_mode_partitioned() {
        assert_eq!(
            resolve_mode("partitioned", Some("sip".into())).unwrap(),
            DistMode::Partitioned { key: "sip".into() },
        );
    }

    #[test]
    fn resolve_mode_partitioned_no_key() {
        assert!(resolve_mode("partitioned", None).is_err());
    }

    #[test]
    fn resolve_mode_unknown() {
        assert!(resolve_mode("distributed", None).is_err());
    }

    #[test]
    fn override_inherits_defaults() {
        let ovr = WindowOverride {
            mode: "local".into(),
            partition_key: None,
            max_window_bytes: None,
            over_cap: "30m".parse().unwrap(),
            evict_policy: None,
            watermark: None,
            allowed_lateness: None,
            late_policy: None,
            table: None,
        };
        let defaults = sample_defaults();
        let wc = ovr.resolve("test".into(), &defaults).unwrap();

        assert_eq!(wc.max_window_bytes, defaults.max_window_bytes);
        assert_eq!(wc.evict_policy, defaults.evict_policy);
        assert_eq!(wc.watermark, defaults.watermark);
        assert_eq!(wc.allowed_lateness, defaults.allowed_lateness);
        assert_eq!(wc.late_policy, defaults.late_policy);
    }

    #[test]
    fn override_replaces_defaults() {
        let ovr = WindowOverride {
            mode: "local".into(),
            partition_key: None,
            max_window_bytes: Some("64MB".parse().unwrap()),
            over_cap: "30m".parse().unwrap(),
            evict_policy: Some(EvictPolicy::Lru),
            watermark: Some("10s".parse().unwrap()),
            allowed_lateness: Some("30s".parse().unwrap()),
            late_policy: Some(LatePolicy::Revise),
            table: None,
        };
        let defaults = sample_defaults();
        let wc = ovr.resolve("test".into(), &defaults).unwrap();

        assert_eq!(wc.max_window_bytes, "64MB".parse::<ByteSize>().unwrap());
        assert_eq!(wc.evict_policy, EvictPolicy::Lru);
        assert_eq!(wc.watermark, "10s".parse::<HumanDuration>().unwrap());
        assert_eq!(wc.allowed_lateness, "30s".parse::<HumanDuration>().unwrap());
        assert_eq!(wc.late_policy, LatePolicy::Revise);
    }
}
