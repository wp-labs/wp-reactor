use std::path::{Path, PathBuf};

use crate::error::{ConfigReason, ConfigResult};
use orion_error::conversion::SourceRawErr;
use serde::{Deserialize, Serialize};

use crate::types::HumanDuration;

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigLoader")]
pub struct RuntimeConfig {
    /// Parse worker pool parallelism (R2). Defaults to `2`.
    #[serde(default = "default_parse_parallelism")]
    pub parse_parallelism: usize,
    /// Number of shard workers per shardable rule (P2a). `1` = single worker
    /// (no sharding); `>1` partitions each rule's match key across this many
    /// shard workers. Defaults to `6`.
    #[serde(default = "default_rule_parallelism")]
    pub rule_parallelism: usize,
    /// Single rule execution timeout.
    pub rule_exec_timeout: HumanDuration,
    /// Optional engine-side ingest rate cap (events/sec). When set, the source
    /// task token-buckets decoded batches so the engine never ingests faster
    /// than this — bounding the allocation-throughput-driven RSS peak even when
    /// a client sends flat-out (learned from warp-parse DynamicRateLimiter).
    /// `None` = unlimited.
    #[serde(default)]
    pub max_ingest_rate: Option<usize>,
    /// Byte budget for decoded batches in flight across the source → parse →
    /// commit chain. A batch holds permits equal to its arrow memory size from
    /// source push until commit completes, so pipeline residency is bounded in
    /// bytes regardless of frame size (item-count channel caps alone would let
    /// big frames park multiple GiB in the channels). Defaults to 256 MiB;
    /// values below 16 MiB are clamped up.
    #[serde(default = "default_parse_buffer_bytes")]
    pub parse_buffer_bytes: usize,
    /// Glob pattern for Window Schema (.wfs) files, relative to config dir.
    pub schemas: String,
    /// Glob pattern for WFL rule (.wfl) files, relative to config dir.
    pub rules: String,
}

fn default_parse_parallelism() -> usize {
    2
}

fn default_parse_buffer_bytes() -> usize {
    256 * 1024 * 1024
}

fn default_rule_parallelism() -> usize {
    6
}

/// Expand a glob `pattern` relative to `base_dir` and return matched paths
/// sorted alphabetically. Returns an error if the pattern matches nothing.
pub fn resolve_glob(pattern: &str, base_dir: &Path) -> ConfigResult<Vec<PathBuf>> {
    let full_pattern = base_dir.join(pattern);
    let pattern_str = full_pattern.to_string_lossy();

    let mut paths: Vec<PathBuf> = glob::glob(&pattern_str)
        .source_raw_err(
            ConfigReason::Path,
            format!("read glob pattern {pattern_str:?}"),
        )?
        .filter_map(|entry| entry.ok())
        .collect();

    if paths.is_empty() {
        return ConfigReason::Path.fail(format!(
            "glob pattern '{}' (resolved to '{}') matched no files",
            pattern, pattern_str,
        ));
    }

    paths.sort();
    Ok(paths)
}
