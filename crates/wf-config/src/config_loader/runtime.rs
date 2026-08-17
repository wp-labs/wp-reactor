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
    /// commit chain. A batch holds permits equal to its **content** byte size
    /// (the actual data bytes — ≈ wire size, Arrow buffer padding excluded),
    /// from source push until commit completes, so pipeline residency is
    /// bounded in bytes regardless of frame size (item-count channel caps
    /// alone would let big frames park multiple GiB in the channels). Charging
    /// content rather than `get_array_memory_size` matches the window mailbox
    /// accounting and keeps ordinary frames at many slots (decoded-size
    /// accounting structurally over-counts IPC-decoded batches ~10× — see
    /// concurrency-scaling.md §2.3 wall ①). Defaults to 128 MiB (≈ 18 slots
    /// for 8 MiB frames): measured q1 100M 6.13M / RSS 5.9GB vs pre-P0-②
    /// default 5.93M / 4.4GB — a small throughput gain at a modest RSS
    /// step-up, short of the 12-14GB plateau that 256 MiB (36 slots) hits
    /// (concurrency-scaling.md §3.1 默认值决策). Values below 16 MiB are
    /// clamped up. NB the budget bounds *content* bytes in flight: the
    /// decoded in-flight footprint is the budget × the IPC decode inflation
    /// (~10× measured), so RSS under a downstream stall can approach ~10×
    /// this value. Raise for throughput (512 MiB ≈ 7.0M, 1–2 GiB ≈ 7.5M+;
    /// 4 GiB overshoots — see preread-budget-design.md §6).
    #[serde(default = "default_parse_buffer_bytes")]
    pub parse_buffer_bytes: usize,
    /// Byte budget per window actor channel (subscription model): a batch
    /// holds permits from parse-worker dispatch until the window actor
    /// appends (or drops) it, so per-window in-flight residency is bounded
    /// in bytes — the explicit backpressure that replaces the removed window
    /// write lock's implicit serialization. Defaults to 64 MiB; values below
    /// 4 MiB are clamped up.
    #[serde(default = "default_window_buffer_bytes")]
    pub window_buffer_bytes: usize,
    /// Glob pattern for Window Schema (.wfs) files, relative to config dir.
    pub schemas: String,
    /// Glob pattern for WFL rule (.wfl) files, relative to config dir.
    pub rules: String,
}

fn default_parse_parallelism() -> usize {
    2
}

fn default_parse_buffer_bytes() -> usize {
    128 * 1024 * 1024
}

fn default_window_buffer_bytes() -> usize {
    64 * 1024 * 1024
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
