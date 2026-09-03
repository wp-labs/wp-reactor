use std::path::{Path, PathBuf};

use crate::error::{ConfigReason, ConfigResult};
use orion_error::conversion::SourceRawErr;
use serde::{Deserialize, Serialize};

use crate::types::HumanDuration;

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[moju(kind = "struct", domain = "Config", module = "Config.FusionConfig")]
pub struct RuntimeConfig {
    /// **Deprecated, ignored**（decode-route-merge 2026-08-31）：parse worker
    /// 池已移除，route 并入源任务内联执行（design: decode-route-merge-design.md）。
    /// 字段仅为旧 conf 兼容保留（serde alias 语义：读取不报错，引擎不再使用）。
    #[serde(default = "default_parse_parallelism")]
    pub parse_parallelism: usize,
    /// Number of shard workers per shardable rule (P2a). `1` = single worker
    /// (no sharding); `>1` partitions each rule's match key across this many
    /// shard workers. Note: rule tasks still run concurrently across the tokio
    /// runtime regardless of this value — this knob only splits **one rule's**
    /// key space. Defaults to `1` (sharding is an expert opt-in: measured
    /// negative for cheap-rule × large-rule-set workloads, qradar_pk 148k →
    /// 49k EPS at shards=10).
    ///
    /// **无状态 each 规则（无 match key）**：不分 key，整批 round-robin 轮转
    /// 给 N 个 worker（每批整批进一个 shard，批内顺序保持，字节一致）。此时
    /// 分片的实际收益是**输出链（告警构建）并行**——求值本就无状态可整批轮
    /// 转，重成本在 emit 构建（q1 实测：full 档 6.5M → 22.7M EPS，增量
    /// +115.6 → +1.6 ns/事件）。对这种形态，`rule_shards` 是隐式的「输出
    /// 并行」旋钮（「分片」的命名只对 match/stats 的 key 分片直观）。
    #[serde(default = "default_rule_shards", alias = "rule_parallelism")]
    pub rule_shards: usize,
    /// Single rule execution timeout.
    pub rule_exec_timeout: HumanDuration,
    /// Optional engine-side ingest rate cap (events/sec). When set, the source
    /// task token-buckets decoded batches so the engine never ingests faster
    /// than this — bounding the allocation-throughput-driven RSS peak even when
    /// a client sends flat-out (learned from warp-parse DynamicRateLimiter).
    /// `None` = unlimited.
    #[serde(default)]
    pub max_ingest_rate: Option<usize>,
    /// **Deprecated, ignored**（decode-route-merge 2026-08-31）：parse 池与
    /// `PrereadBudget` 随 route 内联一并移除；在途背压由 per-window
    /// `window_buffer_bytes` mailbox 预算承担。字段仅为旧 conf 兼容保留。
    #[serde(default = "default_parse_buffer_bytes")]
    pub parse_buffer_bytes: usize,
    /// Byte budget per window actor channel (subscription model): a batch
    /// holds permits from source-task dispatch (`dispatch_parsed`) until the
    /// window actor appends (or drops) it, so per-window in-flight residency
    /// is bounded in bytes. Since the parse-pool removal
    /// (decode-route-merge-design.md) this is the **only** ingest backpressure
    /// — it propagates from the window actor up to the source task and its
    /// TCP read. Defaults to 64 MiB; values below 4 MiB are clamped up.
    #[serde(default = "default_window_buffer_bytes")]
    pub window_buffer_bytes: usize,
    /// Glob pattern for Window Schema (.wfs) files, relative to config dir.
    pub schemas: String,
    /// Glob pattern for WFL rule (.wfl) files, relative to config dir.
    pub rules: String,
}

fn default_parse_parallelism() -> usize {
    1
}

fn default_parse_buffer_bytes() -> usize {
    128 * 1024 * 1024
}

fn default_window_buffer_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_rule_shards() -> usize {
    1
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
