use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::types::HumanDuration;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    /// Rule execution parallelism (Semaphore upper limit).
    pub executor_parallelism: usize,
    /// Single rule execution timeout.
    pub rule_exec_timeout: HumanDuration,
    /// Glob pattern for Window Schema (.wfs) files, relative to config dir.
    pub window_schemas: String,
    /// Glob pattern for WFL rule (.wfl) files, relative to config dir.
    pub wfl_rules: String,
}

/// Expand a glob `pattern` relative to `base_dir` and return matched paths
/// sorted alphabetically. Returns an error if the pattern matches nothing.
pub fn resolve_glob(pattern: &str, base_dir: &Path) -> Result<Vec<PathBuf>> {
    let full_pattern = base_dir.join(pattern);
    let pattern_str = full_pattern.to_string_lossy();

    let mut paths: Vec<PathBuf> = glob::glob(&pattern_str)?
        .filter_map(|entry| entry.ok())
        .collect();

    if paths.is_empty() {
        bail!(
            "glob pattern '{}' (resolved to '{}') matched no files",
            pattern,
            pattern_str,
        );
    }

    paths.sort();
    Ok(paths)
}
