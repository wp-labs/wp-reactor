use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::runtime::resolve_glob;

/// Load and preprocess a .wfl file with variable substitutions.
/// Variables are resolved in order: `vars` (from `--var`) first, then
/// environment variables. An error is returned only if a variable is
/// found in neither source and has no `${VAR:default}` fallback.
pub fn load_wfl(path: &Path, vars: &HashMap<String, String>) -> Result<String> {
    let source =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let preprocessed = wf_lang::preprocess_vars_with_env(&source, vars)?;
    Ok(preprocessed)
}

/// Load all .wfs schema files matching a glob pattern.
pub fn load_schemas(patterns: &[String], base_dir: &Path) -> Result<Vec<wf_lang::WindowSchema>> {
    let mut schemas = Vec::new();
    for pattern in patterns {
        let paths = resolve_schema_glob(pattern, base_dir)?;
        for path in paths {
            let source = std::fs::read_to_string(&path)
                .with_context(|| format!("reading schema {}", path.display()))?;
            let mut parsed = wf_lang::parse_wfs(&source)
                .map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?;
            schemas.append(&mut parsed);
        }
    }
    Ok(schemas)
}

/// Resolve a glob pattern for schema files. If the pattern contains glob
/// characters, use glob expansion; otherwise treat as a literal path.
fn resolve_schema_glob(pattern: &str, base_dir: &Path) -> Result<Vec<PathBuf>> {
    if pattern.contains('*') || pattern.contains('?') || pattern.contains('[') {
        resolve_glob(pattern, base_dir)
    } else {
        let path = base_dir.join(pattern);
        if path.exists() {
            Ok(vec![path])
        } else {
            anyhow::bail!("schema file not found: {}", path.display());
        }
    }
}

/// Parse `KEY=VALUE` variable assignments from CLI arguments.
pub fn parse_vars(var_args: &[String]) -> Result<HashMap<String, String>> {
    let mut vars = HashMap::new();
    for arg in var_args {
        let (key, value) = arg.split_once('=').ok_or_else(|| {
            anyhow::anyhow!("invalid --var format: expected KEY=VALUE, got '{}'", arg)
        })?;
        vars.insert(key.to_string(), value.to_string());
    }
    Ok(vars)
}
