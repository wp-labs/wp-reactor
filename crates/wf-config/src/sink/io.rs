use std::collections::BTreeMap;
use std::path::Path;

use wp_connector_api::ConnectorDef;

use super::build::{build_fixed_group, build_flex_group};
use super::connector::load_connector_defs;
use super::defaults::{DefaultsBody, load_defaults};
use super::group::{FixedGroup, FlexGroup};
use super::route::RouteFile;

// ---------------------------------------------------------------------------
// SinkConfigBundle — aggregated result of loading all sink config files
// ---------------------------------------------------------------------------

/// The complete sink configuration loaded from a `sinks/` directory.
#[derive(Debug)]
pub struct SinkConfigBundle {
    /// Connector definitions loaded from `sink.d/`.
    pub connectors: BTreeMap<String, ConnectorDef>,
    /// Global defaults from `defaults.toml`.
    pub defaults: DefaultsBody,
    /// Business routing groups loaded from `business.d/`.
    pub business: Vec<FlexGroup>,
    /// Infrastructure default group from `infra.d/default.toml`.
    pub infra_default: Option<FixedGroup>,
    /// Infrastructure error group from `infra.d/error.toml`.
    pub infra_error: Option<FixedGroup>,
}

// ---------------------------------------------------------------------------
// Directory loading
// ---------------------------------------------------------------------------

/// Load the complete sink configuration from a `sinks/` root directory.
///
/// Expected directory layout:
/// ```text
/// sinks/
/// ├── sink.d/               # connector definitions (*.toml)
/// ├── business.d/           # business routing groups (*.toml)
/// ├── infra.d/              # infrastructure groups (*.toml)
/// │   ├── default.toml
/// │   └── error.toml
/// └── defaults.toml         # global defaults
/// ```
pub fn load_sink_config(sink_root: &Path) -> anyhow::Result<SinkConfigBundle> {
    if !sink_root.is_dir() {
        anyhow::bail!(
            "sink config directory does not exist: {}",
            sink_root.display()
        );
    }

    // 1. Load connectors from sink.d/
    let connectors = load_connector_defs(&sink_root.join("sink.d"))?;

    // 2. Load defaults
    let defaults = load_defaults(sink_root)?;

    // 3. Load business groups from business.d/
    let business = load_business_groups(&sink_root.join("business.d"), &connectors, &defaults)?;

    // 4. Load infra groups from infra.d/
    let infra_default = load_infra_group(
        &sink_root.join("infra.d").join("default.toml"),
        &connectors,
        &defaults,
    )?;
    let infra_error = load_infra_group(
        &sink_root.join("infra.d").join("error.toml"),
        &connectors,
        &defaults,
    )?;

    Ok(SinkConfigBundle {
        connectors,
        defaults,
        business,
        infra_default,
        infra_error,
    })
}

/// Load all business routing groups from `*.toml` files in a directory.
fn load_business_groups(
    dir: &Path,
    connectors: &BTreeMap<String, ConnectorDef>,
    defaults: &DefaultsBody,
) -> anyhow::Result<Vec<FlexGroup>> {
    let mut groups = Vec::new();

    if !dir.is_dir() {
        return Ok(groups);
    }

    let pattern = dir.join("*.toml");
    let pattern_str = pattern.to_string_lossy();

    let mut entries: Vec<_> = glob::glob(&pattern_str)?
        .filter_map(|e| e.ok())
        .collect();
    entries.sort();

    for path in entries {
        let content = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", path.display()))?;
        let file: RouteFile = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse {}: {e}", path.display()))?;

        let group = build_flex_group(&file.sink_group, connectors, defaults)
            .map_err(|e| anyhow::anyhow!("error in {}: {e}", path.display()))?;
        groups.push(group);
    }

    Ok(groups)
}

/// Load a single infra group from a TOML file (returns None if file doesn't exist).
fn load_infra_group(
    path: &Path,
    connectors: &BTreeMap<String, ConnectorDef>,
    defaults: &DefaultsBody,
) -> anyhow::Result<Option<FixedGroup>> {
    if !path.exists() {
        return Ok(None);
    }

    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", path.display()))?;
    let file: RouteFile = toml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("failed to parse {}: {e}", path.display()))?;

    let group = build_fixed_group(&file.sink_group, connectors, defaults)
        .map_err(|e| anyhow::anyhow!("error in {}: {e}", path.display()))?;
    Ok(Some(group))
}
