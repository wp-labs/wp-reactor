use std::collections::HashMap;

use wf_config::{
    ClassifiedFusionConfigChange, FusionChangeKind, FusionReloadDisposition,
    FusionReloadPlan, RawFusionConfigChange, WindowConfig,
};
use wf_lang::WindowSchema;

use super::compile::CompiledReloadArtifacts;

pub(super) fn append_topology_blockers(
    plan: &mut FusionReloadPlan,
    current: &CompiledReloadArtifacts,
    next: &CompiledReloadArtifacts,
) -> (
    Vec<WindowSchema>,
    Vec<WindowConfig>,
    Vec<WindowSchema>,
    Vec<WindowConfig>,
) {
    // Build a lookup by name for the current artifacts.
    let current_schemas: HashMap<&str, &WindowSchema> = current
        .runtime_schemas
        .iter()
        .map(|s| (s.name.as_str(), s))
        .collect();
    let current_configs: HashMap<&str, &WindowConfig> = current
        .runtime_window_configs
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let next_configs: HashMap<&str, &WindowConfig> = next
        .runtime_window_configs
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    let mut added_schemas: Vec<WindowSchema> = Vec::new();
    let mut added_configs: Vec<WindowConfig> = Vec::new();
    let mut modified_schemas: Vec<WindowSchema> = Vec::new();
    let mut modified_configs: Vec<WindowConfig> = Vec::new();
    let mut has_blocker = false;

    // --- schemas: classify per-window ----------------------------------
    for ns in &next.runtime_schemas {
        match current_schemas.get(ns.name.as_str()) {
            // L2: pure addition — new window that did not exist before.
            None => added_schemas.push(ns.clone()),
            // L3: modification — same name, different definition.
            Some(&cs) if cs != ns => modified_schemas.push(ns.clone()),
            Some(_) => { /* unchanged */ }
        }
    }
    for cs in current_schemas.keys() {
        if !next
            .runtime_schemas
            .iter()
            .any(|ns| ns.name.as_str() == *cs)
        {
            // Schema was removed — still requires restart.
            has_blocker = true;
        }
    }
    if has_blocker {
        plan.requires_restart.push(synthetic_restart_change(
            "__derived.runtime_schemas",
            wf_config::FusionChangeKind::Runtime,
            "compiled runtime schema set changed (removed); full restart required",
        ));
        return (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    }

    // --- window configs: classify per-window ---------------------------
    for (name, nc) in &next_configs {
        match current_configs.get(name) {
            None if added_schemas.iter().any(|s| s.name == *name) => {
                added_configs.push((*nc).clone());
            }
            None if modified_schemas.iter().any(|s| s.name == *name) => {
                modified_configs.push((*nc).clone());
            }
            None => {
                has_blocker = true;
            }
            Some(&cc) if cc != *nc => {
                modified_configs.push((*nc).clone());
            }
            Some(_) => { /* unchanged */ }
        }
    }
    for cn in current_configs.keys() {
        if !next_configs.contains_key(*cn) {
            has_blocker = true;
        }
    }
    if has_blocker {
        plan.requires_restart.push(synthetic_restart_change(
            "__derived.runtime_window_configs",
            wf_config::FusionChangeKind::Windows,
            "compiled runtime window configs changed (removed); full restart required",
        ));
        return (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    }

    // --- post-processing: pair schemas and configs --------------------
    // Schema and config changes are classified independently above
    // (e.g. editing .wfs in-place changes the schema but not the config),
    // but `apply_reload` zips them. Ensure every modified/added schema
    // has a matching config and vice versa, so the zip iterator covers
    // every window exactly once.
    for ms in &modified_schemas {
        if !modified_configs.iter().any(|c| c.name == ms.name)
            && let Some(&nc) = next_configs.get(ms.name.as_str())
        {
            modified_configs.push(nc.clone());
        }
    }
    for mc in &modified_configs {
        if !modified_schemas.iter().any(|s| s.name == mc.name)
            && let Some(ns) = next.runtime_schemas.iter().find(|s| s.name == mc.name)
        {
            modified_schemas.push(ns.clone());
        }
    }
    for as_ in &added_schemas {
        if !added_configs.iter().any(|c| c.name == as_.name)
            && let Some(&nc) = next_configs.get(as_.name.as_str())
        {
            added_configs.push(nc.clone());
        }
    }

    (
        added_schemas,
        added_configs,
        modified_schemas,
        modified_configs,
    )
}

fn synthetic_restart_change(
    path: &'static str,
    kind: FusionChangeKind,
    reason: &'static str,
) -> ClassifiedFusionConfigChange {
    ClassifiedFusionConfigChange {
        change: RawFusionConfigChange {
            path: path.to_string(),
            old_value: None,
            new_value: None,
            old_origin: None,
            new_origin: None,
        },
        kind,
        disposition: FusionReloadDisposition::RequiresRestart,
        reason,
    }
}
