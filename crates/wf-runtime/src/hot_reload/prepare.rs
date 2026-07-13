use std::collections::HashSet;
use std::path::Path;

use wf_config::{FusionConfig, RawFusionConfigTree, WindowConfig};
use wf_lang::WindowSchema;

use crate::error::RuntimeResult;

use super::compile::{
    CompiledReloadArtifacts, append_effective_config_blockers, compile_reload_artifacts,
};
use super::topology::append_topology_blockers;
use super::{PreparedRuleReload, ReloadPreparation};

pub fn prepare_reload(
    current_raw: &RawFusionConfigTree,
    current_config: &FusionConfig,
    next_raw: RawFusionConfigTree,
    next_config: FusionConfig,
    base_dir: &Path,
) -> RuntimeResult<ReloadPreparation> {
    let mut plan = current_raw.build_reload_plan(&next_raw);
    if plan.has_blockers() {
        return Ok(ReloadPreparation::Blocked(plan));
    }

    append_effective_config_blockers(
        &mut plan,
        current_raw,
        current_config,
        &next_raw,
        &next_config,
    );
    if plan.has_blockers() {
        return Ok(ReloadPreparation::Blocked(plan));
    }

    let current_artifacts = compile_reload_artifacts(current_config, base_dir)?;
    let next_artifacts = compile_reload_artifacts(&next_config, base_dir)?;

    let (added_schemas, added_configs, modified_schemas, modified_configs) =
        append_topology_blockers(&mut plan, &current_artifacts, &next_artifacts);
    if plan.has_blockers() {
        return Ok(ReloadPreparation::Blocked(plan));
    }

    Ok(ReloadPreparation::Ready(Box::new(PreparedRuleReload {
        plan,
        next_raw,
        next_config,
        next_rules: next_artifacts.run_rules,
        next_intermediate_targets: next_artifacts.intermediate_targets,
        next_schemas: next_artifacts.runtime_schemas,
        // L2 incremental reload: carry the purely-added schemas/configs so
        // apply_reload can insert the new windows into the running registry.
        added_schemas,
        added_window_configs: added_configs,
        modified_schemas,
        modified_window_configs: modified_configs,
        next_window_configs: next_artifacts.runtime_window_configs,
    })))
}

/// Like [`prepare_reload`] but uses **cached** current schemas / window
/// configs from boot time rather than re-compiling them from the (possibly
/// changed) on-disk config. Required for L3 (schema/config modification
/// detected via in-place file edits): without the cache,
/// `compile_reload_artifacts` would compile both sides from the same
/// (modified) disk state and the topology diff would see no change.
pub fn prepare_reload_with_cached(
    current_raw: &RawFusionConfigTree,
    current_config: &FusionConfig,
    current_runtime_schemas: &[WindowSchema],
    current_runtime_window_configs: &[WindowConfig],
    next_raw: RawFusionConfigTree,
    next_config: FusionConfig,
    base_dir: &Path,
) -> RuntimeResult<ReloadPreparation> {
    let mut plan = current_raw.build_reload_plan(&next_raw);
    if plan.has_blockers() {
        return Ok(ReloadPreparation::Blocked(plan));
    }
    append_effective_config_blockers(
        &mut plan,
        current_raw,
        current_config,
        &next_raw,
        &next_config,
    );
    if plan.has_blockers() {
        return Ok(ReloadPreparation::Blocked(plan));
    }
    // Only compile next from disk; current uses cached boot-time values.
    let next_artifacts = compile_reload_artifacts(&next_config, base_dir)?;
    let current_artifacts = CompiledReloadArtifacts {
        run_rules: Vec::new(),
        intermediate_targets: HashSet::new(),
        runtime_schemas: current_runtime_schemas.to_vec(),
        runtime_window_configs: current_runtime_window_configs.to_vec(),
    };
    let (added_schemas, added_configs, modified_schemas, modified_configs) =
        append_topology_blockers(&mut plan, &current_artifacts, &next_artifacts);
    if plan.has_blockers() {
        return Ok(ReloadPreparation::Blocked(plan));
    }
    Ok(ReloadPreparation::Ready(Box::new(PreparedRuleReload {
        plan,
        next_raw,
        next_config,
        next_rules: next_artifacts.run_rules,
        next_intermediate_targets: next_artifacts.intermediate_targets,
        next_schemas: next_artifacts.runtime_schemas,
        added_schemas,
        added_window_configs: added_configs,
        modified_schemas,
        modified_window_configs: modified_configs,
        next_window_configs: next_artifacts.runtime_window_configs,
    })))
}
