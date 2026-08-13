use std::collections::HashSet;
use std::path::Path;

use orion_error::prelude::*;

use wf_config::{
    ClassifiedFusionConfigChange, FusionChangeKind, FusionConfig, FusionReloadDisposition,
    FusionReloadPlan, RawFusionConfigTree, WindowConfig, validate_over_vs_over_cap,
};
use wf_lang::WindowSchema;

use crate::error::{RuntimeReason, RuntimeResult};

use crate::lifecycle::compile::{
    build_pipeline_internal_windows, build_run_rules, build_runtime_var_context,
    collect_intermediate_targets, compile_rules, load_schemas,
};
use crate::lifecycle::types::RunRule;

pub(super) struct CompiledReloadArtifacts {
    pub(super) run_rules: Vec<RunRule>,
    pub(super) intermediate_targets: HashSet<String>,
    pub(super) runtime_schemas: Vec<WindowSchema>,
    pub(super) runtime_window_configs: Vec<WindowConfig>,
}

pub(super) fn compile_reload_artifacts(
    config: &FusionConfig,
    base_dir: &Path,
) -> RuntimeResult<CompiledReloadArtifacts> {
    let all_schemas = load_schemas(&config.runtime.schemas, base_dir)?;
    let var_ctx = build_runtime_var_context(config, base_dir);
    let (all_rule_plans, effective_schemas) =
        compile_rules(&config.runtime.rules, base_dir, &var_ctx, &all_schemas)?;
    let intermediate_targets = collect_intermediate_targets(&all_rule_plans);
    let (pipeline_schemas, pipeline_window_configs) = build_pipeline_internal_windows(
        &all_rule_plans,
        &effective_schemas,
        &config.window_defaults,
    );

    let mut runtime_schemas = effective_schemas;
    runtime_schemas.extend(pipeline_schemas);

    let mut runtime_window_configs = config.windows.clone();
    runtime_window_configs.extend(pipeline_window_configs);

    let window_overs = runtime_schemas
        .iter()
        .map(|schema| (schema.name.clone(), schema.over))
        .collect();
    validate_over_vs_over_cap(&runtime_window_configs, &window_overs).source_err(
        RuntimeReason::core_conf(),
        "validate window over vs over_cap",
    )?;

    let run_rules = build_run_rules(&all_rule_plans, &runtime_schemas, &config.output);
    Ok(CompiledReloadArtifacts {
        run_rules,
        intermediate_targets,
        runtime_schemas,
        runtime_window_configs,
    })
}

pub(super) fn append_effective_config_blockers(
    plan: &mut FusionReloadPlan,
    current_raw: &RawFusionConfigTree,
    current_config: &FusionConfig,
    next_raw: &RawFusionConfigTree,
    next_config: &FusionConfig,
) {
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "mode",
        wf_config::FusionChangeKind::Mode,
        "effective mode changed after variable expansion; lifecycle semantics require restart",
        current_config.mode != next_config.mode,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "sinks",
        wf_config::FusionChangeKind::Sinks,
        "effective sink root changed after variable expansion; sink topology must be rebuilt",
        current_config.sinks != next_config.sinks,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "work_root",
        wf_config::FusionChangeKind::Sinks,
        "effective work_root changed after variable expansion; sink runtime paths require restart",
        current_config.work_root != next_config.work_root,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "runtime.parse_parallelism",
        wf_config::FusionChangeKind::Runtime,
        "effective runtime.parse_parallelism changed after variable expansion; task layout requires restart",
        current_config.runtime.parse_parallelism != next_config.runtime.parse_parallelism,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "runtime.rule_parallelism",
        wf_config::FusionChangeKind::Runtime,
        "effective runtime.rule_parallelism changed after variable expansion; task layout requires restart",
        current_config.runtime.rule_parallelism != next_config.runtime.rule_parallelism,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "runtime.rule_exec_timeout",
        wf_config::FusionChangeKind::Runtime,
        "effective runtime.rule_exec_timeout changed after variable expansion; rule task behavior requires restart",
        current_config.runtime.rule_exec_timeout != next_config.runtime.rule_exec_timeout,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "runtime.schemas",
        wf_config::FusionChangeKind::Runtime,
        "effective runtime.schemas changed after variable expansion; schema catalog must be rebuilt",
        current_config.runtime.schemas != next_config.runtime.schemas,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "sources",
        wf_config::FusionChangeKind::Sources,
        "effective sources changed after variable expansion; receiver tasks require restart",
        current_config.sources != next_config.sources,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "window_defaults",
        wf_config::FusionChangeKind::Windows,
        "effective window_defaults changed after variable expansion; window lifecycle requires restart",
        current_config.window_defaults != next_config.window_defaults,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "window",
        wf_config::FusionChangeKind::Windows,
        "effective window config changed after variable expansion; window registry requires restart",
        current_config.windows != next_config.windows,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "logging",
        wf_config::FusionChangeKind::Logging,
        "effective logging config changed after variable expansion; logging pipeline is not hot-reloadable",
        current_config.logging != next_config.logging,
    );
    push_effective_blocker_if_changed(
        plan,
        current_raw,
        next_raw,
        "metrics",
        wf_config::FusionChangeKind::Metrics,
        "effective metrics config changed after variable expansion; metrics tasks require restart",
        current_config.metrics != next_config.metrics,
    );
}

fn push_effective_blocker_if_changed(
    plan: &mut FusionReloadPlan,
    current_raw: &RawFusionConfigTree,
    next_raw: &RawFusionConfigTree,
    path: &'static str,
    kind: FusionChangeKind,
    reason: &'static str,
    changed: bool,
) {
    if !changed {
        return;
    }
    plan.requires_restart.push(ClassifiedFusionConfigChange {
        change: wf_config::RawFusionConfigChange {
            path: path.to_string(),
            old_value: None,
            new_value: None,
            old_origin: current_raw.origin_for(path).map(Path::to_path_buf),
            new_origin: next_raw.origin_for(path).map(Path::to_path_buf),
        },
        kind,
        disposition: FusionReloadDisposition::RequiresRestart,
        reason,
    });
}
