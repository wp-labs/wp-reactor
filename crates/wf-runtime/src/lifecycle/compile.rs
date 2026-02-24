use std::collections::HashMap;
use std::path::Path;

use orion_error::prelude::*;

use wf_config::resolve_glob;
use wf_core::rule::{CepStateMachine, RuleExecutor};

use crate::error::{RuntimeReason, RuntimeResult};

use super::types::RunRule;

// ---------------------------------------------------------------------------
// Compile-phase helpers — pure data transforms extracted from start()
// ---------------------------------------------------------------------------

/// Load all `.wfs` schema files matching `glob_pattern` under `base_dir`.
pub(super) fn load_schemas(
    glob_pattern: &str,
    base_dir: &Path,
) -> RuntimeResult<Vec<wf_lang::WindowSchema>> {
    let wfs_paths = resolve_glob(glob_pattern, base_dir).owe_conf()?;
    let mut all_schemas = Vec::new();
    for full_path in &wfs_paths {
        let content = std::fs::read_to_string(full_path)
            .owe_sys()
            .position(full_path.display().to_string())?;
        let schemas = wf_lang::parse_wfs(&content)
            .owe(RuntimeReason::Bootstrap)
            .position(full_path.display().to_string())?;
        wf_debug!(conf, file = %full_path.display(), schemas = schemas.len(), "loaded schema file");
        all_schemas.extend(schemas);
    }
    Ok(all_schemas)
}

/// Load, preprocess, parse, and compile all `.wfl` rule files matching
/// `glob_pattern` under `base_dir`, substituting `vars` and validating
/// against the given `schemas`.
pub(super) fn compile_rules(
    glob_pattern: &str,
    base_dir: &Path,
    vars: &std::collections::HashMap<String, String>,
    schemas: &[wf_lang::WindowSchema],
) -> RuntimeResult<Vec<wf_lang::plan::RulePlan>> {
    let wfl_paths = resolve_glob(glob_pattern, base_dir).owe_conf()?;
    let mut all_rule_plans = Vec::new();
    for full_path in &wfl_paths {
        let raw = std::fs::read_to_string(full_path)
            .owe_sys()
            .position(full_path.display().to_string())?;
        let preprocessed = wf_lang::preprocess_vars(&raw, vars)
            .owe_data()
            .position(full_path.display().to_string())?;
        let wfl_file = wf_lang::parse_wfl(&preprocessed)
            .owe(RuntimeReason::Bootstrap)
            .position(full_path.display().to_string())?;
        let plans = wf_lang::compile_wfl(&wfl_file, schemas).owe(RuntimeReason::Bootstrap)?;
        wf_debug!(conf, file = %full_path.display(), rules = plans.len(), "compiled rule file");
        all_rule_plans.extend(plans);
    }
    Ok(all_rule_plans)
}

/// Build [`RunRule`] instances from compiled plans, pre-computing stream
/// alias routing and constructing the CEP state machines.
pub(super) fn build_run_rules(
    plans: &[wf_lang::plan::RulePlan],
    schemas: &[wf_lang::WindowSchema],
) -> Vec<RunRule> {
    let mut rules = Vec::with_capacity(plans.len());
    for plan in plans {
        let stream_aliases = build_stream_aliases(&plan.binds, schemas);
        let time_field = resolve_time_field(&plan.binds, schemas);
        let limits = plan.limits_plan.clone();
        let machine = CepStateMachine::with_limits(
            plan.name.clone(),
            plan.match_plan.clone(),
            time_field,
            limits,
        );
        let executor = RuleExecutor::new(plan.clone());
        rules.push(RunRule {
            machine,
            executor,
            stream_aliases,
        });
    }
    rules
}

/// Resolve the event-time field name for a rule from its first bind's window schema.
pub(super) fn resolve_time_field(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &[wf_lang::WindowSchema],
) -> Option<String> {
    binds.first().and_then(|bind| {
        schemas
            .iter()
            .find(|ws| ws.name == bind.window)
            .and_then(|ws| ws.time_field.clone())
    })
}

/// Build stream_name → alias routing for a rule, given its binds and the
/// window schemas.
fn build_stream_aliases(
    binds: &[wf_lang::plan::BindPlan],
    schemas: &[wf_lang::WindowSchema],
) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for bind in binds {
        if let Some(ws) = schemas.iter().find(|s| s.name == bind.window) {
            for stream_name in &ws.streams {
                map.entry(stream_name.clone())
                    .or_default()
                    .push(bind.alias.clone());
            }
        }
    }
    map
}
