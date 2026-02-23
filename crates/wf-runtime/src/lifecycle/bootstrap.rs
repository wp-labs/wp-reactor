use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use orion_error::prelude::*;

use wf_config::FusionConfig;
use wf_core::window::{Router, WindowRegistry};

use crate::error::{RuntimeReason, RuntimeResult};
use crate::schema_bridge::schemas_to_window_defs;

use super::compile::{build_alert_sink, build_run_rules, compile_rules, load_schemas};
use super::types::BootstrapData;

// ---------------------------------------------------------------------------
// Phase 1: load_and_compile — pure data transforms
// ---------------------------------------------------------------------------

/// Load schemas, compile rules, validate config, build engines and alert sink.
pub(super) fn load_and_compile(
    config: &FusionConfig,
    base_dir: &Path,
) -> RuntimeResult<BootstrapData> {
    // 1. Load .wfs files → Vec<WindowSchema>
    let all_schemas = load_schemas(&config.runtime.schemas, base_dir)?;

    // 2. Preprocess .wfl with config.vars → parse → compile → Vec<RulePlan>
    let all_rule_plans =
        compile_rules(&config.runtime.rules, base_dir, &config.vars, &all_schemas)?;

    // 3. Cross-validate over vs over_cap
    let window_overs: HashMap<String, Duration> = all_schemas
        .iter()
        .map(|ws| (ws.name.clone(), ws.over))
        .collect();
    wf_config::validate_over_vs_over_cap(&config.windows, &window_overs).owe_conf()?;
    wf_debug!(
        conf,
        windows = config.windows.len(),
        "over vs over_cap validation passed"
    );

    // 4. Schema bridge: WindowSchema × WindowConfig → Vec<WindowDef>
    let window_defs =
        schemas_to_window_defs(&all_schemas, &config.windows).owe(RuntimeReason::Bootstrap)?;

    // 5. WindowRegistry::build → registry
    let registry = WindowRegistry::build(window_defs).err_conv()?;

    // 6. Router::new(registry)
    let router = Arc::new(Router::new(registry));

    // 7. Build RunRules (precompute stream_name → alias routing)
    let rules = build_run_rules(&all_rule_plans, &all_schemas);

    // 8. Build alert sink
    let alert_sink = build_alert_sink(config, base_dir)?;

    let schema_count = all_schemas.len();
    Ok(BootstrapData {
        rules,
        router,
        alert_sink,
        schema_count,
        schemas: all_schemas,
    })
}
