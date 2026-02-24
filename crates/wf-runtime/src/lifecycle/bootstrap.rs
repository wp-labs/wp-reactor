use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use orion_error::prelude::*;

use wf_config::FusionConfig;
use wf_core::window::{Router, WindowRegistry};

use crate::error::{RuntimeReason, RuntimeResult};
use crate::schema_bridge::schemas_to_window_defs;
use crate::sink_build::{SinkFactoryRegistry, build_sink_dispatcher};
use crate::sink_factory::file::FileSinkFactory;

use super::compile::{build_run_rules, compile_rules, load_schemas};
use super::types::BootstrapData;

// ---------------------------------------------------------------------------
// Phase 1: load_and_compile — pure data transforms + async sink build
// ---------------------------------------------------------------------------

/// Load schemas, compile rules, validate config, build engines and sink dispatcher.
pub(super) async fn load_and_compile(
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

    // 8. Build connector-based sink dispatcher
    let sinks_dir = base_dir.join(&config.sinks);
    let bundle = wf_config::sink::load_sink_config(&sinks_dir).owe_conf()?;
    let mut factory_registry = SinkFactoryRegistry::new();
    factory_registry.register(Arc::new(FileSinkFactory));
    let work_root = config
        .work_root
        .as_ref()
        .map(|p| base_dir.join(p))
        .unwrap_or_else(|| base_dir.to_path_buf());
    let window_names: Vec<String> = config.windows.iter().map(|w| w.name.clone()).collect();
    let dispatcher = Arc::new(
        build_sink_dispatcher(&bundle, &factory_registry, &work_root, &window_names)
            .await
            .owe(RuntimeReason::Bootstrap)?,
    );

    let schema_count = all_schemas.len();
    Ok(BootstrapData {
        rules,
        router,
        dispatcher,
        schema_count,
        schemas: all_schemas,
    })
}
