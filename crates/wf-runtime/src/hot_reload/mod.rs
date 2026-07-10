use std::collections::HashSet;
use std::path::Path;

use orion_error::prelude::*;

use wf_config::{
    ClassifiedFusionConfigChange, FusionChangeKind, FusionConfig, FusionReloadDisposition,
    FusionReloadPlan, RawFusionConfigChange, RawFusionConfigTree, WindowConfig,
    validate_over_vs_over_cap,
};
use wf_lang::WindowSchema;

use crate::error::{RuntimeReason, RuntimeResult};

use crate::lifecycle::compile::{
    build_pipeline_internal_windows, build_run_rules, build_runtime_var_context,
    collect_intermediate_targets, compile_rules, load_schemas,
};
use crate::lifecycle::types::RunRule;

#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Orchestra", module = "Orchestra.HotReload")]
pub struct PreparedRuleReload {
    pub plan: FusionReloadPlan,
    pub next_raw: RawFusionConfigTree,
    pub next_config: FusionConfig,
    pub(super) next_rules: Vec<RunRule>,
    pub next_intermediate_targets: HashSet<String>,
    pub next_schemas: Vec<WindowSchema>,
    /// TODO: wire these fields into [`crate::lifecycle::Reactor::apply_reload`]
    /// for L2/L3 incremental window management.
    #[allow(dead_code)]
    pub(crate) added_schemas: Vec<WindowSchema>,
    #[allow(dead_code)]
    pub(crate) added_window_configs: Vec<WindowConfig>,
    /// Schemas that changed definition (same name, different fields/over/…).
    /// L3 partial rebuild: `apply_reload` calls `try_replace_window` for each
    /// so the old window is replaced atomically with a new (empty) one.
    #[allow(dead_code)]
    pub(crate) modified_schemas: Vec<WindowSchema>,
    #[allow(dead_code)]
    pub(crate) modified_window_configs: Vec<WindowConfig>,
    /// Complete runtime window configs for the next generation (from config
    /// plus pipeline internal windows). Cached so `apply_reload` can advance
    /// the boot-time cache after a successful reload.
    #[allow(dead_code)]
    pub(crate) next_window_configs: Vec<WindowConfig>,
}

#[derive(::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Orchestra", module = "Orchestra.HotReload")]
pub enum ReloadPreparation {
    Ready(Box<PreparedRuleReload>),
    Blocked(FusionReloadPlan),
}

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

pub(crate) struct CompiledReloadArtifacts {
    run_rules: Vec<RunRule>,
    intermediate_targets: HashSet<String>,
    runtime_schemas: Vec<WindowSchema>,
    runtime_window_configs: Vec<WindowConfig>,
}

pub(crate) fn compile_reload_artifacts(
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

    let run_rules = build_run_rules(&all_rule_plans, &runtime_schemas);
    Ok(CompiledReloadArtifacts {
        run_rules,
        intermediate_targets,
        runtime_schemas,
        runtime_window_configs,
    })
}

fn append_effective_config_blockers(
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
        "runtime.executor_parallelism",
        wf_config::FusionChangeKind::Runtime,
        "effective runtime.executor_parallelism changed after variable expansion; task layout requires restart",
        current_config.runtime.executor_parallelism != next_config.runtime.executor_parallelism,
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
        change: RawFusionConfigChange {
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

fn append_topology_blockers(
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
    let current_schemas: std::collections::HashMap<&str, &WindowSchema> = current
        .runtime_schemas
        .iter()
        .map(|s| (s.name.as_str(), s))
        .collect();
    let current_configs: std::collections::HashMap<&str, &WindowConfig> = current
        .runtime_window_configs
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();
    let next_configs: std::collections::HashMap<&str, &WindowConfig> = next
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use wf_config::ConfigVarContext;
    use wf_config::FusionConfigLoader;

    use super::compile_reload_artifacts;
    use super::prepare_reload_with_cached;
    use crate::lifecycle::*;

    fn make_temp_dir(name: &str) -> PathBuf {
        let unique = format!(
            "wf-runtime-reload-{}-{}-{}",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_nanos()
        );
        let dir = std::env::temp_dir().join(unique);
        std::fs::create_dir_all(&dir).expect("failed to create temp dir");
        dir
    }

    fn write_file(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("failed to create parent dir");
        }
        std::fs::write(path, content).expect("failed to write test file");
    }

    fn base_config(
        runtime_schemas: &str,
        runtime_rules: &str,
        vars_block: &str,
        windows_path: &str,
    ) -> String {
        format!(
            r#"
mode = "daemon"
sinks = "sinks"
windows = "{windows_path}"

[[sources]]
type = "tcp"
name = "ingress"
listen = "tcp://127.0.0.1:0"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "{runtime_schemas}"
rules = "{runtime_rules}"
{vars_block}
"#
        )
    }

    const WINDOWS_TOML: &str = r#"[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.auth_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"

[window.security_alerts]
mode = "local"
max_window_bytes = "64MB"
over_cap = "1h"
"#;

    fn security_schema() -> &'static str {
        r#"
window auth_events {
    stream_tag = "syslog"
    time = event_time
    over = 5m

    fields {
        sip: ip
        username: chars
        action: chars
        event_time: time
    }
}

window security_alerts {
    over = 0
    fields {
        sip: ip
        fail_count: digit
        message: chars
    }
}
"#
    }

    fn simple_rule() -> &'static str {
        r#"
rule brute_force_then_scan {
  events {
    fail : auth_events && action == "failed"
  }

  match<sip:5m> {
    on event {
      fail | count >= ${FAIL_THRESHOLD:3};
    }
    and close {
      fail | count >= 1;
    }
  } -> score(70.0)

  entity(ip, fail.sip)

  yield security_alerts (
    sip = fail.sip,
    fail_count = count(fail),
    message = fmt("{} brute force detected", fail.sip)
  )
}
"#
    }

    fn pipeline_rule() -> &'static str {
        r#"
rule repeated_fail_bursts {
  events {
    e : auth_events && action == "failed"
  }

  match<sip,username:5m:fixed> {
    on event {
      e | count >= 1;
    }
    and close {
      burst: e | count >= 3;
    }
  }
  |> match<sip:30m:fixed> {
    on event {
      _in | count >= 1;
    }
    and close {
      users: _in.username | distinct | count >= 2;
    }
  } -> score(85.0)

  entity(ip, _in.sip)

  yield security_alerts (
    sip = _in.sip,
    fail_count = 2,
    message = fmt("{} multi-user fail bursts", _in.sip)
  )
}
"#
    }

    fn load_state(
        base_path: &Path,
        overlay_paths: &[PathBuf],
    ) -> (wf_config::RawFusionConfigTree, wf_config::FusionConfig) {
        let ctx = ConfigVarContext::new();
        let loader = FusionConfigLoader::new(base_path, overlay_paths, &ctx, None);
        let raw = loader.load_raw().expect("load raw config");
        let config = loader.load().expect("load config");
        (raw, config)
    }

    #[test]
    fn prepare_reload_accepts_vars_only_rule_recompile() {
        let root = make_temp_dir("vars-ready");
        let base_path = root.join("conf/wfusion.toml");
        let next_overlay = root.join("env/dev/vars.toml");
        write_file(&root.join("models/windows.toml"), WINDOWS_TOML);
        let windows_path = root.join("models/windows.toml");
        write_file(
            &base_path,
            &base_config(
                "../schemas/*.wfs",
                "../rules/current/*.wfl",
                r#"
[vars]
FAIL_THRESHOLD = "3"
"#,
                &windows_path.to_string_lossy(),
            ),
        );
        write_file(&root.join("schemas/security.wfs"), security_schema());
        write_file(&root.join("rules/current/brute_force.wfl"), simple_rule());
        write_file(
            &next_overlay,
            r#"
[vars]
FAIL_THRESHOLD = "5"
"#,
        );

        let (current_raw, current_config) = load_state(&base_path, &[]);
        let (next_raw, next_config) = load_state(&base_path, std::slice::from_ref(&next_overlay));

        let prepared = prepare_reload(
            &current_raw,
            &current_config,
            next_raw,
            next_config,
            base_path.parent().expect("base config dir"),
        )
        .expect("prepare reload");

        match prepared {
            ReloadPreparation::Ready(reload) => {
                assert_eq!(reload.plan.hot_reload.len(), 1);
                assert!(reload.plan.requires_restart.is_empty());
                assert!(reload.plan.unsupported.is_empty());
                assert_eq!(reload.next_rules.len(), 1);
                assert!(reload.next_intermediate_targets.is_empty());
                assert_eq!(reload.next_schemas.len(), 2);
            }
            ReloadPreparation::Blocked(plan) => {
                panic!(
                    "expected hot reload to be ready, blockers: {:?}",
                    plan.requires_restart
                );
            }
        }

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_reload_blocks_vars_that_change_effective_runtime_settings() {
        let root = make_temp_dir("vars-blocked-effective");
        let base_path = root.join("conf/wfusion.toml");
        let next_overlay = root.join("env/dev/vars.toml");
        write_file(&root.join("models/windows.toml"), WINDOWS_TOML);
        let windows_path = root.join("models/windows.toml");
        write_file(
            &base_path,
            &base_config(
                "${SCHEMA_GLOB}",
                "../rules/current/*.wfl",
                r#"
[vars]
SCHEMA_GLOB = "../schemas/*.wfs"
FAIL_THRESHOLD = "3"
"#,
                &windows_path.to_string_lossy(),
            ),
        );
        write_file(&root.join("schemas/security.wfs"), security_schema());
        write_file(&root.join("schemas_alt/security.wfs"), security_schema());
        write_file(&root.join("rules/current/brute_force.wfl"), simple_rule());
        write_file(
            &next_overlay,
            r#"
[vars]
SCHEMA_GLOB = "../schemas_alt/*.wfs"
"#,
        );

        let (current_raw, current_config) = load_state(&base_path, &[]);
        let (next_raw, next_config) = load_state(&base_path, std::slice::from_ref(&next_overlay));

        let prepared = prepare_reload(
            &current_raw,
            &current_config,
            next_raw,
            next_config,
            base_path.parent().expect("base config dir"),
        )
        .expect("prepare reload");

        match prepared {
            ReloadPreparation::Ready(_) => {
                panic!("expected effective runtime.schemas change to require restart");
            }
            ReloadPreparation::Blocked(plan) => {
                assert!(plan.requires_restart.iter().any(|change| {
                    change.change.path == "runtime.schemas"
                        && change.kind == wf_config::FusionChangeKind::Runtime
                }));
            }
        }

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_reload_allows_pipeline_rule_addition() {
        // L2: pipeline rules create internal windows that did not exist
        // before. These are *pure additions* (new window names) — the
        // running registry can accept them at runtime via `try_add_window`.
        let root = make_temp_dir("rules-added-topology");
        let base_path = root.join("conf/wfusion.toml");
        let next_overlay = root.join("env/dev/rules.toml");
        write_file(&root.join("models/windows.toml"), WINDOWS_TOML);
        let windows_path = root.join("models/windows.toml");
        write_file(
            &base_path,
            &base_config(
                "../schemas/*.wfs",
                "../rules/v1/*.wfl",
                "",
                &windows_path.to_string_lossy(),
            ),
        );
        write_file(&root.join("schemas/security.wfs"), security_schema());
        write_file(&root.join("rules/v1/brute_force.wfl"), simple_rule());
        write_file(
            &root.join("rules/v2/repeated_fail_bursts.wfl"),
            pipeline_rule(),
        );
        write_file(
            &next_overlay,
            r#"
[runtime]
rules = "../../rules/v2/*.wfl"
"#,
        );

        let (current_raw, current_config) = load_state(&base_path, &[]);
        let (next_raw, next_config) = load_state(&base_path, std::slice::from_ref(&next_overlay));

        let prepared = prepare_reload(
            &current_raw,
            &current_config,
            next_raw,
            next_config,
            base_path.parent().expect("base config dir"),
        )
        .expect("prepare reload");

        match prepared {
            ReloadPreparation::Ready(_) => {
                // L2: pipeline windows are additions, allowed.
            }
            ReloadPreparation::Blocked(plan) => {
                panic!(
                    "pipeline rule addition should be Ready under L2, got blocked: {:?}",
                    plan.requires_restart
                );
            }
        }

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_reload_blocks_schema_path_change() {
        // Changing `runtime.schemas` path is blocked at the raw-diff level
        // by `build_reload_plan` (RestartRequired). L3 schema modification
        // cannot use path-switching; it relies on the reactor caching
        // compiled artifacts from boot time (future work).
        let root = make_temp_dir("schema-path-blocked");
        let base_path = root.join("conf/wfusion.toml");
        let next_overlay = root.join("env/dev/schemas.toml");
        write_file(&root.join("models/windows.toml"), WINDOWS_TOML);
        let windows_path = root.join("models/windows.toml");
        write_file(
            &base_path,
            &base_config(
                "../schemas/*.wfs",
                "../rules/v1/*.wfl",
                "",
                &windows_path.to_string_lossy(),
            ),
        );
        write_file(&root.join("schemas/security.wfs"), security_schema());
        write_file(&root.join("rules/v1/brute_force.wfl"), simple_rule());
        // Write a modified schema with an extra `severity` field.
        let mod_dir = root.join("schemas_modified");
        std::fs::create_dir_all(&mod_dir).unwrap();
        write_file(
            &mod_dir.join("security.wfs"),
            r#"
window auth_events {
    stream_tag = "syslog"
    time = event_time
    over = 5m

    fields {
        sip: ip
        username: chars
        action: chars
        severity: digit
        event_time: time
    }
}

window security_alerts {
    over = 0
    fields {
        sip: ip
        fail_count: digit
        message: chars
    }
}
"#,
        );
        write_file(
            &next_overlay,
            "[runtime]\nschemas = \"../../schemas_modified/*.wfs\"\n",
        );

        let (current_raw, current_config) = load_state(&base_path, &[]);
        let (next_raw, next_config) = load_state(&base_path, std::slice::from_ref(&next_overlay));

        let prepared = prepare_reload(
            &current_raw,
            &current_config,
            next_raw,
            next_config,
            base_path.parent().expect("base config dir"),
        )
        .expect("prepare reload");

        match prepared {
            ReloadPreparation::Ready(_) => {
                panic!("schema path change should be blocked at raw-diff level");
            }
            ReloadPreparation::Blocked(plan) => {
                assert!(
                    plan.requires_restart
                        .iter()
                        .any(|c| c.change.path == "runtime.schemas"),
                    "expected runtime.schemas blocker"
                );
            }
        }

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn prepare_reload_blocks_window_config_change() {
        // Changing `[window.X].over_cap` is blocked at the raw-diff level
        // by `build_reload_plan` (kind: Windows, RestartRequired). Future
        // work: relax for per-window config changes to enable L3.
        let root = make_temp_dir("window-config-blocked");
        let base_path = root.join("conf/wfusion.toml");
        let next_overlay = root.join("env/dev/config.toml");
        write_file(&root.join("models/windows.toml"), WINDOWS_TOML);
        let windows_path = root.join("models/windows.toml");
        write_file(
            &base_path,
            &base_config(
                "../schemas/*.wfs",
                "../rules/v1/*.wfl",
                "",
                &windows_path.to_string_lossy(),
            ),
        );
        write_file(&root.join("schemas/security.wfs"), security_schema());
        write_file(&root.join("rules/v1/brute_force.wfl"), simple_rule());
        write_file(
            &next_overlay,
            r#"
[window.auth_events]
over_cap = "1h"
"#,
        );

        let (current_raw, current_config) = load_state(&base_path, &[]);
        let (next_raw, next_config) = load_state(&base_path, std::slice::from_ref(&next_overlay));

        let prepared = prepare_reload(
            &current_raw,
            &current_config,
            next_raw,
            next_config,
            base_path.parent().expect("base config dir"),
        )
        .expect("prepare reload");

        match prepared {
            ReloadPreparation::Ready(_) => {
                panic!("window config change should be blocked at raw-diff level");
            }
            ReloadPreparation::Blocked(plan) => {
                assert!(
                    plan.requires_restart
                        .iter()
                        .any(|c| c.kind == wf_config::FusionChangeKind::Windows
                            && c.change.path.starts_with("window")),
                    "expected window config blocker"
                );
            }
        }

        let _ = std::fs::remove_dir_all(root);
    }

    // -- L3 cached-compare tests (prepare_reload_with_cached) --------------

    /// In-place schema edit (same path, different content) is detected when
    /// the current side is cached from boot time and the next side is compiled
    /// from the (now-modified) disk file.
    #[test]
    fn prepare_reload_with_cached_allows_schema_modification() {
        let root = make_temp_dir("cached-schema-mod");
        let base_path = root.join("conf/wfusion.toml");
        write_file(&root.join("models/windows.toml"), WINDOWS_TOML);
        let windows_path = root.join("models/windows.toml");
        write_file(
            &base_path,
            &base_config(
                "../schemas/*.wfs",
                "../rules/v1/*.wfl",
                "",
                &windows_path.to_string_lossy(),
            ),
        );
        write_file(&root.join("schemas/security.wfs"), security_schema());
        write_file(&root.join("rules/v1/brute_force.wfl"), simple_rule());

        // Step 1: capture current state (original schema).
        let (current_raw, current_config) = load_state(&base_path, &[]);
        let current = compile_reload_artifacts(&current_config, base_path.parent().unwrap())
            .expect("compile current");

        // Step 2: modify the schema in-place (same file path).
        write_file(
            &root.join("schemas/security.wfs"),
            r#"
window auth_events {
    stream_tag = "syslog"
    time = event_time
    over = 5m

    fields {
        sip: ip
        username: chars
        action: chars
        severity: digit
        event_time: time
    }
}

window security_alerts {
    over = 0
    fields {
        sip: ip
        fail_count: digit
        message: chars
    }
}
"#,
        );

        // Step 3: load next config (same raw tree, different file content).
        let (next_raw, next_config) = load_state(&base_path, &[]);

        let prepared = prepare_reload_with_cached(
            &current_raw,
            &current_config,
            &current.runtime_schemas,
            &current.runtime_window_configs,
            next_raw,
            next_config,
            base_path.parent().expect("base config dir"),
        )
        .expect("prepare reload");

        match prepared {
            ReloadPreparation::Ready(ready) => {
                assert_eq!(
                    ready.modified_schemas.len(),
                    1,
                    "should detect 1 modified schema"
                );
                assert_eq!(ready.modified_schemas[0].name, "auth_events");
                assert!(ready.added_schemas.is_empty(), "no windows were added");
            }
            ReloadPreparation::Blocked(plan) => {
                panic!(
                    "cached-compare schema mod should be Ready, got blocked: {:?}",
                    plan.requires_restart
                );
            }
        }

        let _ = std::fs::remove_dir_all(root);
    }

    /// Both schemas modified at once — verify each is tracked independently.
    #[test]
    fn prepare_reload_with_cached_allows_multiple_schema_modifications() {
        let root = make_temp_dir("cached-multi-mod");
        let base_path = root.join("conf/wfusion.toml");
        write_file(&root.join("models/windows.toml"), WINDOWS_TOML);
        let windows_path = root.join("models/windows.toml");
        write_file(
            &base_path,
            &base_config(
                "../schemas/*.wfs",
                "../rules/v1/*.wfl",
                "",
                &windows_path.to_string_lossy(),
            ),
        );
        write_file(&root.join("schemas/security.wfs"), security_schema());
        write_file(&root.join("rules/v1/brute_force.wfl"), simple_rule());

        let (current_raw, current_config) = load_state(&base_path, &[]);
        let current = compile_reload_artifacts(&current_config, base_path.parent().unwrap())
            .expect("compile current");

        // Modify both windows: add `severity: digit` to auth_events AND
        // `source_ip: ip` to security_alerts.
        write_file(
            &root.join("schemas/security.wfs"),
            r#"
window auth_events {
    stream_tag = "syslog"
    time = event_time
    over = 5m
    fields {
        sip: ip
        username: chars
        action: chars
        severity: digit
        event_time: time
    }
}

window security_alerts {
    over = 0
    fields {
        sip: ip
        fail_count: digit
        source_ip: ip
        message: chars
    }
}
"#,
        );

        let (next_raw, next_config) = load_state(&base_path, &[]);

        let prepared = prepare_reload_with_cached(
            &current_raw,
            &current_config,
            &current.runtime_schemas,
            &current.runtime_window_configs,
            next_raw,
            next_config,
            base_path.parent().expect("base config dir"),
        )
        .expect("prepare reload");

        match prepared {
            ReloadPreparation::Ready(ready) => {
                assert_eq!(ready.modified_schemas.len(), 2);
                assert!(ready.added_schemas.is_empty());
            }
            ReloadPreparation::Blocked(plan) => {
                panic!(
                    "cached-compare multi-mod should be Ready, got blocked: {:?}",
                    plan.requires_restart
                );
            }
        }

        let _ = std::fs::remove_dir_all(root);
    }

    /// Mixed L2+L3: a pipeline rule adds internal windows (`|>`), and at the
    /// same time an existing schema is modified in-place. Both `added_*` and
    /// `modified_*` must be non-empty.
    #[test]
    fn prepare_reload_with_cached_allows_mixed_add_and_modify() {
        let root = make_temp_dir("cached-mixed");
        let base_path = root.join("conf/wfusion.toml");
        write_file(&root.join("models/windows.toml"), WINDOWS_TOML);
        let windows_path = root.join("models/windows.toml");
        write_file(
            &base_path,
            &base_config(
                "../schemas/*.wfs",
                "../rules/v1/*.wfl",
                "",
                &windows_path.to_string_lossy(),
            ),
        );
        write_file(&root.join("schemas/security.wfs"), security_schema());
        write_file(&root.join("rules/v1/brute_force.wfl"), simple_rule());

        let (current_raw, current_config) = load_state(&base_path, &[]);
        let current = compile_reload_artifacts(&current_config, base_path.parent().unwrap())
            .expect("compile current");

        // (a) Modify auth_events in-place.
        write_file(
            &root.join("schemas/security.wfs"),
            r#"
window auth_events {
    stream_tag = "syslog"
    time = event_time
    over = 5m
    fields {
        sip: ip
        username: chars
        action: chars
        severity: digit
        event_time: time
    }
}

window security_alerts {
    over = 0
    fields {
        sip: ip
        fail_count: digit
        message: chars
    }
}
"#,
        );
        // (b) Replace the rules file with a pipeline rule that creates
        //     internal pipeline windows (pure L2 addition).
        std::fs::create_dir_all(root.join("rules/v2")).unwrap();
        write_file(
            &root.join("rules/v2/repeated_fail_bursts.wfl"),
            pipeline_rule(),
        );
        // Use an overlay to repoint the rules glob (rules changes are
        // hot-reloadable, not blocked by raw-diff).
        let next_overlay = root.join("env/dev/rules.toml");
        write_file(
            &next_overlay,
            "[runtime]\nrules = \"../../rules/v2/*.wfl\"\n",
        );

        let (next_raw, next_config) = load_state(&base_path, std::slice::from_ref(&next_overlay));

        let prepared = prepare_reload_with_cached(
            &current_raw,
            &current_config,
            &current.runtime_schemas,
            &current.runtime_window_configs,
            next_raw,
            next_config,
            base_path.parent().expect("base config dir"),
        )
        .expect("prepare reload");

        match prepared {
            ReloadPreparation::Ready(ready) => {
                assert!(
                    !ready.added_schemas.is_empty(),
                    "pipeline windows should be added (L2)"
                );
                assert!(
                    !ready.modified_schemas.is_empty(),
                    "auth_events schema should be modified (L3)"
                );
            }
            ReloadPreparation::Blocked(plan) => {
                panic!(
                    "mixed add+modify should be Ready, got blocked: {:?}",
                    plan.requires_restart
                );
            }
        }

        let _ = std::fs::remove_dir_all(root);
    }
}
