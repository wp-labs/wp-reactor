use std::path::{Path, PathBuf};

use wf_config::ConfigVarContext;
use wf_config::FusionConfigLoader;

use super::compile::compile_reload_artifacts;
use super::prepare::prepare_reload_with_cached;
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
