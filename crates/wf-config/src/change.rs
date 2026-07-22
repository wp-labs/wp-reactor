use crate::config_loader::loader::{RawFusionConfigChange, RawFusionConfigTree};

#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Config", module = "Config.ConfigChange")]
pub enum FusionChangeKind {
    Rules,
    Vars,
    Runtime,
    Sources,
    Windows,
    Sinks,
    Output,
    Logging,
    Metrics,
    Mode,
    Unknown,
}

#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Config", module = "Config.ConfigChange")]
pub enum FusionReloadDisposition {
    HotReloadSupported,
    RequiresRestart,
    Unsupported,
}

#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigChange")]
pub struct ClassifiedFusionConfigChange {
    pub change: RawFusionConfigChange,
    pub kind: FusionChangeKind,
    pub disposition: FusionReloadDisposition,
    pub reason: &'static str,
}

#[derive(::moju_derive::MoJu, Debug, Clone, Default)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigChange")]
pub struct FusionReloadPlan {
    pub hot_reload: Vec<ClassifiedFusionConfigChange>,
    pub requires_restart: Vec<ClassifiedFusionConfigChange>,
    pub unsupported: Vec<ClassifiedFusionConfigChange>,
}

impl FusionReloadPlan {
    pub fn can_hot_reload(&self) -> bool {
        self.requires_restart.is_empty() && self.unsupported.is_empty()
    }

    pub fn has_blockers(&self) -> bool {
        !self.can_hot_reload()
    }
}

impl RawFusionConfigTree {
    pub fn build_reload_plan(&self, next: &RawFusionConfigTree) -> FusionReloadPlan {
        let mut plan = FusionReloadPlan::default();
        for change in self.diff(next) {
            let classified = classify_change(change);
            match classified.disposition {
                FusionReloadDisposition::HotReloadSupported => plan.hot_reload.push(classified),
                FusionReloadDisposition::RequiresRestart => plan.requires_restart.push(classified),
                FusionReloadDisposition::Unsupported => plan.unsupported.push(classified),
            }
        }
        plan
    }
}

fn classify_change(change: RawFusionConfigChange) -> ClassifiedFusionConfigChange {
    let (kind, disposition, reason) = classify_path(&change.path);
    ClassifiedFusionConfigChange {
        change,
        kind,
        disposition,
        reason,
    }
}

fn classify_path(path: &str) -> (FusionChangeKind, FusionReloadDisposition, &'static str) {
    if path == "runtime.rules" {
        return (
            FusionChangeKind::Rules,
            FusionReloadDisposition::HotReloadSupported,
            "rule file set changed; runtime can recompile and swap rule plans",
        );
    }
    if path == "vars" || path.starts_with("vars.") {
        return (
            FusionChangeKind::Vars,
            FusionReloadDisposition::HotReloadSupported,
            "variable set changed; reload may re-preprocess and recompile rules",
        );
    }
    if path == "mode" {
        return (
            FusionChangeKind::Mode,
            FusionReloadDisposition::RequiresRestart,
            "runtime mode changes alter lifecycle semantics and require restart",
        );
    }
    if path == "sinks" || path == "work_root" {
        return (
            FusionChangeKind::Sinks,
            FusionReloadDisposition::RequiresRestart,
            "sink root changes require rebuilding output topology",
        );
    }
    if path == "runtime.schemas" || path.starts_with("runtime.") {
        return (
            FusionChangeKind::Runtime,
            FusionReloadDisposition::RequiresRestart,
            "runtime execution and schema settings are not hot-reloadable yet",
        );
    }
    if path == "output" || path.starts_with("output.") {
        return (
            FusionChangeKind::Output,
            FusionReloadDisposition::HotReloadSupported,
            "output formatting changed; runtime can recompile and swap rule plans",
        );
    }
    if path == "sources" || path.starts_with("sources[") {
        return (
            FusionChangeKind::Sources,
            FusionReloadDisposition::RequiresRestart,
            "source changes require receiver task reconstruction",
        );
    }
    if path == "window_defaults"
        || path.starts_with("window_defaults.")
        || path == "window"
        || path.starts_with("window.")
    {
        return (
            FusionChangeKind::Windows,
            FusionReloadDisposition::RequiresRestart,
            "window topology changes affect registry and in-memory state layout",
        );
    }
    if path == "logging" || path.starts_with("logging.") {
        return (
            FusionChangeKind::Logging,
            FusionReloadDisposition::RequiresRestart,
            "logging pipeline reconfiguration is not hot-reloadable yet",
        );
    }
    if path == "metrics" || path.starts_with("metrics.") {
        return (
            FusionChangeKind::Metrics,
            FusionReloadDisposition::RequiresRestart,
            "metrics server/task configuration is not hot-reloadable yet",
        );
    }
    (
        FusionChangeKind::Unknown,
        FusionReloadDisposition::Unsupported,
        "unclassified config path; reject until reload semantics are defined",
    )
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::ConfigVarContext;
    use crate::FusionConfigLoader;

    fn make_temp_dir(name: &str) -> PathBuf {
        let unique = format!(
            "wf-config-change-{}-{}-{}",
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

    #[test]
    fn reload_plan_marks_rules_and_vars_as_hot_reloadable() {
        let root = make_temp_dir("hot-reloadable");
        let base_path = root.join("conf/base.toml");
        let old_overlay = root.join("env/dev/old.toml");
        let new_overlay = root.join("env/dev/new.toml");
        write_file(
            &base_path,
            r#"
mode = "batch"
sinks = "sinks"

[[sources]]
type = "file"
path = "data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"

[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.base_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"
"#,
        );
        write_file(
            &old_overlay,
            r#"
[runtime]
rules = "../rules/v1/*.wfl"
"#,
        );
        write_file(
            &new_overlay,
            r#"
[runtime]
rules = "../rules/v2/*.wfl"

[vars]
CASE_PATH = "/tmp/case-a"
"#,
        );

        let old_raw = FusionConfigLoader::new(
            &base_path,
            std::slice::from_ref(&old_overlay),
            &ConfigVarContext::new(),
            None,
        )
        .load_raw()
        .expect("load old raw");
        let new_raw = FusionConfigLoader::new(
            &base_path,
            std::slice::from_ref(&new_overlay),
            &ConfigVarContext::new(),
            None,
        )
        .load_raw()
        .expect("load new raw");

        let plan = old_raw.build_reload_plan(&new_raw);
        assert_eq!(plan.hot_reload.len(), 2);
        assert!(plan.requires_restart.is_empty());
        assert!(plan.unsupported.is_empty());
        assert!(plan.can_hot_reload());
        assert!(
            plan.hot_reload
                .iter()
                .any(|c| c.kind == FusionChangeKind::Rules)
        );
        assert!(
            plan.hot_reload
                .iter()
                .any(|c| c.kind == FusionChangeKind::Vars)
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn reload_plan_marks_output_format_changes_as_hot_reloadable() {
        let root = make_temp_dir("output-hot-reloadable");
        let base_path = root.join("conf/base.toml");
        let old_overlay = root.join("env/dev/old.toml");
        let new_overlay = root.join("env/dev/new.toml");
        write_file(
            &base_path,
            r#"
mode = "batch"
sinks = "sinks"

[[sources]]
type = "file"
path = "data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"

[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.base_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"
"#,
        );
        write_file(
            &old_overlay,
            r#"
[output]
time_format = "%Y-%m-%d %H:%M:%S%.3f"
"#,
        );
        write_file(
            &new_overlay,
            r#"
[output]
time_format = "%Y-%m-%d"
"#,
        );

        let old_raw = FusionConfigLoader::new(
            &base_path,
            std::slice::from_ref(&old_overlay),
            &ConfigVarContext::new(),
            None,
        )
        .load_raw()
        .expect("load old raw");
        let new_raw = FusionConfigLoader::new(
            &base_path,
            std::slice::from_ref(&new_overlay),
            &ConfigVarContext::new(),
            None,
        )
        .load_raw()
        .expect("load new raw");

        let plan = old_raw.build_reload_plan(&new_raw);
        assert_eq!(plan.hot_reload.len(), 1);
        assert!(plan.requires_restart.is_empty());
        assert!(plan.unsupported.is_empty());
        assert!(plan.can_hot_reload());
        assert_eq!(plan.hot_reload[0].kind, FusionChangeKind::Output);
        assert_eq!(
            plan.hot_reload[0].disposition,
            FusionReloadDisposition::HotReloadSupported
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn reload_plan_marks_source_changes_as_restart_required() {
        let root = make_temp_dir("restart-required");
        let base_path = root.join("conf/base.toml");
        let old_overlay = root.join("env/dev/old.toml");
        let new_overlay = root.join("env/dev/new.toml");
        write_file(
            &base_path,
            r#"
mode = "batch"
sinks = "sinks"

[[sources]]
type = "file"
path = "data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"

[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.base_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"
"#,
        );
        write_file(
            &old_overlay,
            r#"
[[sources]]
type = "file"
path = "../data/v1.ndjson"
stream_tag = "syslog"
format = "ndjson"
"#,
        );
        write_file(
            &new_overlay,
            r#"
[[sources]]
type = "file"
path = "../data/v2.ndjson"
stream_tag = "syslog"
format = "ndjson"
"#,
        );

        let old_raw = FusionConfigLoader::new(
            &base_path,
            std::slice::from_ref(&old_overlay),
            &ConfigVarContext::new(),
            None,
        )
        .load_raw()
        .expect("load old raw");
        let new_raw = FusionConfigLoader::new(
            &base_path,
            std::slice::from_ref(&new_overlay),
            &ConfigVarContext::new(),
            None,
        )
        .load_raw()
        .expect("load new raw");

        let plan = old_raw.build_reload_plan(&new_raw);
        assert!(plan.hot_reload.is_empty());
        assert_eq!(plan.requires_restart.len(), 1);
        assert!(plan.has_blockers());
        assert_eq!(plan.requires_restart[0].kind, FusionChangeKind::Sources);
        assert_eq!(
            plan.requires_restart[0].disposition,
            FusionReloadDisposition::RequiresRestart
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn reload_plan_rejects_unclassified_paths() {
        let root = make_temp_dir("unsupported");
        let base_path = root.join("conf/base.toml");
        let old_overlay = root.join("env/dev/old.toml");
        let new_overlay = root.join("env/dev/new.toml");
        write_file(
            &base_path,
            r#"
mode = "batch"
sinks = "sinks"

[[sources]]
type = "file"
path = "data/base.ndjson"
stream_tag = "syslog"
format = "ndjson"

[runtime]
executor_parallelism = 2
rule_exec_timeout = "30s"
schemas = "schemas/base/*.wfs"
rules = "rules/base/*.wfl"

[window_defaults]
evict_interval = "30s"
max_window_bytes = "256MB"
max_total_bytes = "2GB"
evict_policy = "time_first"
watermark = "5s"
allowed_lateness = "0s"
late_policy = "drop"

[window.base_events]
mode = "local"
max_window_bytes = "256MB"
over_cap = "30m"
"#,
        );
        write_file(&old_overlay, "");
        write_file(
            &new_overlay,
            r#"
[experimental]
flag = true
"#,
        );

        let old_raw = FusionConfigLoader::new(
            &base_path,
            std::slice::from_ref(&old_overlay),
            &ConfigVarContext::new(),
            None,
        )
        .load_raw()
        .expect("load old raw");
        let new_raw = FusionConfigLoader::new(
            &base_path,
            std::slice::from_ref(&new_overlay),
            &ConfigVarContext::new(),
            None,
        )
        .load_raw()
        .expect("load new raw");

        let plan = old_raw.build_reload_plan(&new_raw);
        assert!(plan.hot_reload.is_empty());
        assert!(plan.requires_restart.is_empty());
        assert_eq!(plan.unsupported.len(), 1);
        assert_eq!(plan.unsupported[0].kind, FusionChangeKind::Unknown);
        assert_eq!(
            plan.unsupported[0].disposition,
            FusionReloadDisposition::Unsupported
        );

        let _ = std::fs::remove_dir_all(root);
    }
}
