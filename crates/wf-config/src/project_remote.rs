use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Project remote config (mirrors warp-parse [project_remote] pattern)
// ---------------------------------------------------------------------------

/// Remote rule-source sync configuration (`[project_remote]` in wfusion.toml).
///
/// Drives `wfadm conf update` (and, later, admin_api reload): a managed set of
/// directories (`models`, `conf`, `topology`, `connectors`) is synced from a
/// remote git repo at a given version tag, validated, and rolled back on
/// failure. See `docs/design/project_remote_alignment.md`.
///
/// Two modes:
/// - **Single-repo**: `repo` set, `models`/`infra` unset — one repo owns all
///   managed dirs.
/// - **Dual-repo**: `models` and/or `infra` set — each group owns a subset
///   (`models` → `models/`; `infra` → `conf`,`topology`,`connectors`).
#[derive(Debug, Default, PartialEq, Eq, Deserialize, Serialize, Clone, ::moju_derive::MoJu)]
#[serde(deny_unknown_fields)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct ProjectRemoteConf {
    #[serde(default, alias = "enable")]
    pub enabled: bool,
    #[serde(default)]
    pub repo: String,
    #[serde(default)]
    pub init_version: String,
    #[serde(default)]
    pub models: Option<RepoGroupConf>,
    #[serde(default)]
    pub infra: Option<RepoGroupConf>,
}

/// One remote group in dual-repo mode (`[project_remote.models]` /
/// `[project_remote.infra]`).
#[derive(Debug, Default, PartialEq, Eq, Deserialize, Serialize, Clone, ::moju_derive::MoJu)]
#[serde(deny_unknown_fields)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigIo")]
pub struct RepoGroupConf {
    #[serde(default)]
    pub repo: String,
    #[serde(default)]
    pub init_version: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_section_uses_defaults() {
        // Empty TOML → all defaults (mirrors a config without [project_remote]).
        let conf: ProjectRemoteConf = toml::from_str("").unwrap();
        assert!(!conf.enabled);
        assert!(conf.repo.is_empty());
        assert!(conf.init_version.is_empty());
        assert!(conf.models.is_none());
        assert!(conf.infra.is_none());
    }

    #[test]
    fn single_repo_parses() {
        let toml = r#"
enabled = true
repo = "https://github.com/wp-labs/wf-rules.git"
init_version = "v0.1.0"
"#;
        let conf: ProjectRemoteConf = toml::from_str(toml).unwrap();
        assert!(conf.enabled);
        assert_eq!(conf.repo, "https://github.com/wp-labs/wf-rules.git");
        assert_eq!(conf.init_version, "v0.1.0");
        assert!(conf.models.is_none());
        assert!(conf.infra.is_none());
    }

    #[test]
    fn dual_repo_parses() {
        let toml = r#"
enabled = true
init_version = "v0.1.0"

[models]
repo = "https://github.com/wp-labs/wf-rules-models.git"
init_version = "v0.1.0"

[infra]
repo = "https://github.com/wp-labs/wf-rules-infra.git"
"#;
        let conf: ProjectRemoteConf = toml::from_str(toml).unwrap();
        assert!(conf.enabled);
        assert!(conf.repo.is_empty());
        let models = conf.models.unwrap();
        assert_eq!(
            models.repo,
            "https://github.com/wp-labs/wf-rules-models.git"
        );
        let infra = conf.infra.unwrap();
        assert_eq!(infra.repo, "https://github.com/wp-labs/wf-rules-infra.git");
    }

    #[test]
    fn rejects_unknown_field() {
        // deny_unknown_fields must reject bogus keys.
        let toml = r#"
enabled = true
bogus_field = "x"
"#;
        let res: Result<ProjectRemoteConf, _> = toml::from_str(toml);
        assert!(res.is_err(), "expected unknown-field rejection");
    }

    #[test]
    fn enable_alias_works() {
        // `enable` is an alias for `enabled`.
        let toml = "enable = true\n";
        let conf: ProjectRemoteConf = toml::from_str(toml).unwrap();
        assert!(conf.enabled);
    }

    #[test]
    fn roundtrip_serialize() {
        let toml = r#"
enabled = true
repo = "r"
init_version = "v1"

[models]
repo = "rm"
init_version = "vm"
"#;
        let conf: ProjectRemoteConf = toml::from_str(toml).unwrap();
        let serialized = toml::to_string(&conf).unwrap();
        let reparsed: ProjectRemoteConf = toml::from_str(&serialized).unwrap();
        assert_eq!(conf, reparsed);
    }
}
