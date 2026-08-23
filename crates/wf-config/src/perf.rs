//! 性能诊断模式配置（perf-diag）。
//!
//! 独立于 `wfusion.toml` 的诊断配置文件（`wfusion daemon --perf-diag
//! conf/perf-diag.toml`），引擎与 `wfgen perf-diag` 读同一份。诊断点列表
//! 由 sentinel（漂流瓶）驱动依次应用——见
//! `docs/design/perf-diag-mode-design.md`。

use std::path::Path;

use orion_error::conversion::{SourceErr, SourceRawErr};
use serde::{Deserialize, Serialize};

use crate::{ConfigReason, ConfigResult};

/// 诊断模式配置（`--perf-diag <path>` 加载，不进 `wfusion.toml`）。
///
/// 全字段 `#[serde(default)]`——空文件/缺字段即默认关闭。
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct PerfConfig {
    /// 诊断模式总开关：`true` 时引擎注册内置 `__perf_sentinel` 窗口/哨兵规则。
    #[serde(default)]
    pub diag: bool,
    /// 初始门控：禁止规则求值（`process_batch` 直通，ack 保留）。
    #[serde(default)]
    pub cut_rules: bool,
    /// 初始门控：禁止输出链（emit 不 serialize/stage/commit）。
    #[serde(default)]
    pub cut_output: bool,
    /// 诊断点列表（sentinel 驱动依次应用）。缺省/空 = 单点模式（仅初始门控）。
    #[serde(default)]
    pub points: Vec<PerfPoint>,
}

/// 一个诊断点 = 禁止开关组合 + 可选规则子集文件（触发热 reload）。
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct PerfPoint {
    /// 点名称（墙表输出用，如 `floor` / `rules` / `full`）。
    #[serde(default)]
    pub name: String,
    /// 本点生效期间：禁止规则求值。
    #[serde(default)]
    pub cut_rules: bool,
    /// 本点生效期间：禁止输出链。
    #[serde(default)]
    pub cut_output: bool,
    /// 规则子集文件路径（相对 work-dir）。空 = 保持当前规则；非空且与当前
    /// 不同 → 触发既有 `runtime.rules` 热 reload（HotReloadSupported）。
    #[serde(default)]
    pub rules: Option<String>,
}

impl PerfConfig {
    /// 从诊断配置文件加载。文件缺失/解析失败 → 报错（显式 `--perf-diag` 即
    /// 要求诊断模式可用，静默降级会掩盖配置错误）。
    pub fn load(path: &Path) -> ConfigResult<Self> {
        let content = std::fs::read_to_string(path)
            .source_err(ConfigReason::Load, format!("reading perf-diag config {}", path.display()))?;
        let config: PerfConfig = toml::from_str(&content).source_raw_err(
            ConfigReason::Parse,
            format!("parsing perf-diag config {}", path.display()),
        )?;
        Ok(config)
    }

    /// 诊断点数量（0 = 单点模式）。
    pub fn point_count(&self) -> usize {
        self.points.len()
    }

    /// 第 `index` 个诊断点；越界返回 `None`。
    pub fn point_at(&self, index: usize) -> Option<&PerfPoint> {
        self.points.get(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_defaults_to_disabled() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("perf_diag_empty_{}.toml", std::process::id()));
        std::fs::write(&path, "").unwrap();
        let cfg = PerfConfig::load(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert!(!cfg.diag, "empty file must default diag=false");
        assert!(!cfg.cut_rules);
        assert!(!cfg.cut_output);
        assert!(cfg.points.is_empty());
        assert_eq!(cfg.point_count(), 0);
        assert!(cfg.point_at(0).is_none());
    }

    #[test]
    fn minimal_diag_config_parses() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("perf_diag_min_{}.toml", std::process::id()));
        std::fs::write(&path, "diag = true\ncut_rules = true\n").unwrap();
        let cfg = PerfConfig::load(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert!(cfg.diag);
        assert!(cfg.cut_rules);
        assert!(!cfg.cut_output, "omitted cut_output must default false");
        assert!(cfg.points.is_empty());
    }

    #[test]
    fn points_list_parses_in_order() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("perf_diag_points_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
diag = true
[[points]]
name = "floor"
cut_rules = true
cut_output = true
rules = ""
[[points]]
name = "rules"
cut_rules = false
cut_output = true
[[points]]
name = "full"
cut_rules = false
cut_output = false
"#,
        )
        .unwrap();
        let cfg = PerfConfig::load(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(cfg.point_count(), 3);
        let floor = cfg.point_at(0).expect("floor point");
        assert_eq!(floor.name, "floor");
        assert!(floor.cut_rules);
        assert!(floor.cut_output);
        assert_eq!(floor.rules.as_deref(), Some(""), "empty rules = keep current");
        let rules = cfg.point_at(1).unwrap();
        assert_eq!(rules.name, "rules");
        assert!(!rules.cut_rules);
        assert!(rules.cut_output);
        assert!(rules.rules.is_none(), "omitted rules must be None");
        let full = cfg.point_at(2).unwrap();
        assert_eq!(full.name, "full");
        assert!(!full.cut_rules);
        assert!(!full.cut_output);
        assert!(cfg.point_at(3).is_none(), "out of range must be None");
    }

    #[test]
    fn rules_subset_path_is_kept() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("perf_diag_rules_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
diag = true
[[points]]
name = "c_family"
rules = "models/rules/c_family.wfl"
"#,
        )
        .unwrap();
        let cfg = PerfConfig::load(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        let point = cfg.point_at(0).unwrap();
        assert_eq!(
            point.rules.as_deref(),
            Some("models/rules/c_family.wfl"),
            "non-empty rules path must be preserved"
        );
    }

    #[test]
    fn round_trips_through_serialize() {
        let cfg = PerfConfig {
            diag: true,
            cut_rules: false,
            cut_output: false,
            points: vec![PerfPoint {
                name: "floor".into(),
                cut_rules: true,
                cut_output: true,
                rules: None,
            }],
        };
        let toml_str = toml::to_string(&cfg).unwrap();
        let parsed: PerfConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed, cfg);
    }

    #[test]
    fn missing_file_is_a_load_error() {
        let path = std::env::temp_dir().join("perf_diag_does_not_exist.toml");
        let err = PerfConfig::load(&path).expect_err("missing file must error");
        assert!(
            err.to_string().contains("perf-diag config"),
            "error should mention the file: {err}"
        );
    }

    #[test]
    fn malformed_toml_is_a_parse_error() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("perf_diag_bad_{}.toml", std::process::id()));
        std::fs::write(&path, "diag = [unclosed").unwrap();
        let err = PerfConfig::load(&path).expect_err("malformed toml must error");
        let _ = std::fs::remove_file(&path);
        assert!(err.to_string().contains("parsing perf-diag config"));
    }
}
