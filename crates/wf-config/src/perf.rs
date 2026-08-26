//! 性能诊断模式配置（perf-diag）。
//!
//! 独立于 `wfusion.toml` 的诊断配置文件（`wfusion daemon --perf-diag
//! conf/perf-diag.toml`），引擎与 `wfgen perf-diag` 读同一份。诊断档列表
//! 由 sentinel（漂流瓶）驱动依次应用——见
//! `docs/design/perf-diag-mode-design.md`。

use std::path::Path;

use orion_error::conversion::{SourceErr, SourceRawErr};
use serde::{Deserialize, Serialize};

use crate::{ConfigReason, ConfigResult};

/// 诊断模式配置（`--perf-diag <path>` 加载，不进 `wfusion.toml`）。
///
/// 入口是 `--perf-diag` 启动参数本身（wfgen 侧 `--diag`）——文件只承载诊断档
/// 列表；顶层门控/总开关是历史遗留（实际永远被 `stages[0]` 覆盖或不可达），已删。
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct PerfConfig {
    /// 诊断档列表（sentinel 驱动依次应用）。缺省/空 = 仅初始门控（无切换）。
    #[serde(default)]
    pub stages: Vec<PerfStage>,
}

/// 一个诊断档 = 禁止开关组合 + 可选规则子集文件（触发热 reload）。
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
pub struct PerfStage {
    /// 档名称（墙表输出用，如 `floor` / `rules` / `full`）。
    #[serde(default)]
    pub name: String,
    /// 本档生效期间：禁止规则求值。
    #[serde(default)]
    pub cut_rules: bool,
    /// 本档生效期间：禁止输出链。
    #[serde(default)]
    pub cut_output: bool,
    /// 本档生效期间：禁止窗口 append（解码后即丢, 测「注入 + 解码」前序段;
    /// 哨兵流豁免——测量协议必须活）。
    #[serde(default)]
    pub cut_append: bool,
    /// 本档生效期间：禁止解码（只读帧头 tag 识别哨兵, 非哨兵帧 body 即丢——
    /// 测「注入 + TCP 接收」字节率; 哨兵流豁免）。
    #[serde(default)]
    pub cut_recv: bool,
    /// 本档生效期间：从 **sink 消费侧**切——AlertBatch 到 sink 即丢（不物化、
    /// 不序列化、不写盘）——测「输出构建 + 通道投递」; 增量 full−emit =
    /// 列→行物化 + 序列化 + sink 写成本）。
    ///
    /// ⚠ 与 worker 侧的 `append_*` 指标（record→列构建）区分：这里是 sink 侧。
    #[serde(default)]
    pub cut_sink_write: bool,
    /// 规则子集文件路径（相对 work-dir）。空 = 保持当前规则；非空且与当前
    /// 不同 → 触发既有 `runtime.rules` 热 reload（HotReloadSupported）。
    #[serde(default)]
    pub rules: Option<String>,
}

impl PerfConfig {
    /// 从诊断配置文件加载。文件缺失/解析失败 → 报错（显式 `--perf-diag` 即
    /// 要求诊断模式可用，静默降级会掩盖配置错误）。
    pub fn load(path: &Path) -> ConfigResult<Self> {
        let content = std::fs::read_to_string(path).source_err(
            ConfigReason::Load,
            format!("reading perf-diag config {}", path.display()),
        )?;
        let config: PerfConfig = toml::from_str(&content).source_raw_err(
            ConfigReason::Parse,
            format!("parsing perf-diag config {}", path.display()),
        )?;
        Ok(config)
    }

    /// 诊断档数量（0 = 单档模式）。
    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    /// 第 `index` 个诊断档；越界返回 `None`。
    pub fn stage_at(&self, index: usize) -> Option<&PerfStage> {
        self.stages.get(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_has_no_stages() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("perf_diag_empty_{}.toml", std::process::id()));
        std::fs::write(&path, "").unwrap();
        let cfg = PerfConfig::load(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert!(cfg.stages.is_empty());
        assert_eq!(cfg.stage_count(), 0);
        assert!(cfg.stage_at(0).is_none());
    }

    #[test]
    fn legacy_top_level_fields_are_ignored() {
        // 顶层 diag/cut_rules/cut_output 是历史遗留：反序列化静默忽略。
        let dir = std::env::temp_dir();
        let path = dir.join(format!("perf_diag_legacy_{}.toml", std::process::id()));
        std::fs::write(&path, "diag = true\ncut_rules = true\n").unwrap();
        let cfg = PerfConfig::load(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert!(cfg.stages.is_empty(), "顶层字段不产生诊断档");
    }

    #[test]
    fn stages_list_parses_in_order() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("perf_diag_stages_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
[[stages]]
name = "floor"
cut_rules = true
cut_output = true
rules = ""
[[stages]]
name = "rules"
cut_rules = false
cut_output = true
[[stages]]
name = "full"
cut_rules = false
cut_output = false
"#,
        )
        .unwrap();
        let cfg = PerfConfig::load(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(cfg.stage_count(), 3);
        let floor = cfg.stage_at(0).expect("floor stage");
        assert_eq!(floor.name, "floor");
        assert!(floor.cut_rules);
        assert!(floor.cut_output);
        assert_eq!(
            floor.rules.as_deref(),
            Some(""),
            "empty rules = keep current"
        );
        let rules = cfg.stage_at(1).unwrap();
        assert_eq!(rules.name, "rules");
        assert!(!rules.cut_rules);
        assert!(rules.cut_output);
        assert!(rules.rules.is_none(), "omitted rules must be None");
        let full = cfg.stage_at(2).unwrap();
        assert_eq!(full.name, "full");
        assert!(!full.cut_rules);
        assert!(!full.cut_output);
        assert!(cfg.stage_at(3).is_none(), "out of range must be None");
    }

    #[test]
    fn rules_subset_path_is_kept() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("perf_diag_rules_{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
[[stages]]
name = "c_family"
rules = "models/rules/c_family.wfl"
"#,
        )
        .unwrap();
        let cfg = PerfConfig::load(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        let stage = cfg.stage_at(0).unwrap();
        assert_eq!(
            stage.rules.as_deref(),
            Some("models/rules/c_family.wfl"),
            "non-empty rules path must be preserved"
        );
    }

    #[test]
    fn round_trips_through_serialize() {
        let cfg = PerfConfig {
            stages: vec![PerfStage {
                name: "floor".into(),
                cut_rules: true,
                cut_output: true,
                cut_append: false,
                cut_recv: false,
                cut_sink_write: false,
                rules: None,
            }],
        };
        let toml_str = toml::to_string(&cfg).unwrap();
        let parsed: PerfConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed, cfg);

        // cut_append（decode 档）round-trip。
        let decode = PerfConfig {
            stages: vec![PerfStage {
                name: "decode".into(),
                cut_rules: false,
                cut_output: false,
                cut_append: true,
                cut_recv: false,
                cut_sink_write: false,
                rules: None,
            }],
        };
        let toml_str = toml::to_string(&decode).unwrap();
        let parsed: PerfConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed, decode);
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
