use std::collections::HashMap;
use std::path::{Path, PathBuf};

use orion_error::conversion::SourceRawErr;
use toml::Value as TomlValue;

use crate::{ConfigReason, ConfigResult};

#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigLoader")]
pub struct RawFusionConfigTree {
    pub(crate) value: TomlValue,
    pub(crate) origins: HashMap<String, PathBuf>,
}

#[derive(::moju_derive::MoJu, Debug, Clone)]
#[moju(kind = "struct", domain = "Config", module = "Config.ConfigLoader")]
pub struct RawFusionConfigChange {
    pub path: String,
    pub old_value: Option<TomlValue>,
    pub new_value: Option<TomlValue>,
    pub old_origin: Option<PathBuf>,
    pub new_origin: Option<PathBuf>,
}

impl RawFusionConfigTree {
    pub fn new(value: TomlValue, source_path: &Path) -> Self {
        let mut origins = HashMap::new();
        record_origins(&value, source_path, None, &mut origins);
        Self { value, origins }
    }

    pub(crate) fn from_parts(value: TomlValue, origins: HashMap<String, PathBuf>) -> Self {
        Self { value, origins }
    }

    /// Parse a raw config tree directly from a TOML string, recording every
    /// field's origin as `source_path`. Handy for embedders/tests that build a
    /// config inline (no file on disk) but still need a reload baseline - it
    /// sidesteps the consumer needing to depend on the same `toml` version as
    /// this crate.
    pub fn from_toml_str(toml_str: &str, source_path: &Path) -> ConfigResult<Self> {
        let value: TomlValue = toml::from_str(toml_str)
            .source_raw_err(ConfigReason::Parse, "parse inline raw config TOML")?;
        Ok(Self::new(value, source_path))
    }

    pub fn value(&self) -> &TomlValue {
        &self.value
    }

    pub fn to_toml_string(&self) -> ConfigResult<String> {
        toml::to_string(&self.value).source_raw_err(ConfigReason::Parse, "serialize raw config")
    }

    pub fn origin_for(&self, path: &str) -> Option<&Path> {
        self.origins.get(path).map(PathBuf::as_path)
    }

    pub fn origin_entries(&self) -> Vec<(String, PathBuf)> {
        let mut entries: Vec<(String, PathBuf)> = self
            .origins
            .iter()
            .map(|(path, origin)| (path.clone(), origin.clone()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    pub(crate) fn origins(&self) -> &HashMap<String, PathBuf> {
        &self.origins
    }

    pub fn diff(&self, next: &RawFusionConfigTree) -> Vec<RawFusionConfigChange> {
        let mut changes = Vec::new();
        diff_value(
            Some(&self.value),
            Some(&next.value),
            "",
            self,
            next,
            &mut changes,
        );
        changes
    }

    pub fn merge_overlay(&mut self, overlay: RawFusionConfigTree) {
        merge_value_with_origins(
            &mut self.value,
            &mut self.origins,
            overlay.value,
            &overlay.origins,
            None,
        );
    }

    pub(crate) fn refresh_origins(&mut self, source_path: &Path) {
        self.origins.clear();
        record_origins(&self.value, source_path, None, &mut self.origins);
    }

    /// 替换 `runtime.rules` 值（perf-diag 规则子集热切用）。
    ///
    /// 在原始 TOML 树上的 `runtime.rules` 叶节点写入新 glob；路径在重载时
    /// 相对 base_dir 解析（与 `FusionConfig::load` 一致）。无 `runtime` 表则
    /// 创建（缺省 parse/rule 并行度等仍走 serde 默认）。
    pub fn set_runtime_rules(&mut self, rules: &str) {
        let runtime = match self
            .value
            .as_table_mut()
            .and_then(|root| root.get_mut("runtime"))
        {
            Some(toml::Value::Table(table)) => table,
            _ => {
                self.value
                    .as_table_mut()
                    .expect("raw tree root is a table")
                    .insert(
                        "runtime".to_string(),
                        toml::Value::Table(Default::default()),
                    );
                match self
                    .value
                    .as_table_mut()
                    .and_then(|root| root.get_mut("runtime"))
                {
                    Some(toml::Value::Table(table)) => table,
                    _ => return,
                }
            }
        };
        runtime.insert("rules".to_string(), toml::Value::String(rules.to_string()));
        // 该叶子的 origin 已不可信（改写于内存），移除以免 reload diff 被旧
        // origin 误导（diff 只比较值，origin 仅供展示）。
        self.origins.remove("runtime.rules");
    }
}

fn merge_value_with_origins(
    base: &mut TomlValue,
    base_origins: &mut HashMap<String, PathBuf>,
    overlay: TomlValue,
    overlay_origins: &HashMap<String, PathBuf>,
    path: Option<&str>,
) {
    match (base, overlay) {
        (TomlValue::Table(base_table), TomlValue::Table(overlay_table)) => {
            for (key, overlay_value) in overlay_table {
                let child_path = join_path(path, &key);
                match base_table.get_mut(&key) {
                    Some(base_value) => merge_value_with_origins(
                        base_value,
                        base_origins,
                        overlay_value,
                        overlay_origins,
                        Some(&child_path),
                    ),
                    None => {
                        base_table.insert(key, overlay_value);
                        copy_origin_subtree(base_origins, overlay_origins, Some(&child_path));
                    }
                }
            }
        }
        (base_slot, overlay_value) => {
            clear_origin_subtree(base_origins, path);
            *base_slot = overlay_value;
            copy_origin_subtree(base_origins, overlay_origins, path);
        }
    }
}

fn record_origins(
    value: &TomlValue,
    source_path: &Path,
    path: Option<&str>,
    origins: &mut HashMap<String, PathBuf>,
) {
    if let Some(path) = path {
        origins.insert(path.to_string(), source_path.to_path_buf());
    }
    match value {
        TomlValue::Table(table) => {
            for (key, child) in table {
                let child_path = join_path(path, key);
                record_origins(child, source_path, Some(&child_path), origins);
            }
        }
        TomlValue::Array(items) => {
            for (idx, child) in items.iter().enumerate() {
                let child_path = join_indexed_path(path, idx);
                record_origins(child, source_path, Some(&child_path), origins);
            }
        }
        _ => {}
    }
}

fn join_path(parent: Option<&str>, key: &str) -> String {
    match parent {
        Some(parent) if !parent.is_empty() => format!("{parent}.{key}"),
        _ => key.to_string(),
    }
}

fn join_indexed_path(parent: Option<&str>, idx: usize) -> String {
    match parent {
        Some(parent) if !parent.is_empty() => format!("{parent}[{idx}]"),
        _ => format!("[{idx}]"),
    }
}

fn clear_origin_subtree(origins: &mut HashMap<String, PathBuf>, path: Option<&str>) {
    let Some(path) = path else {
        origins.clear();
        return;
    };
    origins.retain(|key, _| !path_matches_or_descends(key, path));
}

fn copy_origin_subtree(
    dst: &mut HashMap<String, PathBuf>,
    src: &HashMap<String, PathBuf>,
    path: Option<&str>,
) {
    match path {
        Some(path) => {
            for (key, origin) in src {
                if path_matches_or_descends(key, path) {
                    dst.insert(key.clone(), origin.clone());
                }
            }
        }
        None => {
            for (key, origin) in src {
                dst.insert(key.clone(), origin.clone());
            }
        }
    }
}

fn path_matches_or_descends(key: &str, path: &str) -> bool {
    key == path
        || key
            .strip_prefix(path)
            .is_some_and(|rest| rest.starts_with('.') || rest.starts_with('['))
}

fn diff_value(
    old_value: Option<&TomlValue>,
    new_value: Option<&TomlValue>,
    path: &str,
    old_tree: &RawFusionConfigTree,
    new_tree: &RawFusionConfigTree,
    changes: &mut Vec<RawFusionConfigChange>,
) {
    match (old_value, new_value) {
        (Some(TomlValue::Table(old_table)), Some(TomlValue::Table(new_table))) => {
            let mut keys: Vec<&str> = old_table
                .keys()
                .map(String::as_str)
                .chain(new_table.keys().map(String::as_str))
                .collect();
            keys.sort_unstable();
            keys.dedup();
            for key in keys {
                let child_path = if path.is_empty() {
                    key.to_string()
                } else {
                    format!("{path}.{key}")
                };
                diff_value(
                    old_table.get(key),
                    new_table.get(key),
                    &child_path,
                    old_tree,
                    new_tree,
                    changes,
                );
            }
        }
        (Some(TomlValue::Array(old_items)), Some(TomlValue::Array(new_items))) => {
            if old_items != new_items {
                changes.push(change_at(path, old_value, new_value, old_tree, new_tree));
            }
        }
        (Some(old_leaf), Some(new_leaf)) => {
            if old_leaf != new_leaf {
                changes.push(change_at(path, old_value, new_value, old_tree, new_tree));
            }
        }
        (None, Some(_)) | (Some(_), None) => {
            changes.push(change_at(path, old_value, new_value, old_tree, new_tree));
        }
        (None, None) => {}
    }
}

fn change_at(
    path: &str,
    old_value: Option<&TomlValue>,
    new_value: Option<&TomlValue>,
    old_tree: &RawFusionConfigTree,
    new_tree: &RawFusionConfigTree,
) -> RawFusionConfigChange {
    RawFusionConfigChange {
        path: path.to_string(),
        old_value: old_value.cloned(),
        new_value: new_value.cloned(),
        old_origin: (!path.is_empty())
            .then(|| old_tree.origin_for(path).map(Path::to_path_buf))
            .flatten(),
        new_origin: (!path.is_empty())
            .then(|| new_tree.origin_for(path).map(Path::to_path_buf))
            .flatten(),
    }
}
