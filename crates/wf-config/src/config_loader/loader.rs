use std::path::{Path, PathBuf};

use crate::vars::{
    ConfigVarContext, SourceAtom, collect_active_external_sources, expand_value_with_sources,
    external_value_with_source, render_source_label, resolve_value_vars_with_sources,
};
use orion_error::conversion::{ConvErr, SourceErr, SourceRawErr};
use toml::Value as TomlValue;

mod overlay_paths;
mod raw_tree;

pub use raw_tree::{RawFusionConfigChange, RawFusionConfigTree};

use crate::config_loader::fusion::FusionConfig;
use crate::vars::{inject_loader_scoped_vars, render_scoped_var_source_label};
use crate::{ConfigReason, ConfigResult};
use overlay_paths::rebase_overlay_paths;

#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq, Eq)]
#[jumo(kind = "struct", domain = "Config", module = "Config.ConfigLoader")]
pub struct ResolvedConfigVar {
    pub key: String,
    pub value: String,
    pub source: String,
}

#[derive(::jumo_derive::Jumo)]
#[jumo(kind = "struct", domain = "Config", module = "Config.ConfigLoader")]
pub struct FusionConfigLoader<'a> {
    base_path: &'a Path,
    overlay_paths: &'a [PathBuf],
    ctx: &'a ConfigVarContext,
    work_dir: Option<&'a Path>,
}

impl<'a> FusionConfigLoader<'a> {
    pub fn new(
        base_path: &'a Path,
        overlay_paths: &'a [PathBuf],
        ctx: &'a ConfigVarContext,
        work_dir: Option<&'a Path>,
    ) -> Self {
        Self {
            base_path,
            overlay_paths,
            ctx,
            work_dir,
        }
    }

    pub fn load(&self) -> ConfigResult<FusionConfig> {
        let raw = self.load_raw()?;
        FusionConfig::from_value_with_context(
            raw.value(),
            self.ctx,
            Some(self.base_path),
            self.work_dir,
        )
    }

    pub fn load_raw(&self) -> ConfigResult<RawFusionConfigTree> {
        let mut merged = read_toml_file(self.base_path)?;
        let Some(base_parent) = self.base_path.parent() else {
            return ConfigReason::Path.fail("base config path must have a parent directory");
        };
        let base_dir = canonicalize_existing_dir(base_parent)?;
        let target_base_dir = match self.work_dir {
            Some(base_dir) => canonicalize_existing_dir(base_dir)?,
            None => base_dir,
        };

        for overlay_path in self.overlay_paths {
            let Some(overlay_parent) = overlay_path.parent() else {
                return ConfigReason::Path.fail("overlay path must have a parent directory");
            };
            let overlay_dir = canonicalize_existing_dir(overlay_parent)?;
            let mut overlay = read_toml_file(overlay_path)?;
            rebase_overlay_paths(&mut overlay.value, &overlay_dir, &target_base_dir);
            overlay.refresh_origins(overlay_path);
            merged.merge_overlay(overlay);
        }

        Ok(merged)
    }

    pub fn load_merged_toml(&self) -> ConfigResult<String> {
        self.load_raw()?.to_toml_string()
    }

    pub fn load_expanded_toml(&self) -> ConfigResult<String> {
        let raw = self.load_raw()?;
        let effective_work_dir = self.work_dir.or_else(|| self.base_path.parent());
        let scoped = inject_loader_scoped_vars(raw.value(), self.base_path, effective_work_dir);
        let expanded_value = crate::vars::expand_value(&scoped, self.ctx).conv_err()?;
        let expanded = toml::to_string(&expanded_value)
            .source_raw_err(ConfigReason::Parse, "serialize expanded config")?;
        let _ = FusionConfig::from_toml_with_context(&expanded, self.ctx)?;
        Ok(expanded)
    }

    pub fn load_expanded_raw(&self) -> ConfigResult<RawFusionConfigTree> {
        let raw = self.load_raw()?;
        let effective_work_dir = self.work_dir.or_else(|| self.base_path.parent());
        let scoped = inject_loader_scoped_vars(raw.value(), self.base_path, effective_work_dir);
        let expanded_with_sources = expand_value_with_sources(&scoped, self.ctx, |path| {
            raw.origin_for(path).map(Path::to_path_buf)
        })
        .conv_err()?;
        let value = expanded_with_sources.value;
        let expanded = toml::to_string(&value)
            .source_raw_err(ConfigReason::Parse, "serialize expanded config")?;
        let _ = FusionConfig::from_toml_with_context(&expanded, self.ctx)?;
        let mut origins = raw.origins().clone();
        for (path, source_set) in expanded_with_sources.sources {
            origins.insert(path, PathBuf::from(render_source_label(&source_set)));
        }
        Ok(RawFusionConfigTree::from_parts(value, origins))
    }

    pub fn load_effective_vars(&self) -> ConfigResult<Vec<ResolvedConfigVar>> {
        let raw = self.load_raw()?;
        let effective_work_dir = self.work_dir.or_else(|| self.base_path.parent());
        let scoped = inject_loader_scoped_vars(raw.value(), self.base_path, effective_work_dir);
        let mut effective_vars = resolve_value_vars_with_sources(&scoped, self.ctx, |path| {
            raw.origin_for(path).map(Path::to_path_buf)
        })
        .conv_err()?;

        for source in
            collect_active_external_sources(&scoped, &effective_vars, self.ctx).conv_err()?
        {
            let ident = match source {
                SourceAtom::Explicit(ident) | SourceAtom::Env(ident) => ident,
                SourceAtom::Default(_) | SourceAtom::File(_) => continue,
            };
            if effective_vars.contains_key(&ident) {
                continue;
            }
            if let Some(value) = external_value_with_source(&ident, self.ctx) {
                effective_vars.insert(ident, value);
            }
        }

        let mut entries = Vec::with_capacity(effective_vars.len());
        for (key, value) in effective_vars {
            let source = if value.sources.is_empty() {
                render_scoped_var_source_label(&key)
                    .unwrap_or_else(|| render_source_label(&value.sources))
            } else {
                render_source_label(&value.sources)
            };
            entries.push(ResolvedConfigVar {
                key,
                value: value.value,
                source,
            });
        }
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(entries)
    }
}

fn canonicalize_existing_dir(path: &Path) -> ConfigResult<PathBuf> {
    path.canonicalize()
        .source_err(ConfigReason::Path, format!("resolve {}", path.display()))
}

fn read_toml_file(path: &Path) -> ConfigResult<RawFusionConfigTree> {
    let content = std::fs::read_to_string(path)
        .source_err(ConfigReason::Load, format!("read {}", path.display()))?;
    let value = parse_toml_table(&content, path)?;
    Ok(RawFusionConfigTree::new(value, path))
}

fn parse_toml_table(content: &str, path: &Path) -> ConfigResult<TomlValue> {
    let value: TomlValue = toml::from_str(content)
        .source_raw_err(ConfigReason::Parse, format!("parse {}", path.display()))?;
    if !value.is_table() {
        return ConfigReason::Parse.fail(format!(
            "fusion config {} must be a TOML table",
            path.display()
        ));
    }
    Ok(value)
}

#[cfg(test)]
#[path = "loader_tests.rs"]
mod loader_tests;
