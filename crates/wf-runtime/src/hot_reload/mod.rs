use std::collections::HashSet;

use wf_config::{FusionConfig, FusionReloadPlan, RawFusionConfigTree, WindowConfig};
use wf_lang::WindowSchema;

use crate::lifecycle::types::RunRule;

mod compile;
mod prepare;
#[cfg(test)]
mod tests;
mod topology;

// Re-exports for external consumers
pub use prepare::{prepare_reload, prepare_reload_with_cached};

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
