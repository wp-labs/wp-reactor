//! Redis backend for the external function runtime.
//!
//! Delegates to `wp_knowledge::facade::external_exists` / `external_value`,
//! which route via `knowdb.toml` `[fun.<name>]` definitions.  Connection,
//! timeouts, and caching are all managed by wp_knowledge.

use wf_engine::match_engine::Value;

#[derive(Default, ::moju_derive::MoJu)]
#[moju(
    kind = "struct",
    domain = "Runtime",
    module = "Runtime.ExternalRuntime"
)]
pub(crate) struct RedisBackend;

impl RedisBackend {
    pub(crate) fn call_bool(&self, service: &str, arg: &str) -> Result<Option<Value>, String> {
        wp_knowledge::facade::external_exists(service, arg)
            .map(|v| Some(Value::Bool(v)))
            .map_err(|e| format!("external_exists '{}': {}", service, e))
    }

    pub(crate) fn call_value(&self, service: &str, arg: &str) -> Result<Option<Value>, String> {
        wp_knowledge::facade::external_value(service, arg)
            .map(|v| v.map(|s| Value::Str(s.into())))
            .map_err(|e| format!("external_value '{}': {}", service, e))
    }
}
