//! ExternalRuntime — bridges WFL `external()` calls to wp_knowledge facade.
//!
//! Service definitions live entirely in `knowdb.toml` `[fun.<name>]`.
//! wfusion.toml needs no `[external]` section.
//!
//! On Redis errors:
//! - `external_exists` returns `Some(Value::Bool(false))` (safe default —
//!   fail-closed for existence checks,宁可漏报不可误报)
//! - `external_value` returns `None` (no value available)
//!
//! P0 limitation: only the first argument is forwarded to the backend.
//! Multi-arg calls like `external("threat_actor", ip, "confidence")`
//! are accepted but only `ip` is used. P1 will forward all args.

use std::sync::Arc;

use wf_engine::external::ExternalCallHandler;
use wf_engine::match_engine::Value;

use super::redis_backend::RedisBackend;

#[derive(Default)]
pub struct ExternalRuntime {
    redis: RedisBackend,
}

impl ExternalRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    /// Forward to wp_knowledge facade.  `service` maps to `[fun.<name>]`.
    ///
    /// Dispatch order: try `external_exists` (bool) first; if the service
    /// is defined as a value query (hget/get), it will return an error and
    /// we fall through to `external_value`.
    pub fn call(&self, service: &str, args: &[Value]) -> Option<Value> {
        let arg = value_to_str(args.first()?)?;
        match self.redis.call_bool(service, &arg) {
            // Exists query succeeded → return the bool result (true or false).
            Ok(Some(v)) => return Some(v),
            // Exists query succeeded but returned false → Bool(false).
            // This is the normal "not in set/bloom" path, not an error.
            Ok(None) => return Some(Value::Bool(false)),
            // Service is not a bool query (e.g. hget/get) or Redis error.
            // Fall through to value query.
            Err(_) => {}
        }
        match self.redis.call_value(service, &arg) {
            Ok(v) => v,
            // Redis error on value query → None (design §6.1).
            Err(e) => {
                wf_warn!(
                    pipe,
                    service = service,
                    error = %e,
                    "external() value query failed"
                );
                None
            }
        }
    }

    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }
}

impl ExternalCallHandler for ExternalRuntime {
    fn call(&self, service: &str, args: &[Value]) -> Option<Value> {
        ExternalRuntime::call(self, service, args)
    }
}

fn value_to_str(v: &Value) -> Option<String> {
    match v {
        Value::Str(s) => Some(s.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_to_str_conversions() {
        assert_eq!(
            value_to_str(&Value::Str("hash".into())),
            Some("hash".into())
        );
        assert_eq!(value_to_str(&Value::Number(42.0)), Some("42".into()));
        assert_eq!(value_to_str(&Value::Bool(true)), None); // not supported
        assert_eq!(value_to_str(&Value::Number(0.0)), Some("0".into()));
    }

    #[test]
    fn call_returns_none_for_empty_args() {
        let rt = ExternalRuntime::default();
        assert_eq!(rt.call("test", &[]), None);
    }

    #[test]
    fn call_returns_none_for_bool_arg() {
        let rt = ExternalRuntime::default();
        // Bool arg can't be converted to string
        assert_eq!(rt.call("test", &[Value::Bool(true)]), None);
    }

    #[test]
    fn default_creates_runtime() {
        let rt = ExternalRuntime::default();
        let arc = Arc::new(rt);
        // verify ExternalCallHandler impl works
        let result = arc.call("nonexistent", &[Value::Str("x".into())]);
        assert!(result.is_none());
    }
}
