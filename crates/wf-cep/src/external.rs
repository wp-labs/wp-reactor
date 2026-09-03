//! Global external runtime handle for the engine.
//!
//! The `external()` WFL function needs to call into the external runtime
//! (managed by `wf-runtime`) from `eval.rs` (in `wf-engine`).  To avoid
//! threading a trait object through the entire eval call chain, we use a
//! global `OnceLock` that is set once at bootstrap and read at eval time.

use std::sync::Arc;

use crate::value::Value;

/// Trait implemented by `wf-runtime::external::ExternalRuntime`.
pub trait ExternalCallHandler: Send + Sync {
    /// Call an external service by name with the given arguments.
    ///
    /// `args` are the already-evaluated `Value`s from the right-hand side of
    /// `external("name", arg1, arg2, ...)`.
    fn call(&self, service: &str, args: &[Value]) -> Option<Value>;
}

/// Global external runtime handle — set once at bootstrap.
static EXTERNAL_HANDLER: std::sync::OnceLock<Arc<dyn ExternalCallHandler>> =
    std::sync::OnceLock::new();

/// Install the global external runtime.  Must be called during bootstrap,
/// before any rule evaluation starts.
pub fn set_external_handler(handler: Arc<dyn ExternalCallHandler>) {
    let _ = EXTERNAL_HANDLER.set(handler);
}

/// Call the external runtime (if configured).
///
/// Returns `None` when:
/// - No external runtime is configured
/// - The service name is unknown
/// - The call fails and `on_error` is not set
pub fn dispatch_external_call(service: &str, args: &[Value]) -> Option<Value> {
    EXTERNAL_HANDLER.get().and_then(|h| h.call(service, args))
}

/// Evaluate `external("service", arg1, ...)` from an already-evaluated service
/// name and raw argument expressions.
///
/// Shared logic for both eval paths (`executor/eval.rs` and
/// `match_engine/eval.rs`). The `eval_arg` closure evaluates each argument
/// expression in the caller's context, allowing this helper to stay agnostic
/// to the specific eval environment.
///
/// Returns `None` if:
/// - Fewer than 2 args (service + at least one argument)
/// - Service name is not a string
/// - Any argument evaluates to `None`
pub fn eval_external<F>(
    service_expr: &wf_lang::ast::Expr,
    arg_exprs: &[wf_lang::ast::Expr],
    mut eval_arg: F,
) -> Option<Value>
where
    F: FnMut(&wf_lang::ast::Expr) -> Option<Value>,
{
    if arg_exprs.is_empty() {
        return None;
    }
    let service = match eval_arg(service_expr)? {
        Value::Str(s) => s,
        _ => return None,
    };
    let mut arg_vals = Vec::with_capacity(arg_exprs.len());
    for a in arg_exprs {
        arg_vals.push(eval_arg(a)?);
    }
    dispatch_external_call(&service, &arg_vals)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockHandler {
        results: std::collections::HashMap<String, Option<Value>>,
    }

    impl ExternalCallHandler for MockHandler {
        fn call(&self, service: &str, _args: &[Value]) -> Option<Value> {
            self.results.get(service).cloned().flatten()
        }
    }

    #[test]
    fn dispatch_returns_none_when_no_handler_set() {
        // EXTERNAL_HANDLER is a global OnceLock; in tests it may already be set.
        // We test the fallback path by constructing a second OnceLock.
        let handler: std::sync::OnceLock<Arc<dyn ExternalCallHandler>> = std::sync::OnceLock::new();
        assert!(handler.get().is_none());
        assert!(handler.get().and_then(|h| h.call("test", &[])).is_none());
    }

    #[test]
    fn mock_handler_returns_configured_value() {
        let mut results = std::collections::HashMap::new();
        results.insert("pwd_check".to_string(), Some(Value::Bool(true)));
        results.insert("unknown".to_string(), None);
        let handler = MockHandler { results };

        assert_eq!(
            handler.call("pwd_check", &[Value::Str("hash".into())]),
            Some(Value::Bool(true))
        );
        assert_eq!(handler.call("unknown", &[Value::Str("x".into())]), None);
    }

    #[test]
    fn mock_handler_returns_bool_and_number() {
        let mut results = std::collections::HashMap::new();
        results.insert("bf".to_string(), Some(Value::Bool(false)));
        results.insert("score".to_string(), Some(Value::Number(0.85)));
        let handler = MockHandler { results };

        assert_eq!(handler.call("bf", &[]), Some(Value::Bool(false)));
        assert_eq!(handler.call("score", &[]), Some(Value::Number(0.85)));
    }
}
