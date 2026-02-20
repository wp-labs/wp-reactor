/// Domain-aware logging macros.
///
/// Each macro injects a `domain` field automatically so callers never need to
/// remember the string literal.  The domain value is one of the five defined in
/// `docs/design/logging-spec.md`: `sys`, `conn`, `pipe`, `res`, `conf`.
///
/// # Usage
///
/// ```ignore
/// use crate::log_macros::*;
///
/// wf_info!(sys, schemas = 1, rules = 2, "engine bootstrap complete");
/// wf_warn!(pipe, error = %e, "execute_match error");
/// wf_debug!(conn, peer = %peer, "accepted connection");
/// ```
///
/// The macros accept any tracing-compatible field syntax after the domain
/// identifier.  The domain identifier is **not** a string — it is a bare
/// identifier that the macro converts to a `&str` literal.

// ---------------------------------------------------------------------------
// Core macro — dispatches to the matching tracing level macro.
// ---------------------------------------------------------------------------

/// Internal helper.  Do not call directly; use `wf_error!` … `wf_trace!`.
#[doc(hidden)]
macro_rules! wf_log {
    // With fields and message
    ($level:ident, $domain:ident, $($field:tt)*) => {
        tracing::$level!(domain = stringify!($domain), $($field)*)
    };
}

// ---------------------------------------------------------------------------
// Public per-level macros
// ---------------------------------------------------------------------------

/// Log at ERROR level with an automatic `domain` field.
///
/// ```ignore
/// wf_error!(pipe, error = %e, "alert sink write failed");
/// ```
macro_rules! wf_error {
    ($domain:ident, $($rest:tt)*) => {
        wf_log!(error, $domain, $($rest)*)
    };
}

/// Log at WARN level with an automatic `domain` field.
///
/// ```ignore
/// wf_warn!(pipe, error = %e, timeout = ?dur, "engine timed out");
/// ```
macro_rules! wf_warn {
    ($domain:ident, $($rest:tt)*) => {
        wf_log!(warn, $domain, $($rest)*)
    };
}

/// Log at INFO level with an automatic `domain` field.
///
/// ```ignore
/// wf_info!(sys, listen = %addr, "engine started");
/// ```
macro_rules! wf_info {
    ($domain:ident, $($rest:tt)*) => {
        wf_log!(info, $domain, $($rest)*)
    };
}

/// Log at DEBUG level with an automatic `domain` field.
///
/// ```ignore
/// wf_debug!(conn, peer = %peer, "accepted connection");
/// ```
macro_rules! wf_debug {
    ($domain:ident, $($rest:tt)*) => {
        wf_log!(debug, $domain, $($rest)*)
    };
}

/// Log at TRACE level with an automatic `domain` field.
///
/// ```ignore
/// wf_trace!(pipe, stream = name, rows = batch.num_rows(), "frame decoded");
/// ```
#[allow(unused_macros)]
macro_rules! wf_trace {
    ($domain:ident, $($rest:tt)*) => {
        wf_log!(trace, $domain, $($rest)*)
    };
}
