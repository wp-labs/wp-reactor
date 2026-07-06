use std::collections::HashMap;
use std::sync::Arc;

use wp_model_core::model::DataRecord;

use super::runtime::SinkRuntime;

// ---------------------------------------------------------------------------
// SinkDispatcher — core routing engine (pre-bound at startup)
// ---------------------------------------------------------------------------

/// Routes alert records to appropriate sinks based on yield-target window name.
///
/// Window→sink bindings are pre-resolved at startup via a `HashMap` lookup,
/// eliminating runtime wildcard matching on every dispatch call.
///
/// Routing logic:
/// 1. Look up `window_name` in the pre-bound `routes` map.
/// 2. If found (and non-empty), send to those sinks.
/// 3. Otherwise, send to the `default_sinks` (if configured).
/// 4. If any send fails, additionally send to `error_sinks` (if configured).
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SinkDispatch")]
pub struct SinkDispatcher {
    /// Pre-resolved routing: window_name → bound sinks
    routes: HashMap<String, Vec<Arc<SinkRuntime>>>,
    /// Fallback sinks when no route matches
    default_sinks: Vec<Arc<SinkRuntime>>,
    /// Error-escalation sinks (sent to on any send failure)
    error_sinks: Vec<Arc<SinkRuntime>>,
    /// Monitor sinks (metrics/telemetry, always dispatched)
    monitor_sinks: Vec<Arc<SinkRuntime>>,
    /// All unique SinkRuntime instances (for stop_all)
    all_sinks: Vec<Arc<SinkRuntime>>,
}

impl SinkDispatcher {
    /// Create a new dispatcher from pre-resolved routes and infra sinks.
    pub fn new(
        routes: HashMap<String, Vec<Arc<SinkRuntime>>>,
        default_sinks: Vec<Arc<SinkRuntime>>,
        error_sinks: Vec<Arc<SinkRuntime>>,
        monitor_sinks: Vec<Arc<SinkRuntime>>,
    ) -> Self {
        // Collect all unique SinkRuntime instances by Arc pointer identity.
        let mut seen = std::collections::HashSet::new();
        let mut all_sinks = Vec::new();

        let iter = routes
            .values()
            .flatten()
            .chain(default_sinks.iter())
            .chain(error_sinks.iter())
            .chain(monitor_sinks.iter());

        for sink in iter {
            let ptr = Arc::as_ptr(sink) as usize;
            if seen.insert(ptr) {
                all_sinks.push(Arc::clone(sink));
            }
        }

        Self {
            routes,
            default_sinks,
            error_sinks,
            monitor_sinks,
            all_sinks,
        }
    }

    /// Route alert records to matching sinks by yield-target window name.
    ///
    /// Returns `(matched, had_error)` where `matched` is 1 if a pre-bound route
    /// was found (0 if only default sinks were used), and `had_error` is true if
    /// any sink send failed.
    pub async fn dispatch(&self, window_name: &str, alert_record: &DataRecord) -> (usize, bool) {
        let (sinks, matched) = match self.routes.get(window_name) {
            Some(s) if !s.is_empty() => (s.as_slice(), 1),
            _ => (self.default_sinks.as_slice(), 0),
        };

        let mut had_error = false;
        for sink in sinks {
            if let Err(e) = sink.send_record(alert_record).await {
                log::warn!("sink dispatch error: {e}");
                had_error = true;
            }
        }

        // Any error → error sinks
        if had_error {
            for sink in &self.error_sinks {
                if let Err(e) = sink.send_record(alert_record).await {
                    log::warn!("error sink error: {e}");
                }
            }
        }

        (matched, had_error)
    }

    /// Check if any monitor sinks are configured.
    pub fn has_monitor_sinks(&self) -> bool {
        !self.monitor_sinks.is_empty()
    }

    /// Returns true when there are no default sinks to fall back to.
    /// Combined with a route miss (`dispatch` returning 0), this means the
    /// alert had nowhere to go.
    pub fn has_no_default_sinks(&self) -> bool {
        self.default_sinks.is_empty()
    }

    /// Route metrics records to all monitor sinks (fan-out, no window routing).
    pub async fn dispatch_to_monitor(&self, record: &DataRecord) {
        for sink in &self.monitor_sinks {
            if let Err(e) = sink.send_record(record).await {
                log::warn!("monitor sink error: {e}");
            }
        }
    }

    /// Gracefully stop all unique sinks.
    pub async fn stop_all(&self) {
        for sink in &self.all_sinks {
            if let Err(e) = sink.stop().await {
                log::warn!("sink stop error: {e}");
            }
        }
    }
}
