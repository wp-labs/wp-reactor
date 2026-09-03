use std::sync::Arc;

use wildmatch::WildMatch;
use wp_model_core::model::DataRecord;

use super::runtime::SinkRuntime;

// ---------------------------------------------------------------------------
// SinkDispatcher — core routing engine
// ---------------------------------------------------------------------------

/// Routes alert records to appropriate sinks based on yield-target window name.
///
/// Business routes are compiled from wildcard patterns at startup and matched
/// against the yield-target window name when records are dispatched.
///
/// Routing logic:
/// 1. Match `window_name` against configured business route patterns.
/// 2. If found (and non-empty), send to those sinks.
/// 3. Otherwise, send to the `default_sinks` (if configured).
/// 4. If any send fails, additionally send to `error_sinks` (if configured).
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SinkDispatch")]
pub struct SinkDispatcher {
    /// Compiled business routes.
    routes: Vec<SinkRouteBinding>,
    /// Fallback sinks when no route matches
    default_sinks: Vec<Arc<SinkRuntime>>,
    /// Error-escalation sinks (sent to on any send failure)
    error_sinks: Vec<Arc<SinkRuntime>>,
    /// Monitor sinks (metrics/telemetry, always dispatched)
    monitor_sinks: Vec<Arc<SinkRuntime>>,
    /// All unique SinkRuntime instances (for stop_all)
    all_sinks: Vec<Arc<SinkRuntime>>,
}

#[derive(Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SinkDispatch")]
struct WildmatchMatcher {
    patterns: Vec<WildMatch>,
}

impl WildmatchMatcher {
    fn new(patterns: &[String]) -> Self {
        Self {
            patterns: patterns
                .iter()
                .map(|pattern| WildMatch::new(pattern))
                .collect(),
        }
    }

    fn matches(&self, value: &str) -> bool {
        self.patterns.iter().any(|pattern| pattern.matches(value))
    }
}

impl std::fmt::Debug for WildmatchMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WildmatchMatcher")
            .field("patterns", &self.patterns.len())
            .finish()
    }
}

#[derive(Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.SinkDispatch")]
struct SinkRouteBinding {
    matcher: WildmatchMatcher,
    sinks: Vec<Arc<SinkRuntime>>,
}

impl SinkRouteBinding {
    fn new(patterns: &[String], sinks: Vec<Arc<SinkRuntime>>) -> Self {
        Self {
            matcher: WildmatchMatcher::new(patterns),
            sinks,
        }
    }

    fn matches(&self, window_name: &str) -> bool {
        !self.sinks.is_empty() && self.matcher.matches(window_name)
    }
}

impl std::fmt::Debug for SinkRouteBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkRouteBinding")
            .field("matcher", &self.matcher)
            .field("sinks", &self.sinks.len())
            .finish()
    }
}

impl SinkDispatcher {
    /// Create a new dispatcher from business route patterns and infra sinks.
    pub fn new(
        routes: Vec<(Vec<String>, Vec<Arc<SinkRuntime>>)>,
        default_sinks: Vec<Arc<SinkRuntime>>,
        error_sinks: Vec<Arc<SinkRuntime>>,
        monitor_sinks: Vec<Arc<SinkRuntime>>,
    ) -> Self {
        let routes: Vec<SinkRouteBinding> = routes
            .into_iter()
            .map(|(patterns, sinks)| SinkRouteBinding::new(&patterns, sinks))
            .collect();

        // Collect all unique SinkRuntime instances by Arc pointer identity.
        let mut seen = std::collections::HashSet::new();
        let mut all_sinks = Vec::new();

        let iter = routes
            .iter()
            .flat_map(|route| route.sinks.iter())
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
    /// Returns `(matched, had_error)` where `matched` is 1 if a business route
    /// was matched (0 if only default sinks were used), and `had_error` is true if
    /// any sink send failed.
    pub async fn dispatch(&self, window_name: &str, alert_record: &DataRecord) -> (usize, bool) {
        let mut matched = false;
        let mut had_error = false;

        for route in &self.routes {
            if route.matches(window_name) {
                matched = true;
                for sink in &route.sinks {
                    if let Err(e) = sink.send_record(alert_record).await {
                        log::warn!("sink dispatch error: {e}");
                        had_error = true;
                    }
                }
            }
        }

        if !matched {
            for sink in &self.default_sinks {
                if let Err(e) = sink.send_record(alert_record).await {
                    log::warn!("sink dispatch error: {e}");
                    had_error = true;
                }
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

        (usize::from(matched), had_error)
    }

    /// Check if any monitor sinks are configured.
    pub fn has_monitor_sinks(&self) -> bool {
        !self.monitor_sinks.is_empty()
    }

    /// Resolve the sinks a yield target routes to — business routes that match,
    /// falling back to the default sinks when none match. Deduplicated by sink
    /// identity. Pure routing (no send); the delivery layer maps these to
    /// per-sink channels.
    pub fn resolve_sinks(&self, window_name: &str) -> Vec<Arc<SinkRuntime>> {
        let mut matched = false;
        let mut sinks: Vec<Arc<SinkRuntime>> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for route in &self.routes {
            if route.matches(window_name) {
                matched = true;
                for sink in &route.sinks {
                    let ptr = Arc::as_ptr(sink) as usize;
                    if seen.insert(ptr) {
                        sinks.push(Arc::clone(sink));
                    }
                }
            }
        }
        if !matched {
            for sink in &self.default_sinks {
                let ptr = Arc::as_ptr(sink) as usize;
                if seen.insert(ptr) {
                    sinks.push(Arc::clone(sink));
                }
            }
        }
        sinks
    }

    /// Error-escalation sinks (sent to on any regular-sink send failure).
    pub fn error_sinks(&self) -> &[Arc<SinkRuntime>] {
        &self.error_sinks
    }

    /// Monitor sinks (metrics telemetry, a separate delivery path).
    pub fn monitor_sinks(&self) -> &[Arc<SinkRuntime>] {
        &self.monitor_sinks
    }

    /// All unique sink runtimes (for building per-sink delivery channels).
    pub fn all_sinks(&self) -> &[Arc<SinkRuntime>] {
        &self.all_sinks
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

    /// Gracefully stop only the monitor sinks (metrics telemetry).
    pub async fn stop_monitor_sinks(&self) {
        for sink in &self.monitor_sinks {
            if let Err(e) = sink.stop().await {
                log::warn!("monitor sink stop error: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn patterns(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn wildmatch_matcher_supports_star_and_question_patterns() {
        let matcher = WildmatchMatcher::new(&patterns(&["security_*", "auth_?"]));

        assert!(matcher.matches("security_alerts"));
        assert!(matcher.matches("security_"));
        assert!(matcher.matches("auth_a"));
        assert!(!matcher.matches("auth_ab"));
        assert!(!matcher.matches("network_alerts"));
    }
}
