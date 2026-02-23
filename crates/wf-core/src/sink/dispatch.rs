use std::sync::Arc;

use wf_config::sink::WildArray;

use super::runtime::SinkRuntime;

// ---------------------------------------------------------------------------
// SinkDispatcher — core routing engine
// ---------------------------------------------------------------------------

/// Routes alert JSON to appropriate sink groups based on yield-target window
/// name matching.
///
/// Routing logic:
/// 1. Iterate business groups — if `group.windows.matches(window_name)`,
///    send to all sinks in that group.
/// 2. If no business group matches, send to the `default_group` (if configured).
/// 3. If any send fails, additionally send to the `error_group` (if configured).
pub struct SinkDispatcher {
    business: Vec<BusinessGroup>,
    default_group: Option<SinkGroup>,
    error_group: Option<SinkGroup>,
}

struct BusinessGroup {
    #[allow(dead_code)]
    name: String,
    windows: WildArray,
    sinks: Vec<Arc<SinkRuntime>>,
}

struct SinkGroup {
    #[allow(dead_code)]
    name: String,
    sinks: Vec<Arc<SinkRuntime>>,
}

impl SinkDispatcher {
    /// Create a new dispatcher from business groups and optional infra groups.
    pub fn new(
        business: Vec<(String, WildArray, Vec<Arc<SinkRuntime>>)>,
        default_group: Option<(String, Vec<Arc<SinkRuntime>>)>,
        error_group: Option<(String, Vec<Arc<SinkRuntime>>)>,
    ) -> Self {
        Self {
            business: business
                .into_iter()
                .map(|(name, windows, sinks)| BusinessGroup {
                    name,
                    windows,
                    sinks,
                })
                .collect(),
            default_group: default_group.map(|(name, sinks)| SinkGroup { name, sinks }),
            error_group: error_group.map(|(name, sinks)| SinkGroup { name, sinks }),
        }
    }

    /// Route alert JSON to matching sink groups by yield-target window name.
    ///
    /// Returns the number of business groups that matched.
    pub async fn dispatch(&self, window_name: &str, alert_json: &str) -> usize {
        let mut matched = 0;
        let mut had_error = false;

        // 1. Try business groups
        for group in &self.business {
            if group.windows.matches(window_name) {
                matched += 1;
                for sink in &group.sinks {
                    if let Err(e) = sink.send_str(alert_json).await {
                        log::warn!("sink dispatch error: {e}");
                        had_error = true;
                    }
                }
            }
        }

        // 2. No business match → default group
        if matched == 0 {
            if let Some(ref default) = self.default_group {
                for sink in &default.sinks {
                    if let Err(e) = sink.send_str(alert_json).await {
                        log::warn!("default sink error: {e}");
                        had_error = true;
                    }
                }
            }
        }

        // 3. Any error → error group
        if had_error {
            if let Some(ref error) = self.error_group {
                for sink in &error.sinks {
                    if let Err(e) = sink.send_str(alert_json).await {
                        log::warn!("error sink error: {e}");
                    }
                }
            }
        }

        matched
    }

    /// Gracefully stop all sinks across all groups.
    pub async fn stop_all(&self) {
        for group in &self.business {
            for sink in &group.sinks {
                if let Err(e) = sink.stop().await {
                    log::warn!("sink stop error: {e}");
                }
            }
        }
        if let Some(ref default) = self.default_group {
            for sink in &default.sinks {
                if let Err(e) = sink.stop().await {
                    log::warn!("default sink stop error: {e}");
                }
            }
        }
        if let Some(ref error) = self.error_group {
            for sink in &error.sinks {
                if let Err(e) = sink.stop().await {
                    log::warn!("error sink stop error: {e}");
                }
            }
        }
    }
}
