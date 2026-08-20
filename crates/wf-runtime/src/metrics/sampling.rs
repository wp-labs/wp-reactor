use std::sync::atomic::Ordering;

use crate::metrics::RuntimeMetrics;
use wf_engine::window::Router;

impl RuntimeMetrics {
    /// Periodically sample expensive window gauges to keep scrape path light.
    pub fn sample_windows(&self, router: &Router) {
        for window_name in router.registry().window_names() {
            if let Some(win) = router.registry().get_window(&window_name) {
                if let Some(v) = self.window_memory_bytes.get(&window_name) {
                    v.store(win.memory_usage() as u64, Ordering::Relaxed);
                }
                if let Some(v) = self.window_capacity_bytes.get(&window_name) {
                    v.store(win.max_window_bytes() as u64, Ordering::Relaxed);
                }
                if let Some(v) = self.window_rows.get(&window_name) {
                    v.store(win.total_rows() as u64, Ordering::Relaxed);
                }
                if let Some(v) = self.window_batches.get(&window_name) {
                    v.store(win.batch_count() as u64, Ordering::Relaxed);
                }
                if let Some(v) = self.window_acked_lag.get(&window_name) {
                    // Number of batches appended but not yet acked by the slowest
                    // live consumer. Unconsumed windows report `min_acked =
                    // u64::MAX`, so `saturating_sub` yields 0 (trivially drained).
                    let min_acked = router
                        .registry()
                        .progress(&window_name)
                        .map(|p| p.min_acked())
                        .unwrap_or(u64::MAX);
                    v.store(win.next_seq().saturating_sub(min_acked), Ordering::Relaxed);
                }
            }
        }
    }
}
