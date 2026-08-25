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
                    // 未完全消费的批数（0 = 已排空）。分组完成判定
                    // `completion_gap`（2026-08-25 review）：row-partitioned
                    // （key/行号分片 match/stats）窗口用 min（最慢分片），
                    // whole-batch（round-robin/单消费者）窗口用 max（每批
                    // 归属唯一 shard）。旧 min/max 混合口径会在最快分片处
                    // 提前报 0——哨兵/bench 因此提前排空/提前 SIGTERM，慢分片
                    // 尾部输出被切。驱逐保护仍用 min_acked（未读不驱逐）。
                    let gap = router
                        .registry()
                        .progress(&window_name)
                        .map(|p| p.completion_gap(win.next_seq()))
                        .unwrap_or(0);
                    v.store(gap, Ordering::Relaxed);
                }
            }
        }
    }
}
