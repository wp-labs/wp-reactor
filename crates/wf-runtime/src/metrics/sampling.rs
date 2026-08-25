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
                    // Number of batches appended but not yet consumed by any
                    // rule. `max_acked`（完成信号）：round-robin 分片消费者每
                    // 个 shard 只 ack 自己的批次，min_acked 恒停在最慢 shard
                    // 最后一批（2026-08-25 q13 分片卡尾）——完成判定必须用
                    // max。驱逐保护仍用 min_acked（未读不驱逐，见
                    // `WindowProgress::max_acked` 注释）。单/pull 消费者
                    // min==max 等价。Unconsumed windows report
                    // `min_acked = u64::MAX`；max_acked 无消费者 = 0 → lag =
                    // next_seq，非零会误报未排空——无消费者窗口单独按
                    // min_acked==u64::MAX 判已排空（与旧行为一致）。
                    let min_acked = router
                        .registry()
                        .progress(&window_name)
                        .map(|p| p.min_acked())
                        .unwrap_or(u64::MAX);
                    let max_acked = router
                        .registry()
                        .progress(&window_name)
                        .map(|p| p.max_acked())
                        .unwrap_or(0);
                    let consumed = if min_acked == u64::MAX {
                        // 无消费者：trivially drained（与旧 min_acked 口径一致）。
                        u64::MAX
                    } else {
                        max_acked
                    };
                    v.store(win.next_seq().saturating_sub(consumed), Ordering::Relaxed);
                }
            }
        }
    }
}
