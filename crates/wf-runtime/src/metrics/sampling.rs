use std::sync::atomic::Ordering;

use crate::metrics::RuntimeMetrics;
use wf_engine::window::Router;

/// 计量槽写入（槽未注册 → 无操作）——采样各处 if-let 收敛为单点。
fn store_gauge(gauge: Option<&std::sync::atomic::AtomicU64>, v: u64) {
    if let Some(g) = gauge {
        g.store(v, Ordering::Relaxed);
    }
}

impl RuntimeMetrics {
    /// Periodically sample expensive window gauges to keep scrape path light.
    pub fn sample_windows(&self, router: &Router) {
        for window_name in router.registry().window_names() {
            let Some(win) = router.registry().get_window(&window_name) else {
                continue;
            };
            // 会计保真度：实际分配字节（含 null bitmap/offsets/容量舍入）。
            store_gauge(
                self.window_memory_bytes.get(&window_name),
                win.memory_usage() as u64,
            );
            store_gauge(
                self.window_allocated_bytes.get(&window_name),
                win.allocated_usage() as u64,
            );
            store_gauge(
                self.window_capacity_bytes.get(&window_name),
                win.max_window_bytes() as u64,
            );
            store_gauge(self.window_rows.get(&window_name), win.total_rows() as u64);
            store_gauge(
                self.window_batches.get(&window_name),
                win.batch_count() as u64,
            );
            // 输出链在途量：fanout 通道排队批数/容量（2026-08-26）。
            if let Some((q, cap)) = router.fanout().queued_items(&window_name) {
                store_gauge(self.window_fanout_queued.get(&window_name), q as u64);
                store_gauge(self.window_fanout_capacity.get(&window_name), cap as u64);
            }
            // 在途量分账（2026-08-25）：该窗 mailbox 已用预算/容量。无 mailbox
            // （同步模式 / 未注册）则保持 0。
            if let Some((used, cap)) = router.mailbox_inflight(&window_name) {
                store_gauge(self.window_mailbox_inflight.get(&window_name), used as u64);
                store_gauge(self.window_mailbox_budget.get(&window_name), cap as u64);
            }
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
            store_gauge(self.window_acked_lag.get(&window_name), gap);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_gauge_writes_and_ignores_absent_slot() {
        let g = std::sync::atomic::AtomicU64::new(0);
        store_gauge(Some(&g), 42);
        assert_eq!(g.load(Ordering::Relaxed), 42);
        // 未注册槽（None）→ 无操作不 panic
        store_gauge(None, 7);
        assert_eq!(g.load(Ordering::Relaxed), 42);
    }
}
