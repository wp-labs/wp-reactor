use std::sync::atomic::Ordering;

use crate::metrics::RuntimeMetrics;
use wf_engine::window::Router;

impl RuntimeMetrics {
    /// 记录 parse pool 预读预算的已用/容量字节（在途量分账，2026-08-25）。
    ///
    /// 由持有 `PrereadBudget` 的 bootstrap 装入 provider（metrics 任务本身拿不到
    /// 预算句柄），之后每次 `sample_windows` 读一次。q13 的
    /// `peak_commit − Σwindow_bytes` 长期有 ~14.7GB 未归因，而所有"猜持有者"的
    /// 假说已逐一被实测否决——本 gauge 把 parse 阶段在途字节变成可对账项
    /// （预算默认 128MiB，nexmark bench 配 2GB）。幂等：只生效一次。
    pub fn set_parse_inflight_provider<F>(&self, provider: F)
    where
        F: Fn() -> (usize, usize) + Send + Sync + 'static,
    {
        let _ = self.parse_inflight_provider.set(Box::new(provider));
    }

    /// Periodically sample expensive window gauges to keep scrape path light.
    pub fn sample_windows(&self, router: &Router) {
        if let Some(provider) = self.parse_inflight_provider.get() {
            let (used, cap) = provider();
            self.parse_inflight_bytes
                .store(used as u64, Ordering::Relaxed);
            self.parse_budget_bytes.store(cap as u64, Ordering::Relaxed);
        }
        for window_name in router.registry().window_names() {
            if let Some(win) = router.registry().get_window(&window_name) {
                if let Some(v) = self.window_memory_bytes.get(&window_name) {
                    v.store(win.memory_usage() as u64, Ordering::Relaxed);
                }
                // 会计保真度：实际分配字节（含 null bitmap/offsets/容量舍入）。
                if let Some(v) = self.window_allocated_bytes.get(&window_name) {
                    v.store(win.allocated_usage() as u64, Ordering::Relaxed);
                }
                // 在途量分账（2026-08-25）：该窗 mailbox 已用预算/容量。无 mailbox
                // （同步模式 / 未注册）则保持 0。
                if let Some((used, cap)) = router.mailbox_inflight(&window_name) {
                    if let Some(v) = self.window_mailbox_inflight.get(&window_name) {
                        v.store(used as u64, Ordering::Relaxed);
                    }
                    if let Some(v) = self.window_mailbox_budget.get(&window_name) {
                        v.store(cap as u64, Ordering::Relaxed);
                    }
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
