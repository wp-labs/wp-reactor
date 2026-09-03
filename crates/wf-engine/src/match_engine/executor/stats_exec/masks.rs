//! 批级 where mask 共享缓存（2026-08-27 q17 分片去重）：分片下同批掩码
//! 只算一次，其余片 Arc 命中；batch_max_time 并入同缓存。

use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// 批级 where mask 共享缓存（2026-08-27 q17 分片去重）
// ---------------------------------------------------------------------------
//
// 背景: 分片（rule_shards=S）下每片 `process_batch_rows` 对**整批**计算
// where mask（`eval_guard_columnar` 向量化全批）——同一批被 S 片重复 S 次
// （q17 10 片: mask 占 rules 段 CPU ~85%, sample 顶栈 eval_vec 家族第一热点）。
// 首片算完写缓存, 其余片复用（保持向量化, 总量 S×→1×）。
//
// 正确性: mask 是「批 + where 表达式」的纯函数（同一 executor 各片 where 相
// 同）。key = 首列 Arc 指针 + 行数——各片持同一批的**列 Arc 共享**（window
// 批存储 `Arc<RecordBatch>` 浅克隆, `read_since_with_shard` 每片拿值副本但列
// 数组 Arc 同源）; 缓存**强持有批 Arc** → 列指针不会释放复用 → 无碰撞。
//
// 内存: 流式批下同窗口的批几乎同时被各片消费, 缓存持有最近 ~max_rows 行
// （列数据 Arc 共享, 防释放不复制）; 超限整体清空（旧批已处理完, 安全）。

/// 分片共享的批级 where mask 缓存（`Arc<RecordBatch>` 强持有防指针复用）。
///
/// key = (首列 Arc 指针, 行数); value = (批 Arc 防释放, mask 结果)。
/// 2026-08-27 扩展: 同批的 [`batch_max_time`] 也是 10× 分片重复——并入本缓存
/// （同 key 批身份, 复用同一 config 字段, 见 [`StatsMaskCache::get_or_compute_time`]）。
type MaskCacheMap =
    std::collections::HashMap<(usize, usize), (Arc<RecordBatch>, Arc<Vec<BooleanArray>>)>;
/// 批级时间信息缓存表: key = (首列 Arc 指针, 行数), value = (批 Arc, max_time)。
type TimeCacheMap = std::collections::HashMap<(usize, usize), (Arc<RecordBatch>, i64)>;

#[derive(Debug, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct StatsMaskCache {
    inner: std::sync::Mutex<MaskCacheMap>,
    time_inner: std::sync::Mutex<TimeCacheMap>,
    /// 容量上限（总行数; 超限整体清空——流式批下旧批已消费完）。
    /// pub(crate) 供测试缩容验证清理。
    pub(crate) max_rows: usize,
    total_rows: std::sync::atomic::AtomicUsize,
    time_total_rows: std::sync::atomic::AtomicUsize,
}

impl Default for StatsMaskCache {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsMaskCache {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(std::collections::HashMap::new()),
            time_inner: std::sync::Mutex::new(std::collections::HashMap::new()),
            max_rows: 4_000_000,
            total_rows: std::sync::atomic::AtomicUsize::new(0),
            time_total_rows: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// 取或算（算完写缓存）。`compute` = 批 → where mask 列表（仅未命中时调用）。
    pub fn get_or_compute(
        &self,
        batch: &RecordBatch,
        compute: impl FnOnce() -> Vec<BooleanArray>,
    ) -> Arc<Vec<BooleanArray>> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Arc::new(compute()); // 空批不缓存
        }
        let key = (
            std::sync::Arc::as_ptr(batch.column(0)) as *const () as usize,
            rows,
        );
        let mut guard = self.inner.lock().expect("mask cache poisoned");
        if let Some((_, masks)) = guard.get(&key) {
            return Arc::clone(masks);
        }
        let masks = Arc::new(compute());
        // 容量检查在插入前: 超限先清旧批（当前批正被各片消费, 保留）。
        let new_total = self
            .total_rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed)
            + rows;
        if new_total > self.max_rows {
            guard.clear();
            self.total_rows
                .store(rows, std::sync::atomic::Ordering::Relaxed);
        }
        guard.insert(key, (Arc::new(batch.clone()), Arc::clone(&masks)));
        masks
    }

    /// 批级时间信息（batch_max_time）共享缓存（2026-08-27 q17）: 与 mask 同
    /// key（批身份——首列 Arc 指针 + 行数）; 首片扫时间列 max 写缓存, 其余片
    /// 命中（免 10× 全批时间扫描重复）。`compute` = 全批时间 max 求值。
    pub fn get_or_compute_time(&self, batch: &RecordBatch, compute: impl FnOnce() -> i64) -> i64 {
        let rows = batch.num_rows();
        if rows == 0 {
            return compute(); // 空批不缓存
        }
        let key = (
            std::sync::Arc::as_ptr(batch.column(0)) as *const () as usize,
            rows,
        );
        let mut guard = self.time_inner.lock().expect("mask cache poisoned");
        if let Some((_, v)) = guard.get(&key) {
            return *v;
        }
        let v = compute();
        let new_total = self
            .time_total_rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed)
            + rows;
        if new_total > self.max_rows {
            guard.clear();
            self.time_total_rows
                .store(rows, std::sync::atomic::Ordering::Relaxed);
        }
        guard.insert(key, (Arc::new(batch.clone()), v));
        v
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().expect("poisoned").len()
    }

    #[cfg(test)]
    pub(crate) fn time_len(&self) -> usize {
        self.time_inner.lock().expect("poisoned").len()
    }
}
