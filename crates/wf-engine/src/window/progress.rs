use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};

/// Consumption progress for one window, shared between rule tasks and the
/// evictor.
///
/// Each consuming task (push worker or pull-path rule task) registers one
/// slot at spawn time and acks `batch_seq + 1` after it has fully processed
/// that batch. The evictor uses [`WindowProgress::min_acked`] as a floor:
/// time-based eviction may only remove batches every live consumer has
/// acknowledged, so a slow rule can never lose unconsumed data to a sweep.
///
/// Consumers come in two groups with different **completion** semantics (see
/// [`WindowProgress::completion_gap`]): whole-batch consumers
/// ([`WindowProgress::register`] — single worker / round-robin shards, each
/// batch owned by exactly one slot) and row-partitioned consumers
/// ([`WindowProgress::register_row_partitioned`] — key / row-index shards,
/// a batch complete only when every shard acked it).
///
/// Slots are held as [`Weak`] handles: a task going away drops its last
/// strong reference and the slot stops counting automatically — hot reload
/// replaces rule tasks wholesale, and strong references would accumulate
/// dead slots (each only pinned to `u64::MAX`) on every reload cycle.
/// Dead entries are swept lazily on `register`/`min_acked` (amortized O(1)
/// per call, no background task).
///
/// Windows with no live consumers report `u64::MAX` (everything is
/// evictable by time). [`WindowProgress::release`] exists as an explicit
/// belt-and-braces for graceful shutdown paths that want the slot
/// deactivated before the task struct itself drops.
///
/// Time eviction ([`crate::window::Window::evict_expired`]) and per-window /
/// global memory eviction all respect this floor: a batch a live consumer has
/// not yet acked is never dropped, so a slow pull rule cannot lose unread
/// data. When nothing is safe to drop under memory pressure, the actor parks
/// (backpressure) instead.
/// ## Retention pins (D4)
///
/// Consumer slots only cover **pull** consumption: a rule that *reads* a window
/// as its source. A rule that uses a window as a **join target** never pulls
/// from it — it only does point lookups — so it owns no slot, and
/// [`WindowProgress::min_acked`] reports `u64::MAX` ("nobody reads this,
/// everything is evictable"). Memory eviction was therefore free to drop rows a
/// deferred join still needed, silently truncating its output: nexmark q9/q4a at
/// 30M lost 17,180,418 of 27.6M bid rows to the `bid_events` byte cap and
/// emitted 62% fewer rows than the oracle (2026-08-24).
///
/// A **retention pin** closes that hole from the other side: a join-target
/// reader publishes the oldest **event time** it can still need, and memory
/// eviction refuses to drop a batch that may hold such rows — the window
/// transiently exceeds its byte budget (and the evictor reports memory pressure
/// → backpressure) instead of losing data, exactly as it already does for
/// unacked pull batches.
///
/// Pins are event-time based rather than seq based because that is what the
/// reader knows: a deferred join needs rows in `[lo_ns, hi_ns]` per pending
/// instance, so its frontier is `min(lo_ns)` over live pendings — no
/// event-time-to-seq mapping needed. `i64::MAX` = "nothing pinned".
///
/// Time eviction (`over`) deliberately ignores pins: `over` is the retention the
/// query *declares*, so dropping rows past it is semantics, not resource
/// pressure (see [`crate::window::Window::evict_expired`]).
pub struct WindowProgress {
    /// Whole-batch consumers: a batch is fully consumed by exactly one such
    /// slot (single worker, or round-robin shards — each batch is delivered to
    /// its owning shard only, so `max` over this group is the completion
    /// signal).
    slots: RwLock<Vec<Weak<AtomicU64>>>,
    /// Row-partitioned consumers (key / row-index sharded match & stats): each
    /// shard processes only its row subset of every batch, so a batch is fully
    /// consumed only when **every** slot in this group has acked past it —
    /// `min` over this group is the completion signal (2026-08-25 review:
    /// `wait_for_data_drain`'s `max||min` fired on the fastest shard of a
    /// key-partitioned window while slower shards were still processing).
    row_slots: RwLock<Vec<Weak<AtomicU64>>>,
    /// Retention pins: oldest event time (nanos) each join-target reader still
    /// needs. Held as [`Weak`] for the same reason as `slots`.
    pins: RwLock<Vec<Weak<AtomicI64>>>,
}

impl Default for WindowProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowProgress {
    pub fn new() -> Self {
        Self {
            slots: RwLock::new(Vec::new()),
            row_slots: RwLock::new(Vec::new()),
            pins: RwLock::new(Vec::new()),
        }
    }

    /// Register a new consumer slot, starting at 0 (nothing acked). Sweeps
    /// slots whose consumers have gone away while holding the write lock.
    pub fn register(&self) -> Arc<AtomicU64> {
        let slot = Arc::new(AtomicU64::new(0));
        let mut slots = self.slots.write().expect("progress lock poisoned");
        // Amortized sweep: drop Weak entries whose consumers are gone so the
        // table does not grow across reload cycles.
        slots.retain(|w| w.strong_count() > 0);
        slots.push(Arc::downgrade(&slot));
        slot
    }

    /// Register a **row-partitioned** consumer slot (key / row-index sharded
    /// match & stats tasks). Each shard only ever processes its own row subset
    /// of every batch, so the batch is complete only when every slot in this
    /// group has acked past it — completion for this group is the **min**,
    /// never the max (a fast shard reaching `next_seq` says nothing about the
    /// slow ones). Eviction still respects the global min over both groups
    /// (unread rows are never dropped).
    ///
    /// Whole-batch consumers (single worker / round-robin shards) use
    /// [`Self::register`] instead.
    pub fn register_row_partitioned(&self) -> Arc<AtomicU64> {
        let slot = Arc::new(AtomicU64::new(0));
        let mut row_slots = self.row_slots.write().expect("progress lock poisoned");
        row_slots.retain(|w| w.strong_count() > 0);
        row_slots.push(Arc::downgrade(&slot));
        slot
    }

    /// Minimum acked position over all live consumers (both groups).
    ///
    /// `u64::MAX` when no consumer is alive: a window nobody reads is
    /// fully evictable by time.
    pub fn min_acked(&self) -> u64 {
        let slots = self.slots.read().expect("progress lock poisoned");
        let row_slots = self.row_slots.read().expect("progress lock poisoned");
        slots
            .iter()
            .chain(row_slots.iter())
            .filter_map(|w| w.upgrade())
            .map(|slot| slot.load(Ordering::Acquire))
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Maximum acked position over live consumers, **skipping released**
    /// (`u64::MAX`) slots — a released consumer must not fabricate a
    /// completion signal while live shards are still draining (hot reload /
    /// partial task exit). `0` when no live (unreleased) consumer: no
    /// consumption progress to report. (The drain criterion itself lives in
    /// [`Self::completion_gap`], which is group-aware — `max_acked` is only a
    /// raw aggregate for diagnostics.)
    pub fn max_acked(&self) -> u64 {
        let slots = self.slots.read().expect("progress lock poisoned");
        let row_slots = self.row_slots.read().expect("progress lock poisoned");
        slots
            .iter()
            .chain(row_slots.iter())
            .filter_map(|w| w.upgrade())
            .map(|slot| slot.load(Ordering::Acquire))
            .filter(|v| *v != u64::MAX)
            .max()
            .unwrap_or(0)
    }

    /// How many batches (at `next_seq`) are still **not fully consumed** — the
    /// completion criterion used by the sentinel drain and the `acked_lag`
    /// metric. `0` = drained.
    ///
    /// Completion is **per consumption group**, because no single aggregate
    /// works for every window shape (2026-08-25 review):
    /// - whole-batch consumers (`slots`, single / round-robin): each batch is
    ///   owned by exactly one slot → `max` over the group;
    /// - row-partitioned consumers (`row_slots`, key / index shards): a batch
    ///   is complete only when every shard acked past it → `min` over the
    ///   group.
    ///
    /// An empty group is trivially complete (no such consumer). Released slots
    /// (`u64::MAX`) are skipped in both groups — a released consumer is gone,
    /// it neither completes nor holds back anything.
    pub fn completion_gap(&self, next_seq: u64) -> u64 {
        let slots = self.slots.read().expect("progress lock poisoned");
        let row_slots = self.row_slots.read().expect("progress lock poisoned");
        let batch_max = slots
            .iter()
            .filter_map(|w| w.upgrade())
            .map(|slot| slot.load(Ordering::Acquire))
            .filter(|v| *v != u64::MAX)
            .max();
        let row_min = row_slots
            .iter()
            .filter_map(|w| w.upgrade())
            .map(|slot| slot.load(Ordering::Acquire))
            .filter(|v| *v != u64::MAX)
            .min();
        let batch_gap = batch_max.map(|m| next_seq.saturating_sub(m)).unwrap_or(0);
        let row_gap = row_min.map(|m| next_seq.saturating_sub(m)).unwrap_or(0);
        batch_gap.max(row_gap)
    }

    /// Register a retention pin (D4), starting **fully pinned** (`i64::MIN`).
    ///
    /// Fail-safe initial value: a reader that has just registered has not
    /// published a frontier yet, so the only safe assumption is "every buffered
    /// row may still be needed". Starting at `i64::MAX` instead leaves a startup
    /// hole — nexmark q9 30M still lost 3.3% of its output to 31 eviction sweeps
    /// in the first 2.6s, before the rule task had processed its first driving
    /// batch (2026-08-24).
    ///
    /// The reader must therefore publish its real frontier as it advances (and
    /// `i64::MAX` when idle / at EOS), otherwise the window can never reclaim
    /// memory. The caller keeps the only strong reference: dropping it (task
    /// shutdown, hot reload) releases the pin automatically.
    pub fn register_retention_pin(&self) -> Arc<AtomicI64> {
        let pin = Arc::new(AtomicI64::new(i64::MIN));
        let mut pins = self.pins.write().expect("progress lock poisoned");
        pins.retain(|w| w.strong_count() > 0);
        pins.push(Arc::downgrade(&pin));
        pin
    }

    /// Oldest event time (nanos) any live retention pin still needs.
    ///
    /// `i64::MAX` when nothing is pinned — memory eviction then behaves exactly
    /// as it did before pins existed.
    pub fn min_retention_ns(&self) -> i64 {
        self.pins
            .read()
            .expect("progress lock poisoned")
            .iter()
            .filter_map(|w| w.upgrade())
            .map(|pin| pin.load(Ordering::Acquire))
            .min()
            .unwrap_or(i64::MAX)
    }

    /// Detach a consumer slot by marking it fully consumed.
    ///
    /// Optional: the slot also stops counting once the task drops its last
    /// strong reference (the table holds only a `Weak`). Kept for shutdown
    /// paths that release explicitly before the task struct goes away.
    ///
    /// Safe against the completion signal: released slots (`u64::MAX`) are
    /// skipped by [`Self::completion_gap`] and [`Self::max_acked`], so a
    /// releasing task can never make a window look drained while live shards
    /// are still processing, and never holds the eviction floor either.
    pub fn release(slot: &Arc<AtomicU64>) {
        slot.fetch_max(u64::MAX, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_acked_is_max_without_consumers() {
        let progress = WindowProgress::new();
        assert_eq!(progress.min_acked(), u64::MAX);
    }

    #[test]
    fn min_acked_tracks_the_slowest_consumer() {
        let progress = WindowProgress::new();
        let a = progress.register();
        let b = progress.register();

        a.store(10, Ordering::Release);
        b.store(4, Ordering::Release);
        assert_eq!(progress.min_acked(), 4);

        b.store(20, Ordering::Release);
        assert_eq!(progress.min_acked(), 10);
    }

    /// 完成信号：round-robin 分片消费者每个 shard 只 ack 自己的批次，
    /// `min_acked` 恒停在最慢 shard 最后一批（q13 分片卡尾），完成判定必须
    /// 用 `max_acked`（每个批次都被其归属消费者消费）。驱逐保护仍用 min。
    #[test]
    fn max_acked_tracks_completion_across_shards() {
        let progress = WindowProgress::new();
        // 无消费者：min=MAX（全部可驱逐），max=0（无消费进度）
        assert_eq!(progress.max_acked(), 0);

        // 10 个 round-robin shard，各 ack 自己份额的最后一批（如 q13b：
        // next_seq=255，最慢 shard 最后一批 245 → min=246）
        let mut slots = Vec::new();
        for i in 0..10u64 {
            let slot = progress.register();
            slot.store(246 + i, Ordering::Release);
            slots.push(slot); // 持有强引用（Weak 表只在强引用存活时计数）
        }
        assert_eq!(progress.min_acked(), 246, "min = 最慢 shard 最后一批+1");
        assert_eq!(progress.max_acked(), 255, "max = 最快 shard 追平 next_seq");

        // 全部追平：min == max == next_seq
        let all = progress.register();
        all.store(256, Ordering::Release);
        slots.push(all);
        assert_eq!(progress.max_acked(), 256);
    }

    #[test]
    fn released_slot_no_longer_pins() {
        let progress = WindowProgress::new();
        let a = progress.register();
        a.store(3, Ordering::Release);
        assert_eq!(progress.min_acked(), 3);

        WindowProgress::release(&a);
        assert_eq!(progress.min_acked(), u64::MAX);
    }

    /// 完成判定是**分组**的（2026-08-25 review）：key/index 分片（row-partitioned）
    /// 窗口用 min——最快 shard 追平 next_seq 不代表最慢 shard 处理完；
    /// round-robin（whole-batch）窗口用 max——每批恰被其归属 shard 消费，
    /// min 恒停在最慢 shard。
    #[test]
    fn completion_gap_waits_for_the_slowest_row_shard() {
        let progress = WindowProgress::new();
        let fast = progress.register_row_partitioned();
        let slow = progress.register_row_partitioned();
        fast.store(4, Ordering::Release);
        slow.store(1, Ordering::Release);
        assert_eq!(
            progress.completion_gap(4),
            3,
            "最快 shard 已追平 next_seq=4，但最慢 shard 还在 1 → 未排空"
        );
        slow.store(4, Ordering::Release);
        assert_eq!(progress.completion_gap(4), 0, "全部 row shard 追平 → 排空");
    }

    #[test]
    fn completion_gap_uses_max_for_round_robin_shards() {
        let progress = WindowProgress::new();
        // 10 个 round-robin shard，最慢停在 245（min=246），最快已处理最后一批
        //（seq=254 → ack 255）→ next_seq=255 时已排空（每批都被其归属 shard 消费）。
        let mut slots = Vec::new();
        for i in 0..10u64 {
            let slot = progress.register();
            slot.store(246 + i, Ordering::Release);
            slots.push(slot);
        }
        assert_eq!(progress.min_acked(), 246);
        assert_eq!(
            progress.completion_gap(255),
            0,
            "round-robin：min 停滞不影响完成判定（max=255 追平）"
        );
        assert_eq!(progress.completion_gap(300), 45, "未追平 → 剩余批数");
    }

    /// 混合消费（同一窗口既有 key 分片 match/stats 又有 round-robin 消费者，如
    /// bid_events = q5 key 分片 + q4a round-robin）：两组的完成条件**都必须**满足。
    #[test]
    fn completion_gap_mixed_requires_both_groups() {
        let progress = WindowProgress::new();
        let row_a = progress.register_row_partitioned();
        let row_b = progress.register_row_partitioned();
        let rr_a = progress.register();
        let rr_b = progress.register();
        row_a.store(5, Ordering::Release);
        row_b.store(5, Ordering::Release);
        rr_a.store(3, Ordering::Release);
        rr_b.store(4, Ordering::Release);
        assert_eq!(
            progress.completion_gap(5),
            1,
            "row 组已追平，但 round-robin 组 max=4 < next=5（最后一批 seq=4 未处理）→ 未排空"
        );
        rr_a.store(5, Ordering::Release);
        assert_eq!(progress.completion_gap(5), 0, "两组都满足 → 排空");
        // row 组落后时同理：
        row_a.store(2, Ordering::Release);
        assert_eq!(progress.completion_gap(5), 3, "row 组最慢在 2 → 未排空");
    }

    /// release（u64::MAX）不能伪造完成信号，也不能拖住驱逐 floor：
    /// 存活 shard 未追平时 completion_gap 必须反映它们，而不是被 release 冲成 0。
    #[test]
    fn released_slot_does_not_poison_completion() {
        let progress = WindowProgress::new();
        let gone = progress.register();
        let live = progress.register();
        gone.store(5, Ordering::Release);
        live.store(7, Ordering::Release);
        assert_eq!(progress.completion_gap(10), 3);

        WindowProgress::release(&gone);
        assert_eq!(
            progress.completion_gap(10),
            3,
            "release 的槽被跳过：completion_gap 由存活槽决定"
        );
        assert_eq!(progress.max_acked(), 7, "max_acked 跳过已释放槽");
        assert_eq!(progress.min_acked(), 7, "release 槽不拖驱逐 floor");

        // 全部释放 → 无存活消费者 → 视为已排空。
        WindowProgress::release(&live);
        assert_eq!(progress.completion_gap(10), 0);
        assert_eq!(progress.max_acked(), 0, "无存活消费者 → 无消费进度");
        assert_eq!(progress.min_acked(), u64::MAX);
    }

    /// Row-partitioned 槽的驱逐保护与 whole-batch 槽合并计算（min over both）。
    #[test]
    fn min_acked_covers_both_consumer_groups() {
        let progress = WindowProgress::new();
        let batch = progress.register();
        let row = progress.register_row_partitioned();
        batch.store(8, Ordering::Release);
        row.store(2, Ordering::Release);
        assert_eq!(progress.min_acked(), 2, "驱逐 floor = 两组最慢");
        assert_eq!(progress.completion_gap(8), 6);
    }

    /// Reload cycles must not accumulate slots: registering N consumers,
    /// dropping them all, and registering one more leaves exactly one live
    /// slot in the table (dead Weak entries swept on register).
    #[test]
    fn dead_slots_do_not_accumulate_across_reload_cycles() {
        let progress = WindowProgress::new();
        for _ in 0..100 {
            let slot = progress.register();
            slot.store(7, Ordering::Release);
            drop(slot);
        }
        let live = progress.register();
        live.store(5, Ordering::Release);

        let table_len = progress.slots.read().unwrap().len();
        assert_eq!(
            table_len, 1,
            "sweep on register must drop dead entries, got {table_len}"
        );
        assert_eq!(progress.min_acked(), 5);
    }

    /// A dropped consumer stops gating eviction even without `release`:
    /// the Weak table must not see it anymore.
    #[test]
    fn dropped_consumer_stops_counting_without_release() {
        let progress = WindowProgress::new();
        let slow = progress.register();
        slow.store(1, Ordering::Release);
        let fast = progress.register();
        fast.store(42, Ordering::Release);
        assert_eq!(progress.min_acked(), 1);

        drop(slow);
        assert_eq!(
            progress.min_acked(),
            42,
            "dropped consumer's slot must not hold the floor"
        );
    }

    /// D4 retention pins are independent of consumer slots: a window with **no**
    /// consumers (`min_acked == u64::MAX`, everything evictable) can still be
    /// pinned by a join-target reader. `min` over live pins wins, and a dropped
    /// reader releases its pin automatically (Weak table) — otherwise a hot
    /// reload would pin a window forever.
    #[test]
    fn retention_pins_are_independent_of_consumer_slots() {
        let progress = WindowProgress::new();
        assert_eq!(
            progress.min_retention_ns(),
            i64::MAX,
            "no pins = nothing held"
        );

        let a = progress.register_retention_pin();
        let b = progress.register_retention_pin();
        // Consumers say "everything evictable"; pins must still be honoured.
        assert_eq!(progress.min_acked(), u64::MAX);
        assert_eq!(
            progress.min_retention_ns(),
            i64::MIN,
            "刚注册的 pin 必须先全保留（fail-safe，否则有启动空档）"
        );

        a.store(500, Ordering::Release);
        b.store(120, Ordering::Release);
        assert_eq!(progress.min_retention_ns(), 120, "最早的前沿胜出");

        // Explicit release (EOS): the reader no longer needs anything.
        b.store(i64::MAX, Ordering::Release);
        assert_eq!(progress.min_retention_ns(), 500);

        // Dropped reader (task shutdown / hot reload) stops pinning.
        drop(a);
        assert_eq!(
            progress.min_retention_ns(),
            i64::MAX,
            "dropped reader must not pin the window forever"
        );
    }
}
