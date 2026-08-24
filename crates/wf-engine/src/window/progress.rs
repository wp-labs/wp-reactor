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
    slots: RwLock<Vec<Weak<AtomicU64>>>,
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

    /// Minimum acked position over all live consumers.
    ///
    /// `u64::MAX` when no consumer is alive: a window nobody reads is
    /// fully evictable by time.
    pub fn min_acked(&self) -> u64 {
        self.slots
            .read()
            .expect("progress lock poisoned")
            .iter()
            .filter_map(|w| w.upgrade())
            .map(|slot| slot.load(Ordering::Acquire))
            .min()
            .unwrap_or(u64::MAX)
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

    #[test]
    fn released_slot_no_longer_pins() {
        let progress = WindowProgress::new();
        let a = progress.register();
        a.store(3, Ordering::Release);
        assert_eq!(progress.min_acked(), 3);

        WindowProgress::release(&a);
        assert_eq!(progress.min_acked(), u64::MAX);
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
