use std::sync::atomic::{AtomicU64, Ordering};
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
/// Memory-pressure eviction (`Evictor` phase 2) deliberately ignores this
/// floor — it is the explicit lossy backstop when the global byte cap is
/// exceeded.
pub struct WindowProgress {
    slots: RwLock<Vec<Weak<AtomicU64>>>,
}

impl WindowProgress {
    pub fn new() -> Self {
        Self {
            slots: RwLock::new(Vec::new()),
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
}
