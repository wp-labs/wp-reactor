use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Consumption progress for one window, shared between rule tasks and the
/// evictor.
///
/// Each consuming task (push worker or pull-path rule task) registers one
/// slot at spawn time and acks `batch_seq + 1` after it has fully processed
/// that batch. The evictor uses [`WindowProgress::min_acked`] as a floor:
/// time-based eviction may only remove batches every live consumer has
/// acknowledged, so a slow rule can never lose unconsumed data to a sweep.
///
/// Windows with no registered consumers report `u64::MAX` (everything is
/// evictable by time). A task going away must release its slot as "done"
/// (see [`WindowProgress::release`]); `RuleTask`'s Drop does this so a
/// shutdown task cannot pin window memory forever.
///
/// Memory-pressure eviction (`Evictor` phase 2) deliberately ignores this
/// floor — it is the explicit lossy backstop when the global byte cap is
/// exceeded.
pub struct WindowProgress {
    slots: RwLock<Vec<Arc<AtomicU64>>>,
}

impl WindowProgress {
    pub fn new() -> Self {
        Self {
            slots: RwLock::new(Vec::new()),
        }
    }

    /// Register a new consumer slot, starting at 0 (nothing acked).
    pub fn register(&self) -> Arc<AtomicU64> {
        let slot = Arc::new(AtomicU64::new(0));
        self.slots.write().expect("progress lock poisoned").push(Arc::clone(&slot));
        slot
    }

    /// Minimum acked position over all live consumers.
    ///
    /// `u64::MAX` when no consumer is registered: a window nobody reads is
    /// fully evictable by time.
    pub fn min_acked(&self) -> u64 {
        self.slots
            .read()
            .expect("progress lock poisoned")
            .iter()
            .map(|slot| slot.load(Ordering::Acquire))
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Detach a consumer slot by marking it fully consumed.
    ///
    /// Called when a rule task goes away (Drop); the slot is kept in the
    /// table (cheap) but no longer holds the floor down.
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
}
