// ---------------------------------------------------------------------------
// SharedLimits — cross-shard rate-limit / budget atomics (P2b)
//
// When a rule is sharded, each shard runs its own CepStateMachine. Without
// shared state, `max_throttle` / `max_instances` / `max_memory_bytes` are
// enforced per-shard, so N shards collectively emit N×limit. `SharedLimits`
// replaces the per-machine fields with atomics shared by every shard of one
// rule, following the existing `Arc<Atomic…>` patterns (RuleFanout round-robin
// cursor, WindowProgress slots, parse_seq).
// ---------------------------------------------------------------------------

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};

use wf_lang::plan::RateSpec;

/// Cross-shard rate-limit and budget state for one rule.
#[derive(Debug, Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct SharedLimits {
    /// Collective emit count within the current throttle window.
    emit_count: AtomicU64,
    /// Shared throttle window start (nanoseconds).
    emit_window_start: AtomicI64,
    /// Delta-mirror of live instances across all shards (for `max_instances`).
    instance_count: AtomicUsize,
    /// Delta-mirror of estimated memory across all shards (for `max_memory_bytes`).
    estimated_memory_bytes: AtomicUsize,
    /// `FailRule` is a rule-wide latch: any shard failing fails the rule.
    failed: AtomicBool,
}

impl SharedLimits {
    /// Create a fresh shared-limits handle (all zeros).
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Best-effort shared sliding-window rate-limit reservation.
    ///
    /// CAS-rotates the shared window start when `now` has advanced past `per`.
    /// All shards of one rule share this counter, so the *collective* emit count
    /// within a window is bounded by `rate.count`. Overshoot is ≤ the number of
    /// shards racing a rotation (a rejected reservation already incremented the
    /// counter, exhausting one slot early). This is a throttle, not an exact
    /// budget — exactness would require a lock on the hot emit path.
    ///
    /// Returns `true` if this reservation is within the window budget.
    pub fn try_acquire_throttle(&self, now_nanos: i64, rate: &RateSpec) -> bool {
        let window = rate.per.as_nanos() as i64;
        loop {
            let start = self.emit_window_start.load(Ordering::Acquire);
            if now_nanos.saturating_sub(start) >= window {
                if self
                    .emit_window_start
                    .compare_exchange(start, now_nanos, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    // This reservation is the first of the new window.
                    self.emit_count.store(1, Ordering::Release);
                    return true;
                }
                // Another shard rotated the window; retry.
                continue;
            }
            let prev = self.emit_count.fetch_add(1, Ordering::Acquire);
            return prev < rate.count;
        }
    }

    /// Try to reserve one instance slot. Exact: fails once at the cap.
    pub fn try_reserve_instance(&self, max: usize) -> bool {
        loop {
            let cur = self.instance_count.load(Ordering::Acquire);
            if cur >= max {
                return false;
            }
            if self
                .instance_count
                .compare_exchange(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Reserve one instance slot (unconditional increment — mirrors a new
    /// instance). Pairs with [`release_instance`](Self::release_instance).
    pub fn add_instance(&self) {
        self.instance_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Release one instance slot.
    pub fn release_instance(&self) {
        self.instance_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Total live instances across all shards.
    pub fn instance_count(&self) -> usize {
        self.instance_count.load(Ordering::Relaxed)
    }

    /// Add an instance's base estimated bytes to the shared budget.
    pub fn add_memory(&self, bytes: usize) {
        self.estimated_memory_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Subtract an instance's base estimated bytes from the shared budget.
    pub fn sub_memory(&self, bytes: usize) {
        self.estimated_memory_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Total estimated memory across all shards.
    pub fn memory_bytes(&self) -> usize {
        self.estimated_memory_bytes.load(Ordering::Relaxed)
    }

    /// Correct the shared memory total by this shard's recalibration delta
    /// (`local_exact - local_before`), since the shared counter is a sum of the
    /// per-shard cached estimates.
    pub fn recalibrate_memory(&self, local_before: usize, local_exact: usize) {
        let delta = local_exact as i128 - local_before as i128;
        if delta == 0 {
            return;
        }
        self.estimated_memory_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                let next = if delta >= 0 {
                    cur.saturating_add(delta as usize)
                } else {
                    cur.saturating_sub((-delta) as usize)
                };
                Some(next)
            })
            .ok();
    }

    /// Latch the rule as failed (`FailRule`).
    pub fn fail(&self) {
        self.failed.store(true, Ordering::Release);
    }

    /// Whether any shard latched the rule as failed.
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn shared_throttle_bounds_collective_emits_across_shards() {
        let shared = SharedLimits::new();
        let rate = RateSpec {
            count: 3,
            per: Duration::from_secs(60),
        };
        let t0 = 1_000_000_000i64;
        // Two "shards" drain the same shared budget.
        assert!(shared.try_acquire_throttle(t0, &rate));
        assert!(shared.try_acquire_throttle(t0, &rate));
        assert!(shared.try_acquire_throttle(t0, &rate));
        // The collective 4th emit in the same window is throttled — this is the
        // P2b fix: previously each shard allowed `count` per window (N×limit).
        assert!(!shared.try_acquire_throttle(t0, &rate));
        // Window rotation after `per` elapses grants a fresh budget.
        assert!(shared.try_acquire_throttle(t0 + 61_000_000_000i64, &rate));
    }

    #[test]
    fn shared_instance_budget_is_exact_at_cap() {
        let shared = SharedLimits::new();
        assert!(shared.try_reserve_instance(2));
        assert!(shared.try_reserve_instance(2));
        assert!(!shared.try_reserve_instance(2));
        shared.release_instance();
        assert!(shared.try_reserve_instance(2));
    }

    #[test]
    fn shared_add_release_mirror_tracks_live_count() {
        let shared = SharedLimits::new();
        shared.add_instance();
        shared.add_instance();
        assert_eq!(shared.instance_count(), 2);
        shared.release_instance();
        assert_eq!(shared.instance_count(), 1);
    }

    #[test]
    fn shared_fail_latch_is_rule_wide() {
        let shared = SharedLimits::new();
        assert!(!shared.is_failed());
        shared.fail();
        assert!(shared.is_failed());
    }

    #[test]
    fn shared_memory_delta_mirror_and_recalibrate() {
        let shared = SharedLimits::new();
        shared.add_memory(100);
        shared.add_memory(50);
        assert_eq!(shared.memory_bytes(), 150);
        shared.sub_memory(30);
        assert_eq!(shared.memory_bytes(), 120);
        // Recalibrate a shard from 120 -> 200 exact: the shared total follows.
        shared.recalibrate_memory(120, 200);
        assert_eq!(shared.memory_bytes(), 200);
    }
}
