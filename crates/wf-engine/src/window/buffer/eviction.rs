use std::sync::atomic::Ordering;
use std::time::Duration;

use super::Window;

impl Window {
    /// Remove front batches whose max event time is older than `now_nanos - over`.
    ///
    /// `acked_floor` is the consumption floor from
    /// [`WindowProgress::min_acked`](crate::window::WindowProgress::min_acked):
    /// a batch is only removable when **every** live consumer has acked past
    /// it (`batch.seq + 1 <= acked_floor`), so a sweep can never drop data a
    /// slow rule has not yet read. Windows without consumers get `u64::MAX`.
    ///
    /// No-op for windows without a time column or with `over == Duration::ZERO`.
    pub fn evict_expired(&self, now_nanos: i64, acked_floor: u64) {
        if self.time_col_index.is_none() || self.over == Duration::ZERO {
            return;
        }

        let over_nanos = self.over.as_nanos() as i64;
        // Saturating: an uninitialized event-time watermark (i64::MIN, e.g.
        // windows appended without `append_with_watermark`) must not panic and
        // must not evict anything (no batch time < i64::MIN).
        let cutoff = now_nanos.saturating_sub(over_nanos);

        let mut evicted = false;
        {
            let mut log = self.log.write().expect("window log lock poisoned");
            loop {
                let removable = {
                    let Some((_, tb)) = log.first_key_value() else {
                        break;
                    };
                    let expired = tb.event_time_range.1 < cutoff;
                    let consumed = tb.seq < acked_floor;
                    expired && consumed
                };
                if !removable {
                    break;
                }
                let (_, tb) = log
                    .pop_first()
                    .expect("front vanished between check and remove");
                let byte_size = tb.byte_size;
                let row_count = tb.row_count;
                self.remove_batch_from_index(&tb);
                // Dropping the owned `TimedBatch` destroys the Arrow batch and
                // parsed events eagerly — no deferred (epoch-GC) reclamation.
                drop(tb);
                self.current_bytes.fetch_sub(byte_size, Ordering::Relaxed);
                self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
                self.batch_count.fetch_sub(1, Ordering::Relaxed);
                evicted = true;
            }
        }
        // Content changed (batches removed): bump the generation so has() /
        // snapshot caches keyed to it invalidate. (append_inner bumps it for
        // append + its own eviction; the evictor sweeps must do the same or a
        // cached distinct-value set goes stale after time eviction.)
        if evicted {
            self.generation.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Pop the oldest (front) batch, returning its byte size.
    ///
    /// Returns `None` if the window is empty.
    pub fn evict_oldest(&self) -> Option<usize> {
        let mut log = self.log.write().expect("window log lock poisoned");
        let (_, tb) = log.pop_first()?;
        let byte_size = tb.byte_size;
        let row_count = tb.row_count;
        self.remove_batch_from_index(&tb);
        drop(tb);
        self.current_bytes.fetch_sub(byte_size, Ordering::Relaxed);
        self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
        self.batch_count.fetch_sub(1, Ordering::Relaxed);
        // Content changed: invalidate has()/snapshot caches (see evict_expired).
        self.generation.fetch_add(1, Ordering::Relaxed);
        Some(byte_size)
    }

    /// Sequence number of the oldest (front) batch, or `None` if the window
    /// is empty. The evictor's memory-pressure phase uses this to decide
    /// whether the front batch is safe to drop (its `seq` is below the
    /// consumption floor, so every live consumer has already read it).
    pub fn oldest_seq(&self) -> Option<u64> {
        let log = self.log.read().expect("window log lock poisoned");
        log.first_key_value().map(|(_, tb)| tb.seq)
    }

    /// Pop the oldest (front) batch **only if** its `seq` is below the
    /// consumption floor `acked_floor` — i.e. every live consumer has
    /// already acked past it. Returns the reclaimed byte size, or `None`
    /// when the window is empty or its oldest batch is still unacked.
    ///
    /// This is the floor-respecting counterpart of [`Window::evict_oldest`].
    /// The old lossy memory backstop dropped the front unconditionally, which
    /// silently discarded pull-mode batches a slow rule had not yet read —
    /// the q3 pull regression. With this variant the evictor can never lose
    /// unread pull data; when nothing is safe to drop it reports
    /// `memory_pressure` and the actor applies backpressure instead.
    pub fn evict_oldest_acked(&self, acked_floor: u64) -> Option<usize> {
        let mut log = self.log.write().expect("window log lock poisoned");
        let removable = {
            let (_, tb) = log.first_key_value()?;
            tb.seq < acked_floor
        };
        if !removable {
            return None;
        }
        let (_, tb) = log
            .pop_first()
            .expect("front vanished between check and remove");
        let byte_size = tb.byte_size;
        let row_count = tb.row_count;
        self.remove_batch_from_index(&tb);
        drop(tb);
        self.current_bytes.fetch_sub(byte_size, Ordering::Relaxed);
        self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
        self.batch_count.fetch_sub(1, Ordering::Relaxed);
        // Content changed: invalidate has()/snapshot caches (see evict_expired).
        self.generation.fetch_add(1, Ordering::Relaxed);
        Some(byte_size)
    }
}
