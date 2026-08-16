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
    pub fn evict_expired(&mut self, now_nanos: i64, acked_floor: u64) {
        if self.time_col_index.is_none() || self.over == Duration::ZERO {
            return;
        }

        let over_nanos = self.over.as_nanos() as i64;
        let cutoff = now_nanos - over_nanos;

        while let Some(front) = self.batches.front() {
            let expired = front.event_time_range.1 < cutoff;
            let consumed = front.seq + 1 <= acked_floor;
            if expired && consumed {
                let evicted = self.batches.pop_front().unwrap();
                self.current_bytes -= evicted.byte_size;
                self.total_rows -= evicted.row_count;
                self.remove_batch_from_index(&evicted);
            } else {
                break;
            }
        }
    }

    /// Pop the oldest (front) batch, returning its byte size.
    ///
    /// Returns `None` if the window is empty.
    pub fn evict_oldest(&mut self) -> Option<usize> {
        let evicted = self.batches.pop_front()?;
        self.current_bytes -= evicted.byte_size;
        self.total_rows -= evicted.row_count;
        self.remove_batch_from_index(&evicted);
        Some(evicted.byte_size)
    }

    /// Remove an evicted batch's rows from the join index (if configured).
    fn remove_batch_from_index(&mut self, evicted: &super::TimedBatch) {
        if let Some(idx) = &mut self.join_index {
            let events = evicted.events(self.materialize_fields.as_deref());
            idx.remove_batch(&events);
        }
    }
}
