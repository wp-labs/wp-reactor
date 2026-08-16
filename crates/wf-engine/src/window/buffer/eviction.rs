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
        let cutoff = now_nanos - over_nanos;

        loop {
            let removable = {
                let Some(front) = self.log.front() else {
                    break;
                };
                let tb = front.value();
                let expired = tb.event_time_range.1 < cutoff;
                let consumed = tb.seq + 1 <= acked_floor;
                expired && consumed
            };
            if !removable {
                break;
            }
            // Remove by key: a concurrent evictor that wins the race makes
            // `remove` return None and we simply retry on the new front.
            // `remove` hands back an `Entry` (the node is already unlinked);
            // read the accounting fields through it before dropping it.
            let key = *self.log.front().expect("front vanished").key();
            if let Some(evicted) = self.log.remove(&key) {
                let tb = evicted.value();
                let byte_size = tb.byte_size;
                let row_count = tb.row_count;
                self.remove_batch_from_index(tb);
                drop(evicted);
                self.current_bytes.fetch_sub(byte_size, Ordering::Relaxed);
                self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
                self.batch_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    /// Pop the oldest (front) batch, returning its byte size.
    ///
    /// Returns `None` if the window is empty.
    pub fn evict_oldest(&self) -> Option<usize> {
        let key = *self.log.front()?.key();
        let evicted = self.log.remove(&key)?;
        let tb = evicted.value();
        let byte_size = tb.byte_size;
        let row_count = tb.row_count;
        self.remove_batch_from_index(tb);
        drop(evicted);
        self.current_bytes
            .fetch_sub(byte_size, Ordering::Relaxed);
        self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
        self.batch_count.fetch_sub(1, Ordering::Relaxed);
        Some(byte_size)
    }
}
