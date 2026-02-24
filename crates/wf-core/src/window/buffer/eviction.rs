use std::time::Duration;

use super::Window;

impl Window {
    /// Remove front batches whose max event time is older than `now_nanos - over`.
    ///
    /// No-op for windows without a time column or with `over == Duration::ZERO`.
    pub fn evict_expired(&mut self, now_nanos: i64) {
        if self.time_col_index.is_none() || self.over == Duration::ZERO {
            return;
        }

        let over_nanos = self.over.as_nanos() as i64;
        let cutoff = now_nanos - over_nanos;

        while let Some(front) = self.batches.front() {
            if front.event_time_range.1 < cutoff {
                let evicted = self.batches.pop_front().unwrap();
                self.current_bytes -= evicted.byte_size;
                self.total_rows -= evicted.row_count;
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
        Some(evicted.byte_size)
    }
}
