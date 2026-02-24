use arrow::record_batch::RecordBatch;

use super::Window;

impl Window {
    /// Read batches appended since the given cursor position.
    ///
    /// Returns `(new_batches, new_cursor, gap_detected)`.
    /// `gap_detected = true` means the cursor fell behind eviction and some
    /// data was lost.
    pub fn read_since(&self, cursor: u64) -> (Vec<RecordBatch>, u64, bool) {
        if self.batches.is_empty() {
            return (Vec::new(), cursor, false);
        }
        let oldest_seq = self.batches.front().unwrap().seq;
        let newest_seq = self.batches.back().unwrap().seq;
        if cursor > newest_seq {
            return (Vec::new(), cursor, false);
        }
        let gap = cursor < oldest_seq;
        let effective_start = if gap { oldest_seq } else { cursor };
        let batches: Vec<RecordBatch> = self
            .batches
            .iter()
            .filter(|tb| tb.seq >= effective_start)
            .map(|tb| tb.batch.clone()) // Arc clone, zero data copy
            .collect();
        (batches, newest_seq + 1, gap)
    }

    /// Next sequence number that will be assigned to the next appended batch.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}
