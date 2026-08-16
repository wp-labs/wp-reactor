use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::match_engine::Event;

use super::Window;

impl Window {
    /// Read batches appended since the given cursor position.
    ///
    /// Returns `(new_batches, new_cursor, gap_detected)`.
    /// `gap_detected = true` means the cursor fell behind eviction and some
    /// data was lost.
    pub fn read_since(&self, cursor: u64) -> (Vec<RecordBatch>, u64, bool) {
        let Some(front) = self.log.front() else {
            return (Vec::new(), cursor, false);
        };
        let oldest_seq = *front.key();
        drop(front);
        let Some(back) = self.log.back() else {
            return (Vec::new(), cursor, false);
        };
        let newest_seq = *back.key();
        if cursor > newest_seq {
            return (Vec::new(), cursor, false);
        }
        let gap = cursor < oldest_seq;
        let effective_start = if gap { oldest_seq } else { cursor };
        // Cap the range at the newest seq observed *before* iterating, so a
        // batch appended concurrently is re-delivered next round instead of
        // being skipped by an already-advanced cursor (at-least-once).
        let batches: Vec<RecordBatch> = self
            .log
            .range(effective_start..=newest_seq)
            .map(|e| e.value().batch.clone()) // Arc clone, zero data copy
            .collect();
        (batches, newest_seq + 1, gap)
    }

    /// Read the *shared parsed events* of batches since the given cursor.
    ///
    /// Like [`Window::read_since`], but returns the lazily-parsed events cached
    /// on each batch — the same `Arc` for every consuming rule, so a window
    /// batch is parsed exactly once instead of once per rule (wp-reactor#19).
    ///
    /// Returns `(events_per_batch, new_cursor, gap_detected)`.
    pub fn events_since(&self, cursor: u64) -> (Vec<Arc<Vec<Arc<Event>>>>, u64, bool) {
        let Some(front) = self.log.front() else {
            return (Vec::new(), cursor, false);
        };
        let oldest_seq = *front.key();
        drop(front);
        let Some(back) = self.log.back() else {
            return (Vec::new(), cursor, false);
        };
        let newest_seq = *back.key();
        if cursor > newest_seq {
            return (Vec::new(), cursor, false);
        }
        let gap = cursor < oldest_seq;
        let effective_start = if gap { oldest_seq } else { cursor };
        let events: Vec<Arc<Vec<Arc<Event>>>> = self
            .log
            .range(effective_start..=newest_seq)
            .map(|e| e.value().events(self.materialize_fields.as_deref()))
            .collect();
        (events, newest_seq + 1, gap)
    }

    /// Next sequence number that will be assigned to the next appended batch.
    pub fn next_seq(&self) -> u64 {
        self.next_seq.load(std::sync::atomic::Ordering::Relaxed)
    }
}
