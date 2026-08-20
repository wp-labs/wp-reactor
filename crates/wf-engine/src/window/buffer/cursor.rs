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
        let log = self.log.read().expect("window log lock poisoned");
        let Some(&oldest_seq) = log.keys().next() else {
            return (Vec::new(), cursor, false);
        };
        let Some(&newest_seq) = log.keys().next_back() else {
            return (Vec::new(), cursor, false);
        };
        if cursor > newest_seq {
            return (Vec::new(), cursor, false);
        }
        let gap = cursor < oldest_seq;
        let effective_start = if gap { oldest_seq } else { cursor };
        // The read lock makes front/back/iteration a single consistent view:
        // a batch appended after this call is re-delivered next round
        // (at-least-once), same as before.
        let batches: Vec<RecordBatch> = log
            .range(effective_start..=newest_seq)
            .map(|(_, tb)| tb.batch.clone()) // Arc clone, zero data copy
            .collect();
        (batches, newest_seq + 1, gap)
    }

    /// Columnar pull entry point for the **pull-model** rule tasks
    /// (window-actor-pull-model.md, M1).
    ///
    /// Like [`Window::read_since`] it returns the shared `RecordBatch` Arcs for
    /// every batch since `cursor` (zero data copy). Additionally, for each
    /// batch it returns the **per-shard row subset** the caller owns:
    ///
    /// - `shard_index = None` (unsharded rule) → `None` for every batch: the
    ///   rule processes the whole batch.
    /// - `shard_index = Some(i)` (sharded match rule) → `Some(rows)` = the
    ///   batch's precomputed `TimedBatch.shard_rows[i]` (Arc clone, zero copy),
    ///   or `None` when this batch was not shard-partitioned (defensive: the
    ///   rule then processes the whole batch).
    ///
    /// This is what lets a sharded rule pull **only its own rows** from the
    /// single shared window log — the P2 zero-re-partition property (the
    /// partition was computed once in the parse stage, not per rule task).
    ///
    /// Returns `(batches, shard_rows_per_batch, new_cursor, gap_detected)`.
    pub fn read_since_with_shard(
        &self,
        cursor: u64,
        shard_index: Option<usize>,
    ) -> (Vec<RecordBatch>, Vec<Option<Arc<Vec<u32>>>>, u64, bool) {
        let log = self.log.read().expect("window log lock poisoned");
        let Some(&oldest_seq) = log.keys().next() else {
            return (Vec::new(), Vec::new(), cursor, false);
        };
        let Some(&newest_seq) = log.keys().next_back() else {
            return (Vec::new(), Vec::new(), cursor, false);
        };
        if cursor > newest_seq {
            return (Vec::new(), Vec::new(), cursor, false);
        }
        let gap = cursor < oldest_seq;
        let effective_start = if gap { oldest_seq } else { cursor };
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut shard_rows_per_batch: Vec<Option<Arc<Vec<u32>>>> = Vec::new();
        // The read lock makes front/back/iteration a single consistent view:
        // a batch appended after this call is re-delivered next round
        // (at-least-once), same as the row-based `events_since`.
        for (_, tb) in log.range(effective_start..=newest_seq) {
            batches.push(tb.batch.clone());
            // `shard_rows[i]` is the absolute row indices this shard owns in
            // this batch. The stored inner type is `Vec<u32>` (one index list
            // per shard), so we wrap it in an `Arc` for the pull-path return.
            // NOTE (M3 follow-up): store the per-shard list itself as
            // `Arc<Vec<u32>>` so this becomes a zero-copy `Arc::clone`.
            let subset: Option<Arc<Vec<u32>>> = shard_index.and_then(|i| {
                tb.shard_rows
                    .as_ref()
                    .and_then(|per_shard| per_shard.get(i).map(|v| Arc::new(v.clone())))
            });
            shard_rows_per_batch.push(subset);
        }
        (batches, shard_rows_per_batch, newest_seq + 1, gap)
    }

    /// Read the *shared parsed events* of batches since the given cursor.
    ///
    /// Like [`Window::read_since`], but returns the lazily-parsed events cached
    /// on each batch — the same `Arc` for every consuming rule, so a window
    /// batch is parsed exactly once instead of once per rule (wp-reactor#19).
    ///
    /// Returns `(events_per_batch, new_cursor, gap_detected)`.
    pub fn events_since(&self, cursor: u64) -> (Vec<Arc<Vec<Arc<Event>>>>, u64, bool) {
        let log = self.log.read().expect("window log lock poisoned");
        let Some(&oldest_seq) = log.keys().next() else {
            return (Vec::new(), cursor, false);
        };
        let Some(&newest_seq) = log.keys().next_back() else {
            return (Vec::new(), cursor, false);
        };
        if cursor > newest_seq {
            return (Vec::new(), cursor, false);
        }
        let gap = cursor < oldest_seq;
        let effective_start = if gap { oldest_seq } else { cursor };
        let events: Vec<Arc<Vec<Arc<Event>>>> = log
            .range(effective_start..=newest_seq)
            .map(|(_, tb)| tb.events(self.materialize_fields.as_deref()))
            .collect();
        (events, newest_seq + 1, gap)
    }

    /// Next sequence number that will be assigned to the next appended batch.
    pub fn next_seq(&self) -> u64 {
        self.next_seq.load(std::sync::atomic::Ordering::Relaxed)
    }
}
