mod cursor;
mod eviction;
mod types;
mod watermark;

#[cfg(test)]
mod tests;

pub use types::{AppendOutcome, WindowParams};

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use arrow::array::{Array, TimestampNanosecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use wf_config::WindowConfig;

use types::TimedBatch;

/// A time-ordered buffer of Arrow RecordBatches with eviction support.
///
/// Batches are appended to the back and evicted from the front, either by
/// time expiry or memory pressure.
pub struct Window {
    pub(super) name: String,
    pub(super) schema: SchemaRef,
    pub(super) time_col_index: Option<usize>,
    pub(super) over: Duration,
    pub(super) config: WindowConfig,
    pub(super) batches: VecDeque<TimedBatch>,
    pub(super) current_bytes: usize,
    pub(super) total_rows: usize,
    pub(super) watermark_nanos: i64,
    /// Next sequence number to assign to an appended batch.
    pub(super) next_seq: u64,
}

impl Window {
    /// Create a new empty window.
    pub fn new(params: WindowParams, config: WindowConfig) -> Self {
        Self {
            name: params.name,
            schema: params.schema,
            time_col_index: params.time_col_index,
            over: params.over,
            config,
            batches: VecDeque::new(),
            current_bytes: 0,
            total_rows: 0,
            watermark_nanos: i64::MIN,
            next_seq: 0,
        }
    }

    /// Append a RecordBatch to this window.
    ///
    /// Empty batches are silently skipped. Returns an error if the batch
    /// schema does not match the window schema. After appending, memory
    /// eviction runs if `current_bytes > max_window_bytes`.
    pub fn append(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if batch.schema() != self.schema {
            bail!(
                "schema mismatch: window {:?} expects {:?}, got {:?}",
                self.name,
                self.schema,
                batch.schema()
            );
        }

        let event_time_range = self.extract_time_range(&batch);
        let row_count = batch.num_rows();
        let byte_size = batch.get_array_memory_size();
        let seq = self.next_seq;
        self.next_seq += 1;

        self.batches.push_back(TimedBatch {
            batch,
            event_time_range,
            ingested_at: Instant::now(),
            row_count,
            byte_size,
            seq,
        });

        self.current_bytes += byte_size;
        self.total_rows += row_count;

        // Memory eviction: pop oldest batches while over budget.
        let max_bytes = self.config.max_window_bytes.as_bytes();
        while self.current_bytes > max_bytes {
            if let Some(evicted) = self.batches.pop_front() {
                self.current_bytes -= evicted.byte_size;
                self.total_rows -= evicted.row_count;
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Return a snapshot of all current batches.
    ///
    /// `RecordBatch::clone()` is Arc-ref-counted — no data copy occurs.
    /// The returned `Vec` remains valid even if the window is subsequently
    /// mutated.
    pub fn snapshot(&self) -> Vec<RecordBatch> {
        self.batches.iter().map(|tb| tb.batch.clone()).collect()
    }

    pub fn memory_usage(&self) -> usize {
        self.current_bytes
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    // -- private helpers ----------------------------------------------------

    /// Extract the (min, max) event-time range from a batch.
    ///
    /// Returns `(i64::MIN, i64::MAX)` sentinel when there is no time column,
    /// the column cannot be downcast, or all values are null.
    fn extract_time_range(&self, batch: &RecordBatch) -> (i64, i64) {
        let Some(idx) = self.time_col_index else {
            return (i64::MIN, i64::MAX);
        };

        let col = batch.column(idx);
        let Some(ts_array) = col.as_any().downcast_ref::<TimestampNanosecondArray>() else {
            return (i64::MIN, i64::MAX);
        };

        let mut min_val = i64::MAX;
        let mut max_val = i64::MIN;
        let mut found = false;

        for i in 0..ts_array.len() {
            if !ts_array.is_null(i) {
                let v = ts_array.value(i);
                min_val = min_val.min(v);
                max_val = max_val.max(v);
                found = true;
            }
        }

        if found {
            (min_val, max_val)
        } else {
            (i64::MIN, i64::MAX)
        }
    }
}
