use std::collections::VecDeque;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use arrow::array::{Array, TimestampNanosecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use wf_config::{LatePolicy, WindowConfig};

// ---------------------------------------------------------------------------
// AppendOutcome
// ---------------------------------------------------------------------------

/// Result of a watermark-aware append.
pub enum AppendOutcome {
    Appended,
    DroppedLate,
}

// ---------------------------------------------------------------------------
// WindowParams
// ---------------------------------------------------------------------------

/// Parameters for constructing a [`Window`].
pub struct WindowParams {
    pub name: String,
    pub schema: SchemaRef,
    /// Index of the time column in the schema, `None` for output windows.
    pub time_col_index: Option<usize>,
    /// Retention duration from the `.wfs` file.
    pub over: Duration,
}

// ---------------------------------------------------------------------------
// TimedBatch (internal)
// ---------------------------------------------------------------------------

struct TimedBatch {
    batch: RecordBatch,
    /// (min, max) event time in nanoseconds.
    event_time_range: (i64, i64),
    #[allow(dead_code)]
    ingested_at: Instant,
    row_count: usize,
    byte_size: usize,
    /// Monotonically increasing sequence number assigned on append.
    seq: u64,
}

// ---------------------------------------------------------------------------
// Window
// ---------------------------------------------------------------------------

/// A time-ordered buffer of Arrow RecordBatches with eviction support.
///
/// Batches are appended to the back and evicted from the front, either by
/// time expiry or memory pressure.
pub struct Window {
    name: String,
    schema: SchemaRef,
    time_col_index: Option<usize>,
    over: Duration,
    config: WindowConfig,
    batches: VecDeque<TimedBatch>,
    current_bytes: usize,
    total_rows: usize,
    watermark_nanos: i64,
    /// Next sequence number to assign to an appended batch.
    next_seq: u64,
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

    /// Append a batch with watermark advancement and lateness checking.
    ///
    /// 1. Extracts the event-time range from the batch.
    /// 2. Advances the watermark: `max(current, max_event_time - watermark_delay)`.
    /// 3. If `min_event_time < watermark - allowed_lateness`, applies the late
    ///    policy (Drop/SideOutput → skip, Revise → append anyway).
    /// 4. Otherwise appends normally via [`Self::append`].
    ///
    /// Windows without a time column never advance the watermark and never
    /// reject data as late.
    pub fn append_with_watermark(&mut self, batch: RecordBatch) -> Result<AppendOutcome> {
        if batch.num_rows() == 0 {
            return Ok(AppendOutcome::Appended);
        }

        if batch.schema() != self.schema {
            bail!(
                "schema mismatch: window {:?} expects {:?}, got {:?}",
                self.name,
                self.schema,
                batch.schema()
            );
        }

        let (min_event_time, max_event_time) = self.extract_time_range(&batch);

        // Lateness check FIRST against the current watermark (before this batch
        // advances it). This ensures a batch cannot be rejected by its own
        // watermark advancement — only by previously established watermarks.
        if self.time_col_index.is_some() && min_event_time != i64::MIN {
            let allowed = self.config.allowed_lateness.as_duration().as_nanos() as i64;
            let cutoff = self.watermark_nanos.saturating_sub(allowed);
            if min_event_time < cutoff {
                match self.config.late_policy {
                    // SideOutput not yet implemented — treated as Drop in M10.
                    LatePolicy::Drop | LatePolicy::SideOutput => {
                        return Ok(AppendOutcome::DroppedLate);
                    }
                    LatePolicy::Revise => { /* fall through to append */ }
                }
            }
        }

        // Advance watermark AFTER lateness check.
        if self.time_col_index.is_some() && max_event_time != i64::MAX {
            let delay = self.config.watermark.as_duration().as_nanos() as i64;
            let candidate = max_event_time.saturating_sub(delay);
            self.watermark_nanos = self.watermark_nanos.max(candidate);
        }

        self.append(batch)?;
        Ok(AppendOutcome::Appended)
    }

    /// Current watermark in nanoseconds.
    pub fn watermark_nanos(&self) -> i64 {
        self.watermark_nanos
    }

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

    /// Pop the oldest (front) batch, returning its byte size.
    ///
    /// Returns `None` if the window is empty.
    pub fn evict_oldest(&mut self) -> Option<usize> {
        let evicted = self.batches.pop_front()?;
        self.current_bytes -= evicted.byte_size;
        self.total_rows -= evicted.row_count;
        Some(evicted.byte_size)
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn test_schema_no_time() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]))
    }

    fn make_batch(schema: &SchemaRef, times: &[i64], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(times.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_batch_no_time(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn test_config(max_bytes: usize) -> WindowConfig {
        WindowConfig {
            name: "test".into(),
            mode: wf_config::DistMode::Local,
            max_window_bytes: max_bytes.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: wf_config::EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(5).into(),
            allowed_lateness: Duration::from_secs(0).into(),
            late_policy: wf_config::LatePolicy::Drop,
        }
    }

    fn test_window(over_secs: u64, max_bytes: usize) -> Window {
        let schema = test_schema();
        Window::new(
            WindowParams {
                name: "test_win".into(),
                schema,
                time_col_index: Some(0),
                over: Duration::from_secs(over_secs),
            },
            test_config(max_bytes),
        )
    }

    // -- 1. append_and_evict_expired ----------------------------------------

    #[test]
    fn append_and_evict_expired() {
        let mut win = test_window(10, usize::MAX);
        let schema = win.schema().clone();

        let t1 = 1_000_000_000; // 1 s
        let t2 = 5_000_000_000; // 5 s
        let t3 = 12_000_000_000; // 12 s

        win.append(make_batch(&schema, &[t1], &[100])).unwrap();
        win.append(make_batch(&schema, &[t2], &[200])).unwrap();
        win.append(make_batch(&schema, &[t3], &[300])).unwrap();
        assert_eq!(win.batch_count(), 3);
        assert_eq!(win.total_rows(), 3);

        // cutoff = 12s - 10s = 2s → batch1 (max=1s) < 2s → evicted
        win.evict_expired(12_000_000_000);
        assert_eq!(win.batch_count(), 2);
        assert_eq!(win.total_rows(), 2);

        // cutoff = 16s - 10s = 6s → batch2 (max=5s) < 6s → evicted
        win.evict_expired(16_000_000_000);
        assert_eq!(win.batch_count(), 1);
        assert_eq!(win.total_rows(), 1);
    }

    // -- 2. snapshot_is_independent_of_mutations ----------------------------

    #[test]
    fn snapshot_is_independent_of_mutations() {
        let mut win = test_window(60, usize::MAX);
        let schema = win.schema().clone();

        win.append(make_batch(&schema, &[1_000_000_000], &[100]))
            .unwrap();
        win.append(make_batch(&schema, &[2_000_000_000], &[200]))
            .unwrap();

        let snap = win.snapshot();
        assert_eq!(snap.len(), 2);

        // Mutate the window after snapshot.
        win.append(make_batch(&schema, &[3_000_000_000], &[300]))
            .unwrap();
        assert_eq!(win.batch_count(), 3);

        // Snapshot is unchanged.
        assert_eq!(snap.len(), 2);
        assert_eq!(snap[0].num_rows(), 1);
        assert_eq!(snap[1].num_rows(), 1);
    }

    // -- 3. empty_batch_is_skipped ------------------------------------------

    #[test]
    fn empty_batch_is_skipped() {
        let mut win = test_window(60, usize::MAX);
        let schema = win.schema().clone();

        win.append(make_batch(&schema, &[], &[])).unwrap();
        assert!(win.is_empty());
        assert_eq!(win.total_rows(), 0);
        assert_eq!(win.memory_usage(), 0);
    }

    // -- 4. schema_mismatch_rejected ----------------------------------------

    #[test]
    fn schema_mismatch_rejected() {
        let mut win = test_window(60, usize::MAX);

        let wrong_schema = Arc::new(Schema::new(vec![Field::new(
            "different",
            DataType::Int64,
            false,
        )]));
        let wrong_batch = RecordBatch::try_new(
            wrong_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        assert!(win.append(wrong_batch).is_err());
    }

    // -- 5. memory_eviction_on_append ---------------------------------------

    #[test]
    fn memory_eviction_on_append() {
        let schema = test_schema();

        // Measure the size of one batch.
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = probe.get_array_memory_size();

        // Allow room for exactly 2 batches.
        let max_bytes = one_batch_size * 2;
        let mut win = Window::new(
            WindowParams {
                name: "mem_win".into(),
                schema,
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
            },
            test_config(max_bytes),
        );

        win.append(probe).unwrap();
        assert_eq!(win.batch_count(), 1);

        win.append(make_batch(win.schema(), &[2_000_000_000], &[200]))
            .unwrap();
        assert_eq!(win.batch_count(), 2);

        // Third batch exceeds budget → oldest evicted.
        win.append(make_batch(win.schema(), &[3_000_000_000], &[300]))
            .unwrap();
        assert_eq!(win.batch_count(), 2);
        assert!(win.memory_usage() <= max_bytes);
    }

    // -- 6. no_time_col_window ----------------------------------------------

    #[test]
    fn no_time_col_window() {
        let schema = test_schema_no_time();
        let mut win = Window::new(
            WindowParams {
                name: "output_win".into(),
                schema: schema.clone(),
                time_col_index: None,
                over: Duration::from_secs(60),
            },
            test_config(usize::MAX),
        );

        win.append(make_batch_no_time(&schema, &[100, 200]))
            .unwrap();
        assert_eq!(win.batch_count(), 1);
        assert_eq!(win.total_rows(), 2);

        // evict_expired is no-op for no-time-column windows.
        win.evict_expired(i64::MAX);
        assert_eq!(win.batch_count(), 1);
        assert_eq!(win.total_rows(), 2);
    }

    // -- 7. evict_on_empty_window_is_noop -----------------------------------

    #[test]
    fn evict_on_empty_window_is_noop() {
        let mut win = test_window(60, usize::MAX);
        win.evict_expired(i64::MAX);
        assert!(win.is_empty());
    }

    // -- 8. memory_usage_tracks_correctly -----------------------------------

    #[test]
    fn memory_usage_tracks_correctly() {
        let mut win = test_window(3600, usize::MAX);
        let schema = win.schema().clone();
        assert_eq!(win.memory_usage(), 0);

        let b1 = make_batch(&schema, &[1_000_000_000], &[100]);
        let b1_size = b1.get_array_memory_size();
        win.append(b1).unwrap();
        assert_eq!(win.memory_usage(), b1_size);

        let b2 = make_batch(&schema, &[2_000_000_000, 3_000_000_000], &[200, 300]);
        let b2_size = b2.get_array_memory_size();
        win.append(b2).unwrap();
        assert_eq!(win.memory_usage(), b1_size + b2_size);
    }

    // -- 9. multi_row_batch_time_range --------------------------------------

    #[test]
    fn multi_row_batch_time_range() {
        let mut win = test_window(10, usize::MAX);
        let schema = win.schema().clone();

        // Rows at 1s, 5s, 8s — batch max time is 8s.
        win.append(make_batch(
            &schema,
            &[1_000_000_000, 5_000_000_000, 8_000_000_000],
            &[10, 20, 30],
        ))
        .unwrap();
        assert_eq!(win.batch_count(), 1);

        // cutoff = 15s - 10s = 5s → batch max=8s >= 5s → NOT evicted
        win.evict_expired(15_000_000_000);
        assert_eq!(win.batch_count(), 1);

        // cutoff = 19s - 10s = 9s → batch max=8s < 9s → evicted
        win.evict_expired(19_000_000_000);
        assert_eq!(win.batch_count(), 0);
    }

    // -- 10. append_with_watermark_on_time ------------------------------------

    #[test]
    fn append_with_watermark_on_time() {
        // watermark delay = 5s, allowed_lateness = 0s
        let mut win = test_window(3600, usize::MAX);
        let schema = win.schema().clone();

        // Initial watermark is i64::MIN. Batch at 10s:
        //   watermark = max(MIN, 10s - 5s) = 5s
        //   min_event_time(10s) >= 5s → on time
        let outcome = win
            .append_with_watermark(make_batch(&schema, &[10_000_000_000], &[1]))
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::Appended));
        assert_eq!(win.batch_count(), 1);
        assert_eq!(win.watermark_nanos(), 5_000_000_000);
    }

    // -- 11. append_with_watermark_drop_late ----------------------------------

    #[test]
    fn append_with_watermark_drop_late() {
        // watermark delay = 5s, allowed_lateness = 0s, late_policy = Drop
        let mut win = test_window(3600, usize::MAX);
        let schema = win.schema().clone();

        // Send fresh batch at 20s → watermark = 15s
        win.append_with_watermark(make_batch(&schema, &[20_000_000_000], &[1]))
            .unwrap();
        assert_eq!(win.watermark_nanos(), 15_000_000_000);

        // Send old batch at 5s → 5s < 15s → DroppedLate
        let outcome = win
            .append_with_watermark(make_batch(&schema, &[5_000_000_000], &[2]))
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::DroppedLate));
        // Only the first batch should be in the window.
        assert_eq!(win.batch_count(), 1);
    }

    // -- 12. watermark_advances_monotonically ---------------------------------

    #[test]
    fn watermark_advances_monotonically() {
        let mut win = test_window(3600, usize::MAX);
        let schema = win.schema().clone();

        // Batch at 20s → watermark = 15s
        win.append_with_watermark(make_batch(&schema, &[20_000_000_000], &[1]))
            .unwrap();
        assert_eq!(win.watermark_nanos(), 15_000_000_000);

        // Batch at 10s (on-time since 10s >= 15s - 0s is false... wait:
        //   10s < 15s → late → DroppedLate). The watermark should NOT regress.
        //   candidate = 10s - 5s = 5s; max(15s, 5s) = 15s → unchanged
        let _ = win
            .append_with_watermark(make_batch(&schema, &[10_000_000_000], &[2]))
            .unwrap();
        assert_eq!(win.watermark_nanos(), 15_000_000_000);

        // Batch at 30s → watermark = max(15s, 25s) = 25s
        win.append_with_watermark(make_batch(&schema, &[30_000_000_000], &[3]))
            .unwrap();
        assert_eq!(win.watermark_nanos(), 25_000_000_000);
    }

    // -- 13. append_with_watermark_schema_mismatch_rejected --------------------

    #[test]
    fn append_with_watermark_schema_mismatch_rejected() {
        let mut win = test_window(3600, usize::MAX);

        let wrong_schema = Arc::new(Schema::new(vec![Field::new(
            "different",
            DataType::Int64,
            false,
        )]));
        let wrong_batch = RecordBatch::try_new(
            wrong_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // Must return Err, not panic.
        assert!(win.append_with_watermark(wrong_batch).is_err());
    }

    // -- 14. read_since_normal -----------------------------------------------

    #[test]
    fn read_since_normal() {
        let mut win = test_window(3600, usize::MAX);
        let schema = win.schema().clone();

        assert_eq!(win.next_seq(), 0);
        win.append(make_batch(&schema, &[1_000_000_000], &[100]))
            .unwrap();
        win.append(make_batch(&schema, &[2_000_000_000], &[200]))
            .unwrap();
        win.append(make_batch(&schema, &[3_000_000_000], &[300]))
            .unwrap();
        assert_eq!(win.next_seq(), 3);

        // Read from cursor 0 → all 3 batches
        let (batches, cursor, gap) = win.read_since(0);
        assert_eq!(batches.len(), 3);
        assert_eq!(cursor, 3);
        assert!(!gap);

        // Read from cursor 1 → last 2 batches
        let (batches, cursor, gap) = win.read_since(1);
        assert_eq!(batches.len(), 2);
        assert_eq!(cursor, 3);
        assert!(!gap);

        // Read from cursor 3 → no new batches
        let (batches, cursor, gap) = win.read_since(3);
        assert!(batches.is_empty());
        assert_eq!(cursor, 3);
        assert!(!gap);
    }

    // -- 15. read_since_gap_detection ----------------------------------------

    #[test]
    fn read_since_gap_detection() {
        let schema = test_schema();
        let probe = make_batch(&schema, &[1_000_000_000], &[100]);
        let one_batch_size = probe.get_array_memory_size();
        // Allow room for exactly 2 batches → oldest evicted when 3rd arrives.
        let max_bytes = one_batch_size * 2;
        let mut win = Window::new(
            WindowParams {
                name: "gap_win".into(),
                schema,
                time_col_index: Some(0),
                over: Duration::from_secs(3600),
            },
            test_config(max_bytes),
        );

        win.append(probe).unwrap(); // seq 0
        win.append(make_batch(win.schema(), &[2_000_000_000], &[200]))
            .unwrap(); // seq 1
        win.append(make_batch(win.schema(), &[3_000_000_000], &[300]))
            .unwrap(); // seq 2 → seq 0 evicted

        // Cursor 0 was evicted → gap
        let (batches, cursor, gap) = win.read_since(0);
        assert!(gap);
        assert_eq!(batches.len(), 2); // seq 1 and 2
        assert_eq!(cursor, 3);
    }

    // -- 16. read_since_empty_window -----------------------------------------

    #[test]
    fn read_since_empty_window() {
        let win = test_window(3600, usize::MAX);
        let (batches, cursor, gap) = win.read_since(0);
        assert!(batches.is_empty());
        assert_eq!(cursor, 0);
        assert!(!gap);
    }

    // -- 17. read_since_cursor_ahead -----------------------------------------

    #[test]
    fn read_since_cursor_ahead() {
        let mut win = test_window(3600, usize::MAX);
        let schema = win.schema().clone();
        win.append(make_batch(&schema, &[1_000_000_000], &[100]))
            .unwrap();

        // Cursor ahead of newest → no data, no gap
        let (batches, cursor, gap) = win.read_since(999);
        assert!(batches.is_empty());
        assert_eq!(cursor, 999);
        assert!(!gap);
    }
}
