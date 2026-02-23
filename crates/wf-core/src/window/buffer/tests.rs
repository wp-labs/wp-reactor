#[cfg(test)]
mod tests {
    use crate::window::buffer::types::WindowParams;
    use crate::window::buffer::Window;
    use crate::window::buffer::types::AppendOutcome;
    use arrow::array::{Int64Array, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use std::time::Duration;
    use wf_config::WindowConfig;

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
