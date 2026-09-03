//! 读路径测试（2026-09-04 自 tests.rs 拆出；`#[path]` 兄弟子模块）：read_since
//! 读游标、sized/parsed append 与 pull 分片读取、读路径并发（rwlock 不饿死写者）。

use super::*;

// -- 14. read_since_normal -----------------------------------------------

#[test]
fn read_since_normal() {
    let win = test_window(3600, usize::MAX);
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
    let one_batch_size = content_bytes(&probe);
    // Allow room for exactly 2 batches → oldest evicted when 3rd arrives.
    let max_bytes = one_batch_size * 2;
    let win = Window::new(
        WindowParams {
            name: "gap_win".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
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
    let win = test_window(3600, usize::MAX);
    let schema = win.schema().clone();
    win.append(make_batch(&schema, &[1_000_000_000], &[100]))
        .unwrap();

    // Cursor ahead of newest → no data, no gap
    let (batches, cursor, gap) = win.read_since(999);
    assert!(batches.is_empty());
    assert_eq!(cursor, 999);
    assert!(!gap);
}

/// Fast-path append (`append_with_watermark_sized`, no pre-parsed events) must
/// leave the batch's `parsed_events` *uninitialized*, so a consumer reading via
/// `events_since()` still lazily parses the real events — a later subscriber
/// (hot reload) must not see empty events for batches that arrived while the
/// window had no rule consumers.
#[test]
fn sized_append_keeps_events_lazily_parseable() {
    let schema = test_schema();
    let batch = make_batch(&schema, &[1_000_000_000, 2_000_000_000], &[42, 99]);
    let content = content_bytes(&batch);
    let cap = content + 10;

    let win = Window::new(
        WindowParams {
            name: "lazy".into(),
            schema: schema.clone(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        WindowConfig {
            name: "lazy".into(),
            mode: DistMode::Local,
            max_window_bytes: cap.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(0).into(),
            allowed_lateness: Duration::from_secs(3600).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        },
    );
    win.append_with_watermark_sized(batch, content, None)
        .unwrap();

    // events_since lazily parses the batch → real events, not empty.
    let (events_list, cursor, gap) = win.events_since(0);
    assert!(!gap, "no cursor gap");
    assert_eq!(events_list.len(), 1, "one batch of events");
    assert_eq!(
        events_list[0].len(),
        2,
        "both rows must be lazily parsed into events"
    );
    assert_eq!(cursor, 1, "cursor advances past the batch");
}

/// Regression: the columnar/deferred append path (`append_with_watermark_sized`,
/// `events = None`) must persist the parse-side precomputed `shard_rows` into
/// the window log. The pull-model rule tasks read their per-shard row subset
/// from `read_since_with_shard(shard_index)` — if the `(None, _)` arm of
/// `append_with_watermark_inner` dropped `shard_rows` (the Q2 30M pull
/// over-production bug, ~9×), every pull shard would process the WHOLE batch.
#[test]
fn sized_append_persists_shard_rows_for_pull() {
    let schema = test_schema();
    // 3 rows; shard 0 owns rows {0, 2}, shard 1 owns row {1}.
    let batch = make_batch(
        &schema,
        &[1_000_000_000, 2_000_000_000, 3_000_000_000],
        &[42, 99, 7],
    );
    let content = content_bytes(&batch);

    let win = Window::new(
        WindowParams {
            name: "sharded".into(),
            schema: schema.clone(),
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        WindowConfig {
            name: "sharded".into(),
            mode: DistMode::Local,
            max_window_bytes: usize::MAX.into(),
            over_cap: Duration::from_secs(3600).into(),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: Duration::from_secs(0).into(),
            allowed_lateness: Duration::from_secs(3600).into(),
            late_policy: LatePolicy::Drop,
            table: None,
        },
    );
    let shard_rows: Option<Arc<Vec<Vec<u32>>>> = Some(Arc::from(vec![vec![0u32, 2], vec![1u32]]));
    win.append_with_watermark_sized(batch, content, shard_rows)
        .unwrap();

    // Shard 0 must pull only its own rows {0, 2}.
    let (_batches, per_shard, _cursor, _gap) = win.read_since_with_shard(0, Some(0));
    assert_eq!(per_shard.len(), 1, "one batch");
    assert_eq!(
        per_shard[0].as_ref().map(|v| v.as_slice()),
        Some(&[0u32, 2][..]),
        "shard 0 owns rows {{0, 2}}"
    );

    // Shard 1 must pull only row {1}.
    let (_batches, per_shard, _cursor, _gap) = win.read_since_with_shard(0, Some(1));
    assert_eq!(
        per_shard[0].as_ref().map(|v| v.as_slice()),
        Some(&[1u32][..]),
        "shard 1 owns row {{1}}"
    );

    // Unpartitioned pull (shard_index = None) sees the whole batch.
    let (_batches, per_shard, _cursor, _gap) = win.read_since_with_shard(0, None);
    assert!(
        per_shard[0].is_none(),
        "unsharded pull gets no row subset (processes whole batch)"
    );
}

/// M1 pull-model invariant (P2 zero re-partition) across **multiple batches
/// and multiple shards**: `read_since_with_shard(shard_index)` returns exactly
/// the per-shard row subset stored in the window log, and the cross-shard
/// (batch × row) union must cover every row **exactly once** — no loss, no
/// duplication. The partition is computed once at write time (here via the
/// production `precompute_shard_rows`) and each shard pulls only its own slice.
#[test]
fn pull_sharded_multi_batch_zero_repartition_union() {
    let schema = test_schema(); // ts(col0), value(col1)
    let fanout = RuleFanout::new();
    fanout.register_window_sharding(
        "auth_events",
        Arc::from(vec![FieldRef::Simple("value".into())].into_boxed_slice()),
        2,
    );

    let win = test_window(3600, usize::MAX);

    const NBATCH: u32 = 3;
    const NROW: u32 = 5;
    for b in 0..NBATCH {
        let times: Vec<i64> = (0..NROW)
            .map(|i| 1_700_000_000_000_000_000i64 + (b * NROW + i) as i64)
            .collect();
        let values: Vec<i64> = (0..NROW).map(|i| (b * NROW + i) as i64).collect();
        let batch = make_batch(&schema, &times, &values);
        // Parse-stage precompute: partition this batch once by the match key.
        let shard_rows = fanout
            .precompute_shard_rows("auth_events", &batch)
            .expect("sharded window has a partition");
        let size = content_bytes(&batch);
        win.append_with_watermark_sized(batch, size, Some(Arc::new(shard_rows.to_vec())))
            .unwrap();
    }

    // Every shard reads ALL batches but only its own row subset. The union
    // across shards must equal the full (batch, row) grid exactly once.
    let mut seen: std::collections::HashSet<(usize, u32)> = std::collections::HashSet::new();
    let mut duplicate = false;
    for shard in 0..2usize {
        let (batches, per_shard, cursor, gap) = win.read_since_with_shard(0, Some(shard));
        assert!(!gap, "no eviction before first read");
        assert_eq!(
            batches.len(),
            NBATCH as usize,
            "every shard sees all batches"
        );
        assert_eq!(cursor, NBATCH as u64, "cursor advances to newest+1");
        for (k, subset) in per_shard.iter().enumerate() {
            let rows = subset.as_ref().expect("shard subset present for batch {k}");
            for &r in rows.iter() {
                if !seen.insert((k, r)) {
                    duplicate = true;
                }
            }
        }
    }
    assert!(!duplicate, "each row must belong to exactly one shard");
    let mut all: Vec<(usize, u32)> = seen.into_iter().collect();
    all.sort();
    let expected: Vec<(usize, u32)> = (0..NBATCH as usize)
        .flat_map(|k| (0..NROW as usize).map(move |r| (k, r as u32)))
        .collect();
    assert_eq!(
        all, expected,
        "union of all shards covers every row exactly once (zero re-partition)"
    );
}

/// M1 regression anchor for consumption-floor safety: if a batch is evicted
/// before the pull cursor reads it, `read_since_with_shard` must report
/// `gap = true` (cursor < oldest_seq) and resume from the oldest surviving
/// batch, while still advancing the cursor to `newest + 1`. A cursor that has
/// caught up (== floor) reads cleanly with no gap.
#[test]
fn pull_gap_detected_when_batch_evicted_before_read() {
    let schema = test_schema();
    let win = test_window(3600, usize::MAX);
    for b in 0..3u32 {
        let times = vec![1_700_000_000_000_000_000i64 + b as i64; 2];
        let values = vec![10i64 + b as i64, 20 + b as i64];
        let batch = make_batch(&schema, &times, &values);
        let size = content_bytes(&batch);
        win.append_with_watermark_sized(batch, size, None).unwrap();
    }
    assert_eq!(win.batch_count(), 3, "three batches appended");

    // Drop the oldest batch (memory eviction ignores the consumption floor).
    assert!(win.evict_oldest().is_some());

    // Cursor still at 0 → 0 < oldest_seq(=1) → gap.
    let (batches, _per, cursor, gap) = win.read_since_with_shard(0, None);
    assert!(gap, "cursor 0 must detect gap after front eviction");
    assert_eq!(batches.len(), 2, "only the surviving batches are returned");
    assert_eq!(cursor, 3, "cursor still advances to newest+1");

    // A cursor that caught up to the floor reads cleanly, no gap.
    let (batches2, _per2, cursor2, gap2) = win.read_since_with_shard(1, None);
    assert!(!gap2, "cursor at floor reads without gap");
    assert_eq!(batches2.len(), 2);
    assert_eq!(cursor2, 3);
}

// ---------------------------------------------------------------------------
// Concurrency diagnostics (q5 pull-mode freeze): these tests reproduce the
// lock-shape of the freeze — 30 pull rule tasks share the window log read lock
// while the single-writer actor takes the write lock on append — and assert the
// writer is not starved.
// ---------------------------------------------------------------------------

/// A platform `RwLock` must not starve a writer under a sustained read burst:
/// q5 runs 30 pull rule tasks that read the shared window log concurrently
/// against one actor writer. If the writer starves, append stalls, the 64 MiB
/// window byte budget exhausts, and the whole pipeline freezes. This test
/// measures the writer's worst-case wait under a 30-reader burst.
#[test]
fn rwlock_writer_not_starved_by_readers() {
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::thread;
    use std::time::{Duration, Instant};

    let lock = Arc::new(RwLock::new(0u64));
    let stop = Arc::new(AtomicBool::new(false));

    // 30 readers: brief read + a short "rule processing" pause, mirroring the
    // pull-loop's read_since_with_shard followed by batch processing.
    let mut readers = Vec::new();
    for _ in 0..30 {
        let lock = Arc::clone(&lock);
        let stop = Arc::clone(&stop);
        readers.push(thread::spawn(move || {
            while !stop.load(AtomicOrdering::Relaxed) {
                let _g = lock.read().unwrap();
                thread::sleep(Duration::from_micros(50));
            }
        }));
    }

    let mut max_wait = Duration::ZERO;
    let mut total = Duration::ZERO;
    let n = 200u64;
    for _ in 0..n {
        let t0 = Instant::now();
        let mut w = lock.write().unwrap();
        let wait = t0.elapsed();
        max_wait = max_wait.max(wait);
        total += wait;
        *w += 1;
        drop(w);
    }

    stop.store(true, AtomicOrdering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    let avg = total / n as u32;
    // If the platform starves the writer, max_wait grows unboundedly under a
    // continuous read burst. 500ms is generous for a non-starving lock.
    assert!(
        max_wait < Duration::from_millis(500),
        "writer starved: max write-lock wait {max_wait:?} (avg {avg:?})"
    );
}

/// `read_since_with_shard` must return the correct per-shard row subset. This
/// also pins the current behaviour: the returned `Arc<Vec<u32>>` is a **deep
/// copy** of the stored subset (the stored type is `Arc<Vec<Vec<u32>>>`, so a
/// zero-copy `Arc::clone` of the inner list is not yet possible). The deep copy
/// runs inside the log read lock; under 30 pull tasks it lengthens every read
/// critical section and amplifies the q5 pull-freeze.
#[test]
fn read_since_with_shard_returns_correct_subset() {
    let schema = test_schema();
    let per_shard: Arc<Vec<Vec<u32>>> = Arc::new(vec![vec![0, 2], vec![1, 3]]);
    let win = Window::new(
        WindowParams {
            name: "sharded".into(),
            schema,
            time_col_index: Some(0),
            over: Duration::from_secs(3600),
            materialize_fields: None,
            defer_materialization: false,
        },
        test_config(usize::MAX),
    );
    win.append_sized(
        make_batch(win.schema(), &[1_000_000_000, 2_000_000_000], &[10, 20]),
        4096,
        Some(Arc::clone(&per_shard)),
    )
    .unwrap();

    let (_, rows, _, _) = win.read_since_with_shard(0, Some(0));
    let returned = rows.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(returned.len(), 1, "one batch → one shard subset");
    assert_eq!(
        returned[0].as_ref().as_slice(),
        &[0u32, 2],
        "shard 0 must see its own row indices"
    );

    // Unsharded pull returns `None` for every batch (whole-batch processing).
    let (_, rows, _, _) = win.read_since_with_shard(0, None);
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].is_none(),
        "unsharded pull must not request a shard subset"
    );
}
