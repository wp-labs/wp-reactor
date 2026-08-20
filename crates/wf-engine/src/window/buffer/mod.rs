mod cursor;
mod eviction;
mod types;
mod watermark;

#[cfg(test)]
mod tests;

pub use types::{AppendOutcome, WindowParams};

use std::collections::{BTreeMap, HashSet};
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, MapArray, StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, IntervalUnit, SchemaRef};
use arrow::record_batch::RecordBatch;
use orion_error::conversion::ToStructError;
use smol_str::SmolStr;
use wf_config::WindowConfig;

use crate::error::{CoreReason, CoreResult};
use crate::match_engine::event_bridge::extract_field_value;
use crate::match_engine::{Event, JoinKey, JoinRow, Value, batch_raw_ts_nanos, build_field_index};
use crate::window::WindowProgress;

use types::TimedBatch;

/// Hash index for join lookups: maps a scalar key value to columnar row
/// locators (`IndexedRow`). Maintained incrementally on append/evict/expire,
/// with **no per-row `Event` materialization** — the index holds `(batch, row)`
/// and reads fields on demand, so join-target windows stay columnar. Only
/// present on windows configured as join targets (`set_join_key`).
pub(super) struct JoinIndex {
    key_field: SmolStr,
    /// The window's `materialize_fields` projection: enrich reads only these
    /// columns from the joined rows. `None` = all columns.
    projection: Option<Arc<HashSet<String>>>,
    /// Columnar row locators per key. `ts_nanos` is `None` for rows without a
    /// `Timestamp(Ns)` time value (valid snapshot-join rows, skipped by asof).
    by_key: crate::match_engine::EngineHashMap<JoinKey, Vec<IndexedRow>>,
}

/// A columnar row locator: `(batch, row)` plus the batch-level field index and
/// the row's raw timestamp. The join index holds these instead of materialized
/// `Event`s.
struct IndexedRow {
    ts_nanos: Option<i64>,
    batch: Arc<RecordBatch>,
    row: usize,
    index: Arc<crate::match_engine::FieldIndex>,
}

impl JoinIndex {
    /// Index every row of `batch` by its `key_field` value. Reads the key column
    /// straight from the Arrow batch through the same [`extract_field_value`]
    /// conversion the eager `Event` path uses, so the produced keys are
    /// byte-identical to the previous materialized-index behavior.
    fn index_batch(&mut self, batch: &Arc<RecordBatch>, ts_list: &[Option<i64>]) {
        let Ok(col_idx) = batch.schema().index_of(self.key_field.as_str()) else {
            return;
        };
        let schema = batch.schema();
        let field = schema.field(col_idx);
        let col = batch.column(col_idx);
        let index = build_field_index(batch);
        for (row, ts) in ts_list.iter().enumerate() {
            if col.is_null(row) {
                continue;
            }
            let Some(value) = extract_field_value(field, col.as_ref(), row) else {
                continue;
            };
            let Some(key) = JoinKey::from_value(&value) else {
                continue;
            };
            self.by_key.entry(key).or_default().push(IndexedRow {
                ts_nanos: *ts,
                batch: Arc::clone(batch),
                row,
                index: Arc::clone(&index),
            });
        }
    }

    /// Remove every row belonging to `batch` (matched by `Arc` pointer
    /// identity — the index holds the same `Arc<RecordBatch>` the log does).
    fn remove_batch(&mut self, batch: &Arc<RecordBatch>) {
        for rows in self.by_key.values_mut() {
            rows.retain(|r| !Arc::ptr_eq(&r.batch, batch));
        }
    }

    /// Snapshot-join view: every indexed row for `key`, as a columnar [`JoinRow`].
    fn lookup(&self, key: &JoinKey) -> Option<Vec<JoinRow>> {
        self.by_key
            .get(key)
            .map(|rows| rows.iter().map(|r| self.row_to_join_row(r)).collect())
    }

    /// Asof-join view: only the timestamped rows for `key`, as `(raw_ts, row)`.
    fn lookup_timestamped(&self, key: &JoinKey) -> Option<Vec<(i64, JoinRow)>> {
        self.by_key.get(key).map(|rows| {
            rows.iter()
                .filter_map(|r| r.ts_nanos.map(|ts| (ts, self.row_to_join_row(r))))
                .collect()
        })
    }

    fn row_to_join_row(&self, r: &IndexedRow) -> JoinRow {
        JoinRow::Columnar {
            batch: Arc::clone(&r.batch),
            row: r.row,
            index: Arc::clone(&r.index),
            projection: self.projection.clone(),
        }
    }
}

/// A time-ordered buffer of Arrow RecordBatches with eviction support.
///
/// Batches are appended by sequence number and evicted from the front, either
/// by time expiry or memory pressure. The ordered log is a
/// `RwLock<BTreeMap<u64, TimedBatch>>`:
///
/// * **Writers** (the window actor's append path, the periodic evictor, and
///   the inline commit path used by file sources / tests) take the write
///   lock. Removal from a `BTreeMap` drops the `TimedBatch` — and its Arrow
///   batch plus pre-parsed events — **eagerly**, unlike the lock-free
///   `crossbeam-skiplist` log this replaced, whose `remove` only unlinked
///   the node and deferred the value's destructor into crossbeam-epoch
///   garbage bags (a quiet system never advanced the epoch, so ~6M evicted
///   events stayed resident — the 2026-08-16 RSS regression).
/// * **Readers** (cursor-based `events_since`/`read_since`, `snapshot`,
///   join-index setup) take the read lock and clone `Arc` handles out; the
///   lock is held only for the clone, never for downstream processing.
/// * In the production wiring (push-mode rules) no reader touches the log on
///   the hot path at all — the window actor broadcasts parsed events through
///   rule channels — so the write lock is effectively uncontended.
///
/// Lock ordering: a path may hold the log lock and then take `join_index`;
/// the reverse order never occurs (`set_join_key` releases the log lock
/// before indexing into `join_index`).
///
/// The optional join index (only present on windows configured as join
/// targets via [`Self::set_join_key`]) is a hash map that needs interior
/// mutability on both insert and eviction, so it keeps a dedicated fine-grained
/// lock behind an `AtomicBool` fast path — windows that are not join targets
/// (the common case) never touch it.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
pub struct Window {
    pub(super) name: String,
    pub(super) schema: SchemaRef,
    pub(super) time_col_index: Option<usize>,
    pub(super) over: Duration,
    pub(super) config: WindowConfig,
    /// Time-ordered append log: batch sequence number → batch. Guarded by an
    /// `RwLock` — see the struct docs for the concurrency contract. Removal
    /// drops the value eagerly (no deferred reclamation).
    log: RwLock<BTreeMap<u64, TimedBatch>>,
    /// Next sequence number to assign to an appended batch.
    next_seq: AtomicU64,
    /// Monotonic event-time watermark (`fetch_max` on append).
    watermark_nanos: AtomicI64,
    /// Aggregate retained content bytes (approximate under concurrency —
    /// exact in the single-writer steady state).
    current_bytes: AtomicUsize,
    /// Aggregate row count (approximate under concurrency).
    total_rows: AtomicUsize,
    /// Number of batches currently in the log.
    batch_count: AtomicUsize,
    /// Monotonic content-generation counter: bumped once per successful append
    /// (which subsumes any accompanying memory eviction — the only other log
    /// mutation). `window.has()` / join snapshot caches key off this to
    /// invalidate stale distinct-value sets without a per-call scan.
    generation: AtomicU64,
    /// Fast path: whether a join index has been configured. Non-join windows
    /// (the common case) skip the join-index lock entirely.
    join_enabled: AtomicBool,
    /// Optional hash index for join lookups (see `set_join_key`). Only
    /// mutated while `join_enabled` is true.
    join_index: RwLock<Option<JoinIndex>>,
    /// Optional per-event field whitelist (see `WindowParams`). Immutable
    /// after construction — readers (`Router::route_parse`) access it with no
    /// synchronization at all.
    pub(super) materialize_fields: Option<Arc<HashSet<String>>>,
    /// L2 deferred materialization (see `WindowParams`). Immutable after
    /// construction.
    pub(super) defer_materialization: bool,
    /// Consumption progress (ack floor) for this window, injected by the
    /// registry. Per-window memory eviction respects this floor so a slow
    /// pull consumer never loses unread batches (`None` until the registry
    /// wires it — treated as "no consumers", i.e. everything evictable).
    progress: RwLock<Option<Arc<WindowProgress>>>,
}

impl Window {
    /// Create a new empty window.
    pub fn new(params: WindowParams, config: WindowConfig) -> Self {
        let materialize_fields = params.materialize_fields.clone();
        let defer_materialization = params.defer_materialization;
        Self {
            name: params.name,
            schema: params.schema,
            time_col_index: params.time_col_index,
            over: params.over,
            config,
            log: RwLock::new(BTreeMap::new()),
            next_seq: AtomicU64::new(0),
            watermark_nanos: AtomicI64::new(i64::MIN),
            current_bytes: AtomicUsize::new(0),
            total_rows: AtomicUsize::new(0),
            batch_count: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            join_enabled: AtomicBool::new(false),
            join_index: RwLock::new(None),
            materialize_fields,
            defer_materialization,
            progress: RwLock::new(None),
        }
    }

    /// Wire this window to its consumption-progress table. Called once by the
    /// registry right after construction, so per-window memory eviction can
    /// respect the ack floor (see [`Self::min_acked`]).
    pub(crate) fn set_progress(&self, progress: Arc<WindowProgress>) {
        *self.progress.write().expect("progress lock poisoned") = Some(progress);
    }

    /// Consumption floor for this window: the lowest acked `seq + 1` across
    /// all live consumers, or `u64::MAX` when there are none (everything is
    /// evictable). Per-window memory eviction uses this to avoid dropping a
    /// batch a slow pull rule has not yet read.
    fn min_acked(&self) -> u64 {
        self.progress
            .read()
            .expect("progress lock poisoned")
            .as_ref()
            .map(|p| p.min_acked())
            .unwrap_or(u64::MAX)
    }

    /// Configure this window as a join target: build a hash index on `key_field`
    /// and index any rows already buffered. Called by the runtime after rules
    /// are loaded (join target windows are only known from rule plans).
    pub fn set_join_key(&self, key_field: String) {
        let key_field = SmolStr::new(&key_field);
        let mut index = JoinIndex {
            key_field,
            projection: self.materialize_fields.clone(),
            by_key: crate::match_engine::EngineHashMap::default(),
        };
        // Read the log under its read lock; the guard is released before the
        // join-index write lock is taken (lock ordering: log → join_index,
        // never the reverse). The index holds columnar row locators — no
        // per-row `Event` materialization.
        let existing: Vec<(Arc<RecordBatch>, Vec<Option<i64>>)> = {
            let log = self.log.read().expect("window log lock poisoned");
            log.values()
                .map(|tb| (Arc::clone(&tb.batch), self.raw_ts_list(tb)))
                .collect()
        };
        for (batch, ts_list) in &existing {
            index.index_batch(batch, ts_list);
        }
        self.join_enabled.store(true, Ordering::Release);
        *self.join_index.write().expect("join index lock poisoned") = Some(index);
    }

    /// O(1) lookup of rows whose `key_field` equals `key`, as columnar
    /// [`JoinRow`]s. `Some(empty)` if this window is indexed but the key has no
    /// matching rows; `None` if it has no join index (not a join target — the
    /// caller falls back to a snapshot scan).
    pub fn join_lookup(&self, key: &JoinKey) -> Option<Vec<JoinRow>> {
        if !self.join_enabled.load(Ordering::Acquire) {
            return None;
        }
        Some(
            self.join_index
                .read()
                .expect("join index lock poisoned")
                .as_ref()?
                .lookup(key)
                .unwrap_or_default(),
        )
    }

    /// O(1) timestamped lookup for the asof-join path: rows whose `key_field`
    /// equals `key`, as `(raw_ts_nanos, JoinRow)` — rows without a
    /// `Timestamp(Ns)` time value are skipped. `Some(empty)` when indexed but
    /// the key has no timestamped rows; `None` when there is no join index
    /// (caller falls back to a timestamped snapshot scan).
    pub fn join_lookup_timestamped(&self, key: &JoinKey) -> Option<Vec<(i64, JoinRow)>> {
        if !self.join_enabled.load(Ordering::Acquire) {
            return None;
        }
        Some(
            self.join_index
                .read()
                .expect("join index lock poisoned")
                .as_ref()?
                .lookup_timestamped(key)
                .unwrap_or_default(),
        )
    }

    /// Raw `Timestamp(Ns)` time values for every row of a batch, aligned with
    /// the batch's row order (row `i` → `ts_list[i]`). `None` for null / non-Ts
    /// rows (the asof path skips them).
    fn raw_ts_list(&self, tb: &TimedBatch) -> Vec<Option<i64>> {
        match self.time_col_index {
            Some(tc) => (0..tb.batch.num_rows())
                .map(|row| batch_raw_ts_nanos(&tb.batch, tc, row))
                .collect(),
            None => vec![None; tb.batch.num_rows()],
        }
    }

    /// Test-only: whether any buffered batch has materialized its
    /// `parsed_events`. The columnar join index must never trigger this — a
    /// join-target window with no rule subscription stays fully columnar.
    #[cfg(test)]
    fn any_parsed_events_materialized(&self) -> bool {
        let log = self.log.read().expect("log lock poisoned");
        log.values().any(|tb| tb.parsed_events.get().is_some())
    }

    /// Append a RecordBatch to this window.
    ///
    /// Empty batches are silently skipped. Returns an error if the batch
    /// schema does not match the window schema. After appending, memory
    /// eviction runs if `current_bytes > max_window_bytes`.
    pub fn append(&self, batch: RecordBatch) -> CoreResult<()> {
        self.append_inner(batch, None, None, None).map(|_| ())
    }

    /// Append a RecordBatch whose events were already parsed *outside* the
    /// window (by the router). Rule tasks then read the pre-parsed `Arc`
    /// with no `OnceLock` contention among the concurrent rule tasks.
    pub fn append_parsed(
        &self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
    ) -> CoreResult<()> {
        self.append_inner(batch, Some(parsed_events), None, None)
            .map(|_| ())
    }

    /// Append a RecordBatch whose events *and content byte size* were precomputed
    /// by the caller (the R2 parse worker), so the O(rows×cols) accounting runs
    /// in parallel rather than on the ordered commit path. Returns the sequence
    /// number assigned to the appended batch.
    pub fn append_parsed_sized(
        &self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
        byte_size: usize,
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
    ) -> CoreResult<u64> {
        self.append_inner(batch, Some(parsed_events), Some(byte_size), shard_rows)
    }

    /// Append a RecordBatch *without* pre-parsed events but *with* a
    /// precomputed content byte size (R2 parse worker) **and** the parse-side
    /// precomputed columnar shard partition (`shard_rows`, the P2 zero
    /// re-partition data). Used by the columnar/deferred commit path (pull
    /// model sharded match rules) where `route_parse` leaves events `None`
    /// but still carries `shard_rows`. The prior `(None, _)` arm of
    /// `append_with_watermark_inner` funnelled here via `self.append(batch)`,
    /// which dropped `shard_rows` — leaving every pull shard to process the
    /// whole batch (Q2 30M pull over-production, ~9×). Returns the sequence
    /// number assigned to the appended batch.
    pub fn append_sized(
        &self,
        batch: RecordBatch,
        byte_size: usize,
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
    ) -> CoreResult<u64> {
        self.append_inner(batch, None, Some(byte_size), shard_rows)
    }

    fn append_inner(
        &self,
        batch: RecordBatch,
        parsed_events: Option<Arc<Vec<Arc<Event>>>>,
        byte_size: Option<usize>,
        shard_rows: Option<Arc<Vec<Vec<u32>>>>,
    ) -> CoreResult<u64> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        // Accept batches that contain at least the window's fields (superset OK).
        if !self.schema.fields().iter().all(|f| {
            batch
                .schema()
                .field_with_name(f.name())
                .is_ok_and(|bf| bf.data_type() == f.data_type())
        }) {
            return CoreReason::DataFormat
                .to_err()
                .with_detail(format!(
                    "schema mismatch: window {:?} expects {:?}, got {:?}",
                    self.name,
                    self.schema,
                    batch.schema()
                ))
                .err();
        }

        let event_time_range = self.extract_time_range(&batch);
        let row_count = batch.num_rows();
        // Account by *content* bytes, not Arrow buffer allocations: IPC decode
        // inflates `get_array_memory_size` with padding (~7x for decoded arrays),
        // so a single padded frame can exceed max_window_bytes and be silently
        // dropped even though its data is small (wp-labs/wp-reactor#18).
        let byte_size = byte_size.unwrap_or_else(|| content_bytes(&batch));
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);

        let parsed_lock = std::sync::OnceLock::new();
        if let Some(events) = parsed_events {
            // Ignore the error: a freshly-created OnceLock is always empty.
            let _ = parsed_lock.set(events);
        }

        self.current_bytes.fetch_add(byte_size, Ordering::Relaxed);
        self.total_rows.fetch_add(row_count, Ordering::Relaxed);
        self.batch_count.fetch_add(1, Ordering::Relaxed);

        // Memory eviction: pop oldest batches while over budget.
        let max_bytes = self.config.max_window_bytes.as_bytes();
        // Per-window eviction is floor-respecting: only drop batches every
        // live consumer has already acked (`seq < ack_floor`). An unacked
        // front batch stops the sweep — the window may transiently exceed
        // `max_window_bytes` rather than lose unread pull data (the periodic
        // evictor reclaims it once consumers advance).
        let ack_floor = self.min_acked();
        let mut evicted_bytes = 0usize;
        let mut evicted_rows = 0usize;
        {
            let mut log = self.log.write().expect("window log lock poisoned");
            log.insert(
                seq,
                TimedBatch {
                    batch: Arc::new(batch),
                    event_time_range,
                    ingested_at: Instant::now(),
                    row_count,
                    byte_size,
                    seq,
                    parsed_events: parsed_lock,
                    shard_rows,
                },
            );
            while self.current_bytes.load(Ordering::Relaxed) > max_bytes {
                let Some((&key, tb)) = log.first_key_value() else {
                    break;
                };
                // Unacked front batch: stop the sweep — never drop a batch a
                // live consumer has not yet read.
                if tb.seq >= ack_floor {
                    break;
                }
                // `BTreeMap::remove` returns the owned value: dropping it
                // destroys the Arrow batch and parsed events eagerly — no
                // deferred (epoch-GC) reclamation to drive.
                let Some(tb) = log.remove(&key) else {
                    continue;
                };
                let byte_size = tb.byte_size;
                let row_count = tb.row_count;
                self.remove_batch_from_index(&tb);
                drop(tb);
                self.current_bytes.fetch_sub(byte_size, Ordering::Relaxed);
                self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
                self.batch_count.fetch_sub(1, Ordering::Relaxed);
                evicted_bytes += byte_size;
                evicted_rows += row_count;
            }

            // Index the newly appended batch (after eviction, so rows evicted
            // by the incoming batch aren't kept in the index).
            if self.join_enabled.load(Ordering::Acquire)
                && let Some(tb) = log.get(&seq)
                && let Some(idx) = self
                    .join_index
                    .write()
                    .expect("join index lock poisoned")
                    .as_mut()
            {
                let ts_list = self.raw_ts_list(tb);
                idx.index_batch(&tb.batch, &ts_list);
            }
        }
        if evicted_rows > 0 {
            // The incoming batch was dropped (in whole or part) because it pushed
            // the window over max_window_bytes — e.g. a single oversized Arrow
            // frame exceeds the cap and is silently discarded. Log it so rules
            // that stop seeing events aren't a mystery.
            log::warn!(
                "window `{}` dropped {} row(s) / {} bytes in memory eviction (max_window_bytes={} bytes, incoming batch = {} rows / {} bytes)",
                self.name,
                evicted_rows,
                evicted_bytes,
                max_bytes,
                row_count,
                byte_size,
            );
        }

        // Content changed (append + any accompanying eviction): bump the
        // generation so `window.has()` / snapshot caches invalidate.
        self.generation.fetch_add(1, Ordering::Relaxed);

        Ok(seq)
    }

    /// Monotonic content-generation counter (see the struct field docs).
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Remove an evicted batch's rows from the join index (if configured).
    fn remove_batch_from_index(&self, evicted: &TimedBatch) {
        if !self.join_enabled.load(Ordering::Acquire) {
            return;
        }
        if let Some(idx) = self
            .join_index
            .write()
            .expect("join index lock poisoned")
            .as_mut()
        {
            idx.remove_batch(&evicted.batch);
        }
    }

    /// Return a snapshot of all current batches.
    ///
    /// `RecordBatch::clone()` is Arc-ref-counted — no data copy occurs.
    /// The returned `Vec` remains valid even if the window is subsequently
    /// mutated.
    pub fn snapshot(&self) -> Vec<RecordBatch> {
        let log = self.log.read().expect("window log lock poisoned");
        log.values().map(|tb| tb.batch.as_ref().clone()).collect()
    }

    /// Return a snapshot of the batches with `seq <= max_seq`.
    ///
    /// M2 (seq-watermark consistency, window-actor-pull-model.md §3.5): when a
    /// rule task is processing batch N, its `window_lookup` must see only the
    /// batches this rule already pulled (`seq <= N`), never the batches the
    /// actor may already have appended past it. `None` returns the full log
    /// (identical to [`Window::snapshot`]) — the legacy view used when no
    /// seq watermark is enforced (push mode / no-join rules).
    ///
    /// `RecordBatch::clone()` is Arc-ref-counted — no data copy occurs.
    pub fn snapshot_up_to(&self, max_seq: Option<u64>) -> Vec<RecordBatch> {
        let log = self.log.read().expect("window log lock poisoned");
        match max_seq {
            Some(n) => log
                .range(..=n)
                .map(|(_, tb)| tb.batch.as_ref().clone())
                .collect(),
            None => log.values().map(|tb| tb.batch.as_ref().clone()).collect(),
        }
    }

    pub fn memory_usage(&self) -> usize {
        self.current_bytes.load(Ordering::Relaxed)
    }

    pub fn max_window_bytes(&self) -> usize {
        self.config.max_window_bytes.as_bytes()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn total_rows(&self) -> usize {
        self.total_rows.load(Ordering::Relaxed)
    }

    pub fn batch_count(&self) -> usize {
        self.batch_count.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.batch_count() == 0
    }

    /// Index of the time column in the schema, if present.
    pub fn time_col_index(&self) -> Option<usize> {
        self.time_col_index
    }

    /// Whether rule tasks defer per-row event materialization (L2).
    pub fn defer_materialization(&self) -> bool {
        self.defer_materialization
    }

    /// Field projection used when materializing events from this window's
    /// batches (L2 deferred materialization). `None` materializes every schema
    /// column. Exposed for the pull-model rule tasks, which read the raw
    /// `RecordBatch` and need the same projection the columnar push path uses.
    pub fn materialize_fields(&self) -> Option<&Arc<HashSet<String>>> {
        self.materialize_fields.as_ref()
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

/// Estimate the retained *content* bytes of a batch — the actual data size, not
/// the Arrow buffer allocations (which IPC decode inflates with padding).
///
/// Used for window memory accounting so a single padded frame doesn't exceed
/// `max_window_bytes` and get dropped by memory eviction even though its data
/// is small (wp-labs/wp-reactor#18).
pub fn content_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|col| column_content_bytes(col.as_ref()))
        .sum()
}

fn column_content_bytes(col: &dyn Array) -> usize {
    let n = col.len();
    match col.data_type() {
        DataType::Null => 0,
        DataType::Boolean => bitmap_bytes(n) * 2, // data + validity bitmaps
        // Fixed-width values: width × rows.
        DataType::Int8 | DataType::UInt8 => n,
        DataType::Int16 | DataType::UInt16 => n * 2,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_) => n * 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(..)
        | DataType::Duration(_) => n * 8,
        DataType::Interval(IntervalUnit::MonthDayNano) => n * 16,
        DataType::Interval(_) => n * 8,
        DataType::Decimal128(..) => n * 16,
        DataType::Decimal256(..) => n * 32,
        DataType::Utf8 => utf8_content(
            n,
            col.as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 column"),
        ),
        DataType::LargeUtf8 => large_utf8_content(
            n,
            col.as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large utf8 column"),
        ),
        DataType::Binary => binary_content(
            n,
            col.as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary column"),
        ),
        DataType::LargeBinary => large_binary_content(
            n,
            col.as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("large binary column"),
        ),
        DataType::FixedSizeBinary(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("fixed-size binary column");
            n * arr.value_length() as usize
        }
        DataType::Struct(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct column");
            // The struct's own validity bitmap plus children.
            bitmap_bytes(n)
                + arr
                    .columns()
                    .iter()
                    .map(|c| column_content_bytes(c.as_ref()))
                    .sum::<usize>()
        }
        DataType::List(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("list column");
            // value(i) slices the child; a null row yields an empty slice → 0 bytes.
            bitmap_bytes(n)
                + offsets_bytes(n, 4)
                + (0..n)
                    .map(|i| column_content_bytes(arr.value(i).as_ref()))
                    .sum::<usize>()
        }
        DataType::LargeList(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("large list column");
            bitmap_bytes(n)
                + offsets_bytes(n, 8)
                + (0..n)
                    .map(|i| column_content_bytes(arr.value(i).as_ref()))
                    .sum::<usize>()
        }
        DataType::FixedSizeList(_, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("fixed-size list column");
            bitmap_bytes(n)
                + (0..n)
                    .map(|i| column_content_bytes(arr.value(i).as_ref()))
                    .sum::<usize>()
        }
        DataType::Map(..) => {
            let arr = col.as_any().downcast_ref::<MapArray>().expect("map column");
            // Offsets + validity, plus the full key/value entries (unreferenced
            // entry slots are included — conservative).
            bitmap_bytes(n)
                + offsets_bytes(n, 4)
                + column_content_bytes(arr.keys().as_ref())
                + column_content_bytes(arr.values().as_ref())
        }
        // Dictionary and anything else: upper-bound estimate (dictionary values
        // are shared, so this overcounts — the safe direction for eviction).
        _ => n * 8,
    }
}

/// Bytes for a bit-packed bitmap over `n` rows.
fn bitmap_bytes(n: usize) -> usize {
    n.div_ceil(8)
}

/// Bytes for an offset buffer of `(n + 1)` entries, `width` bytes each.
fn offsets_bytes(n: usize, width: usize) -> usize {
    (n + 1) * width
}

/// Content bytes of a utf8 column: `(n + 1)` i32 offsets + string payload.
///
/// O(1) payload: `offsets[n] - offsets[0]` (offsets only advance by actual
/// value lengths — null slots carry the previous offset forward), so no
/// per-row `str::len` walk is needed. Called twice per batch on the hot path
/// ([`push_decoded_batch`] + [`Router::route_parse`]); the walk version cost
/// ~2×100k iterator steps per string column per batch at 44M EPS.
fn utf8_content(n: usize, arr: &StringArray) -> usize {
    offsets_bytes(n, 4) + utf8_payload_bytes(arr.value_offsets())
}

fn utf8_payload_bytes(offsets: &[i32]) -> usize {
    let first = offsets.first().copied().unwrap_or(0);
    let last = offsets.last().copied().unwrap_or(first);
    (last as usize).saturating_sub(first as usize)
}

fn large_utf8_content(n: usize, arr: &LargeStringArray) -> usize {
    offsets_bytes(n, 8) + large_utf8_payload_bytes(arr.value_offsets())
}

fn large_utf8_payload_bytes(offsets: &[i64]) -> usize {
    let first = offsets.first().copied().unwrap_or(0);
    let last = offsets.last().copied().unwrap_or(first);
    (last as usize).saturating_sub(first as usize)
}

/// Content bytes of a binary column: `(n + 1)` i32 offsets + payload.
/// O(1) payload via offset span, same as utf8.
fn binary_content(n: usize, arr: &BinaryArray) -> usize {
    offsets_bytes(n, 4) + utf8_payload_bytes(arr.value_offsets())
}

fn large_binary_content(n: usize, arr: &LargeBinaryArray) -> usize {
    offsets_bytes(n, 8) + large_utf8_payload_bytes(arr.value_offsets())
}

// ---------------------------------------------------------------------------
// Parsed-event memory accounting
// ---------------------------------------------------------------------------

/// Estimate the retained bytes of parsed events: each event is an
/// `HashMap<SmolStr, Value>` (a foldhash table). Structured `object` fields
/// decoded from JSON become nested `EngineHashMap`/`Vec` allocations with
/// fixed per-entry overhead (key struct + bucket + hash/ctrl), so a window
/// that also retains these events holds several× the JSON string bytes it
/// accounts for via [`content_bytes`] — memory eviction then fires far past
/// the real water level (wp-labs/wp-reactor#20: `current_bytes` ≈ cap while
/// RSS ran to 2× max).
///
/// The estimate errs toward overcount (safe direction for eviction): it uses
/// `capacity()`-based table sizes and a per-entry hash/ctrl allowance, so a
/// window never retains *more* real memory than its accounting reports.
pub fn events_bytes(events: &[Arc<Event>]) -> usize {
    events.iter().map(|e| event_bytes(e)).sum()
}

/// Retained bytes of one parsed [`Event`]: the `Event`/`HashMap` header, the
/// bucket table, and every nested value's heap payload.
fn event_bytes(e: &Event) -> usize {
    // size_of::<Event>() covers the foldhash table header itself.
    size_of::<Event>()
        + map_heap_bytes(
            e.fields.capacity(),
            size_of::<SmolStr>(),
            size_of::<Value>(),
        )
        + e.fields
            .iter()
            .map(|(k, v)| smol_str_heap_bytes(k) + value_heap_bytes(v))
            .sum::<usize>()
}

/// Extra heap bytes of a [`Value`] *beyond* the enum's inline storage (the enum
/// struct — including an inline `Vec`/`HashMap` header — is already charged by
/// the containing bucket via `map_heap_bytes`). Recurses into nested containers.
fn value_heap_bytes(v: &Value) -> usize {
    match v {
        Value::Number(_) | Value::Bool(_) => 0,
        Value::Str(s) => smol_str_heap_bytes(s),
        Value::Array(items) => {
            items.capacity() * size_of::<Value>()
                + items.iter().map(value_heap_bytes).sum::<usize>()
        }
        Value::Object(fields) => {
            map_heap_bytes(fields.capacity(), size_of::<SmolStr>(), size_of::<Value>())
                + fields
                    .iter()
                    .map(|(k, v)| smol_str_heap_bytes(k) + value_heap_bytes(v))
                    .sum::<usize>()
        }
    }
}

/// Heap allocation of a `HashMap` bucket table + control bytes.
///
/// std's swiss-table layout stores one `u64` hash plus key+value per bucket,
/// one control byte per bucket (SIMD-group padded), and keeps some growth
/// slack. The flat `+ 16` per entry covers control bytes + padding + slack and
/// errs conservative (overcount).
fn map_heap_bytes(capacity: usize, key_size: usize, value_size: usize) -> usize {
    capacity * (size_of::<usize>() + key_size + value_size + 16)
}

/// Heap bytes of a `SmolStr` beyond its inline struct: only strings that
/// outgrew the inline buffer allocate (payload + NUL).
fn smol_str_heap_bytes(s: &SmolStr) -> usize {
    if s.is_heap_allocated() {
        s.len() + 1
    } else {
        0
    }
}
