mod cursor;
mod eviction;
mod types;
mod watermark;

#[cfg(test)]
mod tests;

pub use types::{AppendOutcome, WindowParams};

use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, IntervalUnit, SchemaRef};
use arrow::record_batch::RecordBatch;
use crossbeam_skiplist::SkipMap;
use orion_error::conversion::ToStructError;
use smol_str::SmolStr;
use wf_config::WindowConfig;

use crate::error::{CoreReason, CoreResult};
use crate::match_engine::{Event, JoinKey, Value};

use types::TimedBatch;

/// Hash index for join lookups: maps a scalar key value to the parsed events
/// holding it. Maintained incrementally on append/evict/expire. Only present on
/// windows configured as join targets (`set_join_key`).
pub(super) struct JoinIndex {
    key_field: SmolStr,
    by_key: crate::match_engine::EngineHashMap<JoinKey, Vec<Arc<Event>>>,
}

impl JoinIndex {
    fn index_event(&mut self, ev: &Arc<Event>) {
        if let Some(key) = ev.fields.get(&self.key_field).and_then(JoinKey::from_value) {
            self.by_key.entry(key).or_default().push(Arc::clone(ev));
        }
    }

    fn remove_event(&mut self, ev: &Arc<Event>) {
        if let Some(key) = ev.fields.get(&self.key_field).and_then(JoinKey::from_value) {
            if let Some(v) = self.by_key.get_mut(&key) {
                v.retain(|e| !Arc::ptr_eq(e, ev));
            }
        }
    }

    fn index_batch(&mut self, events: &[Arc<Event>]) {
        for ev in events {
            self.index_event(ev);
        }
    }

    fn remove_batch(&mut self, events: &[Arc<Event>]) {
        for ev in events {
            self.remove_event(ev);
        }
    }

    fn lookup(&self, key: &JoinKey) -> Option<Vec<Arc<Event>>> {
        self.by_key.get(key).cloned()
    }
}

/// A time-ordered buffer of Arrow RecordBatches with eviction support.
///
/// Batches are appended by sequence number and evicted from the front, either
/// by time expiry or memory pressure. The whole data plane is **lock-free**:
/// the ordered log is a `crossbeam_skiplist::SkipMap<u64, TimedBatch>`
/// (lock-free insert / ordered iteration / front removal) and every counter
/// (watermark, bytes, rows, seq) is an atomic. Append, cursor reads, snapshots
/// and eviction never block each other.
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
    /// Lock-free ordered append log: batch sequence number → batch. Insert
    /// (append), ordered iteration (cursor reads / snapshots) and front
    /// removal (eviction) are all lock-free.
    log: SkipMap<u64, TimedBatch>,
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
}

impl Window {
    /// Create a new empty window.
    pub fn new(params: WindowParams, config: WindowConfig) -> Self {
        let materialize_fields = params.materialize_fields.clone();
        Self {
            name: params.name,
            schema: params.schema,
            time_col_index: params.time_col_index,
            over: params.over,
            config,
            log: SkipMap::new(),
            next_seq: AtomicU64::new(0),
            watermark_nanos: AtomicI64::new(i64::MIN),
            current_bytes: AtomicUsize::new(0),
            total_rows: AtomicUsize::new(0),
            batch_count: AtomicUsize::new(0),
            join_enabled: AtomicBool::new(false),
            join_index: RwLock::new(None),
            materialize_fields,
        }
    }

    /// Configure this window as a join target: build a hash index on `key_field`
    /// and index any rows already buffered. Called by the runtime after rules
    /// are loaded (join target windows are only known from rule plans).
    pub fn set_join_key(&self, key_field: String) {
        let key_field = SmolStr::new(&key_field);
        let mut index = JoinIndex {
            key_field,
            by_key: crate::match_engine::EngineHashMap::default(),
        };
        for entry in self.log.iter() {
            let events = entry.value().events(self.materialize_fields.as_deref());
            index.index_batch(&events);
        }
        self.join_enabled.store(true, Ordering::Release);
        *self.join_index.write().expect("join index lock poisoned") = Some(index);
    }

    /// O(1) lookup of parsed events whose `key_field` equals `key`. Returns
    /// `None` if this window has no join index (not a join target).
    pub fn join_lookup(&self, key: &JoinKey) -> Option<Vec<Arc<Event>>> {
        if !self.join_enabled.load(Ordering::Acquire) {
            return None;
        }
        self.join_index
            .read()
            .expect("join index lock poisoned")
            .as_ref()?
            .lookup(key)
    }

    /// Indexed join rows as `HashMap<String, Value>` (matching `WindowLookup`
    /// row shape). `Some(empty)` if indexed but no row matches; `None` if this
    /// window has no join index (caller falls back to a snapshot scan).
    pub fn join_rows(&self, key: &JoinKey) -> Option<Vec<HashMap<String, Value>>> {
        if !self.join_enabled.load(Ordering::Acquire) {
            return None;
        }
        let events = self
            .join_index
            .read()
            .expect("join index lock poisoned")
            .as_ref()?
            .lookup(key)
            .unwrap_or_default();
        Some(
            events
                .into_iter()
                .map(|ev| {
                    ev.fields
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.clone()))
                        .collect()
                })
                .collect(),
        )
    }

    /// Append a RecordBatch to this window.
    ///
    /// Empty batches are silently skipped. Returns an error if the batch
    /// schema does not match the window schema. After appending, memory
    /// eviction runs if `current_bytes > max_window_bytes`.
    pub fn append(&self, batch: RecordBatch) -> CoreResult<()> {
        self.append_inner(batch, None, None).map(|_| ())
    }

    /// Append a RecordBatch whose events were already parsed *outside* the
    /// window (by the router). Rule tasks then read the pre-parsed `Arc`
    /// with no `OnceLock` contention among the concurrent rule tasks.
    pub fn append_parsed(
        &self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
    ) -> CoreResult<()> {
        self.append_inner(batch, Some(parsed_events), None).map(|_| ())
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
    ) -> CoreResult<u64> {
        self.append_inner(batch, Some(parsed_events), Some(byte_size))
    }

    fn append_inner(
        &self,
        batch: RecordBatch,
        parsed_events: Option<Arc<Vec<Arc<Event>>>>,
        byte_size: Option<usize>,
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
        self.log.insert(
            seq,
            TimedBatch {
                batch,
                event_time_range,
                ingested_at: Instant::now(),
                row_count,
                byte_size,
                seq,
                parsed_events: parsed_lock,
            },
        );

        self.current_bytes.fetch_add(byte_size, Ordering::Relaxed);
        self.total_rows.fetch_add(row_count, Ordering::Relaxed);
        self.batch_count.fetch_add(1, Ordering::Relaxed);

        // Memory eviction: pop oldest batches while over budget.
        let max_bytes = self.config.max_window_bytes.as_bytes();
        let mut evicted_bytes = 0usize;
        let mut evicted_rows = 0usize;
        while self.current_bytes.load(Ordering::Relaxed) > max_bytes {
            let Some(front) = self.log.front() else {
                break;
            };
            let key = *front.key();
            drop(front);
            // `remove` is keyed, so a concurrent evictor racing us on the same
            // front batch simply loses this iteration and we retry.
            let Some(evicted_entry) = self.log.remove(&key) else {
                continue;
            };
            // The node is already unlinked; read the accounting fields through
            // the returned entry before dropping it.
            let tb = evicted_entry.value();
            let byte_size = tb.byte_size;
            let row_count = tb.row_count;
            self.remove_batch_from_index(tb);
            drop(evicted_entry);
            self.current_bytes.fetch_sub(byte_size, Ordering::Relaxed);
            self.total_rows.fetch_sub(row_count, Ordering::Relaxed);
            self.batch_count.fetch_sub(1, Ordering::Relaxed);
            evicted_bytes += byte_size;
            evicted_rows += row_count;
        }

        // Index the newly appended batch (after eviction, so rows evicted by the
        // incoming batch aren't kept in the index).
        if self.join_enabled.load(Ordering::Acquire) {
            if let Some(entry) = self.log.get(&seq) {
                let events = entry.value().events(self.materialize_fields.as_deref());
                if let Some(idx) = self
                    .join_index
                    .write()
                    .expect("join index lock poisoned")
                    .as_mut()
                {
                    idx.index_batch(&events);
                }
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
            // The unlinked nodes' values (including the parsed events) are
            // deferred into this thread's crossbeam-epoch bag — drive the
            // collector so they are destroyed instead of lingering.
            reclaim_evicted_nodes();
        }

        Ok(seq)
    }

    /// Remove an evicted batch's rows from the join index (if configured).
    fn remove_batch_from_index(&self, evicted: &TimedBatch) {        if !self.join_enabled.load(Ordering::Acquire) {
            return;
        }
        if let Some(idx) = self
            .join_index
            .write()
            .expect("join index lock poisoned")
            .as_mut()
        {
            let events = evicted.events(self.materialize_fields.as_deref());
            idx.remove_batch(&events);
        }
    }

    /// Return a snapshot of all current batches.
    ///
    /// `RecordBatch::clone()` is Arc-ref-counted — no data copy occurs.
    /// The returned `Vec` remains valid even if the window is subsequently
    /// mutated.
    pub fn snapshot(&self) -> Vec<RecordBatch> {
        self.log.iter().map(|e| e.value().batch.clone()).collect()
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
    batch.columns().iter().map(|col| column_content_bytes(col.as_ref())).sum()
}

fn column_content_bytes(col: &dyn Array) -> usize {
    let n = col.len();
    match col.data_type() {
        DataType::Null => 0,
        DataType::Boolean => bitmap_bytes(n) * 2, // data + validity bitmaps
        // Fixed-width values: width × rows.
        DataType::Int8 | DataType::UInt8 => n,
        DataType::Int16 | DataType::UInt16 => n * 2,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32
        | DataType::Time32(_) => n * 4,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Date64
        | DataType::Time64(_) | DataType::Timestamp(..) | DataType::Duration(_) => n * 8,
        DataType::Interval(unit) => match unit {
            IntervalUnit::MonthDayNano => n * 16,
            _ => n * 8,
        },
        DataType::Decimal128(..) => n * 16,
        DataType::Decimal256(..) => n * 32,
        DataType::Utf8 => {
            utf8_content(n, col.as_any().downcast_ref::<StringArray>().expect("utf8 column"))
        }
        DataType::LargeUtf8 => large_utf8_content(
            n,
            col.as_any().downcast_ref::<LargeStringArray>().expect("large utf8 column"),
        ),
        DataType::Binary => {
            binary_content(n, col.as_any().downcast_ref::<BinaryArray>().expect("binary column"))
        }
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
            let arr = col.as_any().downcast_ref::<StructArray>().expect("struct column");
            // The struct's own validity bitmap plus children.
            bitmap_bytes(n) + arr.columns().iter().map(|c| column_content_bytes(c.as_ref())).sum::<usize>()
        }
        DataType::List(_) => {
            let arr = col.as_any().downcast_ref::<ListArray>().expect("list column");
            // value(i) slices the child; a null row yields an empty slice → 0 bytes.
            bitmap_bytes(n)
                + offsets_bytes(n, 4)
                + (0..n).map(|i| column_content_bytes(arr.value(i).as_ref())).sum::<usize>()
        }
        DataType::LargeList(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("large list column");
            bitmap_bytes(n)
                + offsets_bytes(n, 8)
                + (0..n).map(|i| column_content_bytes(arr.value(i).as_ref())).sum::<usize>()
        }
        DataType::FixedSizeList(_, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("fixed-size list column");
            bitmap_bytes(n)
                + (0..n).map(|i| column_content_bytes(arr.value(i).as_ref())).sum::<usize>()
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
fn utf8_content(n: usize, arr: &StringArray) -> usize {
    offsets_bytes(n, 4) + arr.iter().flatten().map(str::len).sum::<usize>()
}

fn large_utf8_content(n: usize, arr: &LargeStringArray) -> usize {
    offsets_bytes(n, 8) + arr.iter().flatten().map(str::len).sum::<usize>()
}

/// Content bytes of a binary column: `(n + 1)` i32 offsets + payload.
fn binary_content(n: usize, arr: &BinaryArray) -> usize {
    offsets_bytes(n, 4) + arr.iter().flatten().map(|b| b.len()).sum::<usize>()
}

fn large_binary_content(n: usize, arr: &LargeBinaryArray) -> usize {
    offsets_bytes(n, 8) + arr.iter().flatten().map(|b| b.len()).sum::<usize>()
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
        + map_heap_bytes(e.fields.capacity(), size_of::<SmolStr>(), size_of::<Value>())
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
    if s.is_heap_allocated() { s.len() + 1 } else { 0 }
}

/// Drive crossbeam-epoch's collector so skiplist nodes unlinked by an eviction
/// have their `TimedBatch` values destroyed **now**, not whenever some future
/// unrelated `pin()` happens to advance the epoch.
///
/// `SkipMap::remove` only unlinks a node; `Node::finalize` — which drops the
/// key and the value (the Arrow `RecordBatch` and the pre-parsed
/// `Arc<Vec<Arc<Event>>>`) — is deferred into the removing thread's
/// crossbeam-epoch garbage bag. Without this call that memory stays fully
/// referenced after eviction while the window's byte/row accounting already
/// shows it gone: in the nexmark q1 10M run ~6M evicted events (~2.3 GiB)
/// were retained this way while the window gauges read ~270 MiB
/// (wp-reactor RSS regression, 2026-08-16).
///
/// A bag sealed at epoch `E` is only droppable once the global epoch reaches
/// `E + 2`. Each `flush` seals the calling thread's local bag and attempts one
/// epoch advance, so the sequence below expires freshly sealed garbage:
/// flush (seal at `E`, advance to `E + 1`), `repin` (move our own participant
/// to `E + 1` — a guard still pinned in `E` would itself block the next
/// advance), flush (advance to `E + 2`, drop the expired bag).
///
/// The advance is best-effort: a concurrent participant pinned in an older
/// epoch defers collection to a later call (the periodic evictor retries every
/// sweep), so reclamation is bounded-lag, not unconditional, under contention.
pub(crate) fn reclaim_evicted_nodes() {
    let mut guard = crossbeam_epoch::pin();
    guard.flush();
    guard.repin();
    guard.flush();
}
