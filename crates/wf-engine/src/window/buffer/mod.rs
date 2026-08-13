mod cursor;
mod eviction;
mod types;
mod watermark;

#[cfg(test)]
mod tests;

pub use types::{AppendOutcome, WindowParams};

use std::collections::{HashSet, VecDeque};
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, IntervalUnit, SchemaRef};
use arrow::record_batch::RecordBatch;
use orion_error::conversion::ToStructError;
use smol_str::SmolStr;
use wf_config::WindowConfig;

use crate::error::{CoreReason, CoreResult};
use crate::match_engine::{Event, Value};

use types::TimedBatch;

/// A time-ordered buffer of Arrow RecordBatches with eviction support.
///
/// Batches are appended to the back and evicted from the front, either by
/// time expiry or memory pressure.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.WindowManager")]
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
    /// Optional per-event field whitelist (see `WindowParams`).
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
            batches: VecDeque::new(),
            current_bytes: 0,
            total_rows: 0,
            watermark_nanos: i64::MIN,
            next_seq: 0,
            materialize_fields,
        }
    }

    /// Append a RecordBatch to this window.
    ///
    /// Empty batches are silently skipped. Returns an error if the batch
    /// schema does not match the window schema. After appending, memory
    /// eviction runs if `current_bytes > max_window_bytes`.
    pub fn append(&mut self, batch: RecordBatch) -> CoreResult<()> {
        self.append_inner(batch, None, None)
    }

    /// Append a RecordBatch whose events were already parsed *outside* the
    /// window lock (by the router). Rule tasks then read the pre-parsed `Arc`
    /// with no `OnceLock` contention among the concurrent rule tasks.
    pub fn append_parsed(&mut self, batch: RecordBatch, parsed_events: Arc<Vec<Arc<Event>>>) -> CoreResult<()> {
        self.append_inner(batch, Some(parsed_events), None)
    }

    /// Append a RecordBatch whose events *and content byte size* were precomputed
    /// by the caller (the R2 parse worker), so the O(rows×cols) accounting runs
    /// in parallel rather than on the ordered commit path.
    pub fn append_parsed_sized(
        &mut self,
        batch: RecordBatch,
        parsed_events: Arc<Vec<Arc<Event>>>,
        byte_size: usize,
    ) -> CoreResult<()> {
        self.append_inner(batch, Some(parsed_events), Some(byte_size))
    }

    fn append_inner(
        &mut self,
        batch: RecordBatch,
        parsed_events: Option<Arc<Vec<Arc<Event>>>>,
        byte_size: Option<usize>,
    ) -> CoreResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
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
        let seq = self.next_seq;
        self.next_seq += 1;

        let parsed_lock = std::sync::OnceLock::new();
        if let Some(events) = parsed_events {
            // Ignore the error: a freshly-created OnceLock is always empty.
            let _ = parsed_lock.set(events);
        }
        self.batches.push_back(TimedBatch {
            batch,
            event_time_range,
            ingested_at: Instant::now(),
            row_count,
            byte_size,
            seq,
            parsed_events: parsed_lock,
        });

        self.current_bytes += byte_size;
        self.total_rows += row_count;

        // Memory eviction: pop oldest batches while over budget.
        let max_bytes = self.config.max_window_bytes.as_bytes();
        let mut evicted_bytes = 0usize;
        let mut evicted_rows = 0usize;
        while self.current_bytes > max_bytes {
            if let Some(evicted) = self.batches.pop_front() {
                self.current_bytes -= evicted.byte_size;
                self.total_rows -= evicted.row_count;
                evicted_bytes += evicted.byte_size;
                evicted_rows += evicted.row_count;
            } else {
                break;
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
        self.total_rows
    }

    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
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
