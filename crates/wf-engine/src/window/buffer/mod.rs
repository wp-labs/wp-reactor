mod cursor;
mod eviction;
mod types;
mod watermark;

#[cfg(test)]
mod tests;

pub use types::{AppendOutcome, WindowParams};

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, StringArray, StructArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, IntervalUnit, SchemaRef};
use arrow::record_batch::RecordBatch;
use orion_error::conversion::ToStructError;
use wf_config::WindowConfig;

use crate::error::{CoreReason, CoreResult};

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
        let byte_size = content_bytes(&batch);
        let seq = self.next_seq;
        self.next_seq += 1;

        self.batches.push_back(TimedBatch {
            batch,
            event_time_range,
            ingested_at: Instant::now(),
            row_count,
            byte_size,
            seq,
            parsed_events: std::sync::OnceLock::new(),
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
            arr.columns().iter().map(|c| column_content_bytes(c.as_ref())).sum()
        }
        DataType::List(_) => {
            let arr = col.as_any().downcast_ref::<ListArray>().expect("list column");
            // value(i) slices the child; a null row yields an empty slice → 0 bytes.
            offsets_bytes(n, 4)
                + (0..n).map(|i| column_content_bytes(arr.value(i).as_ref())).sum::<usize>()
        }
        DataType::LargeList(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("large list column");
            offsets_bytes(n, 8)
                + (0..n).map(|i| column_content_bytes(arr.value(i).as_ref())).sum::<usize>()
        }
        DataType::FixedSizeList(_, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("fixed-size list column");
            (0..n).map(|i| column_content_bytes(arr.value(i).as_ref())).sum()
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
