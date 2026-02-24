use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// Result of a watermark-aware append.
pub enum AppendOutcome {
    Appended,
    DroppedLate,
}

/// Parameters for constructing a [`Window`](super::Window).
pub struct WindowParams {
    pub name: String,
    pub schema: SchemaRef,
    /// Index of the time column in the schema, `None` for output windows.
    pub time_col_index: Option<usize>,
    /// Retention duration from the `.wfs` file.
    pub over: Duration,
}

pub(in crate::window) struct TimedBatch {
    pub(super) batch: RecordBatch,
    /// (min, max) event time in nanoseconds.
    pub(super) event_time_range: (i64, i64),
    #[allow(dead_code)]
    pub(super) ingested_at: Instant,
    pub(super) row_count: usize,
    pub(super) byte_size: usize,
    /// Monotonically increasing sequence number assigned on append.
    pub(super) seq: u64,
}
