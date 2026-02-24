use std::collections::{HashMap, HashSet};

// ---------------------------------------------------------------------------
// Public types — Event & Value
// ---------------------------------------------------------------------------

/// A thin event abstraction: named fields with heterogeneous values.
///
/// M14 works exclusively with this type. Arrow RecordBatch bridging (M16)
/// will provide a zero-copy adapter later.
#[derive(Debug, Clone)]
pub struct Event {
    pub fields: HashMap<String, Value>,
}

/// Scalar value carried inside an [`Event`].
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Number(f64),
    Str(String),
    Bool(bool),
}

// ---------------------------------------------------------------------------
// Public types — result of advance()
// ---------------------------------------------------------------------------

/// Outcome of feeding one event into the state machine.
#[derive(Debug, Clone, PartialEq)]
pub enum StepResult {
    /// Event was consumed but no step boundary was crossed.
    Accumulate,
    /// A step boundary was crossed (but more steps remain).
    Advance,
    /// All steps satisfied — the match is complete.
    Matched(MatchedContext),
}

/// Context returned when a full match fires.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchedContext {
    pub rule_name: String,
    pub scope_key: Vec<Value>,
    pub step_data: Vec<StepData>,
    pub event_time_nanos: i64,
}

/// Per-step snapshot captured when a step is satisfied.
#[derive(Debug, Clone, PartialEq)]
pub struct StepData {
    pub satisfied_branch_index: usize,
    pub label: Option<String>,
    pub measure_value: f64,
}

// ---------------------------------------------------------------------------
// Public types — close / timeout
// ---------------------------------------------------------------------------

/// Reason why a window instance was closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    Timeout,
    Flush,
    Eos,
}

impl CloseReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            CloseReason::Timeout => "timeout",
            CloseReason::Flush => "flush",
            CloseReason::Eos => "eos",
        }
    }
}

/// Output produced when an instance is closed (by timeout, flush, or eos).
#[derive(Debug, Clone, PartialEq)]
pub struct CloseOutput {
    pub rule_name: String,
    pub scope_key: Vec<Value>,
    pub close_reason: CloseReason,
    pub event_ok: bool,
    pub close_ok: bool,
    pub event_step_data: Vec<StepData>,
    pub close_step_data: Vec<StepData>,
    pub watermark_nanos: i64,
    /// The timestamp of the last event processed by this instance.
    /// Used as the asof join time in the close path to avoid
    /// matching against right-table rows that appeared after the
    /// instance stopped receiving events.
    pub last_event_nanos: i64,
}

// ---------------------------------------------------------------------------
// WindowLookup trait — external window access for has() and join
// ---------------------------------------------------------------------------

/// Trait for accessing external window data at runtime.
/// Used by `window.has()` and join operations.
pub trait WindowLookup: Send + Sync {
    /// Get all distinct values for a field in a static window (for `has()`).
    fn snapshot_field_values(&self, window: &str, field: &str) -> Option<HashSet<String>>;

    /// Get a full snapshot of a window (for join).
    fn snapshot(&self, window: &str) -> Option<Vec<HashMap<String, Value>>>;

    /// Get a full snapshot with per-row timestamps (for asof join).
    ///
    /// Returns `None` if the window doesn't exist or doesn't support timestamps.
    /// Each entry is `(timestamp_nanos, fields)`.
    fn snapshot_with_timestamps(&self, window: &str) -> Option<Vec<(i64, HashMap<String, Value>)>> {
        let _ = window;
        None
    }
}

// ---------------------------------------------------------------------------
// RollingStats — baseline deviation tracking
// ---------------------------------------------------------------------------

/// Cumulative statistics tracker for `baseline()` function.
#[derive(Debug, Clone)]
pub(crate) struct RollingStats {
    count: u64,
    sum: f64,
    sum_sq: f64,
}

impl RollingStats {
    pub(super) fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    pub(super) fn update(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.sum_sq += value * value;
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    fn stddev(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        let n = self.count as f64;
        let variance = (self.sum_sq / n) - (self.mean() * self.mean());
        if variance < 0.0 { 0.0 } else { variance.sqrt() }
    }

    /// How many standard deviations the value is from the mean.
    pub(super) fn deviation(&self, value: f64) -> f64 {
        let std = self.stddev();
        if std == 0.0 {
            0.0
        } else {
            (value - self.mean()) / std
        }
    }
}
