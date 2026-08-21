use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use foldhash::fast::RandomState as FoldRandomState;
use smol_str::SmolStr;

use crate::match_engine::event_bridge::JoinRow;

/// HashMap/HashSet over hot-path keys (InstanceKey, field names, event field
/// keys) using foldhash's fast, minimally-DoS-resistant hasher instead of the
/// default SipHash. SipHash was ~3k samples of the match-engine profile; field
/// names / rule keys are internal, and InstanceKey values carry a random seed
/// via `FoldRandomState` so collision attacks stay hard.
pub type EngineHashMap<K, V> = HashMap<K, V, FoldRandomState>;
pub type EngineHashSet<K> = HashSet<K, FoldRandomState>;

// ---------------------------------------------------------------------------
// Public types — Event & Value
// ---------------------------------------------------------------------------

/// Field name for machine identifier carried in events and batches
/// for per-machine metrics labeling.
pub const MACHINE_ID: &str = "wp_src_ip";

/// A thin event abstraction: named fields with heterogeneous values.
///
/// M14 works exclusively with this type. Arrow RecordBatch bridging (M16)
/// will provide a zero-copy adapter later.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct Event {
    pub fields: EngineHashMap<SmolStr, Value>,
}

/// Scalar value carried inside an [`Event`].
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Engine", module = "Engine.MatchEngine")]
pub enum Value {
    Number(f64),
    Str(SmolStr),
    Bool(bool),
    Array(Vec<Value>),
    Object(EngineHashMap<SmolStr, Value>),
}

// ---------------------------------------------------------------------------
// FieldSource — the per-row event abstraction consumed by the state machine
// ---------------------------------------------------------------------------

/// A per-row source of named fields: either a materialized [`Event`] or a
/// columnar view over one Arrow row ([`crate::match_engine::ColumnarEvent`]).
///
/// The match state machine consumes this instead of a concrete `&Event`, so hit
/// rows never need a per-row `HashMap` materialization on the deferred path
/// (P3 FieldView — see `docs/design/columnar-match-state-machine.md` §6).
pub trait FieldSource {
    /// Field value by name, or `None` when absent / null / fails extraction.
    /// Mirrors `Event.fields.get(name).cloned()` / `ColumnarEvent::field_value`.
    fn field_value(&self, name: &str) -> Option<Value>;

    /// Every field name this row's schema/map carries. Null cells are included;
    /// callers skip them via `field_value() == None`, which is byte-identical
    /// to `batch_to_events` (it drops null cells from the map).
    fn field_names(&self) -> Vec<&str>;

    /// Materialize a full owned [`Event`] for this row (emit-path trigger
    /// event and any concrete-`Event` fallback).
    fn to_event(&self) -> Event;

    /// `Str` field → its string, anything else (absent / non-str) → `""`.
    /// Mirrors `CepStateMachine::extract_event_str`.
    fn field_value_str(&self, name: &str) -> String {
        match self.field_value(name) {
            Some(Value::Str(s)) => s.to_string(),
            _ => String::new(),
        }
    }
}

impl FieldSource for Event {
    fn field_value(&self, name: &str) -> Option<Value> {
        self.fields.get(name).cloned()
    }

    fn field_names(&self) -> Vec<&str> {
        self.fields.keys().map(|k| k.as_str()).collect()
    }

    fn to_event(&self) -> Event {
        self.clone()
    }
}

/// Hashable scalar key for join indexes. `Value` itself is not reliably
/// hashable (f64/recursive), so join lookups convert the key field to this
/// concrete scalar form. Object/array values map to `None` (rejected at
/// compile time — see checker join key constraint).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinKey {
    Int(i64),
    Str(String),
    Bool(bool),
}

impl JoinKey {
    /// Convert a [`Value`] to a hashable scalar key, or `None` for structured
    /// (object/array) values.
    pub fn from_value(v: &Value) -> Option<JoinKey> {
        match v {
            Value::Number(n) => Some(JoinKey::Int(*n as i64)),
            Value::Str(s) => Some(JoinKey::Str(s.to_string())),
            Value::Bool(b) => Some(JoinKey::Bool(*b)),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Public types — result of advance()
// ---------------------------------------------------------------------------

/// Outcome of feeding one event into the state machine.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Engine", module = "Engine.MatchEngine")]
pub enum StepResult {
    /// Event was consumed but no step boundary was crossed.
    Accumulate,
    /// A step boundary was crossed (but more steps remain).
    Advance,
    /// All steps satisfied — the match is complete.
    Matched(MatchedContext),
}

/// Diagnostic progress produced while an event is evaluated against a step.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct StepProgress {
    pub rule_name: String,
    pub scope_key: Vec<Value>,
    pub machine_id: String,
    pub step_index: usize,
    pub step_label: Option<String>,
    pub branch_index: usize,
    pub branch_source: String,
    pub threshold_checked_branches: usize,
    pub measure_value: f64,
    pub cmp: String,
    pub threshold: String,
    pub satisfied: bool,
    pub instances: usize,
}

/// Result of feeding one event with optional diagnostic progress.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct StepOutcome {
    pub result: StepResult,
    pub progress: Option<StepProgress>,
}

/// Context returned when a full match fires.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct MatchedContext {
    pub rule_name: String,
    pub scope_key: Vec<Value>,
    pub step_data: Vec<StepData>,
    pub bind_data: Vec<BindData>,
    pub event_time_nanos: i64,
    pub event_first_time_nanos: i64,
    pub event_last_time_nanos: i64,
    pub window_start_time_nanos: i64,
    pub window_end_time_nanos: i64,
    pub machine_id: String,
    /// The event that triggered this match (on-event fire). Yield's scalar
    /// field reads resolve from it, so rules that don't need the full
    /// `field_values` history can skip collecting it. `None` for fires without
    /// a triggering event (close) — those keep reading from `field_values`.
    pub trigger_event: Option<Arc<Event>>,
}

/// Per-step snapshot captured when a step is satisfied.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct StepData {
    pub satisfied_branch_index: usize,
    pub label: Option<String>,
    pub measure_value: f64,
    pub event_first_time_nanos: Option<i64>,
    pub event_last_time_nanos: Option<i64>,
    /// Collected values for L3 functions (collect_set/list, first/last, stddev/percentile)
    pub collected_values: Vec<Value>,
    /// All accepted field values seen for the satisfied branch, keyed by field name.
    pub field_values: EngineHashMap<String, Vec<Value>>,
}

/// Snapshot of all events accepted by a bound alias within the current instance.
#[derive(Debug, Clone, PartialEq)]
pub struct BindData {
    pub alias: String,
    pub count: u64,
    pub field_values: EngineHashMap<String, Vec<Value>>,
}

// ---------------------------------------------------------------------------
// Public types — close / timeout
// ---------------------------------------------------------------------------

/// Reason why a window instance was closed.
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Engine", module = "Engine.MatchEngine")]
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

use wf_lang::ast::CloseMode;

/// Output produced when an instance is closed (by timeout, flush, or eos).
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.MatchEngine")]
pub struct CloseOutput {
    pub rule_name: String,
    pub scope_key: Vec<Value>,
    pub close_reason: CloseReason,
    pub event_ok: bool,
    pub close_ok: bool,
    pub close_mode: CloseMode,
    pub event_emitted: bool,
    pub event_step_data: Vec<StepData>,
    pub close_step_data: Vec<StepData>,
    pub bind_data: Vec<BindData>,
    pub watermark_nanos: i64,
    pub machine_id: String,
    pub event_first_time_nanos: i64,
    pub event_last_time_nanos: i64,
    pub window_start_time_nanos: i64,
    pub window_end_time_nanos: i64,
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

    /// Get a snapshot of a window as columnar [`JoinRow`]s (for join) — rows
    /// are read on demand, so no whole-window HashMap materialization.
    fn snapshot(&self, window: &str) -> Option<Vec<JoinRow>>;

    /// Get a snapshot with per-row timestamps (for asof join).
    ///
    /// Returns `None` if the window doesn't exist or doesn't support timestamps.
    /// Each entry is `(timestamp_nanos, row)`.
    fn snapshot_with_timestamps(&self, window: &str) -> Option<Vec<(i64, JoinRow)>> {
        let _ = window;
        None
    }

    /// Indexed join lookup: return rows of `window` whose `key_field` equals `key`.
    ///
    /// Default implementation falls back to a snapshot + linear filter
    /// (O(rows)); a window with a maintained hash index overrides this to O(1).
    fn join_lookup(&self, window: &str, key_field: &str, key: &Value) -> Option<Vec<JoinRow>> {
        let rows = self.snapshot(window)?;
        Some(
            rows.into_iter()
                .filter(|row| {
                    row.field_value(key_field)
                        .is_some_and(|v| crate::match_engine::match_engine::values_equal(&v, key))
                })
                .collect(),
        )
    }

    /// Asof-join candidates: rows of `window` whose `key_field` equals `key`,
    /// each with its raw `Timestamp(Ns)` time (rows without a time value are
    /// skipped).
    ///
    /// Default implementation falls back to a full timestamped snapshot +
    /// linear key filter (O(rows)); a window with a maintained timestamped hash
    /// index overrides this to O(1), avoiding the full-window scan on every
    /// event (the Q22 asof-join hot path).
    fn asof_candidates(
        &self,
        window: &str,
        key_field: &str,
        key: &Value,
    ) -> Option<Vec<(i64, JoinRow)>> {
        let rows = self.snapshot_with_timestamps(window)?;
        Some(
            rows.into_iter()
                .filter(|(_, row)| {
                    row.field_value(key_field)
                        .is_some_and(|v| crate::match_engine::match_engine::values_equal(&v, key))
                })
                .collect(),
        )
    }

    /// Asof fast path: return the single row of `window` whose `key_field`
    /// equals `key` and whose raw timestamp is the maximum within
    /// `[event_time - within, event_time]` — O(1) via the index's per-key
    /// `max_ts`, no candidate scan.
    ///
    /// See [`AsofLookup`] for the three outcomes. The default implementation is
    /// always [`AsofLookup::Fallback`].
    fn asof_lookup_max(
        &self,
        window: &str,
        key_field: &str,
        key: &Value,
        event_time_nanos: i64,
        within: Option<&Duration>,
    ) -> AsofLookup {
        let _ = (window, key_field, key, event_time_nanos, within);
        AsofLookup::Fallback
    }
}

/// Outcome of the asof-join O(1) fast path ([`WindowLookup::asof_lookup_max`]).
#[derive(Clone)]
pub enum AsofLookup {
    /// Fast-path hit: the unique row whose timestamp is the maximum within
    /// `[event_time - within, event_time]`.
    Hit(JoinRow),
    /// Definitively no match: the key's max timestamp is already older than the
    /// asof lower bound, so no row can satisfy the time window — the full scan
    /// would also return `None`. The caller should fail the join without a scan.
    Miss,
    /// The fast path cannot answer (max timestamp newer than `event_time`, no
    /// index, or a watermark cuts the window); fall back to `asof_candidates` +
    /// `find_asof_row`.
    Fallback,
}

// ---------------------------------------------------------------------------
// RollingStats — baseline deviation tracking
// ---------------------------------------------------------------------------

/// Cumulative statistics tracker for `baseline()` function.
/// Supports three methods: mean (standard deviation), ewma (exponential weighted), median.
#[derive(Debug, Clone)]
pub(crate) struct RollingStats {
    count: u64,
    sum: f64,
    sum_sq: f64,
    method: String,
    // EWMA specific
    ewma: f64,
    ewma_alpha: f64, // smoothing factor (default 0.3)
    // Median specific
    values: Vec<f64>, // stores recent values for median calculation
}

impl RollingStats {
    #[allow(dead_code)]
    pub(super) fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            method: "mean".to_string(),
            ewma: 0.0,
            ewma_alpha: 0.3,
            values: Vec::new(),
        }
    }

    pub(super) fn new_with_method(method: &str) -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_sq: 0.0,
            method: method.to_string(),
            ewma: 0.0,
            ewma_alpha: 0.3,
            values: Vec::new(),
        }
    }

    pub(super) fn update(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.sum_sq += value * value;

        // Update method-specific accumulators
        match self.method.as_str() {
            "ewma" => {
                if self.count == 1 {
                    self.ewma = value;
                } else {
                    self.ewma = self.ewma_alpha * value + (1.0 - self.ewma_alpha) * self.ewma;
                }
            }
            "median" => {
                self.values.push(value);
                // Keep only last 1000 values to bound memory
                if self.values.len() > 1000 {
                    self.values.remove(0);
                }
            }
            _ => {} // "mean" uses sum/count only
        }
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

    fn median(&self) -> f64 {
        if self.values.is_empty() {
            return 0.0;
        }
        // Filter out NaN values to avoid panic in partial_cmp
        let mut sorted: Vec<f64> = self
            .values
            .iter()
            .copied()
            .filter(|v| !v.is_nan())
            .collect();
        if sorted.is_empty() {
            return 0.0;
        }
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = sorted.len() / 2;
        if sorted.len().is_multiple_of(2) {
            (sorted[mid - 1] + sorted[mid]) / 2.0
        } else {
            sorted[mid]
        }
    }

    fn ewma(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.ewma }
    }

    /// Calculate deviation based on method.
    /// Returns z-score for mean, relative deviation for ewma/median.
    pub(super) fn deviation(&self, value: f64) -> f64 {
        match self.method.as_str() {
            "ewma" => {
                let baseline = self.ewma();
                if baseline == 0.0 {
                    0.0
                } else {
                    (value - baseline) / baseline.abs()
                }
            }
            "median" => {
                let baseline = self.median();
                if baseline == 0.0 {
                    0.0
                } else {
                    (value - baseline) / baseline.abs()
                }
            }
            _ => {
                // default "mean" - use standard z-score
                let std = self.stddev();
                if std == 0.0 {
                    0.0
                } else {
                    (value - self.mean()) / std
                }
            }
        }
    }
}
