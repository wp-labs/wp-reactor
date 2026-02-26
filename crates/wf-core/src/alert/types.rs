use std::fmt;

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::rule::CloseReason;
use crate::rule::Value;

/// Which path produced this alert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlertOrigin {
    Event,
    Close { reason: CloseReason },
}

impl AlertOrigin {
    /// Canonical string form: `"event"`, `"close:timeout"`, `"close:flush"`, `"close:eos"`.
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertOrigin::Event => "event",
            AlertOrigin::Close { reason } => match reason {
                CloseReason::Timeout => "close:timeout",
                CloseReason::Flush => "close:flush",
                CloseReason::Eos => "close:eos",
            },
        }
    }

    pub fn close_reason(&self) -> Option<CloseReason> {
        match self {
            AlertOrigin::Event => None,
            AlertOrigin::Close { reason } => Some(*reason),
        }
    }
}

impl fmt::Display for AlertOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for AlertOrigin {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AlertOrigin {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "event" => Ok(AlertOrigin::Event),
            "close:timeout" => Ok(AlertOrigin::Close {
                reason: CloseReason::Timeout,
            }),
            "close:flush" => Ok(AlertOrigin::Close {
                reason: CloseReason::Flush,
            }),
            "close:eos" => Ok(AlertOrigin::Close {
                reason: CloseReason::Eos,
            }),
            other => Err(serde::de::Error::custom(format!(
                "unknown AlertOrigin: {other}"
            ))),
        }
    }
}

/// An output record produced by [`RuleExecutor`](crate::rule::RuleExecutor)
/// when the CEP state machine signals a match or close.
#[derive(Debug, Clone, serde::Serialize)]
pub struct OutputRecord {
    /// SHA-256 content hash (16 hex).
    pub wfx_id: String,
    /// Name of the rule that fired.
    pub rule_name: String,
    /// Score in `[0, 100]`, clamped.
    pub score: f64,
    /// Entity type from `EntityPlan` (e.g. `"ip"`).
    pub entity_type: String,
    /// Entity id evaluated from `entity_id_expr`.
    pub entity_id: String,
    /// Which path produced this alert.
    pub origin: AlertOrigin,
    /// ISO 8601 UTC timestamp (`SystemTime`-based, no chrono).
    pub fired_at: String,
    /// Matched rows — always `vec![]` for L1 (placeholder for M25 join).
    #[serde(skip)]
    pub matched_rows: Vec<RecordBatch>,
    /// Human-readable summary of the alert.
    pub summary: String,
    /// Yield target window name, used for sink routing.
    #[serde(skip)]
    pub yield_target: String,
    /// Evaluated `yield (...)` fields, used by internal pipeline stages.
    #[serde(skip)]
    pub yield_fields: Vec<(String, Value)>,
    /// Event-time for this output (nanos since epoch), used by internal windows.
    #[serde(skip)]
    pub event_time_nanos: i64,
}
