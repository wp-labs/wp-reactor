use arrow::record_batch::RecordBatch;

/// An alert produced by [`RuleExecutor`](crate::rule::RuleExecutor) when the
/// CEP state machine signals a match or close.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AlertRecord {
    /// Composite alert key: `"rule|key1\x1fkey2|fired_at#seq"`.
    ///
    /// - Each segment is percent-encoded (`|` → `%7C`, `#` → `%23`) so
    ///   splitting on literal `|` always yields exactly three parts
    /// - Keys joined with `\x1f` (unit separator)
    /// - `seq` is a process-wide monotonic counter for same-millisecond uniqueness
    pub alert_id: String,
    /// Name of the rule that fired.
    pub rule_name: String,
    /// Score in `[0, 100]`, clamped.
    pub score: f64,
    /// Entity type from `EntityPlan` (e.g. `"ip"`).
    pub entity_type: String,
    /// Entity id evaluated from `entity_id_expr`.
    pub entity_id: String,
    /// Present when the alert came from the close path.
    pub close_reason: Option<String>,
    /// ISO 8601 UTC timestamp (`SystemTime`-based, no chrono).
    pub fired_at: String,
    /// Matched rows — always `vec![]` for L1 (placeholder for M25 join).
    #[serde(skip)]
    pub matched_rows: Vec<RecordBatch>,
    /// Human-readable summary of the alert.
    pub summary: String,
}
