// ---------------------------------------------------------------------------
// Limits block
// ---------------------------------------------------------------------------

/// `limits { max_state = "256MB" max_cardinality = 10000 ... }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct LimitsBlock {
    pub items: Vec<LimitItem>,
}

/// A single `key = value` entry in a limits block.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct LimitItem {
    pub key: String,
    pub value: String,
}
