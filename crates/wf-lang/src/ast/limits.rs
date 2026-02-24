// ---------------------------------------------------------------------------
// Limits block
// ---------------------------------------------------------------------------

/// `limits { max_memory = "256MB" max_instances = 10000 ... }`
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
