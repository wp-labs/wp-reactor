// ---------------------------------------------------------------------------
// Limits block
// ---------------------------------------------------------------------------

/// `limits { max_memory = "256MB" max_instances = 10000 ... }`
#[non_exhaustive]
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq)]
#[jumo(kind = "struct", domain = "Lang", module = "Lang.LangClauses")]
pub struct LimitsBlock {
    pub items: Vec<LimitItem>,
}

/// A single `key = value` entry in a limits block.
#[non_exhaustive]
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq)]
#[jumo(kind = "struct", domain = "Lang", module = "Lang.LangClauses")]
pub struct LimitItem {
    pub key: String,
    pub value: String,
}
