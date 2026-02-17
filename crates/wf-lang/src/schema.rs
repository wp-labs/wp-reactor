use std::time::Duration;

/// Base data types supported in window schemas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BaseType {
    Chars,
    Digit,
    Float,
    Bool,
    Time,
    Ip,
    Hex,
}

/// A field type: either a base type or an array of a base type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    Base(BaseType),
    Array(BaseType),
}

/// A single field definition within a window schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDef {
    pub name: String,
    pub field_type: FieldType,
}

/// A parsed window schema declaration.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSchema {
    /// Window name (must be globally unique).
    pub name: String,
    /// Stream subscriptions. Empty means yield-only window.
    pub streams: Vec<String>,
    /// Name of the time field (required when `over > 0`).
    pub time_field: Option<String>,
    /// Retention duration. `Duration::ZERO` means static collection.
    pub over: Duration,
    /// Field definitions.
    pub fields: Vec<FieldDef>,
}
