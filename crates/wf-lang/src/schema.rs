use std::time::Duration;

/// Base data types supported in window schemas.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangSchema")]
pub enum BaseType {
    Chars,
    Digit,
    Float,
    Bool,
    Time,
    Ip,
    Hex,
}

/// A field type: either a base type, a typed array, or a structured value.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangSchema")]
pub enum FieldType {
    Base(BaseType),
    /// Heterogeneous structured array.
    ArrayAny,
    Array(BaseType),
    Object,
}

/// A single field definition within a window schema.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, Eq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangSchema")]
pub struct FieldDef {
    pub name: String,
    pub field_type: FieldType,
}

/// A parsed window schema declaration.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangSchema")]
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

/// A static (provider-backed) window schema. No stream, time, or over.
/// Data comes from an external source (knowdb), not event streams.
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangSchema")]
pub struct StaticWindowSchema {
    pub name: String,
    pub fields: Vec<FieldDef>,
}

impl StaticWindowSchema {
    /// Convert to a minimal WindowSchema for rule checking.
    pub fn to_flow_schema(&self) -> WindowSchema {
        WindowSchema {
            name: self.name.clone(),
            streams: vec![],
            time_field: None,
            over: std::time::Duration::ZERO,
            fields: self.fields.clone(),
        }
    }
}
