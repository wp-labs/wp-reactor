use super::*;

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// `events { alias: window [&& filter] ... }`
#[non_exhaustive]
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq)]
#[jumo(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct EventsBlock {
    pub decls: Vec<EventDecl>,
}

/// `alias : window [&& filter]`
#[non_exhaustive]
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq)]
#[jumo(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct EventDecl {
    pub alias: String,
    pub window: String,
    pub filter: Option<Expr>,
}
