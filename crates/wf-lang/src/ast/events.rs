use super::*;

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// `events { alias: window [&& filter] ... }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct EventsBlock {
    pub decls: Vec<EventDecl>,
}

/// `alias : window [&& filter]`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct EventDecl {
    pub alias: String,
    pub window: String,
    pub filter: Option<Expr>,
}
