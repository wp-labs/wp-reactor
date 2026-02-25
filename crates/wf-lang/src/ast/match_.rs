use std::time::Duration;

use super::*;

// ---------------------------------------------------------------------------
// Match clause
// ---------------------------------------------------------------------------

/// Window mode: sliding (default) or fixed (L3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowMode {
    Sliding,
    Fixed,
}

/// Close block mode: OR (independent paths) or AND (both required).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseMode {
    /// `on close { ... }` — event path and close path fire independently.
    Or,
    /// `and close { ... }` — both event and close paths must satisfy.
    And,
}

/// A parsed close block with its mode and steps.
#[derive(Debug, Clone, PartialEq)]
pub struct CloseBlock {
    pub mode: CloseMode,
    pub steps: Vec<MatchStep>,
}

/// `match<keys:dur[:fixed]> { [key {...}] on event { ... } [on close|and close { ... }] }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchClause {
    pub keys: Vec<FieldRef>,
    pub key_mapping: Option<Vec<KeyMapItem>>,
    pub duration: Duration,
    pub window_mode: WindowMode,
    pub on_event: Vec<MatchStep>,
    pub on_close: Option<CloseBlock>,
}

/// Explicit key mapping: `logical = alias.field`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct KeyMapItem {
    pub logical_name: String,
    pub source_field: FieldRef,
}

/// One semicolon-terminated match step, potentially with `||` OR branches.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchStep {
    pub branches: Vec<StepBranch>,
}

/// `[label:] source[.field]["field"] [&& guard] pipe_chain`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct StepBranch {
    pub label: Option<String>,
    pub source: String,
    pub field: Option<FieldSelector>,
    pub guard: Option<Expr>,
    pub pipe: PipeChain,
}

/// `{ | transform } | measure cmp threshold`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PipeChain {
    pub transforms: Vec<Transform>,
    pub measure: Measure,
    pub cmp: CmpOp,
    pub threshold: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Transform {
    Distinct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Measure {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}
