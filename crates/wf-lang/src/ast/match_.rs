use std::time::Duration;

use super::*;

// ---------------------------------------------------------------------------
// Match clause
// ---------------------------------------------------------------------------

/// Window mode: sliding (default), fixed (L3), or session (L3).
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangMatch")]
pub enum WindowMode {
    Sliding,
    Fixed,
    Session(std::time::Duration), // gap duration
}

/// Close block mode: OR (independent paths) or AND (both required).
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangMatch")]
pub enum CloseMode {
    /// `on close { ... }` — event path and close path fire independently.
    Or,
    /// `and close { ... }` — both event and close paths must satisfy.
    And,
}

/// A parsed close block with its mode and steps.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangMatch")]
pub struct CloseBlock {
    pub mode: CloseMode,
    pub steps: Vec<MatchStep>,
}

/// Ordering mode of an `on event` block.
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangMatch")]
pub enum MatchMode {
    /// Ordered (default): step i+1 evaluates only after step i completes.
    Seq,
    /// Unordered co-occurrence: all steps must be satisfied, order irrelevant.
    Any,
}

/// `match<keys:dur[:fixed]> { [key {...}] on event [seq|any] { ... } [on close|and close { ... }] }`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangMatch")]
pub struct MatchClause {
    pub keys: Vec<FieldRef>,
    pub key_mapping: Option<Vec<KeyMapItem>>,
    pub duration: Duration,
    pub window_mode: WindowMode,
    pub on_event: Vec<MatchStep>,
    pub on_close: Option<CloseBlock>,
    /// Ordering mode of the `on event` block. Default: `Seq`.
    pub match_mode: MatchMode,
    /// Ordered-sequence constraints (`on event seq { ... }`): `within` / `not` /
    /// `consec` / `skip` on the steps. When present, `on_event` is empty.
    pub seq: Option<SeqClause>,
    /// `on event<accu>` — within-window accumulation: after the block fires the
    /// count/evidence keep accumulating without reset, and each subsequent
    /// qualifying event re-fires with the running cumulative values, until the
    /// window expires. Orthogonal to `match_mode` / `seq`.
    pub accu: bool,
}

impl MatchClause {
    pub fn placeholder() -> Self {
        Self {
            keys: Vec::new(),
            key_mapping: None,
            duration: Duration::from_secs(1),
            window_mode: WindowMode::Sliding,
            on_event: Vec::new(),
            on_close: None,
            match_mode: MatchMode::Seq,
            seq: None,
            accu: false,
        }
    }
}

/// `on each alias [where expr]`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangMatch")]
pub struct EachClause {
    pub alias: String,
    pub filter: Option<Expr>,
}

/// Explicit key mapping: `logical = alias.field`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangMatch")]
pub struct KeyMapItem {
    pub logical_name: String,
    pub source_field: FieldRef,
}

/// One semicolon-terminated match step, potentially with `||` OR branches.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangMatch")]
pub struct MatchStep {
    pub branches: Vec<StepBranch>,
}

/// `[label:] source[.field]["field"] [&& guard] pipe_chain`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangMatch")]
pub struct StepBranch {
    pub label: Option<String>,
    pub source: String,
    pub field: Option<FieldSelector>,
    pub guard: Option<Expr>,
    pub pipe: PipeChain,
}

/// `{ | transform } | measure cmp threshold`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangMatch")]
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

// ---------------------------------------------------------------------------
// Chain clause — ordered sequence matching (L1/L2)
// ---------------------------------------------------------------------------

/// `chain [consec] [skip = past_last|to_next] { [not] step_body [within dur] ... }`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangMatch")]
pub struct SeqClause {
    /// `consec` — strict adjacency (no other events between steps). Default: gap.
    pub consec: bool,
    /// After-match skip policy.
    pub skip: SeqSkip,
    /// Ordered sequence steps.
    pub steps: Vec<SeqStep>,
}

/// After-match skip policy.
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangMatch")]
pub enum SeqSkip {
    /// Reset all step state after firing (default).
    PastLast,
    /// Keep non-first steps for overlapping matches (L3).
    ToNext,
}

/// One ordered chain step: `[not] <body> [within dur]`.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangMatch")]
pub struct SeqStep {
    /// `not` negation prefix.
    pub neg: bool,
    /// Time gap relative to the previous step's completion.
    pub within: Option<Duration>,
    /// Step body. For `has <alias>` existential steps, `pipe` is synthesized `count >= 1`.
    pub branch: StepBranch,
}
