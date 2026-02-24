use std::time::Duration;

use super::*;

// ---------------------------------------------------------------------------
// Join clause
// ---------------------------------------------------------------------------

/// `join window snapshot/asof on cond`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct JoinClause {
    pub target_window: String,
    pub mode: JoinMode,
    pub conditions: Vec<JoinCondition>,
}

/// Join time-point semantics.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum JoinMode {
    Snapshot,
    Asof { within: Option<Duration> },
}

/// `left == right` in a join on-clause.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct JoinCondition {
    pub left: FieldRef,
    pub right: FieldRef,
}
