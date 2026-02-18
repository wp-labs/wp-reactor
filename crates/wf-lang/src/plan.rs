use std::time::Duration;

use crate::ast::{CmpOp, Expr, FieldRef, FieldSelector, Measure, Transform};

// ---------------------------------------------------------------------------
// ExprPlan — L1 alias for ast::Expr
// ---------------------------------------------------------------------------

/// Expression in the execution plan.
///
/// For L1 this is a zero-cost alias of `ast::Expr`. When L2/L3 introduces
/// expression lowering (e.g. resolving field refs, inlining conv lookups),
/// this will become a distinct type.
pub type ExprPlan = Expr;

// ---------------------------------------------------------------------------
// RulePlan — top-level compiled rule
// ---------------------------------------------------------------------------

/// Compiled rule — the executable representation consumed by MatchEngine.
#[derive(Debug, Clone, PartialEq)]
pub struct RulePlan {
    pub name: String,
    pub binds: Vec<BindPlan>,
    pub match_plan: MatchPlan,
    pub joins: Vec<JoinPlan>,
    pub entity_plan: EntityPlan,
    pub yield_plan: YieldPlan,
    pub score_plan: ScorePlan,
    pub conv_plan: Option<ConvPlan>,
}

// ---------------------------------------------------------------------------
// BindPlan — event source binding
// ---------------------------------------------------------------------------

/// A bound event source: alias + window + optional filter.
#[derive(Debug, Clone, PartialEq)]
pub struct BindPlan {
    pub alias: String,
    pub window: String,
    pub filter: Option<ExprPlan>,
}

// ---------------------------------------------------------------------------
// MatchPlan — temporal matching
// ---------------------------------------------------------------------------

/// The match plan: keys, window spec, event steps, and close steps.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchPlan {
    pub keys: Vec<FieldRef>,
    pub window_spec: WindowSpec,
    pub event_steps: Vec<StepPlan>,
    pub close_steps: Vec<StepPlan>,
}

/// Window specification for the match clause.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowSpec {
    /// Sliding window with a fixed duration.
    Sliding(Duration),
}

/// One match step containing one or more OR branches.
#[derive(Debug, Clone, PartialEq)]
pub struct StepPlan {
    pub branches: Vec<BranchPlan>,
}

/// A single branch within a match step.
#[derive(Debug, Clone, PartialEq)]
pub struct BranchPlan {
    pub label: Option<String>,
    pub source: String,
    pub field: Option<FieldSelector>,
    pub guard: Option<ExprPlan>,
    pub agg: AggPlan,
}

/// Aggregation pipeline: transforms → measure → cmp → threshold.
#[derive(Debug, Clone, PartialEq)]
pub struct AggPlan {
    pub transforms: Vec<Transform>,
    pub measure: Measure,
    pub cmp: CmpOp,
    pub threshold: ExprPlan,
}

// ---------------------------------------------------------------------------
// JoinPlan — cross-source joins (empty for L1)
// ---------------------------------------------------------------------------

/// Cross-source join plan. Empty for L1.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinPlan {
    pub window: Duration,
    pub conditions: Vec<ExprPlan>,
}

// ---------------------------------------------------------------------------
// EntityPlan
// ---------------------------------------------------------------------------

/// Entity identification: lowercase-normalized type string + id expression.
///
/// Both `entity(IP, ...)` and `entity("ip", ...)` compile to `entity_type = "ip"`.
#[derive(Debug, Clone, PartialEq)]
pub struct EntityPlan {
    pub entity_type: String,
    pub entity_id_expr: ExprPlan,
}

// ---------------------------------------------------------------------------
// ScorePlan
// ---------------------------------------------------------------------------

/// Score computation expression.
#[derive(Debug, Clone, PartialEq)]
pub struct ScorePlan {
    pub expr: ExprPlan,
}

// ---------------------------------------------------------------------------
// YieldPlan
// ---------------------------------------------------------------------------

/// Output yield: target window + fields.
#[derive(Debug, Clone, PartialEq)]
pub struct YieldPlan {
    pub target: String,
    pub fields: Vec<YieldField>,
}

/// A single yield field: name = expression.
#[derive(Debug, Clone, PartialEq)]
pub struct YieldField {
    pub name: String,
    pub value: ExprPlan,
}

// ---------------------------------------------------------------------------
// ConvPlan — conversion / enrichment (None for L1)
// ---------------------------------------------------------------------------

/// Conversion plan. `None` for L1 — no conv support yet.
#[derive(Debug, Clone, PartialEq)]
pub struct ConvPlan {
    pub steps: Vec<ExprPlan>,
}
