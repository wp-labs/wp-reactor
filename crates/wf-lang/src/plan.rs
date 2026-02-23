use std::time::Duration;

use crate::ast::{CmpOp, Expr, FieldRef, FieldSelector, JoinMode, Measure, Transform};

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
    pub limits_plan: Option<LimitsPlan>,
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

/// The match plan: keys, window spec, event steps, close steps, and key mapping.
#[derive(Debug, Clone, PartialEq)]
pub struct MatchPlan {
    pub keys: Vec<FieldRef>,
    pub key_map: Option<Vec<KeyMapPlan>>,
    pub window_spec: WindowSpec,
    pub event_steps: Vec<StepPlan>,
    pub close_steps: Vec<StepPlan>,
}

/// Explicit key mapping entry: logical name → source alias + field.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyMapPlan {
    pub logical_name: String,
    pub source_alias: String,
    pub source_field: String,
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
// JoinPlan — cross-source joins
// ---------------------------------------------------------------------------

/// Cross-source join plan.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinPlan {
    pub right_window: String,
    pub mode: JoinMode,
    pub conds: Vec<JoinCondPlan>,
}

/// A single join condition: left field == right field.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinCondPlan {
    pub left: FieldRef,
    pub right: FieldRef,
}

// ---------------------------------------------------------------------------
// LimitsPlan — resource budget enforcement
// ---------------------------------------------------------------------------

/// Compiled limits for runtime enforcement.
#[derive(Debug, Clone, PartialEq)]
pub struct LimitsPlan {
    pub max_state_bytes: Option<usize>,
    pub max_cardinality: Option<usize>,
    pub max_emit_rate: Option<RateSpec>,
    pub on_exceed: ExceedAction,
}

/// What to do when a limit is exceeded.
#[derive(Debug, Clone, PartialEq)]
pub enum ExceedAction {
    Throttle,
    DropOldest,
    FailRule,
}

/// Emit rate specification: count per duration.
#[derive(Debug, Clone, PartialEq)]
pub struct RateSpec {
    pub count: u64,
    pub per: Duration,
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
