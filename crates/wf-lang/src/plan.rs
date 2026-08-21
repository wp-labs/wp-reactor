use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::ast::{
    CloseMode, CmpOp, Expr, FieldRef, FieldSelector, JoinMode, MatchMode, Measure, Transform,
};

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
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct RulePlan {
    pub name: String,
    pub binds: Vec<BindPlan>,
    pub match_plan: MatchPlan,
    pub each_plan: Option<EachPlan>,
    pub joins: Vec<JoinPlan>,
    pub entity_plan: EntityPlan,
    pub yield_plan: YieldPlan,
    pub score_plan: ScorePlan,
    pub pattern_origin: Option<PatternOriginPlan>,
    pub conv_plan: Option<ConvPlan>,
    pub limits_plan: Option<LimitsPlan>,
    /// P2c: when a fixed-window rule carries `conv`, the compiler auto-generates
    /// a conv aggregation window + conv stage. `Some` only for such rules; the
    /// rule is then shardable and closes are aggregated cross-shard. Sliding /
    /// session conv rules keep `None` and stay on the legacy inline path.
    pub conv_window: Option<ConvWindowPlan>,
}

/// Auto-generated conv aggregation descriptor (P2c).
///
/// Marks a fixed-window conv rule as shardable and carries the runtime
/// bucketing parameters for the conv stage. `over` = fixed bucket length (the
/// rule's match window duration); `keys` = scope keys. (The runtime aggregates
/// closes inside the conv stage — a dedicated aggregation window is not
/// materialized.)
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
pub struct ConvWindowPlan {
    /// Fixed bucket length = the rule's match window duration.
    pub over: Duration,
    /// Scope-key fields (same as `MatchPlan.keys`).
    pub keys: Vec<FieldRef>,
}

/// Stateless per-event trigger: `on each alias [where expr] -> score(...)`.
#[derive(Debug, Clone, PartialEq)]
pub struct EachPlan {
    pub alias: String,
    pub filter: Option<ExprPlan>,
}

// ---------------------------------------------------------------------------
// PatternOriginPlan — tracks pattern origin for explain
// ---------------------------------------------------------------------------

/// Tracks the pattern origin for `wf explain` display.
#[derive(Debug, Clone, PartialEq)]
pub struct PatternOriginPlan {
    pub pattern_name: String,
    pub args: Vec<String>,
}

// ---------------------------------------------------------------------------
// BindPlan — event source binding
// ---------------------------------------------------------------------------

/// A bound event source: alias + window + optional filter.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct BindPlan {
    pub alias: String,
    pub window: String,
    pub filter: Option<ExprPlan>,
}

// ---------------------------------------------------------------------------
// MatchPlan — temporal matching
// ---------------------------------------------------------------------------

/// The match plan: keys, window spec, event steps, close steps, key mapping, and close mode.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct MatchPlan {
    pub keys: Vec<FieldRef>,
    pub key_map: Option<Vec<KeyMapPlan>>,
    /// join-then-key (Path A): the match key comes from a snapshot join's right
    /// window (e.g. `match<category:10m>` where `category` is on auction_events
    /// and the driver is bid). `None` = all keys are read from the driver event.
    pub key_join: Option<JoinKeyPlan>,
    pub window_spec: WindowSpec,
    pub event_steps: Vec<StepPlan>,
    pub close_steps: Vec<StepPlan>,
    pub close_mode: CloseMode,
    pub tracked_bind_aliases: HashSet<String>,
    pub tracked_bind_fields: HashMap<String, HashSet<String>>,
    pub tracked_plain_fields: HashSet<String>,
    /// Ordering mode of the `on event` block (`Seq` ordered, `Any` unordered).
    pub match_mode: MatchMode,
    /// Ordered sequence constraints (`on event seq { ... }`). When present, event/close steps are empty.
    pub seq: Option<SeqPlan>,
    /// `on event<accu>` — within-window accumulation: after firing, count and
    /// evidence keep accumulating without reset and each subsequent qualifying
    /// event re-fires with the running cumulative values until the window expires.
    pub accu: bool,
    /// Whether any alias needs the per-field value **history** (`field_values`).
    ///
    /// True when a close step fires (no triggering event to read fields from),
    /// the rule binds multiple events (yield may read a non-trigger alias's
    /// field), joins are present, or a yield/score/entity expression uses an L3
    /// series function (collect_set/list, first/last, stddev, percentile) that
    /// reads the `_step_field` array. False for single-bind on-event rules whose
    /// yield reads scalar fields — those read the scalar from the triggering
    /// event instead, so `collect_alias_event` can be skipped entirely (avoids
    /// per-instance field_values allocation under churn → RSS growth).
    pub needs_field_history: bool,
}

/// Explicit key mapping entry: logical name → source alias + field.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct KeyMapPlan {
    pub logical_name: String,
    pub source_alias: String,
    pub source_field: String,
}

/// join-then-key descriptor: the match key's value is resolved by looking the
/// event's join-left key up in the join's right window, then reading
/// `right_field` from the joined row (e.g. bid.auction → auction_events.id →
/// auction_events.category). Carries everything the runtime needs so the state
/// machine can do the lookup without reaching into `RulePlan.joins`.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct JoinKeyPlan {
    /// Index into `RulePlan.joins` whose right window provides the key value
    /// (kept for explain / match-time join enrichment reuse).
    pub join_idx: usize,
    /// Right window name — the join target that holds the key field.
    pub right_window: String,
    /// Driver-side join key field (e.g. `b.auction`) — extracted from the
    /// event to drive `WindowLookup::join_lookup`.
    pub left_field: FieldRef,
    /// Join condition's right-side key field (e.g. `auction_events.id`) — the
    /// lookup index key on the right window.
    pub right_key_field: String,
    /// Right-row field read as the window key (e.g. `category`).
    pub right_field: String,
    /// Logical key name (defaults to `right_field`).
    pub key_name: String,
}

/// Window specification for the match clause.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangCompile")]
pub enum WindowSpec {
    /// Sliding window with a fixed duration.
    Sliding(Duration),
    /// Fixed window with a fixed duration (non-overlapping buckets).
    Fixed(Duration),
    /// Session window with gap duration (L3 behavior analysis).
    Session(Duration),
}

/// One match step containing one or more OR branches.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct StepPlan {
    pub branches: Vec<BranchPlan>,
}

/// A single branch within a match step.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct BranchPlan {
    pub label: Option<String>,
    pub source: String,
    pub field: Option<FieldSelector>,
    pub guard: Option<ExprPlan>,
    pub agg: AggPlan,
}

/// Aggregation pipeline: transforms → measure → cmp → threshold.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct AggPlan {
    pub transforms: Vec<Transform>,
    pub measure: Measure,
    pub cmp: CmpOp,
    pub threshold: ExprPlan,
}

// ---------------------------------------------------------------------------
// SeqPlan — ordered sequence matching (L1/L2)
// ---------------------------------------------------------------------------

/// Ordered sequence plan: steps complete in order within the match window.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct SeqPlan {
    /// `consec` — strict adjacency; default: gap.
    pub consec: bool,
    /// After-match skip policy.
    pub skip: SeqSkipPlan,
    /// Ordered steps.
    pub steps: Vec<SeqStepPlan>,
}

/// After-match skip policy.
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangCompile")]
pub enum SeqSkipPlan {
    /// Reset all step state after firing (default).
    PastLast,
    /// Keep non-first steps for overlapping matches (L3).
    ToNext,
}

/// One ordered chain step: `[not] <branch> [within]`.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCompile")]
pub struct SeqStepPlan {
    /// `not` negation prefix.
    pub neg: bool,
    /// Time gap relative to the previous step's completion.
    pub within: Option<Duration>,
    /// Step body. For `has <alias>` steps, `agg` is `count >= 1`.
    pub branch: BranchPlan,
}

// ---------------------------------------------------------------------------
// JoinPlan — cross-source joins
// ---------------------------------------------------------------------------

/// Cross-source join plan.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
pub struct JoinPlan {
    pub right_window: String,
    pub mode: JoinMode,
    pub conds: Vec<JoinCondPlan>,
}

/// A single join condition: left field == right field.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
pub struct JoinCondPlan {
    pub left: FieldRef,
    pub right: FieldRef,
}

impl JoinCondPlan {
    /// The right-side (target-window) key field name, if it is a flat
    /// (non-nested) reference. Join keys are validated as flat scalars by the
    /// checker, so this is `Some` for compiled joins.
    pub fn right_field_name(&self) -> Option<&str> {
        match &self.right {
            FieldRef::Simple(f) | FieldRef::Qualified(_, f) | FieldRef::Bracketed(_, f) => {
                Some(f.as_str())
            }
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// LimitsPlan — resource budget enforcement
// ---------------------------------------------------------------------------

/// Compiled limits for runtime enforcement.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
pub struct LimitsPlan {
    pub max_memory_bytes: Option<usize>,
    pub max_instances: Option<usize>,
    pub max_throttle: Option<RateSpec>,
    pub on_exceed: ExceedAction,
}

/// What to do when a limit is exceeded.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangExePlan")]
pub enum ExceedAction {
    Throttle,
    DropOldest,
    FailRule,
}

/// Emit rate specification: count per duration.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
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
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
pub struct EntityPlan {
    pub entity_type: String,
    pub entity_id_expr: ExprPlan,
}

// ---------------------------------------------------------------------------
// ScorePlan
// ---------------------------------------------------------------------------

/// Score computation expression.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
pub struct ScorePlan {
    pub expr: ExprPlan,
}

// ---------------------------------------------------------------------------
// YieldPlan
// ---------------------------------------------------------------------------

/// Output yield: target window + optional version + fields.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
pub struct YieldPlan {
    pub target: String,
    pub version: Option<u32>,
    pub fields: Vec<YieldField>,
}

/// A single yield field: name = expression.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangClauses")]
pub struct YieldField {
    pub name: String,
    pub value: ExprPlan,
}

// ---------------------------------------------------------------------------
// ConvPlan — result set transformations for fixed windows (L3)
// ---------------------------------------------------------------------------

/// Compiled conv plan — post-close result set transformations.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
pub struct ConvPlan {
    pub chains: Vec<ConvChainPlan>,
}

/// One semicolon-separated chain of piped operations.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
pub struct ConvChainPlan {
    pub ops: Vec<ConvOpPlan>,
}

/// A single conv operation.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangExePlan")]
pub enum ConvOpPlan {
    Sort(Vec<SortKeyPlan>),
    Top(u64),
    Dedup(ExprPlan),
    Where(ExprPlan),
}

/// Sort key with direction.
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExePlan")]
pub struct SortKeyPlan {
    pub expr: ExprPlan,
    pub descending: bool,
}
