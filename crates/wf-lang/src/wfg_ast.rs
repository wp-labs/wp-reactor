use std::time::Duration;

// ---------------------------------------------------------------------------
// Top-level
// ---------------------------------------------------------------------------

/// A complete `.wfg` scenario file.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct WfgFile {
    pub uses: Vec<UseDecl>,
    pub scenario: ScenarioDecl,
    /// Parsed new syntax section when the file uses new stream-first syntax.
    pub syntax: Option<SyntaxScenario>,
}

/// `use "path.wfs"` or `use "path.wfl"`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct UseDecl {
    pub path: String,
}

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

/// `scenario NAME seed NUMBER { ... }`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct ScenarioDecl {
    pub name: String,
    pub seed: u64,
    pub time_clause: TimeClause,
    pub total: u64,
    pub streams: Vec<StreamBlock>,
    pub injects: Vec<InjectBlock>,
    pub faults: Option<FaultsBlock>,
    pub oracle: Option<OracleBlock>,
}

/// `time "ISO8601" duration DURATION`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct TimeClause {
    pub start: String,
    pub duration: Duration,
}

// ---------------------------------------------------------------------------
// new syntax (stream-first) extension
// ---------------------------------------------------------------------------

/// new syntax scenario data parsed from the new syntax.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct SyntaxScenario {
    /// `#[key=value, ...]` attributes attached to this scenario.
    pub attrs: Vec<ScenarioAttr>,
    /// `scenario name<k=v, ...>` inline annotations.
    pub inline_annos: Vec<ScenarioAttr>,
    pub traffic: TrafficBlock,
    pub injection: Option<SyntaxInjectionBlock>,
    pub expect: Option<ExpectBlock>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct ScenarioAttr {
    pub key: String,
    pub value: AttrValue,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum AttrValue {
    Number(f64),
    Duration(Duration),
    String(String),
    Bool(bool),
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct TrafficBlock {
    pub streams: Vec<SyntaxStreamDecl>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct SyntaxStreamDecl {
    pub stream: String,
    pub rate: RateExpr,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum RateExpr {
    Constant(Rate),
    Wave {
        base: Rate,
        amp: Rate,
        period: Duration,
        shape: WaveShape,
    },
    Burst {
        base: Rate,
        peak: Rate,
        every: Duration,
        hold: Duration,
    },
    Timeline(Vec<TimelineSegment>),
}

impl RateExpr {
    /// EPS approximation used by datagen for total event budgeting.
    pub fn approx_eps(&self) -> f64 {
        match self {
            RateExpr::Constant(r) => r.events_per_second(),
            RateExpr::Wave { base, .. } => base.events_per_second(),
            RateExpr::Burst { base, .. } => base.events_per_second(),
            RateExpr::Timeline(segments) => segments
                .first()
                .map(|s| s.rate.events_per_second())
                .unwrap_or(0.0),
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum WaveShape {
    Sine,
    Triangle,
    Square,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct TimelineSegment {
    pub start: Duration,
    pub end: Duration,
    pub rate: Rate,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct SyntaxInjectionBlock {
    pub cases: Vec<SyntaxInjectCase>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct SyntaxInjectCase {
    pub mode: InjectCaseMode,
    pub percent: f64,
    pub target_rule: Option<String>,
    pub stream: String,
    pub seq: SeqBlock,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum InjectCaseMode {
    Hit,
    NearMiss,
    Miss,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct SeqBlock {
    pub entity: String,
    pub steps: Vec<SeqStep>,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum SeqStep {
    Use {
        predicates: Vec<FieldPredicate>,
        count: u64,
    },
    Not {
        predicates: Vec<FieldPredicate>,
        within: Duration,
    },
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct FieldPredicate {
    pub field: String,
    pub value: AttrValue,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct ExpectBlock {
    pub checks: Vec<ExpectCheck>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct ExpectCheck {
    pub metric: ExpectMetric,
    pub rule: String,
    pub op: CompareOp,
    pub value: ExpectValue,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum ExpectMetric {
    Hit,
    NearMiss,
    Miss,
    Precision,
    Recall,
    Fpr,
    LatencyP95,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum CompareOp {
    Gte,
    Lte,
    Gt,
    Lt,
    Eq,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum ExpectValue {
    Percent(f64),
    Number(f64),
    Duration(Duration),
}

// ---------------------------------------------------------------------------
// Rate
// ---------------------------------------------------------------------------

/// Event rate, e.g. `100/s`, `50/m`, `10/h`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct Rate {
    pub count: u64,
    pub unit: RateUnit,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum RateUnit {
    PerSecond,
    PerMinute,
    PerHour,
}

impl Rate {
    /// Convert rate to events per second.
    pub fn events_per_second(&self) -> f64 {
        match self.unit {
            RateUnit::PerSecond => self.count as f64,
            RateUnit::PerMinute => self.count as f64 / 60.0,
            RateUnit::PerHour => self.count as f64 / 3600.0,
        }
    }
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

/// Stream declaration.
///
/// Supported forms:
/// - `stream ALIAS : WINDOW RATE { field_override* }` (legacy)
/// - `stream ALIAS from WINDOW rate RATE { field_override* }` (readable)
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct StreamBlock {
    pub alias: String,
    pub window: String,
    pub rate: Rate,
    pub overrides: Vec<FieldOverride>,
}

/// `FIELD_NAME = gen_expr`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct FieldOverride {
    pub field_name: String,
    pub gen_expr: GenExpr,
}

/// Generator expression for a field override.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum GenExpr {
    StringLit(String),
    NumberLit(f64),
    BoolLit(bool),
    GenFunc { name: String, args: Vec<GenArg> },
}

/// A gen function argument, optionally named.
///
/// Supports both positional `ipv4(500)` and named `ipv4(pool: 500)` syntax.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct GenArg {
    pub name: Option<String>,
    pub value: GenExpr,
}

impl GenArg {
    pub fn positional(value: GenExpr) -> Self {
        Self { name: None, value }
    }

    pub fn named(name: impl Into<String>, value: GenExpr) -> Self {
        Self {
            name: Some(name.into()),
            value,
        }
    }
}

// ---------------------------------------------------------------------------
// Inject
// ---------------------------------------------------------------------------

/// `inject for RULE on [STREAM, ...] { inject_line* }`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct InjectBlock {
    pub rule: String,
    pub streams: Vec<String>,
    pub lines: Vec<InjectLine>,
}

/// Inject line.
///
/// Supported forms:
/// - inline params: `MODE PERCENT% key=value key2=value2;`
/// - block params: `MODE PERCENT% { key=value; key2=value2; };`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct InjectLine {
    pub mode: InjectMode,
    pub percent: f64,
    pub params: Vec<ParamAssign>,
    /// Ordered `use(...)` declarations; each declaration describes one rule step.
    pub use_steps: Vec<InjectUseStep>,
}

/// One `use(...) with(count)` declaration captured for inject generation.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct InjectUseStep {
    pub count: u64,
    pub predicates: Vec<FieldPredicate>,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum InjectMode {
    Hit,
    NearMiss,
    NonHit,
}

// ---------------------------------------------------------------------------
// Faults
// ---------------------------------------------------------------------------

/// `faults { fault_line* }`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct FaultsBlock {
    pub faults: Vec<FaultLine>,
}

/// Supported fault types for temporal perturbation.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum FaultType {
    /// Swap adjacent events' arrival order.
    OutOfOrder,
    /// Delay event arrival position (across watermark boundary).
    Late,
    /// Clone event and insert a duplicate.
    Duplicate,
    /// Remove event from the output stream.
    Drop,
}

impl std::fmt::Display for FaultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FaultType::OutOfOrder => write!(f, "out_of_order"),
            FaultType::Late => write!(f, "late"),
            FaultType::Duplicate => write!(f, "duplicate"),
            FaultType::Drop => write!(f, "drop"),
        }
    }
}

/// `FAULT_TYPE PERCENT%`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct FaultLine {
    pub fault_type: FaultType,
    pub percent: f64,
}

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

/// `oracle { param_assigns }`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct OracleBlock {
    pub params: Vec<ParamAssign>,
}

// ---------------------------------------------------------------------------
// Shared
// ---------------------------------------------------------------------------

/// `NAME = VALUE`
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangScenario")]
pub struct ParamAssign {
    pub name: String,
    pub value: ParamValue,
}

/// Value in a parameter assignment.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangScenario")]
pub enum ParamValue {
    Number(f64),
    Duration(Duration),
    String(String),
}
