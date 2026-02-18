use std::time::Duration;

// ---------------------------------------------------------------------------
// Top-level
// ---------------------------------------------------------------------------

/// A complete `.wfl` file.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct WflFile {
    pub uses: Vec<UseDecl>,
    pub rules: Vec<RuleDecl>,
    pub contracts: Vec<ContractBlock>,
}

/// `use "path.ws"`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct UseDecl {
    pub path: String,
}

// ---------------------------------------------------------------------------
// Rule
// ---------------------------------------------------------------------------

/// `rule name { meta events match->score entity yield }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RuleDecl {
    pub name: String,
    pub meta: Option<MetaBlock>,
    pub events: EventsBlock,
    pub match_clause: MatchClause,
    pub score: ScoreExpr,
    pub entity: EntityClause,
    pub yield_clause: YieldClause,
}

/// `meta { key = "value" ... }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MetaBlock {
    pub entries: Vec<MetaEntry>,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MetaEntry {
    pub key: String,
    pub value: String,
}

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

// ---------------------------------------------------------------------------
// Match clause
// ---------------------------------------------------------------------------

/// `match<keys:dur> { on event { ... } [on close { ... }] }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct MatchClause {
    pub keys: Vec<FieldRef>,
    pub duration: Duration,
    pub on_event: Vec<MatchStep>,
    pub on_close: Option<Vec<MatchStep>>,
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

// ---------------------------------------------------------------------------
// Score
// ---------------------------------------------------------------------------

/// `-> score(expr)`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ScoreExpr {
    pub expr: Expr,
}

// ---------------------------------------------------------------------------
// Entity
// ---------------------------------------------------------------------------

/// `entity(type, id_expr)`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct EntityClause {
    pub entity_type: EntityTypeVal,
    pub id_expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum EntityTypeVal {
    Ident(String),
    StringLit(String),
}

// ---------------------------------------------------------------------------
// Yield
// ---------------------------------------------------------------------------

/// `yield target (name = expr, ...)`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct YieldClause {
    pub target: String,
    pub args: Vec<NamedArg>,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct NamedArg {
    pub name: String,
    pub value: Expr,
}

// ---------------------------------------------------------------------------
// Field references
// ---------------------------------------------------------------------------

/// Field selector within a step branch: `.ident` or `["string"]`.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum FieldSelector {
    Dot(String),
    Bracket(String),
}

/// Field reference in expressions.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum FieldRef {
    /// Bare identifier, e.g. `sip`.
    Simple(String),
    /// Qualified, e.g. `fail.sip`.
    Qualified(String, String),
    /// Bracket notation, e.g. `fail["detail.sha256"]`.
    Bracketed(String, String),
}

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BinOp {
    And,
    Or,
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Expr {
    /// Number literal (integer or float).
    Number(f64),
    /// String literal.
    StringLit(String),
    /// Boolean literal.
    Bool(bool),
    /// Field reference.
    Field(FieldRef),
    /// Binary operation.
    BinOp {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary negation.
    Neg(Box<Expr>),
    /// Function call: `name(args...)` or `qualifier.name(args...)`.
    FuncCall {
        qualifier: Option<String>,
        name: String,
        args: Vec<Expr>,
    },
    /// `expr in (v1, v2, ...)` or `expr not in (v1, v2, ...)`.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
}

// ---------------------------------------------------------------------------
// Contract block (rule contract testing)
// ---------------------------------------------------------------------------

/// `contract name for rule_name { given { ... } expect { ... } [options { ... }] }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ContractBlock {
    pub name: String,
    pub rule_name: String,
    pub given: Vec<GivenStmt>,
    pub expect: Vec<ExpectStmt>,
    pub options: Option<ContractOptions>,
}

/// Statement inside a `given { ... }` block.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum GivenStmt {
    /// `row(alias, field = expr, ...);`
    Row {
        alias: String,
        fields: Vec<FieldAssign>,
    },
    /// `tick(duration);`
    Tick(Duration),
}

/// `name = expr` — field assignment in a `row(...)` statement.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FieldAssign {
    pub name: String,
    pub value: Expr,
}

/// Statement inside an `expect { ... }` block.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ExpectStmt {
    /// `hits cmp_op INTEGER;`
    Hits { cmp: CmpOp, count: usize },
    /// `hit[index].assert;`
    HitAssert { index: usize, assert: HitAssert },
}

/// Assertion on a specific hit output.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum HitAssert {
    /// `score cmp_op NUMBER`
    Score { cmp: CmpOp, value: f64 },
    /// `close_reason == STRING`
    CloseReason { value: String },
    /// `entity_type == STRING`
    EntityType { value: String },
    /// `entity_id == STRING`
    EntityId { value: String },
    /// `field(STRING) cmp_op expr`
    Field {
        name: String,
        cmp: CmpOp,
        value: Expr,
    },
}

/// Options for a contract block.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ContractOptions {
    pub close_trigger: Option<CloseTrigger>,
    pub eval_mode: Option<EvalMode>,
}

/// Window close trigger mode for contract testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseTrigger {
    Timeout,
    Flush,
    Eos,
}

/// Evaluation mode for contract testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EvalMode {
    Strict,
    Lenient,
}
