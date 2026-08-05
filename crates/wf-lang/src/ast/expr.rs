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
    /// Bracket notation, e.g. `fail["detail.sha256"]` (flat dotted field name).
    Bracketed(String, String),
    /// Multi-level nested access into `object` / `array` fields,
    /// e.g. `s.roles_obj.source.process.uid` or `s.roles_obj.related[0].name`.
    ///
    /// Invariant: `alias` names the event/set alias whose schema contains
    /// `segments[0]` (always a [`PathSegment::Field`]). At evaluation time the
    /// flat field map is keyed by field name, so the traversal starts from the
    /// root segment — `alias` is only used by the compiler for bind tracking and
    /// by the checker for root resolution.
    Path { alias: String, segments: Vec<PathSegment> },
}

/// One step of a nested field path: a member name or an array index.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathSegment {
    Field(String),
    Index(usize),
}

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
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

#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum SystemVar {
    Score,
    EventFirstTime,
    EventLastTime,
    EvidenceStartTime,
    EvidenceEndTime,
    WindowStartTime,
    WindowEndTime,
    EmitTime,
}

pub use crate::wfu_meta::WfuMetaField;

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExpr")]
pub struct ObjectItem {
    pub targets: Vec<String>,
    pub type_hint: Option<crate::schema::FieldType>,
    pub value: Expr,
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum Expr {
    /// Number literal (integer or float).
    Number(f64),
    /// String literal.
    StringLit(String),
    /// Boolean literal.
    Bool(bool),
    /// System variable reference, e.g. `@score`.
    SystemVar(SystemVar),
    /// wfusion-managed metadata field reference, e.g. `@__wfu_rule_name`.
    WfuMeta(WfuMetaField),
    /// Field reference.
    Field(FieldRef),
    /// Parameter reference inside a parameterized yield preset body, e.g. `$severity`.
    PresetParam(String),
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
    /// Structured object literal: `object { key = expr; }`.
    Object(Vec<ObjectItem>),
    /// Structured array literal: `array [expr, ...]`.
    Array(Vec<Expr>),
    /// `expr in (v1, v2, ...)` or `expr not in (v1, v2, ...)`.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// Conditional expression: `if cond then yes else no`.
    IfThenElse {
        cond: Box<Expr>,
        then_expr: Box<Expr>,
        else_expr: Box<Expr>,
    },
}
