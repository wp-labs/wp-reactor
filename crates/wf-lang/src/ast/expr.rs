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
    /// Conditional expression: `if cond then yes else no`.
    IfThenElse {
        cond: Box<Expr>,
        then_expr: Box<Expr>,
        else_expr: Box<Expr>,
    },
}
