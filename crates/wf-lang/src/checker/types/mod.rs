mod check_expr;
mod check_funcs;
mod infer;
mod pipe;

pub use check_expr::check_expr_type;
pub use infer::infer_type;
pub use pipe::check_pipe_chain;

use crate::schema::BaseType;

// ---------------------------------------------------------------------------
// ValType — lightweight type representation for semantic checks
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValType {
    /// A known scalar base type (Chars, Digit, Float, Time, Ip, Hex).
    Base(BaseType),
    /// Array of a base type.
    Array(BaseType),
    /// Numeric literal — compatible with Digit and Float.
    Numeric,
    /// Boolean value.
    Bool,
}

// ---------------------------------------------------------------------------
// Type compatibility helpers
// ---------------------------------------------------------------------------

pub fn compatible(expected: &ValType, actual: &ValType) -> bool {
    match (expected, actual) {
        (ValType::Base(a), ValType::Base(b)) => a == b,
        (ValType::Array(a), ValType::Array(b)) => a == b,
        (ValType::Base(BaseType::Digit), ValType::Numeric)
        | (ValType::Numeric, ValType::Base(BaseType::Digit)) => true,
        (ValType::Base(BaseType::Float), ValType::Numeric)
        | (ValType::Numeric, ValType::Base(BaseType::Float)) => true,
        (ValType::Numeric, ValType::Numeric) => true,
        (ValType::Bool, ValType::Bool) => true,
        (ValType::Bool, ValType::Base(BaseType::Bool))
        | (ValType::Base(BaseType::Bool), ValType::Bool) => true,
        _ => false,
    }
}

pub fn is_numeric(t: &ValType) -> bool {
    matches!(
        t,
        ValType::Base(BaseType::Digit) | ValType::Base(BaseType::Float) | ValType::Numeric
    )
}

pub fn is_orderable(t: &ValType) -> bool {
    matches!(
        t,
        ValType::Base(BaseType::Digit)
            | ValType::Base(BaseType::Float)
            | ValType::Base(BaseType::Time)
            | ValType::Base(BaseType::Chars)
            | ValType::Numeric
    )
}

/// Whether a type is a scalar identity type usable as entity id (T33).
pub fn is_scalar_identity(t: &ValType) -> bool {
    matches!(
        t,
        ValType::Base(BaseType::Chars)
            | ValType::Base(BaseType::Ip)
            | ValType::Base(BaseType::Hex)
            | ValType::Base(BaseType::Digit)
            | ValType::Numeric
    )
}

/// Numeric promotion: if both sides are numeric, compute the result type.
pub fn numeric_promote(a: &ValType, b: &ValType) -> Option<ValType> {
    if !is_numeric(a) || !is_numeric(b) {
        return None;
    }
    // Float wins over Digit; Numeric is compatible with both.
    if *a == ValType::Base(BaseType::Float) || *b == ValType::Base(BaseType::Float) {
        Some(ValType::Base(BaseType::Float))
    } else if *a == ValType::Base(BaseType::Digit) || *b == ValType::Base(BaseType::Digit) {
        Some(ValType::Base(BaseType::Digit))
    } else {
        Some(ValType::Numeric)
    }
}

pub(super) fn op_symbol(op: crate::ast::BinOp) -> &'static str {
    use crate::ast::BinOp;
    match op {
        BinOp::And => "&&",
        BinOp::Or => "||",
        BinOp::Eq => "==",
        BinOp::Ne => "!=",
        BinOp::Lt => "<",
        BinOp::Gt => ">",
        BinOp::Le => "<=",
        BinOp::Ge => ">=",
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
    }
}
