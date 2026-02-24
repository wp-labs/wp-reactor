use super::*;

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

/// `yield target[@vN] (name = expr, ...)`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct YieldClause {
    pub target: String,
    pub version: Option<u32>,
    pub args: Vec<NamedArg>,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct NamedArg {
    pub name: String,
    pub value: Expr,
}
