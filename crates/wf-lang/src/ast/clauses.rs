use super::*;

// ---------------------------------------------------------------------------
// Score
// ---------------------------------------------------------------------------

/// `-> score(expr)`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangClauses")]
pub struct ScoreExpr {
    pub expr: Expr,
}

// ---------------------------------------------------------------------------
// Entity
// ---------------------------------------------------------------------------

/// `entity(type, id_expr)`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangClauses")]
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

/// `yield target[@vN] [: preset, ...] (name = expr, ...)`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangClauses")]
pub struct YieldClause {
    pub target: String,
    pub version: Option<u32>,
    pub presets: Vec<String>,
    pub args: Vec<NamedArg>,
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct NamedArg {
    pub name: String,
    pub value: Expr,
}
