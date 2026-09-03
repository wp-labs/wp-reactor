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

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangClauses")]
pub enum EntityTypeVal {
    Ident(String),
    StringLit(String),
}

// ---------------------------------------------------------------------------
// Yield
// ---------------------------------------------------------------------------

/// `yield target[@vN] [: preset[<args...>], ...] (name = expr, ...)`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangClauses")]
pub struct YieldClause {
    pub target: String,
    pub version: Option<u32>,
    pub presets: Vec<YieldPresetRef>,
    pub args: Vec<NamedArg>,
}

#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangClauses")]
pub struct YieldPresetRef {
    pub name: String,
    pub args: Vec<Expr>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangClauses")]
pub struct NamedArg {
    pub name: String,
    pub value: Expr,
}
