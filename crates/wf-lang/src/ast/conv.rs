use super::Expr;

/// `conv { chain; chain; ... }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ConvClause {
    pub chains: Vec<ConvChain>,
}

/// `step | step | step ;`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ConvChain {
    pub steps: Vec<ConvStep>,
}

/// A single conv operation: `sort(expr)`, `top(N)`, `dedup(expr)`, `where(expr)`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ConvStep {
    Sort(Vec<SortKey>),
    Top(u64),
    Dedup(Expr),
    Where(Expr),
}

/// Sort key with direction: `expr` (ascending) or `-expr` (descending).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SortKey {
    pub expr: Expr,
    pub descending: bool,
}
