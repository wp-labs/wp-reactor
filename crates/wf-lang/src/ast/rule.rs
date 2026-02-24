use super::*;

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

/// `use "path.wfs"`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct UseDecl {
    pub path: String,
}

// ---------------------------------------------------------------------------
// Rule
// ---------------------------------------------------------------------------

/// `rule name { meta events match->score [join...] entity yield [limits] }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RuleDecl {
    pub name: String,
    pub meta: Option<MetaBlock>,
    pub events: EventsBlock,
    pub match_clause: MatchClause,
    pub score: ScoreExpr,
    pub joins: Vec<JoinClause>,
    pub entity: EntityClause,
    pub yield_clause: YieldClause,
    pub limits: Option<LimitsBlock>,
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
