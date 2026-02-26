use super::*;

// ---------------------------------------------------------------------------
// Top-level
// ---------------------------------------------------------------------------

/// A complete `.wfl` file.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct WflFile {
    pub uses: Vec<UseDecl>,
    pub patterns: Vec<PatternDecl>,
    pub rules: Vec<RuleDecl>,
    pub tests: Vec<TestBlock>,
}

/// A pattern declaration: `pattern name(params) { body }`
///
/// The body is stored as raw text containing a `match<...> { ... } -> score(...)`.
/// When a rule invokes the pattern, parameters are textually substituted and the
/// body is parsed as a concrete `MatchClause` + `ScoreExpr`.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PatternDecl {
    pub name: String,
    pub params: Vec<String>,
    pub body: String,
}

/// Tracks which pattern was used to generate the match clause (for `wf explain`).
#[derive(Debug, Clone, PartialEq)]
pub struct PatternOrigin {
    pub pattern_name: String,
    pub args: Vec<String>,
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

/// One `match ... [-> score(...)] [join ...]*` segment in a pipeline.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct PipelineStage {
    pub match_clause: MatchClause,
    pub joins: Vec<JoinClause>,
}

/// `rule name { meta events stage_chain entity yield [conv] [limits] }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RuleDecl {
    pub name: String,
    pub meta: Option<MetaBlock>,
    pub events: EventsBlock,
    pub match_clause: MatchClause,
    pub score: ScoreExpr,
    pub joins: Vec<JoinClause>,
    pub pipeline_stages: Vec<PipelineStage>,
    pub entity: EntityClause,
    pub yield_clause: YieldClause,
    pub pattern_origin: Option<PatternOrigin>,
    pub conv: Option<ConvClause>,
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
