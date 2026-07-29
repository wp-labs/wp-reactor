use super::*;

// ---------------------------------------------------------------------------
// Top-level
// ---------------------------------------------------------------------------

/// A complete `.wfl` file.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct WflFile {
    pub uses: Vec<UseDecl>,
    pub patterns: Vec<PatternDecl>,
    pub yield_presets: Vec<YieldPresetDecl>,
    pub rules: Vec<RuleDecl>,
    pub tests: Vec<TestBlock>,
}

/// A reusable yield field set: `yield preset name [<params...>] (...)`.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct YieldPresetDecl {
    pub name: String,
    pub params: Vec<YieldPresetParam>,
    pub args: Vec<NamedArg>,
}

/// One parameter declared by a parameterized yield preset.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct YieldPresetParam {
    pub name: String,
    pub default: Option<Expr>,
}

/// A pattern declaration: `pattern name(params) { body }`
///
/// The body is stored as raw text containing a `match<...> { ... } -> score(...)`.
/// When a rule invokes the pattern, parameters are textually substituted and the
/// body is parsed as a concrete `MatchClause` + `ScoreExpr`.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
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
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct UseDecl {
    pub path: String,
}

// ---------------------------------------------------------------------------
// Rule
// ---------------------------------------------------------------------------

/// One `match ... [-> score(...)] [join ...]*` segment in a pipeline.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct PipelineStage {
    pub match_clause: MatchClause,
    pub each_clause: Option<EachClause>,
    pub joins: Vec<JoinClause>,
}

/// `rule name { meta events stage_chain entity yield [conv] [limits] }`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct RuleDecl {
    pub name: String,
    pub meta: Option<MetaBlock>,
    pub events: EventsBlock,
    pub match_clause: MatchClause,
    pub each_clause: Option<EachClause>,
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
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct MetaBlock {
    pub entries: Vec<MetaEntry>,
}

#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct MetaEntry {
    pub key: String,
    pub value: String,
}
