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
    /// 顶层列表声明（issue #73）: `name = (item, ...)` 裸绑定——供规则以
    /// `expr in <name>` 引用, 编译期展开为字面列表。`use "file.wfl"` 把目标
    /// 文件的列表并入当前文件（include 语义, 无可见性控制）。
    pub lists: Vec<ListDecl>,
    pub rules: Vec<RuleDecl>,
    pub tests: Vec<TestBlock>,
}

/// `name = ("a", "b", ...)`——顶层命名字面列表声明。
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct ListDecl {
    pub name: String,
    /// 列表元素（字面量; 编译期检查元素类型与展开处 InList 的类型校验一致）。
    pub items: Vec<Expr>,
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
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
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

/// `rule name { meta events [let ...]* stage_chain entity yield [conv] [limits] }`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct RuleDecl {
    pub name: String,
    pub meta: Option<MetaBlock>,
    pub events: EventsBlock,
    /// Per-event bindings: `let <name> = <expr>` — evaluated once per event
    /// (on-each path), referenced by bare `<name>` in later expressions.
    pub lets: Vec<LetDecl>,
    pub match_clause: MatchClause,
    pub each_clause: Option<EachClause>,
    /// 声明式窗口统计（stats 形态，与 match 互斥——规则体二选一）。
    pub stats_clause: Option<StatsClause>,
    pub score: ScoreExpr,
    pub joins: Vec<JoinClause>,
    /// `where <expr>` — post-join filter evaluated after all joins enrich the
    /// event context and before alert construction. `false`/`None` suppresses
    /// the output (strict semantics, aligning INNER JOIN miss-drop).
    pub r#where: Option<Expr>,
    pub pipeline_stages: Vec<PipelineStage>,
    pub entity: EntityClause,
    pub yield_clause: YieldClause,
    pub pattern_origin: Option<PatternOrigin>,
    pub conv: Option<ConvClause>,
    pub limits: Option<LimitsBlock>,
}

/// One `let <name> = <expr>` binding (evaluated once per event on the on-each
/// path; the bound value is injected into the event's field map, so later
/// expressions reference it by bare name).
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangRule")]
pub struct LetDecl {
    pub name: String,
    pub expr: Expr,
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
