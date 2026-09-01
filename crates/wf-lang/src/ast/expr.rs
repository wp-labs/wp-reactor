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
    /// Bracket notation, e.g. `fail["detail.sha256"]` (flat dotted field name).
    Bracketed(String, String),
    /// Multi-level nested access into `object` / `array` fields,
    /// e.g. `s.roles_obj.source.process.uid` or `s.roles_obj.related[0].name`.
    ///
    /// Invariant: `alias` names the event/set alias whose schema contains
    /// `segments[0]` (always a [`PathSegment::Field`]). At evaluation time the
    /// flat field map is keyed by field name, so the traversal starts from the
    /// root segment — `alias` is only used by the compiler for bind tracking and
    /// by the checker for root resolution.
    Path {
        alias: String,
        segments: Vec<PathSegment>,
    },
}

/// One step of a nested field path: a member name or an array index.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathSegment {
    Field(String),
    Index(usize),
}

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
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

#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum SystemVar {
    Score,
    EventFirstTime,
    EventLastTime,
    EvidenceStartTime,
    EvidenceEndTime,
    WindowStartTime,
    WindowEndTime,
    EmitTime,
}

pub use crate::wfu_meta::WfuMetaField;

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExpr")]
pub struct ObjectItem {
    pub targets: Vec<String>,
    pub type_hint: Option<crate::schema::FieldType>,
    pub value: Expr,
}

/// 模式匹配分支（issue #79 Issue 2）：`pat1 | pat2 => value`。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangExpr")]
pub struct MatchArm {
    /// 本分支的模式值表达式（求值后与 subject 比较；`|` 表示多个模式）。
    pub patterns: Vec<Expr>,
    /// 命中本分支时的结果表达式。
    pub value: Expr,
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangExpr")]
pub enum Expr {
    /// Number literal (integer or float).
    Number(f64),
    /// String literal.
    StringLit(String),
    /// Boolean literal.
    Bool(bool),
    /// System variable reference, e.g. `@score`.
    SystemVar(SystemVar),
    /// wfusion-managed metadata field reference, e.g. `@__wfu_rule_name`.
    WfuMeta(WfuMetaField),
    /// Field reference.
    Field(FieldRef),
    /// Parameter reference inside a parameterized yield preset body, e.g. `$severity`.
    PresetParam(String),
    /// Binary operation.
    BinOp {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary negation.
    Neg(Box<Expr>),
    /// Logical negation (`not <cond>` / `!<cond>`), Sigma 条件取反（issue #22）。
    Not(Box<Expr>),
    /// Function call: `name(args...)` or `qualifier.name(args...)`.
    FuncCall {
        qualifier: Option<String>,
        name: String,
        args: Vec<Expr>,
    },
    /// Structured object literal: `object { key = expr; }`.
    Object(Vec<ObjectItem>),
    /// Structured array literal: `array [expr, ...]`.
    Array(Vec<Expr>),
    /// `expr in (v1, v2, ...)` or `expr not in (v1, v2, ...)`.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// 公共允许列表引用（issue #73）: `expr in <shared_name>` 的右值——解析期
    /// 产出, 编译期由 `resolve_list_refs` 展开为字面列表; checker/运行时
    /// 见不到本变体（展开后仅剩字面 InList）。
    ListRef(String),
    /// Conditional expression: `if cond then yes else no`.
    IfThenElse {
        cond: Box<Expr>,
        then_expr: Box<Expr>,
        else_expr: Box<Expr>,
    },
    /// 模式匹配表达式（issue #79 Issue 2）：
    /// `match <expr> { pat1 | pat2 => arm, ..., _ => default }`。
    /// 模式求值后与 subject 按值比较（同 `in` 的相等语义）；短路：命中即
    /// 返回对应 arm，未命中继续下一分支；`_` 默认分支兜底；无默认且全部
    /// 未命中 → None。
    Match {
        expr: Box<Expr>,
        arms: Vec<MatchArm>,
        default: Option<Box<Expr>>,
    },
}
