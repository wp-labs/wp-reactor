use std::time::Duration;

use super::*;

// ---------------------------------------------------------------------------
// Join clause
// ---------------------------------------------------------------------------

/// `join window [mode] [within ...] [reduce ...] on cond [&& cond] [as label] [emit at expr]`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangJoin")]
pub struct JoinClause {
    pub target_window: String,
    pub mode: JoinMode,
    pub conditions: Vec<JoinCondition>,
    /// `within` 时间区间谓词（`[lo, hi]` 或 `dur` 糖）；与 mode 正交（设计 D1）。
    pub within: Option<WithinSpec>,
    /// `reduce` 归约（maxrow/minrow/last/top）+ `as label`（设计 D6/D7）。
    pub reduce: Option<ReduceClause>,
    /// `emit at <expr>`——deferred 标记 + 触发点（设计 D5；P1 语法/计划，P3 执行）。
    pub emit_at: Option<Expr>,
}

/// Join time-point semantics.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangJoin")]
pub enum JoinMode {
    Snapshot,
    Asof {
        within: Option<Duration>,
    },
    Anti,
    /// 缺省 mode（∅）：纯存在（inner）——命中则输出，miss 丢（设计 D4）。
    Inner,
}

/// `left == right` in a join on-clause.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangJoin")]
pub struct JoinCondition {
    pub left: FieldRef,
    pub right: FieldRef,
}

// ---------------------------------------------------------------------------
// within — 时间区间谓词（与 mode 正交，设计 D1）
// ---------------------------------------------------------------------------

/// `within [lo, hi]` 时间区间谓词。
///
/// 界为「相对左事件 ts 的时长偏移」或「左行绝对时间表达式（字段/函数）」。
/// 常量界由 checker 校验右窗 `over ≥ 跨度`；行内界需 wfs 显式声明（设计 D3）。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangJoin")]
pub struct WithinSpec {
    pub lo: Bound,
    pub hi: Bound,
}

/// 区间界：`['<' | '<='] (dur | expr)`。`open` 表 `<` 前缀（开区间）；`<=`/缺省为闭。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangJoin")]
pub struct Bound {
    pub open: bool,
    pub val: BoundVal,
}

/// 界值：相对时长（可负）或左行绝对时间表达式。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangJoin")]
pub enum BoundVal {
    /// 相对左事件 ts 的时长偏移；`neg` 表负向。
    /// `within 10s` 糖 ≡ `within [-10s, 0s]`（lo = -10s, neg = true）。
    Dur { dur: Duration, neg: bool },
    /// 绝对时间表达式（左行字段或函数，如 `a.expires`、`bucket_end(p.dateTime, 10s)`）。
    Expr(Expr),
}

// ---------------------------------------------------------------------------
// reduce — 匹配集归约（设计 D6/D7）
// ---------------------------------------------------------------------------

/// `reduce measure [as label]`。
///
/// `as label`：归约整行以 object value 注入 eval context（`ctx.fields[label]`），
/// `label.field` 编译为 `FieldRef::Path`（review R2——裸名会丢限定词取错行）。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangJoin")]
pub struct ReduceClause {
    pub measure: ReduceMeasure,
    pub label: Option<String>,
}

/// 归约度量：从匹配集里选行（返回行 family）。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangJoin")]
pub enum ReduceMeasure {
    /// 度量字段最大的那一行（非标量；`max(field)` 是标量值，两者并存）。
    Maxrow {
        field: FieldRef,
        tie: Option<TieSpec>,
    },
    /// 度量字段最小的那一行。
    Minrow {
        field: FieldRef,
        tie: Option<TieSpec>,
    },
    /// 匹配集内最新一行（按右窗时间）。
    Last { field: FieldRef },
    /// 度量字段 top-N（按度量降序取前 N 行）。
    Top { n: u64, field: FieldRef },
}

/// 平手规则：`tie(field asc|desc)` ≡ ORDER BY 次键（设计 §4.2）。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangJoin")]
pub struct TieSpec {
    pub field: FieldRef,
    /// `desc` = 降序；缺省 `asc`。
    pub desc: bool,
}
