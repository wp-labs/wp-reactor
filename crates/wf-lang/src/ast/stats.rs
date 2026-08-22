use std::time::Duration;

use super::*;

// ---------------------------------------------------------------------------
// Stats clause — 声明式窗口统计（与 match/on each 平级的一等执行形态）
// ---------------------------------------------------------------------------

/// `stats<dur[:mode]> [group by (k1, k2, ...)] [tier f [b1, b2, ...]] { measure; ... }`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangStats")]
pub struct StatsClause {
    pub window: StatsWindow,
    /// 桶键表达式列表（`group by`），空 = 空键全局。
    /// v6：tier/bucket 也是桶键函数，统一进此列表。
    pub keys: Vec<Expr>,
    /// 输出形状：行展开（每桶一行，缺省）或列展开（每桶一列，pivot）。
    pub output_shape: StatsOutputShape,
    /// 度量声明（含 where 行过滤）。
    pub measures: Vec<StatsMeasure>,
}

/// 窗口规格：`<dur[:mode]>`。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangStats")]
pub struct StatsWindow {
    pub duration: Duration,
    pub mode: StatsWindowMode,
}

/// stats 窗口模式（复用 match 的 fixed/session；sliding 为后续扩展）。
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangStats")]
pub enum StatsWindowMode {
    Fixed,
    Session,
}

/// 输出形状。
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangStats")]
pub enum StatsOutputShape {
    /// 每桶一行（缺省）。
    Rows,
    /// 每桶一列（单行多列，输出时 pivot 转置）。
    Columns,
}

/// 一个度量声明：`b | agg(field) as label [where expr]`。
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangStats")]
pub struct StatsMeasure {
    pub label: String,
    pub source_alias: String,
    /// 行过滤（与桶键叠加），`where expr`。
    pub where_expr: Option<Expr>,
    pub agg: StatsAgg,
    /// `sum(field)` 等的字段引用。
    pub field: Option<FieldRef>,
    /// `top(N, field)` 的 N。
    pub arg: Option<u64>,
}

/// 统计聚合函数。
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangStats")]
pub enum StatsAgg {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    DistinctCount,
    /// 扩展：保留最新整行（Q18）。
    Last,
    /// 扩展：per-key top-N（Q19）。
    Top,
}
