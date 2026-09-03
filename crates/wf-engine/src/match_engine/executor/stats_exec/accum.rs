//! 状态/累加类型（v6 §6.1）：数值累加器、SoA 组布局、度量累加器枚举、
//! distinct/top 键集与条目。

use wf_cep::rows::RowFields;
use wf_lang::plan::{StatsAggPlan, StatsPlan};

use crate::match_engine::EngineHashSet;

use super::field_name;

// ---------------------------------------------------------------------------
// 状态结构（v6 §6.1 — 无匹配进度, 纯累加）
// ---------------------------------------------------------------------------

/// 数值累加器（count/sum/avg/min/max 度量共享）。avg 不作状态——输出时
/// sum/count 求得（D6）。Box 化后每度量仅 8B 指针（2026-08-26 q18 紧凑化）。
#[derive(Debug, Clone, Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct NumericAccum {
    pub count: u64,
    pub sum: i128,
    pub min: Option<i128>,
    pub max: Option<i128>,
}

/// 纯数值计划桶的 SoA 累加器（2026-08-27 q17）：counts/sums/mins/maxs 平行
/// 数组按类型紧凑存储——免 [`StatsAccum`] 枚举分派 + `Box` 解引用（旧热路径每
/// 度量 1 次 match + 1 次指针追逐 → 数组直写）。
///
/// 仅用于**全 Count/Sum/Avg/Min/Max** 计划（q17 形态）；含 distinct/last/top
/// 的计划仍走 [`StatsAccum`]（[`StatsBucketAccs::Classic`]）。索引映射与同列分
/// 组见 [`NumericSoALayout`]（executor 构造期预计算，热路径零计算）。
#[derive(Debug, Clone, Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct NumericSoA {
    /// 每度量 count（索引 = 度量 idx；含 where 过滤；avg 输出时 sum/count）。
    pub counts: Box<[u64]>,
    /// sum/avg 度量的 sum（紧凑：仅 sum/avg 度量，索引 = `sum_slot[idx]`）。
    pub sums: Box<[i128]>,
    /// min 度量当前最小值（紧凑，索引 = `min_slot[idx]`）。
    pub mins: Box<[Option<i128>]>,
    /// max 度量当前最大值（紧凑，索引 = `max_slot[idx]`）。
    pub maxs: Box<[Option<i128>]>,
}

/// 数值度量聚合类别（SoA 分派用——比 `StatsAggPlan` 全枚举窄的分支宽度）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.StatsEngine")]
pub(crate) enum NumericKind {
    Sum,
    Min,
    Max,
}

/// 同列数值度量分组: 一次列值读取共享给同列多个度量（q17: price 列
/// sum+avg+min+max 4 度量共享 1 次 [`column_i128_at`]——旧路径每度量 1 次
/// 重复读取同一列）。组内度量同一字段（plan 静态, 构造期分组）；列索引运行
/// 期从 `measure_field_cols[entries[0].0]` 取一次（组内共享）。
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub(crate) struct SoAColGroup {
    /// 该字段的 (度量 idx, 聚合类别) 列表。
    pub entries: Box<[(usize, NumericKind)]>,
}

/// 纯数值计划的 SoA 布局（executor 构造期预计算一次）：每度量到紧凑数组的
/// 槽映射 + 同字段分组。仅依赖 plan（无批依赖）——窗口重建后不变。
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub(crate) struct NumericSoALayout {
    /// 度量数（= `counts.len()`）。
    pub n_measures: usize,
    /// 每度量 → sums 槽（None = 非 sum/avg 度量）。
    pub sum_slot: Box<[Option<u32>]>,
    /// 每度量 → mins 槽（None = 非 min 度量）。
    pub min_slot: Box<[Option<u32>]>,
    /// 每度量 → maxs 槽（None = 非 max 度量）。
    pub max_slot: Box<[Option<u32>]>,
    /// 同字段数值度量分组（确定性字段序）。
    pub groups: Box<[SoAColGroup]>,
}

impl NumericSoALayout {
    /// 构建（仅全数值计划调用）。
    pub(crate) fn build(plan: &StatsPlan) -> Self {
        let n = plan.measures.len();
        let mut sum_slot: Vec<Option<u32>> = vec![None; n];
        let mut min_slot: Vec<Option<u32>> = vec![None; n];
        let mut max_slot: Vec<Option<u32>> = vec![None; n];
        let (mut n_sum, mut n_min, mut n_max) = (0u32, 0u32, 0u32);
        for (i, m) in plan.measures.iter().enumerate() {
            match m.agg {
                StatsAggPlan::Sum | StatsAggPlan::Avg => {
                    sum_slot[i] = Some(n_sum);
                    n_sum += 1;
                }
                StatsAggPlan::Min => {
                    min_slot[i] = Some(n_min);
                    n_min += 1;
                }
                StatsAggPlan::Max => {
                    max_slot[i] = Some(n_max);
                    n_max += 1;
                }
                _ => {}
            }
        }
        // 同字段分组: 按字段名聚合「有字段的数值度量」（sum/avg/min/max）。
        let mut by_field: std::collections::HashMap<String, Vec<(usize, NumericKind)>> =
            std::collections::HashMap::new();
        for (i, m) in plan.measures.iter().enumerate() {
            let kind = match m.agg {
                StatsAggPlan::Sum | StatsAggPlan::Avg => NumericKind::Sum,
                StatsAggPlan::Min => NumericKind::Min,
                StatsAggPlan::Max => NumericKind::Max,
                _ => continue,
            };
            let Some(fr) = &m.field else {
                continue;
            };
            by_field
                .entry(field_name(fr).to_string())
                .or_default()
                .push((i, kind));
        }
        let mut fields: Vec<String> = by_field.keys().cloned().collect();
        fields.sort();
        let groups: Vec<SoAColGroup> = fields
            .into_iter()
            .map(|f| SoAColGroup {
                entries: by_field.remove(&f).unwrap().into_boxed_slice(),
            })
            .collect();
        Self {
            n_measures: n,
            sum_slot: sum_slot.into_boxed_slice(),
            min_slot: min_slot.into_boxed_slice(),
            max_slot: max_slot.into_boxed_slice(),
            groups: groups.into_boxed_slice(),
        }
    }

    /// 全零 SoA（新桶首见）。
    pub(crate) fn zeros(&self) -> NumericSoA {
        NumericSoA {
            counts: vec![0u64; self.n_measures].into_boxed_slice(),
            sums: vec![0i128; self.sum_slot.iter().flatten().count()].into_boxed_slice(),
            mins: vec![None; self.min_slot.iter().flatten().count()].into_boxed_slice(),
            maxs: vec![None; self.max_slot.iter().flatten().count()].into_boxed_slice(),
        }
    }
}

/// 度量专用累加器（2026-08-26 q18 紧凑化）：全功能 `StatsAccum` 208B →
/// 按度量类型分派变体（plan 静态已知，每度量一型）——q18 4×last 从 832B →
/// 128B（行字段另共享 1 份 [`RowFields`]）。enum 总大小 32B（tag + 最大变体
/// `Top` 24B）。
///
/// 变体与 `StatsAggPlan` 一一对应：
///   count/sum/avg/min/max → [`Numeric`](StatsAccum::Numeric)（Box 8B）
///   distinct_count        → [`Distinct`](StatsAccum::Distinct)（Box 8B）
///   last                  → [`Last`](StatsAccum::Last)（`Option<Arc<RowFields>>` 16B）
///   top                   → [`Top`](StatsAccum::Top)（`Vec<TopEntry>` 24B）
///
/// 热路径经 [`StatsAccum::numeric_mut`] 等按调用点已分派的 `measure.agg` 取
/// 对应变体（变体不符 = plan/构造不一致的内部错误，panic 尽早暴露）。
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.StatsEngine")]
pub enum StatsAccum {
    Numeric(Box<NumericAccum>),
    Distinct(Box<DistinctSet>),
    /// `last(field)` 用（Q18）: 最近合格行的**行字段紧凑存储**（`Arc` 跨同桶
    /// 多个 last 度量共享，见 [`RowFields`]；null 由内部 mask 标记）。
    Last(Option<std::sync::Arc<RowFields>>),
    /// `top(N, field)` 用（Q19）: 按 key DESC 有序的 top-N 条目。
    Top(Vec<TopEntry>),
}

impl Default for StatsAccum {
    /// 默认 = 空 Numeric（测试/merge bench 构造用；生产按 plan 经
    /// [`StatsAccum::for_measure`] 建变体）。
    fn default() -> Self {
        StatsAccum::Numeric(Box::default())
    }
}

impl StatsAccum {
    /// 按 plan 度量类型构造变体（空键预建桶 / 新桶首见）。
    pub fn for_measure(agg: &wf_lang::plan::StatsAggPlan) -> Self {
        match agg {
            wf_lang::plan::StatsAggPlan::Count
            | wf_lang::plan::StatsAggPlan::Sum
            | wf_lang::plan::StatsAggPlan::Avg
            | wf_lang::plan::StatsAggPlan::Min
            | wf_lang::plan::StatsAggPlan::Max => StatsAccum::Numeric(Box::default()),
            wf_lang::plan::StatsAggPlan::DistinctCount => StatsAccum::Distinct(Box::default()),
            wf_lang::plan::StatsAggPlan::Last => StatsAccum::Last(None),
            wf_lang::plan::StatsAggPlan::Top => StatsAccum::Top(Vec::new()),
        }
    }

    /// 按 plan 度量列表构造累加器数组（索引对齐 `plan.measures`）。
    pub fn accs_for_plan(plan: &wf_lang::plan::StatsPlan) -> Vec<StatsAccum> {
        plan.measures
            .iter()
            .map(|m| StatsAccum::for_measure(&m.agg))
            .collect()
    }

    // -- 热路径访问器（调用点已按 measure.agg 分派；变体不符 = 内部错误）--

    #[track_caller]
    pub fn numeric(&self) -> &NumericAccum {
        match self {
            StatsAccum::Numeric(a) => a,
            _ => panic!(
                "StatsAccum 变体不符: 期望 Numeric, 实际 {:?}",
                std::mem::discriminant(self)
            ),
        }
    }

    #[track_caller]
    pub fn numeric_mut(&mut self) -> &mut NumericAccum {
        match self {
            StatsAccum::Numeric(a) => a,
            _ => panic!("StatsAccum 变体不符: 期望 Numeric"),
        }
    }

    #[track_caller]
    pub fn distinct_mut(&mut self) -> &mut DistinctSet {
        match self {
            StatsAccum::Distinct(d) => d,
            _ => panic!("StatsAccum 变体不符: 期望 Distinct"),
        }
    }

    #[track_caller]
    pub fn last_mut(&mut self) -> &mut Option<std::sync::Arc<RowFields>> {
        match self {
            StatsAccum::Last(r) => r,
            _ => panic!("StatsAccum 变体不符: 期望 Last"),
        }
    }

    #[track_caller]
    pub fn last(&self) -> &Option<std::sync::Arc<RowFields>> {
        match self {
            StatsAccum::Last(r) => r,
            _ => panic!("StatsAccum 变体不符: 期望 Last"),
        }
    }

    #[track_caller]
    pub fn top_mut(&mut self) -> &mut Vec<TopEntry> {
        match self {
            StatsAccum::Top(v) => v,
            _ => panic!("StatsAccum 变体不符: 期望 Top"),
        }
    }

    #[track_caller]
    pub fn top(&self) -> &Vec<TopEntry> {
        match self {
            StatsAccum::Top(v) => v,
            _ => panic!("StatsAccum 变体不符: 期望 Top"),
        }
    }
}

/// top-N 条目: 排序键 + 行字段紧凑存储（yield 经 field_values 注入读 `b.*`）。
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct TopEntry {
    /// 排序键（数值; 与行式 `value_to_f64` 同口径）。
    pub key: f64,
    /// 条目行字段（2026-08-26 紧凑化，同 [`RowFields`]；null 跳过, 与行式
    /// Event 一致; 列序 = `row_field_names`）。
    pub row: RowFields,
}

/// Distinct key: 从列式原生值构造（i64/timestamp 域内哈希, D7）——
/// 禁止 f64 化（ValueKey::from_value 的 >2^53 分歧）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.StatsEngine")]
pub enum DistinctKey {
    Int(i64),
    /// 非整数数值（小数）—— 保持原 f64 位（canonical）。
    Float(u64),
    Str(Box<str>),
}

/// distinct_count 的紧凑存储（2026-08-26 q16 内存）：整数键（q16 的
/// bidder/auction 主战场）走 `HashSet<i64>`（8B/项）——原 enum `DistinctKey`
/// 因 `Box<str>` 变体占 16B/项；Float/Str 键保留 enum 集合。两集合语义互斥
/// （insert 按类型路由），len/merge 各自合并。
#[derive(Debug, Clone, Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct DistinctSet {
    ints: EngineHashSet<i64>,
    others: EngineHashSet<DistinctKey>,
}

impl DistinctSet {
    /// 插入按类型路由；返回是否新值（供内存估算增量记账）。
    pub fn insert(&mut self, key: DistinctKey) -> bool {
        match key {
            DistinctKey::Int(v) => self.ints.insert(v),
            other => self.others.insert(other),
        }
    }

    pub fn len(&self) -> usize {
        self.ints.len() + self.others.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ints.is_empty() && self.others.is_empty()
    }

    /// merge（分片 union）：整数/非整数分别 extend（跨片 hasher 可不同，
    /// 与旧 `EngineHashSet::extend` 同语义）。
    pub fn extend_other(&mut self, other: &DistinctSet) {
        for v in &other.ints {
            self.ints.insert(*v);
        }
        for k in &other.others {
            self.others.insert(k.clone());
        }
    }

    /// owned merge（merge 候选 bench 用；语义与 [`Self::extend_other`] 一致）。
    pub fn extend(&mut self, other: DistinctSet) {
        for v in other.ints {
            self.ints.insert(v);
        }
        for k in other.others {
            self.others.insert(k);
        }
    }

    /// 预扩容（merge 候选 bench 用）。
    pub fn reserve(&mut self, additional: usize) {
        self.ints.reserve(additional);
        self.others.reserve(additional);
    }

    // -- spill 序列化访问器（pub(crate)：仅 wf-engine 内部 spill 模块使用）--

    /// 整数键集合。
    pub(crate) fn ints(&self) -> &EngineHashSet<i64> {
        &self.ints
    }

    /// 非整数键集合（Float/Str）。
    pub(crate) fn others(&self) -> &EngineHashSet<DistinctKey> {
        &self.others
    }

    /// 从两集合构造（spill 读回）。
    pub(crate) fn from_parts(ints: EngineHashSet<i64>, others: EngineHashSet<DistinctKey>) -> Self {
        Self { ints, others }
    }
}

impl DistinctKey {
    pub fn from_i64(v: i64) -> Self {
        DistinctKey::Int(v)
    }
    pub fn from_f64(v: f64) -> Self {
        if v.fract() == 0.0 && v.abs() < 9_007_199_254_740_992.0 {
            DistinctKey::Int(v as i64)
        } else {
            DistinctKey::Float(if v == 0.0 {
                0.0f64.to_bits()
            } else if v.is_nan() {
                f64::NAN.to_bits()
            } else {
                v.to_bits()
            })
        }
    }
    #[allow(clippy::should_implement_trait)] // 与 from_i64/from_f64 平行的构造器命名，非 FromStr 实现
    pub fn from_str(s: &str) -> Self {
        DistinctKey::Str(s.into())
    }
}
