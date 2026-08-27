//! Stats executor — 声明式窗口统计执行器（P1: 空键 fixed count/distinct）。
//!
//! 与 CEP(RuleExecutor/match)完全正交: 消费 fanout 的 raw RecordBatch,
//! 内部维护可交换结合的归并状态, 窗口 close 时产出度量值并复用 alert 构建。
//!
//! 设计依据: docs/stats-executor-design.md v6（§6 执行器）。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsPlan};

use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};
use crate::match_engine::event_bridge::extract_field_value;
use crate::match_engine::match_engine::{Event, ScopeKey, field_ref_name};
use crate::match_engine::{EngineHashMap, EngineHashSet, Value};
use crate::window::scope_key_columnar;
use crate::window::scope_key_from_column;

// ---------------------------------------------------------------------------
// 批级 where mask 共享缓存（2026-08-27 q17 分片去重）
// ---------------------------------------------------------------------------
//
// 背景: 分片（rule_parallelism=S）下每片 `process_batch_rows` 对**整批**计算
// where mask（`eval_guard_columnar` 向量化全批）——同一批被 S 片重复 S 次
// （q17 10 片: mask 占 rules 段 CPU ~85%, sample 顶栈 eval_vec 家族第一热点）。
// 首片算完写缓存, 其余片复用（保持向量化, 总量 S×→1×）。
//
// 正确性: mask 是「批 + where 表达式」的纯函数（同一 executor 各片 where 相
// 同）。key = 首列 Arc 指针 + 行数——各片持同一批的**列 Arc 共享**（window
// 批存储 `Arc<RecordBatch>` 浅克隆, `read_since_with_shard` 每片拿值副本但列
// 数组 Arc 同源）; 缓存**强持有批 Arc** → 列指针不会释放复用 → 无碰撞。
//
// 内存: 流式批下同窗口的批几乎同时被各片消费, 缓存持有最近 ~max_rows 行
// （列数据 Arc 共享, 防释放不复制）; 超限整体清空（旧批已处理完, 安全）。

/// 分片共享的批级 where mask 缓存（`Arc<RecordBatch>` 强持有防指针复用）。
#[derive(Debug)]
pub struct StatsMaskCache {
    inner: std::sync::Mutex<std::collections::HashMap<(usize, usize), (Arc<RecordBatch>, Arc<Vec<BooleanArray>>)>>,
    /// 容量上限（总行数; 超限整体清空——流式批下旧批已消费完）。
    /// pub(crate) 供测试缩容验证清理。
    pub(crate) max_rows: usize,
    total_rows: std::sync::atomic::AtomicUsize,
}

impl Default for StatsMaskCache {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsMaskCache {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(std::collections::HashMap::new()),
            max_rows: 4_000_000,
            total_rows: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// 取或算（算完写缓存）。`compute` = 批 → where mask 列表（仅未命中时调用）。
    pub fn get_or_compute(
        &self,
        batch: &RecordBatch,
        compute: impl FnOnce() -> Vec<BooleanArray>,
    ) -> Arc<Vec<BooleanArray>> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Arc::new(compute()); // 空批不缓存
        }
        let key = (
            std::sync::Arc::as_ptr(batch.column(0)) as *const () as usize,
            rows,
        );
        let mut guard = self.inner.lock().expect("mask cache poisoned");
        if let Some((_, masks)) = guard.get(&key) {
            return Arc::clone(masks);
        }
        let masks = Arc::new(compute());
        // 容量检查在插入前: 超限先清旧批（当前批正被各片消费, 保留）。
        let new_total = self
            .total_rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed)
            + rows;
        if new_total > self.max_rows {
            guard.clear();
            self.total_rows
                .store(rows, std::sync::atomic::Ordering::Relaxed);
        }
        guard.insert(key, (Arc::new(batch.clone()), Arc::clone(&masks)));
        masks
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().expect("poisoned").len()
    }
}

// ---------------------------------------------------------------------------
// 状态结构（v6 §6.1 — 无匹配进度, 纯累加）
// ---------------------------------------------------------------------------

/// 数值累加器（count/sum/avg/min/max 度量共享）。avg 不作状态——输出时
/// sum/count 求得（D6）。Box 化后每度量仅 8B 指针（2026-08-26 q18 紧凑化）。
#[derive(Debug, Clone, Default)]
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
#[derive(Debug, Clone, Default)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericKind {
    Sum,
    Min,
    Max,
}

/// 同列数值度量分组: 一次列值读取共享给同列多个度量（q17: price 列
/// sum+avg+min+max 4 度量共享 1 次 [`column_i128_at`]——旧路径每度量 1 次
/// 重复读取同一列）。组内度量同一字段（plan 静态, 构造期分组）；列索引运行
/// 期从 `measure_field_cols[entries[0].0]` 取一次（组内共享）。
#[derive(Debug, Clone)]
pub struct SoAColGroup {
    /// 该字段的 (度量 idx, 聚合类别) 列表。
    pub entries: Box<[(usize, NumericKind)]>,
}

/// 纯数值计划的 SoA 布局（executor 构造期预计算一次）：每度量到紧凑数组的
/// 槽映射 + 同字段分组。仅依赖 plan（无批依赖）——窗口重建后不变。
#[derive(Debug, Clone)]
pub struct NumericSoALayout {
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
    pub fn build(plan: &StatsPlan) -> Self {
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
    pub fn zeros(&self) -> NumericSoA {
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct TopEntry {
    /// 排序键（数值; 与行式 `value_to_f64` 同口径）。
    pub key: f64,
    /// 条目行字段（2026-08-26 紧凑化，同 [`RowFields`]；null 跳过, 与行式
    /// Event 一致; 列序 = `row_field_names`）。
    pub row: RowFields,
}

/// Distinct key: 从列式原生值构造（i64/timestamp 域内哈希, D7）——
/// 禁止 f64 化（ValueKey::from_value 的 >2^53 分歧）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DistinctKey {
    Int(i64),
    /// 非整数数值（小数）—— 保持原 f64 位（canonical）。
    Float(u64),
    Str(Box<str>),
}

/// 行字段槽型（2026-08-26 q18/q19：stats last/top 行字段紧凑化）。
/// 每字段一个槽位：数字→`numeric`（f64 8B）、字符串→`strings`（SmolStr 24B
/// 内联）、其它→`others`（原 `Option<Value>` 万能盒回退）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowFieldSlot {
    Numeric(usize),
    Str(usize),
    Other(usize),
}

/// 字段类型分派表（executor 级，所有桶共享；列式从 batch schema 构建，
/// 行式无静态类型时退化为全 Other——不紧凑但正确）。
#[derive(Debug, Clone, PartialEq)]
pub struct RowFieldLayout {
    slots: Vec<RowFieldSlot>,
    n_numeric: usize,
    n_strings: usize,
    n_others: usize,
}

impl RowFieldLayout {
    /// 从 batch schema 构建（列式路径：字段类型静态已知）。
    /// `names` = 行字段列序（P5 子集，或全部 schema 字段排序）。
    pub fn from_schema(names: &[String], schema: &arrow::datatypes::Schema) -> Self {
        let mut slots = Vec::with_capacity(names.len());
        let (mut n_num, mut n_str, mut n_oth) = (0, 0, 0);
        for name in names {
            let slot = match schema.column_with_name(name).map(|(_, f)| f.data_type()) {
                Some(arrow::datatypes::DataType::Int8)
                | Some(arrow::datatypes::DataType::Int16)
                | Some(arrow::datatypes::DataType::Int32)
                | Some(arrow::datatypes::DataType::Int64)
                | Some(arrow::datatypes::DataType::UInt8)
                | Some(arrow::datatypes::DataType::UInt16)
                | Some(arrow::datatypes::DataType::UInt32)
                | Some(arrow::datatypes::DataType::UInt64)
                | Some(arrow::datatypes::DataType::Float32)
                | Some(arrow::datatypes::DataType::Float64)
                | Some(arrow::datatypes::DataType::Timestamp(_, _)) => {
                    let s = RowFieldSlot::Numeric(n_num);
                    n_num += 1;
                    s
                }
                Some(arrow::datatypes::DataType::Utf8)
                | Some(arrow::datatypes::DataType::LargeUtf8) => {
                    let s = RowFieldSlot::Str(n_str);
                    n_str += 1;
                    s
                }
                _ => {
                    let s = RowFieldSlot::Other(n_oth);
                    n_oth += 1;
                    s
                }
            };
            slots.push(slot);
        }
        Self {
            slots,
            n_numeric: n_num,
            n_strings: n_str,
            n_others: n_oth,
        }
    }

    /// 全 Other 兜底（行式路径无静态 schema 类型时；不紧凑但语义一致）。
    pub fn all_other(names: &[String]) -> Self {
        Self {
            slots: names
                .iter()
                .enumerate()
                .map(|(i, _)| RowFieldSlot::Other(i))
                .collect(),
            n_numeric: 0,
            n_strings: 0,
            n_others: names.len(),
        }
    }

    pub fn n_fields(&self) -> usize {
        self.slots.len()
    }

    pub fn n_numeric(&self) -> usize {
        self.n_numeric
    }

    pub fn n_strings(&self) -> usize {
        self.n_strings
    }

    pub fn n_others(&self) -> usize {
        self.n_others
    }

    pub fn slot(&self, i: usize) -> RowFieldSlot {
        self.slots[i]
    }
}

/// 行字段紧凑存储（stats last/top 的行字段数组）。
/// `Arc<[Option<Value>]>`（56B/字段）→ 按 [`RowFieldLayout`] 槽分派：
/// 数字 8B / 字符串 24B（内联）/ 其它回退。null 由 `null_mask` 位标记
/// （numeric 的 NaN 与 strings 的空串都是合法数据，不能作哨兵）。
/// 自包含 layout（Arc），下游（stats_task 注入）可独立读取。
#[derive(Debug, Clone, PartialEq)]
pub struct RowFields {
    layout: std::sync::Arc<RowFieldLayout>,
    numeric: Box<[f64]>,
    strings: Box<[smol_str::SmolStr]>,
    others: Box<[Option<Value>]>,
    null_mask: Box<[u64]>,
}

impl RowFields {
    pub fn empty(layout: std::sync::Arc<RowFieldLayout>) -> Self {
        let n = layout.n_fields();
        let n_numeric = layout.n_numeric;
        let n_strings = layout.n_strings;
        let n_others = layout.n_others;
        Self {
            layout,
            numeric: vec![0.0; n_numeric].into_boxed_slice(),
            strings: vec![smol_str::SmolStr::default(); n_strings].into_boxed_slice(),
            others: vec![None; n_others].into_boxed_slice(),
            null_mask: vec![0u64; n.div_ceil(64)].into_boxed_slice(),
        }
    }

    pub fn layout(&self) -> &std::sync::Arc<RowFieldLayout> {
        &self.layout
    }

    fn mask_bit(&mut self, i: usize, is_null: bool) {
        let word = i / 64;
        let bit = i % 64;
        if is_null {
            self.null_mask[word] |= 1 << bit;
        } else {
            self.null_mask[word] &= !(1 << bit);
        }
    }

    fn mask_get(&self, i: usize) -> bool {
        (self.null_mask[i / 64] >> (i % 64)) & 1 == 1
    }

    /// 按字段位置写值（v = None → null）。
    pub fn set(&mut self, i: usize, v: Option<Value>) {
        match (self.layout.slot(i), v) {
            (RowFieldSlot::Numeric(idx), Some(Value::Number(n))) => {
                self.numeric[idx] = n;
                self.mask_bit(i, false);
            }
            (RowFieldSlot::Str(idx), Some(Value::Str(s))) => {
                self.strings[idx] = s;
                self.mask_bit(i, false);
            }
            (RowFieldSlot::Other(idx), Some(v)) => {
                self.others[idx] = Some(v);
                self.mask_bit(i, false);
            }
            (_, None) => {
                self.mask_bit(i, true);
            }
            // 值类型与槽型不符（行式路径按值路由的边界）→ null（与提取失败一致）。
            (_, Some(_)) => {
                self.mask_bit(i, true);
            }
        }
    }

    /// 按字段位置读值（null → None）。
    pub fn value_at(&self, i: usize) -> Option<Value> {
        if self.mask_get(i) {
            return None;
        }
        match self.layout.slot(i) {
            RowFieldSlot::Numeric(idx) => Some(Value::Number(self.numeric[idx])),
            RowFieldSlot::Str(idx) => Some(Value::Str(self.strings[idx].clone())),
            RowFieldSlot::Other(idx) => self.others[idx].clone(),
        }
    }

    /// 按字段位置读数字（top 排序键 / last measure_value）。
    pub fn f64_at(&self, i: usize) -> Option<f64> {
        if self.mask_get(i) {
            return None;
        }
        match self.layout.slot(i) {
            RowFieldSlot::Numeric(idx) => Some(self.numeric[idx]),
            RowFieldSlot::Other(idx) => self.others[idx].as_ref().and_then(value_to_f64),
            RowFieldSlot::Str(_) => None,
        }
    }

    /// 按字段位置迭代（下游 field_values 注入用；与 `Arc<[Option<Value>]>`
    /// 的 iter 同构）。
    pub fn iter_values(&self) -> impl Iterator<Item = Option<Value>> + '_ {
        (0..self.layout.n_fields()).map(move |i| self.value_at(i))
    }
}

/// distinct_count 的紧凑存储（2026-08-26 q16 内存）：整数键（q16 的
/// bidder/auction 主战场）走 `HashSet<i64>`（8B/项）——原 enum `DistinctKey`
/// 因 `Box<str>` 变体占 16B/项；Float/Str 键保留 enum 集合。两集合语义互斥
/// （insert 按类型路由），len/merge 各自合并。
#[derive(Debug, Clone, Default)]
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

// ---------------------------------------------------------------------------
// StatsExecutor — 执行状态
// ---------------------------------------------------------------------------

/// 窗口状态: 桶 → 度量累加器数组（索引对齐 `StatsPlan.measures`）。
/// 空键规则恒单桶（`ScopeKey::Empty` 键, P1 快路径不变）; 带 key（P2）每
/// `(key 组合)` 一桶。
///
/// **复合键优化（P5+）**: 桶表键为**扁平哈希 u64** + 碰撞链 `Vec<StatsBucket>`——
/// 列式路径每事件只做「栈上叶数组 + 哈希 + 链扫描」, 无每事件 `ScopeKey::Pair`
/// Box 分配; 完整 `ScopeKey` 仅**每桶首见**时构建一次（Q18: 27.6M → 5.29M 次盒装）。
/// 哈希为字节级同构 FNV 混合（`scope_key_hash` == `comps_hash`, 行式/列式两路径
/// 同桶）; 碰撞由链内 `ScopeKey` 完整比较消歧（概率极低, 正确性不受影响）。
#[derive(Default)]
pub struct StatsWindowState {
    pub buckets: EngineHashMap<u64, Vec<StatsBucket>>,
    pub window_start_nanos: i64,
    pub last_event_nanos: i64,
    pub event_count: u64,
    /// 状态内存上限（字节; None = 不设防）。由规则 `limits.max_memory` 注入
    /// （spawn 层）。**超限后拒绝新建桶**（已有桶继续累积, 内存有界）——语义上
    /// 是「新键丢失」的优雅降级, 有日志 + 计数可观测, 不是静默膨胀到 OOM。
    limit_bytes: Option<u64>,
    /// 估算的在用状态内存（桶级预算模型: 新桶固定 allowance, 含 top/last 条目
    /// 预算——保守上界, 偏安全方向）。窗口 close 时清零。
    estimated_bytes: u64,
    /// 累计超限拒收的新桶数（跨窗口累计, 供指标/告警）。
    over_limit_new_buckets: u64,
    /// 当前窗口是否已告警（每窗口一次, 防刷屏）。
    limit_warned: bool,
    /// 告警用的规则名（set_memory_limit 注入）。
    rule_name: String,
    /// 纯数值计划 SoA 布局（None = 含 distinct/last/top, 走 Classic 累加器）。
    /// 窗口重建（reset）后不变——按 plan 重算。
    soa_layout: Option<NumericSoALayout>,
}

impl StatsWindowState {
    /// 新建窗口状态（无内存限制, 由 spawn 层按规则 limits 注入）。空键规则
    /// 在此预建 Empty 单桶（快路径）。
    fn new(buckets: EngineHashMap<u64, Vec<StatsBucket>>, plan: &StatsPlan) -> Self {
        // 全数值计划（count/sum/avg/min/max）→ SoA 桶; 含 distinct/last/top → Classic。
        let soa_layout = plan
            .measures
            .iter()
            .all(|m| matches!(m.agg, StatsAggPlan::Count | StatsAggPlan::Sum | StatsAggPlan::Avg | StatsAggPlan::Min | StatsAggPlan::Max))
            .then(|| NumericSoALayout::build(plan));
        let mut buckets = buckets;
        if buckets.is_empty() && plan.keys.is_empty() {
            StatsWindowState::seed_empty_bucket(&mut buckets, plan, soa_layout.as_ref());
        }
        StatsWindowState {
            buckets,
            window_start_nanos: 0,
            last_event_nanos: 0,
            event_count: 0,
            limit_bytes: None,
            estimated_bytes: 0,
            over_limit_new_buckets: 0,
            limit_warned: false,
            rule_name: String::new(),
            soa_layout,
        }
    }

    /// 注入状态内存上限（字节; None = 不设防）。
    pub fn set_memory_limit(&mut self, rule_name: &str, bytes: Option<usize>) {
        self.rule_name = rule_name.to_string();
        self.limit_bytes = bytes.map(|b| b as u64);
    }

    /// 当前估算的在用状态内存（桶级预算）。
    pub fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    /// distinct 集合每项估算字节（2026-08-26 q16）：`HashSet<i64>` 8B/项 +
    /// foldhash 控制字/负载因子（87.5% 满）≈ 12B；others（enum 16B + 同开销）
    /// ≈ 24B。统一取 24B（保守上界，guard 宁可早拒）。
    const DISTINCT_ENTRY_BYTES: u64 = 24;

    /// 批末刷新状态内存估算（2026-08-26 q16）：
    /// `estimated_bytes = 桶数×allowance + Σ桶 distinct len×DISTINCT_ENTRY_BYTES`
    /// ——distinct 集合此前完全不计（q16 带 key + 8 distinct_count 的 19G 实际
    /// vs 8GB 估算形同虚设）。O(桶数) 每批（q16 ~10k 桶 × 2857 批可接受）。
    /// guard 检查（新建桶）用刷新后的值，反映真实。
    fn refresh_estimated_bytes(&mut self, plan: &StatsPlan) {
        let allowance = Self::bucket_allowance(plan, self.soa_layout.is_some());
        let mut distinct_bytes = 0u64;
        for buckets in self.buckets.values() {
            for bucket in buckets {
                if let StatsBucketAccs::Classic(accs) = &bucket.accs {
                    for acc in accs {
                        if let StatsAccum::Distinct(set) = acc {
                            distinct_bytes += set.len() as u64 * Self::DISTINCT_ENTRY_BYTES;
                        }
                    }
                }
            }
        }
        self.estimated_bytes = self.buckets.len() as u64 * allowance + distinct_bytes;
    }

    /// 累计因超限被拒收的新桶数。
    pub fn over_limit_new_buckets(&self) -> u64 {
        self.over_limit_new_buckets
    }

    /// 新桶预算（保守上界）: 固定基数 + Σ每度量变体 + 行字段共享份额 +
    /// top/last 条目预算。`soa` = 纯数值计划（SoA 桶）——平行数组紧凑口径。
    ///
    /// **2026-08-26 q18 校准**（对齐度量专用累加器）: 旧口径 512 + n×128 +
    /// last 160B/度量 → 1664B/键，高估真实 1.55× → 16GB 预算拒收阈值 961 万
    /// 键 < 30M 数据键数 2300 万 → **静默丢键**（over_limit_new_buckets）。
    /// 现按变体实际求和 + 行字段**每桶共享 1 份**（last/top 度量同桶同一
    /// [`RowFields`] Arc——`row_cache` 每行 1 份）。
    ///
    /// **2026-08-27 SoA 校准**: SoA 桶无 Box/枚举——固定基数 + counts n×8 +
    /// sums n_sum×16 + mins n_min×16 + maxs n_max×16（q17 8 度量 → 384B, 旧
    /// Classic 口径 896B 高估 2.3×——预算小/键多时误拒）。1.6× 余量同 Classic。
    ///
    /// **已知限制**: `distinct_set` 值域增长不在固定基数内（q16 教训）——由
    /// [`Self::refresh_estimated_bytes`] 批末按真实 len 计入（保守上界）。
    fn bucket_allowance(plan: &StatsPlan, soa: bool) -> u64 {
        // 桶固定: ScopeKey 栈+堆(~72B) + StatsBucket 头 + accs Vec + HashMap
        // 槽(~64B) ≈ 160B → 取 256（1.6× 保守余量）。
        let mut bytes = 256u64;
        if soa {
            // SoA 桶: 平行数组紧凑布局（无 Box 无枚举）——按类型子集求和。
            let n = plan.measures.len() as u64;
            let (mut n_sum, mut n_min, mut n_max) = (0u64, 0u64, 0u64);
            for m in &plan.measures {
                match m.agg {
                    StatsAggPlan::Sum | StatsAggPlan::Avg => n_sum += 1,
                    StatsAggPlan::Min => n_min += 1,
                    StatsAggPlan::Max => n_max += 1,
                    StatsAggPlan::Count => {}
                    _ => unreachable!("SoA 仅数值度量"),
                }
            }
            return bytes + n * 8 + n_sum * 16 + n_min * 16 + n_max * 16;
        }
        let mut has_row_fields = false;
        for m in &plan.measures {
            match m.agg {
                StatsAggPlan::Top => {
                    bytes += 24; // Vec<TopEntry> 头
                    bytes += m.arg.unwrap_or(10) * 160; // 条目（key + 行字段）
                    has_row_fields = true;
                }
                StatsAggPlan::Last => {
                    bytes += 16; // Option<Arc<RowFields>>
                    has_row_fields = true;
                }
                StatsAggPlan::DistinctCount => bytes += 96, // DistinctSet（Box 外）
                _ => bytes += 80,                           // NumericAccum（Box 外）
            }
        }
        if has_row_fields {
            bytes += 112; // 共享 1 份 RowFields 堆（q18 6 字段 ≈ 104B + 余量）
        }
        bytes
    }
}

/// 限额记账（2026-08-27 拆为自由函数）: `entry` 匹配内 `buckets` 已被占用时,
/// 限额字段（estimated_bytes/over_limit/limit_warned/rule_name）与 `buckets`
/// 借用不相交, 可同时访问——语义与旧 `account_new_bucket` 方法完全一致。
fn account_bucket_allowed(
    limit_bytes: Option<u64>,
    estimated_bytes: &mut u64,
    over_limit_new_buckets: &mut u64,
    limit_warned: &mut bool,
    rule_name: &str,
    allowance: u64,
) -> bool {
    if let Some(limit) = limit_bytes
        && *estimated_bytes + allowance > limit
    {
        *over_limit_new_buckets += 1;
        if !*limit_warned {
            *limit_warned = true;
            log::warn!(
                "stats 状态内存超限（规则 {}, 估算 {}B / 上限 {}B）——拒绝新建键桶, 已有桶继续累积; 累计拒收 {} 行（新桶尝试）",
                rule_name,
                estimated_bytes,
                limit,
                over_limit_new_buckets
            );
        }
        return false;
    }
    *estimated_bytes += allowance;
    true
}

/// 桶累加器载体: 纯数值计划 → [`Numeric`](StatsBucketAccs::Numeric)（SoA, q17
/// 形态）; 含 distinct/last/top → [`Classic`](StatsBucketAccs::Classic)（原有
/// [`StatsAccum`] 数组）。分派在累积/读取/合并入口各一次（每行, 非每度量）。
#[derive(Debug, Clone)]
pub enum StatsBucketAccs {
    Numeric(NumericSoA),
    Classic(Vec<StatsAccum>),
}

/// 单桶: 完整 [`ScopeKey`]（close 排序/输出; 每桶一次构建）+ 累加器载体。
#[derive(Debug, Clone)]
pub struct StatsBucket {
    pub scope_key: ScopeKey,
    pub accs: StatsBucketAccs,
}

/// 新桶累加器载体（按计划形态分派）: 全数值 → SoA; 含 distinct/last/top →
/// Classic（原有 [`StatsAccum`] 数组）。
fn new_bucket_accs(plan: &StatsPlan, soa_layout: Option<&NumericSoALayout>) -> StatsBucketAccs {
    match soa_layout {
        Some(layout) => StatsBucketAccs::Numeric(layout.zeros()),
        None => StatsBucketAccs::Classic(StatsAccum::accs_for_plan(plan)),
    }
}

impl StatsWindowState {
    /// 预建空键单桶（`ScopeKey::Empty`）——哈希路径 `bucket_mut(&Empty)` 命中。
    fn seed_empty_bucket(
        buckets: &mut EngineHashMap<u64, Vec<StatsBucket>>,
        plan: &StatsPlan,
        soa_layout: Option<&NumericSoALayout>,
    ) {
        buckets.insert(
            scope_key_hash(&ScopeKey::Empty),
            vec![StatsBucket {
                scope_key: ScopeKey::Empty,
                accs: new_bucket_accs(plan, soa_layout),
            }],
        );
    }

    /// 取/建一个桶（完整键路径: 行式回退 / 空键规则用）。哈希与列式
    /// `keyed_bucket_mut` 同值, 链内按 ScopeKey 完整比较消歧。
    /// 新桶先过限额检查（超限 → None, 调用方跳过该行——内存有界）。
    ///
    /// **单次 entry 查找（2026-08-27 q17）**: 旧实现 `get`（找 pos）+ `get_mut`
    /// （取链）两次哈希查找——已存在桶的每事件命中是主流, 双查找纯浪费。
    fn bucket_mut(
        &mut self,
        key: &ScopeKey,
        plan: &StatsPlan,
    ) -> Option<&mut StatsBucketAccs> {
        use std::collections::hash_map::Entry;
        let hash = scope_key_hash(key);
        let allowance = Self::bucket_allowance(plan, self.soa_layout.is_some());
        match self.buckets.entry(hash) {
            Entry::Occupied(o) => {
                let chain = o.into_mut();
                if let Some(i) = chain.iter().position(|b| &b.scope_key == key) {
                    return Some(&mut chain[i].accs);
                }
                // 同 hash 不同键（碰撞, 极罕见）= 新桶 → 记账 + push。限额字段与
                // buckets 借用不相交, 直接字段访问。
                if !account_bucket_allowed(
                    self.limit_bytes,
                    &mut self.estimated_bytes,
                    &mut self.over_limit_new_buckets,
                    &mut self.limit_warned,
                    &self.rule_name,
                    allowance,
                ) {
                    return None;
                }
                chain.push(StatsBucket {
                    scope_key: key.clone(),
                    accs: new_bucket_accs(plan, self.soa_layout.as_ref()),
                });
                let last = chain.len() - 1;
                Some(&mut chain[last].accs)
            }
            Entry::Vacant(v) => {
                if !account_bucket_allowed(
                    self.limit_bytes,
                    &mut self.estimated_bytes,
                    &mut self.over_limit_new_buckets,
                    &mut self.limit_warned,
                    &self.rule_name,
                    allowance,
                ) {
                    return None;
                }
                // 链 Vec 容量精确 1（2026-08-26 q18 状态 2.3× 归因）：`or_default()`
                // 空 Vec push 1 个后 capacity=4（Rust 标准库 0→4 起步）→ 每链占 4 桶
                // 容量（192B）实装 1 桶（48B）——q18 每键独立 hash（链均长 1.0）→
                // 2935 万链 × 144B ≈ 4.2G 纯浪费。`vec![..]` 精确 1 桶。
                let chain = v.insert(vec![StatsBucket {
                    scope_key: key.clone(),
                    accs: new_bucket_accs(plan, self.soa_layout.as_ref()),
                }]);
                Some(&mut chain[0].accs)
            }
        }
    }

    /// 取/建一个桶（列式扁平键路径）: `hash` = 叶数组哈希, `comps` = 栈上叶
    /// 数组（列序）。链内按 `comps` 与完整键比较消歧; 未命中时构建完整键
    /// （每桶一次）。新桶先过限额检查（超限 → None）。
    ///
    /// **单次 entry 查找（2026-08-27 q17）**: 命中主流（在航 auction 窗口内
    /// 重复引用 ~100%）, 旧 get + get_mut 双查找纯浪费。
    /// pub(crate) 供 rules 段分解 bench（q17_rules_breakdown）。
    pub(crate) fn keyed_bucket_mut(
        &mut self,
        hash: u64,
        comps: &[ScopeKey],
        plan: &StatsPlan,
    ) -> Option<&mut StatsBucketAccs> {
        use std::collections::hash_map::Entry;
        let allowance = Self::bucket_allowance(plan, self.soa_layout.is_some());
        match self.buckets.entry(hash) {
            Entry::Occupied(o) => {
                let chain = o.into_mut();
                if let Some(i) = chain
                    .iter()
                    .position(|b| comps_match(&b.scope_key, comps, 0, comps.len()))
                {
                    return Some(&mut chain[i].accs);
                }
                // 碰撞（同 hash 不同键 = 新桶, 极罕见）: 记账 + push。
                if !account_bucket_allowed(
                    self.limit_bytes,
                    &mut self.estimated_bytes,
                    &mut self.over_limit_new_buckets,
                    &mut self.limit_warned,
                    &self.rule_name,
                    allowance,
                ) {
                    return None;
                }
                let scope_key = scope_key_from_comps(comps);
                chain.push(StatsBucket {
                    scope_key,
                    accs: new_bucket_accs(plan, self.soa_layout.as_ref()),
                });
                let last = chain.len() - 1;
                Some(&mut chain[last].accs)
            }
            Entry::Vacant(v) => {
                if !account_bucket_allowed(
                    self.limit_bytes,
                    &mut self.estimated_bytes,
                    &mut self.over_limit_new_buckets,
                    &mut self.limit_warned,
                    &self.rule_name,
                    allowance,
                ) {
                    return None;
                }
                let scope_key = scope_key_from_comps(comps);
                let chain = v.insert(vec![StatsBucket {
                    scope_key,
                    accs: new_bucket_accs(plan, self.soa_layout.as_ref()),
                }]);
                Some(&mut chain[0].accs)
            }
        }
    }

    /// 按完整键取桶（测试/调试用; 生产走哈希路径）。
    pub fn find_bucket(&self, key: &ScopeKey) -> Option<&StatsBucketAccs> {
        self.buckets
            .get(&scope_key_hash(key))
            .and_then(|chain| chain.iter().find(|b| &b.scope_key == key))
            .map(|b| &b.accs)
    }

    /// 清空并拍平全部桶（close 用）: `(ScopeKey, accs)` 按 ScopeKey 升序。
    /// 同时清零内存账本（新窗口重新累积; 拒收计数保留——指标用）。
    fn take_buckets(&mut self) -> Vec<(ScopeKey, StatsBucketAccs)> {
        let mut out: Vec<(ScopeKey, StatsBucketAccs)> = std::mem::take(&mut self.buckets)
            .into_values()
            .flat_map(|chain| chain.into_iter().map(|b| (b.scope_key, b.accs)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        self.estimated_bytes = 0;
        self.limit_warned = false;
        out
    }
}

/// 执行器: 消费行/批次, 按 StatsPlan 归并, 窗口 close 时产出度量值。
pub struct StatsExecutor {
    pub plan: StatsPlan,
    /// 窗口状态（桶表; 空键规则仅 Empty 桶）。
    pub window: StatsWindowState,
    /// 当前窗口的过期上界（水印推进, 触发 close）。
    pub watermark_nanos: i64,
    /// 去重后的 where 表达式（相同条件共享一次求值——q15 9 个度量 where → 3
    /// 个唯一条件; 行式实现的关键优化: 每行 1 次 Event 构建 + n_unique 次 eval）。
    unique_wheres: Vec<Expr>,
    /// 每度量对应 `unique_wheres` 的索引; `None` = 无条件度量（恒通过）。
    measure_where: Vec<Option<usize>>,
    /// P5 紧凑化: 子集字段名的**确定排序**（executor 构造时从传入子集排序
    /// 派生; None = 全列, 列序在提取时确定）。行字段列数组按此列序存储。
    row_field_names: Option<std::sync::Arc<Vec<String>>>,
    /// 每度量字段在行字段列数组中的位置（预计算, 热路径免字符串查找;
    /// last/top 且字段在子集内 → Some, 其余 None）。
    measure_field_idx: Vec<Option<usize>>,
    /// 行字段类型分派（2026-08-26 q18/q19 紧凑化）：列式路径首次 `process_batch`
    /// 从 batch schema 构建；行式路径（无静态类型）退化全 Other（不紧凑但正确）。
    row_field_layout: Option<std::sync::Arc<RowFieldLayout>>,
    /// 纯数值计划 SoA 布局（与 `window.soa_layout` 同源; executor 级副本——
    /// 热路径在 `window` 被 `&mut` 借用时仍可读, 免借用冲突）。None = 非 SoA。
    soa_layout: Option<NumericSoALayout>,
}

impl StatsExecutor {
    pub fn new(plan: StatsPlan) -> Self {
        Self::with_row_fields(plan, None)
    }

    /// 行字段 layout（2026-08-26）：列式已建 → 复用；否则按字段名全 Other
    /// （行式无静态 schema 类型——不紧凑但语义一致）。
    fn row_fields_layout_for_row(
        &self,
        names: Option<&[String]>,
    ) -> std::sync::Arc<RowFieldLayout> {
        if let Some(l) = &self.row_field_layout {
            return std::sync::Arc::clone(l);
        }
        let names: Vec<String> = match names {
            Some(ns) => ns.to_vec(),
            None => vec![],
        };
        std::sync::Arc::new(RowFieldLayout::all_other(&names))
    }

    /// 列式路径确保 layout（首次从 batch schema 构建并缓存）。
    fn ensure_row_field_layout(&mut self, batch: &RecordBatch) -> std::sync::Arc<RowFieldLayout> {
        if let Some(l) = &self.row_field_layout {
            return std::sync::Arc::clone(l);
        }
        let names: Vec<String> = match &self.row_field_names {
            Some(ns) => ns.as_ref().clone(),
            None => {
                let mut ns: Vec<String> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();
                ns.sort();
                ns
            }
        };
        let layout = std::sync::Arc::new(RowFieldLayout::from_schema(&names, &batch.schema()));
        self.row_field_layout = Some(std::sync::Arc::clone(&layout));
        layout
    }

    /// 指定 last/top 行字段提取子集（None = 全部 schema 列）。
    ///
    /// **生产路径（spawn）恒传子集**——`None` 仅测试/缺省: 无子集时列数组列序
    /// 在提取时确定（行式行键排序/列式 schema 排序）, `measure_field_idx` 无法
    /// 构造期预计算, last/top 的**度量值**在标量/close 路径退化为 0.0（行字段
    /// 仍保留, 注入需任务层另行约定列序）。Q18/Q19 类规则必须传子集。
    pub fn with_row_fields(
        plan: StatsPlan,
        row_fields: Option<std::sync::Arc<HashSet<String>>>,
    ) -> Self {
        // 同表达式对同行的求值结果一致, 可安全共享（设计 §6.2 段1c 的行式对应）。
        let mut unique_wheres: Vec<Expr> = Vec::new();
        let mut measure_where: Vec<Option<usize>> = Vec::with_capacity(plan.measures.len());
        for m in &plan.measures {
            match &m.where_expr {
                None => measure_where.push(None),
                Some(e) => match unique_wheres.iter().position(|u| u == e) {
                    Some(i) => measure_where.push(Some(i)),
                    None => {
                        measure_where.push(Some(unique_wheres.len()));
                        unique_wheres.push(e.clone());
                    }
                },
            }
        }
        // P5: 子集字段名确定排序（列数组的列序）+ 每度量字段位置（last/top 热
        // 路径零查找）。`None` 子集 = 全列（列序在提取时确定, 见文档注释）。
        let row_field_names = row_fields.as_ref().map(|s| {
            let mut names: Vec<String> = s.iter().cloned().collect();
            names.sort(); // 确定性列序（对拍契约: 同 plan 恒同序）
            std::sync::Arc::new(names)
        });
        let measure_field_idx = plan
            .measures
            .iter()
            .map(|m| match (&m.field, &row_field_names) {
                (Some(fr), Some(names)) => names.iter().position(|n| n == field_name(fr)),
                _ => None,
            })
            .collect();
        // 空键规则预建 Empty 桶由 `StatsWindowState::new` 处理（快路径; 带 key
        // 惰性建桶）。
        let window = StatsWindowState::new(EngineHashMap::default(), &plan);
        let soa_layout = window.soa_layout.clone();
        Self {
            plan,
            window,
            watermark_nanos: 0,
            unique_wheres,
            measure_where,
            row_field_names,
            measure_field_idx,
            row_field_layout: None,
            soa_layout,
        }
    }

    /// 处理一批行（row-based; 列式段为 P1.5）。
    ///
    /// `extract(row, name) -> Option<Value>`: 由调用方提供行字段读取。
    ///
    /// where 过滤**内建求值**: 每行构建 1 次 ctx Event, 对去重后的唯一 where
    /// 表达式求值一次（结果共享给所有同条件度量）, 不再依赖调用方注入。
    /// 三值语义与 CEP `where_ok` 一致（eval 非 `Bool(true)` 即过滤）。
    ///
    /// 桶键（P2）: 按 `plan.keys` 分桶归并; 键缺失/null → 行跳过（对齐 CEP
    /// key 缺失不匹配语义）。空键规则恒单桶。
    pub fn process_rows<F>(&mut self, rows: &[HashMap<String, Value>], extract: F)
    where
        F: Fn(&HashMap<String, Value>, &str) -> Option<Value>,
    {
        // where 结果缓存: 行间复用 buffer（无逐行分配）; 无 where 规则时保持空。
        let mut where_ok: Vec<bool> = Vec::with_capacity(self.unique_wheres.len());
        // SoA 布局（纯数值计划; 行循环内 `self.window` 被 &mut 借用时仍可读）。
        let soa_layout = self.soa_layout.as_ref();
        let has_row_measures = self
            .plan
            .measures
            .iter()
            .any(|m| matches!(m.agg, StatsAggPlan::Last | StatsAggPlan::Top));
        for row in rows {
            where_ok.clear();
            if !self.unique_wheres.is_empty() {
                let ctx = Event {
                    fields: row
                        .iter()
                        .map(|(k, v)| (k.as_str().into(), v.clone()))
                        .collect(),
                };
                for expr in &self.unique_wheres {
                    where_ok.push(matches!(
                        super::eval::eval_bool_expr(expr, &ctx),
                        Some(true)
                    ));
                }
            }
            // 桶键: 缺失/null → 行跳过
            let Some(bucket_key) = eval_row_key(&self.plan.keys, row) else {
                continue;
            };
            // 行字段列名（P5）: 子集 → 直接用; None → 本行键排序（仅测试/缺省,
            // 生产恒有子集）。last/top 度量才需计算。
            let row_names: Option<Box<[String]>> = if has_row_measures {
                match &self.row_field_names {
                    Some(ns) => Some(ns.as_slice().into()),
                    None => {
                        let mut keys: Vec<String> = row.keys().cloned().collect();
                        keys.sort();
                        Some(keys.into_boxed_slice())
                    }
                }
            } else {
                None
            };
            // 行字段列数组懒提取（每行一次, 多 last/top 度量共享同一 Arc——与
            // accumulate_keyed_row 的 row_cache 对齐）。
            let mut row_cache: Option<std::sync::Arc<RowFields>> = None;
            // 2026-08-26 q18/q19：行式路径 layout（列式已建 → 复用；否则全 Other）。
            let row_layout = self.row_fields_layout_for_row(row_names.as_deref());
            // 新桶超限（内存 guard）→ 该行跳过（与列式路径一致）。
            let Some(bucket) = self.window.bucket_mut(&bucket_key, &self.plan) else {
                continue;
            };
            // SoA 桶（纯数值计划）: 数组直写（无枚举/Box——行式路径同构）。
            if let StatsBucketAccs::Numeric(soa) = bucket {
                let layout = soa_layout.unwrap();
                for (idx, measure) in self.plan.measures.iter().enumerate() {
                    if let Some(wi) = self.measure_where[idx]
                        && !where_ok[wi]
                    {
                        continue;
                    }
                    soa.counts[idx] += 1;
                    if let Some(field) = &measure.field
                        && let Some(val) = extract(row, field_name(field))
                        && let Some(n) = value_to_i128(&val)
                    {
                        match measure.agg {
                            StatsAggPlan::Sum | StatsAggPlan::Avg => {
                                soa.sums[layout.sum_slot[idx].unwrap() as usize] += n;
                            }
                            StatsAggPlan::Min => {
                                let s = layout.min_slot[idx].unwrap() as usize;
                                let cur = &mut soa.mins[s];
                                *cur = Some(match *cur {
                                    Some(m) if m <= n => m,
                                    _ => n,
                                });
                            }
                            StatsAggPlan::Max => {
                                let s = layout.max_slot[idx].unwrap() as usize;
                                let cur = &mut soa.maxs[s];
                                *cur = Some(match *cur {
                                    Some(m) if m >= n => m,
                                    _ => n,
                                });
                            }
                            StatsAggPlan::Count => {}
                            _ => unreachable!("SoA 桶仅数值度量"),
                        }
                    }
                }
            } else if let StatsBucketAccs::Classic(accs) = bucket {
                for (idx, measure) in self.plan.measures.iter().enumerate() {
                    if let Some(wi) = self.measure_where[idx]
                        && !where_ok[wi]
                    {
                        continue;
                    }
                    let acc = &mut accs[idx];
                    // count 仅 Numeric 度量维护（avg 的 count/sum 同步——D6）;
                    // distinct/last/top 变体无 count（输出不读, 原字段为死状态）。
                    match measure.agg {
                        StatsAggPlan::Count
                        | StatsAggPlan::Sum
                        | StatsAggPlan::Avg
                        | StatsAggPlan::Min
                        | StatsAggPlan::Max => {
                            let nacc = acc.numeric_mut();
                            nacc.count += 1;
                            if let Some(field) = &measure.field
                                && let Some(val) = extract(row, field_name(field))
                            {
                                match measure.agg {
                                    StatsAggPlan::Count => {}
                                    StatsAggPlan::Sum | StatsAggPlan::Avg => {
                                        if let Some(n) = value_to_i128(&val) {
                                            nacc.sum += n;
                                        }
                                    }
                                    StatsAggPlan::Min => {
                                        if let Some(n) = value_to_i128(&val) {
                                            nacc.min = Some(match nacc.min {
                                                Some(m) if m <= n => m,
                                                _ => n,
                                            });
                                        }
                                    }
                                    StatsAggPlan::Max => {
                                        if let Some(n) = value_to_i128(&val) {
                                            nacc.max = Some(match nacc.max {
                                                Some(m) if m >= n => m,
                                                _ => n,
                                            });
                                        }
                                    }
                                    _ => unreachable!("Numeric 分派内仅数值度量"),
                                }
                            }
                        }
                        StatsAggPlan::DistinctCount => {
                            if let Some(field) = &measure.field
                                && let Some(val) = extract(row, field_name(field))
                            {
                                let key = value_to_distinct_key(&val);
                                acc.distinct_mut().insert(key);
                            }
                        }
                        StatsAggPlan::Last | StatsAggPlan::Top => {
                            if let Some(field) = &measure.field {
                                if let Some(val) = extract(row, field_name(field)) {
                                    // 快速淘汰预检（在构建行字段前）: top 已满且 key 进不了
                                    // 前 N → 跳过, 免每行 row_fields 提取 + Arc 分配。
                                    // 与列式路径同一口径（value_to_f64 同义）。
                                    if measure.agg == StatsAggPlan::Top {
                                        let n = measure.arg.unwrap_or(10) as usize;
                                        if n == 0 {
                                            continue; // top(0): 不保留任何条目
                                        }
                                        if let Some(key) = value_to_f64(&val)
                                            && let entries = acc.top()
                                            && entries.len() == n
                                            && key <= entries[n - 1].key
                                        {
                                            continue;
                                        }
                                    }
                                    // 行式路径: 按 row_names 列序提取（与列式
                                    // row_fields_from_batch 对齐; 同桶多 last 度量 Arc
                                    // 共享 1 份内存）。
                                    let row = row_cache.get_or_insert_with(|| {
                                        row_fields_from_row(row, row_names.as_deref(), &row_layout)
                                    });
                                    let fidx = measure_field_position(
                                        &self.plan,
                                        &self.measure_field_idx,
                                        idx,
                                        row_names.as_deref(),
                                    );
                                    apply_last_top(acc, measure, row, fidx);
                                } else if measure.agg == StatsAggPlan::Last {
                                    // 字段缺失: last 仍保留整行（yield 读其它字段）
                                    let row = row_cache.get_or_insert_with(|| {
                                        row_fields_from_row(row, row_names.as_deref(), &row_layout)
                                    });
                                    *acc.last_mut() = Some(std::sync::Arc::clone(row));
                                }
                            }
                        }
                    }
                }
            }
            self.window.event_count += 1;
        }
        // 2026-08-26 q16：批末刷新估算（distinct 集合计入真实 len）。
        self.window.refresh_estimated_bytes(&self.plan);
    }

    /// last/top 行字段列名（列数组列序; `None` = 无子集且未定——任务层仅在
    /// 生产子集路径使用）。
    pub fn row_field_names(&self) -> Option<&std::sync::Arc<Vec<String>>> {
        self.row_field_names.as_ref()
    }

    /// 计算最终度量值（空键兼容: 取单桶; 带 key 用 by_bucket 版本）。
    pub fn final_measure_values(&self) -> Vec<f64> {
        self.final_measure_values_by_bucket()
            .into_iter()
            .next()
            .map(|(_, values)| values)
            .unwrap_or_else(|| vec![0.0; self.plan.measures.len()])
    }

    /// 按桶的最终度量值（桶序 = ScopeKey 升序, 确定性输出对拍契约; avg 由
    /// sum/count 求得, D6）。标量访问器——last 取字段数值, top 为多值不适用
    /// （用 [`Self::close_window_by_bucket_rows`]）。
    pub fn final_measure_values_by_bucket(&self) -> Vec<(ScopeKey, Vec<f64>)> {
        let mut buckets: Vec<(ScopeKey, Vec<f64>)> = self
            .window
            .buckets
            .values()
            .flat_map(|chain| chain.iter())
            .map(|b| {
                (
                    b.scope_key.clone(),
                    bucket_measure_values(
                        &self.plan,
                        &b.accs,
                        self.window.soa_layout.as_ref(),
                        &self.measure_field_idx,
                    ),
                )
            })
            .collect();
        buckets.sort_by(|a, b| a.0.cmp(&b.0));
        buckets
    }

    /// 按桶 close 输出（rich 版, last/top 用; 标量计划同样适用）: 每桶每度量一个
    /// 值列表——标量恒 1 条目, `top` 产生 N 条目（rank 序, key DESC）。
    /// 条目携带行字段（last/top 的整行）, 供 yield 经 field_values 读 `b.*`。
    /// 桶序 = ScopeKey 升序; 同时清空窗口状态。
    pub fn close_window_by_bucket_rows(&mut self) -> Vec<StatsCloseBucket> {
        let buckets = self.window.take_buckets();
        let out = self.close_buckets_to_rows(buckets);
        self.reset_window();
        out
    }

    /// 一批桶 → StatsCloseBucket（流式 close 用，2026-08-26 q18 100M）: 从
    /// [`Self::take_buckets_up_to`] 分批取桶后转换, 避免一次性全量
    /// `StatsCloseBucket`（2935 万桶 ~5.9G）与状态数据同时驻留的峰值。
    /// 批内桶序 = 传入序（调用方 `take_buckets_up_to` 已按 ScopeKey 升序）。
    pub fn close_buckets_to_rows(
        &self,
        buckets: Vec<(ScopeKey, StatsBucketAccs)>,
    ) -> Vec<StatsCloseBucket> {
        buckets
            .into_iter()
            .map(|(key, accs)| StatsCloseBucket {
                key,
                measures: bucket_close_entries(
                    &self.plan,
                    &accs,
                    self.window.soa_layout.as_ref(),
                    &self.measure_field_idx,
                ),
            })
            .collect()
    }

    /// 流式 close 收尾: 清窗（保留限额配置 + 拒收计数跨窗口）。
    /// [`Self::take_buckets_up_to`] 全部取完（返回空）后调用。
    pub fn finish_close_window(&mut self) {
        self.reset_window();
    }

    /// 一批桶 → 度量值（流式 close 标量路径, 2026-08-26 同 rich 流式）; 批内
    /// 桶序 = 传入序（调用方 `take_buckets_up_to` 已按 ScopeKey 升序）。
    pub fn close_bucket_values(
        &self,
        buckets: Vec<(ScopeKey, StatsBucketAccs)>,
    ) -> Vec<(ScopeKey, Vec<f64>)> {
        buckets
            .into_iter()
            .map(|(key, accs)| {
                (
                    key,
                    bucket_measure_values(
                        &self.plan,
                        &accs,
                        self.window.soa_layout.as_ref(),
                        &self.measure_field_idx,
                    ),
                )
            })
            .collect()
    }

    /// 分批取桶（流式 close）: 从桶表取最多 n 个链并移除（链内桶拍平）, 批内
    /// ScopeKey 升序（保持单批对拍契约）; 全部取完（返回空）后调用方须
    /// [`Self::finish_close_window`]。不 reset（还有剩余桶, 下一批继续）。
    ///
    /// 2026-08-26 review: 用 `retain` 原地移除已取链——v1 用 `mem::take` 全表 +
    /// 剩余重插新 HashMap（每批 O(剩余) 哈希 + 分配, 100M 30 批 ≈ 4.4 亿次重插
    /// close +~9s）; retain 每批 O(n) 轻量回调（无哈希重建, close ~3s）。
    pub fn take_buckets_up_to(&mut self, n: usize) -> Vec<(ScopeKey, StatsBucketAccs)> {
        let mut out = Vec::new();
        if n == 0 {
            return out;
        }
        self.window.buckets.retain(|_hash, chain| {
            if out.len() >= n {
                return true; // 已取够: 保留剩余
            }
            out.extend(
                std::mem::take(chain)
                    .into_iter()
                    .map(|b| (b.scope_key, b.accs)),
            );
            false // 本链已取空: 删除
        });
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// 窗口 close: 冻结当前值（空键单桶）, 清空状态。
    pub fn close_window(&mut self) -> Vec<f64> {
        let values = self.final_measure_values();
        self.reset_window();
        values
    }

    /// 窗口 close（按桶）: 每桶一行 `(key, values)`, 清空状态。
    pub fn close_window_by_bucket(&mut self) -> Vec<(ScopeKey, Vec<f64>)> {
        let out = self.final_measure_values_by_bucket();
        self.reset_window();
        out
    }

    fn reset_window(&mut self) {
        let limit = self.window.limit_bytes;
        let rule_name = self.window.rule_name.clone();
        let over_limit = self.window.over_limit_new_buckets;
        // 空键 Empty 桶由 new 预建（keys 空时）。
        self.window = StatsWindowState::new(EngineHashMap::default(), &self.plan);
        // 保留限额配置 + 拒收计数跨窗口（guard 持续生效; 计数供指标/告警）。
        self.window
            .set_memory_limit(&rule_name, limit.map(|b| b as usize));
        self.window.over_limit_new_buckets = over_limit;
    }

    /// 注入状态内存上限（字节; None = 不设防）——超限拒收新键桶, 已有桶继续。
    pub fn set_memory_limit(&mut self, rule_name: &str, bytes: Option<usize>) {
        self.window.set_memory_limit(rule_name, bytes);
    }

    /// 提取本片已关闭窗口的**原始累加状态**（输入分区分片归并用）并重置窗口。
    /// 返回 `(桶原始状态, 本片事件数)`——协调片把它合并进自己的窗口后再 close。
    /// 仅空键/可交换度量（count/sum/min/max/distinct）分片使用（last/top 被
    /// spawn 门控排除——行序敏感不可归并）。
    pub fn take_partial(&mut self) -> (Vec<(ScopeKey, StatsBucketAccs)>, u64) {
        let buckets = self.window.take_buckets();
        let count = self.window.event_count;
        self.reset_window();
        (buckets, count)
    }

    /// 归并另一片的原始累加状态（输入分区分片）: 计数相加、sum 相加、min/max
    /// 取极值、distinct 集 union（`extend` 用自身 hasher 重插, 跨片 hasher 可
    /// 不同）、事件数相加。last/top 不在此路径（spawn 门控排除）。
    ///
    /// **串行（2026-08-24 实测）**: `thread::scope` 并行 union 在协调片 async
    /// close 里阻塞 tokio worker, 与其余片 ingest 争核 → EPS 5.96~7.86M 波动
    /// （比串行 7.86M 更差）。q15 EOS 归并 ~883ms 是固定尾部成本; 若要并行须
    /// 走 `spawn_blocking`/异步任务（数据移出 `&mut self`, 未做）。
    pub fn merge_partial(&mut self, buckets: Vec<(ScopeKey, StatsBucketAccs)>, event_count: u64) {
        for (key, accs) in buckets {
            // 超限（guard）→ 该片该键跳过（协调片侧同样受桶预算约束）。
            let Some(target) = self.window.bucket_mut(&key, &self.plan) else {
                continue;
            };
            merge_bucket_accs(target, accs);
        }
        self.window.event_count += event_count;
        // 2026-08-26 q16：分片归并后刷新估算（distinct union 可能大幅增长）。
        self.window.refresh_estimated_bytes(&self.plan);
    }

    /// 列式批处理（P1.5, 设计 §6.2）: 消费 fanout 投递的 raw [`RecordBatch`]。
    ///
    /// - 段 1: where 列式 mask（去重后的唯一条件, 每批一次 [`eval_guard_columnar`]）
    /// - 段 1d: count/sum/min/max **整列归并**（无逐行循环; avg 输出时 sum/count 求得）
    /// - 段 2: distinct 行式段——按 mask 的 true 行读**原生列值**插入（每行 1 次
    ///   哈希不可回避; 批内预去重为后续优化）
    ///
    /// 返回 `false` 表示本计划/批不满足列式前置（where 不可列式化, 或 distinct
    /// 字段列类型不在支持集）——调用方**必须回退** [`Self::process_rows`] 保证语义。
    pub fn process_batch(&mut self, batch: &RecordBatch) -> bool {
        self.process_batch_rows(batch, None)
    }

    /// 列式批处理（P1.5, 设计 §6.2）+ 行子集（P2 分片: `rows` = 本片拥有的
    /// 行索引; `None` = 全批）。归并只对行域内的行生效; where mask 仍整批计算。
    pub fn process_batch_rows(&mut self, batch: &RecordBatch, rows: Option<&[u32]>) -> bool {
        self.process_batch_rows_impl(batch, rows, None)
    }

    /// 分片缓存版（2026-08-27 q17）: where mask 经 [`StatsMaskCache`] 分片共享——
    /// 同一批被 S 片各自整批 eval 的重复（S×）消除为首片 1×（其余片 Arc 命中）。
    /// 语义与 [`Self::process_batch_rows`] 完全一致（mask 为批的纯函数）。
    pub fn process_batch_rows_cached(
        &mut self,
        batch: &RecordBatch,
        rows: Option<&[u32]>,
        cache: &StatsMaskCache,
    ) -> bool {
        self.process_batch_rows_impl(batch, rows, Some(cache))
    }

    fn process_batch_rows_impl(
        &mut self,
        batch: &RecordBatch,
        rows: Option<&[u32]>,
        mask_cache: Option<&StatsMaskCache>,
    ) -> bool {
        // 前置（**必须在任何累加副作用之前**——返回 false 时调用方回退
        // process_rows, 部分应用会把已累加的计数再算一遍）:
        // 1. 全部 where 表达式可列式化（eval_guard_columnar 对不可列式表达式
        //    返回全 false, 不可静默使用）
        // 2. distinct 字段列类型在支持集（段 2 失败同样造成部分应用）
        // 3. 桶键可列式化（全部为简单字段; 含 bucket/tier 等函数键 → 回退行式）
        for e in &self.unique_wheres {
            if !wf_lang::columnar::expr_is_columnar(e) {
                return false;
            }
        }
        if !distinct_fields_columnar_safe(batch, &self.plan) {
            return false;
        }
        let key_cols: Option<Vec<usize>> = self
            .plan
            .keys
            .iter()
            .map(|k| match k {
                Expr::Field(fr) => batch.schema().index_of(field_ref_name(fr)).ok(),
                _ => None,
            })
            .collect();
        let Some(key_cols) = key_cols else {
            return false;
        };
        let n = batch.num_rows();
        // 行域（P2 分片）: `rows` = 本片拥有的行索引子集（绝对行号, 升序）。
        // 归并段（段 1d/段 2）改为**行域驱动**（count_domain/sum_domain/
        // insert_distinct_domain 只遍历本片行）——不再构建全批 domain mask。
        let view = ColumnarBatch::from_all_fields(batch);
        // 段 1: where 列式 mask（去重后唯一条件, 每批一次; 分片缓存版共享首片结果）。
        let masks: Vec<BooleanArray> = match mask_cache {
            Some(cache) => (*cache.get_or_compute(batch, || {
                let view = ColumnarBatch::from_all_fields(batch);
                self.unique_wheres
                    .iter()
                    .map(|e| eval_guard_columnar(e, &view))
                    .collect()
            }))
            .clone(), // Vec<BooleanArray> clone = 列数组 Arc 浅拷贝
            None => self
                .unique_wheres
                .iter()
                .map(|e| eval_guard_columnar(e, &view))
                .collect(),
        };
        // 行字段列名（P5）: 子集 → 直接用; None → 排序的 schema 字段名（与行式
        // None 同序, 任务层注入同序）。仅 last/top 计划需要。
        let has_row_measures = self
            .plan
            .measures
            .iter()
            .any(|m| matches!(m.agg, StatsAggPlan::Last | StatsAggPlan::Top));
        let row_names: Option<Box<[String]>> = if has_row_measures {
            match &self.row_field_names {
                Some(ns) => Some(ns.as_slice().into()),
                None => {
                    let mut ns: Vec<String> = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().to_string())
                        .collect();
                    ns.sort();
                    Some(ns.into_boxed_slice())
                }
            }
        } else {
            None
        };
        // 行字段列索引（P5+ 优化: 每批预解析一次, 免逐行 schema.index_of）
        let row_field_cols: Option<Box<[Option<usize>]>> = row_names.as_deref().map(|ns| {
            ns.iter()
                .map(|n| batch.schema().index_of(n).ok())
                .collect::<Vec<_>>()
                .into_boxed_slice()
        });
        // 度量字段列索引（2026-08-27 q17 修复）: 每批预解析一次, 免逐行逐度量
        // `schema().index_of` 线性扫描——q17 4 个数值度量 min/max/avg/sum 全命中
        // 旧缺陷（column_i128 每行 index_of + downcast）。count 无字段 → None。
        let measure_field_cols: Vec<Option<usize>> = self
            .plan
            .measures
            .iter()
            .map(|m| {
                m.field
                    .as_ref()
                    .and_then(|f| batch.schema().index_of(field_name(f)).ok())
            })
            .collect();
        // 带 key（P2）: 逐行按桶归并（mask 列式 + 桶键列式, 无解释器 eval）。
        // 空键保持整列归并快路径（P1.5）。
        if !self.plan.keys.is_empty() {
            // 键列类型每批预解析一次（免逐行 downcast_ref 分派）
            let key_columns = resolve_key_columns(batch, &key_cols);
            return self.process_batch_keyed(
                batch,
                &masks,
                &key_cols,
                &key_columns,
                n,
                rows,
                row_names.as_deref(),
                row_field_cols.as_deref(),
                &measure_field_cols,
            );
        }
        // 段 1d: 纯归并度量整列累加（**行域驱动**——2026-08-24 分片裁剪: 只遍历
        // 本片行, 消除每片对全批的 O(n) 冗余扫描; 全批路径 `rows=None` 行为不变）。
        // 行式语义: 满足 where 的行对**每个**度量都 `count += 1`（在字段读取前）
        // ——avg 的 count 必须与 sum 同步累加, 否则 avg = sum/count 输出 0（D6）。
        let soa_layout = self.soa_layout.as_ref();
        // 空键规则恒单桶（预建, 不参与限额——guard 只针对键空间膨胀）。
        let bucket = self
            .window
            .bucket_mut(&ScopeKey::Empty, &self.plan)
            .expect("Empty 桶恒存在");
        if let StatsBucketAccs::Numeric(soa) = bucket {
            let layout = soa_layout.unwrap();
            for (idx, measure) in self.plan.measures.iter().enumerate() {
                let wi = self.measure_where[idx];
                let rows_in = count_domain(rows, n, &masks, wi);
                match measure.agg {
                    StatsAggPlan::Count => {
                        soa.counts[idx] += rows_in;
                    }
                    StatsAggPlan::Sum | StatsAggPlan::Avg => {
                        soa.counts[idx] += rows_in;
                        if let Some(field) = &measure.field
                            && let Some(col) = numeric_col(batch, field_name(field))
                        {
                            let s = layout.sum_slot[idx].unwrap() as usize;
                            soa.sums[s] += sum_domain(&col, rows, n, &masks, wi);
                        }
                    }
                    StatsAggPlan::Min => {
                        soa.counts[idx] += rows_in;
                        if let Some(field) = &measure.field
                            && let Some(col) = numeric_col(batch, field_name(field))
                        {
                            let s = layout.min_slot[idx].unwrap() as usize;
                            minmax_domain_one(&col, rows, n, &masks, wi, true, &mut soa.mins[s]);
                        }
                    }
                    StatsAggPlan::Max => {
                        soa.counts[idx] += rows_in;
                        if let Some(field) = &measure.field
                            && let Some(col) = numeric_col(batch, field_name(field))
                        {
                            let s = layout.max_slot[idx].unwrap() as usize;
                            minmax_domain_one(&col, rows, n, &masks, wi, false, &mut soa.maxs[s]);
                        }
                    }
                    _ => unreachable!("SoA 桶仅数值度量"),
                }
            }
        } else if let StatsBucketAccs::Classic(accs) = bucket {
            for (idx, measure) in self.plan.measures.iter().enumerate() {
                let wi = self.measure_where[idx];
                let acc = &mut accs[idx];
                let rows_in = count_domain(rows, n, &masks, wi);
                match measure.agg {
                    StatsAggPlan::Count => {
                        acc.numeric_mut().count += rows_in;
                    }
                    StatsAggPlan::Sum | StatsAggPlan::Avg => {
                        let nacc = acc.numeric_mut();
                        nacc.count += rows_in;
                        if let Some(field) = &measure.field
                            && let Some(col) = numeric_col(batch, field_name(field))
                        {
                            nacc.sum += sum_domain(&col, rows, n, &masks, wi);
                        }
                    }
                    StatsAggPlan::Min | StatsAggPlan::Max => {
                        let nacc = acc.numeric_mut();
                        nacc.count += rows_in;
                        if let Some(field) = &measure.field
                            && let Some(col) = numeric_col(batch, field_name(field))
                        {
                            minmax_domain(
                                &col,
                                rows,
                                n,
                                &masks,
                                wi,
                                &mut nacc.min,
                                &mut nacc.max,
                            );
                        }
                    }
                    StatsAggPlan::DistinctCount => {
                        // 输出只用 distinct_set（无 count 字段——原 count 维护为死状态）
                    }
                    StatsAggPlan::Last | StatsAggPlan::Top => {
                        // P1 不实现（Q18/Q19 扩展）
                    }
                }
            }
        }
        // 段 2: distinct/last/top 行式段（原生列值按行域 + where 过滤; last/top 提取
        // 行字段供 yield 注入）
        // 2026-08-26 q18/q19：行字段 layout 在桶借用前 ensure（ensure 需 &mut self）。
        let row_layout = self.ensure_row_field_layout(batch);
        for (idx, measure) in self.plan.measures.iter().enumerate() {
            if !matches!(
                measure.agg,
                StatsAggPlan::DistinctCount | StatsAggPlan::Last | StatsAggPlan::Top
            ) {
                continue;
            }
            let wi = self.measure_where[idx];
            // distinct/last/top 计划恒 Classic（soa_layout 仅纯数值计划）——
            // 解包取数组（形态不符 = 内部错误）。
            let accs = match self
                .window
                .bucket_mut(&ScopeKey::Empty, &self.plan)
                .expect("Empty 桶恒存在")
            {
                StatsBucketAccs::Classic(accs) => accs,
                StatsBucketAccs::Numeric(_) => {
                    unreachable!("distinct/last/top 计划不走 SoA 桶")
                }
            };
            let acc = &mut accs[idx];
            if matches!(measure.agg, StatsAggPlan::DistinctCount) {
                let Some(field) = &measure.field else {
                    continue;
                };
                let set = acc.distinct_mut();
                if !insert_distinct_domain(batch, field_name(field), rows, n, &masks, wi, set) {
                    return false;
                }
            } else {
                // last/top: 逐行按行域 + where 更新（子集行字段提取; 空键 last 规则少用）
                let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
                for r in domain_rows(rows, n).filter(|&r| passes(r)) {
                    let row =
                        row_fields_from_batch(batch, r, row_field_cols.as_deref(), &row_layout);
                    let fidx = measure_field_position(
                        &self.plan,
                        &self.measure_field_idx,
                        idx,
                        row_names.as_deref(),
                    );
                    apply_last_top(acc, measure, &row, fidx);
                }
            }
        }
        self.window.event_count += rows.map_or(n as u64, |rs| rs.len() as u64);
        // 2026-08-26 q16：批末刷新估算（distinct 集合计入真实 len）。
        self.window.refresh_estimated_bytes(&self.plan);
        true
    }

    /// 带 key 的批处理: 逐行按桶归并（P2）。桶键列式（`scope_key_columnar`）,
    /// where 列式 mask, 归并行式（每桶独立累积）。键 null → 行跳过（对齐 CEP
    /// key 缺失语义; 与 fanout 的 missing-key → shard 0 分片口径一致——分片只
    /// 影响投递归属, 桶归并仍按完整键）。
    ///
    /// `rows`（P2 分片）= 本片行子集: **只归并行域内的行**——否则每片处理全批,
    /// 每个键被 N 片各算一遍, close 重复输出 N 倍（Q16 实测 EMIT 10 倍）。
    #[allow(clippy::too_many_arguments)] // 列式批处理签名: 键列/掩码/行域/行字段提取列 4 组参数
    fn process_batch_keyed(
        &mut self,
        batch: &RecordBatch,
        masks: &[BooleanArray],
        key_cols: &[usize],
        key_columns: &[KeyColumn<'_>],
        n: usize,
        rows: Option<&[u32]>,
        row_names: Option<&[String]>,
        row_field_cols: Option<&[Option<usize>]>,
        measure_field_cols: &[Option<usize>],
    ) -> bool {
        // 局部寄存器计数（F1 修复的 event_count 口径 + 零热路径开销）: 归并成功
        // 才计, 批末一次性写回——避免每行一次 `self.window.event_count += 1` 的
        // 内存往返（q19 列式实测 +2.3%）。
        let mut counted: u64 = 0;
        match rows {
            Some(rs) => {
                for &r in rs {
                    if (r as usize) >= n {
                        continue; // 防御: 越界行号（与 materialize_rows 一致跳过）
                    }
                    if self.accumulate_keyed_row(
                        batch,
                        masks,
                        key_cols,
                        key_columns,
                        r as usize,
                        row_names,
                        row_field_cols,
                        measure_field_cols,
                    ) {
                        counted += 1;
                    }
                }
            }
            None => {
                for row in 0..n {
                    if self.accumulate_keyed_row(
                        batch,
                        masks,
                        key_cols,
                        key_columns,
                        row,
                        row_names,
                        row_field_cols,
                        measure_field_cols,
                    ) {
                        counted += 1;
                    }
                }
            }
        }
        self.window.event_count += counted;
        true
    }

    /// 单行桶归并（P2 复合键逐行路径的公共主体, 供全批/行域两分支复用）。
    /// 返回 `true` = 归并成功（行计入 event_count）; `false` = 键 null/超限拒收
    /// （行跳过, 不计入——F1 行式/列式对拍口径）。
    ///
    /// 字段读取走**原生列值**（`column_i128_at`/`column_distinct_key_at`, 列索引
    /// 批级预解析——与空键列式段同精度（D7/D8: ≥2^53 的 Int64 不得经
    /// `Value::Number(f64)` 舍入; Timestamp 列 distinct 也走原生 i64, 不得静默跳过）。
    ///
    /// **复合键优化（P5+）**: 键数 ≤ 4 走扁平键路径——栈上叶数组（`key_column_comp`
    /// 预解析列, 无 Box 分配/无逐行 downcast）→ `comps_hash` → `keyed_bucket_mut`
    /// （链扫描, `ScopeKey` 仅每桶首见构建一次）; 键数 > 4 回退完整键
    /// `scope_key_columnar`（罕见）。
    #[allow(clippy::too_many_arguments)] // 与 process_batch_keyed 同签名族
    fn accumulate_keyed_row(
        &mut self,
        batch: &RecordBatch,
        masks: &[BooleanArray],
        key_cols: &[usize],
        key_columns: &[KeyColumn<'_>],
        row: usize,
        row_names: Option<&[String]>,
        row_field_cols: Option<&[Option<usize>]>,
        measure_field_cols: &[Option<usize>],
    ) -> bool {
        const MAX_STACK_KEYS: usize = 4;
        // 2026-08-26 q18/q19：行字段 layout（首次从 schema 构建并缓存）；
        // 在桶借用前取（ensure 需 &mut self）。soa_layout 同源取——窗口被 &mut
        // 借用时仍可读（字段级拆分借用）。
        let row_layout = self.ensure_row_field_layout(batch);
        let soa_layout = self.soa_layout.as_ref();
        if key_columns.len() <= MAX_STACK_KEYS {
            let mut comps: [ScopeKey; MAX_STACK_KEYS] = std::array::from_fn(|_| ScopeKey::Empty);
            for (i, kc) in key_columns.iter().enumerate() {
                let Some(c) = key_column_comp(kc, batch, row) else {
                    return false; // 键 null → 跳过
                };
                comps[i] = c;
            }
            let comps = &comps[..key_columns.len()];
            let hash = comps_hash(comps);
            // 新桶超限（内存 guard）→ 该行跳过。
            let Some(bucket) = self.window.keyed_bucket_mut(hash, comps, &self.plan) else {
                return false;
            };
            accumulate_bucket_row(
                bucket,
                soa_layout,
                &self.plan,
                &self.measure_where,
                &self.measure_field_idx,
                row_names,
                row_field_cols,
                batch,
                masks,
                row,
                &row_layout,
                measure_field_cols,
            );
            return true;
        }
        let Some(key) = scope_key_columnar(batch, key_cols, row) else {
            return false; // 键 null → 跳过
        };
        // 新桶超限（内存 guard）→ 该行跳过。
        let Some(bucket) = self.window.bucket_mut(&key, &self.plan) else {
            return false;
        };
        accumulate_bucket_row(
            bucket,
            soa_layout,
            &self.plan,
            &self.measure_where,
            &self.measure_field_idx,
            row_names,
            row_field_cols,
            batch,
            masks,
            row,
            &row_layout,
            measure_field_cols,
        );
        true
    }
}

/// 单行桶累加入口（按桶形态分派一次）: SoA → [`accumulate_soa`]; Classic →
/// [`accumulate_column_row`]（枚举/Box 循环）。分派每行一次（非每度量）——
/// SoA 计划的热路径进入后无枚举分派无 Box 解引用。
#[allow(clippy::too_many_arguments)] // 与 accumulate_column_row 同签名族 + soa_layout
fn accumulate_bucket_row(
    bucket: &mut StatsBucketAccs,
    soa_layout: Option<&NumericSoALayout>,
    plan: &StatsPlan,
    measure_where: &[Option<usize>],
    measure_field_idx: &[Option<usize>],
    row_names: Option<&[String]>,
    row_field_cols: Option<&[Option<usize>]>,
    batch: &RecordBatch,
    masks: &[BooleanArray],
    row: usize,
    row_layout: &std::sync::Arc<RowFieldLayout>,
    measure_field_cols: &[Option<usize>],
) {
    match bucket {
        StatsBucketAccs::Numeric(soa) => accumulate_soa(
            soa,
            soa_layout.unwrap(),
            measure_where,
            measure_field_cols,
            batch,
            masks,
            row,
        ),
        StatsBucketAccs::Classic(accs) => accumulate_column_row(
            accs,
            plan,
            measure_where,
            measure_field_idx,
            row_names,
            row_field_cols,
            batch,
            masks,
            row,
            row_layout,
            measure_field_cols,
        ),
    }
}

/// 单行 SoA 桶累加（纯数值计划; 2026-08-27 q17）: 段 1 counts 全度量（含 where
/// 过滤）; 段 2 按字段分组数值更新——同字段度量共享 1 次 [`column_i128_at`]
/// （旧路径 sum/avg/min/max 各自读取同一列, 每行 4 次重复）。免: 每度量枚举
/// 分派 + `Box` 解引用 + 同列重复列读取。pub(crate) 供 SoA 对照 bench。
pub(crate) fn accumulate_soa(
    soa: &mut NumericSoA,
    layout: &NumericSoALayout,
    measure_where: &[Option<usize>],
    measure_field_cols: &[Option<usize>],
    batch: &RecordBatch,
    masks: &[BooleanArray],
    row: usize,
) {
    for (idx, wi) in measure_where.iter().enumerate() {
        if let Some(wi) = wi
            && !masks[*wi].value(row)
        {
            continue;
        }
        soa.counts[idx] += 1;
    }
    for g in layout.groups.iter() {
        // 组内度量同字段 → 列索引相同; 取第一个度量的批级列索引读一次。
        let Some(ci) = measure_field_cols[g.entries[0].0] else {
            continue;
        };
        let Some(v) = column_i128_at(batch, ci, row) else {
            continue;
        };
        for &(idx, kind) in g.entries.iter() {
            match kind {
                NumericKind::Sum => {
                    soa.sums[layout.sum_slot[idx].unwrap() as usize] += v;
                }
                NumericKind::Min => {
                    let s = layout.min_slot[idx].unwrap() as usize;
                    let cur = &mut soa.mins[s];
                    *cur = Some(match *cur {
                        Some(m) if m <= v => m,
                        _ => v,
                    });
                }
                NumericKind::Max => {
                    let s = layout.max_slot[idx].unwrap() as usize;
                    let cur = &mut soa.maxs[s];
                    *cur = Some(match *cur {
                        Some(m) if m >= v => m,
                        _ => v,
                    });
                }
            }
        }
    }
}

/// 单行桶累加主体（列式路径; 自由函数——调用点持有 `&mut self.window` 桶借用,
/// 方法会整 self 借用冲突）。**Classic 路径**（含 distinct/last/top 的计划）;
/// 纯数值计划走 [`accumulate_soa`]。
///
/// last/top 行字段每行懒提取一次, 多度量共享同一 Arc（Q18 4 个 last 度量内存
/// 1 份; 提取列序 = row_names, 免整行 8 字段）。pub(crate) 供 SoA 对照 bench。
#[allow(clippy::too_many_arguments)] // 单行桶累加: 桶/计划/掩码索引/行字段/列/行号 6 组参数
pub(crate) fn accumulate_column_row(
    bucket: &mut [StatsAccum],
    plan: &StatsPlan,
    measure_where: &[Option<usize>],
    measure_field_idx: &[Option<usize>],
    row_names: Option<&[String]>,
    row_field_cols: Option<&[Option<usize>]>,
    batch: &RecordBatch,
    masks: &[BooleanArray],
    row: usize,
    row_layout: &std::sync::Arc<RowFieldLayout>,
    measure_field_cols: &[Option<usize>],
) {
    let mut row_cache: Option<std::sync::Arc<RowFields>> = None;
    for (idx, measure) in plan.measures.iter().enumerate() {
        if let Some(wi) = measure_where[idx]
            && !masks[wi].value(row)
        {
            continue;
        }
        let acc = &mut bucket[idx];
        // count 仅 Numeric 度量维护（distinct/last/top 变体无 count——原 count
        // 字段为死状态, 输出不读）。
        match measure.agg {
            StatsAggPlan::Count
            | StatsAggPlan::Sum
            | StatsAggPlan::Avg
            | StatsAggPlan::Min
            | StatsAggPlan::Max => {
                let nacc = acc.numeric_mut();
                nacc.count += 1;
                // 度量字段列索引（批级预解析, 免每行 schema.index_of——q17 修复）;
                // count 无字段 → None → 只计数。
                let Some(ci) = measure_field_cols[idx] else {
                    continue;
                };
                match measure.agg {
                    StatsAggPlan::Count => {}
                    StatsAggPlan::Sum | StatsAggPlan::Avg => {
                        if let Some(nn) = column_i128_at(batch, ci, row) {
                            nacc.sum += nn;
                        }
                    }
                    StatsAggPlan::Min => {
                        if let Some(nn) = column_i128_at(batch, ci, row) {
                            nacc.min = Some(match nacc.min {
                                Some(m) if m <= nn => m,
                                _ => nn,
                            });
                        }
                    }
                    StatsAggPlan::Max => {
                        if let Some(nn) = column_i128_at(batch, ci, row) {
                            nacc.max = Some(match nacc.max {
                                Some(m) if m >= nn => m,
                                _ => nn,
                            });
                        }
                    }
                    _ => unreachable!("Numeric 分派内仅数值度量"),
                }
            }
            StatsAggPlan::DistinctCount => {
                if let Some(ci) = measure_field_cols[idx]
                    && let Some(k) = column_distinct_key_at(batch, ci, row)
                {
                    acc.distinct_mut().insert(k);
                }
            }
            StatsAggPlan::Last | StatsAggPlan::Top => {
                // 快速淘汰预检（**在构建行字段前**）: top 已满且 key 进不了前 N
                // → 直接跳过, 免每行 row_fields 的 Arc 分配 + 字段提取。q19 每
                // auction 的 bid 绝大多数低于当前 top-10 门槛（bench ~99.8% 行
                // 被此预检挡下）。列索引经构造期预计算的 `measure_field_idx` 取
                // （零 index_of / 零 names 回退——无子集时 idx 恒 None → 不预检,
                // 仅测试/缺省路径, 性能不敏感）。列值口径与行字段提取后
                // `value_to_f64` 一致（Int64→as f64 / Float64 原值）。
                if measure.agg == StatsAggPlan::Top {
                    let n = measure.arg.unwrap_or(10) as usize;
                    if n == 0 {
                        continue; // top(0): 不保留任何条目, 无需行字段
                    }
                    if let Some(ci) = measure_field_idx[idx].and_then(|i| {
                        row_field_cols
                            .and_then(|cols| cols.get(i).copied())
                            .flatten()
                    }) && let Some(key) = column_f64_at(batch, ci, row)
                        && let entries = acc.top()
                        && entries.len() == n
                        && key <= entries[n - 1].key
                    {
                        continue;
                    }
                }
                let row = row_cache.get_or_insert_with(|| {
                    row_fields_from_batch(batch, row, row_field_cols, row_layout)
                });
                let fidx = measure_field_position(plan, measure_field_idx, idx, row_names);
                apply_last_top(acc, measure, row, fidx);
            }
        }
    }
}

fn field_name(fr: &FieldRef) -> &str {
    match fr {
        FieldRef::Simple(n) => n,
        FieldRef::Qualified(_, n) | FieldRef::Bracketed(_, n) => n,
        FieldRef::Path { segments, .. } => match segments.first() {
            Some(wf_lang::ast::PathSegment::Field(root)) => root,
            _ => "",
        },
        _ => "",
    }
}

/// 度量字段在行字段列数组中的位置: 子集模式走构造期预计算; 无子集按
/// `names` 名查（last/top 且字段在列内 → Some, 其余 None）。
/// 自由函数而非方法: 调用点同时持有 `&mut self.window` 的桶借用, 方法会整
/// self 借用冲突; 自由函数只借 `plan` + `measure_field_idx` 两字段。
fn measure_field_position(
    plan: &StatsPlan,
    measure_field_idx: &[Option<usize>],
    idx: usize,
    names: Option<&[String]>,
) -> Option<usize> {
    match measure_field_idx[idx] {
        Some(i) => Some(i),
        None => match (&plan.measures[idx].field, names) {
            (Some(f), Some(ns)) => ns.iter().position(|n| n == field_name(f)),
            _ => None,
        },
    }
}

// ---------------------------------------------------------------------------
// 归并（输入分区分片: 可交换度量）
// ---------------------------------------------------------------------------

/// 归并两个累加器（count 相加 / sum 相加 / min·max 取极值 / distinct 集 union）。
/// 仅可交换度量路径使用（last/top 被 spawn 门控排除——行序敏感不可归并）。
/// 变体不匹配 = plan/构造不一致的内部错误（panic 尽早暴露）。
fn merge_accum(t: &mut StatsAccum, o: &StatsAccum) {
    match (t, o) {
        (StatsAccum::Numeric(t), StatsAccum::Numeric(o)) => {
            t.count += o.count;
            t.sum += o.sum;
            t.min = match (t.min, o.min) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
            t.max = match (t.max, o.max) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
        }
        (StatsAccum::Distinct(t), StatsAccum::Distinct(o)) => t.extend_other(o),
        (StatsAccum::Last(_), StatsAccum::Last(_)) | (StatsAccum::Top(_), StatsAccum::Top(_)) => {
            // 行序敏感度量不走分片归并（spawn 门控）; 防御性静默（对齐旧行为）。
        }
        _ => unreachable!("StatsAccum 归并变体不匹配（plan 与构造不一致的内部错误）"),
    }
}

/// 归并两个桶累加器载体（分派按桶形态; 同 plan 两片恒同形态——不一致为内部
/// 错误）。SoA: 平行数组逐元素合并; Classic: 走 [`merge_accum`]。
fn merge_bucket_accs(t: &mut StatsBucketAccs, o: StatsBucketAccs) {
    match (t, o) {
        (StatsBucketAccs::Numeric(t), StatsBucketAccs::Numeric(o)) => {
            for (i, c) in o.counts.iter().enumerate() {
                t.counts[i] += c;
            }
            for (i, s) in o.sums.iter().enumerate() {
                t.sums[i] += s;
            }
            for (i, m) in o.mins.iter().enumerate() {
                if let Some(v) = *m {
                    t.mins[i] = Some(match t.mins[i] {
                        Some(x) if x <= v => x,
                        _ => v,
                    });
                }
            }
            for (i, m) in o.maxs.iter().enumerate() {
                if let Some(v) = *m {
                    t.maxs[i] = Some(match t.maxs[i] {
                        Some(x) if x >= v => x,
                        _ => v,
                    });
                }
            }
        }
        (StatsBucketAccs::Classic(t), StatsBucketAccs::Classic(o)) => {
            for (t, o) in t.iter_mut().zip(o.iter()) {
                merge_accum(t, o);
            }
        }
        _ => unreachable!("StatsBucketAccs 归并形态不匹配（同 plan 两片恒同形态）"),
    }
}

// ---------------------------------------------------------------------------
// 键列预解析（P5+ 优化: 每批一次类型分派, 免逐行 `downcast_ref`）
// ---------------------------------------------------------------------------
//
// `fanout::scope_key_from_column` 每行每键重复做 `col.data_type()` match +
// `downcast_ref` 动态分派（Q18 双键 × 27.6M 行）。此处每批解析一次键列类型为
// `KeyColumn`（借用 batch 列数组）, 逐行直接 `is_null` + `value`——规范化与
// `scope_key_from_column` 完全一致（Int64/Timestamp → Int, Float64 → 规范化位,
// Utf8 → Str, Boolean → Str "true"/"false"）; 不支持类型回退 `Other`。

enum KeyColumn<'a> {
    Int64(&'a Int64Array),
    Timestamp(&'a arrow::array::TimestampNanosecondArray),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    Boolean(&'a BooleanArray),
    /// 其它类型 → 逐行回退 `scope_key_from_column`（罕见）。
    Other(usize),
}

fn resolve_key_columns<'a>(batch: &'a RecordBatch, key_cols: &[usize]) -> Vec<KeyColumn<'a>> {
    use arrow::datatypes::{DataType, TimeUnit};
    key_cols
        .iter()
        .map(|&ci| {
            let col = batch.column(ci);
            match col.data_type() {
                DataType::Int64 => col
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(KeyColumn::Int64)
                    .unwrap_or(KeyColumn::Other(ci)),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => col
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .map(KeyColumn::Timestamp)
                    .unwrap_or(KeyColumn::Other(ci)),
                DataType::Float64 => col
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map(KeyColumn::Float64)
                    .unwrap_or(KeyColumn::Other(ci)),
                DataType::Utf8 => col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(KeyColumn::Utf8)
                    .unwrap_or(KeyColumn::Other(ci)),
                DataType::Boolean => col
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .map(KeyColumn::Boolean)
                    .unwrap_or(KeyColumn::Other(ci)),
                _ => KeyColumn::Other(ci),
            }
        })
        .collect()
}

/// 从预解析键列读单行叶键（null → None; 规范化与 `scope_key_from_column` 同）。
fn key_column_comp<'a>(
    col: &KeyColumn<'a>,
    batch: &'a RecordBatch,
    row: usize,
) -> Option<ScopeKey> {
    match col {
        KeyColumn::Int64(a) => {
            if a.is_null(row) {
                None
            } else {
                Some(ScopeKey::Int(a.value(row)))
            }
        }
        KeyColumn::Timestamp(a) => {
            if a.is_null(row) {
                None
            } else {
                Some(ScopeKey::Int(a.value(row)))
            }
        }
        KeyColumn::Float64(a) => {
            if a.is_null(row) {
                None
            } else {
                Some(scope_key_from_f64(a.value(row)))
            }
        }
        KeyColumn::Utf8(a) => {
            if a.is_null(row) {
                None
            } else {
                Some(ScopeKey::Str(a.value(row).into()))
            }
        }
        KeyColumn::Boolean(a) => {
            if a.is_null(row) {
                None
            } else {
                Some(ScopeKey::Str(
                    if a.value(row) { "true" } else { "false" }.into(),
                ))
            }
        }
        KeyColumn::Other(ci) => scope_key_from_column(batch, *ci, row),
    }
}

/// f64 → 键叶（与 `ScopeKey::from_value(Number)` 同规范化: 整数 <2^53 → Int,
/// 否则 Float(规范化位)）。
fn scope_key_from_f64(n: f64) -> ScopeKey {
    if n.fract() == 0.0 && n.abs() < TWO_POW_53 {
        ScopeKey::Int(n as i64)
    } else {
        ScopeKey::Float(canonical_f64_bits(n))
    }
}

/// 规范化 f64 位（0.0 → +0.0, NaN → canonical NaN; 与 key.rs 同口径）。
fn canonical_f64_bits(n: f64) -> u64 {
    if n == 0.0 {
        0.0f64.to_bits()
    } else if n.is_nan() {
        f64::NAN.to_bits()
    } else {
        n.to_bits()
    }
}

/// <2^53 的整数可被 f64 精确表示（与 `ScopeKey::from_value` 一致）。
const TWO_POW_53: f64 = 9_007_199_254_740_992.0;

// ---------------------------------------------------------------------------
// 复合键扁平哈希（P5+ 优化）
// ---------------------------------------------------------------------------
//
// 桶表键 = 扁平键的 FNV 式混合 u64。`comps_hash`（列式叶数组）与
// `scope_key_hash`（行式完整树）字节级同构——同一逻辑键两种路径产出同值
// （`stats_composite_key_hash_flat_matches_tree` 锁定）; 碰撞链内以完整键比较
// 消歧。混合风格与 `key.rs::scope_key_shard_index` 一致（确定性, 无随机种子）。

const KEY_HASH_BASE: u64 = 0xcbf2_9ce4_8422_2325;

fn mix_byte(hash: &mut u64, b: u8) {
    *hash ^= u64::from(b);
    *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for &b in bytes {
        mix_byte(hash, b);
    }
}

/// 完整 `ScopeKey` 树哈希（行式路径）——与 `key.rs::scope_key_shard_index`
/// 同字节序列（tag + 嵌套 payload, Pair 以 0x1f 分隔）。pub(crate) 供
/// 哈希同构契约测试。
pub(crate) fn scope_key_hash(key: &ScopeKey) -> u64 {
    let mut h = KEY_HASH_BASE;
    let tag = match key {
        ScopeKey::Empty => 0u8,
        ScopeKey::Int(_) => 1,
        ScopeKey::Float(_) => 2,
        ScopeKey::Str(_) => 3,
        ScopeKey::Pair(_, _) => 4,
    };
    mix_byte(&mut h, tag);
    nested_key_bytes(&mut h, key);
    h
}

fn nested_key_bytes(h: &mut u64, key: &ScopeKey) {
    match key {
        ScopeKey::Empty => {}
        ScopeKey::Int(v) => hash_bytes(h, &v.to_ne_bytes()),
        ScopeKey::Float(bits) => hash_bytes(h, &bits.to_ne_bytes()),
        ScopeKey::Str(s) => hash_bytes(h, s.as_bytes()),
        ScopeKey::Pair(a, b) => {
            nested_key_bytes(h, a);
            mix_byte(h, 0x1f);
            nested_key_bytes(h, b);
        }
    }
}

/// 叶数组（列式扁平键）哈希——**字节级镜像** `scope_key_hash` 的树字节序列:
/// 仅最外层 Pair tag（内层叶只 payload, 与嵌套树一致——内层类型歧义由碰撞链
/// 完整比较消歧）; N-1 个 0x1f 分隔。同逻辑键两路径产出同值（契约测试锁定）。
/// pub(crate) 供哈希同构契约测试。
pub(crate) fn comps_hash(comps: &[ScopeKey]) -> u64 {
    let mut h = KEY_HASH_BASE;
    let n = comps.len();
    match n {
        0 => {}
        1 => mix_leaf(&mut h, &comps[0], true), // 顶层单叶: tag + payload
        _ => {
            mix_byte(&mut h, 4); // 顶层 Pair tag（仅最外层 mix tag）
            for (i, c) in comps.iter().enumerate() {
                mix_leaf(&mut h, c, false); // 内层叶只 payload
                if i + 1 < n {
                    mix_byte(&mut h, 0x1f);
                }
            }
        }
    }
    h
}

/// 叶字节混入: `with_tag` = 顶层叶（mix 类型 tag）; 内层叶仅 payload。
fn mix_leaf(h: &mut u64, c: &ScopeKey, with_tag: bool) {
    match c {
        ScopeKey::Int(v) => {
            if with_tag {
                mix_byte(h, 1);
            }
            hash_bytes(h, &v.to_ne_bytes());
        }
        ScopeKey::Float(bits) => {
            if with_tag {
                mix_byte(h, 2);
            }
            hash_bytes(h, &bits.to_ne_bytes());
        }
        ScopeKey::Str(s) => {
            if with_tag {
                mix_byte(h, 3);
            }
            hash_bytes(h, s.as_bytes());
        }
        _ => unreachable!("comps 只含叶变体"),
    }
}

/// 左深 Pair 树与叶数组比较（列式命中校验）: `comps[start..end]` 是否被
/// `scope` 完全匹配。右叶恒为单键, 与 `comps[end-1]` 直接相等比较。
fn comps_match(scope: &ScopeKey, comps: &[ScopeKey], start: usize, end: usize) -> bool {
    match scope {
        ScopeKey::Empty => start == end,
        ScopeKey::Int(_) | ScopeKey::Float(_) | ScopeKey::Str(_) => {
            start + 1 == end && comps.get(start) == Some(scope)
        }
        ScopeKey::Pair(l, r) => {
            if start >= end {
                return false;
            }
            comps.get(end - 1) == Some(r.as_ref()) && comps_match(l, comps, start, end - 1)
        }
    }
}

/// 叶数组 → 完整 `ScopeKey`（左深 Pair 链; 每桶一次, 建桶时）。
/// pub(crate) 供哈希同构契约测试。
pub(crate) fn scope_key_from_comps(comps: &[ScopeKey]) -> ScopeKey {
    let mut acc: Option<ScopeKey> = None;
    for c in comps {
        acc = Some(match acc {
            None => c.clone(),
            Some(prev) => ScopeKey::Pair(Box::new(prev), Box::new(c.clone())),
        });
    }
    acc.unwrap_or(ScopeKey::Empty)
}

fn value_to_i128(v: &Value) -> Option<i128> {
    match v {
        Value::Number(n) => Some(*n as i128),
        _ => None,
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}

/// last/top 行更新（Q18/Q19, 非归并状态）:
/// - `last`: 最近合格行的行字段列数组替换（流有序 = 事件时间最新, 权威 Q18
///   ORDER BY dateTime DESC）; 同桶多 last 度量共享同一 Arc（内存 1 份）。
/// - `top`: 按 key DESC 插入有界 top-N（同 key 先到者优先——流有序下的确定性
///   tie-break, 权威 Q19 未指定平局顺序）。
///
/// `field_idx` = 度量字段在行字段列数组中的位置（P5 预计算, 免字符串查找）;
/// `None` = 字段不在子集/无字段 → top 无键跳过, last 仍保留行。
fn apply_last_top(
    acc: &mut StatsAccum,
    measure: &StatsMeasurePlan,
    row: &std::sync::Arc<RowFields>,
    field_idx: Option<usize>,
) {
    match measure.agg {
        StatsAggPlan::Last => {
            *acc.last_mut() = Some(std::sync::Arc::clone(row));
        }
        StatsAggPlan::Top => {
            let Some(key) = field_idx.and_then(|i| row.f64_at(i)) else {
                return; // 非数值键 → 跳过（与 sum 跳过非数值一致）
            };
            let n = measure.arg.unwrap_or(10) as usize;
            if n == 0 {
                return; // top(0, ...): 不保留任何条目
            }
            let entries = acc.top_mut();
            // 快速淘汰: 已满且 key 进不了前 N（≤ 当前最小）→ 跳过。同 key 新条目
            // 必插在既有同 key 条目之后（先到者在前）, 满时必被截断——跳过后语义
            // 不变, 免去每事件整行克隆（Q19 绝大部分 bid 低于当前 top-10 门槛）。
            if entries.len() == n && key <= entries[n - 1].key {
                return;
            }
            // Arc 深拷贝为独立 Box（top 条目各自的行, 不共享）
            insert_top(entries, key, row.as_ref().clone(), n);
        }
        _ => {}
    }
}

/// top-N 插入: key DESC 有序保留前 N; 同 key 新条目插在已有同 key 条目之后
/// （先到者在前）。n=0 时清空（top(0, ...) 边界）。
fn insert_top(entries: &mut Vec<TopEntry>, key: f64, row: RowFields, n: usize) {
    if n == 0 {
        return;
    }
    // 快速淘汰: 已满且 key 进不了前 N（≤ 当前最小）→ 跳过。同 key 新条目必插在
    // 既有同 key 条目之后（先到者在前）, 满时必被截断——跳过后语义不变, 免去
    // 每事件整行克隆（Q19 绝大部分 bid 低于当前 top-10 门槛）。
    if entries.len() == n && key <= entries[n - 1].key {
        return;
    }
    let pos = entries
        .iter()
        .position(|e| key > e.key)
        .unwrap_or(entries.len());
    entries.insert(pos, TopEntry { key, row });
    if entries.len() > n {
        entries.truncate(n);
    }
}

fn value_to_distinct_key(v: &Value) -> DistinctKey {
    match v {
        Value::Number(n) => DistinctKey::from_f64(*n),
        Value::Str(s) => DistinctKey::from_str(s),
        Value::Bool(b) => DistinctKey::Int(if *b { 1 } else { 0 }),
        _ => DistinctKey::Str(format!("{:?}", v).into()),
    }
}

// ---------------------------------------------------------------------------
// 桶键求值（P2 复合键: Field / bucket() / tier()）
// ---------------------------------------------------------------------------

/// 由 `plan.measures` + 桶累加器计算度量值（avg 输出时 sum/count, D6）。
/// last 取字段数值（非数值 → 0; 无子集时 idx 未知 → 0, 标量访问器需子集）;
/// top 为多值不适用, 返回 0（rich close 用）。
/// SoA 桶 → 每度量 f64 值（语义与 [`measure_values`] 的 Numeric 分支一致:
/// count/sum 直取、avg = sum/count（count==0 → 0.0）、min/max unwrap_or(0)）。
/// pub(crate) 供 SoA 对照 bench。
pub(crate) fn measure_values_soa(plan: &StatsPlan, soa: &NumericSoA, layout: &NumericSoALayout) -> Vec<f64> {
    plan.measures
        .iter()
        .enumerate()
        .map(|(idx, m)| match m.agg {
            StatsAggPlan::Count => soa.counts[idx] as f64,
            StatsAggPlan::Sum => soa.sums[layout.sum_slot[idx].unwrap() as usize] as f64,
            StatsAggPlan::Avg => {
                let s = layout.sum_slot[idx].unwrap() as usize;
                let n = soa.counts[idx];
                if n == 0 {
                    0.0
                } else {
                    soa.sums[s] as f64 / n as f64
                }
            }
            StatsAggPlan::Min => {
                soa.mins[layout.min_slot[idx].unwrap() as usize].unwrap_or(0) as f64
            }
            StatsAggPlan::Max => {
                soa.maxs[layout.max_slot[idx].unwrap() as usize].unwrap_or(0) as f64
            }
            _ => unreachable!("SoA 桶仅数值度量（count/sum/avg/min/max）"),
        })
        .collect()
}

/// 桶累加器 → 每度量 f64 值（按桶形态分派）。
fn bucket_measure_values(
    plan: &StatsPlan,
    accs: &StatsBucketAccs,
    soa_layout: Option<&NumericSoALayout>,
    measure_field_idx: &[Option<usize>],
) -> Vec<f64> {
    match accs {
        StatsBucketAccs::Numeric(soa) => measure_values_soa(plan, soa, soa_layout.unwrap()),
        StatsBucketAccs::Classic(accs) => measure_values(plan, accs, measure_field_idx),
    }
}

/// 桶累加器 → close 条目列表（按桶形态分派）。SoA 全标量（无 row_fields——
/// SoA 计划不含 last/top）; Classic 走 [`bucket_measure_entries`]。
fn bucket_close_entries(
    plan: &StatsPlan,
    accs: &StatsBucketAccs,
    soa_layout: Option<&NumericSoALayout>,
    measure_field_idx: &[Option<usize>],
) -> Vec<Vec<StatsCloseEntry>> {
    match accs {
        StatsBucketAccs::Numeric(soa) => measure_values_soa(plan, soa, soa_layout.unwrap())
            .into_iter()
            .map(|v| {
                vec![StatsCloseEntry {
                    measure_value: v,
                    row_fields: None,
                }]
            })
            .collect(),
        StatsBucketAccs::Classic(accs) => plan
            .measures
            .iter()
            .zip(accs.iter())
            .zip(measure_field_idx.iter())
            .map(|((m, acc), fidx)| bucket_measure_entries(m, acc, *fidx))
            .collect(),
    }
}

fn measure_values(
    plan: &StatsPlan,
    accs: &[StatsAccum],
    measure_field_idx: &[Option<usize>],
) -> Vec<f64> {
    plan.measures
        .iter()
        .zip(accs.iter())
        .zip(measure_field_idx.iter())
        .map(|((m, acc), fidx)| match m.agg {
            StatsAggPlan::Count => acc.numeric().count as f64,
            StatsAggPlan::Sum => acc.numeric().sum as f64,
            StatsAggPlan::Avg => {
                let n = acc.numeric();
                if n.count == 0 {
                    0.0
                } else {
                    n.sum as f64 / n.count as f64
                }
            }
            StatsAggPlan::Min => acc.numeric().min.unwrap_or(0) as f64,
            StatsAggPlan::Max => acc.numeric().max.unwrap_or(0) as f64,
            StatsAggPlan::DistinctCount => match acc {
                StatsAccum::Distinct(d) => d.len() as f64,
                _ => 0.0,
            },
            StatsAggPlan::Last => match (acc.last(), fidx) {
                (Some(row), Some(i)) => row.f64_at(*i).unwrap_or(0.0),
                _ => 0.0,
            },
            StatsAggPlan::Top => 0.0,
        })
        .collect()
}

/// 每桶输出条目: 度量值 + 可选行字段紧凑存储（last/top 注入 yield 用; 标量 =
/// None）。行字段为 Arc（与状态共享, close 零拷贝; 构造 alert 时才逐值构造）。
/// 列序 = `StatsExecutor::row_field_names()`（None 子集 = schema 列序）。
#[derive(Debug, Clone)]
pub struct StatsCloseEntry {
    pub measure_value: f64,
    pub row_fields: Option<std::sync::Arc<RowFields>>,
}

/// 每桶 close 输出: 每度量一个值列表（标量 = 1; top = N, 按 rank 序）。
#[derive(Debug, Clone)]
pub struct StatsCloseBucket {
    pub key: ScopeKey,
    pub measures: Vec<Vec<StatsCloseEntry>>,
}

/// 单个度量的 close 条目列表（标量 = 1; last = 1 带行字段列数组; top = N 带行
/// 字段列数组）。行字段为 Arc（与状态共享, close 零拷贝; 构造 alert 时才逐值
/// 克隆）。`field_idx` = 该度量字段在列数组中的位置（P5 预计算）。
fn bucket_measure_entries(
    m: &StatsMeasurePlan,
    acc: &StatsAccum,
    field_idx: Option<usize>,
) -> Vec<StatsCloseEntry> {
    let scalar = |value: f64| StatsCloseEntry {
        measure_value: value,
        row_fields: None,
    };
    match m.agg {
        StatsAggPlan::Count => vec![scalar(acc.numeric().count as f64)],
        StatsAggPlan::Sum => vec![scalar(acc.numeric().sum as f64)],
        StatsAggPlan::Avg => {
            let n = acc.numeric();
            vec![scalar(if n.count == 0 {
                0.0
            } else {
                n.sum as f64 / n.count as f64
            })]
        }
        StatsAggPlan::Min => vec![scalar(acc.numeric().min.unwrap_or(0) as f64)],
        StatsAggPlan::Max => vec![scalar(acc.numeric().max.unwrap_or(0) as f64)],
        StatsAggPlan::DistinctCount => {
            let len = match acc {
                StatsAccum::Distinct(d) => d.len(),
                _ => 0,
            };
            vec![scalar(len as f64)]
        }
        StatsAggPlan::Last => {
            let row = acc.last();
            let value = match (row, field_idx) {
                (Some(row), Some(i)) => row.f64_at(i).unwrap_or(0.0),
                _ => 0.0,
            };
            vec![StatsCloseEntry {
                measure_value: value,
                row_fields: row.clone(),
            }]
        }
        StatsAggPlan::Top => {
            let entries = acc.top();
            if entries.is_empty() {
                // 空条目（top(0, ...) 或全部非数值键）: 不产出——n_records 由其它
                // 度量驱动; 全是 top 时整桶不产出（与 CEP 无实例无输出一致）。
                return vec![];
            }
            entries
                .iter()
                .map(|e| StatsCloseEntry {
                    measure_value: e.key,
                    row_fields: Some(std::sync::Arc::from(e.row.clone())),
                })
                .collect()
        }
    }
}

/// 行式桶键（复合键: 逐 key 求值 → Pair 组合）。任一 key 缺失/不可求值 → None。
fn eval_row_key(keys: &[Expr], row: &HashMap<String, Value>) -> Option<ScopeKey> {
    let mut acc: Option<ScopeKey> = None;
    for e in keys {
        let v = eval_row_bucket_key(e, row)?;
        acc = Some(match acc {
            None => v,
            Some(prev) => ScopeKey::Pair(Box::new(prev), Box::new(v)),
        });
    }
    Some(acc.unwrap_or(ScopeKey::Empty))
}

/// 单个桶键表达式求值（P2 桶键函数子集）:
/// - `Field` → 读字段（`ScopeKey::from_value` 与列式 `scope_key_columnar` 同构）
/// - `bucket(field, 'day'|'hour'|...)` → 时间下界（整数 nanos 桶）
/// - `tier(field, b1, b2, ...)` → 区间桶索引（边界升序, `v < b_i` 归属 i）
/// - 其它/不可求值 → None（行跳过）
fn eval_row_bucket_key(expr: &Expr, row: &HashMap<String, Value>) -> Option<ScopeKey> {
    match expr {
        Expr::Field(fr) => row.get(field_name(fr)).map(ScopeKey::from_value),
        Expr::FuncCall {
            qualifier: None,
            name,
            args,
        } => match name.as_str() {
            "bucket" => {
                let field = args.first()?;
                let unit = bucket_unit_nanos(args.get(1)?)?;
                let v = field_value_of(field, row)?;
                let nanos = v as i64;
                Some(ScopeKey::Int((nanos / unit) * unit))
            }
            "tier" => {
                let field = args.first()?;
                let v = field_value_of(field, row)?;
                let bounds: Vec<f64> = args[1..]
                    .iter()
                    .map(|b| match b {
                        Expr::Number(n) => Some(*n),
                        _ => None,
                    })
                    .collect::<Option<_>>()?;
                Some(ScopeKey::Int(tier_index(v, &bounds)))
            }
            _ => None,
        },
        _ => None,
    }
}

fn field_value_of(expr: &Expr, row: &HashMap<String, Value>) -> Option<f64> {
    match expr {
        Expr::Field(fr) => match row.get(field_name(fr)) {
            Some(Value::Number(n)) => Some(*n),
            _ => None,
        },
        Expr::Number(n) => Some(*n),
        _ => None,
    }
}

fn bucket_unit_nanos(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::StringLit(s) => match s.as_str() {
            "day" => Some(86_400_000_000_000),
            "hour" => Some(3_600_000_000_000),
            "minute" => Some(60_000_000_000),
            "second" => Some(1_000_000_000),
            _ => None,
        },
        _ => None,
    }
}

/// 边界升序; `v < bounds[0]` → 0, `< bounds[1]` → 1, ..., 否则 `bounds.len()`。
fn tier_index(v: f64, bounds: &[f64]) -> i64 {
    bounds.iter().position(|b| v < *b).unwrap_or(bounds.len()) as i64
}

/// 行式 last/top 行字段提取（与列式 [`row_fields_from_batch`] 对齐, P5 紧凑化）:
/// 按 `names` 列序返回 `Box<[Option<Value>]>`（缺失 = `None`）。`None` = 全列,
/// 行键**排序**（确定性; 仅测试/缺省——生产经 spawn 恒有子集）。
fn row_fields_from_row(
    row: &HashMap<String, Value>,
    names: Option<&[String]>,
    layout: &std::sync::Arc<RowFieldLayout>,
) -> std::sync::Arc<RowFields> {
    let mut fields = RowFields::empty(std::sync::Arc::clone(layout));
    match names {
        Some(ns) => {
            for (i, n) in ns.iter().enumerate() {
                fields.set(i, row.get(n).cloned());
            }
        }
        None => {
            let mut keys: Vec<&String> = row.keys().collect();
            keys.sort();
            for (i, k) in keys.iter().enumerate() {
                fields.set(i, row.get(*k).cloned());
            }
        }
    }
    std::sync::Arc::new(fields)
}

/// 从 batch 行提取字段列数组（last/top 列式路径用, P5 紧凑化）: 按 `cols`
/// （每字段列索引, 每批预解析一次——免逐行 `schema.index_of`）提取, null/缺失 =
/// `None`。`cols = None` 时全部 schema 列按字段名**排序**（与行式 None 同序——
/// 行键 == schema 字段时两路径列序一致; 测试/缺省路径）。
fn row_fields_from_batch(
    batch: &RecordBatch,
    row: usize,
    cols: Option<&[Option<usize>]>,
    layout: &std::sync::Arc<RowFieldLayout>,
) -> std::sync::Arc<RowFields> {
    let schema = batch.schema();
    let mut fields = RowFields::empty(std::sync::Arc::clone(layout));
    match cols {
        Some(cols) => {
            for (i, ci) in cols.iter().enumerate() {
                let v = match ci {
                    Some(ci) => {
                        let col = batch.column(*ci);
                        if col.is_null(row) {
                            None
                        } else {
                            extract_field_value(schema.field(*ci), col.as_ref(), row)
                        }
                    }
                    None => None, // 字段缺失 → None
                };
                fields.set(i, v);
            }
        }
        None => {
            let mut names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            names.sort();
            for (i, name) in names.iter().enumerate() {
                let col_idx = schema.index_of(name).expect("schema 字段必存在");
                let col = batch.column(col_idx);
                if col.is_null(row) {
                    fields.set(i, None);
                } else {
                    fields.set(
                        i,
                        extract_field_value(schema.field(col_idx), col.as_ref(), row),
                    );
                }
            }
        }
    }
    std::sync::Arc::new(fields)
}

/// 从 batch 列读单行原生数值（Int64 原生 i64 → i128, 不走 f64——D8: ≥2^53 的
/// Int64 经 `Value::Number(f64)` 会丢精度; Float64 按 `sum_masked` 同口径截断）。
/// null / 非数值列 → None（与行式 `value_to_i128` 的 None 一致）。
fn column_i128_at(batch: &RecordBatch, ci: usize, row: usize) -> Option<i128> {
    let col = batch.column(ci);
    if col.is_null(row) {
        return None;
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as i128);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row) as i128);
    }
    None
}

/// 从 batch 列读单行原生数值（top 快速淘汰预检用; 列索引预解析, 零 index_of）。
/// Int64 → as f64 / Float64 → 原值——与行字段提取后 `value_to_f64(Value::Number)`
/// 同口径（event_bridge 契约: Int64 → Number(i as f64), Float64 → Number(f)）。
/// 非数值类型 → None（调用方回退原路径, 语义不变）。
fn column_f64_at(batch: &RecordBatch, ci: usize, row: usize) -> Option<f64> {
    let col = batch.column(ci);
    if col.is_null(row) {
        return None;
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row));
    }
    None
}

/// 索引版 distinct 键读取（列索引批级预解析, 免每行 schema.index_of——q17 同款修复）。
/// 与列式段 `insert_distinct_column` 同类型分派, 原生值构造（D7: 禁止
/// `Value::Number(f64)` 化 ≥2^53 的 Int64）。null / 类型不在支持集 → None。
fn column_distinct_key_at(
    batch: &RecordBatch,
    ci: usize,
    row: usize,
) -> Option<DistinctKey> {
    use arrow::array::TimestampNanosecondArray;
    let col = batch.column(ci);
    if col.is_null(row) {
        return None;
    }
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(DistinctKey::from_i64(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(DistinctKey::from_f64(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return Some(DistinctKey::from_str(a.value(row)));
    }
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        return Some(DistinctKey::from_f64(if a.value(row) { 1.0 } else { 0.0 }));
    }
    if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Some(DistinctKey::from_i64(a.value(row)));
    }
    None
}

// ---------------------------------------------------------------------------
// 列式段辅助（P1.5）
// ---------------------------------------------------------------------------

/// 行域迭代（P2 分片裁剪）: `rows` = 本片行索引（升序, 绝对行号）; `None` =
/// 全批 `0..n`。行域驱动的归并段（count/sum/minmax/distinct）只遍历本片行,
/// 消除每片对全批的 O(n) 冗余扫描（q15 输入分片 10× 冗余归因, 2026-08-24）。
fn domain_rows(rows: Option<&[u32]>, n: usize) -> Box<dyn Iterator<Item = usize> + '_> {
    match rows {
        Some(rs) => Box::new(rs.iter().map(|&r| r as usize)),
        None => Box::new(0..n),
    }
}

/// 行域内满足 where 过滤的行数（`wi` = unique_wheres 索引; `None` = 恒通过）。
/// 等价 `count_true(combine(domain, where))`——逐行查 where mask 位（null slot
/// 读 false, 与 `BooleanArray::value` 一致）。
fn count_domain(rows: Option<&[u32]>, n: usize, masks: &[BooleanArray], wi: Option<usize>) -> u64 {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    match rows {
        Some(rs) => rs.iter().filter(|&&r| passes(r as usize)).count() as u64,
        None => (0..n).filter(|&r| passes(r)).count() as u64,
    }
}

/// 行域驱动求和（null 跳过; 数值按行式 `value_to_i128` 截断, D8）。
fn sum_domain(
    col: &NumCol<'_>,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
) -> i128 {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    match col {
        NumCol::Int64(c) => domain_rows(rows, n)
            .filter(|&r| passes(r) && !c.is_null(r))
            .map(|r| c.value(r) as i128)
            .sum(),
        NumCol::Float64(c) => domain_rows(rows, n)
            .filter(|&r| passes(r) && !c.is_null(r))
            .map(|r| c.value(r) as i128)
            .sum(),
    }
}

/// 行域驱动 min/max（null 跳过）。
fn minmax_domain(
    col: &NumCol<'_>,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
    min: &mut Option<i128>,
    max: &mut Option<i128>,
) {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    let fold = |v: i128, min: &mut Option<i128>, max: &mut Option<i128>| {
        *min = Some(match *min {
            Some(m) if m <= v => m,
            _ => v,
        });
        *max = Some(match *max {
            Some(m) if m >= v => m,
            _ => v,
        });
    };
    match col {
        NumCol::Int64(c) => {
            for r in domain_rows(rows, n) {
                if passes(r) && !c.is_null(r) {
                    fold(c.value(r) as i128, min, max);
                }
            }
        }
        NumCol::Float64(c) => {
            for r in domain_rows(rows, n) {
                if passes(r) && !c.is_null(r) {
                    fold(c.value(r) as i128, min, max);
                }
            }
        }
    }
}

/// 行域驱动的单极值更新（SoA 用: Min 度量只写 min 槽, Max 度量只写 max 槽——
/// SoA 无死状态; 语义 = [`minmax_domain`] 的对应半段）。
fn minmax_domain_one(
    col: &NumCol<'_>,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
    is_min: bool,
    out: &mut Option<i128>,
) {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    let fold = |v: i128, out: &mut Option<i128>| {
        *out = Some(match *out {
            Some(x) if (is_min && x <= v) || (!is_min && x >= v) => x,
            _ => v,
        });
    };
    match col {
        NumCol::Int64(c) => {
            for r in domain_rows(rows, n) {
                if passes(r) && !c.is_null(r) {
                    fold(c.value(r) as i128, out);
                }
            }
        }
        NumCol::Float64(c) => {
            for r in domain_rows(rows, n) {
                if passes(r) && !c.is_null(r) {
                    fold(c.value(r) as i128, out);
                }
            }
        }
    }
}

/// 行域驱动的 distinct 插入（原生列值按行域 + where 过滤）——等价
/// `insert_distinct_column` 的 mask 全批扫描, 但只遍历本片行。
fn insert_distinct_domain(
    batch: &RecordBatch,
    name: &str,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
    set: &mut DistinctSet,
) -> bool {
    let Some(idx) = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == name)
    else {
        return true; // 字段缺失 → 全 null（与行式 extract None 一致）
    };
    let col = batch.column(idx).as_ref();
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    if let Some(c) = col.as_any().downcast_ref::<Int64Array>() {
        for r in domain_rows(rows, n) {
            if passes(r) && !c.is_null(r) {
                set.insert(DistinctKey::from_i64(c.value(r)));
            }
        }
        return true;
    }
    if let Some(c) = col.as_any().downcast_ref::<Float64Array>() {
        for r in domain_rows(rows, n) {
            if passes(r) && !c.is_null(r) {
                set.insert(DistinctKey::from_f64(c.value(r)));
            }
        }
        return true;
    }
    if let Some(c) = col.as_any().downcast_ref::<StringArray>() {
        for r in domain_rows(rows, n) {
            if passes(r) && !c.is_null(r) {
                set.insert(DistinctKey::from_str(c.value(r)));
            }
        }
        return true;
    }
    if let Some(c) = col.as_any().downcast_ref::<BooleanArray>() {
        for r in domain_rows(rows, n) {
            if passes(r) && !c.is_null(r) {
                set.insert(DistinctKey::from_f64(if c.value(r) { 1.0 } else { 0.0 }));
            }
        }
        return true;
    }
    if let Some(c) = col
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
    {
        for r in domain_rows(rows, n) {
            if passes(r) && !c.is_null(r) {
                set.insert(DistinctKey::from_i64(c.value(r)));
            }
        }
        return true;
    }
    false
}

/// distinct 度量字段列类型支持检查（Int64/Float64/Utf8/Bool/TimestampNs）。
/// 字段缺失视为安全（与行式 extract None 一致, 不插入）。
///
/// **必须在 `process_batch` 任何累加副作用之前调用**: 段 2 中途失败返回 false
/// 时调用方会回退 `process_rows`, 此时段 1 已累加的 count/sum 会被重复计算
/// （部分应用 bug）——类型支持与否必须一次性前置判定。
fn distinct_fields_columnar_safe(batch: &RecordBatch, plan: &StatsPlan) -> bool {
    for m in &plan.measures {
        if !matches!(m.agg, StatsAggPlan::DistinctCount) {
            continue;
        }
        let Some(field) = &m.field else {
            continue;
        };
        let Some(idx) = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == field_name(field))
        else {
            continue; // 字段缺失 → 全 null
        };
        let dt = batch.column(idx).data_type();
        let supported = matches!(
            dt,
            DataType::Int64
                | DataType::Float64
                | DataType::Utf8
                | DataType::Boolean
                | DataType::Timestamp(TimeUnit::Nanosecond, _)
        );
        if !supported {
            return false;
        }
    }
    true
}

/// 数值列引用（distinct 支持集之外的列式归并）。
enum NumCol<'a> {
    Int64(&'a Int64Array),
    Float64(&'a Float64Array),
}

fn numeric_col<'a>(batch: &'a RecordBatch, name: &str) -> Option<NumCol<'a>> {
    let idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == name)?;
    let col = batch.column(idx).as_ref();
    if let Some(c) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(NumCol::Int64(c));
    }
    if let Some(c) = col.as_any().downcast_ref::<Float64Array>() {
        return Some(NumCol::Float64(c));
    }
    None
}
