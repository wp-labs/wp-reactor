//! Stats executor — 声明式窗口统计执行器（P1: 空键 fixed count/distinct）。
//!
//! 与 CEP(RuleExecutor/match)完全正交: 消费 fanout 的 raw RecordBatch,
//! 内部维护可交换结合的归并状态, 窗口 close 时产出度量值并复用 alert 构建。
//!
//! 设计依据: docs/stats-executor-design.md v6（§6 执行器）。

use std::collections::{HashMap, HashSet, VecDeque};

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsPlan};

use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};
use crate::match_engine::event_bridge::extract_field_value;
use crate::match_engine::match_engine::{Event, ScopeKey, field_ref_name};
use crate::match_engine::spill::SpillStore;
use crate::match_engine::{EngineHashMap, EngineHashSet, Value};
use crate::window::scope_key_columnar;
use crate::window::scope_key_from_column;

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

    // -- spill 序列化访问器（pub(crate)：仅 wf-engine 内部 spill 模块使用）--

    /// 数字槽数组（layout 槽序）。
    pub(crate) fn numeric(&self) -> &[f64] {
        &self.numeric
    }

    /// 字符串槽数组（layout 槽序）。
    pub(crate) fn strings(&self) -> &[smol_str::SmolStr] {
        &self.strings
    }

    /// 其它槽数组（layout 槽序）。
    pub(crate) fn others(&self) -> &[Option<Value>] {
        &self.others
    }

    /// null 位掩码（layout 槽序，位 1 = null）。
    pub(crate) fn null_mask(&self) -> &[u64] {
        &self.null_mask
    }

    /// 从槽数组构造（spill 读回；槽序与 [`Self::empty`] 一致，布局由
    /// `layout` 描述——序列化不落 layout，读回按当前 executor 的 layout 解释）。
    pub(crate) fn from_parts(
        layout: std::sync::Arc<RowFieldLayout>,
        numeric: Box<[f64]>,
        strings: Box<[smol_str::SmolStr]>,
        others: Box<[Option<Value>]>,
        null_mask: Box<[u64]>,
    ) -> Self {
        Self {
            layout,
            numeric,
            strings,
            others,
            null_mask,
        }
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
    /// **规则级全局语义**（2026-08-27）：同规则全部分片共享一个 `mem_used`
    /// 计数器——`max_memory` 是用户配置的规则总驻留上限（分片数是引擎内部
    /// 细节, 用户不可见）。
    limit_bytes: Option<u64>,
    /// 共享已用状态内存计数器（跨分片; None = 未配置共享 → 用本地
    /// `estimated_bytes`, 测试/单片退化）。检查/驱逐/记账全部走它。
    mem_used_shared: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    /// 估算的在用状态内存（桶级预算模型: 新桶固定 allowance, 含 top/last 条目
    /// 预算——保守上界, 偏安全方向）。窗口 close 时清零。**本片账本**——
    /// 共享模式下与共享计数同步增减, 诊断/close 扣减用。
    estimated_bytes: u64,
    /// 累计超限拒收的新桶数（跨窗口累计, 供指标/告警）。
    over_limit_new_buckets: u64,
    /// 当前窗口是否已告警（每窗口一次, 防刷屏）。
    limit_warned: bool,
    /// 告警用的规则名（set_memory_limit 注入）。
    rule_name: String,
    /// 状态外溢存储（M3，`docs/design/stats-state-spill-redb.md`）。None = 未配置
    /// spill（Noop 语义，热路径零开销）。
    pub(crate) spill: Option<Box<dyn SpillStore + Send + Sync>>,
    /// 已 spill 键的存在性索引（hot path 未命中时 O(1) 查，不碰持久层）。
    pub(crate) spill_index: HashSet<u64>,
    /// 落盘字节上限（None = 不限）。三层预算阶梯第二层（内存→磁盘→拒收兜底）。
    /// **规则级全局语义**（2026-08-27）：同规则全部分片共享一个 `spill_used`
    /// 计数器——`max_disk` 是用户配置的规则总落盘上限（分片数是引擎
    /// 内部细节，用户不可见）。
    spill_limit_bytes: Option<u64>,
    /// 共享已落盘字节计数器（跨分片；None = 未配置 spill）。
    spill_used: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    /// spill 写失败/满后的拒收回退标记（避免反复尝试写）。
    spill_failed: bool,
    /// 落盘满/写失败的告警标记（每窗口一次，防刷屏）。
    spill_warned: bool,
    /// 时钟队列（近似 LRU）：桶**创建序**的 hash 环。驱逐扫描队首：
    /// 二次机会（touch > 0）→ 递减回队尾；否则驱逐。每在内存键至多一个条目。
    clock: VecDeque<u64>,
    /// 已读回（take）的键 hash（M5-2）：take 只读不删——redb 中旧条目在 close
    /// 时按此集合过滤（内存副本更新，避免重复输出）。与 spill_index 互补：
    /// 读回 → 出 spill_index 入 readback；再驱逐 → 入 spill_index 出 readback。
    pub(crate) readback: HashSet<u64>,
    /// 累计驱逐键数（跨窗口，指标/抖动观测用）。
    spill_evictions: u64,
    /// 累计读回次数（跨窗口，指标/抖动观测用）。
    spill_readbacks: u64,
    /// 驱逐分段耗时（ns，跨窗口累计；性能定位用——扫描/clone/redb 写三段的占比）。
    spill_scan_ns: u64,
    spill_clone_ns: u64,
    spill_write_ns: u64,
    /// 驱逐调用次数（分段耗时的分母）。
    spill_evict_calls: u64,
}

impl StatsWindowState {
    /// 新建窗口状态（无内存限制, 由 spawn 层按规则 limits 注入）。
    fn new(buckets: EngineHashMap<u64, Vec<StatsBucket>>) -> Self {
        StatsWindowState {
            buckets,
            window_start_nanos: 0,
            last_event_nanos: 0,
            event_count: 0,
            limit_bytes: None,
            mem_used_shared: None,
            estimated_bytes: 0,
            over_limit_new_buckets: 0,
            limit_warned: false,
            rule_name: String::new(),
            spill: None,
            spill_index: HashSet::new(),
            spill_limit_bytes: None,
            spill_used: None,
            spill_failed: false,
            spill_warned: false,
            clock: VecDeque::new(),
            readback: HashSet::new(),
            spill_evictions: 0,
            spill_readbacks: 0,
            spill_scan_ns: 0,
            spill_clone_ns: 0,
            spill_write_ns: 0,
            spill_evict_calls: 0,
        }
    }

    /// 注入状态内存上限（字节; None = 不设防）。未注入共享计数（测试/直接
    /// 调用）→ 本片独立预算（共享语义退化为单片, 与旧行为一致）。
    pub fn set_memory_limit(&mut self, rule_name: &str, bytes: Option<usize>) {
        self.set_memory_limit_shared(rule_name, bytes, None);
    }

    /// 注入状态内存上限（规则级共享版）：`mem_used_shared` = 同规则全部分片
    /// 共用一个内存占用计数——`max_memory` 是规则总驻留上限, 分片数是引擎
    /// 内部细节（spawn 层规则级创建, 分片 clone 注入）。
    pub fn set_memory_limit_shared(
        &mut self,
        rule_name: &str,
        bytes: Option<usize>,
        mem_used_shared: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) {
        self.rule_name = rule_name.to_string();
        self.limit_bytes = bytes.map(|b| b as u64);
        self.mem_used_shared = mem_used_shared;
    }

    /// 已用状态内存（检查口径）：共享计数读值（规则级）或本片估算（未共享）。
    fn mem_used_bytes(&self) -> u64 {
        self.mem_used_shared
            .as_ref()
            .map(|u| u.load(std::sync::atomic::Ordering::SeqCst))
            .unwrap_or(self.estimated_bytes)
    }

    /// 已用状态内存入账（共享计数同步; 未共享时只走本地 `estimated_bytes`）。
    fn mem_add(&self, n: u64) {
        if let Some(u) = &self.mem_used_shared {
            u.fetch_add(n, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// 已用状态内存出账（共享计数同步; 未共享时只走本地）。
    fn mem_sub(&self, n: u64) {
        if let Some(u) = &self.mem_used_shared {
            u.fetch_sub(n, std::sync::atomic::Ordering::SeqCst);
        }
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
        let allowance = Self::bucket_allowance(plan);
        let mut distinct_bytes = 0u64;
        for buckets in self.buckets.values() {
            for bucket in buckets {
                for acc in &bucket.accs {
                    if let StatsAccum::Distinct(set) = acc {
                        distinct_bytes += set.len() as u64 * Self::DISTINCT_ENTRY_BYTES;
                    }
                }
            }
        }
        let new = self.buckets.len() as u64 * allowance + distinct_bytes;
        // 共享计数同步差值（本片账本刷新 → 共享计数跟随）。
        let old = self.estimated_bytes;
        if new >= old {
            self.mem_add(new - old);
        } else {
            self.mem_sub(old - new);
        }
        self.estimated_bytes = new;
    }

    /// 累计因超限被拒收的新桶数。
    pub fn over_limit_new_buckets(&self) -> u64 {
        self.over_limit_new_buckets
    }

    /// 新桶预算（保守上界）: 固定基数 + Σ每度量变体 + 行字段共享份额 +
    /// top/last 条目预算。
    ///
    /// **2026-08-26 q18 校准**（对齐度量专用累加器）: 旧口径 512 + n×128 +
    /// last 160B/度量 → 1664B/键，高估真实 1.55× → 16GB 预算拒收阈值 961 万
    /// 键 < 30M 数据键数 2300 万 → **静默丢键**（over_limit_new_buckets）。
    /// 现按变体实际求和 + 行字段**每桶共享 1 份**（last/top 度量同桶同一
    /// [`RowFields`] Arc——`row_cache` 每行 1 份）。
    ///
    /// **已知限制**: `distinct_set` 值域增长不在固定基数内（q16 教训）——由
    /// [`Self::refresh_estimated_bytes`] 批末按真实 len 计入（保守上界）。
    fn bucket_allowance(plan: &StatsPlan) -> u64 {
        // 桶固定: ScopeKey 栈+堆(~72B) + StatsBucket 头 + accs Vec + HashMap
        // 槽(~64B) ≈ 160B → 取 256（1.6× 保守余量）。
        let mut bytes = 256u64;
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
        // 校准系数 2.2（2026-08-27 q18 实测）：估算 432B/键 vs 实际 960B/键
        // ——低估来源 = StatsAccum enum 实际 24B（估 16）+ RowFields 实测 168B
        // （估 112）+ hashbrown 桶数组/容量 + mimalloc 16B 对齐×每分配 + Vec
        // 容量。估算低估 → 驱逐水位失真（实际 2.2× 预算才驱逐）。
        // 常量（非 env）：热路径每新键调用, 且避免测试 env 竞态。
        (bytes as f64 * 2.2).round() as u64
    }

    /// 新建桶前的限额检查: 超限 → 先尝试 spill 腾空间（M3 三层预算阶梯第二层）
    /// → 仍超限 → 计数 + 每窗口告警一次 + 拒绝（false）。
    ///
    /// **计数口径（按行/尝试, 非按新键）**: 被拒的键不建桶 → 后续同键行仍走
    /// 查找未命中 → 每次尝试都计数。这是有意取舍——「每新键一次」需记录被拒键
    /// 集合（无界, 违背 guard 的内存有界承诺）; 按行计数不引入新状态, 只对
    /// 已在桶内的键不计数（命中）。告警/metrics 的 `over_limit_new_buckets`
    /// 实际含义是「被拒行数」。
    fn account_new_bucket(&mut self, plan: &StatsPlan) -> bool {
        let allowance = Self::bucket_allowance(plan);
        if let Some(limit) = self.limit_bytes
            && self.mem_used_bytes() + allowance > limit
        {
            // spill 启用且未失败: 先驱逐最老键腾空间（批量, 目标降到上限 90%）。
            if self.spill.is_some() && !self.spill_failed {
                self.evict_to_spill(plan);
                if self.mem_used_bytes() + allowance <= limit {
                    self.estimated_bytes += allowance;
                    self.mem_add(allowance);
                    return true;
                }
                // 落盘满/写失败 → 落到拒收兜底（下面）
            }
            self.over_limit_new_buckets += 1;
            if !self.limit_warned {
                self.limit_warned = true;
                log::warn!(
                    "stats 状态内存超限（规则 {}, 估算 {}B / 上限 {}B, spill {}{}）——拒绝新建键桶, 已有桶继续累积; 累计拒收 {} 行（新桶尝试）",
                    self.rule_name,
                    self.mem_used_bytes(),
                    limit,
                    if self.spill.is_some() {
                        "已满/失败"
                    } else {
                        "未启用"
                    },
                    self.spill_limit_bytes
                        .map(|b| format!(" 落盘 {}/{}B", self.spill_used_bytes(), b))
                        .unwrap_or_default(),
                    self.over_limit_new_buckets
                );
            }
            return false;
        }
        self.estimated_bytes += allowance;
        self.mem_add(allowance);
        true
    }

    /// 已落盘字节（共享计数器读值；诊断/告警）。
    fn spill_used_bytes(&self) -> u64 {
        self.spill_used
            .as_ref()
            .map(|u| u.load(std::sync::atomic::Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// 注入状态外溢存储（窗口开始时由 spawn 层调用）。
    /// `store = None` 关闭 spill（Noop 语义）。`max_spill_bytes = None` 不限落盘。
    /// `spill_used` = 规则级共享落盘计数器（同规则全部分片共用一个——
    /// `max_disk` 是规则总上限, 分片数是引擎内部细节）。
    pub fn set_spill(
        &mut self,
        store: Option<Box<dyn SpillStore + Send + Sync>>,
        max_spill_bytes: Option<usize>,
        spill_used: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) {
        self.spill = store;
        self.spill_limit_bytes = max_spill_bytes.map(|b| b as u64);
        // 未注入规则级共享计数（测试/直接调用）但启用了 store → 自建本片独立
        // 计数：预算检查/记账口径不变（共享语义退化为单片, 与旧行为一致）。
        if self.spill.is_some() && spill_used.is_none() {
            self.spill_used = Some(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)));
        } else {
            self.spill_used = spill_used;
        }
        self.spill_index.clear();
        self.spill_failed = false;
        self.spill_warned = false;
        self.clock.clear();
        self.readback.clear();
    }

    /// 累计驱逐键数（跨窗口；抖动观测——驱逐多 + 读回多 = 抖动）。
    pub fn spill_evictions(&self) -> u64 {
        self.spill_evictions
    }

    /// 当前桶数（诊断）。
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// 驱逐分段耗时（跨窗口累计；性能定位）：(扫描 ns, clone ns, 写 ns, 调用次数)。
    /// 累计读回次数（跨窗口；抖动观测）。
    pub fn spill_readbacks(&self) -> u64 {
        self.spill_readbacks
    }

    /// 驱逐分段耗时（跨窗口累计；性能定位）：(扫描 ns, clone ns, 写 ns, 调用次数)。
    pub fn spill_profile_ns(&self) -> (u64, u64, u64, u64) {
        (
            self.spill_scan_ns,
            self.spill_clone_ns,
            self.spill_write_ns,
            self.spill_evict_calls,
        )
    }

    /// 实际内存字节（诊断/估算校准）：遍历桶表计算真实驻留（含树/盒堆、
    /// 字符串、HashMap 条目），与 `estimated_bytes`（bucket_allowance 估算）
    /// 对比，校准驱逐水位（估算低估 → 实际超预算才驱逐）。
    pub fn actual_bytes(&self) -> u64 {
        fn scope_key_bytes(key: &ScopeKey) -> u64 {
            match key {
                ScopeKey::Empty => 16,
                ScopeKey::Int(_) | ScopeKey::Float(_) => 24,
                ScopeKey::Str(s) => 40 + s.len() as u64,
                ScopeKey::Pair(a, b) => 24 + scope_key_bytes(a) + scope_key_bytes(b),
            }
        }
        fn row_fields_bytes(rf: &RowFields) -> u64 {
            // 固定：struct 头(Arc 8 + 3 Box 8) + 4 Box 头(16×4) ≈ 112
            112 + rf.numeric().len() as u64 * 8
                + rf.strings().len() as u64 * 24 // SmolStr 内联
                + rf.others().len() as u64 * 56
                + rf.null_mask().len() as u64 * 8
        }
        let mut total = 0u64;
        for chain in self.buckets.values() {
            for b in chain {
                // StatsBucket: scope_key + touch + accs Vec 头 + 对齐
                total += scope_key_bytes(&b.scope_key) + 64;
                for acc in &b.accs {
                    total += match acc {
                        StatsAccum::Numeric(_) => 8 + 48,
                        StatsAccum::Last(Some(rf)) => 16 + row_fields_bytes(rf),
                        StatsAccum::Last(None) => 16,
                        StatsAccum::Distinct(_) => 8 + 96,
                        StatsAccum::Top(t) => 24 + t.len() as u64 * 176,
                    };
                }
            }
            total += 24; // hashbrown 条目（hash + 值 + ctrl）
        }
        total
    }

    /// 驱逐最老未更新键到 spill（clock 二次机会近似 LRU），直到内存预算降到
    /// `min(上限-单桶, 上限 90%)`（滞后带：既要放得下新桶，又避免每新键都驱逐）。
    ///
    /// 落盘上限已满/写失败 → 置 `spill_failed`, 由 `account_new_bucket` 落到
    /// 拒收兜底（不丢内存键）。
    fn evict_to_spill(&mut self, plan: &StatsPlan) {
        let Some(limit) = self.limit_bytes else {
            return;
        };
        // 落盘上限已到（规则级共享计数）→ 停止驱逐（拒收兜底）。
        if let Some(sl) = self.spill_limit_bytes
            && self.spill_used_bytes() >= sl
        {
            self.warn_spill_full();
            return;
        }
        let allowance = Self::bucket_allowance(plan);
        // 驱逐目标: 上限-单桶 与 上限 90% 取小（前者保证新桶放得下）。
        let target = limit
            .saturating_sub(allowance)
            .min(limit.saturating_mul(9) / 10);
        let mut batch: Vec<(u64, ScopeKey, Vec<StatsAccum>)> = Vec::new();
        let mut batch_hashes: Vec<u64> = Vec::new();
        // 扫描期待驱逐字节（est 只在落盘后扣——循环条件用 est - pending）。
        let mut pending = 0u64;
        // 防活锁: 最多扫 (TOUCH_MAX+2)× 时钟长度（全活跃时停止——拒收兜底正确）。
        let max_scan = self.clock.len().saturating_mul(TOUCH_MAX as usize + 2);
        let mut scanned = 0usize;
        let mut scan_ns = 0u64;
        let mut clone_ns = 0u64;
        while self.mem_used_bytes().saturating_sub(pending) > target && scanned < max_scan {
            let s0 = std::time::Instant::now();
            let Some(h) = self.clock.pop_front() else {
                break;
            };
            scanned += 1;
            let Some(chain) = self.buckets.get_mut(&h) else {
                continue; // 链已不在（close 分批取走）
            };
            // 二次机会计数（M5-2）: 本轮所有桶 touch 递减 1（命中已刷新回 MAX），
            // 全 0 才驱逐——活跃键多轮保护，死键 MAX 轮内自然衰减。
            if chain.iter().any(|b| b.touch > 0) {
                for b in chain.iter_mut() {
                    b.touch = b.touch.saturating_sub(1);
                }
                self.clock.push_back(h);
                scan_ns += s0.elapsed().as_nanos() as u64;
                continue;
            }
            // 驱逐整链（先 clone 进 batch；落盘成功后才从桶表移除——写失败
            // 不丢内存键）。
            let c0 = std::time::Instant::now();
            pending += allowance * chain.len() as u64;
            for b in chain.iter() {
                batch.push((h, b.scope_key.clone(), b.accs.clone()));
            }
            batch_hashes.push(h);
            scan_ns += s0.elapsed().as_nanos() as u64;
            clone_ns += c0.elapsed().as_nanos() as u64;
        }
        if batch.is_empty() {
            return;
        }
        // 落盘预算检查（写入前）：超出则拒收兜底（规则级共享计数）。
        let add_bytes = batch_hashes.len() as u64 * allowance;
        if let Some(sl) = self.spill_limit_bytes
            && self.spill_used_bytes() + add_bytes > sl
        {
            self.warn_spill_full();
            return;
        }
        // 单事务批量写。
        let w0 = std::time::Instant::now();
        let result = self
            .spill
            .as_mut()
            .expect("调用方已确认 spill 启用")
            .put_batch(batch);
        self.spill_write_ns += w0.elapsed().as_nanos() as u64;
        self.spill_scan_ns += scan_ns;
        self.spill_clone_ns += clone_ns;
        self.spill_evict_calls += 1;
        if let Err(e) = result {
            // 写失败（磁盘满/IO）→ 回退拒收（§5 三层阶梯兜底），不丢内存键。
            self.spill_failed = true;
            log::error!("spill 写失败(规则 {}): {e}——回退拒收新键", self.rule_name);
            return;
        }
        // 落盘成功 → 从桶表移除 + 记账（内存/spill 不相交不变量成立；落盘字节
        // 记入规则级共享计数, 内存占用从共享计数扣减）。
        for h in &batch_hashes {
            if let Some(chain) = self.buckets.remove(h) {
                let n = chain.len() as u64;
                self.estimated_bytes = self.estimated_bytes.saturating_sub(allowance * n);
                self.mem_sub(allowance * n);
                if let Some(u) = &self.spill_used {
                    u.fetch_add(allowance * n, std::sync::atomic::Ordering::SeqCst);
                }
            }
            self.spill_index.insert(*h);
            self.readback.remove(h); // 键已回 redb（覆盖旧条目）——close 不再过滤它
        }
        self.spill_evictions += batch_hashes.len() as u64;
    }

    /// 落盘满/失败告警（每窗口一次）。
    fn warn_spill_full(&mut self) {
        if self.spill_warned {
            return;
        }
        self.spill_warned = true;
        log::warn!(
            "spill 落盘上限/写失败（规则 {}, 已落盘 {}B / 上限 {:?}）——停止驱逐, 回退拒收新键",
            self.rule_name,
            self.spill_used_bytes(),
            self.spill_limit_bytes
        );
    }

    /// close 前把 spill 全部并入内存桶。**take 只读化（M5-2）后**：redb 中
    /// 仍有已读回键的旧条目——drain 后按 `readback` 集合过滤（内存副本更新，
    /// 每个键恰好一次）。并入后走原有 `take_buckets` / `take_buckets_up_to` 路径。
    fn merge_spill_into_buckets(&mut self, plan: &StatsPlan) {
        let Some(spill) = &mut self.spill else { return };
        if spill.is_empty() {
            return;
        }
        let drained = spill.drain();
        self.spill_index.clear();
        self.clock.clear();
        // 规则级共享计数（2026-08-27）：本窗落盘的键随 close 并入内存 → 从共享
        // 计数扣减（每键 allowance 与驱逐记账同口径）。readback 键在读回时已扣过
        // （redb 残留旧条目, 此处跳过）——只扣实际并入的键。
        let mut merged_n = 0u64;
        for (key, accs) in drained {
            let hash = scope_key_hash(&key);
            if self.readback.contains(&hash) {
                continue; // 已读回且在内存（副本更新）——跳过 redb 旧条目
            }
            merged_n += 1;
            let chain = self
                .buckets
                .entry(hash)
                .or_insert_with(|| Vec::with_capacity(1));
            // 不变量: 非 readback 的 spill 键与内存不相交——但防御性合并。
            match chain.iter_mut().find(|b| b.scope_key == key) {
                Some(existing) => {
                    for (t, o) in existing.accs.iter_mut().zip(accs.iter()) {
                        merge_accum(t, o);
                    }
                }
                None => chain.push(StatsBucket {
                    scope_key: key,
                    accs,
                    touch: TOUCH_MAX,
                }),
            }
        }
        if let Some(u) = &self.spill_used {
            u.fetch_sub(
                merged_n * Self::bucket_allowance(plan),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        self.readback.clear();
    }
    /// 分批读回 spill 键（流式 close, M5-3）：store 游标续读 + readback 过滤
    /// （take 只读后 redb 残留旧条目, 内存副本更新——跳过）。批内顺序无要求
    /// （调用方排序）。
    fn spill_drain_up_to(
        &mut self,
        n: usize,
        plan: &StatsPlan,
    ) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        let Some(spill) = &mut self.spill else {
            return Vec::new();
        };
        let drained = spill.drain_up_to(n);
        let mut out = Vec::with_capacity(drained.len());
        for (key, accs) in drained {
            let hash = scope_key_hash(&key);
            if self.readback.contains(&hash) {
                continue; // 已读回且在内存（副本更新）——跳过 redb 旧条目
            }
            out.push((key, accs));
        }
        // 共享计数扣减：只扣实际读回的键（readback 键已在读回时扣过）。
        if let Some(u) = &self.spill_used {
            u.fetch_sub(
                out.len() as u64 * Self::bucket_allowance(plan),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        out
    }
}

/// 时钟二次机会计数上限（M5-2）：命中置此值，驱逐扫描递减到 0 才驱逐。
const TOUCH_MAX: u8 = 3;

/// 流式 close drain 批大小上限（M5-3）：默认 5 万键/批（≈35MB 反序列化驻留）。
/// `WF_SPILL_DRAIN_CHUNK` 可调。与输出 chunk 解耦——输出批大无妨, 读回批
/// 必须小（q18 30M close 峰值 43GB → 22GB 的直接原因）。
const SPILL_DRAIN_CHUNK: usize = 50_000;

/// 单桶: 完整 [`ScopeKey`]（close 排序/输出; 每桶一次构建）+ 累加器数组。
#[derive(Debug, Clone)]
pub struct StatsBucket {
    pub scope_key: ScopeKey,
    pub accs: Vec<StatsAccum>,
    /// 时钟（clock）算法的二次机会计数（M5-2：bool → u8，0..=TOUCH_MAX）。
    /// 命中置 TOUCH_MAX，驱逐扫描递减，到 0 才驱逐——活跃键最多保 3 轮
    /// （q18 每键回访 3.4 次，1 位二次机会过早踢掉活跃键致驱逐-回访抖动）。
    /// spill 未启用时恒 0，零开销。
    touch: u8,
}

impl StatsWindowState {
    /// 预建空键单桶（`ScopeKey::Empty`）——哈希路径 `bucket_mut(&Empty)` 命中。
    fn seed_empty_bucket(buckets: &mut EngineHashMap<u64, Vec<StatsBucket>>, plan: &StatsPlan) {
        buckets.insert(
            scope_key_hash(&ScopeKey::Empty),
            vec![StatsBucket {
                scope_key: ScopeKey::Empty,
                accs: StatsAccum::accs_for_plan(plan),
                touch: 0,
            }],
        );
    }

    /// 取/建一个桶（完整键路径: 行式回退 / 空键规则用）。哈希与列式
    /// `keyed_bucket_mut` 同值, 链内按 ScopeKey 完整比较消歧。
    /// 新桶先过限额检查（超限 → spill 腾空间 → 仍超限 → None, 调用方跳过
    /// 该行——内存有界）。
    fn bucket_mut(&mut self, key: &ScopeKey, plan: &StatsPlan) -> Option<&mut Vec<StatsAccum>> {
        let hash = scope_key_hash(key);
        // 先只读查找（entry 可变借用会与限额记账的 &mut self 冲突）。
        let pos = self
            .buckets
            .get(&hash)
            .and_then(|chain| chain.iter().position(|b| &b.scope_key == key));
        if let Some(i) = pos {
            let chain = self.buckets.get_mut(&hash).expect("命中即存在");
            chain[i].touch = TOUCH_MAX; // 刷新时钟二次机会计数
            return Some(&mut chain[i].accs);
        }
        // 未命中: 键可能已 spill → 读回（take 只读，M5-2）。
        if self.spill_index.contains(&hash) {
            let taken = self
                .spill
                .as_mut()
                .expect("spill_index 非空即有 store")
                .take(hash)
                .unwrap_or_else(|| {
                    panic!("spill 索引与存储不一致(致命): hash {hash:#x} 索引在但 take 空")
                });
            if taken.0 != *key {
                // hash 碰撞且非同一键：致命（绝不静默丢键）。
                panic!("spill hash 碰撞(致命): {hash:#x} 键不匹配");
            }
            return Some(self.readback_bucket_mut(hash, taken, plan));
        }
        if !self.account_new_bucket(plan) {
            return None;
        }
        // 链 Vec 容量精确 1（2026-08-26 q18 状态 2.3× 归因）：`or_default()`
        // 空 Vec push 1 个后 capacity=4（Rust 标准库 0→4 起步）→ 每链占 4 桶
        // 容量（192B）实装 1 桶（48B）——q18 每键独立 hash（链均长 1.0）→
        // 2935 万链 × 144B ≈ 4.2G 纯浪费。`with_capacity(1)` 精确 1 桶。
        let chain = self
            .buckets
            .entry(hash)
            .or_insert_with(|| Vec::with_capacity(1));
        chain.push(StatsBucket {
            scope_key: key.clone(),
            accs: StatsAccum::accs_for_plan(plan),
            touch: 0,
        });
        self.clock.push_back(hash); // 创建序入队（队尾 = 最新）
        Some(&mut chain.last_mut().expect("just pushed").accs)
    }

    /// 读回已 spill 的键并放回内存（take 只读，M5-2）：
    /// 出 `spill_index` 入 `readback`（close 按此过滤 redb 旧条目）；入账
    /// allowance；超限先驱逐最老键（此刻未借用 chain）；建桶 touch=TOUCH_MAX。
    fn readback_bucket_mut(
        &mut self,
        hash: u64,
        taken: (ScopeKey, Vec<StatsAccum>),
        plan: &StatsPlan,
    ) -> &mut Vec<StatsAccum> {
        self.spill_index.remove(&hash);
        self.readback.insert(hash);
        self.spill_readbacks += 1;
        if let Some(u) = &self.spill_used {
            u.fetch_sub(
                Self::bucket_allowance(plan),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        self.estimated_bytes += Self::bucket_allowance(plan);
        self.mem_add(Self::bucket_allowance(plan));
        if let Some(limit) = self.limit_bytes
            && self.mem_used_bytes() > limit
        {
            self.evict_to_spill(plan);
        }
        let chain = self
            .buckets
            .entry(hash)
            .or_insert_with(|| Vec::with_capacity(1));
        chain.push(StatsBucket {
            scope_key: taken.0,
            accs: taken.1,
            touch: TOUCH_MAX,
        });
        self.clock.push_back(hash);
        &mut chain.last_mut().expect("just pushed").accs
    }

    /// 取/建一个桶（列式扁平键路径）: `hash` = 叶数组哈希, `comps` = 栈上叶
    /// 数组（列序）。链内按 `comps` 与完整键比较消歧; 未命中时构建完整键
    /// （每桶一次）。新桶先过限额检查（超限 → spill 腾空间 → 仍超限 → None）。
    fn keyed_bucket_mut(
        &mut self,
        hash: u64,
        comps: &[ScopeKey],
        plan: &StatsPlan,
    ) -> Option<&mut Vec<StatsAccum>> {
        let pos = self.buckets.get(&hash).and_then(|chain| {
            chain
                .iter()
                .position(|b| comps_match(&b.scope_key, comps, 0, comps.len()))
        });
        if let Some(i) = pos {
            let chain = self.buckets.get_mut(&hash).expect("命中即存在");
            chain[i].touch = TOUCH_MAX; // 刷新时钟二次机会计数
            return Some(&mut chain[i].accs);
        }
        // 未命中: 键可能已 spill → 读回（take 只读，M5-2）。
        if self.spill_index.contains(&hash) {
            let taken = self
                .spill
                .as_mut()
                .expect("spill_index 非空即有 store")
                .take(hash)
                .unwrap_or_else(|| {
                    panic!("spill 索引与存储不一致(致命): hash {hash:#x} 索引在但 take 空")
                });
            if !comps_match(&taken.0, comps, 0, comps.len()) {
                // hash 碰撞且非同一键：致命（绝不静默丢键）。
                panic!("spill hash 碰撞(致命): {hash:#x} 键不匹配");
            }
            return Some(self.readback_bucket_mut(hash, taken, plan));
        }
        if !self.account_new_bucket(plan) {
            return None;
        }
        let chain = self
            .buckets
            .entry(hash)
            .or_insert_with(|| Vec::with_capacity(1));
        let scope_key = scope_key_from_comps(comps);
        chain.push(StatsBucket {
            scope_key,
            accs: StatsAccum::accs_for_plan(plan),
            touch: 0,
        });
        self.clock.push_back(hash); // 创建序入队（队尾 = 最新）
        Some(&mut chain.last_mut().expect("just pushed").accs)
    }

    /// 按完整键取桶（测试/调试用; 生产走哈希路径）。
    pub fn find_bucket(&self, key: &ScopeKey) -> Option<&Vec<StatsAccum>> {
        self.buckets
            .get(&scope_key_hash(key))
            .and_then(|chain| chain.iter().find(|b| &b.scope_key == key))
            .map(|b| &b.accs)
    }

    /// 清空并拍平全部桶（close 用）: `(ScopeKey, accs)` 按 ScopeKey 升序。
    /// 同时清零内存账本（新窗口重新累积; 拒收计数保留——指标用）。
    /// spill 启用时先并入 spill 键（每个键恰好一次, 见 [`Self::merge_spill_into_buckets`]）。
    fn take_buckets(&mut self, plan: &StatsPlan) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        self.merge_spill_into_buckets(plan);
        let mut out: Vec<(ScopeKey, Vec<StatsAccum>)> = std::mem::take(&mut self.buckets)
            .into_values()
            .flat_map(|chain| chain.into_iter().map(|b| (b.scope_key, b.accs)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        // 本片账本清零 → 共享计数同步扣减（本片净占用 == estimated_bytes——
        // 每次增减都已同步; 扣完预算随窗口释放可复用）。
        self.mem_sub(self.estimated_bytes);
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
    /// 待创建 redb spill store（M4）：路径 + 落盘上限 + 规则级共享计数。延迟到
    /// 首次 `process_*`（行字段 layout 解析后）创建——store 的 layout 必须与
    /// executor 一致。窗口 reset 后保留（下一窗口沿用同路径, create 语义重建文件）。
    spill_redb: Option<(
        std::path::PathBuf,
        Option<usize>,
        Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    )>,
    /// 规则级共享内存占用计数（`max_memory` = 规则总驻留上限, 分片数是引擎
    /// 内部细节; None = 未配置共享 → 本片独立预算）。reset_window 用它恢复
    /// 新窗口的共享计数（与 `spill_redb` 同模式, 跨窗口保留）。
    mem_used_shared: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
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
        // 空键规则预建 Empty 桶（快路径; 带 key 惰性建桶）。
        let mut buckets = EngineHashMap::default();
        if plan.keys.is_empty() {
            StatsWindowState::seed_empty_bucket(&mut buckets, &plan);
        }
        Self {
            plan,
            window: StatsWindowState::new(buckets),
            watermark_nanos: 0,
            unique_wheres,
            measure_where,
            row_field_names,
            measure_field_idx,
            row_field_layout: None,
            spill_redb: None,
            mem_used_shared: None,
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
        // 延迟创建 redb spill store（layout 已确定——行式 all_other 或列式缓存）。
        self.ensure_spill_store();
        // where 结果缓存: 行间复用 buffer（无逐行分配）; 无 where 规则时保持空。
        let mut where_ok: Vec<bool> = Vec::with_capacity(self.unique_wheres.len());
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
            for (idx, measure) in self.plan.measures.iter().enumerate() {
                if let Some(wi) = self.measure_where[idx]
                    && !where_ok[wi]
                {
                    continue;
                }
                let acc = &mut bucket[idx];
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
                    measure_values(&self.plan, &b.accs, &self.measure_field_idx),
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
        let buckets = self.window.take_buckets(&self.plan);
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
        buckets: Vec<(ScopeKey, Vec<StatsAccum>)>,
    ) -> Vec<StatsCloseBucket> {
        buckets
            .into_iter()
            .map(|(key, accs)| StatsCloseBucket {
                key,
                measures: self
                    .plan
                    .measures
                    .iter()
                    .zip(accs.iter())
                    .zip(self.measure_field_idx.iter())
                    .map(|((m, acc), fidx)| bucket_measure_entries(m, acc, *fidx))
                    .collect(),
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
        buckets: Vec<(ScopeKey, Vec<StatsAccum>)>,
    ) -> Vec<(ScopeKey, Vec<f64>)> {
        buckets
            .into_iter()
            .map(|(key, accs)| {
                (
                    key,
                    measure_values(&self.plan, &accs, &self.measure_field_idx),
                )
            })
            .collect()
    }

    /// 分批取内存桶（流式 close 的一部分）: 从桶表取最多 n 个链并移除（链内桶
    /// 拍平）, 批内 ScopeKey 升序; 全部取完（返回空）后调用方须
    /// [`Self::finish_close_window`]。不 reset（还有剩余桶, 下一批继续）。
    ///
    /// **M5-3**：不再并入 spill（流式 close 用 [`Self::take_next_close_batch`]
    /// 从内存 + spill 两源归并取桶——避免 close 全量 drain 的内存峰值）。
    /// 2026-08-26 review: 用 `retain` 原地移除已取链——v1 用 `mem::take` 全表 +
    /// 剩余重插新 HashMap（每批 O(剩余) 哈希 + 分配, 100M 30 批 ≈ 4.4 亿次重插
    /// close +~9s）; retain 每批 O(n) 轻量回调（无哈希重建, close ~3s）。
    pub fn take_buckets_up_to(&mut self, n: usize) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
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

    /// **流式 close 取桶（M5-3）**：从内存桶（预算内, 小）与 spill（游标续读,
    /// 分批）两源各取一批, 归并排序后返回（批内 ScopeKey 升序——对拍契约）。
    /// 两源都空 → 返回空（close 循环终止）。close 峰值 = 批大小, 不再全量
    /// drain 到内存（q18 30M 曾 43GB → swap 风暴挂死）。
    ///
    /// 批大小 clamp 到 [`SPILL_DRAIN_CHUNK`]（默认 5 万, `WF_SPILL_DRAIN_CHUNK`
    /// 可调）——与输出 `emit_chunk`（默认 100 万）解耦: 输出批大没关系,
    /// 但**从 redb 读回的批必须小**（反序列化驻留是 close 内存峰值的直接来源）。
    pub fn take_next_close_batch(&mut self, n: usize) -> Vec<(ScopeKey, Vec<StatsAccum>)> {
        let n = n.min(SPILL_DRAIN_CHUNK);
        let mut out = Vec::with_capacity(n);
        if n == 0 {
            return out;
        }
        let mem = self.take_buckets_up_to(n);
        // spill 批补足配额（两源之和 ≤ n）; 批内排序后与内存批归并。
        let spill_n = n.saturating_sub(mem.len()).max(1);
        let mut spill = self.window.spill_drain_up_to(spill_n, &self.plan);
        spill.sort_by(|a, b| a.0.cmp(&b.0));
        // 归并两个有序序列（peek 比较 + next 取走, 无 clone）。
        let mut mem_iter = mem.into_iter().peekable();
        let mut spill_iter = spill.into_iter().peekable();
        loop {
            match (mem_iter.peek(), spill_iter.peek()) {
                (Some(x), Some(y)) => {
                    if x.0 <= y.0 {
                        out.push(mem_iter.next().expect("peek 即存在"));
                    } else {
                        out.push(spill_iter.next().expect("peek 即存在"));
                    }
                }
                (Some(_), None) => out.push(mem_iter.next().expect("peek 即存在")),
                (None, Some(_)) => out.push(spill_iter.next().expect("peek 即存在")),
                (None, None) => break,
            }
        }
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
        // 状态内存估算 vs 实际（诊断，2026-08-27 q18 RSS 校准）：bucket_allowance
        // 估算（estimated_bytes）与 actual_bytes 对比——低估则实际超预算才驱逐。
        log::info!(
            "stats 状态内存(规则 {}): 估算 {:>9.1}MB / 实际 {:>9.1}MB / 桶 {}（估算每键 {:.0}B）",
            self.window.rule_name,
            self.window.estimated_bytes as f64 / 1e6,
            self.window.actual_bytes() as f64 / 1e6,
            self.window.buckets.len(),
            if self.window.buckets.is_empty() {
                0.0
            } else {
                self.window.estimated_bytes as f64 / self.window.buckets.len() as f64
            },
        );
        // cleanup 旧 spill（窗口文件删除）——新窗口由 spawn 层重新注入 store。
        if let Some(spill) = &mut self.window.spill {
            spill.cleanup();
        }
        // 释放本窗占用的共享内存计数（流式 close 分批取桶但账本未递减;
        // take_buckets 路径已扣, 此处为 0 幂等）。预算随窗口释放可复用。
        self.window.mem_sub(self.window.estimated_bytes);
        let mut buckets = EngineHashMap::default();
        if self.plan.keys.is_empty() {
            StatsWindowState::seed_empty_bucket(&mut buckets, &self.plan);
        }
        let limit = self.window.limit_bytes;
        let rule_name = self.window.rule_name.clone();
        let over_limit = self.window.over_limit_new_buckets;
        let spill_evictions = self.window.spill_evictions;
        let spill_readbacks = self.window.spill_readbacks;
        let spill_scan_ns = self.window.spill_scan_ns;
        let spill_clone_ns = self.window.spill_clone_ns;
        let spill_write_ns = self.window.spill_write_ns;
        let spill_evict_calls = self.window.spill_evict_calls;
        self.window = StatsWindowState::new(buckets);
        // 保留限额配置 + 规则级共享内存计数（executor 字段持有）+ 拒收/抖动
        // 计数跨窗口（guard 持续生效; 计数供指标/告警）。spill 不跨窗口（store
        // 已 cleanup 丢弃, 文件删除; 共享落盘计数由 ensure_spill_store 重新注入）。
        self.window.set_memory_limit_shared(
            &rule_name,
            limit.map(|b| b as usize),
            self.mem_used_shared.clone(),
        );
        self.window.over_limit_new_buckets = over_limit;
        self.window.spill_evictions = spill_evictions;
        self.window.spill_readbacks = spill_readbacks;
        self.window.spill_scan_ns = spill_scan_ns;
        self.window.spill_clone_ns = spill_clone_ns;
        self.window.spill_write_ns = spill_write_ns;
        self.window.spill_evict_calls = spill_evict_calls;
    }

    /// 注入状态内存上限（字节; None = 不设防）——超限拒收新键桶, 已有桶继续。
    /// 未注入共享计数 → 本片独立预算（测试/单片, 与旧行为一致）。
    pub fn set_memory_limit(&mut self, rule_name: &str, bytes: Option<usize>) {
        self.set_memory_limit_shared(rule_name, bytes, None);
    }

    /// 注入状态内存上限（规则级共享版）：`max_memory` = 规则总驻留上限——同
    /// 规则全部分片共用一个内存占用计数（spawn 层规则级创建, 分片 clone 注入）。
    pub fn set_memory_limit_shared(
        &mut self,
        rule_name: &str,
        bytes: Option<usize>,
        mem_used_shared: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) {
        self.mem_used_shared = mem_used_shared;
        self.window
            .set_memory_limit_shared(rule_name, bytes, self.mem_used_shared.clone());
    }

    /// 注入状态外溢存储（M3; 窗口开始时调用; None = 关闭 spill）。
    /// `max_spill_bytes` = 落盘上限（None = 不限; 三层预算阶梯第二层）。
    /// `spill_used` = 规则级共享落盘计数（同规则全部分片共用一个——
    /// `max_disk` 是规则总上限, 分片数是引擎内部细节; None = 未配置）。
    pub fn set_spill(
        &mut self,
        store: Option<Box<dyn SpillStore + Send + Sync>>,
        max_spill_bytes: Option<usize>,
        spill_used: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) {
        self.spill_redb = None;
        self.window.set_spill(store, max_spill_bytes, spill_used);
    }

    /// 便捷：redb spill（M4, `limits { spill = "redb" }`）——记录待创建配置,
    /// **延迟到首次 `process_*`**（行字段 layout 解析后）创建 store：
    /// store 的 layout 必须与 executor 一致（列式 from_schema / 行式 all_other）。
    /// `max_spill_bytes` = 落盘上限（None = 不限）。
    /// `spill_used` = 规则级共享落盘计数（见 [`Self::set_spill`]）。
    pub fn set_spill_redb(
        &mut self,
        path: impl AsRef<std::path::Path>,
        max_spill_bytes: Option<usize>,
        spill_used: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
    ) {
        self.spill_redb = Some((path.as_ref().to_path_buf(), max_spill_bytes, spill_used));
    }

    /// 延迟创建 redb store（首次 process 时调用; layout 已解析）。
    /// 创建失败 = 致命（panic——配置/磁盘错误, 绝不静默降级为拒收）。
    fn ensure_spill_store(&mut self) {
        let Some((path, max_spill_bytes, spill_used)) = self.spill_redb.clone() else {
            return;
        };
        if self.window.spill.is_some() {
            return; // 已创建
        }
        let layout = match &self.row_field_layout {
            Some(l) => std::sync::Arc::clone(l),
            // 行式路径（无列式 schema）：按行字段子集 all_other（与
            // `row_fields_layout_for_row` 同构; 生产恒有子集, 见其文档）。
            None => {
                let names: Vec<String> = self
                    .row_field_names
                    .clone()
                    .map(|ns| ns.as_ref().clone())
                    .unwrap_or_default();
                std::sync::Arc::new(RowFieldLayout::all_other(&names))
            }
        };
        let store = crate::match_engine::spill::RedbSpillStore::create(&path, layout)
            .unwrap_or_else(|e| panic!("spill redb 创建失败(致命) {}: {e}", path.display()));
        self.window
            .set_spill(Some(Box::new(store)), max_spill_bytes, spill_used);
    }

    /// 提取本片已关闭窗口的**原始累加状态**（输入分区分片归并用）并重置窗口。
    /// 返回 `(桶原始状态, 本片事件数)`——协调片把它合并进自己的窗口后再 close。
    /// 仅空键/可交换度量（count/sum/min/max/distinct）分片使用（last/top 被
    /// spawn 门控排除——行序敏感不可归并）。
    pub fn take_partial(&mut self) -> (Vec<(ScopeKey, Vec<StatsAccum>)>, u64) {
        let buckets = self.window.take_buckets(&self.plan);
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
    pub fn merge_partial(&mut self, buckets: Vec<(ScopeKey, Vec<StatsAccum>)>, event_count: u64) {
        for (key, accs) in buckets {
            // 超限（guard）→ 该片该键跳过（协调片侧同样受桶预算约束）。
            let Some(target) = self.window.bucket_mut(&key, &self.plan) else {
                continue;
            };
            for (t, o) in target.iter_mut().zip(accs.iter()) {
                merge_accum(t, o);
            }
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
        // 延迟创建 redb spill store：先解析行字段 layout（列式 from_schema），
        // 再建 store（layout 一致是读回正确性的前提）。幂等。
        self.ensure_row_field_layout(batch);
        self.ensure_spill_store();
        let n = batch.num_rows();
        // 行域（P2 分片）: `rows` = 本片拥有的行索引子集（绝对行号, 升序）。
        // 归并段（段 1d/段 2）改为**行域驱动**（count_domain/sum_domain/
        // insert_distinct_domain 只遍历本片行）——不再构建全批 domain mask。
        let view = ColumnarBatch::from_all_fields(batch);
        // 段 1: where 列式 mask（去重后唯一条件, 每批一次）
        let masks: Vec<BooleanArray> = self
            .unique_wheres
            .iter()
            .map(|e| eval_guard_columnar(e, &view))
            .collect();
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
            );
        }
        // 段 1d: 纯归并度量整列累加（**行域驱动**——2026-08-24 分片裁剪: 只遍历
        // 本片行, 消除每片对全批的 O(n) 冗余扫描; 全批路径 `rows=None` 行为不变）。
        // 行式语义: 满足 where 的行对**每个**度量都 `count += 1`（在字段读取前）
        // ——avg 的 count 必须与 sum 同步累加, 否则 avg = sum/count 输出 0（D6）。
        for (idx, measure) in self.plan.measures.iter().enumerate() {
            let wi = self.measure_where[idx];
            // 空键规则恒单桶（预建, 不参与限额——guard 只针对键空间膨胀）。
            let acc = &mut self
                .window
                .bucket_mut(&ScopeKey::Empty, &self.plan)
                .expect("Empty 桶恒存在")[idx];
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
                        minmax_domain(&col, rows, n, &masks, wi, &mut nacc.min, &mut nacc.max);
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
            let acc = &mut self
                .window
                .bucket_mut(&ScopeKey::Empty, &self.plan)
                .expect("Empty 桶恒存在")[idx];
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
    /// 字段读取走**原生列值**（`column_i128`/`column_distinct_key`）——与空键
    /// 列式段同精度（D7/D8: ≥2^53 的 Int64 不得经 `Value::Number(f64)` 舍入;
    /// Timestamp 列 distinct 也走原生 i64, 不得静默跳过）。
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
    ) -> bool {
        const MAX_STACK_KEYS: usize = 4;
        // 2026-08-26 q18/q19：行字段 layout（首次从 schema 构建并缓存）；
        // 在桶借用前取（ensure 需 &mut self）。
        let row_layout = self.ensure_row_field_layout(batch);
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
            accumulate_column_row(
                bucket,
                &self.plan,
                &self.measure_where,
                &self.measure_field_idx,
                row_names,
                row_field_cols,
                batch,
                masks,
                row,
                &row_layout,
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
        accumulate_column_row(
            bucket,
            &self.plan,
            &self.measure_where,
            &self.measure_field_idx,
            row_names,
            row_field_cols,
            batch,
            masks,
            row,
            &row_layout,
        );
        true
    }
}

/// 单行桶累加主体（列式路径; 自由函数——调用点持有 `&mut self.window` 桶借用,
/// 方法会整 self 借用冲突）。
///
/// last/top 行字段每行懒提取一次, 多度量共享同一 Arc（Q18 4 个 last 度量内存
/// 1 份; 提取列序 = row_names, 免整行 8 字段）。
#[allow(clippy::too_many_arguments)] // 单行桶累加: 桶/计划/掩码索引/行字段/列/行号 6 组参数
fn accumulate_column_row(
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
                let Some(field) = &measure.field else {
                    continue;
                };
                match measure.agg {
                    StatsAggPlan::Count => {}
                    StatsAggPlan::Sum | StatsAggPlan::Avg => {
                        if let Some(nn) = column_i128(batch, field_name(field), row) {
                            nacc.sum += nn;
                        }
                    }
                    StatsAggPlan::Min => {
                        if let Some(nn) = column_i128(batch, field_name(field), row) {
                            nacc.min = Some(match nacc.min {
                                Some(m) if m <= nn => m,
                                _ => nn,
                            });
                        }
                    }
                    StatsAggPlan::Max => {
                        if let Some(nn) = column_i128(batch, field_name(field), row) {
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
                if let Some(field) = &measure.field
                    && let Some(k) = column_distinct_key(batch, field_name(field), row)
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
fn column_i128(batch: &RecordBatch, name: &str, row: usize) -> Option<i128> {
    let idx = batch.schema().index_of(name).ok()?;
    let col = batch.column(idx);
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

/// 从 batch 列读单行原生 distinct 键（与列式段 `insert_distinct_column` 同类型
/// 分派, 原生值构造——D7: 禁止 `Value::Number(f64)` 化 ≥2^53 的 Int64）。
/// null / 类型不在支持集 → None（与行式 extract None 一致）。
fn column_distinct_key(batch: &RecordBatch, name: &str, row: usize) -> Option<DistinctKey> {
    use arrow::array::TimestampNanosecondArray;
    let idx = batch.schema().index_of(name).ok()?;
    let col = batch.column(idx);
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
