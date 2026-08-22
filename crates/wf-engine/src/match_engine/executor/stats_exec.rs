//! Stats executor — 声明式窗口统计执行器（P1: 空键 fixed count/distinct）。
//!
//! 与 CEP(RuleExecutor/match)完全正交: 消费 fanout 的 raw RecordBatch,
//! 内部维护可交换结合的归并状态, 窗口 close 时产出度量值并复用 alert 构建。
//!
//! 设计依据: docs/stats-executor-design.md v6（§6 执行器）。

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsPlan};

use crate::match_engine::Value;
use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};
use crate::match_engine::match_engine::Event;

// ---------------------------------------------------------------------------
// 状态结构（v6 §6.1 — 无匹配进度, 纯累加）
// ---------------------------------------------------------------------------

/// 单桶累加器。avg 不作状态——输出时 sum/count 求得（D6）。
#[derive(Debug, Clone, Default)]
pub struct StatsAccum {
    pub count: u64,
    pub sum_i128: i128,
    pub min: Option<i128>,
    pub max: Option<i128>,
    pub distinct_set: Option<HashSet<DistinctKey>>,
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
    pub fn from_str(s: &str) -> Self {
        DistinctKey::Str(s.into())
    }
}

// ---------------------------------------------------------------------------
// StatsExecutor — 执行状态
// ---------------------------------------------------------------------------

/// 空键全局单桶的窗口状态（P1; 带 key 分桶为 P2）。
pub struct StatsWindowState {
    /// 度量值索引对齐 StatsPlan.measures。
    pub accum: Vec<StatsAccum>,
    pub window_start_nanos: i64,
    pub last_event_nanos: i64,
    pub event_count: u64,
}

impl StatsWindowState {
    fn new(n_measures: usize, window_start_nanos: i64) -> Self {
        Self {
            accum: vec![StatsAccum::default(); n_measures],
            window_start_nanos,
            last_event_nanos: 0,
            event_count: 0,
        }
    }
}

/// 执行器: 消费行/批次, 按 StatsPlan 归并, 窗口 close 时产出度量值。
pub struct StatsExecutor {
    pub plan: StatsPlan,
    /// 空键单窗口（P1）; P2 为 HashMap<ScopeKey, StatsWindowState>。
    pub window: StatsWindowState,
    /// 当前窗口的过期上界（水印推进, 触发 close）。
    pub watermark_nanos: i64,
    /// 去重后的 where 表达式（相同条件共享一次求值——q15 9 个度量 where → 3
    /// 个唯一条件; 行式实现的关键优化: 每行 1 次 Event 构建 + n_unique 次 eval）。
    unique_wheres: Vec<Expr>,
    /// 每度量对应 `unique_wheres` 的索引; `None` = 无条件度量（恒通过）。
    measure_where: Vec<Option<usize>>,
}

impl StatsExecutor {
    pub fn new(plan: StatsPlan) -> Self {
        // 预计算 where 去重映射: 度量 → 唯一条件索引。Expr 纯函数无副作用,
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
        let n = plan.measures.len();
        Self {
            plan,
            window: StatsWindowState::new(n, 0),
            watermark_nanos: 0,
            unique_wheres,
            measure_where,
        }
    }

    /// 处理一批行（row-based; 列式段为 P1.5）。
    ///
    /// `extract(row, name) -> Option<Value>`: 由调用方提供行字段读取。
    ///
    /// where 过滤**内建求值**: 每行构建 1 次 ctx Event, 对去重后的唯一 where
    /// 表达式求值一次（结果共享给所有同条件度量）, 不再依赖调用方注入。
    /// 三值语义与 CEP `where_ok` 一致（eval 非 `Bool(true)` 即过滤）。
    pub fn process_rows<F>(&mut self, rows: &[HashMap<String, Value>], extract: F)
    where
        F: Fn(&HashMap<String, Value>, &str) -> Option<Value>,
    {
        // where 结果缓存: 行间复用 buffer（无逐行分配）; 无 where 规则时保持空。
        let mut where_ok: Vec<bool> = Vec::with_capacity(self.unique_wheres.len());
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
            for (idx, measure) in self.plan.measures.iter().enumerate() {
                if let Some(wi) = self.measure_where[idx]
                    && !where_ok[wi]
                {
                    continue;
                }
                let acc = &mut self.window.accum[idx];
                acc.count += 1;
                if let Some(field) = &measure.field {
                    if let Some(val) = extract(row, field_name(field)) {
                        match measure.agg {
                            StatsAggPlan::Count => {}
                            StatsAggPlan::Sum | StatsAggPlan::Avg => {
                                if let Some(n) = value_to_i128(&val) {
                                    acc.sum_i128 += n;
                                }
                            }
                            StatsAggPlan::Min => {
                                if let Some(n) = value_to_i128(&val) {
                                    acc.min = Some(match acc.min {
                                        Some(m) if m <= n => m,
                                        _ => n,
                                    });
                                }
                            }
                            StatsAggPlan::Max => {
                                if let Some(n) = value_to_i128(&val) {
                                    acc.max = Some(match acc.max {
                                        Some(m) if m >= n => m,
                                        _ => n,
                                    });
                                }
                            }
                            StatsAggPlan::DistinctCount => {
                                let key = value_to_distinct_key(&val);
                                acc.distinct_set
                                    .get_or_insert_with(HashSet::new)
                                    .insert(key);
                            }
                            StatsAggPlan::Last | StatsAggPlan::Top => {
                                // P1 不实现（Q18/Q19 扩展）
                            }
                        }
                    }
                }
            }
            self.window.event_count += 1;
        }
    }

    /// 计算最终度量值（close 时; avg 由 sum/count 求得）。
    pub fn final_measure_values(&self) -> Vec<f64> {
        self.plan
            .measures
            .iter()
            .zip(self.window.accum.iter())
            .map(|(m, acc)| match m.agg {
                StatsAggPlan::Count => acc.count as f64,
                StatsAggPlan::Sum => acc.sum_i128 as f64,
                StatsAggPlan::Avg => {
                    if acc.count == 0 {
                        0.0
                    } else {
                        acc.sum_i128 as f64 / acc.count as f64
                    }
                }
                StatsAggPlan::Min => acc.min.unwrap_or(0) as f64,
                StatsAggPlan::Max => acc.max.unwrap_or(0) as f64,
                StatsAggPlan::DistinctCount => {
                    acc.distinct_set.as_ref().map_or(0, HashSet::len) as f64
                }
                StatsAggPlan::Last | StatsAggPlan::Top => 0.0,
            })
            .collect()
    }

    /// 窗口 close: 冻结当前值, 清空状态。
    pub fn close_window(&mut self) -> Vec<f64> {
        let values = self.final_measure_values();
        self.window = StatsWindowState::new(self.plan.measures.len(), 0);
        values
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
        // 前置: 全部 where 表达式可列式化（eval_guard_columnar 对不可列式表达式
        // 返回全 false, 不可静默使用）。
        for e in &self.unique_wheres {
            if !wf_lang::columnar::expr_is_columnar(e) {
                return false;
            }
        }
        let n = batch.num_rows();
        let view = ColumnarBatch::from_all_fields(batch);
        // 段 1: where 列式 mask（去重后唯一条件, 每批一次）
        let masks: Vec<BooleanArray> = self
            .unique_wheres
            .iter()
            .map(|e| eval_guard_columnar(e, &view))
            .collect();
        // 段 1d: 纯归并度量整列累加
        for (idx, measure) in self.plan.measures.iter().enumerate() {
            let mask = self.measure_where[idx].map(|wi| &masks[wi]);
            let acc = &mut self.window.accum[idx];
            match measure.agg {
                StatsAggPlan::Count => {
                    acc.count += match mask {
                        Some(m) => count_true(m),
                        None => n as u64,
                    };
                }
                StatsAggPlan::Sum | StatsAggPlan::Avg => {
                    if let Some(field) = &measure.field
                        && let Some(col) = numeric_col(batch, field_name(field))
                    {
                        acc.sum_i128 += sum_masked(&col, mask);
                    }
                }
                StatsAggPlan::Min | StatsAggPlan::Max => {
                    if let Some(field) = &measure.field
                        && let Some(col) = numeric_col(batch, field_name(field))
                    {
                        minmax_masked(&col, mask, &mut acc.min, &mut acc.max);
                    }
                }
                StatsAggPlan::DistinctCount => {
                    // 段 2 处理
                }
                StatsAggPlan::Last | StatsAggPlan::Top => {
                    // P1 不实现（Q18/Q19 扩展）
                }
            }
        }
        // 段 2: distinct 行式段（原生列值按 mask 过滤插入）
        for (idx, measure) in self.plan.measures.iter().enumerate() {
            if !matches!(measure.agg, StatsAggPlan::DistinctCount) {
                continue;
            }
            let Some(field) = &measure.field else {
                continue;
            };
            let mask = self.measure_where[idx].map(|wi| &masks[wi]);
            let acc = &mut self.window.accum[idx];
            let set = acc.distinct_set.get_or_insert_with(HashSet::new);
            if !insert_distinct_column(batch, field_name(field), mask, set) {
                return false;
            }
        }
        self.window.event_count += n as u64;
        true
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

fn value_to_i128(v: &Value) -> Option<i128> {
    match v {
        Value::Number(n) => Some(*n as i128),
        _ => None,
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
// 列式段辅助（P1.5）
// ---------------------------------------------------------------------------

/// mask 中 true 的个数（含 null slot 读 false, 与行式 eval 语义一致）。
fn count_true(mask: &BooleanArray) -> u64 {
    (0..mask.len()).filter(|&i| mask.value(i)).count() as u64
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

/// 按 mask 过滤的列值迭代器（mask null slot / 列 null / 类型 mismatch 均跳过,
/// 与行式 `extract None` 语义一致）。
fn int_values<'a>(
    c: &'a Int64Array,
    mask: Option<&'a BooleanArray>,
) -> impl Iterator<Item = i64> + 'a {
    let len = c.len();
    (0..len).filter_map(move |i| {
        if let Some(m) = mask
            && !m.value(i)
        {
            return None;
        }
        if c.is_null(i) { None } else { Some(c.value(i)) }
    })
}

fn float_values<'a>(
    c: &'a Float64Array,
    mask: Option<&'a BooleanArray>,
) -> impl Iterator<Item = f64> + 'a {
    let len = c.len();
    (0..len).filter_map(move |i| {
        if let Some(m) = mask
            && !m.value(i)
        {
            return None;
        }
        if c.is_null(i) { None } else { Some(c.value(i)) }
    })
}

/// 按 mask 过滤求和（无 mask = 全列; null 跳过）。数值按行式 `value_to_i128`
/// 的 f64→i128 截断转 i128 累加（D8: 整数域, 不用 f64）。
fn sum_masked(col: &NumCol<'_>, mask: Option<&BooleanArray>) -> i128 {
    match col {
        NumCol::Int64(c) => int_values(c, mask).map(|v| v as i128).sum(),
        NumCol::Float64(c) => float_values(c, mask).map(|v| v as i128).sum(),
    }
}

/// 按 mask 过滤更新 min/max（null 跳过; 与行式一致）。
fn minmax_masked(
    col: &NumCol<'_>,
    mask: Option<&BooleanArray>,
    min: &mut Option<i128>,
    max: &mut Option<i128>,
) {
    let mut fold = |v: i128, min: &mut Option<i128>, max: &mut Option<i128>| {
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
            for v in int_values(c, mask) {
                fold(v as i128, min, max);
            }
        }
        NumCol::Float64(c) => {
            for v in float_values(c, mask) {
                fold(v as i128, min, max);
            }
        }
    }
}

/// distinct 列式插入。返回 `false` = 字段列类型不在支持集（调用方须回退行式）。
/// 支持集: Int64/Float64/Utf8/Bool/Timestamp(Ns)——与行式 `value_to_distinct_key`
/// 的 Number/Str/Bool 分派一致（Timestamp 为整数 nanos, 走 Int 域内, D7）。
fn insert_distinct_column(
    batch: &RecordBatch,
    name: &str,
    mask: Option<&BooleanArray>,
    set: &mut HashSet<DistinctKey>,
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
    if let Some(c) = col.as_any().downcast_ref::<Int64Array>() {
        for v in int_values(c, mask) {
            set.insert(DistinctKey::from_i64(v));
        }
        return true;
    }
    if let Some(c) = col.as_any().downcast_ref::<Float64Array>() {
        for v in float_values(c, mask) {
            set.insert(DistinctKey::from_f64(v));
        }
        return true;
    }
    if let Some(c) = col.as_any().downcast_ref::<StringArray>() {
        for v in str_values(c, mask) {
            set.insert(DistinctKey::from_str(v));
        }
        return true;
    }
    if let Some(c) = col.as_any().downcast_ref::<BooleanArray>() {
        for v in bool_values(c, mask) {
            set.insert(DistinctKey::from_f64(if v { 1.0 } else { 0.0 }));
        }
        return true;
    }
    // Timestamp(Ns): 整数 nanos → Int 域内（与行式 Number(f64) 的 from_f64 一致
    // 对 <2^53 整数; >2^53 走原生 i64, 即文档化的 D7 更准语义）。
    if let Some(c) = col
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
    {
        for v in ts_values(c, mask) {
            set.insert(DistinctKey::from_i64(v));
        }
        return true;
    }
    false
}

fn str_values<'a>(
    c: &'a StringArray,
    mask: Option<&'a BooleanArray>,
) -> impl Iterator<Item = &'a str> + 'a {
    let len = c.len();
    (0..len).filter_map(move |i| {
        if let Some(m) = mask
            && !m.value(i)
        {
            return None;
        }
        if c.is_null(i) { None } else { Some(c.value(i)) }
    })
}

fn bool_values<'a>(
    c: &'a BooleanArray,
    mask: Option<&'a BooleanArray>,
) -> impl Iterator<Item = bool> + 'a {
    let len = c.len();
    (0..len).filter_map(move |i| {
        if let Some(m) = mask
            && !m.value(i)
        {
            return None;
        }
        if c.is_null(i) { None } else { Some(c.value(i)) }
    })
}

fn ts_values<'a>(
    c: &'a arrow::array::TimestampNanosecondArray,
    mask: Option<&'a BooleanArray>,
) -> impl Iterator<Item = i64> + 'a {
    let len = c.len();
    (0..len).filter_map(move |i| {
        if let Some(m) = mask
            && !m.value(i)
        {
            return None;
        }
        if c.is_null(i) { None } else { Some(c.value(i)) }
    })
}
