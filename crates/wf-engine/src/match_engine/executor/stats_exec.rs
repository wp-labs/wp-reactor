//! Stats executor — 声明式窗口统计执行器（P1: 空键 fixed count/distinct）。
//!
//! 与 CEP(RuleExecutor/match)完全正交: 消费 fanout 的 raw RecordBatch,
//! 内部维护可交换结合的归并状态, 窗口 close 时产出度量值并复用 alert 构建。
//!
//! 设计依据: docs/stats-executor-design.md v6（§6 执行器）。

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsPlan};

use crate::match_engine::Value;
use crate::match_engine::columnar::{ColumnarBatch, eval_guard_columnar};
use crate::match_engine::match_engine::{Event, ScopeKey, field_ref_name};
use crate::window::scope_key_columnar;

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

/// 窗口状态: 桶 → 度量累加器数组（索引对齐 `StatsPlan.measures`）。
/// 空键规则恒单桶（`ScopeKey::Empty` 键, P1 快路径不变）; 带 key（P2）每
/// `(key 组合)` 一桶。
pub struct StatsWindowState {
    pub buckets: HashMap<ScopeKey, Vec<StatsAccum>>,
    pub window_start_nanos: i64,
    pub last_event_nanos: i64,
    pub event_count: u64,
}

impl StatsWindowState {
    fn new(window_start_nanos: i64) -> Self {
        Self {
            buckets: HashMap::new(),
            window_start_nanos,
            last_event_nanos: 0,
            event_count: 0,
        }
    }

    /// 取/建一个桶的累加器数组（惰性创建）。
    fn bucket_mut(&mut self, key: &ScopeKey, n_measures: usize) -> &mut Vec<StatsAccum> {
        self.buckets
            .entry(key.clone())
            .or_insert_with(|| vec![StatsAccum::default(); n_measures])
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
        // 空键规则预建 Empty 桶（快路径; 带 key 惰性建桶）。
        let mut buckets = HashMap::new();
        if plan.keys.is_empty() {
            buckets.insert(ScopeKey::Empty, vec![StatsAccum::default(); n]);
        }
        Self {
            plan,
            window: StatsWindowState {
                buckets,
                window_start_nanos: 0,
                last_event_nanos: 0,
                event_count: 0,
            },
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
    ///
    /// 桶键（P2）: 按 `plan.keys` 分桶归并; 键缺失/null → 行跳过（对齐 CEP
    /// key 缺失不匹配语义）。空键规则恒单桶。
    pub fn process_rows<F>(&mut self, rows: &[HashMap<String, Value>], extract: F)
    where
        F: Fn(&HashMap<String, Value>, &str) -> Option<Value>,
    {
        // where 结果缓存: 行间复用 buffer（无逐行分配）; 无 where 规则时保持空。
        let mut where_ok: Vec<bool> = Vec::with_capacity(self.unique_wheres.len());
        let n_measures = self.plan.measures.len();
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
            let bucket = self.window.bucket_mut(&bucket_key, n_measures);
            for (idx, measure) in self.plan.measures.iter().enumerate() {
                if let Some(wi) = self.measure_where[idx]
                    && !where_ok[wi]
                {
                    continue;
                }
                let acc = &mut bucket[idx];
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

    /// 计算最终度量值（空键兼容: 取单桶; 带 key 用 by_bucket 版本）。
    pub fn final_measure_values(&self) -> Vec<f64> {
        self.final_measure_values_by_bucket()
            .into_iter()
            .next()
            .map(|(_, values)| values)
            .unwrap_or_else(|| vec![0.0; self.plan.measures.len()])
    }

    /// 按桶的最终度量值（桶序 = ScopeKey 升序, 确定性输出对拍契约; avg 由
    /// sum/count 求得, D6）。
    pub fn final_measure_values_by_bucket(&self) -> Vec<(ScopeKey, Vec<f64>)> {
        let mut buckets: Vec<(ScopeKey, Vec<f64>)> = self
            .window
            .buckets
            .iter()
            .map(|(k, accs)| (k.clone(), measure_values(&self.plan, accs)))
            .collect();
        buckets.sort_by(|a, b| a.0.cmp(&b.0));
        buckets
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
        let n = self.plan.measures.len();
        let mut buckets = HashMap::new();
        if self.plan.keys.is_empty() {
            buckets.insert(ScopeKey::Empty, vec![StatsAccum::default(); n]);
        }
        self.window = StatsWindowState {
            buckets,
            window_start_nanos: 0,
            last_event_nanos: 0,
            event_count: 0,
        };
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
        let n = batch.num_rows();
        // 行域（P2 分片）: `rows` = 本片拥有的行索引子集（绝对行号, 升序）。
        // 归并只对行域内的行生效; `None` = 全批。转为列 mask 与 where mask
        // 逐位 AND, 使整列归并原语（count_true/sum_masked/minmax_masked/
        // insert_distinct_column）无需改动即可按行域过滤。
        let domain = domain_mask(n, rows);
        let view = ColumnarBatch::from_all_fields(batch);
        // 段 1: where 列式 mask（去重后唯一条件, 每批一次）
        let masks: Vec<BooleanArray> = self
            .unique_wheres
            .iter()
            .map(|e| eval_guard_columnar(e, &view))
            .collect();
        // 带 key（P2）: 逐行按桶归并（mask 列式 + 桶键列式, 无解释器 eval）。
        // 空键保持整列归并快路径（P1.5）。
        if !self.plan.keys.is_empty() {
            return self.process_batch_keyed(batch, &masks, &key_cols, n, rows);
        }
        // 段 1d: 纯归并度量整列累加。行式语义: 满足 where 的行对**每个**度量都
        // `count += 1`（在字段读取前）——avg 的 count 必须与 sum 同步累加,
        // 否则 avg = sum/count 输出 0（D6: avg 仅输出时 sum/count 求得）。
        let n_measures = self.plan.measures.len();
        for (idx, measure) in self.plan.measures.iter().enumerate() {
            let mask = combine_masks(
                domain.as_ref(),
                self.measure_where[idx].map(|wi| &masks[wi]),
            );
            let acc = &mut self.window.bucket_mut(&ScopeKey::Empty, n_measures)[idx];
            let rows_in = mask.as_ref().map_or(n as u64, |m| count_true(m));
            match measure.agg {
                StatsAggPlan::Count => {
                    acc.count += rows_in;
                }
                StatsAggPlan::Sum | StatsAggPlan::Avg => {
                    acc.count += rows_in;
                    if let Some(field) = &measure.field
                        && let Some(col) = numeric_col(batch, field_name(field))
                    {
                        acc.sum_i128 += sum_masked(&col, mask.as_ref());
                    }
                }
                StatsAggPlan::Min | StatsAggPlan::Max => {
                    acc.count += rows_in;
                    if let Some(field) = &measure.field
                        && let Some(col) = numeric_col(batch, field_name(field))
                    {
                        minmax_masked(&col, mask.as_ref(), &mut acc.min, &mut acc.max);
                    }
                }
                StatsAggPlan::DistinctCount => {
                    // count 与行式一致维护（输出只用 distinct_set; 状态保持等价）
                    acc.count += rows_in;
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
            let mask = combine_masks(
                domain.as_ref(),
                self.measure_where[idx].map(|wi| &masks[wi]),
            );
            let acc = &mut self.window.bucket_mut(&ScopeKey::Empty, n_measures)[idx];
            let set = acc.distinct_set.get_or_insert_with(HashSet::new);
            if !insert_distinct_column(batch, field_name(field), mask.as_ref(), set) {
                return false;
            }
        }
        self.window.event_count += rows.map_or(n as u64, |rs| rs.len() as u64);
        true
    }

    /// 带 key 的批处理: 逐行按桶归并（P2）。桶键列式（`scope_key_columnar`）,
    /// where 列式 mask, 归并行式（每桶独立累积）。键 null → 行跳过（对齐 CEP
    /// key 缺失语义; 与 fanout 的 missing-key → shard 0 分片口径一致——分片只
    /// 影响投递归属, 桶归并仍按完整键）。
    ///
    /// `rows`（P2 分片）= 本片行子集: **只归并行域内的行**——否则每片处理全批,
    /// 每个键被 N 片各算一遍, close 重复输出 N 倍（Q16 实测 EMIT 10 倍）。
    fn process_batch_keyed(
        &mut self,
        batch: &RecordBatch,
        masks: &[BooleanArray],
        key_cols: &[usize],
        n: usize,
        rows: Option<&[u32]>,
    ) -> bool {
        let n_measures = self.plan.measures.len();
        match rows {
            Some(rs) => {
                for &r in rs {
                    if (r as usize) >= n {
                        continue; // 防御: 越界行号（与 materialize_rows 一致跳过）
                    }
                    self.accumulate_keyed_row(batch, masks, key_cols, r as usize, n_measures);
                }
            }
            None => {
                for row in 0..n {
                    self.accumulate_keyed_row(batch, masks, key_cols, row, n_measures);
                }
            }
        }
        self.window.event_count += rows.map_or(n as u64, |rs| rs.len() as u64);
        true
    }

    /// 单行桶归并（P2 复合键逐行路径的公共主体, 供全批/行域两分支复用）。
    fn accumulate_keyed_row(
        &mut self,
        batch: &RecordBatch,
        masks: &[BooleanArray],
        key_cols: &[usize],
        row: usize,
        n_measures: usize,
    ) {
        let Some(key) = scope_key_columnar(batch, key_cols, row) else {
            return; // 键 null → 跳过
        };
        let bucket = self.window.bucket_mut(&key, n_measures);
        for (idx, measure) in self.plan.measures.iter().enumerate() {
            if let Some(wi) = self.measure_where[idx]
                && !masks[wi].value(row)
            {
                continue;
            }
            let acc = &mut bucket[idx];
            acc.count += 1;
            if let Some(field) = &measure.field
                && let Some(val) = column_value(batch, field_name(field), row)
            {
                match measure.agg {
                    StatsAggPlan::Count => {}
                    StatsAggPlan::Sum | StatsAggPlan::Avg => {
                        if let Some(nn) = value_to_i128(&val) {
                            acc.sum_i128 += nn;
                        }
                    }
                    StatsAggPlan::Min => {
                        if let Some(nn) = value_to_i128(&val) {
                            acc.min = Some(match acc.min {
                                Some(m) if m <= nn => m,
                                _ => nn,
                            });
                        }
                    }
                    StatsAggPlan::Max => {
                        if let Some(nn) = value_to_i128(&val) {
                            acc.max = Some(match acc.max {
                                Some(m) if m >= nn => m,
                                _ => nn,
                            });
                        }
                    }
                    StatsAggPlan::DistinctCount => {
                        acc.distinct_set
                            .get_or_insert_with(HashSet::new)
                            .insert(value_to_distinct_key(&val));
                    }
                    StatsAggPlan::Last | StatsAggPlan::Top => {
                        // P1 不实现（Q18/Q19 扩展）
                    }
                }
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
// 桶键求值（P2 复合键: Field / bucket() / tier()）
// ---------------------------------------------------------------------------

/// 由 `plan.measures` + 桶累加器计算度量值（avg 输出时 sum/count, D6）。
fn measure_values(plan: &StatsPlan, accs: &[StatsAccum]) -> Vec<f64> {
    plan.measures
        .iter()
        .zip(accs.iter())
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
            StatsAggPlan::DistinctCount => acc.distinct_set.as_ref().map_or(0, HashSet::len) as f64,
            StatsAggPlan::Last | StatsAggPlan::Top => 0.0,
        })
        .collect()
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

/// 从 batch 列读单行值（Int64/Float64/Utf8/Bool; null → None）。
/// 用于带 key 批处理的归并（f64 口径与行式一致; >2^53 分歧为文档化 D7）。
fn column_value(batch: &RecordBatch, name: &str, row: usize) -> Option<Value> {
    let idx = batch.schema().index_of(name).ok()?;
    let col = batch.column(idx);
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| Value::Number(a.value(row) as f64)),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| Value::Number(a.value(row))),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| Value::Str(a.value(row).into())),
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| Value::Bool(a.value(row))),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// 列式段辅助（P1.5）
// ---------------------------------------------------------------------------

/// mask 中 true 的个数（含 null slot 读 false, 与行式 eval 语义一致）。
fn count_true(mask: &BooleanArray) -> u64 {
    (0..mask.len()).filter(|&i| mask.value(i)).count() as u64
}

/// 行域 mask（P2 分片）: `rows` 内的行标 true, 其余 false; `None` = 全批。
/// 与 where mask 逐位 AND 后喂给整列归并原语, 使归并只对行域生效。
fn domain_mask(n: usize, rows: Option<&[u32]>) -> Option<BooleanArray> {
    let rows = rows?;
    let mut flags = vec![false; n];
    for &r in rows {
        if (r as usize) < n {
            flags[r as usize] = true;
        }
    }
    Some(flags.into_iter().collect())
}

/// 行域 mask 与 where mask 逐位 AND（null slot 读 false, 语义同 `value(i)`）。
/// 返回 `None` 仅当两者皆无（= 全批全通过）。
fn combine_masks(
    domain: Option<&BooleanArray>,
    where_mask: Option<&BooleanArray>,
) -> Option<BooleanArray> {
    match (domain, where_mask) {
        (None, None) => None,
        (Some(d), None) => Some(d.clone()),
        (None, Some(m)) => Some(m.clone()),
        (Some(d), Some(m)) => Some((0..d.len()).map(|i| d.value(i) && m.value(i)).collect()),
    }
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
