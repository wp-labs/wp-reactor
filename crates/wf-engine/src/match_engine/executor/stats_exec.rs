//! Stats executor — 声明式窗口统计执行器（P1: 空键 fixed count/distinct）。
//!
//! 与 CEP(RuleExecutor/match)完全正交: 消费 fanout 的 raw RecordBatch,
//! 内部维护可交换结合的归并状态, 窗口 close 时产出度量值并复用 alert 构建。
//!
//! 设计依据: docs/stats-executor-design.md v6（§6 执行器）。

use std::collections::{HashMap, HashSet};

use wf_lang::ast::FieldRef;
use wf_lang::plan::{StatsAggPlan, StatsPlan};

use crate::match_engine::Value;

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
}

impl StatsExecutor {
    pub fn new(plan: StatsPlan) -> Self {
        let n = plan.measures.len();
        Self {
            plan,
            window: StatsWindowState::new(n, 0),
            watermark_nanos: 0,
        }
    }

    /// 处理一批行（row-based; 列式段为 P1.5）。
    ///
    /// `extract_field(row, name) -> Option<Value>`: 由调用方提供行字段读取。
    /// where 过滤: 调用方预先求值并注入 `__where_ok` 字段（P1 简化）——所有带
    /// where_expr 的度量共享同一过滤（单 where 场景）。多档 where 用
    /// [`Self::process_rows_where`]。
    pub fn process_rows<F>(&mut self, rows: &[HashMap<String, Value>], extract: F)
    where
        F: Fn(&HashMap<String, Value>, &str) -> Option<Value>,
    {
        self.process_rows_where(rows, extract, |row, _idx| {
            !matches!(row.get("__where_ok"), Some(Value::Bool(false)))
        });
    }

    /// 处理一批行, 支持**逐度量** where 过滤（Q15 多档价格分档等场景）。
    ///
    /// `where_ok(row, measure_idx) -> bool`: 调用方按度量求值 where; 仅在
    /// `measure.where_expr.is_some()` 时被咨询（无 where 的度量恒通过）。
    pub fn process_rows_where<F, W>(
        &mut self,
        rows: &[HashMap<String, Value>],
        extract: F,
        where_ok: W,
    ) where
        F: Fn(&HashMap<String, Value>, &str) -> Option<Value>,
        W: Fn(&HashMap<String, Value>, usize) -> bool,
    {
        for row in rows {
            for (idx, measure) in self.plan.measures.iter().enumerate() {
                if measure.where_expr.is_some() && !where_ok(row, idx) {
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
