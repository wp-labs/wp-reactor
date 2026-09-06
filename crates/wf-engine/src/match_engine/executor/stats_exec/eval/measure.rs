//! measure — eval/ 子模块（从 eval.rs 拆分）。
use super::*;

// ---------------------------------------------------------------------------
// 桶键求值（P2 复合键: Field / bucket() / tier()）
// ---------------------------------------------------------------------------

/// 由 `plan.measures` + 桶累加器计算度量值（avg 输出时 sum/count, D6）。
/// last 取字段数值（非数值 → 0; 无子集时 idx 未知 → 0, 标量访问器需子集）;
/// top 为多值不适用, 返回 0（rich close 用）。
/// SoA 桶 → 每度量 f64 值（语义与 [`measure_values`] 的 Numeric 分支一致:
/// count/sum 直取、avg = sum/count（count==0 → 0.0）、min/max unwrap_or(0)）。
/// pub(crate) 供 SoA 对照 bench。
pub(crate) fn measure_values_soa(
    plan: &StatsPlan,
    soa: &NumericSoA,
    layout: &NumericSoALayout,
) -> Vec<f64> {
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
pub(crate) fn bucket_measure_values(
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
pub(crate) fn bucket_close_entries(
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

pub(crate) fn measure_values(
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
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct StatsCloseEntry {
    pub measure_value: f64,
    pub row_fields: Option<std::sync::Arc<RowFields>>,
}

/// 每桶 close 输出: 每度量一个值列表（标量 = 1; top = N, 按 rank 序）。
#[derive(Debug, Clone, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.StatsEngine")]
pub struct StatsCloseBucket {
    pub key: ScopeKey,
    pub measures: Vec<Vec<StatsCloseEntry>>,
}

/// 单个度量的 close 条目列表（标量 = 1; last = 1 带行字段列数组; top = N 带行
/// 字段列数组）。行字段为 Arc（与状态共享, close 零拷贝; 构造 alert 时才逐值
/// 克隆）。`field_idx` = 该度量字段在列数组中的位置（P5 预计算）。
pub(crate) fn bucket_measure_entries(
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
