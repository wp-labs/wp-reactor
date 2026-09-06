//! 单行/桶累加自由函数簇（exec.rs 过长拆分）。调用点持有 `&mut window` 桶借用
//! 时, 自由函数免整 `self` 借用冲突; 供 `accumulate_keyed_row` 与 SoA 对照
//! bench（lib-tests, 经 `stats_exec` mod re-export 直连）共用。
//!
//! 分派一次（非每度量）: SoA → [`accumulate_soa`]; Classic →
//! [`accumulate_column_row`]（枚举/Box 循环）。`accumulate_soa` /
//! `accumulate_column_row` 为 pub(crate)（SoA 对照 bench 消费）。

use arrow::array::BooleanArray;
use arrow::record_batch::RecordBatch;
use wf_cep::rows::{RowFieldLayout, RowFields};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsPlan};

use super::*;

/// 单行桶累加入口（按桶形态分派一次）: SoA → [`accumulate_soa`]; Classic →
/// [`accumulate_column_row`]（枚举/Box 循环）。分派每行一次（非每度量）——
/// SoA 计划的热路径进入后无枚举分派无 Box 解引用。
#[allow(clippy::too_many_arguments)] // 与 accumulate_column_row 同签名族 + soa_layout
pub(crate) fn accumulate_bucket_row(
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
                    *cur = min_fold(*cur, v);
                }
                NumericKind::Max => {
                    let s = layout.max_slot[idx].unwrap() as usize;
                    let cur = &mut soa.maxs[s];
                    *cur = max_fold(*cur, v);
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
                // count 无字段 → None → 只计数。数值落账集中一处（含同列一次读取）。
                if let Some(ci) = measure_field_cols[idx]
                    && let Some(v) = column_i128_at(batch, ci, row)
                {
                    accumulate_numeric_value(nacc, &measure.agg, v);
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
                // 快速淘汰/整行提取集中于 helper（true = 本行跳过, 继续下一度量）。
                if accumulate_last_top_row(
                    acc,
                    measure,
                    plan,
                    idx,
                    measure_field_idx,
                    row_names,
                    &mut row_cache,
                    batch,
                    row,
                    row_field_cols,
                    row_layout,
                ) {
                    continue;
                }
            }
        }
    }
}

/// Numeric 度量的单值落账（调用方已 `count += 1`）——sum/avg 同路径; min/max
/// 极值折叠。Count 变体无字段更新（调用方给字段值即为无操作）。
fn accumulate_numeric_value(nacc: &mut NumericAccum, agg: &StatsAggPlan, v: i128) {
    match agg {
        StatsAggPlan::Sum | StatsAggPlan::Avg => nacc.sum += v,
        StatsAggPlan::Min => nacc.min = min_fold(nacc.min, v),
        StatsAggPlan::Max => nacc.max = max_fold(nacc.max, v),
        _ => {}
    }
}

/// last/top 单行更新（列式路径; true = 本行跳过——top 已满且 key 进不了前 N,
/// 或 `top(0)` 不保留任何条目; 免每行 row_fields 的 Arc 分配 + 字段提取）。
///
/// 快速淘汰预检（**在构建行字段前**）: q19 每 auction 的 bid 绝大多数低于当前
/// top-10 门槛（bench ~99.8% 行被此预检挡下）。列索引经构造期预计算的
/// `measure_field_idx` 取（零 index_of / 零 names 回退——无子集时 idx 恒 None
/// → 不预检, 仅测试/缺省路径, 性能不敏感）。列值口径与行字段提取后
/// `value_to_f64` 一致（Int64→as f64 / Float64 原值）。
#[allow(clippy::too_many_arguments)] // 同 accumulate_column_row 签名族
fn accumulate_last_top_row(
    acc: &mut StatsAccum,
    measure: &StatsMeasurePlan,
    plan: &StatsPlan,
    idx: usize,
    measure_field_idx: &[Option<usize>],
    row_names: Option<&[String]>,
    row_cache: &mut Option<std::sync::Arc<RowFields>>,
    batch: &RecordBatch,
    row: usize,
    row_field_cols: Option<&[Option<usize>]>,
    row_layout: &std::sync::Arc<RowFieldLayout>,
) -> bool {
    if measure.agg == StatsAggPlan::Top {
        let n = measure.arg.unwrap_or(10) as usize;
        if n == 0 {
            return true; // top(0): 不保留任何条目, 无需行字段
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
            return true;
        }
    }
    let row = row_cache
        .get_or_insert_with(|| row_fields_from_batch(batch, row, row_field_cols, row_layout));
    let fidx = measure_field_position(plan, measure_field_idx, idx, row_names);
    apply_last_top(acc, measure, row, fidx);
    false
}

/// 数值极值折叠（保持旧语义: 取包含当前值的极小/极大; `cur` 为新桶 None 时直接
/// 取 v——多处单行/整列累加/分片归并共用同一口径, 防分支复制漂移）。
pub(crate) fn min_fold(cur: Option<i128>, v: i128) -> Option<i128> {
    Some(match cur {
        Some(m) if m <= v => m,
        _ => v,
    })
}

pub(crate) fn max_fold(cur: Option<i128>, v: i128) -> Option<i128> {
    Some(match cur {
        Some(m) if m >= v => m,
        _ => v,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folds_preserve_extreme_semantics() {
        assert_eq!(min_fold(None, 3), Some(3));
        assert_eq!(min_fold(Some(1), 3), Some(1));
        assert_eq!(min_fold(Some(5), 3), Some(3));
        assert_eq!(max_fold(None, 3), Some(3));
        assert_eq!(max_fold(Some(7), 3), Some(7));
        assert_eq!(max_fold(Some(1), 3), Some(3));
    }

    #[test]
    fn numeric_value_accumulation_matches_agg_kinds() {
        let mut a = NumericAccum::default();
        accumulate_numeric_value(&mut a, &StatsAggPlan::Sum, 10);
        accumulate_numeric_value(&mut a, &StatsAggPlan::Avg, 5); // 与 Sum 同路径
        accumulate_numeric_value(&mut a, &StatsAggPlan::Min, 7);
        accumulate_numeric_value(&mut a, &StatsAggPlan::Max, 7);
        assert_eq!(a.sum, 15);
        assert_eq!(a.min, Some(7));
        assert_eq!(a.max, Some(7));
        // 极值折叠: 更小/更大值才覆盖
        accumulate_numeric_value(&mut a, &StatsAggPlan::Min, 2);
        accumulate_numeric_value(&mut a, &StatsAggPlan::Min, 5);
        accumulate_numeric_value(&mut a, &StatsAggPlan::Max, 20);
        assert_eq!(a.min, Some(2));
        assert_eq!(a.max, Some(20));
        // Count 变体无字段更新（调用方单独计数）
        accumulate_numeric_value(&mut a, &StatsAggPlan::Count, 100);
        assert_eq!(a.sum, 15);
        assert_eq!(a.count, 0);
    }
}
