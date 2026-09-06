//! domain — eval/ 子模块（从 eval.rs 拆分）。
use super::*;

// ---------------------------------------------------------------------------
// 列式段辅助（P1.5）
// ---------------------------------------------------------------------------

/// 行域迭代（P2 分片裁剪）: `rows` = 本片行索引（升序, 绝对行号）; `None` =
/// 全批 `0..n`。行域驱动的归并段（count/sum/minmax/distinct）只遍历本片行,
/// 消除每片对全批的 O(n) 冗余扫描（q15 输入分片 10× 冗余归因, 2026-08-24）。
pub(crate) fn domain_rows(rows: Option<&[u32]>, n: usize) -> Box<dyn Iterator<Item = usize> + '_> {
    match rows {
        Some(rs) => Box::new(rs.iter().map(|&r| r as usize)),
        None => Box::new(0..n),
    }
}

/// 行域内满足 where 过滤的行数（`wi` = unique_wheres 索引; `None` = 恒通过）。
/// 等价 `count_true(combine(domain, where))`——逐行查 where mask 位（null slot
/// 读 false, 与 `BooleanArray::value` 一致）。
pub(crate) fn count_domain(
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
) -> u64 {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    match rows {
        Some(rs) => rs.iter().filter(|&&r| passes(r as usize)).count() as u64,
        None => (0..n).filter(|&r| passes(r)).count() as u64,
    }
}

/// 行域驱动求和（null 跳过; 数值按行式 `value_to_i128` 截断, D8）。
pub(crate) fn sum_domain(
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
pub(crate) fn minmax_domain(
    col: &NumCol<'_>,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
    min: &mut Option<i128>,
    max: &mut Option<i128>,
) {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    fold_numeric_domain(col, rows, n, &passes, &mut |v| {
        *min = min_fold(*min, v);
        *max = max_fold(*max, v);
    });
}

/// 行域驱动的单极值更新（SoA 用: Min 度量只写 min 槽, Max 度量只写 max 槽——
/// SoA 无死状态; 语义 = [`minmax_domain`] 的对应半段）。
pub(crate) fn minmax_domain_one(
    col: &NumCol<'_>,
    rows: Option<&[u32]>,
    n: usize,
    masks: &[BooleanArray],
    wi: Option<usize>,
    is_min: bool,
    out: &mut Option<i128>,
) {
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    fold_numeric_domain(col, rows, n, &passes, &mut |v| {
        *out = if is_min {
            min_fold(*out, v)
        } else {
            max_fold(*out, v)
        };
    });
}

/// 数值列的域折叠（Int64/Float64 双分支共用）: 对每个满足 where 且非 null 的
/// 行调用 `fold(值)`——`minmax_domain`/`minmax_domain_one`/SoA 单极值共用。
fn fold_numeric_domain(
    col: &NumCol<'_>,
    rows: Option<&[u32]>,
    n: usize,
    passes: &dyn Fn(usize) -> bool,
    fold: &mut dyn FnMut(i128),
) {
    match col {
        NumCol::Int64(c) => {
            for r in domain_rows(rows, n) {
                if passes(r) && !c.is_null(r) {
                    fold(c.value(r) as i128);
                }
            }
        }
        NumCol::Float64(c) => {
            for r in domain_rows(rows, n) {
                if passes(r) && !c.is_null(r) {
                    fold(c.value(r) as i128);
                }
            }
        }
    }
}

/// 行域驱动的 distinct 插入（原生列值按行域 + where 过滤）——等价
/// `insert_distinct_column` 的 mask 全批扫描, 但只遍历本片行。
pub(crate) fn insert_distinct_domain(
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
    // 域循环外 downcast 一次为按类型读取器——五个列类型只写一条读取映射, 循环收敛。
    let reader = if let Some(c) = col.as_any().downcast_ref::<Int64Array>() {
        DistinctColReader::Int(c)
    } else if let Some(c) = col.as_any().downcast_ref::<Float64Array>() {
        DistinctColReader::Float(c)
    } else if let Some(c) = col.as_any().downcast_ref::<StringArray>() {
        DistinctColReader::Str(c)
    } else if let Some(c) = col.as_any().downcast_ref::<BooleanArray>() {
        DistinctColReader::Bool(c)
    } else if let Some(c) = col
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
    {
        DistinctColReader::Time(c)
    } else {
        return false;
    };
    let passes = |r: usize| wi.is_none_or(|wi| masks[wi].value(r));
    insert_distinct_rows(rows, n, &passes, &mut |r| reader.read(r), set);
    true
}

/// 列值 → distinct 键的按类型读取器（null 行 → None）——域循环外构造一次。
enum DistinctColReader<'a> {
    Int(&'a Int64Array),
    Float(&'a Float64Array),
    Str(&'a StringArray),
    Bool(&'a BooleanArray),
    Time(&'a arrow::array::TimestampNanosecondArray),
}

impl DistinctColReader<'_> {
    fn read(&self, r: usize) -> Option<DistinctKey> {
        match self {
            DistinctColReader::Int(c) => (!c.is_null(r)).then(|| DistinctKey::from_i64(c.value(r))),
            DistinctColReader::Float(c) => {
                (!c.is_null(r)).then(|| DistinctKey::from_f64(c.value(r)))
            }
            DistinctColReader::Str(c) => (!c.is_null(r)).then(|| DistinctKey::from_str(c.value(r))),
            DistinctColReader::Bool(c) => {
                (!c.is_null(r)).then(|| DistinctKey::from_f64(if c.value(r) { 1.0 } else { 0.0 }))
            }
            DistinctColReader::Time(c) => {
                (!c.is_null(r)).then(|| DistinctKey::from_i64(c.value(r)))
            }
        }
    }
}

/// 域过滤逐行 distinct 插入（null 行跳过）——各列类型循环共用同一形状。
fn insert_distinct_rows(
    rows: Option<&[u32]>,
    n: usize,
    passes: &dyn Fn(usize) -> bool,
    key_of: &mut dyn FnMut(usize) -> Option<DistinctKey>,
    set: &mut DistinctSet,
) {
    for r in domain_rows(rows, n) {
        if passes(r)
            && let Some(k) = key_of(r)
        {
            set.insert(k);
        }
    }
}

/// distinct 度量字段列类型支持检查（Int64/Float64/Utf8/Bool/TimestampNs）。
/// 字段缺失视为安全（与行式 extract None 一致, 不插入）。
///
/// **必须在 `process_batch` 任何累加副作用之前调用**: 段 2 中途失败返回 false
/// 时调用方会回退 `process_rows`, 此时段 1 已累加的 count/sum 会被重复计算
/// （部分应用 bug）——类型支持与否必须一次性前置判定。
pub(crate) fn distinct_fields_columnar_safe(batch: &RecordBatch, plan: &StatsPlan) -> bool {
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
pub(crate) enum NumCol<'a> {
    Int64(&'a Int64Array),
    Float64(&'a Float64Array),
}

pub(crate) fn numeric_col<'a>(batch: &'a RecordBatch, name: &str) -> Option<NumCol<'a>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn int_col(vals: &[Option<i64>]) -> arrow::array::Int64Array {
        arrow::array::Int64Array::from(vals.to_vec())
    }

    #[test]
    fn minmax_domain_tracks_both_extremes_over_null_rows() {
        let arr = int_col(&[Some(3), None, Some(1), Some(9)]);
        let col = NumCol::Int64(&arr);
        let (mut min, mut max) = (None, None);
        minmax_domain(&col, None, arr.len(), &[], None, &mut min, &mut max);
        assert_eq!(min, Some(1));
        assert_eq!(max, Some(9));
    }

    #[test]
    fn minmax_domain_one_min_half_folds() {
        let arr = int_col(&[Some(3), None, Some(1), Some(9)]);
        let col = NumCol::Int64(&arr);
        let mut min = None;
        minmax_domain_one(&col, None, arr.len(), &[], None, true, &mut min);
        assert_eq!(min, Some(1));
        let mut max = None;
        minmax_domain_one(&col, None, arr.len(), &[], None, false, &mut max);
        assert_eq!(max, Some(9));
    }

    #[test]
    fn minmax_domain_respects_where_mask() {
        let arr = int_col(&[Some(7), None, Some(1), Some(9)]);
        let col = NumCol::Int64(&arr);
        let mask = arrow::array::BooleanArray::from(vec![false, true, true, true]);
        let (mut min, mut max) = (None, None);
        minmax_domain(&col, None, arr.len(), &[mask], Some(0), &mut min, &mut max);
        // 行 0 被 where 滤掉（值 7 不参与极值）
        assert_eq!(min, Some(1));
        assert_eq!(max, Some(9));
    }
}
