//! merge — eval/ 子模块（从 eval.rs 拆分）。
use super::*;

// ---------------------------------------------------------------------------
// 归并（输入分区分片: 可交换度量）
// ---------------------------------------------------------------------------

/// 归并两个累加器（count 相加 / sum 相加 / min·max 取极值 / distinct 集 union）。
/// 仅可交换度量路径使用（last/top 被 spawn 门控排除——行序敏感不可归并）。
/// 变体不匹配 = plan/构造不一致的内部错误（panic 尽早暴露）。
pub(crate) fn merge_accum(t: &mut StatsAccum, o: &StatsAccum) {
    match (t, o) {
        (StatsAccum::Numeric(t), StatsAccum::Numeric(o)) => {
            t.count += o.count;
            t.sum += o.sum;
            if let Some(v) = o.min {
                t.min = min_fold(t.min, v);
            }
            if let Some(v) = o.max {
                t.max = max_fold(t.max, v);
            }
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
pub(crate) fn merge_bucket_accs(t: &mut StatsBucketAccs, o: StatsBucketAccs) {
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
                    t.mins[i] = min_fold(t.mins[i], v);
                }
            }
            for (i, m) in o.maxs.iter().enumerate() {
                if let Some(v) = *m {
                    t.maxs[i] = max_fold(t.maxs[i], v);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric(count: u64, sum: i128, min: Option<i128>, max: Option<i128>) -> StatsAccum {
        StatsAccum::Numeric(Box::new(NumericAccum {
            count,
            sum,
            min,
            max,
        }))
    }

    #[test]
    fn merge_accum_numeric_combines_extremes() {
        let mut t = numeric(3, 10, Some(-2), Some(7));
        merge_accum(&mut t, &numeric(5, 20, Some(-5), Some(9)));
        let StatsAccum::Numeric(n) = &t else {
            panic!("expected Numeric");
        };
        assert_eq!(n.count, 8);
        assert_eq!(n.sum, 30);
        assert_eq!(n.min, Some(-5));
        assert_eq!(n.max, Some(9));
        // 单侧 None 合并保留有值侧
        merge_accum(&mut t, &numeric(0, 0, None, None));
        let StatsAccum::Numeric(n) = &t else {
            panic!("expected Numeric");
        };
        assert_eq!(n.min, Some(-5));
        assert_eq!(n.max, Some(9));
    }

    #[test]
    fn merge_accum_distinct_unions() {
        let mut t = StatsAccum::Distinct(Box::default());
        let mut o = StatsAccum::Distinct(Box::default());
        t.distinct_mut().insert(DistinctKey::from_str("a"));
        t.distinct_mut().insert(DistinctKey::from_str("b"));
        o.distinct_mut().insert(DistinctKey::from_str("b"));
        o.distinct_mut().insert(DistinctKey::from_str("c"));
        merge_accum(&mut t, &o);
        assert_eq!(t.distinct_mut().len(), 3);
    }

    #[test]
    fn merge_bucket_accs_soa_arrays() {
        fn soa(
            counts: Vec<u64>,
            sums: Vec<i128>,
            mins: Vec<Option<i128>>,
            maxs: Vec<Option<i128>>,
        ) -> StatsBucketAccs {
            StatsBucketAccs::Numeric(NumericSoA {
                counts: counts.into_boxed_slice(),
                sums: sums.into_boxed_slice(),
                mins: mins.into_boxed_slice(),
                maxs: maxs.into_boxed_slice(),
            })
        }
        let mut t = soa(
            vec![1, 2],
            vec![5, 7],
            vec![Some(1), None],
            vec![Some(9), None],
        );
        let o = soa(
            vec![3, 4],
            vec![5, 3],
            vec![Some(0), Some(8)],
            vec![Some(10), None],
        );
        merge_bucket_accs(&mut t, o);
        match t {
            StatsBucketAccs::Numeric(n) => {
                assert_eq!(n.counts.as_ref(), &[4u64, 6]);
                assert_eq!(n.sums.as_ref(), &[10i128, 10]);
                assert_eq!(n.mins.as_ref(), &[Some(0), Some(8)]);
                assert_eq!(n.maxs.as_ref(), &[Some(10), None]);
            }
            other => panic!("expected Numeric, got {other:?}"),
        }
    }
}
