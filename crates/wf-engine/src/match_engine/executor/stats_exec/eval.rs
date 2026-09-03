//! 归并/求值/读取纯函数簇（键哈希、桶键求值、行字段与列式段辅助、domain 扫描）。

use std::collections::HashMap;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsPlan};

use super::*;
use crate::match_engine::Value;
use crate::match_engine::cep::ScopeKey;
use crate::match_engine::event_bridge::extract_field_value;
use crate::window::scope_key_from_column;
use wf_cep::rows::{RowFieldLayout, RowFields};

pub(crate) fn field_name(fr: &FieldRef) -> &str {
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
pub(crate) fn measure_field_position(
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
pub(crate) fn merge_accum(t: &mut StatsAccum, o: &StatsAccum) {
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

pub(crate) enum KeyColumn<'a> {
    Int64(&'a Int64Array),
    Timestamp(&'a arrow::array::TimestampNanosecondArray),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    Boolean(&'a BooleanArray),
    /// 其它类型 → 逐行回退 `scope_key_from_column`（罕见）。
    Other(usize),
}

pub(crate) fn resolve_key_columns<'a>(
    batch: &'a RecordBatch,
    key_cols: &[usize],
) -> Vec<KeyColumn<'a>> {
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
pub(crate) fn key_column_comp<'a>(
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
pub(crate) fn scope_key_from_f64(n: f64) -> ScopeKey {
    if n.fract() == 0.0 && n.abs() < TWO_POW_53 {
        ScopeKey::Int(n as i64)
    } else {
        ScopeKey::Float(canonical_f64_bits(n))
    }
}

/// 规范化 f64 位（0.0 → +0.0, NaN → canonical NaN; 与 key.rs 同口径）。
pub(crate) fn canonical_f64_bits(n: f64) -> u64 {
    if n == 0.0 {
        0.0f64.to_bits()
    } else if n.is_nan() {
        f64::NAN.to_bits()
    } else {
        n.to_bits()
    }
}

/// <2^53 的整数可被 f64 精确表示（与 `ScopeKey::from_value` 一致）。
pub(crate) const TWO_POW_53: f64 = 9_007_199_254_740_992.0;

// ---------------------------------------------------------------------------
// 复合键扁平哈希（P5+ 优化）
// ---------------------------------------------------------------------------
//
// 桶表键 = 扁平键的 FNV 式混合 u64。`comps_hash`（列式叶数组）与
// `scope_key_hash`（行式完整树）字节级同构——同一逻辑键两种路径产出同值
// （`stats_composite_key_hash_flat_matches_tree` 锁定）; 碰撞链内以完整键比较
// 消歧。混合风格与 `key.rs::scope_key_shard_index` 一致（确定性, 无随机种子）。

pub(crate) const KEY_HASH_BASE: u64 = 0xcbf2_9ce4_8422_2325;

pub(crate) fn mix_byte(hash: &mut u64, b: u8) {
    *hash ^= u64::from(b);
    *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
}

pub(crate) fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
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

pub(crate) fn nested_key_bytes(h: &mut u64, key: &ScopeKey) {
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
pub(crate) fn mix_leaf(h: &mut u64, c: &ScopeKey, with_tag: bool) {
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
pub(crate) fn comps_match(scope: &ScopeKey, comps: &[ScopeKey], start: usize, end: usize) -> bool {
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

pub(crate) fn value_to_i128(v: &Value) -> Option<i128> {
    match v {
        Value::Number(n) => Some(*n as i128),
        _ => None,
    }
}

pub(crate) fn value_to_f64(v: &Value) -> Option<f64> {
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
pub(crate) fn apply_last_top(
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
pub(crate) fn insert_top(entries: &mut Vec<TopEntry>, key: f64, row: RowFields, n: usize) {
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

pub(crate) fn value_to_distinct_key(v: &Value) -> DistinctKey {
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

/// 行式桶键（复合键: 逐 key 求值 → Pair 组合）。任一 key 缺失/不可求值 → None。
pub(crate) fn eval_row_key(keys: &[Expr], row: &HashMap<String, Value>) -> Option<ScopeKey> {
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
pub(crate) fn eval_row_bucket_key(expr: &Expr, row: &HashMap<String, Value>) -> Option<ScopeKey> {
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

pub(crate) fn field_value_of(expr: &Expr, row: &HashMap<String, Value>) -> Option<f64> {
    match expr {
        Expr::Field(fr) => match row.get(field_name(fr)) {
            Some(Value::Number(n)) => Some(*n),
            _ => None,
        },
        Expr::Number(n) => Some(*n),
        _ => None,
    }
}

pub(crate) fn bucket_unit_nanos(expr: &Expr) -> Option<i64> {
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
pub(crate) fn tier_index(v: f64, bounds: &[f64]) -> i64 {
    bounds.iter().position(|b| v < *b).unwrap_or(bounds.len()) as i64
}

/// 行式 last/top 行字段提取（与列式 [`row_fields_from_batch`] 对齐, P5 紧凑化）:
/// 按 `names` 列序返回 `Box<[Option<Value>]>`（缺失 = `None`）。`None` = 全列,
/// 行键**排序**（确定性; 仅测试/缺省——生产经 spawn 恒有子集）。
pub(crate) fn row_fields_from_row(
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
pub(crate) fn row_fields_from_batch(
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
pub(crate) fn column_i128_at(batch: &RecordBatch, ci: usize, row: usize) -> Option<i128> {
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
pub(crate) fn column_f64_at(batch: &RecordBatch, ci: usize, row: usize) -> Option<f64> {
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
pub(crate) fn column_distinct_key_at(
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
