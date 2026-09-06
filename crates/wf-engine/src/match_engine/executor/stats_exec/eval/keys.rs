//! keys — eval/ 子模块（从 eval.rs 拆分）。
use super::*;

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
    key_cols
        .iter()
        .map(|&ci| resolve_key_column(ci, batch.column(ci)))
        .collect()
}

/// 单键列类型分派（Int64/Timestamp→Int 系, Float64/Utf8/Boolean 规范化与
/// `scope_key_from_column` 一致）; 不支持类型回退 `Other(ci)` 逐行走原路径。
fn resolve_key_column<'a>(ci: usize, col: &'a dyn Array) -> KeyColumn<'a> {
    use arrow::datatypes::{DataType, TimeUnit};
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
}

/// 从预解析键列读单行叶键（null → None; 规范化与 `scope_key_from_column` 同）。
pub(crate) fn key_column_comp<'a>(
    col: &KeyColumn<'a>,
    batch: &'a RecordBatch,
    row: usize,
) -> Option<ScopeKey> {
    match col {
        KeyColumn::Int64(a) => non_null_cell(a, row, |a, r| ScopeKey::Int(a.value(r))),
        KeyColumn::Timestamp(a) => non_null_cell(a, row, |a, r| ScopeKey::Int(a.value(r))),
        KeyColumn::Float64(a) => non_null_cell(a, row, |a, r| scope_key_from_f64(a.value(r))),
        KeyColumn::Utf8(a) => non_null_cell(a, row, |a, r| ScopeKey::Str(a.value(r).into())),
        KeyColumn::Boolean(a) => non_null_cell(a, row, |a, r| {
            ScopeKey::Str(if a.value(r) { "true" } else { "false" }.into())
        }),
        KeyColumn::Other(ci) => scope_key_from_column(batch, *ci, row),
    }
}

/// 非 null 单元格取值（null → None; 否则由 `f` 把原生值映射为键叶）。
/// 键列逐行热点: 免每臂手写 if/else 复制。
fn non_null_cell<T: arrow::array::Array + ?Sized, V>(
    a: &T,
    row: usize,
    f: impl FnOnce(&T, usize) -> V,
) -> Option<V> {
    if a.is_null(row) {
        None
    } else {
        Some(f(a, row))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn k_int(v: i64) -> ScopeKey {
        ScopeKey::Int(v)
    }
    fn k_str(s: &str) -> ScopeKey {
        ScopeKey::Str(s.into())
    }
    fn k_float(n: f64) -> ScopeKey {
        ScopeKey::Float(canonical_f64_bits(n))
    }

    #[test]
    fn flat_comps_hash_matches_tree_hash() {
        // 单叶: comps 顶层叶 = 树哈希（tag + payload 同字节）
        assert_eq!(comps_hash(&[k_int(7)]), scope_key_hash(&k_int(7)));
        assert_eq!(comps_hash(&[k_str("x")]), scope_key_hash(&k_str("x")));
        assert_eq!(comps_hash(&[k_float(0.5)]), scope_key_hash(&k_float(0.5)));
        // 多叶: 左深 Pair 树与叶数组字节级同构（tag 仅最外层 + 0x1f 分隔）
        let a = k_int(7);
        let b = k_str("y");
        let pair = ScopeKey::Pair(Box::new(k_int(7)), Box::new(k_str("y")));
        assert_eq!(comps_hash(&[a, b]), scope_key_hash(&pair));
    }

    #[test]
    fn canonical_f64_bits_normalizes_zero_and_nan() {
        assert_eq!(canonical_f64_bits(0.0), 0.0f64.to_bits());
        assert_eq!(canonical_f64_bits(-0.0), 0.0f64.to_bits()); // -0 → +0
        assert_eq!(canonical_f64_bits(f64::NAN), f64::NAN.to_bits());
        assert_eq!(canonical_f64_bits(1.5), 1.5f64.to_bits());
    }

    #[test]
    fn scope_key_from_f64_splits_int_and_float() {
        // 整数 < 2^53 → Int; 其余 → Float(规范化位)
        assert_eq!(scope_key_from_f64(3.0), k_int(3));
        assert!(matches!(scope_key_from_f64(0.5), ScopeKey::Float(_)));
        assert!(matches!(
            scope_key_from_f64(9_007_199_254_740_992.0),
            ScopeKey::Float(_)
        )); // == 2^53: 不精确 → Float
    }

    #[test]
    fn comps_match_resolves_left_deep_ranges() {
        // Empty ↔ 空区间
        assert!(comps_match(&ScopeKey::Empty, &[], 0, 0));
        assert!(!comps_match(&ScopeKey::Empty, &[k_int(1)], 0, 1));
        // 单叶 ↔ 单元素区间
        assert!(comps_match(&k_int(1), &[k_int(1)], 0, 1));
        assert!(!comps_match(&k_int(1), &[k_int(2)], 0, 1));
        // 左深 Pair: 右叶 = comps[end-1], 左子树匹配前缀
        let pair = ScopeKey::Pair(Box::new(k_int(1)), Box::new(k_str("y")));
        let comps = vec![k_int(1), k_str("y")];
        assert!(comps_match(&pair, &comps, 0, 2));
        // 区间只覆盖右叶时左子树失配
        assert!(!comps_match(&pair, &comps, 1, 2));
        // 空区间对非 Empty scope → false
        assert!(!comps_match(&pair, &comps, 1, 1));
    }

    #[test]
    fn scope_key_from_comps_builds_left_deep_tree() {
        assert_eq!(scope_key_from_comps(&[]), ScopeKey::Empty);
        assert_eq!(scope_key_from_comps(&[k_int(1)]), k_int(1));
        let comps = vec![k_int(1), k_str("y"), k_float(0.5)];
        let tree = scope_key_from_comps(&comps);
        // 与 comps_match 互认: 整区间命中
        assert!(comps_match(&tree, &comps, 0, 3));
        // 与 scope_key_hash/comps_hash 同构
        assert_eq!(scope_key_hash(&tree), comps_hash(&comps));
    }

    #[test]
    fn value_number_projections() {
        assert_eq!(value_to_i128(&Value::Number(7.0)), Some(7));
        assert_eq!(value_to_i128(&Value::Str("x".into())), None);
        assert_eq!(value_to_f64(&Value::Number(1.5)), Some(1.5));
        assert_eq!(value_to_f64(&Value::Bool(true)), None);
    }
}
