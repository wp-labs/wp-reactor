//! `Value::Int` 归一语义（2026-09-18 精度修复，第 1 步）。
//!
//! 「`Int(i)` ≡ 整值 `Number(i as f64)`（`|i| < 2^53`）」必须在
//! **相等 / 键同一性 / 哈希 / 排序 / 字符串化 / 数值漏斗** 上处处一致。
//!
//! 任一处漏改，都会让同一逻辑值在不同路径（行式 vs 列式、比较 vs 去重、
//! 拼 ID vs 分片）上静默错配 —— 这正是 `Value::Int` 要根治的问题类型。

use std::hash::{Hash, Hasher};

use wf_lang::ast::BinOp;

use super::*;
use crate::value_extract::{value_to_f64, value_to_int};

/// `|i| < 2^53`：`i as f64` 精确无损，`Int` 与 `Number` 必须**完全等价**。
const EXACT_CASES: &[i64] = &[
    0,
    1,
    -1,
    42,
    -42,
    443,
    421_762,
    (1i64 << 53) - 1,
    -((1i64 << 53) - 1),
];

/// 超出 f64 精确整数域：`Number` 已无法精确表达，`Int` 必须保持精确
/// （这正是 epoch-ns ≈1.77e18 的处境）。
const BEYOND_F64_CASES: &[i64] = &[
    1i64 << 53,
    1_770_000_000_000_000_000,
    1_770_000_000_000_000_001,
    i64::MAX,
    i64::MIN,
];

fn hash_of<T: Hash>(v: &T) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

/// 相等/比较：`Int(i)` 与整值 `Number` 同值（六个比较关系都走数值域）。
#[test]
fn int_and_integral_number_compare_as_equal() {
    for &i in EXACT_CASES.iter().chain(BEYOND_F64_CASES) {
        let ints = Value::Int(i);
        let num = Value::Number(i as f64);
        assert!(values_equal(&ints, &num), "values_equal Int({i}) vs Number");
        assert!(values_equal(&num, &ints), "values_equal Number vs Int({i})");
        assert!(
            compare_values(BinOp::Eq, &ints, &num),
            "Eq Int({i}) vs Number"
        );
        assert!(
            !compare_values(BinOp::Ne, &ints, &num),
            "Ne Int({i}) vs Number"
        );
        assert!(
            compare_values(BinOp::Eq, &num, &ints),
            "Eq Number vs Int({i})"
        );
        assert!(
            !compare_values(BinOp::Ne, &num, &ints),
            "Ne Number vs Int({i})"
        );
    }
}

/// `Int` 对 `Int` 走精确整数比较：>2^53 时两个不同的整数不会被 f64 量化并列。
#[test]
fn int_comparison_is_exact_beyond_f64_precision() {
    let a = 1i64 << 53;
    let b = a + 1;
    assert_eq!(a as f64, b as f64, "前提：两者经 f64 量化后相同");

    assert!(!values_equal(&Value::Int(a), &Value::Int(b)));
    assert!(!compare_values(BinOp::Eq, &Value::Int(a), &Value::Int(b)));
    assert!(compare_values(BinOp::Ne, &Value::Int(a), &Value::Int(b)));
    assert!(compare_values(BinOp::Lt, &Value::Int(a), &Value::Int(b)));
    assert!(compare_values(BinOp::Gt, &Value::Int(b), &Value::Int(a)));

    // 与「量化后的 Number」仍等价（`Number` 只能表达量化值）。
    assert!(values_equal(&Value::Int(a), &Value::Number(a as f64)));
    assert!(values_equal(&Value::Int(b), &Value::Number(b as f64)));
}

/// 键同一性 + 哈希：`ValueKey` / `ScopeKey` / `JoinKey` 都必须把两个变体
/// 归一到同一个键（否则去重 / join / 分片会静默错配）。
#[test]
fn int_and_integral_number_share_identity_key() {
    for &i in EXACT_CASES {
        let ints = Value::Int(i);
        let num = Value::Number(i as f64);

        let ki = ValueKey::from_value(&ints);
        let kn = ValueKey::from_value(&num);
        assert_eq!(ki, kn, "ValueKey 同一 Int({i})/Number");
        assert_eq!(
            hash_of(&ki),
            hash_of(&kn),
            "ValueKey 哈希同一 Int({i})/Number"
        );

        let si = ScopeKey::from_value(&ints);
        let sn = ScopeKey::from_value(&num);
        assert_eq!(si, sn, "ScopeKey 同一 Int({i})/Number");
        assert_eq!(
            hash_of(&si),
            hash_of(&sn),
            "ScopeKey 哈希同一 Int({i})/Number"
        );

        assert_eq!(
            JoinKey::from_value(&ints),
            JoinKey::from_value(&num),
            "JoinKey 同一 Int({i})/Number"
        );
    }

    // 超出 f64 精确域：`ValueKey` 仍按 canonical 位归一到同一键
    // （`Number` 侧本就是量化后的值，故不产生新的错配）。
    for &i in BEYOND_F64_CASES {
        assert_eq!(
            ValueKey::from_value(&Value::Int(i)),
            ValueKey::from_value(&Value::Number(i as f64)),
            "ValueKey 同一 Int({i})/量化 Number"
        );
    }
}

/// 字符串化：`Int(i)` 与整值 `Number` 渲染为同一十进制文本（稳定 ID 依赖它）。
#[test]
fn int_and_integral_number_stringify_identically() {
    for &i in EXACT_CASES {
        let expected = i.to_string();
        assert_eq!(value_to_string(&Value::Int(i)), expected);
        assert_eq!(value_to_string(&Value::Number(i as f64)), expected);
    }
    // `Int` 始终渲染精确十进制（`Number` 在 >2^53 走 f64 Display）。
    assert_eq!(
        value_to_string(&Value::Int(1i64 << 53)),
        (1i64 << 53).to_string()
    );
    assert_eq!(value_to_string(&Value::Int(i64::MIN)), i64::MIN.to_string());
}

/// 数值漏斗：`value_to_f64` / `value_to_int` 必须接受 `Int`，且 `Int` 不走 f64 还原。
#[test]
fn int_and_integral_number_share_numeric_funnel() {
    for &i in EXACT_CASES {
        assert_eq!(value_to_f64(&Value::Int(i)), Some(i as f64));
        assert_eq!(value_to_int(Some(&Value::Int(i))), Some(i));
    }
    // 精确整数不经 f64 —— 超出 2^53 也不丢精度。
    let big = 1_770_000_000_000_000_001_i64;
    assert_eq!(value_to_int(Some(&Value::Int(big))), Some(big));
    assert_eq!(value_to_f64(&Value::Int(big)), Some(big as f64));
    // 非整值依旧拒绝。
    assert_eq!(value_to_int(Some(&Value::Number(7.5))), None);
    assert_eq!(value_to_int(Some(&Value::Str("7".into()))), None);
    assert_eq!(value_to_f64(&Value::Str("7".into())), None);
}

/// 排序：`Int` / `Number` 同序（不会双双落到文本比较）。
#[test]
fn int_and_integral_number_order_identically() {
    let cases: &[(i64, f64)] = &[(1, 2.0), (-5, -4.0), (0, 0.5)];
    for &(i, f) in cases {
        assert!(compare_values(BinOp::Lt, &Value::Int(i), &Value::Number(f)));
        assert!(compare_values(BinOp::Gt, &Value::Number(f), &Value::Int(i)));
        assert!(!compare_values(
            BinOp::Lt,
            &Value::Number(f),
            &Value::Int(i)
        ));
    }
}
