use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use sha2::{Digest, Sha256};
use wf_lang::ast::{BinOp, CmpOp, Expr};

use crate::time::epoch_nanos_to_millis;

use super::super::key::value_to_string;
use super::super::types::{EngineHashMap, FieldSource, RollingStats, Value, WindowLookup};
use super::eval_expr_ext;

/// 数值相等：**epsilon 容差**（既有契约：`0.1 + 0.2 == 0.3` 为真，见
/// `sem_tests::eval_coverage::cmp_number_boundaries` 与
/// `columnar_tests_guard::epsilon_equality_matches_interpreted_on_floats`）
/// 叠加 IEEE 精确相等（`a == b` 保证 `inf == inf` 为真；`NaN` 两项皆假）。
pub fn numeric_eq(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() < f64::EPSILON
}

/// 数值不等：恒为 `!numeric_eq`（`Ne ≡ !Eq`）。
///
/// 此前多处写成 `(a - b).abs() >= f64::EPSILON`：对 `NaN` 与 `inf == inf` 会和 `Eq`
/// **同时为假**，于是 `a != b` 与 `!(a == b)` 得出相反结果（`Not` 走 `eval_not`，
/// 比较式是合法 Bool 操作数 —— 两种同义写法必须同结果）。`NaN != x → true` 也与
/// IEEE 754 及主流语言一致。
pub fn numeric_ne(a: f64, b: f64) -> bool {
    !numeric_eq(a, b)
}

/// 数值比较的**唯一实现**：行式（`cep`）/ 统计阈值（`step`）/ 列式
/// （`columnar_eval`）/ 契约断言（`contract`）四处共用。
///
/// `Lt/Gt/Le/Ge` 保持 IEEE 精确序 —— 容差只作用于 `Eq`/`Ne`，因此亚 epsilon 带内可能
/// 出现 `Eq` 真而 `Ge` 假（例：`0.3` vs `0.1 + 0.2`）。这是既有语义，未在此扩展
/// （扩展会改变所有 `where(agg >= thr)` 的阈值行为）。
pub fn numeric_cmp(cmp: CmpOp, a: f64, b: f64) -> bool {
    match cmp {
        CmpOp::Eq => numeric_eq(a, b),
        CmpOp::Ne => numeric_ne(a, b),
        CmpOp::Lt => a < b,
        CmpOp::Gt => a > b,
        CmpOp::Le => a <= b,
        CmpOp::Ge => a >= b,
        _ => false,
    }
}

/// [`numeric_cmp`] 的 `BinOp` 入口。非比较算子一律 `false`（不再沿用
/// `CmpOp::from_binop` 对非比较算子回退 `CmpOp::Eq` 的旧行为）。
pub fn numeric_cmp_binop(op: BinOp, a: f64, b: f64) -> bool {
    match op {
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
            numeric_cmp(CmpOp::from_binop(op), a, b)
        }
        _ => false,
    }
}

/// `Value` 层面的比较（行式解释器；列式路径复用本函数，见 `columnar` 的
/// `compare_scalars`）。数值走 [`numeric_cmp_binop`]（epsilon），字符串按字典序，
/// 布尔仅支持等/不等。
///
/// **结构化值递归结构相等**（`Array`/`Object`，见 [`super::values_equal`]）：
/// `Eq`/`Ne` 有真实语义，顺序比较在语言层被检查器拒绝（T8 要求数值）→ `false`。
/// 类型不匹配（标量 vs 结构化、不同标量类型）不声称相等，`Ne` 取其补（`Ne ≡ !Eq`）。
pub fn compare_values(op: BinOp, lv: &Value, rv: &Value) -> bool {
    match (lv, rv) {
        (Value::Number(a), Value::Number(b)) => numeric_cmp_binop(op, *a, *b),
        // `Int` 对 `Int` 走精确整数比较（不经 f64，避免 >2^53 量化错判）。
        (Value::Int(a), Value::Int(b)) => compare_int_ints(op, *a, *b),
        // 混合数值：`Int` 升为 f64 后走同一 epsilon 路径（`|i| < 2^53` 精确）。
        (Value::Int(a), Value::Number(b)) => numeric_cmp_binop(op, *a as f64, *b),
        (Value::Number(a), Value::Int(b)) => numeric_cmp_binop(op, *a, *b as f64),
        (Value::Str(a), Value::Str(b)) => compare_strs(op, a, b),
        (Value::Bool(a), Value::Bool(b)) => compare_bools(op, *a, *b),
        (Value::Array(_) | Value::Object(_), Value::Array(_) | Value::Object(_)) => match op {
            BinOp::Eq => super::values_equal(lv, rv),
            BinOp::Ne => !super::values_equal(lv, rv),
            _ => false,
        },
        // 不可比较（标量 vs 结构化 / 不同标量类型）：不声称相等，故 `Ne` 为真。
        _ => op == BinOp::Ne,
    }
}

/// 字符串按字典序比较六种关系。
fn compare_strs(op: BinOp, a: &str, b: &str) -> bool {
    let ord = a.cmp(b);
    match op {
        BinOp::Eq => ord.is_eq(),
        BinOp::Ne => !ord.is_eq(),
        BinOp::Lt => ord.is_lt(),
        BinOp::Gt => ord.is_gt(),
        BinOp::Le => ord.is_le(),
        BinOp::Ge => ord.is_ge(),
        _ => false,
    }
}

/// 布尔仅支持等/不等。
fn compare_bools(op: BinOp, a: bool, b: bool) -> bool {
    match op {
        BinOp::Eq => a == b,
        BinOp::Ne => a != b,
        _ => false,
    }
}

/// 精确整数比较（`Int` 对 `Int`）：不经 f64，`|i| >= 2^53` 时不发生量化错判。
fn compare_int_ints(op: BinOp, a: i64, b: i64) -> bool {
    match op {
        BinOp::Eq => a == b,
        BinOp::Ne => a != b,
        BinOp::Lt => a < b,
        BinOp::Gt => a > b,
        BinOp::Le => a <= b,
        BinOp::Ge => a >= b,
        _ => false,
    }
}

/// Helper trait to convert BinOp comparison variants to CmpOp.
trait FromBinOp {
    fn from_binop(op: BinOp) -> Self;
}

impl FromBinOp for CmpOp {
    fn from_binop(op: BinOp) -> Self {
        match op {
            BinOp::Eq => CmpOp::Eq,
            BinOp::Ne => CmpOp::Ne,
            BinOp::Lt => CmpOp::Lt,
            BinOp::Gt => CmpOp::Gt,
            BinOp::Le => CmpOp::Le,
            BinOp::Ge => CmpOp::Ge,
            _ => CmpOp::Eq, // fallback (should not be reached for comparison ops)
        }
    }
}

// ---------------------------------------------------------------------------
// Threshold expression evaluation
// ---------------------------------------------------------------------------

/// Try to evaluate a threshold expression to f64.
/// Returns `Some(f64)` for Number, Neg, and constant arithmetic (BinOp on
/// numeric literals).  Returns `None` for expressions that cannot be
/// statically resolved to a number (field refs, function calls, etc.)
/// — callers must fall back to value-based comparison.
///
/// 折叠规则的定义在 `wf_lang::const_fold`（单一真源）：checker 的阈值常量性判定
/// 与本函数共用同一份规则，避免两处漂移（warp-fusion#101）。
pub fn try_eval_expr_to_f64(expr: &Expr) -> Option<f64> {
    wf_lang::const_fold::try_eval_expr_to_f64(expr)
}

/// Try to evaluate a threshold expression to a [`Value`].
/// Returns `Some` for literal constants (Number, String, Bool) and
/// constant arithmetic (Neg, BinOp on numeric literals).
/// Returns `None` for non-constant expressions (field refs, func calls, etc.)
/// — `check_threshold` 据此判为「不满足」（分支永不触发）。
pub fn try_eval_expr_to_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone().into())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        _ => try_eval_expr_to_f64(expr).map(Value::Number),
    }
}

pub(super) fn normalize_index(index: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let normalized = if index < 0 { len + index } else { index };
    if normalized < 0 || normalized >= len {
        None
    } else {
        Some(normalized as usize)
    }
}

pub(super) fn compare_sortable_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        // 数值域统一：`Int`/`Number` 同序，避免两者都落到文本比较（`"10" < "9"`）。
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Int(x), Value::Number(y)) => (*x as f64)
            .partial_cmp(y)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Number(x), Value::Int(y)) => x
            .partial_cmp(&(*y as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Str(x), Value::Str(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => value_to_string(a).cmp(&value_to_string(b)),
    }
}

pub(super) fn f64_to_i64_trunc(v: f64) -> Option<i64> {
    if !v.is_finite() {
        return None;
    }
    let truncated = v.trunc();
    if truncated < i64::MIN as f64 || truncated > i64::MAX as f64 {
        return None;
    }
    Some(truncated as i64)
}

pub(super) fn round_with_precision(value: f64, precision: i64) -> Option<f64> {
    if !value.is_finite() {
        return None;
    }
    let p = i32::try_from(precision.unsigned_abs()).ok()?;
    let factor = 10_f64.powi(p);
    if !factor.is_finite() || factor == 0.0 {
        return None;
    }
    if precision >= 0 {
        Some((value * factor).round() / factor)
    } else {
        Some((value / factor).round() * factor)
    }
}

pub fn timestamp_nanos_to_utc(timestamp_nanos: i64) -> Option<DateTime<Utc>> {
    let secs = timestamp_nanos.div_euclid(1_000_000_000);
    let nanos = timestamp_nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
}

pub(super) fn time_nanos_to_value(nanos: i64) -> Value {
    Value::Number(epoch_nanos_to_millis(nanos) as f64)
}

pub(super) fn parse_time_to_timestamp_nanos(text: &str, fmt: &str) -> Option<i64> {
    if let Ok(dt) = DateTime::parse_from_str(text, fmt) {
        return dt.timestamp_nanos_opt();
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(text, fmt) {
        return dt.and_utc().timestamp_nanos_opt();
    }
    if let Ok(date) = NaiveDate::parse_from_str(text, fmt) {
        return date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_nanos_opt();
    }
    None
}

pub(super) fn current_time_nanos() -> Option<i64> {
    super::EVAL_TIME_NANOS.with(|time| {
        if let Some(nanos) = time.get() {
            return Some(nanos);
        }
        let nanos = Utc::now().timestamp_nanos_opt()?;
        time.set(Some(nanos));
        Some(nanos)
    })
}

pub(super) fn is_blank_str(value: &str) -> bool {
    value.trim().is_empty()
}

pub(super) fn eval_single_string_arg(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<String> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Str(s) => Some(s.to_string()),
        _ => None,
    }
}

pub fn update_stable_id_hash(hasher: &mut Sha256, value: &Value) -> Option<()> {
    let (tag, text) = match value {
        // 稳定 ID：`Int(i)` 与整值 `Number` 必须产出同一字节流（同 tag、同文本）
        // —— `value_to_string` 对两者都渲染十进制文本。
        Value::Number(_) | Value::Int(_) => ("n", value_to_string(value)),
        Value::Str(s) => ("s", s.to_string()),
        Value::Bool(_) => ("b", value_to_string(value)),
        Value::Array(_) | Value::Object(_) => return None,
    };
    hasher.update(tag.as_bytes());
    hasher.update(b":");
    hasher.update(text.len().to_string().as_bytes());
    hasher.update(b":");
    hasher.update(text.as_bytes());
    hasher.update(b";");
    Some(())
}

pub fn apply_fmt_template(template: &str, values: &[Value]) -> Option<String> {
    let placeholders = template.matches("{}").count();
    if placeholders != values.len() {
        return None;
    }
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;
    for value in values {
        let (head, tail) = rest.split_once("{}")?;
        rendered.push_str(head);
        rendered.push_str(&value_to_string(value));
        rest = tail;
    }
    rendered.push_str(rest);
    Some(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cep::types::Event;
    use crate::value_extract::value_to_f64;
    use std::cmp::Ordering;
    use wf_lang::ast::FieldRef;

    fn num(v: f64) -> Value {
        Value::Number(v)
    }

    fn strv(v: &str) -> Value {
        Value::Str(v.into())
    }

    fn b(v: bool) -> Value {
        Value::Bool(v)
    }

    fn en(v: f64) -> Expr {
        Expr::Number(v)
    }

    fn bin(op: BinOp, l: Expr, r: Expr) -> Expr {
        Expr::BinOp {
            op,
            left: Box::new(l),
            right: Box::new(r),
        }
    }

    /// 单字段事件（`Event` 是 [`FieldSource`] 的生产实现，直接当测试替身用）。
    fn event_with_str(name: &str, value: &str) -> Event {
        let mut fields = EngineHashMap::default();
        fields.insert(name.into(), strv(value));
        Event { fields }
    }

    #[test]
    fn compare_values_strings_bools_and_mismatch() {
        assert!(compare_values(BinOp::Eq, &strv("a"), &strv("a")));
        assert!(compare_values(BinOp::Ne, &strv("a"), &strv("b")));
        assert!(compare_values(BinOp::Lt, &strv("a"), &strv("b")));
        assert!(compare_values(BinOp::Le, &strv("a"), &strv("a")));
        assert!(compare_values(BinOp::Gt, &strv("b"), &strv("a")));
        assert!(compare_values(BinOp::Ge, &strv("a"), &strv("a")));
        assert!(compare_values(BinOp::Eq, &num(1.0), &num(1.0 + 1e-16))); // epsilon 内相等
        assert!(compare_values(BinOp::Ne, &num(1.0), &num(1.0 + 1e-6)));
        assert!(compare_values(BinOp::Lt, &num(1.0), &num(1.000001)));
        assert!(compare_values(BinOp::Eq, &b(true), &b(true)));
        assert!(compare_values(BinOp::Ne, &b(true), &b(false)));
        // 类型不匹配 / 非比较 op → false
        assert!(!compare_values(BinOp::Eq, &num(1.0), &strv("1")));
        assert!(!compare_values(BinOp::Eq, &strv("a"), &b(true)));
        assert!(!compare_values(BinOp::Add, &strv("a"), &strv("b")));
        assert!(!compare_values(BinOp::Le, &b(false), &b(true)));
    }

    #[test]
    fn try_const_fold_arithmetic_and_neg() {
        assert_eq!(try_eval_expr_to_f64(&en(3.5)), Some(3.5));
        assert_eq!(
            try_eval_expr_to_f64(&Expr::Neg(Box::new(en(2.0)))),
            Some(-2.0)
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Add, en(1.0), bin(BinOp::Mul, en(2.0), en(3.0)))),
            Some(7.0)
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Div, en(1.0), en(0.0))),
            None
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Mod, en(1.0), en(0.0))),
            None
        );
        // 非数字常量表达式 → None
        assert_eq!(
            try_eval_expr_to_f64(&Expr::Field(FieldRef::Simple("x".into()))),
            None
        );
        assert_eq!(try_eval_expr_to_f64(&Expr::StringLit("1".into())), None);
    }

    #[test]
    fn try_eval_expr_to_value_literals() {
        assert_eq!(
            try_eval_expr_to_value(&Expr::StringLit("hi".into())),
            Some(strv("hi"))
        );
        assert_eq!(try_eval_expr_to_value(&Expr::Bool(true)), Some(b(true)));
        assert_eq!(
            try_eval_expr_to_value(&bin(BinOp::Mul, en(2.0), en(3.0))),
            Some(num(6.0))
        );
        assert_eq!(
            try_eval_expr_to_value(&Expr::Field(FieldRef::Simple("x".into()))),
            None
        );
    }

    /// 跨 crate 不变量：触发判定的可求值性必须与 checker 的阈值常量性判据一致
    /// （共用 `wf_lang::const_fold`）。两处一旦漂移，就会出现「编译期放行、运行期
    /// 永不触发」的静默失效（warp-fusion#101）。
    #[test]
    fn foldability_matches_checker_threshold_rule() {
        let cases = vec![
            en(5.0),
            Expr::Neg(Box::new(en(1.0))),
            bin(BinOp::Add, en(1.0), en(2.0)),
            bin(BinOp::Div, en(1.0), en(2.0)),
            bin(BinOp::Mul, bin(BinOp::Add, en(1.0), en(2.0)), en(3.0)),
            // 退化常量：折叠不出结果 → 两侧都判不可用
            bin(BinOp::Div, en(1.0), en(0.0)),
            bin(BinOp::Mod, en(1.0), en(0.0)),
            bin(BinOp::Eq, en(1.0), en(1.0)),
            // 非常量形态
            Expr::Field(FieldRef::Simple("x".into())),
            Expr::Neg(Box::new(Expr::Bool(true))),
            Expr::StringLit("s".into()),
            Expr::Bool(false),
        ];
        for expr in cases {
            assert_eq!(
                try_eval_expr_to_value(&expr).is_some(),
                wf_lang::const_fold::is_foldable_threshold(&expr),
                "可求值性与 checker 判据漂移: {expr:?}"
            );
        }
    }

    #[test]
    fn index_and_f64_truncation_helpers() {
        assert_eq!(normalize_index(-1, 5), Some(4));
        assert_eq!(normalize_index(0, 5), Some(0));
        assert_eq!(normalize_index(4, 5), Some(4));
        assert_eq!(normalize_index(5, 5), None);
        assert_eq!(normalize_index(-6, 5), None);
        assert_eq!(f64_to_i64_trunc(2.9), Some(2));
        assert_eq!(f64_to_i64_trunc(-2.9), Some(-2));
        assert_eq!(f64_to_i64_trunc(f64::INFINITY), None);
        assert_eq!(f64_to_i64_trunc(1e300), None);
        assert_eq!(value_to_f64(&num(1.5)), Some(1.5));
        assert_eq!(value_to_f64(&strv("1.5")), None);
    }

    #[test]
    fn round_with_precision_both_directions() {
        let close = |a: f64, b: f64| (a - b).abs() < 1e-9;
        assert!(close(round_with_precision(2.345, 2).unwrap(), 2.35));
        assert!(close(round_with_precision(2.5, 0).unwrap(), 3.0));
        assert!(close(round_with_precision(-2.5, 0).unwrap(), -3.0));
        assert!(close(round_with_precision(1234.0, -2).unwrap(), 1200.0));
        assert!(close(round_with_precision(1250.0, -2).unwrap(), 1300.0));
        assert_eq!(round_with_precision(f64::NAN, 2), None);
        assert_eq!(round_with_precision(1.0, 400), None); // 10^400 溢出 → None
    }

    #[test]
    fn ordering_and_format_and_time_helpers() {
        assert_eq!(
            compare_sortable_values(&num(1.0), &num(2.0)),
            Ordering::Less
        );
        assert_eq!(
            compare_sortable_values(&strv("b"), &strv("a")),
            Ordering::Greater
        );
        assert_eq!(compare_sortable_values(&b(false), &b(true)), Ordering::Less);
        assert_eq!(
            apply_fmt_template("a={} b={}", &[num(1.0), strv("x")]),
            Some("a=1 b=x".to_string())
        );
        assert_eq!(
            apply_fmt_template("no placeholders", &[]),
            Some("no placeholders".to_string())
        );
        assert_eq!(apply_fmt_template("{}", &[]), None);
        assert_eq!(apply_fmt_template("{}", &[num(1.0), num(2.0)]), None);
        assert!(is_blank_str("  \t "));
        assert!(!is_blank_str("x"));
        assert_eq!(
            parse_time_to_timestamp_nanos("2023-11-14 22:13:20", "%Y-%m-%d %H:%M:%S"),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(
            timestamp_nanos_to_utc(1_700_000_000_000_000_000).map(|dt| dt.timestamp()),
            Some(1_700_000_000)
        );
        let now = current_time_nanos().expect("now");
        assert!(now > 1_700_000_000_000_000_000);
        assert_eq!(current_time_nanos(), Some(now)); // 同一次求值内缓存
    }

    #[test]
    fn stable_id_hash_is_tagged_and_deterministic() {
        let hash_text = |v: &Value| {
            let mut h = Sha256::new();
            update_stable_id_hash(&mut h, v).unwrap();
            h.finalize().to_vec()
        };
        let a = hash_text(&strv("ab"));
        assert_eq!(a, hash_text(&strv("ab")));
        assert_ne!(a, hash_text(&strv("ac")));
        assert_ne!(hash_text(&num(1.0)), hash_text(&strv("1"))); // 类型标签参与
        // 容器类型不参与
        let mut h = Sha256::new();
        assert!(update_stable_id_hash(&mut h, &Value::Array(vec![num(1.0)])).is_none());
        assert!(update_stable_id_hash(&mut h, &Value::Object(EngineHashMap::default())).is_none());
    }
    #[test]
    fn const_fold_zero_guards_and_non_arithmetic_ops() {
        // 除/模零（含 -0.0）→ None; 非算术算子 → None（fold_f64_binop 提取回归）
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Div, en(1.0), en(-0.0))),
            None
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Mod, en(7.0), en(-0.0))),
            None
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Sub, en(5.0), en(2.0))),
            Some(3.0)
        );
        assert_eq!(
            try_eval_expr_to_f64(&bin(BinOp::Eq, en(1.0), en(1.0))),
            None
        );
        // 未知名路径: 与既有的字面折叠一致
        assert_eq!(
            try_eval_expr_to_f64(&Expr::Bool(true)),
            None,
            "Bool 字面量非数值"
        );
    }

    #[test]
    fn sortable_values_mixed_types_fall_back_to_string_order() {
        // 同类型走原生序（数值序）
        assert_eq!(
            compare_sortable_values(&num(9.0), &num(10.0)),
            Ordering::Less
        );
        // 跨类型退化为 value_to_string 的字典序："2" > "10"（不是数值序）
        assert_eq!(
            compare_sortable_values(&num(2.0), &strv("10")),
            Ordering::Greater
        );
        assert_eq!(
            compare_sortable_values(&b(true), &num(1.0)),
            Ordering::Greater
        );
        assert_eq!(
            compare_sortable_values(&strv("1"), &b(true)),
            Ordering::Less
        );
        // 容器无原生序，取 "[array]" 占位串参与比较
        assert_eq!(
            compare_sortable_values(&Value::Array(vec![]), &strv("z")),
            Ordering::Less
        );
    }

    #[test]
    fn time_nanos_to_value_is_millis_with_floor_semantics() {
        assert_eq!(
            time_nanos_to_value(1_700_000_000_123_456_789),
            num(1_700_000_000_123.0)
        );
        assert_eq!(time_nanos_to_value(999_999), num(0.0));
        // 负值按欧几里得取整（向 -inf），同一毫秒内的纳秒落回同一毫秒值
        assert_eq!(time_nanos_to_value(-1_500_000), num(-2.0));
        assert_eq!(time_nanos_to_value(-1), num(-1.0));
    }

    #[test]
    fn parse_time_offset_and_date_only_branches() {
        // 带时区偏移 → DateTime 分支
        assert_eq!(
            parse_time_to_timestamp_nanos("2023-11-14T22:13:20+00:00", "%Y-%m-%dT%H:%M:%S%:z"),
            Some(1_700_000_000_000_000_000)
        );
        // 仅日期 → NaiveDate 分支，补 00:00:00Z
        assert_eq!(
            parse_time_to_timestamp_nanos("2023-11-14", "%Y-%m-%d"),
            Some(1_699_920_000_000_000_000)
        );
        // 三个分支都不匹配 → None（调用方按「时间不可解析」处理）
        assert_eq!(parse_time_to_timestamp_nanos("nope", "%Y-%m-%d"), None);
        assert_eq!(
            parse_time_to_timestamp_nanos("2023-11-14", "%Y-%m-%d %H:%M:%S"),
            None
        );
    }

    #[test]
    fn eval_single_string_arg_requires_exactly_one_string() {
        let mut baselines = EngineHashMap::default();
        let event = event_with_str("msg", "hello");
        let arg = |e: Expr| vec![e];
        // 单个字符串实参：字面量 / 字段引用 → Some
        assert_eq!(
            eval_single_string_arg(
                &arg(Expr::StringLit("lit".into())),
                &event,
                None,
                &mut baselines
            ),
            Some("lit".to_string())
        );
        assert_eq!(
            eval_single_string_arg(
                &arg(Expr::Field(FieldRef::Simple("msg".into()))),
                &event,
                None,
                &mut baselines
            ),
            Some("hello".to_string())
        );
        // 非字符串实参（求值出 Number）→ None
        assert_eq!(
            eval_single_string_arg(&arg(en(1.0)), &event, None, &mut baselines),
            None
        );
        // 参数个数 != 1 → None（调用方据此短路，不进入求值）
        assert_eq!(
            eval_single_string_arg(&[], &event, None, &mut baselines),
            None
        );
        assert_eq!(
            eval_single_string_arg(
                &[Expr::StringLit("a".into()), Expr::StringLit("b".into())],
                &event,
                None,
                &mut baselines
            ),
            None
        );
    }
}
