//! rowkey — eval/ 子模块（从 eval.rs 拆分）。
use super::*;

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
            "bucket" => eval_bucket_key(args, row),
            "tier" => eval_tier_key(args, row),
            _ => None,
        },
        _ => None,
    }
}

/// `bucket(field, 'day'|'hour'|...)` — 时间下界（整数 nanos 桶）。
fn eval_bucket_key(args: &[Expr], row: &HashMap<String, Value>) -> Option<ScopeKey> {
    let field = args.first()?;
    let unit = bucket_unit_nanos(args.get(1)?)?;
    let v = field_value_of(field, row)?;
    let nanos = v as i64;
    Some(ScopeKey::Int((nanos / unit) * unit))
}

/// `tier(field, b1, b2, ...)` — 区间桶索引（边界升序, `v < b_i` 归属 i）。
fn eval_tier_key(args: &[Expr], row: &HashMap<String, Value>) -> Option<ScopeKey> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }
    fn f(name: &str) -> Expr {
        Expr::Field(wf_lang::ast::FieldRef::Simple(name.into()))
    }
    fn call(name: &str, args: Vec<Expr>) -> Expr {
        Expr::FuncCall {
            qualifier: None,
            name: name.into(),
            args,
        }
    }
    fn num(n: f64) -> Expr {
        Expr::Number(n)
    }

    #[test]
    fn eval_row_key_single_and_composite() {
        // 单键字段
        let k = eval_row_key(&[f("a")], &row(&[("a", Value::Number(3.0))]));
        assert_eq!(k, Some(ScopeKey::Int(3)));
        // 复合键 → 左深 Pair
        let k = eval_row_key(
            &[f("a"), f("b")],
            &row(&[("a", Value::Number(3.0)), ("b", Value::Number(4.0))]),
        );
        assert!(matches!(k, Some(ScopeKey::Pair(..))));
        // 任一键缺失 → None（行跳过）
        assert_eq!(
            eval_row_key(&[f("a"), f("nope")], &row(&[("a", Value::Number(1.0))])),
            None
        );
    }

    #[test]
    fn bucket_key_floors_to_unit() {
        // bucket(ts, "day") → 天整数下界（nanos）
        let day = 86_400_000_000_000i64;
        let ts = (day * 3 + 5_000_000_000) as f64;
        let e = call("bucket", vec![f("ts"), Expr::StringLit("day".into())]);
        let k = eval_row_bucket_key(&e, &row(&[("ts", Value::Number(ts))]));
        assert_eq!(k, Some(ScopeKey::Int(day * 3)));
        // 未知单位 → None
        let e2 = call("bucket", vec![f("ts"), Expr::StringLit("week".into())]);
        assert_eq!(
            eval_row_bucket_key(&e2, &row(&[("ts", Value::Number(ts))])),
            None
        );
    }

    #[test]
    fn tier_key_indexes_ordered_bounds() {
        let e = call("tier", vec![f("v"), num(10.0), num(20.0)]);
        let r = |v: f64| eval_row_bucket_key(&e, &row(&[("v", Value::Number(v))]));
        assert_eq!(r(5.0), Some(ScopeKey::Int(0)));
        assert_eq!(r(15.0), Some(ScopeKey::Int(1)));
        assert_eq!(r(25.0), Some(ScopeKey::Int(2)));
    }

    #[test]
    fn unit_and_tier_pure_helpers() {
        assert_eq!(
            bucket_unit_nanos(&Expr::StringLit("hour".into())),
            Some(3_600_000_000_000)
        );
        assert_eq!(bucket_unit_nanos(&Expr::StringLit("week".into())), None);
        assert_eq!(tier_index(1.0, &[5.0, 10.0]), 0);
        assert_eq!(tier_index(7.0, &[5.0, 10.0]), 1);
        assert_eq!(tier_index(99.0, &[5.0, 10.0]), 2);
    }
}
