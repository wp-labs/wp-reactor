//! 解释求值器（逐事件标量）：`cep` 状态机的 step / close / conv / key，以及
//! 列式路径的复杂分支回退（issue #82/#83 多层路径物化）都经此求值。
//!
//! 与之对应的**列式批量后端**在 `executor/eval/`（`builtins*` 等）：同名内建
//! 函数两处各有一份实现，语义必须一致——由 `match_engine/tests/`（eval_coverage、
//! l2 一致性）对拍守护。新增内建函数时两处同改。

use std::cell::Cell;

use wf_lang::ast::{BinOp, Expr, MatchArm, ObjectItem};

use super::key::{eval_field_value_src, field_ref_leaf_name, value_to_string};
use super::types::{EngineHashMap, FieldSource, RollingStats, Value, WindowLookup};

pub mod cmp; // engine columnar_eval 跨 crate 消费

mod funcs;
mod funcs_misc;
mod funcs_num;
mod funcs_str;
mod funcs_time;

use cmp::{coerce_to_f64, compare_values};
use funcs::eval_func_call;

// Re-export items from sub-modules that sibling modules (step, close) need.
pub(super) use cmp::{try_eval_expr_to_f64, try_eval_expr_to_value};

thread_local! {
    pub(super) static EVAL_TIME_NANOS: Cell<Option<i64>> = const { Cell::new(None) };
    static EVAL_TIME_SCOPE_DEPTH: Cell<usize> = const { Cell::new(0) };
}

struct EvalTimeScope;

impl EvalTimeScope {
    fn enter() -> Self {
        EVAL_TIME_SCOPE_DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self
    }
}

impl Drop for EvalTimeScope {
    fn drop(&mut self) {
        EVAL_TIME_SCOPE_DEPTH.with(|depth| {
            let next_depth = depth.get().saturating_sub(1);
            depth.set(next_depth);
            if next_depth == 0 {
                EVAL_TIME_NANOS.with(|time| time.set(None));
            }
        });
    }
}

fn with_eval_time_scope<T>(f: impl FnOnce() -> T) -> T {
    let _scope = EvalTimeScope::enter();
    f()
}

// ---------------------------------------------------------------------------
// Expression evaluator (L1)
// ---------------------------------------------------------------------------

/// Evaluate an expression against an event, returning a [`Value`].
///
/// Supports: literals, field refs, BinOp (And/Or/comparisons/arithmetic),
/// Neg, InList, and basic FuncCall (contains, startswith, endswith, substr, replace, trim, lower, upper, len, mvcount, mvjoin, mvindex, mvappend, split, mvdedup, abs, round, ceil, floor, sqrt, pow, log, exp, clamp, sign, trunc, is_finite, ltrim, rtrim, concat, join, join_by, indexof, replace_plain, startswith_any, endswith_any, coalesce, merge, isnull, isnotnull, is_blank, null_if_blank, default_if_blank, md5, sha1, sha1_n, sha256, hex, stable_id, mvsort, mvreverse, now, now_s, now_ms, now_us, now_ns, strftime, strptime, has, baseline).
pub fn eval_expr(expr: &Expr, event: &dyn FieldSource) -> Option<Value> {
    with_eval_time_scope(|| {
        let mut empty = EngineHashMap::default();
        eval_expr_ext(expr, event, None, &mut empty)
    })
}

/// Extended expression evaluator with window lookup and baseline store access.
///
/// All recursive calls go through this function (not `eval_expr`) to preserve
/// the `windows` and `baselines` context through compound expressions.
pub fn eval_expr_ext(
    expr: &Expr,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let _time_scope = EvalTimeScope::enter();
    match expr {
        Expr::Number(n) => Some(Value::Number(*n)),
        Expr::StringLit(s) => Some(Value::Str(s.clone().into())),
        Expr::Bool(b) => Some(Value::Bool(*b)),
        Expr::Field(fr) => eval_field_value_src(event, fr),
        Expr::Object(items) => eval_object_literal(items, event, windows, baselines),
        Expr::Array(items) => eval_array_literal(items, event, windows, baselines),
        Expr::Neg(inner) => eval_neg(inner, event, windows, baselines),
        Expr::Not(inner) => eval_not(inner, event, windows, baselines),
        Expr::BinOp { op, left, right } => eval_binop(*op, left, right, event, windows, baselines),
        Expr::InList {
            expr,
            list,
            negated,
        } => eval_in_list(expr, list, *negated, event, windows, baselines),
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => eval_func_call_expr(qualifier.as_deref(), name, args, event, windows, baselines),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => eval_if_then_else(cond, then_expr, else_expr, event, windows, baselines),
        Expr::Match {
            expr,
            arms,
            default,
        } => eval_match_expr(expr, arms, default.as_deref(), event, windows, baselines),
        _ => None,
    }
}

/// `object { k1 = e1; ... }` 字面量：逐 item 求值，一个 value 可写多个 target。
fn eval_object_literal(
    items: &[ObjectItem],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let mut map = EngineHashMap::default();
    for item in items {
        let value = eval_expr_ext(&item.value, event, windows, baselines)?;
        for target in &item.targets {
            map.insert(target.clone().into(), value.clone());
        }
    }
    Some(Value::Object(map))
}

/// `array [e1, ...]` 字面量：任一元素求值失败整体为 None。
fn eval_array_literal(
    items: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    items
        .iter()
        .map(|item| eval_expr_ext(item, event, windows, baselines))
        .collect::<Option<Vec<_>>>()
        .map(Value::Array)
}

/// 一元负号：仅数值可取反。
fn eval_neg(
    inner: &Expr,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let v = eval_expr_ext(inner, event, windows, baselines)?;
    match v {
        Value::Number(n) => Some(Value::Number(-n)),
        _ => None,
    }
}

/// 逻辑非：仅布尔可取反。
fn eval_not(
    inner: &Expr,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    match eval_expr_ext(inner, event, windows, baselines)? {
        Value::Bool(b) => Some(Value::Bool(!b)),
        _ => None,
    }
}

/// `expr in (...)` / `expr not in (...)`：任一成员等值即命中（成员通常为字面量）。
fn eval_in_list(
    target: &Expr,
    list: &[Expr],
    negated: bool,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let target_val = eval_expr_ext(target, event, windows, baselines)?;
    // InList items are typically literals — context not needed, but
    // we pass it for correctness in case of field refs / func calls.
    let found = list.iter().any(|item| {
        eval_expr_ext(item, event, windows, baselines)
            .map(|v| values_equal(&target_val, &v))
            .unwrap_or(false)
    });
    Some(Value::Bool(if negated { !found } else { found }))
}

/// 函数调用分派：`window.has(...)` 与 `baseline(...)` 有专门路径，其余走
/// `eval_func_call`（funcs.rs 的内建分派表）。
fn eval_func_call_expr(
    qualifier: Option<&str>,
    name: &str,
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if let Some(window_name) = qualifier
        && name == "has"
    {
        return eval_window_has(window_name, args, event, windows);
    }
    if name == "baseline" && (args.len() == 2 || args.len() == 3) {
        return eval_baseline(args, event, baselines);
    }
    eval_func_call(name, args, event, windows, baselines)
}

/// `if cond then yes else no`：cond 必须为布尔。
fn eval_if_then_else(
    cond: &Expr,
    then_expr: &Expr,
    else_expr: &Expr,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let cond_val = eval_expr_ext(cond, event, windows, baselines);
    match cond_val {
        Some(Value::Bool(true)) => eval_expr_ext(then_expr, event, windows, baselines),
        Some(Value::Bool(false)) => eval_expr_ext(else_expr, event, windows, baselines),
        _ => None,
    }
}

/// `match <subject> { pat => arm, ... , _ => default }`：模式按值比较（同
/// `in` 的相等语义），短路命中；无默认且未命中 → None。
fn eval_match_expr(
    expr: &Expr,
    arms: &[MatchArm],
    default: Option<&Expr>,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let subject = eval_expr_ext(expr, event, windows, baselines)?;
    for arm in arms {
        let hit = arm.patterns.iter().any(|pattern| {
            eval_expr_ext(pattern, event, windows, baselines)
                .map(|v| values_equal(&subject, &v))
                .unwrap_or(false)
        });
        if hit {
            return eval_expr_ext(&arm.value, event, windows, baselines);
        }
    }
    match default {
        Some(d) => eval_expr_ext(d, event, windows, baselines),
        None => None,
    }
}

/// Evaluate `window.has(expr [, "field"])`.
fn eval_window_has(
    window_name: &str,
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
) -> Option<Value> {
    let windows = windows?;
    let lookup_val = eval_expr(&args[0], event)?;
    let lookup_str = value_to_string(&lookup_val);

    // Explicit field name from 2nd arg, or infer from the field ref in 1st arg.
    // For a nested path the inferred column is the leaf member (mirroring how
    // `e.sip` infers `sip`), not the root object field.
    let field_name = match args.get(1) {
        Some(Expr::StringLit(f)) => f.clone(),
        Some(_) => return None,
        None => match &args[0] {
            Expr::Field(fr) => field_ref_leaf_name(fr)?.to_string(),
            _ => return None,
        },
    };

    let values = windows.snapshot_field_values(window_name, &field_name)?;
    Some(Value::Bool(values.contains(&lookup_str)))
}

/// Evaluate `baseline(expr, duration_seconds [, method])`.
///
/// Computes the z-score (number of standard deviations from the running mean)
/// of the current value, then updates the running statistics.
///
/// Supported methods: "mean" (default), "ewma", "median"
fn eval_baseline(
    args: &[Expr],
    event: &dyn FieldSource,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let current_val = match eval_expr(&args[0], event)? {
        Value::Number(n) => n,
        _ => return None,
    };

    // Parse optional method argument (default to "mean")
    let method = args
        .get(2)
        .and_then(|arg| match arg {
            Expr::StringLit(s) => Some(s.as_str()),
            _ => None,
        })
        .unwrap_or("mean");

    // Build a key to identify this baseline expression (including method)
    let key = format!("{:?}:{}", args[0], method);

    let stats = baselines
        .entry(key)
        .or_insert_with(|| RollingStats::new_with_method(method));
    let deviation = stats.deviation(current_val);
    stats.update(current_val);
    Some(Value::Number(deviation))
}

fn eval_binop(
    op: BinOp,
    left: &Expr,
    right: &Expr,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    match op {
        BinOp::And => eval_logic_and(left, right, event, windows, baselines),
        BinOp::Or => eval_logic_or(left, right, event, windows, baselines),
        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
            let lv = eval_expr_ext(left, event, windows, baselines)?;
            let rv = eval_expr_ext(right, event, windows, baselines)?;
            Some(Value::Bool(compare_values(op, &lv, &rv)))
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
            let lv = eval_expr_ext(left, event, windows, baselines)?;
            let rv = eval_expr_ext(right, event, windows, baselines)?;
            let ln = coerce_to_f64(&lv)?;
            let rn = coerce_to_f64(&rv)?;
            eval_arithmetic(op, ln, rn)
        }
        _ => None,
    }
}

/// Three-valued (SQL NULL) logical AND.
///
/// Both sides are always evaluated so that partial information is preserved.
/// This is essential for close-step guards where one side references an
/// event field (missing at close time) and the other references
/// close_reason (missing during accumulation).
fn eval_logic_and(
    left: &Expr,
    right: &Expr,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let lv = eval_expr_ext(left, event, windows, baselines);
    let rv = eval_expr_ext(right, event, windows, baselines);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(Value::Bool(false)), _) | (_, Some(Value::Bool(false))) => Some(Value::Bool(false)),
        (Some(Value::Bool(true)), Some(Value::Bool(true))) => Some(Value::Bool(true)),
        _ => None,
    }
}

/// Three-valued (SQL NULL) logical OR.
fn eval_logic_or(
    left: &Expr,
    right: &Expr,
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    let lv = eval_expr_ext(left, event, windows, baselines);
    let rv = eval_expr_ext(right, event, windows, baselines);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(Value::Bool(true)), _) | (_, Some(Value::Bool(true))) => Some(Value::Bool(true)),
        (Some(Value::Bool(false)), Some(Value::Bool(false))) => Some(Value::Bool(false)),
        _ => None,
    }
}

/// Arithmetic on two numeric values: +, -, *, /, %.
fn eval_arithmetic(op: BinOp, lv: f64, rv: f64) -> Option<Value> {
    let result = match op {
        BinOp::Add => lv + rv,
        BinOp::Sub => lv - rv,
        BinOp::Mul => lv * rv,
        BinOp::Div => {
            if rv == 0.0 {
                return None;
            }
            lv / rv
        }
        BinOp::Mod => {
            if rv == 0.0 {
                return None;
            }
            lv % rv
        }
        _ => return None,
    };
    Some(Value::Number(result))
}

/// Equality check for InList membership.
pub fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => (x - y).abs() < f64::EPSILON,
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cep::types::Event;
    use wf_lang::ast::{FieldRef, MatchArm, ObjectItem};

    fn empty_event() -> Event {
        Event {
            fields: EngineHashMap::default(),
        }
    }

    fn eval_ok(expr: &Expr) -> Value {
        eval_expr(expr, &empty_event()).expect("expr should evaluate")
    }

    fn num(v: f64) -> Expr {
        Expr::Number(v)
    }

    fn field(name: &str) -> Expr {
        Expr::Field(FieldRef::Simple(name.to_string()))
    }

    #[test]
    fn binop_arithmetic_and_zero_division() {
        assert_eq!(
            eval_ok(&Expr::BinOp {
                op: BinOp::Add,
                left: Box::new(num(1.0)),
                right: Box::new(num(2.0)),
            }),
            Value::Number(3.0)
        );
        assert_eq!(
            eval_arithmetic(BinOp::Sub, 5.0, 3.0),
            Some(Value::Number(2.0))
        );
        assert_eq!(
            eval_arithmetic(BinOp::Mul, 2.0, 4.0),
            Some(Value::Number(8.0))
        );
        assert_eq!(
            eval_arithmetic(BinOp::Mod, 5.0, 2.0),
            Some(Value::Number(1.0))
        );
        // 除零 / 模零 → None
        assert_eq!(eval_arithmetic(BinOp::Div, 1.0, 0.0), None);
        assert_eq!(eval_arithmetic(BinOp::Mod, 1.0, 0.0), None);
        // 非算术 op → None（eval_binop 不会下发，纯函数自身防御）
        assert_eq!(eval_arithmetic(BinOp::Eq, 1.0, 1.0), None);
    }

    #[test]
    fn in_list_membership_and_negation() {
        let in_list = Expr::InList {
            expr: Box::new(num(2.0)),
            list: vec![num(1.0), num(2.0), num(3.0)],
            negated: false,
        };
        assert_eq!(eval_ok(&in_list), Value::Bool(true));
        // 值相等按 epsilon 语义：2.0 命中
        assert_eq!(
            eval_ok(&Expr::InList {
                expr: Box::new(num(9.0)),
                list: vec![num(1.0), num(2.0)],
                negated: false,
            }),
            Value::Bool(false)
        );
        // negated 翻转
        assert_eq!(
            eval_ok(&Expr::InList {
                expr: Box::new(num(9.0)),
                list: vec![num(1.0)],
                negated: true,
            }),
            Value::Bool(true)
        );
        // 成员求值失败（含非法字段）不 panic、视为未命中
        assert_eq!(
            eval_ok(&Expr::InList {
                expr: Box::new(num(1.0)),
                list: vec![field("missing")],
                negated: false,
            }),
            Value::Bool(false)
        );
    }

    #[test]
    fn if_then_else_and_unary_ops() {
        let ite = |cond: Expr| Expr::IfThenElse {
            cond: Box::new(cond),
            then_expr: Box::new(num(1.0)),
            else_expr: Box::new(num(2.0)),
        };
        assert_eq!(eval_ok(&ite(Expr::Bool(true))), Value::Number(1.0));
        assert_eq!(eval_ok(&ite(Expr::Bool(false))), Value::Number(2.0));
        assert_eq!(eval_expr(&ite(num(3.0)), &empty_event()), None); // 非布尔条件
        assert_eq!(eval_ok(&Expr::Neg(Box::new(num(3.0)))), Value::Number(-3.0));
        assert_eq!(
            eval_expr(
                &Expr::Neg(Box::new(Expr::StringLit("x".into()))),
                &empty_event()
            ),
            None
        );
        assert_eq!(
            eval_ok(&Expr::Not(Box::new(Expr::Bool(false)))),
            Value::Bool(true)
        );
        assert_eq!(
            eval_expr(&Expr::Not(Box::new(num(1.0))), &empty_event()),
            None
        );
    }

    #[test]
    fn match_expr_short_circuits_and_default() {
        let arms = vec![MatchArm {
            patterns: vec![num(2.0)],
            value: Expr::StringLit("two".into()),
        }];
        let m = |subject: Expr, default: Option<Expr>| Expr::Match {
            expr: Box::new(subject),
            arms: arms.clone(),
            default: default.map(Box::new),
        };
        // 命中分支
        assert_eq!(
            eval_ok(&m(num(2.0), Some(num(0.0)))),
            Value::Str("two".into())
        );
        // 未命中 → 默认分支
        assert_eq!(eval_ok(&m(num(9.0), Some(num(0.0)))), Value::Number(0.0));
        // 未命中且无默认 → None
        assert_eq!(eval_expr(&m(num(9.0), None), &empty_event()), None);
    }

    #[test]
    fn object_and_array_literals() {
        let array = Expr::Array(vec![num(1.0), num(2.0)]);
        assert_eq!(
            eval_ok(&array),
            Value::Array(vec![Value::Number(1.0), Value::Number(2.0)])
        );
        let object = Expr::Object(vec![ObjectItem {
            targets: vec!["a".to_string(), "b".to_string()],
            type_hint: None,
            value: num(7.0),
        }]);
        let Value::Object(map) = eval_ok(&object) else {
            panic!("expected object");
        };
        assert_eq!(map.get("a"), Some(&Value::Number(7.0)));
        assert_eq!(map.get("b"), Some(&Value::Number(7.0)));
    }
}
