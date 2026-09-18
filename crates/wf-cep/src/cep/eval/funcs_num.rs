//! abs 族内置函数（eval/funcs.rs 拆分; 见 eval_func_call）

use super::super::types::EngineHashMap;
use super::super::types::FieldSource;
use super::super::types::RollingStats;
use super::super::types::Value;
use super::super::types::WindowLookup;
use super::cmp::f64_to_i64_trunc;
use super::cmp::round_with_precision;
use super::eval_expr_ext;
use wf_lang::ast::Expr;

pub(super) fn eval_func_abs(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    // 数值内置函数是 f64 域：`Value::Int` 经 `i as f64` 归一，走与 `Number` 相同路径。
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => Some(Value::Number(n.abs())),
        Value::Int(i) => Some(Value::Number((i as f64).abs())),
        _ => None,
    }
}
pub(super) fn eval_func_round(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 && args.len() != 2 {
        return None;
    }
    let value = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => n,
        Value::Int(i) => i as f64,
        _ => return None,
    };
    let precision = if args.len() == 2 {
        match eval_expr_ext(&args[1], event, windows, baselines)? {
            Value::Number(n) => f64_to_i64_trunc(n)?,
            Value::Int(i) => i,
            _ => return None,
        }
    } else {
        0
    };
    let rounded = round_with_precision(value, precision)?;
    Some(Value::Number(rounded))
}
pub(super) fn eval_func_ceil(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => Some(Value::Number(n.ceil())),
        Value::Int(i) => Some(Value::Number((i as f64).ceil())),
        _ => None,
    }
}
pub(super) fn eval_func_floor(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => Some(Value::Number(n.floor())),
        Value::Int(i) => Some(Value::Number((i as f64).floor())),
        _ => None,
    }
}
pub(super) fn eval_func_sqrt(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) if n >= 0.0 => Some(Value::Number(n.sqrt())),
        Value::Int(i) if (i as f64) >= 0.0 => Some(Value::Number((i as f64).sqrt())),
        _ => None,
    }
}
pub(super) fn eval_func_pow(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 2 {
        return None;
    }
    let x = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => n,
        Value::Int(i) => i as f64,
        _ => return None,
    };
    let y = match eval_expr_ext(&args[1], event, windows, baselines)? {
        Value::Number(n) => n,
        Value::Int(i) => i as f64,
        _ => return None,
    };
    let out = x.powf(y);
    if out.is_finite() {
        Some(Value::Number(out))
    } else {
        None
    }
}
pub(super) fn eval_func_log(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 && args.len() != 2 {
        return None;
    }
    let x = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => n,
        Value::Int(i) => i as f64,
        _ => return None,
    };
    if x <= 0.0 {
        return None;
    }
    let out = if args.len() == 2 {
        let base = match eval_expr_ext(&args[1], event, windows, baselines)? {
            Value::Number(n) => n,
            Value::Int(i) => i as f64,
            _ => return None,
        };
        if base <= 0.0 || (base - 1.0).abs() < f64::EPSILON {
            return None;
        }
        x.log(base)
    } else {
        x.ln()
    };
    if out.is_finite() {
        Some(Value::Number(out))
    } else {
        None
    }
}
pub(super) fn eval_func_exp(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    let x = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => n,
        Value::Int(i) => i as f64,
        _ => return None,
    };
    let out = x.exp();
    if out.is_finite() {
        Some(Value::Number(out))
    } else {
        None
    }
}
pub(super) fn eval_func_clamp(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 3 {
        return None;
    }
    let x = match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => n,
        Value::Int(i) => i as f64,
        _ => return None,
    };
    let min = match eval_expr_ext(&args[1], event, windows, baselines)? {
        Value::Number(n) => n,
        Value::Int(i) => i as f64,
        _ => return None,
    };
    let max = match eval_expr_ext(&args[2], event, windows, baselines)? {
        Value::Number(n) => n,
        Value::Int(i) => i as f64,
        _ => return None,
    };
    if min > max {
        return None;
    }
    Some(Value::Number(x.clamp(min, max)))
}
pub(super) fn eval_func_sign(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) if n.is_finite() => Some(Value::Number(n.signum())),
        Value::Int(i) => Some(Value::Number((i as f64).signum())),
        _ => None,
    }
}
pub(super) fn eval_func_trunc(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => Some(Value::Number(n.trunc())),
        Value::Int(i) => Some(Value::Number((i as f64).trunc())),
        _ => None,
    }
}
pub(super) fn eval_func_is_finite(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 1 {
        return None;
    }
    match eval_expr_ext(&args[0], event, windows, baselines)? {
        Value::Number(n) => Some(Value::Bool(n.is_finite())),
        Value::Int(_) => Some(Value::Bool(true)),
        _ => None,
    }
}
