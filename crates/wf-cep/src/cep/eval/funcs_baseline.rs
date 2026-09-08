//! 近端 B 判定族（eval/funcs.rs 拆分）——`baseline_dev(entity, metric, value)`。
//!
//! judge 侧读取跨规则共享的 [`crate::baseline::BaselineStore`]（producer 收盘
//! 写入 / 启动 warm 载入）：返回 `value` 相对该 `(entity, metric)` 近期窗口
//! 基线的 z-score。无基线 / 参数非法 → None（where 求值即 false，不误报）。
//! 语义：见 baseline-online-design.md §11.2 S2-3/S2-4（合并函数唯一、判定热路径
//! 读内存）。

use super::super::key::value_to_string;
use super::super::types::{EngineHashMap, FieldSource, RollingStats, Value, WindowLookup};
use super::eval_expr_ext;
use wf_lang::ast::Expr;

pub(super) fn eval_func_baseline_dev(
    args: &[Expr],
    event: &dyn FieldSource,
    windows: Option<&dyn WindowLookup>,
    baselines: &mut EngineHashMap<String, RollingStats>,
) -> Option<Value> {
    if args.len() != 3 {
        return None;
    }
    let entity = value_to_string(&eval_expr_ext(&args[0], event, windows, baselines)?);
    let metric = value_to_string(&eval_expr_ext(&args[1], event, windows, baselines)?);
    let value = match eval_expr_ext(&args[2], event, windows, baselines)? {
        Value::Number(n) => n,
        _ => return None,
    };
    // 相位同窗：按事件时间（event_time 字段，epoch 纳秒）折叠相位桶——只与历史
    // 同期比较。事件无 event_time 字段 → None：相位关闭时无影响；相位开启时退化为
    // 全桶并集（保守：仅无时间来源的诊断场景）。
    let at = match event.field_value("event_time") {
        Some(Value::Number(n)) if n.is_finite() => Some(n as i64),
        _ => None,
    };
    crate::baseline::store()
        .deviation_at(&entity, &metric, value, at)
        .map(Value::Number)
}
