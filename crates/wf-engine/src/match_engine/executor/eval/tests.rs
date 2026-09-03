use super::builtins::{
    contains_system_var, eval_aggregate_func, eval_aggregate_over_numbers,
    eval_aggregate_over_values, eval_builtin_func_with_l3, eval_l3_func, eval_stat_func,
    is_stat_selector_func, materialize_system_vars, numeric_values, sum_numeric_values,
};
use super::utils;
use super::{
    Event, Value, YieldMeta, eval_bool_expr, eval_expr_with_l3, eval_yield_expr,
    eval_yield_expr_with_score, with_yield_eval_scope,
};
use super::{eval_entity_id, eval_score};
use crate::match_engine::EngineHashMap;
use sha2::{Digest, Sha256};
use wf_lang::ast::{BinOp, Expr, FieldRef, ObjectItem, SystemVar};
use wf_lang::wfu_meta::WfuMetaField;
// 子模块内 `super::` 引用原指向 eval 模块（tests.rs 曾为其直接子级）；拆深一层后
// 经本层 re-export 保语义（tests.rs 自身不直接用这些绑定）。
use super::{
    contains_aggregate_func, contains_eval_time_func, contains_l3_func, contains_stat_selector,
    get_or_init_eval_time_nanos,
};

#[path = "tests_yield_agg.rs"]
mod tests_yield_agg;

#[path = "tests_yield_funcs.rs"]
mod tests_yield_funcs;

#[path = "tests_builtin_str_mv.rs"]
mod tests_builtin_str_mv;

#[path = "tests_builtin_fmt_hash_time.rs"]
mod tests_builtin_fmt_hash_time;

#[path = "tests_builtin_agg_stat.rs"]
mod tests_builtin_agg_stat;

#[path = "tests_utils_l3_expr.rs"]
mod tests_utils_l3_expr;

fn lit(n: &str) -> Expr {
    Expr::StringLit(n.to_string())
}

fn field(name: &str) -> Expr {
    Expr::Field(FieldRef::Simple(name.to_string()))
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FuncCall {
        qualifier: None,
        name: name.to_string(),
        args,
    }
}

fn ctx_with(pairs: Vec<(&str, Value)>) -> Event {
    let mut fields = EngineHashMap::default();
    for (k, v) in pairs {
        fields.insert(k.into(), v);
    }
    Event { fields }
}

fn l3_ctx(expr: &Expr, ctx: &Event) -> Option<Value> {
    eval_expr_with_l3(expr, ctx, YieldMeta::default())
}

fn make_test_event(values: Vec<Value>) -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("_step_0_values".into(), Value::Array(values));
    fields.insert("_step_0_source".into(), Value::Str("e".into()));
    Event { fields }
}

fn arr(values: Vec<Value>) -> Value {
    Value::Array(values)
}

fn step_ctx(values: Vec<Value>) -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("_step_0_values".into(), Value::Array(values));
    fields.insert("_step_0_source".into(), Value::Str("e".into()));
    fields.insert("_step_0_label".into(), Value::Str("fail".into()));
    fields.insert("_step_0_measure".into(), Value::Number(6.0));
    Event { fields }
}
