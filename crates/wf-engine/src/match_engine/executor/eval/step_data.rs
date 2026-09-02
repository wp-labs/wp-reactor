use smol_str::SmolStr;

use super::{FieldSource, Value};

/// Step/bind history accessors for the L3/aggregate eval family.
///
/// The eval context is a [`FieldSource`] (today the synthetic `Event` built by
/// `build_eval_context`; M3 adds a non-`Event` composite ctx). All history is
/// read through the trait's name protocol (`field_value` / `field_names`) —
/// synthetic `_step_{i}_*` / `_bind_{alias}_*` entries are resolved by name,
/// never by map-typed access, so any source that honours the same name protocol
/// (M2 §11.6) serves byte-identical results.
pub(super) fn flatten_bind_series(
    ctx: &dyn FieldSource,
    arg: Option<&wf_lang::ast::Expr>,
) -> Vec<Value> {
    let Some((alias, field_name)) = arg.and_then(extract_bind_field_ref) else {
        return Vec::new();
    };
    get_bind_field_values(ctx, alias, field_name).unwrap_or_default()
}

pub(super) fn flatten_step_values(ctx: &dyn FieldSource, step_indices: &[usize]) -> Vec<Value> {
    let mut out = Vec::new();
    for idx in step_indices {
        if let Some(values) = get_step_values(ctx, *idx) {
            out.extend(values);
        }
    }
    out
}

pub(super) fn flatten_step_series(
    ctx: &dyn FieldSource,
    step_indices: &[usize],
    arg: Option<&wf_lang::ast::Expr>,
) -> Vec<Value> {
    if let Some(field_name) = arg.and_then(extract_qualified_field_name) {
        let values = flatten_step_field_values(ctx, step_indices, field_name);
        if !values.is_empty() {
            return values;
        }
    }
    flatten_step_values(ctx, step_indices)
}

pub(super) fn flatten_step_field_values(
    ctx: &dyn FieldSource,
    step_indices: &[usize],
    field_name: &str,
) -> Vec<Value> {
    let mut out = Vec::new();
    for idx in step_indices {
        if let Some(values) = get_step_field_values(ctx, *idx, field_name) {
            out.extend(values);
        }
    }
    out
}

pub(super) fn resolve_step_indices(
    ctx: &dyn FieldSource,
    arg: Option<&wf_lang::ast::Expr>,
) -> Vec<usize> {
    let all = step_indices(ctx);
    if all.is_empty() {
        return all;
    }
    let Some(alias) = arg.and_then(extract_source_alias) else {
        return all;
    };
    all.iter()
        .copied()
        .filter(|idx| get_step_source(ctx, *idx).is_some_and(|s| s.as_str() == alias))
        .collect()
}

pub(super) fn resolve_aggregate_step_indices(
    ctx: &dyn FieldSource,
    arg: Option<&wf_lang::ast::Expr>,
) -> Vec<usize> {
    let all = step_indices(ctx);
    if all.is_empty() {
        return all;
    }
    let Some(step_ref) = arg.and_then(extract_step_ref) else {
        return Vec::new();
    };
    let by_source: Vec<usize> = all
        .iter()
        .copied()
        .filter(|idx| get_step_source(ctx, *idx).is_some_and(|s| s.as_str() == step_ref))
        .collect();
    if !by_source.is_empty() {
        return prefer_close_steps(ctx, by_source);
    }
    let by_label: Vec<usize> = all
        .iter()
        .copied()
        .filter(|idx| get_step_label(ctx, *idx).is_some_and(|label| label.as_str() == step_ref))
        .collect();
    prefer_close_steps(ctx, by_label)
}

fn prefer_close_steps(ctx: &dyn FieldSource, indices: Vec<usize>) -> Vec<usize> {
    let close_only: Vec<usize> = indices
        .iter()
        .copied()
        .filter(|idx| get_step_stage(ctx, *idx).is_some_and(|s| s.as_str() == "close"))
        .collect();
    if close_only.is_empty() {
        indices
    } else {
        close_only
    }
}

fn step_indices(ctx: &dyn FieldSource) -> Vec<usize> {
    let mut out: Vec<usize> = ctx
        .field_names()
        .into_iter()
        .filter_map(|k| parse_step_field_index(k, "_values"))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn parse_step_field_index(key: &str, suffix: &str) -> Option<usize> {
    let body = key.strip_prefix("_step_")?.strip_suffix(suffix)?;
    body.parse::<usize>().ok()
}

/// Owned collected values for a step (`_step_{i}_values`), moved out of the
/// source's `Value::Array`. FieldSource reads are owned, so callers never hold
/// a borrow into the source (the pre-M2 `&[Value]` borrows died with the
/// map-typed ctx).
pub(super) fn get_step_values(ctx: &dyn FieldSource, step_idx: usize) -> Option<Vec<Value>> {
    let field_name = format!("_step_{}_values", step_idx);
    match ctx.field_value(&field_name) {
        Some(Value::Array(arr)) => Some(arr),
        _ => None,
    }
}

pub(super) fn get_step_field_values(
    ctx: &dyn FieldSource,
    step_idx: usize,
    field_name: &str,
) -> Option<Vec<Value>> {
    let field_name = format!("_step_{}_field_{}", step_idx, field_name);
    match ctx.field_value(&field_name) {
        Some(Value::Array(arr)) => Some(arr),
        _ => None,
    }
}

pub(super) fn get_step_source(ctx: &dyn FieldSource, step_idx: usize) -> Option<SmolStr> {
    let field_name = format!("_step_{}_source", step_idx);
    match ctx.field_value(&field_name) {
        Some(Value::Str(s)) => Some(s),
        _ => None,
    }
}

pub(super) fn get_step_label(ctx: &dyn FieldSource, step_idx: usize) -> Option<SmolStr> {
    let field_name = format!("_step_{}_label", step_idx);
    match ctx.field_value(&field_name) {
        Some(Value::Str(s)) => Some(s),
        _ => None,
    }
}

pub(super) fn get_step_measure(ctx: &dyn FieldSource, step_idx: usize) -> Option<f64> {
    let field_name = format!("_step_{}_measure", step_idx);
    match ctx.field_value(&field_name) {
        Some(Value::Number(n)) => Some(n),
        _ => None,
    }
}

pub(super) fn get_step_stage(ctx: &dyn FieldSource, step_idx: usize) -> Option<SmolStr> {
    let field_name = format!("_step_{}_stage", step_idx);
    match ctx.field_value(&field_name) {
        Some(Value::Str(s)) => Some(s),
        _ => None,
    }
}

pub(super) fn get_bind_count(ctx: &dyn FieldSource, alias: &str) -> Option<f64> {
    let field_name = format!("_bind_{}_count", alias);
    match ctx.field_value(&field_name) {
        Some(Value::Number(n)) => Some(n),
        _ => None,
    }
}

pub(super) fn get_bind_field_values(
    ctx: &dyn FieldSource,
    alias: &str,
    field_name: &str,
) -> Option<Vec<Value>> {
    let field_name = format!("_bind_{}_field_{}", alias, field_name);
    match ctx.field_value(&field_name) {
        Some(Value::Array(arr)) => Some(arr),
        _ => None,
    }
}

pub(super) fn extract_source_alias(expr: &wf_lang::ast::Expr) -> Option<&str> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Qualified(alias, _)) | Expr::Field(FieldRef::Bracketed(alias, _)) => {
            Some(alias.as_str())
        }
        _ => None,
    }
}

pub(super) fn extract_step_ref(expr: &wf_lang::ast::Expr) -> Option<&str> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Simple(name)) => Some(name.as_str()),
        Expr::Field(FieldRef::Qualified(alias, _)) | Expr::Field(FieldRef::Bracketed(alias, _)) => {
            Some(alias.as_str())
        }
        _ => None,
    }
}

pub(super) fn extract_bind_ref(expr: &wf_lang::ast::Expr) -> Option<&str> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Simple(name)) => Some(name.as_str()),
        Expr::Field(FieldRef::Qualified(alias, _)) | Expr::Field(FieldRef::Bracketed(alias, _)) => {
            Some(alias.as_str())
        }
        _ => None,
    }
}

pub(super) fn extract_bind_field_ref(expr: &wf_lang::ast::Expr) -> Option<(&str, &str)> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Qualified(alias, field))
        | Expr::Field(FieldRef::Bracketed(alias, field)) => Some((alias.as_str(), field.as_str())),
        _ => None,
    }
}

pub(super) fn extract_qualified_field_name(expr: &wf_lang::ast::Expr) -> Option<&str> {
    use wf_lang::ast::{Expr, FieldRef};
    match expr {
        Expr::Field(FieldRef::Qualified(_, field)) | Expr::Field(FieldRef::Bracketed(_, field)) => {
            Some(field.as_str())
        }
        _ => None,
    }
}
