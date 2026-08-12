use super::{Event, Value};

pub(super) fn flatten_bind_series(ctx: &Event, arg: Option<&wf_lang::ast::Expr>) -> Vec<Value> {
    let Some((alias, field_name)) = arg.and_then(extract_bind_field_ref) else {
        return Vec::new();
    };
    get_bind_field_values(ctx, alias, field_name)
        .map(|values| values.to_vec())
        .unwrap_or_default()
}

pub(super) fn flatten_step_values(ctx: &Event, step_indices: &[usize]) -> Vec<Value> {
    let mut out = Vec::new();
    for idx in step_indices {
        if let Some(values) = get_step_values(ctx, *idx) {
            out.extend_from_slice(values);
        }
    }
    out
}

pub(super) fn flatten_step_series(
    ctx: &Event,
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
    ctx: &Event,
    step_indices: &[usize],
    field_name: &str,
) -> Vec<Value> {
    let mut out = Vec::new();
    for idx in step_indices {
        if let Some(values) = get_step_field_values(ctx, *idx, field_name) {
            out.extend_from_slice(values);
        }
    }
    out
}

pub(super) fn resolve_step_indices(ctx: &Event, arg: Option<&wf_lang::ast::Expr>) -> Vec<usize> {
    let all = step_indices(ctx);
    if all.is_empty() {
        return all;
    }
    let Some(alias) = arg.and_then(extract_source_alias) else {
        return all;
    };
    all.iter()
        .copied()
        .filter(|idx| get_step_source(ctx, *idx).is_some_and(|s| s == alias))
        .collect()
}

pub(super) fn resolve_aggregate_step_indices(
    ctx: &Event,
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
        .filter(|idx| get_step_source(ctx, *idx).is_some_and(|s| s == step_ref))
        .collect();
    if !by_source.is_empty() {
        return prefer_close_steps(ctx, by_source);
    }
    let by_label: Vec<usize> = all
        .iter()
        .copied()
        .filter(|idx| get_step_label(ctx, *idx).is_some_and(|label| label == step_ref))
        .collect();
    prefer_close_steps(ctx, by_label)
}

fn prefer_close_steps(ctx: &Event, indices: Vec<usize>) -> Vec<usize> {
    let close_only: Vec<usize> = indices
        .iter()
        .copied()
        .filter(|idx| matches!(get_step_stage(ctx, *idx), Some("close")))
        .collect();
    if close_only.is_empty() {
        indices
    } else {
        close_only
    }
}

fn step_indices(ctx: &Event) -> Vec<usize> {
    let mut out: Vec<usize> = ctx
        .fields
        .keys()
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

pub(super) fn get_step_values(ctx: &Event, step_idx: usize) -> Option<&[Value]> {
    let field_name = format!("_step_{}_values", step_idx);
    match ctx.fields.get(field_name.as_str()) {
        Some(Value::Array(arr)) => Some(arr.as_slice()),
        _ => None,
    }
}

pub(super) fn get_step_field_values<'a>(
    ctx: &'a Event,
    step_idx: usize,
    field_name: &str,
) -> Option<&'a [Value]> {
    let field_name = format!("_step_{}_field_{}", step_idx, field_name);
    match ctx.fields.get(field_name.as_str()) {
        Some(Value::Array(arr)) => Some(arr.as_slice()),
        _ => None,
    }
}

pub(super) fn get_step_source(ctx: &Event, step_idx: usize) -> Option<&str> {
    let field_name = format!("_step_{}_source", step_idx);
    match ctx.fields.get(field_name.as_str()) {
        Some(Value::Str(s)) => Some(s.as_str()),
        _ => None,
    }
}

pub(super) fn get_step_label(ctx: &Event, step_idx: usize) -> Option<&str> {
    let field_name = format!("_step_{}_label", step_idx);
    match ctx.fields.get(field_name.as_str()) {
        Some(Value::Str(s)) => Some(s.as_str()),
        _ => None,
    }
}

pub(super) fn get_step_measure(ctx: &Event, step_idx: usize) -> Option<f64> {
    let field_name = format!("_step_{}_measure", step_idx);
    match ctx.fields.get(field_name.as_str()) {
        Some(Value::Number(n)) => Some(*n),
        _ => None,
    }
}

pub(super) fn get_step_stage(ctx: &Event, step_idx: usize) -> Option<&str> {
    let field_name = format!("_step_{}_stage", step_idx);
    match ctx.fields.get(field_name.as_str()) {
        Some(Value::Str(s)) => Some(s.as_str()),
        _ => None,
    }
}

pub(super) fn get_bind_count(ctx: &Event, alias: &str) -> Option<f64> {
    let field_name = format!("_bind_{}_count", alias);
    match ctx.fields.get(field_name.as_str()) {
        Some(Value::Number(n)) => Some(*n),
        _ => None,
    }
}

pub(super) fn get_bind_field_values<'a>(
    ctx: &'a Event,
    alias: &str,
    field_name: &str,
) -> Option<&'a [Value]> {
    let field_name = format!("_bind_{}_field_{}", alias, field_name);
    match ctx.fields.get(field_name.as_str()) {
        Some(Value::Array(arr)) => Some(arr.as_slice()),
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
