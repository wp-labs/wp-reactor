use crate::ast::{Expr, FieldRef};
use crate::plan::{
    AggPlan, BindPlan, BranchPlan, JoinPlan, LimitsPlan, MatchPlan, StepPlan, WindowSpec, YieldPlan,
};
use crate::schema::WindowSchema;

use super::format::{
    format_cmp, format_duration, format_expr, format_field_ref, format_field_selector,
    format_measure, format_transform,
};
use super::{BindingExpl, MatchExpl};

// ---------------------------------------------------------------------------
// Bindings
// ---------------------------------------------------------------------------

pub(super) fn explain_binds(binds: &[BindPlan]) -> Vec<BindingExpl> {
    binds
        .iter()
        .map(|b| BindingExpl {
            alias: b.alias.clone(),
            window: b.window.clone(),
            filter: b.filter.as_ref().map(format_expr),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Match
// ---------------------------------------------------------------------------

pub(super) fn explain_match(mp: &MatchPlan) -> MatchExpl {
    let keys = if mp.keys.is_empty() {
        "(none)".to_string()
    } else {
        mp.keys
            .iter()
            .map(format_field_ref)
            .collect::<Vec<_>>()
            .join(", ")
    };

    let window_spec = match &mp.window_spec {
        WindowSpec::Sliding(d) => format!("sliding {}", format_duration(d)),
        WindowSpec::Fixed(d) => format!("fixed {}", format_duration(d)),
    };

    let event_steps = mp.event_steps.iter().map(format_step).collect();
    let close_steps = mp.close_steps.iter().map(format_step).collect();

    MatchExpl {
        keys,
        window_spec,
        event_steps,
        close_steps,
    }
}

fn format_step(step: &StepPlan) -> String {
    step.branches
        .iter()
        .map(format_branch)
        .collect::<Vec<_>>()
        .join(" || ")
}

fn format_branch(branch: &BranchPlan) -> String {
    let mut parts = Vec::new();

    if let Some(ref label) = branch.label {
        parts.push(format!("{}:", label));
    }

    let mut source = branch.source.clone();
    if let Some(ref field) = branch.field {
        source.push_str(&format_field_selector(field));
    }
    parts.push(source);

    if let Some(ref guard) = branch.guard {
        parts.push(format!("&& {}", format_expr(guard)));
    }

    parts.push(format!("|{}", format_agg(&branch.agg)));

    parts.join(" ")
}

fn format_agg(agg: &AggPlan) -> String {
    let mut chain = String::new();
    for t in &agg.transforms {
        chain.push_str(&format!(" {} |", format_transform(t)));
    }
    chain.push_str(&format!(
        " {} {} {}",
        format_measure(agg.measure),
        format_cmp(agg.cmp),
        format_expr(&agg.threshold)
    ));
    chain
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

pub(super) fn explain_joins(joins: &[JoinPlan]) -> Vec<String> {
    joins
        .iter()
        .map(|j| {
            let mode = match &j.mode {
                crate::ast::JoinMode::Snapshot => "snapshot".to_string(),
                crate::ast::JoinMode::Asof { within: None } => "asof".to_string(),
                crate::ast::JoinMode::Asof { within: Some(d) } => {
                    format!("asof within {}", format_duration(d))
                }
            };
            let conds: Vec<String> = j
                .conds
                .iter()
                .map(|c| {
                    format!(
                        "{} == {}",
                        format_field_ref(&c.left),
                        format_field_ref(&c.right)
                    )
                })
                .collect();
            format!("join {} {} on {}", j.right_window, mode, conds.join(" && "))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

pub(super) fn explain_limits(lp: &LimitsPlan) -> String {
    let mut parts = Vec::new();
    if let Some(max_mem) = lp.max_memory_bytes {
        parts.push(format!("max_memory={}B", max_mem));
    }
    if let Some(max_inst) = lp.max_instances {
        parts.push(format!("max_instances={}", max_inst));
    }
    if let Some(ref rate) = lp.max_throttle {
        parts.push(format!(
            "max_throttle={}/{}",
            rate.count,
            format_duration(&rate.per)
        ));
    }
    parts.push(format!("on_exceed={:?}", lp.on_exceed));
    parts.join(", ")
}

// ---------------------------------------------------------------------------
// Yield + lineage
// ---------------------------------------------------------------------------

pub(super) fn explain_yield(yp: &YieldPlan) -> Vec<(String, String)> {
    yp.fields
        .iter()
        .map(|f| (f.name.clone(), format_expr(&f.value)))
        .collect()
}

pub(super) fn compute_lineage(
    binds: &[BindPlan],
    yield_plan: &YieldPlan,
    _schemas: &[WindowSchema],
) -> Vec<(String, String)> {
    yield_plan
        .fields
        .iter()
        .map(|f| {
            let origin = trace_field_origin(&f.value, binds);
            (f.name.clone(), origin)
        })
        .collect()
}

fn trace_field_origin(expr: &Expr, binds: &[BindPlan]) -> String {
    match expr {
        Expr::Field(FieldRef::Qualified(alias, field)) => {
            let window = binds
                .iter()
                .find(|b| b.alias == *alias)
                .map(|b| b.window.as_str())
                .unwrap_or("?");
            format!("{}.{} (via {})", window, field, alias)
        }
        Expr::Field(FieldRef::Simple(name)) => {
            if let Some(bind) = binds.iter().find(|b| b.alias == *name) {
                format!("set-level ref to {}", bind.window)
            } else {
                format!("field `{}`", name)
            }
        }
        Expr::FuncCall { name, args, .. } => {
            let arg_str = args.iter().map(format_expr).collect::<Vec<_>>().join(", ");
            let inner = args.first().map(|a| trace_field_origin(a, binds));
            match inner {
                Some(origin) => format!("{}({}) over {}", name, arg_str, origin),
                None => format!("{}()", name),
            }
        }
        _ => format_expr(expr),
    }
}
