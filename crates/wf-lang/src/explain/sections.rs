use crate::ast::{Expr, FieldRef};
use crate::plan::{
    AggPlan, BindPlan, BranchPlan, ConvOpPlan, ConvPlan, JoinPlan, LimitsPlan, MatchPlan,
    SeqStepPlan, StepPlan, WindowSpec, YieldPlan,
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
        WindowSpec::Session(gap) => format!("session(gap={})", format_duration(gap)),
        WindowSpec::Hop { size, slide } => format!(
            "hop(size={}, slide={})",
            format_duration(size),
            format_duration(slide)
        ),
    };

    let event_steps = mp.event_steps.iter().map(format_step).collect();
    let close_steps: Vec<String> = mp.close_steps.iter().map(format_step).collect();
    let close_mode = if close_steps.is_empty() {
        None
    } else {
        Some(mp.close_mode)
    };
    let seq = mp
        .seq
        .as_ref()
        .map(|seq_plan| seq_plan.steps.iter().map(format_seq_step).collect());

    MatchExpl {
        keys,
        window_spec,
        event_steps,
        close_steps,
        close_mode,
        seq,
        accu: mp.accu,
    }
}

fn format_step(step: &StepPlan) -> String {
    step.branches
        .iter()
        .map(format_branch)
        .collect::<Vec<_>>()
        .join(" || ")
}

fn format_seq_step(step: &SeqStepPlan) -> String {
    let mut s = String::new();
    if step.neg {
        s.push_str("not ");
    }
    s.push_str(&format_branch(&step.branch));
    if let Some(w) = &step.within {
        s.push_str(&format!(" within {}", format_duration(w)));
    }
    s
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
                crate::ast::JoinMode::Anti => "anti".to_string(),
                crate::ast::JoinMode::Inner => "inner".to_string(),
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
            let mut parts = vec![format!("join {} {}", j.right_window, mode)];
            if let Some(w) = &j.within {
                parts.push(format!("within {}", format_within(w)));
            }
            if let Some(r) = &j.reduce {
                parts.push(format!("reduce {}", format_reduce(r)));
            }
            parts.push(format!("on {}", conds.join(" && ")));
            if let Some(label) = j.reduce.as_ref().and_then(|r| r.label.as_ref()) {
                parts.push(format!("as {}", label));
            }
            if let Some(e) = &j.emit_at {
                parts.push(format!("emit at {}", format_expr(e)));
            }
            parts.join(" ")
        })
        .collect()
}

fn format_within(w: &crate::ast::WithinSpec) -> String {
    format!("[{} , {}]", format_bound(&w.lo), format_bound(&w.hi))
}

fn format_bound(b: &crate::ast::Bound) -> String {
    let marker = if b.open { "<" } else { "" };
    match &b.val {
        crate::ast::BoundVal::Dur { dur, neg } => format!(
            "{}{}",
            marker,
            if *neg {
                format!("-{}", format_duration(dur))
            } else {
                format_duration(dur)
            }
        ),
        crate::ast::BoundVal::Expr(e) => format!("{}{}", marker, format_expr(e)),
    }
}

fn format_reduce(r: &crate::ast::ReduceClause) -> String {
    let m = match &r.measure {
        crate::ast::ReduceMeasure::Maxrow { field, tie } => {
            format!("maxrow({}){}", format_field_ref(field), format_tie(tie))
        }
        crate::ast::ReduceMeasure::Minrow { field, tie } => {
            format!("minrow({}){}", format_field_ref(field), format_tie(tie))
        }
        crate::ast::ReduceMeasure::Last { field } => {
            format!("last({})", format_field_ref(field))
        }
        crate::ast::ReduceMeasure::Top { n, field } => {
            format!("top({}, {})", n, format_field_ref(field))
        }
    };
    match &r.label {
        Some(l) => format!("{} as {}", m, l),
        None => m,
    }
}

fn format_tie(tie: &Option<crate::ast::TieSpec>) -> String {
    match tie {
        Some(t) => format!(
            " tie({} {})",
            format_field_ref(&t.field),
            if t.desc { "desc" } else { "asc" }
        ),
        None => String::new(),
    }
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
    if let Some(spill) = &lp.spill {
        parts.push(format!("spill={spill:?}"));
        if let Some(b) = lp.max_disk_bytes {
            parts.push(format!("max_disk={b}B"));
        }
    }
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
        Expr::SystemVar(_) => format_expr(expr),
        _ => format_expr(expr),
    }
}

// ---------------------------------------------------------------------------
// Conv
// ---------------------------------------------------------------------------

pub(super) fn explain_conv(plan: &ConvPlan) -> Vec<String> {
    plan.chains
        .iter()
        .map(|chain| {
            chain
                .ops
                .iter()
                .map(|op| match op {
                    ConvOpPlan::Sort(keys) => {
                        let k: Vec<String> = keys
                            .iter()
                            .map(|k| {
                                let prefix = if k.descending { "-" } else { "" };
                                format!("{}{}", prefix, format_expr(&k.expr))
                            })
                            .collect();
                        format!("sort({})", k.join(", "))
                    }
                    ConvOpPlan::Top(n) => format!("top({})", n),
                    ConvOpPlan::TopTies { n, .. } => format!("top_ties({})", n),
                    ConvOpPlan::Dedup(e) => format!("dedup({})", format_expr(e)),
                    ConvOpPlan::Where(e) => format!("where({})", format_expr(e)),
                })
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect()
}
