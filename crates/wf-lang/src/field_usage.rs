//! Window event field usage analysis.
//!
//! The engine materializes every schema field into a per-event
//! `HashMap<String, Value>` when rules consume a window batch — the dominant
//! peak RSS on wide windows (see wfusion PK-Nexmark diagnostics: ~2.5KB/event,
//! all fields). This pass walks all compiled rules and reports, conservatively,
//! which field names are actually read from window events, so the window layer
//! can materialize only those instead of the full schema.
//!
//! Conservative by construction: any construct the walker cannot fully analyze
//! pushes a window into `needs_all`, which keeps full materialization there.

use std::collections::{HashMap, HashSet};

use crate::ast::{Expr, FieldRef, FieldSelector, ObjectItem, PathSegment};
use crate::columnar::expr_is_columnar;
use crate::plan::{BranchPlan, RulePlan, StepPlan};

/// Field name carrying the machine/source identifier. The match engine reads
/// it from every event it processes, so it must always be materialized.
pub const MACHINE_ID: &str = "wp_src_ip";

/// Result of the per-window field usage analysis.
#[derive(Debug, Default, Clone)]
pub struct WindowFieldUsage {
    /// Union of all field names any rule reads from any window event.
    pub global_fields: HashSet<String>,
    /// Window names that must materialize every field (wholesale scans).
    pub needs_all: HashSet<String>,
    /// Window names where **every** bound rule has a columnar bind filter, so
    /// the window can defer per-row event materialization to the rule tasks
    /// (they materialize only the rows their bind filter accepts).
    pub defer_materialization: HashSet<String>,
}

impl WindowFieldUsage {
    /// Materialization filter for `window` given its schema's field names.
    ///
    /// Returns `None` to keep full materialization (window in `needs_all`, or
    /// the referenced subset is empty / not a real reduction). Otherwise
    /// returns the schema fields to materialize.
    pub fn filter_for<'a>(
        &self,
        window: &str,
        schema_fields: impl IntoIterator<Item = &'a str>,
    ) -> Option<HashSet<String>> {
        if self.needs_all.contains(window) || self.global_fields.is_empty() {
            return None;
        }
        let schema: HashSet<&str> = schema_fields.into_iter().collect();
        let subset: HashSet<String> = self
            .global_fields
            .iter()
            .filter(|f| schema.contains(f.as_str()))
            .cloned()
            .collect();
        if subset.is_empty() {
            // No rule reads any field from this window (e.g. an intermediate /
            // output-only window): materialize nothing. The window's time column
            // is appended by the caller (`schema_bridge`) so event-time
            // extraction still works. Falls back to full only when a wholesale
            // scan is possible (`needs_all`, checked above).
            Some(HashSet::new())
        } else if subset.len() == schema.len() {
            // Every schema field is referenced — no reduction.
            None
        } else {
            Some(subset)
        }
    }
}

/// Compute window event field usage across all compiled rules.
pub fn compute_window_field_usage(plans: &[RulePlan]) -> WindowFieldUsage {
    let mut global: HashSet<String> = HashSet::new();
    let mut needs_all: HashSet<String> = HashSet::new();
    // window -> whether every bind seen so far has a columnar filter (L2 defer).
    let mut defer_candidates: HashMap<&str, bool> = HashMap::new();
    // The match engine reads MACHINE_ID from every event it processes.
    global.insert(MACHINE_ID.to_string());

    for plan in plans {
        let binds: HashMap<&str, &str> = plan
            .binds
            .iter()
            .map(|b| (b.alias.as_str(), b.window.as_str()))
            .collect();
        let m = &plan.match_plan;

        // Keys and explicit key map (read from the triggering event's fields).
        for key in &m.keys {
            global.insert(field_ref_name(key).to_string());
        }
        if let Some(key_map) = &m.key_map {
            for entry in key_map {
                global.insert(entry.logical_name.clone());
                global.insert(entry.source_field.clone());
            }
        }

        // A window may defer (broadcast the raw batch and let rule tasks
        // materialize) only when EVERY rule binding it is defer-safe: an
        // `on each` rule (the rule task materializes the raw batch itself,
        // filtered), or a state-machine rule (P3 FieldView feeds the machine
        // straight from the columns, so hit rows need no HashMap
        // materialization — a bind filter, columnar or not, is evaluated over
        // the columnar view). The `all binds columnar` conjunct is kept for
        // symmetry/legacy; the match-plan arm already covers every match rule.
        let plan_defer_safe = plan.each_plan.is_some()
            || !plan.match_plan.event_steps.is_empty()
            || !plan.match_plan.close_steps.is_empty()
            || plan.match_plan.seq.is_some()
            || plan
                .binds
                .iter()
                .all(|b| b.filter.as_ref().is_some_and(expr_is_columnar));

        // Bind filters are evaluated against the bound event.
        for bind in &plan.binds {
            if let Some(filter) = &bind.filter {
                collect_expr_fields(filter, &mut global);
            }
            defer_candidates
                .entry(bind.window.as_str())
                .and_modify(|all| *all &= plan_defer_safe)
                .or_insert(plan_defer_safe);
        }

        // Event / close steps and sequence branches — read the source alias's
        // event fields (field selector, guard, threshold).
        for step in m.event_steps.iter().chain(m.close_steps.iter()) {
            collect_step_fields(step, &mut global);
        }
        if let Some(seq) = &m.seq {
            for s in &seq.steps {
                collect_branch_fields(&s.branch, &mut global);
            }
        }

        // Tracked fields are read into the eval context for yield / L3.
        for fields in m.tracked_bind_fields.values() {
            global.extend(fields.iter().cloned());
        }
        global.extend(m.tracked_plain_fields.iter().cloned());

        // Stateless each-rule filter.
        if let Some(each) = &plan.each_plan
            && let Some(filter) = &each.filter
        {
            collect_expr_fields(filter, &mut global);
        }

        // Entity key / score / yield expressions are evaluated (mostly against
        // the eval context); collect their field refs conservatively so any
        // window that carries them keeps them materialized.
        collect_expr_fields(&plan.entity_plan.entity_id_expr, &mut global);
        collect_expr_fields(&plan.score_plan.expr, &mut global);
        for field in &plan.yield_plan.fields {
            collect_expr_fields(&field.value, &mut global);
        }

        // Join conditions reference fields on both sides.
        for join in &plan.joins {
            for cond in &join.conds {
                global.insert(field_ref_name(&cond.left).to_string());
                global.insert(field_ref_name(&cond.right).to_string());
            }
        }

        // Wholesale-scan fallback: a bound alias missing from
        // `tracked_bind_fields` makes the close path iterate every field of
        // the event, which requires full materialization.
        for bind in &plan.binds {
            if !m.tracked_bind_fields.contains_key(bind.alias.as_str())
                && let Some(w) = binds.get(bind.alias.as_str())
            {
                needs_all.insert((*w).to_string());
            }
        }
    }

    let defer_materialization: HashSet<String> = defer_candidates
        .into_iter()
        .filter(|(_, all)| *all)
        .map(|(w, _)| w.to_string())
        .collect();

    WindowFieldUsage {
        global_fields: global,
        needs_all,
        defer_materialization,
    }
}

fn collect_step_fields(step: &StepPlan, out: &mut HashSet<String>) {
    for branch in &step.branches {
        collect_branch_fields(branch, out);
    }
}

fn collect_branch_fields(branch: &BranchPlan, out: &mut HashSet<String>) {
    if let Some(name) = field_selector_name(&branch.field) {
        out.insert(name.to_string());
    }
    if let Some(guard) = &branch.guard {
        collect_expr_fields(guard, out);
    }
    collect_expr_fields(&branch.agg.threshold, out);
}

fn field_selector_name(sel: &Option<FieldSelector>) -> Option<&str> {
    match sel {
        Some(FieldSelector::Dot(name)) | Some(FieldSelector::Bracket(name)) => Some(name.as_str()),
        None => None,
    }
}

fn field_ref_name(fr: &FieldRef) -> &str {
    match fr {
        FieldRef::Simple(name) => name,
        FieldRef::Qualified(_, name) | FieldRef::Bracketed(_, name) => name,
        FieldRef::Path { segments, .. } => match segments.first() {
            Some(PathSegment::Field(root)) => root,
            _ => "",
        },
    }
}

fn collect_expr_fields(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::Field(fr) => {
            out.insert(field_ref_name(fr).to_string());
        }
        Expr::BinOp { left, right, .. } => {
            collect_expr_fields(left, out);
            collect_expr_fields(right, out);
        }
        Expr::Neg(inner) => collect_expr_fields(inner, out),
        Expr::FuncCall { args, .. } => {
            for arg in args {
                collect_expr_fields(arg, out);
            }
        }
        Expr::Object(items) => {
            for ObjectItem { value, .. } in items {
                collect_expr_fields(value, out);
            }
        }
        Expr::Array(items) => {
            for item in items {
                collect_expr_fields(item, out);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_fields(expr, out);
            for item in list {
                collect_expr_fields(item, out);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            collect_expr_fields(cond, out);
            collect_expr_fields(then_expr, out);
            collect_expr_fields(else_expr, out);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Expr;
    use crate::plan::{BindPlan, MatchPlan, RulePlan, WindowSpec};

    fn field_ref(name: &str) -> FieldRef {
        FieldRef::Simple(name.to_string())
    }

    fn make_rule(binds: Vec<BindPlan>, match_plan: MatchPlan) -> RulePlan {
        RulePlan {
            conv_window: None,
            name: "t".into(),
            binds,
            match_plan,
            each_plan: None,
            joins: Vec::new(),
            entity_plan: crate::plan::EntityPlan {
                entity_type: String::new(),
                entity_id_expr: Expr::Bool(false),
            },
            yield_plan: crate::plan::YieldPlan {
                target: String::new(),
                version: None,
                fields: vec![],
            },
            score_plan: crate::plan::ScorePlan {
                expr: Expr::Number(1.0),
            },
            pattern_origin: None,
            conv_plan: None,
            limits_plan: None,
        }
    }

    #[test]
    fn collects_global_fields_and_tracks_bind_aliases() {
        let bind = BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        };
        let match_plan = MatchPlan {
            keys: vec![field_ref("auction")],
            key_map: None,
            window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(600)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: crate::ast::CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::from(["b".into()]),
            tracked_bind_fields: std::collections::HashMap::from([(
                "b".to_string(),
                std::collections::HashSet::from(["price".to_string()]),
            )]),
            tracked_plain_fields: std::collections::HashSet::new(),
            match_mode: crate::ast::MatchMode::Any,
            seq: None,
            accu: false,
            needs_field_history: false,
        };
        let plans = vec![make_rule(vec![bind], match_plan)];
        let usage = compute_window_field_usage(&plans);
        assert!(usage.global_fields.contains("auction"));
        assert!(usage.global_fields.contains("price"));
        assert!(usage.global_fields.contains(MACHINE_ID));
        assert!(!usage.needs_all.contains("bid_events"));
    }

    #[test]
    fn untracked_bind_falls_back_to_needs_all() {
        let bind = BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        };
        let match_plan = MatchPlan {
            keys: vec![field_ref("auction")],
            key_map: None,
            window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(600)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: crate::ast::CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            match_mode: crate::ast::MatchMode::Any,
            seq: None,
            accu: false,
            needs_field_history: false,
        };
        let plans = vec![make_rule(vec![bind], match_plan)];
        let usage = compute_window_field_usage(&plans);
        assert!(usage.needs_all.contains("bid_events"));
        assert!(usage.filter_for("bid_events", ["auction"]).is_none());
    }

    #[test]
    fn filter_for_intersects_with_schema() {
        let usage = WindowFieldUsage {
            global_fields: HashSet::from(["auction".into(), "price".into(), "missing".into()]),
            needs_all: HashSet::new(),
            defer_materialization: HashSet::new(),
        };
        let f = usage.filter_for("bid_events", ["auction", "price", "channel"]);
        let f = f.expect("should produce a filter");
        assert!(f.contains("auction"));
        assert!(f.contains("price"));
        assert!(!f.contains("missing"));
        assert!(!f.contains("channel"));
    }

    #[test]
    fn defer_materialization_only_when_every_bind_is_columnar() {
        use crate::ast::BinOp;

        let columnar = Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::BinOp {
                op: BinOp::Mod,
                left: Box::new(Expr::Field(FieldRef::Simple("auction".into()))),
                right: Box::new(Expr::Number(123.0)),
            }),
            right: Box::new(Expr::Number(0.0)),
        };
        let func = Expr::FuncCall {
            qualifier: None,
            name: "length".into(),
            args: vec![Expr::Field(FieldRef::Simple("auction".into()))],
        };

        let col_bind = BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: Some(columnar),
        };
        let no_filter_bind = BindPlan {
            alias: "b".into(),
            window: "no_filter".into(),
            filter: None,
        };
        let func_bind = BindPlan {
            alias: "b".into(),
            window: "func".into(),
            filter: Some(func),
        };

        let match_plan = MatchPlan {
            keys: vec![field_ref("auction")],
            key_map: None,
            window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(600)),
            event_steps: vec![],
            close_steps: vec![],
            close_mode: crate::ast::CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            match_mode: crate::ast::MatchMode::Any,
            seq: None,
            accu: false,
            needs_field_history: false,
        };

        let plans = vec![
            make_rule(vec![col_bind], match_plan.clone()),
            make_rule(vec![no_filter_bind], match_plan.clone()),
            make_rule(vec![func_bind], match_plan),
        ];
        let usage = compute_window_field_usage(&plans);
        assert!(usage.defer_materialization.contains("bid_events"));
        assert!(!usage.defer_materialization.contains("no_filter"));
        assert!(!usage.defer_materialization.contains("func"));
    }

    #[test]
    fn match_rule_defers_without_columnar_bind_filter() {
        use crate::ast::{CmpOp, Measure};
        use crate::plan::AggPlan;
        // P3 FieldView: a state-machine rule is defer-safe even with no bind
        // filter — hit rows are fed to the machine as columnar views, so the
        // old "every bind must have a columnar filter" requirement no longer
        // applies (the rule task evaluates the (absent) filter over the
        // columnar view and materializes nothing).
        let no_filter_bind = BindPlan {
            alias: "b".into(),
            window: "no_filter".into(),
            filter: None,
        };
        let match_plan = MatchPlan {
            keys: vec![field_ref("auction")],
            key_map: None,
            window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(600)),
            event_steps: vec![StepPlan {
                branches: vec![BranchPlan {
                    label: None,
                    source: "b".into(),
                    field: None,
                    guard: None,
                    agg: AggPlan {
                        transforms: vec![],
                        measure: Measure::Count,
                        cmp: CmpOp::Ge,
                        threshold: Expr::Number(1.0),
                    },
                }],
            }],
            close_steps: vec![],
            close_mode: crate::ast::CloseMode::Or,
            tracked_bind_aliases: std::collections::HashSet::new(),
            tracked_bind_fields: std::collections::HashMap::new(),
            tracked_plain_fields: std::collections::HashSet::new(),
            match_mode: crate::ast::MatchMode::Any,
            seq: None,
            accu: false,
            needs_field_history: false,
        };
        let usage = compute_window_field_usage(&[make_rule(vec![no_filter_bind], match_plan)]);
        assert!(usage.defer_materialization.contains("no_filter"));
    }

    #[test]
    fn each_rule_defers_materialization_without_needs_all() {
        let bind = BindPlan {
            alias: "b".into(),
            window: "bid_events".into(),
            filter: None,
        };
        let mut plan = make_rule(
            vec![bind],
            MatchPlan {
                keys: vec![],
                key_map: None,
                window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(600)),
                event_steps: vec![],
                close_steps: vec![],
                close_mode: crate::ast::CloseMode::Or,
                tracked_bind_aliases: std::collections::HashSet::from(["b".to_string()]),
                tracked_bind_fields: std::collections::HashMap::from([(
                    "b".to_string(),
                    std::collections::HashSet::from(["auction".to_string()]),
                )]),
                tracked_plain_fields: std::collections::HashSet::new(),
                match_mode: crate::ast::MatchMode::Any,
                seq: None,
                accu: false,
                needs_field_history: false,
            },
        );
        plan.each_plan = Some(crate::plan::EachPlan {
            alias: "b".into(),
            filter: None,
        });
        plan.entity_plan.entity_id_expr =
            Expr::Field(FieldRef::Qualified("b".into(), "auction".into()));
        plan.yield_plan.fields = vec![crate::plan::YieldField {
            name: "id".into(),
            value: Expr::Field(FieldRef::Qualified("b".into(), "auction".into())),
        }];

        let usage = compute_window_field_usage(&[plan]);
        assert!(usage.defer_materialization.contains("bid_events"));
        assert!(!usage.needs_all.contains("bid_events"));
        let f = usage
            .filter_for(
                "bid_events",
                ["auction", "bidder", "price", "dateTime", MACHINE_ID],
            )
            .expect("each rule must reduce materialization");
        assert!(f.contains("auction"));
        assert!(f.contains(MACHINE_ID));
        assert!(!f.contains("bidder"));
        assert!(!f.contains("price"));
    }
}
