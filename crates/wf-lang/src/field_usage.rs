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

use crate::ast::{BoundVal, Expr, FieldRef, FieldSelector, ObjectItem, PathSegment, ReduceMeasure};
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
        // Post-join `where` filter reads joined fields from the eval context
        // (e.g. `person_events.state`) — the join target window must keep them
        // materialized or the enrichment is empty and the strict where
        // suppresses every output (q3 state filter regression).
        if let Some(w) = &plan.r#where {
            collect_expr_fields(w, &mut global);
        }

        // Join conditions reference fields on both sides; `within` bounds and
        // `emit at` triggers reference **driver (left) fields** (P3 deferred:
        // `deferred_pending_for` 在挂起时求值界与触发点，缺字段则挂起失败 →
        // 静默 0 输出——q9 实证 expires 被裁剪、60 万 auction 全挂起失败）；
        // `reduce` 度量/tie 字段读**右窗行**（评估时从命中行取）。全部必须
        // 物化，否则编译产物正确但运行静默无输出。
        for join in &plan.joins {
            for cond in &join.conds {
                global.insert(field_ref_name(&cond.left).to_string());
                global.insert(field_ref_name(&cond.right).to_string());
            }
            if let Some(wspec) = &join.within {
                for bound in [&wspec.lo, &wspec.hi] {
                    if let BoundVal::Expr(e) = &bound.val {
                        collect_expr_fields(e, &mut global);
                    }
                }
            }
            if let Some(emit_at) = &join.emit_at {
                collect_expr_fields(emit_at, &mut global);
            }
            if let Some(rc) = &join.reduce {
                match &rc.measure {
                    ReduceMeasure::Maxrow { field, tie } | ReduceMeasure::Minrow { field, tie } => {
                        global.insert(field_ref_name(field).to_string());
                        if let Some(t) = tie {
                            global.insert(field_ref_name(&t.field).to_string());
                        }
                    }
                    ReduceMeasure::Last { field } => {
                        global.insert(field_ref_name(field).to_string());
                    }
                    ReduceMeasure::Top { field, .. } => {
                        global.insert(field_ref_name(field).to_string());
                    }
                }
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

pub(crate) fn field_ref_name(fr: &FieldRef) -> &str {
    match fr {
        FieldRef::Simple(name) => name,
        FieldRef::Qualified(_, name) | FieldRef::Bracketed(_, name) => name,
        FieldRef::Path { segments, .. } => match segments.first() {
            Some(PathSegment::Field(root)) => root,
            _ => "",
        },
    }
}

/// True when `name` is a stat-selector function whose argument is a step label
/// or bind alias (`final(label)`, `trigger(label)`, `window_event(alias)`, …)
/// resolved from the eval context — never an event field.
fn is_stat_selector_name(name: &str) -> bool {
    matches!(
        name,
        "window_event" | "match_event" | "match_distinct" | "trigger" | "final"
    )
}

pub(crate) fn collect_expr_fields(expr: &Expr, out: &mut HashSet<String>) {
    match expr {
        Expr::Field(fr) => {
            out.insert(field_ref_name(fr).to_string());
        }
        Expr::BinOp { left, right, .. } => {
            collect_expr_fields(left, out);
            collect_expr_fields(right, out);
        }
        Expr::Neg(inner) => collect_expr_fields(inner, out),
        Expr::Not(inner) => collect_expr_fields(inner, out),
        Expr::FuncCall { name, args, .. } => {
            // Stat-selector args (`final(label)`, `window_event(alias)`, …) are
            // step labels / bind aliases resolved from the eval context, not
            // event fields — collecting them as fields makes empty-key close
            // rules (q15) look like they read non-key fields and forces the
            // per-event field history on for nothing.
            if !is_stat_selector_name(name) {
                for arg in args {
                    collect_expr_fields(arg, out);
                }
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
            lets: Vec::new(),
            match_plan,
            each_plan: None,
            stats_plan: None,
            joins: Vec::new(),
            r#where: None,
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
    fn where_filter_fields_are_collected_for_join_target_materialization() {
        // Regression: the post-join `where` reads joined fields from the eval
        // context (e.g. `person_events.state`); the join target window must
        // keep them materialized or the enrichment is empty and the strict
        // where suppresses every output (q3 state filter regression).
        let mut rule = make_rule(
            Vec::new(),
            MatchPlan {
                keys: vec![],
                key_map: None,
                key_join: None,
                window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(600)),
                event_steps: vec![],
                close_steps: vec![],
                close_mode: crate::ast::CloseMode::Or,
                tracked_bind_aliases: std::collections::HashSet::new(),
                tracked_bind_fields: std::collections::HashMap::new(),
                tracked_plain_fields: std::collections::HashSet::new(),
                match_mode: crate::ast::MatchMode::Seq,
                seq: None,
                accu: false,
                needs_field_history: false,
                trigger_event_needed: false,
            },
        );
        rule.r#where = Some(Expr::InList {
            expr: Box::new(Expr::Field(FieldRef::Qualified(
                "person_events".to_string(),
                "state".to_string(),
            ))),
            list: vec![Expr::StringLit("OR".into())],
            negated: false,
        });
        let usage = compute_window_field_usage(&[rule]);
        assert!(
            usage.global_fields.contains("state"),
            "where-referenced joined field `state` must be materialized, got {:?}",
            usage.global_fields
        );
    }

    #[test]
    fn deferred_join_within_emit_at_reduce_fields_collected() {
        // Regression (q9 引擎 0 输出根因)：field_usage 只统计 join 条件两侧字段，
        // `within` 界（a.dateTime/a.expires）、`emit at`（a.expires）、`reduce`
        // 度量/tie（price/dateTime）若被物化裁剪 → `deferred_pending_for` 挂起
        // 失败/评估失败 → 静默 0 输出（60 万 auction 全挂起失败实证）。
        let mut rule = make_rule(
            Vec::new(),
            MatchPlan {
                keys: vec![],
                key_map: None,
                key_join: None,
                window_spec: WindowSpec::Sliding(std::time::Duration::from_secs(600)),
                event_steps: vec![],
                close_steps: vec![],
                close_mode: crate::ast::CloseMode::Or,
                tracked_bind_aliases: std::collections::HashSet::new(),
                tracked_bind_fields: std::collections::HashMap::new(),
                tracked_plain_fields: std::collections::HashSet::new(),
                match_mode: crate::ast::MatchMode::Seq,
                seq: None,
                accu: false,
                needs_field_history: false,
                trigger_event_needed: false,
            },
        );
        rule.each_plan = Some(crate::plan::EachPlan {
            alias: "a".into(),
            filter: None,
        });
        rule.joins = vec![crate::plan::JoinPlan {
            right_window: "bid_events".to_string(),
            mode: crate::ast::JoinMode::Inner,
            conds: vec![crate::plan::JoinCondPlan {
                left: FieldRef::Qualified("a".into(), "id".into()),
                right: FieldRef::Qualified("bid_events".into(), "auction".into()),
            }],
            within: Some(crate::ast::WithinSpec {
                lo: crate::ast::Bound {
                    open: false,
                    val: crate::ast::BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                        "a".into(),
                        "dateTime".into(),
                    ))),
                },
                hi: crate::ast::Bound {
                    open: false,
                    val: crate::ast::BoundVal::Expr(Expr::Field(FieldRef::Qualified(
                        "a".into(),
                        "expires".into(),
                    ))),
                },
            }),
            reduce: Some(crate::ast::ReduceClause {
                measure: crate::ast::ReduceMeasure::Maxrow {
                    field: FieldRef::Simple("price".into()),
                    tie: Some(crate::ast::TieSpec {
                        field: FieldRef::Simple("dateTime".into()),
                        desc: false,
                    }),
                },
                label: Some("winner".into()),
            }),
            emit_at: Some(Expr::Field(FieldRef::Qualified(
                "a".into(),
                "expires".into(),
            ))),
        }];
        let usage = compute_window_field_usage(&[rule]);
        // 驱动（左）字段：join 键 id + within 界 dateTime/expires + emit_at expires
        assert!(usage.global_fields.contains("id"), "join cond left field");
        assert!(
            usage.global_fields.contains("dateTime"),
            "within lo / tie field must be materialized, got {:?}",
            usage.global_fields
        );
        assert!(
            usage.global_fields.contains("expires"),
            "within hi / emit_at field must be materialized, got {:?}",
            usage.global_fields
        );
        // 右窗字段：join 键 auction + reduce 度量 price
        assert!(
            usage.global_fields.contains("auction"),
            "join cond right field"
        );
        assert!(
            usage.global_fields.contains("price"),
            "reduce measure field must be materialized, got {:?}",
            usage.global_fields
        );
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
            key_join: None,
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
            trigger_event_needed: false,
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
            key_join: None,
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
            trigger_event_needed: false,
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
            key_join: None,
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
            trigger_event_needed: false,
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
            key_join: None,
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
            trigger_event_needed: false,
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
                key_join: None,
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
                trigger_event_needed: false,
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

    fn minimal_match_plan() -> MatchPlan {
        MatchPlan {
            keys: vec![],
            key_map: None,
            key_join: None,
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
            trigger_event_needed: false,
        }
    }

    #[test]
    fn filter_for_returns_none_when_window_needs_all() {
        let mut plan = make_rule(
            vec![BindPlan {
                alias: "e".into(),
                window: "auth_events".into(),
                filter: None,
            }],
            minimal_match_plan(),
        );
        plan.match_plan.keys = vec![field_ref("sip")];
        let usage = compute_window_field_usage(&[plan]);
        assert!(usage.needs_all.contains("auth_events"));
        assert_eq!(usage.filter_for("auth_events", ["sip", "dip"]), None);
    }

    #[test]
    fn filter_for_empty_subset_materializes_nothing() {
        // A window that no rule reads (e.g. an output-only window) gets an
        // empty materialization filter, not full materialization.
        let plan = make_rule(Vec::new(), minimal_match_plan());
        let usage = compute_window_field_usage(&[plan]);
        let f = usage
            .filter_for("out", ["x", "y", "n"])
            .expect("unused window should reduce to an empty set");
        assert!(f.is_empty());
    }

    #[test]
    fn filter_for_full_subset_keeps_full_materialization() {
        let mut plan = make_rule(
            vec![BindPlan {
                alias: "e".into(),
                window: "auth_events".into(),
                filter: Some(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            }],
            minimal_match_plan(),
        );
        // A columnar bind filter keeps the window out of needs_all.
        plan.match_plan.keys = vec![field_ref("sip")];
        plan.match_plan.tracked_bind_fields = std::collections::HashMap::from([(
            "e".to_string(),
            std::collections::HashSet::from(["sip".to_string()]),
        )]);
        let usage = compute_window_field_usage(&[plan]);
        assert!(!usage.needs_all.contains("auth_events"));
        // Every schema field is referenced → no reduction.
        assert_eq!(usage.filter_for("auth_events", ["sip", MACHINE_ID]), None);
    }

    #[test]
    fn machine_id_always_collected() {
        let plan = make_rule(Vec::new(), minimal_match_plan());
        let usage = compute_window_field_usage(&[plan]);
        assert!(
            usage.global_fields.contains(MACHINE_ID),
            "the match engine reads wp_src_ip from every event"
        );
    }

    #[test]
    fn key_map_and_keys_are_collected() {
        let mut plan = make_rule(
            vec![BindPlan {
                alias: "a".into(),
                window: "auth_events".into(),
                filter: None,
            }],
            minimal_match_plan(),
        );
        plan.match_plan.keys = vec![field_ref("sip")];
        plan.match_plan.key_map = Some(vec![crate::plan::KeyMapPlan {
            logical_name: "user_id".into(),
            source_alias: "a".into(),
            source_field: "user".into(),
        }]);
        plan.match_plan.tracked_bind_fields = std::collections::HashMap::from([(
            "a".to_string(),
            std::collections::HashSet::from(["sip".to_string()]),
        )]);
        let usage = compute_window_field_usage(&[plan]);
        assert!(usage.global_fields.contains("sip"));
        assert!(usage.global_fields.contains("user_id"));
        assert!(usage.global_fields.contains("user"));
    }

    #[test]
    fn join_condition_fields_collected() {
        let mut plan = make_rule(Vec::new(), minimal_match_plan());
        plan.joins = vec![crate::plan::JoinPlan {
            right_window: "bid_events".into(),
            mode: crate::ast::JoinMode::Snapshot,
            conds: vec![crate::plan::JoinCondPlan {
                left: FieldRef::Qualified("b".into(), "auction".into()),
                right: FieldRef::Qualified("auction_events".into(), "id".into()),
            }],
            within: None,
            reduce: None,
            emit_at: None,
        }];
        let usage = compute_window_field_usage(&[plan]);
        assert!(usage.global_fields.contains("auction"));
        assert!(usage.global_fields.contains("id"));
    }

    #[test]
    fn seq_branch_fields_collected() {
        let mut plan = make_rule(Vec::new(), minimal_match_plan());
        plan.match_plan.seq = Some(crate::plan::SeqPlan {
            consec: false,
            skip: crate::plan::SeqSkipPlan::PastLast,
            steps: vec![crate::plan::SeqStepPlan {
                neg: false,
                within: None,
                branch: crate::plan::BranchPlan {
                    label: None,
                    source: "e".into(),
                    field: Some(crate::ast::FieldSelector::Dot("action".into())),
                    guard: None,
                    agg: crate::plan::AggPlan {
                        transforms: vec![],
                        measure: crate::ast::Measure::Count,
                        cmp: crate::ast::CmpOp::Ge,
                        threshold: Expr::Number(1.0),
                    },
                },
            }],
        });
        let usage = compute_window_field_usage(&[plan]);
        assert!(usage.global_fields.contains("action"));
    }

    #[test]
    fn stat_selector_args_are_not_treated_as_fields() {
        let mut out = std::collections::HashSet::new();
        collect_expr_fields(
            &Expr::FuncCall {
                qualifier: None,
                name: "final".into(),
                args: vec![Expr::Field(field_ref("lbl"))],
            },
            &mut out,
        );
        assert!(
            out.is_empty(),
            "stat-selector labels resolve from the eval context, not events"
        );

        let mut out2 = std::collections::HashSet::new();
        collect_expr_fields(
            &Expr::FuncCall {
                qualifier: None,
                name: "window_event".into(),
                args: vec![Expr::Field(field_ref("auth"))],
            },
            &mut out2,
        );
        assert!(out2.is_empty());
    }

    #[test]
    fn field_ref_name_edge_cases() {
        assert_eq!(field_ref_name(&FieldRef::Simple("x".into())), "x");
        assert_eq!(
            field_ref_name(&FieldRef::Qualified("a".into(), "b".into())),
            "b"
        );
        assert_eq!(
            field_ref_name(&FieldRef::Bracketed("a".into(), "b".into())),
            "b"
        );
        assert_eq!(
            field_ref_name(&FieldRef::Path {
                alias: "a".into(),
                segments: vec![crate::ast::PathSegment::Field("root".into())],
            }),
            "root"
        );
        // A path starting with an index has no root field name.
        assert_eq!(
            field_ref_name(&FieldRef::Path {
                alias: "a".into(),
                segments: vec![crate::ast::PathSegment::Index(0)],
            }),
            ""
        );
    }

    #[test]
    fn untracked_bind_forces_needs_all() {
        let plan = make_rule(
            vec![BindPlan {
                alias: "e".into(),
                window: "auth_events".into(),
                filter: None,
            }],
            minimal_match_plan(),
        );
        // tracked_bind_fields has no entry for `e` → close path iterates every
        // field → full materialization required.
        let usage = compute_window_field_usage(&[plan]);
        assert!(usage.needs_all.contains("auth_events"));
    }
}
