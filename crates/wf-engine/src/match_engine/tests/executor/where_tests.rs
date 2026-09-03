//! Post-join `where` filter tests: the rule-level `where <expr>` is evaluated
//! after joins enrich the event context and before alert construction. Strict
//! semantics — `false` or a missing joined field (`None`) suppresses output,
//! aligning INNER JOIN miss-drop (q3 state filter / q20 category filter).
use std::sync::Arc;

use std::collections::HashSet;

use wf_lang::ast::{BinOp, Expr, FieldRef, JoinMode};
use wf_lang::plan::{EachPlan, JoinCondPlan, JoinPlan};

use crate::match_engine::RuleExecutor;
use crate::match_engine::cep::{AsofLookup, EngineHashMap, Event, Value, WindowLookup};
use crate::match_engine::event_bridge::JoinRow;

use super::super::helpers::*;
use super::helpers::*;

/// Snapshot lookup returning zero or one person row (state = configured).
struct PersonLookup {
    rows: Vec<JoinRow>,
}

impl WindowLookup for PersonLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
        Some(self.rows.clone())
    }
    fn asof_lookup_max(
        &self,
        _w: &str,
        _k: &str,
        _v: &Value,
        _t: i64,
        _within: Option<&std::time::Duration>,
    ) -> AsofLookup {
        AsofLookup::Miss
    }
}

fn person_row(state: &str) -> JoinRow {
    let mut fields = EngineHashMap::default();
    fields.insert("id".into(), Value::Str("10.0.0.1".into()));
    fields.insert("state".into(), Value::Str(state.into()));
    JoinRow::Event(Arc::new(Event { fields }))
}

fn where_state_eq_or() -> Expr {
    Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Qualified(
            "person_events".to_string(),
            "state".to_string(),
        ))),
        right: Box::new(Expr::StringLit("OR".to_string())),
    }
}

fn person_join() -> JoinPlan {
    JoinPlan {
        right_window: "person_events".to_string(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("sip".to_string()),
            right: FieldRef::Simple("id".to_string()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }
}

// ---------------------------------------------------------------------------
// match + snapshot join + where
// ---------------------------------------------------------------------------

#[test]
fn match_join_where_hit_passes_and_false_suppresses() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(1.0),
        "host",
        Expr::StringLit("x".into()),
    );
    plan.joins = vec![person_join()];
    plan.r#where = Some(where_state_eq_or());

    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();

    // join hit, state == "OR" → where true → output
    let ok = exec
        .execute_match_with_joins(
            &matched,
            &PersonLookup {
                rows: vec![person_row("OR")],
            },
        )
        .expect("execute should succeed");
    assert!(ok.is_some(), "where true (state=OR) must emit");

    // join hit, state == "CA" → where false → suppressed
    let suppressed = exec
        .execute_match_with_joins(
            &matched,
            &PersonLookup {
                rows: vec![person_row("CA")],
            },
        )
        .expect("execute should succeed");
    assert!(suppressed.is_none(), "where false (state=CA) must suppress");
}

#[test]
fn match_join_where_miss_suppresses() {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(1.0),
        "host",
        Expr::StringLit("x".into()),
    );
    plan.joins = vec![person_join()];
    plan.r#where = Some(where_state_eq_or());

    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();

    // join miss (no person row) → joined field absent → strict where → None → suppress
    let miss = exec
        .execute_match_with_joins(&matched, &PersonLookup { rows: vec![] })
        .expect("execute should succeed");
    assert!(
        miss.is_none(),
        "join miss must suppress (INNER JOIN semantics)"
    );
}

#[test]
fn match_without_where_still_emits_on_join_miss() {
    // Regression guard: without `where`, a join miss must NOT suppress (the
    // historical optional-enrichment behavior).
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(1.0),
        "host",
        Expr::StringLit("x".into()),
    );
    plan.joins = vec![person_join()];
    plan.r#where = None;

    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();
    let out = exec
        .execute_match_with_joins(&matched, &PersonLookup { rows: vec![] })
        .expect("execute should succeed");
    assert!(
        out.is_some(),
        "no where → join miss keeps the event (historical behavior)"
    );
}

// ---------------------------------------------------------------------------
// on each + snapshot join + where
// ---------------------------------------------------------------------------

fn each_rule_plan() -> wf_lang::plan::RulePlan {
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(1.0),
        "host",
        Expr::StringLit("x".into()),
    );
    plan.each_plan = Some(EachPlan {
        alias: "fail".to_string(),
        filter: None,
    });
    plan.joins = vec![person_join()];
    plan.r#where = Some(where_state_eq_or());
    plan
}

fn each_event() -> Event {
    let mut fields = EngineHashMap::default();
    fields.insert("sip".into(), Value::Str("10.0.0.1".into()));
    Event { fields }
}

#[test]
fn each_join_where_hit_and_miss() {
    let exec = RuleExecutor::new(each_rule_plan());

    let hit = exec
        .execute_each_with_joins(
            &each_event(),
            0,
            &PersonLookup {
                rows: vec![person_row("OR")],
            },
            &[],
            0,
        )
        .expect("execute should succeed");
    assert!(hit.is_some(), "each + where true must emit");

    let rejected = exec
        .execute_each_with_joins(
            &each_event(),
            0,
            &PersonLookup {
                rows: vec![person_row("ID")],
            },
            &[],
            0,
        )
        .expect("execute should succeed");
    assert!(rejected.is_none(), "each + where false must suppress");

    let miss = exec
        .execute_each_with_joins(&each_event(), 0, &PersonLookup { rows: vec![] }, &[], 0)
        .expect("execute should succeed");
    assert!(miss.is_none(), "each + join miss must suppress");
}

// ---------------------------------------------------------------------------
// direct (columnar) on-each path
// ---------------------------------------------------------------------------

#[test]
fn each_direct_join_where_suppresses() {
    use crate::alert::AlertColumnBuilder;

    let exec = RuleExecutor::new(each_rule_plan());
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let event = each_event();

    // where true → appended
    let appended = exec
        .execute_each_direct(
            &event,
            0,
            &PersonLookup {
                rows: vec![person_row("OR")],
            },
            &[],
            0,
            &mut builder,
        )
        .expect("execute should succeed");
    assert!(appended, "direct path + where true must append");

    // where false → skipped
    let rejected = exec
        .execute_each_direct(
            &event,
            0,
            &PersonLookup {
                rows: vec![person_row("CA")],
            },
            &[],
            0,
            &mut builder,
        )
        .expect("execute should succeed");
    assert!(!rejected, "direct path + where false must skip");

    // join miss → skipped
    let miss = exec
        .execute_each_direct(
            &event,
            0,
            &PersonLookup { rows: vec![] },
            &[],
            0,
            &mut builder,
        )
        .expect("execute should succeed");
    assert!(!miss, "direct path + join miss must skip");
}
