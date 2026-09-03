//! coverage_extra 拆出的兄弟子模块（2026-09-04）：`context.rs` 的
//! `build_eval_context`（all / named 合成字段）与 `execute_joins`（inner / snapshot /
//! anti / asof / interval 模式、in_interval / enrich_join_row 边角），以及 `match_exec.rs`
//! 带 join 的 `execute_match` 拒路。共享 harness 在父模块 `coverage_extra.rs`，此处经
//! `use super::*` 复用。

use super::*;

use crate::match_engine::TriggerEvent;
use crate::match_engine::cep::BindData;
use wf_lang::ast::{BinOp, Bound, BoundVal, JoinMode, WithinSpec};
use wf_lang::plan::{JoinCondPlan, JoinPlan};

// ---------------------------------------------------------------------------
// context.rs — build_eval_context
// ---------------------------------------------------------------------------

#[test]
fn build_eval_context_all_and_named_synthetic_fields() {
    use crate::match_engine::executor::context::{CloseCtxFields, build_eval_context};

    let keys = vec![simple_key("sip"), simple_key("dport")];
    let scope_key = vec![str_val("10.0.0.1"), num(443.0)];
    let mut fv1 = EngineHashMap::default();
    fv1.insert("price".into(), vec![num(1.0), num(2.0), num(3.0)]);
    let sd1 = step_data(Some("login"), 3.0, fv1);
    let mut fv2 = EngineHashMap::default();
    fv2.insert("price".into(), vec![num(9.0)]);
    let sd2 = step_data(Some("brute"), 1.0, fv2);

    let step_plan_login = StepPlan {
        branches: vec![BranchPlan {
            label: Some("login".into()),
            source: "e".into(),
            field: None,
            guard: None,
            agg: count_ge(1.0),
        }],
    };
    let step_plan_brute = StepPlan {
        branches: vec![BranchPlan {
            label: Some("brute".into()),
            source: "f".into(),
            field: None,
            guard: None,
            agg: count_ge(1.0),
        }],
    };
    let step_plans = vec![&step_plan_login, &step_plan_brute];

    // All build: every synthetic field present.
    let all = CloseCtxFields::All;
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[sd1.clone(), sd2.clone()],
        &[],
        &step_plans,
        None,
        &all,
        None,
    );
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("dport"), Some(&num(443.0)));
    assert_eq!(ctx.fields.get("login"), Some(&num(3.0)));
    assert_eq!(ctx.fields.get("brute"), Some(&num(1.0)));
    // L3 collected values / measure / label / source fields.
    assert_eq!(
        ctx.fields.get("_step_0_values"),
        Some(&Value::Array(vec![]))
    );
    assert_eq!(ctx.fields.get("_step_0_measure"), Some(&num(3.0)));
    assert_eq!(ctx.fields.get("_step_0_label"), Some(&str_val("login")));
    assert_eq!(ctx.fields.get("_step_0_source"), Some(&str_val("e")));
    // Per-field history arrays + last-value injection.
    assert_eq!(
        ctx.fields.get("_step_0_field_price"),
        Some(&Value::Array(vec![num(1.0), num(2.0), num(3.0)]))
    );
    assert_eq!(ctx.fields.get("price"), Some(&num(3.0)));
    // Keys are not overwritten by a colliding label.
    // Colliding key/label: label must not overwrite the key.
    let colliding = step_data(Some("sip"), 99.0, EngineHashMap::default());
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[colliding],
        &[],
        &[&StepPlan { branches: vec![] }],
        None,
        &all,
        None,
    );
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("_step_0_measure"), Some(&num(99.0)));

    // Trigger event scalars are included (keys win).
    let trigger = event(vec![("sip", str_val("override")), ("raw", num(7.0))]);
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        std::slice::from_ref(&sd1),
        &[],
        &[&StepPlan { branches: vec![] }],
        Some(&TriggerEvent::from_event(Arc::new(trigger.clone()))),
        &all,
        None,
    );
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("raw"), Some(&num(7.0)));

    // Named build: only requested names materialized (last values only).
    let named = CloseCtxFields::Named(HashSet::from(["price".to_string()]));
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[sd1, sd2],
        &[],
        &step_plans,
        None,
        &named,
        None,
    );
    assert_eq!(ctx.fields.get("price"), Some(&num(3.0)));
    assert!(!ctx.fields.contains_key("login"), "label not requested");
    assert!(!ctx.fields.contains_key("_step_0_measure"));
    // Keys are always present.
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));

    // Bind data: _bind_<alias>_count / _bind_<alias>_field_<name> + last value.
    let bd = BindData {
        alias: "win".to_string(),
        count: 2,
        field_values: EngineHashMap::from_iter([(
            "amount".to_string(),
            vec![num(10.0), num(20.0)],
        )]),
    };
    let all = CloseCtxFields::All;
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[],
        std::slice::from_ref(&bd),
        &[],
        None,
        &all,
        None,
    );
    assert_eq!(ctx.fields.get("_bind_win_count"), Some(&num(2.0)));
    assert_eq!(
        ctx.fields.get("_bind_win_field_amount"),
        Some(&Value::Array(vec![num(10.0), num(20.0)]))
    );
    assert_eq!(ctx.fields.get("amount"), Some(&num(20.0)));
    let named = CloseCtxFields::Named(HashSet::from(["amount".to_string()]));
    let ctx = build_eval_context(&keys, &scope_key, &[], &[bd], &[], None, &named, None);
    assert_eq!(ctx.fields.get("amount"), Some(&num(20.0)));
    assert!(!ctx.fields.contains_key("_bind_win_count"));
}

// ---------------------------------------------------------------------------
// context.rs — execute_joins
// ---------------------------------------------------------------------------

#[test]
fn execute_joins_inner_snapshot_anti_modes() {
    use crate::match_engine::executor::context::execute_joins;

    let cond = |left: &str, right: &str| JoinCondPlan {
        left: FieldRef::Simple(left.to_string()),
        right: FieldRef::Qualified("w".into(), right.to_string()),
    };
    let jp = |mode: JoinMode, conds: Vec<JoinCondPlan>| JoinPlan {
        right_window: "w".to_string(),
        mode,
        conds,
        within: None,
        reduce: None,
        emit_at: None,
    };

    // Inner hit: enriches; miss: drops (returns false).
    let lookup = RowsLookup::new(vec![join_row("id", 1.0, vec![("amt", num(5.0))])]);
    let joins = vec![jp(JoinMode::Inner, vec![cond("key", "id")])];
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));
    assert_eq!(ctx.fields.get("w.amt"), Some(&num(5.0)));
    assert_eq!(ctx.fields.get("amt"), Some(&num(5.0)));
    let mut ctx = event(vec![("key", num(9.0))]);
    assert!(!execute_joins(&joins, &mut ctx, &lookup, 0));

    // Inner with missing key field / no rows → drop.
    let mut ctx = event(vec![]);
    assert!(!execute_joins(&joins, &mut ctx, &lookup, 0));
    let mut ctx = event(vec![("key", num(2.0))]);
    assert!(!execute_joins(&joins, &mut ctx, &lookup, 0));

    // Snapshot: miss keeps the event unenriched.
    let joins = vec![jp(JoinMode::Snapshot, vec![cond("key", "id")])];
    let mut ctx = event(vec![("key", num(9.0))]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));
    assert!(!ctx.fields.contains_key("amt"));
    let mut ctx = event(vec![]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));

    // Snapshot with no window data → keep.
    let joins = vec![jp(JoinMode::Snapshot, vec![cond("key", "id")])];
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&joins, &mut ctx, &EmptyLookup, 0));

    // Anti: matching row drops; no row keeps.
    let joins = vec![jp(JoinMode::Anti, vec![cond("key", "id")])];
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(!execute_joins(&joins, &mut ctx, &lookup, 0));
    let mut ctx = event(vec![("key", num(7.0))]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));
    // Anti without window data → keep.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&joins, &mut ctx, &EmptyLookup, 0));
    // Anti with missing key → keep (continue).
    let mut ctx = event(vec![]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));

    // Deferred (`emit_at`) joins are skipped on the eager path entirely.
    let joins = vec![JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Inner,
        conds: vec![cond("key", "id")],
        within: None,
        reduce: None,
        emit_at: Some(Expr::Number(1.0)),
    }];
    let mut ctx = event(vec![("key", num(9.0))]);
    assert!(execute_joins(&joins, &mut ctx, &lookup, 0));
    assert!(!ctx.fields.contains_key("amt"));
}

#[test]
fn execute_joins_asof_single_cond_hit_miss_fallback() {
    use crate::match_engine::executor::context::execute_joins;

    let cond = JoinCondPlan {
        left: FieldRef::Simple("key".into()),
        right: FieldRef::Qualified("w".into(), "id".into()),
    };
    let jp = |within: Option<Duration>| JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Asof { within },
        conds: vec![cond.clone()],
        within: None,
        reduce: None,
        emit_at: None,
    };
    let row = join_row("id", 1.0, vec![("amt", num(5.0))]);

    // Fast-path Hit.
    let lookup = RowsLookup {
        rows: vec![row.clone()],
        ts_rows: vec![],
        asof_outcome: Some(AsofLookup::Hit(row.clone())),
    };
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&[jp(None)], &mut ctx, &lookup, 1_000));
    assert_eq!(ctx.fields.get("amt"), Some(&num(5.0)));

    // Fast-path Miss → None (no enrichment, keep).
    let lookup = RowsLookup {
        rows: vec![row.clone()],
        ts_rows: vec![],
        asof_outcome: Some(AsofLookup::Miss),
    };
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&[jp(None)], &mut ctx, &lookup, 1_000));
    assert!(!ctx.fields.contains_key("amt"));

    // Fallback → candidate scan; picks the latest ts ≤ event_time.
    let lookup = RowsLookup::with_ts(vec![
        (100, join_row("id", 1.0, vec![("amt", num(1.0))])),
        (200, join_row("id", 1.0, vec![("amt", num(2.0))])),
        (300, join_row("id", 1.0, vec![("amt", num(3.0))])),
        (999, join_row("id", 9.0, vec![("amt", num(99.0))])),
    ]);
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&[jp(None)], &mut ctx, &lookup, 250));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));

    // `within` filters older rows: latest within [250-100, 250] is ts=200.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp(Some(Duration::from_secs(100)))],
        &mut ctx,
        &lookup,
        250
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));

    // Asof with missing key → keep unenriched (continue).
    let mut ctx = event(vec![]);
    assert!(execute_joins(&[jp(None)], &mut ctx, &lookup, 250));

    // Asof with no candidates → keep unenriched.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&[jp(None)], &mut ctx, &EmptyLookup, 250));
}

#[test]
fn execute_joins_asof_multi_cond_uses_scan() {
    use crate::match_engine::executor::context::execute_joins;

    let conds = vec![
        JoinCondPlan {
            left: FieldRef::Simple("key".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        },
        JoinCondPlan {
            left: FieldRef::Simple("chan".into()),
            right: FieldRef::Qualified("w".into(), "channel".into()),
        },
    ];
    let join = JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Asof { within: None },
        conds,
        within: None,
        reduce: None,
        emit_at: None,
    };
    let lookup = RowsLookup::with_ts(vec![
        (
            100,
            join_row(
                "id",
                1.0,
                vec![("channel", str_val("a")), ("amt", num(1.0))],
            ),
        ),
        (
            200,
            join_row(
                "id",
                1.0,
                vec![("channel", str_val("b")), ("amt", num(2.0))],
            ),
        ),
        (
            300,
            join_row(
                "id",
                1.0,
                vec![("channel", str_val("a")), ("amt", num(3.0))],
            ),
        ),
    ]);
    let mut ctx = event(vec![("key", num(1.0)), ("chan", str_val("a"))]);
    assert!(execute_joins(&[join], &mut ctx, &lookup, 1_000));
    // Latest matching both conds = ts=300.
    assert_eq!(ctx.fields.get("amt"), Some(&num(3.0)));
}

#[test]
fn execute_joins_interval_within_modes() {
    use crate::match_engine::executor::context::execute_joins;

    let cond = JoinCondPlan {
        left: FieldRef::Simple("key".into()),
        right: FieldRef::Qualified("w".into(), "id".into()),
    };
    let within = WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(100),
                neg: true,
            },
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(100),
                neg: false,
            },
        },
    };
    let jp = |mode: JoinMode| JoinPlan {
        right_window: "w".to_string(),
        mode,
        conds: vec![cond.clone()],
        within: Some(within.clone()),
        reduce: None,
        emit_at: None,
    };
    const T0: i64 = 1_700_000_000_000_000_000;
    let lookup = RowsLookup::with_ts(vec![
        (
            T0 - 200_000_000_000,
            join_row("id", 1.0, vec![("amt", num(1.0))]),
        ),
        (
            T0 - 50_000_000_000,
            join_row("id", 1.0, vec![("amt", num(2.0))]),
        ),
        (
            T0 + 50_000_000_000,
            join_row("id", 1.0, vec![("amt", num(3.0))]),
        ),
        (
            T0 + 400_000_000_000,
            join_row("id", 1.0, vec![("amt", num(4.0))]),
        ),
    ]);
    // Event at T0: interval [T0-100s, T0+100s] → rows at T0-50s and T0+50s.
    // Inner/Snapshot pick the earliest, Asof the latest.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&[jp(JoinMode::Inner)], &mut ctx, &lookup, T0));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp(JoinMode::Snapshot)],
        &mut ctx,
        &lookup,
        T0
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(2.0)));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp(JoinMode::Asof { within: None })],
        &mut ctx,
        &lookup,
        T0
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(3.0)));
    // Anti within: an interval match drops the event.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(!execute_joins(&[jp(JoinMode::Anti)], &mut ctx, &lookup, T0));
    // Event at T0+500s: interval [T0+400s, T0+600s] → the T0+400s row qualifies.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        T0 + 500_000_000_000
    ));
    assert_eq!(ctx.fields.get("amt"), Some(&num(4.0)));
    // Event at T0-1000s: interval [T0-1100s, T0-900s] → nothing in range.
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(!execute_joins(
        &[jp(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        T0 - 1_000_000_000_000
    ));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp(JoinMode::Anti)],
        &mut ctx,
        &lookup,
        T0 - 1_000_000_000_000
    ));

    // Interval bound eval failure (missing key field) → inner drops, others keep.
    let jp2 = |mode: JoinMode| JoinPlan {
        right_window: "w".to_string(),
        mode,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("missing".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        }],
        within: Some(within.clone()),
        reduce: None,
        emit_at: None,
    };
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(!execute_joins(
        &[jp2(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        T0
    ));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(
        &[jp2(JoinMode::Snapshot)],
        &mut ctx,
        &lookup,
        T0
    ));
    let mut ctx = event(vec![("key", num(1.0))]);
    assert!(execute_joins(&[jp2(JoinMode::Anti)], &mut ctx, &lookup, T0));
}

#[test]
fn in_interval_and_eval_interval_bound() {
    use crate::match_engine::executor::context::{eval_interval_bound, in_interval};

    // Closed bounds include boundaries; open exclude.
    assert!(in_interval(100, 100, 200, false, false));
    assert!(in_interval(200, 100, 200, false, false));
    assert!(!in_interval(99, 100, 200, false, false));
    assert!(!in_interval(100, 100, 200, true, false));
    assert!(!in_interval(200, 100, 200, false, true));
    assert!(in_interval(101, 100, 200, true, false));
    assert!(in_interval(199, 100, 200, false, true));

    // Dur bounds (positive / negative / huge overflow saturates to i64::MAX).
    let dur = |secs: u64, neg: bool, open: bool| Bound {
        open,
        val: BoundVal::Dur {
            dur: Duration::from_secs(secs),
            neg,
        },
    };
    let ctx = event(vec![]);
    assert_eq!(
        eval_interval_bound(&dur(10, true, false), &ctx, 1_000),
        Some(-9_999_999_000i64)
    );
    assert_eq!(
        eval_interval_bound(&dur(10, false, false), &ctx, 1_000),
        Some(10_000_001_000i64)
    );

    // Expr bounds: numeric → epoch nanos; non-numeric / missing → None.
    let expr_bound = |e: Expr| Bound {
        open: false,
        val: BoundVal::Expr(e),
    };
    let ctx = event(vec![
        ("ts", num(1_700_000_000_000_000_000.0)),
        ("s", str_val("x")),
    ]);
    assert_eq!(
        eval_interval_bound(
            &expr_bound(Expr::Field(FieldRef::Simple("ts".into()))),
            &ctx,
            0
        ),
        Some(1_700_000_000_000_000_000)
    );
    assert!(
        eval_interval_bound(
            &expr_bound(Expr::Field(FieldRef::Simple("s".into()))),
            &ctx,
            0
        )
        .is_none()
    );
    assert!(
        eval_interval_bound(
            &expr_bound(Expr::Field(FieldRef::Simple("missing".into()))),
            &ctx,
            0
        )
        .is_none()
    );
    // Direct number literal expr.
    assert_eq!(
        eval_interval_bound(&expr_bound(Expr::Number(1.7e18)), &ctx, 0),
        Some(1_700_000_000_000_000_000)
    );
}

#[test]
fn enrich_join_row_and_first_join_key() {
    use crate::match_engine::executor::context::{
        enrich_join_row, first_join_key, row_matches_conds,
    };

    let join = JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Inner,
        conds: vec![],
        within: None,
        reduce: None,
        emit_at: None,
    };
    let row = join_row("id", 1.0, vec![("amt", num(5.0))]);
    let mut ctx = event(vec![("key", num(1.0)), ("existing", num(1.0))]);
    enrich_join_row(&mut ctx, &join, &row);
    assert_eq!(ctx.fields.get("w.id"), Some(&num(1.0)));
    assert_eq!(ctx.fields.get("id"), Some(&num(1.0)));
    assert_eq!(ctx.fields.get("w.amt"), Some(&num(5.0)));
    // Plain-name insertion does not override an existing field.
    assert_eq!(ctx.fields.get("key"), Some(&num(1.0)));
    assert_eq!(ctx.fields.get("existing"), Some(&num(1.0)));

    // first_join_key: empty conds / missing field → None.
    let conds = vec![JoinCondPlan {
        left: FieldRef::Simple("key".into()),
        right: FieldRef::Qualified("w".into(), "id".into()),
    }];
    assert_eq!(
        first_join_key(&ctx, &conds),
        Some(("id".to_string(), num(1.0)))
    );
    assert_eq!(first_join_key(&ctx, &[]), None);
    assert_eq!(first_join_key(&event(vec![]), &conds), None);

    // row_matches_conds: all conditions must hold; missing side → false.
    let conds = vec![
        JoinCondPlan {
            left: FieldRef::Simple("key".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        },
        JoinCondPlan {
            left: FieldRef::Simple("amt".into()),
            right: FieldRef::Qualified("w".into(), "amt".into()),
        },
    ];
    assert!(row_matches_conds(
        &row,
        &conds,
        &event(vec![("key", num(1.0)), ("amt", num(5.0))])
    ));
    assert!(!row_matches_conds(
        &row,
        &conds,
        &event(vec![("key", num(1.0)), ("amt", num(9.0))])
    ));
    assert!(!row_matches_conds(
        &row,
        &conds,
        &event(vec![("key", num(1.0))])
    ));
    assert!(!row_matches_conds(
        &row,
        &conds,
        &event(vec![("amt", num(5.0))])
    ));
    // Row missing the right field → false.
    let partial = join_row("other", 2.0, vec![]);
    assert!(!row_matches_conds(
        &partial,
        &conds,
        &event(vec![("key", num(1.0)), ("amt", num(5.0))])
    ));
}

// ---------------------------------------------------------------------------
// match_exec.rs — execute_match with joins / where
// ---------------------------------------------------------------------------

#[test]
fn execute_match_with_joins_rejections() {
    // Right-window row whose `id` matches the scope value (a string, matching
    // the rule key `sip`).
    let matched_row = || {
        let mut fields = EngineHashMap::default();
        fields.insert("id".into(), str_val("10.0.0.1"));
        fields.insert("amt".into(), num(5.0));
        JoinRow::Event(Arc::new(Event { fields }))
    };

    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.joins = vec![JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("sip".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();
    let lookup = RowsLookup::new(vec![matched_row()]);

    // Join hit → record; join miss → None.
    let rec = exec
        .execute_match_with_joins_at(&matched, &lookup, 123)
        .unwrap()
        .unwrap();
    assert_eq!(rec.score, 50.0);
    let mut missed = matched.clone();
    missed.scope_key = vec![str_val("10.9.9.9")];
    assert!(
        exec.execute_match_with_joins_at(&missed, &lookup, 123)
            .unwrap()
            .is_none()
    );

    // Post-join where reject → None.
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.joins = vec![JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("sip".into()),
            right: FieldRef::Qualified("w".into(), "id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("amt".into()))),
        right: Box::new(Expr::Number(5.0)),
    });
    let exec = RuleExecutor::new(plan);
    let matched = default_matched_context();
    let lookup = RowsLookup::new(vec![matched_row()]);
    // Snapshot join enriches; where checks the enriched `amt`.
    let rec = exec
        .execute_match_with_joins_at(&matched, &lookup, 123)
        .unwrap();
    assert!(rec.is_some());
    // Where reads an absent field → suppressed.
    let mut plan = simple_rule_plan(
        "r1",
        default_match_plan(),
        Expr::Number(50.0),
        "ip",
        Expr::Field(FieldRef::Simple("sip".into())),
    );
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("missing".into()))),
        right: Box::new(Expr::Number(5.0)),
    });
    let exec = RuleExecutor::new(plan);
    assert!(
        exec.execute_match_with_joins_at(&matched, &EmptyLookup, 123)
            .unwrap()
            .is_none()
    );
}
