//! coverage_r4 拆出的兄弟子模块（2026-09-04）：`executor/context.rs`
//! 覆盖——join 模式（inner / snapshot / anti / asof / 区间 within）、
//! enrich_join_row 与 eval-context 窄化（CloseCtxFields::Named）/ All 构建。
//! 共享 harness 与 plan 构造 helper 在父模块 `coverage_r4.rs`，此处经
//! `use super::*` 复用。

use super::*;

use crate::match_engine::TriggerEvent;
use crate::match_engine::cep::BindData;
use crate::match_engine::executor::context::{
    build_eval_context, enrich_join_row, execute_joins, in_interval,
};
use wf_lang::ast::{Bound, BoundVal, WithinSpec};

// ---------------------------------------------------------------------------
// executor/context.rs — join modes + interval joins + eval-context narrowing
// ---------------------------------------------------------------------------

#[test]
fn execute_joins_inner_snapshot_anti_and_enrich() {
    let row = join_row_event(vec![("id", num(1.0)), ("name", str_val("alice"))]);
    let lookup = RowsLookup::new(vec![row]);

    // Inner: hit enriches; missing left key drops; lookup miss drops.
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        1000
    ));
    assert_eq!(ctx.fields.get("w.id"), Some(&num(1.0)));
    assert_eq!(ctx.fields.get("name"), Some(&str_val("alice")));
    assert_eq!(ctx.fields.get("id"), Some(&num(1.0)));

    let mut ctx = event(vec![]);
    assert!(!execute_joins(
        &[one_cond_join(JoinMode::Inner)],
        &mut ctx,
        &lookup,
        1000
    ));

    let empty_lookup = RowsLookup::new(vec![]);
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(!execute_joins(
        &[one_cond_join(JoinMode::Inner)],
        &mut ctx,
        &empty_lookup,
        1000
    ));

    // Snapshot: hit enriches; miss keeps the event (optional join).
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Snapshot)],
        &mut ctx,
        &lookup,
        1000
    ));
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Snapshot)],
        &mut ctx,
        &empty_lookup,
        1000
    ));
    // Missing key on snapshot → continue (keep event).
    let mut ctx = event(vec![]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Snapshot)],
        &mut ctx,
        &lookup,
        1000
    ));

    // Anti: match drops; no match keeps.
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(!execute_joins(
        &[one_cond_join(JoinMode::Anti)],
        &mut ctx,
        &lookup,
        1000
    ));
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Anti)],
        &mut ctx,
        &empty_lookup,
        1000
    ));

    // emit_at set → join skipped entirely.
    let mut join = one_cond_join(JoinMode::Inner);
    join.emit_at = Some(Expr::Number(0.0));
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[join], &mut ctx, &lookup, 1000));
    assert!(!ctx.fields.contains_key("w.id"));
}

#[test]
fn execute_joins_asof_single_and_multi_cond_scan() {
    let row = join_row_event(vec![("id", num(1.0)), ("name", str_val("alice"))]);
    let lookup = RowsLookup::with_ts(vec![(500, row.clone())]);

    // Single-condition asof via Fallback → candidate scan.
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Asof {
            within: Some(Duration::from_secs(1))
        })],
        &mut ctx,
        &lookup,
        1000
    ));
    assert_eq!(ctx.fields.get("w.id"), Some(&num(1.0)));

    // Timestamp outside the asof window → no match, event kept.
    let late = RowsLookup::with_ts(vec![(1_000_000, row.clone())]);
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(
        &[one_cond_join(JoinMode::Asof {
            within: Some(Duration::from_millis(1))
        })],
        &mut ctx,
        &late,
        1000
    ));

    // Multi-condition asof → full scan path; no matching condition → kept.
    let mut join = one_cond_join(JoinMode::Asof { within: None });
    join.conds.push(JoinCondPlan {
        left: FieldRef::Simple("other".into()),
        right: FieldRef::Simple("name".into()),
    });
    let mut ctx = event(vec![("bidder", num(1.0)), ("other", str_val("bob"))]);
    assert!(execute_joins(&[join], &mut ctx, &lookup, 1000));
    assert!(!ctx.fields.contains_key("w.id"));
}

#[test]
fn execute_interval_join_modes() {
    // `within [0s, 60s]` inner: row at ts=30 matches.
    let row = join_row_event(vec![("id", num(1.0))]);
    let lookup = RowsLookup::with_ts(vec![(30, row)]);
    let mut join = one_cond_join(JoinMode::Inner);
    join.within = Some(WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::ZERO,
                neg: false,
            },
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(60),
                neg: false,
            },
        },
    });
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[join.clone()], &mut ctx, &lookup, 0));
    assert_eq!(ctx.fields.get("w.id"), Some(&num(1.0)));

    // Inner miss (no row in interval) → dropped.
    let lookup_miss = RowsLookup::with_ts(vec![(
        61_000_000_000,
        join_row_event(vec![("id", num(1.0))]),
    )]);
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(!execute_joins(&[join.clone()], &mut ctx, &lookup_miss, 0));

    // Anti interval: row in interval → dropped; none → kept.
    let mut anti = join.clone();
    anti.mode = JoinMode::Anti;
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(!execute_joins(&[anti.clone()], &mut ctx, &lookup, 0));
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[anti.clone()], &mut ctx, &lookup_miss, 0));

    // Asof interval: picks the max-ts matching row.
    let mut asof = join.clone();
    asof.mode = JoinMode::Asof { within: None };
    let multi = RowsLookup::with_ts(vec![
        (10, join_row_event(vec![("id", num(1.0))])),
        (50, join_row_event(vec![("id", num(1.0))])),
    ]);
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[asof], &mut ctx, &multi, 0));
    assert!(ctx.fields.contains_key("w.id"));

    // Snapshot interval: picks the min-ts row; open bounds respected.
    let mut snap = join.clone();
    snap.mode = JoinMode::Snapshot;
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[snap], &mut ctx, &multi, 0));

    // Bound eval failure (missing left field) → inner drops, snapshot keeps.
    let mut bad_join = one_cond_join(JoinMode::Inner);
    bad_join.within = Some(WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Expr(field("ghost_bound")),
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(60),
                neg: false,
            },
        },
    });
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(!execute_joins(&[bad_join], &mut ctx, &lookup, 0));
    let mut snap = one_cond_join(JoinMode::Snapshot);
    snap.within = Some(WithinSpec {
        lo: Bound {
            open: false,
            val: BoundVal::Expr(field("ghost_bound")),
        },
        hi: Bound {
            open: false,
            val: BoundVal::Dur {
                dur: Duration::from_secs(60),
                neg: false,
            },
        },
    });
    let mut ctx = event(vec![("bidder", num(1.0))]);
    assert!(execute_joins(&[snap], &mut ctx, &lookup, 0));
}

#[test]
fn in_interval_open_closed_bounds() {
    assert!(in_interval(5, 0, 10, false, false));
    assert!(!in_interval(0, 0, 10, true, false));
    assert!(!in_interval(10, 0, 10, false, true));
    assert!(in_interval(0, 0, 10, false, false));
}

#[test]
fn enrich_join_row_skips_null_fields() {
    let mut ctx = event(vec![]);
    // A JoinRow::Event with a null-free map — all fields enriched.
    let row = join_row_event(vec![("a", num(1.0)), ("b", str_val("x"))]);
    enrich_join_row(&mut ctx, &one_cond_join(JoinMode::Inner), &row);
    assert_eq!(ctx.fields.get("w.a"), Some(&num(1.0)));
    assert_eq!(ctx.fields.get("b"), Some(&str_val("x")));
}

#[test]
fn build_eval_context_narrow_and_all() {
    use crate::match_engine::executor::context::CloseCtxFields;

    let sd = StepData {
        satisfied_branch_index: 0,
        label: Some("fail".to_string()),
        measure_value: 3.0,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: vec![num(1.0), num(2.0)],
        field_values: EngineHashMap::from_iter([("src".to_string(), vec![str_val("10.0.0.1")])]),
    };
    let bind = BindData {
        alias: "b".into(),
        count: 2,
        field_values: EngineHashMap::from_iter([("dip".to_string(), vec![str_val("8.8.8.8")])]),
    };
    let keys = vec![simple_key("sip")];
    let scope = vec![str_val("10.0.0.1")];
    let step_plans: Vec<&StepPlan> = vec![];
    let trigger = event(vec![("raw", num(9.0))]);

    // Narrow build: only requested names.
    let narrow = CloseCtxFields::Named(HashSet::from([
        "sip".to_string(),
        "fail".to_string(),
        "src".to_string(),
        "dip".to_string(),
    ]));
    let ctx = build_eval_context(
        &keys,
        &scope,
        std::slice::from_ref(&sd),
        std::slice::from_ref(&bind),
        &step_plans,
        Some(&TriggerEvent::from_event(Arc::new(trigger.clone()))),
        &narrow,
        None,
    );
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("fail"), Some(&num(3.0)));
    assert_eq!(ctx.fields.get("src"), Some(&str_val("10.0.0.1")));
    assert_eq!(ctx.fields.get("dip"), Some(&str_val("8.8.8.8")));
    // `_step_*` synthetic fields absent in the narrow build.
    assert!(!ctx.fields.contains_key("_step_0_values"));
    // Named 窄化（2026-08 hotpath）：trigger_event 字段只注入 Named 集合内的；
    // "raw" 不在集合中 → 不注入（旧行为全量注入，是 per-fire 热路径浪费——
    // Q13 每事件 8 字段 → 1 字段）。All 模式下仍全量。
    assert!(
        !ctx.fields.contains_key("raw"),
        "narrow 构建不注入集合外字段"
    );

    // All build: synthetic fields present, key collision skips the label.
    let all = CloseCtxFields::All;
    let ctx = build_eval_context(
        &keys,
        &scope,
        std::slice::from_ref(&sd),
        std::slice::from_ref(&bind),
        &step_plans,
        None,
        &all,
        None,
    );
    assert_eq!(
        ctx.fields.get("_step_0_values"),
        Some(&Value::Array(vec![num(1.0), num(2.0)]))
    );
    assert_eq!(
        ctx.fields.get("_step_0_field_src"),
        Some(&Value::Array(vec![str_val("10.0.0.1")]))
    );
    assert_eq!(ctx.fields.get("_step_0_measure"), Some(&num(3.0)));
    assert_eq!(ctx.fields.get("_step_0_label"), Some(&str_val("fail")));
    assert_eq!(ctx.fields.get("_bind_b_count"), Some(&num(2.0)));
    assert_eq!(
        ctx.fields.get("_bind_b_field_dip"),
        Some(&Value::Array(vec![str_val("8.8.8.8")]))
    );
    // The `sip` key collides with the label-less name; key wins.
    assert_eq!(ctx.fields.get("sip"), Some(&str_val("10.0.0.1")));

    // All build with a step plan → `_step_0_source` injected.
    let plan = default_match_plan();
    let ctx = build_eval_context(
        &keys,
        &scope,
        &[sd],
        &[],
        &[&plan.event_steps[0]],
        None,
        &all,
        None,
    );
    assert_eq!(ctx.fields.get("_step_0_source"), Some(&str_val("fail")));
}
