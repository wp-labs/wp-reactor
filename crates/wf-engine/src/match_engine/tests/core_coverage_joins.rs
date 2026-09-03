//! core_coverage 拆出的兄弟子模块（2026-09-04）：`executor/context.rs` 覆盖——
//! build_eval_context（All / Named 物化、trigger 事件优先级、label 与 key 冲突）
//! 与 execute_joins 模式分派（inner / snapshot / anti / asof 快路径与回退 /
//! 区间 within 与 Expr 边界）、deferred emit_at 跳过、bound 表达式求值与 L1
//! match 表达式（eval_expr）。
//! JoinLookup / join_plan / step_data harness 在父模块 core_coverage.rs（close /
//! each 子模块共用），此处经 `use super::*` 复用。

use super::*;

// ===========================================================================
// executor/context.rs — build_eval_context + execute_joins mode dispatch
// ===========================================================================

#[test]
fn build_eval_context_all_mode_materializes_every_synthetic_field() {
    let keys = vec![simple_key("sip")];
    let scope_key = vec![str_val("10.0.0.1")];
    let sd = step_data(
        Some("fail"),
        5.0,
        vec![("user", vec![str_val("a"), str_val("b")])],
    );
    let bd = BindData {
        alias: "w".into(),
        count: 3,
        field_values: [("dport".to_string(), vec![num(80.0)])]
            .into_iter()
            .collect(),
    };
    let step_plans = [step(vec![branch("b", count_ge(1.0))])];
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[sd],
        &[bd],
        &[&step_plans[0]],
        None,
        &CloseCtxFields::All,
        None,
    );

    assert_eq!(ctx.fields["sip"], str_val("10.0.0.1"));
    assert_eq!(ctx.fields["fail"], Value::Number(5.0));
    assert_eq!(
        ctx.fields["_step_0_values"],
        Value::Array(vec![Value::Number(1.0), Value::Number(2.0)])
    );
    assert_eq!(
        ctx.fields["_step_0_field_user"],
        Value::Array(vec![str_val("a"), str_val("b")])
    );
    assert_eq!(
        ctx.fields["user"],
        str_val("b"),
        "last value wins for bare names"
    );
    assert_eq!(ctx.fields["_step_0_measure"], Value::Number(5.0));
    assert_eq!(ctx.fields["_step_0_label"], Value::Str("fail".into()));
    assert_eq!(ctx.fields["_step_0_source"], Value::Str("b".into()));
    assert_eq!(ctx.fields["_bind_w_count"], Value::Number(3.0));
    assert_eq!(
        ctx.fields["_bind_w_field_dport"],
        Value::Array(vec![num(80.0)])
    );
}

#[test]
fn build_eval_context_named_mode_and_trigger_event_precedence() {
    let keys = vec![simple_key("sip")];
    let scope_key = vec![str_val("10.0.0.1")];
    let sd = step_data(
        Some("fail"),
        5.0,
        vec![("user", vec![str_val("a"), str_val("b")])],
    );
    let bd = BindData {
        alias: "w".into(),
        count: 3,
        field_values: [("dport".to_string(), vec![num(80.0)])]
            .into_iter()
            .collect(),
    };
    let step_plans = [step(vec![branch("b", count_ge(1.0))])];
    let ctx = build_eval_context(
        &keys,
        &scope_key,
        &[sd],
        &[bd],
        &[&step_plans[0]],
        None,
        &CloseCtxFields::Named(HashSet::from(["user".to_string()])),
        None,
    );
    // Only the key + the one requested bare field are present.
    assert_eq!(ctx.fields["sip"], str_val("10.0.0.1"));
    assert_eq!(ctx.fields["user"], str_val("b"));
    assert!(!ctx.fields.contains_key("fail"));
    assert!(!ctx.fields.contains_key("_step_0_measure"));
    assert!(!ctx.fields.contains_key("_bind_w_count"));

    // Trigger event fields inject scalars the history lacks (keys win).
    let trigger = event(vec![("user", str_val("trigger-user")), ("extra", num(7.0))]);
    let ctx2 = build_eval_context(
        &keys,
        &scope_key,
        &[],
        &[],
        &[],
        Some(&TriggerEvent::from_event(Arc::new(trigger.clone()))),
        &CloseCtxFields::All,
        None,
    );
    assert_eq!(
        ctx2.fields["sip"],
        str_val("10.0.0.1"),
        "key wins over trigger"
    );
    assert_eq!(ctx2.fields["extra"], Value::Number(7.0));
    assert_eq!(ctx2.fields["user"], str_val("trigger-user"));

    // A step label colliding with a key field is skipped (key priority).
    let collision = step_data(Some("sip"), 99.0, vec![]);
    let ctx3 = build_eval_context(
        &keys,
        &scope_key,
        &[collision],
        &[],
        &[&step_plans[0]],
        None,
        &CloseCtxFields::All,
        None,
    );
    assert_eq!(
        ctx3.fields["sip"],
        str_val("10.0.0.1"),
        "label must not overwrite key"
    );
}

#[test]
fn execute_joins_inner_drops_on_miss_and_enriches_on_hit() {
    // Inner miss (no rows) → drop.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let lookup = JoinLookup::new();
    assert!(!execute_joins(
        &[join_plan(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
    assert!(!ctx.fields.contains_key("geo.ip"));

    // Inner hit → enriched with qualified + plain names.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("US"))],
    );
    assert!(execute_joins(
        &[join_plan(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
    assert_eq!(ctx.fields["geo.ip"], str_val("10.0.0.1"));
    assert_eq!(ctx.fields["geo.country"], str_val("US"));
    assert_eq!(
        ctx.fields["country"],
        str_val("US"),
        "plain name enriched when absent"
    );

    // Inner with the left key field missing → drop without a lookup.
    let mut ctx = event(vec![]);
    assert!(!execute_joins(
        &[join_plan(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
}

#[test]
fn execute_joins_snapshot_miss_keeps_event_and_anti_drops_on_match() {
    // Snapshot miss → keep the event, no enrichment.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let lookup = JoinLookup::new();
    assert!(execute_joins(
        &[join_plan(JoinMode::Snapshot, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
    assert!(!ctx.fields.contains_key("geo.ip"));

    // Snapshot miss with rows present but none matching → still kept.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.2")), ("country", str_val("DE"))],
    );
    assert!(execute_joins(
        &[join_plan(JoinMode::Snapshot, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
    assert!(!ctx.fields.contains_key("geo.country"));

    // Anti: matching row → drop.
    let mut lookup_match = JoinLookup::new();
    lookup_match.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("US"))],
    );
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(!execute_joins(
        &[join_plan(JoinMode::Anti, "geo", "sip", "ip")],
        &mut ctx,
        &lookup_match,
        0
    ));
    // Anti: no matching row → keep, no enrichment.
    let mut ctx = event(vec![("sip", str_val("10.0.0.9"))]);
    assert!(execute_joins(
        &[join_plan(JoinMode::Anti, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        0
    ));
    assert!(!ctx.fields.contains_key("geo.ip"), "anti never enriches");
}

#[test]
fn execute_joins_asof_fast_path_hit_miss_and_fallback() {
    // Fast-path Hit → enriched with the provided row.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.asof_fast = Some(AsofOutcome::Hit(JoinLookup::row(vec![
        ("ip", str_val("10.0.0.1")),
        ("risk", num(90.0)),
    ])));
    assert!(execute_joins(
        &[join_plan(
            JoinMode::Asof { within: None },
            "ti",
            "sip",
            "ip"
        )],
        &mut ctx,
        &lookup,
        1_000
    ));
    assert_eq!(ctx.fields["ti.risk"], Value::Number(90.0));

    // Fast-path Miss → keep the event without enrichment.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.asof_fast = Some(AsofOutcome::Miss);
    assert!(execute_joins(
        &[join_plan(
            JoinMode::Asof { within: None },
            "ti",
            "sip",
            "ip"
        )],
        &mut ctx,
        &lookup,
        1_000
    ));
    assert!(!ctx.fields.contains_key("ti.risk"));

    // Fallback → timestamped candidate scan picks the latest ts ≤ event time.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "ti",
        200,
        vec![("ip", str_val("10.0.0.1")), ("risk", num(50.0))],
    );
    lookup.add_ts_row(
        "ti",
        800,
        vec![("ip", str_val("10.0.0.1")), ("risk", num(90.0))],
    );
    lookup.add_ts_row(
        "ti",
        2_000,
        vec![("ip", str_val("10.0.0.1")), ("risk", num(99.0))],
    );
    assert!(execute_joins(
        &[join_plan(
            JoinMode::Asof { within: None },
            "ti",
            "sip",
            "ip"
        )],
        &mut ctx,
        &lookup,
        1_000
    ));
    assert_eq!(
        ctx.fields["ti.risk"],
        Value::Number(90.0),
        "latest row ≤ event time"
    );

    // with `within`, rows older than event_time - within are excluded.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "ti",
        100,
        vec![("ip", str_val("10.0.0.1")), ("risk", num(10.0))],
    );
    lookup.add_ts_row(
        "ti",
        900,
        vec![("ip", str_val("10.0.0.1")), ("risk", num(90.0))],
    );
    let within_join = JoinPlan {
        mode: JoinMode::Asof {
            within: Some(Duration::from_millis(500)),
        },
        ..join_plan(JoinMode::Asof { within: None }, "ti", "sip", "ip")
    };
    assert!(execute_joins(&[within_join], &mut ctx, &lookup, 1_000));
    assert_eq!(ctx.fields["ti.risk"], Value::Number(90.0));
}

#[test]
fn execute_joins_asof_multi_condition_uses_candidate_scan() {
    // Two conditions force the full asof_candidates scan (no fast path).
    let mut ctx = event(vec![("sip", str_val("10.0.0.1")), ("zone", num(7.0))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "ti",
        100,
        vec![
            ("ip", str_val("10.0.0.1")),
            ("zone", num(7.0)),
            ("risk", num(10.0)),
        ],
    );
    lookup.add_ts_row(
        "ti",
        500,
        vec![
            ("ip", str_val("10.0.0.1")),
            ("zone", num(8.0)),
            ("risk", num(99.0)),
        ],
    );
    lookup.add_ts_row(
        "ti",
        900,
        vec![
            ("ip", str_val("10.0.0.1")),
            ("zone", num(7.0)),
            ("risk", num(80.0)),
        ],
    );
    let multi = JoinPlan {
        conds: vec![
            JoinCondPlan {
                left: FieldRef::Simple("sip".into()),
                right: FieldRef::Simple("ip".into()),
            },
            JoinCondPlan {
                left: FieldRef::Simple("zone".into()),
                right: FieldRef::Simple("zone".into()),
            },
        ],
        ..join_plan(JoinMode::Asof { within: None }, "ti", "sip", "ip")
    };
    assert!(execute_joins(&[multi], &mut ctx, &lookup, 1_000));
    // The zone=8 row (ts=500, newer than zone=7 ts=100) fails the second cond;
    // the newest matching row is ts=900 (zone=7).
    assert_eq!(ctx.fields["ti.risk"], Value::Number(80.0));
}

fn interval_join(mode: JoinMode, window: &str, left: &str, right: &str) -> JoinPlan {
    JoinPlan {
        right_window: window.to_string(),
        mode,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple(left.to_string()),
            right: FieldRef::Simple(right.to_string()),
        }],
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::from_millis(500),
                    neg: true,
                },
            },
            hi: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::ZERO,
                    neg: false,
                },
            },
        }),
        reduce: None,
        emit_at: None,
    }
}

#[test]
fn execute_joins_interval_inner_hit_miss_and_open_bound() {
    let event_time = 1_000_000_000i64; // 1s

    // Inner hit inside [event-500ms, event] → enriched, kept.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "geo",
        750_000_000,
        vec![("ip", str_val("10.0.0.1")), ("city", str_val("NYC"))],
    );
    assert!(execute_joins(
        &[interval_join(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));
    assert_eq!(ctx.fields["geo.city"], str_val("NYC"));

    // Inner miss (row outside the interval) → dropped.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "geo",
        100_000_000,
        vec![("ip", str_val("10.0.0.1")), ("city", str_val("SF"))],
    );
    assert!(!execute_joins(
        &[interval_join(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));

    // Inner miss (no candidate rows at all) → dropped.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let lookup = JoinLookup::new();
    assert!(!execute_joins(
        &[interval_join(JoinMode::Inner, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));

    // Open upper bound: a row exactly at `event_time` is excluded.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "geo",
        event_time,
        vec![("ip", str_val("10.0.0.1")), ("city", str_val("BOS"))],
    );
    let open_hi = JoinPlan {
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Dur {
                    dur: Duration::from_millis(500),
                    neg: true,
                },
            },
            hi: Bound {
                open: true,
                val: BoundVal::Dur {
                    dur: Duration::ZERO,
                    neg: false,
                },
            },
        }),
        ..interval_join(JoinMode::Inner, "geo", "sip", "ip")
    };
    assert!(!execute_joins(&[open_hi], &mut ctx, &lookup, event_time));
}

#[test]
fn execute_joins_interval_modes_anti_asof_snapshot_and_bound_expression() {
    let event_time = 1_000_000_000i64;

    // Anti: a row inside the interval → drop; none → keep.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row("geo", 800_000_000, vec![("ip", str_val("10.0.0.1"))]);
    assert!(!execute_joins(
        &[interval_join(JoinMode::Anti, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));
    let mut ctx = event(vec![("sip", str_val("10.0.0.9"))]);
    assert!(execute_joins(
        &[interval_join(JoinMode::Anti, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));

    // Asof inside interval → latest ts; Snapshot → earliest ts.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "geo",
        600_000_000,
        vec![("ip", str_val("10.0.0.1")), ("v", num(1.0))],
    );
    lookup.add_ts_row(
        "geo",
        900_000_000,
        vec![("ip", str_val("10.0.0.1")), ("v", num(2.0))],
    );
    assert!(execute_joins(
        &[interval_join(
            JoinMode::Asof { within: None },
            "geo",
            "sip",
            "ip"
        )],
        &mut ctx,
        &lookup,
        event_time
    ));
    assert_eq!(
        ctx.fields["geo.v"],
        Value::Number(2.0),
        "interval asof picks latest"
    );
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(execute_joins(
        &[interval_join(JoinMode::Snapshot, "geo", "sip", "ip")],
        &mut ctx,
        &lookup,
        event_time
    ));
    assert_eq!(
        ctx.fields["geo.v"],
        Value::Number(1.0),
        "interval snapshot picks earliest"
    );

    // Expr bound: evaluates the left row's absolute time field. A missing
    // field on an Inner join → conservative drop.
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row("geo", 800_000_000, vec![("ip", str_val("10.0.0.1"))]);
    let expr_bounds = JoinPlan {
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Simple("lo_field".into()))),
            },
            hi: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Simple("hi_field".into()))),
            },
        }),
        ..interval_join(JoinMode::Inner, "geo", "sip", "ip")
    };
    assert!(!execute_joins(
        &[expr_bounds],
        &mut ctx,
        &lookup,
        event_time
    ));

    // Expr bound with a valid numeric field on the left row. Epoch
    // normalization maps 0.8 → 8e8 ns and 1.0 → 1e9 ns, which contains the
    // 8e8-ns candidate row.
    let mut ctx = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("lo_field", num(0.8)),
        ("hi_field", num(1.0)),
    ]);
    let mut lookup = JoinLookup::new();
    lookup.add_ts_row(
        "geo",
        800_000_000,
        vec![("ip", str_val("10.0.0.1")), ("city", str_val("CHI"))],
    );
    let expr_bounds = JoinPlan {
        within: Some(WithinSpec {
            lo: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Simple("lo_field".into()))),
            },
            hi: Bound {
                open: false,
                val: BoundVal::Expr(Expr::Field(FieldRef::Simple("hi_field".into()))),
            },
        }),
        ..interval_join(JoinMode::Inner, "geo", "sip", "ip")
    };
    assert!(execute_joins(&[expr_bounds], &mut ctx, &lookup, event_time));
    assert_eq!(ctx.fields["geo.city"], str_val("CHI"));
}

#[test]
fn execute_joins_skips_emit_at_deferred_joins() {
    let mut ctx = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut lookup = JoinLookup::new();
    lookup.add_row(
        "geo",
        vec![("ip", str_val("10.0.0.1")), ("country", str_val("US"))],
    );
    let deferred = JoinPlan {
        emit_at: Some(Expr::Number(1.0)),
        ..join_plan(JoinMode::Inner, "geo", "sip", "ip")
    };
    // `emit at` joins are handled by the deferred path — eager path skips.
    assert!(execute_joins(&[deferred], &mut ctx, &lookup, 0));
    assert!(!ctx.fields.contains_key("geo.country"));
}

#[test]
fn eval_expr_resolves_event_fields_for_bound_expressions() {
    let ev = event(vec![("x", num(42.0)), ("s", str_val("ok"))]);
    assert_eq!(
        eval_expr(&Expr::Field(FieldRef::Simple("x".into())), &ev),
        Some(Value::Number(42.0))
    );
    assert_eq!(
        eval_expr(
            &Expr::BinOp {
                op: BinOp::Add,
                left: Box::new(Expr::Field(FieldRef::Simple("x".into()))),
                right: Box::new(Expr::Number(8.0)),
            },
            &ev
        ),
        Some(Value::Number(50.0))
    );
    assert_eq!(
        eval_expr(&Expr::Field(FieldRef::Simple("missing".into())), &ev),
        None
    );
}

/// L1 求值器（eval_expr_ext，guard/where 路径）的 match 表达式（issue #79
/// Issue 2）：多模式命中、默认分支、无默认未命中 → None。
#[test]
fn eval_expr_l1_match_expression() {
    use wf_lang::ast::MatchArm;
    let ev = event(vec![("sev", str_val("crit")), ("n", num(2.0))]);
    let sev = Expr::Match {
        expr: Box::new(Expr::Field(FieldRef::Simple("sev".into()))),
        arms: vec![MatchArm {
            patterns: vec![
                Expr::StringLit("crit".into()),
                Expr::StringLit("alert".into()),
            ],
            value: Expr::StringLit("CRITICAL".into()),
        }],
        default: Some(Box::new(Expr::Field(FieldRef::Simple("sev".into())))),
    };
    assert_eq!(
        eval_expr(&sev, &ev),
        Some(Value::Str("CRITICAL".into())),
        "crit | alert → CRITICAL"
    );
    let ev2 = event(vec![("sev", str_val("info"))]);
    assert_eq!(
        eval_expr(&sev, &ev2),
        Some(Value::Str("info".into())),
        "未命中 → 默认分支（原值透传）"
    );
    // 无默认且未命中 → None（guard 语义：filter 不通过）。
    let no_default = Expr::Match {
        expr: Box::new(Expr::Field(FieldRef::Simple("n".into()))),
        arms: vec![MatchArm {
            patterns: vec![Expr::Number(1.0), Expr::Number(2.0)],
            value: Expr::Bool(true),
        }],
        default: None,
    };
    assert_eq!(
        eval_expr(&no_default, &ev),
        Some(Value::Bool(true)),
        "n=2 命中数字模式"
    );
    let ev3 = event(vec![("n", num(9.0))]);
    assert_eq!(eval_expr(&no_default, &ev3), None, "无默认且未命中 → None");
}
