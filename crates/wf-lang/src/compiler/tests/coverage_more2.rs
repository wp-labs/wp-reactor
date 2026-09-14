//! Third-wave compiler coverage tests for compiler/mod.rs: stats-rule
//! compilation (session windows / columns shape / every agg kind / tracked
//! fields), limits + byte-size/rate parsing, conv dedup/where/ascending ops,
//! `needs_field_history` multi-bind and join paths, pipeline stage unlabeled
//! measure outputs and no-key pipeline entities, plus direct unit coverage for
//! the bind-tracking and label-rewrite helpers.

use std::time::Duration;

use crate::ast::{BinOp, Expr, FieldRef, ObjectItem, PathSegment};
use crate::compiler::{collect_rule_bind_tracking, compile_wfl, compile_wfl_after_semantic_checks};
use crate::plan::{
    ConvOpPlan, ExceedAction, RateSpec, StatsAggPlan, StatsOutputShapePlan, WindowSpec, YieldField,
};
use crate::wfl_parser::parse_wfl;

use super::*;

/// Driver window for join tests (id / category / expires shapes).
fn auction_events_window() -> WindowSchema {
    make_window(
        "auction_events",
        vec!["auction_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("category", bt(BaseType::Chars)),
            ("dateTime", bt(BaseType::Time)),
            ("expires", bt(BaseType::Time)),
        ],
    )
}

/// Snapshot-join target carrying `auction` / `bidder` / `price`.
fn bid_events_window() -> WindowSchema {
    make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Chars)),
            ("price", bt(BaseType::Digit)),
            ("dateTime", bt(BaseType::Time)),
        ],
    )
}

/// Output window with bool + numeric fields for rewrite tests.
fn labeled_out_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("n", bt(BaseType::Digit)),
            ("b", bt(BaseType::Bool)),
            ("y", bt(BaseType::Chars)),
        ],
    )
}

// ---------------------------------------------------------------------------
// compile_stats_rule — session windows / output shapes / all agg kinds
// ---------------------------------------------------------------------------

#[test]
fn stats_rule_session_columns_shape_all_agg_kinds() {
    let plans = compile_with(
        r#"
rule stats_all {
    events { a : auth_events }
    stats<30s:session> tier a.count [<100, <1000] {
        a | count as total where a.count > 0;
        a | sum(a.count) as s;
        a | avg(a.count) as v;
        a | min(a.count) as lo;
        a | max(a.count) as hi;
        a | distinct_count(a.sip) as uniq;
        a | last(a.sip) as latest;
        a | top(2, a.sip) as top_sip;
    }
    entity(digit, 1)
    yield out (y = "x")
}
rule stats_group {
    events { a : auth_events }
    stats<10s> group by (a.sip) {
        a | count as total;
    }
    entity(digit, 1)
    yield out (y = "x")
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = &plans[0];
    let stats = plan.stats_plan.as_ref().expect("stats plan");
    assert_eq!(
        stats.window_spec,
        WindowSpec::Session(Duration::from_secs(30))
    );
    // tier bucket key → 1 key expression, columns output shape.
    assert_eq!(stats.keys.len(), 1, "tier bucket key");
    assert_eq!(stats.output_shape, StatsOutputShapePlan::Columns);

    let aggs: Vec<StatsAggPlan> = stats.measures.iter().map(|m| m.agg).collect();
    assert_eq!(
        aggs,
        vec![
            StatsAggPlan::Count,
            StatsAggPlan::Sum,
            StatsAggPlan::Avg,
            StatsAggPlan::Min,
            StatsAggPlan::Max,
            StatsAggPlan::DistinctCount,
            StatsAggPlan::Last,
            StatsAggPlan::Top,
        ]
    );
    assert!(
        stats.measures[0].where_expr.is_some(),
        "measure `where` expression must be carried into the plan"
    );
    assert_eq!(stats.measures[7].arg, Some(2), "top(2, ...) arg");

    // measure field + where/keys fields are tracked for materialization.
    let tracked = stats
        .tracked_bind_fields
        .get("a")
        .expect("source alias a tracked");
    assert!(tracked.contains("count"), "sum(a.count) + where a.count");
    assert!(tracked.contains("sip"), "distinct_count/last/top fields");

    // stats rules keep an empty match plan placeholder.
    assert!(plan.match_plan.event_steps.is_empty());
    assert_eq!(
        plan.match_plan.window_spec,
        WindowSpec::Fixed(Duration::ZERO),
        "empty match plan placeholder window"
    );
    assert!(plan.each_plan.is_none());
    assert!(plan.joins.is_empty());
    assert!(plan.pattern_origin.is_none());
    assert!(plan.conv_plan.is_none());
    assert!(plan.conv_window.is_none());

    // A plain group-by stats rule stays rows-shaped with a bucket key.
    let group_plan = &plans[1];
    let group_stats = group_plan.stats_plan.as_ref().expect("group stats plan");
    assert_eq!(
        group_stats.window_spec,
        WindowSpec::Fixed(Duration::from_secs(10))
    );
    assert_eq!(group_stats.output_shape, StatsOutputShapePlan::Rows);
    assert_eq!(group_stats.keys.len(), 1, "group-by bucket key");
}

#[test]
fn stats_rule_rows_shape_default_fixed() {
    let plans = compile_with(
        r#"
rule stats_rows {
    events { a : auth_events }
    stats<10s> {
        a | count as total;
    }
    entity(digit, 1)
    yield out (y = "x")
}
"#,
        &[auth_events_window(), output_window()],
    );
    let stats = plans[0].stats_plan.as_ref().expect("stats plan");
    assert_eq!(
        stats.window_spec,
        WindowSpec::Fixed(Duration::from_secs(10))
    );
    assert_eq!(stats.output_shape, StatsOutputShapePlan::Rows);
    assert!(stats.keys.is_empty(), "no group by / tier → no bucket keys");
}

// ---------------------------------------------------------------------------
// compile_limits — parse_byte_size / parse_rate_spec / on_exceed
// ---------------------------------------------------------------------------

fn limits_rule(name: &str, limits: &str) -> String {
    format!(
        r#"
rule {name} {{
    events {{ e : auth_events }}
    match<sip:5m> {{ on event {{ e | count >= 1; }} }} -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits {{ {limits} }}
}}
"#
    )
}

#[test]
fn limits_all_keys_and_units_compiled() {
    let src = [
        limits_rule("lim_gb", "max_memory = 2GB; max_instances = 100; max_throttle = 1000/min; on_exceed = drop_oldest;"),
        limits_rule("lim_mb", "max_memory = 256MB; max_throttle = 5/s; on_exceed = fail_rule;"),
        limits_rule("lim_kb", "max_memory = 512KB; max_throttle = 2/h; on_exceed = throttle;"),
        limits_rule("lim_m", "max_throttle = 3/m;"),
        limits_rule("lim_sec", "max_throttle = 1/sec;"),
        limits_rule("lim_hr", "max_throttle = 1/hr;"),
        limits_rule("lim_hour", "max_throttle = 1/hour;"),
        limits_rule("lim_d", "max_throttle = 10/d;"),
        limits_rule("lim_day", "max_throttle = 1/day;"),
    ]
    .join("\n");
    let plans = compile_with(&src, &[auth_events_window(), output_window()]);

    let by_name = |name: &str| {
        plans
            .iter()
            .find(|p| p.name == name)
            .unwrap_or_else(|| panic!("missing plan {name}"))
            .limits_plan
            .clone()
            .expect("limits plan")
    };

    let gb = by_name("lim_gb");
    assert_eq!(gb.max_memory_bytes, Some(2 * 1024 * 1024 * 1024));
    assert_eq!(gb.max_instances, Some(100));
    assert_eq!(
        gb.max_throttle,
        Some(RateSpec {
            count: 1000,
            per: Duration::from_secs(60)
        })
    );
    assert_eq!(gb.on_exceed, ExceedAction::DropOldest);

    let mb = by_name("lim_mb");
    assert_eq!(mb.max_memory_bytes, Some(256 * 1024 * 1024));
    assert_eq!(
        mb.max_throttle,
        Some(RateSpec {
            count: 5,
            per: Duration::from_secs(1)
        })
    );
    assert_eq!(mb.on_exceed, ExceedAction::FailRule);

    let kb = by_name("lim_kb");
    assert_eq!(kb.max_memory_bytes, Some(512 * 1024));
    assert_eq!(
        kb.max_throttle,
        Some(RateSpec {
            count: 2,
            per: Duration::from_secs(3600)
        })
    );
    // default action when `on_exceed` omitted.
    assert_eq!(kb.on_exceed, ExceedAction::Throttle);
    assert!(kb.max_instances.is_none());

    let by_unit = |name: &str, count: u64, per_secs: u64| {
        let l = by_name(name);
        assert_eq!(
            l.max_throttle,
            Some(RateSpec {
                count,
                per: Duration::from_secs(per_secs)
            }),
            "unit parsing for {name}"
        );
    };
    by_unit("lim_m", 3, 60);
    by_unit("lim_sec", 1, 1);
    by_unit("lim_hr", 1, 3600);
    by_unit("lim_hour", 1, 3600);
    by_unit("lim_d", 10, 86400);
    by_unit("lim_day", 1, 86400);
}

// ---------------------------------------------------------------------------
// compile_conv — dedup / where / ascending sort ops
// ---------------------------------------------------------------------------

#[test]
fn conv_all_op_kinds_compiled() {
    let plans = compile_with(
        r#"
rule conv_all {
    events { e : auth_events }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv {
        sort(-e.count) | top(10);
        sort(e.sip) | dedup(e.sip) | where(e.count > 1);
    }
}
"#,
        &[auth_events_window(), output_window()],
    );
    let conv = plans[0].conv_plan.as_ref().expect("conv plan");
    assert_eq!(conv.chains.len(), 2);

    let ops0 = &conv.chains[0].ops;
    // `sort(-e.count)` — the `-` prefix is the descending marker, so the
    // expression stays `e.count` while `descending` is true.
    assert!(matches!(
        &ops0[0],
        ConvOpPlan::Sort(keys) if keys.len() == 1 && keys[0].descending
            && matches!(&keys[0].expr, Expr::Field(FieldRef::Qualified(a, f)) if a == "e" && f == "count")
    ));
    assert!(matches!(&ops0[1], ConvOpPlan::Top(10)));

    let ops1 = &conv.chains[1].ops;
    assert!(matches!(
        &ops1[0],
        ConvOpPlan::Sort(keys) if keys.len() == 1 && !keys[0].descending
    ));
    assert!(matches!(
        &ops1[1],
        ConvOpPlan::Dedup(Expr::Field(FieldRef::Qualified(a, f))) if a == "e" && f == "sip"
    ));
    assert!(matches!(&ops1[2], ConvOpPlan::Where(_)));

    // fixed-window conv rule also gets the auto conv aggregation window.
    let cw = plans[0].conv_window.as_ref().expect("conv window");
    assert_eq!(cw.over, Duration::from_secs(3600));
}

// ---------------------------------------------------------------------------
// compute_needs_field_history — multi-bind / join paths
// ---------------------------------------------------------------------------

#[test]
fn multi_bind_rule_needs_field_history() {
    let plans = compile_with(
        r#"
rule multi_bind {
    events { a : auth_events  b : fw_events }
    match<sip:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#,
        &[auth_events_window(), fw_events_window(), output_window()],
    );
    assert!(
        plans[0].match_plan.needs_field_history,
        "multi-bind rules always need the per-alias field history"
    );
}

#[test]
fn join_rule_needs_field_history() {
    let plans = compile_with(
        r#"
rule join_hist {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events snapshot on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#,
        &[
            auction_events_window(),
            bid_events_window(),
            output_window(),
        ],
    );
    assert!(
        plans[0].match_plan.needs_field_history,
        "joins always need the field history"
    );
}

// ---------------------------------------------------------------------------
// compile_pipeline_rule — unlabeled measures / no-key pipeline entities
// ---------------------------------------------------------------------------

#[test]
fn pipeline_stage_unlabeled_measures_use_measure_names() {
    let plans = compile_with(
        r#"
rule pipe_measures {
    events { a : auth_events }
    match<sip:5m> {
        on event {
            a | count >= 1;
            a.count | sum >= 1;
            a.count | avg >= 1;
            a.count | min >= 1;
            a.count | max >= 1;
        }
    }
    |> match<sip:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, _in.sip)
    yield out (x = _in.sip)
}
"#,
        &[auth_events_window(), output_window()],
    );
    assert_eq!(plans.len(), 2);
    let stage1 = &plans[0];
    assert_eq!(stage1.name, "__wf_pipe_pipe_measures_s1");

    // Non-final stage branches get implicit labels from the measure name, and
    // the auto stage yield emits those labels as fields.
    let names: Vec<&str> = stage1
        .yield_plan
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    for measure in ["sip", "count", "sum", "avg", "min", "max"] {
        assert!(
            names.contains(&measure),
            "stage 1 yield should carry {measure}, got {names:?}"
        );
    }

    // Each `;`-terminated entry is its own step; the unlabeled branches of a
    // non-final stage are labeled with the measure name.
    assert_eq!(stage1.match_plan.event_steps.len(), 5);
    let labels: Vec<Option<&str>> = stage1
        .match_plan
        .event_steps
        .iter()
        .map(|s| s.branches[0].label.as_deref())
        .collect();
    assert_eq!(
        labels,
        vec![
            Some("count"),
            Some("sum"),
            Some("avg"),
            Some("min"),
            Some("max")
        ]
    );
}

#[test]
fn pipeline_no_key_stage_uses_pipeline_entity() {
    let plans = compile_with(
        r#"
rule no_key_pipe {
    events { a : fw_events }
    match<sip:5m> { on event { a | count >= 1; } }
    |> match<:5m> { on event { _in | count >= 1; } }
    |> match<:5m> { on event { _in | count >= 1; } } -> score(1.0)
    entity(ip, "static")
    yield out (y = "x")
}
"#,
        &[fw_events_window(), output_window()],
    );
    assert_eq!(plans.len(), 3);

    // Stage 1 keys on `sip` → pipeline entity reads the key output field.
    let stage1 = &plans[0];
    assert_eq!(stage1.entity_plan.entity_type, "pipeline");
    assert_eq!(
        stage1.entity_plan.entity_id_expr,
        Expr::Field(FieldRef::Simple("sip".into()))
    );

    // Stage 2 has no keys → pipeline entity falls back to the `__pipeline` id.
    let stage2 = &plans[1];
    assert_eq!(stage2.entity_plan.entity_type, "pipeline");
    assert_eq!(
        stage2.entity_plan.entity_id_expr,
        Expr::StringLit("__pipeline".to_string())
    );

    // Final stage uses the user-declared entity clause.
    let final_plan = &plans[2];
    assert_eq!(final_plan.entity_plan.entity_type, "ip");
    assert_eq!(
        final_plan.entity_plan.entity_id_expr,
        Expr::StringLit("static".to_string())
    );
}

// ---------------------------------------------------------------------------
// collect_bind_tracking — composite expression shapes
// ---------------------------------------------------------------------------

#[test]
fn collect_bind_tracking_covers_composite_expr_shapes() {
    let score = Expr::BinOp {
        op: BinOp::Add,
        left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
        right: Box::new(Expr::Neg(Box::new(Expr::Field(FieldRef::Qualified(
            "e".into(),
            "dip".into(),
        ))))),
    };
    // Bare stat selector: early-return — nothing under it is tracked.
    let entity = Expr::FuncCall {
        qualifier: None,
        name: "window_event".into(),
        args: vec![Expr::Field(FieldRef::Simple("alias_x".into()))],
    };
    let yield_fields = vec![
        YieldField {
            name: "in_".into(),
            value: Expr::InList {
                expr: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "dport".into()))),
                list: vec![Expr::Field(FieldRef::Simple("plain_1".into()))],
                negated: false,
            },
        },
        YieldField {
            name: "ite".into(),
            value: Expr::IfThenElse {
                cond: Box::new(Expr::Bool(true)),
                then_expr: Box::new(Expr::FuncCall {
                    qualifier: None,
                    // series func whose first arg is a bare alias → not a
                    // column projection, so only the plain field is tracked.
                    name: "count".into(),
                    args: vec![Expr::Field(FieldRef::Simple("e".into()))],
                }),
                else_expr: Box::new(Expr::Number(0.0)),
            },
        },
        YieldField {
            name: "obj".into(),
            value: Expr::Object(vec![ObjectItem {
                targets: vec!["k".into()],
                type_hint: None,
                value: Expr::Field(FieldRef::Qualified("e".into(), "user".into())),
            }]),
        },
        YieldField {
            name: "arr".into(),
            value: Expr::Array(vec![Expr::Field(FieldRef::Qualified(
                "e".into(),
                "action".into(),
            ))]),
        },
        YieldField {
            name: "path".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "e".into(),
                segments: vec![PathSegment::Field("roles_obj".into())],
            }),
        },
    ];

    let tracking = collect_rule_bind_tracking(&score, &entity, &yield_fields, &[]);
    assert!(tracking.aliases.contains("e"), "alias e from refs/paths");
    assert!(
        !tracking.aliases.contains("alias_x"),
        "bare stat selector args are not tracked"
    );
    assert!(tracking.plain_fields.contains("plain_1"));
    assert!(tracking.plain_fields.contains("e"), "series-func bare arg");

    let fields = tracking.fields.get("e").expect("alias e tracked fields");
    for f in ["sip", "dip", "dport", "user", "action", "roles_obj"] {
        assert!(fields.contains(f), "expected {f} in {fields:?}");
    }
}

// ---------------------------------------------------------------------------
// rewrite_expr_label_refs — InList / IfThenElse recursion
// ---------------------------------------------------------------------------

#[test]
fn label_refs_rewritten_inside_inlist_and_if_then_else() {
    let plans = compile_with(
        r#"
rule q9b {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(price) on a.id == bid_events.auction as winner
    entity(digit, a.id)
    yield out (
        n = if winner.price > 100.0 then 1 else 0,
        b = winner.bidder in ("x", "y"),
        y = "ok"
    )
}
"#,
        &[
            auction_events_window(),
            bid_events_window(),
            labeled_out_window(),
        ],
    );
    let plan = &plans[0];

    let ite = plan
        .yield_plan
        .fields
        .iter()
        .find(|f| f.name == "n")
        .expect("n field");
    assert!(
        matches!(
            &ite.value,
            Expr::IfThenElse {
                cond,
                then_expr,
                else_expr,
            } if matches!(
                cond.as_ref(),
                Expr::BinOp { left, op: BinOp::Gt, .. } if matches!(
                    left.as_ref(),
                    Expr::Field(FieldRef::Path { alias, segments }) if alias == "winner"
                        && segments == &[PathSegment::Field("price".into())]
                )
            ) && matches!(then_expr.as_ref(), Expr::Number(1.0))
                && matches!(else_expr.as_ref(), Expr::Number(0.0))
        ),
        "label ref inside if-then-else must be rewritten to a Path: {:?}",
        ite.value
    );

    let in_list = plan
        .yield_plan
        .fields
        .iter()
        .find(|f| f.name == "b")
        .expect("b field");
    assert!(
        matches!(
            &in_list.value,
            Expr::InList { expr, list, negated } if matches!(
                expr.as_ref(),
                Expr::Field(FieldRef::Path { alias, segments }) if alias == "winner"
                    && segments == &[PathSegment::Field("bidder".into())]
            ) && list.len() == 2
                && !negated
        ),
        "label ref inside `in` must be rewritten to a Path: {:?}",
        in_list.value
    );
}

// ---------------------------------------------------------------------------
// rewrite_within_label_refs — constant (duration) bounds with labels present
// ---------------------------------------------------------------------------

#[test]
fn within_duration_bounds_rewritten_when_labels_present() {
    let plans = compile_with(
        r#"
rule within_dur {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(price) on a.id == bid_events.auction as winner
    join bid_events within 10s on a.id == bid_events.auction
    entity(digit, a.id)
    yield out (n = a.id)
}
"#,
        &[
            auction_events_window(),
            bid_events_window(),
            output_window(),
        ],
    );
    // `within 10s` sugar = [-10s, 0s]; with a reduce label present in the rule,
    // the duration-bound rewrite path runs without touching the bounds.
    let join = &plans[0].joins[1];
    let w = join.within.as_ref().expect("within plan");
    assert!(
        matches!(&w.lo.val, crate::ast::BoundVal::Dur { dur, neg } if dur.as_secs() == 10 && *neg)
    );
    assert!(matches!(&w.hi.val, crate::ast::BoundVal::Dur { dur, neg } if dur.is_zero() && !*neg));
    // The reduce label must survive the joins rewrite untouched.
    assert_eq!(
        plans[0].joins[0]
            .reduce
            .as_ref()
            .and_then(|r| r.label.clone())
            .as_deref(),
        Some("winner")
    );
}

// ---------------------------------------------------------------------------
// compile_wfl error plumbing — semantic errors surface as Compile errors
// ---------------------------------------------------------------------------

#[test]
fn compile_wfl_reports_semantic_errors() {
    let file = parse_wfl(
        r#"
rule bad {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.bogus)
    yield out (x = e.sip)
}
"#,
    )
    .expect("parse should succeed");
    let err = compile_wfl(&file, &[auth_events_window(), output_window()])
        .expect_err("unknown field must fail compilation");
    assert!(
        err.to_string().contains("semantic errors"),
        "unexpected error: {err}"
    );
}

// ---------------------------------------------------------------------------
// HOP 窗口 + top_ties conv —— 编译到 plan（compiler/mod.rs 新臂）
// ---------------------------------------------------------------------------

#[test]
fn compile_hop_window_spec() {
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    match<sip:hop(10s, 2s)> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &[auth_events_window(), output_window()],
    );
    assert!(
        matches!(
            plans[0].match_plan.window_spec,
            WindowSpec::Hop { size, slide }
                if size == Duration::from_secs(10) && slide == Duration::from_secs(2)
        ),
        "hop(10s, 2s) 编译为 WindowSpec::Hop: {:?}",
        plans[0].match_plan.window_spec
    );
    // conv 规则 + hop 窗口同时编译通过（conv_window 自动装配）。
    let plans = compile_with(
        r#"
rule r2 {
    events { e : auth_events }
    match<sip:hop(10s, 2s)> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-count) | top(5); }
}
"#,
        &[auth_events_window(), output_window()],
    );
    let conv = plans[0].conv_plan.as_ref().expect("conv plan");
    assert!(matches!(conv.chains[0].ops[0], ConvOpPlan::Sort(_)));
    assert!(matches!(conv.chains[0].ops[1], ConvOpPlan::Top(5)));
}

#[test]
fn compile_top_ties_copies_preceding_sort_keys() {
    // `sort(-count) | top_ties(10)`：TopTies 的 sort_keys 从前导 sort 复制。
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-count, e.sip) | top_ties(10); }
}
"#,
        &[auth_events_window(), output_window()],
    );
    let ops = &plans[0].conv_plan.as_ref().expect("conv plan").chains[0].ops;
    match (&ops[0], &ops[1]) {
        (ConvOpPlan::Sort(keys), ConvOpPlan::TopTies { n, sort_keys }) => {
            assert_eq!(*n, 10);
            assert_eq!(keys.len(), 2, "两键排序");
            assert_eq!(sort_keys.len(), 2, "并列判定键复制自前导 sort");
            assert!(sort_keys[0].descending, "降序标记保留");
            assert!(!sort_keys[1].descending);
            assert_eq!(
                sort_keys[0].expr,
                Expr::Field(FieldRef::Simple("count".into()))
            );
        }
        other => panic!("expected Sort + TopTies, got {other:?}"),
    }
}

#[test]
fn compile_top_ties_tracks_sort_per_chain() {
    // 每个 chain 独立跟踪前导 sort。chain1 的 top_ties 无前导 sort 时编译侧
    // 退化为空 sort_keys（防御分支）——checker 会拒绝，故经
    // `compile_wfl_after_semantic_checks` 直调编译（coverage_r4 同款模式）。
    let file = parse_wfl(
        r#"
rule r {
    events { e : auth_events }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv {
        sort(-count) | top_ties(3);
        where(count >= 2) | top_ties(2);
    }
}
"#,
    )
    .expect("parse should succeed");
    let plans = compile_wfl_after_semantic_checks(&file, &[auth_events_window(), output_window()])
        .expect("direct compile should succeed");
    let cp = plans[0].conv_plan.as_ref().expect("conv plan");
    let chain0 = &cp.chains[0].ops;
    assert!(matches!(&chain0[0], ConvOpPlan::Sort(_)));
    match &chain0[1] {
        ConvOpPlan::TopTies { n, sort_keys } => {
            assert_eq!(*n, 3);
            assert_eq!(sort_keys.len(), 1, "chain0 的 top_ties 复制了前导 sort 键");
        }
        other => panic!("expected TopTies in chain0, got {other:?}"),
    }
    // chain1 无前导 sort：last_sort_keys 为空 → sort_keys 空（退化 top）。
    match &cp.chains[1].ops[1] {
        ConvOpPlan::TopTies { n, sort_keys } => {
            assert_eq!(*n, 2);
            assert!(sort_keys.is_empty(), "无前导 sort → 空 sort_keys");
        }
        other => panic!("expected TopTies in chain1, got {other:?}"),
    }
}

#[test]
fn tracking_and_history_include_l3_series_hidden_in_let() {
    // issue #99：L3 序列函数（first/...）只出现在规则级 let 中、由 yield 间接
    // 引用时，仍必须触发绑定追踪与字段历史物化——否则运行时 let 求值拿到空
    // 序列，产出空值（entity 输出 alert_id 为空）。
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    let dk = join_by("|", e.user, first(e.event_time))
    match<:5m> { on event { e | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (y = dk)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = plans.iter().find(|p| p.name == "r").expect("rule");
    assert!(
        plan.match_plan.needs_field_history,
        "let 中的 L3 序列函数必须触发字段历史物化（issue #99）"
    );
    assert!(
        plan.match_plan.tracked_bind_aliases.contains("e"),
        "let 引用的别名必须进入绑定追踪"
    );
    assert!(
        plan.match_plan
            .tracked_bind_fields
            .get("e")
            .is_some_and(|fields| fields.contains("event_time")),
        "let 中 first(e.event_time) 的字段必须进入绑定追踪"
    );

    // 对照：无 L3、单绑定、无 join/close → 不物化历史（保持既有热路径开关）。
    let plans = compile_with(
        r#"
rule plain {
    events { e : auth_events }
    let who = e.user
    match<:5m> { on event { e | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (y = who)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = plans.iter().find(|p| p.name == "plain").expect("rule");
    assert!(
        !plan.match_plan.needs_field_history,
        "无 L3 的普通 let 规则不应开启字段历史（避免热路径退化）"
    );
}

#[test]
fn let_l3_nested_in_if_triggers_history() {
    // L3 嵌套在 object/if 内、仅出现在 let 中 → 仍需历史物化与字段追踪。
    let plans = compile_with(
        r#"
rule nested {
    events { e : auth_events }
    let dk = if len(e.user) > 0 then first(e.user) else "x"
    match<:5m> { on event { e | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (y = e.user)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = plans.iter().find(|p| p.name == "nested").expect("rule");
    assert!(
        plan.match_plan.needs_field_history,
        "嵌套在 if 分支中的 let L3 必须触发历史物化"
    );
    let fields = plan
        .match_plan
        .tracked_bind_fields
        .get("e")
        .expect("alias e tracked");
    assert!(fields.contains("user"));
}

#[test]
fn let_collect_list_tracks_field_and_history() {
    // collect_list 形态（同族 L3）同样必须入绑定追踪与历史开关。
    let plans = compile_with(
        r#"
rule cl {
    events { e : auth_events }
    let who = collect_list(e.user)
    match<:5m> { on event { e | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (y = e.user)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = plans.iter().find(|p| p.name == "cl").expect("rule");
    assert!(plan.match_plan.needs_field_history);
    assert!(
        plan.match_plan
            .tracked_bind_fields
            .get("e")
            .is_some_and(|f| f.contains("user"))
    );
}

#[test]
fn let_l3_bracket_field_form_tracks_field() {
    // bracket 形态字段（扁平点号字段名）在 let L3 中同样被追踪。
    let plans = compile_with(
        r#"
rule br {
    events { e : auth_events }
    let dk = first(e["event_time"])
    match<:5m> { on event { e | count >= 1; } } -> score(1.0)
    entity(ip, e.sip)
    yield out (y = e.user)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = plans.iter().find(|p| p.name == "br").expect("rule");
    assert!(plan.match_plan.needs_field_history);
    assert!(
        plan.match_plan
            .tracked_bind_fields
            .get("e")
            .is_some_and(|f| f.contains("event_time"))
    );
}

#[test]
fn close_rule_let_l3_triggers_history_via_close_path() {
    // close 步路径：let 中的 L3 决定 close 输出取值 → 必须保留每事件历史。
    let plans = compile_with(
        r#"
rule closelet {
    events { d : auth_events }
    let dk = first(d.event_time)
    match<sip:5m> {
        on event { ev_count: d | count >= 1; }
        on close { close_count: d | count >= 1; }
    } -> score(1.0)
    entity(ip, d.sip)
    yield out (y = dk)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = plans.iter().find(|p| p.name == "closelet").expect("rule");
    assert!(
        plan.match_plan.needs_field_history,
        "close 路径的 let L3 必须触发历史物化"
    );
}

#[test]
fn close_rule_let_non_key_field_triggers_history() {
    // close 路径：let 读取非 key 字段（event_time）→ 需要每事件历史，否则
    // close 时 let 求值读到空字段。
    let plans = compile_with(
        r#"
rule closenonkey {
    events { d : auth_events }
    let who = d.user
    match<sip:5m> {
        on event { ev_count: d | count >= 1; }
        on close { close_count: d | count >= 1; }
    } -> score(1.0)
    entity(ip, d.sip)
    yield out (y = who)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = plans
        .iter()
        .find(|p| p.name == "closenonkey")
        .expect("rule");
    assert!(
        plan.match_plan.needs_field_history,
        "close 路径 let 引用的非 key 字段必须触发历史物化"
    );
}

#[test]
fn close_rule_let_key_only_field_keeps_history_off() {
    // 对照：let 只引用 match key 字段（ctx 由 scope_key 提供）且无 L3 →
    // 不开启每事件历史（避免 close 热路径退化）。
    let plans = compile_with(
        r#"
rule closekeyonly {
    events { d : auth_events }
    let who = d.sip
    match<sip:5m> {
        on event { ev_count: d | count >= 1; }
        on close { close_count: d | count >= 1; }
    } -> score(1.0)
    entity(ip, d.sip)
    yield out (y = d.sip)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = plans
        .iter()
        .find(|p| p.name == "closekeyonly")
        .expect("rule");
    assert!(
        !plan.match_plan.needs_field_history,
        "仅 key 字段的 let 不应开启历史（热路径开关不退化）"
    );
}

#[test]
fn pipeline_stage_let_l3_triggers_history() {
    // pipeline 规则的绑定追踪调用点同样纳入 let（stage0 真实 binds）。
    let plans = compile_with(
        r#"
rule pipelet {
    events { d : fw_events }
    let dk = first(d.dport)
    match<sip,dport:5m> {
        on event { ev_count: d | count >= 1; }
        on close { close_count: d | count >= 3; }
    }
    |> match<sip:10m> {
        on event { ev_count: _in | count >= 1; }
        on close { close_count: _in | count >= 10; }
    } -> score(80.0)
    entity(ip, _in.sip)
    yield out (x = _in.sip)
}
"#,
        &[fw_events_window(), output_window()],
    );
    let stage0 = plans.first().expect("stage0");
    assert!(
        stage0.match_plan.needs_field_history,
        "pipeline 规则 let 中的 L3 必须触发历史物化"
    );
}
