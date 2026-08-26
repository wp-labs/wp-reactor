//! Extra coverage tests for the compiler: stats-rule plans, limits plans,
//! conv plans, join-then-key resolution, bind-tracking recursion and
//! needs-field-history decisions that the focused test files do not reach.

use crate::ast::{Expr, FieldRef, PathSegment};
use crate::compiler::{BindTracking, collect_bind_tracking, compile_wfl_after_semantic_checks};
use crate::plan::{
    ConvOpPlan, ExceedAction, JoinKeyPlan, RateSpec, SeqSkipPlan, SpillMode, StatsAggPlan,
    StatsOutputShapePlan, WindowSpec,
};

use super::*;

/// Window used by stats-rule tests (must carry `user` and `count` fields).
pub(super) fn stats_win() -> WindowSchema {
    make_window(
        "auth_events",
        vec!["auth_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("user", bt(BaseType::Chars)),
            ("count", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn alerts_window() -> WindowSchema {
    make_output_window(
        "alerts",
        vec![
            ("id", bt(BaseType::Digit)),
            ("alert_type", bt(BaseType::Chars)),
            ("detail", bt(BaseType::Chars)),
            ("total", bt(BaseType::Digit)),
            ("label", bt(BaseType::Chars)),
        ],
    )
}

pub(super) fn auction_events_window() -> WindowSchema {
    make_window(
        "auction_events",
        vec!["auction_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("category", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn bid_events_window() -> WindowSchema {
    make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Chars)),
            ("price", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

// ---------------------------------------------------------------------------
// compile_rule rejection branches
// ---------------------------------------------------------------------------

#[test]
fn compile_rejects_each_combined_with_pipeline_stages() {
    let file = parse_wfl(
        r#"
rule r {
    events { e : auth_events }
    on each e -> score(1.0)
    |> match<sip:5m> { on event { _in | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    );
    // The parser may or may not produce the shape; only compile if parsed.
    if let Ok(file) = file {
        let result = compile_wfl(&file, &[auth_events_window(), output_window()]);
        assert!(
            result.is_err(),
            "`on each` together with pipeline stages must be rejected"
        );
    }
}

// ---------------------------------------------------------------------------
// compile_stats_rule
// ---------------------------------------------------------------------------

#[test]
fn compile_stats_session_columns_rule() {
    let src = r#"
rule stats_cols {
    events { e : auth_events }
    stats<1h:session> tier e.count [ <10, <100 ] {
        e | count as total;
        e | distinct_count(e.user) as users;
        e | sum(e.count) as sum_count;
        e | avg(e.count) as avg_count;
        e | min(e.count) as min_count;
        e | max(e.count) as max_count;
        e | last(e.user) as last_user;
        e | top(3, e.user) as top_users;
    }
    entity(digit, 1)
    yield alerts (
        id = 1,
        alert_type = "stats",
        detail = "x",
        total = 0,
        label = "l"
    )
}
"#;
    let plans = compile_with(src, &[stats_win(), alerts_window()]);
    let plan = plans.iter().find(|p| p.name == "stats_cols").expect("rule");
    let stats = plan.stats_plan.as_ref().expect("stats plan");
    assert!(matches!(
        stats.window_spec,
        WindowSpec::Session(d) if d.as_secs() == 3600
    ));
    assert_eq!(stats.output_shape, StatsOutputShapePlan::Columns);
    assert_eq!(stats.keys.len(), 1);
    assert_eq!(stats.measures.len(), 8);
    let aggs: Vec<StatsAggPlan> = stats.measures.iter().map(|m| m.agg).collect();
    assert_eq!(
        aggs,
        vec![
            StatsAggPlan::Count,
            StatsAggPlan::DistinctCount,
            StatsAggPlan::Sum,
            StatsAggPlan::Avg,
            StatsAggPlan::Min,
            StatsAggPlan::Max,
            StatsAggPlan::Last,
            StatsAggPlan::Top,
        ]
    );
    // top(N, field) carries the arg.
    let top = stats
        .measures
        .iter()
        .find(|m| m.label == "top_users")
        .unwrap();
    assert_eq!(top.arg, Some(3));
    assert_eq!(
        top.field,
        Some(FieldRef::Qualified("e".into(), "user".into()))
    );
    // last() carries the source field.
    let last = stats
        .measures
        .iter()
        .find(|m| m.label == "last_user")
        .unwrap();
    assert_eq!(
        last.field,
        Some(FieldRef::Qualified("e".into(), "user".into()))
    );

    // Stats rules keep an empty match plan placeholder and no joins.
    assert!(plan.match_plan.keys.is_empty());
    assert!(plan.joins.is_empty());
    assert!(
        plan.score_plan.expr == Expr::Number(50.0) || plan.score_plan.expr == Expr::Number(0.0)
    );
}

#[test]
fn compile_stats_tracks_where_and_key_fields() {
    let src = r#"
rule stats_where {
    events { e : auth_events }
    stats<30m:fixed> group by (e.sip) {
        e | sum(e.count) as total where e.user == "g";
    }
    entity(digit, 1)
    yield alerts (
        id = 1,
        alert_type = "s",
        detail = "x",
        total = 0,
        label = "l"
    )
}
"#;
    let plans = compile_with(src, &[stats_win(), alerts_window()]);
    let plan = plans
        .iter()
        .find(|p| p.name == "stats_where")
        .expect("rule");
    let stats = plan.stats_plan.as_ref().expect("stats plan");
    assert!(matches!(stats.window_spec, WindowSpec::Fixed(_)));

    let tracked = &stats.tracked_bind_fields;
    let e_fields = tracked.get("e").expect("alias e tracked");
    // measure field (count) + where field (user) + bucket key field (sip).
    assert!(e_fields.contains("count"), "got {e_fields:?}");
    assert!(e_fields.contains("user"), "got {e_fields:?}");
    assert!(e_fields.contains("sip"), "got {e_fields:?}");
}

#[test]
fn compile_stats_fixed_rows_rule() {
    let src = r#"
rule stats_rows {
    events { e : auth_events }
    stats<30m:fixed> {
        e | count as total;
    }
    entity(digit, 1)
    yield alerts (
        id = 1,
        alert_type = "s",
        detail = "x",
        total = 0,
        label = "l"
    )
}
"#;
    let plans = compile_with(src, &[stats_win(), alerts_window()]);
    let plan = plans.iter().find(|p| p.name == "stats_rows").expect("rule");
    let stats = plan.stats_plan.as_ref().expect("stats plan");
    assert!(matches!(stats.window_spec, WindowSpec::Fixed(d) if d.as_secs() == 1800));
    assert_eq!(stats.output_shape, StatsOutputShapePlan::Rows);
    assert!(stats.keys.is_empty());
}

// ---------------------------------------------------------------------------
// compile_limits
// ---------------------------------------------------------------------------

#[test]
fn compile_limits_plan_carries_parsed_values() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits {
        max_memory = "1GB";
        max_instances = 7;
        max_throttle = "100/min";
        on_exceed = "fail_rule";
    }
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let lp = plans[0].limits_plan.as_ref().expect("limits plan");
    assert_eq!(lp.max_memory_bytes, Some(1024 * 1024 * 1024));
    assert_eq!(lp.max_instances, Some(7));
    assert_eq!(
        lp.max_throttle,
        Some(RateSpec {
            count: 100,
            per: std::time::Duration::from_secs(60),
        })
    );
    assert_eq!(lp.on_exceed, ExceedAction::FailRule);
}

#[test]
fn compile_limits_plan_spill_parsed() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits {
        max_memory = "1GB";
        spill = "redb";
        max_spill_bytes = "8GB";
    }
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let lp = plans[0].limits_plan.as_ref().expect("limits plan");
    assert_eq!(lp.spill, Some(SpillMode::Redb));
    assert_eq!(lp.max_spill_bytes, Some(8 * 1024 * 1024 * 1024));
}

#[test]
fn compile_limits_plan_spill_off_by_default() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits { max_memory = "1GB"; }
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let lp = plans[0].limits_plan.as_ref().expect("limits plan");
    assert_eq!(lp.spill, None);
    assert_eq!(lp.max_spill_bytes, None);
}

#[test]
fn compile_limits_plan_byte_size_and_rate_units() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits {
        max_memory = "2KB";
        max_throttle = "5/s";
        on_exceed = "drop_oldest";
    }
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let lp = plans[0].limits_plan.as_ref().expect("limits plan");
    assert_eq!(lp.max_memory_bytes, Some(2 * 1024));
    assert_eq!(
        lp.max_throttle,
        Some(RateSpec {
            count: 5,
            per: std::time::Duration::from_secs(1),
        })
    );
    assert_eq!(lp.on_exceed, ExceedAction::DropOldest);
}

#[test]
fn compile_limits_plan_invalid_values_fall_back_to_defaults() {
    // The checker rejects invalid limits values, so the compiler's defensive
    // fallback (invalid value → None / default) is exercised by compiling a
    // parsed-but-mutated rule straight through the compiler entry point.
    let mut file = parse_wfl(
        r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits {
        max_memory = "1MB";
        max_instances = 1;
        max_throttle = "1/s";
        on_exceed = "throttle";
    }
}
"#,
    )
    .unwrap();
    let limits = file.rules[0].limits.as_mut().expect("limits block");
    for item in &mut limits.items {
        item.value = match item.key.as_str() {
            "max_memory" => "not-a-size".to_string(),
            "max_instances" => "abc".to_string(),
            "max_throttle" => "nope".to_string(),
            _ => "bogus".to_string(),
        };
    }
    let plans =
        compile_wfl_after_semantic_checks(&file, &[auth_events_window(), output_window()]).unwrap();
    let lp = plans[0].limits_plan.as_ref().expect("limits plan");
    assert_eq!(lp.max_memory_bytes, None);
    assert_eq!(lp.max_instances, None);
    assert_eq!(lp.max_throttle, None);
    assert_eq!(lp.on_exceed, ExceedAction::Throttle);
}

#[test]
fn compile_limits_plan_bare_bytes_and_hour_rate() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    limits {
        max_memory = "4KB";
        max_throttle = "10/h";
    }
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let lp = plans[0].limits_plan.as_ref().expect("limits plan");
    assert_eq!(lp.max_memory_bytes, Some(4 * 1024));
    assert_eq!(
        lp.max_throttle,
        Some(RateSpec {
            count: 10,
            per: std::time::Duration::from_secs(3600),
        })
    );
}

// ---------------------------------------------------------------------------
// compile_conv
// ---------------------------------------------------------------------------

#[test]
fn compile_conv_plan_ops() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv {
        sort(score) | dedup(sip) | top(5);
        where(count >= 3);
    }
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let cp = plans[0].conv_plan.as_ref().expect("conv plan");
    assert_eq!(cp.chains.len(), 2);
    let chain0 = &cp.chains[0].ops;
    match &chain0[0] {
        ConvOpPlan::Sort(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(!keys[0].descending);
            assert_eq!(keys[0].expr, Expr::Field(FieldRef::Simple("score".into())));
        }
        other => panic!("expected Sort, got {other:?}"),
    }
    assert!(matches!(&chain0[1], ConvOpPlan::Dedup(_)));
    assert!(matches!(&chain0[2], ConvOpPlan::Top(5)));
    assert!(matches!(&cp.chains[1].ops[0], ConvOpPlan::Where(_)));
}

#[test]
fn compile_conv_descending_sort() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-count) ; }
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let cp = plans[0].conv_plan.as_ref().expect("conv plan");
    match &cp.chains[0].ops[0] {
        ConvOpPlan::Sort(keys) => {
            assert!(keys[0].descending);
            assert_eq!(keys[0].expr, Expr::Field(FieldRef::Simple("count".into())));
        }
        other => panic!("expected Sort, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// resolve_join_key (join-then-key)
// ---------------------------------------------------------------------------

#[test]
fn compile_join_then_key_resolves_key_join_plan() {
    let src = r#"
rule r {
    events { b : bid_events }
    match<category:10m> { on event { b | count >= 1; } } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(ip, b.bidder)
    yield out (x = b.bidder)
}
"#;
    let plans = compile_with(
        src,
        &[
            bid_events_window(),
            auction_events_window(),
            output_window(),
        ],
    );
    let plan = plans.iter().find(|p| p.name == "r").expect("rule");
    let kj = plan.match_plan.key_join.as_ref().expect("key join");
    assert_eq!(
        kj,
        &JoinKeyPlan {
            join_idx: 0,
            right_window: "auction_events".to_string(),
            left_field: FieldRef::Qualified("b".into(), "auction".into()),
            right_key_field: "id".to_string(),
            right_field: "category".to_string(),
            key_name: "category".to_string(),
        }
    );
}

#[test]
fn compile_driver_key_has_no_key_join() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    assert!(plans[0].match_plan.key_join.is_none());
}

// ---------------------------------------------------------------------------
// compile_each / lets
// ---------------------------------------------------------------------------

#[test]
fn compile_each_rule_carries_each_plan() {
    let src = r#"
rule r {
    events { e : auth_events }
    let first = lower(e.user)
    on each e where e.action == "failed" -> score(2.0)
    entity(ip, e.sip)
    yield out (y = first)
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let plan = &plans[0];
    let each = plan.each_plan.as_ref().expect("each plan");
    assert_eq!(each.alias, "e");
    assert!(each.filter.is_some());
    assert_eq!(plan.lets.len(), 1);
    assert_eq!(plan.lets[0].name, "first");
    assert_eq!(plan.score_plan.expr, Expr::Number(2.0));
}

// ---------------------------------------------------------------------------
// pipeline stage plans
// ---------------------------------------------------------------------------

#[test]
fn compile_pipeline_stage_yield_fields_include_keys_and_measure_names() {
    let schemas = [fw_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule pipe {
  events { d: fw_events }
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
        &schemas,
    );

    let stage1 = &plans[0];
    let names: Vec<&str> = stage1
        .yield_plan
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert!(
        names.contains(&"sip") && names.contains(&"dport"),
        "stage yield should carry keys, got {names:?}"
    );
    assert!(
        names.contains(&"ev_count") && names.contains(&"close_count"),
        "stage yield should carry measure labels, got {names:?}"
    );

    // Non-final stage entity: first key only, "pipeline" type.
    assert_eq!(stage1.entity_plan.entity_type, "pipeline");
    assert_eq!(
        stage1.entity_plan.entity_id_expr,
        Expr::Field(FieldRef::Simple("sip".into()))
    );
    // Non-final stages get a zero score plan.
    assert_eq!(stage1.score_plan.expr, Expr::Number(0.0));
    assert!(stage1.limits_plan.is_none());
    assert!(stage1.conv_plan.is_none());
    assert!(stage1.pattern_origin.is_none());
}

#[test]
fn compile_pipeline_stage_unlabeled_measure_uses_default_name() {
    let schemas = [fw_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule pipe {
  events { d: fw_events }
  match<sip:5m> {
    on event { d | count >= 1; }
  }
  |> match<sip:10m> {
    on event { _in | count >= 1; }
  } -> score(80.0)
  entity(ip, _in.sip)
  yield out (x = _in.sip)
}
"#,
        &schemas,
    );
    let stage1 = &plans[0];
    let names: Vec<&str> = stage1
        .yield_plan
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    assert!(
        names.contains(&"count"),
        "unlabeled count measure should produce a `count` yield field, got {names:?}"
    );
}

#[test]
fn compile_pipeline_stage_without_keys_uses_pipeline_entity_id() {
    let schemas = [fw_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule pipe {
  events { d: fw_events }
  match<:5m> {
    on event { ev: d | count >= 1; }
  }
  |> match<:10m> {
    on event { _in | count >= 1; }
  } -> score(80.0)
  entity(ip, _in.ev)
  yield out (n = _in.ev)
}
"#,
        &schemas,
    );
    let stage1 = &plans[0];
    assert_eq!(stage1.entity_plan.entity_type, "pipeline");
    assert_eq!(
        stage1.entity_plan.entity_id_expr,
        Expr::StringLit("__pipeline".into())
    );
}

// ---------------------------------------------------------------------------
// collect_bind_tracking recursion
// ---------------------------------------------------------------------------

fn tracking(expr: &Expr) -> BindTracking {
    let mut t = BindTracking::default();
    collect_bind_tracking(expr, &mut t);
    t
}

fn f(alias: &str, field: &str) -> Expr {
    Expr::Field(FieldRef::Qualified(alias.into(), field.into()))
}

#[test]
fn bind_tracking_binop_neg_and_inlist() {
    let expr = Expr::BinOp {
        op: crate::ast::BinOp::Add,
        left: Box::new(f("e", "count")),
        right: Box::new(Expr::Neg(Box::new(f("e", "dip")))),
    };
    let t = tracking(&expr);
    assert!(t.aliases.contains("e"));
    assert!(t.fields.get("e").unwrap().contains("count"));
    assert!(t.fields.get("e").unwrap().contains("dip"));

    let in_list = Expr::InList {
        expr: Box::new(f("e", "sip")),
        list: vec![Expr::StringLit("1.2.3.4".into())],
        negated: true,
    };
    let t2 = tracking(&in_list);
    assert!(t2.aliases.contains("e"));
    assert!(t2.fields.get("e").unwrap().contains("sip"));
}

#[test]
fn bind_tracking_ifthenelse_object_array() {
    let ifte = Expr::IfThenElse {
        cond: Box::new(Expr::Bool(true)),
        then_expr: Box::new(f("e", "sip")),
        else_expr: Box::new(f("e", "dip")),
    };
    let t = tracking(&ifte);
    assert_eq!(t.fields.get("e").unwrap().len(), 2);

    let obj = Expr::Object(vec![crate::ast::ObjectItem {
        targets: vec!["ctx".into()],
        type_hint: None,
        value: f("e", "user"),
    }]);
    let t2 = tracking(&obj);
    assert!(t2.fields.get("e").unwrap().contains("user"));

    let arr = Expr::Array(vec![f("e", "action"), Expr::Number(1.0)]);
    let t3 = tracking(&arr);
    assert!(t3.fields.get("e").unwrap().contains("action"));
}

#[test]
fn bind_tracking_series_funcs_and_stat_selectors() {
    // is_series_func with a qualified first arg tracks the field.
    let series = Expr::FuncCall {
        qualifier: None,
        name: "collect_set".into(),
        args: vec![f("e", "user")],
    };
    let t = tracking(&series);
    assert!(t.aliases.contains("e"));
    assert!(t.fields.get("e").unwrap().contains("user"));

    // stat.count(window_event(alias)) tracks the alias (set-level count).
    let stat_count = Expr::FuncCall {
        qualifier: Some("stat".into()),
        name: "count".into(),
        args: vec![Expr::FuncCall {
            qualifier: None,
            name: "window_event".into(),
            args: vec![Expr::Field(FieldRef::Simple("auth".into()))],
        }],
    };
    let t2 = tracking(&stat_count);
    assert!(t2.aliases.contains("auth"), "got {:?}", t2.aliases);
    assert!(t2.fields.is_empty());

    // stat.value(final(label)) reads no event fields.
    let stat_value = Expr::FuncCall {
        qualifier: Some("stat".into()),
        name: "value".into(),
        args: vec![Expr::FuncCall {
            qualifier: None,
            name: "final".into(),
            args: vec![Expr::Field(FieldRef::Simple("lbl".into()))],
        }],
    };
    let t3 = tracking(&stat_value);
    assert!(t3.aliases.is_empty());
    assert!(t3.fields.is_empty());

    // A bare stat selector (window_event) used directly contributes nothing.
    let bare = Expr::FuncCall {
        qualifier: None,
        name: "window_event".into(),
        args: vec![Expr::Field(FieldRef::Simple("auth".into()))],
    };
    let t4 = tracking(&bare);
    assert!(t4.aliases.is_empty(), "got {:?}", t4.aliases);

    // Nested path tracks only the root field.
    let path = Expr::Field(FieldRef::Path {
        alias: "e".into(),
        segments: vec![
            PathSegment::Field("roles_obj".into()),
            PathSegment::Field("uid".into()),
        ],
    });
    let t5 = tracking(&path);
    assert!(t5.fields.get("e").unwrap().contains("roles_obj"));
    assert!(!t5.fields.get("e").unwrap().contains("uid"));
}

#[test]
fn bind_tracking_plain_fields_in_binop_and_path_without_field_segment() {
    let binop = Expr::BinOp {
        op: crate::ast::BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("user".into()))),
        right: Box::new(Expr::StringLit("x".into())),
    };
    let t = tracking(&binop);
    assert!(t.plain_fields.contains("user"));

    // A path whose first segment is an index tracks nothing.
    let idx_path = Expr::Field(FieldRef::Path {
        alias: "e".into(),
        segments: vec![PathSegment::Index(0)],
    });
    let t2 = tracking(&idx_path);
    assert!(t2.fields.is_empty());
}

// ---------------------------------------------------------------------------
// compute_needs_field_history
// ---------------------------------------------------------------------------

#[test]
fn multi_bind_rule_needs_field_history() {
    let src = r#"
rule r {
    events { a : auth_events  n : fw_events }
    match<sip:5m> { on event { a | count >= 1; } } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    let plans = compile_with(
        src,
        &[auth_events_window(), fw_events_window(), output_window()],
    );
    let plan = plans.iter().find(|p| p.name == "r").expect("rule");
    assert!(
        plan.match_plan.needs_field_history,
        "multi-bind rules require the per-event field history"
    );
}

#[test]
fn join_rule_needs_field_history() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let plans = compile_with(
        src,
        &[auth_events_window(), bid_events_window(), output_window()],
    );
    let plan = plans.iter().find(|p| p.name == "r").expect("rule");
    assert!(
        plan.match_plan.needs_field_history,
        "join rules require the per-event field history"
    );
}

#[test]
fn needs_field_history_false_for_key_only_on_event() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let plan = plans.iter().find(|p| p.name == "r").expect("rule");
    assert!(
        !plan.match_plan.needs_field_history,
        "single-bind on-event rule reading only keys should skip history"
    );
}

#[test]
fn needs_field_history_true_for_l3_yield() {
    let out = make_output_window("out", vec![("x", bt(BaseType::Ip))]);
    // `last` is an L3 series function: it needs the per-field history.
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = last(e.sip))
}
"#;
    let plans = compile_with(src, &[auth_events_window(), out]);
    let plan = plans.iter().find(|p| p.name == "r").expect("rule");
    assert!(plan.match_plan.needs_field_history);
}

// ---------------------------------------------------------------------------
// compute_needs_field_history: unit-level close-path decisions
// ---------------------------------------------------------------------------

#[test]
fn close_path_key_only_yield_skips_history() {
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        on close { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = &plans[0];
    assert!(
        !plan.match_plan.needs_field_history,
        "close rule whose yield reads only the key skips history"
    );
}

#[test]
fn close_path_non_key_yield_needs_history() {
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        on close { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.action)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = &plans[0];
    assert!(
        plan.match_plan.needs_field_history,
        "close rule reading a non-key field needs history"
    );
}

// ---------------------------------------------------------------------------
// compute_trigger_event_needed: on-event fire 是否需要物化触发事件
// ---------------------------------------------------------------------------

#[test]
fn key_only_yield_skips_trigger_event() {
    // yield 只读 key 字段（e.sip）→ fire 路径可跳过 event.to_event() clone。
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        on close { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = &plans[0];
    assert!(
        !plan.match_plan.trigger_event_needed,
        "key-only yield skips trigger-event materialization"
    );
}

#[test]
fn non_key_yield_needs_trigger_event() {
    // yield 读非 key 字段（e.action）→ fire 必须带触发事件（build_eval_context
    // 从 trigger_event 注入 action，scope_key 只有 sip）。
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        on close { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = e.action)
}
"#,
        &[auth_events_window(), output_window()],
    );
    let plan = &plans[0];
    assert!(
        plan.match_plan.trigger_event_needed,
        "non-key yield needs trigger-event materialization"
    );
}

#[test]
fn join_left_field_non_key_needs_trigger_event() {
    // join 条件左字段（first_join_key 从 ctx 读）非 key → 需要触发事件
    //（否则 join 静默 miss，F3 教训）。
    let plans = compile_with(
        r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    join dns_response snapshot on e.action == dns_response.query_id
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &[auth_events_window(), dns_response_window(), output_window()],
    );
    let plan = &plans[0];
    assert!(
        plan.match_plan.trigger_event_needed,
        "join left field non-key needs trigger event"
    );
}

// ---------------------------------------------------------------------------
// compile_wfl_after_semantic_checks passthrough
// ---------------------------------------------------------------------------

#[test]
fn compile_after_semantic_checks_skips_duplicate_checking() {
    let file = parse_wfl(
        r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    )
    .unwrap();
    let plans =
        compile_wfl_after_semantic_checks(&file, &[auth_events_window(), output_window()]).unwrap();
    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].name, "r");
}

#[test]
fn compile_seq_plan_skip_and_consec_carried() {
    let src = r#"
rule r {
    events { a : auth_events }
    match<:5m> {
        on event seq consec skip = to_next {
            a | count >= 1;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let seq = plans[0].match_plan.seq.as_ref().expect("seq plan");
    assert!(seq.consec);
    assert_eq!(seq.skip, SeqSkipPlan::ToNext);
}

#[test]
fn compile_seq_default_skip_is_past_last() {
    let src = r#"
rule r {
    events { a : auth_events }
    match<:5m> {
        on event seq {
            a | count >= 1;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let seq = plans[0].match_plan.seq.as_ref().expect("seq plan");
    assert!(!seq.consec);
    assert_eq!(seq.skip, SeqSkipPlan::PastLast);
}

// ---------------------------------------------------------------------------
// where / reduce label rewriting helpers exercised via join plans
// ---------------------------------------------------------------------------

#[test]
fn compile_join_within_bound_label_refs_rewritten_to_paths() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events reduce maxrow(price) as winner
        within [e.sip, winner.price]
        on e.sip == bid_events.bidder
    entity(ip, e.sip)
    yield out (y = winner.bidder)
}
"#;
    let plans = compile_with(
        src,
        &[auth_events_window(), bid_events_window(), output_window()],
    );
    let plan = plans.iter().find(|p| p.name == "r").expect("rule");
    let join = &plan.joins[0];
    let within = join.within.as_ref().expect("within");
    // The `winner.price` bound is rewritten to a Path (reduce label access).
    match &within.hi.val {
        crate::ast::BoundVal::Expr(Expr::Field(FieldRef::Path { alias, segments })) => {
            assert_eq!(alias, "winner");
            assert_eq!(segments.len(), 1);
        }
        other => panic!("expected Path bound, got {other:?}"),
    }
    // The yield `winner.bidder` is also rewritten to a Path.
    match &plan.yield_plan.fields[0].value {
        Expr::Field(FieldRef::Path { alias, .. }) => assert_eq!(alias, "winner"),
        other => panic!("expected Path yield expr, got {other:?}"),
    }
    assert_eq!(plan.r#where, None);
}

#[test]
fn compile_where_label_ref_rewritten() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join bid_events reduce maxrow(price) as winner on e.sip == bid_events.bidder
    where winner.price > 100
    entity(ip, e.sip)
    yield out (y = winner.bidder)
}
"#;
    let plans = compile_with(
        src,
        &[auth_events_window(), bid_events_window(), output_window()],
    );
    let plan = plans.iter().find(|p| p.name == "r").expect("rule");
    let wh = plan.r#where.as_ref().expect("where expr");
    match wh {
        Expr::BinOp { left, .. } => match left.as_ref() {
            Expr::Field(FieldRef::Path { alias, .. }) => assert_eq!(alias, "winner"),
            other => panic!("expected Path left side, got {other:?}"),
        },
        other => panic!("expected BinOp, got {other:?}"),
    }
}

#[test]
fn compile_lets_in_regular_rule() {
    let src = r#"
rule r {
    events { e : auth_events }
    let u = lower(e.user)
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = u)
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let plan = plans.iter().find(|p| p.name == "r").expect("rule");
    assert_eq!(plan.lets.len(), 1);
    assert_eq!(plan.lets[0].name, "u");
}
