use std::time::Duration;

use crate::ast::{Expr, FieldRef, SystemVar};
use crate::compile_wfl;
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
use crate::wfl_parser::parse_wfl;

use super::explain_rules;
use super::format::{format_duration, format_expr};

fn bt(b: BaseType) -> FieldType {
    FieldType::Base(b)
}

fn auth_events_window() -> WindowSchema {
    WindowSchema {
        name: "auth_events".to_string(),
        streams: vec!["auth_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: bt(BaseType::Ip),
            },
            FieldDef {
                name: "action".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: bt(BaseType::Time),
            },
        ],
    }
}

fn security_alerts_window() -> WindowSchema {
    WindowSchema {
        name: "security_alerts".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: bt(BaseType::Ip),
            },
            FieldDef {
                name: "fail_count".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "message".to_string(),
                field_type: bt(BaseType::Chars),
            },
        ],
    }
}

#[test]
fn explain_brute_force_rule() {
    let input = r#"
rule brute_force_then_scan {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
        }
        on close {
            fail | count >= 1;
        }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} brute force detected", fail.sip)
    )
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let explanations = explain_rules(&plans, schemas);

    assert_eq!(explanations.len(), 1);
    let expl = &explanations[0];
    assert_eq!(expl.name, "brute_force_then_scan");
    assert_eq!(expl.bindings.len(), 1);
    assert_eq!(expl.bindings[0].alias, "fail");
    assert_eq!(expl.bindings[0].window, "auth_events");
    assert!(expl.bindings[0].filter.is_some());

    assert_eq!(expl.match_expl.event_steps.len(), 1);
    assert_eq!(expl.match_expl.close_steps.len(), 1);
    assert_eq!(expl.score, "70.0");
    assert_eq!(expl.entity_type, "ip");
    assert_eq!(expl.entity_id, "fail.sip");
    assert_eq!(expl.yield_target, "security_alerts");
    assert_eq!(expl.yield_fields.len(), 3);

    // Verify Display output
    let output = format!("{}", expl);
    assert!(output.contains("Rule: brute_force_then_scan"));
    assert!(output.contains("fail -> auth_events"));
    assert!(output.contains("action == \"failed\""));
    assert!(output.contains("Score: 70.0"));
    assert!(output.contains("Entity: ip = fail.sip"));
    assert!(output.contains("sip"));
    assert!(output.contains("Field Lineage:"));
}

#[test]
fn format_expr_variants() {
    assert_eq!(format_expr(&Expr::Number(42.0)), "42.0");
    assert_eq!(format_expr(&Expr::Number(3.24)), "3.24");
    assert_eq!(format_expr(&Expr::StringLit("hello".into())), "\"hello\"");
    assert_eq!(format_expr(&Expr::Bool(true)), "true");
    assert_eq!(format_expr(&Expr::SystemVar(SystemVar::Score)), "@score");
    assert_eq!(
        format_expr(&Expr::Field(FieldRef::Qualified("a".into(), "b".into()))),
        "a.b"
    );
    assert_eq!(
        format_expr(&Expr::FuncCall {
            qualifier: None,
            name: "count".into(),
            args: vec![Expr::Field(FieldRef::Simple("fail".into()))]
        }),
        "count(fail)"
    );
}

#[test]
fn format_path_segments_renders_without_stray_dots() {
    use crate::ast::PathSegment;

    // `.member` is dot-joined; `[index]` is not — no `.[0]`-style artifacts.
    let segments = vec![
        PathSegment::Field("roles_obj".into()),
        PathSegment::Field("related".into()),
        PathSegment::Index(0),
        PathSegment::Field("process".into()),
        PathSegment::Field("name".into()),
    ];
    assert_eq!(
        super::format::format_path_segments(&segments),
        "roles_obj.related[0].process.name"
    );
    // Single member renders bare; empty path renders empty.
    assert_eq!(
        super::format::format_path_segments(&[PathSegment::Field("uid".into())]),
        "uid"
    );
    assert_eq!(super::format::format_path_segments(&[]), "");

    // format_field_ref prefixes the alias.
    assert_eq!(
        super::format::format_field_ref(&FieldRef::Path {
            alias: "e".into(),
            segments,
        }),
        "e.roles_obj.related[0].process.name"
    );
}

#[test]
fn explain_shows_pattern_origin() {
    let input = r#"
pattern burst(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(50.0)
}

rule brute_force {
    events {
        fail : auth_events && action == "failed"
    }
    burst(fail, sip, 5m, 5)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} brute force detected", fail.sip)
    )
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let explanations = explain_rules(&plans, schemas);

    assert_eq!(explanations.len(), 1);
    let expl = &explanations[0];

    // Pattern origin should be present
    let (pat_name, pat_args) = expl
        .pattern_origin
        .as_ref()
        .expect("pattern_origin should be Some");
    assert_eq!(pat_name, "burst");
    assert_eq!(pat_args, &["fail", "sip", "5m", "5"]);

    // Display output should include Pattern line
    let output = format!("{}", expl);
    assert!(
        output.contains("Pattern: burst(fail, sip, 5m, 5)"),
        "explain output should show pattern origin: {}",
        output
    );
    assert!(output.contains("Rule: brute_force"));
    assert!(output.contains("Score: 50.0"));
}

#[test]
fn explain_no_pattern_origin_for_standard_rule() {
    let input = r#"
rule brute_force_then_scan {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
        }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} brute force detected", fail.sip)
    )
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let explanations = explain_rules(&plans, schemas);

    assert!(
        explanations[0].pattern_origin.is_none(),
        "standard rule should not have pattern_origin"
    );
    let output = format!("{}", explanations[0]);
    assert!(
        !output.contains("Pattern:"),
        "standard rule should not show Pattern line"
    );
}

#[test]
fn format_duration_preserves_milliseconds() {
    assert_eq!(format_duration(&Duration::from_millis(100)), "100ms");
    assert_eq!(format_duration(&Duration::from_millis(1500)), "1500ms");
}

#[test]
fn explain_shows_on_event_accu() {
    let input = r#"
rule accu_rule {
    events { s : auth_events }
    match<sip:5m> {
        on event<accu> { s | count >= 2; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield security_alerts (sip = s.sip, fail_count = 2)
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let explanations = explain_rules(&plans, schemas);
    let expl = &explanations[0];

    assert!(expl.match_expl.accu, "match explanation must carry accu");
    let output = format!("{}", expl);
    assert!(
        output.contains("on event<accu>"),
        "explain output must render <accu>, got:\n{output}"
    );
}

// ---------------------------------------------------------------------------
// Extra coverage: format_expr variants, explain sections, lineage, limits
// ---------------------------------------------------------------------------

use crate::ast::{BinOp, CmpOp, Measure, Transform};

#[test]
fn format_expr_extra_variants() {
    // WfuMeta
    assert_eq!(
        format_expr(&Expr::WfuMeta(crate::wfu_meta::WfuMetaField::EmitTime)),
        "@__wfu_emit_time"
    );
    // PresetParam
    assert_eq!(format_expr(&Expr::PresetParam("x".into())), "$x");
    // Neg
    assert_eq!(format_expr(&Expr::Neg(Box::new(Expr::Number(3.0)))), "-3.0");
    // BinOp arithmetic
    assert_eq!(
        format_expr(&Expr::BinOp {
            op: BinOp::Mul,
            left: Box::new(Expr::Number(2.0)),
            right: Box::new(Expr::Number(3.0)),
        }),
        "2.0 * 3.0"
    );
    // FuncCall with qualifier
    assert_eq!(
        format_expr(&Expr::FuncCall {
            qualifier: Some("stat".into()),
            name: "count".into(),
            args: vec![Expr::Field(FieldRef::Simple("fail".into()))],
        }),
        "stat.count(fail)"
    );
    // InList (not in)
    assert_eq!(
        format_expr(&Expr::InList {
            expr: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            list: vec![
                Expr::StringLit("1.2.3.4".into()),
                Expr::StringLit("5.6.7.8".into()),
            ],
            negated: true,
        }),
        "e.sip not in (\"1.2.3.4\", \"5.6.7.8\")"
    );
    // InList (in)
    assert_eq!(
        format_expr(&Expr::InList {
            expr: Box::new(Expr::Field(FieldRef::Simple("x".into()))),
            list: vec![Expr::Number(1.0)],
            negated: false,
        }),
        "x in (1.0)"
    );
    // IfThenElse
    assert_eq!(
        format_expr(&Expr::IfThenElse {
            cond: Box::new(Expr::Bool(true)),
            then_expr: Box::new(Expr::Number(1.0)),
            else_expr: Box::new(Expr::Number(2.0)),
        }),
        "if true then 1.0 else 2.0"
    );
    // Bracketed field ref
    assert_eq!(
        format_expr(&Expr::Field(FieldRef::Bracketed("e".into(), "dip".into()))),
        "e[\"dip\"]"
    );
    // Path with empty segments renders alias only
    assert_eq!(
        format_expr(&Expr::Field(FieldRef::Path {
            alias: "e".into(),
            segments: vec![],
        })),
        "e"
    );
    // SystemVar variants
    assert_eq!(
        format_expr(&Expr::SystemVar(SystemVar::EventFirstTime)),
        "@event_first_time"
    );
    assert_eq!(
        format_expr(&Expr::SystemVar(SystemVar::WindowStartTime)),
        "@window_start_time"
    );
    assert_eq!(
        format_expr(&Expr::SystemVar(SystemVar::EmitTime)),
        "@emit_time"
    );
}

#[test]
fn format_helpers_cover_all_variants() {
    use super::format::{format_cmp, format_measure, format_transform};

    assert_eq!(format_cmp(CmpOp::Eq), "==");
    assert_eq!(format_cmp(CmpOp::Ne), "!=");
    assert_eq!(format_cmp(CmpOp::Lt), "<");
    assert_eq!(format_cmp(CmpOp::Gt), ">");
    assert_eq!(format_cmp(CmpOp::Le), "<=");
    assert_eq!(format_cmp(CmpOp::Ge), ">=");

    assert_eq!(format_measure(Measure::Count), "count");
    assert_eq!(format_measure(Measure::Sum), "sum");
    assert_eq!(format_measure(Measure::Avg), "avg");
    assert_eq!(format_measure(Measure::Min), "min");
    assert_eq!(format_measure(Measure::Max), "max");

    assert_eq!(format_transform(&Transform::Distinct), "distinct");
}

#[test]
fn format_duration_all_units() {
    use super::format::format_duration;

    assert_eq!(format_duration(&Duration::ZERO), "0s");
    assert_eq!(format_duration(&Duration::from_millis(1500)), "1500ms");
    assert_eq!(format_duration(&Duration::from_millis(100)), "100ms");
    assert_eq!(format_duration(&Duration::from_secs(86400 * 2)), "2d");
    assert_eq!(format_duration(&Duration::from_secs(7200)), "2h");
    assert_eq!(format_duration(&Duration::from_secs(180)), "3m");
    assert_eq!(format_duration(&Duration::from_secs(45)), "45s");
}

#[test]
fn explain_session_window_and_close_mode() {
    let input = r#"
rule r {
    events { s : auth_events }
    match<sip:session(30m)> {
        on event { ev: s | count >= 1; }
        on close { cl: s | count >= 1; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield security_alerts (sip = s.sip, fail_count = 2)
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let expl = &explain_rules(&plans, schemas)[0];

    assert_eq!(expl.match_expl.window_spec, "session(gap=30m)");
    assert_eq!(expl.match_expl.close_steps.len(), 1);
    assert_eq!(expl.match_expl.close_mode, Some(crate::ast::CloseMode::Or));
    assert_eq!(expl.match_expl.event_steps.len(), 1);
}

#[test]
fn explain_seq_step_formats_neg_and_within() {
    let input = r#"
rule r {
    events { a : auth_events }
    match<:5m> {
        on event seq {
            has a within 2s;
            not has a within 3s;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield security_alerts (sip = a.sip, fail_count = 2)
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let expl = &explain_rules(&plans, schemas)[0];
    let seq = expl.match_expl.seq.as_ref().expect("seq steps");
    assert_eq!(seq.len(), 2);
    assert!(
        seq[0].contains("within 2s"),
        "seq step should carry within, got: {}",
        seq[0]
    );
    assert!(
        seq[1].contains("not "),
        "negated seq step should be prefixed with not, got: {}",
        seq[1]
    );
    assert!(seq[1].contains("within 3s"), "got: {}", seq[1]);
}

#[test]
fn explain_joins_renders_modes_reduce_and_emit() {
    let auction_win = WindowSchema {
        name: "auction_events".to_string(),
        streams: vec!["auction_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "category".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "price".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "dateTime".to_string(),
                field_type: bt(BaseType::Time),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: bt(BaseType::Time),
            },
        ],
    };
    let bid_win = WindowSchema {
        name: "bid_events".to_string(),
        streams: vec!["bid_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "auction".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "bidder".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "price".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "event_time".to_string(),
                field_type: bt(BaseType::Time),
            },
        ],
    };
    let out = WindowSchema {
        name: "out".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![FieldDef {
            name: "bidder".to_string(),
            field_type: bt(BaseType::Chars),
        }],
    };
    let input = r#"
rule r {
    events { b : bid_events }
    match<auction:5m> { on event { b | count >= 1; } } -> score(50.0)
    join auction_events snapshot reduce maxrow(price) tie(dateTime desc) as winner
        within [b.event_time, b.event_time]
        on b.auction == auction_events.id
    join bid_events anti within 10s on b.auction == bid_events.auction
    join auction_events asof within 30s on b.auction == auction_events.id
    entity(ip, b.bidder)
    yield out (bidder = b.bidder)
}
"#;
    let schemas = &[bid_win, auction_win, out];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let expl = &explain_rules(&plans, schemas)[0];

    let joined = expl.joins.join("\n");
    assert!(joined.contains("snapshot"), "got: {joined}");
    assert!(
        joined.contains("reduce maxrow(price) tie(dateTime desc) as winner"),
        "got: {joined}"
    );
    assert!(joined.contains("within ["), "got: {joined}");
    assert!(joined.contains("anti"), "got: {joined}");
    assert!(joined.contains("asof within 30s"), "got: {joined}");
}

#[test]
fn explain_limits_and_conv_and_lineage() {
    let input = r#"
rule r {
    events { s : auth_events }
    match<sip:1h:fixed> {
        on event { fail: s.sip | distinct | count >= 1; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield security_alerts (
        sip = s.sip,
        fail_count = count(s),
        message = "m"
    )
    conv { sort(-sip) | top(5) ; where(fail_count >= 1) ; }
    limits {
        max_memory = "256MB";
        max_instances = 3;
        max_throttle = "100/min";
        on_exceed = "fail_rule";
    }
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let expl = &explain_rules(&plans, schemas)[0];

    let conv = expl.conv.as_ref().expect("conv");
    assert_eq!(conv.len(), 2);
    assert!(conv[0].contains("sort(-sip) | top(5)"), "got: {}", conv[0]);
    assert!(
        conv[1].contains("where(fail_count >= 1.0)"),
        "got: {}",
        conv[1]
    );

    let limits = expl.limits.as_ref().expect("limits");
    assert!(limits.contains("max_memory=268435456B"), "got: {limits}");
    assert!(limits.contains("max_instances=3"), "got: {limits}");
    assert!(limits.contains("max_throttle=100/1m"), "got: {limits}");
    assert!(limits.contains("on_exceed=FailRule"), "got: {limits}");

    // Lineage: s.sip traces through the bind alias.
    let (name, origin) = &expl.lineage[0];
    assert_eq!(name, "sip");
    assert_eq!(origin, "auth_events.sip (via s)");
    // count(s) traces over the set-level alias reference.
    let (name, origin) = &expl.lineage[1];
    assert_eq!(name, "fail_count");
    assert_eq!(origin, "count(s) over set-level ref to auth_events");
    // A string literal lineage falls back to the formatted expression.
    let (_, origin) = &expl.lineage[2];
    assert_eq!(origin, "\"m\"");
}

#[test]
fn explain_empty_keys_and_field_lineage_plain() {
    let input = r#"
rule r {
    events { s : auth_events }
    match<:5m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield security_alerts (sip = s.sip, fail_count = 2)
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let expl = &explain_rules(&plans, schemas)[0];

    assert_eq!(expl.match_expl.keys, "(none)");
    assert_eq!(expl.score, "50.0");
    // Bind-alias-free plain field name lineage.
    let (name, origin) = &expl.lineage[0];
    assert_eq!(name, "sip");
    assert_eq!(origin, "auth_events.sip (via s)");
}

/// stats 规则的 `disk_provider`/`max_disk` 在 explain limits 节显示
/// （2026-08-27 改名: 旧名 spill 不再出现）。
#[test]
fn explain_limits_disk_provider_shown_for_stats_rule() {
    let input = r#"
rule r {
    events { e : auth_events }
    stats<10s:fixed> group by (e.sip) { e | count as n; }
    entity(digit, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 2, message = "m")
    limits { disk_provider = "redb"; max_disk = "8GB"; }
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).unwrap();
    let plans = compile_wfl(&file, schemas).unwrap();
    let expl = &explain_rules(&plans, schemas)[0];
    let limits = expl.limits.as_ref().expect("limits");
    assert!(limits.contains("disk_provider=Redb"), "got: {limits}");
    assert!(limits.contains("max_disk=8589934592B"), "got: {limits}");
}
