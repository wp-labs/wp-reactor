use std::time::Duration;

use crate::ast::*;
use crate::compiler::compile_wfl;
use crate::plan::*;
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
use crate::wfl_parser::parse_wfl;

mod basic;
mod edge;
mod keys_entity;
mod pipeline;
mod seq;
mod yield_score;

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

pub(super) fn bt(b: BaseType) -> FieldType {
    FieldType::Base(b)
}

pub(super) fn make_window(
    name: &str,
    streams: Vec<&str>,
    fields: Vec<(&str, FieldType)>,
) -> WindowSchema {
    WindowSchema {
        name: name.to_string(),
        streams: streams.into_iter().map(String::from).collect(),
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: fields
            .into_iter()
            .map(|(n, ft)| FieldDef {
                name: n.to_string(),
                field_type: ft,
            })
            .collect(),
    }
}

pub(super) fn make_output_window(name: &str, fields: Vec<(&str, FieldType)>) -> WindowSchema {
    WindowSchema {
        name: name.to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: fields
            .into_iter()
            .map(|(n, ft)| FieldDef {
                name: n.to_string(),
                field_type: ft,
            })
            .collect(),
    }
}

pub(super) fn auth_events_window() -> WindowSchema {
    make_window(
        "auth_events",
        vec!["auth_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("action", bt(BaseType::Chars)),
            ("user", bt(BaseType::Chars)),
            ("count", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn fw_events_window() -> WindowSchema {
    make_window(
        "fw_events",
        vec!["fw_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("dport", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Generic window used by many tests as "win".
pub(super) fn generic_window() -> WindowSchema {
    make_window(
        "win",
        vec!["stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("dport", bt(BaseType::Digit)),
            ("action", bt(BaseType::Chars)),
            ("host", bt(BaseType::Chars)),
            ("active", bt(BaseType::Bool)),
            ("detail.sha256", bt(BaseType::Hex)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Second generic window used by tests as "win2".
pub(super) fn generic_window2() -> WindowSchema {
    make_window(
        "win2",
        vec!["stream2"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dport", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn dns_query_window() -> WindowSchema {
    make_window(
        "dns_query",
        vec!["dns_stream"],
        vec![
            ("query_id", bt(BaseType::Chars)),
            ("sip", bt(BaseType::Ip)),
            ("domain", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn dns_response_window() -> WindowSchema {
    make_window(
        "dns_response",
        vec!["dns_stream"],
        vec![
            ("query_id", bt(BaseType::Chars)),
            ("sip", bt(BaseType::Ip)),
            ("close_reason", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn output_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("y", bt(BaseType::Chars)),
            ("n", bt(BaseType::Digit)),
        ],
    )
}

pub(super) fn security_alerts_window() -> WindowSchema {
    make_output_window(
        "security_alerts",
        vec![
            ("sip", bt(BaseType::Ip)),
            ("fail_count", bt(BaseType::Digit)),
            ("port_count", bt(BaseType::Digit)),
            ("message", bt(BaseType::Chars)),
        ],
    )
}

// ---------------------------------------------------------------------------
// Compile helper
// ---------------------------------------------------------------------------

/// Compile a WFL source string with given schemas, asserting parse + compile
/// both succeed.
pub(super) fn compile_with(src: &str, schemas: &[WindowSchema]) -> Vec<RulePlan> {
    let file = parse_wfl(src).expect("parse should succeed");
    compile_wfl(&file, schemas).expect("compile should succeed")
}

#[test]
fn collect_aliases_from_qualified_field_ref() {
    let expr = crate::ast::Expr::Field(crate::ast::FieldRef::Qualified("e".into(), "dip".into()));
    let mut aliases = std::collections::HashSet::new();
    super::collect_bind_tracking_aliases(&expr, &mut aliases);
    assert!(
        aliases.contains("e"),
        "alias 'e' should be collected from e.dip"
    );
}

#[test]
fn collect_aliases_from_bracketed_field_ref() {
    let expr = crate::ast::Expr::Field(crate::ast::FieldRef::Bracketed("e".into(), "dip".into()));
    let mut aliases = std::collections::HashSet::new();
    super::collect_bind_tracking_aliases(&expr, &mut aliases);
    assert!(
        aliases.contains("e"),
        "alias 'e' should be collected from e[\"dip\"]"
    );
}

#[test]
fn simple_field_ref_not_collected() {
    let expr = crate::ast::Expr::Field(crate::ast::FieldRef::Simple("dip".into()));
    let mut aliases = std::collections::HashSet::new();
    super::collect_bind_tracking_aliases(&expr, &mut aliases);
    assert!(aliases.is_empty(), "simple field ref should not add alias");
}

#[test]
fn stat_window_event_tracks_bind_alias_count() {
    let src = r#"
rule stat_tracking {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(window_event(auth)))
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let plan = plans
        .iter()
        .find(|plan| plan.name == "stat_tracking")
        .expect("compiled rule should exist");
    assert!(
        plan.match_plan.tracked_bind_aliases.contains("auth"),
        "stat.count(window_event(auth)) should track alias auth count"
    );
}

#[test]
fn yield_preset_expression_tracks_bind_fields() {
    let src = r#"
yield preset base (
    x = e.dip
)

rule preset_tracking {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, "static")
    yield out : base (
        y = "ok",
        n = 1
    )
}
"#;
    let plans = compile_with(src, &[auth_events_window(), output_window()]);
    let plan = plans
        .iter()
        .find(|plan| plan.name == "preset_tracking")
        .expect("compiled rule should exist");
    let fields = plan
        .match_plan
        .tracked_bind_fields
        .get("e")
        .expect("preset expression should track alias e");
    assert!(fields.contains("dip"), "yield preset e.dip");
    assert_eq!(fields.len(), 1, "only preset-referenced field is tracked");
}

#[test]
fn yield_expression_collects_aliases() {
    let score_expr = crate::ast::Expr::Number(70.0);
    let entity_expr =
        crate::ast::Expr::Field(crate::ast::FieldRef::Qualified("e".into(), "sip".into()));
    let yield_fields = vec![
        super::YieldField {
            name: "sip".into(),
            value: crate::ast::Expr::Field(crate::ast::FieldRef::Qualified(
                "e".into(),
                "sip".into(),
            )),
        },
        super::YieldField {
            name: "dip".into(),
            value: crate::ast::Expr::Field(crate::ast::FieldRef::Qualified(
                "e".into(),
                "dip".into(),
            )),
        },
        super::YieldField {
            name: "alert_type".into(),
            value: crate::ast::Expr::StringLit("test".into()),
        },
        super::YieldField {
            name: "plain_user".into(),
            value: crate::ast::Expr::Field(crate::ast::FieldRef::Simple("user".into())),
        },
    ];
    let aliases =
        super::collect_rule_bind_tracking_aliases(&score_expr, &entity_expr, &yield_fields);
    assert!(aliases.contains("e"), "alias 'e' should be collected");
    assert_eq!(aliases.len(), 1, "only 'e' should be collected");

    let tracking = super::collect_rule_bind_tracking(&score_expr, &entity_expr, &yield_fields);
    let fields = tracking
        .fields
        .get("e")
        .expect("tracked fields should include alias 'e'");
    assert!(fields.contains("sip"), "field 'sip' should be tracked");
    assert!(fields.contains("dip"), "field 'dip' should be tracked");
    assert_eq!(fields.len(), 2, "only referenced fields should be tracked");
    assert!(
        tracking.plain_fields.contains("user"),
        "plain field 'user' should be tracked"
    );
}

#[test]
fn compiled_plan_tracks_only_fields_needed_by_outputs_and_l3_exprs() {
    let src = r#"
rule tracked_fields {
    events { e : auth_events }
    match<sip:5m> {
        on event { e.dport | distinct | count >= 2; }
        and close { e.dport | distinct | count >= 2; }
    } -> score(avg(e.count))
    entity(user, last(e.user))
    yield tracked_out (
        sip = e.dip,
        fail_count = avg(e.count),
        actions = collect_set(e.action),
        message = sip
    )
}
"#;
    let tracked_in = make_window(
        "auth_events",
        vec!["auth_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("dip", bt(BaseType::Ip)),
            ("dport", bt(BaseType::Digit)),
            ("action", bt(BaseType::Chars)),
            ("user", bt(BaseType::Chars)),
            ("count", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let tracked_out = make_output_window(
        "tracked_out",
        vec![
            ("sip", bt(BaseType::Ip)),
            ("fail_count", bt(BaseType::Float)),
            ("actions", FieldType::Array(BaseType::Chars)),
            ("message", bt(BaseType::Ip)),
        ],
    );
    let plans = compile_with(src, &[tracked_in, tracked_out]);
    let plan = plans
        .iter()
        .find(|plan| plan.name == "tracked_fields")
        .expect("compiled rule should exist");

    let fields = plan
        .match_plan
        .tracked_bind_fields
        .get("e")
        .expect("alias e should have tracked fields");
    assert!(fields.contains("action"), "collect_set(e.action)");
    assert!(fields.contains("count"), "avg(e.count)");
    assert!(fields.contains("dip"), "yield e.dip");
    assert!(fields.contains("user"), "entity last(e.user)");
    assert!(
        !fields.contains("dport"),
        "branch field is handled by close/event branch collection, not alias tracking"
    );
    assert_eq!(fields.len(), 4, "only referenced alias fields are tracked");
    assert!(
        plan.match_plan.tracked_plain_fields.contains("sip"),
        "plain yield field should be tracked for close-step field collection"
    );
}
