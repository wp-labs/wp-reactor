use super::*;
use crate::wfg_ast::*;
use std::time::Duration;
use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

/// Helper: build a minimal WfgFile.
fn minimal_wfg(streams: Vec<StreamBlock>, injects: Vec<InjectBlock>) -> WfgFile {
    WfgFile {
        uses: vec![],
        scenario: ScenarioDecl {
            name: "test".into(),
            seed: 1,
            time_clause: TimeClause {
                start: "2024-01-01T00:00:00Z".into(),
                duration: Duration::from_secs(3600),
            },
            total: 100,
            streams,
            injects,
            faults: None,
            oracle: None,
        },
    }
}

/// Helper: build a WindowSchema.
fn make_schema(name: &str, fields: Vec<(&str, BaseType)>) -> WindowSchema {
    WindowSchema {
        name: name.into(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(300),
        fields: fields
            .into_iter()
            .map(|(n, bt)| FieldDef {
                name: n.into(),
                field_type: FieldType::Base(bt),
            })
            .collect(),
    }
}

/// Helper: build a minimal WflFile with one rule that references given event windows.
///
/// Uses the parser to avoid `#[non_exhaustive]` construction issues.
fn make_wfl(rule_name: &str, event_windows: Vec<(&str, &str)>) -> wf_lang::ast::WflFile {
    let events_str: String = event_windows
        .iter()
        .map(|(alias, window)| format!("        {alias} : {window}"))
        .collect::<Vec<_>>()
        .join("\n");
    let first_alias = event_windows.first().map(|(a, _)| *a).unwrap_or("e");
    let wfl_src = format!(
        r#"rule {rule_name} {{
    events {{
{events_str}
    }}
    match<sip : 1m> {{
        on event {{
            {first_alias} | count >= 1;
        }}
    }}
    -> score(1)
    entity(ip, {first_alias}.sip)
    yield AlertWindow()
}}"#
    );
    wf_lang::parse_wfl(&wfl_src)
        .unwrap_or_else(|e| panic!("make_wfl parse failed: {e}\nsource:\n{wfl_src}"))
}

fn stream(alias: &str, window: &str) -> StreamBlock {
    StreamBlock {
        alias: alias.into(),
        window: window.into(),
        rate: Rate {
            count: 10,
            unit: RateUnit::PerSecond,
        },
        overrides: vec![],
    }
}

fn stream_with_override(alias: &str, window: &str, field: &str, expr: GenExpr) -> StreamBlock {
    StreamBlock {
        alias: alias.into(),
        window: window.into(),
        rate: Rate {
            count: 10,
            unit: RateUnit::PerSecond,
        },
        overrides: vec![FieldOverride {
            field_name: field.into(),
            gen_expr: expr,
        }],
    }
}

fn inject(rule: &str, streams: Vec<&str>) -> InjectBlock {
    InjectBlock {
        rule: rule.into(),
        streams: streams.into_iter().map(|s| s.into()).collect(),
        lines: vec![InjectLine {
            mode: InjectMode::Hit,
            percent: 20.0,
            params: vec![],
        }],
    }
}

// -----------------------------------------------------------------------
// SC2 / SC2a tests
// -----------------------------------------------------------------------

#[test]
fn test_sc2_stream_alias_not_in_any_rule() {
    let wfg = minimal_wfg(vec![stream("s_missing", "LoginWindow")], vec![]);
    let schemas = vec![make_schema("LoginWindow", vec![])];
    let wfl = make_wfl("my_rule", vec![("e", "LoginWindow")]);
    let errors = validate_wfg(&wfg, &schemas, &[wfl]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SC2" && e.message.contains("s_missing"))
    );
}

#[test]
fn test_sc2a_stream_alias_window_mismatch() {
    let wfg = minimal_wfg(vec![stream("e", "DnsWindow")], vec![]);
    let schemas = vec![
        make_schema("DnsWindow", vec![]),
        make_schema("LoginWindow", vec![]),
    ];
    let wfl = make_wfl("my_rule", vec![("e", "LoginWindow")]);
    let errors = validate_wfg(&wfg, &schemas, &[wfl]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SC2a" && e.message.contains("DnsWindow"))
    );
}

// -----------------------------------------------------------------------
// SC6 / SC2a tests
// -----------------------------------------------------------------------

#[test]
fn test_sc6_inject_stream_not_in_scenario() {
    let wfg = minimal_wfg(
        vec![stream("s1", "LoginWindow")],
        vec![inject("my_rule", vec!["s1", "s_missing"])],
    );
    let schemas = vec![make_schema("LoginWindow", vec![])];
    let wfl = make_wfl("my_rule", vec![("s1", "LoginWindow")]);
    let errors = validate_wfg(&wfg, &schemas, &[wfl]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SC6" && e.message.contains("s_missing"))
    );
}

#[test]
fn test_sc6_sc2a_stream_window_not_in_rule_events() {
    // Stream s1 uses DnsWindow, but the rule only references LoginWindow.
    let wfg = minimal_wfg(
        vec![stream("s1", "DnsWindow")],
        vec![inject("my_rule", vec!["s1"])],
    );
    let schemas = vec![
        make_schema("DnsWindow", vec![]),
        make_schema("LoginWindow", vec![]),
    ];
    let wfl = make_wfl("my_rule", vec![("s1", "LoginWindow")]);
    let errors = validate_wfg(&wfg, &schemas, &[wfl]);
    assert!(errors.iter().any(|e| {
        e.code == "SC6" && e.message.contains("DnsWindow") && e.message.contains("LoginWindow")
    }));
}

#[test]
fn test_sc6_inject_alias_not_in_target_rule_events() {
    let wfg = minimal_wfg(
        vec![stream("s1", "LoginWindow")],
        vec![inject("my_rule", vec!["s1"])],
    );
    let schemas = vec![make_schema("LoginWindow", vec![])];
    let wfl = make_wfl("my_rule", vec![("other_alias", "LoginWindow")]);
    let errors = validate_wfg(&wfg, &schemas, &[wfl]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SC6" && e.message.contains("alias"))
    );
}

#[test]
fn test_sc6_sc2a_valid_stream_window_matches_rule() {
    let wfg = minimal_wfg(
        vec![stream("s1", "LoginWindow")],
        vec![inject("my_rule", vec!["s1"])],
    );
    let schemas = vec![make_schema("LoginWindow", vec![])];
    let wfl = make_wfl("my_rule", vec![("s1", "LoginWindow")]);
    let errors = validate_wfg(&wfg, &schemas, &[wfl]);
    // No SC6 errors expected
    assert!(
        !errors.iter().any(|e| e.code == "SC6"),
        "unexpected SC6: {:?}",
        errors
    );
}

// -----------------------------------------------------------------------
// SV7 tests
// -----------------------------------------------------------------------

#[test]
fn test_sv7_string_lit_on_digit_field() {
    let wfg = minimal_wfg(
        vec![stream_with_override(
            "s1",
            "W",
            "count",
            GenExpr::StringLit("hello".into()),
        )],
        vec![],
    );
    let schemas = vec![make_schema("W", vec![("count", BaseType::Digit)])];
    let errors = validate_wfg(&wfg, &schemas, &[]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SV7" && e.message.contains("count"))
    );
}

#[test]
fn test_sv7_number_lit_on_bool_field() {
    let wfg = minimal_wfg(
        vec![stream_with_override(
            "s1",
            "W",
            "flag",
            GenExpr::NumberLit(42.0),
        )],
        vec![],
    );
    let schemas = vec![make_schema("W", vec![("flag", BaseType::Bool)])];
    let errors = validate_wfg(&wfg, &schemas, &[]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SV7" && e.message.contains("flag"))
    );
}

#[test]
fn test_sv7_bool_lit_on_chars_field() {
    let wfg = minimal_wfg(
        vec![stream_with_override(
            "s1",
            "W",
            "name",
            GenExpr::BoolLit(true),
        )],
        vec![],
    );
    let schemas = vec![make_schema("W", vec![("name", BaseType::Chars)])];
    let errors = validate_wfg(&wfg, &schemas, &[]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SV7" && e.message.contains("name"))
    );
}

#[test]
fn test_sv7_ipv4_on_digit_field() {
    let wfg = minimal_wfg(
        vec![stream_with_override(
            "s1",
            "W",
            "port",
            GenExpr::GenFunc {
                name: "ipv4".into(),
                args: vec![],
            },
        )],
        vec![],
    );
    let schemas = vec![make_schema("W", vec![("port", BaseType::Digit)])];
    let errors = validate_wfg(&wfg, &schemas, &[]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SV7" && e.message.contains("port"))
    );
}

#[test]
fn test_sv7_range_on_ip_field() {
    let wfg = minimal_wfg(
        vec![stream_with_override(
            "s1",
            "W",
            "addr",
            GenExpr::GenFunc {
                name: "range".into(),
                args: vec![],
            },
        )],
        vec![],
    );
    let schemas = vec![make_schema("W", vec![("addr", BaseType::Ip)])];
    let errors = validate_wfg(&wfg, &schemas, &[]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SV7" && e.message.contains("addr"))
    );
}

#[test]
fn test_sv7_enum_compatible_with_any_type() {
    let wfg = minimal_wfg(
        vec![stream_with_override(
            "s1",
            "W",
            "val",
            GenExpr::GenFunc {
                name: "enum".into(),
                args: vec![],
            },
        )],
        vec![],
    );
    let schemas = vec![make_schema("W", vec![("val", BaseType::Digit)])];
    let errors = validate_wfg(&wfg, &schemas, &[]);
    assert!(
        !errors.iter().any(|e| e.code == "SV7"),
        "enum should be compatible with any type"
    );
}

#[test]
fn test_sv7_valid_combinations() {
    // String on Chars, Number on Float, ipv4 on Ip, range on Digit -- all valid
    let wfg = minimal_wfg(
        vec![StreamBlock {
            alias: "s1".into(),
            window: "W".into(),
            rate: Rate {
                count: 10,
                unit: RateUnit::PerSecond,
            },
            overrides: vec![
                FieldOverride {
                    field_name: "name".into(),
                    gen_expr: GenExpr::StringLit("test".into()),
                },
                FieldOverride {
                    field_name: "score".into(),
                    gen_expr: GenExpr::NumberLit(3.21),
                },
                FieldOverride {
                    field_name: "addr".into(),
                    gen_expr: GenExpr::GenFunc {
                        name: "ipv4".into(),
                        args: vec![],
                    },
                },
                FieldOverride {
                    field_name: "count".into(),
                    gen_expr: GenExpr::GenFunc {
                        name: "range".into(),
                        args: vec![],
                    },
                },
            ],
        }],
        vec![],
    );
    let schemas = vec![make_schema(
        "W",
        vec![
            ("name", BaseType::Chars),
            ("score", BaseType::Float),
            ("addr", BaseType::Ip),
            ("count", BaseType::Digit),
        ],
    )];
    let errors = validate_wfg(&wfg, &schemas, &[]);
    assert!(
        !errors.iter().any(|e| e.code == "SV7"),
        "all valid: {:?}",
        errors
    );
}

// -----------------------------------------------------------------------
// SV8 tests (oracle param validation)
// -----------------------------------------------------------------------

fn wfg_with_oracle(oracle: OracleBlock) -> WfgFile {
    WfgFile {
        uses: vec![],
        scenario: ScenarioDecl {
            name: "test".into(),
            seed: 1,
            time_clause: TimeClause {
                start: "2024-01-01T00:00:00Z".into(),
                duration: Duration::from_secs(3600),
            },
            total: 100,
            streams: vec![],
            injects: vec![],
            faults: None,
            oracle: Some(oracle),
        },
    }
}

#[test]
fn test_sv8_time_tolerance_must_be_duration() {
    let oracle = OracleBlock {
        params: vec![ParamAssign {
            name: "time_tolerance".into(),
            value: ParamValue::Number(42.0), // wrong: should be Duration
        }],
    };
    let wfg = wfg_with_oracle(oracle);
    let errors = validate_wfg(&wfg, &[], &[]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SV8" && e.message.contains("time_tolerance")),
        "errors: {:?}",
        errors
    );
}

#[test]
fn test_sv8_score_tolerance_must_be_nonneg_number() {
    let oracle = OracleBlock {
        params: vec![ParamAssign {
            name: "score_tolerance".into(),
            value: ParamValue::Number(-0.5), // negative
        }],
    };
    let wfg = wfg_with_oracle(oracle);
    let errors = validate_wfg(&wfg, &[], &[]);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SV8" && e.message.contains("score_tolerance")),
        "errors: {:?}",
        errors
    );
}

#[test]
fn test_sv8_score_tolerance_string_rejected() {
    let oracle = OracleBlock {
        params: vec![ParamAssign {
            name: "score_tolerance".into(),
            value: ParamValue::String("hello".into()),
        }],
    };
    let wfg = wfg_with_oracle(oracle);
    let errors = validate_wfg(&wfg, &[], &[]);
    assert!(
        errors.iter().any(|e| e.code == "SV8"),
        "errors: {:?}",
        errors
    );
}

#[test]
fn test_sv8_valid_oracle_params() {
    let oracle = OracleBlock {
        params: vec![
            ParamAssign {
                name: "time_tolerance".into(),
                value: ParamValue::Duration(Duration::from_secs(2)),
            },
            ParamAssign {
                name: "score_tolerance".into(),
                value: ParamValue::Number(0.05),
            },
        ],
    };
    let wfg = wfg_with_oracle(oracle);
    let errors = validate_wfg(&wfg, &[], &[]);
    assert!(
        !errors.iter().any(|e| e.code == "SV8"),
        "should pass: {:?}",
        errors
    );
}
