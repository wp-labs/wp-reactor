use wf_lang::ast::WflFile;
use wf_lang::{BaseType, FieldType, WindowSchema};

use crate::wfg_ast::{GenExpr, ParamValue, WfgFile};

/// A validation error found in a `.wfg` file.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationError {
    pub code: &'static str,
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

/// Validate a parsed `.wfg` file against schemas and WFL rules.
///
/// Returns a list of validation errors (empty if valid).
pub fn validate_wfg(
    wfg: &WfgFile,
    schemas: &[WindowSchema],
    wfl_files: &[WflFile],
) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    let scenario = &wfg.scenario;

    // SV2: total > 0
    if scenario.total == 0 {
        errors.push(ValidationError {
            code: "SV2",
            message: "total must be greater than 0".to_string(),
        });
    }

    // SV3: rate.count > 0 for all streams
    for stream in &scenario.streams {
        if stream.rate.count == 0 {
            errors.push(ValidationError {
                code: "SV3",
                message: format!(
                    "stream '{}': rate count must be greater than 0",
                    stream.alias
                ),
            });
        }
    }

    // SV4: percent in (0, 100] for inject lines
    for inject in &scenario.injects {
        for line in &inject.lines {
            if line.percent <= 0.0 || line.percent > 100.0 {
                errors.push(ValidationError {
                    code: "SV4",
                    message: format!(
                        "inject for '{}': percent {} must be in (0, 100]",
                        inject.rule, line.percent
                    ),
                });
            }
        }
    }

    // SV4: percent in (0, 100] for fault lines
    if let Some(faults) = &scenario.faults {
        for fault in &faults.faults {
            if fault.percent <= 0.0 || fault.percent > 100.0 {
                errors.push(ValidationError {
                    code: "SV4",
                    message: format!(
                        "fault '{}': percent {} must be in (0, 100]",
                        fault.fault_type, fault.percent
                    ),
                });
            }
        }
    }

    // SV5: inject line percentages sum <= 100%
    for inject in &scenario.injects {
        let sum: f64 = inject.lines.iter().map(|l| l.percent).sum();
        if sum > 100.0 {
            errors.push(ValidationError {
                code: "SV5",
                message: format!(
                    "inject for '{}': percentages sum to {}, which exceeds 100%",
                    inject.rule, sum
                ),
            });
        }
    }

    // SV6: fault line percentages sum <= 100%
    if let Some(faults) = &scenario.faults {
        let sum: f64 = faults.faults.iter().map(|f| f.percent).sum();
        if sum > 100.0 {
            errors.push(ValidationError {
                code: "SV6",
                message: format!("faults: percentages sum to {}, which exceeds 100%", sum),
            });
        }
    }

    // SC3: stream.window must exist in schemas
    for stream in &scenario.streams {
        if !schemas.iter().any(|s| s.name == stream.window) {
            errors.push(ValidationError {
                code: "SC3",
                message: format!(
                    "stream '{}': window '{}' not found in schemas",
                    stream.alias, stream.window
                ),
            });
        }
    }

    // SC4: field_override field names must exist in the window schema
    for stream in &scenario.streams {
        if let Some(schema) = schemas.iter().find(|s| s.name == stream.window) {
            for ov in &stream.overrides {
                if !schema.fields.iter().any(|f| f.name == ov.field_name) {
                    errors.push(ValidationError {
                        code: "SC4",
                        message: format!(
                            "stream '{}': field '{}' not found in window '{}'",
                            stream.alias, ov.field_name, stream.window
                        ),
                    });
                }
            }
        }
    }

    // SV7: gen_expr type compatibility with field type
    for stream in &scenario.streams {
        if let Some(schema) = schemas.iter().find(|s| s.name == stream.window) {
            for ov in &stream.overrides {
                if let Some(field_def) = schema.fields.iter().find(|f| f.name == ov.field_name) {
                    let base = match &field_def.field_type {
                        FieldType::Base(b) => b,
                        FieldType::Array(b) => b,
                    };
                    if let Some(reason) = check_gen_expr_compat(&ov.gen_expr, base) {
                        errors.push(ValidationError {
                            code: "SV7",
                            message: format!(
                                "stream '{}': field '{}' ({:?}) incompatible with override — {}",
                                stream.alias, ov.field_name, base, reason
                            ),
                        });
                    }
                }
            }
        }
    }

    // Collect all rules from WFL files
    let all_rules: Vec<_> = wfl_files.iter().flat_map(|f| f.rules.iter()).collect();

    // SC2 / SC2a: each stream alias should be declared in some WFL rule's events,
    // and alias->window binding should be consistent with at least one rule.
    if !all_rules.is_empty() {
        for stream in &scenario.streams {
            let alias_decls: Vec<_> = all_rules
                .iter()
                .flat_map(|r| r.events.decls.iter())
                .filter(|e| e.alias == stream.alias)
                .collect();

            if alias_decls.is_empty() {
                errors.push(ValidationError {
                    code: "SC2",
                    message: format!(
                        "stream '{}': alias '{}' is not referenced by any rule events",
                        stream.alias, stream.alias
                    ),
                });
                continue;
            }

            if !alias_decls.iter().any(|e| e.window == stream.window) {
                let windows = alias_decls
                    .iter()
                    .map(|e| e.window.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                errors.push(ValidationError {
                    code: "SC2a",
                    message: format!(
                        "stream '{}': alias '{}' maps to window '{}' but rules map it to: {}",
                        stream.alias, stream.alias, stream.window, windows
                    ),
                });
            }
        }
    }

    // SC5: inject.rule must exist in WFL files
    for inject in &scenario.injects {
        if !all_rules.iter().any(|r| r.name == inject.rule) {
            errors.push(ValidationError {
                code: "SC5",
                message: format!("inject: rule '{}' not found in WFL files", inject.rule),
            });
        }
    }

    // SC6: inject.streams must be aliases of streams defined in the scenario
    // and must be declared in the target rule's events (alias + window).
    for inject in &scenario.injects {
        // Find the target rule (if it exists — SC5 catches missing rules).
        let target_rule = all_rules.iter().find(|r| r.name == inject.rule);

        for stream_name in &inject.streams {
            let scenario_stream = scenario.streams.iter().find(|s| s.alias == *stream_name);
            match scenario_stream {
                None => {
                    errors.push(ValidationError {
                        code: "SC6",
                        message: format!(
                            "inject for '{}': stream alias '{}' not found in scenario streams",
                            inject.rule, stream_name
                        ),
                    });
                }
                Some(stream) => {
                    // SC6/SC2a: target rule must have the alias and matching window.
                    if let Some(rule) = target_rule {
                        let alias_events: Vec<_> = rule
                            .events
                            .decls
                            .iter()
                            .filter(|e| e.alias == *stream_name)
                            .collect();

                        if alias_events.is_empty() {
                            errors.push(ValidationError {
                                code: "SC6",
                                message: format!(
                                    "inject for '{}': stream alias '{}' is not declared in rule '{}' events",
                                    inject.rule,
                                    stream_name,
                                    inject.rule
                                ),
                            });
                            continue;
                        }

                        if !alias_events.iter().any(|e| e.window == stream.window) {
                            let windows = alias_events
                                .iter()
                                .map(|e| e.window.as_str())
                                .collect::<Vec<_>>()
                                .join(", ");
                            errors.push(ValidationError {
                                code: "SC6",
                                message: format!(
                                    "inject for '{}': stream '{}' uses window '{}' but rule '{}' maps alias '{}' to: {}",
                                    inject.rule,
                                    stream_name,
                                    stream.window,
                                    inject.rule,
                                    stream_name,
                                    windows
                                ),
                            });
                        }
                    }
                }
            }
        }
    }

    // SV8: oracle param type/range validation
    if let Some(oracle) = &scenario.oracle {
        for param in &oracle.params {
            match param.name.as_str() {
                "time_tolerance" => {
                    if !matches!(&param.value, ParamValue::Duration(_)) {
                        errors.push(ValidationError {
                            code: "SV8",
                            message: "oracle.time_tolerance must be a duration (e.g. 1s, 500ms)"
                                .to_string(),
                        });
                    }
                }
                "score_tolerance" => match &param.value {
                    ParamValue::Number(n) if *n >= 0.0 => {}
                    ParamValue::Number(n) => {
                        errors.push(ValidationError {
                            code: "SV8",
                            message: format!("oracle.score_tolerance must be >= 0, got {}", n),
                        });
                    }
                    _ => {
                        errors.push(ValidationError {
                            code: "SV8",
                            message: "oracle.score_tolerance must be a number".to_string(),
                        });
                    }
                },
                _ => {}
            }
        }
    }

    errors
}

/// Check whether a `GenExpr` is type-compatible with a field's `BaseType`.
///
/// Returns `None` if compatible, or `Some(reason)` if not.
fn check_gen_expr_compat(expr: &GenExpr, base: &BaseType) -> Option<String> {
    match expr {
        GenExpr::StringLit(_) => match base {
            BaseType::Chars | BaseType::Ip | BaseType::Hex | BaseType::Time => None,
            _ => Some(format!("string literal not compatible with {:?}", base)),
        },
        GenExpr::NumberLit(_) => match base {
            BaseType::Digit | BaseType::Float => None,
            _ => Some(format!("number literal not compatible with {:?}", base)),
        },
        GenExpr::BoolLit(_) => match base {
            BaseType::Bool => None,
            _ => Some(format!("boolean literal not compatible with {:?}", base)),
        },
        GenExpr::GenFunc { name, .. } => check_gen_func_compat(name, base),
    }
}

/// Check whether a known gen function is compatible with a field's `BaseType`.
fn check_gen_func_compat(func_name: &str, base: &BaseType) -> Option<String> {
    match func_name {
        "ipv4" => match base {
            BaseType::Ip | BaseType::Chars => None,
            _ => Some(format!(
                "ipv4() produces IP addresses, not compatible with {:?}",
                base
            )),
        },
        "pattern" => match base {
            BaseType::Chars | BaseType::Ip | BaseType::Hex => None,
            _ => Some(format!(
                "pattern() produces strings, not compatible with {:?}",
                base
            )),
        },
        "range" => match base {
            BaseType::Digit | BaseType::Float => None,
            _ => Some(format!(
                "range() produces numbers, not compatible with {:?}",
                base
            )),
        },
        "timestamp" => match base {
            BaseType::Time | BaseType::Chars => None,
            _ => Some(format!(
                "timestamp() produces time values, not compatible with {:?}",
                base
            )),
        },
        // `enum` is a generic selector — compatible with any type
        "enum" => None,
        // Unknown gen functions — skip validation (user-extensible)
        _ => None,
    }
}

#[cfg(test)]
mod tests {
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
        // String on Chars, Number on Float, ipv4 on Ip, range on Digit — all valid
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
                        gen_expr: GenExpr::NumberLit(3.14),
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
}
