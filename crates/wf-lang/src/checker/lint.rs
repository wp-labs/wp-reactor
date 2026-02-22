use std::collections::HashSet;

use crate::ast::{CmpOp, Expr, MatchStep, WflFile};
use crate::schema::WindowSchema;

use super::{CheckError, Severity};

/// System field names that yield arguments must not shadow.
const SYSTEM_FIELD_NAMES: &[&str] = &[
    "rule_name",
    "emit_time",
    "score",
    "entity_type",
    "entity_id",
    "close_reason",
    "score_contrib",
];

/// Run lint checks on a parsed WflFile, producing `Severity::Warning` diagnostics.
///
/// These are best-practice warnings that do not prevent compilation.
/// Call this *after* `check_wfl` passes (or alongside it, since lint checks
/// are independent of error-level checks).
pub fn lint_wfl(file: &WflFile, _schemas: &[WindowSchema]) -> Vec<CheckError> {
    let mut warnings = Vec::new();

    for rule in &file.rules {
        let name = &rule.name;

        // W001: unused event alias
        lint_unused_alias(rule, name, &mut warnings);

        // W002: missing on_close
        lint_missing_on_close(rule, name, &mut warnings);

        // W003: high cardinality key
        lint_high_cardinality_key(rule, name, &mut warnings);

        // W004 + W005: threshold/score zero checks
        lint_steps(&rule.match_clause.on_event, name, &mut warnings);
        if let Some(ref close_steps) = rule.match_clause.on_close {
            lint_steps(close_steps, name, &mut warnings);
        }

        // W005: score always zero
        lint_score_zero(rule, name, &mut warnings);

        // W006: yield field name near-matches a system field
        lint_yield_case_collision(rule, name, &mut warnings);
    }

    warnings
}

// ---------------------------------------------------------------------------
// W001: unused event alias
// ---------------------------------------------------------------------------

fn lint_unused_alias(
    rule: &crate::ast::RuleDecl,
    rule_name: &str,
    warnings: &mut Vec<CheckError>,
) {
    let declared: HashSet<&str> = rule.events.decls.iter().map(|d| d.alias.as_str()).collect();
    let mut used: HashSet<&str> = HashSet::new();

    // Collect aliases referenced in match steps
    collect_step_sources(&rule.match_clause.on_event, &mut used);
    if let Some(ref close_steps) = rule.match_clause.on_close {
        collect_step_sources(close_steps, &mut used);
    }

    // Collect aliases referenced in score expression
    collect_expr_aliases(&rule.score.expr, &declared, &mut used);

    // Collect aliases referenced in entity id expression
    collect_expr_aliases(&rule.entity.id_expr, &declared, &mut used);

    // Collect aliases referenced in yield arguments
    for arg in &rule.yield_clause.args {
        collect_expr_aliases(&arg.value, &declared, &mut used);
    }

    for alias in &declared {
        if !used.contains(alias) {
            warnings.push(CheckError {
                severity: Severity::Warning,
                rule: Some(rule_name.to_string()),
                contract: None,
                message: format!(
                    "[W001] event alias `{}` is declared but never referenced in match steps or expressions",
                    alias
                ),
            });
        }
    }
}

fn collect_step_sources<'a>(steps: &'a [MatchStep], used: &mut HashSet<&'a str>) {
    for step in steps {
        for branch in &step.branches {
            used.insert(&branch.source);
        }
    }
}

fn collect_expr_aliases<'a>(
    expr: &'a Expr,
    declared: &HashSet<&str>,
    used: &mut HashSet<&'a str>,
) {
    match expr {
        Expr::Field(crate::ast::FieldRef::Simple(name)) => {
            if declared.contains(name.as_str()) {
                used.insert(name);
            }
        }
        Expr::Field(crate::ast::FieldRef::Qualified(alias, _))
        | Expr::Field(crate::ast::FieldRef::Bracketed(alias, _)) => {
            used.insert(alias);
        }
        Expr::BinOp { left, right, .. } => {
            collect_expr_aliases(left, declared, used);
            collect_expr_aliases(right, declared, used);
        }
        Expr::Neg(inner) => collect_expr_aliases(inner, declared, used),
        Expr::FuncCall { args, .. } => {
            for arg in args {
                collect_expr_aliases(arg, declared, used);
            }
        }
        Expr::InList { expr: inner, list, .. } => {
            collect_expr_aliases(inner, declared, used);
            for item in list {
                collect_expr_aliases(item, declared, used);
            }
        }
        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => {}
    }
}

// ---------------------------------------------------------------------------
// W002: missing on_close
// ---------------------------------------------------------------------------

fn lint_missing_on_close(
    rule: &crate::ast::RuleDecl,
    rule_name: &str,
    warnings: &mut Vec<CheckError>,
) {
    if rule.match_clause.on_close.is_none() {
        warnings.push(CheckError {
            severity: Severity::Warning,
            rule: Some(rule_name.to_string()),
            contract: None,
            message: "[W002] match clause has no `on close` block; window timeout will not trigger close-phase evaluation".to_string(),
        });
    }
}

// ---------------------------------------------------------------------------
// W003: high cardinality key (>= 4 keys)
// ---------------------------------------------------------------------------

fn lint_high_cardinality_key(
    rule: &crate::ast::RuleDecl,
    rule_name: &str,
    warnings: &mut Vec<CheckError>,
) {
    if rule.match_clause.keys.len() >= 4 {
        warnings.push(CheckError {
            severity: Severity::Warning,
            rule: Some(rule_name.to_string()),
            contract: None,
            message: format!(
                "[W003] match clause has {} keys; high-cardinality keys may cause excessive memory usage",
                rule.match_clause.keys.len()
            ),
        });
    }
}

// ---------------------------------------------------------------------------
// W004: threshold is 0 with >= or >
// ---------------------------------------------------------------------------

fn lint_steps(
    steps: &[MatchStep],
    rule_name: &str,
    warnings: &mut Vec<CheckError>,
) {
    for step in steps {
        for branch in &step.branches {
            // W004: threshold == 0 with >= or >
            if is_zero(&branch.pipe.threshold)
                && matches!(branch.pipe.cmp, CmpOp::Ge | CmpOp::Gt)
            {
                warnings.push(CheckError {
                    severity: Severity::Warning,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "[W004] step threshold is 0 with `{}`; this condition is trivially true for any non-negative measure",
                        cmp_symbol(branch.pipe.cmp)
                    ),
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// W005: score expression is literally 0
// ---------------------------------------------------------------------------

fn lint_score_zero(
    rule: &crate::ast::RuleDecl,
    rule_name: &str,
    warnings: &mut Vec<CheckError>,
) {
    if is_zero(&rule.score.expr) {
        warnings.push(CheckError {
            severity: Severity::Warning,
            rule: Some(rule_name.to_string()),
            contract: None,
            message: "[W005] score expression is always 0.0; this rule will produce zero-score alerts".to_string(),
        });
    }
}

// ---------------------------------------------------------------------------
// W006: yield field name case-collides with a system field
// ---------------------------------------------------------------------------

fn lint_yield_case_collision(
    rule: &crate::ast::RuleDecl,
    rule_name: &str,
    warnings: &mut Vec<CheckError>,
) {
    for arg in &rule.yield_clause.args {
        let lower = arg.name.to_ascii_lowercase();
        // Exact matches are already caught as errors by rules.rs; skip those.
        if SYSTEM_FIELD_NAMES.contains(&arg.name.as_str()) {
            continue;
        }
        if SYSTEM_FIELD_NAMES.contains(&lower.as_str()) {
            warnings.push(CheckError {
                severity: Severity::Warning,
                rule: Some(rule_name.to_string()),
                contract: None,
                message: format!(
                    "[W006] yield field `{}` differs from system field `{}` only by case; this may cause confusion",
                    arg.name, lower
                ),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_zero(expr: &Expr) -> bool {
    matches!(expr, Expr::Number(n) if *n == 0.0)
}

fn cmp_symbol(cmp: CmpOp) -> &'static str {
    match cmp {
        CmpOp::Eq => "==",
        CmpOp::Ne => "!=",
        CmpOp::Lt => "<",
        CmpOp::Gt => ">",
        CmpOp::Le => "<=",
        CmpOp::Ge => ">=",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
    use crate::wfl_parser::parse_wfl;

    use super::*;

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
                FieldDef { name: "sip".to_string(), field_type: bt(BaseType::Ip) },
                FieldDef { name: "dip".to_string(), field_type: bt(BaseType::Ip) },
                FieldDef { name: "action".to_string(), field_type: bt(BaseType::Chars) },
                FieldDef { name: "user".to_string(), field_type: bt(BaseType::Chars) },
                FieldDef { name: "count".to_string(), field_type: bt(BaseType::Digit) },
                FieldDef { name: "event_time".to_string(), field_type: bt(BaseType::Time) },
            ],
        }
    }

    fn output_window() -> WindowSchema {
        WindowSchema {
            name: "out".to_string(),
            streams: vec![],
            time_field: None,
            over: Duration::from_secs(3600),
            fields: vec![
                FieldDef { name: "x".to_string(), field_type: bt(BaseType::Ip) },
                FieldDef { name: "y".to_string(), field_type: bt(BaseType::Chars) },
                FieldDef { name: "n".to_string(), field_type: bt(BaseType::Digit) },
            ],
        }
    }

    fn lint_warnings(input: &str, schemas: &[WindowSchema]) -> Vec<String> {
        let file = parse_wfl(input).expect("parse should succeed");
        let warnings = lint_wfl(&file, schemas);
        warnings.into_iter().map(|e| e.message).collect()
    }

    fn assert_has_warning(input: &str, schemas: &[WindowSchema], substring: &str) {
        let warnings = lint_warnings(input, schemas);
        assert!(
            warnings.iter().any(|w| w.contains(substring)),
            "expected a warning containing {:?}, got: {:?}",
            substring,
            warnings
        );
    }

    fn assert_no_warning(input: &str, schemas: &[WindowSchema], substring: &str) {
        let warnings = lint_warnings(input, schemas);
        assert!(
            !warnings.iter().any(|w| w.contains(substring)),
            "expected no warning containing {:?}, got: {:?}",
            substring,
            warnings
        );
    }

    // W001: unused event alias
    #[test]
    fn w001_unused_alias_detected() {
        let input = r#"
rule r {
    events {
        e : auth_events && action == "failed"
        unused : auth_events
    }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        assert_has_warning(
            input,
            &[auth_events_window(), output_window()],
            "W001",
        );
    }

    #[test]
    fn w001_all_used_no_warning() {
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        assert_no_warning(
            input,
            &[auth_events_window(), output_window()],
            "W001",
        );
    }

    // W002: missing on_close
    #[test]
    fn w002_no_on_close() {
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        assert_has_warning(
            input,
            &[auth_events_window(), output_window()],
            "W002",
        );
    }

    #[test]
    fn w002_has_on_close_no_warning() {
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e | count >= 1; }
        on close { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        assert_no_warning(
            input,
            &[auth_events_window(), output_window()],
            "W002",
        );
    }

    // W003: high cardinality key
    #[test]
    fn w003_four_keys() {
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip, dip, action, user:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        assert_has_warning(
            input,
            &[auth_events_window(), output_window()],
            "W003",
        );
    }

    #[test]
    fn w003_three_keys_no_warning() {
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip, dip, action:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        assert_no_warning(
            input,
            &[auth_events_window(), output_window()],
            "W003",
        );
    }

    // W004: threshold zero with >= or >
    #[test]
    fn w004_threshold_zero_ge() {
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 0; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        assert_has_warning(
            input,
            &[auth_events_window(), output_window()],
            "W004",
        );
    }

    #[test]
    fn w004_threshold_nonzero_no_warning() {
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        assert_no_warning(
            input,
            &[auth_events_window(), output_window()],
            "W004",
        );
    }

    // W005: score always zero
    #[test]
    fn w005_score_zero() {
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(0.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        assert_has_warning(
            input,
            &[auth_events_window(), output_window()],
            "W005",
        );
    }

    #[test]
    fn w005_score_nonzero_no_warning() {
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
        assert_no_warning(
            input,
            &[auth_events_window(), output_window()],
            "W005",
        );
    }

    // W006: yield field case collision with system field
    #[test]
    fn w006_case_collision() {
        let out = WindowSchema {
            name: "out".to_string(),
            streams: vec![],
            time_field: None,
            over: Duration::from_secs(3600),
            fields: vec![
                FieldDef { name: "Score".to_string(), field_type: bt(BaseType::Float) },
            ],
        };
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (Score = 42.0)
}
"#;
        assert_has_warning(
            input,
            &[auth_events_window(), out],
            "W006",
        );
    }

    #[test]
    fn w006_exact_match_no_warning() {
        // Exact match is already an error in rules.rs, lint should skip it
        let out = WindowSchema {
            name: "out".to_string(),
            streams: vec![],
            time_field: None,
            over: Duration::from_secs(3600),
            fields: vec![
                FieldDef { name: "sip".to_string(), field_type: bt(BaseType::Ip) },
            ],
        };
        let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (sip = e.sip)
}
"#;
        assert_no_warning(
            input,
            &[auth_events_window(), out],
            "W006",
        );
    }
}
