use std::collections::HashSet;

use crate::ast::{CmpOp, Expr, MatchStep, WflFile};
use crate::schema::WindowSchema;

use super::{CheckError, Severity};

#[cfg(test)]
mod tests;

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

fn lint_unused_alias(rule: &crate::ast::RuleDecl, rule_name: &str, warnings: &mut Vec<CheckError>) {
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
                test: None,
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

fn collect_expr_aliases<'a>(expr: &'a Expr, declared: &HashSet<&str>, used: &mut HashSet<&'a str>) {
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
        Expr::InList {
            expr: inner, list, ..
        } => {
            collect_expr_aliases(inner, declared, used);
            for item in list {
                collect_expr_aliases(item, declared, used);
            }
        }
        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => {}
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            collect_expr_aliases(cond, declared, used);
            collect_expr_aliases(then_expr, declared, used);
            collect_expr_aliases(else_expr, declared, used);
        }
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
            test: None,
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
            test: None,
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

fn lint_steps(steps: &[MatchStep], rule_name: &str, warnings: &mut Vec<CheckError>) {
    for step in steps {
        for branch in &step.branches {
            // W004: threshold == 0 with >= or >
            if is_zero(&branch.pipe.threshold) && matches!(branch.pipe.cmp, CmpOp::Ge | CmpOp::Gt) {
                warnings.push(CheckError {
                    severity: Severity::Warning,
                    rule: Some(rule_name.to_string()),
                    test: None,
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

fn lint_score_zero(rule: &crate::ast::RuleDecl, rule_name: &str, warnings: &mut Vec<CheckError>) {
    if is_zero(&rule.score.expr) {
        warnings.push(CheckError {
            severity: Severity::Warning,
            rule: Some(rule_name.to_string()),
            test: None,
            message:
                "[W005] score expression is always 0.0; this rule will produce zero-score alerts"
                    .to_string(),
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
        if SYSTEM_FIELD_NAMES.contains(&arg.name.as_str()) {
            continue;
        }
        if SYSTEM_FIELD_NAMES.contains(&lower.as_str()) {
            warnings.push(CheckError {
                severity: Severity::Warning,
                rule: Some(rule_name.to_string()),
                test: None,
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
