use std::collections::HashSet;

use crate::ast::RuleDecl;
use crate::schema::WindowSchema;

use crate::checker::scope::Scope;
use crate::checker::types::check_expr_type;
use crate::checker::{CheckError, Severity};

pub fn build_scope<'a>(
    rule: &'a RuleDecl,
    schemas: &'a [WindowSchema],
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) -> Scope<'a> {
    let mut scope = Scope::new();
    let mut seen_aliases = HashSet::new();

    for decl in &rule.events.decls {
        // EV1: alias uniqueness
        if !seen_aliases.insert(decl.alias.as_str()) {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!("duplicate event alias `{}`", decl.alias),
            });
        }

        // EV2: window must exist in schemas
        match schemas.iter().find(|s| s.name == decl.window) {
            Some(ws) => {
                scope.aliases.insert(&decl.alias, ws);
            }
            None => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "event alias `{}` references unknown window `{}`",
                        decl.alias, decl.window
                    ),
                });
            }
        }

        // Check filter expression if present
        if let Some(ref filter) = decl.filter {
            check_expr_type(filter, &scope, rule_name, errors);
        }
    }

    // Register per-event `let` bindings: type-check the binding expression and
    // record its inferred type so later expressions can reference it by name.
    // Bindings are registered in order, so a later `let` may reference an
    // earlier one (no forward references).
    for l in &rule.lets {
        if let Some(t) = crate::checker::types::infer_type(&l.expr, &scope) {
            scope.let_types.insert(l.name.clone(), t);
        }
        crate::checker::types::check_expr_type(&l.expr, &scope, rule_name, errors);
    }

    // Register join target windows so yield expressions can reference join_window.field
    for join in &rule.joins {
        let target = &join.target_window;
        if let Some(ws) = schemas.iter().find(|s| s.name == *target)
            && !scope.aliases.contains_key(target.as_str())
        {
            scope.aliases.insert(target.as_str(), ws);
            scope.join_windows.push(target.as_str());
        }
        // `reduce ... as label`：归约标签注册为 object 别名（review R2）
        register_reduce_labels(&mut scope, &rule.joins);
    }

    scope
}

/// 将 joins 的 `reduce ... as label` 标签注册进 scope（object 别名）。
pub fn register_reduce_labels(scope: &mut Scope<'_>, joins: &[crate::ast::JoinClause]) {
    for join in joins {
        if let Some(label) = join.reduce.as_ref().and_then(|r| r.label.as_ref())
            && !scope.reduce_labels.iter().any(|l| l == label)
        {
            scope.reduce_labels.push(label.clone());
        }
    }
}
