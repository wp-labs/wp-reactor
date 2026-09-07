use std::collections::HashSet;

use crate::ast::RuleDecl;
use crate::schema::WindowSchema;

use crate::checker::scope::Scope;
use crate::checker::types::check_expr_type;
use crate::checker::{CheckError, Severity};

pub(crate) fn build_scope<'a>(
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

        // Check filter expression if present. Const-string rule-level `let`s
        // (regex reuse, issue #90) are inlined to literals first so the checker
        // sees the same AST as the compiler/runtime; references to non-literal
        // lets are rejected explicitly (bind filters run before per-event `let`
        // injection, so only string-literal lets can be referenced there).
        if let Some(ref filter) = decl.filter {
            let mut visiting = Vec::new();
            let inlined = crate::ast::inline_const_string_lets(filter, &rule.lets, &mut visiting);
            let leftover_lets = crate::ast::collect_rule_let_refs(&inlined, &rule.lets);
            if !leftover_lets.is_empty() {
                // 非字面量 let 引用已显式报错；继续检查会级联出
                // “field not found” 噪音，跳过本条件的深层检查。
                for name in leftover_lets {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "rule-level let `{}` used in the events condition must be a string literal (regex pattern); non-literal lets are evaluated after the bind filter and cannot be referenced here",
                            name
                        ),
                    });
                }
                continue;
            }
            check_expr_type(&inlined, &scope, rule_name, errors);
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
pub(crate) fn register_reduce_labels(scope: &mut Scope<'_>, joins: &[crate::ast::JoinClause]) {
    for join in joins {
        if let Some(label) = join.reduce.as_ref().and_then(|r| r.label.as_ref())
            && !scope.reduce_labels.iter().any(|l| l == label)
        {
            scope.reduce_labels.push(label.clone());
        }
    }
}
