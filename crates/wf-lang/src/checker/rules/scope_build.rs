use std::collections::HashSet;

use crate::ast::RuleDecl;
use crate::schema::WindowSchema;

use crate::checker::scope::Scope;
use crate::checker::types::{
    ExprPosition, check_expr_position, check_expr_type, rule_expr_position,
};
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
            // 绑定阶段由 wf-cep 逐事件求值：L3 集合函数在此恒求值为空（issue #101
            // 同类位置——编译放行则规则静默不触发）。
            check_expr_position(&inlined, ExprPosition::BindFilter, rule_name, errors);
        }
    }

    // Register per-event `let` bindings: type-check the binding expression and
    // record its inferred type so later expressions can reference it by name.
    // Bindings are registered in order, so a later `let` may reference an
    // earlier one (no forward references).
    let mut declared_lets: HashSet<String> = HashSet::new();
    for l in &rule.lets {
        let first_new = errors.len();
        if let Some(t) = crate::checker::types::infer_type(&l.expr, &scope) {
            scope.let_types.insert(l.name.clone(), t);
        }
        crate::checker::types::check_expr_type(&l.expr, &scope, rule_name, errors);
        // `on each`（含 deferred）规则的 `let` 在单事件上求值（无任何动态上下文）；
        // match/close 规则的 `let` 在 instance 上下文求值——L3 可用，但
        // `window.has` / `baseline` 仍不可用（该求值路径不传窗口表 / 滚动状态）。
        check_expr_position(&l.expr, rule_expr_position(rule), rule_name, errors);
        // 前向引用（`let a = b` 而 `b` 声明在后）在表达式类型检查里表现为
        // 「字段 `b` 不存在」——那只是名字没能解析成字段，指向的是 window schema，
        // 会把用户带偏。若该名字确实是本规则中**声明在后**的 `let`，就把这条消息
        // 改写为声明顺序问题。**只改文案**：报错与否、规则接受/拒绝的判定不变
        // （名字同时是字段时本就不会产生该错误，也就不会被改写）。
        let later: Vec<String> = crate::ast::collect_rule_let_refs(&l.expr, &rule.lets)
            .into_iter()
            .filter(|n| !declared_lets.contains(n))
            .collect();
        for name in later {
            let noisy = format!("field `{name}` not found in any event source");
            for e in errors[first_new..].iter_mut() {
                if e.message == noisy {
                    e.message = format!(
                        "rule-level let `{name}` is referenced by `{}` before its declaration; rule-level lets resolve in declaration order (forward references are not supported)",
                        l.name
                    );
                }
            }
        }
        declared_lets.insert(l.name.clone());
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
