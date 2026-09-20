use crate::ast::RuleDecl;

use crate::checker::scope::Scope;
use crate::checker::types::{
    check_expr_position, check_expr_type, infer_type, is_numeric, is_scalar_identity,
    rule_expr_position,
};
use crate::checker::{CheckError, Severity};

pub(crate) fn check_score(rule: &RuleDecl, scope: &Scope<'_>, errors: &mut Vec<CheckError>) {
    let name = &rule.name;
    check_expr_type(&rule.score.expr, scope, name, errors);
    // score 的运行期位置：match/close 规则在 instance 上下文（L3 可用，但无窗口表 /
    // 滚动状态）；`on each` 规则在单事件上求值（三项能力皆无）。用 `rule_expr_position`
    // 而不是写死 `Instance`，否则 `on each` 的 score/entity 会被当成 instance 上下文
    // 而放行 L3 集合函数（运行期恒为空）。
    check_expr_position(&rule.score.expr, rule_expr_position(rule), name, errors);

    if let Some(t) = infer_type(&rule.score.expr, scope)
        && !is_numeric(&t)
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(name.to_string()),
            test: None,
            message: format!("score expression must be numeric, got {:?}", t),
        });
    }
}

pub(crate) fn check_entity(rule: &RuleDecl, scope: &Scope<'_>, errors: &mut Vec<CheckError>) {
    let name = &rule.name;
    check_expr_type(&rule.entity.id_expr, scope, name, errors);
    // entity 与 score 同位置（用途同 score）。
    check_expr_position(&rule.entity.id_expr, rule_expr_position(rule), name, errors);

    if let Some(t) = infer_type(&rule.entity.id_expr, scope)
        && !is_scalar_identity(&t)
    {
        errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(name.to_string()),
                test: None,
                message: format!(
                    "entity id expression must be a scalar identity type (chars/ip/hex/digit), got {:?}",
                    t
                ),
            });
    }
}
