use crate::ast::RuleDecl;

use crate::checker::scope::Scope;
use crate::checker::types::{check_expr_type, infer_type, is_numeric, is_scalar_identity};
use crate::checker::{CheckError, Severity};

pub fn check_score(rule: &RuleDecl, scope: &Scope<'_>, errors: &mut Vec<CheckError>) {
    let name = &rule.name;
    check_expr_type(&rule.score.expr, scope, name, errors);

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

pub fn check_entity(rule: &RuleDecl, scope: &Scope<'_>, errors: &mut Vec<CheckError>) {
    let name = &rule.name;
    check_expr_type(&rule.entity.id_expr, scope, name, errors);

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
