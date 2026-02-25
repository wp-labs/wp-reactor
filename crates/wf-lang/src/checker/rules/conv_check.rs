use crate::ast::{RuleDecl, WindowMode};

use crate::checker::{CheckError, Severity};

pub fn check_conv(rule: &RuleDecl, rule_name: &str, errors: &mut Vec<CheckError>) {
    if rule.conv.is_some() && rule.match_clause.window_mode != WindowMode::Fixed {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: "conv block requires fixed window mode (match<key:dur:fixed>)".to_string(),
        });
    }
}
