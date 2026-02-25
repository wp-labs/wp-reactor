mod conv_check;
mod joins;
mod keys;
mod limits;
mod scope_build;
mod score_entity;
mod steps;
mod yield_check;
pub(crate) mod yield_version;

use std::collections::HashSet;

use crate::ast::FieldRef;
use crate::schema::WindowSchema;

use super::{CheckError, Severity};

use crate::ast::RuleDecl;

/// System fields that must not appear in yield named arguments.
const SYSTEM_FIELDS: &[&str] = &[
    "rule_name",
    "emit_time",
    "score",
    "entity_type",
    "entity_id",
    "origin",
    "score_contrib",
];

/// Check a single rule declaration against the provided schemas.
pub fn check_rule(rule: &RuleDecl, schemas: &[WindowSchema], errors: &mut Vec<CheckError>) {
    let name = &rule.name;

    // Build scope from events block
    let scope = scope_build::build_scope(rule, schemas, name, errors);

    // Check match keys
    keys::check_match_keys(rule, &scope, name, errors);

    // Check key mapping (K3, K4)
    keys::check_key_mapping(rule, &scope, name, errors);

    // Check match steps (shared labels_seen across on_event and on_close)
    let mut labels_seen = HashSet::new();
    steps::check_match_steps(
        &rule.match_clause.on_event,
        &scope,
        name,
        errors,
        &mut labels_seen,
    );
    if let Some(ref close_block) = rule.match_clause.on_close {
        steps::check_match_steps(&close_block.steps, &scope, name, errors, &mut labels_seen);
    }

    // Check label vs key name collision
    for key in &rule.match_clause.keys {
        let key_name = match key {
            FieldRef::Simple(n) | FieldRef::Qualified(_, n) | FieldRef::Bracketed(_, n) => {
                n.as_str()
            }
            #[allow(unreachable_patterns)]
            _ => continue,
        };
        if labels_seen.contains(key_name) {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(name.to_string()),
                test: None,
                message: format!(
                    "step label `{}` conflicts with match key field of the same name",
                    key_name
                ),
            });
        }
    }

    // Check score expression (T27)
    score_entity::check_score(rule, &scope, errors);

    // Check entity clause (T33)
    score_entity::check_entity(rule, &scope, errors);

    // Check yield clause
    yield_check::check_yield(rule, schemas, &scope, errors);

    // Check joins
    joins::check_joins(rule, schemas, &scope, name, errors);

    // Check limits
    limits::check_limits(rule, name, errors);

    // Check conv (L3: requires fixed window)
    conv_check::check_conv(rule, name, errors);
}
