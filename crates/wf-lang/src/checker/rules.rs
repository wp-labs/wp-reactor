use std::collections::HashSet;

use crate::ast::{FieldRef, FieldSelector, MatchStep, RuleDecl};
use crate::schema::WindowSchema;

use super::scope::Scope;
use super::types::{
    ValType, check_expr_type, check_pipe_chain, compatible, infer_type, is_numeric,
    is_scalar_identity,
};
use super::{CheckError, Severity};

/// System fields that must not appear in yield named arguments.
const SYSTEM_FIELDS: &[&str] = &[
    "rule_name",
    "emit_time",
    "score",
    "entity_type",
    "entity_id",
    "close_reason",
    "score_contrib",
];

/// Check a single rule declaration against the provided schemas.
pub fn check_rule(rule: &RuleDecl, schemas: &[WindowSchema], errors: &mut Vec<CheckError>) {
    let name = &rule.name;

    // Build scope from events block
    let scope = build_scope(rule, schemas, name, errors);

    // Check match keys
    check_match_keys(rule, &scope, name, errors);

    // Check key mapping (K3, K4)
    check_key_mapping(rule, &scope, name, errors);

    // Check match steps (shared labels_seen across on_event and on_close)
    let mut labels_seen = HashSet::new();
    check_match_steps(
        &rule.match_clause.on_event,
        &scope,
        name,
        errors,
        &mut labels_seen,
    );
    if let Some(ref close_steps) = rule.match_clause.on_close {
        check_match_steps(close_steps, &scope, name, errors, &mut labels_seen);
    }

    // Check label vs key name collision: a label with the same name as a match
    // key field would shadow the key in the eval context, corrupting score/entity
    // expressions that reference the key.
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
                contract: None,
                message: format!(
                    "step label `{}` conflicts with match key field of the same name",
                    key_name
                ),
            });
        }
    }

    // Check score expression (T27)
    check_score(rule, &scope, errors);

    // Check entity clause (T33)
    check_entity(rule, &scope, errors);

    // Check yield clause
    check_yield(rule, schemas, &scope, errors);

    // Check joins
    check_joins(rule, schemas, &scope, name, errors);

    // Check limits
    check_limits(rule, name, errors);
}

// ---------------------------------------------------------------------------
// Scope construction + events validation
// ---------------------------------------------------------------------------

fn build_scope<'a>(
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
                contract: None,
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
                    contract: None,
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

    scope
}

// ---------------------------------------------------------------------------
// Match key validation (K1-K5)
// ---------------------------------------------------------------------------

fn check_match_keys(
    rule: &RuleDecl,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    for key in &rule.match_clause.keys {
        match key {
            FieldRef::Simple(field) => {
                // K1: unqualified key must exist in ALL event sources
                for (alias, schema) in &scope.aliases {
                    if !schema.fields.iter().any(|f| f.name == *field) {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "match key `{}` not found in event source `{}` (window `{}`)",
                                field, alias, schema.name
                            ),
                        });
                    }
                }
                // K4: types must be consistent across sources
                check_key_type_consistency(field, scope, rule_name, errors);
            }
            FieldRef::Qualified(alias, field) => {
                // K2: qualified key — alias must exist and field must be in its window
                if !scope.aliases.contains_key(alias.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "match key `{}.{}` references unknown alias `{}`",
                            alias, field, alias
                        ),
                    });
                } else if !scope.alias_has_field(alias, field) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "match key `{}.{}`: field `{}` not found in window",
                            alias, field, field
                        ),
                    });
                }
            }
            FieldRef::Bracketed(alias, key) => {
                if !scope.aliases.contains_key(alias.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "match key `{}[\"{}\"]` references unknown alias `{}`",
                            alias, key, alias
                        ),
                    });
                } else if !scope.alias_has_field(alias, key) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "match key `{}[\"{}\"]`: field `{}` not found in window",
                            alias, key, key
                        ),
                    });
                }
            }
        }
    }
}

/// K4: check that a simple key field has the same type across all event sources.
fn check_key_type_consistency(
    field: &str,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let mut found_type: Option<(ValType, String)> = None; // (type, alias)
    for (alias, schema) in &scope.aliases {
        if let Some(fd) = schema.fields.iter().find(|f| f.name == field) {
            let vt = super::scope::field_type_to_val(&fd.field_type);
            if let Some((ref prev_type, ref prev_alias)) = found_type {
                if !compatible(prev_type, &vt) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "match key `{}` type mismatch: {:?} in `{}` vs {:?} in `{}`",
                            field, prev_type, prev_alias, vt, alias
                        ),
                    });
                }
            } else {
                found_type = Some((vt, alias.to_string()));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Match steps validation
// ---------------------------------------------------------------------------

fn check_match_steps<'a>(
    steps: &'a [MatchStep],
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
    labels_seen: &mut HashSet<&'a str>,
) {
    for step in steps {
        for branch in &step.branches {
            // R5: source must be a declared alias
            if !scope.aliases.contains_key(branch.source.as_str()) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "match step source `{}` is not a declared event alias",
                        branch.source
                    ),
                });
            }

            // R1: label uniqueness within this match block
            if let Some(ref label) = branch.label
                && !labels_seen.insert(label.as_str())
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!("duplicate step label `{}`", label),
                });
            }

            // Validate field selector resolves against source's window
            if let Some(ref fs) = branch.field {
                let field_name = match fs {
                    FieldSelector::Dot(n) | FieldSelector::Bracket(n) => n.as_str(),
                };
                if scope.aliases.contains_key(branch.source.as_str())
                    && !scope.alias_has_field(&branch.source, field_name)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "field `{}` not found in source `{}`'s window",
                            field_name, branch.source
                        ),
                    });
                }
            }

            // Type-check the pipe chain
            check_pipe_chain(branch, scope, rule_name, errors);
        }
    }
}

// ---------------------------------------------------------------------------
// Score check (T27)
// ---------------------------------------------------------------------------

fn check_score(rule: &RuleDecl, scope: &Scope<'_>, errors: &mut Vec<CheckError>) {
    let name = &rule.name;
    check_expr_type(&rule.score.expr, scope, name, errors);

    if let Some(t) = infer_type(&rule.score.expr, scope)
        && !is_numeric(&t)
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(name.to_string()),
            contract: None,
            message: format!("score expression must be numeric, got {:?}", t),
        });
    }
}

// ---------------------------------------------------------------------------
// Entity check (T33)
// ---------------------------------------------------------------------------

fn check_entity(rule: &RuleDecl, scope: &Scope<'_>, errors: &mut Vec<CheckError>) {
    let name = &rule.name;
    check_expr_type(&rule.entity.id_expr, scope, name, errors);

    if let Some(t) = infer_type(&rule.entity.id_expr, scope)
        && !is_scalar_identity(&t)
    {
        errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(name.to_string()),
                contract: None,
                message: format!(
                    "entity id expression must be a scalar identity type (chars/ip/hex/digit), got {:?}",
                    t
                ),
            });
    }
}

// ---------------------------------------------------------------------------
// Yield checks (Y1, Y2, Y3, Y8, T10, T36)
// ---------------------------------------------------------------------------

fn check_yield(
    rule: &RuleDecl,
    schemas: &[WindowSchema],
    scope: &Scope<'_>,
    errors: &mut Vec<CheckError>,
) {
    let name = &rule.name;
    let yc = &rule.yield_clause;

    // Y1: target window must exist
    let target_schema = schemas.iter().find(|s| s.name == yc.target);

    // T51: yield version must match meta.contract_version
    if let Some(yield_ver) = yc.version {
        let meta_ver = rule.meta.as_ref().and_then(|m| {
            m.entries
                .iter()
                .find(|e| e.key == "contract_version")
                .and_then(|e| e.value.parse::<u32>().ok())
        });
        match meta_ver {
            Some(mv) if mv == yield_ver => {} // OK
            Some(mv) => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(name.to_string()),
                    contract: None,
                    message: format!(
                        "yield version @v{} does not match meta contract_version = {}",
                        yield_ver, mv
                    ),
                });
            }
            None => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(name.to_string()),
                    contract: None,
                    message: format!(
                        "yield specifies @v{} but no contract_version in meta block",
                        yield_ver
                    ),
                });
            }
        }
    }

    match target_schema {
        None => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(name.to_string()),
                contract: None,
                message: format!("yield target window `{}` does not exist", yc.target),
            }); // Can't check further without schema
        }
        Some(ws) => {
            // Y1: target window must be an output window (stream is empty)
            if !ws.streams.is_empty() {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(name.to_string()),
                    contract: None,
                    message: format!(
                        "yield target `{}` has stream subscriptions; it must be an output-only window",
                        yc.target
                    ),
                });
            }

            for arg in &yc.args {
                // T36/Y8: no system fields in yield arguments
                if SYSTEM_FIELDS.contains(&arg.name.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(name.to_string()),
                        contract: None,
                        message: format!(
                            "yield argument `{}` is a system field and cannot be manually assigned",
                            arg.name
                        ),
                    });
                    continue;
                }

                // Y2/Y3: argument name must be a field in the target window
                let target_field = ws.fields.iter().find(|f| f.name == arg.name);
                match target_field {
                    None => {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(name.to_string()),
                            contract: None,
                            message: format!(
                                "yield argument `{}` is not a field in target window `{}`",
                                arg.name, yc.target
                            ),
                        });
                    }
                    Some(fd) => {
                        // T10: type must match
                        check_expr_type(&arg.value, scope, name, errors);
                        if let Some(val_type) = infer_type(&arg.value, scope) {
                            let expected = super::scope::field_type_to_val(&fd.field_type);
                            if !compatible(&expected, &val_type) {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(name.to_string()),
                                    contract: None,
                                    message: format!(
                                        "yield argument `{}` type mismatch: expected {:?}, got {:?}",
                                        arg.name, expected, val_type
                                    ),
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Key mapping validation (K3, K4)
// ---------------------------------------------------------------------------

fn check_key_mapping(
    rule: &RuleDecl,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let mapping = match &rule.match_clause.key_mapping {
        Some(m) => m,
        None => return,
    };

    // K4: source field alias must exist in events, field must exist
    for item in mapping {
        match &item.source_field {
            FieldRef::Qualified(alias, field) => {
                if !scope.aliases.contains_key(alias.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "key mapping `{} = {}.{}`: alias `{}` not declared in events",
                            item.logical_name, alias, field, alias
                        ),
                    });
                } else if !scope.alias_has_field(alias, field) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "key mapping `{} = {}.{}`: field `{}` not found in window",
                            item.logical_name, alias, field, field
                        ),
                    });
                }
            }
            _ => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "key mapping `{}`: source field must be qualified (alias.field)",
                        item.logical_name
                    ),
                });
            }
        }
    }

    // K4: check type consistency for same logical key name across sources
    let mut logical_types: std::collections::HashMap<&str, (ValType, String)> =
        std::collections::HashMap::new();
    for item in mapping {
        if let FieldRef::Qualified(alias, field) = &item.source_field
            && scope.aliases.contains_key(alias.as_str())
            && let Some(vt) = scope.get_field_type_for_alias(alias, field)
        {
            if let Some((prev_type, prev_source)) = logical_types.get(item.logical_name.as_str()) {
                if !compatible(prev_type, &vt) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "key mapping `{}` type mismatch: {:?} (from {}) vs {:?} (from {}.{})",
                            item.logical_name, prev_type, prev_source, vt, alias, field
                        ),
                    });
                }
            } else {
                logical_types.insert(&item.logical_name, (vt, format!("{}.{}", alias, field)));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Join validation
// ---------------------------------------------------------------------------

fn check_joins(
    rule: &RuleDecl,
    schemas: &[WindowSchema],
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    for join in &rule.joins {
        // Target window must exist in schemas
        let target = schemas.iter().find(|s| s.name == join.target_window);
        match target {
            None => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    contract: None,
                    message: format!(
                        "join target window `{}` does not exist in schemas",
                        join.target_window
                    ),
                });
            }
            Some(target_schema) => {
                // Validate conditions
                for cond in &join.conditions {
                    // Left side must resolve in scope
                    if let Err(msg) = scope.resolve_field_ref(&cond.left) {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!("join condition left side: {}", msg),
                        });
                    }

                    // Right side must be qualified with target window name
                    match &cond.right {
                        FieldRef::Qualified(qualifier, field) => {
                            if qualifier != &join.target_window {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    contract: None,
                                    message: format!(
                                        "join condition right side `{}.{}` must be qualified with target window `{}`",
                                        qualifier, field, join.target_window
                                    ),
                                });
                            } else if !target_schema.fields.iter().any(|f| f.name == *field) {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    contract: None,
                                    message: format!(
                                        "join condition: field `{}` not found in window `{}`",
                                        field, join.target_window
                                    ),
                                });
                            }
                        }
                        _ => {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                contract: None,
                                message: format!(
                                    "join condition right side must be qualified with window name (e.g. `{}.field`)",
                                    join.target_window
                                ),
                            });
                        }
                    }
                }

                // T49: asof mode requires time field on right table
                if let crate::ast::JoinMode::Asof { within } = &join.mode {
                    if target_schema.time_field.is_none() {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "join `{}` uses asof mode but target window has no time field",
                                join.target_window
                            ),
                        });
                    }
                    if let Some(dur) = within
                        && dur.is_zero()
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "join `{}` asof within must be > 0",
                                join.target_window
                            ),
                        });
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Limits validation
// ---------------------------------------------------------------------------

const VALID_LIMIT_KEYS: &[&str] = &["max_state", "max_cardinality", "max_emit_rate", "on_exceed"];

const VALID_ON_EXCEED: &[&str] = &["throttle", "drop_oldest", "fail_rule"];

fn check_limits(rule: &RuleDecl, rule_name: &str, errors: &mut Vec<CheckError>) {
    let limits = match &rule.limits {
        Some(l) => l,
        None => return,
    };

    for item in &limits.items {
        if !VALID_LIMIT_KEYS.contains(&item.key.as_str()) {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                contract: None,
                message: format!(
                    "unknown limits key `{}`; valid keys are: {}",
                    item.key,
                    VALID_LIMIT_KEYS.join(", ")
                ),
            });
            continue;
        }

        match item.key.as_str() {
            "on_exceed" => {
                if !VALID_ON_EXCEED.contains(&item.value.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "on_exceed value `{}` invalid; valid values are: {}",
                            item.value,
                            VALID_ON_EXCEED.join(", ")
                        ),
                    });
                }
            }
            "max_cardinality" => {
                if item.value.parse::<usize>().is_err() {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "max_cardinality value `{}` must be a positive integer",
                            item.value
                        ),
                    });
                }
            }
            "max_emit_rate" => {
                if !item.value.contains('/') {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "max_emit_rate value `{}` must be in format count/unit (e.g. \"1000/min\")",
                            item.value
                        ),
                    });
                } else {
                    let parts: Vec<&str> = item.value.splitn(2, '/').collect();
                    if parts.len() == 2 {
                        if parts[0].trim().parse::<u64>().is_err() {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                contract: None,
                                message: format!(
                                    "max_emit_rate count `{}` is not a valid integer",
                                    parts[0].trim()
                                ),
                            });
                        }
                        let valid_units = ["s", "sec", "m", "min", "h", "hr", "hour", "d", "day"];
                        if !valid_units.contains(&parts[1].trim()) {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                contract: None,
                                message: format!(
                                    "max_emit_rate unit `{}` invalid; valid units are: s, sec, m, min, h, hr, hour, d, day",
                                    parts[1].trim()
                                ),
                            });
                        }
                    }
                }
            }
            "max_state" => {
                let s = item.value.to_uppercase();
                if !(s.ends_with("MB") || s.ends_with("GB") || s.ends_with("KB")) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        contract: None,
                        message: format!(
                            "max_state value `{}` must end with KB, MB, or GB (e.g. \"256MB\")",
                            item.value
                        ),
                    });
                } else {
                    // Validate numeric prefix (suffix is always 2 chars: GB, MB, etc.)
                    let num_str = &s[..s.len() - 2];
                    if num_str.trim().parse::<usize>().is_err() {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            contract: None,
                            message: format!(
                                "max_state numeric prefix `{}` in `{}` is not a valid positive integer",
                                num_str.trim(),
                                item.value
                            ),
                        });
                    }
                }
            }
            _ => {}
        }
    }
}
