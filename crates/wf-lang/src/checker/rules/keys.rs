use crate::ast::{FieldRef, JoinMode, MatchClause, WindowMode};

use crate::checker::scope::{self, Scope};
use crate::checker::types::{ValType, compatible};
use crate::checker::{CheckError, Severity};
use crate::schema::BaseType;

pub fn check_session_gap_clause(
    match_clause: &MatchClause,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    if let WindowMode::Session(gap) = match_clause.window_mode
        && gap.is_zero()
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: "session(gap) gap must be > 0".to_string(),
        });
    }
}

pub fn check_match_keys_clause(
    match_clause: &MatchClause,
    joins_list: &[crate::ast::JoinClause],
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    // K1b: at most one simple key may resolve to a snapshot join's right window
    // (join-then-key). Records `(key field, join index)` when one is found.
    let mut join_key: Option<(String, usize)> = None;

    for key in &match_clause.keys {
        match key {
            FieldRef::Simple(field) => {
                // K1: unqualified key must exist in ALL event sources (skip join windows)
                let driver_missing: Vec<(&str, &crate::schema::WindowSchema)> = scope
                    .aliases
                    .iter()
                    .filter(|(alias, _)| !scope.join_windows.contains(*alias))
                    .filter(|(_, schema)| !schema.fields.iter().any(|f| f.name == *field))
                    .map(|(alias, schema)| (*alias, *schema))
                    .collect();
                if driver_missing.is_empty() {
                    // K4: types must be consistent across sources
                    check_key_type_consistency(field, scope, rule_name, errors);
                } else {
                    // K1b: fall back to a snapshot join's right window (join-then-key)
                    match resolve_join_key_source(field, joins_list, scope) {
                        JoinKeySource::Resolved { join_idx } => {
                            // Join-then-key needs a hashable scalar on the right
                            // row (same rule as join index keys: float excluded).
                            if let Some(join) = joins_list.get(join_idx)
                                && let Some(schema) = scope.aliases.get(join.target_window.as_str())
                                && let Some(fd) = schema.fields.iter().find(|f| f.name == *field)
                                && !is_scalar_key_type(&fd.field_type)
                            {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    test: None,
                                    message: format!(
                                        "match key `{}` resolves to join window `{}` but its type \
                                         is not a scalar base type (digit/chars/bool/time/ip/hex; \
                                         float excluded)",
                                        field, join.target_window
                                    ),
                                });
                            }
                            if let Some((prev_field, _)) = &join_key {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    test: None,
                                    message: format!(
                                        "match keys `{}` and `{}` both resolve to join-side fields; \
                                         compound join keys are not supported yet (v1: at most one \
                                         join key per rule)",
                                        prev_field, field
                                    ),
                                });
                            } else {
                                join_key = Some((field.clone(), join_idx));
                            }
                        }
                        JoinKeySource::Ambiguous { windows } => {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "match key `{}` exists on multiple join windows ({}); \
                                     ambiguous join-then-key source",
                                    field,
                                    windows.join(", ")
                                ),
                            });
                        }
                        JoinKeySource::NonSnapshot { windows } => {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "match key `{}` is only available on non-snapshot join window(s) ({}); \
                                     join-then-key requires a snapshot join",
                                    field,
                                    windows.join(", ")
                                ),
                            });
                        }
                        JoinKeySource::NotFound => {
                            for (alias, schema) in driver_missing {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    test: None,
                                    message: format!(
                                        "match key `{}` not found in event source `{}` (window `{}`)",
                                        field, alias, schema.name
                                    ),
                                });
                            }
                        }
                    }
                }
            }
            FieldRef::Qualified(alias, field) => {
                // K2: qualified key
                if !scope.aliases.contains_key(alias.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "match key `{}.{}` references unknown alias `{}`",
                            alias, field, alias
                        ),
                    });
                } else if scope.join_windows.contains(&alias.as_str()) {
                    // K2b: a join-side key must be written unqualified so the
                    // compiler can route it through join-then-key (v1).
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "match key `{}.{}` references join window `{}`; join-side keys must \
                             be unqualified (e.g. `match<{}:10m>`)",
                            alias, field, alias, field
                        ),
                    });
                } else if !scope.alias_has_field(alias, field) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
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
                        test: None,
                        message: format!(
                            "match key `{}[\"{}\"]` references unknown alias `{}`",
                            alias, key, alias
                        ),
                    });
                } else if scope.join_windows.contains(&alias.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "match key `{}[\"{}\"]` references join window `{}`; join-side keys must \
                             be unqualified (e.g. `match<{}:10m>`)",
                            alias, key, alias, key
                        ),
                    });
                } else if !scope.alias_has_field(alias, key) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "match key `{}[\"{}\"]`: field `{}` not found in window",
                            alias, key, key
                        ),
                    });
                }
            }
            FieldRef::Path { .. } => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "nested field path not supported in match key".to_string(),
                });
            }
        }
    }

    // K1c: rule-level constraints once a join key is present.
    if let Some((field, join_idx)) = &join_key {
        if match_clause.key_mapping.is_some() {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "match key `{}` resolves to a join-side field; key mapping \
                     (key_block) is not supported together with a join key yet (v1)",
                    field
                ),
            });
        }
        let driver_alias_count = scope
            .aliases
            .keys()
            .filter(|alias| !scope.join_windows.contains(*alias))
            .count();
        if driver_alias_count > 1 {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "match key `{}` resolves to a join-side field; join-then-key requires a \
                     single event bind (found {} event sources; multi-bind is not supported yet)",
                    field, driver_alias_count
                ),
            });
        }
        if match_clause.keys.len() > 1 {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "match key `{}` resolves to a join-side field; mixed driver/join keys are not \
                     supported yet (v1: exactly one key when using a join key)",
                    field
                ),
            });
        }
        let join = joins_list.get(*join_idx);
        if let Some(join) = join {
            let left_is_driver = join.conditions.iter().any(|c| match &c.left {
                FieldRef::Qualified(alias, _) | FieldRef::Bracketed(alias, _) => {
                    !scope.join_windows.contains(&alias.as_str())
                }
                FieldRef::Simple(name) => !scope.join_windows.iter().any(|a| {
                    scope
                        .aliases
                        .get(a)
                        .is_some_and(|s| s.fields.iter().any(|f| f.name == *name))
                }),
                _ => false,
            });
            if !left_is_driver {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "match key `{}` resolves to a join-side field; the join condition's left \
                         side must reference the driver event (e.g. `b.auction`)",
                        field
                    ),
                });
            }
            if join.conditions.len() != 1 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "match key `{}` resolves to a join-side field; the providing join must \
                         have exactly one condition (v1: single join key)",
                        field
                    ),
                });
            }
            // K1d: join-key index-key soundness. The join index key truncates
            // f64 (`JoinKey::from_value` does `*n as i64` — the same "f64
            // truncation would false-match" hazard the right-side scalar rule
            // guards against). The driver-side condition field (e.g.
            // `b.auction`) is not checked anywhere else, so a float driver
            // field would silently hit truncated rows at runtime; the
            // match-time join path re-checks with `values_equal`, the
            // join-then-key path must too (compiler/checker keep the same
            // invariant).
            if let Some(cond) = join.conditions.first() {
                if matches!(cond.right, FieldRef::Path { .. }) {
                    // compiler's resolve_join_key returns None for a Path
                    // right side (silently degrading to ordinary-key
                    // extraction → every event skips). Keep the asymmetry
                    // explicit: reject at compile time instead.
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "match key `{}` resolves to a join-side field; the join condition's \
                             right side must be a plain field (nested paths unsupported)",
                            field
                        ),
                    });
                }
                let left_ft = field_ref_field_type(&cond.left, scope);
                if let Some(ft) = &left_ft
                    && !is_scalar_key_type(ft)
                {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "match key `{}` resolves to a join-side field; the join condition's \
                             left field type must be scalar (digit/chars/bool/time/ip/hex; float \
                             excluded — f64 truncation would false-match truncated index rows)",
                            field
                        ),
                    });
                }
                if let Some(right_ft) = field_ref_field_type(&cond.right, scope)
                    && let Some(left_ft) = left_ft
                {
                    let lv = scope::field_type_to_val(left_ft);
                    let rv = scope::field_type_to_val(right_ft);
                    if !compatible(&lv, &rv) {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "match key `{}` resolves to a join-side field; join condition \
                                 type mismatch: left {:?} vs right {:?}",
                                field, lv, rv
                            ),
                        });
                    }
                }
            }
        }
    }
}

/// Resolve a field reference's declared type against the rule scope (driver
/// aliases first for unqualified names, then join windows). Returns `None`
/// when the field can't be resolved (missing alias/field) — the join
/// condition checks report existence separately.
fn field_ref_field_type<'a>(
    fr: &'a FieldRef,
    scope: &'a Scope<'_>,
) -> Option<&'a crate::schema::FieldType> {
    match fr {
        FieldRef::Qualified(alias, field) | FieldRef::Bracketed(alias, field) => scope
            .aliases
            .get(alias.as_str())
            .and_then(|s| s.fields.iter().find(|f| &f.name == field))
            .map(|f| &f.field_type),
        FieldRef::Simple(name) => scope
            .aliases
            .iter()
            .filter(|(alias, _)| !scope.join_windows.contains(alias))
            .find_map(|(_, s)| s.fields.iter().find(|f| f.name == *name))
            .map(|f| &f.field_type),
        FieldRef::Path { .. } => None,
    }
}

/// K1b result of resolving a simple key to a snapshot join right window.
pub(super) enum JoinKeySource<'a> {
    /// Exactly one snapshot join's target window provides the key field.
    Resolved { join_idx: usize },
    /// The field exists on multiple snapshot join windows (ambiguous source).
    Ambiguous { windows: Vec<&'a str> },
    /// The field only exists on non-snapshot join windows (asof/anti).
    NonSnapshot { windows: Vec<&'a str> },
    /// The field is absent from every join window.
    NotFound,
}

/// K1b: resolve a simple key absent from the driver events to a snapshot
/// join's right window (join-then-key). Only snapshot joins qualify — asof has
/// window-timing semantics and anti has no row to read the value from.
fn resolve_join_key_source<'a>(
    field: &str,
    joins_list: &'a [crate::ast::JoinClause],
    scope: &Scope<'a>,
) -> JoinKeySource<'a> {
    let mut snapshots: Vec<(usize, &'a str)> = Vec::new();
    let mut non_snapshot: Vec<&'a str> = Vec::new();
    for (idx, join) in joins_list.iter().enumerate() {
        let alias = join.target_window.as_str();
        if !scope.join_windows.contains(&alias) {
            continue;
        }
        let Some(schema) = scope.aliases.get(alias) else {
            continue;
        };
        if !schema.fields.iter().any(|f| f.name == field) {
            continue;
        }
        if join.mode == JoinMode::Snapshot {
            snapshots.push((idx, alias));
        } else {
            non_snapshot.push(alias);
        }
    }
    match snapshots.len() {
        0 => {
            if non_snapshot.is_empty() {
                JoinKeySource::NotFound
            } else {
                JoinKeySource::NonSnapshot {
                    windows: non_snapshot,
                }
            }
        }
        1 => JoinKeySource::Resolved {
            join_idx: snapshots[0].0,
        },
        _ => JoinKeySource::Ambiguous {
            windows: snapshots.into_iter().map(|(_, a)| a).collect(),
        },
    }
}

/// Whether a field type can serve as a window key (scalar base types only —
/// same rule as join index keys; float excluded: f64 truncation would
/// false-match).
pub(super) fn is_scalar_key_type(ft: &crate::schema::FieldType) -> bool {
    matches!(
        ft,
        crate::schema::FieldType::Base(
            BaseType::Digit
                | BaseType::Chars
                | BaseType::Bool
                | BaseType::Time
                | BaseType::Ip
                | BaseType::Hex
        )
    )
}

/// K4: check that a simple key field has the same type across all event sources.
fn check_key_type_consistency(
    field: &str,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let mut found_type: Option<(ValType, String)> = None;
    for (alias, schema) in &scope.aliases {
        if scope.join_windows.contains(alias) {
            continue;
        }
        if let Some(fd) = schema.fields.iter().find(|f| f.name == field) {
            let vt = scope::field_type_to_val(&fd.field_type);
            if let Some((ref prev_type, ref prev_alias)) = found_type {
                if !compatible(prev_type, &vt) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
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

pub fn check_key_mapping_clause(
    match_clause: &MatchClause,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let mapping = match &match_clause.key_mapping {
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
                        test: None,
                        message: format!(
                            "key mapping `{} = {}.{}`: alias `{}` not declared in events",
                            item.logical_name, alias, field, alias
                        ),
                    });
                } else if !scope.alias_has_field(alias, field) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
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
                    test: None,
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
                        test: None,
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
