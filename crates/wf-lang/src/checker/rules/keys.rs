use crate::ast::{Expr, FieldRef, JoinMode, MatchClause, PathSegment, WindowMode};

use crate::checker::scope::{self, Scope};
use crate::checker::types::{ValType, compatible};
use crate::checker::{CheckError, Severity};
use crate::schema::{BaseType, FieldType};

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
    lets: &[crate::ast::LetDecl],
    derived_ok: bool,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    // K1b: at most one simple key may resolve to a snapshot join's right window
    // (join-then-key). Records `(key field, join index)` when one is found.
    let mut join_key: Option<(String, usize)> = None;

    // 派生/嵌套路径 key（issue #83）：v1 仅支持单事件源（driver）规则——派生
    // key 按事件在事件域上求值，多源规则的事件域不唯一。
    let driver_count = scope.aliases.len().saturating_sub(scope.join_windows.len());
    let derived_error = |key_desc: &str, errors: &mut Vec<CheckError>| {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "match key `{key_desc}` 是派生/嵌套路径 key，仅支持单事件源规则（v1）；多事件源规则请用各源共有的顶层字段或 key mapping",
            ),
        });
    };

    for key in &match_clause.keys {
        match key {
            FieldRef::Simple(field) => {
                // 派生 key：`let` 绑定优先于事件源字段（与表达式解析一致）。
                if lets.iter().any(|l| &l.name == field) {
                    if !derived_ok {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "match key `{field}` 引用 let 派生字段，pipeline stage 暂不支持（v1）"
                            ),
                        });
                        continue;
                    }
                    let desc = field.clone();
                    if driver_count != 1 {
                        derived_error(&desc, errors);
                        continue;
                    }
                    // key 派生定义（issue #80）：不再限定纯字段/路径形态——
                    // 任意 let 表达式（coalesce/concat/case/字面量等）只要类型
                    // 可推断为标量 key 类型即可作 match key。checker 在下方
                    // 已保证 let 定义本身通过 check_expr_type（scope_build），
                    // 此处只要求最终派生值为可哈希标量。
                    // key 值类型必须是可哈希标量（float 排除，与现有 key 一致）。
                    // infer 为 None（嵌套路径叶等运行时类型）→ 引擎按运行时值
                    // 判定（结构化叶跳过，见 key.rs path walk），静默放行。
                    match scope.let_types.get(field.as_str()) {
                        Some(vt) if val_type_is_key_scalar(vt) => {}
                        Some(_) => {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "match key `{field}`：let 派生值必须是标量 key 类型（digit/chars/bool/time/ip/hex；float/object/array 除外）"
                                ),
                            });
                        }
                        None => {}
                    }
                    // review 3：派生表达式必须**无状态纯事件**可求值——引擎对触发
                    // 事件逐事件 eval（无窗口/history/baseline/时间上下文）。窗口统计
                    // /查询类函数（first/last/collect_*/stddev/percentile、has、
                    // baseline）混入会静默失效（求值 None → 事件全跳；now* 同理）。
                    if lets
                        .iter()
                        .find(|l| &l.name == field)
                        .is_some_and(|decl| key_expr_has_state_dependent_func(&decl.expr))
                    {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!(
                                "match key `{field}`：let 派生表达式含窗口/状态依赖函数（first/last/collect_set/collect_list/stddev/percentile/has/baseline/now*）；key 按触发事件逐事件求值，仅支持无状态纯事件函数/字段/字面量"
                            ),
                        });
                        continue;
                    }
                    continue;
                }
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
            FieldRef::Path { alias, segments } => {
                // issue #83：多层嵌套路径作为分组 key。v1 范围：单事件源规则、
                // root 字段存在且为结构化（object/array）；更深的段无 schema，
                // 缺失/类型不符由运行期按现有 key 缺失语义跳过。
                let desc = crate::explain::format_expr(&Expr::Field(key.clone()));
                if !derived_ok {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "match key `{desc}`：嵌套路径 key，pipeline stage 暂不支持（v1）"
                        ),
                    });
                    continue;
                }
                if !scope.aliases.contains_key(alias.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("match key `{desc}` references unknown alias `{alias}`"),
                    });
                    continue;
                }
                if scope.join_windows.contains(&alias.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "match key `{desc}` references join window `{alias}`; join-side keys \
                             must be unqualified"
                        ),
                    });
                    continue;
                }
                if driver_count != 1 {
                    derived_error(&desc, errors);
                    continue;
                }
                let Some(PathSegment::Field(root)) = segments.first() else {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "match key `{desc}`：嵌套路径必须以成员名开始（不支持裸索引路径）"
                        ),
                    });
                    continue;
                };
                let root_exists = scope
                    .aliases
                    .get(alias.as_str())
                    .is_some_and(|s| s.fields.iter().any(|f| f.name == *root));
                if !root_exists {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!("match key `{desc}`: field `{root}` not found in window",),
                    });
                    continue;
                }
                let root_is_structured = scope
                    .aliases
                    .get(alias.as_str())
                    .and_then(|s| s.fields.iter().find(|f| f.name == *root))
                    .is_some_and(|fd| {
                        matches!(
                            fd.field_type,
                            FieldType::Object | FieldType::ArrayAny | FieldType::Array(_)
                        )
                    });
                if !root_is_structured {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "match key `{desc}`: field `{root}` is not an object/array; use a flat \
                             top-level key or `let` over a structured field",
                        ),
                    });
                    continue;
                }
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

/// let 派生 key 的值类型必须是可哈希标量（float/object/array 排除）。
pub(super) fn val_type_is_key_scalar(vt: &ValType) -> bool {
    matches!(
        vt,
        ValType::Base(
            BaseType::Digit
                | BaseType::Chars
                | BaseType::Bool
                | BaseType::Time
                | BaseType::Ip
                | BaseType::Hex,
        ) | ValType::Bool
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

/// review 3（issue #80）：派生 key 表达式是否含**窗口/状态依赖**函数。
///
/// key 表达式由引擎在**触发事件**上逐事件求值（`extract_scope_key_mixed` →
/// `eval_expr_ext`，无窗口查找/history/baseline/求值墙钟上下文）。以下函数在
/// 该路径上要么求值为 None（事件按 key 缺失跳过 → 规则永不触发），要么依赖
/// 全局状态产生无意义分组：
///
/// - 窗口统计/历史：first / last / collect_set / collect_list / stddev /
///   percentile（需 instance 历史或窗口收集）；
/// - 窗口事件查询：has()（需 window lookup）；
/// - rolling 基线：baseline()（需跨事件 RollingStats 状态）。
///
/// 递归检查嵌套调用（concat/case/… 参数内部命中同样拒绝）。
fn key_expr_has_state_dependent_func(expr: &Expr) -> bool {
    const STATE_DEPENDENT: &[&str] = &[
        "first",
        "last",
        "collect_set",
        "collect_list",
        "stddev",
        "percentile",
        "has",
        "baseline",
    ];
    match expr {
        Expr::FuncCall { name, args, .. } => {
            STATE_DEPENDENT.contains(&name.as_str())
                || args.iter().any(key_expr_has_state_dependent_func)
        }
        Expr::BinOp { left, right, .. } => {
            key_expr_has_state_dependent_func(left) || key_expr_has_state_dependent_func(right)
        }
        Expr::Neg(inner) | Expr::Not(inner) => key_expr_has_state_dependent_func(inner),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            key_expr_has_state_dependent_func(cond)
                || key_expr_has_state_dependent_func(then_expr)
                || key_expr_has_state_dependent_func(else_expr)
        }
        Expr::Match {
            expr: subject,
            arms,
            default,
        } => {
            key_expr_has_state_dependent_func(subject)
                || arms.iter().any(|arm| {
                    key_expr_has_state_dependent_func(&arm.value)
                        || arm.patterns.iter().any(key_expr_has_state_dependent_func)
                })
                || default
                    .as_deref()
                    .is_some_and(key_expr_has_state_dependent_func)
        }
        Expr::Object(items) => items
            .iter()
            .any(|it| key_expr_has_state_dependent_func(&it.value)),
        Expr::Array(items) => items.iter().any(key_expr_has_state_dependent_func),
        Expr::InList {
            expr: target,
            list,
            negated: _,
        } => {
            key_expr_has_state_dependent_func(target)
                || list.iter().any(key_expr_has_state_dependent_func)
        }
        // 叶子与其余变体：无函数调用。
        _ => false,
    }
}
