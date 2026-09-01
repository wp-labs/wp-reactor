use std::collections::HashSet;

use crate::ast::{BoundVal, Expr, FieldRef, JoinMode, ReduceMeasure};
use crate::schema::{FieldType, WindowSchema};

use crate::checker::scope::Scope;
use crate::checker::types::check_expr_type;
use crate::checker::{CheckError, Severity};

pub fn check_joins_list(
    joins: &[crate::ast::JoinClause],
    schemas: &[WindowSchema],
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    // 同一规则内 `as label` 必须唯一（多个 join 同标签会互相覆盖注入）
    let mut seen_labels: HashSet<String> = HashSet::new();
    for join in joins {
        // asof `within DUR` 与 interval `within [...]` 互斥（同一时间谓词不能两处声明）
        if matches!(join.mode, JoinMode::Asof { within: Some(_) }) && join.within.is_some() {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "join `{}` 同时使用 `asof within <dur>` 与 `within [...]`——同一时间谓词只能声明一次",
                    join.target_window
                ),
            });
        }

        // Target window must exist in schemas
        let target = schemas.iter().find(|s| s.name == join.target_window);
        match target {
            None => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "join target window `{}` does not exist in schemas",
                        join.target_window
                    ),
                });
            }
            Some(target_schema) => {
                // Validate conditions
                for cond in &join.conditions {
                    // Left side must resolve in scope; nested paths are not
                    // supported in join conditions (runtime extracts flat keys).
                    if matches!(cond.left, FieldRef::Path { .. }) {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: "nested field path not supported in join condition"
                                .to_string(),
                        });
                    } else if let Err(msg) = scope.resolve_field_ref(&cond.left) {
                        errors.push(CheckError {
                            severity: Severity::Error,
                            rule: Some(rule_name.to_string()),
                            test: None,
                            message: format!("join condition left side: {}", msg),
                        });
                    }

                    // Right side must be qualified with target window name
                    match &cond.right {
                        FieldRef::Path { .. } => {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: "nested field path not supported in join condition"
                                    .to_string(),
                            });
                        }
                        FieldRef::Qualified(qualifier, field) => {
                            if qualifier != &join.target_window {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    test: None,
                                    message: format!(
                                        "join condition right side `{}.{}` must be qualified with target window `{}`",
                                        qualifier, field, join.target_window
                                    ),
                                });
                            } else if let Some(field_def) =
                                target_schema.fields.iter().find(|f| f.name == *field)
                            {
                                // Hash-join index requires a scalar base-type key
                                // (object/array values are not reliably hashable).
                                // Float is excluded: JoinKey::Int truncates f64,
                                // so 42.5 and 42.4 would collide (false match).
                                let scalar_ok = matches!(
                                    field_def.field_type,
                                    FieldType::Base(
                                        crate::schema::BaseType::Digit
                                            | crate::schema::BaseType::Chars
                                            | crate::schema::BaseType::Bool
                                            | crate::schema::BaseType::Time
                                            | crate::schema::BaseType::Ip
                                            | crate::schema::BaseType::Hex
                                    )
                                );
                                if !scalar_ok {
                                    errors.push(CheckError {
                                        severity: Severity::Error,
                                        rule: Some(rule_name.to_string()),
                                        test: None,
                                        message: format!(
                                            "join key `{}.{}` must be a scalar base type \
                                             (digit/chars/bool/time/ip/hex; float excluded — \
                                             f64 truncation would false-match); got a structured \
                                             type (object/array) or float",
                                            qualifier, field
                                        ),
                                    });
                                }
                            } else {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    test: None,
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
                                test: None,
                                message: format!(
                                    "join condition right side must be qualified with window name (e.g. `{}.field`)",
                                    join.target_window
                                ),
                            });
                        }
                    }
                }

                // P4：provider/静态窗口（side input，无 stream/time/over）仅支持
                // snapshot（及缺省 inner）join——anti/asof/interval/reduce/deferred
                // 对无时序静态表无意义（设计 §7/§8 P4；`StaticWindowSchema::to_flow_schema`）。
                if is_static_window(target_schema) {
                    check_static_window_join(join, rule_name, errors);
                } else {
                    // T49: asof mode requires time field on right table
                    if let JoinMode::Asof { within } = &join.mode {
                        if target_schema.time_field.is_none() {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
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
                                test: None,
                                message: format!(
                                    "join `{}` asof within must be > 0",
                                    join.target_window
                                ),
                            });
                        }
                    }

                    check_within(join, target_schema, scope, rule_name, errors);
                    check_emit_at(join, scope, rule_name, errors);
                    check_reduce(join, target_schema, rule_name, errors);
                }
            }
        }

        // `reduce ... as label` 与事件/窗口别名冲突 + 同规则内标签唯一
        if let Some(label) = join.reduce.as_ref().and_then(|r| r.label.as_ref()) {
            if scope.aliases.contains_key(label.as_str()) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "reduce label `{}` conflicts with an event/window alias",
                        label
                    ),
                });
            }
            if !seen_labels.insert(label.clone()) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "reduce label `{}` is duplicated across joins in rule `{}`——`as label` 必须唯一（运行时同一标签会互相覆盖）",
                        label, rule_name
                    ),
                });
            }
        }
    }
}

/// 是否 provider/静态窗口（side input）：无 stream、无 time 字段、over=0——
/// 即 `StaticWindowSchema::to_flow_schema()` 的投影（schema.rs）。注意输出窗口
/// （yield-only，如 nexmark_alerts）也是无 stream 窗口，但其 over 通常非 0 或
/// 无 join 目标场景；此处按「无 stream + 无 time + over=0」判定静态侧输入。
fn is_static_window(ws: &WindowSchema) -> bool {
    ws.streams.is_empty() && ws.time_field.is_none() && ws.over.is_zero()
}

/// provider/静态窗口（side input）join 限制：v1 支持 snapshot、缺省 inner 与
/// **anti**。anti 是纯键存在性否定（`join_lookup` → 有匹配丢、无匹配留），不依赖
/// 时间——静态表（无 time/over）上语义清晰（如白名单排除，NEXMark Q21 形状），
/// provider `join_lookup` 已有 O(1) 行索引支撑（window_lookup.rs）。
/// 仍拒绝 asof/interval（within）/reduce/deferred：asof/interval 需要时间列，
/// reduce/deferred 需要窗口生命周期（设计 §7 side input / §8 P4）。
fn check_static_window_join(
    join: &crate::ast::JoinClause,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let what = format!(
        "provider/静态窗口 `{}`（side input）join",
        join.target_window
    );
    if !matches!(
        join.mode,
        JoinMode::Snapshot | JoinMode::Inner | JoinMode::Anti
    ) {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!("{what}；`{:?}` 模式对无时序静态表无意义", join.mode),
        });
    }
    if join.within.is_some() {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!("{what}；`within` interval 需要右窗 time 字段，静态表没有"),
        });
    }
    if join.emit_at.is_some() {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!("{what}；`emit at` deferred 触发需要窗口生命周期，静态表没有"),
        });
    }
    if join.reduce.is_some() {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!("{what}；`reduce` 归约对静态表 v1 不支持"),
        });
    }
}

/// `within` 区间校验：
/// - lo/hi 必须同为相对时长或同为绝对时间表达式；
/// - 常量界：右窗 `over ≥ 跨度`（设计 D3）；lo ≤ hi；
/// - 行内界（表达式）：必须能解析（左行字段/函数）；
/// - interval 需要右窗 time field。
fn check_within(
    join: &crate::ast::JoinClause,
    target_schema: &WindowSchema,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let Some(wspec) = &join.within else {
        return;
    };

    if target_schema.time_field.is_none() {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "join `{}` uses `within` interval but target window has no time field",
                join.target_window
            ),
        });
    }

    // 1) lo/hi 类型一致
    match (&wspec.lo.val, &wspec.hi.val) {
        (BoundVal::Dur { .. }, BoundVal::Expr(_)) | (BoundVal::Expr(_), BoundVal::Dur { .. }) => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "join `{}` within 下界/上界必须同为相对时长或同为绝对时间表达式（如 `[a.dateTime, a.expires]` 或 `[-10s, 0s]`）",
                    join.target_window
                ),
            });
        }
        _ => {}
    }

    // 2) 常量界：over ≥ 跨度 + lo ≤ hi（设计 D3）
    if let (BoundVal::Dur { .. }, BoundVal::Dur { .. }) = (&wspec.lo.val, &wspec.hi.val) {
        let lo_ns = signed_bound_nanos(&wspec.lo);
        let hi_ns = signed_bound_nanos(&wspec.hi);
        if hi_ns < lo_ns {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "join `{}` within 下界必须 ≤ 上界（lo={}ns, hi={}ns）",
                    join.target_window, lo_ns, hi_ns
                ),
            });
        } else {
            let span = hi_ns - lo_ns;
            let over = target_schema.over.as_nanos() as i128;
            if over < span {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "join `{}` within 区间跨度 {} 超过右窗 over {}（设计 D3：到期查找时行必须仍在窗内）",
                        join.target_window,
                        span,
                        target_schema.over.as_nanos()
                    ),
                });
            }
        }
    }

    // 3) 行内界表达式解析（左行字段/函数）
    // 行内界（表达式）：必须能解析（左行字段/函数），且只能引用驱动事件字段
    for bound in [&wspec.lo, &wspec.hi] {
        if let BoundVal::Expr(e) = &bound.val {
            check_expr_type(e, scope, rule_name, errors);
            check_expr_driver_aliases_only(e, join, "within 界表达式", scope, rule_name, errors);
        }
    }
}

/// `emit at <expr>`（deferred 标记）校验：
/// - 必须搭配 `within`（deferred 触发依赖区间上界）；
/// - within 上界必须是绝对时间表达式（相对时长无法静态保证触发时行已到齐）；
/// - 同为字段时 `emit_at ≥ within 上界`（Q9：`emit at a.expires` == 上界）。
fn check_emit_at(
    join: &crate::ast::JoinClause,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let Some(emit_at) = &join.emit_at else {
        return;
    };
    check_expr_type(emit_at, scope, rule_name, errors);
    // 触发点是左行绝对时间（设计 §2.2）——不能引用 join 右窗
    check_expr_driver_aliases_only(emit_at, join, "`emit at` 表达式", scope, rule_name, errors);

    let Some(wspec) = &join.within else {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "join `{}` `emit at` 需要 `within` 区间（deferred 触发依赖区间上界）",
                join.target_window
            ),
        });
        return;
    };

    if let BoundVal::Dur { .. } = &wspec.hi.val {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "join `{}` `emit at` 要求 within 上界为绝对时间表达式（字段/函数）；相对时长上界无法保证触发时行已到齐",
                join.target_window
            ),
        });
        return;
    }

    // 上界与触发点同为字段时，必须指向同一字段（emit_at ≥ 上界）。
    // 裸名 vs 限定名按叶子名比较（裸名经 resolve 解析到唯一匹配字段）。
    if let BoundVal::Expr(Expr::Field(hi_field)) = &wspec.hi.val
        && let Expr::Field(emit_field) = emit_at
        && !same_leaf_field(hi_field, emit_field)
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "join `{}` `emit at` 必须 ≥ within 上界（行内界下应为同一字段，如 `within [a.dateTime, a.expires] ... emit at a.expires`）",
                join.target_window
            ),
        });
    }
}

/// 两个字段引用是否指向同一字段：叶子名相同，且（两个都限定/括号时）别名一致。
fn same_leaf_field(a: &FieldRef, b: &FieldRef) -> bool {
    use FieldRef::*;
    fn leaf(f: &FieldRef) -> Option<&str> {
        match f {
            Simple(n) | Qualified(_, n) | Bracketed(_, n) => Some(n.as_str()),
            Path { .. } => None,
        }
    }
    let (Some(fa), Some(fb)) = (leaf(a), leaf(b)) else {
        return false;
    };
    if fa != fb {
        return false;
    }
    match (a, b) {
        (Qualified(alias_a, _), Qualified(alias_b, _))
        | (Bracketed(alias_a, _), Bracketed(alias_b, _)) => alias_a == alias_b,
        _ => true,
    }
}

/// 表达式只能引用驱动事件字段（左行），不能引用 join 右窗——
/// within 界与 emit at 触发点均相对左事件（设计 §3：界 = 相对时长或左行字段引用）。
fn check_expr_driver_aliases_only(
    expr: &Expr,
    join: &crate::ast::JoinClause,
    what: &str,
    scope: &Scope<'_>,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let mut aliases = Vec::new();
    collect_qualified_aliases(expr, &mut aliases);
    for alias in aliases {
        if scope.join_windows.contains(&alias) {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "join `{}` {what} 只能引用驱动事件字段（左行），不能引用 join 右窗 `{}`",
                    join.target_window, alias
                ),
            });
        }
    }
}

/// 收集表达式中的限定字段引用别名（`a.f` / `a["f"]` / 路径根 `a.x.y`）。
fn collect_qualified_aliases<'a>(expr: &'a Expr, out: &mut Vec<&'a str>) {
    match expr {
        Expr::Field(FieldRef::Qualified(alias, _) | FieldRef::Bracketed(alias, _)) => {
            out.push(alias.as_str());
        }
        Expr::Field(FieldRef::Path { alias, .. }) => out.push(alias.as_str()),
        Expr::BinOp { left, right, .. } => {
            collect_qualified_aliases(left, out);
            collect_qualified_aliases(right, out);
        }
        Expr::Neg(inner) => collect_qualified_aliases(inner, out),
        Expr::Not(inner) => collect_qualified_aliases(inner, out),
        Expr::FuncCall { args, .. } => {
            for a in args {
                collect_qualified_aliases(a, out);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_qualified_aliases(expr, out);
            for i in list {
                collect_qualified_aliases(i, out);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            collect_qualified_aliases(cond, out);
            collect_qualified_aliases(then_expr, out);
            collect_qualified_aliases(else_expr, out);
        }
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            collect_qualified_aliases(expr, out);
            for arm in arms {
                for pattern in &arm.patterns {
                    collect_qualified_aliases(pattern, out);
                }
                collect_qualified_aliases(&arm.value, out);
            }
            if let Some(d) = default {
                collect_qualified_aliases(d, out);
            }
        }
        Expr::Array(items) => {
            for i in items {
                collect_qualified_aliases(i, out);
            }
        }
        Expr::Object(items) => {
            for item in items {
                collect_qualified_aliases(&item.value, out);
            }
        }
        _ => {}
    }
}

/// `reduce` 校验：度量/tie 字段必须在右窗 schema；top N ≥ 1。
fn check_reduce(
    join: &crate::ast::JoinClause,
    target_schema: &WindowSchema,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    let Some(rc) = &join.reduce else {
        return;
    };

    match &rc.measure {
        ReduceMeasure::Maxrow { field, tie } | ReduceMeasure::Minrow { field, tie } => {
            check_reduce_field(join, target_schema, field, "measure", rule_name, errors);
            if let Some(t) = tie {
                check_reduce_field(join, target_schema, &t.field, "tie", rule_name, errors);
            }
        }
        ReduceMeasure::Last { field } => {
            check_reduce_field(join, target_schema, field, "measure", rule_name, errors);
        }
        ReduceMeasure::Top { n, field } => {
            if *n == 0 {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!("join `{}` reduce top(N) N must be ≥ 1", join.target_window),
                });
            }
            check_reduce_field(join, target_schema, field, "measure", rule_name, errors);
        }
    }

    // 度量字段须为数值/时间（maxrow/minrow/top 按字段序选行）——非标量类型无法排序
    let measure_field = match &rc.measure {
        ReduceMeasure::Maxrow { field, .. }
        | ReduceMeasure::Minrow { field, .. }
        | ReduceMeasure::Top { field, .. } => Some(field),
        ReduceMeasure::Last { .. } => None,
    };
    if let Some(FieldRef::Simple(f) | FieldRef::Qualified(_, f) | FieldRef::Bracketed(_, f)) =
        measure_field
        && let Some(fd) = target_schema.fields.iter().find(|fd| fd.name == *f)
        && matches!(
            fd.field_type,
            FieldType::Object | FieldType::ArrayAny | FieldType::Array(_)
        )
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "join `{}` reduce measure field `{}` must be scalar (structured type cannot be ordered)",
                join.target_window, f
            ),
        });
    }
}

/// 单个 reduce 字段（度量/tie）的右窗存在性与限定词校验。
fn check_reduce_field(
    join: &crate::ast::JoinClause,
    target_schema: &WindowSchema,
    fr: &FieldRef,
    what: &str,
    rule_name: &str,
    errors: &mut Vec<CheckError>,
) {
    match fr {
        FieldRef::Simple(f) => {
            if !target_schema.fields.iter().any(|fd| fd.name == *f) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "join `{}` reduce {what} field `{}` not found in window `{}`",
                        join.target_window, f, join.target_window
                    ),
                });
            }
        }
        FieldRef::Qualified(q, f) | FieldRef::Bracketed(q, f) => {
            if q != &join.target_window {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "join `{}` reduce {what} field `{}.{}` must be qualified with target window `{}`",
                        join.target_window, q, f, join.target_window
                    ),
                });
            } else if !target_schema.fields.iter().any(|fd| fd.name == *f) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "join `{}` reduce {what} field `{}.{}` not found in window `{}`",
                        join.target_window, q, f, join.target_window
                    ),
                });
            }
        }
        FieldRef::Path { .. } => {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "join `{}` reduce {what} does not support nested field path",
                    join.target_window
                ),
            });
        }
    }
}

/// 常量界的带符号纳秒（Expr 界不参与常量跨度计算）。
fn signed_bound_nanos(b: &crate::ast::Bound) -> i128 {
    match &b.val {
        BoundVal::Dur { dur, neg } => {
            let ns = dur.as_nanos() as i128;
            if *neg { -ns } else { ns }
        }
        BoundVal::Expr(_) => 0,
    }
}
