use crate::ast::{Expr, FieldSelector, Measure, StepBranch, Transform};
use crate::explain::format_expr;
use crate::schema::BaseType;

use super::check_expr::{check_expr_type, check_guard_expr_type};
use super::infer::infer_type;
use super::{ValType, compatible, is_numeric, is_orderable};
use crate::checker::scope::Scope;
use crate::checker::{CheckError, Severity};

/// Type-check a match step branch's pipe chain.
pub(crate) fn check_pipe_chain(
    branch: &StepBranch,
    scope: &Scope<'_>,
    rule_name: &str,
    lets: &[crate::ast::LetDecl],
    errors: &mut Vec<CheckError>,
) {
    let has_field = branch.field.is_some();

    // Determine the field type if there is a field selector
    let field_val_type: Option<ValType> = branch.field.as_ref().and_then(|fs| {
        let field_name = match fs {
            FieldSelector::Dot(n) | FieldSelector::Bracket(n) => n.as_str(),
        };
        scope.get_field_type_for_alias(&branch.source, field_name)
    });

    // Check transforms
    for transform in &branch.pipe.transforms {
        match transform {
            Transform::Distinct => {
                // T3: distinct requires a column projection (field selector)
                if !has_field {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "distinct requires a field selector (column projection), but step source `{}` has none",
                            branch.source
                        ),
                    });
                }
            }
        }
    }

    // Check measure
    match branch.pipe.measure {
        Measure::Count => {
            // T4: count operates on a set level. If there's a field but no distinct, it's an error.
            if has_field && !branch.pipe.transforms.contains(&Transform::Distinct) {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "count operates on sets; use `distinct | count` for column `{}`",
                        field_selector_name(branch.field.as_ref().unwrap())
                    ),
                });
            }
        }
        Measure::Sum | Measure::Avg => {
            // T1: field must be numeric
            if let Some(ref vt) = field_val_type
                && !is_numeric(vt)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() requires a numeric field, `{}` is {:?}",
                        measure_name(branch.pipe.measure),
                        field_selector_name(branch.field.as_ref().unwrap()),
                        vt
                    ),
                });
            }
            if !has_field {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() requires a field selector",
                        measure_name(branch.pipe.measure)
                    ),
                });
            }
        }
        Measure::Min | Measure::Max => {
            // T2: field must be orderable
            if let Some(ref vt) = field_val_type
                && !is_orderable(vt)
            {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() requires an orderable field, `{}` is {:?}",
                        measure_name(branch.pipe.measure),
                        field_selector_name(branch.field.as_ref().unwrap()),
                        vt
                    ),
                });
            }
            if !has_field {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "{}() requires a field selector",
                        measure_name(branch.pipe.measure)
                    ),
                });
            }
        }
    }

    // 阈值先内联规则级常量 `let`（与 events 条件同机制，issue #90）：阈值必须能被
    // 触发判定折叠，`let THRESHOLD = 5` 这类命名常量属于常量，内联后与手写字面量
    // 完全一致；不可折叠的引用保持原样，由下面的常量性检查拒绝。
    let threshold = {
        let mut visiting = Vec::new();
        crate::ast::inline_const_scalar_lets(&branch.pipe.threshold, lets, &mut visiting)
    };

    // Check threshold expression type
    check_expr_type(&threshold, scope, rule_name, errors);

    // F1（warp-fusion#101）：阈值必须是触发判定可求值的编译期常量。判据与
    // wf-cep `check_threshold` 共用 `wf_lang::const_fold`——折叠不出结果时该
    // 分支恒判「不满足」，运行期没有任何信号。
    if let Some(message) = threshold_constant_violation(&threshold) {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message,
        });
    }

    // T5: threshold type must be compatible with measure result type
    if let Some(result_type) = measure_result_type(branch.pipe.measure, &field_val_type)
        && let Some(threshold_type) = infer_type(&threshold, scope)
        && !compatible(&result_type, &threshold_type)
        && !(is_numeric(&result_type) && is_numeric(&threshold_type))
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: format!(
                "threshold type {:?} is not compatible with {}() result type {:?}",
                threshold_type,
                measure_name(branch.pipe.measure),
                result_type
            ),
        });
    }

    // Check guard expression if present
    if let Some(ref guard) = branch.guard {
        check_guard_expr_type(guard, scope, rule_name, errors);
    }
}

fn field_selector_name(fs: &FieldSelector) -> &str {
    match fs {
        FieldSelector::Dot(n) | FieldSelector::Bracket(n) => n.as_str(),
    }
}

/// 阈值常量性检查（warp-fusion#101）：返回违规描述。
///
/// 判据就是「触发判定能否求值」：`wf_lang::const_fold` 折叠不出结果的阈值在
/// `wf-cep` 的 `check_threshold` 里恒判「不满足」，分支永不触发且运行期无信号。
/// 覆盖两类写法：非常量形态（字段引用、规则级 `let` 引用、函数调用——含 L3 集合
/// 函数与 `now*` / `baseline`），以及折叠不出结果的字面量算术（除零 / 模零）。
fn threshold_constant_violation(threshold: &Expr) -> Option<String> {
    if crate::const_fold::is_foldable_threshold(threshold) {
        return None;
    }
    match threshold {
        Expr::FuncCall { name, .. } => Some(format!(
            "{name}() is not allowed in threshold expressions; use it in score/entity/yield instead \
             (thresholds must be compile-time constants)"
        )),
        // 字段引用与规则级 `let` 引用在此同形（都不参与逐事件求值），故不区分措辞。
        _ => Some(format!(
            "threshold `{}` cannot be evaluated by the trigger check; thresholds must be \
             compile-time constants (number / string literal, optionally negated or in \
             parentheses) — this branch would never trigger",
            format_expr(threshold)
        )),
    }
}

fn measure_name(m: Measure) -> &'static str {
    match m {
        Measure::Count => "count",
        Measure::Sum => "sum",
        Measure::Avg => "avg",
        Measure::Min => "min",
        Measure::Max => "max",
    }
}

/// Infer the result type of a measure given its field type.
fn measure_result_type(measure: Measure, field_val_type: &Option<ValType>) -> Option<ValType> {
    match measure {
        Measure::Count => Some(ValType::Base(BaseType::Digit)),
        Measure::Sum => field_val_type.clone(),
        Measure::Avg => Some(ValType::Base(BaseType::Float)),
        Measure::Min | Measure::Max => field_val_type.clone(),
    }
}
