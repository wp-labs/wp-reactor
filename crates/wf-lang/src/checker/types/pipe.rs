use crate::ast::{FieldSelector, Measure, StepBranch, Transform};
use crate::schema::BaseType;

use super::check_expr::check_expr_type;
use super::infer::infer_type;
use super::{ValType, compatible, is_numeric, is_orderable};
use crate::checker::scope::Scope;
use crate::checker::{CheckError, Severity};

/// Type-check a match step branch's pipe chain.
pub fn check_pipe_chain(
    branch: &StepBranch,
    scope: &Scope<'_>,
    rule_name: &str,
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
                        contract: None,
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
                    contract: None,
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
                    contract: None,
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
                    contract: None,
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
                    contract: None,
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
                    contract: None,
                    message: format!(
                        "{}() requires a field selector",
                        measure_name(branch.pipe.measure)
                    ),
                });
            }
        }
    }

    // Check threshold expression type
    check_expr_type(&branch.pipe.threshold, scope, rule_name, errors);

    // T5: threshold type must be compatible with measure result type
    if let Some(result_type) = measure_result_type(branch.pipe.measure, &field_val_type)
        && let Some(threshold_type) = infer_type(&branch.pipe.threshold, scope)
        && !compatible(&result_type, &threshold_type)
        && !(is_numeric(&result_type) && is_numeric(&threshold_type))
    {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            contract: None,
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
        check_expr_type(guard, scope, rule_name, errors);
    }
}

fn field_selector_name(fs: &FieldSelector) -> &str {
    match fs {
        FieldSelector::Dot(n) | FieldSelector::Bracket(n) => n.as_str(),
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
