use std::collections::HashMap;

use anyhow::Result;

use wf_lang::ast::{
    CloseTrigger, CmpOp, ContractBlock, ExpectStmt, Expr, FieldAssign, GivenStmt, HitAssert,
};
use wf_lang::plan::RulePlan;

use crate::alert::AlertRecord;
use crate::rule::match_engine::eval_expr;
use crate::rule::{CepStateMachine, CloseReason, Event, RuleExecutor, StepResult, Value};

/// Result of running a single contract block against a rule.
pub struct ContractResult {
    pub contract_name: String,
    pub rule_name: String,
    pub passed: bool,
    pub failures: Vec<String>,
    pub alert_count: usize,
}

/// Run a single contract block against a pre-compiled rule plan.
///
/// Simulates event injection and close triggers, then validates
/// the `expect` assertions against the resulting alerts.
pub fn run_contract(
    contract: &ContractBlock,
    plan: &RulePlan,
    time_field: Option<String>,
) -> Result<ContractResult> {
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), time_field);
    let executor = RuleExecutor::new(plan.clone());

    let base_nanos: i64 = 1_700_000_000_000_000_000;
    let mut current_nanos = base_nanos;
    let mut alerts: Vec<AlertRecord> = Vec::new();

    // Resolve the alias for the rule (first bind's alias)
    let default_alias = plan
        .binds
        .first()
        .map(|b| b.alias.clone())
        .unwrap_or_default();

    // Process given statements
    for stmt in &contract.given {
        match stmt {
            GivenStmt::Row { alias, fields } => {
                let event = fields_to_event(fields);
                let use_alias = if alias.is_empty() {
                    &default_alias
                } else {
                    alias
                };

                match sm.advance_at(use_alias, &event, current_nanos) {
                    StepResult::Matched(ctx) => {
                        if let Ok(alert) = executor.execute_match(&ctx) {
                            alerts.push(alert);
                        }
                    }
                    StepResult::Advance | StepResult::Accumulate => {}
                }

                current_nanos += 1_000_000_000; // +1 second per row
            }
            GivenStmt::Tick(dur) => {
                current_nanos += dur.as_nanos() as i64;

                // Scan for expired instances at the new watermark
                let expired = sm.scan_expired_at(current_nanos);
                for close in expired {
                    if let Ok(Some(alert)) = executor.execute_close(&close) {
                        alerts.push(alert);
                    }
                }
            }
            _ => {} // future-proof for non_exhaustive
        }
    }

    // Apply close trigger from contract options
    let close_trigger = contract.options.as_ref().and_then(|o| o.close_trigger);

    match close_trigger {
        None | Some(CloseTrigger::Eos) => {
            let closes = sm.close_all(CloseReason::Eos);
            for close in closes {
                if let Ok(Some(alert)) = executor.execute_close(&close) {
                    alerts.push(alert);
                }
            }
        }
        Some(CloseTrigger::Timeout) => {
            // Advance time by 1 day to force timeout
            current_nanos += 86_400_000_000_000i64;
            let expired = sm.scan_expired_at(current_nanos);
            for close in expired {
                if let Ok(Some(alert)) = executor.execute_close(&close) {
                    alerts.push(alert);
                }
            }
        }
        Some(CloseTrigger::Flush) => {
            let closes = sm.close_all(CloseReason::Flush);
            for close in closes {
                if let Ok(Some(alert)) = executor.execute_close(&close) {
                    alerts.push(alert);
                }
            }
        }
        _ => {
            // future-proof for non_exhaustive CloseTrigger
            let closes = sm.close_all(CloseReason::Eos);
            for close in closes {
                if let Ok(Some(alert)) = executor.execute_close(&close) {
                    alerts.push(alert);
                }
            }
        }
    }

    // Validate expect assertions
    let mut failures = Vec::new();
    for expect in &contract.expect {
        match expect {
            ExpectStmt::Hits { cmp, count } => {
                if !compare_usize(*cmp, alerts.len(), *count) {
                    failures.push(format!(
                        "hits: expected {} {} {}, got {}",
                        "hits",
                        cmp_op_str(*cmp),
                        count,
                        alerts.len()
                    ));
                }
            }
            ExpectStmt::HitAssert { index, assert } => {
                if *index >= alerts.len() {
                    failures.push(format!(
                        "hit[{}]: index out of range (only {} alerts)",
                        index,
                        alerts.len()
                    ));
                    continue;
                }
                let alert = &alerts[*index];
                validate_hit_assert(*index, alert, assert, &mut failures);
            }
            _ => {} // future-proof for non_exhaustive
        }
    }

    let passed = failures.is_empty();
    Ok(ContractResult {
        contract_name: contract.name.clone(),
        rule_name: contract.rule_name.clone(),
        passed,
        failures,
        alert_count: alerts.len(),
    })
}

fn validate_hit_assert(
    index: usize,
    alert: &AlertRecord,
    assert: &HitAssert,
    failures: &mut Vec<String>,
) {
    match assert {
        HitAssert::Score { cmp, value } => {
            if !compare_f64(*cmp, alert.score, *value) {
                failures.push(format!(
                    "hit[{}].score: expected {} {}, got {}",
                    index,
                    cmp_op_str(*cmp),
                    value,
                    alert.score
                ));
            }
        }
        HitAssert::CloseReason { value } => {
            let actual = alert.close_reason.as_deref().unwrap_or("");
            if actual != value {
                failures.push(format!(
                    "hit[{}].close_reason: expected {:?}, got {:?}",
                    index, value, actual
                ));
            }
        }
        HitAssert::EntityType { value } => {
            if alert.entity_type != *value {
                failures.push(format!(
                    "hit[{}].entity_type: expected {:?}, got {:?}",
                    index, value, alert.entity_type
                ));
            }
        }
        HitAssert::EntityId { value } => {
            if alert.entity_id != *value {
                failures.push(format!(
                    "hit[{}].entity_id: expected {:?}, got {:?}",
                    index, value, alert.entity_id
                ));
            }
        }
        HitAssert::Field {
            name,
            cmp,
            value: _,
        } => {
            // Field assertion on yield outputs requires yield output capture.
            failures.push(format!(
                "hit[{}].field({}): field-level assertions not yet supported (cmp: {:?})",
                index, name, cmp
            ));
        }
        _ => {} // future-proof for non_exhaustive
    }
}

/// Convert contract field assignments to an Event.
fn fields_to_event(fields: &[FieldAssign]) -> Event {
    let mut map = HashMap::new();
    for f in fields {
        if let Some(v) = expr_to_value(&f.value) {
            map.insert(f.name.clone(), v);
        }
    }
    Event { fields: map }
}

/// Convert a literal expression to a Value.
fn expr_to_value(expr: &Expr) -> Option<Value> {
    let empty_event = Event {
        fields: HashMap::new(),
    };
    eval_expr(expr, &empty_event)
}

fn compare_f64(cmp: CmpOp, actual: f64, expected: f64) -> bool {
    match cmp {
        CmpOp::Eq => (actual - expected).abs() < f64::EPSILON,
        CmpOp::Ne => (actual - expected).abs() >= f64::EPSILON,
        CmpOp::Lt => actual < expected,
        CmpOp::Gt => actual > expected,
        CmpOp::Le => actual <= expected,
        CmpOp::Ge => actual >= expected,
        #[allow(unreachable_patterns)]
        _ => false,
    }
}

fn compare_usize(cmp: CmpOp, actual: usize, expected: usize) -> bool {
    match cmp {
        CmpOp::Eq => actual == expected,
        CmpOp::Ne => actual != expected,
        CmpOp::Lt => actual < expected,
        CmpOp::Gt => actual > expected,
        CmpOp::Le => actual <= expected,
        CmpOp::Ge => actual >= expected,
        #[allow(unreachable_patterns)]
        _ => false,
    }
}

fn cmp_op_str(cmp: CmpOp) -> &'static str {
    match cmp {
        CmpOp::Eq => "==",
        CmpOp::Ne => "!=",
        CmpOp::Lt => "<",
        CmpOp::Gt => ">",
        CmpOp::Le => "<=",
        CmpOp::Ge => ">=",
        #[allow(unreachable_patterns)]
        _ => "??",
    }
}
