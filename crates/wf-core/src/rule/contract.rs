use std::collections::HashMap;

use anyhow::Result;

use wf_lang::ast::{
    CloseTrigger, CmpOp, ExpectStmt, Expr, FieldAssign, HitAssert, InputStmt, TestBlock,
};
use wf_lang::plan::RulePlan;

use crate::alert::OutputRecord;
use crate::rule::match_engine::eval_expr;
use crate::rule::{CepStateMachine, CloseReason, Event, RuleExecutor, StepResult, Value};

/// Result of running a single test block against a rule.
pub struct TestResult {
    pub test_name: String,
    pub rule_name: String,
    pub passed: bool,
    pub failures: Vec<String>,
    pub output_count: usize,
}

/// Run a single test block against a pre-compiled rule plan.
///
/// Simulates event injection and close triggers, then validates
/// the `expect` assertions against the resulting alerts.
pub fn run_test(
    test: &TestBlock,
    plan: &RulePlan,
    time_field: Option<String>,
) -> Result<TestResult> {
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), time_field);
    let executor = RuleExecutor::new(plan.clone());
    let conv_plan = plan.conv_plan.as_ref();

    let base_nanos: i64 = 1_700_000_000_000_000_000;
    let mut current_nanos = base_nanos;
    let mut alerts: Vec<OutputRecord> = Vec::new();

    // Resolve the alias for the rule (first bind's alias)
    let default_alias = plan
        .binds
        .first()
        .map(|b| b.alias.clone())
        .unwrap_or_default();

    // Process input statements
    for stmt in &test.input {
        match stmt {
            InputStmt::Row { alias, fields } => {
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
            InputStmt::Tick(dur) => {
                current_nanos += dur.as_nanos() as i64;

                // Scan for expired instances at the new watermark (with conv)
                let expired = sm.scan_expired_at_with_conv(current_nanos, conv_plan);
                for close in expired {
                    if let Ok(Some(alert)) = executor.execute_close(&close) {
                        alerts.push(alert);
                    }
                }
            }
            _ => {} // future-proof for non_exhaustive
        }
    }

    // Apply close trigger from test options
    let close_trigger = test.options.as_ref().and_then(|o| o.close_trigger);

    match close_trigger {
        None | Some(CloseTrigger::Eos) => {
            for close in sm.close_all_with_conv(CloseReason::Eos, conv_plan) {
                if let Ok(Some(alert)) = executor.execute_close(&close) {
                    alerts.push(alert);
                }
            }
        }
        Some(CloseTrigger::Timeout) => {
            // Advance time by 1 day to force timeout
            current_nanos += 86_400_000_000_000i64;
            let expired = sm.scan_expired_at_with_conv(current_nanos, conv_plan);
            for close in expired {
                if let Ok(Some(alert)) = executor.execute_close(&close) {
                    alerts.push(alert);
                }
            }
        }
        Some(CloseTrigger::Flush) => {
            for close in sm.close_all_with_conv(CloseReason::Flush, conv_plan) {
                if let Ok(Some(alert)) = executor.execute_close(&close) {
                    alerts.push(alert);
                }
            }
        }
        _ => {
            // future-proof for non_exhaustive CloseTrigger
            for close in sm.close_all_with_conv(CloseReason::Eos, conv_plan) {
                if let Ok(Some(alert)) = executor.execute_close(&close) {
                    alerts.push(alert);
                }
            }
        }
    }

    // Validate expect assertions
    let mut failures = Vec::new();
    for expect in &test.expect {
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
                let output = &alerts[*index];
                validate_hit_assert(*index, output, assert, &mut failures);
            }
            _ => {} // future-proof for non_exhaustive
        }
    }

    let passed = failures.is_empty();
    Ok(TestResult {
        test_name: test.name.clone(),
        rule_name: test.rule_name.clone(),
        passed,
        failures,
        output_count: alerts.len(),
    })
}

fn validate_hit_assert(
    index: usize,
    output: &OutputRecord,
    assert: &HitAssert,
    failures: &mut Vec<String>,
) {
    match assert {
        HitAssert::Score { cmp, value } => {
            if !compare_f64(*cmp, output.score, *value) {
                failures.push(format!(
                    "hit[{}].score: expected {} {}, got {}",
                    index,
                    cmp_op_str(*cmp),
                    value,
                    output.score
                ));
            }
        }
        HitAssert::CloseReason { value } => {
            let actual = output.close_reason.as_deref().unwrap_or("");
            if actual != value {
                failures.push(format!(
                    "hit[{}].close_reason: expected {:?}, got {:?}",
                    index, value, actual
                ));
            }
        }
        HitAssert::EntityType { value } => {
            if output.entity_type != *value {
                failures.push(format!(
                    "hit[{}].entity_type: expected {:?}, got {:?}",
                    index, value, output.entity_type
                ));
            }
        }
        HitAssert::EntityId { value } => {
            if output.entity_id != *value {
                failures.push(format!(
                    "hit[{}].entity_id: expected {:?}, got {:?}",
                    index, value, output.entity_id
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

/// Convert field assignments to an Event.
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
