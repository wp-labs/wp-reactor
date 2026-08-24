use wf_lang::ast::{
    CloseTrigger, CmpOp, ExpectStmt, Expr, FieldAssign, FieldRef, HitAssert, InputStmt,
    PermutationMode, TestBlock,
};
use wf_lang::plan::RulePlan;

use crate::alert::OutputRecord;
use crate::error::CoreResult;
use crate::match_engine::match_engine::EngineHashMap;
use crate::match_engine::match_engine::eval_expr;
use crate::match_engine::{
    CepStateMachine, CloseReason, Event, RuleExecutor, StepResult, Value, WindowLookup,
};

/// Empty [`WindowLookup`] for the inline test harness — only reachable for
/// `on each` rules whose joins are rejected up front, so no window is ever
/// consulted.
struct NoLookup;

impl WindowLookup for NoLookup {
    fn snapshot_field_values(
        &self,
        _window: &str,
        _field: &str,
    ) -> Option<std::collections::HashSet<String>> {
        None
    }
    fn snapshot(&self, _window: &str) -> Option<Vec<crate::match_engine::JoinRow>> {
        None
    }
    fn snapshot_with_timestamps(
        &self,
        _window: &str,
    ) -> Option<Vec<(i64, crate::match_engine::JoinRow)>> {
        None
    }
}

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
) -> CoreResult<TestResult> {
    // join-then-key rules can't be asserted by inline tests: the harness runs
    // `advance_at` without a WindowLookup, so the key-join branch treats every
    // event as a join miss and skips it (hits == 0 no matter what the input
    // says) — an `expect hits: 0` would pass vacuously. Reject loudly instead
    // of silently green-lighting a vacuous assertion (P1 guard; join-key
    // behavior is anchored by engine-level tests + E2E verify).
    if plan.match_plan.key_join.is_some() {
        return Ok(TestResult {
            test_name: test.name.clone(),
            rule_name: test.rule_name.clone(),
            passed: false,
            failures: vec![
                "join-then-key rule: inline tests cannot assert hit counts (the harness runs \
                 advance_at without a WindowLookup, so every event is skipped as a join miss); \
                 use the engine-level join-key tests or E2E verify"
                    .to_string(),
            ],
            output_count: 0,
        });
    }
    // `on each` rules with joins need a WindowLookup the harness does not
    // provide — reject loudly (same rationale as the key-join guard).
    if plan.each_plan.is_some() && !plan.joins.is_empty() {
        return Ok(TestResult {
            test_name: test.name.clone(),
            rule_name: test.rule_name.clone(),
            passed: false,
            failures: vec![
                "on-each rule with joins: inline tests cannot assert hit counts (the harness \
                 runs without a WindowLookup, so every join is a miss); use E2E verify"
                    .to_string(),
            ],
            output_count: 0,
        });
    }
    // `where` post-join filters that reference join-window fields need a
    // WindowLookup the harness does not provide — reject loudly. Without the
    // lookup every join misses, so the joined field is absent and the strict
    // `where` suppresses every output (hits == 0 would pass vacuously).
    if let Some(w) = &plan.r#where
        && let Some(join_win) = plan
            .joins
            .iter()
            .find(|j| expr_refs_window(w, &j.right_window))
    {
        return Ok(TestResult {
            test_name: test.name.clone(),
            rule_name: test.rule_name.clone(),
            passed: false,
            failures: vec![format!(
                "rule `where` references joined window `{}`: inline tests cannot assert hit \
                 counts (the harness runs without a WindowLookup, so the join misses and the \
                 strict where suppresses every output); use E2E verify",
                join_win.right_window
            )],
            output_count: 0,
        });
    }
    let permutation = test.options.as_ref().and_then(|o| o.permutation);
    let runs = test.options.as_ref().and_then(|o| o.runs).unwrap_or(1);

    let mut failures = Vec::new();
    let mut output_count = 0usize;

    for run_idx in 0..runs {
        let run_input = match permutation {
            Some(PermutationMode::Shuffle) => shuffle_row_input(&test.input, run_idx as u64 + 1),
            _ => test.input.clone(),
        };
        let alerts = execute_test_run(test, plan, time_field.clone(), &run_input);
        if run_idx == 0 {
            output_count = alerts.len();
        }
        let run_failures = validate_expect_stmts(&test.expect, &alerts);
        for failure in run_failures {
            failures.push(format!("run {}: {}", run_idx + 1, failure));
        }
    }

    let passed = failures.is_empty();
    Ok(TestResult {
        test_name: test.name.clone(),
        rule_name: test.rule_name.clone(),
        passed,
        failures,
        output_count,
    })
}

fn execute_test_run(
    test: &TestBlock,
    plan: &RulePlan,
    time_field: Option<String>,
    input: &[InputStmt],
) -> Vec<OutputRecord> {
    let mut sm = CepStateMachine::new(plan.name.clone(), plan.match_plan.clone(), time_field);
    let executor = RuleExecutor::new(plan.clone());
    let conv_plan = plan.conv_plan.as_ref();
    let mut alerts: Vec<OutputRecord> = Vec::new();
    let base_nanos: i64 = 1_700_000_000_000_000_000;
    let mut current_nanos = base_nanos;
    let default_alias = plan
        .binds
        .first()
        .map(|b| b.alias.clone())
        .unwrap_or_default();

    for stmt in input {
        match stmt {
            InputStmt::Row { alias, fields } => {
                let event = fields_to_event(fields);
                let use_alias = if alias.is_empty() {
                    &default_alias
                } else {
                    alias
                };

                // 评估 bind 级过滤（events { b : bid_events && guard }），与引擎
                // rule_task 的 alias_accepts 一致——否则 test 会绕过 guard 全部放行。
                if !executor.event_matches_alias(use_alias, &event, None) {
                    current_nanos += 1_000_000_000;
                    continue;
                }

                match sm.advance_at(use_alias, &event, current_nanos) {
                    StepResult::Matched(ctx) => {
                        if let Ok(alert) = executor.execute_match(&ctx) {
                            alerts.push(alert);
                        }
                    }
                    StepResult::Advance | StepResult::Accumulate => {}
                }

                // `on each` rules: the match machine has no state for them, so
                // drive the on-each path directly (joins are rejected up front,
                // so an empty lookup never matters here).
                if plan.each_plan.is_some()
                    && let Ok(Some(alert)) = executor.execute_each_with_joins(
                        &event,
                        current_nanos,
                        &NoLookup,
                        &[],
                        current_nanos,
                    )
                {
                    alerts.push(alert);
                }

                current_nanos += 1_000_000_000;
            }
            InputStmt::Tick(dur) => {
                current_nanos += dur.as_nanos() as i64;
                let expired = sm.scan_expired_at_with_conv(current_nanos, conv_plan);
                for close in expired {
                    if let Ok(Some(alert)) = executor.execute_close(&close) {
                        alerts.push(alert);
                    }
                }
            }
            _ => {}
        }
    }

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
            for close in sm.close_all_with_conv(CloseReason::Eos, conv_plan) {
                if let Ok(Some(alert)) = executor.execute_close(&close) {
                    alerts.push(alert);
                }
            }
        }
    }

    alerts
}

fn validate_expect_stmts(expect_stmts: &[ExpectStmt], alerts: &[OutputRecord]) -> Vec<String> {
    let mut failures = Vec::new();
    for expect in expect_stmts {
        match expect {
            ExpectStmt::Hits { cmp, count } if !compare_usize(*cmp, alerts.len(), *count) => {
                failures.push(format!(
                    "hits: expected {} {} {}, got {}",
                    "hits",
                    cmp_op_str(*cmp),
                    count,
                    alerts.len()
                ));
            }
            ExpectStmt::Hits { .. } => {}
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
            _ => {}
        }
    }
    failures
}

/// Whether `expr` references a field of window `window` (qualified/path form,
/// e.g. `person_events.state`). Used to reject inline tests for rules whose
/// `where` filters joined fields the harness cannot populate.
fn expr_refs_window(expr: &Expr, window: &str) -> bool {
    match expr {
        Expr::Field(fr) => match fr {
            FieldRef::Qualified(q, _) | FieldRef::Bracketed(q, _) => q == window,
            FieldRef::Path { alias, .. } => alias == window,
            FieldRef::Simple(_) => false,
            _ => false, // non_exhaustive FieldRef: unknown forms are not window refs
        },
        Expr::Neg(inner) => expr_refs_window(inner, window),
        Expr::Not(inner) => expr_refs_window(inner, window),
        Expr::BinOp { left, right, .. } => {
            expr_refs_window(left, window) || expr_refs_window(right, window)
        }
        Expr::InList {
            expr: inner, list, ..
        } => expr_refs_window(inner, window) || list.iter().any(|e| expr_refs_window(e, window)),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            expr_refs_window(cond, window)
                || expr_refs_window(then_expr, window)
                || expr_refs_window(else_expr, window)
        }
        Expr::FuncCall { args, .. } => args.iter().any(|e| expr_refs_window(e, window)),
        Expr::Array(items) => items.iter().any(|e| expr_refs_window(e, window)),
        Expr::Object(items) => items
            .iter()
            .any(|item| expr_refs_window(&item.value, window)),
        _ => false,
    }
}

fn shuffle_row_input(input: &[InputStmt], seed: u64) -> Vec<InputStmt> {
    let mut out = input.to_vec();
    let mut row_positions = Vec::new();
    let mut rows = Vec::new();
    for (idx, stmt) in out.iter().enumerate() {
        if let InputStmt::Row { .. } = stmt {
            row_positions.push(idx);
            rows.push(stmt.clone());
        }
    }
    if rows.len() < 2 {
        return out;
    }
    shuffle_in_place(&mut rows, seed);
    for (pos, row) in row_positions.into_iter().zip(rows) {
        out[pos] = row;
    }
    out
}

fn shuffle_in_place<T>(items: &mut [T], seed: u64) {
    let mut state = seed
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0xBF58_476D_1CE4_E5B9);
    if state == 0 {
        state = 0xA5A5_A5A5_A5A5_A5A5;
    }
    for i in (1..items.len()).rev() {
        let j = (next_u64(&mut state) % ((i + 1) as u64)) as usize;
        items.swap(i, j);
    }
}

fn next_u64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn validate_hit_assert(
    index: usize,
    output: &OutputRecord,
    assert: &HitAssert,
    failures: &mut Vec<String>,
) {
    match assert {
        HitAssert::Score { cmp, value } if !compare_f64(*cmp, output.score, *value) => {
            failures.push(format!(
                "hit[{}].score: expected {} {}, got {}",
                index,
                cmp_op_str(*cmp),
                value,
                output.score
            ));
        }
        HitAssert::Score { .. } => {}
        HitAssert::Origin { value } => {
            let actual = output.origin.as_str();
            if actual != value {
                failures.push(format!(
                    "hit[{}].origin: expected {:?}, got {:?}",
                    index, value, actual
                ));
            }
        }
        HitAssert::EntityType { value } if &*output.entity_type != value.as_str() => {
            failures.push(format!(
                "hit[{}].entity_type: expected {:?}, got {:?}",
                index, value, output.entity_type
            ));
        }
        HitAssert::EntityType { .. } => {}
        HitAssert::EntityId { value } if output.entity_id != *value => {
            failures.push(format!(
                "hit[{}].entity_id: expected {:?}, got {:?}",
                index, value, output.entity_id
            ));
        }
        HitAssert::EntityId { .. } => {}
        HitAssert::Field { name, cmp, value } => {
            // Field assertion on yield outputs (validates against the record's
            // evaluated `yield (...)` fields). Only Eq/Ne supported for now.
            let Some(actual) = output
                .yield_fields
                .iter()
                .find(|(n, _)| **n == *name)
                .map(|(_, v)| v)
            else {
                failures.push(format!(
                    "hit[{}].field({}): no such yield field",
                    index, name
                ));
                return;
            };
            let Some(expected) = expr_to_value(value) else {
                failures.push(format!(
                    "hit[{}].field({}): unsupported expected value",
                    index, name
                ));
                return;
            };
            let eq = actual == &expected;
            let ok = match cmp {
                CmpOp::Eq => eq,
                CmpOp::Ne => !eq,
                _ => {
                    failures.push(format!(
                        "hit[{}].field({}): only ==/!= comparisons supported, got {:?}",
                        index, name, cmp
                    ));
                    return;
                }
            };
            if !ok {
                failures.push(format!(
                    "hit[{}].field({}): expected {:?} {}, got {:?}",
                    index,
                    name,
                    expected,
                    if *cmp == CmpOp::Eq { "==" } else { "!=" },
                    actual
                ));
            }
        }
        _ => {} // future-proof for non_exhaustive
    }
}

/// Convert field assignments to an Event.
fn fields_to_event(fields: &[FieldAssign]) -> Event {
    let mut map = EngineHashMap::default();
    for f in fields {
        if let Some(v) = expr_to_value(&f.value) {
            map.insert(f.name.clone().into(), v);
        }
    }
    Event { fields: map }
}

/// Convert a literal expression to a Value.
fn expr_to_value(expr: &Expr) -> Option<Value> {
    let empty_event = Event {
        fields: EngineHashMap::default(),
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
