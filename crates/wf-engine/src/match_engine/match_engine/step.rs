use std::collections::{HashMap, HashSet};

use wf_lang::ast::{BinOp, CmpOp, Expr, FieldSelector, Measure, Transform};
use wf_lang::plan::{AggPlan, StepPlan};

use super::eval::{eval_expr_ext, try_eval_expr_to_f64, try_eval_expr_to_value};
use super::key::ValueKey;
use super::state::{AliasState, BranchState, StepState};
use super::types::{Event, RollingStats, StepProgress, Value, WindowLookup};

// ---------------------------------------------------------------------------
// Step evaluation
// ---------------------------------------------------------------------------

pub(super) struct StepEvaluationInput<'a> {
    pub alias: &'a str,
    pub event: &'a Event,
    pub event_time_nanos: i64,
    pub windows: Option<&'a dyn WindowLookup>,
    pub progress: Option<StepProgressCapture<'a>>,
}

#[derive(Clone, Copy)]
pub(super) struct StepProgressCapture<'a> {
    pub rule_name: &'a str,
    pub scope_key: &'a [Value],
    pub machine_id: &'a str,
    pub step_index: usize,
}

/// Evaluate all branches in a step and optionally capture progress details.
pub(super) fn evaluate_step_with_progress(
    input: StepEvaluationInput<'_>,
    step_plan: &StepPlan,
    step_state: &mut StepState,
    baselines: &mut HashMap<String, RollingStats>,
) -> (Option<(usize, f64)>, Option<StepProgress>) {
    let mut progress = None;
    let mut threshold_checked_branches = 0usize;
    for (branch_idx, branch) in step_plan.branches.iter().enumerate() {
        // Source must match alias
        if branch.source != input.alias {
            continue;
        }

        // Guard check
        if let Some(guard) = &branch.guard {
            match eval_expr_ext(guard, input.event, input.windows, baselines) {
                Some(Value::Bool(true)) => {} // guard passed
                _ => continue,                // guard failed or non-bool
            }
        }

        // Extract field value (for aggregation)
        let field_value = extract_branch_field(input.event, &branch.field);

        let bs = &mut step_state.branch_states[branch_idx];

        // Apply transforms (Distinct dedup)
        if !apply_transforms(&branch.agg.transforms, &field_value, bs) {
            continue; // filtered out by transform (e.g. duplicate in distinct)
        }
        threshold_checked_branches += 1;

        record_evidence_time(bs, input.event_time_nanos);

        // Update measure accumulators
        update_measure(&branch.agg.measure, &field_value, bs);

        // Check threshold
        let satisfied = check_threshold(&branch.agg, bs);
        let measure_val = if input.progress.is_some() || satisfied {
            compute_measure(&branch.agg.measure, bs)
        } else {
            0.0
        };

        if let Some(progress_capture) = input.progress {
            let branch_progress = StepProgress {
                rule_name: progress_capture.rule_name.to_string(),
                scope_key: progress_capture.scope_key.to_vec(),
                machine_id: progress_capture.machine_id.to_string(),
                step_index: progress_capture.step_index,
                branch_index: branch_idx,
                step_label: branch.label.clone(),
                branch_source: branch.source.clone(),
                threshold_checked_branches,
                measure_value: measure_val,
                cmp: cmp_symbol(branch.agg.cmp).to_string(),
                threshold: expr_debug_string(&branch.agg.threshold),
                satisfied,
                instances: 0,
            };
            if satisfied {
                return (Some((branch_idx, measure_val)), Some(branch_progress));
            }
            progress = Some(branch_progress);
        }
        if satisfied {
            return (Some((branch_idx, measure_val)), progress);
        }
    }
    (None, progress)
}

fn expr_debug_string(expr: &Expr) -> String {
    match expr {
        Expr::Number(value) => value.to_string(),
        Expr::StringLit(value) => format!("{value:?}"),
        Expr::Bool(value) => value.to_string(),
        Expr::Neg(inner) => format!("-{}", expr_debug_atom(inner)),
        Expr::BinOp { op, left, right } if is_arithmetic(*op) => {
            format!(
                "{} {} {}",
                expr_debug_atom(left),
                binop_symbol(*op),
                expr_debug_atom(right)
            )
        }
        _ => format!("{expr:?}"),
    }
}

fn expr_debug_atom(expr: &Expr) -> String {
    match expr {
        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) | Expr::Neg(_) => {
            expr_debug_string(expr)
        }
        Expr::BinOp { .. } => format!("({})", expr_debug_string(expr)),
        _ => format!("{expr:?}"),
    }
}

fn is_arithmetic(op: BinOp) -> bool {
    matches!(
        op,
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod
    )
}

fn binop_symbol(op: BinOp) -> &'static str {
    match op {
        BinOp::Add => "+",
        BinOp::Sub => "-",
        BinOp::Mul => "*",
        BinOp::Div => "/",
        BinOp::Mod => "%",
        _ => "?",
    }
}

fn cmp_symbol(cmp: wf_lang::ast::CmpOp) -> &'static str {
    match cmp {
        wf_lang::ast::CmpOp::Eq => "==",
        wf_lang::ast::CmpOp::Ne => "!=",
        wf_lang::ast::CmpOp::Lt => "<",
        wf_lang::ast::CmpOp::Gt => ">",
        wf_lang::ast::CmpOp::Le => "<=",
        wf_lang::ast::CmpOp::Ge => ">=",
        _ => "?",
    }
}

pub(super) fn record_evidence_time(bs: &mut BranchState, event_time_nanos: i64) {
    match bs.event_first_time_nanos {
        Some(first) if first <= event_time_nanos => {}
        _ => bs.event_first_time_nanos = Some(event_time_nanos),
    }
    match bs.event_last_time_nanos {
        Some(last) if last >= event_time_nanos => {}
        _ => bs.event_last_time_nanos = Some(event_time_nanos),
    }
}

pub(super) fn collect_event_fields(
    event: &Event,
    bs: &mut BranchState,
    tracked_fields: Option<&HashSet<String>>,
    tracked_plain_fields: &HashSet<String>,
    branch_field: Option<&FieldSelector>,
) {
    if let Some(fields) = tracked_fields {
        for field_name in fields {
            push_event_field(event, bs, field_name);
        }
        for field_name in tracked_plain_fields {
            push_event_field(event, bs, field_name);
        }
        if let Some(field_name) = selected_field_name(branch_field)
            && !fields.contains(field_name)
            && !tracked_plain_fields.contains(field_name)
        {
            push_event_field(event, bs, field_name);
        }
    } else {
        for (field_name, value) in &event.fields {
            let values = bs.field_values.entry(field_name.clone()).or_default();
            push_capped(values, value.clone());
        }
    }
}

fn push_event_field(event: &Event, bs: &mut BranchState, field_name: &str) {
    if let Some(value) = event.fields.get(field_name) {
        let values = bs.field_values.entry(field_name.to_string()).or_default();
        push_capped(values, value.clone());
    }
}

fn selected_field_name(field: Option<&FieldSelector>) -> Option<&str> {
    match field {
        Some(FieldSelector::Dot(name)) | Some(FieldSelector::Bracket(name)) => Some(name.as_str()),
        _ => None,
    }
}

/// Maximum number of values retained per field in alias/branch field history.
///
/// `collect_alias_event` / `collect_event_fields` / `update_measure`
/// accumulate field values across matching events. Without a cap this grows
/// unboundedly on high-volume windows (e.g. 30k events × N fields), risking OOM.
/// We keep a recent sample per field, which:
/// - preserves yield field resolution (`e.dip` reads `.last()`, always present)
/// - keeps L3 collection functions such as `collect_set(e.event_id)` bounded.
///
/// `stat.count(window_event(alias))` counts all accepted alias events. Collection
/// functions over alias fields can therefore return fewer values than the count
/// for large windows or duplicate field values. Close-step threshold evaluation
/// (count/sum/min/max/distinct) uses separate accumulators and is not affected by
/// this cap.
const MAX_TRACKED_FIELD_VALUES: usize = 1024;

/// Push `value` onto `values`, trimming to the most recent
/// `MAX_TRACKED_FIELD_VALUES` entries.
fn push_capped(values: &mut Vec<Value>, value: Value) {
    values.push(value);
    if values.len() > MAX_TRACKED_FIELD_VALUES {
        let keep_from = values.len() - MAX_TRACKED_FIELD_VALUES;
        values.drain(..keep_from);
    }
}

pub(super) fn collect_alias_event(
    event: &Event,
    alias_state: &mut AliasState,
    tracked_fields: Option<&HashSet<String>>,
) {
    alias_state.count += 1;
    if let Some(fields) = tracked_fields {
        for field_name in fields {
            if let Some(value) = event.fields.get(field_name) {
                let values = alias_state
                    .field_values
                    .entry(field_name.clone())
                    .or_default();
                push_capped(values, value.clone());
            }
        }
    } else {
        for (field_name, value) in &event.fields {
            let values = alias_state
                .field_values
                .entry(field_name.clone())
                .or_default();
            push_capped(values, value.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// Branch field extraction
// ---------------------------------------------------------------------------

pub(super) fn extract_branch_field(event: &Event, field: &Option<FieldSelector>) -> Option<Value> {
    match field {
        Some(FieldSelector::Dot(name)) | Some(FieldSelector::Bracket(name)) => {
            event.fields.get(name).cloned()
        }
        Some(_) => None,
        None => None,
    }
}

// ---------------------------------------------------------------------------
// Transform application
// ---------------------------------------------------------------------------

/// Apply transforms. Returns `false` if the event should be skipped
/// (e.g. duplicate value in a Distinct pipeline).
pub(super) fn apply_transforms(
    transforms: &[Transform],
    field_value: &Option<Value>,
    bs: &mut BranchState,
) -> bool {
    for t in transforms {
        if t == &Transform::Distinct {
            let key = match field_value {
                Some(v) => ValueKey::from_value(v),
                None => return false,
            };
            if !bs.distinct_set.insert(key) {
                return false; // duplicate
            }
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Measure update & computation
// ---------------------------------------------------------------------------

pub(super) fn update_measure(measure: &Measure, field_value: &Option<Value>, bs: &mut BranchState) {
    let fval = field_value.as_ref().and_then(value_to_f64);

    // Collect raw values for L3 functions (collect_set/list, first/last, stddev/percentile)
    if let Some(val) = field_value {
        push_capped(&mut bs.collected_values, val.clone());
    }

    match measure {
        Measure::Count => {
            bs.count += 1;
        }
        Measure::Sum => {
            if let Some(v) = fval {
                bs.sum += v;
            }
        }
        Measure::Avg => {
            if let Some(v) = fval {
                bs.avg_sum += v;
                bs.avg_count += 1;
            }
        }
        Measure::Min => {
            update_extreme(fval, field_value, &mut bs.min, &mut bs.min_val, true);
        }
        Measure::Max => {
            update_extreme(fval, field_value, &mut bs.max, &mut bs.max_val, false);
        }
        _ => {} // unknown measure — no-op
    }
}

/// Update numeric extreme + Value-based extreme in one shot.
fn update_extreme(
    fval: Option<f64>,
    field_value: &Option<Value>,
    num_acc: &mut f64,
    val_acc: &mut Option<Value>,
    is_min: bool,
) {
    if let Some(v) = fval
        && ((is_min && v < *num_acc) || (!is_min && v > *num_acc))
    {
        *num_acc = v;
    }
    if let Some(val) = field_value {
        let replace = match val_acc.as_ref() {
            None => true,
            Some(cur) => {
                let ord = value_ordering(val, cur);
                if is_min { ord.is_lt() } else { ord.is_gt() }
            }
        };
        if replace {
            *val_acc = Some(val.clone());
        }
    }
}

pub(super) fn compute_measure(measure: &Measure, bs: &BranchState) -> f64 {
    match measure {
        Measure::Count => bs.count as f64,
        Measure::Sum => bs.sum,
        Measure::Avg => {
            if bs.avg_count == 0 {
                0.0
            } else {
                bs.avg_sum / bs.avg_count as f64
            }
        }
        Measure::Min => bs.min,
        Measure::Max => bs.max,
        _ => 0.0, // unknown measure
    }
}

/// Unified threshold check for a branch's aggregation plan.
///
/// Strategy:
/// 1. Try `try_eval_expr_to_f64` on the threshold expression.
///    - If it succeeds AND the numeric measure value is usable → f64 compare.
/// 2. For min/max where the numeric path gives ±INF (non-numeric field)
///    OR the threshold is non-constant → fall back to Value-based comparison.
/// 3. If neither path resolves, the check returns `false` (not satisfied).
pub(super) fn check_threshold(agg: &AggPlan, bs: &BranchState) -> bool {
    let measure_f64 = compute_measure(&agg.measure, bs);

    // Fast path: threshold is a constant numeric expression
    if let Some(threshold_f64) = try_eval_expr_to_f64(&agg.threshold) {
        match agg.measure {
            Measure::Min | Measure::Max if !measure_f64.is_finite() => {
                // Numeric accumulator is ±INF → non-numeric field, fall through
                // to value-based path below
            }
            _ => return compare(agg.cmp, measure_f64, threshold_f64),
        }
    }

    // Value-based path: needed for min/max on non-numeric fields,
    // or when threshold expression is non-constant.
    match agg.measure {
        Measure::Min => {
            if let (Some(val), Some(threshold_val)) =
                (&bs.min_val, try_eval_expr_to_value(&agg.threshold))
            {
                compare_value_threshold(agg.cmp, val, &threshold_val)
            } else {
                false
            }
        }
        Measure::Max => {
            if let (Some(val), Some(threshold_val)) =
                (&bs.max_val, try_eval_expr_to_value(&agg.threshold))
            {
                compare_value_threshold(agg.cmp, val, &threshold_val)
            } else {
                false
            }
        }
        _ => {
            // count/sum/avg with a non-constant threshold (e.g. field ref):
            // cannot evaluate — treat as unsatisfied rather than silently
            // comparing against 0.0
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

fn compare(cmp: CmpOp, lhs: f64, rhs: f64) -> bool {
    match cmp {
        CmpOp::Eq => (lhs - rhs).abs() < f64::EPSILON,
        CmpOp::Ne => (lhs - rhs).abs() >= f64::EPSILON,
        CmpOp::Lt => lhs < rhs,
        CmpOp::Gt => lhs > rhs,
        CmpOp::Le => lhs <= rhs,
        CmpOp::Ge => lhs >= rhs,
        _ => false,
    }
}

/// Ordering for Value (used by min/max on orderable fields).
/// Number < Str < Bool < Array < Object for cross-type (shouldn't happen in practice).
fn value_ordering(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::Str(x), Value::Str(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => x.len().cmp(&y.len()),
        (Value::Object(x), Value::Object(y)) => x.len().cmp(&y.len()),
        // Cross-type: Number < Str < Bool < Array < Object
        (Value::Number(_), _) => std::cmp::Ordering::Less,
        (_, Value::Number(_)) => std::cmp::Ordering::Greater,
        (Value::Str(_), Value::Bool(_) | Value::Array(_) | Value::Object(_)) => {
            std::cmp::Ordering::Less
        }
        (Value::Bool(_) | Value::Array(_) | Value::Object(_), Value::Str(_)) => {
            std::cmp::Ordering::Greater
        }
        (Value::Bool(_), Value::Array(_) | Value::Object(_)) => std::cmp::Ordering::Less,
        (Value::Array(_) | Value::Object(_), Value::Bool(_)) => std::cmp::Ordering::Greater,
        (Value::Array(_), Value::Object(_)) => std::cmp::Ordering::Less,
        (Value::Object(_), Value::Array(_)) => std::cmp::Ordering::Greater,
    }
}

/// Compare a Value against a threshold Value using CmpOp.
/// Returns `false` for cross-type comparisons (e.g. Str vs Number)
/// to prevent false positives from the arbitrary cross-type ordering.
fn compare_value_threshold(cmp: CmpOp, val: &Value, threshold: &Value) -> bool {
    let same_type = matches!(
        (val, threshold),
        (Value::Number(_), Value::Number(_))
            | (Value::Str(_), Value::Str(_))
            | (Value::Bool(_), Value::Bool(_))
    );
    if !same_type {
        return false;
    }
    let ord = value_ordering(val, threshold);
    match cmp {
        CmpOp::Eq => ord.is_eq(),
        CmpOp::Ne => !ord.is_eq(),
        CmpOp::Lt => ord.is_lt(),
        CmpOp::Gt => ord.is_gt(),
        CmpOp::Le => ord.is_le(),
        CmpOp::Ge => ord.is_ge(),
        _ => false,
    }
}

fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => Some(*n),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::state::{AliasState, BranchState};
    use super::super::types::Value;
    use super::*;

    fn event_with(field: &str, value: i64) -> Event {
        let mut fields = std::collections::HashMap::new();
        fields.insert(field.to_string(), Value::Number(value as f64));
        Event { fields }
    }

    #[test]
    fn collect_alias_event_caps_field_values_and_keeps_most_recent() {
        let mut state = AliasState::new();
        // Feed well past 2× the cap to force multiple trims.
        let over = MAX_TRACKED_FIELD_VALUES * 5;
        for i in 0..over as i64 {
            collect_alias_event(&event_with("dip", i), &mut state, None);
        }

        let values = state.field_values.get("dip").expect("dip collected");
        assert_eq!(values.len(), MAX_TRACKED_FIELD_VALUES);
        // The retained window is the most recent entries; `.last()` is the latest event,
        // which is what yield field resolution (`e.dip`) reads.
        assert_eq!(values.last(), Some(&Value::Number((over - 1) as f64)));
        // count tracks every event regardless of the value cap.
        assert_eq!(state.count, over as u64);
    }

    #[test]
    fn collect_alias_event_caps_explicit_tracked_fields_and_keeps_most_recent() {
        let mut state = AliasState::new();
        let tracked = HashSet::from(["event_id".to_string()]);
        let over = MAX_TRACKED_FIELD_VALUES * 2 + 17;
        for i in 0..over as i64 {
            collect_alias_event(&event_with("event_id", i), &mut state, Some(&tracked));
        }

        let values = state
            .field_values
            .get("event_id")
            .expect("event_id collected");
        assert_eq!(values.len(), MAX_TRACKED_FIELD_VALUES);
        assert!(!values.contains(&Value::Number(0.0)));
        assert_eq!(values.last(), Some(&Value::Number((over - 1) as f64)));
        assert_eq!(state.count, over as u64);
    }

    #[test]
    fn collect_event_fields_caps_branch_field_values_and_keeps_most_recent() {
        // Close-step accumulation path: collect_event_fields feeds BranchState,
        // whose field_values are only consumed by yield/L3 (not threshold eval).
        // Same cap semantics as the alias path must hold.
        let mut bs = BranchState::new();
        let over = MAX_TRACKED_FIELD_VALUES * 5;
        for i in 0..over as i64 {
            collect_event_fields(
                &event_with("dport", i),
                &mut bs,
                None,
                &HashSet::new(),
                None,
            );
        }

        let values = bs.field_values.get("dport").expect("dport collected");
        assert_eq!(values.len(), MAX_TRACKED_FIELD_VALUES);
        // `.last()` — the value yield field resolution reads — stays correct.
        assert_eq!(values.last(), Some(&Value::Number((over - 1) as f64)));
    }

    #[test]
    fn collect_alias_event_tracks_only_requested_fields() {
        let mut state = AliasState::new();
        let mut fields = std::collections::HashMap::new();
        fields.insert("sip".to_string(), Value::Str("10.0.0.1".to_string()));
        fields.insert("dport".to_string(), Value::Number(443.0));
        let event = Event { fields };
        let tracked = HashSet::from(["sip".to_string()]);

        collect_alias_event(&event, &mut state, Some(&tracked));

        assert_eq!(state.count, 1);
        assert!(state.field_values.contains_key("sip"));
        assert!(!state.field_values.contains_key("dport"));
    }

    #[test]
    fn collect_event_fields_tracks_requested_fields_and_branch_field() {
        let mut bs = BranchState::new();
        let mut fields = std::collections::HashMap::new();
        fields.insert("sip".to_string(), Value::Str("10.0.0.1".to_string()));
        fields.insert("dport".to_string(), Value::Number(443.0));
        fields.insert("bytes".to_string(), Value::Number(100.0));
        let event = Event { fields };
        let tracked = HashSet::from(["sip".to_string()]);
        let branch_field = FieldSelector::Dot("dport".to_string());

        collect_event_fields(
            &event,
            &mut bs,
            Some(&tracked),
            &HashSet::new(),
            Some(&branch_field),
        );

        assert!(bs.field_values.contains_key("sip"));
        assert!(bs.field_values.contains_key("dport"));
        assert!(!bs.field_values.contains_key("bytes"));
    }

    #[test]
    fn collect_event_fields_tracks_plain_fields() {
        let mut bs = BranchState::new();
        let mut fields = std::collections::HashMap::new();
        fields.insert("sip".to_string(), Value::Str("10.0.0.1".to_string()));
        fields.insert("dport".to_string(), Value::Number(443.0));
        let event = Event { fields };
        let tracked_alias_fields = HashSet::from(["sip".to_string()]);
        let tracked_plain_fields = HashSet::from(["dport".to_string()]);

        collect_event_fields(
            &event,
            &mut bs,
            Some(&tracked_alias_fields),
            &tracked_plain_fields,
            None,
        );

        assert!(bs.field_values.contains_key("sip"));
        assert!(bs.field_values.contains_key("dport"));
    }

    #[test]
    fn update_measure_caps_collected_values_and_preserves_count() {
        let mut bs = BranchState::new();
        let over = MAX_TRACKED_FIELD_VALUES * 5;
        for i in 0..over as i64 {
            update_measure(&Measure::Count, &Some(Value::Number(i as f64)), &mut bs);
        }

        assert_eq!(bs.collected_values.len(), MAX_TRACKED_FIELD_VALUES);
        assert_eq!(
            bs.collected_values.last(),
            Some(&Value::Number((over - 1) as f64))
        );
        // Threshold accumulators still see every event; only the raw value list is capped.
        assert_eq!(bs.count, over as u64);
    }

    #[test]
    fn distinct_transform_keeps_value_types_separate() {
        let mut bs = BranchState::new();

        assert!(apply_transforms(
            &[Transform::Distinct],
            &Some(Value::Number(1.0)),
            &mut bs
        ));
        assert!(apply_transforms(
            &[Transform::Distinct],
            &Some(Value::Str("1".to_string())),
            &mut bs
        ));
        assert!(!apply_transforms(
            &[Transform::Distinct],
            &Some(Value::Number(1.0)),
            &mut bs
        ));
    }

    #[test]
    fn distinct_transform_uses_canonical_float_keys() {
        let mut bs = BranchState::new();

        assert!(apply_transforms(
            &[Transform::Distinct],
            &Some(Value::Number(-0.0)),
            &mut bs
        ));
        assert!(!apply_transforms(
            &[Transform::Distinct],
            &Some(Value::Number(0.0)),
            &mut bs
        ));
        assert!(apply_transforms(
            &[Transform::Distinct],
            &Some(Value::Number(f64::NAN)),
            &mut bs
        ));
        assert!(!apply_transforms(
            &[Transform::Distinct],
            &Some(Value::Number(f64::from_bits(0x7ff8_0000_0000_0001))),
            &mut bs
        ));
    }
}
