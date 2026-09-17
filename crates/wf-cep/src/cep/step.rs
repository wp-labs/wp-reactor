use std::collections::{HashSet, VecDeque};

use wf_lang::ast::{BinOp, CmpOp, Expr, FieldSelector, Measure, Transform};
use wf_lang::plan::{AggPlan, StepPlan};

use super::eval::{eval_expr_ext, try_eval_expr_to_f64, try_eval_expr_to_value};
use super::key::ValueKey;
use super::state::{AliasState, BranchState, StepState};
use super::types::{EngineHashMap, FieldSource, RollingStats, StepProgress, Value, WindowLookup};
use crate::masks::GuardMasks;

// ---------------------------------------------------------------------------
// Step evaluation
// ---------------------------------------------------------------------------

pub(super) struct StepEvaluationInput<'a, E: FieldSource> {
    pub alias: &'a str,
    pub event: &'a E,
    pub event_time_nanos: i64,
    pub windows: Option<&'a dyn WindowLookup>,
    pub progress: Option<StepProgressCapture<'a>>,
    /// Index of the step within `match_plan.event_steps` (for guard-mask lookup).
    pub step_index: usize,
    /// Row index within the current batch (for guard-mask lookup).
    pub row: usize,
    /// Precomputed columnar branch-guard masks, when the runtime supplied them.
    pub masks: Option<&'a GuardMasks>,
    /// 收集 collected_values（L3 序列函数经 ctx `_step_{i}_values` 读取）。
    /// gate = `plan.needs_field_history`（编译器：L3/close 非键读取/join/多
    /// bind 置 true；q6 等单 bind avg 规则为 false——每事件 push + StepData
    /// clone 是纯浪费，2026-08-23 F9）。
    pub collect_step_values: bool,
}

#[derive(Clone, Copy)]
pub(super) struct StepProgressCapture<'a> {
    pub rule_name: &'a str,
    pub scope_key: &'a [Value],
    pub machine_id: &'a str,
    pub step_index: usize,
}

/// Evaluate all branches in a step and optionally capture progress details.
pub(super) fn evaluate_step_with_progress<E: FieldSource>(
    input: StepEvaluationInput<'_, E>,
    step_plan: &StepPlan,
    step_state: &mut StepState,
    baselines: &mut EngineHashMap<String, RollingStats>,
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
            let guard_ok = match input
                .masks
                .and_then(|m| m.event_value(input.step_index, branch_idx, input.row))
            {
                Some(ok) => ok,
                None => matches!(
                    eval_expr_ext(guard, input.event, input.windows, baselines),
                    Some(Value::Bool(true))
                ),
            };
            if !guard_ok {
                continue;
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

        // Collect raw values for L3 functions（collect_set/list、first/last、
        // stddev/percentile）——仅在编译期判定需要时（L3 表达式 / close 非键
        // 读取 / join / 多 bind → needs_field_history=true）。q6 等规则跳过
        // 每事件 VecDeque push + StepData/MatchedContext 的 collected clone。
        if input.collect_step_values
            && let Some(val) = &field_value
        {
            bs.push_collected(val.clone());
        }

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
        Expr::Not(inner) => format!("not {}", expr_debug_atom(inner)),
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
        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) | Expr::Neg(_) | Expr::Not(_) => {
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

pub(super) fn collect_event_fields<E: FieldSource>(
    event: &E,
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
        // No tracked set: collect every non-null field. `field_names` covers the
        // whole schema/map; null/missing cells read `None` and are skipped, which
        // matches the eager path (batch_to_events drops nulls from the map).
        for field_name in event.field_names() {
            if let Some(value) = event.field_value(field_name) {
                bs.push_field_value(field_name, value);
            }
        }
    }
}

fn push_event_field<E: FieldSource>(event: &E, bs: &mut BranchState, field_name: &str) {
    if let Some(value) = event.field_value(field_name) {
        bs.push_field_value(field_name, value);
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
/// for large windows or duplicate field values. 序列首值是窗内最早样本
/// （被裁剪丢弃的首值由 `PinnedFirst` 钉扎，warp-fusion#100），序列长度上界
/// 1025 = 首值 + 最多 1024 个最近样本。Close-step threshold evaluation
/// (count/sum/min/max/distinct) uses separate accumulators and is not affected by
/// this cap.
const MAX_TRACKED_FIELD_VALUES: usize = 1024;

/// Push `value` onto `values`, trimming to the most recent
/// `MAX_TRACKED_FIELD_VALUES` entries.
///
/// Returns the value dropped by this call (`None` when no trim happened).
/// 调用方必须把返回的首个被丢弃值交给 `PinnedFirst` 钉扎（warp-fusion#100），
/// 否则 `first()` 会随窗口增长漂移——工程上应调 `BranchState::push_collected` /
/// `push_field_value`（已封装钉扎），不要直接调本函数。
///
/// 环形维护（VecDeque push_back + pop_front 均 O(1)）：旧实现用 `Vec::drain(..1)`
/// 每次把剩余元素整体 memmove 左移（drain 后 len=1024，下一次 push 又触发 drain）——
/// 每 push 一次 O(1024) 搬运。q15 每事件 8 个 distinct branch 收集 → 每事件
/// 8×32KB memmove，占 q15 CPU 的 88%（macOS sample 2026-08-22 实测，q15 10M
/// EPS 37k 的主因）。
#[must_use = "被裁减掉的首值必须交给 PinnedFirst 钉扎（warp-fusion#100）"]
pub(super) fn push_capped(values: &mut VecDeque<Value>, value: Value) -> Option<Value> {
    values.push_back(value);
    if values.len() > MAX_TRACKED_FIELD_VALUES {
        return values.pop_front(); // O(1)：保留最近 MAX 个（与旧 drain 语义一致）
    }
    None
}

pub(super) fn collect_alias_event<E: FieldSource>(
    event: &E,
    alias_state: &mut AliasState,
    tracked_fields: Option<&HashSet<String>>,
) {
    alias_state.count += 1;
    if let Some(fields) = tracked_fields {
        for field_name in fields {
            if let Some(value) = event.field_value(field_name.as_str()) {
                alias_state.push_field_value(field_name.as_str(), value);
            }
        }
    } else {
        for field_name in event.field_names() {
            if let Some(value) = event.field_value(field_name) {
                alias_state.push_field_value(field_name, value);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Branch field extraction
// ---------------------------------------------------------------------------

pub(super) fn extract_branch_field<E: FieldSource>(
    event: &E,
    field: &Option<FieldSelector>,
) -> Option<Value> {
    match field {
        Some(FieldSelector::Dot(name)) | Some(FieldSelector::Bracket(name)) => {
            event.field_value(name.as_str())
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
            if !bs
                .distinct_set
                .get_or_insert_with(|| Box::new(crate::value::EngineHashSet::default()))
                .insert(key)
            {
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
    val_acc: &mut Option<Box<Value>>,
    is_min: bool,
) {
    if let Some(v) = fval
        && ((is_min && v < *num_acc) || (!is_min && v > *num_acc))
    {
        *num_acc = v;
    }
    if let Some(val) = field_value {
        let replace = match val_acc.as_deref() {
            None => true,
            Some(cur) => {
                let ord = value_ordering(val, cur);
                if is_min { ord.is_lt() } else { ord.is_gt() }
            }
        };
        if replace {
            *val_acc = Some(Box::new(val.clone()));
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
            if let (Some(val), Some(threshold_val)) = (
                bs.min_val.as_deref(),
                try_eval_expr_to_value(&agg.threshold),
            ) {
                compare_value_threshold(agg.cmp, val, &threshold_val)
            } else {
                false
            }
        }
        Measure::Max => {
            if let (Some(val), Some(threshold_val)) = (
                bs.max_val.as_deref(),
                try_eval_expr_to_value(&agg.threshold),
            ) {
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
    use super::super::types::Event;
    use super::super::types::Value;
    use super::*;

    fn event_with(field: &str, value: i64) -> Event {
        let mut fields = EngineHashMap::default();
        fields.insert(field.to_string().into(), Value::Number(value as f64));
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

        let values = state
            .field_values
            .as_deref()
            .and_then(|m| m.get("dip"))
            .expect("dip collected");
        assert_eq!(values.len(), MAX_TRACKED_FIELD_VALUES);
        // The retained window is the most recent entries; `.last()` is the latest event,
        // which is what yield field resolution (`e.dip`) reads.
        assert_eq!(values.back(), Some(&Value::Number((over - 1) as f64)));
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
            .as_deref()
            .and_then(|m| m.get("event_id"))
            .expect("event_id collected");
        assert_eq!(values.len(), MAX_TRACKED_FIELD_VALUES);
        assert!(!values.contains(&Value::Number(0.0)));
        assert_eq!(values.back(), Some(&Value::Number((over - 1) as f64)));
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

        let values = bs
            .field_values
            .as_deref()
            .and_then(|m| m.get("dport"))
            .expect("dport collected");
        assert_eq!(values.len(), MAX_TRACKED_FIELD_VALUES);
        // `.last()` — the value yield field resolution reads — stays correct.
        assert_eq!(values.back(), Some(&Value::Number((over - 1) as f64)));
    }

    #[test]
    fn collect_event_fields_pins_first_value_for_series() {
        // warp-fusion#100：裁剪丢弃的最早样本必须钉扎，否则 `first()` 漂移。
        let mut bs = BranchState::new();
        let over = MAX_TRACKED_FIELD_VALUES * 2 + 3;
        for i in 0..over as i64 {
            collect_event_fields(
                &event_with("dport", i),
                &mut bs,
                None,
                &HashSet::new(),
                None,
            );
        }

        let series = bs.field_series();
        let values = series.get("dport").expect("dport series");
        // `[首值] ++ 最近 MAX 个`：长度上界 1025，首值 / 尾值均正确。
        assert_eq!(values.len(), MAX_TRACKED_FIELD_VALUES + 1);
        assert_eq!(values.first(), Some(&Value::Number(0.0)));
        assert_eq!(values.last(), Some(&Value::Number((over - 1) as f64)));
        // 原始环形队列仍只保留最近 MAX 个（内存上界不变）。
        assert_eq!(
            bs.field_values
                .as_deref()
                .and_then(|m| m.get("dport"))
                .map(|v| v.len()),
            Some(MAX_TRACKED_FIELD_VALUES)
        );
    }

    #[test]
    fn collect_alias_event_pins_first_value_for_series() {
        let mut state = AliasState::new();
        let over = MAX_TRACKED_FIELD_VALUES + 1;
        for i in 0..over as i64 {
            collect_alias_event(&event_with("event_id", i), &mut state, None);
        }

        let series = state.field_series();
        let values = series.get("event_id").expect("event_id series");
        assert_eq!(values.len(), MAX_TRACKED_FIELD_VALUES + 1);
        assert_eq!(values.first(), Some(&Value::Number(0.0)));
        assert_eq!(values.last(), Some(&Value::Number(over as f64 - 1.0)));
        // count 不受钉扎影响。
        assert_eq!(state.count, over as u64);
    }

    #[test]
    fn series_without_trim_is_unchanged_by_pinning() {
        // 未裁剪 → 无钉扎槽，序列与历史逐位一致（无额外首值 / 无重复）。
        let mut bs = BranchState::new();
        for i in 0..3i64 {
            bs.push_field_value("dport", Value::Number(i as f64));
            bs.push_collected(Value::Number(i as f64));
        }

        assert!(bs.pinned_first.is_none());
        assert_eq!(
            bs.field_series().get("dport").cloned(),
            Some(vec![0.0, 1.0, 2.0].into_iter().map(Value::Number).collect())
        );
        assert_eq!(
            bs.collected_series(),
            vec![0.0, 1.0, 2.0]
                .into_iter()
                .map(Value::Number)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn push_collected_pins_first_dropped_value() {
        let mut bs = BranchState::new();
        let over = MAX_TRACKED_FIELD_VALUES + 1;
        for i in 0..over as i64 {
            bs.push_collected(Value::Number(i as f64));
        }

        let series = bs.collected_series();
        assert_eq!(series.len(), MAX_TRACKED_FIELD_VALUES + 1);
        assert_eq!(series.first(), Some(&Value::Number(0.0)));
        assert_eq!(series.last(), Some(&Value::Number(over as f64 - 1.0)));
    }

    #[test]
    fn pinned_first_value_is_not_replaced_by_later_trims() {
        // 首次裁剪记录首值后，后续裁剪丢弃的值更晚，不得覆盖。
        let mut bs = BranchState::new();
        for i in 0..(MAX_TRACKED_FIELD_VALUES * 3) as i64 {
            bs.push_collected(Value::Number(i as f64));
        }

        assert_eq!(
            bs.collected_series().first(),
            Some(&Value::Number(0.0)),
            "首值必须始终是最早样本"
        );
    }

    #[test]
    fn collect_alias_event_tracks_only_requested_fields() {
        let mut state = AliasState::new();
        let mut fields = EngineHashMap::default();
        fields.insert("sip".into(), Value::Str("10.0.0.1".into()));
        fields.insert("dport".into(), Value::Number(443.0));
        let event = Event { fields };
        let tracked = HashSet::from(["sip".to_string()]);

        collect_alias_event(&event, &mut state, Some(&tracked));

        assert_eq!(state.count, 1);
        assert!(
            state
                .field_values
                .as_deref()
                .is_some_and(|m| m.contains_key("sip"))
        );
        assert!(
            !state
                .field_values
                .as_deref()
                .is_some_and(|m| m.contains_key("dport"))
        );
    }

    #[test]
    fn collect_event_fields_tracks_requested_fields_and_branch_field() {
        let mut bs = BranchState::new();
        let mut fields = EngineHashMap::default();
        fields.insert("sip".into(), Value::Str("10.0.0.1".into()));
        fields.insert("dport".into(), Value::Number(443.0));
        fields.insert("bytes".into(), Value::Number(100.0));
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

        assert!(
            bs.field_values
                .as_deref()
                .is_some_and(|m| m.contains_key("sip"))
        );
        assert!(
            bs.field_values
                .as_deref()
                .is_some_and(|m| m.contains_key("dport"))
        );
        assert!(
            !bs.field_values
                .as_deref()
                .is_some_and(|m| m.contains_key("bytes"))
        );
    }

    #[test]
    fn collect_event_fields_tracks_plain_fields() {
        let mut bs = BranchState::new();
        let mut fields = EngineHashMap::default();
        fields.insert("sip".into(), Value::Str("10.0.0.1".into()));
        fields.insert("dport".into(), Value::Number(443.0));
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

        assert!(
            bs.field_values
                .as_deref()
                .is_some_and(|m| m.contains_key("sip"))
        );
        assert!(
            bs.field_values
                .as_deref()
                .is_some_and(|m| m.contains_key("dport"))
        );
    }

    #[test]
    fn update_measure_caps_collected_values_and_preserves_count() {
        let mut bs = BranchState::new();
        let over = MAX_TRACKED_FIELD_VALUES * 5;
        for i in 0..over as i64 {
            update_measure(&Measure::Count, &Some(Value::Number(i as f64)), &mut bs);
            // F9：collected_values 收集移到调用方（gate = needs_field_history），
            // update_measure 自身不再收集——测试补 push 以保持对 cap 的断言。
            // 走 `push_collected`（而非裸 `push_capped`）以免演示绕过首值钉扎的写法。
            bs.push_collected(Value::Number(i as f64));
        }

        assert_eq!(
            bs.collected_values.as_deref().map(|v| v.len()).unwrap_or(0),
            MAX_TRACKED_FIELD_VALUES
        );
        assert_eq!(
            bs.collected_values.as_deref().and_then(|v| v.back()),
            Some(&Value::Number((over - 1) as f64))
        );
        // Threshold accumulators still see every event; only the raw value list is capped.
        assert_eq!(bs.count, over as u64);
        // 裁剪丢弃的首值已被钉扎（同一序列的 first 语义来源）。
        assert_eq!(
            bs.collected_series().first(),
            Some(&Value::Number(0.0)),
            "collected_values 裁剪后的首项必须是钉扎的最早样本"
        );
    }

    #[test]
    fn push_capped_reports_dropped_head_and_keeps_bounded_tail() {
        // `push_capped` 的返回值是整套钉扎机制的根基：必须是本次被丢弃的首值，
        // 且只在真正发生裁剪时返回。
        let mut values: VecDeque<Value> = VecDeque::new();
        for i in 0..MAX_TRACKED_FIELD_VALUES as i64 {
            assert_eq!(
                push_capped(&mut values, Value::Number(i as f64)),
                None,
                "上限内不得报告丢弃（第 {i} 次）"
            );
        }
        assert_eq!(values.len(), MAX_TRACKED_FIELD_VALUES);
        assert_eq!(
            push_capped(&mut values, Value::Number(MAX_TRACKED_FIELD_VALUES as f64)),
            Some(Value::Number(0.0)),
            "首次裁剪报告最早样本"
        );
        assert_eq!(values.len(), MAX_TRACKED_FIELD_VALUES);
        assert_eq!(
            values.back(),
            Some(&Value::Number(MAX_TRACKED_FIELD_VALUES as f64))
        );
        assert_eq!(
            push_capped(
                &mut values,
                Value::Number(MAX_TRACKED_FIELD_VALUES as f64 + 1.0)
            ),
            Some(Value::Number(1.0)),
            "后续裁剪逐次报告当时的首项"
        );
    }

    #[test]
    fn field_pins_are_independent_per_field() {
        // 参差不齐的字段到达节奏：被裁剪的字段有独立钉扎，样本稀少的字段既无钉扎
        // 也不丢首值（`PinnedFirst.fields` 按字段名分槽，不是单一槽位）。
        let mut state = AliasState::new();
        let over = MAX_TRACKED_FIELD_VALUES * 2 + 7;
        for i in 0..over as i64 {
            state.push_field_value("event_id", Value::Number(i as f64));
            if i < 3 {
                state.push_field_value("rare", Value::Str(format!("rare-{i}").into()));
            }
        }

        let series = state.field_series();
        let dense = series.get("event_id").expect("event_id series");
        assert_eq!(dense.len(), MAX_TRACKED_FIELD_VALUES + 1);
        assert_eq!(dense.first(), Some(&Value::Number(0.0)));
        assert_eq!(dense.last(), Some(&Value::Number((over - 1) as f64)));

        let rare = series.get("rare").expect("rare series");
        assert_eq!(
            rare,
            &vec![
                Value::Str("rare-0".into()),
                Value::Str("rare-1".into()),
                Value::Str("rare-2".into()),
            ],
            "样本稀少的字段必须完整保留（无钉扎、无裁剪）"
        );

        let pins = state.pinned_first.as_deref().expect("稠密字段应产生钉扎槽");
        assert!(pins.fields.contains_key("event_id"));
        assert!(
            !pins.fields.contains_key("rare"),
            "未被裁剪的字段不得凭空产生钉扎值"
        );
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
            &Some(Value::Str("1".into())),
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
