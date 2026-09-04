//! Coverage-extra tests for the `match_engine::cep` core: the
//! submodule branches the feature suites only reach indirectly — `RollingStats`
//! (ewma/median), `extract_key` key-map fallbacks, step progress threshold
//! debug strings, cross-type measure ordering, expiry-heap edge cases
//! (stale candidates / session re-queue / dedup), raw-conv / FailRule
//! rate-limit close paths, mask-driven guard evaluation, and conv mixed-type
//! sorting.
//!
//! Lives inside the module so it can reach the private submodules directly.
//! Only test code lives here — no production logic is modified.
use std::collections::HashSet;
use std::time::Duration;

use arrow::array::BooleanArray;
use wf_lang::ast::{
    BinOp, CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure, PathSegment, Transform,
};
use wf_lang::plan::{
    AggPlan, BranchPlan, ConvChainPlan, ConvOpPlan, ConvPlan, ExceedAction, KeyMapPlan, LimitsPlan,
    MatchPlan, RateSpec, SeqPlan, SeqSkipPlan, SeqStepPlan, SortKeyPlan, WindowSpec,
};

use super::key::{InstanceKey, ScopeKey, extract_key, field_ref_leaf_name};
use super::state::BranchState;
use super::step::{apply_transforms, compute_measure, update_measure};
use super::types::{CloseOutput, CloseReason, RollingStats, StepData, Value, WindowLookup};
use super::{CepStateMachine, EngineHashMap, Event, StepResult, apply_conv, close_is_qualified};
use crate::cep::{apply_conv_filtered, throttle_allows};
use crate::masks::GuardMasks;

fn event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn str_val(s: &str) -> Value {
    Value::Str(s.into())
}

fn count_ge(n: f64) -> AggPlan {
    AggPlan {
        transforms: vec![],
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        threshold: Expr::Number(n),
    }
}

fn simple_key(name: &str) -> FieldRef {
    FieldRef::Simple(name.to_string())
}

fn simple_plan(keys: Vec<FieldRef>, steps: Vec<wf_lang::plan::StepPlan>) -> MatchPlan {
    MatchPlan {
        keys,
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps: steps,
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

fn branch(source: &str, agg: AggPlan) -> BranchPlan {
    BranchPlan {
        label: None,
        source: source.to_string(),
        field: None,
        guard: None,
        agg,
    }
}

fn step(branches: Vec<BranchPlan>) -> wf_lang::plan::StepPlan {
    wf_lang::plan::StepPlan { branches }
}

fn plan_with_close(
    keys: Vec<FieldRef>,
    event_steps: Vec<wf_lang::plan::StepPlan>,
    close_steps: Vec<wf_lang::plan::StepPlan>,
    window_dur: Duration,
) -> MatchPlan {
    MatchPlan {
        keys,
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(window_dur),
        event_steps,
        close_steps,
        close_mode: CloseMode::And,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

// ===========================================================================
// types.rs — RollingStats (ewma / median / mean)
// ===========================================================================

#[test]
fn rolling_stats_mean_stddev_and_deviation() {
    // Default method is "mean".
    let mut stats = RollingStats::new();
    assert_eq!(stats.deviation(5.0), 0.0, "no samples → 0");
    stats.update(10.0);
    // Single sample: stddev undefined (count < 2) → deviation 0.
    assert_eq!(stats.deviation(20.0), 0.0);
    stats.update(20.0);
    // mean = 15, stddev = 5.
    assert_eq!(stats.deviation(15.0), 0.0);
    assert!((stats.deviation(20.0) - 1.0).abs() < 1e-9);
    assert!((stats.deviation(10.0) - (-1.0)).abs() < 1e-9);
}

#[test]
fn rolling_stats_ewma_method() {
    let mut stats = RollingStats::new_with_method("ewma");
    // Baseline 0 → deviation 0 before any update.
    assert_eq!(stats.deviation(3.0), 0.0);
    stats.update(1.0); // ewma = 1.0
    stats.update(2.0); // ewma = 0.3*2 + 0.7*1 = 1.3
    let d = stats.deviation(2.6);
    assert!(
        (d - 1.0).abs() < 1e-9,
        "ewma deviation should be 1.0, got {d}"
    );
    // Relative deviation, not z-score.
    let d2 = stats.deviation(1.3);
    assert!(d2.abs() < 1e-9);
}

#[test]
fn rolling_stats_median_method() {
    let mut stats = RollingStats::new_with_method("median");
    assert_eq!(stats.deviation(9.0), 0.0, "empty median → 0 baseline");
    stats.update(1.0);
    stats.update(2.0);
    // Even count → average of the two middle values.
    let d = stats.deviation(1.5);
    assert!(d.abs() < 1e-9);
    stats.update(3.0);
    // Odd count → middle value 2.
    let d = stats.deviation(2.0);
    assert!(d.abs() < 1e-9);
}

#[test]
fn rolling_stats_median_filters_nan_and_trims_to_1000() {
    let mut stats = RollingStats::new_with_method("median");
    // NaN samples must not panic partial_cmp; they are filtered from the sort.
    stats.update(f64::NAN);
    stats.update(f64::NAN);
    // All-NaN median → 0 baseline.
    assert_eq!(stats.deviation(1.0), 0.0);

    let mut stats = RollingStats::new_with_method("median");
    for i in 0..1200 {
        stats.update(i as f64);
    }
    // Median of the last 1000 values [200.0..1200.0): even count → the average
    // of the two middle values 699.0 and 700.0.
    let d = stats.deviation(699.5);
    assert!(d.abs() < 1e-9, "median after trim should be 699.5");
    let d = stats.deviation(699.0);
    assert!((d - (699.0 - 699.5) / 699.5).abs() < 1e-9);
}

// ===========================================================================
// key.rs — extract_key with key_map / leaf names
// ===========================================================================

#[test]
fn extract_key_key_map_fallback_and_partial_rejection() {
    let km = vec![
        KeyMapPlan {
            logical_name: "ip".to_string(),
            source_alias: "login".to_string(),
            source_field: "src_ip".to_string(),
        },
        KeyMapPlan {
            logical_name: "ip".to_string(),
            source_alias: "dns".to_string(),
            source_field: "client_ip".to_string(),
        },
    ];

    // Alias-specific mapping hit.
    let e = event(vec![("src_ip", str_val("10.0.0.1"))]);
    assert_eq!(
        extract_key(&e, &[simple_key("ip")], Some(&km), "login"),
        Some(vec![str_val("10.0.0.1")])
    );
    // Unmapped alias → fall back to the logical field name.
    let e = event(vec![("ip", str_val("10.0.0.2"))]);
    assert_eq!(
        extract_key(&e, &[simple_key("ip")], Some(&km), "other"),
        Some(vec![str_val("10.0.0.2")])
    );
    // Mapping present but the mapped value missing → fall back to the logical
    // field name on the same alias.
    let e = event(vec![("ip", str_val("10.0.0.3"))]);
    assert_eq!(
        extract_key(&e, &[simple_key("ip")], Some(&km), "login"),
        Some(vec![str_val("10.0.0.3")])
    );
    // Partial keys rejected: one logical key resolves, the other does not.
    let km2 = vec![
        KeyMapPlan {
            logical_name: "a".to_string(),
            source_alias: "s".to_string(),
            source_field: "x".to_string(),
        },
        KeyMapPlan {
            logical_name: "b".to_string(),
            source_alias: "s".to_string(),
            source_field: "y".to_string(),
        },
    ];
    let e = event(vec![("x", num(1.0))]);
    assert_eq!(
        extract_key(&e, &[simple_key("a"), simple_key("b")], Some(&km2), "s"),
        None,
        "partial key resolution must be rejected"
    );
    // Empty logical names + empty keys → shared instance.
    assert_eq!(
        extract_key(&e, &[], Some(&[]), "s"),
        Some(vec![]),
        "empty key_map + empty keys → shared instance key"
    );
}

#[test]
fn field_ref_leaf_name_variants() {
    assert_eq!(
        field_ref_leaf_name(&FieldRef::Simple("a".into())),
        Some("a")
    );
    assert_eq!(
        field_ref_leaf_name(&FieldRef::Qualified("e".into(), "a".into())),
        Some("a")
    );
    assert_eq!(
        field_ref_leaf_name(&FieldRef::Bracketed("e".into(), "a.b".into())),
        Some("a.b")
    );
    // Path: leaf member after the root; trailing index falls back to the last field.
    assert_eq!(
        field_ref_leaf_name(&FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("root".into()),
                PathSegment::Field("mid".into()),
                PathSegment::Index(0),
            ],
        }),
        Some("mid")
    );
    assert_eq!(
        field_ref_leaf_name(&FieldRef::Path {
            alias: "e".into(),
            segments: vec![PathSegment::Field("root".into())],
        }),
        Some("root")
    );
}

// ===========================================================================
// step.rs — progress threshold debug strings + measure extremes
// ===========================================================================

#[test]
fn progress_threshold_debug_strings_cover_expr_shapes() {
    let cases: Vec<(Expr, &str)> = vec![
        (Expr::Number(3.0), "3"),
        (Expr::StringLit("x".into()), "\"x\""),
        (Expr::Bool(true), "true"),
        (Expr::Neg(Box::new(Expr::Number(1.0))), "-1"),
        (
            Expr::BinOp {
                op: BinOp::Add,
                left: Box::new(Expr::Number(1.0)),
                right: Box::new(Expr::Number(2.0)),
            },
            "1 + 2",
        ),
        (
            Expr::BinOp {
                op: BinOp::Sub,
                left: Box::new(Expr::Number(1.0)),
                right: Box::new(Expr::Number(2.0)),
            },
            "1 - 2",
        ),
        (
            Expr::BinOp {
                op: BinOp::Mul,
                left: Box::new(Expr::Number(1.0)),
                right: Box::new(Expr::Number(2.0)),
            },
            "1 * 2",
        ),
        (
            Expr::BinOp {
                op: BinOp::Div,
                left: Box::new(Expr::Number(1.0)),
                right: Box::new(Expr::Number(2.0)),
            },
            "1 / 2",
        ),
        (
            Expr::BinOp {
                op: BinOp::Mod,
                left: Box::new(Expr::Number(5.0)),
                right: Box::new(Expr::Number(2.0)),
            },
            "5 % 2",
        ),
        // Nested arithmetic: the inner binop renders parenthesized.
        (
            Expr::BinOp {
                op: BinOp::Add,
                left: Box::new(Expr::BinOp {
                    op: BinOp::Add,
                    left: Box::new(Expr::Number(1.0)),
                    right: Box::new(Expr::Number(2.0)),
                }),
                right: Box::new(Expr::Number(3.0)),
            },
            "(1 + 2) + 3",
        ),
    ];
    for (threshold, expected) in cases {
        let plan = simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![BranchPlan {
                label: Some("l".into()),
                source: "fail".to_string(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold,
                },
            }])],
        );
        let mut sm = CepStateMachine::new("r".into(), plan, None);
        let e = event(vec![("sip", str_val("10.0.0.1"))]);
        let outcome = sm.advance_at_with_progress("fail", &e, 0, None);
        let progress = outcome.progress.expect("progress captured");
        assert_eq!(progress.threshold, expected);
    }
}

#[test]
fn progress_cmp_symbols_cover_all_operators() {
    for (cmp, symbol) in [
        (CmpOp::Eq, "=="),
        (CmpOp::Ne, "!="),
        (CmpOp::Lt, "<"),
        (CmpOp::Gt, ">"),
        (CmpOp::Le, "<="),
        (CmpOp::Ge, ">="),
    ] {
        let plan = simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![BranchPlan {
                label: Some("l".into()),
                source: "fail".to_string(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp,
                    threshold: Expr::Number(1.0),
                },
            }])],
        );
        let mut sm = CepStateMachine::new("r".into(), plan, None);
        let e = event(vec![("sip", str_val("10.0.0.1"))]);
        let outcome = sm.advance_at_with_progress("fail", &e, 0, None);
        let progress = outcome.progress.expect("progress captured");
        assert_eq!(progress.cmp, symbol);
    }
}

#[test]
fn update_measure_cross_type_extremes_use_value_ordering() {
    // min/max with mixed-type fields: the numeric accumulator tracks finite
    // numbers only; the Value accumulator uses cross-type ordering
    // (Number < Str < Bool < Array < Object).
    let mut bs = BranchState::new();
    // Min: Number 5 first, then a smaller number, then a Str (never wins).
    update_measure(&Measure::Min, &Some(num(5.0)), &mut bs);
    assert_eq!(bs.min, 5.0);
    assert_eq!(bs.min_val.as_deref(), Some(&num(5.0)));
    update_measure(&Measure::Min, &Some(num(2.0)), &mut bs);
    assert_eq!(bs.min, 2.0);
    update_measure(&Measure::Min, &Some(str_val("z")), &mut bs);
    // Str > Number in the ordering → min stays 2.
    assert_eq!(bs.min, 2.0);
    assert_eq!(bs.min_val.as_deref(), Some(&num(2.0)));

    // Max: Str beats Number, Bool beats Str, Array beats Bool, Object beats Array.
    let mut bs = BranchState::new();
    update_measure(&Measure::Max, &Some(num(1.0)), &mut bs);
    assert_eq!(bs.max_val.as_deref(), Some(&num(1.0)));
    update_measure(&Measure::Max, &Some(str_val("a")), &mut bs);
    assert_eq!(bs.max_val.as_deref(), Some(&str_val("a")));
    update_measure(&Measure::Max, &Some(Value::Bool(true)), &mut bs);
    assert_eq!(bs.max_val.as_deref(), Some(&Value::Bool(true)));
    let arr = Value::Array(vec![num(1.0)]);
    update_measure(&Measure::Max, &Some(arr.clone()), &mut bs);
    assert_eq!(bs.max_val.as_deref(), Some(&arr));
    let mut obj = EngineHashMap::default();
    obj.insert("k".into(), num(1.0));
    let obj = Value::Object(obj);
    update_measure(&Measure::Max, &Some(obj.clone()), &mut bs);
    assert_eq!(bs.max_val.as_deref(), Some(&obj));
    // Same-type extremes still compare numerically / lexicographically.
    let mut bs = BranchState::new();
    update_measure(&Measure::Max, &Some(str_val("b")), &mut bs);
    update_measure(&Measure::Max, &Some(str_val("a")), &mut bs);
    assert_eq!(bs.max_val.as_deref(), Some(&str_val("b")));
    // NaN never replaces an existing extreme (partial_cmp → Equal).
    let mut bs = BranchState::new();
    update_measure(&Measure::Max, &Some(num(1.0)), &mut bs);
    update_measure(&Measure::Max, &Some(num(f64::NAN)), &mut bs);
    assert_eq!(bs.max_val.as_deref(), Some(&num(1.0)));
}

#[test]
fn update_measure_avg_and_sum_ignore_non_numeric() {
    let mut bs = BranchState::new();
    // Avg with a non-numeric field: no avg accumulation, compute → 0.0.
    update_measure(&Measure::Avg, &Some(str_val("n/a")), &mut bs);
    assert_eq!(bs.avg_count, 0);
    assert_eq!(compute_measure(&Measure::Avg, &bs), 0.0);
    // Sum with a non-numeric field: sum stays 0.
    let mut bs = BranchState::new();
    update_measure(&Measure::Sum, &Some(str_val("n/a")), &mut bs);
    assert_eq!(bs.sum, 0.0);
    // F9：collected_values 收集移到调用方（gate = needs_field_history），
    // update_measure 自身不再记录原始值——此处不再断言 collected。
    assert_eq!(bs.collected_values.as_deref().map(|q| q.len()), None);
}

#[test]
fn distinct_transform_with_none_field_skips() {
    // `distinct` on a branch with no field → filtered out (None → false).
    let mut bs = BranchState::new();
    let applied = apply_transforms(&[Transform::Distinct], &None, &mut bs);
    assert!(!applied, "distinct with no field value must skip");
    // And it must not have allocated a distinct_set.
    assert!(bs.distinct_set.is_none());
}

// ===========================================================================
// mod.rs — expiry heap edge cases
// ===========================================================================

#[test]
fn scan_expired_stale_candidate_is_skipped() {
    // A candidate whose instance was already removed (e.g. by an explicit
    // close) must be dropped, not produce a phantom CloseOutput.
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &e, 0);
    // Remove the instance via an explicit close → the expiry candidate is stale.
    assert!(
        sm.close(&[str_val("10.0.0.1")], CloseReason::Flush)
            .is_some()
    );
    let outputs = sm.scan_expired_at(61_000_000_000);
    assert!(outputs.is_empty(), "stale candidate must not emit");
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn scan_expired_session_requeues_when_expiry_refreshed() {
    // Session windows refresh expiry as events arrive; a scan before the
    // refreshed expiry must re-queue the key, not close it.
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.window_spec = WindowSpec::Session(Duration::from_secs(60));
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("e", &e, 0);
    // Event at t=50s refreshes expiry to 110s.
    sm.advance_at("e", &e, 50_000_000_000);
    // Scan at t=80s: the candidate (expiry 50s) is stale; current expiry (110s)
    // is beyond the watermark → re-queued, no output.
    let outputs = sm.scan_expired_at(80_000_000_000);
    assert!(outputs.is_empty());
    assert_eq!(
        sm.instance_count(),
        1,
        "session instance survives the sweep"
    );
    // Scan past the refreshed expiry → closed.
    let outputs = sm.scan_expired_at(111_000_000_000);
    assert_eq!(outputs.len(), 1);
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn fire_reset_reuses_single_expiry_candidate() {
    // A count>=1 rule fires and resets on every event: each fire re-pushes an
    // expiry candidate for the same key. The pending_expiry dedup must keep a
    // single heap entry — the sweep produces exactly one output, not two.
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    for t in 0..3 {
        assert!(matches!(
            sm.advance_at("e", &e, t * 1_000_000_000),
            StepResult::Matched(_)
        ));
    }
    // The reset moved created_at forward, so the single candidate re-queues
    // at the refreshed expiry on the first sweep (no output yet), then closes
    // exactly once on the next.
    assert!(sm.scan_expired_at(301_000_000_000).is_empty());
    let outputs = sm.scan_expired_at(303_000_000_000);
    assert_eq!(outputs.len(), 1, "deduped candidate → one close output");
}

#[test]
fn scan_expired_at_skip_non_alerting_omits_unqualified() {
    // And-mode rule with no close steps: an instance that never matched has
    // event_ok=false → the skip variant drops the close entirely, while the
    // full-close scan still returns it (event_ok=false, close_ok=true).
    let plan = || {
        let mut plan = simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(3.0))])],
        );
        plan.close_mode = CloseMode::And;
        plan
    };
    let mut sm = CepStateMachine::new("r".into(), plan(), None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("e", &e, 0); // one event, threshold 3 → never matches

    // Non-skip variant returns the unqualified close.
    let full = sm.scan_expired_at(301_000_000_000);
    assert_eq!(full.len(), 1);
    assert!(!full[0].event_ok);
    assert!(full[0].close_ok, "no close steps → close_ok stays true");

    let mut sm = CepStateMachine::new("r".into(), plan(), None);
    sm.advance_at("e", &e, 0);
    let skipped = sm.scan_expired_at_skip_non_alerting(301_000_000_000);
    assert!(skipped.is_empty(), "unqualified instance must be skipped");
    assert_eq!(sm.instance_count(), 0, "instance removed either way");
}

#[test]
fn close_is_qualified_and_rate_limit_close_qualifying_modes() {
    let mk =
        |event_ok: bool, close_ok: bool, mode: CloseMode, close_data: Vec<StepData>| CloseOutput {
            rule_name: "r".into(),
            scope_key: vec![num(1.0)],
            close_reason: CloseReason::Timeout,
            event_ok,
            close_ok,
            close_mode: mode,
            event_emitted: false,
            event_step_data: vec![],
            close_step_data: close_data,
            bind_data: vec![],
            watermark_nanos: 0,
            machine_id: String::new(),
            event_first_time_nanos: 0,
            event_last_time_nanos: 0,
            first_match_time_nanos: None,
            evidence_first_time_nanos: 0,
            evidence_last_time_nanos: 0,
            window_start_time_nanos: 0,
            window_end_time_nanos: 0,
            last_event_nanos: 0,
            row_fields: None,
            row_field_names: None,
        };
    // And mode: needs both event_ok and close_ok.
    assert!(close_is_qualified(&mk(true, true, CloseMode::And, vec![])));
    assert!(!close_is_qualified(&mk(
        true,
        false,
        CloseMode::And,
        vec![]
    )));
    assert!(!close_is_qualified(&mk(
        false,
        true,
        CloseMode::And,
        vec![]
    )));
    // Or mode: needs close_ok AND non-empty close_step_data.
    let data = vec![StepData {
        satisfied_branch_index: 0,
        label: Some("l".into()),
        measure_value: 1.0,
        event_first_time_nanos: Some(0),
        event_last_time_nanos: Some(0),
        collected_values: vec![],
        field_values: EngineHashMap::default(),
    }];
    assert!(close_is_qualified(&mk(
        true,
        true,
        CloseMode::Or,
        data.clone()
    )));
    assert!(!close_is_qualified(&mk(true, true, CloseMode::Or, vec![])));
    assert!(!close_is_qualified(&mk(
        true,
        false,
        CloseMode::Or,
        data.clone()
    )));
}

// ===========================================================================
// mod.rs — conv filtering & rate-limit helpers
// ===========================================================================

#[test]
fn apply_conv_filtered_separates_qualifying_and_appends_rest() {
    let mk = |event_ok: bool, close_ok: bool, label: &str, _measure: f64| CloseOutput {
        rule_name: "r".into(),
        scope_key: vec![str_val(label)],
        close_reason: CloseReason::Timeout,
        event_ok,
        close_ok,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 0,
        machine_id: String::new(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        last_event_nanos: 0,
        row_fields: None,
        row_field_names: None,
    };
    // Two qualifying outputs (sorted desc by scope key string) + one non-qualifying.
    let outputs = vec![
        mk(true, true, "b", 2.0),
        mk(false, true, "c", 3.0), // non-qualifying → appended back unchanged
        mk(true, true, "a", 1.0),
    ];
    let conv = ConvPlan {
        chains: vec![ConvChainPlan {
            ops: vec![
                ConvOpPlan::Sort(vec![SortKeyPlan {
                    expr: Expr::Field(simple_key("label")),
                    descending: true,
                }]),
                ConvOpPlan::Top(1),
            ],
        }],
    };
    // label field maps scope_key[0] → "label" via build_eval_context.
    let keys = vec![FieldRef::Simple("label".into())];
    let result = apply_conv_filtered(outputs, Some(&conv), &keys);
    assert_eq!(
        result.len(),
        2,
        "top(1) over qualifying + non-qualifying back"
    );
    assert_eq!(
        result[0].scope_key,
        vec![str_val("b")],
        "descending sort keeps 'b'"
    );
    assert_eq!(
        result[1].scope_key,
        vec![str_val("c")],
        "non-qualifying appended unchanged"
    );
    assert!(!result[1].event_ok);

    // No conv plan → passthrough (order preserved).
    let outputs = vec![mk(true, true, "x", 1.0), mk(false, true, "y", 2.0)];
    let result = apply_conv_filtered(outputs, None, &keys);
    assert_eq!(result.len(), 2);

    // No qualifying outputs → the non-qualifying batch is returned as-is.
    let outputs = vec![mk(false, true, "y", 2.0)];
    let result = apply_conv_filtered(outputs, Some(&conv), &keys);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].scope_key, vec![str_val("y")]);
}

#[test]
fn conv_apply_sorts_mixed_types_and_dedups_none() {
    // Scope-key values of mixed types sort by the cross-type ordering
    // (Number < Str < Bool < Array < Object), and a dedup over a missing key
    // field collapses to "__none__".
    let mk = |scope: Vec<Value>| CloseOutput {
        rule_name: "r".into(),
        scope_key: scope,
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
        close_step_data: vec![],
        bind_data: vec![],
        watermark_nanos: 0,
        machine_id: String::new(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        last_event_nanos: 0,
        row_fields: None,
        row_field_names: None,
    };
    let keys = vec![simple_key("k")];
    let plan = ConvPlan {
        chains: vec![ConvChainPlan {
            ops: vec![ConvOpPlan::Sort(vec![SortKeyPlan {
                expr: Expr::Field(simple_key("k")),
                descending: false,
            }])],
        }],
    };
    let outputs = vec![
        mk(vec![str_val("s")]),
        mk(vec![num(1.0)]),
        mk(vec![Value::Bool(true)]),
    ];
    let sorted = apply_conv(&plan, &keys, outputs);
    assert_eq!(
        sorted
            .iter()
            .map(|o| o.scope_key.clone())
            .collect::<Vec<_>>(),
        vec![vec![num(1.0)], vec![str_val("s")], vec![Value::Bool(true)]],
        "Number < Str < Bool ascending (compare_values cross-type order)"
    );

    // Dedup over a missing key field → both collapse to "__none__".
    let dedup = ConvPlan {
        chains: vec![ConvChainPlan {
            ops: vec![ConvOpPlan::Dedup(Expr::Field(simple_key("ghost")))],
        }],
    };
    let outputs = vec![mk(vec![str_val("a")]), mk(vec![str_val("b")])];
    let deduped = apply_conv(&dedup, &keys, outputs);
    assert_eq!(deduped.len(), 1, "both rows have no 'ghost' → one survives");

    // Where over a scope-key field: only a Bool(true) value passes.
    let where_plan = ConvPlan {
        chains: vec![ConvChainPlan {
            ops: vec![ConvOpPlan::Where(Expr::Field(simple_key("k")))],
        }],
    };
    let outputs = vec![mk(vec![Value::Bool(true)]), mk(vec![num(1.0)])];
    let filtered = apply_conv(&where_plan, &keys, outputs);
    assert_eq!(filtered.len(), 1, "Bool(true) passes; Number dropped");
    assert_eq!(filtered[0].scope_key, vec![Value::Bool(true)]);
}

#[test]
fn throttle_allows_legacy_window_rotation() {
    // Legacy per-machine sliding window: count 2 per 60s.
    let rate = RateSpec {
        count: 2,
        per: Duration::from_secs(60),
    };
    let shared = None;
    let mut count = 0u64;
    let mut start = 0i64;
    assert!(throttle_allows(
        &shared, &mut count, &mut start, 1_000, &rate
    ));
    assert!(throttle_allows(
        &shared, &mut count, &mut start, 2_000, &rate
    ));
    assert!(!throttle_allows(
        &shared, &mut count, &mut start, 3_000, &rate
    ));
    // Window rotation after `per` elapses resets the counter.
    assert!(throttle_allows(
        &shared,
        &mut count,
        &mut start,
        61_000_000_000,
        &rate
    ));
    assert_eq!(count, 1);
    assert_eq!(start, 61_000_000_000);
}

// ===========================================================================
// mod.rs — rate limiting on the close path
// ===========================================================================

#[test]
fn rate_limit_close_raw_conv_mode_skips_inline_throttle() {
    // P2c: a shard in raw-conv mode must bypass the inline rate limit (the
    // conv stage applies it to the aggregated batch instead).
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
        Duration::from_secs(60),
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
            count: 1,
            per: Duration::from_secs(60),
        }),
        on_exceed: ExceedAction::Throttle,
        disk_provider: None,
        max_disk_bytes: None,
    };
    let mut sm = CepStateMachine::with_limits("r".into(), plan, None, Some(limits));
    sm.set_raw_conv_mode();
    assert!(sm.raw_conv_mode());

    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 0);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    sm.advance_at("req", &e2, 1_000_000_000);
    sm.advance_at("c", &e2, 1_000_000_000);
    let outputs = sm.scan_expired_at(61_000_000_000);
    assert_eq!(outputs.len(), 2);
    assert!(
        outputs.iter().all(|o| o.close_ok),
        "raw-conv mode must not throttle inline (both closes kept)"
    );
}

#[test]
fn rate_limit_close_fail_rule_latches_and_rejects() {
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
        Duration::from_secs(60),
    );
    let limits = LimitsPlan {
        max_memory_bytes: None,
        max_instances: None,
        max_throttle: Some(RateSpec {
            count: 1,
            per: Duration::from_secs(60),
        }),
        on_exceed: ExceedAction::FailRule,
        disk_provider: None,
        max_disk_bytes: None,
    };
    let mut sm = CepStateMachine::with_limits("r".into(), plan, None, Some(limits));
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    // Instance 1 fires and expires within the throttle window.
    sm.advance_at("req", &e, 0);
    sm.advance_at("c", &e, 0);
    let outputs = sm.scan_expired_at(61_000_000_000);
    assert_eq!(outputs.len(), 1);
    assert!(outputs[0].close_ok, "first close within the window emits");

    // A second close in the same window hits the throttle → FailRule latches
    // and suppresses the output.
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    sm.advance_at("req", &e2, 0);
    sm.advance_at("c", &e2, 0);
    let outputs = sm.scan_expired_at(61_000_000_000);
    assert_eq!(outputs.len(), 1);
    assert!(
        !outputs[0].close_ok,
        "FailRule suppresses the throttled close"
    );
    // The latch rejects all future events.
    assert_eq!(
        sm.advance_at("req", &e, 0),
        StepResult::Accumulate,
        "failed rule rejects further events"
    );
}

// ===========================================================================
// mod.rs — event-time extraction / session instance keys / close on session
// ===========================================================================

#[test]
fn extract_event_time_falls_back_on_non_numeric_field() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let sm = CepStateMachine::new("r".into(), plan, Some("ts".into()));
    assert_eq!(sm.time_field(), Some("ts"));
    // Numeric → value.
    assert_eq!(sm.event_time_nanos(&event(vec![("ts", num(42.0))])), 42);
    // String in the time field → 0.
    assert_eq!(sm.event_time_nanos(&event(vec![("ts", str_val("x"))])), 0);
    // Missing field → 0.
    assert_eq!(sm.event_time_nanos(&event(vec![])), 0);
    // Bool in the time field → 0.
    assert_eq!(
        sm.event_time_nanos(&event(vec![("ts", Value::Bool(true))])),
        0
    );
}

#[test]
fn session_window_uses_sliding_style_instance_key() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    plan.window_spec = WindowSpec::Session(Duration::from_secs(60));
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    sm.advance_at("e", &e1, 0);
    sm.advance_at("e", &e1, 10_000_000_000);
    assert_eq!(sm.instance_count(), 1, "same key → one session instance");
    sm.advance_at("e", &e2, 20_000_000_000);
    assert_eq!(sm.instance_count(), 2, "different key → separate instance");
    // close() resolves the session instance by scope key.
    let out = sm.close(&[str_val("10.0.0.1")], CloseReason::Flush);
    assert!(out.is_some());
    assert_eq!(sm.instance_count(), 1);
}

// ===========================================================================
// mod.rs — mask-driven guard evaluation
// ===========================================================================

#[test]
fn advance_with_masks_event_guard_short_circuits_interpreted_eval() {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "e".to_string(),
            field: None,
            guard: Some(Expr::Bool(true)), // would pass interpreted
            agg: count_ge(1.0),
        }])],
    );
    plan.match_mode = MatchMode::Any;
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // Mask says false → guard blocked despite the interpreted true.
    let mut masks = GuardMasks::default();
    masks.insert_event(0, 0, BooleanArray::from(vec![false]));
    assert_eq!(
        sm.advance_at_with_masks("e", &e, 0, None, 0, Some(&masks)),
        StepResult::Accumulate
    );

    // Mask says true → match fires.
    let mut masks = GuardMasks::default();
    masks.insert_event(0, 0, BooleanArray::from(vec![true]));
    assert!(matches!(
        sm.advance_at_with_masks("e", &e, 1_000, None, 0, Some(&masks)),
        StepResult::Matched(_)
    ));
}

#[test]
fn advance_with_masks_close_guard_permissive_semantics() {
    // Close-step accumulation guards are permissive: only an explicit false
    // blocks; null (missing field) passes through. Use the realistic
    // `close_reason == "timeout"` guard: it passes at close time (synthetic
    // event) and is absent during accumulation (real event), so the mask is
    // the only thing that can block accumulation.
    let guard = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("close_reason".into()))),
        right: Box::new(Expr::StringLit("timeout".into())),
    };
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![BranchPlan {
            label: None,
            source: "c".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(1.0),
        }])],
        Duration::from_secs(60),
    );
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    let base = 1_700_000_000_000_000_000i64;

    // Null mask (permissive) → the guard does not block accumulation.
    let mut sm = CepStateMachine::new("r".into(), plan.clone(), None);
    sm.advance_at("req", &e, base);
    let mut masks = GuardMasks::default();
    let nulls = BooleanArray::from(vec![None::<bool>]);
    masks.insert_close(0, 0, nulls);
    sm.advance_at_with_masks("c", &e, base, None, 0, Some(&masks));
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.event_ok);
    assert!(
        out.close_ok,
        "permissive null mask must not block accumulation"
    );

    // Explicit false mask → blocks accumulation → close step unsatisfied.
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    sm.advance_at("req", &e, base);
    let mut masks = GuardMasks::default();
    masks.insert_close(0, 0, BooleanArray::from(vec![false]));
    sm.advance_at_with_masks("c", &e, base, None, 0, Some(&masks));
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert!(out.event_ok);
    assert!(!out.close_ok, "explicit false mask must block accumulation");
}

#[test]
fn advance_with_masks_negation_guard() {
    // scan → not fail (guard) → login: the mask marks the neg guard true,
    // so a fail event violates the chain and suppresses the final fire.
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("scan", count_ge(1.0))]),
            step(vec![branch("login", count_ge(1.0))]),
        ],
    );
    plan.seq = Some(SeqPlan {
        consec: false,
        skip: SeqSkipPlan::PastLast,
        steps: vec![
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("scan", count_ge(1.0)),
            },
            SeqStepPlan {
                neg: true,
                within: Some(Duration::from_secs(300)),
                branch: BranchPlan {
                    label: None,
                    source: "fail".to_string(),
                    field: None,
                    guard: Some(Expr::Bool(false)), // interpreted false
                    agg: count_ge(1.0),
                },
            },
            SeqStepPlan {
                neg: false,
                within: None,
                branch: branch("login", count_ge(1.0)),
            },
        ],
    });
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    assert_eq!(sm.advance_at("scan", &e, 0), StepResult::Advance);
    // Mask says the neg guard is true → violation.
    let mut masks = GuardMasks::default();
    masks.insert_neg(0, 0, BooleanArray::from(vec![true]));
    assert_eq!(
        sm.advance_at_with_masks("fail", &e, 1_000, None, 0, Some(&masks)),
        StepResult::Accumulate
    );
    // The chain is suppressed: login no longer fires.
    assert_eq!(sm.advance_at("login", &e, 2_000), StepResult::Accumulate);
    // The instance was reset for a fresh chain; a re-run still must not fire
    // while the negation state is gone (nothing matches the scan step now).
    assert_eq!(sm.advance_at("login", &e, 3_000), StepResult::Accumulate);
}

// ===========================================================================
// key.rs — InstanceKey / ScopeKey internals (reachable from inside the module)
// ===========================================================================

#[test]
fn instance_key_sliding_fixed_and_scope_matching() {
    let skey = ScopeKey::Int(7);
    let sliding = InstanceKey::sliding(&skey);
    assert_eq!(sliding.bucket_start, None);
    assert_eq!(sliding.scope_key, skey);
    assert!(sliding.matches_scope(&ScopeKey::Int(7)));
    assert!(!sliding.matches_scope(&ScopeKey::Int(8)));
    assert_eq!(sliding.scope_key_values(), vec![num(7.0)]);

    let fixed = InstanceKey::fixed(&skey, 100);
    assert_eq!(fixed.bucket_start, Some(100));
    assert!(fixed.matches_scope(&skey));

    // Str + Float scope keys flatten back to their original Value types.
    let pair = super::key::scope_key_from_values(&[num(1.5), str_val("x")]);
    assert_eq!(
        InstanceKey::sliding(&pair).scope_key_values(),
        vec![num(1.5), str_val("x")],
        "float keys round-trip through canonical bits"
    );
}

// ===========================================================================
// mod.rs — remaining plumbing
// ===========================================================================

#[test]
fn machine_id_extraction_and_fail_latch_on_missing_key() {
    // MACHINE_ID field is read off the event on instance creation; a missing
    // key field skips the event entirely (no instance, no panic).
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let no_key = event(vec![("other", num(1.0))]);
    assert_eq!(sm.advance_at("e", &no_key, 0), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 0);

    // extract_event_str via the state machine helper (machine id extraction).
    let with_machine = event(vec![
        ("sip", str_val("10.0.0.1")),
        (super::super::MACHINE_ID, str_val("10.0.0.9")),
    ]);
    assert_eq!(
        CepStateMachine::extract_event_str(&with_machine, super::super::MACHINE_ID),
        "10.0.0.9"
    );
}

#[test]
fn window_lookup_default_asof_candidates_with_ts_snapshot() {
    // Drive the default `asof_candidates` over a timestamped snapshot and the
    // `join_lookup` fallback with a structured key (never matches).
    use crate::row_views::JoinRow;
    use std::collections::HashMap;
    struct TsLookup {
        rows: Vec<(i64, HashMap<String, Value>)>,
    }
    impl WindowLookup for TsLookup {
        fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
            None
        }
        fn snapshot(&self, _w: &str) -> Option<Vec<JoinRow>> {
            None
        }
        fn snapshot_with_timestamps(&self, _w: &str) -> Option<Vec<(i64, JoinRow)>> {
            Some(
                self.rows
                    .iter()
                    .map(|(ts, fields)| {
                        let ev = Event {
                            fields: fields
                                .iter()
                                .map(|(k, v)| (k.clone().into(), v.clone()))
                                .collect(),
                        };
                        (*ts, JoinRow::Event(std::sync::Arc::new(ev)))
                    })
                    .collect(),
            )
        }
    }
    let lookup = TsLookup {
        rows: vec![
            (10, HashMap::from([("id".to_string(), num(1.0))])),
            (20, HashMap::from([("id".to_string(), num(2.0))])),
        ],
    };
    let candidates = lookup
        .asof_candidates("w", "id", &Value::Number(2.0))
        .expect("timestamped snapshot exists");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].0, 20);
    // Structured key never matches scalar rows.
    let none = lookup
        .asof_candidates("w", "id", &Value::Array(vec![]))
        .expect("snapshot exists");
    assert!(none.is_empty());
}

#[test]
fn advance_with_progress_reports_instance_count() {
    // After a Matched + reset, a follow-up progress capture reports the live
    // instance count (instances field populated at the end of advance).
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(matches!(sm.advance_at("e", &e, 0), StepResult::Matched(_)));
    let outcome = sm.advance_at_with_progress("e", &e, 1_000, None);
    assert!(matches!(outcome.result, StepResult::Matched(_)));
    let progress = outcome.progress.expect("progress captured");
    assert_eq!(progress.instances, 1);
}

#[test]
fn close_output_carries_window_times_and_last_event() {
    let plan = plan_with_close(
        vec![simple_key("sip")],
        vec![step(vec![branch("req", count_ge(1.0))])],
        vec![step(vec![branch("c", count_ge(1.0))])],
        Duration::from_secs(60),
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    sm.advance_at("req", &e, 1_000);
    sm.advance_at("c", &e, 2_000);
    let out = sm
        .close(&[str_val("10.0.0.1")], CloseReason::Timeout)
        .unwrap();
    assert_eq!(out.close_reason, CloseReason::Timeout);
    assert!(out.event_ok);
    assert!(out.close_ok);
    assert_eq!(out.event_first_time_nanos, 1_000);
    assert_eq!(out.event_last_time_nanos, 2_000);
    assert_eq!(out.last_event_nanos, 2_000);
    // close() uses the machine's watermark as the window end.
    assert_eq!(out.window_end_time_nanos, 2_000);
}
