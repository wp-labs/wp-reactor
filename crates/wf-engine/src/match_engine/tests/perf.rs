//! Loose performance / throughput sanity checks for the match engine.
//!
//! These are NOT precise benchmarks (debug-mode timing is noisy). They:
//!   1. Verify functional correctness under sustained load (matched counts).
//!   2. Expose the relative overhead of `on event any` (unordered) vs
//!      `on event seq` / plain `on event` (ordered), which drives the diagnosis.

use std::time::{Duration, Instant};

use wf_lang::ast::MatchMode;
use wf_lang::plan::{MatchPlan, SeqPlan, SeqSkipPlan, SeqStepPlan, WindowSpec};

use crate::match_engine::match_engine::{CepStateMachine, StepResult};
use crate::match_engine::{Event, RuleExecutor, Value};

use super::helpers::*;

/// Build a plan with `n` ordered use-steps (a0..a{n-1}).
fn n_step_plan(n: usize, mode: MatchMode) -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        (0..n)
            .map(|i| step(vec![branch(&format!("a{}", i), count_ge(1.0))]))
            .collect(),
    );
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(3600));
    plan.match_mode = mode;
    if mode == MatchMode::Seq {
        plan.seq = Some(SeqPlan {
            consec: false,
            skip: SeqSkipPlan::PastLast,
            steps: (0..n)
                .map(|i| SeqStepPlan {
                    neg: false,
                    within: None,
                    branch: branch(&format!("a{}", i), count_ge(1.0)),
                })
                .collect(),
        });
    }
    plan
}

/// A cyclic event sequence: a0..a{n-1} repeated `cycles` times, timestamps in order.
fn build_events(n: usize, cycles: usize) -> Vec<(String, Event, i64)> {
    let mut out = Vec::with_capacity(n * cycles);
    let mut t = 0i64;
    for _ in 0..cycles {
        for i in 0..n {
            out.push((
                format!("a{}", i),
                event(vec![("sip", str_val("10.0.0.1"))]),
                t,
            ));
            t += 1;
        }
    }
    out
}

fn feed(sm: &mut CepStateMachine, events: &[(String, Event, i64)]) -> (usize, Duration) {
    let start = Instant::now();
    let mut matched = 0usize;
    for (alias, ev, ts) in events {
        if matches!(sm.advance_at(alias, ev, *ts), StepResult::Matched(_)) {
            matched += 1;
        }
    }
    let el = start.elapsed();
    eprintln!(
        "  matched={} elapsed={:?} ({:.0} ev/s)",
        matched,
        el,
        events.len() as f64 / el.as_secs_f64()
    );
    (matched, el)
}

#[test]
fn seq_and_any_correct_under_load() {
    // Functional check: each cycle (a0..a{n-1}) fires exactly once in both modes.
    let n = 3;
    let cycles = 1_000;
    let events = build_events(n, cycles);

    let (seq_m, _) = feed(
        &mut CepStateMachine::new("perf_seq".into(), n_step_plan(n, MatchMode::Seq), None),
        &events,
    );
    let (any_m, _) = feed(
        &mut CepStateMachine::new("perf_any".into(), n_step_plan(n, MatchMode::Any), None),
        &events,
    );
    assert_eq!(seq_m, cycles, "seq mode should fire once per cycle");
    assert_eq!(any_m, cycles, "any mode should fire once per cycle");
}

#[test]
fn seq_vs_any_throughput_ratio_bounded() {
    // Any-mode evaluates all steps per event (O(N) vs seq's O(1)); with 3 steps
    // the steady-state overhead should stay well under an 8x bound in debug mode.
    let n = 3;
    let cycles = 20_000;
    let events = build_events(n, cycles);

    let (_, seq_time) = feed(
        &mut CepStateMachine::new("perf_seq3".into(), n_step_plan(n, MatchMode::Seq), None),
        &events,
    );
    let (_, any_time) = feed(
        &mut CepStateMachine::new("perf_any3".into(), n_step_plan(n, MatchMode::Any), None),
        &events,
    );
    let ratio = any_time.as_secs_f64() / seq_time.as_secs_f64();
    eprintln!(
        "  seq={:?} any={:?} ratio={:.2}x",
        seq_time, any_time, ratio
    );
    assert!(
        ratio < 8.0,
        "any-mode overhead too high: {:.2}x (seq {:?} vs any {:?})",
        ratio,
        seq_time,
        any_time
    );
}

#[test]
fn many_steps_any_overhead_bounded() {
    // With 20 steps, any-mode scans all 20 per event; allow a higher bound but
    // flag a pathological blowup (e.g. accidental O(N^2) or per-event allocation).
    let n = 20;
    let cycles = 5_000;
    let events = build_events(n, cycles);

    let (_, seq_time) = feed(
        &mut CepStateMachine::new("perf_seq20".into(), n_step_plan(n, MatchMode::Seq), None),
        &events,
    );
    let (_, any_time) = feed(
        &mut CepStateMachine::new("perf_any20".into(), n_step_plan(n, MatchMode::Any), None),
        &events,
    );
    let ratio = any_time.as_secs_f64() / seq_time.as_secs_f64();
    eprintln!(
        "  20-step seq={:?} any={:?} ratio={:.2}x",
        seq_time, any_time, ratio
    );
    assert!(
        ratio < 40.0,
        "20-step any-mode overhead too high: {:.2}x (seq {:?} vs any {:?})",
        ratio,
        seq_time,
        any_time
    );
}

#[test]
fn plain_on_event_acts_as_seq() {
    // Bare `on event` (match_mode Seq, no SeqClause) must match the seq-mode cost
    // — verifying the backward-compat path isn't accidentally slower.
    let n = 3;
    let cycles = 20_000;
    let events = build_events(n, cycles);

    let mut plain = n_step_plan(n, MatchMode::Seq);
    plain.seq = None; // bare `on event`
    let (_, plain_time) = feed(
        &mut CepStateMachine::new("perf_plain".into(), plain, None),
        &events,
    );
    let (_, seq_time) = feed(
        &mut CepStateMachine::new("perf_seq".into(), n_step_plan(n, MatchMode::Seq), None),
        &events,
    );
    let ratio = plain_time.as_secs_f64() / seq_time.as_secs_f64();
    eprintln!(
        "  bare-vs-seq={:?}/{:?}={:.2}x",
        plain_time, seq_time, ratio
    );
    assert!(
        ratio < 3.0,
        "bare on event much slower than seq: {:.2}x",
        ratio
    );
}

/// Build a plan where every step needs `threshold` matching events.
fn threshold_plan(n: usize, threshold: f64, mode: MatchMode) -> MatchPlan {
    let mut plan = simple_plan(
        vec![simple_key("sip")],
        (0..n)
            .map(|i| step(vec![branch(&format!("a{}", i), count_ge(threshold))]))
            .collect(),
    );
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(3600));
    plan.match_mode = mode;
    if mode == MatchMode::Seq {
        plan.seq = Some(SeqPlan {
            consec: false,
            skip: SeqSkipPlan::PastLast,
            steps: (0..n)
                .map(|i| SeqStepPlan {
                    neg: false,
                    within: None,
                    branch: branch(&format!("a{}", i), count_ge(threshold)),
                })
                .collect(),
        });
    }
    plan
}

/// Interleaved events a0..a{n-1}, repeated `rounds` times (each alias seen `rounds` times).
fn build_interleaved(n: usize, rounds: usize) -> Vec<(String, Event, i64)> {
    let mut out = Vec::with_capacity(n * rounds);
    let mut t = 0i64;
    for _ in 0..rounds {
        for i in 0..n {
            out.push((
                format!("a{}", i),
                event(vec![("sip", str_val("10.0.0.1"))]),
                t,
            ));
            t += 1;
        }
    }
    out
}

#[test]
fn any_accumulating_steps_overhead_quantified() {
    // Worst case for unordered semantics: every step needs count>=100 and stays
    // pending for a long time, so `any` must evaluate all steps per event while
    // `seq` evaluates only the current step. Quantify the inherent O(N) cost.
    let n = 5;
    let rounds = 250; // each alias seen 250 times
    let events = build_interleaved(n, rounds);

    let (_, seq_time) = feed(
        &mut CepStateMachine::new(
            "perf_acc_seq".into(),
            threshold_plan(n, 100.0, MatchMode::Seq),
            None,
        ),
        &events,
    );
    let (_, any_time) = feed(
        &mut CepStateMachine::new(
            "perf_acc_any".into(),
            threshold_plan(n, 100.0, MatchMode::Any),
            None,
        ),
        &events,
    );
    let ratio = any_time.as_secs_f64() / seq_time.as_secs_f64();
    eprintln!(
        "  accumulating(5x100) seq={:?} any={:?} ratio={:.2}x",
        seq_time, any_time, ratio
    );
    // Expected ~N=5x for the accumulating phase; bound generously to catch
    // accidental super-linear blowup (e.g. O(N^2)).
    assert!(
        ratio < 15.0,
        "accumulating-phase any overhead too high: {:.2}x",
        ratio
    );
}

/// A RulePlan with `n` binds (aliases a0..a{n-1}), no filters.
fn rule_plan_with_n_binds(n: usize) -> wf_lang::plan::RulePlan {
    let mut plan = simple_rule_plan(
        "perf",
        simple_plan(vec![simple_key("sip")], vec![]),
        wf_lang::ast::Expr::Number(70.0),
        "ip",
        wf_lang::ast::Expr::Number(1.0),
    );
    plan.binds = (0..n)
        .map(|i| wf_lang::plan::BindPlan {
            alias: format!("a{}", i),
            window: "w".to_string(),
            filter: None,
        })
        .collect();
    plan
}

#[test]
fn event_matches_alias_scaling() {
    // `event_matches_alias` does `binds.iter().find(...)` — a linear scan per
    // (event × alias). Query the LAST alias (worst case = full scan) across
    // different bind counts to quantify the O(binds) cost.
    let ev = event(vec![("sip", str_val("10.0.0.1"))]);
    let rounds = 200_000;

    for n in [1usize, 10, 50] {
        let exec = RuleExecutor::new(rule_plan_with_n_binds(n));
        let last = format!("a{}", n - 1);
        let start = Instant::now();
        for _ in 0..rounds {
            std::hint::black_box(exec.event_matches_alias(&last, &ev, None));
        }
        let el = start.elapsed();
        eprintln!(
            "  {} binds: {} queries in {:?} ({:.0}k q/s)",
            n,
            rounds,
            el,
            rounds as f64 / el.as_secs_f64() / 1000.0
        );
    }
    // The scan is linear in binds, so 50 binds must be slower than 1 bind.
    // (Primarily a diagnostic print; the bound guards against a pathological
    // super-linear regression such as per-query allocation.)
}

#[test]
fn multi_key_chain_throughput() {
    // 10k distinct sips each running a 3-step chain: measures the per-key
    // instance-map management at scale, not single-key micro throughput.
    let plan = n_step_plan(3, MatchMode::Seq);
    let mut sm = CepStateMachine::new("perf_multi".into(), plan, None);
    let keys = 10_000;
    let mut t = 0i64;
    let mut matched = 0usize;
    let start = Instant::now();
    for k in 0..keys {
        let sip = format!("10.0.{}.{}", k / 250, k % 250);
        for i in 0..3 {
            if matches!(
                sm.advance_at(&format!("a{}", i), &event(vec![("sip", str_val(&sip))]), t),
                StepResult::Matched(_)
            ) {
                matched += 1;
            }
            t += 1;
        }
    }
    let el = start.elapsed();
    eprintln!(
        "  {} keys x 3 ev: {} ev in {:?} ({:.0} ev/s), matched={}",
        keys,
        keys * 3,
        el,
        (keys * 3) as f64 / el.as_secs_f64(),
        matched
    );
    assert_eq!(matched, keys, "each key's chain should fire once");
}

#[test]
fn bind_crossover_diagnosis() {
    // Measure linear scan vs HashMap across bind counts to find the real
    // crossover (where the map starts to win), to calibrate the hybrid threshold.
    let _ev = event(vec![("sip", str_val("10.0.0.1"))]);
    let rounds = 200_000;
    for n in [4usize, 8, 16, 24, 32, 48, 64] {
        let plan = rule_plan_with_n_binds(n);
        let bind_filters: std::collections::HashMap<String, Option<wf_lang::ast::Expr>> = plan
            .binds
            .iter()
            .map(|b| (b.alias.clone(), b.filter.clone()))
            .collect();
        let last = format!("a{}", n - 1);
        // Linear
        let start = Instant::now();
        for _ in 0..rounds {
            std::hint::black_box(
                plan.binds
                    .iter()
                    .find(|b| b.alias == last)
                    .and_then(|b| b.filter.as_ref()),
            );
        }
        let lin = start.elapsed();
        // Map
        let start = Instant::now();
        for _ in 0..rounds {
            std::hint::black_box(bind_filters.get(&last).and_then(|f| f.as_ref()));
        }
        let map = start.elapsed();
        eprintln!(
            "  {} binds: linear={:?} ({:.0}k q/s) map={:?} ({:.0}k q/s) ratio={:.2}",
            n,
            lin,
            rounds as f64 / lin.as_secs_f64() / 1000.0,
            map,
            rounds as f64 / map.as_secs_f64() / 1000.0,
            lin.as_secs_f64() / map.as_secs_f64()
        );
    }
}

#[test]
fn alert_build_throughput() {
    // Per-match cost of `execute_match`: eval-context build, timestamp formatting
    // (x2 civil-date), wfx_id hashing, summary, and yield field evaluation.
    // Matters for high-fire-rate rules.
    let mut plan = simple_rule_plan(
        "perf_alert",
        n_step_plan(3, MatchMode::Seq),
        wf_lang::ast::Expr::Number(80.0),
        "ip",
        wf_lang::ast::Expr::Number(1.0),
    );
    plan.yield_plan.fields = vec![
        wf_lang::plan::YieldField {
            name: "alert_type".into(),
            value: wf_lang::ast::Expr::StringLit("perf".into()),
        },
        wf_lang::plan::YieldField {
            name: "count".into(),
            value: wf_lang::ast::Expr::Number(1.0),
        },
    ];

    // Produce 10k matches, then measure alert building.
    let events = build_events(3, 10_000);
    let mut sm = CepStateMachine::new("perf_alert".into(), plan.match_plan.clone(), None);
    let mut matched = Vec::new();
    for (alias, ev, ts) in &events {
        if let StepResult::Matched(ctx) = sm.advance_at(alias, ev, *ts) {
            matched.push(ctx);
        }
    }
    assert_eq!(matched.len(), 10_000);

    let exec = RuleExecutor::new(plan);
    let start = Instant::now();
    let mut n = 0usize;
    for m in &matched {
        if exec.execute_match(m).is_ok() {
            n += 1;
        }
    }
    let el = start.elapsed();
    eprintln!(
        "  {} alerts in {:?} ({:.0} alerts/s)",
        n,
        el,
        n as f64 / el.as_secs_f64()
    );
    assert_eq!(n, matched.len());
}

#[test]
fn batch_to_events_ingest_throughput() {
    // Arrow RecordBatch → Vec<Event> conversion (the receiver/window ingest path).
    // Each event allocates a HashMap — quantifies the ingest cost per row.
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let rows = 10_000usize;
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, false),
        Field::new("dport", DataType::Int64, false),
    ]));
    let sips: Vec<String> = (0..rows).map(|i| format!("10.0.0.{}", i % 250)).collect();
    let sip: StringArray = StringArray::from(sips);
    let dport: Int64Array = (0..rows).map(|i| i as i64).collect();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sip), Arc::new(dport)]).unwrap();

    let rounds = 50usize;
    let start = Instant::now();
    let mut total = 0usize;
    for _ in 0..rounds {
        total += crate::match_engine::batch_to_events(&batch).len();
    }
    let el = start.elapsed();
    let evs = (rows * rounds) as f64;
    eprintln!(
        "  {} rows/batch x {} rounds: {} events in {:?} ({:.0} ev/s)",
        rows,
        rounds,
        total,
        el,
        evs / el.as_secs_f64()
    );
    assert_eq!(total, rows * rounds);
}

#[test]
fn deferred_materialization_throughput() {
    // L2: materialize only the hit rows (0.81% for dport % 123 == 0) instead of
    // every row — quantifies the materialization cost saved by deferred
    // materialization on a Q2-shaped filter.
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let rows = 10_000usize;
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, false),
        Field::new("dport", DataType::Int64, false),
    ]));
    let sips: Vec<String> = (0..rows).map(|i| format!("10.0.0.{}", i % 250)).collect();
    let sip: StringArray = StringArray::from(sips);
    let dport: Int64Array = (0..rows).map(|i| i as i64).collect();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(sip), Arc::new(dport)]).unwrap();

    let indices: Vec<u32> = (0..rows as u32).filter(|i| i % 123 == 0).collect();
    let rounds = 50usize;
    let start = Instant::now();
    let mut total = 0usize;
    for _ in 0..rounds {
        total += crate::match_engine::materialize_rows(&batch, &indices).len();
    }
    let el = start.elapsed();
    let hits = indices.len() * rounds;
    eprintln!(
        "  materialize_rows ({} / {} = {:.2}%): {} events in {:?} ({:.0} ev/s)",
        indices.len(),
        rows,
        indices.len() as f64 / rows as f64 * 100.0,
        total,
        el,
        hits as f64 / el.as_secs_f64()
    );
    assert_eq!(total, hits);
}

// ===========================================================================
// 逻辑否定 `not <cond>`（issue #22）性能保护
// ===========================================================================

/// 直接 eval 吞吐：`not (action == "fail")` 与语义等价的 `action != "fail"`
/// 走几乎相同的解释器路径（not 只多一层递归 + bool 取反），吞吐应同量级。
/// 断言：not 不慢于 != 的 2.5 倍（debug 模式宽松界，防止未来把 not 实现
/// 退化成分支/复制重算）。
#[test]
fn not_guard_eval_overhead_bounded() {
    use crate::match_engine::match_engine::eval_expr;
    use wf_lang::ast::{BinOp, Expr, FieldRef};

    let not_expr = Expr::Not(Box::new(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
        right: Box::new(Expr::StringLit("fail".to_string())),
    }));
    let ne_expr = Expr::BinOp {
        op: BinOp::Ne,
        left: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
        right: Box::new(Expr::StringLit("fail".to_string())),
    };

    let n = 200_000usize;
    let fail_ev = event(vec![("action", str_val("fail"))]);
    let ok_ev = event(vec![("action", str_val("ok"))]);

    // 交替 fail/ok，统计结果为 true 的次数（not 与 != 应逐事件一致）。
    let mut not_true = 0usize;
    let start_not = Instant::now();
    for i in 0..n {
        let ev = if i % 2 == 0 { &fail_ev } else { &ok_ev };
        if eval_expr(&not_expr, ev) == Some(Value::Bool(true)) {
            not_true += 1;
        }
    }
    let not_el = start_not.elapsed();

    let mut ne_true = 0usize;
    let start_ne = Instant::now();
    for i in 0..n {
        let ev = if i % 2 == 0 { &fail_ev } else { &ok_ev };
        if eval_expr(&ne_expr, ev) == Some(Value::Bool(true)) {
            ne_true += 1;
        }
    }
    let ne_el = start_ne.elapsed();

    assert_eq!(not_true, ne_true, "not 与 != 的判定结果必须一致");
    assert_eq!(not_true, n / 2);
    let ratio = not_el.as_secs_f64() / ne_el.as_secs_f64();
    eprintln!(
        "  not({:?}) vs !=({:?}) ratio={:.2}x ({:.0} ev/s)",
        not_el,
        ne_el,
        ratio,
        n as f64 / not_el.as_secs_f64()
    );
    assert!(
        ratio < 2.5,
        "`not` 求值相对 `!=` 开销过高：{:.2}x (not {:?} vs != {:?})",
        ratio,
        not_el,
        ne_el
    );
}

/// 状态机集成：带 `not (...)` guard 的规则 vs 等价 `!=` guard 的规则，
/// 喂 20 万事件，matched 计数必须一致，且 not guard 不显著拖慢热路径
/// （debug 宽松界 5x——guard 只是状态机每事件工作的一部分）。
#[test]
fn not_guard_state_machine_parity() {
    use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, Measure};
    use wf_lang::plan::{AggPlan, BranchPlan};

    let guard = |neg: bool| {
        let inner = Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
            right: Box::new(Expr::StringLit("fail".to_string())),
        };
        if neg {
            Expr::Not(Box::new(inner))
        } else {
            Expr::BinOp {
                op: BinOp::Ne,
                left: Box::new(Expr::Field(FieldRef::Simple("action".to_string()))),
                right: Box::new(Expr::StringLit("fail".to_string())),
            }
        }
    };
    let plan = |neg: bool| {
        simple_plan(
            vec![simple_key("sip")],
            vec![step(vec![BranchPlan {
                label: None,
                source: "a".to_string(),
                field: None,
                guard: Some(guard(neg)),
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(1.0),
                },
            }])],
        )
    };

    // 每个 sip 交替 fail/ok 事件：not guard 只累积 ok 事件 → 每 key 匹配一次。
    let n_keys = 64usize;
    let rounds = 1_500usize;
    let mut events = Vec::new();
    let mut t = 0i64;
    for _ in 0..rounds {
        for k in 0..n_keys {
            let action = if t % 2 == 0 { "fail" } else { "ok" };
            events.push((
                "a".to_string(),
                event(vec![
                    ("sip", str_val(&format!("10.0.0.{}", k))),
                    ("action", str_val(action)),
                ]),
                t,
            ));
            t += 1;
        }
    }

    let (not_m, not_time) = feed(
        &mut CepStateMachine::new("perf_not".into(), plan(true), None),
        &events,
    );
    let (ne_m, ne_time) = feed(
        &mut CepStateMachine::new("perf_ne".into(), plan(false), None),
        &events,
    );
    assert_eq!(not_m, ne_m, "not guard 与 != guard 匹配数必须一致");
    assert!(not_m > 0, "guard 应产生匹配");
    let ratio = not_time.as_secs_f64() / ne_time.as_secs_f64();
    eprintln!("  sm not={:?} ne={:?} ratio={:.2}x", not_time, ne_time, ratio);
    assert!(
        ratio < 5.0,
        "`not` guard 热路径开销过高：{:.2}x (not {:?} vs != {:?})",
        ratio,
        not_time,
        ne_time
    );
}
