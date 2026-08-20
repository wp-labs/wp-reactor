//! Micro-benchmark: q5-like fixed-window count rule — measure per-event
//! `advance_at` + per-row `scan_expired_at` cost as the instance heap grows.
//!
//! Mirrors the rule_task hot path: for each row, scan_expired_at(event_time)
//! then advance_at. Reports the per-event cost in several heap-size regimes to
//! expose O(instances)-style scaling.
//!
//! Usage:
//!   cargo run --release -p wf-engine --example q5_expiry_bench [events=1_000_000]
//!
//! Note: examples build in release too (`cargo run --release --example ...`).

use std::collections::HashSet;
use std::time::{Duration, Instant};

use smol_str::SmolStr;

use wf_lang::ast::{CmpOp, Expr, FieldRef, Measure};
use wf_lang::plan::{
    AggPlan, BranchPlan, MatchPlan, StepPlan, WindowSpec,
};

use wf_engine::match_engine::{CepStateMachine, Event, StepResult, Value, EngineHashMap};

const NANOS_PER_SEC: i64 = 1_000_000_000;

fn event(auction: f64) -> Event {
    let mut fields: EngineHashMap<SmolStr, Value> = EngineHashMap::default();
    fields.insert("auction".into(), Value::Number(auction));
    Event { fields }
}

fn build_plan(window_dur: Duration) -> MatchPlan {
    MatchPlan {
        keys: vec![FieldRef::Simple("auction".to_string())],
        key_map: None,
        window_spec: WindowSpec::Fixed(window_dur),
        event_steps: vec![StepPlan {
            branches: vec![BranchPlan {
                label: None,
                source: "b".to_string(),
                field: None,
                guard: None,
                agg: AggPlan {
                    transforms: vec![],
                    measure: Measure::Count,
                    cmp: CmpOp::Ge,
                    threshold: Expr::Number(100.0),
                },
            }],
        }],
        close_steps: vec![],
        close_mode: wf_lang::ast::CloseMode::And,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: wf_lang::ast::MatchMode::Seq,
        accu: false,
        needs_field_history: false,
    }
}

fn main() {
    let total: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    // Span in minutes: default 5 (< window 10) so instances stay live and the
    // heap grows monotonically, isolating heap-size scaling. Pass >10 to see
    // the steady-state expiry-drain pattern.
    let span_min: i64 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    // q5: fixed 10-minute window per auction; event time spans `span_min`.
    let window = Duration::from_secs(10 * 60);
    let span_nanos = span_min * 60 * NANOS_PER_SEC;
    let mut sm = CepStateMachine::new("q5_bench".to_string(), build_plan(window), None);

    // Sequential unique auctions → the heap grows monotonically with `total`.
    let t0 = 1_700_000_000i64 * NANOS_PER_SEC;
    let mut advance_ns: u128 = 0;
    let mut scan_ns: u128 = 0;
    let mut matched = 0usize;
    let mut expired_total = 0usize;

    // Report per-event cost in size regimes.
    let checkpoints: Vec<usize> = vec![100_000, 1_000_000, 5_000_000, total]
        .into_iter()
        .filter(|&n| n <= total)
        .collect();
    let mut next_checkpoint = checkpoints.iter().copied().peekable();

    for i in 0..total {
        let event_nanos = t0 + ((i as i64 * span_nanos) / total as i64);
        // rule_task hot path: per-row scan first, then advance.
        let s = Instant::now();
        let expired = sm.scan_expired_at(event_nanos);
        scan_ns += s.elapsed().as_nanos();
        expired_total += expired.len();

        let a = Instant::now();
        let result = sm.advance_at("b", &event(i as f64), event_nanos);
        advance_ns += a.elapsed().as_nanos();
        if matches!(result, StepResult::Matched(_)) {
            matched += 1;
        }

        if let Some(&n) = next_checkpoint.peek()
            && i + 1 == n
        {
            let live = sm.instance_count();
            println!(
                "after {n:>10} events: instances={live:>8}  advance={:>8.0}ns/ev  scan={:>8.0}ns/ev",
                advance_ns as f64 / (i + 1) as f64,
                scan_ns as f64 / (i + 1) as f64,
            );
            next_checkpoint.next();
        }
    }

    // Drain everything with a far-ahead watermark and time the full sweep.
    let t = Instant::now();
    let remaining = sm.scan_expired_at(t0 + span_nanos + window.as_nanos() as i64 + NANOS_PER_SEC);
    let drain_ms = t.elapsed().as_millis();
    expired_total += remaining.len();

    println!("----");
    println!("total: {total} events, live={} matched={matched} expired_total={expired_total}", sm.instance_count());
    println!("final sweep ({}) took {drain_ms}ms", remaining.len());
    println!(
        "advance total {:.1}ms  scan total {:.1}ms",
        advance_ns as f64 / 1e6,
        scan_ns as f64 / 1e6
    );

    // -- Wave test: N instances created in ONE 10-min bucket, then expire at
    // the same event-time. The per-row scan (budget 1024/call) drains them --
    // each row pops up to 1024 and builds a CloseOutput per instance.
    wave_test();
}

/// Reproduce the expiry "wave": many instances sharing one expiry time, drained
/// by repeated per-row `scan_expired_at` calls. This is the rule_task per-row
/// pattern during a bucket-boundary crossing. Returns (rows_to_drain, ms).
fn wave_test() {
    let total: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let window = Duration::from_secs(10 * 60);
    let mut sm = CepStateMachine::new("wave".to_string(), build_plan(window), None);
    let bucket_start = 1_700_000_000i64 * NANOS_PER_SEC;
    let bucket_start_ns = bucket_start;
    // Create `total` instances all inside the same 10-min bucket.
    for i in 0..total {
        let t = bucket_start_ns + 1; // same bucket
        sm.advance_at("b", &event(i as f64), t);
    }
    let created = sm.instance_count();
    let expire_at = bucket_start_ns + window.as_nanos() as i64;

    // Drain with per-row scans (budget 1024/call), measuring per-row cost.
    let mut rows = 0usize;
    let mut popped = 0usize;
    let t = Instant::now();
    for _ in 0..(total / 1024 + 10) {
        let before = popped;
        let expired = sm.scan_expired_at(expire_at);
        popped += expired.len();
        rows += 1;
        if sm.instance_count() == 0 {
            break;
        }
    }
    let ms = t.elapsed().as_millis();
    println!("---- wave test: created={created} drained={popped} in {rows} rows over {ms}ms ({:.1}ms/row, {:.1}µs/close)",
        ms as f64 / rows.max(1) as f64,
        ms as f64 * 1000.0 / popped.max(1) as f64,
    );
    let remaining = sm.instance_count();
    println!("wave remaining instances (heap front not drained?): {remaining}");
    // A second far-ahead scan drains the rest under a larger budget.
    let t2 = Instant::now();
    let rest = sm.scan_expired_at(i64::MAX);
    println!("second drain: {} closes in {}ms", rest.len(), t2.elapsed().as_millis());
}
