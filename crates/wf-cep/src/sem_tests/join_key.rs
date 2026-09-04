use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use wf_lang::ast::{FieldRef, MatchMode};
use wf_lang::plan::{JoinKeyPlan, MatchPlan, WindowSpec};

use crate::cep::{CepStateMachine, StepResult, Value, WindowLookup};
use crate::row_views::JoinRow;

use super::helpers::{branch, count_ge, event, num, simple_plan, step};

/// Minimal `WindowLookup` for join-then-key tests: keyed by
/// `(window, key_field, index_key)` where the index key mirrors the real join
/// index's f64 truncation (`JoinKey::from_value` does `*n as i64`) — so a
/// fractional driver value lands in the truncated slot, exercising the
/// `values_equal` re-check the key-join branch now performs.
struct TestLookup {
    rows: HashMap<(String, String, i64), Vec<JoinRow>>,
}

impl TestLookup {
    fn row(auction_id: f64, category: f64) -> JoinRow {
        JoinRow::Event(Arc::new(event(vec![
            ("id", num(auction_id)),
            ("category", num(category)),
        ])))
    }
}

impl WindowLookup for TestLookup {
    fn snapshot_field_values(&self, _window: &str, _field: &str) -> Option<HashSet<String>> {
        None
    }

    fn snapshot(&self, _window: &str) -> Option<Vec<JoinRow>> {
        None
    }

    fn join_lookup(&self, window: &str, key_field: &str, key: &Value) -> Option<Vec<JoinRow>> {
        let Value::Number(n) = key else {
            return None;
        };
        // Same truncation as the real hash index (JoinKey::from_value).
        let index_key = *n as i64;
        self.rows
            .get(&(window.to_string(), key_field.to_string(), index_key))
            .cloned()
    }
}

/// `match<category:10m>` where `category` comes from `auction_events` via
/// `b.auction == auction_events.id`.
fn join_key_plan() -> MatchPlan {
    let mut plan = simple_plan(
        vec![FieldRef::Simple("category".into())],
        vec![step(vec![branch("b", count_ge(1.0))])],
    );
    plan.key_join = Some(JoinKeyPlan {
        join_idx: 0,
        right_window: "auction_events".into(),
        left_field: FieldRef::Qualified("b".into(), "auction".into()),
        right_key_field: "id".into(),
        right_field: "category".into(),
        key_name: "category".into(),
    });
    plan.window_spec = WindowSpec::Sliding(Duration::from_secs(600));
    plan.match_mode = MatchMode::Any;
    plan
}

#[test]
fn join_key_hit_routes_to_joined_key_instance() {
    let plan = join_key_plan();
    let mut sm = CepStateMachine::new("t".into(), plan, None);
    let lookup = TestLookup {
        rows: HashMap::from([(
            ("auction_events".into(), "id".into(), 42),
            vec![TestLookup::row(42.0, 7.0)],
        )]),
    };

    let e = event(vec![("auction", num(42.0))]);
    let StepResult::Matched(ctx) = sm.advance_at_with("b", &e, 1_000, Some(&lookup)) else {
        panic!("join key hit should fire");
    };
    // The instance key is the *joined* field value (category=7), not the driver
    // auction id.
    assert_eq!(ctx.scope_key, vec![num(7.0)]);
    assert_eq!(sm.instance_count(), 1);
}

#[test]
fn join_key_hit_routes_distinct_joined_keys_to_distinct_instances() {
    let plan = join_key_plan();
    let mut sm = CepStateMachine::new("t".into(), plan.clone(), None);
    let lookup = TestLookup {
        rows: HashMap::from([
            (
                ("auction_events".into(), "id".into(), 1),
                vec![TestLookup::row(1.0, 10.0)],
            ),
            (
                ("auction_events".into(), "id".into(), 2),
                vec![TestLookup::row(2.0, 20.0)],
            ),
        ]),
    };

    let e1 = event(vec![("auction", num(1.0))]);
    assert!(matches!(
        sm.advance_at_with("b", &e1, 1_000, Some(&lookup)),
        StepResult::Matched(_)
    ));
    let e2 = event(vec![("auction", num(2.0))]);
    assert!(matches!(
        sm.advance_at_with("b", &e2, 2_000, Some(&lookup)),
        StepResult::Matched(_)
    ));
    assert_eq!(
        sm.instance_count(),
        2,
        "distinct joined keys → distinct instances"
    );

    // Same joined category on both rows → same instance (2 events, 1 instance).
    let same_category = TestLookup {
        rows: HashMap::from([
            (
                ("auction_events".into(), "id".into(), 1),
                vec![TestLookup::row(1.0, 5.0)],
            ),
            (
                ("auction_events".into(), "id".into(), 2),
                vec![TestLookup::row(2.0, 5.0)],
            ),
        ]),
    };
    let mut sm2 = CepStateMachine::new("t".into(), plan.clone(), None);
    let e1 = event(vec![("auction", num(1.0))]);
    assert!(matches!(
        sm2.advance_at_with("b", &e1, 1_000, Some(&same_category)),
        StepResult::Matched(_)
    ));
    let e2 = event(vec![("auction", num(2.0))]);
    assert!(matches!(
        sm2.advance_at_with("b", &e2, 2_000, Some(&same_category)),
        StepResult::Matched(_)
    ));
    assert_eq!(sm2.instance_count(), 1, "same joined key → shared instance");
}

#[test]
fn join_key_miss_skips_event() {
    let plan = join_key_plan();
    let mut sm = CepStateMachine::new("t".into(), plan, None);
    // No auction row for id=42 → join miss → event skipped, no instance.
    let lookup = TestLookup {
        rows: HashMap::new(),
    };
    let e = event(vec![("auction", num(42.0))]);
    assert_eq!(
        sm.advance_at_with("b", &e, 1_000, Some(&lookup)),
        StepResult::Accumulate
    );
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn join_key_missing_left_field_skips_event() {
    let plan = join_key_plan();
    let mut sm = CepStateMachine::new("t".into(), plan, None);
    let lookup = TestLookup {
        rows: HashMap::from([(
            ("auction_events".into(), "id".into(), 42),
            vec![TestLookup::row(42.0, 7.0)],
        )]),
    };
    // Event without the join-left field `auction`.
    let e = event(vec![("bidder", num(1.0))]);
    assert_eq!(
        sm.advance_at_with("b", &e, 1_000, Some(&lookup)),
        StepResult::Accumulate
    );
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn join_key_without_lookup_skips_event() {
    // No WindowLookup (e.g. the inline WFL test harness) → treated as join miss.
    let plan = join_key_plan();
    let mut sm = CepStateMachine::new("t".into(), plan, None);
    let e = event(vec![("auction", num(42.0))]);
    assert_eq!(sm.advance_at("b", &e, 1_000), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn join_key_fractional_left_value_does_not_false_match() {
    // The join index key truncates f64 (`JoinKey::from_value` `as i64`): a
    // fractional driver value 1.5 lands in the Int(1) slot next to auction
    // id=1. The key-join branch re-checks with `values_equal` (like the
    // match-time join path) and must reject the row — no instance, no match.
    let plan = join_key_plan();
    let mut sm = CepStateMachine::new("t".into(), plan, None);
    let lookup = TestLookup {
        rows: HashMap::from([(
            ("auction_events".into(), "id".into(), 1),
            vec![TestLookup::row(1.0, 7.0)],
        )]),
    };
    let e = event(vec![("auction", num(1.5))]);
    assert_eq!(
        sm.advance_at_with("b", &e, 1_000, Some(&lookup)),
        StepResult::Accumulate,
        "fractional driver value must not false-match the truncated index slot"
    );
    assert_eq!(sm.instance_count(), 0);

    // Exact integer 1.0 still joins normally.
    let e = event(vec![("auction", num(1.0))]);
    let StepResult::Matched(ctx) = sm.advance_at_with("b", &e, 2_000, Some(&lookup)) else {
        panic!("exact integer driver value should join");
    };
    assert_eq!(ctx.scope_key, vec![num(7.0)]);
}

#[test]
fn join_key_key_absent_on_joined_row_skips_event() {
    let plan = join_key_plan();
    let mut sm = CepStateMachine::new("t".into(), plan, None);
    // Joined row exists but lacks the `category` key field.
    let lookup = TestLookup {
        rows: HashMap::from([(
            ("auction_events".into(), "id".into(), 42),
            vec![JoinRow::Event(Arc::new(event(vec![("id", num(42.0))])))],
        )]),
    };
    let e = event(vec![("auction", num(42.0))]);
    assert_eq!(
        sm.advance_at_with("b", &e, 1_000, Some(&lookup)),
        StepResult::Accumulate
    );
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn join_key_without_key_join_field_uses_plain_key_path() {
    // Regression guard: a rule with no key_join must behave exactly as before
    // (driver key extraction; windows irrelevant).
    let mut plan = simple_plan(
        vec![FieldRef::Simple("sip".into())],
        vec![step(vec![branch("b", count_ge(1.0))])],
    );
    plan.match_mode = MatchMode::Any;
    let mut sm = CepStateMachine::new("t".into(), plan, None);
    let e = event(vec![("sip", Value::Str("10.0.0.1".into()))]);
    let StepResult::Matched(ctx) = sm.advance_at("b", &e, 0) else {
        panic!("plain key rule should still fire");
    };
    assert_eq!(ctx.scope_key, vec![Value::Str("10.0.0.1".into())]);
}

#[test]
fn join_key_fixed_window_routes_to_bucket() {
    // Join-then-key + fixed window: the instance key is the joined field, and
    // fixed bucketing uses the event time. Two bids on different auctions that
    // join to the SAME category land in the same fixed bucket instance only if
    // they fall in the same bucket; different buckets → distinct instances.
    let mut plan = join_key_plan();
    plan.window_spec = WindowSpec::Fixed(Duration::from_secs(600)); // 10m buckets
    let lookup = TestLookup {
        rows: HashMap::from([
            (
                ("auction_events".into(), "id".into(), 1),
                vec![TestLookup::row(1.0, 7.0)],
            ),
            (
                ("auction_events".into(), "id".into(), 2),
                vec![TestLookup::row(2.0, 7.0)],
            ),
        ]),
    };

    // Both events at t=100s → bucket 0. Same joined category → same instance.
    let mut sm = CepStateMachine::new("t".into(), plan.clone(), None);
    let e1 = event(vec![("auction", num(1.0))]);
    assert!(matches!(
        sm.advance_at_with("b", &e1, 100, Some(&lookup)),
        StepResult::Matched(_)
    ));
    let e2 = event(vec![("auction", num(2.0))]);
    assert!(matches!(
        sm.advance_at_with("b", &e2, 150, Some(&lookup)),
        StepResult::Matched(_)
    ));
    assert_eq!(
        sm.instance_count(),
        1,
        "same category + same bucket → 1 instance"
    );

    // Same category but different buckets (t=100s vs t=610s → bucket 1) →
    // distinct fixed instances.
    let mut sm2 = CepStateMachine::new("t".into(), plan, None);
    let e1 = event(vec![("auction", num(1.0))]);
    assert!(matches!(
        sm2.advance_at_with("b", &e1, 100, Some(&lookup)),
        StepResult::Matched(_)
    ));
    let e2 = event(vec![("auction", num(2.0))]);
    // 610s into bucket 1 (t = 610_000_000_000 ns > 600s bucket) — same joined
    // category, different fixed bucket → distinct instances.
    assert!(matches!(
        sm2.advance_at_with("b", &e2, 610_000_000_000, Some(&lookup)),
        StepResult::Matched(_)
    ));
    assert_eq!(
        sm2.instance_count(),
        2,
        "same category, different buckets → 2 instances"
    );
}

#[test]
fn join_key_fixed_with_epoch_ns_timestamp_matches() {
    // Regression: oracle passes real epoch-ns timestamps (~1.7e18) — verify
    // fixed-bucket routing + count>=1 still matches at that magnitude.
    let mut plan = join_key_plan();
    plan.window_spec = WindowSpec::Fixed(Duration::from_secs(600));
    let lookup = TestLookup {
        rows: HashMap::from([(
            ("auction_events".into(), "id".into(), 1),
            vec![TestLookup::row(1.0, 7.0)],
        )]),
    };
    let mut sm = CepStateMachine::new("t".into(), plan, None);
    let e = event(vec![("auction", num(1.0))]);
    let ns: i64 = 1_704_067_260_000_000_000; // 2024-01-01T00:01:00Z
    assert!(matches!(
        sm.advance_at_with("b", &e, ns, Some(&lookup)),
        StepResult::Matched(_)
    ));
}

#[test]
fn join_key_fixed_with_seq_mode_matches() {
    // Regression: nexmark fixed rules default to Seq mode (`on event { }`
    // without seq/any keyword). Fixed + Seq + count>=1 must match on the
    // first event (oracle hit this path returning Advance instead of Matched).
    let mut plan = join_key_plan();
    plan.window_spec = WindowSpec::Fixed(Duration::from_secs(600));
    plan.match_mode = MatchMode::Seq;
    let lookup = TestLookup {
        rows: HashMap::from([(
            ("auction_events".into(), "id".into(), 1),
            vec![TestLookup::row(1.0, 7.0)],
        )]),
    };
    let mut sm = CepStateMachine::new("t".into(), plan, None);
    let e = event(vec![("auction", num(1.0))]);
    let ns: i64 = 1_704_067_260_000_000_000; // 2024-01-01T00:01:00Z
    let result = sm.advance_at_with("b", &e, ns, Some(&lookup));
    assert!(
        matches!(result, StepResult::Matched(_)),
        "fixed + Seq + count>=1 must match on first event, got {result:?}"
    );
}
