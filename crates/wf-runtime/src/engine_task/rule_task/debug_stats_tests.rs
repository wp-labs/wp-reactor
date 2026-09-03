use super::debug::DEBUG_DETAIL_LIMIT;
use super::*;
use wf_engine::alert::AlertOrigin;

fn output_record(target: &str) -> OutputRecord {
    OutputRecord {
        wfx_id: "id".to_string(),
        rule_name: "rule".into(),
        score: 1.0,
        entity_type: "ip".into(),
        entity_id: "10.0.0.1".to_string(),
        origin: AlertOrigin::Event,
        fired_at: "2026-01-01T00:00:00Z".to_string(),
        emit_time: "2026-01-01T00:00:00Z".into(),
        matched_rows: Vec::new(),
        summary: "".into(),
        yield_target: target.into(),
        yield_fields: Vec::new(),
        yield_field_types: Vec::new().into(),
        event_time_nanos: 0,
        machine_id: Arc::from(""),
        scope_key: "".into(),
    }
}

#[test]
fn detail_budget_caps_at_first_twenty_entries() {
    let mut stats = RuleBatchDebugStats::default();

    for _ in 0..DEBUG_DETAIL_LIMIT {
        assert!(stats.allow_detail());
    }

    assert!(!stats.allow_detail());
    assert!(!stats.allow_detail());
    assert_eq!(stats.detail_logged, DEBUG_DETAIL_LIMIT);
    assert_eq!(stats.detail_suppressed, 2);
}

#[test]
fn exhausted_detail_budget_still_counts_suppressed_entries() {
    let mut stats = RuleBatchDebugStats::default();

    for _ in 0..DEBUG_DETAIL_LIMIT {
        assert!(stats.can_log_detail());
        assert!(stats.allow_detail());
    }

    assert!(!stats.can_log_detail());
    assert!(!stats.allow_detail());
    assert_eq!(stats.detail_logged, DEBUG_DETAIL_LIMIT);
    assert_eq!(stats.detail_suppressed, 1);
}

#[test]
fn output_counts_split_alert_and_intermediate_targets() {
    let mut stats = RuleBatchDebugStats::default();
    let intermediate_targets = HashSet::from(["internal_events".to_string()]);

    stats.count_output(&output_record("alerts"), &intermediate_targets);
    stats.count_output(&output_record("internal_events"), &intermediate_targets);
    stats.count_output(&output_record("alerts"), &intermediate_targets);

    assert_eq!(stats.output_emitted, 2);
    assert_eq!(stats.intermediate_emitted, 1);
}
