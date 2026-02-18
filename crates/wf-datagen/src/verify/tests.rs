use crate::oracle::OracleAlert;
use crate::verify::{verify, ActualAlert};

#[test]
fn exact_match_passes() {
    let expected = vec![OracleAlert {
        rule_name: "r1".to_string(),
        score: 85.0,
        entity_type: "ip".to_string(),
        entity_id: "10.0.0.1".to_string(),
        close_reason: None,
        emit_time: "2024-01-01T00:05:00Z".to_string(),
    }];

    let actual = vec![ActualAlert {
        rule_name: "r1".to_string(),
        score: 85.0,
        entity_type: "ip".to_string(),
        entity_id: "10.0.0.1".to_string(),
        close_reason: None,
        fired_at: "2024-01-01T00:05:00Z".to_string(),
    }];

    let report = verify(&expected, &actual, 0.01);
    assert_eq!(report.status, "pass");
    assert_eq!(report.summary.matched, 1);
    assert_eq!(report.summary.missing, 0);
    assert_eq!(report.summary.unexpected, 0);
    assert_eq!(report.summary.field_mismatch, 0);
}

#[test]
fn missing_alert_fails() {
    let expected = vec![OracleAlert {
        rule_name: "r1".to_string(),
        score: 85.0,
        entity_type: "ip".to_string(),
        entity_id: "10.0.0.1".to_string(),
        close_reason: None,
        emit_time: "2024-01-01T00:05:00Z".to_string(),
    }];

    let actual = vec![];

    let report = verify(&expected, &actual, 0.01);
    assert_eq!(report.status, "fail");
    assert_eq!(report.summary.missing, 1);
}

#[test]
fn unexpected_alert_fails() {
    let expected = vec![];

    let actual = vec![ActualAlert {
        rule_name: "r1".to_string(),
        score: 85.0,
        entity_type: "ip".to_string(),
        entity_id: "10.0.0.1".to_string(),
        close_reason: None,
        fired_at: "2024-01-01T00:05:00Z".to_string(),
    }];

    let report = verify(&expected, &actual, 0.01);
    assert_eq!(report.status, "fail");
    assert_eq!(report.summary.unexpected, 1);
}

#[test]
fn score_mismatch_fails() {
    let expected = vec![OracleAlert {
        rule_name: "r1".to_string(),
        score: 85.0,
        entity_type: "ip".to_string(),
        entity_id: "10.0.0.1".to_string(),
        close_reason: None,
        emit_time: "2024-01-01T00:05:00Z".to_string(),
    }];

    let actual = vec![ActualAlert {
        rule_name: "r1".to_string(),
        score: 50.0,
        entity_type: "ip".to_string(),
        entity_id: "10.0.0.1".to_string(),
        close_reason: None,
        fired_at: "2024-01-01T00:05:00Z".to_string(),
    }];

    let report = verify(&expected, &actual, 0.01);
    assert_eq!(report.status, "fail");
    assert_eq!(report.summary.field_mismatch, 1);
}

#[test]
fn score_within_tolerance_passes() {
    let expected = vec![OracleAlert {
        rule_name: "r1".to_string(),
        score: 85.0,
        entity_type: "ip".to_string(),
        entity_id: "10.0.0.1".to_string(),
        close_reason: None,
        emit_time: "2024-01-01T00:05:00Z".to_string(),
    }];

    let actual = vec![ActualAlert {
        rule_name: "r1".to_string(),
        score: 85.005,
        entity_type: "ip".to_string(),
        entity_id: "10.0.0.1".to_string(),
        close_reason: None,
        fired_at: "2024-01-01T00:05:00Z".to_string(),
    }];

    let report = verify(&expected, &actual, 0.01);
    assert_eq!(report.status, "pass");
    assert_eq!(report.summary.matched, 1);
}

#[test]
fn empty_both_passes() {
    let report = verify(&[], &[], 0.01);
    assert_eq!(report.status, "pass");
    assert_eq!(report.summary.matched, 0);
}
