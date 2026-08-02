//! Chain clause checker tests: alias validation + chain-specific warnings.

use super::*;

fn chain_check(input: &str, schemas: &[WindowSchema]) -> Vec<crate::checker::CheckError> {
    let file = parse_wfl(input).expect("parse should succeed");
    check_wfl(&file, schemas)
}

fn all_errors<'a>(errs: &'a [crate::checker::CheckError]) -> impl Iterator<Item = &'a str> {
    errs.iter()
        .filter(|e| e.severity == Severity::Error)
        .map(|e| e.message.as_str())
}

#[test]
fn chain_valid_rule_has_no_errors() {
    let src = r#"
rule chain_ok {
    events {
        scan  : fw_events
        login : auth_events
    }
    match<sip:30m> {
        on event seq {
            has scan;
            has login within 10m;
        }
    } -> score(80.0)
    entity(ip, scan.sip)
    yield security_alerts (sip = scan.sip, fail_count = 1, port_count = 1, message = "ok")
}
"#;
    let errs = chain_check(
        src,
        &[
            auth_events_window(),
            fw_events_window(),
            security_alerts_window(),
        ],
    );
    let errors: Vec<_> = all_errors(&errs).collect();
    assert!(errors.is_empty(), "expected no errors, got: {:?}", errors);
}

#[test]
fn chain_unknown_alias_is_error() {
    let src = r#"
rule chain_bad_alias {
    events {
        scan  : fw_events
    }
    match<sip:30m> {
        on event seq {
            has scan;
            has nonexistent;
        }
    } -> score(80.0)
    entity(ip, scan.sip)
    yield security_alerts (sip = scan.sip, fail_count = 1, port_count = 1, message = "ok")
}
"#;
    let errs = chain_check(src, &[fw_events_window(), security_alerts_window()]);
    let errors: Vec<_> = all_errors(&errs).collect();
    assert!(
        errors.iter().any(|m| m.contains("nonexistent")),
        "expected unknown-alias error, got: {:?}",
        errors
    );
}

#[test]
fn chain_within_exceeding_window_warns() {
    let src = r#"
rule chain_within_too_big {
    events {
        scan  : fw_events
        login : auth_events
    }
    match<sip:30m> {
        on event seq {
            has scan;
            has login within 1h;
        }
    } -> score(80.0)
    entity(ip, scan.sip)
    yield security_alerts (sip = scan.sip, fail_count = 1, port_count = 1, message = "ok")
}
"#;
    let errs = chain_check(
        src,
        &[
            auth_events_window(),
            fw_events_window(),
            security_alerts_window(),
        ],
    );
    assert!(
        errs.iter().any(|e| {
            e.severity == Severity::Warning
                && e.message.contains("within")
                && e.message.contains("redundant")
        }),
        "expected within-exceeds-window warning, got: {:?}",
        errs
    );
}

#[test]
fn chain_not_first_step_warns() {
    let src = r#"
rule chain_not_first {
    events {
        fail : auth_events
        ok   : auth_events
    }
    match<sip:30m> {
        on event seq {
            not has fail within 5m;
            has ok;
        }
    } -> score(80.0)
    entity(ip, ok.sip)
    yield security_alerts (sip = ok.sip, fail_count = 1, port_count = 1, message = "ok")
}
"#;
    let errs = chain_check(src, &[auth_events_window(), security_alerts_window()]);
    assert!(
        errs.iter()
            .any(|e| e.severity == Severity::Warning && e.message.contains("first chain step")),
        "expected not-first-step warning, got: {:?}",
        errs
    );
}

#[test]
fn chain_not_on_aggregate_is_error() {
    let src = r#"
rule chain_not_agg {
    events {
        a : fw_events
        b : fw_events
    }
    match<sip:30m> {
        on event seq {
            has a;
            not b.dport | sum >= 5;
        }
    } -> score(80.0)
    entity(ip, a.sip)
    yield security_alerts (sip = a.sip, fail_count = 1, port_count = 1, message = "ok")
}
"#;
    let errs = chain_check(src, &[fw_events_window(), security_alerts_window()]);
    let errors: Vec<_> = all_errors(&errs).collect();
    assert!(
        errors
            .iter()
            .any(|m| m.contains("references a field aggregation")),
        "expected aggregate-negation error, got: {:?}",
        errors
    );
}

#[test]
fn chain_skip_to_next_warns() {
    let src = r#"
rule chain_to_next {
    events {
        a : fw_events
    }
    match<sip:30m> {
        on event seq skip = to_next {
            has a;
        }
    } -> score(80.0)
    entity(ip, a.sip)
    yield security_alerts (sip = a.sip, fail_count = 1, port_count = 1, message = "ok")
}
"#;
    let errs = chain_check(src, &[fw_events_window(), security_alerts_window()]);
    assert!(
        errs.iter()
            .any(|e| e.severity == Severity::Warning && e.message.contains("to_next")),
        "expected to_next warning, got: {:?}",
        errs
    );
}

#[test]
fn pipeline_stage_seq_rejected() {
    let src = r#"
rule pipe_seq {
    events {
        a : fw_events
    }
    match<sip:30m> {
        on event seq {
            has a;
        }
    }
    |> match<sip:30m> {
        on event seq {
            has a;
        }
    } -> score(80.0)
    entity(ip, a.sip)
    yield security_alerts (sip = a.sip, fail_count = 1, port_count = 1, message = "ok")
}
"#;
    let errs = chain_check(src, &[fw_events_window(), security_alerts_window()]);
    let errors: Vec<_> = all_errors(&errs).collect();
    assert!(
        errors.iter().any(|m| m.contains("pipeline stage")),
        "expected pipeline-stage rejection, got: {:?}",
        errors
    );
}
