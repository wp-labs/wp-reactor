use super::*;
use crate::checker::Severity;

#[test]
fn t52_field_added_in_higher_version() {
    let input = r#"
rule r1 {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts@v1 (sip = e.sip, fail_count = 5)
}

rule r2 {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts@v2 (sip = e.sip, fail_count = 5, message = "added")
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, schemas);
    let warnings: Vec<_> = errs
        .iter()
        .filter(|e| e.severity == Severity::Warning)
        .collect();
    assert!(
        warnings
            .iter()
            .any(|e| e.message.contains("field `message` added in @v2")),
        "expected warning about added field `message`, got: {:?}",
        warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
    );
}

#[test]
fn t52_field_removed_in_higher_version() {
    let input = r#"
rule r1 {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts@v1 (sip = e.sip, fail_count = 5)
}

rule r2 {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts@v2 (sip = e.sip)
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, schemas);
    let warnings: Vec<_> = errs
        .iter()
        .filter(|e| e.severity == Severity::Warning)
        .collect();
    assert!(
        warnings
            .iter()
            .any(|e| e.message.contains("field `fail_count` removed in @v2")),
        "expected warning about removed field `fail_count`, got: {:?}",
        warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
    );
}

#[test]
fn t52_same_fields_no_warning() {
    let input = r#"
rule r1 {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts@v1 (sip = e.sip, fail_count = 5)
}

rule r2 {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts@v2 (sip = e.sip, fail_count = 10)
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, schemas);
    let warnings: Vec<_> = errs
        .iter()
        .filter(|e| e.severity == Severity::Warning && e.message.contains("yield"))
        .collect();
    assert!(
        warnings.is_empty(),
        "expected no warnings when fields match, got: {:?}",
        warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
    );
}

#[test]
fn t52_no_version_skipped() {
    // Rules without @vN should not participate in cross-version comparison
    let input = r#"
rule r1 {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip)
}

rule r2 {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 5; } } -> score(70.0)
    entity(ip, e.sip)
    yield security_alerts (sip = e.sip, fail_count = 5)
}
"#;
    let schemas = &[auth_events_window(), security_alerts_window()];
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, schemas);
    let warnings: Vec<_> = errs
        .iter()
        .filter(|e| e.severity == Severity::Warning && e.message.contains("yield"))
        .collect();
    assert!(
        warnings.is_empty(),
        "expected no warnings for unversioned rules, got: {:?}",
        warnings.iter().map(|e| &e.message).collect::<Vec<_>>()
    );
}
