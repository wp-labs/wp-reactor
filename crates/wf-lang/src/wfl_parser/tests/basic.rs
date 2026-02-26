use crate::parse_wfl;

// -----------------------------------------------------------------------
// Comments
// -----------------------------------------------------------------------

#[test]
fn parse_slash_slash_comments() {
    let input = r#"
// Header comment
use "security.wfs"

rule brute_force {
    // Inside rule comment
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        // Inside match comment
        on event {
            fail | count >= 3; // end of line comment
        }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (sip = fail.sip)
}

// Between rule and test
test close_hit for brute_force {
    // Inside test comment
    input {
        row(fail, sip = "10.0.0.1", action = "failed", event_time = "2026-01-01T00:00:00Z");
    }
    expect {
        hits == 1;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.rules.len(), 1);
    assert_eq!(file.tests.len(), 1);
}

#[test]
fn parse_comments_in_various_positions() {
    let input = r#"
// Header comment
use "security.wfs"
// Between use and rule

rule test_rule {
    // Inside rule
    events { e : win }
    match<:5m> {
        // Inside match
        on event { e | count >= 1; } // end of line
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}

test close_hit for test_rule {
    // Inside test
    input {
        row(e, sip = "10.0.0.1", event_time = "2026-01-01T00:00:00Z");
    }
    expect {
        hits == 1;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.rules.len(), 1);
    assert_eq!(file.tests.len(), 1);
}

// -----------------------------------------------------------------------
// use declarations
// -----------------------------------------------------------------------

#[test]
fn parse_use_decl() {
    let file = parse_wfl(r#"use "security.wfs""#).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.uses[0].path, "security.wfs");
}

#[test]
fn parse_multiple_uses() {
    let file = parse_wfl(
        r#"
use "security.wfs"
use "dns.wfs"
"#,
    )
    .unwrap();
    assert_eq!(file.uses.len(), 2);
}

// -----------------------------------------------------------------------
// Minimal rule
// -----------------------------------------------------------------------

#[test]
fn parse_minimal_rule() {
    let input = r#"
use "security.wfs"

rule brute_force {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= 3;
        }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} failed {} times", fail.sip, count(fail))
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.rules.len(), 1);

    let rule = &file.rules[0];
    assert_eq!(rule.name, "brute_force");
    assert!(rule.meta.is_none());
    assert_eq!(rule.events.decls.len(), 1);
    assert_eq!(rule.events.decls[0].alias, "fail");
    assert_eq!(rule.events.decls[0].window, "auth_events");
    assert!(rule.events.decls[0].filter.is_some());
}

// -----------------------------------------------------------------------
// Meta block
// -----------------------------------------------------------------------

#[test]
fn parse_meta_block() {
    let input = r#"
rule test_rule {
    meta {
        description = "Test rule"
        mitre = "T1110"
    }
    events { e : win }
    match<:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let meta = file.rules[0].meta.as_ref().unwrap();
    assert_eq!(meta.entries.len(), 2);
    assert_eq!(meta.entries[0].key, "description");
    assert_eq!(meta.entries[0].value, "Test rule");
    assert_eq!(meta.entries[1].key, "mitre");
    assert_eq!(meta.entries[1].value, "T1110");
}

// -----------------------------------------------------------------------
// Events block
// -----------------------------------------------------------------------

#[test]
fn parse_events_with_filter() {
    let input = r#"
rule r {
    events {
        fail : auth_events && action == "failed"
        scan : fw_events
    }
    match<sip:5m> {
        on event { fail | count >= 1; }
    } -> score(50.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let events = &file.rules[0].events;
    assert_eq!(events.decls.len(), 2);
    assert_eq!(events.decls[0].alias, "fail");
    assert!(events.decls[0].filter.is_some());
    assert_eq!(events.decls[1].alias, "scan");
    assert!(events.decls[1].filter.is_none());
}
