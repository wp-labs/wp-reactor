use std::time::Duration;

use crate::ast::*;
use crate::parse_wfl;

// -----------------------------------------------------------------------
// Match clause - Session window (L3)
// -----------------------------------------------------------------------

#[test]
fn parse_match_session_window() {
    let input = r#"
rule session_test {
    events { e : win }
    match<uid:session(30m)> {
        on event { e | count >= 1; }
        on close { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules.len(), 1);
    let match_clause = &file.rules[0].match_clause;
    assert_eq!(match_clause.keys.len(), 1);
    match match_clause.window_mode {
        WindowMode::Session(gap) => {
            assert_eq!(gap.as_secs(), 30 * 60);
        }
        _ => panic!("expected Session window mode"),
    }
}

#[test]
fn parse_match_session_window_no_keys() {
    let input = r#"
rule session_test {
    events { e : win }
    match<:session(5m)> {
        on event { e | count >= 1; }
        on close { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let match_clause = &file.rules[0].match_clause;
    assert!(match_clause.keys.is_empty());
    match match_clause.window_mode {
        WindowMode::Session(gap) => {
            assert_eq!(gap.as_secs(), 5 * 60);
        }
        _ => panic!("expected Session window mode"),
    }
}

// -----------------------------------------------------------------------
// Match clause - Sliding/Fixed window
// -----------------------------------------------------------------------

#[test]
fn parse_match_single_key() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.keys, vec![FieldRef::Simple("sip".into())]);
    assert_eq!(mc.duration, Duration::from_secs(300));
}

#[test]
fn parse_match_compound_keys() {
    let input = r#"
rule r {
    events { e : win }
    match<sip,dport:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.keys.len(), 2);
    assert_eq!(mc.keys[0], FieldRef::Simple("sip".into()));
    assert_eq!(mc.keys[1], FieldRef::Simple("dport".into()));
}

#[test]
fn parse_match_no_keys() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert!(file.rules[0].match_clause.keys.is_empty());
}

// -----------------------------------------------------------------------
// Match steps and OR branches
// -----------------------------------------------------------------------

#[test]
fn parse_multiple_steps() {
    let input = r#"
rule r {
    events { fail : auth_events  scan : fw_events }
    match<sip:5m> {
        on event {
            fail | count >= 3;
            scan.dport | distinct | count > 10;
        }
    } -> score(80.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let steps = &file.rules[0].match_clause.on_event;
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].branches[0].source, "fail");
    assert!(steps[0].branches[0].field.is_none());
    assert_eq!(steps[0].branches[0].pipe.measure, Measure::Count);
    assert_eq!(steps[0].branches[0].pipe.cmp, CmpOp::Ge);

    assert_eq!(steps[1].branches[0].source, "scan");
    assert_eq!(
        steps[1].branches[0].field,
        Some(FieldSelector::Dot("dport".into()))
    );
    assert_eq!(
        steps[1].branches[0].pipe.transforms,
        vec![Transform::Distinct]
    );
    assert_eq!(steps[1].branches[0].pipe.measure, Measure::Count);
    assert_eq!(steps[1].branches[0].pipe.cmp, CmpOp::Gt);
}

#[test]
fn parse_or_branches() {
    let input = r#"
rule r {
    events { a : win  b : win2 }
    match<sip:5m> {
        on event {
            a | count >= 3 || b | count >= 5;
        }
    } -> score(60.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let step = &file.rules[0].match_clause.on_event[0];
    assert_eq!(step.branches.len(), 2);
    assert_eq!(step.branches[0].source, "a");
    assert_eq!(step.branches[1].source, "b");
}

// -----------------------------------------------------------------------
// on close block
// -----------------------------------------------------------------------

#[test]
fn parse_on_close() {
    let input = r#"
rule r {
    events { req : dns_query  resp : dns_response }
    match<query_id:30s> {
        on event {
            req | count >= 1;
        }
        on close {
            resp && close_reason == "timeout" | count == 0;
        }
    } -> score(50.0)
    entity(ip, req.sip)
    yield out (x = req.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.on_event.len(), 1);
    assert!(mc.on_close.is_some());
    let close_block = mc.on_close.as_ref().unwrap();
    assert_eq!(close_block.mode, CloseMode::Or);
    assert_eq!(close_block.steps.len(), 1);
    assert_eq!(close_block.steps[0].branches[0].source, "resp");
    assert!(close_block.steps[0].branches[0].guard.is_some());
    assert_eq!(
        close_block.steps[0].branches[0].pipe.measure,
        Measure::Count
    );
    assert_eq!(close_block.steps[0].branches[0].pipe.cmp, CmpOp::Eq);
}

#[test]
fn parse_and_close() {
    let input = r#"
rule r {
    events { req : dns_query  resp : dns_response }
    match<query_id:30s> {
        on event {
            req | count >= 1;
        }
        and close {
            resp && close_reason == "timeout" | count == 0;
        }
    } -> score(50.0)
    entity(ip, req.sip)
    yield out (x = req.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.on_event.len(), 1);
    assert!(mc.on_close.is_some());
    let close_block = mc.on_close.as_ref().unwrap();
    assert_eq!(close_block.mode, CloseMode::And);
    assert_eq!(close_block.steps.len(), 1);
    assert_eq!(close_block.steps[0].branches[0].source, "resp");
}

// -----------------------------------------------------------------------
// Fixed window
// -----------------------------------------------------------------------

#[test]
fn parse_fixed_window() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:1h:fixed> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.keys, vec![FieldRef::Simple("sip".into())]);
    assert_eq!(mc.duration, Duration::from_secs(3600));
    assert_eq!(mc.window_mode, WindowMode::Fixed);
}

#[test]
fn parse_sliding_default() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.duration, Duration::from_secs(300));
    assert_eq!(mc.window_mode, WindowMode::Sliding);
}

#[test]
fn parse_fixed_no_keys() {
    let input = r#"
rule r {
    events { e : win }
    match<:10s:fixed> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert!(mc.keys.is_empty());
    assert_eq!(mc.duration, Duration::from_secs(10));
    assert_eq!(mc.window_mode, WindowMode::Fixed);
}
