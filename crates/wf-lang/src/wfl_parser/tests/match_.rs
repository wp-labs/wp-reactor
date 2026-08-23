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

#[test]
fn parse_match_hop_window() {
    let input = r#"
rule hop_test {
    events { e : win }
    match<uid:hop(10s, 2s)> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules.len(), 1);
    let match_clause = &file.rules[0].match_clause;
    match match_clause.window_mode {
        WindowMode::Hop { size, slide } => {
            assert_eq!(size.as_secs(), 10);
            assert_eq!(slide.as_secs(), 2);
        }
        _ => panic!("expected Hop window mode"),
    }
}

#[test]
fn parse_match_hop_rejects_non_multiple_slide() {
    let input = r#"
rule hop_bad {
    events { e : win }
    match<uid:hop(10s, 3s)> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err(), "hop slide must divide size");
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
fn parse_match_millisecond_window() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:100ms> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.keys, vec![FieldRef::Simple("sip".into())]);
    assert_eq!(mc.duration, Duration::from_millis(100));
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

#[test]
fn parse_on_each_with_where() {
    let input = r#"
rule r {
    events { e : win }
    on each e where e.sip == "10.0.0.1" -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let rule = &file.rules[0];

    assert!(rule.match_clause.on_event.is_empty());
    let each = rule.each_clause.as_ref().expect("missing on each clause");
    assert_eq!(each.alias, "e");
    assert_eq!(
        each.filter,
        Some(Expr::BinOp {
            op: BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        })
    );
}

// -----------------------------------------------------------------------
// Chain clause — ordered sequence matching (L1/L2)
// -----------------------------------------------------------------------

#[test]
fn parse_seq_basic() {
    let input = r#"
rule rat_propagation {
    events {
        scan  : conn_events
        login : auth_events
        xfer  : conn_events
    }
    match<sip,dip:30m> {
        on event seq {
            has scan;
            has login within 10m;
            has xfer;
        }
    } -> score(95.0)
    entity(ip, scan.sip)
    yield out (x = scan.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    let chain = mc.seq.as_ref().expect("expected chain clause");
    assert!(!chain.consec, "default is gap (non-consec)");
    assert_eq!(chain.skip, SeqSkip::PastLast, "default skip is past_last");
    assert_eq!(chain.steps.len(), 3);
    // step 1: has scan — synthesized count >= 1
    let s0 = &chain.steps[0];
    assert!(!s0.neg);
    assert!(s0.within.is_none());
    assert_eq!(s0.branch.source, "scan");
    assert_eq!(s0.branch.pipe.transforms, vec![]);
    assert_eq!(s0.branch.pipe.measure, Measure::Count);
    assert_eq!(s0.branch.pipe.cmp, CmpOp::Ge);
    // step 2: has login within 10m
    let s1 = &chain.steps[1];
    assert_eq!(s1.branch.source, "login");
    assert_eq!(s1.within.unwrap().as_secs(), 10 * 60);
    // step 3
    assert_eq!(chain.steps[2].branch.source, "xfer");
}

#[test]
fn parse_seq_modifiers_negation_aggregate() {
    let input = r#"
rule seq_mods {
    events {
        fail  : auth_events
        ok    : auth_events
        spray : auth_events
    }
    match<password_hash:10m> {
        on event seq consec skip = to_next {
            spray.user | distinct | count >= 5;
            has ok within 5m;
            not has fail within 5m;
        }
    } -> score(85.0)
    entity(credential, spray.password_hash)
    yield out (u = ok.user)
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    let chain = mc.seq.as_ref().expect("expected chain clause");
    assert!(chain.consec);
    assert_eq!(chain.skip, SeqSkip::ToNext);
    assert_eq!(chain.steps.len(), 3);
    // aggregate step: spray.user | distinct | count >= 5
    let s0 = &chain.steps[0];
    assert!(!s0.neg);
    assert_eq!(s0.branch.source, "spray");
    assert_eq!(s0.branch.pipe.transforms, vec![Transform::Distinct]);
    assert_eq!(s0.branch.pipe.measure, Measure::Count);
    assert_eq!(s0.branch.pipe.cmp, CmpOp::Ge);
    assert!(s0.within.is_none());
    // has ok within 5m
    let s1 = &chain.steps[1];
    assert!(!s1.neg);
    assert_eq!(s1.branch.source, "ok");
    assert_eq!(s1.within.unwrap().as_secs(), 5 * 60);
    // not has fail within 5m
    let s2 = &chain.steps[2];
    assert!(s2.neg);
    assert_eq!(s2.branch.source, "fail");
    assert_eq!(s2.within.unwrap().as_secs(), 5 * 60);
}

#[test]
fn parse_on_event_any_mode() {
    let input = r#"
rule any_mode {
    events {
        a : win1
        b : win2
    }
    match<k:5m> {
        on event any {
            a | count >= 1;
            b | count >= 1;
        }
    } -> score(50.0)
    entity(ip, "x")
    yield out (x = "y")
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.match_mode, MatchMode::Any);
    assert!(mc.seq.is_none());
    assert_eq!(mc.on_event.len(), 2);
}

#[test]
fn parse_on_event_seq_mode() {
    let input = r#"
rule seq_mode {
    events {
        a : win1
        b : win2
    }
    match<k:5m> {
        on event seq {
            has a;
            has b within 1m;
        }
    } -> score(50.0)
    entity(ip, "x")
    yield out (x = "y")
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.match_mode, MatchMode::Seq);
    let seq = mc.seq.as_ref().expect("expected seq clause");
    assert_eq!(seq.steps.len(), 2);
    assert!(mc.on_event.is_empty());
}

#[test]
fn parse_has_in_any_mode() {
    let input = r#"
rule any_has {
    events {
        a : win1
        b : win2
    }
    match<k:5m> {
        on event any {
            has a;
            has b;
        }
    } -> score(50.0)
    entity(ip, "x")
    yield out (x = "y")
}
"#;
    let file = parse_wfl(input).unwrap();
    let mc = &file.rules[0].match_clause;
    assert_eq!(mc.match_mode, MatchMode::Any);
    assert_eq!(mc.on_event.len(), 2);
}

#[test]
fn within_in_any_mode_is_parse_error() {
    let input = r#"
rule bad_any {
    events {
        a : win1
        b : win2
    }
    match<k:5m> {
        on event any {
            a | count >= 1 within 5s;
        }
    } -> score(50.0)
    entity(ip, "x")
    yield out (x = "y")
}
"#;
    assert!(
        parse_wfl(input).is_err(),
        "within in `on event any` should be a parse error"
    );
}
