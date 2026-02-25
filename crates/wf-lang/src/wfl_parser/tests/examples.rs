use std::time::Duration;

use crate::ast::*;
use crate::parse_wfl;

// -----------------------------------------------------------------------
// Full examples from design docs
// -----------------------------------------------------------------------

#[test]
fn parse_brute_force_then_scan() {
    let input = r#"
use "security.wfs"

rule brute_force_then_scan {
    meta {
        description = "Login failures followed by port scan from same IP"
        mitre       = "T1110, T1046"
    }

    events {
        fail : auth_events && action == "failed"
        scan : fw_events
    }

    match<sip:5m> {
        on event {
            fail | count >= 3;
            scan.dport | distinct | count > 10;
        }
    } -> score(80.0)

    entity(ip, fail.sip)

    yield security_alerts (
        sip        = fail.sip,
        fail_count = count(fail),
        port_count = distinct(scan.dport),
        message    = fmt("{}: brute force then port scan detected", fail.sip)
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.rules.len(), 1);

    let rule = &file.rules[0];
    assert_eq!(rule.name, "brute_force_then_scan");
    assert!(rule.meta.is_some());
    assert_eq!(rule.meta.as_ref().unwrap().entries.len(), 2);
    assert_eq!(rule.events.decls.len(), 2);
    assert_eq!(rule.match_clause.keys, vec![FieldRef::Simple("sip".into())]);
    assert_eq!(rule.match_clause.duration, Duration::from_secs(300));
    assert_eq!(rule.match_clause.on_event.len(), 2);
    assert!(rule.match_clause.on_close.is_none());
    assert_eq!(rule.score.expr, Expr::Number(80.0));
    assert_eq!(rule.entity.entity_type, EntityTypeVal::Ident("ip".into()));
    assert_eq!(rule.yield_clause.target, "security_alerts");
    assert_eq!(rule.yield_clause.args.len(), 4);
}

#[test]
fn parse_dns_no_response() {
    let input = r#"
use "dns.wfs"

rule dns_no_response {
    events {
        req : dns_query
        resp : dns_response
    }
    match<query_id:30s> {
        on event {
            req | count >= 1;
        }
        on close {
            resp && close_reason == "timeout" | count == 0;
        }
    } -> score(50.0)
    entity(ip, req.sip)
    yield security_alerts (
        sip = req.sip,
        domain = req.domain,
        message = fmt("{} query {} no response", req.sip, req.domain)
    )
}
"#;
    let file = parse_wfl(input).unwrap();
    let rule = &file.rules[0];
    assert_eq!(rule.name, "dns_no_response");
    assert!(rule.match_clause.on_close.is_some());
    let close = rule.match_clause.on_close.as_ref().unwrap();
    assert_eq!(close.steps.len(), 1);
    assert_eq!(close.steps[0].branches[0].source, "resp");
}

// -----------------------------------------------------------------------
// Multiple rules
// -----------------------------------------------------------------------

#[test]
fn parse_multiple_rules() {
    let input = r#"
use "sec.wfs"

rule r1 {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}

rule r2 {
    events { e : win }
    match<:1h> { on event { e | sum >= 100; } } -> score(30.0)
    entity(host, e.host)
    yield out (x = e.host)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules.len(), 2);
    assert_eq!(file.rules[0].name, "r1");
    assert_eq!(file.rules[1].name, "r2");
}

// -----------------------------------------------------------------------
// Empty file
// -----------------------------------------------------------------------

#[test]
fn parse_empty_file() {
    let file = parse_wfl("").unwrap();
    assert!(file.uses.is_empty());
    assert!(file.rules.is_empty());
    assert!(file.tests.is_empty());
}
