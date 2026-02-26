use std::time::Duration;

use super::super::parse_wfs;

// -----------------------------------------------------------------------
// Window declarations
// -----------------------------------------------------------------------

#[test]
fn parse_minimal_window() {
    let input = r#"
window ip_blocklist {
    over = 0
    fields {
        ip: ip
        category: chars
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 1);
    let w = &schemas[0];
    assert_eq!(w.name, "ip_blocklist");
    assert!(w.streams.is_empty());
    assert!(w.time_field.is_none());
    assert_eq!(w.over, Duration::ZERO);
    assert_eq!(w.fields.len(), 2);
}

#[test]
fn parse_window_with_stream_and_time() {
    let input = r#"
window auth_events {
    stream = "auth"
    time = event_time
    over = 30m

    fields {
        username: chars
        sip: ip
        event_time: time
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 1);
    let w = &schemas[0];
    assert_eq!(w.name, "auth_events");
    assert_eq!(w.streams, vec!["auth"]);
    assert_eq!(w.time_field.as_deref(), Some("event_time"));
    assert_eq!(w.over, Duration::from_secs(30 * 60));
    assert_eq!(w.fields.len(), 3);
}

#[test]
fn parse_window_multi_stream() {
    let input = r#"
window fw_events {
    stream = ["firewall", "netflow"]
    time = event_time
    over = 30m
    fields {
        sip: ip
        dip: ip
        event_time: time
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    let w = &schemas[0];
    assert_eq!(w.streams, vec!["firewall", "netflow"]);
}

#[test]
fn parse_multiple_windows() {
    let input = r#"
window a {
    over = 0
    fields { x: chars }
}

window b {
    time = ts
    over = 5m
    fields { ts: time  v: digit }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 2);
    assert_eq!(schemas[0].name, "a");
    assert_eq!(schemas[1].name, "b");
}

#[test]
fn parse_with_comments() {
    let input = r#"
// Security window definitions
window alerts {
    // No stream — yield only
    over = 0
    fields {
        // Alert fields
        rule_name: chars
        score: float
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].fields.len(), 2);
}

// -----------------------------------------------------------------------
// Full security.wfs example
// -----------------------------------------------------------------------

#[test]
fn parse_security_ws() {
    let input = r#"
window auth_events {
    stream = "auth"
    time = event_time
    over = 30m
    fields {
        username: chars
        uid: digit
        sip: ip
        dport: digit
        action: chars
        result: chars
        event_time: time
        process: chars
    }
}

window fw_events {
    stream = ["firewall", "netflow"]
    time = event_time
    over = 30m
    fields {
        sip: ip
        dip: ip
        dport: digit
        action: chars
        bytes: digit
        event_time: time
        protocol: chars
    }
}

window ip_blocklist {
    over = 0
    fields {
        ip: ip
        threat_level: chars
        category: chars
    }
}

window security_alerts {
    time = emit_time
    over = 48h
    fields {
        sip: ip
        rule_name: chars
        fail_count: digit
        port_count: digit
        threat_level: chars
        message: chars
        emit_time: time
        score: float
    }
}
"#;
    let schemas = parse_wfs(input).unwrap();
    assert_eq!(schemas.len(), 4);
    assert_eq!(schemas[0].name, "auth_events");
    assert_eq!(schemas[0].streams, vec!["auth"]);
    assert_eq!(schemas[0].fields.len(), 8);
    assert_eq!(schemas[1].name, "fw_events");
    assert_eq!(schemas[1].streams, vec!["firewall", "netflow"]);
    assert_eq!(schemas[1].fields.len(), 7);
    assert_eq!(schemas[2].name, "ip_blocklist");
    assert!(schemas[2].streams.is_empty());
    assert_eq!(schemas[2].over, Duration::ZERO);
    assert_eq!(schemas[3].name, "security_alerts");
    assert_eq!(schemas[3].over, Duration::from_secs(48 * 3600));
}
