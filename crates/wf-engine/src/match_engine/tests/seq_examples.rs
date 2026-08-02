//! End-to-end verification of the `chain` examples transformed per the design
//! spec §6: compile the chain rule + its inline contract tests, then run every
//! test block through the engine and assert it passes.

use std::time::Duration;

use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use crate::match_engine::contract::run_test;

fn conn_events_schema() -> WindowSchema {
    WindowSchema {
        name: "conn_events".to_string(),
        streams: vec!["network_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef { name: "sip".into(), field_type: FieldType::Base(BaseType::Ip) },
            FieldDef { name: "dip".into(), field_type: FieldType::Base(BaseType::Ip) },
            FieldDef { name: "dport".into(), field_type: FieldType::Base(BaseType::Digit) },
            FieldDef { name: "bytes_out".into(), field_type: FieldType::Base(BaseType::Digit) },
            FieldDef { name: "action".into(), field_type: FieldType::Base(BaseType::Chars) },
            FieldDef { name: "event_time".into(), field_type: FieldType::Base(BaseType::Time) },
        ],
    }
}

fn auth_events_schema() -> WindowSchema {
    WindowSchema {
        name: "auth_events".to_string(),
        streams: vec!["auth_stream".to_string()],
        time_field: Some("event_time".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef { name: "sip".into(), field_type: FieldType::Base(BaseType::Ip) },
            FieldDef { name: "dip".into(), field_type: FieldType::Base(BaseType::Ip) },
            FieldDef { name: "result".into(), field_type: FieldType::Base(BaseType::Chars) },
            FieldDef { name: "service".into(), field_type: FieldType::Base(BaseType::Chars) },
            FieldDef { name: "user".into(), field_type: FieldType::Base(BaseType::Chars) },
            FieldDef { name: "password_hash".into(), field_type: FieldType::Base(BaseType::Chars) },
            FieldDef { name: "event_time".into(), field_type: FieldType::Base(BaseType::Time) },
        ],
    }
}

fn security_alerts_schema() -> WindowSchema {
    WindowSchema {
        name: "security_alerts".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef { name: "sip".into(), field_type: FieldType::Base(BaseType::Ip) },
            FieldDef { name: "dip".into(), field_type: FieldType::Base(BaseType::Ip) },
            FieldDef { name: "user".into(), field_type: FieldType::Base(BaseType::Chars) },
            FieldDef { name: "alert_type".into(), field_type: FieldType::Base(BaseType::Chars) },
            FieldDef { name: "detail".into(), field_type: FieldType::Base(BaseType::Chars) },
        ],
    }
}

/// Parse + compile the source, then run EVERY inline test block and assert it passes.
fn run_all_contracts(source: &str) {
    let schemas = vec![conn_events_schema(), auth_events_schema(), security_alerts_schema()];
    let wfl_file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plans = wf_lang::compile_wfl(&wfl_file, &schemas).expect("compile should succeed");
    assert!(!wfl_file.tests.is_empty(), "expected at least one test block");
    for test in &wfl_file.tests {
        let plan = plans
            .iter()
            .find(|p| p.name == test.rule_name)
            .unwrap_or_else(|| panic!("rule `{}` not found in plans", test.rule_name));
        let time_field = schemas
            .iter()
            .find(|s| plan.binds.iter().any(|b| b.window == s.name))
            .and_then(|s| s.time_field.clone());
        let result = run_test(test, plan, time_field).expect("run_test should succeed");
        assert!(
            result.passed,
            "test `{}` failed: {:?}",
            result.test_name,
            result.failures
        );
    }
}

#[test]
fn chain_rat_propagation_contracts_pass() {
    // Transformed example (spec §6.1): ordered chain + within.
    let source = r#"
rule rat_propagation {
    events {
        scan  : conn_events && (dport == 22 || dport == 445 || dport == 3389) && bytes_out < 1000
        login : auth_events && result == "success"
        xfer  : conn_events && bytes_out >= 10000
    }
    match<sip,dip:30m> {
        on event seq {
            has scan;
            has login within 10m;
            has xfer;
        }
    } -> score(95.0)
    entity(ip, scan.sip)
    yield security_alerts (
        sip = scan.sip,
        dip = scan.dip,
        alert_type = "rat_propagation",
        detail = "scan -> login -> xfer on multiple hosts"
    )
    limits {
        max_memory = "64MB";
        max_instances = 10000;
        on_exceed = throttle;
    }
}

test full_chain_detected for rat_propagation {
  input {
    row(scan, sip = "10.0.0.99", dip = "192.168.1.10", dport = "22", bytes_out = "100", event_time = "2026-01-01T00:00:00Z");
    row(login, sip = "10.0.0.99", dip = "192.168.1.10", result = "success", event_time = "2026-01-01T00:01:00Z");
    row(xfer, sip = "10.0.0.99", dip = "192.168.1.10", bytes_out = "50000", event_time = "2026-01-01T00:02:00Z");
    row(scan, sip = "10.0.0.99", dip = "192.168.1.20", dport = "445", bytes_out = "200", event_time = "2026-01-01T00:03:00Z");
    row(login, sip = "10.0.0.99", dip = "192.168.1.20", result = "success", event_time = "2026-01-01T00:04:00Z");
    row(xfer, sip = "10.0.0.99", dip = "192.168.1.20", bytes_out = "200000", event_time = "2026-01-01T00:05:00Z");
    row(scan, sip = "10.0.0.99", dip = "192.168.1.30", dport = "3389", bytes_out = "150", event_time = "2026-01-01T00:06:00Z");
    row(login, sip = "10.0.0.99", dip = "192.168.1.30", result = "success", event_time = "2026-01-01T00:07:00Z");
    row(xfer, sip = "10.0.0.99", dip = "192.168.1.30", bytes_out = "150000", event_time = "2026-01-01T00:08:00Z");
  }
  expect { hits == 3; }
}

test missing_xfer_step for rat_propagation {
  input {
    row(scan, sip = "10.0.0.50", dip = "192.168.1.10", dport = "22", bytes_out = "100", event_time = "2026-01-01T00:00:00Z");
    row(login, sip = "10.0.0.50", dip = "192.168.1.10", result = "success", event_time = "2026-01-01T00:01:00Z");
    row(scan, sip = "10.0.0.50", dip = "192.168.1.20", dport = "445", bytes_out = "200", event_time = "2026-01-01T00:02:00Z");
    row(login, sip = "10.0.0.50", dip = "192.168.1.20", result = "success", event_time = "2026-01-01T00:03:00Z");
  }
  expect { hits == 0; }
}

test admin_scan_only for rat_propagation {
  input {
    row(scan, sip = "10.0.0.200", dip = "192.168.1.01", dport = "22", bytes_out = "100", event_time = "2026-01-01T00:00:00Z");
    row(scan, sip = "10.0.0.200", dip = "192.168.1.02", dport = "22", bytes_out = "100", event_time = "2026-01-01T00:01:00Z");
    row(scan, sip = "10.0.0.200", dip = "192.168.1.03", dport = "22", bytes_out = "100", event_time = "2026-01-01T00:02:00Z");
  }
  expect { hits == 0; }
}

test single_target_full_chain for rat_propagation {
  input {
    row(scan, sip = "10.0.0.77", dip = "192.168.1.10", dport = "22", bytes_out = "100", event_time = "2026-01-01T00:00:00Z");
    row(login, sip = "10.0.0.77", dip = "192.168.1.10", result = "success", event_time = "2026-01-01T00:01:00Z");
    row(xfer, sip = "10.0.0.77", dip = "192.168.1.10", bytes_out = "50000", event_time = "2026-01-01T00:02:00Z");
  }
  expect { hits == 1; }
}

test out_of_order_login for rat_propagation {
  input {
    row(login, sip = "10.0.0.88", dip = "192.168.1.10", result = "success", event_time = "2026-01-01T00:00:00Z");
    row(scan, sip = "10.0.0.88", dip = "192.168.1.10", dport = "22", bytes_out = "100", event_time = "2026-01-01T00:01:00Z");
    row(xfer, sip = "10.0.0.88", dip = "192.168.1.10", bytes_out = "50000", event_time = "2026-01-01T00:02:00Z");
  }
  expect { hits == 0; }
}
"#;
    run_all_contracts(source);
}

#[test]
fn chain_password_spraying_contracts_pass() {
    // Transformed example (spec §6.2): aggregate step + success terminal step.
    let source = r#"
rule password_spraying {
    events {
        spray : auth_events && result == "failed"
        ok    : auth_events && result == "success"
    }
    match<password_hash:10m> {
        on event seq {
            spray.user | distinct | count >= 5;
            has ok within 5m;
        }
    } -> score(85.0)
    entity(credential, spray.password_hash)
    yield security_alerts (
        sip = ok.sip,
        user = ok.user,
        alert_type = "password_spraying",
        detail = "sprayed >= 5 users then a success followed"
    )
    limits {
        max_memory = "64MB";
        max_instances = 10000;
        on_exceed = throttle;
    }
}

test spray_then_success for password_spraying {
  input {
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u1", event_time = "2026-01-01T00:00:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u2", event_time = "2026-01-01T00:01:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u3", event_time = "2026-01-01T00:02:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u4", event_time = "2026-01-01T00:03:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u5", event_time = "2026-01-01T00:04:00Z");
    row(ok, password_hash = "h1", sip = "10.0.0.9", user = "u5", event_time = "2026-01-01T00:06:00Z");
  }
  expect { hits == 1; }
}

test success_too_late for password_spraying {
  input {
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u1", event_time = "2026-01-01T00:00:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u2", event_time = "2026-01-01T00:01:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u3", event_time = "2026-01-01T00:02:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u4", event_time = "2026-01-01T00:03:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u5", event_time = "2026-01-01T00:04:00Z");
    tick(6m);
    row(ok, password_hash = "h1", sip = "10.0.0.9", user = "u5", event_time = "2026-01-01T00:10:00Z");
  }
  expect { hits == 0; }
}

test spray_only for password_spraying {
  input {
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u1", event_time = "2026-01-01T00:00:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u2", event_time = "2026-01-01T00:01:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u3", event_time = "2026-01-01T00:02:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u4", event_time = "2026-01-01T00:03:00Z");
    row(spray, password_hash = "h1", sip = "10.0.0.9", user = "u5", event_time = "2026-01-01T00:04:00Z");
  }
  expect { hits == 0; }
}
"#;
    run_all_contracts(source);
}
