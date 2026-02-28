use super::parse_wfg;
use crate::wfg_ast::*;

#[test]
fn test_minimal_scenario() {
    let input = r#"
scenario basic seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100

    stream s1 : LoginWindow 10/s
}
"#;
    let wfg = parse_wfg(input).unwrap();
    assert_eq!(wfg.scenario.name, "basic");
    assert_eq!(wfg.scenario.seed, 42);
    assert_eq!(wfg.scenario.total, 100);
    assert_eq!(wfg.scenario.streams.len(), 1);
    assert_eq!(wfg.scenario.streams[0].alias, "s1");
    assert_eq!(wfg.scenario.streams[0].window, "LoginWindow");
    assert_eq!(wfg.scenario.streams[0].rate.count, 10);
    assert_eq!(wfg.scenario.streams[0].rate.unit, RateUnit::PerSecond);
}

#[test]
fn test_use_declarations() {
    let input = r#"
use "schemas/login.wfs"
use "rules/brute_force.wfl"

scenario test seed 1 {
    time "2024-01-01T00:00:00Z" duration 30m
    total 50

    stream s1 : LoginWindow 5/m
}
"#;
    let wfg = parse_wfg(input).unwrap();
    assert_eq!(wfg.uses.len(), 2);
    assert_eq!(wfg.uses[0].path, "schemas/login.wfs");
    assert_eq!(wfg.uses[1].path, "rules/brute_force.wfl");
}

#[test]
fn test_use_with_semicolons_rejected() {
    let input = r#"
use "schemas/login.wfs";
use "rules/brute_force.wfl";

scenario test seed 1 {
    time "2024-01-01T00:00:00Z" duration 30m
    total 50
    stream s1 : LoginWindow 5/m
}
"#;
    assert!(parse_wfg(input).is_err());
}

#[test]
fn test_stream_with_overrides() {
    let input = r#"
scenario override_test seed 99 {
    time "2024-06-15T12:00:00Z" duration 2h
    total 1000

    stream s1 : LoginWindow 100/s {
        src_ip = ipv4(500)
        username = pattern("user_{}")
        action = enum("login", "logout", "timeout")
        score = range(0, 100)
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let stream = &wfg.scenario.streams[0];
    assert_eq!(stream.overrides.len(), 4);
    assert_eq!(stream.overrides[0].field_name, "src_ip");

    match &stream.overrides[0].gen_expr {
        GenExpr::GenFunc { name, args } => {
            assert_eq!(name, "ipv4");
            assert_eq!(args.len(), 1);
        }
        _ => panic!("expected GenFunc"),
    }

    match &stream.overrides[2].gen_expr {
        GenExpr::GenFunc { name, args } => {
            assert_eq!(name, "enum");
            assert_eq!(args.len(), 3);
        }
        _ => panic!("expected GenFunc"),
    }
}

#[test]
fn test_stream_readable_syntax() {
    let input = r#"
scenario readable_stream seed 99 {
    time "2024-06-15T12:00:00Z" duration 2h
    total 1000

    stream s1 from LoginWindow rate 100/s {
        src_ip = ipv4(500)
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let stream = &wfg.scenario.streams[0];
    assert_eq!(stream.alias, "s1");
    assert_eq!(stream.window, "LoginWindow");
    assert_eq!(stream.rate.count, 100);
    assert_eq!(stream.rate.unit, RateUnit::PerSecond);
}

#[test]
fn test_named_gen_args() {
    let input = r#"
scenario named_args seed 1 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100

    stream s1 : LoginWindow 10/s {
        src_ip = ipv4(pool: 500)
        username = pattern(format: "user_{}")
        score = range(min: 0, max: 100)
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let stream = &wfg.scenario.streams[0];

    match &stream.overrides[0].gen_expr {
        GenExpr::GenFunc { name, args } => {
            assert_eq!(name, "ipv4");
            assert_eq!(args.len(), 1);
            assert_eq!(args[0].name.as_deref(), Some("pool"));
            assert!(matches!(args[0].value, GenExpr::NumberLit(n) if n == 500.0));
        }
        _ => panic!("expected GenFunc"),
    }

    match &stream.overrides[2].gen_expr {
        GenExpr::GenFunc { name, args } => {
            assert_eq!(name, "range");
            assert_eq!(args.len(), 2);
            assert_eq!(args[0].name.as_deref(), Some("min"));
            assert_eq!(args[1].name.as_deref(), Some("max"));
        }
        _ => panic!("expected GenFunc"),
    }
}

#[test]
fn test_backtick_field_override() {
    let input = r#"
scenario backtick_test seed 1 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100

    stream s1 : LoginWindow 10/s {
        `detail.sha256` = pattern("abc{}")
        src_ip = ipv4(100)
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let stream = &wfg.scenario.streams[0];
    assert_eq!(stream.overrides[0].field_name, "detail.sha256");
    assert_eq!(stream.overrides[1].field_name, "src_ip");
}

#[test]
fn test_inject_block() {
    let input = r#"
scenario inject_test seed 7 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 500

    stream s1 : LoginWindow 50/s

    inject for brute_force on [s1] {
        hit 20%;
        near_miss 10%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    assert_eq!(wfg.scenario.injects.len(), 1);
    let inject = &wfg.scenario.injects[0];
    assert_eq!(inject.rule, "brute_force");
    assert_eq!(inject.streams, vec!["s1"]);
    assert_eq!(inject.lines.len(), 2);
    assert_eq!(inject.lines[0].mode, InjectMode::Hit);
    assert_eq!(inject.lines[0].percent, 20.0);
    assert_eq!(inject.lines[1].mode, InjectMode::NearMiss);
    assert_eq!(inject.lines[1].percent, 10.0);
}

#[test]
fn test_inject_with_semicolons() {
    let input = r#"
scenario inject_semi seed 7 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 500

    stream s1 : LoginWindow 50/s

    inject for brute_force on [s1] {
        hit 20%;
        near_miss 10%;
        non_hit 70%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    assert_eq!(wfg.scenario.injects[0].lines.len(), 3);
}

#[test]
fn test_inject_without_semicolon_rejected() {
    let input = r#"
scenario inject_no_semi seed 7 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 500

    stream s1 : LoginWindow 50/s

    inject for brute_force on [s1] {
        hit 20%
        near_miss 10%;
    }
}
"#;
    assert!(parse_wfg(input).is_err());
}

#[test]
fn test_inject_flat_params() {
    let input = r#"
scenario inject_flat seed 7 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 500

    stream s1 : LoginWindow 50/s

    inject for brute_force on [s1] {
        hit 20% threshold = 5 window = 30m;
        near_miss 10%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let inject = &wfg.scenario.injects[0];
    assert_eq!(inject.lines[0].params.len(), 2);
    assert_eq!(inject.lines[0].params[0].name, "threshold");
    assert!(matches!(inject.lines[0].params[0].value, ParamValue::Number(n) if n == 5.0));
    assert_eq!(inject.lines[0].params[1].name, "window");
    assert!(
        matches!(&inject.lines[0].params[1].value, ParamValue::Duration(d) if d.as_secs() == 1800)
    );
}

#[test]
fn test_inject_block_params_supported() {
    let input = r#"
scenario inject_block seed 7 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 500

    stream s1 : LoginWindow 50/s

    inject for brute_force on [s1] {
        hit 20% {
            threshold = 5;
            window = 30m;
        };
        near_miss 10%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let inject = &wfg.scenario.injects[0];
    assert_eq!(inject.lines[0].params.len(), 2);
    assert_eq!(inject.lines[0].params[0].name, "threshold");
    assert_eq!(inject.lines[0].params[1].name, "window");
}

#[test]
fn test_inject_block_params_missing_semicolon_rejected() {
    let input = r#"
scenario inject_block_invalid seed 7 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 500

    stream s1 : LoginWindow 50/s

    inject for brute_force on [s1] {
        hit 20% {
            threshold = 5
        };
    }
}
"#;
    assert!(parse_wfg(input).is_err());
}

#[test]
fn test_faults_block() {
    let input = r#"
scenario faults_test seed 3 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 200

    stream s1 : LoginWindow 10/s

    faults {
        late 5%;
        duplicate 2%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let faults = wfg.scenario.faults.as_ref().unwrap();
    assert_eq!(faults.faults.len(), 2);
    assert_eq!(faults.faults[0].fault_type, FaultType::Late);
    assert_eq!(faults.faults[0].percent, 5.0);
    assert_eq!(faults.faults[1].fault_type, FaultType::Duplicate);
    assert_eq!(faults.faults[1].percent, 2.0);
}

#[test]
fn test_faults_with_semicolons() {
    let input = r#"
scenario faults_semi seed 3 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 200

    stream s1 : LoginWindow 10/s

    faults {
        late 5%;
        duplicate 2%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let faults = wfg.scenario.faults.as_ref().unwrap();
    assert_eq!(faults.faults.len(), 2);
}

#[test]
fn test_oracle_block() {
    let input = r#"
scenario oracle_test seed 5 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100

    stream s1 : LoginWindow 10/s

    oracle {
        expected_hits = 42;
        window_duration = 30m;
        label = "test_label";
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let oracle = wfg.scenario.oracle.as_ref().unwrap();
    assert_eq!(oracle.params.len(), 3);
    assert_eq!(oracle.params[0].name, "expected_hits");
    assert!(matches!(oracle.params[0].value, ParamValue::Number(n) if n == 42.0));
    assert!(matches!(&oracle.params[1].value, ParamValue::Duration(d) if d.as_secs() == 1800));
    assert!(matches!(&oracle.params[2].value, ParamValue::String(s) if s == "test_label"));
}

#[test]
fn test_oracle_without_semicolon_rejected() {
    let input = r#"
scenario oracle_no_semi seed 5 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100

    stream s1 : LoginWindow 10/s

    oracle {
        expected_hits = 42
    }
}
"#;
    assert!(parse_wfg(input).is_err());
}

#[test]
fn test_comment_skipping() {
    let input = r#"
// This is a comment
scenario commented seed 1 {
    // Time settings
    time "2024-01-01T00:00:00Z" duration 1h
    total 50 // inline comment

    // Stream definition
    stream s1 : LoginWindow 5/s
}
"#;
    let wfg = parse_wfg(input).unwrap();
    assert_eq!(wfg.scenario.name, "commented");
    assert_eq!(wfg.scenario.total, 50);
}

#[test]
fn test_rate_units() {
    // per second
    let input = r#"
scenario rate_s seed 1 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100
    stream s1 : W 100/s
}
"#;
    let wfg = parse_wfg(input).unwrap();
    assert_eq!(wfg.scenario.streams[0].rate.unit, RateUnit::PerSecond);

    // per minute
    let input = r#"
scenario rate_m seed 1 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100
    stream s1 : W 50/m
}
"#;
    let wfg = parse_wfg(input).unwrap();
    assert_eq!(wfg.scenario.streams[0].rate.unit, RateUnit::PerMinute);

    // per hour
    let input = r#"
scenario rate_h seed 1 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100
    stream s1 : W 10/h
}
"#;
    let wfg = parse_wfg(input).unwrap();
    assert_eq!(wfg.scenario.streams[0].rate.unit, RateUnit::PerHour);
}

#[test]
fn test_missing_scenario_keyword_fails() {
    let input = r#"
seed 42 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100
}
"#;
    assert!(parse_wfg(input).is_err());
}

#[test]
fn test_time_clause_standard() {
    let input = r#"
scenario inline_time seed 1 {
    time "2024-01-01T00:00:00Z" duration 45m
    total 100
    stream s1 : W 10/s
}
"#;
    let wfg = parse_wfg(input).unwrap();
    assert_eq!(wfg.scenario.time_clause.start, "2024-01-01T00:00:00Z");
    assert_eq!(
        wfg.scenario.time_clause.duration,
        std::time::Duration::from_secs(2700)
    );
}

#[test]
fn test_time_start_keyword_rejected() {
    let input = r#"
scenario old_time seed 1 {
    time start "2024-01-01T00:00:00Z" duration 1h
    total 100
    stream s1 : W 10/s
}
"#;
    assert!(parse_wfg(input).is_err());
}

#[test]
fn test_time_block_syntax_rejected() {
    let input = r#"
scenario old_time_block seed 1 {
    time {
        start = "2024-01-01T00:00:00Z"
        duration = 1h
    }
    total 100
    stream s1 : W 10/s
}
"#;
    assert!(parse_wfg(input).is_err());
}

#[test]
fn test_full_scenario() {
    let input = r#"
use "login.wfs"
use "brute_force.wfl"

scenario full_test seed 12345 {
    time "2024-01-01T00:00:00Z" duration 2h
    total 10000

    stream login_events : LoginWindow 100/s {
        src_ip = ipv4(1000)
        username = pattern("user_{}")
    }

    stream dns_events : DnsWindow 50/s

    inject for brute_force on [login_events] {
        hit 15%;
        near_miss 5%;
        non_hit 80%;
    }

    faults {
        late 3%;
        duplicate 1%;
    }

    oracle {
        expected_hits = 150;
        tolerance = 10;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    assert_eq!(wfg.uses.len(), 2);
    assert_eq!(wfg.scenario.name, "full_test");
    assert_eq!(wfg.scenario.seed, 12345);
    assert_eq!(wfg.scenario.total, 10000);
    assert_eq!(wfg.scenario.streams.len(), 2);
    assert_eq!(wfg.scenario.injects.len(), 1);
    assert!(wfg.scenario.faults.is_some());
    assert!(wfg.scenario.oracle.is_some());
}

#[test]
fn test_field_override_with_semicolons_rejected() {
    let input = r#"
scenario semi_test seed 1 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100

    stream s1 : LoginWindow 10/s {
        src_ip = ipv4(500);
        username = pattern("user_{}");
    }
}
"#;
    assert!(parse_wfg(input).is_err());
}

#[test]
fn test_bool_literal_vs_ident_prefix() {
    // Identifiers like `true_x` and `false_flag` must NOT be consumed as
    // boolean literals — they are gen function names.
    let input = r#"
scenario bool_prefix seed 1 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100

    stream s1 : W 10/s {
        a = true
        b = false
        c = true_x
        d = false_flag(1, 2)
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let ov = &wfg.scenario.streams[0].overrides;
    assert_eq!(ov[0].gen_expr, GenExpr::BoolLit(true));
    assert_eq!(ov[1].gen_expr, GenExpr::BoolLit(false));
    assert!(
        matches!(&ov[2].gen_expr, GenExpr::GenFunc { name, args } if name == "true_x" && args.is_empty())
    );
    assert!(
        matches!(&ov[3].gen_expr, GenExpr::GenFunc { name, args } if name == "false_flag" && args.len() == 2)
    );
}

#[test]
fn test_fault_type_all_variants() {
    let input = r#"
scenario all_faults seed 1 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100

    stream s1 : W 10/s

    faults {
        out_of_order 10%;
        late 5%;
        duplicate 3%;
        drop 2%;
    }
}
"#;
    let wfg = parse_wfg(input).unwrap();
    let faults = wfg.scenario.faults.as_ref().unwrap();
    assert_eq!(faults.faults.len(), 4);
    assert_eq!(faults.faults[0].fault_type, FaultType::OutOfOrder);
    assert_eq!(faults.faults[0].percent, 10.0);
    assert_eq!(faults.faults[1].fault_type, FaultType::Late);
    assert_eq!(faults.faults[1].percent, 5.0);
    assert_eq!(faults.faults[2].fault_type, FaultType::Duplicate);
    assert_eq!(faults.faults[2].percent, 3.0);
    assert_eq!(faults.faults[3].fault_type, FaultType::Drop);
    assert_eq!(faults.faults[3].percent, 2.0);
}

#[test]
fn test_fault_unknown_type_rejected() {
    let input = r#"
scenario bad_fault seed 1 {
    time "2024-01-01T00:00:00Z" duration 1h
    total 100

    stream s1 : W 10/s

    faults {
        late_arrival 5%;
    }
}
"#;
    assert!(parse_wfg(input).is_err());
}
