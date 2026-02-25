use crate::ast::*;
use crate::parse_wfl;

// ---------------------------------------------------------------------------
// Pattern declaration parsing
// ---------------------------------------------------------------------------

#[test]
fn parse_pattern_decl() {
    let input = r#"
pattern burst(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(50.0)
}

rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.patterns.len(), 1);
    let pat = &file.patterns[0];
    assert_eq!(pat.name, "burst");
    assert_eq!(pat.params, vec!["alias", "key", "win", "threshold"]);
    assert!(pat.body.contains("match<${key}:${win}>"));
    assert!(pat.body.contains("score(50.0)"));
}

// ---------------------------------------------------------------------------
// Pattern invocation in rule
// ---------------------------------------------------------------------------

#[test]
fn parse_rule_with_pattern_invocation() {
    let input = r#"
pattern burst(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(50.0)
}

rule brute_force {
    events { e : auth_events }
    burst(e, sip, 5m, 5)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules.len(), 1);
    let rule = &file.rules[0];
    assert_eq!(rule.name, "brute_force");

    // Match clause should be expanded from pattern
    assert_eq!(rule.match_clause.keys, vec![FieldRef::Simple("sip".into())]);
    assert_eq!(rule.match_clause.duration, std::time::Duration::from_secs(300));
    assert_eq!(rule.match_clause.on_event.len(), 1);
    assert_eq!(rule.match_clause.on_event[0].branches[0].source, "e");

    // Score should be from the pattern body
    assert_eq!(rule.score.expr, Expr::Number(50.0));

    // Pattern origin should be recorded
    let origin = rule.pattern_origin.as_ref().expect("pattern_origin should be Some");
    assert_eq!(origin.pattern_name, "burst");
    assert_eq!(origin.args, vec!["e", "sip", "5m", "5"]);
}

// ---------------------------------------------------------------------------
// Wrong arg count
// ---------------------------------------------------------------------------

#[test]
fn parse_pattern_invocation_wrong_arg_count() {
    let input = r#"
pattern burst(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(50.0)
}

rule r {
    events { e : auth_events }
    burst(e, sip)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let result = parse_wfl(input);
    assert!(result.is_err(), "should fail with wrong arg count");
}

// ---------------------------------------------------------------------------
// Multiple patterns
// ---------------------------------------------------------------------------

#[test]
fn parse_multiple_patterns() {
    let input = r#"
pattern burst(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(50.0)
}

pattern scan(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | distinct | count >= ${threshold}; }
    } -> score(60.0)
}

rule r1 {
    events { e : auth_events }
    burst(e, sip, 5m, 5)
    entity(ip, e.sip)
    yield out (x = e.sip)
}

rule r2 {
    events { c : conn_events }
    scan(c, sip, 10m, 10)
    entity(ip, c.sip)
    yield out (x = c.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.patterns.len(), 2);
    assert_eq!(file.patterns[0].name, "burst");
    assert_eq!(file.patterns[1].name, "scan");
    assert_eq!(file.rules.len(), 2);

    // r1 uses burst
    let origin1 = file.rules[0].pattern_origin.as_ref().unwrap();
    assert_eq!(origin1.pattern_name, "burst");

    // r2 uses scan, which has distinct transform
    let origin2 = file.rules[1].pattern_origin.as_ref().unwrap();
    assert_eq!(origin2.pattern_name, "scan");
    assert_eq!(
        file.rules[1].match_clause.on_event[0].branches[0].pipe.transforms,
        vec![Transform::Distinct]
    );
}

// ---------------------------------------------------------------------------
// Rule without pattern (still works)
// ---------------------------------------------------------------------------

#[test]
fn parse_rule_without_pattern_still_works() {
    let input = r#"
pattern burst(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(50.0)
}

rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(70.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.patterns.len(), 1);
    assert_eq!(file.rules.len(), 1);
    assert!(file.rules[0].pattern_origin.is_none());
    assert_eq!(file.rules[0].score.expr, Expr::Number(70.0));
}

// ---------------------------------------------------------------------------
// Pattern with fixed window
// ---------------------------------------------------------------------------

#[test]
fn parse_pattern_with_fixed_window() {
    let input = r#"
pattern fixed_burst(alias, key, win, threshold) {
    match<${key}:${win}:fixed> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(80.0)
}

rule r {
    events { e : win }
    fixed_burst(e, sip, 1h, 10)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let rule = &file.rules[0];
    assert_eq!(rule.match_clause.window_mode, WindowMode::Fixed);
    assert_eq!(rule.match_clause.duration, std::time::Duration::from_secs(3600));
}
