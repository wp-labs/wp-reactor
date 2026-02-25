use crate::ast::*;
use crate::parse_wfl;

// ---------------------------------------------------------------------------
// Single chain
// ---------------------------------------------------------------------------

#[test]
fn parse_conv_single_chain_sort_top() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-score) | top(10) ; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let conv = file.rules[0].conv.as_ref().expect("conv should be Some");
    assert_eq!(conv.chains.len(), 1);
    assert_eq!(conv.chains[0].steps.len(), 2);
    match &conv.chains[0].steps[0] {
        ConvStep::Sort(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(keys[0].descending);
            assert_eq!(keys[0].expr, Expr::Field(FieldRef::Simple("score".into())));
        }
        other => panic!("expected Sort, got {:?}", other),
    }
    match &conv.chains[0].steps[1] {
        ConvStep::Top(n) => assert_eq!(*n, 10),
        other => panic!("expected Top, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Multiple chains
// ---------------------------------------------------------------------------

#[test]
fn parse_conv_multiple_chains() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-score) ; where(count > 5) ; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let conv = file.rules[0].conv.as_ref().expect("conv should be Some");
    assert_eq!(conv.chains.len(), 2);
    assert_eq!(conv.chains[0].steps.len(), 1);
    assert_eq!(conv.chains[1].steps.len(), 1);
    assert!(matches!(&conv.chains[0].steps[0], ConvStep::Sort(_)));
    assert!(matches!(&conv.chains[1].steps[0], ConvStep::Where(_)));
}

// ---------------------------------------------------------------------------
// All ops in one chain
// ---------------------------------------------------------------------------

#[test]
fn parse_conv_all_ops() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-f) | top(5) | dedup(sip) | where(count > 3) ; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let conv = file.rules[0].conv.as_ref().expect("conv should be Some");
    assert_eq!(conv.chains.len(), 1);
    let steps = &conv.chains[0].steps;
    assert_eq!(steps.len(), 4);
    assert!(matches!(&steps[0], ConvStep::Sort(_)));
    assert!(matches!(&steps[1], ConvStep::Top(5)));
    assert!(matches!(&steps[2], ConvStep::Dedup(_)));
    assert!(matches!(&steps[3], ConvStep::Where(_)));
}

// ---------------------------------------------------------------------------
// Sort ascending (no dash)
// ---------------------------------------------------------------------------

#[test]
fn parse_conv_sort_ascending() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(name) ; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let conv = file.rules[0].conv.as_ref().unwrap();
    match &conv.chains[0].steps[0] {
        ConvStep::Sort(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(!keys[0].descending);
            assert_eq!(keys[0].expr, Expr::Field(FieldRef::Simple("name".into())));
        }
        other => panic!("expected Sort, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Sort with multiple keys
// ---------------------------------------------------------------------------

#[test]
fn parse_conv_sort_multiple_keys() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { sort(-score, name) ; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let conv = file.rules[0].conv.as_ref().unwrap();
    match &conv.chains[0].steps[0] {
        ConvStep::Sort(keys) => {
            assert_eq!(keys.len(), 2);
            assert!(keys[0].descending);
            assert!(!keys[1].descending);
        }
        other => panic!("expected Sort, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Rule without conv → conv is None
// ---------------------------------------------------------------------------

#[test]
fn parse_rule_without_conv() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert!(file.rules[0].conv.is_none());
}

// ---------------------------------------------------------------------------
// Conv + limits coexist
// ---------------------------------------------------------------------------

#[test]
fn parse_conv_with_limits() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { top(5) ; }
    limits { max_instances = 100; on_exceed = throttle; }
}
"#;
    let file = parse_wfl(input).unwrap();
    assert!(file.rules[0].conv.is_some());
    assert!(file.rules[0].limits.is_some());
}

// ---------------------------------------------------------------------------
// Dedup with qualified field
// ---------------------------------------------------------------------------

#[test]
fn parse_conv_dedup_qualified_field() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { dedup(e.sip) ; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let conv = file.rules[0].conv.as_ref().unwrap();
    match &conv.chains[0].steps[0] {
        ConvStep::Dedup(expr) => {
            assert_eq!(
                *expr,
                Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))
            );
        }
        other => panic!("expected Dedup, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Where with boolean expression
// ---------------------------------------------------------------------------

#[test]
fn parse_conv_where_complex_expr() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:1h:fixed> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv { where(count > 5 && score >= 10) ; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let conv = file.rules[0].conv.as_ref().unwrap();
    assert!(matches!(&conv.chains[0].steps[0], ConvStep::Where(_)));
}
