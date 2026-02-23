use super::*;

use crate::wfl_parser::parse_wfl;

#[test]
fn contract_unknown_rule() {
    let input = r#"
contract ct for nonexistent {
    given { row(e, x = 1); }
    expect { hits == 1; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let errs = check_wfl(&file, &[]);
    assert!(
        errs.iter().any(|e| e.message.contains("not found")),
        "expected error about unknown rule, got: {:?}",
        errs
    );
}

#[test]
fn contract_unknown_alias() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
contract ct for r {
    given { row(bad, x = 1); }
    expect { hits == 1; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let errs = check_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        errs.iter().any(|e| e.message.contains("not declared")),
        "expected error about unknown alias in contract, got: {:?}",
        errs
    );
}

#[test]
fn contract_valid() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
contract ct for r {
    given { row(e, action = "failed"); }
    expect { hits == 1; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let errs = check_wfl(&file, &[auth_events_window(), output_window()]);
    // The rule itself is valid and contract refs are valid
    assert!(errs.is_empty(), "expected no errors, got: {:?}", errs);
}
