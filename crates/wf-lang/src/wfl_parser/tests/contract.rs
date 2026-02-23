use std::time::Duration;

use crate::ast::*;
use crate::parse_wfl;

// -----------------------------------------------------------------------
// Contract blocks
// -----------------------------------------------------------------------

#[test]
fn parse_contract_full() {
    let input = r#"
contract dns_no_response_timeout for dns_no_response {
    given {
        row(req,
            query_id = "q-1",
            sip = "10.0.0.8",
            domain = "evil.test",
            event_time = "2026-02-17T10:00:00Z"
        );
        tick(31s);
    }
    expect {
        hits == 1;
        hit[0].score == 50.0;
        hit[0].close_reason == "timeout";
        hit[0].entity_type == "ip";
        hit[0].entity_id == "10.0.0.8";
        hit[0].field("domain") == "evil.test";
    }
    options {
        close_trigger = timeout;
        eval_mode = strict;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.contracts.len(), 1);

    let c = &file.contracts[0];
    assert_eq!(c.name, "dns_no_response_timeout");
    assert_eq!(c.rule_name, "dns_no_response");

    // given
    assert_eq!(c.given.len(), 2);
    match &c.given[0] {
        GivenStmt::Row { alias, fields } => {
            assert_eq!(alias, "req");
            assert_eq!(fields.len(), 4);
            assert_eq!(fields[0].name, "query_id");
            assert_eq!(fields[1].name, "sip");
            assert_eq!(fields[2].name, "domain");
            assert_eq!(fields[3].name, "event_time");
        }
        other => panic!("expected Row, got {other:?}"),
    }
    assert_eq!(c.given[1], GivenStmt::Tick(Duration::from_secs(31)));

    // expect
    assert_eq!(c.expect.len(), 6);
    assert_eq!(
        c.expect[0],
        ExpectStmt::Hits {
            cmp: CmpOp::Eq,
            count: 1
        }
    );
    match &c.expect[1] {
        ExpectStmt::HitAssert { index, assert } => {
            assert_eq!(*index, 0);
            assert_eq!(
                *assert,
                HitAssert::Score {
                    cmp: CmpOp::Eq,
                    value: 50.0
                }
            );
        }
        other => panic!("expected HitAssert, got {other:?}"),
    }
    match &c.expect[2] {
        ExpectStmt::HitAssert { assert, .. } => {
            assert_eq!(
                *assert,
                HitAssert::CloseReason {
                    value: "timeout".into()
                }
            );
        }
        other => panic!("expected HitAssert, got {other:?}"),
    }
    match &c.expect[5] {
        ExpectStmt::HitAssert { assert, .. } => {
            assert!(
                matches!(assert, HitAssert::Field { name, cmp: CmpOp::Eq, .. } if name == "domain")
            );
        }
        other => panic!("expected HitAssert field, got {other:?}"),
    }

    // options
    let opts = c.options.as_ref().unwrap();
    assert_eq!(opts.close_trigger, Some(CloseTrigger::Timeout));
    assert_eq!(opts.eval_mode, Some(EvalMode::Strict));
}

#[test]
fn parse_contract_no_options() {
    let input = r#"
contract simple_test for my_rule {
    given {
        row(e, action = "failed");
        tick(5m);
    }
    expect {
        hits >= 1;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    let c = &file.contracts[0];
    assert_eq!(c.name, "simple_test");
    assert_eq!(c.rule_name, "my_rule");
    assert!(c.options.is_none());
    assert_eq!(c.given.len(), 2);
    assert_eq!(
        c.expect[0],
        ExpectStmt::Hits {
            cmp: CmpOp::Ge,
            count: 1
        }
    );
}

#[test]
fn parse_contract_options_only_close_trigger() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect { hits == 0; }
    options { close_trigger = flush; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let opts = file.contracts[0].options.as_ref().unwrap();
    assert_eq!(opts.close_trigger, Some(CloseTrigger::Flush));
    assert_eq!(opts.eval_mode, None);
}

#[test]
fn parse_contract_options_only_eval_mode() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect { hits == 0; }
    options { eval_mode = lenient; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let opts = file.contracts[0].options.as_ref().unwrap();
    assert_eq!(opts.close_trigger, None);
    assert_eq!(opts.eval_mode, Some(EvalMode::Lenient));
}

#[test]
fn parse_contract_options_eos() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect { hits == 1; }
    options { close_trigger = eos; }
}
"#;
    let file = parse_wfl(input).unwrap();
    let opts = file.contracts[0].options.as_ref().unwrap();
    assert_eq!(opts.close_trigger, Some(CloseTrigger::Eos));
}

#[test]
fn parse_contract_field_assert_expr() {
    let input = r#"
contract ct for r {
    given { row(e, count = 10); }
    expect {
        hits == 1;
        hit[0].field("count") >= 5 + 3;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    match &file.contracts[0].expect[1] {
        ExpectStmt::HitAssert {
            index,
            assert: HitAssert::Field { name, cmp, value },
        } => {
            assert_eq!(*index, 0);
            assert_eq!(name, "count");
            assert_eq!(*cmp, CmpOp::Ge);
            assert!(matches!(value, Expr::BinOp { op: BinOp::Add, .. }));
        }
        other => panic!("expected field assert, got {other:?}"),
    }
}

#[test]
fn parse_contract_string_field_name() {
    let input = r#"
contract ct for r {
    given {
        row(e, "detail.sha256" = "abc123");
    }
    expect { hits == 1; }
}
"#;
    let file = parse_wfl(input).unwrap();
    match &file.contracts[0].given[0] {
        GivenStmt::Row { fields, .. } => {
            assert_eq!(fields[0].name, "detail.sha256");
        }
        other => panic!("expected Row, got {other:?}"),
    }
}

#[test]
fn parse_multiple_contracts() {
    let input = r#"
contract ct1 for r1 {
    given { row(e, x = 1); }
    expect { hits == 1; }
}
contract ct2 for r2 {
    given { row(e, x = 2); tick(10s); }
    expect { hits == 0; }
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.contracts.len(), 2);
    assert_eq!(file.contracts[0].name, "ct1");
    assert_eq!(file.contracts[1].name, "ct2");
}

#[test]
fn parse_rules_and_contracts() {
    let input = r#"
use "security.wfs"

rule brute_force {
    events { fail : auth_events && action == "failed" }
    match<sip:5m> {
        on event { fail | count >= 3; }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (sip = fail.sip)
}

contract brute_test for brute_force {
    given {
        row(fail, action = "failed", sip = "1.2.3.4");
        row(fail, action = "failed", sip = "1.2.3.4");
        row(fail, action = "failed", sip = "1.2.3.4");
        tick(6m);
    }
    expect {
        hits == 1;
        hit[0].score == 70.0;
        hit[0].entity_id == "1.2.3.4";
    }
    options {
        close_trigger = timeout;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.uses.len(), 1);
    assert_eq!(file.rules.len(), 1);
    assert_eq!(file.contracts.len(), 1);
    assert_eq!(file.contracts[0].rule_name, "brute_force");
    assert_eq!(file.contracts[0].given.len(), 4);

    // 3 rows + 1 tick
    let rows: Vec<_> = file.contracts[0]
        .given
        .iter()
        .filter(|s| matches!(s, GivenStmt::Row { .. }))
        .collect();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        file.contracts[0].given[3],
        GivenStmt::Tick(Duration::from_secs(360))
    );
}

#[test]
fn parse_contract_multiple_rows() {
    let input = r#"
contract ct for r {
    given {
        row(req, query_id = "q-1", sip = "10.0.0.1");
        row(resp, query_id = "q-1", sip = "10.0.0.1");
        tick(31s);
    }
    expect {
        hits == 0;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    let c = &file.contracts[0];
    assert_eq!(c.given.len(), 3);
    match &c.given[0] {
        GivenStmt::Row { alias, fields } => {
            assert_eq!(alias, "req");
            assert_eq!(fields.len(), 2);
        }
        other => panic!("expected Row, got {other:?}"),
    }
    match &c.given[1] {
        GivenStmt::Row { alias, fields } => {
            assert_eq!(alias, "resp");
            assert_eq!(fields.len(), 2);
        }
        other => panic!("expected Row, got {other:?}"),
    }
}

#[test]
fn parse_contract_hit_score_cmp() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect {
        hit[0].score >= 50.0;
        hit[0].score <= 100.0;
        hit[1].score != 0;
    }
}
"#;
    let file = parse_wfl(input).unwrap();
    let stmts = &file.contracts[0].expect;
    assert_eq!(stmts.len(), 3);
    match &stmts[0] {
        ExpectStmt::HitAssert {
            index: 0,
            assert: HitAssert::Score { cmp, value },
        } => {
            assert_eq!(*cmp, CmpOp::Ge);
            assert_eq!(*value, 50.0);
        }
        other => panic!("expected score >= 50, got {other:?}"),
    }
    match &stmts[2] {
        ExpectStmt::HitAssert {
            index: 1,
            assert: HitAssert::Score { cmp, value },
        } => {
            assert_eq!(*cmp, CmpOp::Ne);
            assert_eq!(*value, 0.0);
        }
        other => panic!("expected score != 0, got {other:?}"),
    }
}
