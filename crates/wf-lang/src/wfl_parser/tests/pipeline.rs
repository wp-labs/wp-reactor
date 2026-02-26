use crate::parse_wfl;

#[test]
fn parse_two_stage_pipeline() {
    let input = r#"
rule r_pipe {
  events { d: fw_events }
  match<sip,dport:5m> {
    on event { d | count >= 1; }
    on close { d | count >= 3; }
  }
  |> match<sip:10m> {
    on event { _in | count >= 1; }
    on close { _in | count >= 10; }
  } -> score(80.0)
  entity(ip, _in.sip)
  yield out (x = _in.sip)
}
"#;

    let file = parse_wfl(input).expect("parse should succeed");
    assert_eq!(file.rules.len(), 1);
    let rule = &file.rules[0];
    assert_eq!(rule.pipeline_stages.len(), 1);
    assert_eq!(rule.pipeline_stages[0].match_clause.keys.len(), 2);
    assert_eq!(rule.match_clause.keys.len(), 1);
    assert_eq!(rule.match_clause.on_close.as_ref().unwrap().steps.len(), 1);
}

#[test]
fn parse_three_stage_pipeline() {
    let input = r#"
rule r_pipe3 {
  events { e: fw_events }
  match<sip,dport:5m> {
    on event { e | count >= 1; }
    on close { e | count >= 1; }
  }
  |> match<sip:10m> {
    on event { _in | count >= 1; }
    on close { _in | count >= 2; }
  }
  |> match<sip:30m> {
    on event { _in | count >= 1; }
    on close { _in | count >= 3; }
  } -> score(90.0)
  entity(ip, _in.sip)
  yield out (x = _in.sip)
}
"#;

    let file = parse_wfl(input).expect("parse should succeed");
    let rule = &file.rules[0];
    assert_eq!(rule.pipeline_stages.len(), 2);
}

#[test]
fn reject_pipeline_non_final_stage_with_score() {
    let input = r#"
rule bad_pipe {
  events { e: fw_events }
  match<sip:5m> {
    on event { e | count >= 1; }
  } -> score(10.0)
  |> match<sip:10m> {
    on event { _in | count >= 1; }
  } -> score(20.0)
  entity(ip, _in.sip)
  yield out (x = _in.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}
