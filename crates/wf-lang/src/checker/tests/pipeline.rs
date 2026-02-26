use super::{assert_has_error, assert_no_errors, fw_events_window, output_window};

#[test]
fn pipeline_two_stage_accepts_injected_in_alias() {
    let input = r#"
rule pipe_ok {
  events { d: fw_events }
  match<sip,dport:5m> {
    on event { ev: d | count >= 1; }
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
    let schemas = vec![fw_events_window(), output_window()];
    assert_no_errors(input, &schemas);
}

#[test]
fn pipeline_rejects_user_defined_in_alias() {
    let input = r#"
rule bad_alias {
  events { _in: fw_events }
  match<sip:5m> { on event { _in | count >= 1; } } -> score(10.0)
  entity(ip, _in.sip)
  yield out (x = _in.sip)
}
"#;
    let schemas = vec![fw_events_window(), output_window()];
    assert_has_error(input, &schemas, "reserved for pipeline stage inputs");
}

#[test]
fn pipeline_rejects_duplicate_implicit_output_field_names() {
    let input = r#"
rule bad_implicit_output {
  events { d: fw_events }
  match<sip:5m> {
    on event { d | count >= 1; }
    on close {
      d | count >= 1;
      d.dport | count >= 1;
    }
  }
  |> match<sip:10m> {
    on event { _in | count >= 1; }
    on close { _in | count >= 1; }
  } -> score(10.0)
  entity(ip, _in.sip)
  yield out (x = _in.sip)
}
"#;
    let schemas = vec![fw_events_window(), output_window()];
    assert_has_error(input, &schemas, "duplicate implicit output field");
}

#[test]
fn pipeline_final_stage_cannot_reference_original_event_aliases() {
    let input = r#"
rule bad_scope {
  events { d: fw_events }
  match<sip,dport:5m> {
    on event { d | count >= 1; }
    on close { d | count >= 1; }
  }
  |> match<sip:10m> {
    on event { d | count >= 1; }
    on close { d | count >= 1; }
  } -> score(10.0)
  entity(ip, d.sip)
  yield out (x = d.sip)
}
"#;
    let schemas = vec![fw_events_window(), output_window()];
    assert_has_error(input, &schemas, "is not a declared event alias");
}

#[test]
fn pipeline_key_mapping_duplicate_logical_name_is_allowed() {
    let input = r#"
rule keymap_pipe_ok {
  events {
    a: fw_events
    b: fw_events
  }
  match<:5m> {
    key {
      user_id = a.sip;
      user_id = b.sip;
    }
    on event { a | count >= 1; }
  }
  |> match<user_id:5m> {
    on event { _in | count >= 1; }
  } -> score(10.0)
  entity(ip, _in.user_id)
  yield out (x = _in.user_id)
}
"#;
    let schemas = vec![fw_events_window(), output_window()];
    assert_no_errors(input, &schemas);
}
