use super::*;

#[test]
fn compile_two_stage_pipeline_desugars_into_two_rule_plans() {
    let schemas = [fw_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule pipe {
  events { d: fw_events }
  match<sip,dport:5m> {
    on event { ev_count: d | count >= 1; }
    on close { close_count: d | count >= 3; }
  }
  |> match<sip:10m> {
    on event { ev_count: _in | count >= 1; }
    on close { close_count: _in | count >= 10; }
  } -> score(80.0)
  entity(ip, _in.sip)
  yield out (x = _in.sip)
}
"#,
        &schemas,
    );

    assert_eq!(plans.len(), 2);

    let stage1 = &plans[0];
    assert_eq!(stage1.name, "__wf_pipe_pipe_s1");
    assert_eq!(stage1.binds.len(), 1);
    assert_eq!(stage1.binds[0].alias, "d");
    assert_eq!(stage1.yield_plan.target, "__wf_pipe_pipe_w1");
    assert_eq!(stage1.entity_plan.entity_type, "pipeline");
    assert_eq!(stage1.score_plan.expr, Expr::Number(0.0));

    let final_stage = &plans[1];
    assert_eq!(final_stage.name, "pipe");
    assert_eq!(final_stage.binds.len(), 1);
    assert_eq!(final_stage.binds[0].alias, "_in");
    assert_eq!(final_stage.binds[0].window, "__wf_pipe_pipe_w1");
    assert_eq!(final_stage.yield_plan.target, "out");
}

#[test]
fn compile_three_stage_pipeline_uses_chained_internal_windows() {
    let schemas = [fw_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule pipe3 {
  events { e: fw_events }
  match<sip,dport:5m> {
    on event { ev_count: e | count >= 1; }
    on close { close_count: e | count >= 1; }
  }
  |> match<sip:10m> {
    on event { ev_count: _in | count >= 1; }
    on close { close_count: _in | count >= 2; }
  }
  |> match<sip:30m> {
    on event { ev_count: _in | count >= 1; }
    on close { close_count: _in | count >= 3; }
  } -> score(90.0)
  entity(ip, _in.sip)
  yield out (x = _in.sip)
}
"#,
        &schemas,
    );

    assert_eq!(plans.len(), 3);
    assert_eq!(plans[0].name, "__wf_pipe_pipe3_s1");
    assert_eq!(plans[0].yield_plan.target, "__wf_pipe_pipe3_w1");
    assert_eq!(plans[1].name, "__wf_pipe_pipe3_s2");
    assert_eq!(plans[1].binds[0].window, "__wf_pipe_pipe3_w1");
    assert_eq!(plans[1].yield_plan.target, "__wf_pipe_pipe3_w2");
    assert_eq!(plans[2].name, "pipe3");
    assert_eq!(plans[2].binds[0].window, "__wf_pipe_pipe3_w2");
}

#[test]
fn compile_pipeline_stage_yield_dedups_key_mapping_logical_names() {
    let schemas = [fw_events_window(), output_window()];
    let plans = compile_with(
        r#"
rule keymap_pipe {
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
  } -> score(80.0)
  entity(ip, _in.user_id)
  yield out (x = _in.user_id)
}
"#,
        &schemas,
    );

    let stage1 = &plans[0];
    let user_id_fields: Vec<_> = stage1
        .yield_plan
        .fields
        .iter()
        .filter(|f| f.name == "user_id")
        .collect();
    assert_eq!(user_id_fields.len(), 1);
}
