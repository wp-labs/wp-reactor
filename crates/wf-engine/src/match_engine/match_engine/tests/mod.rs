//! Unit tests for the `match_engine::match_engine` module internals.
//!
//! Lives inside the module so tests can reach the private submodules
//! (key/state/step/close/conv/seq/limits) directly.

mod coverage_extra;
mod coverage_more;
mod coverage_r4;

use super::*;

// ---------------------------------------------------------------------------
// conv：Sort 预构建 ctx 优化回归（2026-08 nexmark hotpath 审查）
// ---------------------------------------------------------------------------

fn conv_close(label: &str, measure: f64, scope: Vec<Value>) -> CloseOutput {
    CloseOutput {
        rule_name: "t".into(),
        scope_key: scope,
        close_reason: CloseReason::Timeout,
        event_ok: true,
        close_ok: true,
        close_mode: wf_lang::ast::CloseMode::And,
        event_emitted: false,
        event_step_data: vec![],
        close_step_data: vec![StepData {
            satisfied_branch_index: 0,
            label: Some(label.to_string()),
            measure_value: measure,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        }],
        bind_data: vec![],
        watermark_nanos: 0,
        machine_id: "".into(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        last_event_nanos: 0,
    }
}

fn conv_sort_plan(desc: bool) -> wf_lang::plan::ConvPlan {
    wf_lang::plan::ConvPlan {
        chains: vec![wf_lang::plan::ConvChainPlan {
            ops: vec![wf_lang::plan::ConvOpPlan::Sort(vec![
                wf_lang::plan::SortKeyPlan {
                    expr: wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Simple("m".into())),
                    descending: desc,
                },
            ])],
        }],
    }
}

fn conv_measures(out: &[CloseOutput]) -> Vec<f64> {
    out.iter()
        .map(|o| o.close_step_data[0].measure_value)
        .collect()
}

#[test]
fn conv_sort_preserves_values_and_stable_order() {
    let keys = vec![wf_lang::ast::FieldRef::Simple("auction".into())];
    let outputs = vec![
        conv_close("m", 30.0, vec![Value::Number(3.0)]),
        conv_close("m", 10.0, vec![Value::Number(1.0)]),
        conv_close("m", 20.0, vec![Value::Number(2.0)]),
        conv_close("m", 10.0, vec![Value::Number(9.0)]), // 与第 2 个同键 → 稳定序
    ];
    // 降序：30 > 20 > 10 == 10（稳定序：auction 1 在 auction 9 前）
    let sorted = apply_conv(&conv_sort_plan(true), &keys, outputs.clone());
    assert_eq!(conv_measures(&sorted), vec![30.0, 20.0, 10.0, 10.0]);
    let scope_keys: Vec<f64> = sorted
        .iter()
        .map(|o| match &o.scope_key[0] {
            Value::Number(n) => *n,
            _ => 0.0,
        })
        .collect();
    assert_eq!(scope_keys, vec![3.0, 2.0, 1.0, 9.0], "稳定排序：同键保持输入序");
    // 升序
    let asc = apply_conv(&conv_sort_plan(false), &keys, outputs);
    assert_eq!(conv_measures(&asc), vec![10.0, 10.0, 20.0, 30.0]);
}

#[test]
fn conv_dedup_drops_duplicate_keys() {
    let keys = vec![wf_lang::ast::FieldRef::Simple("auction".into())];
    let plan = wf_lang::plan::ConvPlan {
        chains: vec![wf_lang::plan::ConvChainPlan {
            ops: vec![wf_lang::plan::ConvOpPlan::Dedup(wf_lang::ast::Expr::Field(
                wf_lang::ast::FieldRef::Simple("m".into()),
            ))],
        }],
    };
    let outputs = vec![
        conv_close("m", 10.0, vec![]),
        conv_close("m", 20.0, vec![]),
        conv_close("m", 10.0, vec![]),
        conv_close("m", 30.0, vec![]),
    ];
    let deduped = apply_conv(&plan, &keys, outputs);
    assert_eq!(deduped.len(), 3, "去重后剩 3 条");
    assert_eq!(conv_measures(&deduped), vec![10.0, 20.0, 30.0]);
}

#[test]
fn conv_top_after_sort_keeps_first_n() {
    let keys = vec![wf_lang::ast::FieldRef::Simple("auction".into())];
    let plan = wf_lang::plan::ConvPlan {
        chains: vec![wf_lang::plan::ConvChainPlan {
            ops: vec![
                wf_lang::plan::ConvOpPlan::Sort(vec![wf_lang::plan::SortKeyPlan {
                    expr: wf_lang::ast::Expr::Field(wf_lang::ast::FieldRef::Simple("m".into())),
                    descending: true,
                }]),
                wf_lang::plan::ConvOpPlan::Top(2),
            ],
        }],
    };
    let outputs = vec![
        conv_close("m", 10.0, vec![]),
        conv_close("m", 50.0, vec![]),
        conv_close("m", 30.0, vec![]),
        conv_close("m", 20.0, vec![]),
    ];
    let top = apply_conv(&plan, &keys, outputs);
    assert_eq!(top.len(), 2);
    assert_eq!(conv_measures(&top), vec![50.0, 30.0]);
}

fn make_event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}

#[test]
fn extract_event_str() {
    let e = make_event(vec![
        ("sip", Value::Str("10.0.0.1".into())),
        ("n", Value::Number(5.0)),
        ("flag", Value::Bool(true)),
    ]);
    assert_eq!(CepStateMachine::extract_event_str(&e, "sip"), "10.0.0.1");
    let empty = make_event(vec![]);
    assert_eq!(CepStateMachine::extract_event_str(&empty, "any"), "");
}
