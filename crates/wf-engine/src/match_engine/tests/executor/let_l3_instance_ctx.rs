//! 规则级 `let` 中的 L3 集合函数在 instance 上下文真的可用
//! （warp-fusion#101 同类位置修复的对照端）。
//!
//! checker 现在拒绝把 L3 写进逐事件/逐行求值的位置（bind filter / `within` 界 /
//! `emit at` / on-each 的 `let`/`where`），但 match/close 规则的 `let` 在
//! instance 上下文（`build_eval_context` 注入 `_step_*`）求值，必须继续放行且
//! **真的取到值**——否则「放行」本身就是另一种静默失效。本用例锁定该端到端行为：
//! `let first_count = first(e.count)` 在 yield 中应产出首条事件的 `count`。

use super::super::helpers::*;

use crate::match_engine::RuleExecutor;
use crate::match_engine::cep::{CepStateMachine, StepResult};
use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

const NANOS_PER_MS: i64 = 1_000_000;

fn input_window() -> WindowSchema {
    WindowSchema {
        name: "auth_events".into(),
        streams: vec!["auth_stream".into()],
        time_field: Some("event_time".into()),
        over: std::time::Duration::from_secs(300),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "count".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
            FieldDef {
                name: "event_time".into(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    }
}

fn output_window() -> WindowSchema {
    WindowSchema {
        name: "out".into(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(300),
        fields: vec![
            FieldDef {
                name: "sip".into(),
                field_type: FieldType::Base(BaseType::Ip),
            },
            FieldDef {
                name: "n".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    }
}

const SOURCE: &str = r#"
rule let_l3 {
    events { e : auth_events }
    let first_count = first(e.count)
    match<sip:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (sip = e.sip, n = first_count)
}
"#;

#[test]
fn match_rule_let_l3_reads_instance_series() {
    let file = wf_lang::parse_wfl(SOURCE).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[input_window(), output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    assert!(
        plan.match_plan.needs_field_history,
        "let 里的 first(e.count) 必须触发每事件字段历史物化"
    );

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(
        plan.name.clone(),
        plan.match_plan.clone(),
        Some("event_time".to_string()),
    );

    // 首条事件 count=7（first 应取到它），后续事件 count=8/9 不得影响结果。
    let counts = [7.0_f64, 8.0, 9.0];
    let mut matched = None;
    for (seq, count) in counts.iter().enumerate() {
        let ev = event(vec![("sip", str_val("10.0.0.1")), ("count", num(*count))]);
        if let StepResult::Matched(ctx) = sm.advance_at("e", &ev, (seq as i64 + 1) * NANOS_PER_MS) {
            matched = Some(ctx);
            break;
        }
    }

    let matched = matched.expect("count >= 1 应在首条事件命中");
    let alert = exec.execute_match(&matched).expect("match execution");
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };
    assert_eq!(field("sip"), Some(str_val("10.0.0.1")));
    assert_eq!(
        field("n"),
        Some(num(7.0)),
        "first(e.count) 在 let 中应取 instance 收集序列的首个值，而非空"
    );
}
