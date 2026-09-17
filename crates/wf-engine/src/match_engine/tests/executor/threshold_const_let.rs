//! 规则级常量 `let` 作为阈值（warp-fusion#101 后续）。
//!
//! 触发判定只做常量折叠，因此阈值必须是编译期常量。`let THRESHOLD = 3` 这类命名
//! 常量在编译期被内联为字面量，与手写 `count >= 3` 完全一致——本用例锁定「真的会
//! 触发」这一端到端行为（未内联时该分支永不满足，规则静默不触发）。

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
                name: "action".into(),
                field_type: FieldType::Base(BaseType::Chars),
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
                name: "hits".into(),
                field_type: FieldType::Base(BaseType::Digit),
            },
        ],
    }
}

/// 阈值来自规则级常量 `let`（而非内联字面量）。
const SOURCE: &str = r#"
rule let_threshold {
    events { e : auth_events }

    let FAIL_THRESHOLD = 3

    match<sip:5m> {
        on event { fail: e | count >= FAIL_THRESHOLD; }
    } -> score(70.0)

    entity(ip, e.sip)

    yield out (sip = e.sip, hits = count(e))
}
"#;

#[test]
fn constant_let_threshold_fires() {
    let file = wf_lang::parse_wfl(SOURCE).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[input_window(), output_window()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");

    // 编译期已把常量 let 内联为字面量（与手写 `count >= 3` 等价）。
    assert_eq!(
        plan.match_plan.event_steps[0].branches[0].agg.threshold,
        wf_lang::ast::Expr::Number(3.0),
        "常量 let 必须内联为字面量"
    );

    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(
        plan.name.clone(),
        plan.match_plan.clone(),
        Some("event_time".to_string()),
    );

    let ev = event(vec![("sip", str_val("10.0.0.1"))]);
    let mut matched = None;
    for seq in 0..3 {
        let result = sm.advance_at("e", &ev, (seq as i64 + 1) * NANOS_PER_MS);
        match result {
            StepResult::Matched(ctx) => matched = Some(ctx),
            StepResult::Accumulate | StepResult::Advance => {}
        }
    }

    let matched = matched.expect("第 3 条事件应满足常量 let 阈值并命中");
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
        field("hits"),
        Some(num(3.0)),
        "命中时的计数应为 3（阈值来自 let 常量）"
    );
}
