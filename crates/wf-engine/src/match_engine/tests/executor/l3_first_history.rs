//! issue #100：`first()` 在字段历史被环形裁剪后漂移。
//!
//! 字段历史只保留最近 `MAX_TRACKED_FIELD_VALUES` 个样本（`wf-cep/cep/step.rs`），
//! 而 `count` 用独立累加器。若规则用 `first(field)` 组成聚合唯一键 / `alert_id`，
//! 窗口内事件数超过上限后同一实例会输出多个唯一键。
//!
//! 验收（issue #100「验收标准」）：2000 条同一分组事件只产生一个稳定 `alert_id`；
//! `first_seen` 保持首条事件时间；`event_count` 累计到 2000。

use std::collections::HashSet;

use super::super::helpers::*;

use crate::alert::OutputRecord;
use crate::match_engine::RuleExecutor;
use crate::match_engine::cep::{
    CepStateMachine, CloseOutput, CloseReason, MatchedContext, StepResult, Value,
};
use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

const BASE_MS: i64 = 1_700_000_000_000;

/// 事件时间毫秒 → 传入 `advance_at` 的纳秒。
fn nanos_at(seq: usize) -> i64 {
    (BASE_MS + seq as i64 * 1_000) * 1_000_000
}

fn input_window() -> WindowSchema {
    WindowSchema {
        name: "input_events".into(),
        streams: vec!["input_events".into()],
        time_field: Some("event_time".into()),
        over: std::time::Duration::from_secs(86_400),
        fields: vec![
            FieldDef {
                name: "event_id".into(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "event_time".into(),
                field_type: FieldType::Base(BaseType::Time),
            },
            FieldDef {
                name: "group_key".into(),
                field_type: FieldType::Base(BaseType::Chars),
            },
        ],
    }
}

fn output_window() -> WindowSchema {
    WindowSchema {
        name: "alert_output".into(),
        streams: vec![],
        time_field: None,
        over: std::time::Duration::from_secs(86_400),
        fields: vec![
            FieldDef {
                name: "alert_id".into(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "first_seen".into(),
                field_type: FieldType::Base(BaseType::Float),
            },
            FieldDef {
                name: "event_count".into(),
                field_type: FieldType::Base(BaseType::Float),
            },
        ],
    }
}

/// issue #100 的最小复现规则（原文照搬，补上解析器要求的 `entity` 子句）。
const SOURCE: &str = r#"
rule aggregate_with_first_key {
    events {
        e : input_events
    }

    let aggregation_key = join_by(
        "|",
        e.group_key,
        first(e.event_time)
    )

    match<e.group_key:1d:fixed> {
        on event<accu> { e | count >= 1; }
    } -> score(1.0)

    entity(group, e.group_key)

    yield alert_output (
        alert_id = aggregation_key,
        first_seen = time_to_ms(first(e.event_time)),
        event_count = count(e)
    )
}
"#;

/// 一次输出快照：`(alert_id, first_seen, event_count)`。
type Alert = (String, f64, f64);

fn alert_of(alert: &OutputRecord) -> Alert {
    let field = |name: &str| {
        alert
            .yield_fields
            .iter()
            .find(|(field_name, _)| &**field_name == name)
            .map(|(_, value)| value.clone())
    };
    let as_str = |v: Option<Value>| match v {
        Some(Value::Str(s)) => s.to_string(),
        other => panic!("alert_id 应为字符串，实际 {other:?}"),
    };
    let as_num = |name: &str| match field(name) {
        Some(Value::Float(n)) => n,
        other => panic!("{name} 应为数值，实际 {other:?}"),
    };
    (
        as_str(field("alert_id")),
        as_num("first_seen"),
        as_num("event_count"),
    )
}

/// close 步骤变体（同时覆盖 `close.rs` 侧的序列暴露路径）。
const CLOSE_SOURCE: &str = r#"
rule aggregate_with_first_key_close {
    events {
        e : input_events
    }

    match<e.group_key:1d:fixed> {
        on event { e.event_time | distinct | count >= 1; }
        and close { e.event_time | distinct | count >= 1; }
    } -> score(1.0)

    entity(group, e.group_key)

    yield alert_output (
        alert_id = join_by("|", e.group_key, first(e.event_time)),
        first_seen = time_to_ms(first(e.event_time)),
        event_count = count(e)
    )
}
"#;

/// 事件步骤采集变体（步骤带字段 → 覆盖 `window.rs` 的 `collected_series` 暴露）。
const STEP_VALUES_SOURCE: &str = r#"
rule aggregate_with_first_key_accu {
    events {
        e : input_events
    }

    match<e.group_key:1d:fixed> {
        on event<accu> { e.event_time | distinct | count >= 1; }
    } -> score(1.0)

    entity(group, e.group_key)

    yield alert_output (
        alert_id = join_by("|", e.group_key, first(e.event_time)),
        first_seen = time_to_ms(first(e.event_time)),
        event_count = count(e)
    )
}
"#;

/// 单次运行的产物：输出告警、收口输出（携带 close/event 步骤的序列快照）、
/// 最后一次 match 上下文（accu 规则用于直接校验 `_step_*` 序列）。
struct Run {
    alerts: Vec<Alert>,
    closes: Vec<CloseOutput>,
    last_matched: Option<MatchedContext>,
    /// 最后一条产出（match 或 close）；用于断言三元组以外的字段。
    last_alert: Option<OutputRecord>,
}

/// 取 yield 字段值。
fn yield_field(alert: &OutputRecord, name: &str) -> Option<Value> {
    alert
        .yield_fields
        .iter()
        .find(|(field_name, _)| &**field_name == name)
        .map(|(_, value)| value.clone())
}

/// 编译 `source`、灌入 `count` 条事件，收集实例存活期间产生的全部输出。
fn run_events(source: &str, count: usize) -> Run {
    run_events_with_output(source, count, &output_window())
}

fn run_events_with_output(source: &str, count: usize, output: &WindowSchema) -> Run {
    let file = wf_lang::parse_wfl(source).expect("parse should succeed");
    let plan = wf_lang::compile_wfl(&file, &[input_window(), output.clone()])
        .expect("compile should succeed")
        .into_iter()
        .next()
        .expect("rule plan should exist");
    let exec = RuleExecutor::new(plan.clone());
    let mut sm = CepStateMachine::new(
        plan.name.clone(),
        plan.match_plan.clone(),
        Some("event_time".to_string()),
    );

    let mut alerts = Vec::new();
    let mut last_matched = None;
    let mut last_alert = None;
    for seq in 0..count {
        let ev = event(vec![
            ("event_id", str_val(&format!("evt-{seq:04}"))),
            ("event_time", num((BASE_MS + seq as i64 * 1_000) as f64)),
            ("group_key", str_val("group-1")),
        ]);
        if let StepResult::Matched(matched) = sm.advance_at("e", &ev, nanos_at(seq)) {
            let alert = exec.execute_match(&matched).expect("match execution");
            alerts.push(alert_of(&alert));
            last_alert = Some(alert);
            last_matched = Some(matched);
        }
    }
    let closes = sm.close_all(CloseReason::Timeout);
    for output in &closes {
        if let Some(alert) = exec.execute_close(output).expect("close execution") {
            alerts.push(alert_of(&alert));
            last_alert = Some(alert);
        }
    }
    Run {
        alerts,
        closes,
        last_matched,
        last_alert,
    }
}

fn run(count: usize) -> Vec<Alert> {
    run_events(SOURCE, count).alerts
}

#[test]
fn first_stays_at_window_head_past_field_history_cap() {
    // 1024（恰好在上限内）与 2000（超出上限）必须产生同一 first_seen / alert_id。
    let within_cap = run(1024);
    let past_cap = run(2000);
    assert!(!within_cap.is_empty(), "1024 条事件应至少产生一条输出");
    assert!(!past_cap.is_empty(), "2000 条事件应至少产生一条输出");

    let expected_id = format!("group-1|{BASE_MS}");
    let expected_seen = BASE_MS as f64;
    // 上限内（1024）与超出上限（2000）必须给出同一个 alert_id / first_seen。
    for (id, first_seen, _) in within_cap.iter().chain(past_cap.iter()) {
        assert_eq!(id, &expected_id, "alert_id 不得随窗口增长漂移");
        assert_eq!(
            *first_seen, expected_seen,
            "first_seen 必须保持首条事件时间"
        );
    }

    // 不变量：窗口规模变化不得改变输出键（不依赖数值 → 字符串格式化的细节）。
    let ids = |alerts: &[Alert]| -> HashSet<String> {
        alerts.iter().map(|(id, _, _)| id.clone()).collect()
    };
    assert_eq!(
        ids(&within_cap),
        ids(&past_cap),
        "1024 与 2000 条事件的 alert_id 集合必须一致"
    );

    // count 走独立累加器：最后一条输出必须累计全部事件。
    let last_count = past_cap.last().expect("非空").2;
    assert_eq!(last_count, 2000.0, "event_count 应累计到 2000");

    // `on event<accu>` 每事件触发一次，但唯一键只有一个——下游按唯一键 upsert
    // 后仍是一条逻辑记录（issue #100 验收标准）。
    let unique: HashSet<&str> = past_cap.iter().map(|(id, _, _)| id.as_str()).collect();
    assert_eq!(
        unique.len(),
        1,
        "同一实例应只产生一个唯一键，实际 {unique:?}"
    );
}

#[test]
fn first_is_stable_for_every_event_past_the_cap() {
    // 上限之后的每一次输出都必须仍然读到最早事件的值（逐条核对漂移点）。
    let alerts = run(1025);
    let expected_id = format!("group-1|{BASE_MS}");
    for (idx, (id, first_seen, _)) in alerts.iter().enumerate() {
        assert_eq!(id, &expected_id, "第 {idx} 条输出 alert_id 漂移");
        assert_eq!(
            *first_seen, BASE_MS as f64,
            "第 {idx} 条输出 first_seen 漂移"
        );
    }
}

#[test]
fn first_is_stable_for_close_step_output_past_the_cap() {
    // close 步骤路径：收口时由 `close.rs` 侧的序列（`collected_series` /
    // `field_series`）组装，同样必须钉扎首值，且 `count` 累计全部事件。
    let run = run_events(CLOSE_SOURCE, 2000);
    assert!(!run.alerts.is_empty(), "close 应至少产生一条输出");

    let expected_id = format!("group-1|{BASE_MS}");
    for (id, first_seen, _) in &run.alerts {
        assert_eq!(id, &expected_id, "close 输出的 alert_id 不得漂移");
        assert_eq!(
            *first_seen, BASE_MS as f64,
            "close 输出的 first_seen 不得漂移"
        );
    }
    assert_eq!(
        run.alerts.last().expect("非空").2,
        2000.0,
        "close 输出的 event_count 应累计到 2000"
    );

    // 直接校验 close 步骤携带的序列：`first` 走 bind 序列（优先级更高），
    // 上面的 alert 断言无法覆盖 `close.rs` 的字段序列暴露——这里单独断言。
    let close = run.closes.first().expect("收口输出");
    let step = close.close_step_data.first().expect("close 步骤快照");
    assert_eq!(
        step.collected_values.len(),
        1025,
        "close 步骤 collected_values = 首值 + 最近 1024 个"
    );
    assert_eq!(
        step.collected_values.first(),
        Some(&num(BASE_MS as f64)),
        "close 步骤 collected_values 必须钉扎首值"
    );
    assert_eq!(
        step.collected_values.last(),
        Some(&num((BASE_MS + 1999 * 1_000) as f64)),
        "close 步骤 collected_values 末项仍是最新样本"
    );
    assert_eq!(
        step.field_values.get("event_time").map(|v| v.len()),
        Some(1025),
        "close 步骤字段序列 = 首值 + 最近 1024 个"
    );
    assert_eq!(
        step.field_values.get("event_time").and_then(|v| v.first()),
        Some(&num(BASE_MS as f64)),
        "close 步骤字段序列必须钉扎首值"
    );
}

#[test]
fn event_step_series_exposes_pinned_first_value() {
    // 事件步骤路径：`on event<accu>` 的 `_step_i_values` 序列同样钉扎首值。
    let run = run_events(STEP_VALUES_SOURCE, 2000);
    let matched = run.last_matched.expect("accu 规则应每事件命中");
    let step = matched.step_data.first().expect("事件步骤快照");
    assert_eq!(step.collected_values.len(), 1025);
    assert_eq!(step.collected_values.first(), Some(&num(BASE_MS as f64)));
    assert_eq!(
        step.collected_values.last(),
        Some(&num((BASE_MS + 1999 * 1_000) as f64))
    );
    // 同一规则下 alert_id 依旧稳定（issue #100 验收标准）。
    for (id, _, _) in &run.alerts {
        assert_eq!(id, &format!("group-1|{BASE_MS}"));
    }
}

/// 非 accu 规则：命中后实例 reset 并重新累积，因此同一桶内可再次命中。
const RESET_SOURCE: &str = r#"
rule aggregate_with_first_key_non_accu {
    events {
        e : input_events
    }

    match<e.group_key:1d:fixed> {
        on event { e.event_time | distinct | count >= 1030; }
    } -> score(1.0)

    entity(group, e.group_key)

    yield alert_output (
        alert_id = join_by("|", e.group_key, first(e.event_time)),
        first_seen = time_to_ms(first(e.event_time)),
        event_count = count(e)
    )
}
"#;

#[test]
fn pinned_first_does_not_survive_instance_reset() {
    // 每个实例周期的 `first()` 必须是该周期首条事件的值：钉扎槽随
    // `Instance::reset` 清空，不得从上一个周期泄到下一个周期。
    let run = run_events(RESET_SOURCE, 2100);
    assert!(
        run.alerts.len() >= 2,
        "阈值 1030 + 2100 条事件应至少命中两次，实际 {}",
        run.alerts.len()
    );

    let second_period_first = BASE_MS + 1030 * 1_000;
    assert_eq!(
        run.alerts[0],
        (format!("group-1|{BASE_MS}"), BASE_MS as f64, 1030.0),
        "第一周期：首值 = 首条事件，行数 1030"
    );
    assert_eq!(
        run.alerts[1],
        (
            format!("group-1|{second_period_first}"),
            second_period_first as f64,
            1030.0
        ),
        "第二周期：首值 = 该周期首条事件（reset 已清空钉扎），行数重新累计"
    );
}

/// 限定字段聚合（`min/max(alias.field)`）走同一组样本 → 能看到钉扎的最早样本。
const AGG_OVER_FIELD_SOURCE: &str = r#"
rule aggregate_over_sample_series {
    events {
        e : input_events
    }

    match<e.group_key:1d:fixed> {
        on event<accu> { e | count >= 1; }
    } -> score(1.0)

    entity(group, e.group_key)

    yield alert_output (
        alert_id = join_by("|", e.group_key, first(e.event_time)),
        first_seen = time_to_ms(first(e.event_time)),
        event_count = count(e),
        min_seen = time_to_ms(min(e.event_time))
    )
}
"#;

fn agg_output_window() -> WindowSchema {
    let mut window = output_window();
    window.fields.push(FieldDef {
        name: "min_seen".into(),
        field_type: FieldType::Base(BaseType::Float),
    });
    window
}

#[test]
fn qualified_field_aggregate_sees_pinned_head_sample() {
    // 行为变更锁定（已文档化）：`count(alias)` 是独立累加器（事件总数，不受采样
    // 上限影响），而 `min(alias.field)` 等**限定字段聚合**走同一序列，裁剪发生后
    // 仍能看到钉扎的最早样本（旧行为下 min 会变成第 7 条事件的 1700000006000）。
    let run = run_events_with_output(AGG_OVER_FIELD_SOURCE, 1030, &agg_output_window());
    let alert = run.last_alert.as_ref().expect("accu 规则应命中");

    assert_eq!(
        yield_field(alert, "event_count"),
        Some(num(1030.0)),
        "count(alias) 是独立累加器：事件总数"
    );
    assert_eq!(
        yield_field(alert, "min_seen"),
        Some(num(BASE_MS as f64)),
        "限定字段聚合必须看到钉扎的最早样本"
    );
    assert_eq!(
        yield_field(alert, "first_seen"),
        Some(num(BASE_MS as f64)),
        "first 与限定字段聚合取自同一序列的最早样本"
    );
}
