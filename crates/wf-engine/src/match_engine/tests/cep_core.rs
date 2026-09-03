//! M14 core CEP state machine tests (1–11).

use wf_lang::ast::{CmpOp, Expr, FieldRef, FieldSelector, Measure, PathSegment, Transform};
use wf_lang::plan::{AggPlan, BranchPlan, WindowSpec};

use crate::match_engine::cep::{CepStateMachine, EngineHashMap, StepResult, Value};

use super::helpers::*;

#[test]
fn single_step_threshold() {
    // 3 events → Accumulate, Accumulate, Matched
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let mut sm = CepStateMachine::new("rule1".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);

    if let StepResult::Matched(ctx) = sm.advance("fail", &e) {
        assert_eq!(ctx.rule_name, "rule1");
        assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1")]);
        assert_eq!(ctx.step_data.len(), 1);
        assert_eq!(ctx.step_data[0].measure_value, 3.0);
    } else {
        panic!("expected Matched");
    }
}

#[test]
fn advance_with_progress_reports_unsatisfied_measure() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch_with_label(
            "fail",
            "failures",
            count_ge(3.0),
        )])],
    );
    let mut sm = CepStateMachine::new("rule_progress".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    let outcome = sm.advance_at_with_progress("fail", &e, 1_000_000_000, None);

    assert_eq!(outcome.result, StepResult::Accumulate);
    let progress = outcome.progress.expect("progress should be captured");
    assert_eq!(progress.rule_name, "rule_progress");
    assert_eq!(progress.scope_key, vec![str_val("10.0.0.1")]);
    assert_eq!(progress.step_index, 0);
    assert_eq!(progress.step_label.as_deref(), Some("failures"));
    assert_eq!(progress.branch_index, 0);
    assert_eq!(progress.branch_source, "fail");
    assert_eq!(progress.threshold_checked_branches, 1);
    assert_eq!(progress.measure_value, 1.0);
    assert_eq!(progress.cmp, ">=");
    assert_eq!(progress.threshold, "3");
    assert!(!progress.satisfied);
    assert_eq!(progress.instances, 1);
}

#[test]
fn advance_with_progress_reports_last_checked_unsatisfied_branch() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![
            branch_with_label("fail", "first", count_ge(3.0)),
            branch_with_label("fail", "second", count_ge(4.0)),
        ])],
    );
    let mut sm = CepStateMachine::new("rule_progress_multi".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    let outcome = sm.advance_at_with_progress("fail", &e, 1_000_000_000, None);

    assert_eq!(outcome.result, StepResult::Accumulate);
    let progress = outcome.progress.expect("progress should be captured");
    assert_eq!(progress.step_label.as_deref(), Some("second"));
    assert_eq!(progress.branch_index, 1);
    assert_eq!(progress.threshold_checked_branches, 2);
    assert_eq!(progress.measure_value, 1.0);
    assert_eq!(progress.threshold, "4");
    assert!(!progress.satisfied);
}

#[test]
fn sliding_match_context_tracks_event_and_window_times() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let mut sm = CepStateMachine::new("rule_time".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(
        sm.advance_at("fail", &e, 1_000_000_000),
        StepResult::Accumulate
    );
    assert_eq!(
        sm.advance_at("fail", &e, 2_000_000_000),
        StepResult::Accumulate
    );

    let StepResult::Matched(ctx) = sm.advance_at("fail", &e, 3_000_000_000) else {
        panic!("expected sliding match");
    };

    assert_eq!(ctx.event_time_nanos, 3_000_000_000);
    assert_eq!(ctx.event_first_time_nanos, 1_000_000_000);
    assert_eq!(ctx.event_last_time_nanos, 3_000_000_000);
    assert_eq!(ctx.window_start_time_nanos, 1_000_000_000);
    assert_eq!(ctx.window_end_time_nanos, 301_000_000_000);
}

#[test]
fn evidence_time_ignores_events_not_consumed_by_current_step() {
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("fail", count_ge(1.0))]),
            step(vec![branch("scan", count_ge(1.0))]),
        ],
    );
    let mut sm = CepStateMachine::new("rule_time".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    assert_eq!(
        sm.advance_at("scan", &e, 1_000_000_000),
        StepResult::Accumulate
    );
    assert_eq!(
        sm.advance_at("fail", &e, 10_000_000_000),
        StepResult::Advance
    );
    let StepResult::Matched(ctx) = sm.advance_at("scan", &e, 20_000_000_000) else {
        panic!("expected multi-step match");
    };

    // 证据跨度（issue #82 方案 A）：只含被 step 消费的事件——step1(fail)
    // 之前的 scan@1s 未消费，不计入证据。
    assert_eq!(ctx.evidence_first_time_nanos, 10_000_000_000);
    assert_eq!(ctx.evidence_last_time_nanos, 20_000_000_000);
    // 候选事件跨度：scan@1s 已进入实例（作为 step2 的早期事件）→ 计入。
    assert_eq!(ctx.event_first_time_nanos, 1_000_000_000);
    assert_eq!(ctx.event_last_time_nanos, 20_000_000_000);
}

/// `roles_obj` 为含 `id` 叶的结构化对象值（嵌套 key 测试用）。
fn obj_roles(id: &str) -> Value {
    Value::Object(EngineHashMap::<smol_str::SmolStr, Value>::from_iter([(
        "id".into(),
        Value::Str(id.into()),
    )]))
}

#[test]
fn event_span_tracks_arrival_order_first_and_time_max_last() {
    // issue #82 方案 A：候选跨度 = 到达序首条事件时间 + 时间序最大。乱序到达
    // （先 9s 后 3s）时 event_first=9s（首条到达）、event_last=9s（时间 max）；
    // 证据跨度按 branch 记录的时间 min/max = [3s, 9s]——两组独立可辨。
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let mut sm = CepStateMachine::new("rule_out_of_order".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    assert_eq!(
        sm.advance_at("fail", &e, 9_000_000_000),
        StepResult::Accumulate
    );
    let StepResult::Matched(ctx) = sm.advance_at("fail", &e, 3_000_000_000) else {
        panic!("expected match on second (out-of-order) event");
    };
    assert_eq!(ctx.event_first_time_nanos, 9_000_000_000);
    assert_eq!(ctx.event_last_time_nanos, 9_000_000_000);
    assert_eq!(ctx.evidence_first_time_nanos, 3_000_000_000);
    assert_eq!(ctx.evidence_last_time_nanos, 9_000_000_000);
}

#[test]
fn nested_path_match_key_groups_by_leaf_value() {
    // issue #83：嵌套路径作为 match key —— 按叶值分组，同叶值事件进入同一
    // 实例累积；缺 root/叶的事件按现有缺失 key 语义跳过。
    let plan = simple_plan(
        vec![FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("id".into()),
            ],
        }],
        vec![step(vec![branch("e", count_ge(2.0))])],
    );
    let mut sm = CepStateMachine::new("rule_path_key".to_string(), plan, None);
    let ev_a = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("roles_obj", obj_roles("k1")),
    ]);

    // 同叶 `k1` 两个事件 → 同一实例计数到 2 → 命中。
    assert_eq!(
        sm.advance_at("e", &ev_a, 1_000_000_000),
        StepResult::Accumulate
    );
    let StepResult::Matched(ctx) = sm.advance_at("e", &ev_a, 2_000_000_000) else {
        panic!("同叶第二事件应命中同一实例");
    };
    assert_eq!(ctx.scope_key, vec![str_val("k1")], "分组键 = 路径叶值");

    // 不同叶 `k2` → 独立实例（第一条仅累积）。
    let ev_b = event(vec![
        ("sip", str_val("10.0.0.2")),
        ("roles_obj", obj_roles("k2")),
    ]);
    assert_eq!(
        sm.advance_at("e", &ev_b, 3_000_000_000),
        StepResult::Accumulate
    );
    let StepResult::Matched(ctx) = sm.advance_at("e", &ev_b, 4_000_000_000) else {
        panic!("不同叶 k2 的第二事件应命中 k2 实例");
    };
    assert_eq!(ctx.scope_key, vec![str_val("k2")]);
}

#[test]
fn nested_path_match_key_missing_root_skips_event() {
    // 嵌套 key 的 root 字段缺失 → 与普通 key 缺失一致：事件跳过、不建实例。
    let plan = simple_plan(
        vec![FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("id".into()),
            ],
        }],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("rule_path_key_missing".to_string(), plan, None);
    let ev_no_root = event(vec![("sip", str_val("10.0.0.1"))]);
    assert_eq!(
        sm.advance_at("e", &ev_no_root, 1_000_000_000),
        StepResult::Accumulate,
        "root 缺失 → 跳过（不 fire、不建实例）"
    );
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn nested_path_key_object_leaf_skips_event() {
    // 路径少写一段（叶是 object）→ 事件跳过、不建实例（不聚到 [object]）。
    let plan = simple_plan(
        vec![FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("attacker".into()),
            ],
        }],
        vec![step(vec![branch("e", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("rule_path_key_object_leaf".to_string(), plan, None);
    let ev = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("roles_obj", obj_roles("k1")),
    ]);
    assert_eq!(
        sm.advance_at("e", &ev, 1_000_000_000),
        StepResult::Accumulate,
        "object 叶 → key 缺失跳过"
    );
    assert_eq!(sm.instance_count(), 0);
}

#[test]
fn nested_path_key_groups_within_fixed_window_bucket() {
    // fixed 窗口 + 嵌套 key：同桶（桶起点对齐）内同叶事件进入同一实例计数。
    let mut plan = simple_plan(
        vec![FieldRef::Path {
            alias: "e".into(),
            segments: vec![
                PathSegment::Field("roles_obj".into()),
                PathSegment::Field("id".into()),
            ],
        }],
        vec![step(vec![branch("e", count_ge(2.0))])],
    );
    plan.window_spec = WindowSpec::Fixed(std::time::Duration::from_secs(10));
    let mut sm = CepStateMachine::new("rule_path_key_fixed".to_string(), plan, None);
    let ev = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("roles_obj", obj_roles("k1")),
    ]);
    // 桶 [0s,10s)：t=1s 与 t=2s 同桶 → 同实例累积到 2。
    assert_eq!(
        sm.advance_at("e", &ev, 1_000_000_000),
        StepResult::Accumulate
    );
    let StepResult::Matched(ctx) = sm.advance_at("e", &ev, 2_000_000_000) else {
        panic!("同桶第二事件应命中同一实例");
    };
    assert_eq!(ctx.scope_key, vec![str_val("k1")]);
}

#[test]
fn evidence_time_ignores_guard_rejected_events() {
    let guard = Expr::BinOp {
        op: wf_lang::ast::BinOp::Eq,
        left: Box::new(Expr::Field(wf_lang::ast::FieldRef::Simple(
            "action".to_string(),
        ))),
        right: Box::new(Expr::StringLit("failed".to_string())),
    };
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "auth".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(2.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule_time_guard".to_string(), plan, None);

    let success = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("success")),
    ]);
    let failed = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("failed")),
    ]);

    assert_eq!(
        sm.advance_at("auth", &success, 1_000_000_000),
        StepResult::Accumulate
    );
    assert_eq!(
        sm.advance_at("auth", &failed, 10_000_000_000),
        StepResult::Accumulate
    );
    let StepResult::Matched(ctx) = sm.advance_at("auth", &failed, 20_000_000_000) else {
        panic!("expected guarded match");
    };

    // 证据跨度（issue #82 方案 A）：branch guard 拒绝的事件（success@1s）
    // 不计入证据——证据只含 guard 通过并被接受的事件。
    assert_eq!(ctx.evidence_first_time_nanos, 10_000_000_000);
    assert_eq!(ctx.evidence_last_time_nanos, 20_000_000_000);
    // 候选事件跨度：success@1s 已推进到状态机（生产管道中 bind/guard 过滤在
    // alias_accepts 层提前进行，不会到状态机；此处直接 advance 驱动反映的是
    // 状态机收到即候选的口径）。
    assert_eq!(ctx.event_first_time_nanos, 1_000_000_000);
    assert_eq!(ctx.event_last_time_nanos, 20_000_000_000);
}

#[test]
fn multi_step_sequential() {
    // step2 events before step1 don't match; step1 done → Advance; step2 done → Matched
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![
            step(vec![branch("fail", count_ge(2.0))]),
            step(vec![branch("scan", count_ge(1.0))]),
        ],
    );
    let mut sm = CepStateMachine::new("rule2".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // scan event before step1 is done — should accumulate (wrong step)
    assert_eq!(sm.advance("scan", &e), StepResult::Accumulate);

    // first fail event — accumulate
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    // second fail event — step1 satisfied → Advance
    assert_eq!(sm.advance("fail", &e), StepResult::Advance);

    // now scan should match step2
    if let StepResult::Matched(ctx) = sm.advance("scan", &e) {
        assert_eq!(ctx.step_data.len(), 2);
    } else {
        panic!("expected Matched");
    }
}

#[test]
fn or_branch_first_wins() {
    // Two branches in one step; branch 0 completes first
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![
            branch("fail", count_ge(2.0)),  // branch 0
            branch("error", count_ge(1.0)), // branch 1
        ])],
    );
    let mut sm = CepStateMachine::new("rule3".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    if let StepResult::Matched(ctx) = sm.advance("fail", &e) {
        assert_eq!(ctx.step_data[0].satisfied_branch_index, 0);
    } else {
        panic!("expected Matched");
    }
}

#[test]
fn composite_key_isolation() {
    let plan = simple_plan(
        vec![simple_key("sip"), simple_key("dport")],
        vec![step(vec![branch("fail", count_ge(3.0))])],
    );
    let mut sm = CepStateMachine::new("rule4".to_string(), plan, None);

    let e1 = event(vec![("sip", str_val("10.0.0.1")), ("dport", num(22.0))]);
    let e2 = event(vec![("sip", str_val("10.0.0.1")), ("dport", num(80.0))]);

    // Two events to each key — both accumulate (threshold=3)
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate); // key1 count=1
    assert_eq!(sm.advance("fail", &e2), StepResult::Accumulate); // key2 count=1
    assert_eq!(sm.advance("fail", &e1), StepResult::Accumulate); // key1 count=2
    assert_eq!(sm.advance("fail", &e2), StepResult::Accumulate); // key2 count=2
    assert_eq!(sm.instance_count(), 2);

    // Third event to key1 → matched
    if let StepResult::Matched(ctx) = sm.advance("fail", &e1) {
        assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1"), num(22.0)]);
    } else {
        panic!("expected Matched for key1");
    }

    // key2 still needs one more
    if let StepResult::Matched(ctx) = sm.advance("fail", &e2) {
        assert_eq!(ctx.scope_key, vec![str_val("10.0.0.1"), num(80.0)]);
    } else {
        panic!("expected Matched for key2");
    }
}

#[test]
fn guard_filter_skips() {
    // events not matching `action == "failed"` don't count
    let guard = Expr::BinOp {
        op: wf_lang::ast::BinOp::Eq,
        left: Box::new(Expr::Field(wf_lang::ast::FieldRef::Simple(
            "action".to_string(),
        ))),
        right: Box::new(Expr::StringLit("failed".to_string())),
    };

    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "auth".to_string(),
            field: None,
            guard: Some(guard),
            agg: count_ge(2.0),
        }])],
    );
    let mut sm = CepStateMachine::new("rule5".to_string(), plan, None);

    let ok_event = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("success")),
    ]);
    let fail_event = event(vec![
        ("sip", str_val("10.0.0.1")),
        ("action", str_val("failed")),
    ]);

    // success events don't count
    assert_eq!(sm.advance("auth", &ok_event), StepResult::Accumulate);
    assert_eq!(sm.advance("auth", &ok_event), StepResult::Accumulate);

    // first failed event → accumulate
    assert_eq!(sm.advance("auth", &fail_event), StepResult::Accumulate);
    // second failed event → matched
    assert!(matches!(
        sm.advance("auth", &fail_event),
        StepResult::Matched(_)
    ));
}

#[test]
fn distinct_transform() {
    // duplicate dport values not counted; 3 unique > 2 → Matched
    let agg = AggPlan {
        transforms: vec![Transform::Distinct],
        measure: Measure::Count,
        cmp: CmpOp::Gt,
        threshold: Expr::Number(2.0),
    };
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "conn".to_string(),
            field: Some(FieldSelector::Dot("dport".to_string())),
            guard: None,
            agg,
        }])],
    );
    let mut sm = CepStateMachine::new("rule6".to_string(), plan, None);

    let mk = |port: f64| event(vec![("sip", str_val("10.0.0.1")), ("dport", num(port))]);

    // port 22 twice — only counted once
    assert_eq!(sm.advance("conn", &mk(22.0)), StepResult::Accumulate);
    assert_eq!(sm.advance("conn", &mk(22.0)), StepResult::Accumulate); // dup, still count=1

    // port 80 — count=2
    assert_eq!(sm.advance("conn", &mk(80.0)), StepResult::Accumulate);

    // port 443 — count=3 > 2 → Matched
    assert!(matches!(
        sm.advance("conn", &mk(443.0)),
        StepResult::Matched(_)
    ));
}

#[test]
fn source_matching() {
    // events with wrong alias don't contribute to branch
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let mut sm = CepStateMachine::new("rule7".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // wrong alias
    assert_eq!(sm.advance("scan", &e), StepResult::Accumulate);
    assert_eq!(sm.advance("scan", &e), StepResult::Accumulate);
    assert_eq!(sm.advance("scan", &e), StepResult::Accumulate);

    // correct alias
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));
}

#[test]
fn no_key_match() {
    // all events share one instance
    let plan = simple_plan(vec![], vec![step(vec![branch("alert", count_ge(3.0))])]);
    let mut sm = CepStateMachine::new("rule8".to_string(), plan, None);

    let e1 = event(vec![("sip", str_val("10.0.0.1"))]);
    let e2 = event(vec![("sip", str_val("10.0.0.2"))]);
    let e3 = event(vec![("sip", str_val("10.0.0.3"))]);

    assert_eq!(sm.advance("alert", &e1), StepResult::Accumulate);
    assert_eq!(sm.advance("alert", &e2), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 1); // all in one instance

    assert!(matches!(sm.advance("alert", &e3), StepResult::Matched(_)));
}

#[test]
fn sum_measure() {
    // sum(bytes) reaches threshold
    let agg = AggPlan {
        transforms: vec![],
        measure: Measure::Sum,
        cmp: CmpOp::Ge,
        threshold: Expr::Number(1000.0),
    };
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![BranchPlan {
            label: None,
            source: "traffic".to_string(),
            field: Some(FieldSelector::Dot("bytes".to_string())),
            guard: None,
            agg,
        }])],
    );
    let mut sm = CepStateMachine::new("rule9".to_string(), plan, None);

    let mk = |bytes: f64| event(vec![("sip", str_val("10.0.0.1")), ("bytes", num(bytes))]);

    assert_eq!(sm.advance("traffic", &mk(400.0)), StepResult::Accumulate); // sum=400
    assert_eq!(sm.advance("traffic", &mk(500.0)), StepResult::Accumulate); // sum=900

    if let StepResult::Matched(ctx) = sm.advance("traffic", &mk(200.0)) {
        // sum=1100
        assert!((ctx.step_data[0].measure_value - 1100.0).abs() < f64::EPSILON);
    } else {
        panic!("expected Matched");
    }
}

#[test]
fn missing_key_skips() {
    // event without key field → Accumulate (skipped)
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("rule10".to_string(), plan, None);

    // event missing "sip" field
    let e_no_key = event(vec![("dport", num(22.0))]);
    assert_eq!(sm.advance("fail", &e_no_key), StepResult::Accumulate);
    assert_eq!(sm.instance_count(), 0); // no instance created

    // event with "sip" field → should match immediately (count >= 1)
    let e_ok = event(vec![("sip", str_val("10.0.0.1"))]);
    assert!(matches!(sm.advance("fail", &e_ok), StepResult::Matched(_)));
}

#[test]
fn instance_resets_after_match() {
    // same key can match again after reset
    let plan = simple_plan(
        vec![simple_key("sip")],
        vec![step(vec![branch("fail", count_ge(2.0))])],
    );
    let mut sm = CepStateMachine::new("rule11".to_string(), plan, None);
    let e = event(vec![("sip", str_val("10.0.0.1"))]);

    // First match
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    assert!(matches!(sm.advance("fail", &e), StepResult::Matched(_)));

    // Second match — instance was reset, counts from zero again
    assert_eq!(sm.advance("fail", &e), StepResult::Accumulate);
    if let StepResult::Matched(ctx) = sm.advance("fail", &e) {
        assert_eq!(ctx.rule_name, "rule11");
        assert_eq!(ctx.step_data[0].measure_value, 2.0);
    } else {
        panic!("expected second Matched");
    }
}

#[test]
fn numeric_key_type_preserved_through_pipeline() {
    // dport is a numeric key (443.0). After advance + match, scope_key
    // should contain Value::Number(443.0), not Value::Str("443").
    let plan = simple_plan(
        vec![simple_key("sip"), simple_key("dport")],
        vec![step(vec![branch("conn", count_ge(1.0))])],
    );
    let mut sm = CepStateMachine::new("rule_num_key".to_string(), plan, None);

    let e = event(vec![("sip", str_val("10.0.0.1")), ("dport", num(443.0))]);
    if let StepResult::Matched(ctx) = sm.advance("conn", &e) {
        assert_eq!(ctx.scope_key.len(), 2);
        assert_eq!(ctx.scope_key[0], str_val("10.0.0.1"));
        assert_eq!(ctx.scope_key[1], num(443.0));
    } else {
        panic!("expected Matched");
    }
}
