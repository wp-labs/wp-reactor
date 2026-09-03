//! coverage_r4 拆出的兄弟子模块（2026-09-04）：`executor/close_exec.rs`
//! 与 `executor/each_exec.rs` 覆盖——qualified-close alert 构建 / 直写
//! 列式批 / step-stage 注解，以及 on-each filter / lets / joins / `where` /
//! 错误道 / deferred pipe 轻量 build 对拍。共享 harness 在父模块
//! `coverage_r4.rs`，此处经 `use super::*` 复用。

use super::*;

use crate::alert::AlertColumnBuilder;
use crate::match_engine::cep::{BindData, CloseOutput, CloseReason};
use wf_lang::ast::SystemVar;
use wf_lang::plan::YieldField;

fn plan_with_close(
    keys: Vec<FieldRef>,
    event_steps: Vec<StepPlan>,
    close_steps: Vec<StepPlan>,
) -> MatchPlan {
    MatchPlan {
        keys,
        key_exprs: Vec::new(),
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(300)),
        event_steps,
        close_steps,
        close_mode: CloseMode::And,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Seq,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

fn close_output(
    event_ok: bool,
    close_ok: bool,
    close_mode: CloseMode,
    event_step_data: Vec<StepData>,
    close_step_data: Vec<StepData>,
) -> CloseOutput {
    CloseOutput {
        rule_name: "r1".to_string(),
        scope_key: vec![str_val("10.0.0.1")],
        close_reason: CloseReason::Timeout,
        event_ok,
        close_ok,
        close_mode,
        event_emitted: false,
        event_step_data,
        close_step_data,
        bind_data: vec![],
        watermark_nanos: 1_700_000_000_000_000_000,
        machine_id: String::new(),
        event_first_time_nanos: 0,
        event_last_time_nanos: 0,
        first_match_time_nanos: None,
        evidence_first_time_nanos: 0,
        evidence_last_time_nanos: 0,
        window_start_time_nanos: 0,
        window_end_time_nanos: 0,
        last_event_nanos: 1_700_000_000_000_000_000,
        row_fields: None,
        row_field_names: None,
    }
}

// ---------------------------------------------------------------------------
// executor/close_exec.rs — qualified-close alert building
// ---------------------------------------------------------------------------

#[test]
fn execute_close_qualified_and_error_paths() {
    // And-mode close with event_ok && close_ok → record.
    let mut plan = simple_rule_plan(
        "close_r",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
            vec![step(vec![branch("done", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "score_field".into(),
            value: Expr::SystemVar(SystemVar::Score),
        },
        YieldField {
            name: "label_field".into(),
            value: field("fail"),
        },
    ];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("score_field".into(), FieldType::Base(BaseType::Float)),
            ("label_field".into(), FieldType::Base(BaseType::Chars)),
        ]),
    );

    let qualified = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![step_data(Some("done"), 2.0)],
    );
    let out = exec
        .execute_close(&qualified)
        .unwrap()
        .expect("qualified close emits");
    assert_eq!(out.rule_name.as_ref(), "close_r");
    assert_eq!(out.score, 70.0);
    assert_eq!(out.entity_id, "10.0.0.1");

    // Unqualified (event not ok) → Ok(None).
    let unqualified = close_output(
        false,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );
    assert!(exec.execute_close(&unqualified).unwrap().is_none());

    // OR-mode close with close steps → qualifies.
    let or_qualified = close_output(
        true,
        true,
        CloseMode::Or,
        vec![step_data(Some("fail"), 1.0)],
        vec![step_data(Some("done"), 2.0)],
    );
    assert!(exec.execute_close(&or_qualified).unwrap().is_some());
    // OR-mode without close steps → does not qualify.
    let or_empty = close_output(
        true,
        true,
        CloseMode::Or,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );
    assert!(exec.execute_close(&or_empty).unwrap().is_none());

    // Score error: non-numeric score expression → Err.
    let bad_plan = simple_rule_plan(
        "bad_score",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
            vec![],
        ),
        field("ghost"),
        "ip",
        field("sip"),
    );
    let bad_exec = RuleExecutor::new(bad_plan);
    let qualified = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );
    assert!(bad_exec.execute_close(&qualified).is_err());

    // Entity id error: a non-field entity expr evaluating to None → Err.
    let bad_entity_plan = simple_rule_plan(
        "bad_entity",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
            vec![],
        ),
        Expr::Number(70.0),
        "ip",
        Expr::Neg(Box::new(field("ghost_entity"))),
    );
    let bad_entity_exec = RuleExecutor::new(bad_entity_plan);
    let qualified = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );
    // Entity fallback：eval_yield_expr 对缺失字段回退空串 → entity_id=""，不 err。
    let rec = bad_entity_exec
        .execute_close(&qualified)
        .expect("entity 空串 fallback 不 err");
    assert!(rec.is_some(), "entity 回退空串仍输出记录");
}

#[test]
fn execute_close_with_joins_miss_and_where_reject() {
    let mut plan = simple_rule_plan(
        "close_join",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
            vec![],
        ),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    // Join the scope key (`sip`) against the right window's `id` so the close
    // ctx actually has the left field present.
    plan.joins = vec![JoinPlan {
        right_window: "w".to_string(),
        mode: JoinMode::Inner,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple("sip".into()),
            right: FieldRef::Simple("id".into()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("flag")),
        right: Box::new(Expr::Bool(true)),
    });
    let exec = RuleExecutor::new(plan);
    let qualified = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );

    // Inner join miss (empty right) → Ok(None).
    let empty_lookup = RowsLookup::new(vec![]);
    assert!(
        exec.execute_close_with_joins(&qualified, &empty_lookup)
            .unwrap()
            .is_none()
    );

    // Join hit but `where` false (no flag in the close ctx) → Ok(None).
    let lookup = RowsLookup::new(vec![join_row_event(vec![("id", str_val("10.0.0.1"))])]);
    assert!(
        exec.execute_close_with_joins(&qualified, &lookup)
            .unwrap()
            .is_none()
    );

    // Unqualified close → Ok(None) before any join work.
    let unqualified = close_output(
        false,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![],
    );
    assert!(
        exec.execute_close_with_joins(&unqualified, &lookup)
            .unwrap()
            .is_none()
    );
}

#[test]
fn close_exec_direct_batch_columnar_resolve_close_field() {
    // A columnar-safe close plan: constant score, field entity, field yields.
    let mut plan = simple_rule_plan(
        "col_close",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("fail", count_ge(1.0))])],
            vec![],
        ),
        Expr::Number(80.0),
        "ip",
        field("sip"),
    );
    plan.yield_plan.fields = vec![
        YieldField {
            name: "sip_out".into(),
            value: field("sip"),
        },
        YieldField {
            name: "label_out".into(),
            value: field("fail"),
        },
        YieldField {
            name: "field_values_out".into(),
            value: field("src"),
        },
        YieldField {
            name: "bind_out".into(),
            value: field("bind_v"),
        },
    ];
    let exec = RuleExecutor::new(plan);
    assert!(exec.close_plan_columnar_safe());

    let mut sd = step_data(Some("fail"), 2.0);
    sd.field_values = EngineHashMap::from_iter([("src".to_string(), vec![str_val("10.0.0.2")])]);
    let mut qualified = close_output(true, true, CloseMode::And, vec![sd], vec![]);
    qualified.bind_data = vec![BindData {
        alias: "b".into(),
        count: 1,
        field_values: EngineHashMap::from_iter([("bind_v".to_string(), vec![str_val("b-value")])]),
    }];

    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let stats = exec.execute_close_direct_batch_columnar(&[qualified], &mut builder, 0);
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.rejected, 0);

    // Unqualified close → rejected.
    let unqualified = close_output(
        false,
        true,
        CloseMode::And,
        vec![step_data(None, 1.0)],
        vec![],
    );
    let stats = exec.execute_close_direct_batch_columnar(&[unqualified], &mut builder, 0);
    assert_eq!(stats.rejected, 1);
}

#[test]
fn close_stage_annotation_marks_event_and_close_steps() {
    // A close rule whose yield reads an aggregate over the step series. The
    // `_step_*_stage` annotation must mark the close-stage step so the
    // aggregate prefers it (otherwise both steps would be "event" and the sum
    // would include the event step's measure).
    let mut plan = simple_rule_plan(
        "close_stage",
        plan_with_close(
            vec![simple_key("sip")],
            vec![step(vec![branch("e", count_ge(1.0))])],
            vec![step(vec![branch("e", count_ge(1.0))])],
        ),
        Expr::Number(70.0),
        "ip",
        field("sip"),
    );
    // A function-call yield forces the all-fields ctx build (close_ctx_fields
    // → All), which carries the `_step_*` fields the annotation walks.
    plan.yield_plan.fields = vec![YieldField {
        name: "close_sum".into(),
        value: Expr::FuncCall {
            qualifier: None,
            name: "sum".into(),
            args: vec![field("e")],
        },
    }];
    let exec = RuleExecutor::new(plan);
    let qualified = close_output(
        true,
        true,
        CloseMode::And,
        vec![step_data(Some("fail"), 1.0)],
        vec![step_data(Some("done"), 2.0)],
    );
    let rec = exec
        .execute_close(&qualified)
        .unwrap()
        .expect("qualified close");
    // sum over the close-stage step only → 2.0 (not 3.0).
    assert_eq!(rec.yield_fields[0].1, num(2.0));
}

// ---------------------------------------------------------------------------
// executor/each_exec.rs — on-each paths
// ---------------------------------------------------------------------------

fn each_rule(filter: Option<Expr>, lets: Vec<wf_lang::plan::LetPlan>) -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "each_r",
        simple_plan(vec![], vec![]),
        Expr::Number(42.0),
        "ip",
        field("sip"),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter,
    });
    plan.lets = lets;
    plan.yield_plan.fields = vec![YieldField {
        name: "out".into(),
        value: field("sip"),
    }];
    RuleExecutor::new(plan)
}

#[test]
fn execute_each_error_filter_lets_and_where() {
    // Non-`on each` rule → Err.
    let plain = RuleExecutor::new(simple_rule_plan(
        "m",
        default_match_plan(),
        Expr::Number(1.0),
        "ip",
        field("sip"),
    ));
    assert!(
        plain
            .execute_each(&event(vec![("sip", str_val("1.1.1.1"))]), 0)
            .is_err()
    );

    // Filter rejects → Ok(None).
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("kind")),
        right: Box::new(Expr::StringLit("pass".into())),
    };
    let exec = each_rule(Some(filter.clone()), vec![]);
    let ev = event(vec![("sip", str_val("1.1.1.1")), ("kind", str_val("drop"))]);
    assert!(exec.execute_each(&ev, 1000).unwrap().is_none());
    let ev = event(vec![("sip", str_val("1.1.1.1")), ("kind", str_val("pass"))]);
    let rec = exec
        .execute_each(&ev, 1000)
        .unwrap()
        .expect("passes filter");
    assert_eq!(rec.score, 42.0);

    // With `let` bindings → clone + apply_lets + build.
    let lets = vec![wf_lang::plan::LetPlan {
        name: "computed".into(),
        expr: Expr::BinOp {
            op: BinOp::Add,
            left: Box::new(field("n")),
            right: Box::new(Expr::Number(1.0)),
        },
    }];
    let exec = each_rule(Some(filter), lets);
    let ev = event(vec![
        ("sip", str_val("1.1.1.1")),
        ("kind", str_val("pass")),
        ("n", num(5.0)),
    ]);
    let rec = exec
        .execute_each(&ev, 1000)
        .unwrap()
        .expect("passes filter");
    assert_eq!(rec.score, 42.0);
    // `let` that fails to evaluate leaves no injected field (no panic).
    let lets = vec![wf_lang::plan::LetPlan {
        name: "computed".into(),
        expr: field("ghost"),
    }];
    let exec = each_rule(None, lets);
    let rec = exec.execute_each(&ev, 1000).unwrap().expect("no filter");
    assert_eq!(rec.score, 42.0);
}

#[test]
fn execute_each_with_joins_and_direct_branches() {
    let mut plan = simple_rule_plan(
        "each_join",
        simple_plan(vec![], vec![]),
        Expr::Number(42.0),
        "ip",
        field("sip"),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.joins = vec![one_cond_join(JoinMode::Inner)];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("flag")),
        right: Box::new(Expr::Bool(true)),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "out".into(),
        value: field("sip"),
    }];
    let exec = RuleExecutor::new(plan);
    let ev = event(vec![
        ("sip", str_val("1.1.1.1")),
        ("bidder", num(1.0)),
        ("flag", Value::Bool(true)),
    ]);

    // Join miss → Ok(None).
    let empty = RowsLookup::new(vec![]);
    assert!(
        exec.execute_each_with_joins(&ev, 1000, &empty, &[], 2000)
            .unwrap()
            .is_none()
    );

    // Join hit, where true → record.
    let lookup = RowsLookup::new(vec![join_row_event(vec![("id", num(1.0))])]);
    let rec = exec
        .execute_each_with_joins(&ev, 1000, &lookup, &[], 2000)
        .unwrap()
        .expect("record");
    assert_eq!(rec.score, 42.0);

    // Where false → Ok(None).
    let ev_no_flag = event(vec![("sip", str_val("1.1.1.1")), ("bidder", num(1.0))]);
    assert!(
        exec.execute_each_with_joins(&ev_no_flag, 1000, &lookup, &[], 2000)
            .unwrap()
            .is_none()
    );

    // Direct path: filter miss → Ok(false); hit → Ok(true) and rows appended.
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let ok = exec
        .execute_each_direct(&ev, 1000, &lookup, &[], 2000, &mut builder)
        .unwrap();
    assert!(ok);
    let ok = exec
        .execute_each_direct(&ev_no_flag, 1000, &lookup, &[], 2000, &mut builder)
        .unwrap();
    assert!(!ok);
}

#[test]
fn execute_each_direct_batch_rejections_and_errors() {
    // Non-`on each` rule → failed = rows.len().
    let plain = RuleExecutor::new(simple_rule_plan(
        "m",
        default_match_plan(),
        Expr::Number(1.0),
        "ip",
        field("sip"),
    ));
    let ev = event(vec![("sip", str_val("1.1.1.1"))]);
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let mut appended = Vec::new();
    let stats = plain.execute_each_direct_batch(
        &[(&ev, 1000)],
        &RowsLookup::new(vec![]),
        &[],
        2000,
        &mut builder,
        &mut appended,
    );
    assert_eq!(stats.failed, 1);

    // Filter rejection → rejected.
    let filter = Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("kind")),
        right: Box::new(Expr::StringLit("pass".into())),
    };
    let exec = each_rule(Some(filter), vec![]);
    let drop_ev = event(vec![("sip", str_val("1.1.1.1")), ("kind", str_val("drop"))]);
    let stats = exec.execute_each_direct_batch(
        &[(&drop_ev, 1000)],
        &RowsLookup::new(vec![]),
        &[],
        2000,
        &mut builder,
        &mut appended,
    );
    assert_eq!(stats.rejected, 1);
    assert_eq!(stats.appended, 0);
    assert!(appended.is_empty());

    // Join + where rejections on the batch path.
    let mut plan = simple_rule_plan(
        "each_batch",
        simple_plan(vec![], vec![]),
        Expr::Number(42.0),
        "ip",
        field("sip"),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.joins = vec![one_cond_join(JoinMode::Inner)];
    plan.r#where = Some(Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(field("flag")),
        right: Box::new(Expr::Bool(true)),
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "out".into(),
        value: field("sip"),
    }];
    let exec = RuleExecutor::new(plan);
    let hit_ev = event(vec![
        ("sip", str_val("1.1.1.1")),
        ("bidder", num(1.0)),
        ("flag", Value::Bool(true)),
    ]);
    let miss_ev = event(vec![("sip", str_val("2.2.2.2")), ("bidder", num(2.0))]);
    let lookup = RowsLookup::new(vec![join_row_event(vec![("id", num(1.0))])]);
    let stats = exec.execute_each_direct_batch(
        &[(&hit_ev, 1000), (&miss_ev, 1000)],
        &lookup,
        &[],
        2000,
        &mut builder,
        &mut appended,
    );
    assert_eq!(stats.appended, 1);
    assert_eq!(stats.rejected, 1);
    assert_eq!(appended, vec![0]);

    // Score-evaluation error on the batch path → failed.
    let mut bad_score = simple_rule_plan(
        "each_bad_score",
        simple_plan(vec![], vec![]),
        field("ghost_score"),
        "ip",
        field("sip"),
    );
    bad_score.binds[0].alias = "e".into();
    bad_score.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    let bad_exec = RuleExecutor::new(bad_score);
    let mut appended = Vec::new();
    let stats = bad_exec.execute_each_direct_batch(
        &[(&ev, 1000)],
        &RowsLookup::new(vec![]),
        &[],
        2000,
        &mut builder,
        &mut appended,
    );
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.appended, 0);
}

#[test]
fn build_each_alert_with_deferred_origin() {
    // build_each_alert_with is exercised through the record path with a custom
    // origin — the direct public route is execute_each (Event origin). We use
    // the each rule to confirm the alert record carries machine_id from the
    // event (extract_event_str on MACHINE_ID).
    let exec = each_rule(None, vec![]);
    let ev = event(vec![
        ("sip", str_val("1.1.1.1")),
        (crate::match_engine::MACHINE_ID, str_val("mid-1")),
    ]);
    let rec = exec.execute_each(&ev, 1000).unwrap().expect("record");
    assert_eq!(rec.machine_id.as_ref(), "mid-1");
    assert_eq!(rec.scope_key.as_ref(), "each_r");
    // machine_id_of reads the same extraction helper.
    assert_eq!(RuleExecutor::machine_id_of(&ev), "mid-1");
    assert_eq!(RuleExecutor::machine_id_of(&event(vec![])), "");
}

/// q4a 形状的 each plan：yield 4 字段（id/category/final=winner.price Path/
/// dateTime=expires），entity=digit(id)——中间窗轻量化（build_each_alert_pipe）
/// 的对拍对象。
fn q4a_pipe_rule() -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "q4a_pipe_r",
        simple_plan(vec![], vec![]),
        Expr::Number(20.0),
        "digit",
        field("id"),
    );
    plan.binds[0].alias = "a".into();
    plan.each_plan = Some(EachPlan {
        alias: "a".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "id".into(),
            value: field("id"),
        },
        YieldField {
            name: "category".into(),
            value: field("category"),
        },
        YieldField {
            name: "final".into(),
            value: Expr::Field(FieldRef::Path {
                alias: "winner".into(),
                segments: vec![wf_lang::ast::PathSegment::Field("price".into())],
            }),
        },
        YieldField {
            name: "dateTime".into(),
            value: field("expires"),
        },
    ];
    RuleExecutor::new(plan)
}

/// 中间窗轻量化对拍（2026-08-26 q4a）：`build_each_alert_pipe`（轻量）与
/// `build_each_alert_with`（全量）产出的**中间窗相关字段逐位一致**——
/// yield_fields（含 winner.price Path 读取）、event_time_nanos、meta
/// （rule_name/score/entity_type/entity_id）、yield_target。轻量只跳过 sink
/// 才需要的告警字段（wfx_id/fired_at/summary/machine_id 空值），不得影响
/// 中间窗列内容。
#[test]
fn deferred_pipe_light_build_matches_full_build() {
    let exec = q4a_pipe_rule();
    assert!(
        exec.pipe_light_build_ready(),
        "q4a 形状（纯 Field/Path yield）必须走轻量 build"
    );
    let ctx = event(vec![
        ("id", num(5.0)),
        ("category", num(3.0)),
        ("price", num(25.5)), // winner.price 富化后的裸名字段
        ("expires", num(1_000.0)),
    ]);
    let full = exec
        .build_each_alert_with(&ctx, 1_000, crate::alert::AlertOrigin::Deferred, &[], 1_000)
        .unwrap()
        .expect("full build");
    let light = exec
        .build_each_alert_pipe(&ctx, 1_000)
        .unwrap()
        .expect("light build");

    // 中间窗消费者相关的字段必须逐位一致。
    assert_eq!(full.yield_fields, light.yield_fields, "yield_fields");
    assert_eq!(full.event_time_nanos, light.event_time_nanos, "event_time");
    assert_eq!(full.rule_name, light.rule_name, "rule_name");
    assert_eq!(full.score, light.score, "score");
    assert_eq!(full.entity_type, light.entity_type, "entity_type");
    assert_eq!(full.entity_id, light.entity_id, "entity_id");
    assert_eq!(full.yield_target, light.yield_target, "yield_target");

    // 轻量跳过的告警字段为空（语义：中间窗消费者不读这些列）。
    assert_eq!(light.wfx_id, "");
    assert_eq!(light.fired_at, "");
    assert_eq!(light.summary.as_ref(), "");
}

/// 轻量门控判定（2026-08-26 q4a）：yield 引用 `__wfu_*` meta → 回退全量
/// （light YieldMeta 的空槽不可观测性不成立）；SystemVar（light 提供真值）/
/// 纯 Field/Path → 放行。
#[test]
fn pipe_light_build_ready_gate() {
    use wf_lang::ast::WfuMetaField;
    // 引用 wfx_id meta → 回退。
    let mut plan = simple_rule_plan(
        "gate_r",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        field("sip"),
    );
    plan.yield_plan.fields = vec![YieldField {
        name: "id".into(),
        value: Expr::WfuMeta(WfuMetaField::Id),
    }];
    assert!(
        !RuleExecutor::new(plan).pipe_light_build_ready(),
        "WfuMeta → 回退"
    );

    // SystemVar（score）→ light meta 提供真值，放行。
    let mut plan2 = simple_rule_plan(
        "gate_r2",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        field("sip"),
    );
    plan2.yield_plan.fields = vec![YieldField {
        name: "s".into(),
        value: Expr::SystemVar(SystemVar::Score),
    }];
    assert!(
        RuleExecutor::new(plan2).pipe_light_build_ready(),
        "SystemVar → 放行"
    );

    // 纯 Field → 放行（q4a 同款）。
    let mut plan3 = simple_rule_plan(
        "gate_r3",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        field("sip"),
    );
    plan3.yield_plan.fields = vec![YieldField {
        name: "f".into(),
        value: field("sip"),
    }];
    assert!(
        RuleExecutor::new(plan3).pipe_light_build_ready(),
        "Field → 放行"
    );
}
