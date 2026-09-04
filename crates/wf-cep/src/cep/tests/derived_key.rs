//! issue #80 — 表达式派生（函数/字面量）match key 引擎测试。
//!
//! 覆盖 `extract_scope_key_mixed` 的行式/混合提取与 `CepStateMachine` 装配：
//! - 派生表达式 key 与「预计算字段」对照组产生**相同**实例分组与 close 结果；
//! - 表达式键缺失（求值 None）→ 事件跳过（与普通 key 缺失语义一致）；
//! - None 槽位（普通字段/嵌套路径）与 extract_key_simple 提取结果一致。
//!
//! 位于模块内部以直接触达 key.rs 私有函数。

use std::collections::HashSet;
use std::time::Duration;

use wf_lang::ast::{CloseMode, CmpOp, Expr, FieldRef, MatchMode, Measure, PathSegment};
use wf_lang::plan::{AggPlan, BranchPlan, MatchPlan, WindowSpec};

use super::key::{
    ScopeKey, extract_key_simple, extract_scope_key_from_row, extract_scope_key_mixed,
    scope_key_from_values,
};
use super::types::{CloseReason, Value};
use super::{CepStateMachine, Event, StepResult};

fn event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}

fn str_val(s: &str) -> Value {
    Value::Str(s.into())
}

fn count_ge(n: f64) -> AggPlan {
    AggPlan {
        transforms: vec![],
        measure: Measure::Count,
        cmp: CmpOp::Ge,
        threshold: Expr::Number(n),
    }
}

fn branch(source: &str, agg: AggPlan) -> BranchPlan {
    BranchPlan {
        label: None,
        source: source.to_string(),
        field: None,
        guard: None,
        agg,
    }
}

fn step(branches: Vec<BranchPlan>) -> wf_lang::plan::StepPlan {
    wf_lang::plan::StepPlan { branches }
}

/// 单步 count>=2、sliding 60s、Any 模式计划；`keys`/`key_exprs` 由调用方决定。
fn counting_plan(keys: Vec<FieldRef>, key_exprs: Vec<Option<Expr>>) -> MatchPlan {
    MatchPlan {
        keys,
        key_exprs,
        key_map: None,
        key_join: None,
        window_spec: WindowSpec::Sliding(Duration::from_secs(60)),
        event_steps: vec![step(vec![branch("e", count_ge(2.0))])],
        close_steps: vec![],
        close_mode: CloseMode::Or,
        tracked_bind_aliases: HashSet::new(),
        tracked_bind_fields: std::collections::HashMap::new(),
        tracked_plain_fields: HashSet::new(),
        seq: None,
        match_mode: MatchMode::Any,
        accu: false,
        needs_field_history: false,
        trigger_event_needed: false,
    }
}

/// `concat(src, ":", dst)` —— #80 派生 key 表达式。
fn concat_pair_expr() -> Expr {
    Expr::FuncCall {
        qualifier: None,
        name: "concat".into(),
        args: vec![
            Expr::Field(FieldRef::Qualified("s".into(), "src".into())),
            Expr::StringLit(":".into()),
            Expr::Field(FieldRef::Qualified("s".into(), "dst".into())),
        ],
    }
}

fn concat_outcome(res: &StepResult) -> bool {
    matches!(res, StepResult::Matched(_))
}

#[test]
fn expr_key_machine_groups_like_precomputed_field() {
    // 派生 key（事件无 pair 字段，引擎按表达式求值）与预计算 pair 字段的
    // 对照组：实例数、命中点、close 回读的 scope_key 完全一致。
    let derived_plan = counting_plan(
        vec![FieldRef::Simple("pair".into())],
        vec![Some(concat_pair_expr())],
    );
    let control_plan = counting_plan(vec![FieldRef::Simple("pair".into())], vec![None]);

    let mut derived = CepStateMachine::new("r".into(), derived_plan, None);
    let mut control = CepStateMachine::new("r".into(), control_plan, None);

    // 镜像事件流：derived 喂 src/dst（pair 由 concat 派生），control 喂 pair。
    let derived_events = [
        event(vec![("src", str_val("a")), ("dst", str_val("b"))]),
        event(vec![("src", str_val("a")), ("dst", str_val("b"))]),
        event(vec![("src", str_val("c")), ("dst", str_val("d"))]),
        event(vec![("src", str_val("a")), ("dst", str_val("b"))]), // 第三发 a:b：不应命中（count>=2 已 fire，非 accu）
    ];
    let control_events = [
        event(vec![("pair", str_val("a:b"))]),
        event(vec![("pair", str_val("a:b"))]),
        event(vec![("pair", str_val("c:d"))]),
        event(vec![("pair", str_val("a:b"))]),
    ];
    let mut derived_outcomes = Vec::new();
    for (i, ev) in derived_events.iter().enumerate() {
        let res = derived.advance_at("e", ev, 1_000_000_000 + i as i64 * 1_000_000_000);
        derived_outcomes.push(concat_outcome(&res));
    }
    let mut control_outcomes = Vec::new();
    for (i, ev) in control_events.iter().enumerate() {
        let res = control.advance_at("e", ev, 1_000_000_000 + i as i64 * 1_000_000_000);
        control_outcomes.push(concat_outcome(&res));
    }
    // 命中模式一致：a:b 在第 2 发命中一次，第 4 发（已 fire 非 accu）不再命中。
    assert_eq!(derived_outcomes, control_outcomes);
    assert_eq!(derived_outcomes, vec![false, true, false, false]);
    assert_eq!(derived.instance_count(), 2, "a:b 与 c:d 两个独立实例");
    assert_eq!(control.instance_count(), 2);

    // close 按派生值回读同一实例（scope_key 值 = concat 结果）。
    let derived_close = derived.close(&[str_val("c:d")], CloseReason::Flush);
    let control_close = control.close(&[str_val("c:d")], CloseReason::Flush);
    let d_out = derived_close.expect("derived c:d instance exists");
    let c_out = control_close.expect("control c:d instance exists");
    assert_eq!(d_out.scope_key, vec![str_val("c:d")]);
    assert_eq!(c_out.scope_key, vec![str_val("c:d")]);
}

#[test]
fn expr_key_missing_field_skips_event() {
    // concat 任一字段缺失 → 表达式求值 None → 事件按 key 缺失跳过（不计入
    // 任何实例）：两条缺字段事件 + 两条有效同组事件 → 仅在有效第二条命中。
    let plan = counting_plan(
        vec![FieldRef::Simple("pair".into())],
        vec![Some(concat_pair_expr())],
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);

    let outcomes: Vec<bool> = [
        event(vec![("src", str_val("a"))]), // 缺 dst → concat None → skip
        event(vec![("dst", str_val("b"))]), // 缺 src → concat None → skip
        event(vec![("src", str_val("x")), ("dst", str_val("y"))]),
        event(vec![("src", str_val("x")), ("dst", str_val("y"))]),
    ]
    .iter()
    .enumerate()
    .map(|(i, ev)| {
        concat_outcome(&sm.advance_at("e", ev, 2_000_000_000 + i as i64 * 1_000_000_000))
    })
    .collect();
    assert_eq!(outcomes, vec![false, false, false, true]);
    assert_eq!(sm.instance_count(), 1, "跳过的两事件不建实例");
    let out = sm
        .close(&[str_val("x:y")], CloseReason::Flush)
        .expect("x:y instance");
    assert_eq!(out.scope_key, vec![str_val("x:y")]);
}

#[test]
fn expr_key_slot_mixed_with_path_slot_extracts_in_order() {
    // 混合键：位 0 = 表达式（coalesce 字面量回退），位 1 = None（嵌套路径）。
    // 验证 mixed 的 None 槽与 extract_key_simple 一致、表达式槽按事件求值。
    let coalesce_expr = Expr::FuncCall {
        qualifier: None,
        name: "coalesce".into(),
        args: vec![
            Expr::Field(FieldRef::Qualified("s".into(), "host".into())),
            Expr::StringLit("unknown".into()),
        ],
    };
    let keys = vec![
        FieldRef::Simple("host_k".into()),
        FieldRef::Path {
            alias: "s".into(),
            segments: vec![
                PathSegment::Field("obj".into()),
                PathSegment::Field("id".into()),
            ],
        },
    ];
    let key_exprs = vec![Some(coalesce_expr), None];
    let ev = event(vec![
        (
            "obj",
            Value::Object([("id".into(), str_val("9"))].into_iter().collect()),
        ),
        // host 缺失 → coalesce 回退 "unknown"
    ]);

    let mixed = extract_scope_key_mixed(&ev, &keys, &key_exprs, "e");
    // 两键位 → Pair(Str("unknown"), Str("9"))：表达式槽 coalesce 回退、None 槽取 obj.id。
    let expected = scope_key_from_values(&[str_val("unknown"), str_val("9")]);
    assert_eq!(mixed, Some(expected));

    // None 槽位单独走 extract_key_simple：只取 obj.id，结果与上面第 2 位一致。
    let simple = extract_key_simple(&ev, &[keys[1].clone()]).expect("path present");
    assert_eq!(simple, vec![str_val("9")]);

    // 表达式位缺失 + 无回退 → None（事件跳过）。
    let no_fallback = Expr::FuncCall {
        qualifier: None,
        name: "concat".into(),
        args: vec![
            Expr::Field(FieldRef::Qualified("s".into(), "host".into())),
            Expr::StringLit("!".into()),
        ],
    };
    let skip = extract_scope_key_mixed(&ev, &keys, &[Some(no_fallback), None], "e");
    assert_eq!(skip, None);
}

#[test]
fn mixed_all_none_matches_row_based_scope_key() {
    // 全 None 槽（防御性构造）→ 与 extract_scope_key_from_row 结果一致：
    // 保证装配层只在确有表达式键时启用 mixed 路径、行为不漂移。
    let keys = vec![FieldRef::Simple("sip".into())];
    let key_exprs = vec![None];
    let ev = event(vec![("sip", str_val("10.0.0.1"))]);
    let mixed = extract_scope_key_mixed(&ev, &keys, &key_exprs, "e");
    let row = extract_scope_key_from_row(&ev, &keys, None, "e");
    assert_eq!(mixed, row);
    assert_eq!(mixed, Some(ScopeKey::Str("10.0.0.1".into())));
}

#[test]
fn expr_key_object_result_skips_like_structured_leaf() {
    // 表达式结果为结构化值（object）→ 与嵌套路径 key 叶为 object 一样视为
    // key 缺失跳过（不让整对象坍缩进固定 [object] 桶）。
    let obj_expr = Expr::Field(FieldRef::Qualified("s".into(), "roles".into()));
    let keys = vec![FieldRef::Simple("k".into())];
    let key_exprs = vec![Some(obj_expr)];
    let ev = event(vec![(
        "roles",
        Value::Object([("a".into(), str_val("1"))].into_iter().collect()),
    )]);
    assert_eq!(extract_scope_key_mixed(&ev, &keys, &key_exprs, "e"), None);
    // 标量结果则正常成键。
    let scalar_ev = event(vec![("roles", str_val("x"))]);
    let got = extract_scope_key_mixed(&scalar_ev, &keys, &key_exprs, "e");
    assert_eq!(got, Some(ScopeKey::Str("x".into())));
}

#[test]
fn expr_key_numeric_and_bool_results_type_consistently() {
    // review 4：表达式键的数值/布尔结果经 ScopeKey::from_value 归一，与同值
    // 普通字段键同构（Int 塌缩、Bool → "true"/"false" 字符串）——typed key
    // 与列式/行式分组跨路径一致。
    let num_ev = event(vec![("a", Value::Number(2.0)), ("b", Value::Number(4.0))]);
    let add = Expr::BinOp {
        op: wf_lang::ast::BinOp::Add,
        left: Box::new(Expr::Field(FieldRef::Qualified("s".into(), "a".into()))),
        right: Box::new(Expr::Field(FieldRef::Qualified("s".into(), "b".into()))),
    };
    let keys = vec![FieldRef::Simple("sum".into())];
    assert_eq!(
        extract_scope_key_mixed(&num_ev, &keys, &[Some(add)], "e"),
        Some(ScopeKey::Int(6)),
        "2+4 表达式键 → Int(6)（与普通字段 6 同构）"
    );
    // 缺字段 → 求值 None → 跳过。
    let missing = event(vec![("a", Value::Number(2.0))]);
    let add2 = Expr::BinOp {
        op: wf_lang::ast::BinOp::Add,
        left: Box::new(Expr::Field(FieldRef::Qualified("s".into(), "a".into()))),
        right: Box::new(Expr::Field(FieldRef::Qualified("s".into(), "b".into()))),
    };
    assert_eq!(
        extract_scope_key_mixed(&missing, &keys, &[Some(add2)], "e"),
        None
    );

    // 比较表达式键 → Bool → 归一为 "true"，与普通 bool 字段键一致。
    let cmp_ev = event(vec![("a", Value::Number(7.0)), ("b", Value::Number(3.0))]);
    let gt = Expr::BinOp {
        op: wf_lang::ast::BinOp::Gt,
        left: Box::new(Expr::Field(FieldRef::Qualified("s".into(), "a".into()))),
        right: Box::new(Expr::Field(FieldRef::Qualified("s".into(), "b".into()))),
    };
    assert_eq!(
        extract_scope_key_mixed(&cmp_ev, &keys, &[Some(gt)], "e"),
        Some(ScopeKey::Str("true".into()))
    );
}

#[test]
fn expr_key_machine_columnar_source_groups_like_row_source() {
    // 列式事件源（ColumnarEvent，deferred 快照路径）：mixed 提取逐行经
    // `FieldSource::field_value` 读列，与行式 Event 命中模式/实例数一致。
    use crate::row_views::ColumnarEvent;
    use arrow::array::{ArrayRef, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field as ArrowField, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("src", DataType::Utf8, true),
        ArrowField::new("dst", DataType::Utf8, true),
        ArrowField::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("c"),
                Some("a"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("b"),
                Some("b"),
                Some("d"),
                Some("b"),
            ])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_000),
                Some(2_000),
                Some(3_000),
                Some(4_000),
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let plan = counting_plan(
        vec![FieldRef::Simple("pair".into())],
        vec![Some(concat_pair_expr())],
    );
    let mut sm = CepStateMachine::new("r".into(), plan, None);
    let col_outcomes: Vec<bool> = (0..4)
        .map(|row| {
            let ce = ColumnarEvent::new(&batch, row);
            let res = sm.advance_at_with_masks(
                "e",
                &ce,
                1_000_000_000 + row as i64 * 1_000_000_000,
                None,
                row,
                None,
            );
            matches!(res, StepResult::Matched(_))
        })
        .collect();
    assert_eq!(col_outcomes, vec![false, true, false, false]);
    assert_eq!(sm.instance_count(), 2, "列式源同样按派生值分组");
}
