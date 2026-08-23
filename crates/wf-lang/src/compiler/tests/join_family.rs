// P1 join 算子族 compiler 计划透传测试：`within`/`reduce`/`emit at` 进入 JoinPlan，
// 缺省 mode = Inner，`as label` 引用编译为 FieldRef::Path。

use std::time::Duration;

use super::*;
use crate::schema::{BaseType, FieldDef, WindowSchema};

fn auction_events_window() -> WindowSchema {
    WindowSchema {
        name: "auction_events".to_string(),
        streams: vec!["auction".to_string()],
        time_field: Some("dateTime".to_string()),
        over: Duration::from_secs(1800),
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "seller".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "dateTime".to_string(),
                field_type: bt(BaseType::Time),
            },
            FieldDef {
                name: "expires".to_string(),
                field_type: bt(BaseType::Time),
            },
        ],
    }
}

fn bid_events_window() -> WindowSchema {
    WindowSchema {
        name: "bid_events".to_string(),
        streams: vec!["bid".to_string()],
        time_field: Some("dateTime".to_string()),
        over: Duration::from_secs(1800),
        fields: vec![
            FieldDef {
                name: "auction".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "bidder".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "price".to_string(),
                field_type: bt(BaseType::Digit),
            },
            FieldDef {
                name: "dateTime".to_string(),
                field_type: bt(BaseType::Time),
            },
        ],
    }
}

fn out_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("id", bt(BaseType::Digit)),
            ("winner_id", bt(BaseType::Digit)),
        ],
    )
}

fn schemas() -> Vec<WindowSchema> {
    vec![auction_events_window(), bid_events_window(), out_window()]
}

/// JoinPlan 携带 within/reduce/emit_at；缺省 mode = Inner；`as label` 编译为 Path。
#[test]
fn join_plan_carries_within_reduce_emit_and_label_path() {
    let input = r#"
rule q9 {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(price) tie(dateTime asc)
        within [a.dateTime, a.expires]
        on a.id == bid_events.auction as winner
        emit at a.expires
    entity(digit, a.id)
    yield out (id = a.id, winner_id = winner.bidder)
}
"#;
    let plans = compile_with(input, &schemas());
    assert_eq!(plans.len(), 1);
    let plan = &plans[0];
    assert_eq!(plan.joins.len(), 1);
    let join = &plan.joins[0];

    assert_eq!(join.mode, JoinMode::Inner);
    assert_eq!(join.right_window, "bid_events");

    // within 透传
    let w = join.within.as_ref().expect("within in plan");
    assert_eq!(
        w.lo.val,
        BoundVal::Expr(Expr::Field(FieldRef::Qualified(
            "a".into(),
            "dateTime".into()
        )))
    );

    // reduce 透传
    let rc = join.reduce.as_ref().expect("reduce in plan");
    assert_eq!(rc.label.as_deref(), Some("winner"));
    assert!(matches!(
        &rc.measure,
        ReduceMeasure::Maxrow { field, tie: Some(_) } if field == &FieldRef::Simple("price".into())
    ));

    // emit_at 透传
    assert_eq!(
        join.emit_at,
        Some(Expr::Field(FieldRef::Qualified(
            "a".into(),
            "expires".into()
        )))
    );

    // `winner.bidder` 编译为 FieldRef::Path（review R2）
    let winner_field = plan
        .yield_plan
        .fields
        .iter()
        .find(|f| f.name == "winner_id")
        .expect("winner_id yield field");
    assert_eq!(
        winner_field.value,
        Expr::Field(FieldRef::Path {
            alias: "winner".into(),
            segments: vec![PathSegment::Field("bidder".into())],
        })
    );
}

/// Q8 形态：emit_at 表达式（bucket_end）进入计划。
#[test]
fn join_plan_carries_emit_at_func_call() {
    let input = r#"
rule q8 {
    events { p : auction_events }
    on each p -> score(1.0)
    join bid_events within [p.dateTime, <bucket_end(p.dateTime, 10s)]
        on p.id == bid_events.auction
        emit at bucket_end(p.dateTime, 10s)
    entity(digit, p.id)
    yield out (id = p.id)
}
"#;
    let plans = compile_with(input, &schemas());
    let join = &plans[0].joins[0];
    let w = join.within.as_ref().expect("within in plan");
    assert!(w.hi.open);
    assert!(matches!(
        &w.hi.val,
        BoundVal::Expr(Expr::FuncCall { name, .. }) if name == "bucket_end"
    ));
    assert!(matches!(
        &join.emit_at,
        Some(Expr::FuncCall { name, .. }) if name == "bucket_end"
    ));
}

/// Explain 对新语法字符串化不 panic 且信息完整。
#[test]
fn explain_renders_join_family_syntax() {
    let input = r#"
rule q9 {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(price) tie(dateTime asc)
        within [a.dateTime, a.expires]
        on a.id == bid_events.auction as winner
        emit at a.expires
    entity(digit, a.id)
    yield out (id = a.id)
}
"#;
    let plans = compile_with(input, &schemas());
    let expl = crate::explain::explain_rules(&plans, &schemas());
    assert_eq!(expl.len(), 1);
    let joins = &expl[0].joins;
    assert_eq!(joins.len(), 1);
    let j = &joins[0];
    assert!(j.contains("within [a.dateTime , a.expires]"), "got: {}", j);
    assert!(
        j.contains("reduce maxrow(price) tie(dateTime asc)"),
        "got: {}",
        j
    );
    assert!(j.contains("as winner"), "got: {}", j);
    assert!(j.contains("emit at a.expires"), "got: {}", j);
}

/// review 补充：`where` 里的 `label.field` 也重写为 Path；非标签限定符（如 `a.id`）
/// 保持 Qualified 不变。
#[test]
fn where_clause_label_ref_rewritten_and_plain_qualified_preserved() {
    let input = r#"
rule q9 {
    events { a : auction_events }
    on each a -> score(1.0)
    join bid_events reduce maxrow(price) on a.id == bid_events.auction as winner
        where winner.bidder > 0
    entity(digit, a.id)
    yield out (id = a.id, winner_id = winner.bidder)
}
"#;
    let plans = compile_with(input, &schemas());
    let plan = &plans[0];

    // where 重写为 Path
    let w = plan.r#where.as_ref().expect("where plan");
    assert!(matches!(
        w,
        Expr::BinOp {
            left,
            op: BinOp::Gt,
            ..
        } if matches!(
            left.as_ref(),
            Expr::Field(FieldRef::Path { alias, segments }) if alias == "winner"
                && segments == &[PathSegment::Field("bidder".into())]
        )
    ));

    // 非标签限定符保留 Qualified
    let id_field = plan
        .yield_plan
        .fields
        .iter()
        .find(|f| f.name == "id")
        .expect("id yield field");
    assert_eq!(
        id_field.value,
        Expr::Field(FieldRef::Qualified("a".into(), "id".into()))
    );
}
