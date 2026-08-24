//! Wiring tests: the columnar batch-filter masks on [`RuleExecutor`] must match
//! the per-event interpreted filter path bit-for-bit (below `2^53`, where the
//! native-i64 dispatch is identical to the f64 path).
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, Expr, FieldRef};
use wf_lang::plan::{BindPlan, EachPlan};

use crate::match_engine::RuleExecutor;
use crate::match_engine::batch_to_events;
use crate::match_engine::match_engine::FieldSource;

use super::helpers::{branch, count_ge, simple_plan, simple_rule_plan, step};

fn auction_mod_123_expr() -> Expr {
    Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::BinOp {
            op: BinOp::Mod,
            left: Box::new(Expr::Field(FieldRef::Simple("auction".to_string()))),
            right: Box::new(Expr::Number(123.0)),
        }),
        right: Box::new(Expr::Number(0.0)),
    }
}

fn func_filter() -> Expr {
    Expr::FuncCall {
        qualifier: None,
        name: "length".to_string(),
        args: vec![Expr::Field(FieldRef::Simple("auction".to_string()))],
    }
}

fn bind_executor(filter: Option<Expr>) -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "columnar_bind",
        simple_plan(
            vec![FieldRef::Simple("auction".to_string())],
            vec![step(vec![branch("b", count_ge(1.0))])],
        ),
        Expr::Number(5.0),
        "digit",
        Expr::Field(FieldRef::Simple("auction".to_string())),
    );
    plan.binds = vec![BindPlan {
        alias: "b".into(),
        window: "bid_events".into(),
        filter,
    }];
    RuleExecutor::new(plan)
}

fn each_executor(filter: Option<Expr>) -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "columnar_each",
        simple_plan(vec![], vec![]),
        Expr::Number(5.0),
        "digit",
        Expr::Field(FieldRef::Simple("auction".to_string())),
    );
    plan.each_plan = Some(EachPlan {
        alias: "b".into(),
        filter,
    });
    RuleExecutor::new(plan)
}

fn auction_batch(values: Vec<Option<i64>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "auction",
        DataType::Int64,
        true,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef]).unwrap()
}

#[test]
fn bind_filter_columnar_mask_matches_per_event() {
    let values: Vec<Option<i64>> = (0..1000).map(Some).collect();
    let batch = auction_batch(values);
    let exec = bind_executor(Some(auction_mod_123_expr()));

    let mask = exec
        .bind_filter_columnar_mask("b", &batch)
        .expect("columnar mask");
    let events = batch_to_events(&batch);
    assert_eq!(mask.len(), events.len());
    for (row, event) in events.iter().enumerate() {
        assert_eq!(
            mask.value(row),
            exec.event_matches_alias("b", event, None),
            "row {row}"
        );
    }
}

#[test]
fn each_filter_columnar_mask_matches_per_event() {
    let values: Vec<Option<i64>> = (0..1000).map(Some).collect();
    let batch = auction_batch(values);
    let exec = each_executor(Some(auction_mod_123_expr()));

    let mask = exec
        .each_filter_columnar_mask(&batch)
        .expect("columnar mask");
    let events = batch_to_events(&batch);
    assert_eq!(mask.len(), events.len());
    for (row, event) in events.iter().enumerate() {
        // `execute_each` returns Ok(Some(..)) on pass, Ok(None) on rejection.
        let passed = exec.execute_each(event, 0).unwrap().is_some();
        assert_eq!(mask.value(row), passed, "row {row}");
    }
}

#[test]
fn non_columnar_filter_returns_none() {
    let batch = auction_batch(vec![Some(1), Some(2)]);

    let bind = bind_executor(Some(func_filter()));
    assert!(bind.bind_filter_columnar_mask("b", &batch).is_none());

    let each = each_executor(Some(func_filter()));
    assert!(each.each_filter_columnar_mask(&batch).is_none());
}

/// contains 函数 filter：非列式（走解释器），Str 字段必须命中正确行。
#[test]
fn contains_filter_matches_per_event_interpreted() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, false),
        Field::new("event_time", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "10.0.0.1", "10.0.0.2", "9.0.0.3", "10.0.0.4",
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        ],
    )
    .unwrap();
    let filter = Expr::FuncCall {
        qualifier: None,
        name: "contains".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".into())),
            Expr::StringLit("0.0".into()),
        ],
    };
    let exec = bind_executor(Some(filter));
    assert!(
        exec.bind_filter_columnar_mask("b", &batch).is_none(),
        "contains 应非列式（返回 None mask）"
    );
    let events = batch_to_events(&batch);
    // 全部含子串 "0.0"：10.0.0.1 / 10.0.0.2 / 9.0.0.3 / 10.0.0.4 均命中。
    let expect = [true, true, true, true];
    for (row, ev) in events.iter().enumerate() {
        assert_eq!(
            exec.event_matches_alias("b", ev, None),
            expect[row],
            "row {row}: sip={} contains \"0.0\"",
            ev.field_value_str("sip")
        );
    }
}

#[test]
fn no_filter_returns_none() {
    let batch = auction_batch(vec![Some(1), Some(2)]);

    let bind = bind_executor(None);
    assert!(bind.bind_filter_columnar_mask("b", &batch).is_none());

    let each = each_executor(None);
    assert!(each.each_filter_columnar_mask(&batch).is_none());
}
