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

/// contains 函数 filter：字面量形态列式（mask 与逐行解释器逐位一致）。
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
    let mask = exec
        .bind_filter_columnar_mask("b", &batch)
        .expect("contains 字面量形态应列式（返回 Some mask）");
    let events = batch_to_events(&batch);
    // 全部含子串 "0.0"：10.0.0.1 / 10.0.0.2 / 9.0.0.3 / 10.0.0.4 均命中。
    let expect = [true, true, true, true];
    for (row, ev) in events.iter().enumerate() {
        assert_eq!(mask.value(row), expect[row], "row {row}: 列式 mask");
        assert_eq!(
            exec.event_matches_alias("b", ev, None),
            expect[row],
            "row {row}: sip={} contains \"0.0\"",
            ev.field_value_str("sip")
        );
    }
}

/// cidr_match 列式：字面量子网编译期解析一次，mask 与逐行解释器逐位一致。
#[test]
fn cidr_match_columnar_mask_matches_per_event() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, true),
        Field::new("event_time", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("10.1.2.3"),   // 命中 10/8
                Some("11.0.0.1"),   // 不命中
                None,               // null → 不匹配
                Some("fe80::1"),    // v6 vs v4 网段 → 版本不一致不匹配
                Some("172.31.0.1"), // 不命中
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )
    .unwrap();
    let filter = Expr::FuncCall {
        qualifier: None,
        name: "cidr_match".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".into())),
            Expr::StringLit("10.0.0.0/8".into()),
        ],
    };
    let exec = bind_executor(Some(filter));
    let mask = exec
        .bind_filter_columnar_mask("b", &batch)
        .expect("cidr_match 应列式（返回 Some mask）");
    let events = batch_to_events(&batch);
    let expect = [true, false, false, false, false];
    for (row, ev) in events.iter().enumerate() {
        assert_eq!(mask.value(row), expect[row], "row {row}");
        assert_eq!(
            exec.event_matches_alias("b", ev, None),
            expect[row],
            "row {row}: sip={}",
            ev.field_value_str("sip")
        );
    }
}

/// cidr_match 与非列式子表达组合 → 整体回落解释器，仍逐位正确。
#[test]
fn cidr_match_non_literal_subnet_falls_back_interpreted() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sip", DataType::Utf8, false),
        Field::new("event_time", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["10.1.2.3", "11.0.0.1"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .unwrap();
    // 子网来自字段（动态）→ 非列式。
    let filter = Expr::FuncCall {
        qualifier: None,
        name: "cidr_match".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".into())),
            Expr::Field(FieldRef::Simple("subnet".into())),
        ],
    };
    let exec = bind_executor(Some(filter));
    assert!(exec.bind_filter_columnar_mask("b", &batch).is_none());
    // 解释路径：subnet 字段缺失 → None（不匹配）。
    let events = batch_to_events(&batch);
    for ev in &events {
        assert!(!exec.event_matches_alias("b", ev, None));
    }
}

/// regex_match 列式：字面量 pattern 编译期编译一次，mask 与逐行解释器逐位一致。
#[test]
fn regex_match_columnar_mask_matches_per_event() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("action", DataType::Utf8, true),
        Field::new("event_time", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("failed_login"), // 命中
                Some("success"),      // 不命中
                None,                 // null → 不匹配
                Some("fail fast"),    // 命中
                Some("FAILED"),       // 大小写敏感 → 不命中
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )
    .unwrap();
    let filter = Expr::FuncCall {
        qualifier: None,
        name: "regex_match".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("action".into())),
            Expr::StringLit("fail.*".into()),
        ],
    };
    let exec = bind_executor(Some(filter));
    let mask = exec
        .bind_filter_columnar_mask("b", &batch)
        .expect("regex_match 应列式（返回 Some mask）");
    let events = batch_to_events(&batch);
    let expect = [true, false, false, true, false];
    for (row, ev) in events.iter().enumerate() {
        assert_eq!(mask.value(row), expect[row], "row {row}");
        assert_eq!(
            exec.event_matches_alias("b", ev, None),
            expect[row],
            "row {row}: action={}",
            ev.field_value_str("action")
        );
    }
}

/// contains/startswith/endswith 列式：mask 与逐行解释器逐位一致（字面量与字段
/// needle 两种形态）。
#[test]
fn str_search_columnar_mask_matches_per_event() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("action", DataType::Utf8, true),
        Field::new("pattern", DataType::Utf8, true),
        Field::new("event_time", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("failed_login"), // contains/startswith "fail" 命中
                Some("login_fail"),   // contains 命中，startswith 不命中
                Some("success"),      // 都不命中
                None,                 // null
                Some("FAILED"),       // 大小写敏感
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("fail"),
                Some("login"),
                Some("fail"),
                Some("fail"),
                None,
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )
    .unwrap();
    let mk = |name: &str, needle: Expr| Expr::FuncCall {
        qualifier: None,
        name: name.into(),
        args: vec![Expr::Field(FieldRef::Simple("action".into())), needle],
    };
    let cases: &[(&str, Expr, [bool; 5])] = &[
        (
            "contains",
            mk("contains", Expr::StringLit("fail".into())),
            [true, true, false, false, false],
        ),
        (
            "startswith",
            mk("startswith", Expr::StringLit("fail".into())),
            [true, false, false, false, false],
        ),
        (
            "endswith",
            mk("endswith", Expr::StringLit("fail".into())),
            [false, true, false, false, false],
        ),
        (
            "contains_field_needle",
            mk("contains", Expr::Field(FieldRef::Simple("pattern".into()))),
            [true, true, false, false, false],
        ),
    ];
    for (label, filter, expect) in cases {
        let exec = bind_executor(Some(filter.clone()));
        let mask = exec
            .bind_filter_columnar_mask("b", &batch)
            .expect("字符串搜索函数应列式（返回 Some mask）");
        let events = batch_to_events(&batch);
        for (row, ev) in events.iter().enumerate() {
            assert_eq!(mask.value(row), expect[row], "{label} row {row}");
            assert_eq!(
                exec.event_matches_alias("b", ev, None),
                expect[row],
                "{label} row {row}: action={}",
                ev.field_value_str("action")
            );
        }
    }
}

/// 编译树缓存：同一 executor 跨 batch 复用（batch 无关列索引），schema drift
/// 的 batch 重新编译仍正确。
#[test]
fn compiled_guard_cache_reuses_across_batches() {
    let schema_of = |extra: bool| {
        let mut fields = vec![
            Field::new("sip", DataType::Utf8, true),
            Field::new("event_time", DataType::Int64, false),
        ];
        if extra {
            fields.push(Field::new("extra", DataType::Utf8, true));
        }
        Arc::new(Schema::new(fields))
    };
    let mk_batch = |schema: Arc<Schema>, rows: Vec<Option<&str>>| {
        let n = rows.len();
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(rows)) as ArrayRef,
            Arc::new(Int64Array::from(vec![1; n])) as ArrayRef,
        ];
        if schema.fields().len() > 2 {
            cols.push(Arc::new(StringArray::from(vec![Some("x"); n])) as ArrayRef);
        }
        RecordBatch::try_new(schema, cols).unwrap()
    };
    let filter = Expr::FuncCall {
        qualifier: None,
        name: "cidr_match".into(),
        args: vec![
            Expr::Field(FieldRef::Simple("sip".into())),
            Expr::StringLit("10.0.0.0/8".into()),
        ],
    };
    let exec = bind_executor(Some(filter));

    // 同 schema 的两个 batch：编译树缓存复用，结果逐位正确。
    let b1 = mk_batch(schema_of(false), vec![Some("10.1.1.1"), Some("11.2.3.4")]);
    let b2 = mk_batch(
        schema_of(false),
        vec![Some("10.9.9.9"), Some("192.168.1.1")],
    );
    let m1 = exec.bind_filter_columnar_mask("b", &b1).expect("列式 mask");
    let m2 = exec.bind_filter_columnar_mask("b", &b2).expect("列式 mask");
    assert!(m1.value(0) && !m1.value(1), "b1: 10/8 命中与否");
    assert!(m2.value(0) && !m2.value(1), "b2: 10/8 命中与否");

    // schema drift（多一列）→ 指纹变化 → 重新编译，仍正确。
    let b3 = mk_batch(schema_of(true), vec![Some("10.0.0.1"), Some("172.16.0.1")]);
    let m3 = exec.bind_filter_columnar_mask("b", &b3).expect("列式 mask");
    assert!(m3.value(0) && !m3.value(1), "b3: schema drift 后仍正确");
}

#[test]
fn no_filter_returns_none() {
    let batch = auction_batch(vec![Some(1), Some(2)]);

    let bind = bind_executor(None);
    assert!(bind.bind_filter_columnar_mask("b", &batch).is_none());

    let each = each_executor(None);
    assert!(each.each_filter_columnar_mask(&batch).is_none());
}
