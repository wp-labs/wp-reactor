//! Plan C2 equivalence tests: the direct-write on-each emit
//! (`execute_each_direct` → `AlertColumnBuilder` staging) must produce
//! byte-equivalent rows to the record path
//! (`execute_each_with_joins` → `OutputRecord` → `append_record`).

use std::collections::HashMap;
use std::sync::Arc;

use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{EachPlan, YieldField};
use wf_lang::{BaseType, FieldType};

use crate::alert::AlertColumnBuilder;
use crate::match_engine::match_engine::WindowLookup;
use crate::match_engine::{Event, RuleExecutor, Value};

use super::super::helpers::*;

/// Empty lookup — the no-join plans used here never consult windows.
struct EmptyLookup;

impl WindowLookup for EmptyLookup {
    fn snapshot_field_values(
        &self,
        _window: &str,
        _field: &str,
    ) -> Option<std::collections::HashSet<String>> {
        None
    }
    fn snapshot(&self, _window: &str) -> Option<Vec<HashMap<String, Value>>> {
        None
    }
    fn snapshot_with_timestamps(&self, _window: &str) -> Option<Vec<(i64, HashMap<String, Value>)>>
    {
        None
    }
}

fn each_plan_rule() -> RuleExecutor {
    let mut plan = simple_rule_plan(
        "q1_pass",
        simple_plan(vec![], vec![]),
        Expr::Number(42.5),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![
        YieldField {
            name: "auction_id".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "auction_id".into())),
        },
        YieldField {
            name: "price".into(),
            value: Expr::Field(FieldRef::Qualified("e".into(), "price".into())),
        },
    ];
    RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([
            ("auction_id".into(), FieldType::Base(BaseType::Float)),
            ("price".into(), FieldType::Base(BaseType::Float)),
        ]),
    )
}

fn sample_events() -> Vec<Event> {
    vec![
        event(vec![
            ("sip", str_val("10.0.0.1")),
            ("auction_id", num(1000.0)),
            ("price", num(99.5)),
        ]),
        event(vec![
            ("sip", str_val("10.0.0.2")),
            ("auction_id", num(1001.0)),
            ("price", num(79.25)),
        ]),
        // Missing optional `price` → the field must be omitted (#62),
        // exercising the sparse-column layout drift on the direct path.
        event(vec![("sip", str_val("10.0.0.3")), ("auction_id", num(1002.0))]),
    ]
}

#[test]
fn execute_each_direct_matches_record_path_rows() {
    let exec = each_plan_rule();
    let events = sample_events();
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_000_000_000;

    // Record path.
    let mut via_records = AlertColumnBuilder::new(Arc::from("alerts"));
    for ev in &events {
        let record = exec
            .execute_each_with_joins(ev, NANOS, &lookup, &[], NANOS + 1)
            .expect("record path must succeed")
            .expect("filter passes");
        via_records.append_record(&record).unwrap();
    }

    // Direct path.
    let mut via_direct = AlertColumnBuilder::new(Arc::from("alerts"));
    for ev in &events {
        let appended = exec
            .execute_each_direct(ev, NANOS, &lookup, &[], NANOS + 1, &mut via_direct)
            .expect("direct path must succeed");
        assert!(appended, "filter passes on the direct path too");
    }

    let record_batch = via_records.finish();
    let direct_batch = via_direct.finish();
    assert_eq!(record_batch.len(), direct_batch.len());
    for row in 0..record_batch.len() {
        let a = record_batch.iter_data_records().nth(row).unwrap().unwrap();
        let b = direct_batch.iter_data_records().nth(row).unwrap().unwrap();
        assert_eq!(a.items.len(), b.items.len(), "row {row} field count");
        for (fa, fb) in a.items.iter().zip(b.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(fa.get_meta(), fb.get_meta(), "row {row} field meta");
            assert_eq!(fa.get_value(), fb.get_value(), "row {row} field value");
        }
    }
}

#[test]
fn execute_each_direct_filter_rejection_appends_nothing() {
    let mut plan = simple_rule_plan(
        "filtered",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: Some(Expr::BinOp {
            op: wf_lang::ast::BinOp::Eq,
            left: Box::new(Expr::Field(FieldRef::Qualified("e".into(), "sip".into()))),
            right: Box::new(Expr::StringLit("10.0.0.1".into())),
        }),
    });
    let exec = RuleExecutor::new(plan);
    let lookup = EmptyLookup;
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let appended = exec
        .execute_each_direct(
            &event(vec![("sip", str_val("10.9.9.9"))]),
            1_000_000,
            &lookup,
            &[],
            1_000_001,
            &mut builder,
        )
        .unwrap();
    assert!(!appended, "where filter rejects the event");
    assert!(builder.is_empty());
}

#[test]
fn execute_each_direct_surfaces_eval_errors() {
    // Explicit NaN against a Float-typed yield must fail identically to the
    // record path (no partial row committed).
    let mut plan = simple_rule_plan(
        "nan_rule",
        simple_plan(vec![], vec![]),
        Expr::Number(1.0),
        "ip",
        Expr::Field(FieldRef::Qualified("e".into(), "sip".into())),
    );
    plan.binds[0].alias = "e".into();
    plan.each_plan = Some(EachPlan {
        alias: "e".into(),
        filter: None,
    });
    plan.yield_plan.fields = vec![YieldField {
        name: "lat".into(),
        value: Expr::Number(f64::NAN),
    }];
    let exec = RuleExecutor::new_with_yield_field_types(
        plan,
        HashMap::from([("lat".into(), FieldType::Base(BaseType::Float))]),
    );
    let lookup = EmptyLookup;
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    let result = exec.execute_each_direct(
        &event(vec![("sip", str_val("10.0.0.1"))]),
        1_000_000,
        &lookup,
        &[],
        1_000_001,
        &mut builder,
    );
    assert!(result.is_err(), "explicit NaN must fail the direct path");
    assert!(builder.is_empty(), "failed row must not touch columns");
}

#[test]
fn direct_path_wfx_id_matches_record_path() {
    // wfx_id depends on the event fields — the direct path must hash the
    // identical byte stream (spot-check via the row view).
    let exec = each_plan_rule();
    let ev = sample_events().remove(0);
    let lookup = EmptyLookup;
    const NANOS: i64 = 1_750_000_000_123_456_789;

    let record = exec
        .execute_each_with_joins(&ev, NANOS, &lookup, &[], NANOS + 7)
        .unwrap()
        .unwrap();
    let mut builder = AlertColumnBuilder::new(Arc::from("alerts"));
    exec.execute_each_direct(&ev, NANOS, &lookup, &[], NANOS + 7, &mut builder)
        .unwrap();
    let batch = builder.finish();
    let direct_row = batch.iter_data_records().next().unwrap().unwrap();
    let record_row = record.to_data_record().unwrap();
    let direct_id = direct_row
        .items
        .iter()
        .find(|f| f.get_name() == "__wfu_id")
        .unwrap();
    let record_id = record_row
        .items
        .iter()
        .find(|f| f.get_name() == "__wfu_id")
        .unwrap();
    assert_eq!(direct_id.get_value(), record_id.get_value());
}
