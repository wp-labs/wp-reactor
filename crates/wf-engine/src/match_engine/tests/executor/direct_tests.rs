//! Plan C2 equivalence tests: the direct-write on-each emit
//! (`execute_each_direct` → `AlertColumnBuilder` staging) must produce
//! byte-equivalent rows to the record path
//! (`execute_each_with_joins` → `OutputRecord` → `append_record`).
use std::sync::{Arc, Mutex};

use std::collections::HashMap;

use wf_lang::ast::{BinOp, Expr, FieldRef, JoinMode};
use wf_lang::plan::{EachPlan, JoinCondPlan, JoinPlan, YieldField};
use wf_lang::{BaseType, FieldType};

use crate::alert::AlertColumnBuilder;
use crate::match_engine::cep::WindowLookup;
use crate::match_engine::{Event, RuleExecutor};

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
    fn snapshot(&self, _window: &str) -> Option<Vec<crate::match_engine::JoinRow>> {
        None
    }
    fn snapshot_with_timestamps(
        &self,
        _window: &str,
    ) -> Option<Vec<(i64, crate::match_engine::JoinRow)>> {
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

/// Row-view comparison helper: two finished batches must expose identical
/// `DataRecord` row views.
fn assert_batches_equal_rows(
    a: &crate::alert::AlertColumnBatch,
    b: &crate::alert::AlertColumnBatch,
) {
    assert_eq!(a.len(), b.len(), "row count");
    for row in 0..a.len() {
        let ra = a.iter_data_records().nth(row).unwrap().unwrap();
        let rb = b.iter_data_records().nth(row).unwrap().unwrap();
        assert_eq!(ra.items.len(), rb.items.len(), "row {row} field count");
        for (fa, fb) in ra.items.iter().zip(rb.items.iter()) {
            assert_eq!(fa.get_name(), fb.get_name(), "row {row} field name");
            assert_eq!(fa.get_meta(), fb.get_meta(), "row {row} field meta");
            assert_eq!(fa.get_value(), fb.get_value(), "row {row} field value");
        }
    }
}

#[path = "direct_join_tests.rs"]
mod direct_join_tests;

#[path = "direct_row_tests.rs"]
mod direct_row_tests;

#[path = "direct_columnar_tests.rs"]
mod direct_columnar_tests;
