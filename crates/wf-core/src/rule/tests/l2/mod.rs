//! L2 feature tests: limits, key_map, window.has(), baseline(), joins.

mod baseline;
mod execute;
mod expr;
mod guards;
mod joins;
mod keymap;
mod limits;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use wf_lang::ast::{Expr, FieldRef, JoinMode};
use wf_lang::plan::{
    ExceedAction, JoinCondPlan, JoinPlan, KeyMapPlan, LimitsPlan, MatchPlan, WindowSpec,
};

use crate::rule::RuleExecutor;
use crate::rule::match_engine::{
    CepStateMachine, MatchedContext, StepData, StepResult, Value, WindowLookup,
};

use super::helpers::*;

// ---------------------------------------------------------------------------
// Mock WindowLookup
// ---------------------------------------------------------------------------

struct MockWindowLookup {
    field_values: HashMap<(String, String), HashSet<String>>,
    snapshots: HashMap<String, Vec<HashMap<String, Value>>>,
    timestamped_snapshots: HashMap<String, Vec<(i64, HashMap<String, Value>)>>,
}

impl MockWindowLookup {
    fn new() -> Self {
        Self {
            field_values: HashMap::new(),
            snapshots: HashMap::new(),
            timestamped_snapshots: HashMap::new(),
        }
    }

    #[allow(dead_code)]
    fn add_field_values(&mut self, window: &str, field: &str, values: Vec<&str>) {
        self.field_values.insert(
            (window.to_string(), field.to_string()),
            values.into_iter().map(|s| s.to_string()).collect(),
        );
    }

    fn add_snapshot(&mut self, window: &str, rows: Vec<HashMap<String, Value>>) {
        self.snapshots.insert(window.to_string(), rows);
    }

    fn add_timestamped_snapshot(
        &mut self,
        window: &str,
        rows: Vec<(i64, HashMap<String, Value>)>,
    ) {
        self.timestamped_snapshots
            .insert(window.to_string(), rows);
    }
}

impl WindowLookup for MockWindowLookup {
    fn snapshot_field_values(&self, window: &str, field: &str) -> Option<HashSet<String>> {
        self.field_values
            .get(&(window.to_string(), field.to_string()))
            .cloned()
    }

    fn snapshot(&self, window: &str) -> Option<Vec<HashMap<String, Value>>> {
        self.snapshots.get(window).cloned()
    }

    fn snapshot_with_timestamps(&self, window: &str) -> Option<Vec<(i64, HashMap<String, Value>)>> {
        self.timestamped_snapshots.get(window).cloned()
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Build a HashMap row from key-value pairs.
fn row(fields: Vec<(&str, Value)>) -> HashMap<String, Value> {
    fields
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
}

/// Build a simple snapshot JoinPlan: `join <window> snapshot on left == right`.
fn snapshot_join(window: &str, left_field: &str, right_field: &str) -> JoinPlan {
    JoinPlan {
        right_window: window.to_string(),
        mode: wf_lang::ast::JoinMode::Snapshot,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple(left_field.to_string()),
            right: FieldRef::Simple(right_field.to_string()),
        }],
    }
}

/// Build an asof JoinPlan without a within duration.
fn asof_join(window: &str, left_field: &str, right_field: &str) -> JoinPlan {
    JoinPlan {
        right_window: window.to_string(),
        mode: JoinMode::Asof { within: None },
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple(left_field.to_string()),
            right: FieldRef::Simple(right_field.to_string()),
        }],
    }
}

/// Build an asof JoinPlan with a within duration.
fn asof_join_within(
    window: &str,
    left_field: &str,
    right_field: &str,
    within: Duration,
) -> JoinPlan {
    JoinPlan {
        right_window: window.to_string(),
        mode: JoinMode::Asof {
            within: Some(within),
        },
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple(left_field.to_string()),
            right: FieldRef::Simple(right_field.to_string()),
        }],
    }
}
