//! Regression tests for P0/P1 bug fixes (22–34).

use std::time::Duration;

use wf_lang::ast::{BinOp, CmpOp, Expr, FieldRef, FieldSelector, Measure};
use wf_lang::plan::{AggPlan, BranchPlan};

use crate::rule::match_engine::{CepStateMachine, CloseReason, StepResult};

use super::helpers::*;

const NANOS_PER_SEC: i64 = 1_000_000_000;

mod aggregation;
mod close;
mod guards;
mod threshold;
