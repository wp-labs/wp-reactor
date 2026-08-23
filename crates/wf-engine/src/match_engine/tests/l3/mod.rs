//! L3 feature tests: conv transformations.

mod conv;
mod hop;
mod session;

use std::time::Duration;

use wf_lang::ast::{BinOp, CloseMode, Expr, FieldRef};
use wf_lang::plan::{ConvChainPlan, ConvOpPlan, ConvPlan, ExprPlan, SortKeyPlan};

use crate::match_engine::match_engine::{
    CepStateMachine, CloseOutput, CloseReason, EngineHashMap, StepData, StepResult, Value,
};

use super::helpers::*;
