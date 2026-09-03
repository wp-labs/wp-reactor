use std::collections::HashMap;

use wf_config::OutputConfig;
use wf_lang::ast::{BinOp, Expr, FieldRef, ObjectItem, PathSegment, SystemVar};
use wf_lang::plan::{EachPlan, StepPlan, YieldField};
use wf_lang::wfu_meta::WfuMetaField;
use wf_lang::{BaseType, FieldDef, FieldType, WindowSchema};

use crate::match_engine::EngineHashMap;
use crate::match_engine::Value;
use crate::match_engine::cep::{BindData, CloseOutput, CloseReason, StepData};
use crate::match_engine::{RuleExecutor, RuleExecutorOptions};

use super::super::helpers::*;
use super::helpers::{default_match_plan, default_matched_context};

#[path = "each_match_yield_tests.rs"]
mod each_match_yield_tests;

#[path = "close_yield_tests.rs"]
mod close_yield_tests;

#[path = "stat_evidence_yield_tests.rs"]
mod stat_evidence_yield_tests;

#[path = "nested_path_yield_tests.rs"]
mod nested_path_yield_tests;
