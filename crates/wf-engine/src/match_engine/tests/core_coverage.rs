//! Core-coverage tests: exercise the match_engine building blocks that the
//! feature tests only reach indirectly — Event/Value/JoinKey/ScopeKey type
//! conversions, `WindowLookup` default impls, RecordBatch ↔ Event/JoinRow
//! columnar bridging, `RuleExecutor` query/build interfaces, `execute_joins`
//! mode dispatch (inner/snapshot/asof/anti/interval), close/on-each 收口 paths,
//! conv, and the inline-contract harness failure branches.
//!
//! 测试本体已按主题拆入兄弟 `#[path]` 子模块（同目录文件，2026-09-04；机制见
//! refactor handoff 坑 #24）：本文件保留共享 import 与跨子模块 harness
//! （eq_str_expr / step_data / JoinLookup / join_plan），子模块经 `use super::*`
//! 复用；子模块内独占 helper 随测试迁走。
//! - `core_coverage_types`：types.rs / key.rs / event_bridge.rs（值/键/批表示转换）
//! - `core_coverage_executor`：executor/mod.rs（RuleExecutor 构建/查询接口、列式掩码）
//! - `core_coverage_joins`：executor/context.rs（eval ctx、execute_joins 模式分派）
//! - `core_coverage_close_each`：close_exec / each_exec / conv / contract（收口与 harness）
//!
//! Only test code lives here — no production logic is modified.
use std::sync::Arc;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, LargeListArray,
    ListArray, StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{
    BinOp, Bound, BoundVal, CloseMode, Expr, FieldRef, JoinMode, PathSegment, WithinSpec,
};
use wf_lang::plan::{
    BindPlan, BranchPlan, ConvChainPlan, ConvOpPlan, EachPlan, JoinCondPlan, JoinPlan, LetPlan,
    SeqPlan, SeqSkipPlan, SeqStepPlan, SortKeyPlan, YieldField,
};
use wf_lang::{BaseType, FieldType};

use crate::match_engine::cep::{
    AsofLookup, BindData, CloseOutput, CloseReason, EngineHashMap, Event, FieldSource, JoinKey,
    MACHINE_ID, ScopeKey, StepData, Value, ValueKey, WindowLookup, eval_expr, field_ref_name,
    push_i64_exact_decimal, scope_key_from_values, scope_key_shard_index, value_to_string,
    values_equal,
};
use crate::match_engine::executor::{CloseCtxFields, build_eval_context, execute_joins};
use crate::match_engine::{
    ColumnarEvent, JoinRow, RuleExecutor, TriggerEvent, WFL_FIELD_TYPE_ARRAY,
    WFL_FIELD_TYPE_METADATA_KEY, WFL_FIELD_TYPE_OBJECT, apply_conv, batch_event_time_nanos,
    batch_event_time_nanos_at, batch_raw_ts_nanos, batch_time_col_index, batch_to_events,
    batch_to_events_filtered, batch_to_timestamped_rows, build_field_index, column_scalar_string,
    columnar_join_rows, columnar_timestamped_join_rows, extract_key_simple,
    is_wfl_structured_field, mask_to_indices, materialize_rows, materialize_rows_filtered,
    wfl_structured_field_kind,
};

use super::helpers::*;

// ===========================================================================
// 共享测试 harness（eq_str_expr / step_data / JoinLookup / join_plan 被多个
// 兄弟子模块消费，留此供 `use super::*` 复用）
// ===========================================================================
fn eq_str_expr(field: &str, val: &str) -> Expr {
    Expr::BinOp {
        op: BinOp::Eq,
        left: Box::new(Expr::Field(FieldRef::Simple(field.to_string()))),
        right: Box::new(Expr::StringLit(val.to_string())),
    }
}

fn step_data(label: Option<&str>, measure: f64, field_values: Vec<(&str, Vec<Value>)>) -> StepData {
    StepData {
        satisfied_branch_index: 0,
        label: label.map(|s| s.to_string()),
        measure_value: measure,
        event_first_time_nanos: None,
        event_last_time_nanos: None,
        collected_values: vec![Value::Number(1.0), Value::Number(2.0)],
        field_values: field_values
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    }
}

/// A `WindowLookup` for the `execute_joins` unit tests: `snapshot` /
/// `snapshot_with_timestamps` back the default `join_lookup` / `asof_candidates`
/// impls; `asof_fast` drives the single-condition O(1) fast path.
///
/// `rows`/`ts_rows` 按字符串键索引（多键 join 用）；`asof_fast` 命中时
/// `join_lookup_asof` 走 O(1) 快路径。
type JoinLookupRows = HashMap<String, Vec<HashMap<String, Value>>>;
type JoinLookupTsRows = HashMap<String, Vec<(i64, HashMap<String, Value>)>>;
struct JoinLookup {
    rows: JoinLookupRows,
    ts_rows: JoinLookupTsRows,
    asof_fast: Option<AsofOutcome>,
}

enum AsofOutcome {
    Hit(HashMap<String, Value>),
    Miss,
}

impl JoinLookup {
    fn new() -> Self {
        Self {
            rows: HashMap::new(),
            ts_rows: HashMap::new(),
            asof_fast: None,
        }
    }
    fn row(fields: Vec<(&str, Value)>) -> HashMap<String, Value> {
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }
    fn add_row(&mut self, window: &str, fields: Vec<(&str, Value)>) {
        self.rows
            .entry(window.to_string())
            .or_default()
            .push(Self::row(fields));
    }
    fn add_ts_row(&mut self, window: &str, ts: i64, fields: Vec<(&str, Value)>) {
        self.ts_rows
            .entry(window.to_string())
            .or_default()
            .push((ts, Self::row(fields)));
    }
    fn to_join_row(map: HashMap<String, Value>) -> JoinRow {
        JoinRow::Event(Arc::new(Event {
            fields: map.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }))
    }
}

impl WindowLookup for JoinLookup {
    fn snapshot_field_values(&self, _w: &str, _f: &str) -> Option<HashSet<String>> {
        None
    }
    fn snapshot(&self, window: &str) -> Option<Vec<JoinRow>> {
        self.rows
            .get(window)
            .map(|rows| rows.iter().cloned().map(Self::to_join_row).collect())
    }
    fn snapshot_with_timestamps(&self, window: &str) -> Option<Vec<(i64, JoinRow)>> {
        self.ts_rows.get(window).map(|rows| {
            rows.iter()
                .map(|(ts, r)| (*ts, Self::to_join_row(r.clone())))
                .collect()
        })
    }
    fn asof_lookup_max(
        &self,
        _w: &str,
        _kf: &str,
        _k: &Value,
        _event_time_nanos: i64,
        _within: Option<&Duration>,
    ) -> AsofLookup {
        match &self.asof_fast {
            Some(AsofOutcome::Hit(row)) => AsofLookup::Hit(Self::to_join_row(row.clone())),
            Some(AsofOutcome::Miss) => AsofLookup::Miss,
            None => AsofLookup::Fallback,
        }
    }
}

fn join_plan(mode: JoinMode, window: &str, left: &str, right: &str) -> JoinPlan {
    JoinPlan {
        right_window: window.to_string(),
        mode,
        conds: vec![JoinCondPlan {
            left: FieldRef::Simple(left.to_string()),
            right: FieldRef::Simple(right.to_string()),
        }],
        within: None,
        reduce: None,
        emit_at: None,
    }
}

// ---- 兄弟子模块（2026-09-04 按主题拆分；#[path] 相对本文件目录，机制同 compile_tests.rs / coverage_r4.rs）----
#[path = "core_coverage_close_each.rs"]
mod core_coverage_close_each;
#[path = "core_coverage_executor.rs"]
mod core_coverage_executor;
#[path = "core_coverage_joins.rs"]
mod core_coverage_joins;
#[path = "core_coverage_types.rs"]
mod core_coverage_types;
