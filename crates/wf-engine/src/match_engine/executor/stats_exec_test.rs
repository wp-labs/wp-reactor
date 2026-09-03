//! StatsExecutor 单元测试。本文件收口共享 harness（import/基础断言与计划构建
//! 辅助）并按主题分派到兄弟测试子模块（`#[path]` 相对本文件目录，机制同
//! compile_tests.rs；子模块 `use super::*` 引用本文件项）：
//! - `stats_exec_basic`: P1 空键 accumulator + 输入分片归并;
//! - `stats_exec_columnar`: Q15 与列式执行段对拍 + 批级 mask/time 缓存;
//! - `stats_exec_grouped`: group by / 复合键 / 分片行子集;
//! - `stats_exec_last_top`: last/top / row fields / close 键字段注入;
//! - `stats_exec_state`: 窗口状态内存 guard / SoA 桶。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    BooleanArray, Date32Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{BinOp, Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

use crate::match_engine::Value;
use crate::match_engine::cep::ScopeKey;
use crate::match_engine::executor::stats_exec::{
    StatsAccum, StatsBucketAccs, StatsExecutor, StatsMaskCache, TopEntry,
};
use crate::match_engine::executor::{
    NumericSoALayout, accumulate_column_row, accumulate_soa, measure_values_soa,
};
use wf_cep::rows::{RowFieldLayout, RowFields};

fn num(n: f64) -> Value {
    Value::Number(n)
}

fn str_val(s: &str) -> Value {
    Value::Str(s.into())
}

fn simple_plan(measures: Vec<StatsMeasurePlan>) -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
        keys: vec![],
        output_shape: StatsOutputShapePlan::Rows,
        measures,
        tracked_bind_fields: HashMap::new(),
    }
}

fn count_measure(label: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Count,
        field: None,
        arg: None,
    }
}

fn distinct_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::DistinctCount,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn sum_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Sum,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn avg_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Avg,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn last_measure(label: &str, field: &str) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Last,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: None,
    }
}

fn top_measure(label: &str, field: &str, n: u64) -> StatsMeasurePlan {
    StatsMeasurePlan {
        label: label.into(),
        source_alias: "b".into(),
        where_expr: None,
        agg: StatsAggPlan::Top,
        field: Some(FieldRef::Qualified("b".into(), field.into())),
        arg: Some(n),
    }
}

fn row(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn extract(row: &HashMap<String, Value>, name: &str) -> Option<Value> {
    row.get(name).cloned()
}

/// 行字段列数组按名取值（P5 紧凑化测试辅助; `names` = 提取列序）。
fn row_val(row: &std::sync::Arc<RowFields>, names: &[String], name: &str) -> Option<Value> {
    names
        .iter()
        .position(|n| n == name)
        .and_then(|i| row.value_at(i))
}

/// batch schema 字段名排序——与执行器 None 子集提取列序一致（行式/列式同序）。
fn sorted_schema_names(batch: &RecordBatch) -> Vec<String> {
    let mut ns: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    ns.sort();
    ns
}

/// rows_to_batch 的排序字段名（price/bidder/auction → auction, bidder, price）。
fn sorted_bid_names() -> Vec<String> {
    ["auction", "bidder", "price"]
        .into_iter()
        .map(String::from)
        .collect()
}

/// 全字段子集（等价 None 全列, 但带确定列序——last/top 度量值提取需要子集,
/// 生产经 spawn 恒有子集）。
fn full_bid_subset() -> Arc<HashSet<String>> {
    Arc::new(sorted_bid_names().into_iter().collect())
}

// ---- 跨主题共享辅助（供上述兄弟测试子模块经 `use super::*` 引用） ----

fn price_field() -> Expr {
    Expr::Field(FieldRef::Qualified("b".into(), "price".into()))
}

fn price_lt(threshold: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::Lt,
        left: Box::new(price_field()),
        right: Box::new(Expr::Number(threshold)),
    }
}

fn price_ge(threshold: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::Ge,
        left: Box::new(price_field()),
        right: Box::new(Expr::Number(threshold)),
    }
}

fn price_range(lo: f64, hi: f64) -> Expr {
    Expr::BinOp {
        op: BinOp::And,
        left: Box::new(price_ge(lo)),
        right: Box::new(price_lt(hi)),
    }
}

/// 行 → Int64 列 RecordBatch（price/bidder/auction, 对齐 nexmark 数据; null 保留）。
fn rows_to_batch(rows: &[HashMap<String, Value>]) -> RecordBatch {
    fn i64_of(row: &HashMap<String, Value>, name: &str) -> Option<i64> {
        match row.get(name) {
            Some(Value::Number(n)) => Some(*n as i64),
            _ => None,
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
    ]));
    let price: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "price")).collect();
    let bidder: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "bidder")).collect();
    let auction: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "auction")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(price)),
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(auction)),
        ],
    )
    .expect("batch")
}

/// 同 rows_to_batch, 但指定行（`null_price_rows` 集合中的行号）price 列为 null——
/// 列式 last 字段缺失语义的模拟（行式 = 缺 price 键）。
fn rows_to_batch_with_null_price(rows: &[HashMap<String, Value>]) -> RecordBatch {
    fn i64_of(row: &HashMap<String, Value>, name: &str) -> Option<i64> {
        match row.get(name) {
            Some(Value::Number(n)) => Some(*n as i64),
            _ => None,
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
    ]));
    let price: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "price")).collect();
    let bidder: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "bidder")).collect();
    let auction: Vec<Option<i64>> = rows.iter().map(|r| i64_of(r, "auction")).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(price)),
            Arc::new(Int64Array::from(bidder)),
            Arc::new(Int64Array::from(auction)),
        ],
    )
    .expect("batch")
}

/// 带键的 plan（keys 为桶键表达式）。
fn keyed_plan(keys: Vec<Expr>, measures: Vec<StatsMeasurePlan>) -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
        keys,
        output_shape: StatsOutputShapePlan::Rows,
        measures,
        tracked_bind_fields: HashMap::new(),
    }
}

fn field_key(alias: &str, name: &str) -> Expr {
    Expr::Field(FieldRef::Qualified(alias.into(), name.into()))
}

fn auction_price_rows(pairs: &[(f64, f64)]) -> Vec<HashMap<String, Value>> {
    pairs
        .iter()
        .map(|(a, p)| row(&[("auction", num(*a)), ("price", num(*p))]))
        .collect()
}

/// q17 形状计划: 带 auction 键 + 8 度量（total/r1/r2/r3 分档 count + min/max/avg/sum）。
fn q17_shape_plan() -> StatsPlan {
    let mk =
        |label: &str, agg: StatsAggPlan, field: Option<&str>, w: Option<Expr>| StatsMeasurePlan {
            label: label.into(),
            source_alias: "b".into(),
            where_expr: w,
            agg,
            field: field.map(|f| FieldRef::Qualified("b".into(), f.into())),
            arg: None,
        };
    keyed_plan(
        vec![field_key("b", "auction")],
        vec![
            mk("total", StatsAggPlan::Count, None, None),
            mk("r1", StatsAggPlan::Count, None, Some(price_lt(10_000.0))),
            mk(
                "r2",
                StatsAggPlan::Count,
                None,
                Some(price_range(10_000.0, 1_000_000.0)),
            ),
            mk("r3", StatsAggPlan::Count, None, Some(price_ge(1_000_000.0))),
            mk("minp", StatsAggPlan::Min, Some("price"), None),
            mk("maxp", StatsAggPlan::Max, Some("price"), None),
            mk("avgp", StatsAggPlan::Avg, Some("price"), None),
            mk("sump", StatsAggPlan::Sum, Some("price"), None),
        ],
    )
}

// 测试按主题拆分为兄弟子模块（`#[path]` 相对本文件目录）。
#[path = "stats_exec_basic.rs"]
mod stats_exec_basic;
#[path = "stats_exec_columnar.rs"]
mod stats_exec_columnar;
#[path = "stats_exec_grouped.rs"]
mod stats_exec_grouped;
#[path = "stats_exec_last_top.rs"]
mod stats_exec_last_top;
#[path = "stats_exec_state.rs"]
mod stats_exec_state;
