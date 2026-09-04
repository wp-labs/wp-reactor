use super::*;
use crate::match_engine::EngineHashMap;
use crate::match_engine::{WFL_FIELD_TYPE_ARRAY, WFL_FIELD_TYPE_METADATA_KEY};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float64Array, Int64Array,
    LargeListArray, ListArray, StringArray, TimestampNanosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;
use std::sync::Arc;
use wf_lang::ast::PathSegment;

use crate::match_engine::cep::{Event, Value, eval_expr, eval_expr_ext};
use crate::match_engine::event_bridge::{batch_to_events, materialize_rows};

fn field(name: &str) -> Expr {
    Expr::Field(FieldRef::Simple(name.to_string()))
}

fn num(n: f64) -> Expr {
    Expr::Number(n)
}

fn bin(op: BinOp, left: Expr, right: Expr) -> Expr {
    Expr::BinOp {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// The interpreted guard semantics: `eval_expr_ext` → `Value::Bool`.
fn interpreted_bool(expr: &Expr, event: &Event) -> bool {
    eval_expr_ext(expr, event, None, &mut EngineHashMap::default())
        .and_then(|v| match v {
            Value::Bool(b) => Some(b),
            _ => None,
        })
        .unwrap_or(false)
}

/// Assert columnar mask == interpreted bool per row.
fn assert_equiv(expr: &Expr, batch: &RecordBatch) {
    let events = batch_to_events(batch);
    let view = ColumnarBatch::from_all_fields(batch);
    let mask = eval_guard_columnar(expr, &view);
    assert_eq!(mask.len(), events.len());
    for (row, event) in events.iter().enumerate() {
        let columnar = mask.value(row);
        let interpreted = interpreted_bool(expr, event);
        assert_eq!(
            columnar, interpreted,
            "row {row}: expr={expr:?} columnar={columnar} interpreted={interpreted}"
        );
    }
}

fn make_batch(
    auction: Vec<Option<i64>>,
    price: Vec<Option<f64>>,
    channel: Vec<Option<&str>>,
    flag: Vec<Option<bool>>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("auction", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("channel", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(auction)) as ArrayRef,
            Arc::new(Float64Array::from(price)) as ArrayRef,
            Arc::new(StringArray::from(channel)) as ArrayRef,
            Arc::new(BooleanArray::from(flag)) as ArrayRef,
        ],
    )
    .unwrap()
}

// ---- 兄弟子模块（2026-09-04 按主题拆分；#[path] 相对本文件目录）----

#[path = "columnar_tests_guard.rs"]
mod columnar_tests_guard;

#[path = "columnar_tests_output.rs"]
mod columnar_tests_output;
