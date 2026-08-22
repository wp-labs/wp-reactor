//! Targeted coverage-fill for `stats_exec.rs` helper branches that the main
//! battery (`stats_exec_test.rs`) does not reach: `DistinctKey::from_f64`
//! normalization edges, bucket-unit parsing, unknown bucket-key functions,
//! top-N non-numeric keys, the `KeyColumn::Other` columnar fallback, and
//! out-of-range row-subset indices.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wf_lang::ast::{Expr, FieldRef};
use wf_lang::plan::{StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan, StatsPlan, WindowSpec};

use crate::match_engine::Value;
use crate::match_engine::executor::stats_exec::{DistinctKey, StatsExecutor};

fn num(n: f64) -> Value {
    Value::Number(n)
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

fn str_val(s: &str) -> Value {
    Value::Str(s.into())
}

fn plan(keys: Vec<Expr>, measures: Vec<StatsMeasurePlan>) -> StatsPlan {
    StatsPlan {
        window_spec: WindowSpec::Fixed(std::time::Duration::from_secs(1800)),
        keys,
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

fn field_key(alias: &str, name: &str) -> Expr {
    Expr::Field(FieldRef::Qualified(alias.into(), name.into()))
}

#[test]
fn stats_distinct_key_from_f64_normalization_edges() {
    // Integer-valued and < 2^53 → Int.
    assert_eq!(DistinctKey::from_f64(0.0), DistinctKey::Int(0));
    assert_eq!(DistinctKey::from_f64(-0.0), DistinctKey::Int(0));
    assert_eq!(DistinctKey::from_f64(5.0), DistinctKey::Int(5));
    assert_eq!(DistinctKey::from_f64(-7.0), DistinctKey::Int(-7));
    // NaN → canonical Float bits.
    assert_eq!(
        DistinctKey::from_f64(f64::NAN),
        DistinctKey::Float(f64::NAN.to_bits())
    );
    // ≥ 2^53 integers stay Float (f64 can't hold them exactly).
    let two_pow_53 = 9_007_199_254_740_992.0;
    assert_eq!(
        DistinctKey::from_f64(two_pow_53),
        DistinctKey::Float(two_pow_53.to_bits())
    );
    // Fractional → Float bits.
    assert_eq!(
        DistinctKey::from_f64(1.5),
        DistinctKey::Float(1.5f64.to_bits())
    );
    // String / int constructors.
    assert_eq!(DistinctKey::from_i64(42), DistinctKey::Int(42));
    assert_eq!(DistinctKey::from_str("x"), DistinctKey::Str("x".into()));
}

#[test]
fn stats_bucket_unit_parsing_and_unknown_keys() {
    // bucket(..., 'minute' / 'second') parse; an unknown unit drops the row.
    let bucket_key = |unit: &str| Expr::FuncCall {
        qualifier: None,
        name: "bucket".into(),
        args: vec![field_key("b", "ts"), Expr::StringLit(unit.into())],
    };
    let minute = 60_000_000_000i64;
    let second = 1_000_000_000i64;
    let mut exec = StatsExecutor::new(plan(vec![bucket_key("minute")], vec![count_measure("n")]));
    exec.process_rows(
        &[
            row(&[("ts", num(minute as f64 + 1.0))]),
            row(&[("ts", num(2.0 * minute as f64))]),
        ],
        extract,
    );
    assert_eq!(
        exec.final_measure_values_by_bucket(),
        vec![
            (crate::match_engine::ScopeKey::Int(minute), vec![1.0]),
            (crate::match_engine::ScopeKey::Int(2 * minute), vec![1.0]),
        ],
        "minute 桶聚合"
    );

    let mut exec = StatsExecutor::new(plan(vec![bucket_key("second")], vec![count_measure("n")]));
    exec.process_rows(&[row(&[("ts", num(second as f64 + 1.0))])], extract);
    assert_eq!(
        exec.final_measure_values_by_bucket(),
        vec![(crate::match_engine::ScopeKey::Int(second), vec![1.0])],
        "second 桶聚合"
    );

    // Unknown unit → key eval fails → row skipped entirely.
    let mut exec = StatsExecutor::new(plan(
        vec![bucket_key("fortnight")],
        vec![count_measure("n")],
    ));
    exec.process_rows(&[row(&[("ts", num(1.0))])], extract);
    assert!(exec.final_measure_values_by_bucket().is_empty());

    // Unknown function as bucket key → row skipped.
    let bogus_key = Expr::FuncCall {
        qualifier: None,
        name: "mystery".into(),
        args: vec![field_key("b", "ts")],
    };
    let mut exec = StatsExecutor::new(plan(vec![bogus_key], vec![count_measure("n")]));
    exec.process_rows(&[row(&[("ts", num(1.0))])], extract);
    assert!(exec.final_measure_values_by_bucket().is_empty());

    // A bare non-field, non-func expression as the key (e.g. a literal number)
    // → row skipped.
    let mut exec = StatsExecutor::new(plan(vec![Expr::Number(1.0)], vec![count_measure("n")]));
    exec.process_rows(&[row(&[("ts", num(1.0))])], extract);
    assert!(exec.final_measure_values_by_bucket().is_empty());

    // tier() with a non-number bound → key eval fails → row skipped.
    let tier_key = Expr::FuncCall {
        qualifier: None,
        name: "tier".into(),
        args: vec![field_key("b", "price"), Expr::StringLit("high".into())],
    };
    let mut exec = StatsExecutor::new(plan(vec![tier_key], vec![count_measure("n")]));
    exec.process_rows(&[row(&[("price", num(5.0))])], extract);
    assert!(exec.final_measure_values_by_bucket().is_empty());
}

#[test]
fn stats_top_non_numeric_key_skips_row() {
    // top's sort key must be numeric — a string key value is skipped
    // (matching the sum/skip-non-numeric convention).
    let plan = plan(
        vec![field_key("b", "auction")],
        vec![top_measure("top_price", "price", 3)],
    );
    let mut exec = StatsExecutor::new(plan.clone());
    exec.process_rows(
        &[
            row(&[("auction", num(1.0)), ("price", str_val("high"))]),
            row(&[("auction", num(1.0)), ("price", str_val("low"))]),
        ],
        extract,
    );
    let buckets = exec.close_window_by_bucket_rows();
    assert_eq!(buckets.len(), 1, "桶仍创建（count 驱动）");
    assert!(buckets[0].measures[0].is_empty(), "非数值 top 键不产生条目");

    // Same shape but numeric keys do produce entries.
    exec = StatsExecutor::new(plan);
    exec.process_rows(
        &[
            row(&[("auction", num(1.0)), ("price", num(5.0))]),
            row(&[("auction", num(1.0)), ("price", num(3.0))]),
        ],
        extract,
    );
    let entries = &exec.close_window_by_bucket_rows()[0].measures[0];
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].measure_value, 5.0);
    assert_eq!(entries[1].measure_value, 3.0);
}

#[test]
fn stats_final_measure_values_empty_window() {
    // Empty window: avg falls back to 0 (count==0), min/max unwrap to 0.
    let measures = vec![
        count_measure("n"),
        StatsMeasurePlan {
            label: "avg".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Avg,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: None,
        },
        StatsMeasurePlan {
            label: "min".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Min,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: None,
        },
        StatsMeasurePlan {
            label: "max".into(),
            source_alias: "b".into(),
            where_expr: None,
            agg: StatsAggPlan::Max,
            field: Some(FieldRef::Qualified("b".into(), "price".into())),
            arg: None,
        },
    ];
    let mut exec = StatsExecutor::new(plan(vec![], measures));
    assert_eq!(exec.final_measure_values(), vec![0.0, 0.0, 0.0, 0.0]);
    // close_window_by_bucket on an empty window returns the seeded empty bucket.
    let buckets = exec.close_window_by_bucket();
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0].1, vec![0.0, 0.0, 0.0, 0.0]);
}

#[test]
fn stats_columnar_other_key_column_falls_back() {
    // A Struct key column is not in the fast key lanes (`Int64`/`Timestamp`/
    // `Float64`/`Utf8`/`Boolean`) → `KeyColumn::Other` → the per-row
    // `scope_key_from_column` fallback. Struct columns extract to `Object` →
    // `ScopeKey::Str("[object]")`, so every row lands in one bucket.
    use arrow::array::{Array as _, StructArray};
    let struct_col = StructArray::from(vec![(
        Arc::new(Field::new("x", DataType::Int64, false)),
        Arc::new(Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
    )]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("day", struct_col.data_type().clone(), true),
        Field::new("price", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(struct_col) as arrow::array::ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();
    let exec_plan = plan(vec![field_key("b", "day")], vec![count_measure("n")]);
    let mut exec = StatsExecutor::new(exec_plan);
    assert!(exec.process_batch(&batch), "Struct 键走 Other 回退仍可处理");
    let buckets = exec.final_measure_values_by_bucket();
    assert_eq!(buckets.len(), 1, "Struct 键归一为 [object] 单桶");
    assert_eq!(
        buckets[0].0,
        crate::match_engine::ScopeKey::Str("[object]".into())
    );
    assert_eq!(buckets[0].1, vec![3.0]);
}

#[test]
fn stats_process_batch_keyed_out_of_range_rows_skipped() {
    // Defensive row-subset handling: out-of-range indices are skipped; the
    // event count still counts the subset length (caller contract).
    let exec_plan = plan(vec![field_key("b", "price")], vec![count_measure("n")]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("price", DataType::Int64, true),
        Field::new("bidder", DataType::Int64, true),
        Field::new("auction", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ],
    )
    .unwrap();
    let mut exec = StatsExecutor::new(exec_plan);
    assert!(exec.process_batch_rows(&batch, Some(&[0, 999])));
    assert_eq!(
        exec.final_measure_values_by_bucket(),
        vec![(crate::match_engine::ScopeKey::Int(1), vec![1.0])],
        "越界行号跳过, 行 0 归并"
    );
}
