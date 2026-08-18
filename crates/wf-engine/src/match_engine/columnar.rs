//! Columnar guard evaluation (L1).
//!
//! The dual-track columnar evaluator for the pure field-arithmetic /
//! comparison / constant guard subset gated by
//! [`wf_lang::columnar::expr_is_columnar`]. It reads native Arrow columns
//! directly (no per-row `HashMap<SmolStr, Value>` lookup or `Value` conversion)
//! and produces one boolean per row with the same three-valued semantics as the
//! interpreted evaluator (`match_engine::eval_expr_ext`):
//!
//! - null / missing field → `None` → not matched
//! - `&&` / `||` use SQL three-valued logic
//!
//! Numeric dispatch (per `columnar-execution-design.md` §3.4):
//! - `Int64` / `Timestamp(Ns)` columns are read as native `i64`
//! - `%` and comparison (`== != < > <= >=`) over two `i64` operands use native
//!   integer ops (more precise than the interpreted f64 path)
//! - all other arithmetic (`+ - * /`) and any mixed `i64`/`f64` operand stays f64
//!   (matching interpreted `eval_arithmetic` / `compare_cmp` exactly)
//! - `==` / `!=` over floats keep the interpreted epsilon comparison
//!
//! The native `i64` dispatch diverges from the interpreted f64 path only for
//! `>2^53` integers and nanosecond timestamps — the documented "更准" semantic
//! change in §3.4. The differential tests assert 100% equivalence below `2^53`
//! and lock the divergence above it.

use arrow::array::{
    Array, BooleanArray, BooleanBuilder, Float64Array, Int64Array, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;
use wf_lang::ast::{BinOp, Expr, FieldRef};

use super::match_engine::{EngineHashMap, field_ref_name};

/// Three-valued scalar read from an Arrow column — the scalar subset of
/// [`super::match_engine::Value`] (no object/array; those expressions fall back
/// to the interpreted track). `Int` carries native integer precision for
/// `Int64` / `Timestamp(Ns)` columns and integer-valued literals.
#[derive(Debug, Clone, PartialEq)]
enum CScalar {
    Int(i64),
    Float(f64),
    Str(SmolStr),
    Bool(bool),
}

/// A columnar view over a [`RecordBatch`]: resolves the normalized field name of
/// a [`FieldRef`] to its batch column index.
///
/// `projection` is the rule-visible field list mapped to batch column indices;
/// `field_map` maps a normalized field name (`Simple` / `Qualified` /
/// `Bracketed` all collapse to the bare field name) to its projection index.
pub struct ColumnarBatch<'a> {
    batch: &'a RecordBatch,
    projection: Vec<usize>,
    field_map: EngineHashMap<SmolStr, usize>,
}

impl<'a> ColumnarBatch<'a> {
    /// Project only the given field names (the rule's read set). Fields absent
    /// from the batch schema are simply not projected; referencing them later
    /// yields `None` (null → not matched), matching `batch_to_events`.
    pub fn new(batch: &'a RecordBatch, fields: &[SmolStr]) -> Self {
        let mut projection = Vec::with_capacity(fields.len());
        let mut field_map = EngineHashMap::default();
        for name in fields {
            if let Some(col_idx) = schema_index_of(batch, name) {
                let proj_idx = projection.len();
                projection.push(col_idx);
                field_map.insert(name.clone(), proj_idx);
            }
        }
        Self {
            batch,
            projection,
            field_map,
        }
    }

    /// Project every column in the batch schema (useful for tests / one-off
    /// views where the rule reads a superset of the schema).
    pub fn from_all_fields(batch: &'a RecordBatch) -> Self {
        let fields: Vec<SmolStr> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str().into())
            .collect();
        Self::new(batch, &fields)
    }

    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    fn resolve_field(&self, field: &FieldRef) -> ColRef<'_> {
        let Some(proj_idx) = self.field_map.get(field_ref_name(field)) else {
            return ColRef::Null;
        };
        let col_idx = self.projection[*proj_idx];
        col_ref_from_array(self.batch.column(col_idx).as_ref())
    }
}

/// Batch-level columnar **branch-guard** masks for the three guard sites the
/// state machine evaluates per event:
///
/// - `event` — `match_plan.event_steps` (keyed `(event_step_idx, branch_idx)`);
/// - `close` — `match_plan.close_steps` accumulation guard (keyed
///   `(close_step_idx, branch_idx)`);
/// - `neg` — `match_plan.seq` negation steps (keyed `(neg_idx, 0)`, the same
///   negation-only ordering `SeqRuntime::build` produces).
#[derive(Default)]
pub struct GuardMasks {
    event: EngineHashMap<(usize, usize), BooleanArray>,
    close: EngineHashMap<(usize, usize), BooleanArray>,
    neg: EngineHashMap<(usize, usize), BooleanArray>,
}

impl GuardMasks {
    pub fn insert_event(&mut self, step: usize, branch: usize, mask: BooleanArray) {
        self.event.insert((step, branch), mask);
    }

    pub fn insert_close(&mut self, step: usize, branch: usize, mask: BooleanArray) {
        self.close.insert((step, branch), mask);
    }

    pub fn insert_neg(&mut self, neg: usize, branch: usize, mask: BooleanArray) {
        self.neg.insert((neg, branch), mask);
    }

    /// Two-valued lookup (null → false) for "must be true" guards (event steps).
    /// `None` = no columnar mask for this `(step, branch)`.
    pub fn event_value(&self, step: usize, branch: usize, row: usize) -> Option<bool> {
        self.event.get(&(step, branch)).map(|m| m.value(row))
    }

    /// Three-valued lookup for permissive guards (close steps): `Some(Some(b))`
    /// = explicit bool, `Some(None)` = null / missing field (permissive), `None`
    /// = no columnar mask for this `(step, branch)`.
    pub fn close_value(&self, step: usize, branch: usize, row: usize) -> Option<Option<bool>> {
        self.close.get(&(step, branch)).map(|m| {
            if m.is_null(row) {
                None
            } else {
                Some(m.value(row))
            }
        })
    }

    /// Two-valued lookup (null → false) for negation guards. `None` = no
    /// columnar mask for this `(neg, branch)`.
    pub fn neg_value(&self, neg: usize, branch: usize, row: usize) -> Option<bool> {
        self.neg.get(&(neg, branch)).map(|m| m.value(row))
    }

    pub fn is_empty(&self) -> bool {
        self.event.is_empty() && self.close.is_empty() && self.neg.is_empty()
    }
}

/// Collect the hit row indices of a boolean mask into an ascending `Vec<u32>`
/// (the `Mask → Indices` step in `columnar-execution-design.md` §3.1).
pub fn mask_to_indices(mask: &BooleanArray) -> Vec<u32> {
    (0..mask.len())
        .filter(|&i| mask.value(i))
        .map(|i| i as u32)
        .collect()
}

fn schema_index_of(batch: &RecordBatch, name: &str) -> Option<usize> {
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == name)
}

/// A resolved, typed reference to a batch column (or `Null` for a field absent
/// from the schema / an unsupported type — both read as null, matching
/// `event_bridge::extract_value`).
enum ColRef<'a> {
    Int64(&'a Int64Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    Bool(&'a BooleanArray),
    TimestampNs(&'a TimestampNanosecondArray),
    Null,
}

impl ColRef<'_> {
    fn value(&self, row: usize) -> Option<CScalar> {
        match self {
            ColRef::Int64(a) => (!a.is_null(row)).then(|| CScalar::Int(a.value(row))),
            ColRef::Float64(a) => (!a.is_null(row)).then(|| CScalar::Float(a.value(row))),
            ColRef::Utf8(a) => (!a.is_null(row)).then(|| CScalar::Str(a.value(row).into())),
            ColRef::Bool(a) => (!a.is_null(row)).then(|| CScalar::Bool(a.value(row))),
            ColRef::TimestampNs(a) => (!a.is_null(row)).then(|| CScalar::Int(a.value(row))),
            ColRef::Null => None,
        }
    }
}

/// Map a non-null scalar at `row` of `col` to a [`CScalar`], mirroring
/// `event_bridge::extract_value`'s scalar mapping exactly.
fn col_ref_from_array(col: &dyn Array) -> ColRef<'_> {
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(ColRef::Int64)
            .unwrap_or(ColRef::Null),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(ColRef::Float64)
            .unwrap_or(ColRef::Null),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(ColRef::Utf8)
            .unwrap_or(ColRef::Null),
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(ColRef::Bool)
            .unwrap_or(ColRef::Null),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(ColRef::TimestampNs)
            .unwrap_or(ColRef::Null),
        _ => ColRef::Null,
    }
}

/// A precompiled columnar expression tree: field refs are resolved once to
/// [`ColRef`]s, so the per-row hot loop reads native columns directly with no
/// `HashMap` lookup or per-row downcast.
enum ColumnExpr<'a> {
    Lit(CScalar),
    Col(ColRef<'a>),
    Neg(Box<ColumnExpr<'a>>),
    And(Box<ColumnExpr<'a>>, Box<ColumnExpr<'a>>),
    Or(Box<ColumnExpr<'a>>, Box<ColumnExpr<'a>>),
    Cmp {
        op: BinOp,
        left: Box<ColumnExpr<'a>>,
        right: Box<ColumnExpr<'a>>,
    },
    Arith {
        op: BinOp,
        left: Box<ColumnExpr<'a>>,
        right: Box<ColumnExpr<'a>>,
    },
}

/// Evaluate a columnar guard expression over every row of `view`, producing one
/// boolean per row. Null / non-boolean / missing-field rows are emitted as
/// **null slots** (so permissive consumers can distinguish them); two-valued
/// consumers read null as `false` via [`BooleanArray::value`], matching the
/// interpreted `passes_bind_filter` → `false` fallback.
pub fn eval_guard_columnar(expr: &Expr, view: &ColumnarBatch<'_>) -> BooleanArray {
    let Some(plan) = compile_expr(expr, view) else {
        // Non-columnar expression (the gate keeps these out): all rows miss.
        return BooleanArray::from(vec![false; view.num_rows()]);
    };
    let mut builder = BooleanBuilder::with_capacity(view.num_rows());
    for row in 0..view.num_rows() {
        match eval_cx_bool(&plan, row) {
            Some(b) => builder.append_value(b),
            // Preserve null (missing field / non-bool) as a null slot instead of
            // collapsing it to `false`, so permissive (close-step) guards can
            // distinguish "explicit false" from "absent". Two-valued consumers
            // (`value()`) still read null as `false` — unchanged.
            None => builder.append_null(),
        }
    }
    builder.finish()
}

fn compile_expr<'a>(expr: &Expr, view: &'a ColumnarBatch<'a>) -> Option<ColumnExpr<'a>> {
    match expr {
        Expr::Number(n) => Some(ColumnExpr::Lit(number_literal(*n))),
        Expr::StringLit(s) => Some(ColumnExpr::Lit(CScalar::Str(s.clone().into()))),
        Expr::Bool(b) => Some(ColumnExpr::Lit(CScalar::Bool(*b))),
        Expr::Field(field) => Some(ColumnExpr::Col(view.resolve_field(field))),
        Expr::Neg(inner) => Some(ColumnExpr::Neg(Box::new(compile_expr(inner, view)?))),
        Expr::BinOp { op, left, right } => match op {
            BinOp::And => Some(ColumnExpr::And(
                Box::new(compile_expr(left, view)?),
                Box::new(compile_expr(right, view)?),
            )),
            BinOp::Or => Some(ColumnExpr::Or(
                Box::new(compile_expr(left, view)?),
                Box::new(compile_expr(right, view)?),
            )),
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge => {
                Some(ColumnExpr::Cmp {
                    op: *op,
                    left: Box::new(compile_expr(left, view)?),
                    right: Box::new(compile_expr(right, view)?),
                })
            }
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
                Some(ColumnExpr::Arith {
                    op: *op,
                    left: Box::new(compile_expr(left, view)?),
                    right: Box::new(compile_expr(right, view)?),
                })
            }
            _ => None,
        },
        _ => None,
    }
}

fn eval_cx_bool(expr: &ColumnExpr<'_>, row: usize) -> Option<bool> {
    match eval_cx(expr, row)? {
        CScalar::Bool(b) => Some(b),
        _ => None,
    }
}

fn eval_cx(expr: &ColumnExpr<'_>, row: usize) -> Option<CScalar> {
    match expr {
        ColumnExpr::Lit(v) => Some(v.clone()),
        ColumnExpr::Col(col) => col.value(row),
        ColumnExpr::Neg(inner) => match eval_cx(inner, row)? {
            CScalar::Int(i) => Some(CScalar::Float(-(i as f64))),
            CScalar::Float(f) => Some(CScalar::Float(-f)),
            _ => None,
        },
        ColumnExpr::And(left, right) => cx_logic_and(left, right, row),
        ColumnExpr::Or(left, right) => cx_logic_or(left, right, row),
        ColumnExpr::Cmp { op, left, right } => {
            let lv = eval_cx(left, row)?;
            let rv = eval_cx(right, row)?;
            Some(CScalar::Bool(compare_scalars(*op, &lv, &rv)))
        }
        ColumnExpr::Arith { op, left, right } => {
            let lv = eval_cx(left, row)?;
            let rv = eval_cx(right, row)?;
            arithmetic(*op, &lv, &rv)
        }
    }
}

/// SQL three-valued AND (mirrors `eval_logic_and`).
fn cx_logic_and(left: &ColumnExpr<'_>, right: &ColumnExpr<'_>, row: usize) -> Option<CScalar> {
    let lv = eval_cx(left, row);
    let rv = eval_cx(right, row);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(CScalar::Bool(false)), _) | (_, Some(CScalar::Bool(false))) => {
            Some(CScalar::Bool(false))
        }
        (Some(CScalar::Bool(true)), Some(CScalar::Bool(true))) => Some(CScalar::Bool(true)),
        _ => None,
    }
}

/// SQL three-valued OR (mirrors `eval_logic_or`).
fn cx_logic_or(left: &ColumnExpr<'_>, right: &ColumnExpr<'_>, row: usize) -> Option<CScalar> {
    let lv = eval_cx(left, row);
    let rv = eval_cx(right, row);
    match (lv.as_ref(), rv.as_ref()) {
        (Some(CScalar::Bool(true)), _) | (_, Some(CScalar::Bool(true))) => {
            Some(CScalar::Bool(true))
        }
        (Some(CScalar::Bool(false)), Some(CScalar::Bool(false))) => Some(CScalar::Bool(false)),
        _ => None,
    }
}

fn compare_scalars(op: BinOp, lv: &CScalar, rv: &CScalar) -> bool {
    match (lv, rv) {
        (CScalar::Int(a), CScalar::Int(b)) => compare_int(op, *a, *b),
        (CScalar::Str(a), CScalar::Str(b)) => {
            let ord = a.cmp(b);
            match op {
                BinOp::Eq => ord.is_eq(),
                BinOp::Ne => !ord.is_eq(),
                BinOp::Lt => ord.is_lt(),
                BinOp::Gt => ord.is_gt(),
                BinOp::Le => ord.is_le(),
                BinOp::Ge => ord.is_ge(),
                _ => false,
            }
        }
        (CScalar::Bool(a), CScalar::Bool(b)) => match op {
            BinOp::Eq => a == b,
            BinOp::Ne => a != b,
            _ => false,
        },
        // Mixed i64/f64 (and any other numeric pairing) → f64 (epsilon) semantics.
        (a, b) => match (to_f64(a), to_f64(b)) {
            (Some(x), Some(y)) => compare_numeric(op, x, y),
            _ => false,
        },
    }
}

/// Native `i64` comparison: exact equality, truncating-consistent ordering.
/// Only diverges from the interpreted f64 path above `2^53` (documented).
fn compare_int(op: BinOp, a: i64, b: i64) -> bool {
    match op {
        BinOp::Eq => a == b,
        BinOp::Ne => a != b,
        BinOp::Lt => a < b,
        BinOp::Gt => a > b,
        BinOp::Le => a <= b,
        BinOp::Ge => a >= b,
        _ => false,
    }
}

/// Numeric comparison with the interpreted evaluator's epsilon `==` / `!=`.
fn compare_numeric(op: BinOp, a: f64, b: f64) -> bool {
    match op {
        BinOp::Eq => (a - b).abs() < f64::EPSILON,
        BinOp::Ne => (a - b).abs() >= f64::EPSILON,
        BinOp::Lt => a < b,
        BinOp::Gt => a > b,
        BinOp::Le => a <= b,
        BinOp::Ge => a >= b,
        _ => false,
    }
}

fn to_f64(v: &CScalar) -> Option<f64> {
    match v {
        CScalar::Int(i) => Some(*i as f64),
        CScalar::Float(f) => Some(*f),
        _ => None,
    }
}

/// An integer-valued `Number` literal (`fract() == 0`, `|n| < 2^53` where f64 is
/// exact) becomes a native `i64` so `Int % Int` and `Int <op> Int` take the
/// native path. Non-integer or `>= 2^53` literals stay f64.
fn number_literal(n: f64) -> CScalar {
    const TWO_POW_53: f64 = 9_007_199_254_740_992.0;
    if n.fract() == 0.0 && n.abs() < TWO_POW_53 {
        CScalar::Int(n as i64)
    } else {
        CScalar::Float(n)
    }
}

/// Numeric arithmetic. `%` over two `i64` operands is native (more precise);
/// every other case (`+ - * /`, and any `i64`/`f64` mix) is f64 to match
/// `eval_arithmetic`.
fn arithmetic(op: BinOp, lv: &CScalar, rv: &CScalar) -> Option<CScalar> {
    if op == BinOp::Mod {
        if let (CScalar::Int(a), CScalar::Int(b)) = (lv, rv) {
            if *b == 0 {
                return None;
            }
            return Some(CScalar::Int(a % b));
        }
    }
    let ln = to_f64(lv)?;
    let rn = to_f64(rv)?;
    let out = match op {
        BinOp::Add => ln + rn,
        BinOp::Sub => ln - rn,
        BinOp::Mul => ln * rn,
        BinOp::Div => {
            if rn == 0.0 {
                return None;
            }
            ln / rn
        }
        BinOp::Mod => {
            if rn == 0.0 {
                return None;
            }
            ln % rn
        }
        _ => return None,
    };
    Some(CScalar::Float(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    use crate::match_engine::event_bridge::{batch_to_events, materialize_rows};
    use crate::match_engine::match_engine::{Event, Value, eval_expr_ext};

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

    #[test]
    fn q2_guard_matches_interpreted() {
        let auction: Vec<Option<i64>> = (0..1000).map(Some).collect();
        let batch = make_batch(
            auction,
            vec![Some(7.0); 1000],
            vec![Some("mobile"); 1000],
            vec![Some(true); 1000],
        );
        let expr = bin(
            BinOp::Eq,
            bin(BinOp::Mod, field("auction"), num(123.0)),
            num(0.0),
        );
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        assert_equiv(&expr, &batch);
    }

    #[test]
    fn comparison_arithmetic_and_logic_match_interpreted() {
        let auction: Vec<Option<i64>> = vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(-1),
            None,
            Some(1_000_000),
        ];
        let price: Vec<Option<f64>> = vec![
            Some(0.0),
            Some(1.5),
            Some(2.0),
            Some(-3.25),
            Some(7.0),
            None,
            Some(1e300),
        ];
        let channel: Vec<Option<&str>> = vec![
            Some("a"),
            Some("b"),
            Some("a"),
            None,
            Some(""),
            Some("z"),
            Some("a"),
        ];
        let flag: Vec<Option<bool>> = vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            Some(true),
            None,
        ];
        let batch = make_batch(auction, price, channel, flag);

        let exprs = vec![
            bin(BinOp::Gt, field("auction"), num(1.0)),
            bin(BinOp::Eq, field("auction"), field("auction")),
            bin(BinOp::Ne, field("auction"), num(0.0)),
            bin(BinOp::Le, field("price"), num(2.0)),
            bin(BinOp::Ge, field("price"), num(-3.25)),
            bin(BinOp::Eq, field("channel"), Expr::StringLit("a".into())),
            bin(BinOp::Lt, field("channel"), Expr::StringLit("b".into())),
            bin(BinOp::Eq, field("flag"), Expr::Bool(true)),
            bin(BinOp::Add, field("auction"), num(2.0)),
            bin(BinOp::Sub, field("auction"), num(1.0)),
            bin(BinOp::Mul, field("auction"), num(3.0)),
            bin(BinOp::Div, field("auction"), num(2.0)),
            bin(BinOp::Mod, field("auction"), num(3.0)),
            Expr::Neg(Box::new(field("auction"))),
            bin(
                BinOp::And,
                bin(BinOp::Gt, field("auction"), num(0.0)),
                bin(BinOp::Lt, field("auction"), num(3.0)),
            ),
            bin(
                BinOp::Or,
                bin(BinOp::Eq, field("channel"), Expr::StringLit("a".into())),
                bin(BinOp::Eq, field("flag"), Expr::Bool(false)),
            ),
            bin(
                BinOp::And,
                field("flag"),
                bin(BinOp::Gt, field("auction"), num(0.0)),
            ),
        ];

        for expr in exprs {
            assert!(
                wf_lang::columnar::expr_is_columnar(&expr),
                "expr should be columnar: {expr:?}"
            );
            assert_equiv(&expr, &batch);
        }
    }

    #[test]
    fn native_int_matches_interpreted_below_2_53() {
        const TWO_POW_53: i64 = 9_007_199_254_740_992;
        let auction: Vec<Option<i64>> = vec![
            Some(0),
            Some(1),
            Some(-1),
            Some(TWO_POW_53 - 2),
            Some(TWO_POW_53 - 1),
            None,
        ];
        let n = auction.len();
        let batch = make_batch(
            auction,
            vec![Some(0.0); n],
            vec![Some("x"); n],
            vec![Some(true); n],
        );
        let exprs = vec![
            bin(
                BinOp::Eq,
                bin(BinOp::Mod, field("auction"), num(123.0)),
                num(0.0),
            ),
            bin(BinOp::Gt, field("auction"), num(0.0)),
            bin(BinOp::Le, field("auction"), field("auction")),
            bin(BinOp::Ne, field("auction"), num(1.0)),
        ];
        for expr in exprs {
            assert_equiv(&expr, &batch);
        }
    }

    #[test]
    fn native_int_comparison_diverges_above_2_53() {
        const TWO_POW_53: i64 = 9_007_199_254_740_992;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![TWO_POW_53])) as ArrayRef,
                Arc::new(Int64Array::from(vec![TWO_POW_53 + 1])) as ArrayRef,
            ],
        )
        .unwrap();

        // a == b with a=2^53, b=2^53+1. Native i64 sees them distinct; the
        // interpreted f64 path rounds b down to 2^53 and reports equal.
        let expr = bin(BinOp::Eq, field("a"), field("b"));
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        let view = ColumnarBatch::from_all_fields(&batch);
        let mask = eval_guard_columnar(&expr, &view);
        assert!(
            !mask.value(0),
            "native i64 should distinguish 2^53 and 2^53+1"
        );

        let events = batch_to_events(&batch);
        let interpreted = interpreted_bool(&expr, &events[0]);
        assert!(interpreted, "interpreted f64 rounds 2^53+1 to 2^53");
        assert_ne!(mask.value(0), interpreted);
    }

    #[test]
    fn missing_field_is_null_and_not_matched() {
        let batch = make_batch(
            vec![Some(1), Some(2)],
            vec![Some(1.0), Some(2.0)],
            vec![Some("x"), Some("y")],
            vec![Some(true), Some(false)],
        );
        // `missing` is absent from the schema → columnar null, interpreted None.
        let expr = bin(BinOp::Gt, field("missing"), num(0.0));
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        assert_equiv(&expr, &batch);
    }

    #[test]
    fn epsilon_equality_matches_interpreted_on_floats() {
        // 0.1 + 0.2 == 0.3 is true under epsilon equality; both tracks must agree.
        let batch = make_batch(
            vec![Some(1)],
            vec![Some(0.1 + 0.2)],
            vec![Some("x")],
            vec![Some(true)],
        );
        let expr = bin(BinOp::Eq, field("price"), num(0.3));
        assert_equiv(&expr, &batch);
    }

    #[test]
    fn non_boolean_top_level_is_not_matched() {
        let batch = make_batch(
            vec![Some(5)],
            vec![Some(1.0)],
            vec![Some("x")],
            vec![Some(true)],
        );
        // Numeric expression at guard top level → interpreted `None` → false.
        let expr = bin(BinOp::Add, field("auction"), num(1.0));
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        assert_equiv(&expr, &batch);
    }

    #[test]
    fn mask_to_indices_and_materialize_rows_match_batch_to_events() {
        let auction: Vec<Option<i64>> = vec![Some(0), Some(1), Some(2), Some(3), Some(4)];
        let batch = make_batch(
            auction,
            vec![Some(1.0); 5],
            vec![Some("x"); 5],
            vec![Some(true); 5],
        );
        // auction % 2 == 0 → hits rows 0, 2, 4.
        let expr = bin(
            BinOp::Eq,
            bin(BinOp::Mod, field("auction"), num(2.0)),
            num(0.0),
        );
        let view = ColumnarBatch::from_all_fields(&batch);
        let mask = eval_guard_columnar(&expr, &view);
        let indices = mask_to_indices(&mask);
        assert_eq!(indices, vec![0, 2, 4]);

        let hits = materialize_rows(&batch, &indices);
        let all = batch_to_events(&batch);
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0], all[0]);
        assert_eq!(hits[1], all[2]);
        assert_eq!(hits[2], all[4]);
    }
}
