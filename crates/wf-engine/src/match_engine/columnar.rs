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
//! Structured array fields are handled natively in two shapes:
//! - the **list-index path** `root[i]` (a `FieldRef::Path` of exactly one root
//!   field + one constant index) compiles to an offset read of the array
//!   column — a `Utf8` cell holding JSON array text (`JsonArray`, the frame
//!   storage shape for `array/...` schema fields) or a native Arrow `List` /
//!   `LargeList` / `FixedSizeList` column. It mirrors the interpreted path
//!   walk exactly: null cells, parse failures, non-array roots, and
//!   out-of-range indices read null; null elements are dropped before
//!   indexing; object / array elements are a definite false on compare (not
//!   null) — so close-step permissive semantics stay byte-identical.
//! - a **bare array field** reads a non-null structured value per row
//!   (`CScalar::Structured`): compares false to every scalar, reads null as a
//!   boolean, and is not numeric.
//!
//! The native `i64` dispatch diverges from the interpreted f64 path only for
//! `>2^53` integers and nanosecond timestamps — the documented "更准" semantic
//! change in §3.4. The differential tests assert 100% equivalence below `2^53`
//! and lock the divergence above it.

use arrow::array::{
    Array, BooleanArray, BooleanBuilder, FixedSizeListArray, Float64Array, Int64Array,
    LargeListArray, ListArray, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;
use wf_lang::ast::{BinOp, Expr, FieldRef, PathSegment};

use super::match_engine::{EngineHashMap, field_ref_name};
use crate::match_engine::{WFL_FIELD_TYPE_ARRAY, wfl_structured_field_kind};

/// Three-valued scalar read from an Arrow column — the scalar subset of
/// [`super::match_engine::Value`], plus `Structured` for a non-null
/// `Value::Object` / `Value::Array` (e.g. a whole array field read bare).
/// `Int` carries native integer precision for `Int64` / `Timestamp(Ns)`
/// columns and integer-valued literals.
#[derive(Debug, Clone, PartialEq)]
enum CScalar {
    Int(i64),
    Float(f64),
    Str(SmolStr),
    Bool(bool),
    /// A non-null structured value (`Value::Array` / `Value::Object`). It
    /// compares `false` to every scalar, reads `None` as a boolean, and is
    /// not numeric — byte-identical to the interpreted structured values
    /// flowing through the same operator kernels.
    Structured,
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
        let col = self.batch.column(col_idx);
        // A `Utf8` column marked as a structured JSON array (the frame storage
        // shape for `array/...` schema fields) reads as a JSON-array column.
        if matches!(col.data_type(), DataType::Utf8)
            && wfl_structured_field_kind(self.batch.schema().field(col_idx))
                == Some(WFL_FIELD_TYPE_ARRAY)
        {
            return col
                .as_any()
                .downcast_ref::<StringArray>()
                .map(ColRef::JsonArray)
                .unwrap_or(ColRef::Null);
        }
        col_ref_from_array(col.as_ref())
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
///
/// `JsonArray` is a `Utf8` column whose field metadata marks it as a structured
/// JSON array (`wf.wfl.field_type = "array"`): each cell holds JSON array text
/// like `["prod","edge"]`. `List` / `LargeList` / `FixedSizeList` are native
/// Arrow list columns. All four carry the array shape used by
/// [`ColumnExpr::ListIndex`]; read as a bare field they are a non-null
/// structured value.
enum ColRef<'a> {
    Int64(&'a Int64Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    Bool(&'a BooleanArray),
    TimestampNs(&'a TimestampNanosecondArray),
    JsonArray(&'a StringArray),
    List(&'a ListArray),
    LargeList(&'a LargeListArray),
    FixedSizeList(&'a FixedSizeListArray),
    Null,
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
        DataType::List(_) => col
            .as_any()
            .downcast_ref::<ListArray>()
            .map(ColRef::List)
            .unwrap_or(ColRef::Null),
        DataType::LargeList(_) => col
            .as_any()
            .downcast_ref::<LargeListArray>()
            .map(ColRef::LargeList)
            .unwrap_or(ColRef::Null),
        DataType::FixedSizeList(_, _) => col
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .map(ColRef::FixedSizeList)
            .unwrap_or(ColRef::Null),
        _ => ColRef::Null,
    }
}

/// The string-search operation of a [`ColumnExpr::StrFunc`] node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StrFuncOp {
    Contains,
    StartsWith,
    EndsWith,
}

impl StrFuncOp {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "contains" => Some(StrFuncOp::Contains),
            "startswith" => Some(StrFuncOp::StartsWith),
            "endswith" => Some(StrFuncOp::EndsWith),
            _ => None,
        }
    }
}

/// The second operand of a [`ColumnExpr::StrFunc`] node: a shared literal
/// needle (the gate admits `StringLit`) or a per-row string column (a flat
/// field ref).
enum Needle<'a> {
    Lit(SmolStr),
    Col(ColRef<'a>),
}

/// A precompiled columnar expression tree: field refs are resolved once to
/// [`ColRef`]s, so the per-row hot loop reads native columns directly with no
/// `HashMap` lookup or per-row downcast.
enum ColumnExpr<'a> {
    Lit(CScalar),
    Col(ColRef<'a>),
    /// `root[i]` — the `i`-th **non-null** element of the array column `col`,
    /// per row. Mirror of the interpreted path walk: a null / non-array cell,
    /// a non-array root column, a parse failure, or an out-of-range index all
    /// read null (the path produces `None`); object / array elements read a
    /// [`CScalar::Structured`] (definite false on compare, null as boolean).
    ListIndex {
        col: ColRef<'a>,
        index: usize,
    },
    Neg(Box<ColumnExpr<'a>>),
    Not(Box<ColumnExpr<'a>>),
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
    /// `cidr_match(field, "addr/prefix")` — lowered natively: the subnet is
    /// parsed at compile time (once per batch — `compile_expr` runs for every
    /// `eval_guard_columnar` call, not per row; the checker enforces a
    /// literal), the
    /// field reads as a string column, and each non-null cell is parsed as an
    /// IP and compared against the net (mirroring the interpreted path exactly:
    /// non-Utf8 columns / null cells / non-IP strings read null / false).
    CidrMatch {
        col: ColRef<'a>,
        net: wf_lang::cidr::Cidr,
    },
    /// `regex_match(field, "pattern")` — lowered natively: the regex is
    /// compiled at compile time (once per batch, mirroring `CidrMatch`), the
    /// field reads as a string column, and each non-null
    /// cell is matched against the compiled regex (non-Utf8 columns / null
    /// cells read null, mirroring the interpreted `Value::Str`-only path).
    RegexMatch {
        col: ColRef<'a>,
        re: regex::Regex,
    },
    /// `contains` / `startswith` / `endswith` — lowered natively over two
    /// string operands. The haystack is always a flat field (string column);
    /// the needle is a shared literal or a second string column. Non-Utf8 /
    /// null cells read null, mirroring the interpreted `Value::Str`-only path.
    StrFunc {
        op: StrFuncOp,
        hay: ColRef<'a>,
        needle: Needle<'a>,
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
    let out = plan.eval_vec(view.num_rows());
    match out {
        // Top-level boolean column: materialize, preserving null slots.
        CVec::Bool(col) => {
            let mut builder = BooleanBuilder::with_capacity(col.len());
            for b in col {
                match b {
                    Some(true) => builder.append_value(true),
                    Some(false) => builder.append_value(false),
                    // Null (missing field / non-bool) → null slot.
                    None => builder.append_null(),
                }
            }
            builder.finish()
        }
        // Non-boolean top-level (e.g. `auction + 1`) → interpreted `None` per
        // row → all null slots (two-valued consumers read them as `false`).
        _ => BooleanArray::from(vec![None; view.num_rows()]),
    }
}

fn compile_expr<'a>(expr: &Expr, view: &'a ColumnarBatch<'a>) -> Option<ColumnExpr<'a>> {
    match expr {
        Expr::Number(n) => Some(ColumnExpr::Lit(number_literal(*n))),
        Expr::StringLit(s) => Some(ColumnExpr::Lit(CScalar::Str(s.clone().into()))),
        Expr::Bool(b) => Some(ColumnExpr::Lit(CScalar::Bool(*b))),
        Expr::Field(field) => match field {
            // `root[i]` — the list-index path the columnar evaluator handles
            // natively (the static gate admits exactly this shape).
            FieldRef::Path { segments, .. }
                if matches!(
                    segments.as_slice(),
                    [PathSegment::Field(_), PathSegment::Index(_)]
                ) =>
            {
                let index = match segments.last() {
                    Some(PathSegment::Index(idx)) => *idx,
                    _ => 0, // unreachable: shape matched above
                };
                Some(ColumnExpr::ListIndex {
                    col: view.resolve_field(field),
                    index,
                })
            }
            _ => Some(ColumnExpr::Col(view.resolve_field(field))),
        },
        Expr::Neg(inner) => Some(ColumnExpr::Neg(Box::new(compile_expr(inner, view)?))),
        Expr::Not(inner) => Some(ColumnExpr::Not(Box::new(compile_expr(inner, view)?))),
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
        // `cidr_match(field, "addr/prefix")` / `regex_match(field, "pattern")`
        // — the gate (wf-lang columnar) admits exactly this shape: a flat field
        // + a string-literal constant. The constant is parsed/compiled here
        // (once per batch — `compile_expr` runs per `eval_guard_columnar`
        // call), never per row.
        Expr::FuncCall {
            qualifier: None,
            name,
            args,
        } if (name == "cidr_match" || name == "regex_match")
            && args.len() == 2
            && matches!(
                &args[0],
                Expr::Field(
                    FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)
                )
            ) =>
        {
            let Expr::StringLit(constant) = &args[1] else {
                return None;
            };
            let Expr::Field(field) = &args[0] else {
                unreachable!("shape matched above");
            };
            let col = view.resolve_field(field);
            if name == "cidr_match" {
                Some(ColumnExpr::CidrMatch {
                    col,
                    net: wf_lang::cidr::Cidr::parse(constant)?,
                })
            } else {
                Some(ColumnExpr::RegexMatch {
                    col,
                    re: regex::Regex::new(constant).ok()?,
                })
            }
        }
        // `contains` / `startswith` / `endswith` — the gate admits a flat-field
        // haystack and a literal-or-flat-field needle. The literal needle is
        // shared across the row loop; a field needle resolves to its column.
        Expr::FuncCall {
            qualifier: None,
            name,
            args,
        } if args.len() == 2 && StrFuncOp::from_name(name).is_some() => {
            let Expr::Field(hay_field) = &args[0] else {
                return None;
            };
            let op = StrFuncOp::from_name(name).unwrap();
            let hay = view.resolve_field(hay_field);
            let needle = match &args[1] {
                Expr::StringLit(s) => Needle::Lit(s.clone().into()),
                Expr::Field(FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)) => {
                    let Expr::Field(f) = &args[1] else {
                        unreachable!("shape matched above");
                    };
                    Needle::Col(view.resolve_field(f))
                }
                _ => return None,
            };
            Some(ColumnExpr::StrFunc { op, hay, needle })
        }
        _ => None,
    }
}

/// A materialized whole-column output of a vectorized expression node (P3).
///
/// Each `ColumnExpr` node is evaluated **column-at-a-time**: it pulls its input
/// column(s), computes over the entire batch in one linear pass, and produces a
/// typed column of per-row `Option`s. The root of a guard yields
/// [`CVec::Bool`], which [`eval_guard_columnar`] materializes into a
/// [`BooleanArray`]. This is the vectorized-execution kernel form that replaces
/// the old per-row recursive tree walk — the row loop now iterates contiguous
/// native columns instead of re-descending the AST every row.
///
/// Semantics are byte-for-byte identical to the interpreted evaluator: the
/// per-row calls below reconstruct [`CScalar`] and delegate to the exact same
/// `compare_scalars` / `arithmetic` kernels, so null propagation, three-valued
/// `&&` / `||`, native `i64`, epsilon float compare, and the documented `>2^53`
/// divergence are all unchanged.
enum CVec {
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    Str(Vec<Option<SmolStr>>),
    Bool(Vec<Option<bool>>),
    /// Heterogeneous scalar cells — e.g. elements of a JSON-array column whose
    /// element type can vary row to row. Each cell is one [`CScalar`] (or
    /// null). Keeps the null / definite-false distinction alive for structured
    /// cells (`CScalar::Structured`).
    Scalar(Vec<Option<CScalar>>),
}

impl CVec {
    /// Per-row [`CScalar`] view (used only by compare / arithmetic kernels that
    /// delegate to the shared interpreted-semantics helpers).
    fn scalar_at(&self, row: usize) -> Option<CScalar> {
        match self {
            CVec::Int(v) => v[row].map(CScalar::Int),
            CVec::Float(v) => v[row].map(CScalar::Float),
            CVec::Str(v) => v[row].clone().map(CScalar::Str),
            CVec::Bool(v) => v[row].map(CScalar::Bool),
            CVec::Scalar(v) => v[row].clone(),
        }
    }

    /// The SQL three-valued boolean view of a cell: `Bool(b)` → `b`, any
    /// non-boolean scalar (and null) → `None`. Mirrors what
    /// `eval_cx` + the `&&` / `||` match arms saw for a non-`Bool` scalar.
    fn bool_at(&self, row: usize) -> Option<bool> {
        match self {
            CVec::Bool(v) => v[row],
            CVec::Scalar(v) => match v[row].as_ref() {
                Some(CScalar::Bool(b)) => Some(*b),
                _ => None,
            },
            _ => None,
        }
    }

    fn len(&self) -> usize {
        match self {
            CVec::Int(v) => v.len(),
            CVec::Float(v) => v.len(),
            CVec::Str(v) => v.len(),
            CVec::Bool(v) => v.len(),
            CVec::Scalar(v) => v.len(),
        }
    }
}

impl ColumnExpr<'_> {
    /// Evaluate this node over the whole batch (vectorized) into a typed column.
    /// One linear pass per node; intermediate columns are materialized and flow
    /// bottom-up to the root.
    fn eval_vec(&self, n: usize) -> CVec {
        match self {
            ColumnExpr::Lit(v) => lit_vec(v, n),
            ColumnExpr::Col(col) => col_vec(col, n),
            ColumnExpr::ListIndex { col, index } => list_index_vec(col, *index, n),
            ColumnExpr::Neg(inner) => neg_vec(inner.eval_vec(n)),
            ColumnExpr::Not(inner) => not_vec(inner.eval_vec(n)),
            ColumnExpr::And(left, right) => logic_vec::<true>(left.eval_vec(n), right.eval_vec(n)),
            ColumnExpr::Or(left, right) => logic_vec::<false>(left.eval_vec(n), right.eval_vec(n)),
            ColumnExpr::Cmp { op, left, right } => {
                cmp_vec(*op, left.eval_vec(n), right.eval_vec(n))
            }
            ColumnExpr::Arith { op, left, right } => {
                arith_vec(*op, left.eval_vec(n), right.eval_vec(n))
            }
            ColumnExpr::CidrMatch { col, net } => cidr_vec(col, net, n),
            ColumnExpr::RegexMatch { col, re } => regex_vec(col, re, n),
            ColumnExpr::StrFunc { op, hay, needle } => {
                strfunc_vec(*op, hay, needle, n)
            }
        }
    }
}

/// A literal constant column (one value repeated over `n` rows).
fn lit_vec(v: &CScalar, n: usize) -> CVec {
    match v {
        CScalar::Int(i) => CVec::Int(vec![Some(*i); n]),
        CScalar::Float(f) => CVec::Float(vec![Some(*f); n]),
        CScalar::Str(s) => CVec::Str((0..n).map(|_| Some(s.clone())).collect()),
        CScalar::Bool(b) => CVec::Bool(vec![Some(*b); n]),
        // `Lit` never carries a structured value (compile_expr only builds
        // literals from Number / StringLit / Bool); the arm keeps the match
        // total and is semantically inert.
        CScalar::Structured => CVec::Scalar(vec![Some(CScalar::Structured); n]),
    }
}

/// Materialize a [`ColRef`] leaf into a typed column in a single pass. A
/// `Timestamp(Ns)` column reads as native `i64`; a `Null` column (missing field
/// / unsupported type) reads as all-null, matching `ColRef` → `None`.
fn col_vec(col: &ColRef<'_>, n: usize) -> CVec {
    match col {
        ColRef::Int64(a) => CVec::Int(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                .collect(),
        ),
        ColRef::TimestampNs(a) => CVec::Int(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                .collect(),
        ),
        ColRef::Float64(a) => CVec::Float(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                .collect(),
        ),
        ColRef::Utf8(a) => CVec::Str(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| a.value(r).into()))
                .collect(),
        ),
        ColRef::Bool(a) => CVec::Bool(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                .collect(),
        ),
        // Array-shaped columns read bare are a non-null structured value per
        // row (`Value::Array`), never a scalar — compares false, reads null as
        // a boolean, and is not numeric (byte-identical to interpreted).
        ColRef::JsonArray(a) => structured_col(n, |r| !a.is_null(r)),
        ColRef::List(a) => structured_col(n, |r| !a.is_null(r)),
        ColRef::LargeList(a) => structured_col(n, |r| !a.is_null(r)),
        ColRef::FixedSizeList(a) => structured_col(n, |r| !a.is_null(r)),
        ColRef::Null => CVec::Int(vec![None; n]),
    }
}

/// One `CVec::Scalar` slot per row: `Some(CScalar::Structured)` for a non-null
/// cell, `None` for null.
fn structured_col(n: usize, non_null: impl Fn(usize) -> bool) -> CVec {
    CVec::Scalar(
        (0..n)
            .map(|r| non_null(r).then_some(CScalar::Structured))
            .collect(),
    )
}

/// Vectorized `cidr_match(field, net)` over a string column. Mirrors the
/// interpreted path: only a `Utf8` column can carry an IP string; null cells
/// read null; non-UTF8 / array-shaped / missing columns read all-null; a
/// non-IP string parses to `false` (via `Cidr::contains`). The subnet is
/// already parsed — this kernel never re-parses the CIDR.
fn cidr_vec(col: &ColRef<'_>, net: &wf_lang::cidr::Cidr, n: usize) -> CVec {
    match col {
        ColRef::Utf8(a) => CVec::Bool(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| net.contains(a.value(r))))
                .collect(),
        ),
        _ => CVec::Bool(vec![None; n]),
    }
}

/// Vectorized `regex_match(field, re)` over a string column. Mirrors the
/// interpreted path: only a `Utf8` column can carry a haystack; null cells
/// read null; non-UTF8 / array-shaped / missing columns read all-null. The
/// regex is already compiled — this kernel never recompiles it.
fn regex_vec(col: &ColRef<'_>, re: &regex::Regex, n: usize) -> CVec {
    match col {
        ColRef::Utf8(a) => CVec::Bool(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| re.is_match(a.value(r))))
                .collect(),
        ),
        _ => CVec::Bool(vec![None; n]),
    }
}

/// Vectorized `contains` / `startswith` / `endswith` over string columns.
/// Mirrors the interpreted path: both operands must be `Value::Str` (a `Utf8`
/// column); null on either side reads null; non-Utf8 columns read all-null.
/// A literal needle is shared across the row loop (no per-row clone).
fn strfunc_vec(op: StrFuncOp, hay: &ColRef<'_>, needle: &Needle<'_>, n: usize) -> CVec {
    let apply = |h: &str, nd: &str| match op {
        StrFuncOp::Contains => h.contains(nd),
        StrFuncOp::StartsWith => h.starts_with(nd),
        StrFuncOp::EndsWith => h.ends_with(nd),
    };
    match (hay, needle) {
        (ColRef::Utf8(h), Needle::Lit(nd)) => CVec::Bool(
            (0..n)
                .map(|r| (!h.is_null(r)).then(|| apply(h.value(r), nd)))
                .collect(),
        ),
        (ColRef::Utf8(h), Needle::Col(ColRef::Utf8(nc))) => CVec::Bool(
            (0..n)
                .map(|r| {
                    if h.is_null(r) || nc.is_null(r) {
                        None
                    } else {
                        Some(apply(h.value(r), nc.value(r)))
                    }
                })
                .collect(),
        ),
        _ => CVec::Bool(vec![None; n]),
    }
}

/// Vectorized unary negation. `Int` negates to `Float` (widening, mirroring the
/// interpreted `-(i as f64)`); `Str`/`Bool` (and null) → all-null.
fn neg_vec(inner: CVec) -> CVec {
    let n = inner.len();
    match inner {
        CVec::Int(v) => CVec::Float(v.into_iter().map(|o| o.map(|i| -(i as f64))).collect()),
        CVec::Float(v) => CVec::Float(v.into_iter().map(|o| o.map(|f| -f)).collect()),
        // Heterogeneous cells negate per cell: numbers → -n, everything else
        // (and null) → null, matching the interpreted `Neg` on `Value`.
        CVec::Scalar(v) => CVec::Float(
            v.into_iter()
                .map(|o| {
                    o.and_then(|s| match s {
                        CScalar::Int(i) => Some(-(i as f64)),
                        CScalar::Float(f) => Some(-f),
                        _ => None,
                    })
                })
                .collect(),
        ),
        _ => CVec::Float(vec![None; n]),
    }
}

/// Vectorized logical negation. `Bool` negates per cell; non-boolean scalars
/// and null → null slots (interpreted `Not` returns `None` for non-`Bool`).
fn not_vec(inner: CVec) -> CVec {
    let n = inner.len();
    match inner {
        CVec::Bool(v) => CVec::Bool(v.into_iter().map(|o| o.map(|b| !b)).collect()),
        // Heterogeneous cells negate per cell: bool → !b, everything else (and
        // null) → null, matching the interpreted `Not` on `Value`.
        CVec::Scalar(v) => CVec::Bool(
            v.into_iter()
                .map(|o| o.and_then(|s| match s { CScalar::Bool(b) => Some(!b), _ => None }))
                .collect(),
        ),
        _ => CVec::Bool(vec![None; n]),
    }
}

/// Vectorized `root[i]`: per row, read the `index`-th non-null element of the
/// array cell as a scalar (null cell / parse failure / out of range → null).
/// A non-array column reads all-null — the interpreted path walk yields `None`
/// for an index segment on a non-array root, so this is byte-identical.
fn list_index_vec(col: &ColRef<'_>, index: usize, n: usize) -> CVec {
    match col {
        ColRef::JsonArray(a) => CVec::Scalar(
            (0..n)
                .map(|r| {
                    if a.is_null(r) {
                        None
                    } else {
                        nth_json_array_scalar(a.value(r), index)
                    }
                })
                .collect(),
        ),
        ColRef::List(a) => CVec::Scalar(
            (0..n)
                .map(|r| {
                    if a.is_null(r) {
                        None
                    } else {
                        list_slice_nth_scalar(a.value(r).as_ref(), index)
                    }
                })
                .collect(),
        ),
        ColRef::LargeList(a) => CVec::Scalar(
            (0..n)
                .map(|r| {
                    if a.is_null(r) {
                        None
                    } else {
                        list_slice_nth_scalar(a.value(r).as_ref(), index)
                    }
                })
                .collect(),
        ),
        ColRef::FixedSizeList(a) => CVec::Scalar(
            (0..n)
                .map(|r| {
                    if a.is_null(r) {
                        None
                    } else {
                        list_slice_nth_scalar(a.value(r).as_ref(), index)
                    }
                })
                .collect(),
        ),
        // Non-array root column: the interpreted walk hits an index segment on
        // a non-array value → `None` for every row.
        _ => CVec::Scalar(vec![None; n]),
    }
}

/// The `index`-th **non-null** element of the JSON array in `cell` as a
/// scalar, byte-identical to the interpreted structured-array path
/// (`serde_json` parse → `json_to_value` null-drop → index → scalar mapping):
///
/// - `null` cells, parse failures, non-array JSON, and out-of-range indices →
///   `None` (the interpreted walk yields null);
/// - null elements are skipped (`json_to_value(Null)` is dropped), so
///   `["a", null, "b"][1]` is `"b"`;
/// - object / array elements map to [`CScalar::Structured`] (definite false on
///   compare, null as boolean).
///
/// Parsing never materializes the whole array `Value` (the per-row allocation
/// the interpreted path pays): elements up to the found one are parsed, the
/// rest are skipped as [`serde::de::IgnoredAny`].
fn nth_json_array_scalar(cell: &str, index: usize) -> Option<CScalar> {
    let mut de = serde_json::Deserializer::from_str(cell);
    nth_json_element(&mut de, index)
        .ok()?
        .as_ref()
        .map(json_scalar)
}

/// Map one non-null JSON array element to a [`CScalar`], mirroring
/// `event_bridge::json_to_value`: `Bool` → bool, `Number` → f64, `String` →
/// str, and anything structured → [`CScalar::Structured`].
fn json_scalar(v: &serde_json::Value) -> CScalar {
    match v {
        serde_json::Value::Bool(b) => CScalar::Bool(*b),
        serde_json::Value::Number(n) => match n.as_f64() {
            Some(f) => CScalar::Float(f),
            // `Number::as_f64` is total in practice; a theoretical failure
            // would mean the interpreted path drops this element.
            None => CScalar::Structured,
        },
        serde_json::Value::String(s) => CScalar::Str(s.as_str().into()),
        _ => CScalar::Structured,
    }
}

/// The `index`-th **non-null** element of a native Arrow list slice, mirroring
/// `extract_list_values` (null cells skipped, unsupported cell types dropped)
/// plus `extract_value`'s scalar mapping.
fn list_slice_nth_scalar(slice: &dyn Array, index: usize) -> Option<CScalar> {
    let mut seen = 0usize;
    for row in 0..slice.len() {
        if slice.is_null(row) {
            continue;
        }
        let Some(cell) = arrow_cell_scalar(slice, row) else {
            continue; // unsupported child type → dropped, like `extract_value`
        };
        if seen == index {
            return Some(cell);
        }
        seen += 1;
    }
    None
}

/// Scalar mapping of one Arrow cell, mirroring `extract_value`: supported
/// scalar columns read natively; structured children (`Struct` → object,
/// nested list → array) read [`CScalar::Structured`]; unsupported types →
/// `None` (the element is dropped, shifting later indices).
fn arrow_cell_scalar(col: &dyn Array, row: usize) -> Option<CScalar> {
    match col.data_type() {
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| CScalar::Int(a.value(row))),
        DataType::Float64 => col
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| CScalar::Float(a.value(row))),
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| CScalar::Str(a.value(row).into())),
        DataType::Boolean => col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| CScalar::Bool(a.value(row))),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|a| CScalar::Int(a.value(row))),
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _) => Some(CScalar::Structured),
        _ => None,
    }
}

/// Drive `de` to read "the `index`-th non-null element of a JSON array".
/// Parses only the prefix up to the found element (later elements are skipped,
/// not materialized); any top-level shape that is not an array (object / scalar
/// / null) errors → the caller treats the cell as null, matching the
/// interpreted non-array path.
fn nth_json_element<'de, D>(de: D, index: usize) -> Result<Option<serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct NthVisitor {
        index: usize,
    }
    impl<'de> serde::de::Visitor<'de> for NthVisitor {
        type Value = Option<serde_json::Value>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "a JSON array")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut seen = 0usize;
            let mut found = None;
            while let Some(v) = seq.next_element::<serde_json::Value>()? {
                if v.is_null() {
                    continue; // json_to_value drops null elements
                }
                if seen == self.index {
                    found = Some(v);
                    // serde_json's `deserialize_any` validates the closing `]`
                    // after `visit_seq` returns, so drain the rest (skipped via
                    // `IgnoredAny`, no `Value` allocation) before returning.
                    while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                    break;
                }
                seen += 1;
            }
            Ok(found)
        }
    }
    de.deserialize_any(NthVisitor { index })
}

/// Vectorized comparison: per row, null cell on either side → null; else
/// `compare_scalars` (which returns `false` for a non-numeric pair, including
/// any [`CScalar::Structured`] operand).
fn cmp_vec(op: BinOp, left: CVec, right: CVec) -> CVec {
    let n = left.len();
    let mut out = Vec::with_capacity(n);
    for row in 0..n {
        let cell = match (left.scalar_at(row), right.scalar_at(row)) {
            (Some(lv), Some(rv)) => Some(compare_scalars(op, &lv, &rv)),
            _ => None,
        };
        out.push(cell);
    }
    CVec::Bool(out)
}

/// Vectorized arithmetic. The output column type is deterministic from the
/// homogeneous input column types: `%` over two `Int` columns stays `Int`;
/// every other case goes through `arithmetic`'s f64 path → `Float` (div-by-zero
/// and non-numeric cells → null).
fn arith_vec(op: BinOp, left: CVec, right: CVec) -> CVec {
    let n = left.len();
    let mut out = Vec::with_capacity(n);
    let int_mod = op == BinOp::Mod && matches!(left, CVec::Int(_)) && matches!(right, CVec::Int(_));
    for row in 0..n {
        out.push(arith_cell(op, &left, &right, row));
    }
    if int_mod {
        CVec::Int(
            out.into_iter()
                .map(|o| match o {
                    Some(CScalar::Int(i)) => Some(i),
                    _ => None,
                })
                .collect(),
        )
    } else {
        CVec::Float(
            out.into_iter()
                .map(|o| match o {
                    Some(CScalar::Float(f)) => Some(f),
                    _ => None,
                })
                .collect(),
        )
    }
}

/// Per-row arithmetic via the shared interpreted-semantics `arithmetic` helper
/// (null propagation and div-by-zero both surface as `Ok(None)` → null).
fn arith_cell(op: BinOp, left: &CVec, right: &CVec, row: usize) -> Option<CScalar> {
    let lv = left.scalar_at(row)?;
    let rv = right.scalar_at(row)?;
    arithmetic(op, &lv, &rv)
}

/// Vectorized SQL three-valued `&&` (`AND=true`) / `||` (`AND=false`).
fn logic_vec<const AND: bool>(left: CVec, right: CVec) -> CVec {
    let n = left.len();
    let mut out = Vec::with_capacity(n);
    for row in 0..n {
        let cell = match (left.bool_at(row), right.bool_at(row)) {
            (Some(false), _) | (_, Some(false)) if AND => Some(false),
            (Some(true), Some(true)) if AND => Some(true),
            (Some(true), _) | (_, Some(true)) if !AND => Some(true),
            (Some(false), Some(false)) if !AND => Some(false),
            _ => None,
        };
        out.push(cell);
    }
    CVec::Bool(out)
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
        // A structured operand is a definite type mismatch → false (the
        // interpreted `compare_values` catch-all for non-scalar `Value`s).
        (CScalar::Structured, _) | (_, CScalar::Structured) => false,
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
    if op == BinOp::Mod
        && let (CScalar::Int(a), CScalar::Int(b)) = (lv, rv)
    {
        if *b == 0 {
            return None;
        }
        return Some(CScalar::Int(a % b));
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
    use crate::match_engine::WFL_FIELD_TYPE_METADATA_KEY;
    use arrow::array::{ArrayRef, BinaryArray};
    use arrow::buffer::OffsetBuffer;
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
            // 逻辑否定：not 比较 / not flag / 双重 not（列式 == 解释器）。
            Expr::Not(Box::new(bin(BinOp::Eq, field("auction"), num(1.0)))),
            Expr::Not(Box::new(field("flag"))),
            Expr::Not(Box::new(Expr::Not(Box::new(bin(BinOp::Eq, field("auction"), num(1.0)))))),
        ];

        for expr in exprs {
            assert!(
                wf_lang::columnar::expr_is_columnar(&expr),
                "expr should be columnar: {expr:?}"
            );
            assert_equiv(&expr, &batch);
        }
    }

    /// 列式 `not (auction == 1)` vs `auction != 1`：语义等价、路径几乎相同
    /// （not_vec 只对 bool 列逐格取反），吞吐应同量级，且 mask 逐位一致。
    /// 保护 `not` 的列式实现不被退化成每行 fallback 或额外全列扫描。
    #[test]
    fn not_columnar_throughput_parity() {
        use std::time::Instant;

        let rows = 1_000usize;
        let auction: Vec<Option<i64>> = (0..rows).map(|i| Some((i % 50) as i64)).collect();
        let n = auction.len();
        let batch = make_batch(
            auction,
            vec![Some(0.0); n],
            vec![Some("a"); n],
            vec![Some(true); n],
        );
        let view = ColumnarBatch::from_all_fields(&batch);

        let not_expr = Expr::Not(Box::new(bin(BinOp::Eq, field("auction"), num(1.0))));
        let ne_expr = bin(BinOp::Ne, field("auction"), num(1.0));

        let rounds = 200usize;
        let start_not = Instant::now();
        let mut mask_not = BooleanArray::from(vec![false; rows]);
        for _ in 0..rounds {
            mask_not = eval_guard_columnar(&not_expr, &view);
        }
        let not_el = start_not.elapsed();

        let start_ne = Instant::now();
        let mut mask_ne = BooleanArray::from(vec![false; rows]);
        for _ in 0..rounds {
            mask_ne = eval_guard_columnar(&ne_expr, &view);
        }
        let ne_el = start_ne.elapsed();

        assert_eq!(mask_not.len(), rows);
        for r in 0..rows {
            assert_eq!(
                mask_not.value(r),
                mask_ne.value(r),
                "row {r}: not(...) 与 != 的列式结果必须一致"
            );
        }
        let ratio = not_el.as_secs_f64() / ne_el.as_secs_f64();
        eprintln!(
            "  columnar not={:?} ne={:?} ratio={:.2}x",
            not_el, ne_el, ratio
        );
        assert!(
            ratio < 2.5,
            "columnar `not` 相对 `!=` 开销过高：{:.2}x (not {:?} vs != {:?})",
            ratio,
            not_el,
            ne_el
        );
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

    /// A `tags`-style column: `Utf8` cells holding JSON arrays, marked with the
    /// structured-array metadata the receiver attaches to `array/...` fields
    /// (`wf.wfl.field_type = "array"`), plus an `auction` column for
    /// composition tests.
    fn json_array_batch(tags: Vec<Option<&str>>, auction: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tags", DataType::Utf8, true).with_metadata(
                std::collections::HashMap::from([(
                    WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                    WFL_FIELD_TYPE_ARRAY.to_string(),
                )]),
            ),
            Field::new("auction", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(tags)) as ArrayRef,
                Arc::new(Int64Array::from(auction)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    /// `c.tags[i]` — the list-index path under test.
    fn tags_index(index: usize) -> Expr {
        Expr::Field(FieldRef::Path {
            alias: "c".to_string(),
            segments: vec![
                PathSegment::Field("tags".to_string()),
                PathSegment::Index(index),
            ],
        })
    }

    #[test]
    fn list_index_json_array_matches_interpreted() {
        let batch = json_array_batch(
            vec![
                Some(r#"["prod","edge","dmz"]"#),
                Some(r#"["edge"]"#),
                Some(r#"["prod"]"#),
                Some(r#"[]"#),
                None,
            ],
            vec![Some(1); 5],
        );
        let exprs = vec![
            bin(BinOp::Eq, tags_index(0), Expr::StringLit("prod".into())),
            bin(BinOp::Eq, tags_index(1), Expr::StringLit("edge".into())),
            bin(BinOp::Eq, tags_index(2), Expr::StringLit("dmz".into())),
            // Out of range / null cell → null → not matched.
            bin(BinOp::Eq, tags_index(3), Expr::StringLit("x".into())),
            bin(BinOp::Ne, tags_index(0), Expr::StringLit("edge".into())),
            bin(BinOp::Gt, tags_index(0), Expr::StringLit("a".into())),
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
    fn list_index_json_array_null_elements_are_dropped() {
        let batch = json_array_batch(
            vec![
                Some(r#"["a", null, "b"]"#),
                Some(r#"[null, null, "c"]"#),
                Some(r#"[1, null, "x"]"#),
            ],
            vec![Some(1); 3],
        );
        // json_to_value drops null elements: [a, null, b] → [a, b], so [1] is "b".
        let expr = bin(BinOp::Eq, tags_index(1), Expr::StringLit("b".into()));
        assert_equiv(&expr, &batch);
        // [null, null, "c"] → ["c"], so [2] is out of range → null.
        let expr = bin(BinOp::Eq, tags_index(2), Expr::StringLit("c".into()));
        assert_equiv(&expr, &batch);
        // [1, null, "x"] → [1, "x"]; index 1 is the string "x".
        let expr = bin(BinOp::Eq, tags_index(1), Expr::StringLit("x".into()));
        assert_equiv(&expr, &batch);
        // And the numeric element before the null: index 0 == 1.
        let expr = bin(BinOp::Eq, tags_index(0), num(1.0));
        assert_equiv(&expr, &batch);
    }

    #[test]
    fn list_index_json_numeric_and_bool_elements() {
        let batch = json_array_batch(
            vec![
                Some(r#"[5, 6.5]"#),
                Some(r#"[true, false]"#),
                Some(r#"[1e2]"#),
            ],
            vec![Some(1); 3],
        );
        // Number elements compare as f64 (interpreted `Value::Number`).
        let expr = bin(BinOp::Eq, tags_index(0), num(5.0));
        assert_equiv(&expr, &batch);
        let expr = bin(BinOp::Gt, tags_index(0), num(4.0));
        assert_equiv(&expr, &batch);
        // 1e2 → 100.
        let expr = bin(BinOp::Eq, tags_index(0), num(100.0));
        assert_equiv(&expr, &batch);
        // Bool elements compare as bools; a number never equals a bool.
        let expr = bin(BinOp::Eq, tags_index(0), Expr::Bool(true));
        assert_equiv(&expr, &batch);
        let expr = bin(BinOp::Eq, tags_index(1), Expr::Bool(false));
        assert_equiv(&expr, &batch);
    }

    #[test]
    fn list_index_structured_elements_compare_false_not_null() {
        let batch = json_array_batch(
            vec![
                Some(r#"[{"k":1}, "prod"]"#),
                Some(r#"[[1,2]]"#),
                Some(r#"["prod"]"#),
            ],
            vec![Some(1); 3],
        );
        // Object / array elements are a definite false on compare, never null.
        let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("prod".into()));
        assert_equiv(&expr, &batch);
        // Out-of-range index reads a null slot (the close-step permissive
        // distinction) — lock it directly on the mask.
        let out_of_range = bin(BinOp::Eq, tags_index(2), Expr::StringLit("prod".into()));
        let view = ColumnarBatch::from_all_fields(&batch);
        let mask = eval_guard_columnar(&expr, &view);
        let mask_oob = eval_guard_columnar(&out_of_range, &view);
        assert!(
            !mask.value(0) && !mask.is_null(0),
            "object element → false, not null"
        );
        assert!(
            !mask.value(1) && !mask.is_null(1),
            "array element → false, not null"
        );
        assert!(mask.value(2), "string element compares equal");
        for row in 0..3 {
            assert!(
                mask_oob.is_null(row),
                "out-of-range reads null (permissive)"
            );
        }
    }

    /// A single-column batch whose `tags` column is a native Arrow list shape.
    fn native_list_batch(col: ArrayRef, list_dt: DataType) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("tags", list_dt, true)]));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn list_index_native_list_columns_match_interpreted() {
        // List<Utf8>: rows ["prod","edge"] / [null] / [] / ["dmz"].
        let values = StringArray::from(vec![Some("prod"), Some("edge"), None, Some("dmz")]);
        let list = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0i32, 2, 3, 3, 4].into()),
            Arc::new(values) as ArrayRef,
            None,
        )
        .unwrap();
        let batch = native_list_batch(
            Arc::new(list) as ArrayRef,
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        );
        // [null] drops the null element → empty → index 0 out of range → null.
        let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("prod".into()));
        assert_equiv(&expr, &batch);
        let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("dmz".into()));
        assert_equiv(&expr, &batch);

        // LargeList<Int64>: rows [1, 2] / [3, 4, 5].
        let large = LargeListArray::try_new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(vec![0i64, 2, 5].into()),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            None,
        )
        .unwrap();
        let batch = native_list_batch(
            Arc::new(large) as ArrayRef,
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
        );
        let expr = bin(BinOp::Eq, tags_index(1), num(2.0));
        assert_equiv(&expr, &batch);
        let expr = bin(BinOp::Eq, tags_index(2), num(5.0));
        assert_equiv(&expr, &batch);

        // FixedSizeList<Utf8> size 2: rows ["a","b"] / ["c", null] → ["c"].
        let values = StringArray::from(vec![Some("a"), Some("b"), Some("c"), None]);
        let fixed = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            2,
            Arc::new(values) as ArrayRef,
            None,
        );
        let batch = native_list_batch(
            Arc::new(fixed) as ArrayRef,
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Utf8, true)), 2),
        );
        let expr = bin(BinOp::Eq, tags_index(1), Expr::StringLit("b".into()));
        assert_equiv(&expr, &batch);
        // [c, null] → [c]: index 1 out of range → null; index 0 == "c".
        let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("c".into()));
        assert_equiv(&expr, &batch);
        let expr = bin(BinOp::Eq, tags_index(1), Expr::StringLit("c".into()));
        assert_equiv(&expr, &batch);
    }

    #[test]
    fn list_index_non_array_root_degrades_to_null() {
        // A plain Utf8 column named `tags` (no array metadata) whose cells are
        // JSON-array text: the interpreted walk hits `[0]` on a Str root → null.
        let schema = Arc::new(Schema::new(vec![Field::new("tags", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some(r#"["prod"]"#)])) as ArrayRef],
        )
        .unwrap();
        let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("prod".into()));
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        let events = batch_to_events(&batch);
        let view = ColumnarBatch::from_all_fields(&batch);
        let mask = eval_guard_columnar(&expr, &view);
        assert!(
            !mask.value(0) && mask.is_null(0),
            "non-array root reads null"
        );
        assert_eq!(mask.value(0), interpreted_bool(&expr, &events[0]));

        // An Int64 column named `tags`: index on a Number root → null too.
        let schema = Arc::new(Schema::new(vec![Field::new("tags", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef],
        )
        .unwrap();
        let events = batch_to_events(&batch);
        let view = ColumnarBatch::from_all_fields(&batch);
        let mask = eval_guard_columnar(&expr, &view);
        assert!(!mask.value(0) && mask.is_null(0));
        assert_eq!(mask.value(0), interpreted_bool(&expr, &events[0]));
    }

    #[test]
    fn bare_array_field_is_structured() {
        // `c.tags` (Qualified, no index) reads the whole array as a structured
        // value: never equal to a scalar, `!=` always true, present-but-null
        // distinguished (a present array is a definite false, not null).
        let batch = json_array_batch(vec![Some(r#"["prod","edge"]"#), None], vec![Some(1); 2]);
        let tags_field = || Expr::Field(FieldRef::Qualified("c".into(), "tags".into()));
        let expr = bin(BinOp::Eq, tags_field(), Expr::StringLit("prod".into()));
        assert_equiv(&expr, &batch);
        let expr = bin(BinOp::Ne, tags_field(), Expr::StringLit("prod".into()));
        assert_equiv(&expr, &batch);
        let view = ColumnarBatch::from_all_fields(&batch);
        let mask = eval_guard_columnar(
            &bin(BinOp::Eq, tags_field(), Expr::StringLit("x".into())),
            &view,
        );
        assert!(
            !mask.is_null(0),
            "present array is a definite false, not null"
        );
        assert!(mask.is_null(1), "null cell reads null");
    }

    #[test]
    fn list_index_bool_logic_and_negation() {
        // tags is a JSON-array column; flag is a flat Bool column. Rows cover
        // bool elements, null-dropped arrays, and number elements — the
        // three-valued `&&` and unary negation over heterogeneous cells.
        let schema = Arc::new(Schema::new(vec![
            Field::new("tags", DataType::Utf8, true).with_metadata(
                std::collections::HashMap::from([(
                    WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                    WFL_FIELD_TYPE_ARRAY.to_string(),
                )]),
            ),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some(r#"[true]"#),
                    Some(r#"[false]"#),
                    Some(r#"[null]"#),
                    Some(r#"[3.5]"#),
                    Some(r#"[5]"#),
                ])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true); 5])) as ArrayRef,
            ],
        )
        .unwrap();
        // Bool elements flow through the three-valued `&&` (bool_at over a
        // heterogeneous cell); non-bool elements read null, exactly like
        // `Value::Bool` vs `Value::Number` in the interpreted evaluator.
        let expr = bin(BinOp::And, tags_index(0), field("flag"));
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        assert_equiv(&expr, &batch);
        // Unary negation widens Int/Float cells to -n and nulls everything else.
        let expr = bin(BinOp::Eq, Expr::Neg(Box::new(tags_index(0))), num(-5.0));
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        assert_equiv(&expr, &batch);
    }

    #[test]
    fn bare_native_list_field_is_structured() {
        // `c.tags` (no index) over native List / LargeList / FixedSizeList
        // columns: a non-null structured value per row — never equal to a
        // scalar, `!=` always true.
        let tags_field = || Expr::Field(FieldRef::Qualified("c".into(), "tags".into()));
        let eq = bin(BinOp::Eq, tags_field(), Expr::StringLit("a".into()));
        let ne = bin(BinOp::Ne, tags_field(), Expr::StringLit("a".into()));

        let values = StringArray::from(vec![Some("a"), None]);
        let list = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0i32, 1, 2].into()),
            Arc::new(values) as ArrayRef,
            None,
        )
        .unwrap();
        let batch = native_list_batch(
            Arc::new(list) as ArrayRef,
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        );
        assert_equiv(&eq, &batch);
        assert_equiv(&ne, &batch);

        let large = LargeListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0i64, 1, 2].into()),
            Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
            None,
        )
        .unwrap();
        let batch = native_list_batch(
            Arc::new(large) as ArrayRef,
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
        );
        assert_equiv(&eq, &batch);
        assert_equiv(&ne, &batch);

        let fixed = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            1,
            Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
            None,
        );
        let batch = native_list_batch(
            Arc::new(fixed) as ArrayRef,
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Utf8, true)), 1),
        );
        assert_equiv(&eq, &batch);
        assert_equiv(&ne, &batch);
    }

    #[test]
    fn list_index_native_list_child_types() {
        // List<Timestamp(Ns)>: timestamp children read as native i64 (the same
        // documented precision as `TimestampNs` columns).
        let ts_values =
            TimestampNanosecondArray::from(vec![Some(1_700_000_000_000_000i64), Some(2)]);
        let ts_list = ListArray::try_new(
            Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )),
            OffsetBuffer::new(vec![0i32, 2].into()),
            Arc::new(ts_values) as ArrayRef,
            None,
        )
        .unwrap();
        let batch = native_list_batch(
            Arc::new(ts_list) as ArrayRef,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ))),
        );
        let expr = bin(BinOp::Eq, tags_index(0), num(1_700_000_000_000_000.0));
        assert_equiv(&expr, &batch);
        let expr = bin(BinOp::Eq, tags_index(1), num(2.0));
        assert_equiv(&expr, &batch);

        // List<Binary>: an unsupported child type is dropped before indexing
        // (like `extract_value` → None), so index 0 reads null.
        let bin_list = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Binary, true)),
            OffsetBuffer::new(vec![0i32, 1].into()),
            Arc::new(BinaryArray::from(vec![Some(&b"x"[..])])) as ArrayRef,
            None,
        )
        .unwrap();
        let batch = native_list_batch(
            Arc::new(bin_list) as ArrayRef,
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
        );
        let expr = bin(BinOp::Eq, tags_index(0), Expr::StringLit("x".into()));
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        assert_equiv(&expr, &batch);
    }

    #[test]
    fn list_index_composes_with_flat_guards() {
        let batch = json_array_batch(
            vec![Some(r#"["prod"]"#), Some(r#"["edge"]"#), None],
            vec![Some(1); 3],
        );
        // tags[0] == "prod" && auction > 0 — the qradar g_tag_prod guard shape.
        let expr = bin(
            BinOp::And,
            bin(BinOp::Eq, tags_index(0), Expr::StringLit("prod".into())),
            bin(BinOp::Gt, field("auction"), num(0.0)),
        );
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

    /// `sip` Utf8 column + `count` Int64 column — the cidr_match guard shape.
    fn ip_batch(sip: Vec<Option<&str>>, count: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("sip", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(sip)) as ArrayRef,
                Arc::new(Int64Array::from(count)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn cidr_call(ip: Expr, net: &str) -> Expr {
        Expr::FuncCall {
            qualifier: None,
            name: "cidr_match".into(),
            args: vec![ip, Expr::StringLit(net.into())],
        }
    }

    #[test]
    fn cidr_match_matches_interpreted_and_composes() {
        let batch = ip_batch(
            vec![
                Some("10.1.2.3"),  // 10/8 命中
                Some("172.31.0.1"), // 不命中
                Some("fe80::1"),  // v6 与 v4 网段版本不一致
                Some("8.8.8.8"),  // 不命中
                None,             // null
                Some("11.0.0.1"), // 不命中
            ],
            vec![Some(1), Some(5), Some(2), Some(0), Some(9), Some(7)],
        );
        let expr = cidr_call(field("sip"), "10.0.0.0/8");
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        assert_equiv(&expr, &batch);

        // 组合：cidr_match && count > 1 — 整体列式且逐位一致。
        let combo = bin(BinOp::And, expr, bin(BinOp::Gt, field("count"), num(1.0)));
        assert!(wf_lang::columnar::expr_is_columnar(&combo));
        assert_equiv(&combo, &batch);

        // v6 网段。
        let v6 = cidr_call(field("sip"), "fe80::/10");
        assert!(wf_lang::columnar::expr_is_columnar(&v6));
        assert_equiv(&v6, &batch);

        // 字面量 IP 首参 → 非列式（回落解释器）。
        let lit_ip = cidr_call(Expr::StringLit("10.0.0.1".into()), "10.0.0.0/8");
        assert!(!wf_lang::columnar::expr_is_columnar(&lit_ip));
    }

    #[test]
    fn regex_match_matches_interpreted_and_composes() {
        let batch = ip_batch(
            vec![
                Some("failed_login"), // 命中 fail.*
                Some("success"),      // 不命中
                Some("fail fast"),    // 命中
                Some("login"),        // 不命中
                None,                 // null
                Some("FAILED"),       // 大小写敏感 → 不命中
            ],
            vec![Some(1), Some(5), Some(2), Some(0), Some(9), Some(7)],
        );
        let rm = |arg1: Expr| Expr::FuncCall {
            qualifier: None,
            name: "regex_match".into(),
            args: vec![field("sip"), arg1],
        };
        let expr = rm(Expr::StringLit("fail.*".into()));
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        assert_equiv(&expr, &batch);

        // 组合：regex_match && count > 1 — 整体列式且逐位一致。
        let combo = bin(BinOp::And, expr, bin(BinOp::Gt, field("count"), num(1.0)));
        assert!(wf_lang::columnar::expr_is_columnar(&combo));
        assert_equiv(&combo, &batch);

        // 非字面量 pattern → 非列式（回落解释器）。
        let dyn_pat = rm(field("pat"));
        assert!(!wf_lang::columnar::expr_is_columnar(&dyn_pat));
    }

    /// `action` + `pattern` 双 Utf8 列 + `count` Int64 列 —— contains / startswith
    /// / endswith 的两种 needle 形态（字面量 / 字段）都覆盖。
    fn str_batch(
        action: Vec<Option<&str>>,
        pattern: Vec<Option<&str>>,
        count: Vec<Option<i64>>,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("action", DataType::Utf8, true),
            Field::new("pattern", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(action)) as ArrayRef,
                Arc::new(StringArray::from(pattern)) as ArrayRef,
                Arc::new(Int64Array::from(count)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn str_func_call(name: &str, hay: Expr, needle: Expr) -> Expr {
        Expr::FuncCall {
            qualifier: None,
            name: name.into(),
            args: vec![hay, needle],
        }
    }

    #[test]
    fn str_search_matches_interpreted_literal_and_field_needle() {
        let batch = str_batch(
            vec![
                Some("failed_login"), // 含 "fail"、以 "fail" 开头、以 "login" 结尾
                Some("login_fail"),   // 含 "fail"、不以 "fail" 开头、以 "fail" 结尾
                Some("success"),      // 都不命中
                None,                 // null
                Some("FAILED"),       // 大小写敏感 → 不命中
            ],
            vec![Some("fail"), Some("login"), Some("fail"), Some("fail"), None],
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
        );
        // 字面量 needle。
        for (name, expected) in [
            ("contains", vec![true, true, false, false, false]),
            ("startswith", vec![true, false, false, false, false]),
            ("endswith", vec![false, true, false, false, false]),
        ] {
            let expr = str_func_call(name, field("action"), Expr::StringLit("fail".into()));
            assert!(
                wf_lang::columnar::expr_is_columnar(&expr),
                "{name} 字面量形态应列式"
            );
            let mask = {
                let view = ColumnarBatch::from_all_fields(&batch);
                eval_guard_columnar(&expr, &view)
            };
            for (row, want) in expected.iter().enumerate() {
                assert_eq!(mask.value(row), *want, "{name} row {row}");
            }
            assert_equiv(&expr, &batch);
        }

        // 字段 needle（pattern 列）：null pattern 行 → null → false。
        let expr = str_func_call("contains", field("action"), field("pattern"));
        assert!(wf_lang::columnar::expr_is_columnar(&expr));
        assert_equiv(&expr, &batch);
        let sw = str_func_call("startswith", field("action"), field("pattern"));
        assert!(wf_lang::columnar::expr_is_columnar(&sw));
        assert_equiv(&sw, &batch);
        let ew = str_func_call("endswith", field("action"), field("pattern"));
        assert!(wf_lang::columnar::expr_is_columnar(&ew));
        assert_equiv(&ew, &batch);

        // 组合：contains(..., "fail") && count > 1 → 整体列式且逐位一致。
        let combo = bin(
            BinOp::And,
            str_func_call("contains", field("action"), Expr::StringLit("fail".into())),
            bin(BinOp::Gt, field("count"), num(1.0)),
        );
        assert!(wf_lang::columnar::expr_is_columnar(&combo));
        assert_equiv(&combo, &batch);

        // 空 needle 语义与解释一致（starts_with("") == true）。
        let empty = str_func_call("contains", field("action"), Expr::StringLit(String::new()));
        assert!(wf_lang::columnar::expr_is_columnar(&empty));
        assert_equiv(&empty, &batch);
    }
}
