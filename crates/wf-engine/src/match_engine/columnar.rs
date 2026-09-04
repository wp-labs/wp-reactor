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
//!
//! 子模块边界（#[path] sibling）：`columnar_compile`（guard/yield 编译面）、
//! `columnar_eval`（列式求值核）、`columnar_tests`（cfg(test) 对拍）——公开面逐
//! 路径经下方 re-export 保持。

use arrow::array::{Array, BooleanArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;
use wf_lang::ast::{BinOp, Expr, FieldRef};

use super::cep::{EngineHashMap, Value, field_ref_name};
use crate::match_engine::{WFL_FIELD_TYPE_ARRAY, wfl_structured_field_kind};

// 子模块 #[path] sibling 文件：类型/视图核心（ColumnarBatch/ColRef/GuardMasks/
// ColumnExpr）声明留本层（子模块经 `use super::*` 复用其私有字段与方法，见上）。
#[path = "columnar_compile.rs"]
mod columnar_compile;
#[path = "columnar_eval.rs"]
mod columnar_eval;

pub(crate) use columnar_compile::{
    arg_reads_structured, compile_guard, compile_yield_cvec, eval_compiled_guard, inline_lets,
    materialize_fields,
};
pub(crate) use columnar_eval::CVec;

#[cfg(test)]
#[path = "columnar_tests.rs"]
mod tests;

/// Three-valued scalar read from an Arrow column — the scalar subset of
/// [`super::cep::Value`], plus `Structured` for a non-null
/// `Value::Object` / `Value::Array` (e.g. a whole array field read bare).
/// `Int` carries native integer precision for `Int64` / `Timestamp(Ns)`
/// columns and integer-valued literals.
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.ColumnarBatch")]
pub(crate) enum CScalar {
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
#[derive(::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.ColumnarBatch")]
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

    fn resolve_field(&self, field: &FieldRef) -> ColRef {
        let Some(proj_idx) = self.field_map.get(field_ref_name(field)) else {
            return ColRef {
                proj: 0,
                kind: ColKind::Null,
            };
        };
        let col_idx = self.projection[*proj_idx];
        let col = self.batch.column(col_idx);
        // A `Utf8` column marked as a structured JSON array (the frame storage
        // shape for `array/...` schema fields) reads as a JSON-array column.
        if matches!(col.data_type(), DataType::Utf8)
            && wfl_structured_field_kind(self.batch.schema().field(col_idx))
                == Some(WFL_FIELD_TYPE_ARRAY)
        {
            return ColRef {
                proj: *proj_idx,
                kind: ColKind::JsonArray,
            };
        }
        ColRef {
            proj: *proj_idx,
            kind: col_kind(col.data_type()),
        }
    }

    /// Eval-time column resolution: the projection slot behind a compiled
    /// [`ColRef`] maps to a concrete batch column. `Null` (or a stale slot from
    /// a reused tree whose schema no longer matches) yields `None` → the read
    /// kernels degrade to all-null, matching the compiled `ColKind::Null`.
    fn column_at(&self, col: &ColRef) -> Option<&dyn Array> {
        if col.kind == ColKind::Null {
            return None;
        }
        let col_idx = self.projection.get(col.proj)?;
        Some(self.batch.column(*col_idx))
    }
}

/// Batch-level columnar **branch-guard** masks（2026-09-04 P4-B0 下沉
/// `wf_cep::masks`，本层 re-export 保 `crate::match_engine::columnar::GuardMasks`
/// 路径与可见级；consumers 见 `wf_cep::masks`）。
pub use wf_cep::masks::GuardMasks;

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

/// Compile-time column type tag — batch **independent** (a projection slot +
/// the Arrow type the schema declared when the tree was compiled). The compiled
/// [`ColumnExpr`] tree carries these instead of `&'a Array` references, so the
/// same tree can be reused across batches of a window (same schema) without
/// recompiling per batch. At eval time the view resolves the projection slot
/// and downcasts by kind; a downcast failure reads null (the batch no longer
/// matches the compiled schema — defensive, mirrors `ColKind::Null`).
///
/// `JsonArray` is a `Utf8` column whose field metadata marks it as a structured
/// JSON array (`wf.wfl.field_type = "array"`): each cell holds JSON array text
/// like `["prod","edge"]`. `List` / `LargeList` / `FixedSizeList` are native
/// Arrow list columns. All four carry the array shape used by
/// [`ColumnExpr::ListIndex`]; read as a bare field they are a non-null
/// structured value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.ColumnarBatch")]
pub(crate) enum ColKind {
    Int64,
    Float64,
    Utf8,
    Bool,
    TimestampNs,
    JsonArray,
    List,
    LargeList,
    FixedSizeList,
    /// Field absent from the schema / unsupported type — reads null, matching
    /// `event_bridge::extract_value`.
    Null,
}

/// A resolved, typed reference to a batch column: a projection slot index into
/// the eval-time [`ColumnarBatch`] plus the compile-time [`ColKind`]. Carrying
/// no `'a`, it is the leaf of a reusable compiled [`ColumnExpr`] tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.ColumnarBatch")]
pub(crate) struct ColRef {
    proj: usize,
    kind: ColKind,
}

/// Map an Arrow `DataType` to its [`ColKind`] tag (mirrors the old
/// `col_ref_from_array` downcasts; unsupported types read null).
fn col_kind(data_type: &DataType) -> ColKind {
    match data_type {
        DataType::Int64 => ColKind::Int64,
        DataType::Float64 => ColKind::Float64,
        DataType::Utf8 => ColKind::Utf8,
        DataType::Boolean => ColKind::Bool,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => ColKind::TimestampNs,
        DataType::List(_) => ColKind::List,
        DataType::LargeList(_) => ColKind::LargeList,
        DataType::FixedSizeList(_, _) => ColKind::FixedSizeList,
        _ => ColKind::Null,
    }
}

/// The string-search operation of a [`ColumnExpr::StrFunc`] node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.ColumnarBatch")]
pub(crate) enum StrFuncOp {
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
pub(crate) enum Needle {
    Lit(SmolStr),
    Col(ColRef),
}

/// A precompiled columnar expression tree — **batch-independent**: leaf columns
/// are [`ColRef`] (projection slot + type tag), so the tree is built once and
/// reused across batches of a window (same schema) instead of recompiling per
/// batch. At eval time the view resolves each [`ColRef`] to its column; the
/// per-row hot loop still reads native columns with no `HashMap` lookup.
#[derive(::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.ColumnarBatch")]
pub(crate) enum ColumnExpr {
    Lit(CScalar),
    Col(ColRef),
    /// `root[i]` — the `i`-th **non-null** element of the array column `col`,
    /// per row. Mirror of the interpreted path walk: a null / non-array cell,
    /// a non-array root column, a parse failure, or an out-of-range index all
    /// read null (the path produces `None`); object / array elements read a
    /// [`CScalar::Structured`] (definite false on compare, null as boolean).
    ListIndex {
        col: ColRef,
        index: usize,
    },
    Neg(Box<ColumnExpr>),
    Not(Box<ColumnExpr>),
    And(Box<ColumnExpr>, Box<ColumnExpr>),
    Or(Box<ColumnExpr>, Box<ColumnExpr>),
    Cmp {
        op: BinOp,
        left: Box<ColumnExpr>,
        right: Box<ColumnExpr>,
    },
    Arith {
        op: BinOp,
        left: Box<ColumnExpr>,
        right: Box<ColumnExpr>,
    },
    /// `cidr_match(field, "addr/prefix")` — lowered natively: the subnet is
    /// parsed at compile time (once per **compiled tree**, reused across
    /// batches; the checker enforces a literal), the
    /// field reads as a string column, and each non-null cell is parsed as an
    /// IP and compared against the net (mirroring the interpreted path exactly:
    /// non-Utf8 columns / null cells / non-IP strings read null / false).
    CidrMatch {
        col: ColRef,
        net: wf_lang::cidr::Cidr,
    },
    /// `regex_match(field, "pattern")` — lowered natively: the regex is
    /// compiled once per **compiled tree** (mirroring `CidrMatch`), the
    /// field reads as a string column, and each non-null
    /// cell is matched against the compiled regex (non-Utf8 columns / null
    /// cells read null, mirroring the interpreted `Value::Str`-only path).
    RegexMatch {
        col: ColRef,
        re: regex::Regex,
    },
    /// `contains` / `startswith` / `endswith` — lowered natively over two
    /// string operands. The haystack is always a flat field (string column);
    /// the needle is a shared literal or a second string column. Non-Utf8 /
    /// null cells read null, mirroring the interpreted `Value::Str`-only path.
    StrFunc {
        op: StrFuncOp,
        hay: ColRef,
        needle: Needle,
    },
    /// `fmt(template, v1, ...)` — yield-cell output function (on-each / match /
    /// close columnar output): a literal template rendered over per-row scalar
    /// arguments (`value_to_string`, byte-identical to the interpreted path).
    /// Any argument cell that reads null renders the whole row null, matching
    /// `apply_fmt_template`'s `None` (the interpreted `eval_yield` substitutes
    /// an empty string for a missing field *before* the call).
    Fmt {
        template: SmolStr,
        args: Vec<ColumnExpr>,
    },
    /// `strftime(ts, [fmt])` — yield-cell output function: a numeric epoch
    /// nanos cell formatted via chrono with the (literal or default) format.
    /// Null / non-numeric cells read null, matching the interpreted path.
    Strftime {
        ts: Box<ColumnExpr>,
        fmt: SmolStr,
    },
    /// `count_char(text, ch)` — yield-cell output function: occurrence count of
    /// `ch`'s first char in `text` → numeric, matching the interpreted path.
    CountChar {
        text: Box<ColumnExpr>,
        needle: Box<ColumnExpr>,
    },
    /// `mvindex(split(field, sep), idx)` — q22 let 形态融合节点（编译期内联
    /// let 后得到）：per row 分割字符串一次、`normalize_index` 取第 idx 个元素。
    /// 空 sep → 按字符切分（与解释 `split` 的 chars 分支一致）；非 Utf8 /
    /// null cell / 越界 / 空串 → null（解释路径 `split` 非 Str → None、
    /// `arr.get(idx)` 越界 → None）。
    SplitIndex {
        col: ColRef,
        sep: SmolStr,
        index: i64,
    },
    /// `concat(a, b, ...)` — 字符串拼接（`value_to_string` 渲染，与解释路径
    /// 逐参 eval + value_to_string 字节一致）；任一参数 cell null → 整行 null
    /// （解释路径 `?` 传播 → yield 空串）。
    Concat {
        args: Vec<ColumnExpr>,
    },
    /// `expr in (lit, ...)` — per-row value membership over the compile-time
    /// literal list (`values_equal` semantics, `negated` flips). Target null /
    /// non-literal list items read null / false, matching the interpreted path.
    InList {
        expr: Box<ColumnExpr>,
        list: Vec<Value>,
        negated: bool,
    },
    /// `if cond then a else b` — per-row three-valued pick over the Bool
    /// condition column; a non-Bool / null cond reads null, matching the
    /// interpreted path.
    IfThenElse {
        cond: Box<ColumnExpr>,
        then_expr: Box<ColumnExpr>,
        else_expr: Box<ColumnExpr>,
    },
}

/// Evaluate a columnar guard expression over every row of `view`, producing one
/// boolean per row. Null / non-boolean / missing-field rows are emitted as
/// **null slots** (so permissive consumers can distinguish them); two-valued
/// consumers read null as `false` via [`BooleanArray::value`], matching the
/// interpreted `passes_bind_filter` → `false` fallback.
///
/// Compiles the expression per call; hot callers that repeat the same filter
/// over many batches should cache [`compile_guard`] and reuse
/// [`eval_compiled_guard`] instead (see `RuleExecutor::compiled_guards`).
pub fn eval_guard_columnar(expr: &Expr, view: &ColumnarBatch<'_>) -> BooleanArray {
    match compile_guard(expr, view) {
        Some(plan) => eval_compiled_guard(&plan, view),
        // Non-columnar expression (the gate keeps these out): all rows miss.
        None => BooleanArray::from(vec![false; view.num_rows()]),
    }
}

/// Convert a columnar scalar cell to its interpreted [`Value`] equivalent
/// (yield-cell consumers of the batch-evaluated output columns). Structured
/// cells are unreachable here: the columnar-output gate excludes structured
/// fields from `fmt`/`strftime`/`count_char` arguments (they compile to `None`
/// and fall back to the interpreted per-row path).
pub(crate) fn cscalar_to_value(s: &CScalar) -> Value {
    match s {
        CScalar::Int(i) => Value::Number(*i as f64),
        CScalar::Float(f) => Value::Number(*f),
        CScalar::Str(s) => Value::Str(s.clone()),
        CScalar::Bool(b) => Value::Bool(*b),
        CScalar::Structured => Value::Array(Vec::new()),
    }
}
