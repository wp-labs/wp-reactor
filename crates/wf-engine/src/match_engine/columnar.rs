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

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, FixedSizeListArray, Float64Array,
    Float64Builder, Int64Array, LargeListArray, ListArray, StringArray, StringBuilder,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field as ArrowField, TimeUnit};
use arrow::record_batch::RecordBatch;
use smol_str::SmolStr;
use wf_lang::ast::{BinOp, Expr, FieldRef, MatchArm, PathSegment};

use super::match_engine::eval::cmp::{apply_fmt_template, timestamp_nanos_to_utc};
use super::match_engine::{EngineHashMap, Value, field_ref_name, value_to_string, values_equal};
use crate::match_engine::{WFL_FIELD_TYPE_ARRAY, wfl_structured_field_kind};
use crate::time::normalize_epoch_timestamp_float_nanos;

/// Three-valued scalar read from an Arrow column — the scalar subset of
/// [`super::match_engine::Value`], plus `Structured` for a non-null
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

/// Batch-level columnar **branch-guard** masks for the three guard sites the
/// state machine evaluates per event:
///
/// - `event` — `match_plan.event_steps` (keyed `(event_step_idx, branch_idx)`);
/// - `close` — `match_plan.close_steps` accumulation guard (keyed
///   `(close_step_idx, branch_idx)`);
/// - `neg` — `match_plan.seq` negation steps (keyed `(neg_idx, 0)`, the same
///   negation-only ordering `SeqRuntime::build` produces).
#[derive(Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Engine", module = "Engine.ColumnarBatch")]
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

/// Compile a (gate-admitted) guard into a batch-independent [`ColumnExpr`]
/// tree. `None` = not compilable (a non-columnar shape — the gate keeps these
/// out — or an invalid constant literal that `Cidr::parse` / `Regex::new`
/// reject, which reads as all-false, matching the interpreted path).
pub(crate) fn compile_guard(expr: &Expr, view: &ColumnarBatch<'_>) -> Option<ColumnExpr> {
    compile_expr(expr, view)
}

/// 统一入口：把 yield 字段的 General 表达式（fmt/strftime/count_char/split/
/// mvindex/concat 等输出函数 + 任意可列式表达式）编译为批级 cell。Lit/Field 走
/// 各自快通道不编译；编译失败（结构化列参数等）→ `None` → 调用方逐行解释回退。
///
/// each（`each_batch_prepare`）与 close（`close_batch_prepare`）共用——同一
/// 编译语义保证两条列式 emit 路径字节一致（2026-08-25 层 1：close 输出链
/// 列式化；层 2：q22 let+split+mvindex+concat 形态）。`lets` 供编译期内联
/// （`Field(Simple(let_name))` → let RHS，见 [`inline_lets`]）；close 路径传
/// 空（解释 close 无 let 视图，内联会与解释路径分叉）。
pub(crate) fn compile_yield_cvec(
    field: &wf_lang::plan::YieldField,
    view: &ColumnarBatch<'_>,
    n: usize,
    lets: &[wf_lang::plan::LetPlan],
) -> Option<CVec> {
    let value: std::borrow::Cow<'_, Expr> = if lets.is_empty() {
        std::borrow::Cow::Borrowed(&field.value)
    } else {
        std::borrow::Cow::Owned(inline_lets(&field.value, lets, &mut Vec::new()))
    };
    match &*value {
        Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => None,
        // flat Field 走各自快通道（不编译）；list-index 字段（`c.tags[0]`，
        // gap-5 2026-09-02）编译为 ListIndex cvec——快通道 `value_at` 只读
        // flat 列，索引元素需 offset 读。
        Expr::Field(fr) if !wf_lang::columnar::field_ref_is_list_index(fr) => None,
        other if wf_lang::columnar::expr_is_columnar(other) => {
            compile_guard(other, view).map(|plan| plan.eval_vec(view, n))
        }
        _ => {
            let is_output_func = matches!(
                &*value,
                Expr::FuncCall {
                    qualifier: None,
                    name,
                    ..
                } if wf_lang::columnar::columnar_output_func(name).is_some()
            );
            if is_output_func {
                compile_guard(&value, view).map(|plan| plan.eval_vec(view, n))
            } else {
                None
            }
        }
    }
}

/// 编译期内联 let 绑定（q22 形态）：把 `Field(Simple(let_name))` 替换为 let
/// RHS 表达式（递归内联，let 可引用更早的 let）——列式视图只有 schema 列、无
/// let 视图，解释路径 `apply_lets` 逐行注入的语义靠内联展开等价。`visiting`
/// 防自引用死循环：引用自己时保持原 Field（编译成 Null ColRef → null），与
/// 解释路径（自引用 let 求值读缺字段 → None → 不注入）同义。
pub(crate) fn inline_lets(
    expr: &Expr,
    lets: &[wf_lang::plan::LetPlan],
    visiting: &mut Vec<String>,
) -> Expr {
    match expr {
        Expr::Field(FieldRef::Simple(name)) => {
            if !visiting.iter().any(|v| v == name)
                && let Some(rhs) = lets.iter().find(|l| &l.name == name)
            {
                visiting.push(name.clone());
                let out = inline_lets(&rhs.expr, lets, visiting);
                visiting.pop();
                return out;
            }
            expr.clone()
        }
        Expr::BinOp { op, left, right } => Expr::BinOp {
            op: *op,
            left: Box::new(inline_lets(left, lets, visiting)),
            right: Box::new(inline_lets(right, lets, visiting)),
        },
        Expr::Neg(inner) => Expr::Neg(Box::new(inline_lets(inner, lets, visiting))),
        Expr::Not(inner) => Expr::Not(Box::new(inline_lets(inner, lets, visiting))),
        Expr::Array(items) => Expr::Array(
            items
                .iter()
                .map(|i| inline_lets(i, lets, visiting))
                .collect(),
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(inline_lets(expr, lets, visiting)),
            list: list
                .iter()
                .map(|i| inline_lets(i, lets, visiting))
                .collect(),
            negated: *negated,
        },
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => Expr::IfThenElse {
            cond: Box::new(inline_lets(cond, lets, visiting)),
            then_expr: Box::new(inline_lets(then_expr, lets, visiting)),
            else_expr: Box::new(inline_lets(else_expr, lets, visiting)),
        },
        Expr::Match {
            expr,
            arms,
            default,
        } => Expr::Match {
            expr: Box::new(inline_lets(expr, lets, visiting)),
            arms: arms
                .iter()
                .map(|arm| MatchArm {
                    patterns: arm
                        .patterns
                        .iter()
                        .map(|p| inline_lets(p, lets, visiting))
                        .collect(),
                    value: inline_lets(&arm.value, lets, visiting),
                })
                .collect(),
            default: default
                .as_ref()
                .map(|d| Box::new(inline_lets(d, lets, visiting))),
        },
        Expr::Object(items) => Expr::Object(
            items
                .iter()
                .map(|it| wf_lang::ast::ObjectItem {
                    targets: it.targets.clone(),
                    type_hint: it.type_hint.clone(),
                    value: inline_lets(&it.value, lets, visiting),
                })
                .collect(),
        ),
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => Expr::FuncCall {
            qualifier: qualifier.clone(),
            name: name.clone(),
            args: args
                .iter()
                .map(|a| inline_lets(a, lets, visiting))
                .collect(),
        },
        _ => expr.clone(),
    }
}

/// 统一字段物化器（层 2 收口，2026-08-25）：把任意行式输入（`CloseOutput` /
/// `MatchedContext` / `Event` 数组）的引用字段物化为 Arrow 列，供
/// `ColumnarBatch` 视图 + [`compile_yield_cvec`] 列式求值。
///
/// 两遍直推（类型探测 + 直写 builder，无 `Value` 中间态）。全 None 列 → 不建
/// 列（视图解析为 Null ColKind → null cell，与 ctx 缺字段一致）；类型不一致 /
/// 结构化值（Array/Object）→ `None` → 调用方整体回退逐行（保守）。
/// Number→Float64 / Str→Utf8 / Bool→Boolean（`cscalar_to_value` 还原为原
/// `Value`，渲染字节一致）。
pub(crate) fn materialize_fields<F>(
    ref_fields: &[String],
    n: usize,
    mut resolve: F,
) -> Option<(Vec<ArrowField>, Vec<ArrayRef>)>
where
    F: FnMut(usize, &str) -> Option<Value>,
{
    #[derive(Clone, Copy)]
    enum ColKind {
        Num,
        Str,
        Bool,
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ref_fields.len());
    let mut schema_fields: Vec<ArrowField> = Vec::with_capacity(ref_fields.len());
    for fname in ref_fields {
        // pass 1：类型探测（第一个非 None 值变体）
        let mut kind: Option<ColKind> = None;
        for row in 0..n {
            match resolve(row, fname) {
                Some(Value::Number(_)) => {
                    kind = Some(ColKind::Num);
                    break;
                }
                Some(Value::Str(_)) => {
                    kind = Some(ColKind::Str);
                    break;
                }
                Some(Value::Bool(_)) => {
                    kind = Some(ColKind::Bool);
                    break;
                }
                Some(_) => return None, // 结构化 → 整批回退逐行
                None => {}
            }
        }
        let Some(kind) = kind else {
            continue; // 全缺失 → 不建列（Null ColKind）
        };
        // pass 2：直写 builder（无 Value 中间态）
        let array: ArrayRef = match kind {
            ColKind::Num => {
                let mut b = Float64Builder::with_capacity(n);
                for row in 0..n {
                    match resolve(row, fname) {
                        Some(Value::Number(f)) => b.append_value(f),
                        Some(_) => return None,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ColKind::Str => {
                let mut b = StringBuilder::with_capacity(n, n * 16);
                for row in 0..n {
                    match resolve(row, fname) {
                        Some(Value::Str(s)) => b.append_value(s.as_str()),
                        Some(_) => return None,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            ColKind::Bool => {
                let mut b = BooleanBuilder::with_capacity(n);
                for row in 0..n {
                    match resolve(row, fname) {
                        Some(Value::Bool(x)) => b.append_value(x),
                        Some(_) => return None,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        schema_fields.push(ArrowField::new(
            fname.as_str(),
            array.data_type().clone(),
            true,
        ));
        arrays.push(array);
    }
    Some((schema_fields, arrays))
}

/// Evaluate a compiled guard tree over every row of `view` (one `BooleanArray`
/// per batch, null slots preserved). The same tree is reusable across batches
/// of the same schema.
pub(crate) fn eval_compiled_guard(plan: &ColumnExpr, view: &ColumnarBatch<'_>) -> BooleanArray {
    let out = plan.eval_vec(view, view.num_rows());
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

fn compile_expr(expr: &Expr, view: &ColumnarBatch<'_>) -> Option<ColumnExpr> {
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
        // 原生列式函数：守卫（cidr/regex/strsearch）与输出（fmt/strftime/
        // count_char）两套清单，单一权威来源（`columnar_func` /
        // `columnar_output_func`）。常量在编译期解析一次，字段解析为其列。
        // 列式输出函数（fmt/strftime/count_char）——用于 yield cell 批量求值。
        // 参数形状由 `columnar_output_expr` 保证（flat 字段/字面量）；编译失败的
        // 输出表达式（如结构化列参数）由调用方回落行式解释。
        Expr::FuncCall {
            qualifier: None,
            name,
            args,
        } => {
            if let Some(func) = wf_lang::columnar::columnar_func(name) {
                compile_guard_func(name, func, args, view)
            } else if wf_lang::columnar::columnar_output_func(name).is_some() {
                compile_output_func(name, args, view)
            } else {
                None
            }
        }
        // `expr in (lit, ...)` — 值 ∈ 编译期字面量列表（Q14 fmt 参数的
        // strftime(...) in (...)：`if x in ("00","01","02") then ...`）。
        // 列表项限定字面量（gate 保证）；其他形状回落解释器。
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let list_values: Option<Vec<Value>> = list
                .iter()
                .map(|item| match item {
                    Expr::Number(n) => Some(Value::Number(*n)),
                    Expr::StringLit(s) => Some(Value::Str(s.clone().into())),
                    Expr::Bool(b) => Some(Value::Bool(*b)),
                    _ => None,
                })
                .collect();
            Some(ColumnExpr::InList {
                expr: Box::new(compile_expr(expr, view)?),
                list: list_values?,
                negated: *negated,
            })
        }
        // `if c then a else b` — 三值条件选值（Q14 fmt 参数的 dayTime/nightTime）。
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => Some(ColumnExpr::IfThenElse {
            cond: Box::new(compile_expr(cond, view)?),
            then_expr: Box::new(compile_expr(then_expr, view)?),
            else_expr: Box::new(compile_expr(else_expr, view)?),
        }),
        _ => None,
    }
}

/// Compile a gate-admitted guard function (`cidr_match` / `regex_match` /
/// `contains` / `startswith` / `endswith`) into a [`ColumnExpr`] node.
fn compile_guard_func(
    name: &str,
    func: wf_lang::columnar::ColumnarFunc,
    args: &[Expr],
    view: &ColumnarBatch<'_>,
) -> Option<ColumnExpr> {
    // 门控已保证形态（`columnar_func_args_ok`），这里再防御性校验。
    if !wf_lang::columnar::columnar_func_args_ok(func, args) {
        return None;
    }
    let Expr::Field(field) = &args[0] else {
        unreachable!("columnar_func_args_ok 保证 args[0] 为 flat 字段");
    };
    let col = view.resolve_field(field);
    match func {
        wf_lang::columnar::ColumnarFunc::CidrMatch
        | wf_lang::columnar::ColumnarFunc::RegexMatch => {
            let Expr::StringLit(constant) = &args[1] else {
                unreachable!("columnar_func_args_ok 保证 args[1] 为字面量");
            };
            match func {
                wf_lang::columnar::ColumnarFunc::CidrMatch => Some(ColumnExpr::CidrMatch {
                    col,
                    net: wf_lang::cidr::Cidr::parse(constant)?,
                }),
                _ => Some(ColumnExpr::RegexMatch {
                    col,
                    re: regex::Regex::new(constant).ok()?,
                }),
            }
        }
        wf_lang::columnar::ColumnarFunc::StrSearch => {
            let op = StrFuncOp::from_name(name).expect("columnar_func 已确认名字");
            let needle = match &args[1] {
                Expr::StringLit(s) => Needle::Lit(s.clone().into()),
                Expr::Field(
                    FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _),
                ) => {
                    let Expr::Field(f) = &args[1] else {
                        unreachable!("columnar_func_args_ok 保证 args[1] 为字段");
                    };
                    Needle::Col(view.resolve_field(f))
                }
                _ => unreachable!("columnar_func_args_ok 保证 args[1] 形态"),
            };
            Some(ColumnExpr::StrFunc {
                op,
                hay: col,
                needle,
            })
        }
    }
}

/// 输出函数参数（**递归**）是否读取结构化列（`wf.wfl.field_type` = array/object
/// 元数据）。结构化列在解释路径解析成 `Value::Array`/`Value::Object`（fmt 渲染
/// `[array]`/`[object]`、count_char 对非 Str → None），列式读原始 JSON 文本
/// （OBJECT）或 `CScalar::Structured`（ARRAY）——OBJECT 列的原始文本会被
/// fmt 直接渲染、count_char 对其计数，字节分叉 → 相关输出表达式整体回退行式。
/// 递归覆盖 IfThenElse/InList/嵌套函数调用：结构化字段藏在分支里时 gate 仍会
/// 放行（flat FieldRef 不含元数据信息），必须在此编译期拦截。
pub(crate) fn arg_reads_structured(view: &ColumnarBatch<'_>, expr: &Expr) -> bool {
    match expr {
        Expr::Field(field) => {
            let Some(&proj) = view.field_map.get(field_ref_name(field)) else {
                return false;
            };
            let col_idx = view.projection[proj];
            wfl_structured_field_kind(view.batch.schema().field(col_idx)).is_some()
        }
        Expr::BinOp { left, right, .. } => {
            arg_reads_structured(view, left) || arg_reads_structured(view, right)
        }
        Expr::Neg(inner) | Expr::Not(inner) => arg_reads_structured(view, inner),
        Expr::FuncCall { args, .. } => args.iter().any(|a| arg_reads_structured(view, a)),
        Expr::InList { expr, list, .. } => {
            arg_reads_structured(view, expr) || list.iter().any(|a| arg_reads_structured(view, a))
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            arg_reads_structured(view, cond)
                || arg_reads_structured(view, then_expr)
                || arg_reads_structured(view, else_expr)
        }
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            arg_reads_structured(view, expr)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(|p| arg_reads_structured(view, p))
                        || arg_reads_structured(view, &arm.value)
                })
                || default
                    .as_ref()
                    .is_some_and(|d| arg_reads_structured(view, d))
        }
        _ => false,
    }
}

/// Compile a gate-admitted output function (`fmt` / `strftime` / `count_char`)
/// into a yield-cell [`ColumnExpr`] node. Argument shapes are guaranteed by
/// `columnar_output_expr` (flat field / literal); a failure here (e.g. a
/// structured-array column argument) tells the caller to fall back to the
/// interpreted per-row path for that yield expression.
fn compile_output_func(name: &str, args: &[Expr], view: &ColumnarBatch<'_>) -> Option<ColumnExpr> {
    let func = wf_lang::columnar::columnar_output_func(name)?;
    // 结构化参数（ARRAY / OBJECT 元数据列，含 IfThenElse/InList/嵌套调用里的
    // 递归分支）→ 回退行式：解释路径解析成 Value::Array/Object 并渲染
    // `[array]`/`[object]`（fmt）或对非 Str 取 None（count_char/strftime），
    // 列式读原始 JSON 文本（OBJECT）渲染/计数字节不同。
    if args.iter().any(|a| arg_reads_structured(view, a)) {
        return None;
    }
    match func {
        wf_lang::columnar::ColumnarOutputFunc::Fmt => {
            let Expr::StringLit(template) = &args[0] else {
                return None;
            };
            let cargs: Option<Vec<ColumnExpr>> =
                args[1..].iter().map(|a| compile_expr(a, view)).collect();
            Some(ColumnExpr::Fmt {
                template: template.clone().into(),
                args: cargs?,
            })
        }
        wf_lang::columnar::ColumnarOutputFunc::Strftime => {
            if args.is_empty() || args.len() > 2 {
                return None;
            }
            let ts = compile_expr(&args[0], view)?;
            let fmt = match args.get(1) {
                Some(Expr::StringLit(f)) => f.clone(),
                Some(_) => return None,
                None => wf_config::DEFAULT_OUTPUT_TIME_FORMAT.to_string(),
            };
            Some(ColumnExpr::Strftime {
                ts: Box::new(ts),
                fmt: fmt.into(),
            })
        }
        wf_lang::columnar::ColumnarOutputFunc::CountChar => {
            if args.len() != 2 {
                return None;
            }
            Some(ColumnExpr::CountChar {
                text: Box::new(compile_expr(&args[0], view)?),
                needle: Box::new(compile_expr(&args[1], view)?),
            })
        }
        wf_lang::columnar::ColumnarOutputFunc::Split => {
            // split 只作为 mvindex 的 list 参数被融合（SplitIndex）；独立列式
            // 无列表值类型 → 编译失败，调用方回落行式。
            None
        }
        wf_lang::columnar::ColumnarOutputFunc::MvIndex => {
            // mvindex(list, idx)：list 必须是 `split(flat_field, "lit")`（let
            // 内联后的形态）→ 融合为 SplitIndex { col, sep, index }。其他
            // list 形态（字段列表、Path 等）→ None → 行式回退。
            if args.len() != 2 {
                return None;
            }
            let index = match &args[1] {
                Expr::Number(n) => n.trunc() as i64,
                _ => return None,
            };
            let Expr::FuncCall {
                qualifier: None,
                name,
                args: list_args,
            } = &args[0]
            else {
                return None;
            };
            if name != "split" || list_args.len() != 2 {
                return None;
            }
            let Expr::Field(text_field) = &list_args[0] else {
                return None;
            };
            // 仅 flat 字段（Simple/Qualified/Bracketed）——Path 的语义在
            // 解释路径作用于索引后的值，融合节点只读原始列，分叉 → 回退。
            if !matches!(
                text_field,
                FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)
            ) {
                return None;
            }
            let Expr::StringLit(sep) = &list_args[1] else {
                return None;
            };
            Some(ColumnExpr::SplitIndex {
                col: view.resolve_field(text_field),
                sep: sep.clone().into(),
                index,
            })
        }
        wf_lang::columnar::ColumnarOutputFunc::Concat => {
            let cargs: Option<Vec<ColumnExpr>> =
                args.iter().map(|a| compile_expr(a, view)).collect();
            Some(ColumnExpr::Concat { args: cargs? })
        }
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
#[derive(::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Engine", module = "Engine.ColumnarBatch")]
pub(crate) enum CVec {
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
    /// Per-row [`CScalar`] view (used by compare / arithmetic kernels and by
    /// columnar yield-cell consumers of batch-evaluated output columns).
    pub(crate) fn scalar_at(&self, row: usize) -> Option<CScalar> {
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
    pub(crate) fn bool_at(&self, row: usize) -> Option<bool> {
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

impl ColumnExpr {
    /// Evaluate this node over the whole batch (vectorized) into a typed column.
    /// One linear pass per node; intermediate columns are materialized and flow
    /// bottom-up to the root. The `view` resolves compiled [`ColRef`] leaves to
    /// concrete columns for *this* batch — the same tree evaluates any batch of
    /// the window's schema.
    pub(crate) fn eval_vec(&self, view: &ColumnarBatch<'_>, n: usize) -> CVec {
        match self {
            ColumnExpr::Lit(v) => lit_vec(v, n),
            ColumnExpr::Col(col) => view.col_vec(col, n),
            ColumnExpr::ListIndex { col, index } => view.list_index_vec(col, *index, n),
            ColumnExpr::Neg(inner) => neg_vec(inner.eval_vec(view, n)),
            ColumnExpr::Not(inner) => not_vec(inner.eval_vec(view, n)),
            ColumnExpr::And(left, right) => {
                logic_vec::<true>(left.eval_vec(view, n), right.eval_vec(view, n))
            }
            ColumnExpr::Or(left, right) => {
                logic_vec::<false>(left.eval_vec(view, n), right.eval_vec(view, n))
            }
            ColumnExpr::Cmp { op, left, right } => {
                cmp_vec(*op, left.eval_vec(view, n), right.eval_vec(view, n))
            }
            ColumnExpr::Arith { op, left, right } => {
                arith_vec(*op, left.eval_vec(view, n), right.eval_vec(view, n))
            }
            ColumnExpr::CidrMatch { col, net } => view.cidr_vec(col, net, n),
            ColumnExpr::RegexMatch { col, re } => view.regex_vec(col, re, n),
            ColumnExpr::StrFunc { op, hay, needle } => view.strfunc_vec(*op, hay, needle, n),
            ColumnExpr::Fmt { template, args } => {
                let arg_vecs: Vec<CVec> = args.iter().map(|a| a.eval_vec(view, n)).collect();
                fmt_vec(template, &arg_vecs, n)
            }
            ColumnExpr::Strftime { ts, fmt } => strftime_vec(ts.eval_vec(view, n), fmt, n),
            ColumnExpr::CountChar { text, needle } => {
                count_char_vec(text.eval_vec(view, n), needle.eval_vec(view, n), n)
            }
            ColumnExpr::SplitIndex { col, sep, index } => view.split_index_vec(col, sep, *index, n),
            ColumnExpr::Concat { args } => {
                let arg_vecs: Vec<CVec> = args.iter().map(|a| a.eval_vec(view, n)).collect();
                concat_vec(&arg_vecs, n)
            }
            ColumnExpr::InList {
                expr,
                list,
                negated,
            } => inlist_vec(expr.eval_vec(view, n), list, *negated, n),
            ColumnExpr::IfThenElse {
                cond,
                then_expr,
                else_expr,
            } => ifthenelse_vec(
                cond.eval_vec(view, n),
                then_expr.eval_vec(view, n),
                else_expr.eval_vec(view, n),
                n,
            ),
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

/// Vectorized column-leaf kernels — `ColumnarBatch` methods so a compiled
/// (batch-independent) [`ColumnExpr`] tree resolves its [`ColRef`] leaves to
/// *this* batch's columns at eval time. A downcast failure (stale reused tree /
/// schema drift) degrades to all-null, matching the compiled `ColKind::Null`.
impl ColumnarBatch<'_> {
    fn int64_array(&self, col: &ColRef) -> Option<&Int64Array> {
        self.column_at(col)
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
    }

    fn float64_array(&self, col: &ColRef) -> Option<&Float64Array> {
        self.column_at(col)
            .and_then(|a| a.as_any().downcast_ref::<Float64Array>())
    }

    fn string_array(&self, col: &ColRef) -> Option<&StringArray> {
        self.column_at(col)
            .and_then(|a| a.as_any().downcast_ref::<StringArray>())
    }

    fn bool_array(&self, col: &ColRef) -> Option<&BooleanArray> {
        self.column_at(col)
            .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
    }

    fn ts_array(&self, col: &ColRef) -> Option<&TimestampNanosecondArray> {
        self.column_at(col)
            .and_then(|a| a.as_any().downcast_ref::<TimestampNanosecondArray>())
    }

    fn list_array(&self, col: &ColRef) -> Option<&ListArray> {
        self.column_at(col)
            .and_then(|a| a.as_any().downcast_ref::<ListArray>())
    }

    fn large_list_array(&self, col: &ColRef) -> Option<&LargeListArray> {
        self.column_at(col)
            .and_then(|a| a.as_any().downcast_ref::<LargeListArray>())
    }

    fn fixed_size_list_array(&self, col: &ColRef) -> Option<&FixedSizeListArray> {
        self.column_at(col)
            .and_then(|a| a.as_any().downcast_ref::<FixedSizeListArray>())
    }

    /// Materialize a [`ColRef`] leaf into a typed column in a single pass. A
    /// `Timestamp(Ns)` column reads as native `i64`; a `Null` column (missing
    /// field / unsupported type) reads as all-null, matching `ColRef` → `None`.
    fn col_vec(&self, col: &ColRef, n: usize) -> CVec {
        match col.kind {
            ColKind::Null => CVec::Int(vec![None; n]),
            ColKind::Int64 => match self.int64_array(col) {
                Some(a) => CVec::Int(
                    (0..n)
                        .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                        .collect(),
                ),
                None => CVec::Int(vec![None; n]),
            },
            ColKind::TimestampNs => match self.ts_array(col) {
                Some(a) => CVec::Int(
                    (0..n)
                        .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                        .collect(),
                ),
                None => CVec::Int(vec![None; n]),
            },
            ColKind::Float64 => match self.float64_array(col) {
                Some(a) => CVec::Float(
                    (0..n)
                        .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                        .collect(),
                ),
                None => CVec::Float(vec![None; n]),
            },
            ColKind::Utf8 => match self.string_array(col) {
                Some(a) => CVec::Str(
                    (0..n)
                        .map(|r| (!a.is_null(r)).then(|| a.value(r).into()))
                        .collect(),
                ),
                None => CVec::Str(vec![None; n]),
            },
            ColKind::Bool => match self.bool_array(col) {
                Some(a) => CVec::Bool(
                    (0..n)
                        .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                        .collect(),
                ),
                None => CVec::Bool(vec![None; n]),
            },
            // Array-shaped columns read bare are a non-null structured value per
            // row (`Value::Array`), never a scalar — compares false, reads null as
            // a boolean, and is not numeric (byte-identical to interpreted).
            ColKind::JsonArray => match self.string_array(col) {
                Some(a) => structured_col(n, |r| !a.is_null(r)),
                None => structured_col(n, |_| false),
            },
            ColKind::List => match self.list_array(col) {
                Some(a) => structured_col(n, |r| !a.is_null(r)),
                None => structured_col(n, |_| false),
            },
            ColKind::LargeList => match self.large_list_array(col) {
                Some(a) => structured_col(n, |r| !a.is_null(r)),
                None => structured_col(n, |_| false),
            },
            ColKind::FixedSizeList => match self.fixed_size_list_array(col) {
                Some(a) => structured_col(n, |r| !a.is_null(r)),
                None => structured_col(n, |_| false),
            },
        }
    }

    /// Vectorized `cidr_match(field, net)` over a string column. Mirrors the
    /// interpreted path: only a `Utf8` column can carry an IP string; null cells
    /// read null; non-UTF8 / array-shaped / missing columns read all-null; a
    /// non-IP string parses to `false` (via `Cidr::contains`). The subnet is
    /// already parsed — this kernel never re-parses the CIDR.
    fn cidr_vec(&self, col: &ColRef, net: &wf_lang::cidr::Cidr, n: usize) -> CVec {
        match self.string_array(col) {
            Some(a) => CVec::Bool(
                (0..n)
                    .map(|r| (!a.is_null(r)).then(|| net.contains(a.value(r))))
                    .collect(),
            ),
            None => CVec::Bool(vec![None; n]),
        }
    }

    /// Vectorized `regex_match(field, re)` over a string column. Mirrors the
    /// interpreted path: only a `Utf8` column can carry a haystack; null cells
    /// read null; non-UTF8 / array-shaped / missing columns read all-null. The
    /// regex is already compiled — this kernel never recompiles it.
    fn regex_vec(&self, col: &ColRef, re: &regex::Regex, n: usize) -> CVec {
        match self.string_array(col) {
            Some(a) => CVec::Bool(
                (0..n)
                    .map(|r| (!a.is_null(r)).then(|| re.is_match(a.value(r))))
                    .collect(),
            ),
            None => CVec::Bool(vec![None; n]),
        }
    }

    /// Vectorized `contains` / `startswith` / `endswith` over string columns.
    /// Mirrors the interpreted path: both operands must be `Value::Str` (a `Utf8`
    /// column); null on either side reads null; non-Utf8 columns read all-null.
    /// A literal needle is shared across the row loop (no per-row clone).
    fn strfunc_vec(&self, op: StrFuncOp, hay: &ColRef, needle: &Needle, n: usize) -> CVec {
        let apply = |h: &str, nd: &str| match op {
            StrFuncOp::Contains => h.contains(nd),
            StrFuncOp::StartsWith => h.starts_with(nd),
            StrFuncOp::EndsWith => h.ends_with(nd),
        };
        match (self.string_array(hay), needle) {
            (Some(h), Needle::Lit(nd)) => CVec::Bool(
                (0..n)
                    .map(|r| (!h.is_null(r)).then(|| apply(h.value(r), nd)))
                    .collect(),
            ),
            (Some(h), Needle::Col(nc)) => match self.string_array(nc) {
                Some(nc) => CVec::Bool(
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
                None => CVec::Bool(vec![None; n]),
            },
            _ => CVec::Bool(vec![None; n]),
        }
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

/// Vectorized `fmt(template, args...)` yield-cell evaluation: per row, all
/// argument cells read (non-null) as scalars, render the template via
/// `apply_fmt_template` (byte-identical to the interpreted path). A null cell
/// or a placeholder-count mismatch reads null — the yield wrapper substitutes
/// an empty string, exactly like the interpreted `eval_yield_expr_with_meta`.
fn fmt_vec(template: &SmolStr, args: &[CVec], n: usize) -> CVec {
    let mut out = Vec::with_capacity(n);
    // 复用临时值缓冲区（apply_fmt_template 只借用不持有）：避免每行一次
    // Vec 分配——close/each 高输出量路径的 per-row 热点（2026-08-25 层 1）。
    let mut values: Vec<Value> = Vec::with_capacity(args.len());
    for row in 0..n {
        values.clear();
        let mut ok = true;
        for a in args {
            match a.scalar_at(row) {
                Some(s) => values.push(cscalar_to_value(&s)),
                None => {
                    ok = false;
                    break;
                }
            }
        }
        out.push(if ok {
            apply_fmt_template(template, &values).map(SmolStr::from)
        } else {
            None
        });
    }
    CVec::Str(out)
}

/// `mvindex(split(field, sep), idx)` 融合求值（q22 let 形态）：per row 读
/// Utf8 cell → 按 sep 分割（空 sep 按字符）→ `normalize_index` 取第 idx 个
/// 元素（负数从尾数；越界 / 空 / null / 非 Utf8 列 → null，与解释路径
/// `split` 非 Str → None + `arr.get(idx)` 越界 → None 一致）。
impl ColumnarBatch<'_> {
    fn split_index_vec(&self, col: &ColRef, sep: &SmolStr, index: i64, n: usize) -> CVec {
        let mut out: Vec<Option<SmolStr>> = Vec::with_capacity(n);
        let Some(arr) = self
            .column_at(col)
            .and_then(|a| a.as_any().downcast_ref::<StringArray>())
        else {
            return CVec::Str(vec![None; n]);
        };
        for i in 0..n {
            if arr.is_null(i) {
                out.push(None);
                continue;
            }
            let text = arr.value(i);
            let picked: Option<SmolStr> = if sep.is_empty() {
                let chars: Vec<char> = text.chars().collect();
                normalize_index_simple(index, chars.len())
                    .map(|k| SmolStr::from(chars[k].to_string()))
            } else {
                // 惰性取段（2026-08-26 q22）：正索引不建 Vec、只扫描到第 k 段
                // （url 3 段目录 + query 全分割是纯浪费——3×split 建 Vec 曾致
                // 消费慢 → 驱逐被挡 → 输入窗全量驻留 24G）。负索引需总段数：
                // `Split` 迭代器可 clone，count 不消耗原迭代器。
                let mut it = text.split(sep.as_str());
                let normalized = if index < 0 {
                    let len = it.clone().count();
                    len as i64 + index
                } else {
                    index
                };
                if normalized < 0 {
                    None
                } else {
                    it.nth(normalized as usize).map(SmolStr::from)
                }
            };
            out.push(picked);
        }
        CVec::Str(out)
    }
}

/// `concat(a, b, ...)` 批量求值：per row 逐参 cell → 字符串拼接
/// （与解释路径逐参 eval + value_to_string 字节一致）；任一参数 cell null →
/// 整行 null（解释路径 `?` 传播 → yield 空串）。
/// 2026-08-26（q22 输出链加速）：两处 per-row 热点修复——① 按参数类型直接取
/// 字符串（Str 零拷贝借用、Number/Bool 走 value_to_string），不再经
/// `cscalar_to_value` 的 Value 克隆中转；② 结果 String 预分配（各参长度 + 余量），
/// 避免逐 push 扩容（q22 每行 5 参数 concat）。字节一致对拍由既有
/// `columnar_*_matches_interpreted_path` 测试钉死。
fn concat_vec(args: &[CVec], n: usize) -> CVec {
    let mut out: Vec<Option<SmolStr>> = Vec::with_capacity(n);
    // 每行结果长度下界（非 Str 参数无法预知长度，按 24B 余量估计）——
    // 分配一次即够，避免扩容（q22 形态：3 段目录 + 2 个 "/"）。
    let row_cap: usize = args
        .iter()
        .map(|a| match a {
            CVec::Str(v) => v.first().map_or(0, |s| s.as_deref().map_or(0, str::len)),
            CVec::Int(_) | CVec::Float(_) | CVec::Bool(_) | CVec::Scalar(_) => 24,
        })
        .sum();
    for row in 0..n {
        let mut s = String::with_capacity(row_cap);
        let mut ok = true;
        for a in args {
            match a.scalar_at(row) {
                Some(CScalar::Str(ss)) => s.push_str(&ss),
                Some(c) => s.push_str(&value_to_string(&cscalar_to_value(&c))),
                None => {
                    ok = false;
                    break;
                }
            }
        }
        out.push(if ok { Some(SmolStr::from(s)) } else { None });
    }
    CVec::Str(out)
}

/// `normalize_index`（负数从尾数；越界 → None）——与解释路径 `mvindex` 的
/// `utils::normalize_index` 同语义（此处内联避免可见性纠缠）。
fn normalize_index_simple(index: i64, len: usize) -> Option<usize> {
    let len = len as i64;
    let normalized = if index < 0 { len + index } else { index };
    if normalized < 0 || normalized >= len {
        None
    } else {
        Some(normalized as usize)
    }
}

/// Vectorized `strftime(ts, fmt)` yield-cell evaluation: per row, a numeric
/// epoch-nanos cell is normalized (the same f64 heuristic as the interpreted
/// path) and formatted via chrono; null / non-numeric / out-of-range read null
/// (the yield wrapper substitutes an empty string).
fn strftime_vec(ts: CVec, fmt: &SmolStr, n: usize) -> CVec {
    let mut out = Vec::with_capacity(n);
    for row in 0..n {
        let cell = match ts.scalar_at(row) {
            Some(s) => s,
            None => {
                out.push(None);
                continue;
            }
        };
        let nanos = match cscalar_to_value(&cell) {
            Value::Number(v) => normalize_epoch_timestamp_float_nanos(v),
            _ => None,
        };
        out.push(
            nanos
                .and_then(timestamp_nanos_to_utc)
                .map(|dt| dt.format(fmt).to_string().into()),
        );
    }
    CVec::Str(out)
}

/// Vectorized `count_char(text, ch)` yield-cell evaluation: per row, count the
/// first char of the needle in the text (empty needle → 0, matching the
/// interpreted path); either operand non-string / null reads null (the yield
/// wrapper substitutes an empty string).
fn count_char_vec(text: CVec, needle: CVec, n: usize) -> CVec {
    let mut out = Vec::with_capacity(n);
    for row in 0..n {
        let (t, nd) = match (text.scalar_at(row), needle.scalar_at(row)) {
            (Some(t), Some(nd)) => (t, nd),
            _ => {
                out.push(None);
                continue;
            }
        };
        let (Value::Str(t), Value::Str(nd)) = (cscalar_to_value(&t), cscalar_to_value(&nd)) else {
            out.push(None);
            continue;
        };
        let count = match nd.chars().next() {
            Some(ch) => t.chars().filter(|&c| c == ch).count() as i64,
            None => 0,
        };
        out.push(Some(CScalar::Int(count)));
    }
    CVec::Scalar(out)
}

/// Vectorized `expr in (lit, ...)`: per row, membership over the compile-time
/// literal list via `values_equal` (byte-identical to the interpreted `InList`
/// — number epsilon equality, Str/Bool equality). A null target cell reads
/// null (the interpreted `eval_expr_ext(target)?` propagates `None`).
fn inlist_vec(expr: CVec, list: &[Value], negated: bool, n: usize) -> CVec {
    let mut out = Vec::with_capacity(n);
    for row in 0..n {
        match expr.scalar_at(row) {
            Some(s) => {
                let v = cscalar_to_value(&s);
                let found = list.iter().any(|item| values_equal(&v, item));
                out.push(Some(if negated { !found } else { found }));
            }
            None => out.push(None),
        }
    }
    CVec::Bool(out)
}

/// Vectorized `if c then a else b`: per row, pick the then/else cell by the
/// Bool condition; a non-Bool / null cond reads null, matching the interpreted
/// three-valued path. Output is a heterogeneous scalar column (then/else may
/// differ in type).
fn ifthenelse_vec(cond: CVec, then_c: CVec, else_c: CVec, n: usize) -> CVec {
    let mut out = Vec::with_capacity(n);
    for row in 0..n {
        match cond.scalar_at(row) {
            Some(CScalar::Bool(true)) => out.push(then_c.scalar_at(row)),
            Some(CScalar::Bool(false)) => out.push(else_c.scalar_at(row)),
            _ => out.push(None),
        }
    }
    CVec::Scalar(out)
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
                .map(|o| {
                    o.and_then(|s| match s {
                        CScalar::Bool(b) => Some(!b),
                        _ => None,
                    })
                })
                .collect(),
        ),
        _ => CVec::Bool(vec![None; n]),
    }
}

/// Vectorized `root[i]`: per row, read the `index`-th non-null element of the
/// array cell as a scalar (null cell / parse failure / out of range → null).
/// A non-array column reads all-null — the interpreted path walk yields `None`
/// for an index segment on a non-array root, so this is byte-identical.
impl ColumnarBatch<'_> {
    fn list_index_vec(&self, col: &ColRef, index: usize, n: usize) -> CVec {
        match col.kind {
            ColKind::JsonArray => match self.string_array(col) {
                Some(a) => CVec::Scalar(
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
                None => CVec::Scalar(vec![None; n]),
            },
            ColKind::List => match self.list_array(col) {
                Some(a) => CVec::Scalar(
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
                None => CVec::Scalar(vec![None; n]),
            },
            ColKind::LargeList => match self.large_list_array(col) {
                Some(a) => CVec::Scalar(
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
                None => CVec::Scalar(vec![None; n]),
            },
            ColKind::FixedSizeList => match self.fixed_size_list_array(col) {
                Some(a) => CVec::Scalar(
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
                None => CVec::Scalar(vec![None; n]),
            },
            // Non-array root column: the interpreted walk hits an index segment on
            // a non-array value → `None` for every row.
            _ => CVec::Scalar(vec![None; n]),
        }
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
    use crate::match_engine::match_engine::{Event, Value, eval_expr, eval_expr_ext};

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
            Expr::Not(Box::new(Expr::Not(Box::new(bin(
                BinOp::Eq,
                field("auction"),
                num(1.0),
            ))))),
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

    /// 单一权威清单同步：wf-lang 的 `ColumnarFunc` 分类与 wf-engine 的
    /// `StrFuncOp` 语义映射必须一致——`StrSearch` 分类 ↔ `StrFuncOp::from_name`
    /// 一一对应，防止未来加函数时两处清单 drift。
    #[test]
    fn strfunc_op_stays_in_sync_with_columnar_func() {
        use wf_lang::columnar::{ColumnarFunc, columnar_func};

        // StrSearch 分类下的每个名字必须有 op；其他分类无 op。
        for name in ["contains", "startswith", "endswith"] {
            assert_eq!(columnar_func(name), Some(ColumnarFunc::StrSearch), "{name}");
            assert!(
                StrFuncOp::from_name(name).is_some(),
                "{name} 应有 StrFuncOp"
            );
        }
        for name in ["cidr_match", "regex_match"] {
            assert!(columnar_func(name).is_some(), "{name}");
            assert!(
                StrFuncOp::from_name(name).is_none(),
                "{name} 不应有 StrFuncOp"
            );
        }
        // 非列式函数两边都不认。
        for name in ["lower", "concat", "startswith_any", "bogus"] {
            assert!(columnar_func(name).is_none(), "{name}");
            assert!(StrFuncOp::from_name(name).is_none(), "{name}");
        }
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
                Some("10.1.2.3"),   // 10/8 命中
                Some("172.31.0.1"), // 不命中
                Some("fe80::1"),    // v6 与 v4 网段版本不一致
                Some("8.8.8.8"),    // 不命中
                None,               // null
                Some("11.0.0.1"),   // 不命中
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
            vec![
                Some("fail"),
                Some("login"),
                Some("fail"),
                Some("fail"),
                None,
            ],
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

    /// 列式输出 cell（fmt/strftime/count_char）与解释路径逐行对拍，含 yield
    /// 语义的 None→空串包装（`eval_yield_expr_with_meta` 对缺字段/null 参数
    /// 替换空串）。
    fn assert_output_equiv(expr: &Expr, batch: &RecordBatch) {
        let events = batch_to_events(batch);
        let view = ColumnarBatch::from_all_fields(batch);
        let plan = compile_guard(expr, &view).expect("输出函数应可编译");
        let cvec = plan.eval_vec(&view, view.num_rows());
        for (row, event) in events.iter().enumerate() {
            let columnar = match cvec.scalar_at(row) {
                Some(s) => cscalar_to_value(&s),
                None => Value::Str(SmolStr::default()),
            };
            let interpreted =
                eval_expr(expr, event).unwrap_or_else(|| Value::Str(SmolStr::default()));
            assert_eq!(columnar, interpreted, "row {row}: expr={expr:?}");
        }
    }

    #[test]
    fn output_funcs_match_interpreted_cells() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("action", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
            Field::new("ts", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("fail_login"),
                    None,
                    Some("success"),
                    Some("aa"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(3), Some(7), None, Some(2)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(1_700_000_000_000_000_000),
                    Some(1_700_000_000_000_000_001),
                    None,
                    Some(1_700_000_000_000_000_002),
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
            qualifier: None,
            name: name.into(),
            args,
        };
        let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));

        // fmt：字面量模板 + 字段参数（null action/count 行 → 空串）。
        let fmt = call(
            "fmt",
            vec![Expr::StringLit("a={}|n={}".into()), f("action"), f("count")],
        );
        assert!(wf_lang::columnar::columnar_output_expr(&fmt));
        assert_output_equiv(&fmt, &batch);
        // fmt：纯字面量参数。
        assert_output_equiv(
            &call(
                "fmt",
                vec![Expr::StringLit("x={}".into()), Expr::Number(42.0)],
            ),
            &batch,
        );

        // strftime：默认格式 + 自定义格式 + 常量 ts。
        assert_output_equiv(&call("strftime", vec![f("ts")]), &batch);
        assert_output_equiv(
            &call(
                "strftime",
                vec![f("ts"), Expr::StringLit("%Y-%m-%d".into())],
            ),
            &batch,
        );
        assert_output_equiv(
            &call("strftime", vec![Expr::Number(1_700_000_000_000_000_000.0)]),
            &batch,
        );

        // count_char：字面量 / 字段 needle；null 参数（action null 行）→ 空串。
        assert_output_equiv(
            &call("count_char", vec![f("action"), Expr::StringLit("a".into())]),
            &batch,
        );
        assert_output_equiv(
            &call("count_char", vec![f("action"), Expr::StringLit("l".into())]),
            &batch,
        );
        // 空 needle → 0。
        assert_output_equiv(
            &call(
                "count_char",
                vec![f("action"), Expr::StringLit(String::new())],
            ),
            &batch,
        );
    }

    #[test]
    fn output_funcs_split_mvindex_concat_match_interpreted() {
        // 层 2（2026-08-25，q22 形态）：`mvindex(split(field, sep), idx)` 融合
        // 节点（SplitIndex）与 `concat` 必须与解释路径逐行对拍——含 null 行 /
        // 越界 / 空 sep（按字符切分）/ 负数索引（从尾数）。
        let schema = Arc::new(Schema::new(vec![Field::new("url", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("https://www.nexmark.com/aaaaa/bbbbb/ccccc/item.htm?query=1"),
                None,           // null 行
                Some("short"),  // 段数不足 → mvindex 越界 → null
                Some("a/b//d"), // 空段
            ])) as ArrayRef],
        )
        .unwrap();
        let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
            qualifier: None,
            name: name.into(),
            args,
        };
        let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
        let split = |text: Expr, sep: &str| call("split", vec![text, Expr::StringLit(sep.into())]);
        let mvindex = |list: Expr, idx: f64| call("mvindex", vec![list, Expr::Number(idx)]);

        // mvindex(split(url, "/"), 3)——融合节点（正索引）。
        let idx3 = mvindex(split(f("url"), "/"), 3.0);
        assert!(wf_lang::columnar::columnar_output_expr(&idx3));
        assert_value_equiv(&idx3, &batch);
        // 负数索引（从尾数）。
        assert_value_equiv(&mvindex(split(f("url"), "/"), -1.0), &batch);
        // 空 sep → 按字符切分。
        assert_value_equiv(&mvindex(split(f("url"), ""), 4.0), &batch);

        // concat：字段 + 字面量；q22 detail 形态（3 段 mvindex 拼接）。
        let concat_suffix = call("concat", vec![f("url"), Expr::StringLit("-suffix".into())]);
        assert!(wf_lang::columnar::columnar_output_expr(&concat_suffix));
        assert_output_equiv(&concat_suffix, &batch);
        let q22_detail = call(
            "concat",
            vec![
                mvindex(split(f("url"), "/"), 3.0),
                Expr::StringLit("/".into()),
                mvindex(split(f("url"), "/"), 4.0),
                Expr::StringLit("/".into()),
                mvindex(split(f("url"), "/"), 5.0),
            ],
        );
        assert!(wf_lang::columnar::columnar_output_expr(&q22_detail));
        assert_output_equiv(&q22_detail, &batch);
    }

    /// Exact per-cell parity (incl. null-ness and value type): columnar
    /// `eval_vec` vs interpreted `eval_expr` — the strictest lock for the
    /// InList / IfThenElse output nodes.
    fn assert_value_equiv(expr: &Expr, batch: &RecordBatch) {
        let events = batch_to_events(batch);
        let view = ColumnarBatch::from_all_fields(batch);
        let plan = compile_guard(expr, &view).expect("应可编译");
        let cvec = plan.eval_vec(&view, view.num_rows());
        for (row, event) in events.iter().enumerate() {
            let columnar = cvec.scalar_at(row).map(|s| cscalar_to_value(&s));
            let interpreted = eval_expr(expr, event);
            assert_eq!(columnar, interpreted, "row {row}: expr={expr:?}");
        }
    }

    /// InList：`values_equal` 成员语义（数字 epsilon 等值 / Str / Bool）、negated
    /// 翻转、null 目标传播 None——与解释器逐行一致。
    #[test]
    fn inlist_matches_interpreted_cells() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, true),
            Field::new("ts", DataType::Int64, true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(3),
                    Some(7),
                    Some(8),
                    None,
                    Some(3),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    // 01:00 / 02:30 / 00:15 / null / 13:00 UTC（%H 小时）。
                    Some(1_700_000_000_000_000_000),
                    Some(1_700_000_000_000_000_000 + 90 * 3_600_000_000_000),
                    Some(1_700_000_000_000_000_000 - 45 * 3_600_000_000_000),
                    None,
                    Some(1_700_000_000_000_000_000 + 13 * 3_600_000_000_000),
                ])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                    Some(true),
                    Some(false),
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
        let in_list = |expr: Expr, list: Vec<Expr>, negated: bool| Expr::InList {
            expr: Box::new(expr),
            list,
            negated,
        };

        // 数字成员（Int64 列 vs 数字字面量列表）。
        let nums = in_list(f("count"), vec![num(3.0), num(7.0)], false);
        assert_value_equiv(&nums, &batch);
        // negated 翻转（None 目标行仍然 None，不因否定变 true——解释器同）。
        assert_value_equiv(&in_list(f("count"), vec![num(3.0)], true), &batch);
        // Bool 成员。
        assert_value_equiv(&in_list(f("flag"), vec![Expr::Bool(true)], false), &batch);
        // Q14 形态：strftime(ts, "%H") in ("00","01","02")。
        let hour = Expr::FuncCall {
            qualifier: None,
            name: "strftime".into(),
            args: vec![f("ts"), Expr::StringLit("%H".into())],
        };
        assert!(wf_lang::columnar::columnar_output_expr(&hour));
        assert_value_equiv(
            &in_list(
                hour,
                vec![
                    Expr::StringLit("00".into()),
                    Expr::StringLit("01".into()),
                    Expr::StringLit("02".into()),
                ],
                false,
            ),
            &batch,
        );
    }

    /// IfThenElse：Bool cond 三值选值；非 Bool / null cond → None（解释器同）。
    #[test]
    fn ifthenelse_matches_interpreted_cells() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(3), Some(7), Some(8), None])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                    Some(true),
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
        let ite = |cond: Expr, then_e: Expr, else_e: Expr| Expr::IfThenElse {
            cond: Box::new(cond),
            then_expr: Box::new(then_e),
            else_expr: Box::new(else_e),
        };

        // 比较条件（列式 Bool）→ 三值选值；分支类型切换（字符串 vs 数字）。
        let by_flag = ite(
            f("flag"),
            Expr::StringLit("yes".into()),
            Expr::StringLit("no".into()),
        );
        assert_value_equiv(&by_flag, &batch);
        // 分支类型不同：数字 vs 字符串（列式异构 Scalar 列）。
        let mixed = ite(f("flag"), num(1.0), Expr::StringLit("no".into()));
        assert_value_equiv(&mixed, &batch);
        // 非 Bool cond（数字列）→ 全 None。
        let non_bool = ite(f("count"), num(1.0), num(2.0));
        assert_value_equiv(&non_bool, &batch);
        // InList cond 组合（`count in (3,7)` 做条件）。
        let in_cond = Expr::InList {
            expr: Box::new(f("count")),
            list: vec![num(3.0), num(7.0)],
            negated: false,
        };
        assert_value_equiv(
            &ite(
                in_cond,
                Expr::StringLit("hit".into()),
                Expr::StringLit("miss".into()),
            ),
            &batch,
        );
    }

    /// Q14 全形态 value 对拍：`fmt("{} c={}", if strftime(ts,"%H") in (...)
    /// then "nightTime" else "dayTime", count_char(extra,"c"))`。
    #[test]
    fn q14_fmt_shape_matches_interpreted_cells() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1_700_000_000_000_000_000),
                    Some(1_700_000_000_000_000_000 + 90 * 3_600_000_000_000),
                    None,
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("abc c cc"),
                    Some("no-c"),
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
        let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
            qualifier: None,
            name: name.into(),
            args,
        };
        let is_night = Expr::InList {
            expr: Box::new(call(
                "strftime",
                vec![f("ts"), Expr::StringLit("%H".into())],
            )),
            list: vec![
                Expr::StringLit("00".into()),
                Expr::StringLit("01".into()),
                Expr::StringLit("02".into()),
            ],
            negated: false,
        };
        let detail = call(
            "fmt",
            vec![
                Expr::StringLit("{} c={}".into()),
                Expr::IfThenElse {
                    cond: Box::new(is_night),
                    then_expr: Box::new(Expr::StringLit("nightTime".into())),
                    else_expr: Box::new(Expr::StringLit("dayTime".into())),
                },
                call("count_char", vec![f("extra"), Expr::StringLit("c".into())]),
            ],
        );
        assert!(wf_lang::columnar::columnar_output_expr(&detail));
        assert_value_equiv(&detail, &batch);
    }

    /// 真实 `q14.wfl` 形状：**嵌套 3 档 CASE**（nightTime/dayTime/otherTime，
    /// 10/9 项 InList）——else 分支里再嵌 IfThenElse。列式 gate/编译/求值必须
    /// 与解释器逐行一致（三档都覆盖 + null ts）。
    #[test]
    fn q14_real_three_way_case_matches_interpreted() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![
                    // 22 时 → nightTime；10 时 → dayTime；07 时 → otherTime；null。
                    Some(1_700_000_000_000_000_000),
                    Some(1_700_000_000_000_000_000 - 12 * 3_600_000_000_000),
                    Some(1_700_000_000_000_000_000 - 15 * 3_600_000_000_000),
                    None,
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("abc c cc"),
                    Some("no-c"),
                    Some("zz"),
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
        let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
            qualifier: None,
            name: name.into(),
            args,
        };
        let in_hours = |hours: &[&str]| Expr::InList {
            expr: Box::new(call(
                "strftime",
                vec![f("ts"), Expr::StringLit("%H".into())],
            )),
            list: hours.iter().map(|h| Expr::StringLit((*h).into())).collect(),
            negated: false,
        };
        let bid_time_type = Expr::IfThenElse {
            cond: Box::new(in_hours(&[
                "00", "01", "02", "03", "04", "05", "06", "20", "21", "22", "23",
            ])),
            then_expr: Box::new(Expr::StringLit("nightTime".into())),
            else_expr: Box::new(Expr::IfThenElse {
                cond: Box::new(in_hours(&[
                    "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18",
                ])),
                then_expr: Box::new(Expr::StringLit("dayTime".into())),
                else_expr: Box::new(Expr::StringLit("otherTime".into())),
            }),
        };
        let detail = call(
            "fmt",
            vec![
                Expr::StringLit("{} c={}".into()),
                bid_time_type,
                call("count_char", vec![f("extra"), Expr::StringLit("c".into())]),
            ],
        );
        assert!(
            wf_lang::columnar::columnar_output_expr(&detail),
            "真实 q14 嵌套 3 档 CASE 必须可列式"
        );
        assert_value_equiv(&detail, &batch);
        // 语义抽查：三档分型 + count_char。
        let events = batch_to_events(&batch);
        assert_eq!(
            eval_expr(&detail, &events[0]).unwrap(),
            Value::Str("nightTime c=4".into())
        );
        assert_eq!(
            eval_expr(&detail, &events[1]).unwrap(),
            Value::Str("dayTime c=1".into())
        );
        assert_eq!(
            eval_expr(&detail, &events[2]).unwrap(),
            Value::Str("otherTime c=0".into())
        );
        assert_eq!(eval_expr(&detail, &events[3]), None);
    }

    #[test]
    fn fmt_structured_arg_falls_back_to_row() {
        use crate::match_engine::WFL_FIELD_TYPE_OBJECT;

        // OBJECT 元数据的 Utf8 列：解释路径解析成 Value::Object 渲染
        // `[object]`，列式读原始 JSON 文本——字节不同，必须行式回退。
        let schema = Arc::new(Schema::new(vec![
            Field::new("ext", DataType::Utf8, true).with_metadata(std::collections::HashMap::from(
                [(
                    WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                    WFL_FIELD_TYPE_OBJECT.to_string(),
                )],
            )),
            Field::new("id", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some(r#"{"k":1}"#)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef,
            ],
        )
        .unwrap();
        let fmt = Expr::FuncCall {
            qualifier: None,
            name: "fmt".into(),
            args: vec![
                Expr::StringLit("x={}".into()),
                Expr::Field(FieldRef::Simple("ext".into())),
            ],
        };
        // 形状 gate 放行（flat 字段参数），但编译必须失败 → 行式回退。
        assert!(wf_lang::columnar::columnar_output_expr(&fmt));
        let view = ColumnarBatch::from_all_fields(&batch);
        assert!(
            compile_guard(&fmt, &view).is_none(),
            "fmt 结构化参数必须编译失败（行式回退）"
        );
        // 行式渲染：Value::Object → value_to_string → "[object]"。
        let events = batch_to_events(&batch);
        assert_eq!(
            eval_expr(&fmt, &events[0]).unwrap_or_else(|| Value::Str(SmolStr::default())),
            Value::Str("x=[object]".into()),
            "解释路径渲染 [object]"
        );
    }

    /// 结构化字段藏在 IfThenElse 分支 / InList 目标里：gate 放行（flat FieldRef
    /// 不含元数据），但编译期 `arg_reads_structured` **递归**拦截 → 行式回退。
    /// 否则列式读 OBJECT 列原始 JSON 文本，fmt 渲染原始 JSON / count_char 对
    /// JSON 计数——与解释器 `[object]`/None 字节分叉。
    #[test]
    fn structured_nested_in_branch_compiles_fail() {
        use crate::match_engine::WFL_FIELD_TYPE_OBJECT;

        let schema = Arc::new(Schema::new(vec![
            Field::new("ext", DataType::Utf8, true).with_metadata(std::collections::HashMap::from(
                [(
                    WFL_FIELD_TYPE_METADATA_KEY.to_string(),
                    WFL_FIELD_TYPE_OBJECT.to_string(),
                )],
            )),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some(r#"{"k":1}"#), None])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as ArrayRef,
            ],
        )
        .unwrap();
        let view = ColumnarBatch::from_all_fields(&batch);
        let f = |n: &str| Expr::Field(FieldRef::Simple(n.into()));
        let call = |name: &str, args: Vec<Expr>| Expr::FuncCall {
            qualifier: None,
            name: name.into(),
            args,
        };

        // fmt("{} {}", if flag then ext else "x", "y")——结构化藏在 then 分支。
        let fmt_branch = call(
            "fmt",
            vec![
                Expr::StringLit("{} {}".into()),
                Expr::IfThenElse {
                    cond: Box::new(f("flag")),
                    then_expr: Box::new(f("ext")),
                    else_expr: Box::new(Expr::StringLit("x".into())),
                },
                Expr::StringLit("y".into()),
            ],
        );
        // gate 放行（分支是 flat FieldRef）……
        assert!(wf_lang::columnar::columnar_output_expr(&fmt_branch));
        // ……但编译必须失败（递归 arg_reads_structured 拦截）。
        assert!(
            compile_guard(&fmt_branch, &view).is_none(),
            "fmt 分支里的结构化字段必须编译失败"
        );

        // count_char(ext, "c")——结构化直接作 text 参数。
        let cc = call("count_char", vec![f("ext"), Expr::StringLit("c".into())]);
        assert!(wf_lang::columnar::columnar_output_expr(&cc));
        assert!(
            compile_guard(&cc, &view).is_none(),
            "count_char 结构化 text 参数必须编译失败"
        );

        // count_char("abc", ext)——结构化作 needle 参数（首字符计数分叉）。
        let cc2 = call("count_char", vec![Expr::StringLit("abc".into()), f("ext")]);
        assert!(wf_lang::columnar::columnar_output_expr(&cc2));
        assert!(
            compile_guard(&cc2, &view).is_none(),
            "count_char 结构化 needle 参数必须编译失败"
        );

        // InList 目标为结构化列，藏在 fmt 的 IfThenElse cond 里（极端形态）：
        // gate 放行（InList 列表字面量 + ext flat），但递归拦截必须使其编译失败。
        let fmt_inlist_cond = call(
            "fmt",
            vec![
                Expr::StringLit("{} {}".into()),
                Expr::IfThenElse {
                    cond: Box::new(Expr::InList {
                        expr: Box::new(f("ext")),
                        list: vec![Expr::StringLit("{\"k\":1}".into())],
                        negated: false,
                    }),
                    then_expr: Box::new(Expr::StringLit("a".into())),
                    else_expr: Box::new(Expr::StringLit("b".into())),
                },
                Expr::StringLit("y".into()),
            ],
        );
        assert!(wf_lang::columnar::columnar_output_expr(&fmt_inlist_cond));
        assert!(
            compile_guard(&fmt_inlist_cond, &view).is_none(),
            "fmt 内 InList 目标结构化必须编译失败"
        );
        // 裸 IfThenElse（非输出函数参数）作顶层 yield 从不走列式（executor 只对
        // 输出函数编译 general 槽位）——此处仅确认它不 panic 且不误报结构化。
        let bare_ite = Expr::IfThenElse {
            cond: Box::new(f("flag")),
            then_expr: Box::new(Expr::StringLit("a".into())),
            else_expr: Box::new(Expr::StringLit("b".into())),
        };
        assert!(wf_lang::columnar::columnar_output_expr(&bare_ite));
        assert!(compile_guard(&bare_ite, &view).is_some());

        // 行式基准：true 分支渲染 [object]；count_char 对 Object → None。
        let events = batch_to_events(&batch);
        assert_eq!(
            eval_expr(&fmt_branch, &events[0]).unwrap_or_else(|| Value::Str(SmolStr::default())),
            Value::Str("[object] y".into()),
            "解释路径：true 分支渲染 [object]"
        );
        assert_eq!(
            eval_expr(&cc, &events[0]).unwrap_or_else(|| Value::Str(SmolStr::default())),
            Value::Str(SmolStr::default()),
            "解释路径：count_char(Object) → None → 空串"
        );
    }
}
