//! Columnar 向量化求值核（2026-09-04 自 columnar.rs 拆出）：[`CVec`] 物化列 +
//! [`ColumnExpr::eval_vec`] 整批分派 + ColumnarBatch 读列 kernel 与 per-op 逐行
//! helpers（与解释求值器字节一致的对拍语义见 columnar_tests）。

use arrow::array::{
    Array, BooleanArray, FixedSizeListArray, Float64Array, Int64Array, LargeListArray, ListArray,
    StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use smol_str::SmolStr;
use wf_lang::ast::BinOp;

use super::*;
use crate::match_engine::cep::eval::cmp::{apply_fmt_template, timestamp_nanos_to_utc};
use crate::match_engine::cep::{Value, value_to_string, values_equal};
use crate::time::normalize_epoch_timestamp_float_nanos;

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
            // 列叶子与带 ColRef/常参的内核（ColRef → 本批列; 其余形态委托
            // [`Self::eval_vec_kernel`]——两层分派把单函数圈复杂度压到阈值内,
            // 各臂体逐字保持, 语义不变）。
            ColumnExpr::Lit(v) => lit_vec(v, n),
            ColumnExpr::Col(col) => view.col_vec(col, n),
            ColumnExpr::ListIndex { col, index } => view.list_index_vec(col, *index, n),
            ColumnExpr::CidrMatch { col, net } => view.cidr_vec(col, net, n),
            ColumnExpr::RegexMatch { col, re } => view.regex_vec(col, re, n),
            ColumnExpr::StrFunc { op, hay, needle } => view.strfunc_vec(*op, hay, needle, n),
            ColumnExpr::SplitIndex { col, sep, index } => view.split_index_vec(col, sep, *index, n),
            other => other.eval_vec_kernel(view, n),
        }
    }

    /// 一元/二元/参数组形态内核的分派（被 [`Self::eval_vec`] 的叶子层调用）。
    fn eval_vec_kernel(&self, view: &ColumnarBatch<'_>, n: usize) -> CVec {
        match self {
            // 列叶子/带常参形态已由 eval_vec 直接分派——此处仅防呆（不可达）。
            ColumnExpr::Lit(_)
            | ColumnExpr::Col(_)
            | ColumnExpr::ListIndex { .. }
            | ColumnExpr::CidrMatch { .. }
            | ColumnExpr::RegexMatch { .. }
            | ColumnExpr::StrFunc { .. }
            | ColumnExpr::SplitIndex { .. } => {
                unreachable!("列叶子形态由 eval_vec 直接分派")
            }
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
            ColumnExpr::Fmt { template, args } => {
                fmt_vec(template, &eval_arg_vecs(args, view, n), n)
            }
            ColumnExpr::Strftime { ts, fmt } => strftime_vec(ts.eval_vec(view, n), fmt, n),
            ColumnExpr::CountChar { text, needle } => {
                count_char_vec(text.eval_vec(view, n), needle.eval_vec(view, n), n)
            }
            ColumnExpr::Concat { args } => concat_vec(&eval_arg_vecs(args, view, n), n),
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

/// 批量求值一组子表达式为列缓冲（Fmt / Concat 的 `args` 共用）。
fn eval_arg_vecs(args: &[ColumnExpr], view: &ColumnarBatch<'_>, n: usize) -> Vec<CVec> {
    args.iter().map(|a| a.eval_vec(view, n)).collect()
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
    /// 各列类型的取值/空值映射收敛到下方 `*_col_vec` 自由函数（单臂无内联
    /// match, 降低分派圈复杂度）。
    fn col_vec(&self, col: &ColRef, n: usize) -> CVec {
        match col.kind {
            ColKind::Null => CVec::Int(vec![None; n]),
            ColKind::Int64 => int64_col_vec(self.int64_array(col), n),
            ColKind::TimestampNs => ts_col_vec(self.ts_array(col), n),
            ColKind::Float64 => float_col_vec(self.float64_array(col), n),
            ColKind::Utf8 => str_col_vec(self.string_array(col), n),
            ColKind::Bool => bool_col_vec(self.bool_array(col), n),
            // Array-shaped columns read bare are a non-null structured value per
            // row (`Value::Array`), never a scalar — compares false, reads null as
            // a boolean, and is not numeric (byte-identical to interpreted).
            ColKind::JsonArray => {
                presence_col_vec(self.string_array(col).map(|a| a as &dyn Array), n)
            }
            ColKind::List => presence_col_vec(self.list_array(col).map(|a| a as &dyn Array), n),
            ColKind::LargeList => {
                presence_col_vec(self.large_list_array(col).map(|a| a as &dyn Array), n)
            }
            ColKind::FixedSizeList => {
                presence_col_vec(self.fixed_size_list_array(col).map(|a| a as &dyn Array), n)
            }
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

/// 原生 Int64 列物化（[`ColumnarBatch::col_vec`] 的 Int64/TimestampNs 共用形状）;
/// 列缺失/形态不符 → 全 null（对齐 `ColKind::Null`）。
fn int64_col_vec(a: Option<&Int64Array>, n: usize) -> CVec {
    match a {
        Some(a) => CVec::Int(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                .collect(),
        ),
        None => CVec::Int(vec![None; n]),
    }
}

/// Timestamp(Ns) 列以原生 i64 读取（物化为 Int 列）。
fn ts_col_vec(a: Option<&TimestampNanosecondArray>, n: usize) -> CVec {
    match a {
        Some(a) => CVec::Int(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                .collect(),
        ),
        None => CVec::Int(vec![None; n]),
    }
}

/// 原生 Float64 列物化。
fn float_col_vec(a: Option<&Float64Array>, n: usize) -> CVec {
    match a {
        Some(a) => CVec::Float(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                .collect(),
        ),
        None => CVec::Float(vec![None; n]),
    }
}

/// 原生 Utf8 列物化（值转 SmolStr）。
fn str_col_vec(a: Option<&StringArray>, n: usize) -> CVec {
    match a {
        Some(a) => CVec::Str(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| a.value(r).into()))
                .collect(),
        ),
        None => CVec::Str(vec![None; n]),
    }
}

/// 原生 Bool 列物化。
fn bool_col_vec(a: Option<&BooleanArray>, n: usize) -> CVec {
    match a {
        Some(a) => CVec::Bool(
            (0..n)
                .map(|r| (!a.is_null(r)).then(|| a.value(r)))
                .collect(),
        ),
        None => CVec::Bool(vec![None; n]),
    }
}

/// 结构化列（JsonArray/List 族）裸读的「每行非 null 即 Structured」列;
/// 列缺失/形态不符 → 全 null。
fn presence_col_vec(a: Option<&dyn Array>, n: usize) -> CVec {
    match a {
        Some(a) => structured_col(n, |r| !a.is_null(r)),
        None => structured_col(n, |_| false),
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
            ColKind::JsonArray => json_index_scalar_vec(self.string_array(col), index, n),
            ColKind::List => list_index_scalar_vec(self.list_array(col), index, n),
            ColKind::LargeList => large_list_index_scalar_vec(self.large_list_array(col), index, n),
            ColKind::FixedSizeList => {
                fixed_list_index_scalar_vec(self.fixed_size_list_array(col), index, n)
            }
            // Non-array root column: the interpreted walk hits an index segment on
            // a non-array value → `None` for every row.
            _ => CVec::Scalar(vec![None; n]),
        }
    }
}

/// JsonArray 列（Utf8 JSON 文本）第 `index` 个非 null 元素标量列; 缺失列 → 全 null。
fn json_index_scalar_vec(a: Option<&StringArray>, index: usize, n: usize) -> CVec {
    match a {
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
    }
}

/// List 列第 `index` 个非 null 元素标量列; 缺失列 → 全 null。
fn list_index_scalar_vec(a: Option<&ListArray>, index: usize, n: usize) -> CVec {
    match a {
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
    }
}

/// LargeList 列第 `index` 个非 null 元素标量列; 缺失列 → 全 null。
fn large_list_index_scalar_vec(a: Option<&LargeListArray>, index: usize, n: usize) -> CVec {
    match a {
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
    }
}

/// FixedSizeList 列第 `index` 个非 null 元素标量列; 缺失列 → 全 null。
fn fixed_list_index_scalar_vec(a: Option<&FixedSizeListArray>, index: usize, n: usize) -> CVec {
    match a {
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
        (CScalar::Str(a), CScalar::Str(b)) => compare_str(op, a, b),
        (CScalar::Bool(a), CScalar::Bool(b)) => compare_bool(op, *a, *b),
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

/// Str 有序比较（`Eq/Ne/Lt/Gt/Le/Ge`; 非比较算子 → false）。
fn compare_str(op: BinOp, a: &SmolStr, b: &SmolStr) -> bool {
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

/// Bool 相等比较（仅 `Eq/Ne`; 其余算子 → false）。
fn compare_bool(op: BinOp, a: bool, b: bool) -> bool {
    match op {
        BinOp::Eq => a == b,
        BinOp::Ne => a != b,
        _ => false,
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

/// Numeric arithmetic. `%` over two `i64` operands is native (more precise);
/// every other case (`+ - * /`, and any `i64`/`f64` mix) is f64 to match
/// `eval_arithmetic`.
fn arithmetic(op: BinOp, lv: &CScalar, rv: &CScalar) -> Option<CScalar> {
    if op == BinOp::Mod
        && let (CScalar::Int(a), CScalar::Int(b)) = (lv, rv)
    {
        // i64 原生取模（更精确）; 除零 → null（与 f64 路径一致）。
        return (*b != 0).then(|| CScalar::Int(a % b));
    }
    let ln = to_f64(lv)?;
    let rn = to_f64(rv)?;
    arith_f64(op, ln, rn).map(CScalar::Float)
}

/// f64 算术（`+ - * / %`; 除/模零 → None; 非算术算子 → None）。
fn arith_f64(op: BinOp, ln: f64, rn: f64) -> Option<f64> {
    if rn == 0.0 && matches!(op, BinOp::Div | BinOp::Mod) {
        return None;
    }
    match op {
        BinOp::Add => Some(ln + rn),
        BinOp::Sub => Some(ln - rn),
        BinOp::Mul => Some(ln * rn),
        BinOp::Div => Some(ln / rn),
        BinOp::Mod => Some(ln % rn),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 数值内核（2026-09-06 拆分回归）: `%` 双 Int 原生取模（更精确）; 非 Mod 的
    /// Int/Int 与任何含 Float 的组合走 f64; 除/模零与非数值/非算术算子 → None。
    #[test]
    fn arithmetic_int_mod_native_and_zero_guards() {
        assert_eq!(
            arithmetic(BinOp::Mod, &CScalar::Int(7), &CScalar::Int(3)),
            Some(CScalar::Int(1))
        );
        assert_eq!(
            arithmetic(BinOp::Mod, &CScalar::Int(-7), &CScalar::Int(3)),
            Some(CScalar::Int(-1))
        );
        assert_eq!(
            arithmetic(BinOp::Mod, &CScalar::Int(7), &CScalar::Int(0)),
            None,
            "int 除零 → null"
        );
        assert_eq!(
            arithmetic(BinOp::Div, &CScalar::Float(1.0), &CScalar::Float(0.0)),
            None,
            "f64 除零 → null"
        );
        assert_eq!(
            arithmetic(BinOp::Mod, &CScalar::Int(5), &CScalar::Float(0.0)),
            None,
            "混合路径模零 → null"
        );
        // 混合 Int/Float → f64; 非 Mod 的 Int/Int 同样 f64。
        assert_eq!(
            arithmetic(BinOp::Add, &CScalar::Int(2), &CScalar::Float(0.5)),
            Some(CScalar::Float(2.5))
        );
        assert_eq!(
            arithmetic(BinOp::Mul, &CScalar::Int(3), &CScalar::Int(4)),
            Some(CScalar::Float(12.0))
        );
        // 比较算子不是算术 → None; 非数值操作数 → None。
        assert_eq!(arithmetic(BinOp::Eq, &CScalar::Int(1), &CScalar::Int(1)), None);
        assert_eq!(arithmetic(BinOp::Add, &CScalar::Str("a".into()), &CScalar::Int(1)), None);
    }

    /// 比较内核（2026-09-06 拆分回归）: Str 有序 / Bool 仅 Eq/Ne / Int 原生 /
    /// Int↔Float epsilon 相等 / Structured 恒 false。
    #[test]
    fn compare_scalars_str_bool_numeric_and_structured() {
        assert!(compare_scalars(
            BinOp::Lt,
            &CScalar::Str("a".into()),
            &CScalar::Str("b".into())
        ));
        assert!(compare_scalars(
            BinOp::Ge,
            &CScalar::Str("b".into()),
            &CScalar::Str("a".into())
        ));
        assert!(!compare_scalars(
            BinOp::Lt,
            &CScalar::Str("a".into()),
            &CScalar::Str("a".into())
        ));
        assert!(compare_scalars(
            BinOp::Eq,
            &CScalar::Bool(true),
            &CScalar::Bool(true)
        ));
        assert!(compare_scalars(
            BinOp::Ne,
            &CScalar::Bool(true),
            &CScalar::Bool(false)
        ));
        assert!(!compare_scalars(
            BinOp::Eq,
            &CScalar::Bool(true),
            &CScalar::Bool(false)
        ));
        assert!(
            !compare_scalars(BinOp::Lt, &CScalar::Bool(false), &CScalar::Bool(true)),
            "Bool 仅支持 Eq/Ne"
        );
        assert!(compare_scalars(BinOp::Lt, &CScalar::Int(1), &CScalar::Int(2)));
        assert!(compare_scalars(
            BinOp::Eq,
            &CScalar::Int(1),
            &CScalar::Float(1.0)
        ));
        // Structured 与任何值比较 → false。
        assert!(!compare_scalars(
            BinOp::Eq,
            &CScalar::Structured,
            &CScalar::Int(1)
        ));
        assert!(!compare_scalars(
            BinOp::Ne,
            &CScalar::Str("x".into()),
            &CScalar::Structured
        ));
    }
}
