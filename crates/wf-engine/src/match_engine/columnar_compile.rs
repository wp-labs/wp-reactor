//! Columnar 编译面（2026-09-04 自 columnar.rs 拆出）：guard / yield 表达式 → 批无关
//! [`ColumnExpr`] 树（编译期内联 let、字段物化、结构化参数递归拦截）。

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, BooleanBuilder, Float64Builder, StringBuilder};
use arrow::datatypes::Field as ArrowField;
use wf_lang::ast::{BinOp, Expr, FieldRef, MatchArm, PathSegment};

use super::*;
use crate::match_engine::cep::{Value, field_ref_name};
use crate::match_engine::wfl_structured_field_kind;

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
