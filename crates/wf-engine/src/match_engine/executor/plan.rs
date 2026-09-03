//! on-each 列式门控/join plan 解析组（2026-09-04 自 each_exec.rs 拆出）：
//! 形状分类原语（ScoreShape/ScorePlan、score/entity 一般表达式门控）、each+join
//! 列式化解析（EachJoinPlan/WherePred/parse_each_join_columnar）与列式门使用的
//! 表达式静态检查（fmt 单参恒等、let 引用）。执行器组织见 `executor/mod.rs`。

use wf_lang::ast::{BinOp, Expr, FieldRef, JoinMode};
use wf_lang::plan::{JoinPlan, RulePlan};

use crate::match_engine::cep::{Value, field_ref_name};
use crate::match_engine::event_bridge::ColumnarEvent;

/// Score 表达式形状（列式门控）：常量，或「常量 × 字段」（q1 的
/// `score(0.908 * b.price)`）。常量×字段可在列式路径按行从 Arrow 列读 f64
/// 乘常量——与解释求值 `ln * rn` 字节一致（IEEE f64 乘法交换，clamp 相同）。
/// 其他形状（含 Add/Div、字段×字段）仍回退行式，保持两路径字节一致。
pub(crate) enum ScoreShape<'a> {
    Const(f64),
    MulConst { const_v: f64, field: &'a FieldRef },
}

pub(crate) fn score_shape(expr: &Expr) -> Option<ScoreShape<'_>> {
    match expr {
        Expr::Number(n) => Some(ScoreShape::Const(*n)),
        Expr::BinOp {
            op: BinOp::Mul,
            left,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expr::Number(c), Expr::Field(fr)) | (Expr::Field(fr), Expr::Number(c)) => {
                Some(ScoreShape::MulConst {
                    const_v: *c,
                    field: fr,
                })
            }
            _ => None,
        },
        _ => None,
    }
}

/// 列式执行时的 score 求值计划（`ScoreShape` 的拥有版本）。
#[derive(Clone)]
pub(crate) enum ScorePlan {
    Const(f64),
    MulConst { const_v: f64, field: FieldRef },
}

impl ScorePlan {
    pub(super) fn parse(expr: &Expr) -> Option<ScorePlan> {
        match score_shape(expr)? {
            ScoreShape::Const(n) => Some(ScorePlan::Const(n)),
            ScoreShape::MulConst { const_v, field } => Some(ScorePlan::MulConst {
                const_v,
                field: field.clone(),
            }),
        }
    }

    pub(super) fn field(&self) -> Option<&FieldRef> {
        match self {
            ScorePlan::Const(_) => None,
            ScorePlan::MulConst { field, .. } => Some(field),
        }
    }

    /// 按行求值：常量直接返回；常量×字段从 `score_idx` 列读 f64 乘常量。
    /// 返回 None = 字段缺失/非数值（与解释路径 `eval_score` 的 Err 对应）。
    pub(super) fn eval(&self, event: &ColumnarEvent<'_>, score_idx: Option<usize>) -> Option<f64> {
        match self {
            ScorePlan::Const(n) => Some(n.clamp(0.0, 100.0)),
            ScorePlan::MulConst { const_v, .. } => {
                let idx = score_idx?;
                let v = event.value_at(idx)?;
                match v {
                    Value::Number(n) => Some((n * const_v).clamp(0.0, 100.0)),
                    _ => None,
                }
            }
        }
    }
}

/// flat FieldRef（Simple/Qualified/Bracketed）——score/entity 快通道字段定义
/// （无活 join 时 out_shape_ok 的字段形状；本执行器只服务无活 join 的
/// each-direct 列式路径）。
fn is_flat_field(field: &FieldRef) -> bool {
    matches!(
        field,
        FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)
    )
}

/// score 是否为「一般列式表达式」（非 常量 / 常量×**flat** 快通道形状）——gate
/// 的 score_ok 与列式执行器的 score_cvec 槽位共用同一分类（gap-6 2026-09-02）。
/// 常量×list-index 字段（`0.5 * c.tags[0]`）**不是**快通道形状：快车道
/// `value_at` 只读 flat 列，索引元素需 offset 读 → 归一般（编译
/// ListIndex × 常量 cvec）。
pub(crate) fn score_is_general(expr: &Expr) -> bool {
    match score_shape(expr) {
        Some(ScoreShape::Const(_)) => false,
        Some(ScoreShape::MulConst { field, .. }) => !is_flat_field(field),
        None => true,
    }
}

/// entity 是否为「一般列式表达式」（非 StringLit / flat Field 快通道形状）——
/// gate 放行与执行器 entity_cvec 槽位共用同一分类（gap-7 2026-09-02）。
pub(crate) fn entity_is_general(expr: &Expr) -> bool {
    !matches!(expr, Expr::StringLit(_)) && !matches!(expr, Expr::Field(fr) if is_flat_field(fr))
}

// L3 batched write (now unconditional): collect a segment's column values and
// bulk-`extend` each builder column once at the end via
// `commit_each_rows_batch`, instead of per-row `commit_each_row`. Cell staging
// still runs through the builder (same validation+export); only the final
// column push is batched. Byte-identical to the per-row commit (see the
// `commit_each_rows_batch_*` equivalence tests) — Q1 on-each is fill-bound and
// this is ~4× cheaper on CPU and ~half the RSS.

/// Columnar join-enrichment plan for `on each` + one live Snapshot join
/// (2026-08-23, 列式 join 富化 — q20 等 each+join 查询 2.5M/s → 列式量级).
///
/// v1 形状（q20 等）：单 Snapshot join、单条件、左右均 flat 限定引用；
/// `where` 为「右窗限定字段 <cmp> 字面量」的合取；yield/entity 为字面量 /
/// 左窗（驱动）限定字段 / 右窗限定字段。行式路径（`execute_each_direct`）
/// 每事件 `Event::clone()` + `enrich_join_row` 全字段注入 + `find_matching_row`
/// 复核；列式版批级去重 join_lookup + 列式读右窗字段，输出字节一致。
#[derive(Debug, Clone)]
pub(crate) struct EachJoinPlan {
    /// 右窗名（enrich 限定前缀，如 `auction_events`）。
    pub(super) right_window: String,
    /// 右窗 join key 字段（索引键，如 `auction_events.id`）。
    pub(super) right_key_field: String,
    /// 左字段名（驱动列，如 `b.auction`）。
    pub(super) left_field: String,
    /// 驱动 bind alias（如 `b`），区分左窗/右窗限定引用。
    pub(super) left_alias: String,
    /// `where` 谓词（右窗字段 <cmp> 字面量，合取）。空 = 无 where。
    pub(super) where_preds: Vec<WherePred>,
}

/// 一个 `where` 谓词：右窗字段 `<op> 字面量`。
#[derive(Debug, Clone)]
pub(super) struct WherePred {
    pub(super) field: String,
    pub(super) op: wf_lang::ast::BinOp,
    pub(super) const_val: Value,
}

/// 解析 each 规则的列式 join 支持性。`Some` = 可走列式 join 路径；
/// `None` = 形状不支持（回退行式 `execute_each_direct`）。
///
/// 基于 `live_joins`（死 join 消除后）解析——死 join 不参与执行，规则有 1 死
/// 1 活 join 时活 join 若满足形状仍可列式化（2026-08-23 review：旧版基于
/// `plan.joins`，死 join 存在时误拒活 join）。
pub(crate) fn parse_each_join_columnar(
    plan: &RulePlan,
    live_joins: &[JoinPlan],
) -> Option<EachJoinPlan> {
    let join = live_joins.first()?;
    if live_joins.len() != 1 {
        return None;
    }
    if !matches!(join.mode, JoinMode::Snapshot) {
        return None;
    }
    if join.within.is_some() || join.reduce.is_some() || join.emit_at.is_some() {
        return None;
    }
    if join.conds.len() != 1 {
        return None;
    }
    let cond = &join.conds[0];
    let left_field = field_ref_name(&cond.left).to_string();
    let right_key_field = field_ref_name(&cond.right).to_string();
    if left_field.is_empty() || right_key_field.is_empty() {
        return None;
    }
    // 左右 key 必须 flat（Simple/Qualified/Bracketed）——Path（嵌套 object）
    // 在列式路径下无法按列名解析。
    let flat = |fr: &FieldRef| {
        matches!(
            fr,
            FieldRef::Simple(_) | FieldRef::Qualified(_, _) | FieldRef::Bracketed(_, _)
        )
    };
    if !flat(&cond.left) || !flat(&cond.right) {
        return None;
    }
    let left_alias = plan.each_plan.as_ref()?.alias.clone();
    let right_window = join.right_window.clone();
    // join 条件左字段的限定符必须是驱动别名或裸字段（checker 保证左字段来自
    // 驱动事件；此处防御——Qualified 其他窗名时列式无法从驱动列解析）。
    if let FieldRef::Qualified(win, _) = &cond.left
        && win.as_str() != left_alias
    {
        return None;
    }

    // where：右窗限定字段 <cmp> 字面量 的合取（&&）。其他形状（左窗字段、
    // 函数、Simple 引用、`in` 列表）→ 不支持 → 回退行式。
    let mut where_preds = Vec::new();
    if let Some(w) = &plan.r#where
        && !parse_where_preds(w, &right_window, &mut where_preds)
    {
        return None;
    }

    // 输出字段来源：每个引用必须是 字面量 / 左窗限定 / 右窗限定。
    // Simple/Bracketed/Path/一般表达式 → 不支持（无法确定来源，保守回退）。
    let out_ok = |fr: &FieldRef| -> bool {
        match fr {
            FieldRef::Qualified(win, _) => win == &left_alias || win == &right_window,
            _ => false,
        }
    };
    for field in &plan.yield_plan.fields {
        match &field.value {
            Expr::Number(_) | Expr::StringLit(_) | Expr::Bool(_) => {}
            Expr::Field(fr) => {
                if !out_ok(fr) {
                    return None;
                }
            }
            // 2026-08-25 q13b 列式化：`fmt("{}", 左/右窗 flat 字段)` = 字段值的
            // 字符串渲染（fmt 单参数恒等，模板恰为 "{}"）。列式 join 路径读
            // 字段后按 fmt 语义渲染（Str 透传 / 非 Str `value_to_string`），
            // 免 row path 的 Event clone + fmt 解释（q13b 1.3µs → 列式 462ns，
            // 分配量大降——q13a 分片放开后 RSS 28.9GB 的分配大头）。
            Expr::FuncCall {
                qualifier: None,
                name,
                args,
            } if name == "fmt"
                && args.len() == 2
                && matches!(&args[0], Expr::StringLit(t) if t == "{}")
                && matches!(&args[1], Expr::Field(fr) if out_ok(fr)) => {}
            _ => return None,
        }
    }
    match &plan.entity_plan.entity_id_expr {
        Expr::StringLit(_) => {}
        Expr::Field(fr) => {
            if !out_ok(fr) {
                return None;
            }
        }
        _ => return None,
    }
    Some(EachJoinPlan {
        right_window,
        right_key_field,
        left_field,
        left_alias,
        where_preds,
    })
}

/// 递归解析 `where` 为右窗字段比较的合取。
fn parse_where_preds(expr: &Expr, right_window: &str, out: &mut Vec<WherePred>) -> bool {
    match expr {
        Expr::BinOp {
            op: BinOp::And,
            left,
            right,
        } => {
            parse_where_preds(left, right_window, out)
                && parse_where_preds(right, right_window, out)
        }
        Expr::BinOp { op, left, right }
            if matches!(
                op,
                BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Gt | BinOp::Le | BinOp::Ge
            ) =>
        {
            let Expr::Field(FieldRef::Qualified(win, f)) = left.as_ref() else {
                return false;
            };
            if win != right_window {
                return false;
            }
            let const_val = match right.as_ref() {
                Expr::Number(n) => Value::Number(*n),
                Expr::StringLit(s) => Value::Str(s.clone().into()),
                Expr::Bool(b) => Value::Bool(*b),
                _ => return false,
            };
            out.push(WherePred {
                field: f.clone(),
                op: *op,
                const_val,
            });
            true
        }
        _ => false,
    }
}

/// `fmt("{}", fr)` 单参数恒等（q13b 列式化）：模板恰为 `"{}"` 且参数是
/// 单字段引用。语义 = `value_to_string(字段值)`；Str 透传、非 Str 渲染——
/// 与解释器 fmt 的 `apply_fmt_template` 逐字节一致（对拍锁定）。
/// `None` = 不是该形状 → 行式回退。
pub(crate) fn fmt_identity_field(expr: &Expr) -> Option<&FieldRef> {
    match expr {
        Expr::FuncCall {
            qualifier: None,
            name,
            args,
        } if name == "fmt"
            && args.len() == 2
            && matches!(&args[0], Expr::StringLit(t) if t == "{}") =>
        {
            match &args[1] {
                Expr::Field(fr) => Some(fr),
                _ => None,
            }
        }
        _ => None,
    }
}

/// 表达式是否引用（裸名）let 变量——列式 mask/score 无 let 视图，非 yield
/// 表达式引用 let 变量会静默读空（失真）；只有 yield 的 let 引用经编译期
/// 内联展开（安全）。只匹配 `FieldRef::Simple`（let 以裸名注入 ctx，限定
/// 引用走窗口字段）。
pub(crate) fn expr_refs_let(expr: &Expr, let_names: &std::collections::HashSet<&str>) -> bool {
    match expr {
        Expr::Field(fr) => {
            matches!(fr, FieldRef::Simple(name) if let_names.contains(name.as_str()))
        }
        Expr::BinOp { left, right, .. } => {
            expr_refs_let(left, let_names) || expr_refs_let(right, let_names)
        }
        Expr::Neg(inner) | Expr::Not(inner) => expr_refs_let(inner, let_names),
        Expr::Array(items) => items.iter().any(|i| expr_refs_let(i, let_names)),
        Expr::InList {
            expr: inner, list, ..
        } => expr_refs_let(inner, let_names) || list.iter().any(|i| expr_refs_let(i, let_names)),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            expr_refs_let(cond, let_names)
                || expr_refs_let(then_expr, let_names)
                || expr_refs_let(else_expr, let_names)
        }
        Expr::Match {
            expr,
            arms,
            default,
        } => {
            expr_refs_let(expr, let_names)
                || arms.iter().any(|arm| {
                    arm.patterns.iter().any(|p| expr_refs_let(p, let_names))
                        || expr_refs_let(&arm.value, let_names)
                })
                || default
                    .as_ref()
                    .is_some_and(|d| expr_refs_let(d, let_names))
        }
        Expr::Object(items) => items.iter().any(|it| expr_refs_let(&it.value, let_names)),
        Expr::FuncCall { args, .. } => args.iter().any(|a| expr_refs_let(a, let_names)),
        _ => false,
    }
}
