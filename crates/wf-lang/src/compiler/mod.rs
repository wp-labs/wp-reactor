use std::collections::HashSet;
use std::time::Duration;

use crate::ast::{
    BoundVal, CloseMode, EachClause, EntityClause, EntityTypeVal, EventsBlock, Expr, FieldRef,
    LetDecl, MatchArm, MatchClause, Measure, PathSegment, RuleDecl, ScoreExpr, SeqSkip, WflFile,
    WindowMode, WithinSpec, YieldClause,
};
use crate::checker::check_wfl;
use crate::plan::{
    AggPlan, BindPlan, BranchPlan, ConvChainPlan, ConvOpPlan, ConvPlan, ConvWindowPlan, EachPlan,
    EntityPlan, ExceedAction, ExprPlan, JoinCondPlan, JoinKeyPlan, JoinPlan, KeyMapPlan, LetPlan,
    LimitsPlan, MatchPlan, PatternOriginPlan, RateSpec, RulePlan, ScorePlan, SeqPlan, SeqSkipPlan,
    SeqStepPlan, SortKeyPlan, SpillMode, StatsAggPlan, StatsMeasurePlan, StatsOutputShapePlan,
    StatsPlan, StepPlan, WindowSpec, YieldField, YieldPlan,
};
use crate::schema::WindowSchema;
use crate::yield_preset::expand_yield_args;
use crate::{LangReason, LangResult};
use orion_error::conversion::ToStructError;

pub mod lists;

// 拆件（2026-09-04）：规则装配 / match 结构 / 绑定跟踪·字段历史 / 部件 plan 四组
// 下沉为 sibling 子模块，各以 `use super::*` 继承根 import 命名空间；根层保留编译
// 入口与可见性接线。被引路径与可见级不变：`parse_byte_size`（checker/rules/limits
// 直调）经对等 `pub(crate)` re-export；绑定跟踪族仅 compiler/tests 直调，为
// cfg(test) 绑定——lib 构建无生产消费点。
mod bind_tracking;
mod clause_build;
mod match_build;
mod rules;

pub(crate) use clause_build::parse_byte_size;

#[cfg(test)]
pub(crate) use bind_tracking::{
    BindTracking, collect_bind_tracking, collect_bind_tracking_aliases, collect_rule_bind_tracking,
    collect_rule_bind_tracking_aliases,
};

// 编译入口派发（compile_wfl_after_semantic_checks 直调）。
use rules::compile_rule;

#[cfg(test)]
mod tests;

/// Compile a parsed WFL file into executable `RulePlan`s.
///
/// Runs semantic checks (`check_wfl`) first; returns an error if any check
/// fails. This validates the current file against the provided schemas,
/// including intermediate-window system fields and file-local yield topology.
///
/// Contracts, use declarations, and meta blocks are stripped — only rule
/// logic is compiled.
pub fn compile_wfl(file: &WflFile, schemas: &[WindowSchema]) -> LangResult<Vec<RulePlan>> {
    // 顶层列表引用（issue #73）先展开——checker 只见到字面 InList（既有类型
    // 检查原样生效）, 未知名/非法位置在此报错。use 导入在加载层完成
    // （`lists::resolve_imports`）, 这里只管本文件内已合并的列表展开。
    let file = lists::resolve_list_refs(file)?;
    let errors = check_wfl(&file, schemas);
    let hard_errors: Vec<_> = errors
        .iter()
        .filter(|e| e.severity == crate::checker::Severity::Error)
        .collect();
    if !hard_errors.is_empty() {
        let msgs: Vec<String> = hard_errors.iter().map(|e| e.to_string()).collect();
        return LangReason::Compile
            .to_err()
            .with_detail(format!("semantic errors:\n{}", msgs.join("\n")))
            .err();
    }
    compile_wfl_after_semantic_checks(&file, schemas)
}

pub(crate) fn compile_wfl_after_semantic_checks(
    file: &WflFile,
    schemas: &[WindowSchema],
) -> LangResult<Vec<RulePlan>> {
    let mut plans = Vec::new();
    for rule in &file.rules {
        plans.extend(compile_rule(rule, file, schemas)?);
    }
    Ok(plans)
}
