//! M2a 辅助工具：统计规则集中 guard 表达式的「纯列运算」覆盖率。
//!
//! 用法：
//!   cargo run -p wf-engine --example guard_coverage -- <throughput.wfl> <network.wfs>
//!
//! 编译规则集后遍历每个 RulePlan 的 guard（bind filter / 事件步骤 branch guard /
//! close 步骤 branch guard / seq branch guard / on-each filter），用
//! `wf_lang::columnar::expr_is_columnar` 分类并打印统计。

use std::env;
use std::fs;

use wf_lang::ast::Expr;
use wf_lang::plan::{BranchPlan, RulePlan};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: guard_coverage <throughput.wfl> <network.wfs>");
        std::process::exit(2);
    }
    let wfl_path = &args[1];
    let wfs_path = &args[2];

    let wfl_src = fs::read_to_string(wfl_path).expect("read wfl");
    let wfs_src = fs::read_to_string(wfs_path).expect("read wfs");

    let wfl_file = wf_lang::parse_wfl(&wfl_src).expect("parse wfl");
    let schemas = wf_lang::parse_wfs(&wfs_src).expect("parse wfs");
    let plans = wf_lang::compile_wfl(&wfl_file, &schemas).expect("compile wfl");

    let mut total = 0usize;
    let mut columnar = 0usize;
    let mut non_columnar: Vec<(String, String)> = Vec::new();

    for plan in &plans {
        for guard in plan_guards(plan) {
            total += 1;
            if wf_lang::columnar::expr_is_columnar(&guard) {
                columnar += 1;
            } else {
                non_columnar.push((plan.name.clone(), format!("{guard:?}")));
            }
        }
    }

    println!("rules: {}", plans.len());
    println!("guards: {total}");
    println!(
        "columnar: {columnar} ({:.1}%)",
        columnar as f64 / total.max(1) as f64 * 100.0
    );
    println!(
        "non-columnar: {} ({:.1}%)",
        non_columnar.len(),
        non_columnar.len() as f64 / total.max(1) as f64 * 100.0
    );
    println!();
    println!("--- non-columnar guards ---");
    for (rule, expr) in &non_columnar {
        println!("{rule}: {expr}");
    }
}

/// All guard expressions a rule evaluates on bound events.
fn plan_guards(plan: &RulePlan) -> Vec<Expr> {
    let mut out = Vec::new();

    for bind in &plan.binds {
        if let Some(filter) = &bind.filter {
            out.push(filter.clone());
        }
    }

    for step in plan
        .match_plan
        .event_steps
        .iter()
        .chain(plan.match_plan.close_steps.iter())
    {
        for branch in &step.branches {
            collect_branch_guard(branch, &mut out);
        }
    }

    if let Some(seq) = &plan.match_plan.seq {
        for step in &seq.steps {
            collect_branch_guard(&step.branch, &mut out);
        }
    }

    if let Some(each) = &plan.each_plan
        && let Some(filter) = &each.filter
    {
        out.push(filter.clone());
    }

    out
}

fn collect_branch_guard(branch: &BranchPlan, out: &mut Vec<Expr>) {
    if let Some(guard) = &branch.guard {
        out.push(guard.clone());
    }
}
