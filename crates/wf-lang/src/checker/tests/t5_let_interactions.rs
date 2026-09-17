//! T5 规则级 `let` 的**跨特性交互**用例（阈值常量内联的边界，issue #101 review A 轮）。
//!
//! 单点行为已在 `t5_threshold.rs` 锁定；这里补充「与 `||` 分支 / 非阈值位置 /
//! 字段同名 / 子句顺序 / 前向引用」相遇时的行为，避免内联只覆盖最直白的一条路径。

use super::*;

use crate::explain::explain_rules;
use crate::wfl_parser::parse_wfl;

fn compile(src: &str) -> Vec<crate::plan::RulePlan> {
    let file = parse_wfl(src).expect("parse should succeed");
    crate::compile_wfl(&file, &[auth_events_window(), output_window()]).expect("compile")
}

fn explain_entity_id(src: &str) -> String {
    let schemas = [auth_events_window(), output_window()];
    explain_rules(&compile(src), &schemas)[0].entity_id.clone()
}

/// `||` 多分支各自引用**不同**的常量 `let`：两侧都必须内联为字面量
/// （内联是按分支独立做的，不能只覆盖第一个分支）。
#[test]
fn or_branches_inline_their_own_const_let() {
    let src = r#"
rule r {
    events { e : auth_events }
    let A = 2
    let B = 3
    match<sip:5m> {
        on event { e.action | distinct | count >= A || e.action | distinct | count >= B; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(src, &[auth_events_window(), output_window()]);
    let schemas = [auth_events_window(), output_window()];
    let expl = explain_rules(&compile(src), &schemas);
    let step = &expl[0].match_expl.event_steps[0];
    assert_eq!(
        step, "e.action | distinct | count >= 2.0 || e.action | distinct | count >= 3.0",
        "两个分支都必须内联为字面量，实际: {step}"
    );
}

/// `||` 分支里只要有一个引用非常量 `let`，该分支就必须在编译期被拒绝
/// （不能因为另一个分支合法而整体放行）。
#[test]
fn or_branch_nonconst_let_rejected() {
    let src = r#"
rule r {
    events { e : auth_events }
    let B = e.action
    match<sip:5m> {
        on event { e.action | distinct | count >= 2 || e.action | distinct | count >= B; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        src,
        &[auth_events_window(), output_window()],
        "cannot be evaluated by the trigger check",
    );
}

/// 布尔常量 `let` 作阈值：内联成功，但类型判定必须给出 T5 的「类型不兼容」，
/// 而不是把问题归到「不可求值」（错误分类决定用户该去改类型还是改常量）。
#[test]
fn bool_const_let_threshold_is_type_error() {
    let src = r#"
rule r {
    events { e : auth_events }
    let T = true
    match<sip:5m> {
        on event { e.action | distinct | count >= T; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        src,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
    let errs = check_errors(src, &[auth_events_window(), output_window()]);
    assert!(
        !errs.iter().any(|e| e.contains("cannot be evaluated")),
        "应是类型错误而不是常量性错误，实际: {errs:?}"
    );
}

/// `let` 与字段同名时不得改变**非阈值位置**的解析：`entity(ip, sip)` 仍解析为
/// 事件字段（而不是被 `let sip = 3` 抢走），否则实体键会静默变成常量。
#[test]
fn let_shadowing_field_name_does_not_change_entity_resolution() {
    let control = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e.action | distinct | count >= 1; }
    } -> score(50.0)
    entity(ip, sip)
    yield out (x = e.sip)
}
"#;
    let shadowed = r#"
rule r {
    events { e : auth_events }
    let sip = 3
    match<sip:5m> {
        on event { e.action | distinct | count >= 1; }
    } -> score(50.0)
    entity(ip, sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(shadowed, &[auth_events_window(), output_window()]);
    assert_eq!(
        explain_entity_id(control),
        explain_entity_id(shadowed),
        "let 与字段同名不得把实体键变成常量"
    );
}

/// `let` 声明在 `match` 之后是**语法错误**（子句顺序固定），不可能形成
/// 「阈值先引用、后声明」的隐性前向引用。
#[test]
fn let_declared_after_match_is_a_syntax_error() {
    let src = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e.action | distinct | count >= T; }
    } -> score(50.0)
    let T = 3
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(
        parse_wfl(src).is_err(),
        "match 之后的 let 必须解析失败（编译期失败闭合）"
    );
}

/// 前向引用（`let A = B` 而 `B` 声明在后）：必须报出**声明顺序**错误，
/// 不能退化成误导性的 `field ... not found in any event source`。
#[test]
fn forward_let_reference_reports_declaration_order() {
    let src = r#"
rule r {
    events { e : auth_events }
    let A = B
    let B = 3
    match<sip:5m> {
        on event { e.action | distinct | count >= A; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let schemas = [auth_events_window(), output_window()];
    assert_has_error(src, &schemas, "before its declaration");
    assert!(
        !check_errors(src, &schemas)
            .iter()
            .any(|e| e.contains("field `B` not found")),
        "不应再出现误导性的字段缺失噪音"
    );
}

/// 名字既是**字段**又是**声明在后**的 `let` 时（`let A = sip` + `let sip = 3`）：
/// 该引用仍解析为字段（既有语义，`let` 名不与字段冲突时字段生效），不得因为
/// 新增的前向引用提示而误拒——文案改进必须是零行为变化。
#[test]
fn forward_looking_name_that_is_also_a_field_is_still_resolved_as_field() {
    let src = r#"
rule r {
    events { e : auth_events }
    let A = sip
    let sip = 3
    match<sip:5m> {
        on event { e.action | distinct | count >= 1; }
    } -> score(50.0)
    entity(ip, A)
    yield out (x = e.sip)
}
"#;
    let schemas = [auth_events_window(), output_window()];
    assert_no_errors(src, &schemas);
    assert_eq!(explain_entity_id(src), "A");
}
