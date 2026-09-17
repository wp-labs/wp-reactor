use super::*;

use crate::wfl_parser::parse_wfl;

// =========================================================================
// T5: Threshold type compatibility — unit tests
// =========================================================================

#[test]
fn min_chars_vs_numeric_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | min >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

#[test]
fn max_chars_vs_numeric_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | max >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

#[test]
fn min_digit_vs_numeric_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.count | min >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn min_chars_vs_string_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | min >= "abc"; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn count_vs_string_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e | count >= "abc"; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

// =========================================================================
// T5: Threshold type compatibility — integration tests
// =========================================================================

/// Integration: T5 error propagates through compile_wfl, rejecting the rule.
#[test]
fn t5_compile_rejects_min_chars_numeric() {
    let input = r#"
rule hostname_anomaly {
    events { e : auth_events && action == "login" }
    match<sip:5m> {
        on event {
            e.user | min >= 1;
        }
    } -> score(60.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let errs = check_errors(input, &[auth_events_window(), output_window()]);
    assert!(
        errs.iter()
            .any(|e| e.contains("not compatible") && e.contains("min()")),
        "expected T5 error for min(Chars) vs Digit threshold, got: {:?}",
        errs
    );
}

/// Integration: a multi-branch rule where only one branch has a T5 mismatch.
#[test]
fn t5_multi_branch_only_bad_branch_errors() {
    let input = r#"
rule multi_measure {
    events { e : auth_events }
    match<sip:5m> {
        on event {
            e | count >= 3;
            e.count | sum >= 10;
            e.action | max >= 100;
        }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let errs = check_errors(input, &[auth_events_window(), output_window()]);
    let t5_errors: Vec<_> = errs
        .iter()
        .filter(|e| e.contains("not compatible"))
        .collect();
    assert_eq!(
        t5_errors.len(),
        1,
        "expected exactly 1 T5 error (from the max(Chars) branch), got: {:?}",
        t5_errors
    );
    assert!(
        t5_errors[0].contains("max()"),
        "T5 error should mention max(), got: {}",
        t5_errors[0]
    );
}

/// Integration: T5 catches mismatch when threshold is a float literal.
#[test]
fn t5_float_threshold_vs_chars_caught() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.action | min >= 1.5; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

/// Integration: Time field with min — string literal threshold is Chars vs Time.
#[test]
fn t5_min_time_vs_string_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.event_time | min >= "2024-01-01"; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

/// Integration: avg() always returns Float. A Digit threshold is compatible.
#[test]
fn t5_avg_digit_threshold_compatible() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.count | avg >= 5; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

/// Integration: avg() returns Float, but a Chars threshold is incompatible.
#[test]
fn t5_avg_string_threshold_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<:5m> {
        on event { e.count | avg >= "high"; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not compatible",
    );
}

// =========================================================================
// T5: Illustrative example — walkthrough of the check data flow
// =========================================================================

#[test]
fn t5_example_walkthrough() {
    let input = r#"
rule bad_hostname_check {
    events { e : auth_events && action == "login" }
    match<sip:5m> {
        on event {
            e.action | min >= 1;
        }
    } -> score(40.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("rule is syntactically valid");

    let schemas = &[auth_events_window(), output_window()];
    let errs = check_wfl(&file, schemas);

    let t5_errs: Vec<_> = errs
        .iter()
        .filter(|e| e.message.contains("not compatible"))
        .collect();
    assert_eq!(
        t5_errs.len(),
        1,
        "expected exactly 1 T5 error, got: {:?}",
        t5_errs
    );

    let msg = &t5_errs[0].message;
    assert!(
        msg.contains("min()"),
        "error should name the measure: {msg}"
    );
    assert!(
        msg.contains("Chars"),
        "error should mention the result type Chars: {msg}"
    );
    assert!(
        msg.contains("Digit"),
        "error should mention the threshold type Digit: {msg}"
    );

    assert_eq!(
        t5_errs[0].rule.as_deref(),
        Some("bad_hostname_check"),
        "error should be attributed to the rule"
    );
}

// =========================================================================
// F1（warp-fusion#101）：阈值必须是编译期常量
//
// 触发判定（wf-cep `check_threshold`）只做常量折叠；字段引用 / 函数调用求值不出
// 结果 → 分支永久「不满足」且运行期无信号。此前 checker 漏检，这里锁定为编译期错误。
// =========================================================================

/// issue #101 的最小复现：L3 序列函数写在 threshold 里。
#[test]
fn l3_function_rejected_in_threshold() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e.action | distinct | count >= first(e.count); }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, &[auth_events_window(), output_window()]);
    let hit: Vec<_> = errs
        .iter()
        .filter(|e| {
            e.severity == Severity::Error
                && e.message.contains("not allowed in threshold expressions")
        })
        .collect();
    assert_eq!(
        hit.len(),
        1,
        "expected exactly 1 constant-threshold error, got: {errs:?}"
    );
    let msg = &hit[0].message;
    assert!(msg.contains("first()"), "错误应点名函数: {msg}");
    assert!(
        msg.contains("score/entity/yield"),
        "错误应给出可去处（score/entity/yield）: {msg}"
    );
    assert_eq!(hit[0].rule.as_deref(), Some("r"), "错误应归属到规则");
    // 用户实际加载规则的路径（compile_wfl）同样拒绝，而不是只在 check_wfl 层面。
    assert!(
        crate::compile_wfl(&file, &[auth_events_window(), output_window()]).is_err(),
        "compile_wfl 也必须拒绝该规则"
    );
}

/// 字段引用：可由事件求值，但触发判定不逐事件求值 → 同样静默失效。
#[test]
fn field_ref_threshold_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e.action | distinct | count >= e.count; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, &[auth_events_window(), output_window()]);
    let msgs: Vec<&str> = errs
        .iter()
        .filter(|e| e.severity == Severity::Error)
        .map(|e| e.message.as_str())
        .collect();
    assert_eq!(msgs.len(), 1, "应恰好一条错误，实际: {msgs:?}");
    assert!(
        msgs[0].contains("cannot be evaluated by the trigger check"),
        "错误应说明触发判定无法求值: {}",
        msgs[0]
    );
    assert!(
        msgs[0].contains("`e.count`"),
        "错误应回显字段引用: {}",
        msgs[0]
    );
}

/// 规则级**常量** `let` 作为阈值：编译期内联为字面量，因此合法可用。
#[test]
fn rule_let_const_threshold_accepted() {
    for branch in [
        "e.action | distinct | count >= THRESHOLD",
        "e.count | min >= THRESHOLD",
    ] {
        let input = format!(
            r#"
rule r {{
    events {{ e : auth_events }}
    let THRESHOLD = 5
    match<sip:5m> {{
        on event {{ {branch}; }}
    }} -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}}
"#
        );
        assert_no_errors(&input, &[auth_events_window(), output_window()]);
    }
}

/// 非常量 `let`（RHS 依赖事件字段）在阈值位置仍被拒绝。
#[test]
fn rule_let_non_const_threshold_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    let thr = e.count
    match<sip:5m> {
        on event { e.action | distinct | count >= thr; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, &[auth_events_window(), output_window()]);
    let msgs: Vec<&str> = errs
        .iter()
        .filter(|e| e.severity == Severity::Error)
        .map(|e| e.message.as_str())
        .collect();
    assert_eq!(msgs.len(), 1, "应恰好一条错误，实际: {msgs:?}");
    assert!(
        msgs[0].contains("cannot be evaluated by the trigger check"),
        "错误应说明触发判定无法求值: {}",
        msgs[0]
    );
}

/// `now*` / `baseline` 与 L3 同类：阈值位置求值不出结果。
#[test]
fn state_dependent_func_threshold_rejected() {
    for threshold in ["now_ms()", "baseline(e.count, 60)"] {
        let input = format!(
            r#"
rule r {{
    events {{ e : auth_events }}
    match<sip:5m> {{
        on event {{ e.action | distinct | count >= {threshold}; }}
    }} -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}}
"#
        );
        assert_has_error(
            &input,
            &[auth_events_window(), output_window()],
            "not allowed in threshold expressions",
        );
    }
}

/// close 步骤走同一个触发判定（wf-cep `close.rs` → `check_threshold`）→ 同样拒绝。
#[test]
fn close_step_threshold_must_be_constant() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e.action | distinct | count >= 1; }
        and close { e.action | distinct | count >= first(e.count); }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in threshold expressions",
    );
}

/// 正例：字面量 / 取负字面量 / 字符串字面量（min/max on chars）保持合法。
#[test]
fn constant_thresholds_accepted() {
    for branch in [
        "e.action | distinct | count >= 1", // 数字字面量
        "e.count | min >= -1",              // 取负字面量
        "e.action | min >= \"abc\"",        // 字符串字面量
    ] {
        let input = format!(
            r#"
rule r {{
    events {{ e : auth_events }}
    match<sip:5m> {{
        on event {{ {branch}; }}
    }} -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}}
"#
        );
        assert_no_errors(&input, &[auth_events_window(), output_window()]);
    }
}

/// 括号常量算术是合法阈值（与引擎的折叠能力一致）。
#[test]
fn parenthesized_constant_arithmetic_thresholds_accepted() {
    for branch in [
        "e.count | min >= (2)",
        "e.count | min >= (1 + 2)",
        "e.count | min >= -(2)",
        "e.action | distinct | count >= (2 * 3)",
    ] {
        let input = format!(
            r#"
rule r {{
    events {{ e : auth_events }}
    match<sip:5m> {{
        on event {{ {branch}; }}
    }} -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}}
"#
        );
        assert_no_errors(&input, &[auth_events_window(), output_window()]);
    }
}

/// 退化常量（除零 / 模零）折叠不出结果 → 与字段引用同判据：编译期拒绝，
/// 而不是放行到运行期「永不触发」。
#[test]
fn degenerate_constant_threshold_rejected() {
    for branch in ["e.count | min >= (1 / 0)", "e.count | min >= (1 % 0)"] {
        let input = format!(
            r#"
rule r {{
    events {{ e : auth_events }}
    match<sip:5m> {{
        on event {{ {branch}; }}
    }} -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}}
"#
        );
        assert_has_error(
            &input,
            &[auth_events_window(), output_window()],
            "cannot be evaluated by the trigger check",
        );
    }
}

/// 不可折叠形态的错误信息必须可读（用 `format_expr` 渲染，而非 AST Debug）。
#[test]
fn non_foldable_threshold_message_is_readable() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e.count | min >= if true then 1 else 2; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    let errs = check_wfl(&file, &[auth_events_window(), output_window()]);
    let msgs: Vec<&str> = errs
        .iter()
        .filter(|e| e.severity == Severity::Error)
        .map(|e| e.message.as_str())
        .collect();
    assert_eq!(msgs.len(), 1, "应恰好一条错误，实际: {msgs:?}");
    assert!(
        msgs[0].contains("threshold `"),
        "应回显阈值表达式: {}",
        msgs[0]
    );
    assert!(
        !msgs[0].contains("IfThenElse"),
        "不应输出 AST Debug: {}",
        msgs[0]
    );
}

/// pipeline stage（`|> match`）的阈值走同一检查。
#[test]
fn pipeline_stage_threshold_must_be_constant() {
    let input = r#"
rule r {
    events { d: auth_events }
    match<sip:5m> {
        on event { ev: d | count >= 1; }
        on close { d | count >= 1; }
    }
    |> match<sip:10m> {
        on event { _in | count >= now_ms(); }
        on close { _in | count >= 1; }
    } -> score(80.0)
    entity(ip, _in.sip)
    yield out (x = _in.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in threshold expressions",
    );
}

/// seq 步骤（`on event seq`）的阈值走同一检查。
#[test]
fn seq_step_threshold_must_be_constant() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event seq {
            e.action | distinct | count >= first(e.count);
        }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in threshold expressions",
    );
}

/// 规则级 `let` 不适用于 pipeline stage（stage scope 无 let 绑定，checker 与
/// compiler 两侧一致传空）→ 常量 let 阈值在这里仍被拒绝，且不会静默失效。
#[test]
fn pipeline_stage_const_let_threshold_rejected() {
    let input = r#"
rule r {
    events { d: auth_events }
    let THRESHOLD = 3
    match<sip:5m> {
        on event { ev: d | count >= 1; }
        on close { d | count >= 1; }
    }
    |> match<sip:10m> {
        on event { _in | count >= THRESHOLD; }
        on close { _in | count >= 1; }
    } -> score(80.0)
    entity(ip, _in.sip)
    yield out (x = _in.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "cannot be evaluated by the trigger check",
    );
}

/// 重名规则级 `let`：编译期报错（否则「接受/拒绝」与绑定语义随声明顺序变化）。
#[test]
fn duplicate_rule_let_names_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    let T = "abc"
    let T = 3
    match<sip:5m> {
        on event { e.action | distinct | count >= T; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "duplicate rule-level `let` name `T`",
    );
}

/// 常量 `let` 在 close 步骤 / seq 链 / `on event<accu>` 位置同样可用（内联后与
/// 手写字面量等价）。
#[test]
fn const_let_threshold_accepted_in_all_match_positions() {
    let cases = [
        // on close 步骤
        r#"
rule r {
    events { e : auth_events }
    let T = 2
    match<sip:5m> {
        on event { e.action | distinct | count >= 1; }
        and close { e.action | distinct | count >= T; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        // seq 链步骤
        r#"
rule r {
    events { e : auth_events }
    let T = 2
    match<sip:5m> {
        on event seq { e.action | distinct | count >= T; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        // accu 事件步骤
        r#"
rule r {
    events { e : auth_events }
    let T = 1
    match<sip:5m> {
        on event<accu> { e.action | distinct | count >= T; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
    ];
    for input in cases {
        assert_no_errors(input, &[auth_events_window(), output_window()]);
    }
}

/// 字符串常量 `let` 可用于 min/max（Chars 字段）阈值——内联为字符串字面量。
#[test]
fn string_const_let_threshold_accepted() {
    let input = r#"
rule r {
    events { e : auth_events }
    let BASELINE = "abc"
    match<sip:5m> {
        on event { e.action | min >= BASELINE; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

// =========================================================================
// 规则级 let 引用链深度上限（与表达式嵌套同口径 5 层；防下游按链递归吃栈）
// =========================================================================

fn chain_rule(depth: usize, use_in_threshold: bool) -> String {
    let mut lets = String::from("let L0 = 7\n");
    for i in 1..depth {
        lets.push_str(&format!("    let L{i} = L{}\n", i - 1));
    }
    let threshold = if use_in_threshold {
        format!("e.action | distinct | count >= L{}", depth - 1)
    } else {
        "e.action | distinct | count >= 1".to_string()
    };
    let key = if use_in_threshold {
        "sip".to_string()
    } else {
        format!("L{}", depth - 1)
    };
    format!(
        r#"
rule r {{
    events {{ e : auth_events }}
{lets}
    match<{key}:5m> {{
        on event {{ {threshold}; }}
    }} -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}}
"#
    )
}

/// 5 层链（上限内）合法。
#[test]
fn let_chain_at_limit_accepted() {
    let input = chain_rule(5, true);
    assert_no_errors(&input, &[auth_events_window(), output_window()]);
}

/// 超过 5 层的链：编译期报错（而不是留给键展开/阈值内联去递归吃栈）。
#[test]
fn let_chain_beyond_limit_rejected() {
    let input = chain_rule(6, true);
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "reference chain through",
    );
}

/// 深链（200 层）用**作为 match key**：checker 必须先报错（此前键展开会按链递归，
/// 存在栈溢出 → 进程 abort 的风险），绝不能崩。
#[test]
fn deep_let_chain_key_fails_closed_without_crashing() {
    let input = chain_rule(200, false);
    let file = parse_wfl(&input).expect("parse should succeed");
    let errs = check_wfl(&file, &[auth_events_window(), output_window()]);
    assert!(
        errs.iter().any(|e| e.severity == Severity::Error
            && e.message.contains("reference chain through")),
        "深链必须编译期报错，实际: {errs:?}"
    );
    assert!(
        crate::compile_wfl(&file, &[auth_events_window(), output_window()]).is_err(),
        "compile_wfl 也必须失败（不得崩溃）"
    );
}

/// 链成环（a → b → a）：同样报错。
#[test]
fn cyclic_let_chain_rejected() {
    let input = r#"
rule r {
    events { e : auth_events }
    let A = B
    let B = A
    match<sip:5m> {
        on event { e.action | distinct | count >= A; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "reference chain",
    );
}
