use crate::parse_wfl;

// -----------------------------------------------------------------------
// Error cases
// -----------------------------------------------------------------------

#[test]
fn reject_missing_events() {
    let input = r#"
rule r {
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_missing_score() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } }
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_digit_leading_ident() {
    // rule name starting with digit
    let input = r#"
rule 1bad {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_empty_events() {
    let input = r#"
rule r {
    events { }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_empty_on_event() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_contract_decimal_hit_index() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect {
        hit[0.5].score == 50.0;
    }
}
"#;
    assert!(parse_wfl(input).is_err());
}

#[test]
fn reject_contract_decimal_hits_count() {
    let input = r#"
contract ct for r {
    given { row(e, x = 1); }
    expect {
        hits == 1.5;
    }
}
"#;
    assert!(parse_wfl(input).is_err());
}

/// 回归：超长算子链（`1 + 1 + ...`）曾经构造出与项数同阶的**左深 AST**——
/// 5000 项会让语义检查栈溢出 `abort`（CI 只报 `signal: 6`、无位置信息），
/// 20000 项在解析阶段就崩。现在必须在**解析阶段**快速失败并给出可读错误。
#[test]
fn reject_pathological_operator_chain() {
    for n in [64usize, 5_000, 20_000] {
        let chain = std::iter::repeat_n("1", n).collect::<Vec<_>>().join(" + ");
        let input = format!(
            r#"
rule r {{
    events {{ e : win }}
    match<:5m> {{ on event {{ e | count >= 1; }} }} -> score(50.0)
    entity(ip, e.sip)
    yield out (x = {chain})
}}
"#
        );
        let err = parse_wfl(&input).expect_err("超长算子链必须解析失败（不得崩溃）");
        assert!(
            format!("{err:?}").contains("operator levels"),
            "错误信息应说明链长上限（n={n}），实际: {err:?}"
        );
    }
}

/// 正向对照：预算内的链不受影响（现有语料实测最长 6 项）。
#[test]
fn accept_operator_chain_within_budget() {
    let chain = std::iter::repeat_n("1", 16).collect::<Vec<_>>().join(" + ");
    let input = format!(
        r#"
rule r {{
    events {{ e : win }}
    match<:5m> {{ on event {{ e | count >= 1; }} }} -> score(50.0)
    entity(ip, e.sip)
    yield out (x = {chain})
}}
"#
    );
    assert!(parse_wfl(&input).is_ok(), "预算内的算子链应当解析成功");
}

/// 预算允许的**最深**形状（5 层分组 × 每层预算内的链，路径深度 ≈ 95）必须能走完
/// 解析 / 语义检查 / 编译——预算不能宽松到又能打爆下游递归栈。
#[test]
fn deepest_allowed_chain_shape_survives_full_pipeline() {
    use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};
    use std::time::Duration;

    fn schema(name: &str, streams: &[&str]) -> WindowSchema {
        WindowSchema {
            name: name.into(),
            streams: streams.iter().map(|s| s.to_string()).collect(),
            time_field: Some("event_time".into()),
            over: Duration::from_secs(300),
            fields: vec![
                FieldDef {
                    name: "sip".into(),
                    field_type: FieldType::Base(BaseType::Ip),
                },
                FieldDef {
                    name: "n".into(),
                    field_type: FieldType::Base(BaseType::Digit),
                },
                FieldDef {
                    name: "event_time".into(),
                    field_type: FieldType::Base(BaseType::Time),
                },
            ],
        }
    }

    // 最深的合法形状：每层分组内一条用满预算的左深链，且**左深链正好落在路径上**
    // （所以真正叠加）：15 + 5 x 15 = 90 层，逼近 96 的上界。
    let tail = " + 1".repeat(15);
    let mut expr = std::iter::repeat_n("1", 16).collect::<Vec<_>>().join(" + ");
    for _ in 0..5 {
        expr = format!("({expr}){tail}");
    }
    let input = format!(
        r#"
rule r {{
    events {{ e : win }}
    match<:5m> {{ on event {{ e | count >= 1; }} }} -> score(50.0)
    entity(ip, e.sip)
    yield out (n = {expr})
}}
"#
    );
    let file = parse_wfl(&input).expect("最深的合法形状应当解析成功");
    let schemas = [schema("win", &["s"]), schema("out", &[])];
    let errs = crate::check_wfl(&file, &schemas);
    assert!(
        !errs.iter().any(|e| e.severity == crate::Severity::Error),
        "最深合法形状不应有语义错误: {errs:?}"
    );
    crate::compile_wfl(&file, &schemas).expect("最深合法形状应当编译成功");
}

#[test]
fn reject_unknown_object_array_item_type() {
    let input = r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (
        risk_context = object {
            tags: array/notatype = array ["ssh"];
        }
    )
}
"#;
    assert!(parse_wfl(input).is_err());
}
