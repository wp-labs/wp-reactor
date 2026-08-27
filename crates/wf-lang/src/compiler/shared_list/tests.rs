//! 公共允许列表（issue #73）解析 + 展开 + 错误面测试。
//!
//! 契约：`shared <name> = ("a", ...)` 一处定义；多规则 `expr in <name>` /
//! `expr not in <name>` 引用；编译期展开为字面 InList（与手写列表逐字节等价）。

use std::time::Duration;

use crate::ast::Expr;
use crate::compile_wfl;
use crate::parse_wfl;
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};

fn schemas() -> Vec<WindowSchema> {
    let mut out = vec![WindowSchema {
        name: "sdm_event".to_string(),
        streams: vec!["sdm".to_string()],
        time_field: Some("ts".to_string()),
        over: Duration::from_secs(3600),
        fields: vec![
            FieldDef {
                name: "log_type".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "sip".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            },
            FieldDef {
                name: "ts".to_string(),
                field_type: FieldType::Base(BaseType::Time),
            },
        ],
    }];
    // yield 目标窗口（checker 要求 yield target 是已声明窗口且参数为窗口字段）。
    for name in ["alerts", "entities", "evidence", "out"] {
        out.push(WindowSchema {
            name: name.to_string(),
            streams: Vec::new(),
            time_field: None,
            over: Duration::from_secs(3600),
            fields: vec![FieldDef {
                name: "x".to_string(),
                field_type: FieldType::Base(BaseType::Chars),
            }],
        });
    }
    out
}

/// 三条规则（告警/实体/证据）共用同一允许列表——issue 原场景。
fn three_rules() -> String {
    r#"
shared security_log_types = (
    "edr_alert_log",
    "fw_ips_protect_log",
    "topas_waf_virus"
)

rule alert_rule {
    events { s : sdm_event && s.log_type in security_log_types }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield alerts (x = s.sip)
}

rule alert_entity_rule {
    events { s : sdm_event && s.log_type in security_log_types }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield entities (x = s.sip)
}

rule event_evidence_rule {
    events { s : sdm_event && s.log_type in security_log_types }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield evidence (x = s.sip)
}
"#
    .to_string()
}

// ---- 解析 ----

#[test]
fn parse_shared_decl_and_in_ref() {
    let file = parse_wfl(&three_rules()).unwrap();
    assert_eq!(file.shared_lists.len(), 1, "shared 声明解析");
    assert_eq!(file.shared_lists[0].name, "security_log_types");
    assert_eq!(file.shared_lists[0].items.len(), 3);

    // 每条规则的 events 过滤 = InList 且列表为单元素 ListRef。
    for rule in &file.rules {
        let filter = rule.events.decls[0].filter.as_ref().expect("bind filter");
        match filter {
            Expr::InList {
                list,
                negated: false,
                ..
            } => {
                assert_eq!(list.len(), 1);
                assert!(
                    matches!(&list[0], Expr::ListRef(n) if n == "security_log_types"),
                    "`in <name>` 应产出 ListRef, got {list:?}"
                );
            }
            other => panic!("expected InList, got {other:?}"),
        }
    }
}

#[test]
fn parse_not_in_ref() {
    let file = parse_wfl(
        r#"
shared blocked = ("a", "b")

rule r {
    events { s : sdm_event && s.log_type not in blocked }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    match file.rules[0].events.decls[0].filter.as_ref().unwrap() {
        Expr::InList {
            list,
            negated: true,
            ..
        } => assert!(matches!(&list[0], Expr::ListRef(n) if n == "blocked")),
        other => panic!("expected negated InList(ListRef), got {other:?}"),
    }
}

#[test]
fn parse_shared_decl_requires_precede_rules() {
    // 与 yield preset/pattern 一致：顶层声明在 rules 之前（winnow 文法顺序）。
    let err = parse_wfl(
        r#"
rule r {
    events { s : sdm_event && s.log_type in security_log_types }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}

shared security_log_types = ("a")
"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("parse"),
        "shared 声明须在规则前: {err}"
    );
}

// ---- 编译展开 ----

#[test]
fn compile_expands_shared_list_in_all_rules() {
    let file = parse_wfl(&three_rules()).unwrap();
    let plans = compile_wfl(&file, &schemas()).unwrap();
    assert_eq!(plans.len(), 3, "三规则全部编译");

    for plan in &plans {
        let filter = plan.binds[0].filter.as_ref().expect("bind filter");
        match filter {
            Expr::InList {
                list,
                negated: false,
                ..
            } => {
                assert_eq!(list.len(), 3, "ListRef 应展开为 shared 全部元素");
                assert!(
                    list.iter().all(|i| matches!(i, Expr::StringLit(_))),
                    "展开后应为字面字符串列表, got {list:?}"
                );
                assert_eq!(list[0], Expr::StringLit("edr_alert_log".to_string()));
                assert_eq!(list[2], Expr::StringLit("topas_waf_virus".to_string()));
            }
            other => panic!("expected literal InList after expansion, got {other:?}"),
        }
    }
}

/// 展开结果与手写字面列表逐字节等价（不引入额外语义）。
#[test]
fn expansion_equals_literal_list() {
    let shared = parse_wfl(&three_rules()).unwrap();
    let literal = parse_wfl(
        r#"
rule r {
    events { s : sdm_event && s.log_type in ("edr_alert_log", "fw_ips_protect_log", "topas_waf_virus") }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    let shared_plan = &compile_wfl(&shared, &schemas()).unwrap()[0];
    let literal_plan = &compile_wfl(&literal, &schemas()).unwrap()[0];
    assert_eq!(
        shared_plan.binds[0].filter, literal_plan.binds[0].filter,
        "shared 展开应与手写字面列表等价"
    );
}

/// `not in <name>` 展开保留否定。
#[test]
fn not_in_expansion_keeps_negation() {
    let file = parse_wfl(
        r#"
shared blocked = ("a", "b")

rule r {
    events { s : sdm_event && s.log_type not in blocked }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    let plan = &compile_wfl(&file, &schemas()).unwrap()[0];
    match &plan.binds[0].filter {
        Some(Expr::InList {
            list,
            negated: true,
            ..
        }) => {
            assert_eq!(list.len(), 2);
            assert!(list.iter().all(|i| matches!(i, Expr::StringLit(_))));
        }
        other => panic!("expected negated literal InList, got {other:?}"),
    }
}

// ---- 错误面 ----

#[test]
fn unknown_shared_list_errors_with_name() {
    let file = parse_wfl(
        r#"
rule r {
    events { s : sdm_event && s.log_type in no_such_list }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    let err = compile_wfl(&file, &schemas()).unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("no_such_list"),
        "未知名应在错误中可定位: {text}"
    );
    assert!(text.contains("rule `r`"), "错误应带规则名: {text}");
}

#[test]
fn duplicate_shared_list_errors() {
    let file = parse_wfl(
        r#"
shared x = ("a")
shared x = ("b")
"#,
    )
    .unwrap();
    let err = compile_wfl(&file, &schemas()).unwrap_err();
    assert!(err.to_string().contains("more than once"), "{err}");
}

/// 展开后元素类型检查与手写列表一致：数字元素混入字符串列表在两类写法下
/// 表现相同（同一 checker 路径, 无额外限制也无放宽）。
#[test]
fn mixed_type_items_behave_like_literal_list() {
    let shared = parse_wfl(
        r#"
shared s = ("a", 1)

rule r {
    events { s : sdm_event && s.log_type in s }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    let literal = parse_wfl(
        r#"
rule r {
    events { s : sdm_event && s.log_type in ("a", 1) }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    let shared_ok = compile_wfl(&shared, &schemas()).is_ok();
    let literal_ok = compile_wfl(&literal, &schemas()).is_ok();
    assert_eq!(
        shared_ok, literal_ok,
        "shared 展开与手写列表的检查结果必须一致"
    );
}
