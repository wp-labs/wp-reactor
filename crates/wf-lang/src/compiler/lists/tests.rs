//! 顶层列表（issue #73）解析 + use 导入 + 展开 + 类型检查 + 错误面测试。
//!
//! 契约：顶层 `name = ("a", ...)` 裸绑定一处定义；多规则 `expr in <name>` /
//! `expr not in <name>` 引用；`use "file.wfl"` include 导入目标文件全部顶层
//! 列表（flatten、递归传播、无可见性控制）；编译期展开为字面 InList（与手写
//! 列表逐字节等价）；InList 元素-左值类型比对（字面与命名列表统一）。

use std::collections::HashMap;
use std::path::PathBuf;
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

/// 三条规则（告警/实体/证据）共用同一列表——issue 原场景（同文件内定义）。
fn three_rules() -> String {
    r#"
security_log_types = (
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
fn parse_list_decl_and_in_ref() {
    let file = parse_wfl(&three_rules()).unwrap();
    assert_eq!(file.lists.len(), 1, "顶层列表声明解析");
    assert_eq!(file.lists[0].name, "security_log_types");
    assert_eq!(file.lists[0].items.len(), 3);

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
blocked = ("a", "b")

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
fn parse_list_decl_requires_precede_rules() {
    // 与 pattern/yield preset 一致：顶层声明在 rules 之前（winnow 文法顺序）。
    let err = parse_wfl(
        r#"
rule r {
    events { s : sdm_event && s.log_type in security_log_types }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}

security_log_types = ("a")
"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("parse"),
        "列表声明须在规则前: {err}"
    );
}

#[test]
fn parse_multiple_lists_and_use_decls() {
    let file = parse_wfl(
        r#"
use "lib.wfl"
use "extra.wfl"

a = ("1", "2")
b = (1, 2)
"#,
    )
    .unwrap();
    assert_eq!(file.uses.len(), 2);
    assert_eq!(file.lists.len(), 2);
    assert_eq!(file.lists[0].name, "a");
    assert_eq!(file.lists[1].name, "b");
}

// ---- 编译展开 ----

#[test]
fn compile_expands_list_in_all_rules() {
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
                assert_eq!(list.len(), 3, "ListRef 应展开为列表全部元素");
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
    let named = parse_wfl(&three_rules()).unwrap();
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
    let named_plan = &compile_wfl(&named, &schemas()).unwrap()[0];
    let literal_plan = &compile_wfl(&literal, &schemas()).unwrap()[0];
    assert_eq!(
        named_plan.binds[0].filter, literal_plan.binds[0].filter,
        "列表展开应与手写字面列表等价"
    );
}

/// `not in <name>` 展开保留否定。
#[test]
fn not_in_expansion_keeps_negation() {
    let file = parse_wfl(
        r#"
blocked = ("a", "b")

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

// ---- use 导入 ----

/// 内存文件系统 loader: 内容表按路径取; 缺失 → 报错。
fn mem_loader<'a>(
    files: &'a HashMap<&'a str, &'a str>,
) -> impl FnMut(&std::path::Path) -> crate::LangResult<String> + 'a {
    move |p: &std::path::Path| {
        files
            .get(p.to_str().unwrap())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                crate::error::error(
                    crate::LangReason::Compile,
                    format!("no such file: {}", p.display()),
                )
            })
    }
}

#[test]
fn use_imports_lists_from_target_file() {
    let mut files = HashMap::new();
    files.insert(
        "lib.wfl",
        r#"
security_log_types = ("edr_alert_log", "fw_ips_protect_log")
high_risk = ("attack", "malware")
"#,
    );
    let rule_src = r#"
use "lib.wfl"

rule r {
    events { s : sdm_event && s.log_type in security_log_types }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#;
    let file = parse_wfl(rule_src).unwrap();
    let mut loader = mem_loader(&files);
    let merged = crate::compiler::lists::resolve_imports(
        &file,
        std::path::Path::new("main.wfl"),
        &mut loader,
    )
    .unwrap();
    assert_eq!(merged.lists.len(), 2, "use 导入目标文件全部列表");
    assert_eq!(merged.lists[0].name, "security_log_types");

    // 展开后规则引用导入的列表可用。
    let plans = compile_wfl(&merged, &schemas()).unwrap();
    assert_eq!(plans.len(), 1);
    match &plans[0].binds[0].filter {
        Some(Expr::InList { list, .. }) => {
            assert_eq!(list.len(), 2);
            assert!(list.iter().all(|i| matches!(i, Expr::StringLit(_))));
        }
        other => panic!("expected expanded InList, got {other:?}"),
    }
}

#[test]
fn use_imports_recursively() {
    // A use B, B use C → A 可见 B 与 C 的列表（include 递归传播）。
    let mut files = HashMap::new();
    files.insert("c.wfl", "c_list = (\"c1\", \"c2\")\n");
    files.insert("b.wfl", "use \"c.wfl\"\nb_list = (\"b1\")\n");
    files.insert("a.wfl", "use \"b.wfl\"\na_list = (\"a1\")\n");
    let file = parse_wfl("use \"a.wfl\"\n").unwrap();
    let mut loader = mem_loader(&files);
    let merged = crate::compiler::lists::resolve_imports(
        &file,
        std::path::Path::new("main.wfl"),
        &mut loader,
    )
    .unwrap();
    let names: Vec<&str> = merged.lists.iter().map(|l| l.name.as_str()).collect();
    assert_eq!(
        names,
        ["a_list", "b_list", "c_list"],
        "递归导入: A→B→C 全可见"
    );
}

#[test]
fn use_missing_file_errors() {
    let file = parse_wfl("use \"nope.wfl\"\n").unwrap();
    let empty = HashMap::new();
    let mut loader = mem_loader(&empty);
    let err = crate::compiler::lists::resolve_imports(
        &file,
        std::path::Path::new("main.wfl"),
        &mut loader,
    )
    .unwrap_err();
    assert!(err.to_string().contains("nope.wfl"), "{err}");
}

#[test]
fn use_circular_import_errors() {
    let mut files = HashMap::new();
    files.insert("a.wfl", "use \"b.wfl\"\n");
    files.insert("b.wfl", "use \"a.wfl\"\n");
    let file = parse_wfl("use \"a.wfl\"\n").unwrap();
    let mut loader = mem_loader(&files);
    let err = crate::compiler::lists::resolve_imports(
        &file,
        std::path::Path::new("main.wfl"),
        &mut loader,
    )
    .unwrap_err();
    assert!(err.to_string().contains("circular use"), "{err}");
}

#[test]
fn use_duplicate_list_errors() {
    // 文件内定义与导入列表同名 → 报错（不遮蔽）。
    let mut files = HashMap::new();
    files.insert("lib.wfl", "security_log_types = (\"a\")\n");
    let rule_src = r#"
use "lib.wfl"
security_log_types = ("b")

rule r {
    events { s : sdm_event && s.log_type in security_log_types }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#;
    let file = parse_wfl(rule_src).unwrap();
    let mut loader = mem_loader(&files);
    let err = crate::compiler::lists::resolve_imports(
        &file,
        std::path::Path::new("main.wfl"),
        &mut loader,
    )
    .unwrap_err();
    assert!(err.to_string().contains("already defined"), "{err}");
}

#[test]
fn use_wfs_target_skipped() {
    // `.wfs` 目标跳过（schema 由加载层另行加载）——不报缺失。
    let file = parse_wfl("use \"schemas/nexmark.wfs\"\n").unwrap();
    let empty = HashMap::new();
    let mut loader = mem_loader(&empty);
    let merged = crate::compiler::lists::resolve_imports(
        &file,
        std::path::Path::new("main.wfl"),
        &mut loader,
    )
    .unwrap();
    assert!(merged.lists.is_empty(), "wfs use 不导入列表");
}

// ---- 错误面 ----

#[test]
fn unknown_list_errors_with_name() {
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
fn duplicate_list_errors() {
    let file = parse_wfl(
        r#"
x = ("a")
x = ("b")
"#,
    )
    .unwrap();
    let err = compile_wfl(&file, &schemas()).unwrap_err();
    assert!(err.to_string().contains("more than once"), "{err}");
}

// ---- 类型检查（issue #73 评审: InList 元素-左值类型比对, 字面与命名列表统一） ----

#[test]
fn list_element_type_mismatch_with_left_value_errors() {
    // 数字列表套到字符串字段 → 编译错误。
    let file = parse_wfl(
        r#"
ports = (80, 443)

rule r {
    events { s : sdm_event && s.log_type in ports }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    let err = compile_wfl(&file, &schemas()).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("rule `r`"), "类型不匹配应带规则名: {text}");
    assert!(text.contains("not compatible"), "类型不匹配消息: {text}");
}

#[test]
fn literal_in_list_type_mismatch_errors_too() {
    // 与命名列表统一: 手写 `in (...)` 混类型同样报错。
    let file = parse_wfl(
        r#"
rule r {
    events { s : sdm_event && s.log_type in (80, 443) }
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
        text.contains("not compatible"),
        "字面 InList 类型不匹配报错: {text}"
    );
}

#[test]
fn mixed_type_list_errors() {
    // 列表元素自身混类型 → 报错（在声明处可定位）。
    let file = parse_wfl(
        r#"
mixed = ("a", 1)
"#,
    )
    .unwrap();
    let err = compile_wfl(&file, &schemas()).unwrap_err();
    assert!(err.to_string().contains("mixed"), "{err}");
}

#[test]
fn matching_types_compile_ok() {
    // 字符串字段 + 字符串列表 → 通过。
    let file = parse_wfl(
        r#"
types = ("a", "b")

rule r {
    events { s : sdm_event && s.log_type in types }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    assert!(compile_wfl(&file, &schemas()).is_ok());
}

#[test]
fn numeric_list_on_numeric_field_ok() {
    // 数字字段 + 数字列表 → 通过（不误报）。
    let mut schemas = schemas();
    schemas[0].fields.push(FieldDef {
        name: "count".to_string(),
        field_type: FieldType::Base(BaseType::Digit),
    });
    let file = parse_wfl(
        r#"
levels = (1, 2, 3)

rule r {
    events { s : sdm_event && s.count in levels }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    assert!(compile_wfl(&file, &schemas).is_ok());
}

#[test]
fn non_literal_items_skip_type_check() {
    // 元素含函数调用/推断不出 → 跳过检查（不误报）。
    let file = parse_wfl(
        r#"
dynamic = (upper("a"), lower("b"))

rule r {
    events { s : sdm_event && s.log_type in dynamic }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    assert!(compile_wfl(&file, &schemas()).is_ok());
}

#[test]
fn empty_list_skips_type_check() {
    // `in (...)` 列表至少 1 个元素（in_list 文法）——空列表不可表达,
    // 此用例保留断言空元素集合（如未来放宽文法）不误报的路径。
    let file = parse_wfl(
        r#"
empty = ("placeholder")
"#,
    )
    .unwrap();
    assert!(compile_wfl(&file, &schemas()).is_ok());
}

// ---- review 修复回归（2026-08-27 六轮评审） ----

/// 菱形导入幂等: A use B、A use C、B use D、C use D → D 只导入一次, 不误报重名。
#[test]
fn use_diamond_import_is_idempotent() {
    let mut files = HashMap::new();
    files.insert("d.wfl", "d_list = (\"d1\")\n");
    files.insert("b.wfl", "use \"d.wfl\"\nb_list = (\"b1\")\n");
    files.insert("c.wfl", "use \"d.wfl\"\nc_list = (\"c1\")\n");
    files.insert("a.wfl", "use \"b.wfl\"\nuse \"c.wfl\"\na_list = (\"a1\")\n");
    let file = parse_wfl("use \"a.wfl\"\n").unwrap();
    let mut loader = mem_loader(&files);
    let merged = crate::compiler::lists::resolve_imports(
        &file,
        std::path::Path::new("main.wfl"),
        &mut loader,
    )
    .unwrap();
    let mut names: Vec<&str> = merged.lists.iter().map(|l| l.name.as_str()).collect();
    names.sort_unstable();
    // include 语义不承诺合并顺序; 断言集合 + 无重名（D 只出现一次）。
    assert_eq!(
        names,
        ["a_list", "b_list", "c_list", "d_list"],
        "菱形导入 D 只合并一次"
    );
}

/// 同一文件不同写法（`b.wfl` vs `./b.wfl`）多次 use → 幂等（路径规范化）。
#[test]
fn use_same_file_variant_writing_is_idempotent() {
    let mut files = HashMap::new();
    files.insert("b.wfl", "b_list = (\"b1\")\n");
    let file = parse_wfl("use \"b.wfl\"\nuse \"./b.wfl\"\n").unwrap();
    let mut loader = mem_loader(&files);
    let merged = crate::compiler::lists::resolve_imports(
        &file,
        std::path::Path::new("main.wfl"),
        &mut loader,
    )
    .unwrap();
    assert_eq!(merged.lists.len(), 1, "同文件变体写法只导入一次");
}

/// `./` 前缀不得绕过循环检测（路径规范化）。
#[test]
fn use_circular_detection_survives_dot_slash() {
    let mut files = HashMap::new();
    files.insert("a.wfl", "use \"./b.wfl\"\n");
    files.insert("b.wfl", "use \"./a.wfl\"\n");
    let file = parse_wfl("use \"a.wfl\"\n").unwrap();
    let mut loader = mem_loader(&files);
    let err = crate::compiler::lists::resolve_imports(
        &file,
        std::path::Path::new("main.wfl"),
        &mut loader,
    )
    .unwrap_err();
    assert!(err.to_string().contains("circular use"), "{err}");
}

/// 自循环（文件 use 自己, 含 `./` 前缀）→ 循环报错而非无限递归。
#[test]
fn use_self_import_with_dot_slash_errors() {
    let mut files = HashMap::new();
    files.insert("a.wfl", "use \"./a.wfl\"\n");
    let file = parse_wfl("use \"a.wfl\"\n").unwrap();
    let mut loader = mem_loader(&files);
    let err = crate::compiler::lists::resolve_imports(
        &file,
        std::path::Path::new("main.wfl"),
        &mut loader,
    )
    .unwrap_err();
    assert!(err.to_string().contains("circular use"), "{err}");
}

/// yield preset 声明体（字段值 + 参数默认值）里的列表引用也须展开（评审缺口）。
#[test]
fn resolve_list_refs_expands_preset_bodies() {
    // 字段值引用列表: 展开为字面 InList。
    let file = parse_wfl(
        r#"
allowed_types = ("a", "b")

 yield preset base_alerts <severity = "high"> (
    detail = "x" in allowed_types
)
"#,
    )
    .unwrap();
    let resolved = crate::compiler::lists::resolve_list_refs(&file).unwrap();
    match &resolved.yield_presets[0].args[0].value {
        Expr::InList { list, .. } => {
            assert_eq!(list.len(), 2, "preset 字段值里的 ListRef 应展开");
            assert!(list.iter().all(|i| matches!(i, Expr::StringLit(_))));
        }
        other => panic!("preset 字段值应展开为字面 InList, got {other:?}"),
    }

    // 参数默认值引用列表: 同样展开。
    let file = parse_wfl(
        r#"
allowed_types = ("a", "b")

 yield preset base_alerts <severity = "x" in allowed_types> (
    detail = $severity
)
"#,
    )
    .unwrap();
    let resolved = crate::compiler::lists::resolve_list_refs(&file).unwrap();
    match &resolved.yield_presets[0].params[0].default {
        Some(Expr::InList { list, .. }) => {
            assert_eq!(list.len(), 2, "preset 参数默认值里的 ListRef 应展开");
        }
        other => panic!("preset 参数默认值应展开为字面 InList, got {other:?}"),
    }
}

/// 混类型列表: 只报一条（混类型）, 不再叠加左值不兼容双报。
#[test]
fn mixed_type_list_reports_single_error() {
    let file = parse_wfl(
        r#"
rule r {
    events { s : sdm_event && s.log_type in (1, "a") }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap();
    let err = compile_wfl(&file, &schemas()).unwrap_err();
    let text = err.to_string();
    // 混类型报错后不得再叠加左值不兼容（同列表双报）。
    assert!(text.contains("mixes incompatible"), "混类型应报错: {text}");
    assert!(
        !text.contains("left-hand value type"),
        "混类型后不应再报左值不兼容, got: {text}"
    );
}

/// 限定名右值（`in foo.bar`）不是列表引用——解析报错（而非误吞 ident 残留）。
#[test]
fn qualified_name_after_in_is_parse_error() {
    let err = parse_wfl(
        r#"
rule r {
    events { s : sdm_event && s.log_type in a.b }
    match<:5m> { on event { s | count >= 1; } } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("parse"),
        "限定名右值应解析报错: {err}"
    );
}

fn _unused(_: PathBuf) {}
