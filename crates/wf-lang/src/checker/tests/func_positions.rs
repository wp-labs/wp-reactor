//! 位置相关的函数可用性闸门（warp-fusion#101 及同类位置）。
//!
//! 三类依赖动态上下文的内建函数写进缺乏该上下文的位置时，编译期曾放行、运行期恒求值
//! 为空 → 规则静默不触发 / 事件被静默过滤 / 输出失真：
//!
//! - **instance 收集序列**（`first` / `last` / `collect_set` / `collect_list` /
//!   `stddev` / `percentile`）：`events` bind filter、`join ... within` 表达式界、
//!   `emit at`、`on each` 规则的规则级 `let` / post-join `where`；
//! - **窗口查找**（`window.has(...)`）：`within` 界、`emit at`、`on each` 上下文
//!   （bind filter 带窗口表，仍支持）；
//! - **滚动基线状态**（`baseline(...)`）：上述所有位置——该状态只在 event guard 上
//!   跨事件累积（`baseline_dev` 走全局 store，不受限）。
//!
//! 另外两处同样逐行求值、三项能力皆无的位置：stats 度量的 `where`（引擎
//! `stats_exec` 逐行）与 `conv` 链的 `sort` / `dedup` / `where`（wf-cep 在收口批上
//! 按 output 逐行）。
//!
//! 对照：match/close 规则的规则级 `let` 与 post-join `where` 在 instance 上下文求值，
//! L3 可用，必须继续放行。

use super::*;

fn threat_list_window() -> WindowSchema {
    make_window(
        "threat_list",
        vec!["threat_stream"],
        vec![("sip", bt(BaseType::Ip))],
    )
}

fn join_schemas() -> Vec<WindowSchema> {
    vec![
        auth_events_window(),
        fw_events_window(),
        threat_list_window(),
        output_window(),
    ]
}

// ---------------------------------------------------------------------------
// 拒绝：wf-cep 逐事件 / 逐行求值的位置
// ---------------------------------------------------------------------------

#[test]
fn l3_rejected_in_events_bind_filter() {
    let input = r#"
rule r {
    events { e : auth_events && first(e.count) > 0 }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in `events` bind filter",
    );
}

#[test]
fn l3_rejected_in_events_bind_filter_when_nested() {
    // 嵌套在其它函数实参里同样拒绝（mvcount(collect_set(...))）。
    let input = r#"
rule r {
    events { e : auth_events && mvcount(collect_set(e.action)) > 0 }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in `events` bind filter",
    );
}

#[test]
fn non_l3_function_still_allowed_in_bind_filter() {
    let input = r#"
rule r {
    events { e : auth_events && len(e.action) > 0 }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn l3_rejected_in_join_within_bound() {
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    join fw_events within [first(a.event_time), first(a.event_time)] on a.dip == fw_events.sip
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &join_schemas(),
        "not allowed in join `within` bound expressions",
    );
}

#[test]
fn l3_rejected_in_emit_at() {
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    join fw_events within [a.event_time, a.event_time] on a.dip == fw_events.sip
        emit at first(a.event_time)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &join_schemas(),
        "not allowed in `emit at` expressions",
    );
}

// ---------------------------------------------------------------------------
// 拒绝：窗口查找（`window.has`）在无窗口表的位置
// ---------------------------------------------------------------------------

#[test]
fn window_lookup_rejected_in_join_within_bound() {
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    join fw_events within [threat_list.has(a.sip), threat_list.has(a.sip)] on a.dip == fw_events.sip
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(input, &join_schemas(), "window lookups are not available");
}

#[test]
fn window_lookup_rejected_in_emit_at() {
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    join fw_events within [a.event_time, a.event_time] on a.dip == fw_events.sip
        emit at threat_list.has(a.sip)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(input, &join_schemas(), "window lookups are not available");
}

#[test]
fn unqualified_has_rejected_in_bind_filter() {
    // `has(...)` 是窗口方法调用；无限定 `has` 在任何求值器里都没实现（恒 None）。
    let input = r#"
rule r {
    events { e : auth_events && has(e.sip) }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(input, &join_schemas(), "has() requires a window qualifier");
}

#[test]
fn unqualified_has_rejected_in_guard() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e && has(e.sip) | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(input, &join_schemas(), "has() requires a window qualifier");
}

#[test]
fn qualified_has_allowed_in_guard() {
    // guard 带窗口表求值（与 bind filter 同）；限定形态正常。
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e && threat_list.has(e.sip) | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &join_schemas());
}

#[test]
fn window_lookup_allowed_in_bind_filter() {
    // bind filter 带窗口表求值（`events { e : W && lookup.has(e.field) }` 是受支持写法）。
    let input = r#"
rule r {
    events { e : auth_events && threat_list.has(e.sip) }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &join_schemas());
}

// ---------------------------------------------------------------------------
// 拒绝：滚动基线状态（`baseline`）在所有逐事件/逐行位置
// ---------------------------------------------------------------------------

#[test]
fn baseline_rejected_in_bind_filter() {
    let input = r#"
rule r {
    events { e : auth_events && baseline(e.count, 300) > 3 }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "rolling baseline state only accumulates",
    );
}

#[test]
fn baseline_rejected_in_join_within_bound() {
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    join fw_events within [baseline(a.count, 300), baseline(a.count, 300)] on a.dip == fw_events.sip
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &join_schemas(),
        "rolling baseline state only accumulates",
    );
}

#[test]
fn baseline_rejected_in_emit_at() {
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    join fw_events within [a.event_time, a.event_time] on a.dip == fw_events.sip
        emit at baseline(a.count, 300)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &join_schemas(),
        "rolling baseline state only accumulates",
    );
}

#[test]
fn baseline_rejected_in_on_each_rule_let() {
    let input = r#"
rule r {
    events { e : auth_events }
    let b = baseline(e.count, 300)
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip, n = b)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "rolling baseline state only accumulates",
    );
}

#[test]
fn baseline_dev_allowed_in_on_each_where() {
    // baseline_dev 走全局 store（不依赖调用方的滚动状态表），on each where 受支持。
    let input = r#"
rule r {
    events { a : auth_events }
    on each a where baseline_dev(a.sip, a.user, a.count) > 3.0 -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_no_errors(input, &join_schemas());
}

/// `on each` 的 where filter（引擎 `each_exec` 逐事件 `eval_bool_expr`：无窗口表 /
/// 实例序列 / 滚动状态）。
fn on_each_where_rule(filter: &str) -> String {
    format!(
        r#"
rule r {{
    events {{ a : auth_events }}
    on each a where {filter} -> score(1.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}}
"#
    )
}

#[test]
fn window_lookup_rejected_in_on_each_where() {
    // 修复前：限定 `has` 不在 `is_disallowed_on_each_func` 名单里，filter 也没有位置
    // 闸门 → 编译通过、运行期 `None` → 每个事件被静默滤掉（实测：2 条输入 → 0 条输出）。
    let input = on_each_where_rule("threat_list.has(a.sip)");
    assert_has_error(
        &input,
        &join_schemas(),
        "not allowed in `on each` expressions",
    );
}

#[test]
fn l3_rejected_in_on_each_where() {
    // L3 现由位置闸门（OnEach）拒绝，不再靠 on-each 名单的第二份拷贝。
    let input = on_each_where_rule("first(a.count) > 0");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "not allowed in `on each` expressions",
    );
}

#[test]
fn baseline_rejected_in_on_each_where() {
    let input = on_each_where_rule("baseline(a.count, 300) > 1");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "not allowed in `on each` expressions",
    );
}

#[test]
fn l3_rejected_in_on_each_yield() {
    // on-each 名单里的 L3/`baseline` 委托给闸门后（单一事实源），`on each` 的
    // score / entity / yield 仍必须拒绝——这三处靠 `rule_expr_position` → OnEach。
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    entity(ip, a.sip)
    yield out (n = first(a.count))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in `on each` expressions",
    );
}

#[test]
fn on_each_where_allows_plain_predicates() {
    // 放行对照：不依赖三类运行期上下文的表达式照旧可用。
    let input = on_each_where_rule("startswith(a.action, \"x\") && len(a.user) > 0");
    assert_no_errors(&input, &[auth_events_window(), output_window()]);
}

// ---------------------------------------------------------------------------
// 拒绝：`on each` 规则的逐事件求值位置
// ---------------------------------------------------------------------------

#[test]
fn l3_rejected_in_on_each_rule_let() {
    let input = r#"
rule r {
    events { e : auth_events }
    let m = first(e.count)
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip, n = m)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in `on each` expressions",
    );
}

#[test]
fn window_lookup_rejected_in_on_each_rule_let() {
    let input = r#"
rule r {
    events { a : auth_events }
    let h = threat_list.has(a.sip)
    on each a -> score(1.0)
    entity(ip, a.sip)
    yield out (y = h)
}
"#;
    assert_has_error(
        input,
        &join_schemas(),
        "not allowed in `on each` expressions",
    );
}

#[test]
fn baseline_rejected_in_on_each_post_join_where() {
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    join fw_events within [a.event_time, a.event_time] on a.dip == fw_events.sip
        emit at a.event_time
    where baseline(a.count, 300) > 3
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &join_schemas(),
        "rolling baseline state only accumulates",
    );
}

#[test]
fn unqualified_has_rejected_in_on_each_where() {
    let input = r#"
rule r {
    events { e : auth_events }
    on each e where has(e.sip) -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(input, &join_schemas(), "has() requires a window qualifier");
}

#[test]
fn l3_rejected_in_on_each_post_join_where() {
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    join fw_events within [1s, 5s] on a.dip == fw_events.sip
    where first(a.count) > 0
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &join_schemas(),
        "not allowed in `on each` expressions",
    );
}

#[test]
fn l3_rejected_in_deferred_rule_let() {
    // deferred 规则（emit at）已被限定为 on-each 形态：let 在挂起时的左行上求值。
    let input = r#"
rule r {
    events { a : auth_events }
    let m = first(a.count)
    on each a -> score(1.0)
    join fw_events within [a.event_time, a.event_time] on a.dip == fw_events.sip
        emit at a.event_time
    entity(ip, a.sip)
    yield out (x = a.sip, n = m)
}
"#;
    assert_has_error(
        input,
        &join_schemas(),
        "not allowed in `on each` expressions",
    );
}

// ---------------------------------------------------------------------------
// 拒绝：遍历分支 / 包装符里的调用（闸门递归覆盖）
// ---------------------------------------------------------------------------

#[test]
fn l3_rejected_when_nested_in_in_list() {
    let input = r#"
rule r {
    events { e : auth_events && first(e.count) in (1, 2) }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in `events` bind filter",
    );
}

#[test]
fn l3_rejected_when_nested_under_not_and_neg() {
    let input = r#"
rule r {
    events { e : auth_events && not (first(e.count) > 0) }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in `events` bind filter",
    );
}

#[test]
fn reports_only_first_unavailable_call() {
    // 一条表达式里多处命中只报一次（避免刷屏），且错误归属规则。
    let input = r#"
rule r {
    events { e : auth_events && mvcount(collect_set(e.action)) > first(e.count) }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let errs = check_errors(input, &[auth_events_window(), output_window()]);
    let gate: Vec<&String> = errs
        .iter()
        .filter(|m| m.contains("not allowed in `events` bind filter"))
        .collect();
    assert_eq!(gate.len(), 1, "应恰好一条位置错误，实际: {errs:?}");
}

#[test]
fn gate_error_is_also_rejected_by_compile_wfl() {
    // 用户实际加载规则的路径（compile_wfl）同样拒绝，而非只在 check_wfl 层面。
    let input = r#"
rule r {
    events { e : auth_events && first(e.count) > 0 }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).expect("parse should succeed");
    assert!(
        crate::compile_wfl(&file, &[auth_events_window(), output_window()]).is_err(),
        "compile_wfl 也必须拒绝该规则"
    );
}

// ---------------------------------------------------------------------------
// 拒绝：stats 度量的 `where`（引擎逐行求值）
// ---------------------------------------------------------------------------

fn stats_rule(measure: &str) -> String {
    format!(
        r#"
rule r {{
    events {{ a : auth_events }}
    stats<10s:fixed> {{
        {measure}
    }}
    entity(digit, 1)
    yield out (y = fmt("{{}}", stat.value(final(total))))
}}
"#
    )
}

#[test]
fn l3_rejected_in_stats_measure_where() {
    let input = stats_rule("a | count as total where first(a.count) > 0;");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "not allowed in stats measure `where` expressions",
    );
}

#[test]
fn window_lookup_rejected_in_stats_measure_where() {
    let input = stats_rule("a | count as total where threat_list.has(a.sip);");
    assert_has_error(&input, &join_schemas(), "window lookups are not available");
}

#[test]
fn baseline_rejected_in_stats_measure_where() {
    let input = stats_rule("a | count as total where baseline(a.count, 300) > 3;");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "rolling baseline state only accumulates",
    );
}

#[test]
fn non_l3_function_allowed_in_stats_measure_where() {
    let input = stats_rule("a | count as total where startswith(a.action, \"x\");");
    assert_no_errors(&input, &[auth_events_window(), output_window()]);
}

// ---------------------------------------------------------------------------
// stats 桶键（`group by`）：引擎只实现 Field / bucket / tier
// ---------------------------------------------------------------------------

fn stats_rule_with_keys(keys: &str, measure: &str) -> String {
    format!(
        r#"
rule r {{
    events {{ a : auth_events }}
    stats<10s:fixed> group by ({keys}) {{
        {measure}
    }}
    entity(digit, 1)
    yield out (y = fmt("{{}}", stat.value(final(total))))
}}
"#
    )
}

#[test]
fn stats_bucket_key_must_be_field_or_bucket_tier() {
    // 引擎桶键求值只实现 `Field` / `bucket` / `tier`（`stats_exec/eval/rowkey.rs`），
    // 其它表达式恒返回 `None` → `exec.rs` 直接 `continue`：整行被跳过、桶恒空、
    // 无任何告警（warp-fusion#101 同类）。字段算术连函数都没有，只能靠本白名单。
    for keys in ["first(a.count)", "a.count + 1", "threat_list.has(a.sip)"] {
        let input = stats_rule_with_keys(keys, "a | count as total;");
        assert_has_error(&input, &join_schemas(), "stats bucket key");
    }
}

#[test]
fn stats_bucket_key_allows_field_bucket_tier() {
    let input = stats_rule_with_keys(
        "a.sip, bucket(a.event_time, \"day\"), tier(a.count, 10, 20)",
        "a | count as total;",
    );
    assert_no_errors(&input, &[auth_events_window(), output_window()]);
}

#[test]
fn stats_bucket_key_bucket_unit_whitelist() {
    // 未知单位 → 引擎 `bucket_unit_nanos` 返回 None → 同样整行被跳过。
    let input = stats_rule_with_keys("bucket(a.event_time, \"week\")", "a | count as total;");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "second argument must be one of",
    );
}

#[test]
fn stats_bucket_key_tier_bounds_must_be_numeric() {
    let input = stats_rule_with_keys("tier(a.count, \"x\")", "a | count as total;");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "must be numeric",
    );
}

#[test]
fn stats_bucket_key_unknown_field_rejected() {
    let input = stats_rule_with_keys("a.nope", "a | count as total;");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "stats bucket key:",
    );
}

// ---------------------------------------------------------------------------
// 拒绝：`conv` 链表达式（wf-cep 在收口批上逐 output 求值）
// ---------------------------------------------------------------------------

fn conv_rule(conv: &str) -> String {
    format!(
        r#"
rule r {{
    events {{ e : auth_events }}
    match<sip:5m:fixed> {{ on event {{ e | count >= 1; }} }} -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
    conv {{ {conv} }}
}}
"#
    )
}

#[test]
fn l3_rejected_in_conv_where() {
    let input = conv_rule("where(first(e.count) > 0) ;");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "not allowed in `conv` expressions",
    );
}

#[test]
fn l3_rejected_in_conv_sort_key() {
    let input = conv_rule("sort(-first(e.count)) | top(3) ;");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "not allowed in `conv` expressions",
    );
}

#[test]
fn l3_rejected_in_conv_dedup() {
    let input = conv_rule("dedup(collect_set(e.action)) ;");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "not allowed in `conv` expressions",
    );
}

#[test]
fn baseline_rejected_in_conv_where() {
    let input = conv_rule("where(baseline(e.count, 300) > 3) ;");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "rolling baseline state only accumulates",
    );
}

#[test]
fn window_lookup_rejected_in_conv_where() {
    let input = conv_rule("where(threat_list.has(e.sip)) ;");
    assert_has_error(&input, &join_schemas(), "window lookups are not available");
}

#[test]
fn window_lookup_rejected_in_conv_dedup() {
    let input = conv_rule("dedup(threat_list.has(e.sip)) ;");
    assert_has_error(&input, &join_schemas(), "window lookups are not available");
}

#[test]
fn baseline_rejected_in_conv_sort_key() {
    let input = conv_rule("sort(-baseline(e.count, 300)) | top(3) ;");
    assert_has_error(
        &input,
        &[auth_events_window(), output_window()],
        "rolling baseline state only accumulates",
    );
}

#[test]
fn non_l3_conv_expression_allowed() {
    let input = conv_rule("sort(-e.count) | top(3) ;");
    assert_no_errors(&input, &[auth_events_window(), output_window()]);
}

// ---------------------------------------------------------------------------
// 拒绝：嵌套在控制流 / 字面量里的调用（闸门遍历覆盖）
// ---------------------------------------------------------------------------

#[test]
fn l3_rejected_when_nested_in_if_then_else() {
    let input = r#"
rule r {
    events { e : auth_events }
    let m = if e.count > 0 then first(e.count) else 0
    on each e -> score(1.0)
    entity(ip, e.sip)
    yield out (x = e.sip, n = m)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in `on each` expressions",
    );
}

#[test]
fn l3_rejected_when_nested_in_array_literal() {
    let input = r#"
rule r {
    events { a : auth_events }
    let m = mvcount(array [first(a.count)])
    on each a -> score(1.0)
    entity(ip, a.sip)
    yield out (x = a.sip, n = m)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in `on each` expressions",
    );
}

// ---------------------------------------------------------------------------
// 放行：无需动态上下文的能力不受影响
// ---------------------------------------------------------------------------

#[test]
fn now_allowed_in_emit_at() {
    // `now*()` 由求值墙钟提供（不需要窗口/实例/基线状态）。
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    join fw_events within [a.event_time, a.event_time] on a.dip == fw_events.sip
        emit at now()
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_no_errors(input, &join_schemas());
}

#[test]
fn baseline_dev_allowed_in_bind_filter() {
    // baseline_dev 走全局基线库（wf_cep::baseline::store()），不依赖调用方滚动表。
    let input = r#"
rule r {
    events { e : auth_events && baseline_dev(e.sip, e.user, e.count) > 3.0 }
    match<sip:5m> { on event { e.action | distinct | count >= 3; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn non_l3_function_allowed_in_join_within_bound() {
    let input = r#"
rule r {
    events { a : auth_events }
    on each a -> score(1.0)
    join fw_events within [bucket_end(a.event_time, 10s), bucket_end(a.event_time, 10s)]
        on a.dip == fw_events.sip
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_no_errors(input, &join_schemas());
}

// ---------------------------------------------------------------------------
// 拒绝：instance 上下文位置（score/entity/yield、`let`、post-join `where`）
//
// 这些位置由引擎在 instance 上下文求值：L3 可用，但求值路径不传窗口表与滚动
// 状态表，因此 `window.has` / `baseline` 仍恒为空。
// ---------------------------------------------------------------------------

#[test]
fn baseline_rejected_in_match_rule_yield() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = baseline(e.count, 300))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "rolling baseline state only accumulates",
    );
}

#[test]
fn baseline_rejected_in_match_rule_let() {
    let input = r#"
rule r {
    events { e : auth_events }
    let b = baseline(e.count, 300)
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = b)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in instance-context expressions",
    );
}

#[test]
fn baseline_rejected_in_match_post_join_where() {
    let input = r#"
rule r {
    events { a : auth_events }
    match<a.sip:5m> { on event { a | count >= 1; } } -> score(1.0)
    join fw_events within [a.event_time, a.event_time] on a.dip == fw_events.sip
    where baseline(a.count, 300) > 3
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &join_schemas(),
        "rolling baseline state only accumulates",
    );
}

#[test]
fn baseline_rejected_in_score() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(baseline(e.count, 300))
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "not allowed in instance-context expressions",
    );
}

#[test]
fn window_lookup_rejected_in_match_rule_let() {
    let input = r#"
rule r {
    events { e : auth_events }
    let h = threat_list.has(e.sip)
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = h)
}
"#;
    assert_has_error(
        input,
        &join_schemas(),
        "not allowed in instance-context expressions",
    );
}

#[test]
fn window_lookup_rejected_in_match_post_join_where() {
    let input = r#"
rule r {
    events { a : auth_events }
    match<a.sip:5m> { on event { a | count >= 1; } } -> score(1.0)
    join fw_events within [a.event_time, a.event_time] on a.dip == fw_events.sip
    where threat_list.has(a.sip)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(input, &join_schemas(), "window lookups are not available");
}

#[test]
fn window_lookup_rejected_in_match_yield() {
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = threat_list.has(e.sip))
}
"#;
    assert_has_error(input, &join_schemas(), "window lookups are not available");
}

// ---------------------------------------------------------------------------
// 对照：instance 上下文位置必须继续放行
// ---------------------------------------------------------------------------

#[test]
fn baseline_allowed_in_guard() {
    // 分支 guard 同时带窗口表与滚动状态表（`baseline` 的唯一可用位置）。
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> {
        on event { e && baseline(e.count, 300) > 1 | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert_no_errors(input, &join_schemas());
}

#[test]
fn baseline_dev_allowed_in_match_yield() {
    // baseline_dev 走全局基线库，instance 位置可用。
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = baseline_dev(e.sip, e.user, e.count))
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn now_allowed_in_match_yield() {
    // `now*()` 由求值墙钟提供，不需要任何动态上下文。
    let input = r#"
rule r {
    events { e : auth_events }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = now())
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn l3_allowed_in_instance_positions() {
    // score / entity / yield / `let` / post-join `where` 均有实例序列，L3 可用。
    let input = r#"
rule r {
    events { a : auth_events }
    let first_count = first(a.count)
    let all_actions = collect_set(a.action)
    match<a.sip:10m> {
        on event { a | count >= 1; }
    } -> score(stddev(a.count))
    join fw_events within [a.event_time, a.event_time] on a.dip == fw_events.sip
    where percentile(a.count, 50) >= 0
    entity(chars, first_count)
    yield out (y = last(a.action), n = mvcount(all_actions))
}
"#;
    assert_no_errors(input, &join_schemas());
}

#[test]
fn l3_allowed_in_match_rule_let() {
    // `first()` 可作稳定聚合键（文档 language-reference 亦如此说明）。
    let input = r#"
rule r {
    events { a : auth_events }
    let tenant = first(a.sip)
    let dedup = join_by("|", tenant, "x")
    match<a.sip:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(chars, tenant)
    yield out (y = dedup)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn l3_allowed_in_match_post_join_where() {
    let input = r#"
rule r {
    events { a : auth_events }
    match<a.sip:5m> { on event { a | count >= 1; } } -> score(1.0)
    join fw_events within [a.event_time, a.event_time] on a.dip == fw_events.sip
    where first(a.count) > 0
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_no_errors(input, &join_schemas());
}
