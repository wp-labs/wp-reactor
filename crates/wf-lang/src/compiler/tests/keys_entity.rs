use super::*;

// =========================================================================
// 5. compile_no_key
// =========================================================================

#[test]
fn compile_no_key() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule nokey {
    events { e : win }
    match<:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert!(plans[0].match_plan.keys.is_empty());
}

// =========================================================================
// 6. compile_compound_keys
// =========================================================================

#[test]
fn compile_compound_keys() {
    let schemas = [generic_window(), output_window()];
    let plans = compile_with(
        r#"
rule compound {
    events { e : win }
    match<sip,dport:5m> {
        on event { e | count >= 1; }
    } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    let keys = &plans[0].match_plan.keys;
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], FieldRef::Simple("sip".into()));
    assert_eq!(keys[1], FieldRef::Simple("dport".into()));
}

// =========================================================================
// 6c. issue #83 — 派生（let）/嵌套路径 key 编译
// =========================================================================

/// security window with a structured `roles_obj` object field.
fn structured_window() -> WindowSchema {
    make_window(
        "sec_events",
        vec!["sec_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("roles_obj", crate::schema::FieldType::Object),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

#[test]
fn compile_let_derived_key_inlines_to_field_ref() {
    // match key 引用 let（纯字段路径定义）→ 编译内联为等值 FieldRef::Path。
    let schemas = [structured_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { s : sec_events }
    let attacker_ip = s.roles_obj.attacker.endpoint.ip
    match<attacker_ip:5m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
        &schemas,
    );
    let keys = &plans[0].match_plan.keys;
    assert_eq!(keys.len(), 1);
    assert_eq!(
        keys[0],
        FieldRef::Path {
            alias: "s".into(),
            segments: vec![
                crate::ast::PathSegment::Field("roles_obj".into()),
                crate::ast::PathSegment::Field("attacker".into()),
                crate::ast::PathSegment::Field("endpoint".into()),
                crate::ast::PathSegment::Field("ip".into()),
            ],
        }
    );
}

#[test]
fn compile_nested_path_key_equals_inlined_let_key() {
    // 直接嵌套路径 key 与等价 let key 编译产物一致（issue #83 验收：聚合结果一致）。
    let schemas = [structured_window(), output_window()];
    let direct = compile_with(
        r#"
rule r {
    events { s : sec_events }
    match<s.roles_obj.attacker.endpoint.ip:5m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
        &schemas,
    );
    let via_let = compile_with(
        r#"
rule r {
    events { s : sec_events }
    let attacker_ip = s.roles_obj.attacker.endpoint.ip
    match<attacker_ip:5m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
        &schemas,
    );
    assert_eq!(direct[0].match_plan.keys, via_let[0].match_plan.keys);
    assert_eq!(
        direct[0].match_plan.keys,
        vec![FieldRef::Path {
            alias: "s".into(),
            segments: vec![
                crate::ast::PathSegment::Field("roles_obj".into()),
                crate::ast::PathSegment::Field("attacker".into()),
                crate::ast::PathSegment::Field("endpoint".into()),
                crate::ast::PathSegment::Field("ip".into()),
            ],
        }]
    );
}

/// window 同时含顶层 `attacker_ip` 列与结构化 `roles_obj`——验证 let 同名遮蔽。
fn shadow_window() -> WindowSchema {
    make_window(
        "sec_events",
        vec!["sec_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("attacker_ip", bt(BaseType::Chars)),
            ("roles_obj", crate::schema::FieldType::Object),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

#[test]
fn compile_let_key_shadows_same_name_flat_field() {
    // schema 有顶层 `attacker_ip`，同时规则定义了同名 let → key 按 let 优先
    // 解析（与表达式解析一致）→ 编译内联为嵌套路径。
    let schemas = [shadow_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { s : sec_events }
    let attacker_ip = s.roles_obj.attacker.endpoint.ip
    match<attacker_ip:5m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
        &schemas,
    );
    assert_eq!(
        plans[0].match_plan.keys,
        vec![FieldRef::Path {
            alias: "s".into(),
            segments: vec![
                crate::ast::PathSegment::Field("roles_obj".into()),
                crate::ast::PathSegment::Field("attacker".into()),
                crate::ast::PathSegment::Field("endpoint".into()),
                crate::ast::PathSegment::Field("ip".into()),
            ],
        }]
    );
}

#[test]
fn compile_flat_key_untouched_without_let() {
    // 无 let 时同名字段保持普通顶层 key（不被 let 解析干扰）。
    let schemas = [shadow_window(), output_window()];
    let plans = compile_with(
        r#"
rule r {
    events { s : sec_events }
    match<attacker_ip:5m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
        &schemas,
    );
    assert_eq!(
        plans[0].match_plan.keys,
        vec![FieldRef::Simple("attacker_ip".into())]
    );
}

// =========================================================================
// 6c-2. issue #80 — 表达式派生（函数/字面量）key 编译装配
// =========================================================================

/// 标量流窗口：chars/digit 字段，供 #80 函数/字面量派生 key 测试。
fn scalar_window() -> WindowSchema {
    make_window(
        "flow_events",
        vec!["flow_stream"],
        vec![
            ("src", bt(BaseType::Chars)),
            ("dst", bt(BaseType::Chars)),
            ("proto", bt(BaseType::Chars)),
            ("dport", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

fn flow_out_window() -> WindowSchema {
    make_output_window("out", vec![("x", bt(BaseType::Chars))])
}

#[test]
fn compile_expr_derived_key_keeps_logical_name_with_expr_slot() {
    // #80：`let k = concat(...)` 作 key —— 无法内联成 FieldRef：
    // keys[0] 保留 `Simple(k)` 作逻辑名，展开后的纯事件字段表达式进 key_exprs[0]。
    let schemas = [scalar_window(), flow_out_window()];
    let plans = compile_with(
        r#"
rule r {
    events { s : flow_events }
    let pair = concat(s.src, ":", s.dst)
    match<pair:10m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(chars, s.src)
    yield out (x = s.src)
}
"#,
        &schemas,
    );
    let plan = &plans[0].match_plan;
    assert_eq!(plan.keys, vec![FieldRef::Simple("pair".into())]);
    assert_eq!(plan.key_exprs.len(), 1);
    assert_eq!(
        plan.key_exprs[0],
        Some(Expr::FuncCall {
            qualifier: None,
            name: "concat".into(),
            args: vec![
                Expr::Field(FieldRef::Qualified("s".into(), "src".into())),
                Expr::StringLit(":".into()),
                Expr::Field(FieldRef::Qualified("s".into(), "dst".into())),
            ],
        })
    );
}

#[test]
fn compile_expr_derived_key_expands_let_chains() {
    // #80：let 链（`base = s.src` → `pair = concat(base, ...)`）在展开时
    // 递归内联 —— 展开结果不残留任何 let 名引用。
    let schemas = [scalar_window(), flow_out_window()];
    let plans = compile_with(
        r#"
rule r {
    events { s : flow_events }
    let base = s.src
    let pair = concat(base, s.dst)
    match<pair:10m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(chars, s.src)
    yield out (x = s.src)
}
"#,
        &schemas,
    );
    let plan = &plans[0].match_plan;
    assert_eq!(plan.keys, vec![FieldRef::Simple("pair".into())]);
    assert_eq!(
        plan.key_exprs[0],
        Some(Expr::FuncCall {
            qualifier: None,
            name: "concat".into(),
            args: vec![
                Expr::Field(FieldRef::Qualified("s".into(), "src".into())),
                Expr::Field(FieldRef::Qualified("s".into(), "dst".into())),
            ],
        })
    );
}

#[test]
fn compile_literal_derived_key_uses_expr_slot() {
    // #80：字面量派生 key（`let k = "vip"`）同样走表达式槽。
    let schemas = [scalar_window(), flow_out_window()];
    let plans = compile_with(
        r#"
rule r {
    events { s : flow_events }
    let tier = "vip"
    match<tier:10m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(chars, s.src)
    yield out (x = s.src)
}
"#,
        &schemas,
    );
    let plan = &plans[0].match_plan;
    assert_eq!(plan.keys, vec![FieldRef::Simple("tier".into())]);
    assert_eq!(plan.key_exprs[0], Some(Expr::StringLit("vip".into())));
}

#[test]
fn compile_mixed_expr_and_plain_keys_keep_positions() {
    // #80：表达式 key 与普通顶层 key 混用，keys/key_exprs 逐位对齐。
    let schemas = [scalar_window(), flow_out_window()];
    let plans = compile_with(
        r#"
rule r {
    events { s : flow_events }
    let proto_src = concat(s.proto, s.src)
    match<src, proto_src:10m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(chars, s.src)
    yield out (x = s.src)
}
"#,
        &schemas,
    );
    let plan = &plans[0].match_plan;
    // 位 0：普通字段（原样）；位 1：表达式 let（逻辑名保留 + 槽位）。
    assert_eq!(plan.keys.len(), 2);
    assert_eq!(plan.keys[0], FieldRef::Simple("src".into()));
    assert_eq!(plan.keys[1], FieldRef::Simple("proto_src".into()));
    assert_eq!(plan.key_exprs.len(), 2);
    assert_eq!(plan.key_exprs[0], None);
    assert_eq!(
        plan.key_exprs[1],
        Some(Expr::FuncCall {
            qualifier: None,
            name: "concat".into(),
            args: vec![
                Expr::Field(FieldRef::Qualified("s".into(), "proto".into())),
                Expr::Field(FieldRef::Qualified("s".into(), "src".into())),
            ],
        })
    );
}

#[test]
fn compile_no_expr_keys_leaves_slot_empty() {
    // 回归：普通字段 key 与 #83 纯字段 let key 均不产生表达式槽（空 Vec），
    // 引擎列式直读/快路径不受影响。
    let schemas = [scalar_window(), flow_out_window()];
    let plain = compile_with(
        r#"
rule r {
    events { s : flow_events }
    match<src, dport:10m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(chars, s.src)
    yield out (x = s.src)
}
"#,
        &schemas,
    );
    assert_eq!(plain[0].match_plan.key_exprs.len(), 0);

    // #83：纯字段 let key（roles_obj 结构体）内联为 Path、无表达式槽。
    let schemas2 = [structured_window(), flow_out_window()];
    let field_let = compile_with(
        r#"
rule r {
    events { s : sec_events }
    let attacker_ip = s.roles_obj.attacker.endpoint.ip
    match<attacker_ip:10m> {
        on event { s | count >= 1; }
    } -> score(50.0)
    entity(ip, s.sip)
    yield out (x = s.sip)
}
"#,
        &schemas2,
    );
    assert_eq!(field_let[0].match_plan.key_exprs.len(), 0);
    assert_eq!(
        field_let[0].match_plan.keys,
        vec![FieldRef::Path {
            alias: "s".into(),
            segments: vec![
                crate::ast::PathSegment::Field("roles_obj".into()),
                crate::ast::PathSegment::Field("attacker".into()),
                crate::ast::PathSegment::Field("endpoint".into()),
                crate::ast::PathSegment::Field("ip".into()),
            ],
        }]
    );
}

// =========================================================================
// 6b. join-then-key (Path A): key_join compilation
// =========================================================================

/// `match<category:10m>` where `category` lives on the snapshot-joined
/// auction window → `key_join` descriptor is populated with the lookup
/// parameters the runtime needs.
#[test]
fn compile_join_key_populates_key_join() {
    let bid = make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Digit)),
            ("price", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let auction = make_window(
        "auction_events",
        vec!["auction_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("seller", bt(BaseType::Digit)),
            ("category", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let out = make_output_window("out", vec![("id", bt(BaseType::Digit))]);
    let plans = compile_with(
        r#"
rule r {
    events { b : bid_events }
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#,
        &[bid, auction, out],
    );
    let plan = &plans[0];
    assert_eq!(
        plan.match_plan.keys,
        vec![FieldRef::Simple("category".into())]
    );
    let kj = plan
        .match_plan
        .key_join
        .as_ref()
        .expect("category is a join-side key → key_join should be populated");
    assert_eq!(kj.join_idx, 0, "single snapshot join → index 0");
    assert_eq!(kj.right_window, "auction_events");
    assert_eq!(
        kj.left_field,
        FieldRef::Qualified("b".into(), "auction".into())
    );
    assert_eq!(kj.right_key_field, "id");
    assert_eq!(kj.right_field, "category");
    assert_eq!(kj.key_name, "category");
}

/// A driver key on a rule with a join (e.g. q21 anti-join style) must NOT be
/// treated as a join key — `key_join` stays `None`.
#[test]
fn compile_join_present_with_driver_key_keeps_key_join_none() {
    let bid = make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Digit)),
            ("price", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let person = make_window(
        "person_events",
        vec!["person_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("name", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let out = make_output_window("out", vec![("id", bt(BaseType::Digit))]);
    let plans = compile_with(
        r#"
rule r {
    events { b : bid_events }
    match<auction:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join person_events anti on b.bidder == person_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#,
        &[bid, person, out],
    );
    assert!(
        plans[0].match_plan.key_join.is_none(),
        "driver key `auction` must not resolve to a join key"
    );
}

/// A field that exists on BOTH the driver and the join window resolves as a
/// driver key (driver wins — checker K1 requires presence in all event sources
/// before falling back to a join window).
#[test]
fn compile_join_key_skipped_when_driver_also_has_field() {
    let bid = make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Digit)),
            // `seller` exists on the driver too → must stay a driver key even
            // though auction_events also carries it.
            ("seller", bt(BaseType::Digit)),
            ("price", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let auction = make_window(
        "auction_events",
        vec!["auction_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("seller", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let out = make_output_window("out", vec![("id", bt(BaseType::Digit))]);
    let plans = compile_with(
        r#"
rule r {
    events { b : bid_events }
    match<seller:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#,
        &[bid, auction, out],
    );
    assert!(
        plans[0].match_plan.key_join.is_none(),
        "driver field `seller` wins over the join window field"
    );
    assert_eq!(
        plans[0].match_plan.keys,
        vec![FieldRef::Simple("seller".into())]
    );
}

/// A `let` key shadows a same-named snapshot-join window field: the key
/// semantics switch from join-then-key to let-derived, so `key_join` must be
/// cleared even though `resolve_join_key` (which cannot see lets) would have
/// populated it — otherwise the engine's advance prefers the stale `key_join`
/// branch and silently ignores the derived key (issue #80 review 1).
#[test]
fn compile_join_key_cleared_when_let_shadows_join_field() {
    let bid = make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Digit)),
            ("price", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let auction = make_window(
        "auction_events",
        vec!["auction_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("category", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let out = make_output_window("out", vec![("id", bt(BaseType::Digit))]);

    // #80 表达式派生 let 与 join 右窗字段同名：let 优先，key_join 必须清空。
    let expr_let = compile_with(
        r#"
rule r {
    events { b : bid_events }
    let category = concat("c", b.bidder)
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#,
        &[bid.clone(), auction.clone(), out.clone()],
    );
    let mp = &expr_let[0].match_plan;
    assert!(
        mp.key_join.is_none(),
        "let 遮蔽 join 右窗字段 → key_join 必须清空"
    );
    assert_eq!(mp.keys, vec![FieldRef::Simple("category".into())]);
    assert!(mp.key_exprs[0].is_some(), "表达式派生落表达式槽");

    // #83 纯字段 let（指向 driver 字段）同样遮蔽：key_join 清空、内联 driver 字段。
    let field_let = compile_with(
        r#"
rule r {
    events { b : bid_events }
    let category = b.auction
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#,
        &[bid, auction, out],
    );
    let mp = &field_let[0].match_plan;
    assert!(
        mp.key_join.is_none(),
        "#83 纯字段 let 遮蔽 → key_join 必须清空"
    );
    assert_eq!(
        mp.keys,
        vec![FieldRef::Qualified("b".into(), "auction".into())]
    );
    assert!(mp.key_exprs.is_empty(), "纯字段内联无表达式槽");
}

// =========================================================================
// 7. compile_entity_type_normalization
// =========================================================================

#[test]
fn compile_entity_type_normalization() {
    let schemas = [generic_window(), output_window()];

    // Ident form: ip
    let plans_ident = compile_with(
        r#"
rule r1 {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans_ident[0].entity_plan.entity_type, "ip");

    // StringLit form: "ip"
    let plans_str = compile_with(
        r#"
rule r2 {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity("ip", e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans_str[0].entity_plan.entity_type, "ip");

    // Both normalize to the same string
    assert_eq!(
        plans_ident[0].entity_plan.entity_type,
        plans_str[0].entity_plan.entity_type
    );
}

/// Uppercase entity type is lowercased during compilation.
#[test]
fn compile_entity_type_case_normalization() {
    let schemas = [generic_window(), output_window()];

    let plans = compile_with(
        r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(IP, e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans[0].entity_plan.entity_type, "ip");

    let plans2 = compile_with(
        r#"
rule r {
    events { e : win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity("IP", e.sip)
    yield out (x = e.sip)
}
"#,
        &schemas,
    );
    assert_eq!(plans2[0].entity_plan.entity_type, "ip");
}
