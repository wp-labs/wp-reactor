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
