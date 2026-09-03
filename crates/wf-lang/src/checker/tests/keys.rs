use super::*;

/// Minimal bid-style driver window (no `category`/`seller` — those live on the
/// joined auction window).
fn bid_window() -> WindowSchema {
    make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Digit)),
            ("price", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Minimal auction-style lookup window (snapshot join target).
fn auction_window() -> WindowSchema {
    make_window(
        "auction_events",
        vec!["auction_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("seller", bt(BaseType::Digit)),
            ("category", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Output window with a digit `id` field (matches `b.auction`).
fn out_id_window() -> WindowSchema {
    make_output_window("out", vec![("id", bt(BaseType::Digit))])
}

// ===========================================================================
// issue #83 — 派生（let）/嵌套路径 match key
// ===========================================================================

/// auth_events with a structured `roles_obj` object field（嵌套 key 的 root）。
fn derived_auth_window() -> WindowSchema {
    make_window(
        "auth_events",
        vec!["auth_stream"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("roles_obj", crate::schema::FieldType::Object),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

fn derived_out_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![("x", bt(BaseType::Chars)), ("n", bt(BaseType::Digit))],
    )
}

#[test]
fn nested_path_match_key_accepted() {
    // issue #83 验收 2：多层嵌套字段直接作为 match key（root 为结构化字段）。
    let input = r#"
rule r {
    events { a : auth_events }
    match<a.roles_obj.attacker.endpoint.ip:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_no_errors(input, &[derived_auth_window(), derived_out_window()]);
}

#[test]
fn nested_path_match_key_with_index_accepted() {
    // 嵌套 key 可含数组索引段（`related[0].x`）。
    let input = r#"
rule r {
    events { a : auth_events }
    match<a.roles_obj.related[0].process.uid:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_no_errors(input, &[derived_auth_window(), derived_out_window()]);
}

#[test]
fn let_derived_match_key_accepted() {
    // issue #83 验收 1：match key 引用 let 派生字段（纯字段路径定义）。
    let input = r#"
rule r {
    events { a : auth_events }
    let attacker_ip = a.roles_obj.attacker.endpoint.ip
    let victim_ip = a.roles_obj.victim.endpoint.ip
    match<attacker_ip, victim_ip:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_no_errors(input, &[derived_auth_window(), derived_out_window()]);
}

#[test]
fn nested_path_match_key_requires_structured_root() {
    // 嵌套 key 的 root 必须是结构化字段（object/array）。
    let input = r#"
rule r {
    events { a : auth_events }
    match<a.sip.attacker:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &[derived_auth_window(), derived_out_window()],
        "is not an object/array",
    );
}

#[test]
fn nested_path_match_key_missing_root_errors() {
    let input = r#"
rule r {
    events { a : auth_events }
    match<a.nope.obj.id:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &[derived_auth_window(), derived_out_window()],
        "field `nope` not found in window",
    );
}

/// flow_events with chars/digit/float scalars — issue #80 表达式派生 key 用。
fn derived_flow_window() -> WindowSchema {
    make_window(
        "flow_events",
        vec!["flow_stream"],
        vec![
            ("src", bt(BaseType::Chars)),
            ("dst", bt(BaseType::Chars)),
            ("proto", bt(BaseType::Chars)),
            ("dport", bt(BaseType::Digit)),
            ("score", bt(BaseType::Float)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

#[test]
fn let_derived_match_key_accepts_expression_definition() {
    // issue #80 验收 1：match key 引用**函数派生** let（concat/coalesce/case 等
    // 表达式结果）——不再要求 let 定义为纯字段/嵌套路径形态。
    let input = r#"
rule r {
    events { a : flow_events }
    let pair = concat(a.src, ":", a.dst)
    match<pair:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(chars, a.src)
    yield out (x = a.src)
}
"#;
    assert_no_errors(input, &[derived_flow_window(), derived_out_window()]);
}

#[test]
fn let_derived_match_key_accepts_literal_definition() {
    // issue #80 验收 2：字面量派生 let 也可作 key（所有事件同组）。
    let input = r#"
rule r {
    events { a : flow_events }
    let watch = "watch-all"
    match<watch:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(chars, a.src)
    yield out (x = a.src)
}
"#;
    assert_no_errors(input, &[derived_flow_window(), derived_out_window()]);
}

#[test]
fn let_derived_match_key_accepts_coalesce_definition() {
    // issue #80 验收 3：coalesce 回退派生 key（chars + 字面量回退）。
    let input = r#"
rule r {
    events { a : flow_events }
    let target = coalesce(a.dst, a.src, "unknown")
    match<target:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(chars, a.src)
    yield out (x = a.src)
}
"#;
    assert_no_errors(input, &[derived_flow_window(), derived_out_window()]);
}

#[test]
fn let_derived_match_key_rejects_float_definition() {
    // #80：float 派生值仍不能作 key（与普通 key/嵌套路径约束一致）。
    let input = r#"
rule r {
    events { a : flow_events }
    let s = a.score
    match<s:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(chars, a.src)
    yield out (x = a.src)
}
"#;
    assert_has_error(
        input,
        &[derived_flow_window(), derived_out_window()],
        "标量 key 类型",
    );
}

#[test]
fn expr_derived_match_key_mixed_with_plain_keys() {
    // #80：表达式派生 key 与普通顶层 key / #83 纯字段 let key 混用均可。
    let input = r#"
rule r {
    events { a : flow_events }
    let proto_pair = concat(a.proto, a.dst)
    match<proto_pair, src:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(chars, a.src)
    yield out (x = a.src)
}
"#;
    assert_no_errors(input, &[derived_flow_window(), derived_out_window()]);
}

#[test]
fn let_derived_match_key_rejects_state_dependent_func() {
    // review 3：窗口/状态依赖函数（first/last/has/baseline/collect_*…）混入
    // key 派生表达式必须拒绝——引擎对触发事件逐事件求值无这些上下文，静默
    // 失效（求值 None → 全跳 / 全同组）。
    for rhs in [
        "first(a.src)",
        "has(a.dport, 443)",
        "collect_set(a.src)",
        "now_s()",
        "concat(a.src, first(a.dst))", // 嵌套命中同样拒绝
    ] {
        let input = format!(
            r#"
rule r {{
    events {{ a : flow_events }}
    let k = {rhs}
    match<k:10m> {{
        on event {{ a | count >= 1; }}
    }} -> score(50.0)
    entity(chars, a.src)
    yield out (x = a.src)
}}
"#
        );
        assert_has_error(
            &input,
            &[derived_flow_window(), derived_out_window()],
            "窗口/状态依赖函数",
        );
    }
}

#[test]
fn let_derived_match_key_rejects_non_scalar_type() {
    // key 的 let 派生值必须是标量（object 直接作 key 拒绝）。
    let input = r#"
rule r {
    events { a : auth_events }
    let o = a.roles_obj
    match<o:10m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &[derived_auth_window(), derived_out_window()],
        "标量 key 类型",
    );
}

#[test]
fn let_key_rejects_join_window_field_reference_in_rhs() {
    // review 2：let 派生 key 的 RHS 只能引用 driver 事件源字段——引擎在
    // **driver 事件**上求 key（join 窗事件不参与 advance），引用 join 右窗
    // 字段（auction.category）会被放行但运行时永远读不到 → 规则全跳。
    // 实测 scope_build 里 let 在 join 窗口注册前检查 → join 别名不可解析
    // 即被拒（与 join-then-key 的 key_join 语义互斥，不可经 let 混用）。
    let input = r#"
rule r {
    events { b : bid_events }
    let category = auction.category
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid_window(), auction_window(), out_id_window()],
        "not a declared alias",
    );
}

#[test]
fn join_key_resolves_from_snapshot_join() {
    // `category` is absent from bid_events but present on the snapshot-joined
    // auction_events → join-then-key is accepted.
    let input = r#"
rule r {
    events { b : bid_events }
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_no_errors(input, &[bid_window(), auction_window(), out_id_window()]);
}

#[test]
fn join_key_requires_snapshot_join() {
    let input = r#"
rule r {
    events { b : bid_events }
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events anti on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid_window(), auction_window(), out_id_window()],
        "requires a snapshot join",
    );
}

#[test]
fn join_key_compound_rejected() {
    // Two keys both resolving to the join window — v1 rejects compound join keys.
    let input = r#"
rule r {
    events { b : bid_events }
    match<category, seller:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid_window(), auction_window(), out_id_window()],
        "compound join keys",
    );
}

#[test]
fn join_key_mixed_with_driver_key_rejected() {
    // `auction` is a driver field; `category` is a join field — mixed keys are
    // rejected in v1.
    let input = r#"
rule r {
    events { b : bid_events }
    match<auction, category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid_window(), auction_window(), out_id_window()],
        "mixed driver/join keys",
    );
}

#[test]
fn join_key_with_key_mapping_rejected() {
    let input = r#"
rule r {
    events { b : bid_events }
    match<category:10m> {
        key {
            category = b.auction;
        }
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid_window(), auction_window(), out_id_window()],
        "not supported together with a join key",
    );
}

#[test]
fn qualified_join_window_key_rejected() {
    // Join-side keys must be written unqualified so the compiler can route them
    // through join-then-key.
    let input = r#"
rule r {
    events { b : bid_events }
    match<auction_events.category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid_window(), auction_window(), out_id_window()],
        "must be unqualified",
    );
}

#[test]
fn join_key_ambiguous_across_snapshot_joins_rejected() {
    // Two snapshot joins whose windows both carry `category` — ambiguous source.
    let other = make_window(
        "auction_events2",
        vec!["auction2_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("category", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let input = r#"
rule r {
    events { b : bid_events }
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    join auction_events2 snapshot on b.auction == auction_events2.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid_window(), auction_window(), other, out_id_window()],
        "ambiguous",
    );
}

#[test]
fn join_key_left_side_must_reference_driver() {
    // Join left referencing the join window itself (not the driver) — rejected.
    let input = r#"
rule r {
    events { b : bid_events }
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on auction_events.id == auction_events.category
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid_window(), auction_window(), out_id_window()],
        "must reference the driver event",
    );
}

#[test]
fn join_key_scalar_type_required() {
    // Join key field of structured type — cannot hash → rejected.
    let auction = make_window(
        "auction_events",
        vec!["auction_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("category", FieldType::Object),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let input = r#"
rule r {
    events { b : bid_events }
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid_window(), auction, out_id_window()],
        "not a scalar base type",
    );
}

#[test]
fn key_field_not_in_all_sources() {
    // "dport" only exists in fw_events, not in auth_events
    let input = r#"
rule r {
    events { a : auth_events  b : fw_events }
    match<dport:5m> {
        on event {
            a | count >= 1;
        }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), fw_events_window(), output_window()],
        "match key `dport` not found in event source `a`",
    );
}

#[test]
fn key_type_mismatch() {
    // Create two windows where 'sip' has different types.
    let w1 = make_window("win1", vec!["s"], vec![("sip", bt(BaseType::Ip))]);
    let w2 = make_window("win2", vec!["s"], vec![("sip", bt(BaseType::Chars))]);
    let out = output_window();

    let input = r#"
rule r {
    events { a : win1  b : win2 }
    match<sip:5m> {
        on event { a | count >= 1; }
    } -> score(50.0)
    entity(ip, a.sip)
    yield out (x = a.sip)
}
"#;
    assert_has_error(input, &[w1, w2, out], "type mismatch");
}

#[test]
fn qualified_key_valid() {
    let input = r#"
rule r {
    events { fail : auth_events }
    match<fail.sip:5m> {
        on event { fail | count >= 1; }
    } -> score(50.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#;
    assert_no_errors(input, &[auth_events_window(), output_window()]);
}

#[test]
fn key_field_not_in_window() {
    let input = r#"
rule r {
    events { fail : auth_events }
    match<fail.nonexistent:5m> {
        on event { fail | count >= 1; }
    } -> score(50.0)
    entity(ip, fail.sip)
    yield out (x = fail.sip)
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), output_window()],
        "field `nonexistent` not found",
    );
}

#[test]
fn join_key_left_field_float_rejected() {
    // The join index key truncates f64 (`JoinKey::from_value` does `*n as i64`)
    // — a float driver-side condition field (b.auction) would hash into the
    // truncated slot and false-match a real row. The right-side scalar rule
    // already excludes float; K1d mirrors it on the left.
    let bid = make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Float)),
            ("bidder", bt(BaseType::Digit)),
            ("price", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let input = r#"
rule r {
    events { b : bid_events }
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid, auction_window(), out_id_window()],
        "f64 truncation would false-match",
    );
}

#[test]
fn join_key_left_right_type_mismatch_rejected() {
    // Driver-side condition field Chars vs right-side key Digit: the join
    // index would never match consistently — rejected at compile time.
    let bid = make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Chars)),
            ("bidder", bt(BaseType::Digit)),
            ("price", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    );
    let input = r#"
rule r {
    events { b : bid_events }
    match<category:10m> {
        on event { b | count >= 1; }
    } -> score(50.0)
    join auction_events snapshot on b.auction == auction_events.id
    entity(digit, b.auction)
    yield out (id = b.auction)
}
"#;
    assert_has_error(
        input,
        &[bid, auction_window(), out_id_window()],
        "type mismatch",
    );
}
