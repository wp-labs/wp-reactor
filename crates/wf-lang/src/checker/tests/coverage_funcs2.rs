//! check_funcs.rs 参数/分支校验（续）：mv 族、聚合函数 set-level 限制，
//! 以及 stat.* selector 校验分支（coverage_funcs.rs 的后续段，自 coverage_extra.rs
//! 拆出；经 `use super::*` 复用父模块 window harness）。

use super::*;

#[test]
fn func_mv_family_validation() {
    let mvcount = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount())
}
"#;
    assert_has_error(
        mvcount,
        &[auth_events_window(), wide_output_window()],
        "mvcount() requires exactly 1 argument",
    );

    let mvjoin_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvjoin(split(e.user, ",")))
}
"#;
    assert_has_error(
        mvjoin_count,
        &[auth_events_window(), wide_output_window()],
        "mvjoin() requires exactly 2 arguments",
    );

    let mvjoin_sep = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvjoin(split(e.user, ","), e.sip))
}
"#;
    assert_has_error(
        mvjoin_sep,
        &[auth_events_window(), wide_output_window()],
        "mvjoin() second argument must be chars separator",
    );

    let split_type = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount(split(e.sip, ",")))
}
"#;
    assert_has_error(
        split_type,
        &[auth_events_window(), wide_output_window()],
        "split() first argument must be chars",
    );

    let mvsort_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvjoin(mvsort(), ","))
}
"#;
    assert_has_error(
        mvsort_count,
        &[auth_events_window(), wide_output_window()],
        "mvsort() requires exactly 1 argument",
    );

    let mvindex_count = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvindex(split(e.user, ",")))
}
"#;
    assert_has_error(
        mvindex_count,
        &[auth_events_window(), wide_output_window()],
        "mvindex() requires 2 or 3 arguments",
    );

    let mvindex_idx = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvindex(split(e.user, ","), e.sip))
}
"#;
    assert_has_error(
        mvindex_idx,
        &[auth_events_window(), wide_output_window()],
        "mvindex() second argument must be numeric index",
    );

    let mvindex_end = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (y = mvindex(split(e.user, ","), 1, e.sip))
}
"#;
    assert_has_error(
        mvindex_end,
        &[auth_events_window(), wide_output_window()],
        "mvindex() third argument must be numeric index",
    );
}

#[test]
fn func_mvappend_validation() {
    let empty = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvappend())
}
"#;
    assert_has_error(
        empty,
        &[auth_events_window(), wide_output_window()],
        "mvappend() requires at least 1 argument",
    );

    // split(...) is Array(Chars); e.sip is Ip — element types do not unify.
    let incompatible = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount(mvappend(split(e.user, ","), e.sip)))
}
"#;
    assert_has_error(
        incompatible,
        &[auth_events_window(), wide_output_window()],
        "mvappend() argument 2 type",
    );

    // Object arg is neither scalar nor array.
    let object_arg = r#"
rule r {
    events { e : obj_win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount(mvappend(e.obj)))
}
"#;
    assert_has_error(
        object_arg,
        &[obj_win(), wide_output_window()],
        "mvappend() argument 1 must be scalar or array expression",
    );

    // Bool element + array-of-chars element is compatible-typed via Base(bool).
    let bool_ok = r#"
rule r {
    events { e : obj_win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = mvcount(mvappend(e.active, e.active)))
}
"#;
    assert_no_errors(bool_ok, &[obj_win(), wide_output_window()]);
}

#[test]
fn func_aggregates_reject_set_level_alias() {
    let sum_alias = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = sum(e))
}
"#;
    assert_has_error(
        sum_alias,
        &[auth_events_window(), wide_output_window()],
        "sum() requires a field projection like alias.field; set-level alias `e` is not allowed",
    );

    let min_alias = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = min(e))
}
"#;
    assert_has_error(
        min_alias,
        &[auth_events_window(), wide_output_window()],
        "min() requires a field projection like alias.field; set-level alias `e` is not allowed",
    );

    // count() with a field projection (without distinct) is rejected.
    let count_field = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = count(e.sip))
}
"#;
    assert_has_error(
        count_field,
        &[auth_events_window(), wide_output_window()],
        "count() expects a set-level argument (alias), not a field projection",
    );

    // sum() over a nested path is rejected as non-column.
    let sum_path = r#"
rule r {
    events { e : obj_win }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = sum(e.obj.inner))
}
"#;
    assert_has_error(
        sum_path,
        &[obj_win(), wide_output_window()],
        "sum() argument must be a column projection (alias.field)",
    );

    // count(e) — a set-level alias — is valid.
    let count_ok = r#"
rule r {
    events { e : auth_events }
    match<:5m> { on event { e | count >= 1; } } -> score(50.0)
    entity(ip, e.sip)
    yield out (n = count(e))
}
"#;
    assert_no_errors(count_ok, &[auth_events_window(), wide_output_window()]);
}

// ===========================================================================
// check_funcs.rs — stat.* selector validation branches
// ===========================================================================

#[test]
fn stat_count_rejects_wrong_selector() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(trigger(fail)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "stat.count() accepts window_event(...), match_event(...), or match_distinct(...), got trigger(...)",
    );
}

#[test]
fn stat_value_rejects_wrong_selector() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.value(window_event(auth)))
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "stat.value() accepts trigger(...) or final(...), got window_event(...)",
    );
}

#[test]
fn stat_count_requires_one_selector_arg() {
    let input = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count())
}
"#;
    assert_has_error(
        input,
        &[auth_events_window(), wide_output_window()],
        "stat.count() requires exactly 1 stat selector argument",
    );
}

#[test]
fn stat_selector_parse_errors() {
    let unknown = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(bogus(fail)))
}
"#;
    assert_has_error(
        unknown,
        &[auth_events_window(), wide_output_window()],
        "unknown stat selector `bogus(...)`",
    );

    let wrong_args = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(window_event(auth, extra)))
}
"#;
    assert_has_error(
        wrong_args,
        &[auth_events_window(), wide_output_window()],
        "stat selector `window_event(...)` requires exactly 1 symbol argument",
    );

    let non_func = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { fail: auth | count >= 1; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(auth))
}
"#;
    assert_has_error(
        non_func,
        &[auth_events_window(), wide_output_window()],
        "stat functions require a selector such as window_event(alias) or trigger(label)",
    );

    // stat.count(match_event(label)) requires the label measure to be count.
    let non_count = r#"
rule r {
    events { auth : auth_events }
    match<sip:5m> { on event { ports: auth.count | sum >= 2; } } -> score(50.0)
    entity(ip, auth.sip)
    yield out (n = stat.count(match_event(ports)))
}
"#;
    assert_has_error(
        non_count,
        &[auth_events_window(), wide_output_window()],
        "stat.count(match_event(ports)) requires step label `ports` to use count",
    );
}
