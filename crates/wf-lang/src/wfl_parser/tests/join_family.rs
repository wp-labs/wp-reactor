// P1 join 算子族：`within [lo,hi]` / `reduce ... as label` / `emit at` 语法解析测试。
// 设计文档：docs/design/join-family-design.md §3（定稿 BNF）与 §6（Q8/Q9 落地形态）。

use std::time::Duration;

use crate::ast::*;
use crate::wfl_parser::parse_wfl;

/// Q9 形态：reduce maxrow + tie + within 行内界 + `as label` 在 on 条件后 + emit at。
#[test]
fn parse_join_q9_reduce_within_emit() {
    let input = r#"
rule q9 {
    events { a : auction_events }
    match<id:10m> { on event { a | count >= 1; } } -> score(1.0)
    join bid_events reduce maxrow(price) tie(dateTime asc)
        within [a.dateTime, a.expires]
        on a.id == bid_events.auction as winner
        emit at a.expires
    entity(digit, a.id)
    yield out (id = a.id, winner_id = winner.bidder)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j = &file.rules[0].joins[0];
    assert_eq!(j.target_window, "bid_events");
    // 无 mode 关键字 → 缺省 inner
    assert_eq!(j.mode, JoinMode::Inner);
    // reduce
    let rc = j.reduce.as_ref().expect("reduce");
    assert_eq!(rc.label.as_deref(), Some("winner"));
    assert_eq!(
        rc.measure,
        ReduceMeasure::Maxrow {
            field: FieldRef::Simple("price".into()),
            tie: Some(TieSpec {
                field: FieldRef::Simple("dateTime".into()),
                desc: false,
            }),
        }
    );
    // within 行内界（绝对时间表达式）
    let w = j.within.as_ref().expect("within");
    assert_eq!(w.lo.open, false);
    assert_eq!(
        w.lo.val,
        BoundVal::Expr(Expr::Field(FieldRef::Qualified(
            "a".into(),
            "dateTime".into()
        )))
    );
    assert_eq!(w.hi.open, false);
    assert_eq!(
        w.hi.val,
        BoundVal::Expr(Expr::Field(FieldRef::Qualified(
            "a".into(),
            "expires".into()
        )))
    );
    // emit at
    assert_eq!(
        j.emit_at,
        Some(Expr::Field(FieldRef::Qualified(
            "a".into(),
            "expires".into()
        )))
    );
}

/// Q8 形态：within 开区间上界（`<bucket_end(...)`）+ 函数调用界 + emit at 同表达式。
#[test]
fn parse_join_q8_within_open_upper_bucket_end() {
    let input = r#"
rule q8 {
    events { p : person_events }
    match<id:10m> { on event { p | count >= 1; } } -> score(1.0)
    join auction_events within [p.dateTime, <bucket_end(p.dateTime, 10s)]
        on p.id == auction_events.seller
        emit at bucket_end(p.dateTime, 10s)
    entity(digit, p.id)
    yield out (id = p.id)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j = &file.rules[0].joins[0];
    assert_eq!(j.mode, JoinMode::Inner);
    let w = j.within.as_ref().expect("within");
    // 上开
    assert_eq!(w.hi.open, true);
    match &w.hi.val {
        BoundVal::Expr(Expr::FuncCall { name, args, .. }) => {
            assert_eq!(name, "bucket_end");
            assert_eq!(args.len(), 2);
        }
        other => panic!("expected bucket_end func call, got {:?}", other),
    }
    match &j.emit_at {
        Some(Expr::FuncCall { name, .. }) => assert_eq!(name, "bucket_end"),
        other => panic!("expected emit at bucket_end(...), got {:?}", other),
    }
}

/// `within 10s` 糖 ≡ `within [-10s, 0s]`。
#[test]
fn parse_within_duration_sugar() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join ip_repdb snapshot within 10s on sip == ip_repdb.ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j = &file.rules[0].joins[0];
    assert_eq!(j.mode, JoinMode::Snapshot);
    let w = j.within.as_ref().expect("within");
    assert_eq!(
        w.lo.val,
        BoundVal::Dur {
            dur: Duration::from_secs(10),
            neg: true
        }
    );
    assert_eq!(
        w.hi.val,
        BoundVal::Dur {
            dur: Duration::ZERO,
            neg: false
        }
    );
}

/// 常量区间界 + 开闭记号：`[1s, <5s]`、`<=` 显式闭。
#[test]
fn parse_within_constant_bounds_open_markers() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join ip_repdb snapshot within [1s, <5s] on sip == ip_repdb.ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let w = file.rules[0].joins[0].within.as_ref().expect("within");
    assert_eq!(
        w.lo.val,
        BoundVal::Dur {
            dur: Duration::from_secs(1),
            neg: false
        }
    );
    assert_eq!(w.lo.open, false);
    assert_eq!(
        w.hi.val,
        BoundVal::Dur {
            dur: Duration::from_secs(5),
            neg: false
        }
    );
    assert_eq!(w.hi.open, true);

    // `<=` 显式闭 + `within 10m` 前置于 reduce 之前（顺序无关）
    let input = r#"
rule r2 {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join ip_repdb within [<=1s, <=5s] reduce last(ip) on sip == ip_repdb.ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j = &file.rules[0].joins[0];
    let w = j.within.as_ref().expect("within");
    assert_eq!(w.lo.open, false);
    assert_eq!(w.hi.open, false);
    assert!(matches!(
        j.reduce.as_ref().map(|r| &r.measure),
        Some(ReduceMeasure::Last { .. })
    ));
}

/// reduce 变体：minrow + tie desc、top(N, field)、reduce 后的 `as label`（BNF 形态）。
#[test]
fn parse_reduce_variants() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join ip_repdb reduce minrow(count) tie(ts desc) as best on sip == ip_repdb.ip
    join geo_log reduce top(3, dist) on sip == geo_log.src_ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j0 = &file.rules[0].joins[0];
    assert_eq!(
        j0.reduce.as_ref().map(|r| r.label.as_deref()),
        Some(Some("best"))
    );
    assert_eq!(
        j0.reduce.as_ref().map(|r| &r.measure),
        Some(&ReduceMeasure::Minrow {
            field: FieldRef::Simple("count".into()),
            tie: Some(TieSpec {
                field: FieldRef::Simple("ts".into()),
                desc: true,
            }),
        })
    );
    let j1 = &file.rules[0].joins[1];
    assert_eq!(
        j1.reduce.as_ref().map(|r| &r.measure),
        Some(&ReduceMeasure::Top {
            n: 3,
            field: FieldRef::Simple("dist".into()),
        })
    );
}

/// `asof within 10s` 保持兼容（mode 携带 within；无 interval within 字段）。
#[test]
fn parse_asof_within_duration_unchanged() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join geo_log asof within 10m on sip == geo_log.src_ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j = &file.rules[0].joins[0];
    assert_eq!(
        j.mode,
        JoinMode::Asof {
            within: Some(Duration::from_secs(600))
        }
    );
    assert!(j.within.is_none());
}

/// `asof within [1s, 2s]`：duration 解析回溯 → interval within 交给 within 子句。
#[test]
fn parse_asof_within_bracket_interval() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join geo_log asof within [1s, 2s] on sip == geo_log.src_ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j = &file.rules[0].joins[0];
    assert_eq!(j.mode, JoinMode::Asof { within: None });
    let w = j.within.as_ref().expect("within interval");
    assert_eq!(
        w.lo.val,
        BoundVal::Dur {
            dur: Duration::from_secs(1),
            neg: false
        }
    );
    assert_eq!(
        w.hi.val,
        BoundVal::Dur {
            dur: Duration::from_secs(2),
            neg: false
        }
    );
}

/// `as label` 无 reduce → 解析失败。
#[test]
fn parse_as_label_without_reduce_rejected() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join ip_repdb snapshot on sip == ip_repdb.ip as winner
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

/// reduce 标签重复（reduce 后 + on 后各一个）→ 解析失败。
#[test]
fn parse_duplicate_label_rejected() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join ip_repdb reduce maxrow(ip) as a on sip == ip_repdb.ip as b
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    assert!(parse_wfl(input).is_err());
}

/// 无 mode 关键字（缺省 inner）与 `within`/`reduce` 前的 `within` 顺序。
#[test]
fn parse_default_inner_mode() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join ip_repdb on sip == ip_repdb.ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    assert_eq!(file.rules[0].joins[0].mode, JoinMode::Inner);
    assert!(file.rules[0].joins[0].within.is_none());
    assert!(file.rules[0].joins[0].reduce.is_none());
    assert!(file.rules[0].joins[0].emit_at.is_none());
}

/// review 补充：`tie(field, desc)` 逗号变体 + `within [<1s, <5s]` 双开区间。
#[test]
fn parse_tie_comma_variant_and_both_open_bounds() {
    let input = r#"
rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
    join ip_repdb reduce minrow(count) tie(ts, desc) within [<1s, <5s] on sip == ip_repdb.ip
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let file = parse_wfl(input).unwrap();
    let j = &file.rules[0].joins[0];
    assert_eq!(
        j.reduce.as_ref().map(|r| &r.measure),
        Some(&ReduceMeasure::Minrow {
            field: FieldRef::Simple("count".into()),
            tie: Some(TieSpec {
                field: FieldRef::Simple("ts".into()),
                desc: true,
            }),
        })
    );
    let w = j.within.as_ref().expect("within");
    assert_eq!(w.lo.open, true);
    assert_eq!(w.hi.open, true);
}
