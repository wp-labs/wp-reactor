use winnow::ascii::dec_uint;
use winnow::combinator::{cut_err, delimited, opt, separated, separated_pair};
use winnow::error::{AddContext, ContextError, ErrMode, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{duration_value, ident, kw, quoted_string, ws_skip};

/// Parse match params:
///   `[key, key, ...] : duration`               (sliding window)
///   `[key, key, ...] : duration : fixed`       (fixed window)
///   `[key, key, ...] : session(gap)`           (session window, L3)
///   `[key, key, ...] : hop(size, slide)`       (HOP sliding window)
pub(super) fn match_params(
    input: &mut &str,
) -> ModalResult<(Vec<FieldRef>, std::time::Duration, WindowMode)> {
    ws_skip.parse_next(input)?;

    let keys = parse_match_keys(input)?;
    ws_skip.parse_next(input)?;

    // session(gap) window（L3）
    if opt(kw("session")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let gap = parse_session_gap(input)?;
        return Ok((keys, gap, WindowMode::Session(gap)));
    }

    // hop(size, slide) window
    if opt(kw("hop")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let (size, slide) = parse_hop_size_slide(input)?;
        return Ok((keys, size, WindowMode::Hop { size, slide }));
    }

    // sliding / fixed
    let (dur, window_mode) = parse_sliding_fixed(input)?;
    Ok((keys, dur, window_mode))
}

/// sliding / fixed 路径：`<duration> [: fixed]` → (时长, 模式)。
fn parse_sliding_fixed(input: &mut &str) -> ModalResult<(std::time::Duration, WindowMode)> {
    let dur = cut_err(duration_value)
        .context(StrContext::Expected(StrContextValue::Description(
            "duration value",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    let window_mode = parse_fixed_suffix(input)?;
    Ok((dur, window_mode))
}

/// `[: fixed]` 窗口模式后缀：无 `:` → Sliding；`: fixed` → Fixed。
/// （调用方已消费 duration 及其后的空白）
fn parse_fixed_suffix(input: &mut &str) -> ModalResult<WindowMode> {
    if opt(literal(":")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        cut_err(kw("fixed"))
            .context(StrContext::Expected(StrContextValue::Description(
                "'fixed'",
            )))
            .parse_next(input)?;
        ws_skip.parse_next(input)?;
        Ok(WindowMode::Fixed)
    } else {
        Ok(WindowMode::Sliding)
    }
}

/// 解析 `[key, ...]` 前导键列表；以 `:` 开头表示无键（空列表）。
fn parse_match_keys(input: &mut &str) -> ModalResult<Vec<FieldRef>> {
    if opt(literal(":")).parse_next(input)?.is_some() {
        return Ok(vec![]);
    }
    let keys: Vec<FieldRef> =
        separated(1.., field_ref, (ws_skip, literal(","), ws_skip)).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(":")).parse_next(input)?;
    Ok(keys)
}

/// 解析 `(gap)`（调用方已消费 `session` 与其后的空白）。组合子化整段解析：
/// 各 token 的细分错误提示合并为该形态的描述，接受/拒绝与 cut 语义不变。
fn parse_session_gap(input: &mut &str) -> ModalResult<std::time::Duration> {
    let gap = delimited(
        (cut_err(literal("(")), ws_skip),
        cut_err(duration_value),
        (ws_skip, cut_err(literal(")"))),
    )
    .context(StrContext::Expected(StrContextValue::Description(
        "session window gap duration: session(<gap>)",
    )))
    .parse_next(input)?;
    ws_skip.parse_next(input)?;
    Ok(gap)
}

/// 解析 `(size, slide)` 并校验 size 是 slide 的正整数倍（调用方已消费 `hop` 与其后的空白）。
/// 组合子化整段解析：各 token 的细分错误提示合并为该形态的描述。
fn parse_hop_size_slide(
    input: &mut &str,
) -> ModalResult<(std::time::Duration, std::time::Duration)> {
    let (size, slide) = delimited(
        (cut_err(literal("(")), ws_skip),
        separated_pair(
            cut_err(duration_value),
            (ws_skip, cut_err(literal(",")), ws_skip),
            cut_err(duration_value),
        ),
        (ws_skip, cut_err(literal(")"))),
    )
    .context(StrContext::Expected(StrContextValue::Description(
        "hop window size and slide: hop(<size>, <slide>)",
    )))
    .parse_next(input)?;
    ws_skip.parse_next(input)?;
    if !hop_ratio_ok(size, slide) {
        return Err(ErrMode::Cut(ContextError::new().add_context(
            input,
            &input.checkpoint(),
            StrContext::Expected(StrContextValue::Description(
                "hop size must be a positive multiple of slide",
            )),
        )));
    }
    Ok((size, slide))
}

/// hop 校验：size 必须为 slide 的正整数倍（二者均非零）。
fn hop_ratio_ok(size: std::time::Duration, slide: std::time::Duration) -> bool {
    size.as_nanos() != 0
        && slide.as_nanos() != 0
        && size.as_nanos().is_multiple_of(slide.as_nanos())
}

/// Parse a field reference for match keys: `ident`, `ident.ident`, a multi-level
/// nested path `ident.a.b.c` / `ident.obj[0].name` (issue #83), or `ident["string"]`.
///
/// 与表达式上下文（`expr.rs` 字段解析）同构：单层限定 → `Qualified`（向后兼容），
/// 更深 → `Path`；段可含 `[index]`。
pub(super) fn field_ref(input: &mut &str) -> ModalResult<FieldRef> {
    ws_skip.parse_next(input)?;
    let first = ident.parse_next(input)?;

    if opt(literal(".")).parse_next(input)?.is_some() {
        // `first.second[.…][[…]…]` — 限定字段 / 嵌套 path
        field_ref_dot(first, input)
    } else if opt(literal("[")).parse_next(input)?.is_some() {
        // `first["string"]` — 括号字段
        field_ref_bracketed(first, input)
    } else {
        Ok(FieldRef::Simple(first.to_string()))
    }
}

/// `first.second[(…)…]`——单层 → Qualified（向后兼容），更深 → Path
/// （段可含 `[index]`）。调用方已消费 `.`。
fn field_ref_dot(first: &str, input: &mut &str) -> ModalResult<FieldRef> {
    ws_skip.parse_next(input)?;
    let second = cut_err(ident).parse_next(input)?;
    ws_skip.parse_next(input)?;
    // Consume further segments: `.ident` members and `[integer]` indices.
    let mut segments = vec![PathSegment::Field(second.to_string())];
    while let Some(segment) = parse_path_segment_tail(input)? {
        segments.push(segment);
    }
    // Single level → backward-compatible Qualified; deeper → Path.
    Ok(if segments.len() == 1 {
        FieldRef::Qualified(first.to_string(), second.to_string())
    } else {
        FieldRef::Path {
            alias: first.to_string(),
            segments,
        }
    })
}

/// `first["string"]`——调用方已消费 `[`。
fn field_ref_bracketed(first: &str, input: &mut &str) -> ModalResult<FieldRef> {
    let key = delimited(
        ws_skip,
        cut_err(quoted_string),
        (ws_skip, cut_err(literal("]"))),
    )
    .parse_next(input)?;
    Ok(FieldRef::Bracketed(first.to_string(), key))
}

/// 解析限定路径的后续段：`.member` 或 `[index]`；无后续段返回 `None`。
fn parse_path_segment_tail(input: &mut &str) -> ModalResult<Option<PathSegment>> {
    if let Some(segment) = parse_path_member_tail(input)? {
        return Ok(Some(segment));
    }
    parse_path_index_tail(input)
}

/// `.member` 段；不是 `.` 开头则不消费并返回 `None`。
fn parse_path_member_tail(input: &mut &str) -> ModalResult<Option<PathSegment>> {
    if opt(literal(".")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let seg = cut_err(ident).parse_next(input)?;
        ws_skip.parse_next(input)?;
        Ok(Some(PathSegment::Field(seg.to_string())))
    } else {
        Ok(None)
    }
}

/// `[index]` 段；不是 `[` 开头则不消费并返回 `None`。
fn parse_path_index_tail(input: &mut &str) -> ModalResult<Option<PathSegment>> {
    if opt(literal("[")).parse_next(input)?.is_some() {
        ws_skip.parse_next(input)?;
        let idx: usize = cut_err(dec_uint).parse_next(input)?;
        ws_skip.parse_next(input)?;
        cut_err(literal("]")).parse_next(input)?;
        ws_skip.parse_next(input)?;
        Ok(Some(PathSegment::Index(idx)))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn params(input: &str) -> (Vec<FieldRef>, std::time::Duration, WindowMode) {
        let mut s = input;
        match_params
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("match_params failed for {input:?}: {e:?}"))
    }

    fn params_err(input: &str) {
        let mut s = input;
        assert!(
            match_params.parse_next(&mut s).is_err(),
            "expected match_params error for {input:?}"
        );
    }

    fn fref(input: &str) -> FieldRef {
        let mut s = input;
        field_ref
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("field_ref failed for {input:?}: {e:?}"))
    }

    #[test]
    fn params_session_and_hop() {
        let (keys, dur, mode) = params(": session(5m)");
        assert!(keys.is_empty());
        assert_eq!(dur, std::time::Duration::from_secs(300));
        match mode {
            WindowMode::Session(gap) => assert_eq!(gap, std::time::Duration::from_secs(300)),
            other => panic!("expected Session, got {other:?}"),
        }

        let (_, dur, mode) = params(": hop(10s, 5s)");
        assert_eq!(dur, std::time::Duration::from_secs(10));
        match mode {
            WindowMode::Hop { size, slide } => {
                assert_eq!(size, std::time::Duration::from_secs(10));
                assert_eq!(slide, std::time::Duration::from_secs(5));
            }
            other => panic!("expected Hop, got {other:?}"),
        }
        // size 必须是 slide 的正整数倍
        params_err(": hop(10s, 3s)");
        params_err(": hop(0s, 1s)");
    }

    #[test]
    fn params_keys_sliding_and_fixed() {
        let (keys, dur, mode) = params("a.b, c : 1m");
        assert_eq!(keys.len(), 2);
        assert_eq!(dur, std::time::Duration::from_secs(60));
        assert!(matches!(mode, WindowMode::Sliding));

        let (keys, dur, mode) = params("e : 30s : fixed");
        assert_eq!(keys, vec![FieldRef::Simple("e".into())]);
        assert_eq!(dur, std::time::Duration::from_secs(30));
        assert!(matches!(mode, WindowMode::Fixed));
    }

    #[test]
    fn field_ref_forms() {
        assert_eq!(fref("x"), FieldRef::Simple("x".into()));
        assert_eq!(fref("a.b"), FieldRef::Qualified("a".into(), "b".into()));
        match fref("a.b.c") {
            FieldRef::Path { alias, segments } => {
                assert_eq!(alias, "a");
                assert_eq!(segments.len(), 2);
                assert!(matches!(segments[1], PathSegment::Field(_)));
            }
            other => panic!("expected Path, got {other:?}"),
        }
        match fref("a.b[0].c") {
            FieldRef::Path { segments, .. } => {
                assert_eq!(segments.len(), 3);
                assert!(matches!(segments[1], PathSegment::Index(0)));
            }
            other => panic!("expected Path with index, got {other:?}"),
        }
        assert_eq!(
            fref("a[\"k\"]"),
            FieldRef::Bracketed("a".into(), "k".into())
        );
    }

    #[test]
    fn params_invalid_fixed_suffix_and_bracket_key() {
        // `: fixed` 后缀外其余关键字均报错
        params_err("e : 30s : tumble");

        // 键列表支持括号字段名
        let (keys, dur, mode) = params("a[\"k\"] : 5m");
        assert_eq!(keys, vec![FieldRef::Bracketed("a".into(), "k".into())]);
        assert_eq!(dur, std::time::Duration::from_secs(300));
        assert!(matches!(mode, WindowMode::Sliding));
    }

    #[test]
    fn path_member_and_index_tails() {
        let mut s = ".b";
        assert_eq!(
            parse_path_member_tail.parse_next(&mut s).unwrap().unwrap(),
            PathSegment::Field("b".into())
        );
        assert!(s.is_empty());

        let mut s = "x";
        assert!(parse_path_member_tail.parse_next(&mut s).unwrap().is_none());
        assert_eq!(s, "x");

        let mut s = "[3]";
        assert_eq!(
            parse_path_index_tail.parse_next(&mut s).unwrap().unwrap(),
            PathSegment::Index(3)
        );
        // 非整数下标与溢出均报错
        let mut s = "[x]";
        assert!(parse_path_index_tail.parse_next(&mut s).is_err());
        let mut s = "[99999999999999999999999999]";
        assert!(parse_path_index_tail.parse_next(&mut s).is_err());
    }

    #[test]
    fn hop_ratio_validation() {
        use std::time::Duration;
        let s = Duration::from_secs;
        let h = Duration::from_secs;
        assert!(hop_ratio_ok(h(10), s(5)));
        assert!(hop_ratio_ok(s(60), s(10))); // 1min / 10s
        assert!(!hop_ratio_ok(h(10), s(3))); // 非整数倍
        assert!(!hop_ratio_ok(Duration::ZERO, s(5))); // size 为零
        assert!(!hop_ratio_ok(h(10), Duration::ZERO)); // slide 为零
    }

    #[test]
    fn hop_and_session_token_errors() {
        // hop：size / slide / 括号任一段缺失或错位都报错
        params_err(": hop(10s, 5s"); // 缺 `)`
        params_err(": hop(10s 5s)"); // size 后缺 `,`
        params_err(": hop(10s)"); // 缺 slide
        params_err(": hop(10s, )"); // slide 时长缺失
        // session：缺 `)`
        params_err(": session(5m");
        params_err(": session(5m 1s)"); // gap 后多段
    }
}
