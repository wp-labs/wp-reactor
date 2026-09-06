mod expect;
mod input;
mod options;

use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{ident, kw, ws_skip};

// ---------------------------------------------------------------------------
// test block
// ---------------------------------------------------------------------------

/// `test NAME for RULE_NAME { input { ... } expect { ... } [options { ... }] }`
pub(super) fn test_block(input_str: &mut &str) -> ModalResult<TestBlock> {
    ws_skip.parse_next(input_str)?;
    kw("test").parse_next(input_str)?;
    ws_skip.parse_next(input_str)?;

    let (name, rule_name) = parse_test_header(input_str)?;

    ws_skip.parse_next(input_str)?;
    cut_err(literal("{")).parse_next(input_str)?;

    let (input_stmts, expect, options) = parse_test_body(input_str)?;

    ws_skip.parse_next(input_str)?;
    cut_err(literal("}")).parse_next(input_str)?;

    Ok(TestBlock {
        name,
        rule_name,
        input: input_stmts,
        expect,
        options,
    })
}

/// `input { ... } expect { ... } [options { ... }]` 主体（`{` 已消费）。
fn parse_test_body(
    input: &mut &str,
) -> ModalResult<(Vec<InputStmt>, Vec<ExpectStmt>, Option<TestOptions>)> {
    ws_skip.parse_next(input)?;
    let input_stmts = parse_input_block(input)?;
    ws_skip.parse_next(input)?;
    let expect = parse_expect_block(input)?;
    ws_skip.parse_next(input)?;
    let options = opt(options::options_block).parse_next(input)?;
    Ok((input_stmts, expect, options))
}

/// `NAME for RULE_NAME`（`test` 关键字与前置空白已消费）。
fn parse_test_header(input: &mut &str) -> ModalResult<(String, String)> {
    let name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "test name",
        )))
        .parse_next(input)?
        .to_string();

    ws_skip.parse_next(input)?;
    cut_err(kw("for"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'for' after test name",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;

    let rule_name = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "rule name after 'for'",
        )))
        .parse_next(input)?
        .to_string();
    Ok((name, rule_name))
}

/// `input { ... }`（cut + 诊断上下文）。
fn parse_input_block(input: &mut &str) -> ModalResult<Vec<InputStmt>> {
    cut_err(input::input_block)
        .context(StrContext::Expected(StrContextValue::Description(
            "input block",
        )))
        .parse_next(input)
}

/// `expect { ... }`（cut + 诊断上下文）。
fn parse_expect_block(input: &mut &str) -> ModalResult<Vec<ExpectStmt>> {
    cut_err(expect::expect_block)
        .context(StrContext::Expected(StrContextValue::Description(
            "expect block",
        )))
        .parse_next(input)
}

// ---------------------------------------------------------------------------
// shared: cmp_op
// ---------------------------------------------------------------------------

pub(super) fn cmp_op(input: &mut &str) -> ModalResult<CmpOp> {
    alt((
        literal("==").value(CmpOp::Eq),
        literal("!=").value(CmpOp::Ne),
        literal("<=").value(CmpOp::Le),
        literal(">=").value(CmpOp::Ge),
        literal("<").value(CmpOp::Lt),
        literal(">").value(CmpOp::Gt),
    ))
    .parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    #[test]
    fn test_block_parses_full_body_with_options() {
        let mut s = "test bt for brute_force { input { row(e, action = \"failed\"); } \
                     expect { hits == 1; } options { runs = 3; } }";
        let t = test_block
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("test_block parse failed: {e:?}"));
        assert_eq!(t.name, "bt");
        assert_eq!(t.rule_name, "brute_force");
        assert_eq!(t.input.len(), 1);
        assert_eq!(t.expect.len(), 1);
        assert_eq!(t.options.as_ref().and_then(|o| o.runs), Some(3));
        assert!(s.is_empty());
    }

    #[test]
    fn test_block_without_options_and_error_cases() {
        let mut s = "test t for r { input { tick(1m); } expect { hits == 0; } }";
        let t = test_block
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("test_block parse failed: {e:?}"));
        assert!(t.options.is_none(), "options 可缺省");
        assert!(matches!(&t.input[0], crate::ast::InputStmt::Tick(_)));

        // 缺 expect 块 → 错误
        let mut bad = "test t for r { input { } }";
        assert!(test_block.parse_next(&mut bad).is_err());
        // 缺 'for'
        let mut bad2 = "test t r { input { } expect { } }";
        assert!(test_block.parse_next(&mut bad2).is_err());
    }
}
