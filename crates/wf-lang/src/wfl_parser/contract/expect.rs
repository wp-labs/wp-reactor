use winnow::combinator::{alt, cut_err};
use winnow::error::{StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::ast::*;
use crate::parse_utils::{kw, nonneg_integer, number_literal, quoted_string, ws_skip};

use super::super::expr;
use super::cmp_op;

// ---------------------------------------------------------------------------
// expect block
// ---------------------------------------------------------------------------

/// `expect { hits cmp NUMBER; ... hit[i].assert; ... }`
pub(super) fn expect_block(input: &mut &str) -> ModalResult<Vec<ExpectStmt>> {
    kw("expect").parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("{")).parse_next(input)?;

    let mut stmts = Vec::new();
    loop {
        ws_skip.parse_next(input)?;
        if input.starts_with('}') {
            break;
        }
        let stmt = cut_err(expect_stmt)
            .context(StrContext::Expected(StrContextValue::Description(
                "expect statement (hits or hit[i].assert)",
            )))
            .parse_next(input)?;
        stmts.push(stmt);
    }

    cut_err(literal("}")).parse_next(input)?;
    Ok(stmts)
}

fn expect_stmt(input: &mut &str) -> ModalResult<ExpectStmt> {
    alt((expect_hits, expect_hit_assert)).parse_next(input)
}

/// `hits cmp_op INTEGER;`
fn expect_hits(input: &mut &str) -> ModalResult<ExpectStmt> {
    kw("hits").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let cmp = cut_err(cmp_op).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let count = cut_err(nonneg_integer)
        .context(StrContext::Expected(StrContextValue::Description(
            "non-negative integer for hits count",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;
    Ok(ExpectStmt::Hits { cmp, count })
}

/// `hit[INTEGER].assert;`
fn expect_hit_assert(input: &mut &str) -> ModalResult<ExpectStmt> {
    kw("hit").parse_next(input)?;
    let index = parse_hit_index(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(".")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let assert = cut_err(hit_assert).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(";")).parse_next(input)?;
    Ok(ExpectStmt::HitAssert { index, assert })
}

/// `[<index>]` 命中下标（调用方已消费 `hit`）。
fn parse_hit_index(input: &mut &str) -> ModalResult<usize> {
    ws_skip.parse_next(input)?;
    cut_err(literal("[")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let index = cut_err(nonneg_integer)
        .context(StrContext::Expected(StrContextValue::Description(
            "non-negative integer index",
        )))
        .parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal("]")).parse_next(input)?;
    Ok(index)
}

fn hit_assert(input: &mut &str) -> ModalResult<HitAssert> {
    alt((
        hit_assert_score,
        hit_assert_origin,
        hit_assert_entity_type,
        hit_assert_entity_id,
        hit_assert_field,
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "hit assertion (score|origin|entity_type|entity_id|field)",
    )))
    .parse_next(input)
}

/// `score cmp_op NUMBER`
fn hit_assert_score(input: &mut &str) -> ModalResult<HitAssert> {
    kw("score").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let cmp = cut_err(cmp_op).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(number_literal).parse_next(input)?;
    Ok(HitAssert::Score { cmp, value })
}

/// `origin == STRING`
fn hit_assert_origin(input: &mut &str) -> ModalResult<HitAssert> {
    kw("origin").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = hit_assert_eq_string(input)?;
    Ok(HitAssert::Origin { value })
}

/// `entity_type == STRING`
fn hit_assert_entity_type(input: &mut &str) -> ModalResult<HitAssert> {
    kw("entity_type").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = hit_assert_eq_string(input)?;
    Ok(HitAssert::EntityType { value })
}

/// `entity_id == STRING`
fn hit_assert_entity_id(input: &mut &str) -> ModalResult<HitAssert> {
    kw("entity_id").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = hit_assert_eq_string(input)?;
    Ok(HitAssert::EntityId { value })
}

/// `== "STRING"` 尾部（调用方已消费关键字与空白）——
/// origin / entity_type / entity_id 三个断言共用。
fn hit_assert_eq_string(input: &mut &str) -> ModalResult<String> {
    cut_err(literal("==")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(quoted_string).parse_next(input)
}

/// `field(STRING) cmp_op expr`
fn hit_assert_field(input: &mut &str) -> ModalResult<HitAssert> {
    kw("field").parse_next(input)?;
    ws_skip.parse_next(input)?;
    let name = parse_paren_quoted(input)?;
    ws_skip.parse_next(input)?;
    let cmp = cut_err(cmp_op).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let value = cut_err(expr::parse_expr).parse_next(input)?;
    Ok(HitAssert::Field { name, cmp, value })
}

/// `( "STRING" )` 括号内引号串（调用方已消费 `field` 与空白）。
fn parse_paren_quoted(input: &mut &str) -> ModalResult<String> {
    cut_err(literal("(")).parse_next(input)?;
    ws_skip.parse_next(input)?;
    let name = cut_err(quoted_string).parse_next(input)?;
    ws_skip.parse_next(input)?;
    cut_err(literal(")")).parse_next(input)?;
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use winnow::Parser;

    fn expect_of(input: &str) -> Vec<ExpectStmt> {
        let mut s = input;
        expect_block
            .parse_next(&mut s)
            .unwrap_or_else(|e| panic!("expect_block failed for {input:?}: {e:?}"))
    }

    fn expect_err(input: &str) {
        let mut s = input;
        assert!(
            expect_block.parse_next(&mut s).is_err(),
            "expected expect_block error for {input:?}"
        );
    }

    #[test]
    fn expect_block_all_stmt_shapes() {
        let stmts = expect_of(
            "expect { hits == 1; hit[0].score == 50.0; hit[0].origin == \"close:timeout\"; \
             hit[0].entity_type == \"ip\"; hit[0].entity_id == \"10.0.0.8\"; \
             hit[0].field(\"domain\") == \"evil.test\"; }",
        );
        assert_eq!(stmts.len(), 6);

        match &stmts[0] {
            ExpectStmt::Hits { cmp, count } => {
                assert_eq!(*cmp, CmpOp::Eq);
                assert_eq!(*count, 1);
            }
            other => panic!("expected Hits, got {other:?}"),
        }

        match &stmts[1] {
            ExpectStmt::HitAssert { index, assert } => {
                assert_eq!(*index, 0);
                assert_eq!(
                    *assert,
                    HitAssert::Score {
                        cmp: CmpOp::Eq,
                        value: 50.0
                    }
                );
            }
            other => panic!("expected HitAssert, got {other:?}"),
        }

        match &stmts[2] {
            ExpectStmt::HitAssert { assert, .. } => {
                assert_eq!(
                    *assert,
                    HitAssert::Origin {
                        value: "close:timeout".into()
                    }
                );
            }
            other => panic!("expected HitAssert, got {other:?}"),
        }
        match &stmts[3] {
            ExpectStmt::HitAssert { assert, .. } => {
                assert_eq!(*assert, HitAssert::EntityType { value: "ip".into() });
            }
            other => panic!("expected HitAssert, got {other:?}"),
        }
        match &stmts[4] {
            ExpectStmt::HitAssert { assert, .. } => {
                assert_eq!(
                    *assert,
                    HitAssert::EntityId {
                        value: "10.0.0.8".into()
                    }
                );
            }
            other => panic!("expected HitAssert, got {other:?}"),
        }
        match &stmts[5] {
            ExpectStmt::HitAssert { assert, .. } => {
                assert_eq!(
                    *assert,
                    HitAssert::Field {
                        name: "domain".into(),
                        cmp: CmpOp::Eq,
                        value: Expr::StringLit("evil.test".into()),
                    }
                );
            }
            other => panic!("expected HitAssert, got {other:?}"),
        }
    }

    #[test]
    fn expect_block_other_cmp_and_float_score() {
        // 其它 cmp 与数值 field 值
        let stmts =
            expect_of("expect { hits <= 2; hit[1].score > 1.5; hit[2].field(\"amount\") >= 10; }");
        match &stmts[0] {
            ExpectStmt::Hits { cmp, count } => {
                assert_eq!(*cmp, CmpOp::Le);
                assert_eq!(*count, 2);
            }
            other => panic!("expected Hits, got {other:?}"),
        }
        match &stmts[1] {
            ExpectStmt::HitAssert { index, assert } => {
                assert_eq!(*index, 1);
                assert_eq!(
                    *assert,
                    HitAssert::Score {
                        cmp: CmpOp::Gt,
                        value: 1.5
                    }
                );
            }
            other => panic!("expected HitAssert, got {other:?}"),
        }
        match &stmts[2] {
            ExpectStmt::HitAssert { index, assert } => {
                assert_eq!(*index, 2);
                assert_eq!(
                    *assert,
                    HitAssert::Field {
                        name: "amount".into(),
                        cmp: CmpOp::Ge,
                        value: Expr::Number(10.0),
                    }
                );
            }
            other => panic!("expected HitAssert, got {other:?}"),
        }
    }

    #[test]
    fn expect_block_rejects_bad_syntax() {
        expect_err("expect { hits == 1.5; }"); // hits 必须整数
        expect_err("expect { hit[x].score == 1; }"); // 下标必须整数
        expect_err("expect { hits >= 1 }"); // 缺 `;`
        expect_err("expect { hit[0].score == 1;"); // 缺 `}`
        expect_err("expect { hit[0].bogus == 1; }"); // 未知断言关键字
    }
}
