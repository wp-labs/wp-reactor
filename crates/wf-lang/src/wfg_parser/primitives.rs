use winnow::ascii::multispace0;
use winnow::combinator::opt;
use winnow::prelude::*;
use winnow::token::{literal, take_while};

use crate::wfg_ast::{Rate, RateUnit};

// ---------------------------------------------------------------------------
// Whitespace & comments (// style for .wfg)
// ---------------------------------------------------------------------------

/// Skip whitespace and `// ...` line comments.
pub(crate) fn ws_skip(input: &mut &str) -> ModalResult<()> {
    loop {
        let _ = multispace0.parse_next(input)?;
        if opt(literal("//")).parse_next(input)?.is_none() {
            return Ok(());
        }
        let _ = take_while(0.., |c: char| c != '\n').parse_next(input)?;
    }
}

// ---------------------------------------------------------------------------
// Rate: NUMBER "/" ("s"|"m"|"h")
// ---------------------------------------------------------------------------

pub(crate) fn rate(input: &mut &str) -> ModalResult<Rate> {
    let num = crate::parse_utils::number_literal(input)?;
    let count = num as u64;
    literal("/").parse_next(input)?;
    let unit = winnow::combinator::alt((
        literal("s").value(RateUnit::PerSecond),
        literal("m").value(RateUnit::PerMinute),
        literal("h").value(RateUnit::PerHour),
    ))
    .parse_next(input)?;
    Ok(Rate { count, unit })
}

// ---------------------------------------------------------------------------
// Percent: NUMBER "%"
// ---------------------------------------------------------------------------

pub(crate) fn percent(input: &mut &str) -> ModalResult<f64> {
    let num = crate::parse_utils::number_literal(input)?;
    literal("%").parse_next(input)?;
    Ok(num)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_skip_consumes_spaces_and_line_comments() {
        let mut input = "  // note\n\t// second\nabc";
        ws_skip(&mut input).expect("skip");
        assert_eq!(input, "abc");
        // 纯空白（含换行）
        let mut plain = " \n\t x";
        ws_skip(&mut plain).expect("skip");
        assert_eq!(plain, "x");
        // 文件尾注释（无换行）
        let mut eof = "// only comment";
        ws_skip(&mut eof).expect("skip");
        assert_eq!(eof, "");
        // 非空白不动
        let mut rest = "payload";
        ws_skip(&mut rest).expect("skip");
        assert_eq!(rest, "payload");
    }

    #[test]
    fn rate_parses_units_and_rejects_bad_suffix() {
        let mut per_s = "5/s";
        assert_eq!(
            rate(&mut per_s).expect("per second"),
            Rate {
                count: 5,
                unit: RateUnit::PerSecond
            }
        );
        let mut per_m = "10/m";
        assert_eq!(
            rate(&mut per_m).expect("per minute"),
            Rate {
                count: 10,
                unit: RateUnit::PerMinute
            }
        );
        let mut per_h = "3600/h";
        assert_eq!(
            rate(&mut per_h).expect("per hour"),
            Rate {
                count: 3600,
                unit: RateUnit::PerHour
            }
        );
        // 缺单位 / 未知单位 → 报错；带尾随内容则只消费前缀
        let mut no_unit = "5";
        assert!(rate(&mut no_unit).is_err());
        let mut bad_unit = "5/x";
        assert!(rate(&mut bad_unit).is_err());
        let mut trailing = "5/s rest";
        rate(&mut trailing).expect("prefix rate");
        assert_eq!(trailing, " rest");
    }

    #[test]
    fn percent_parses_number_and_percent_sign() {
        let mut p = "30%";
        assert_eq!(percent(&mut p).expect("30%"), 30.0);
        let mut frac = "2.5%";
        assert_eq!(percent(&mut frac).expect("2.5%"), 2.5);
        let mut missing = "30";
        assert!(percent(&mut missing).is_err());
    }
}
