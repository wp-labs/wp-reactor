use std::time::Duration;

use winnow::combinator::{alt, cut_err, opt};
use winnow::error::{AddContext, StrContext, StrContextValue};
use winnow::prelude::*;
use winnow::token::literal;

use crate::parse_utils::{ident, number_literal};

use crate::wfg_ast::*;

use super::primitives::{percent, rate, ws_skip};

pub(super) fn scenario_attrs(input: &mut &str) -> ModalResult<Vec<ScenarioAttr>> {
    ws_skip(input)?;
    cut_err(literal("#["))
        .context(StrContext::Expected(StrContextValue::Description(
            "scenario annotation '#['",
        )))
        .parse_next(input)?;
    let attrs = parse_attr_list(input, "]")?;
    cut_err(literal("]"))
        .context(StrContext::Expected(StrContextValue::Description(
            "closing ']' for scenario annotation",
        )))
        .parse_next(input)?;
    Ok(attrs)
}

pub(super) fn inline_annos(input: &mut &str) -> ModalResult<Vec<ScenarioAttr>> {
    ws_skip(input)?;
    cut_err(literal("<"))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening '<' for inline annotations",
        )))
        .parse_next(input)?;
    let attrs = parse_attr_list(input, ">")?;
    cut_err(literal(">"))
        .context(StrContext::Expected(StrContextValue::Description(
            "closing '>' for inline annotations",
        )))
        .parse_next(input)?;
    Ok(attrs)
}

fn parse_attr_list(input: &mut &str, end_delim: &str) -> ModalResult<Vec<ScenarioAttr>> {
    let mut attrs = Vec::new();
    ws_skip(input)?;
    if input.starts_with(end_delim) {
        return Ok(attrs);
    }

    attrs.push(parse_attr(input)?);
    loop {
        ws_skip(input)?;
        if opt(literal(",")).parse_next(input)?.is_some() {
            ws_skip(input)?;
            attrs.push(parse_attr(input)?);
        } else {
            break;
        }
    }
    Ok(attrs)
}

fn parse_attr(input: &mut &str) -> ModalResult<ScenarioAttr> {
    let key = ident(input)?.to_string();
    ws_skip(input)?;
    cut_err(literal("="))
        .context(StrContext::Expected(StrContextValue::Description(
            "'=' in annotation",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    let value = parse_attr_value(input)?;
    Ok(ScenarioAttr { key, value })
}

fn parse_attr_value(input: &mut &str) -> ModalResult<AttrValue> {
    if let Some(s) = opt(crate::parse_utils::quoted_string).parse_next(input)? {
        return Ok(AttrValue::String(s));
    }

    // Duration is parsed before bare number to avoid consuming `10m` as `10`.
    let duration_saved = *input;
    if let Ok(d) = crate::parse_utils::duration_value.parse_next(input) {
        return Ok(AttrValue::Duration(d));
    }
    *input = duration_saved;

    let number_saved = *input;
    if let Ok(n) = number_literal.parse_next(input) {
        return Ok(AttrValue::Number(n));
    }
    *input = number_saved;

    let word = ident(input)?.to_string();
    match word.as_str() {
        "true" => Ok(AttrValue::Bool(true)),
        "false" => Ok(AttrValue::Bool(false)),
        _ => Ok(AttrValue::String(word)),
    }
}

pub(super) fn parse_syntax_body(
    input: &mut &str,
    name: String,
    attrs: Vec<ScenarioAttr>,
    inline_annos: Vec<ScenarioAttr>,
) -> ModalResult<(ScenarioDecl, SyntaxScenario)> {
    ws_skip(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening brace for scenario body",
        )))
        .parse_next(input)?;

    let mut traffic: Option<TrafficBlock> = None;
    let mut injection: Option<SyntaxInjectionBlock> = None;
    let mut expect: Option<ExpectBlock> = None;

    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }

        if opt(crate::parse_utils::kw("traffic"))
            .parse_next(input)?
            .is_some()
        {
            traffic = Some(parse_traffic_block(input)?);
            continue;
        }
        if opt(crate::parse_utils::kw("injection"))
            .parse_next(input)?
            .is_some()
        {
            injection = Some(parse_injection_block(input)?);
            continue;
        }
        if opt(crate::parse_utils::kw("expect"))
            .parse_next(input)?
            .is_some()
        {
            expect = Some(parse_expect_block(input)?);
            continue;
        }

        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new().add_context(
                input,
                &input.checkpoint(),
                StrContext::Expected(StrContextValue::Description(
                    "traffic, injection, expect, or closing brace",
                )),
            ),
        ));
    }

    let Some(traffic) = traffic else {
        return Err(winnow::error::ErrMode::Cut(
            winnow::error::ContextError::new().add_context(
                input,
                &input.checkpoint(),
                StrContext::Expected(StrContextValue::Description("traffic block")),
            ),
        ));
    };

    let seed = extract_seed(&inline_annos).unwrap_or(0);
    let duration = extract_duration(&attrs).unwrap_or_else(|| Duration::from_secs(60));
    let total = derive_total(&traffic, duration);
    let streams = derive_legacy_streams(&traffic);

    let scenario = ScenarioDecl {
        name,
        seed,
        time_clause: TimeClause {
            start: "2026-01-01T00:00:00Z".to_string(),
            duration,
        },
        total,
        streams,
        injects: Vec::new(),
        faults: None,
        oracle: None,
    };

    let syntax = SyntaxScenario {
        attrs,
        inline_annos,
        traffic,
        injection,
        expect,
    };

    Ok((scenario, syntax))
}

fn parse_traffic_block(input: &mut &str) -> ModalResult<TrafficBlock> {
    ws_skip(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening brace for traffic block",
        )))
        .parse_next(input)?;

    let mut streams = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        cut_err(crate::parse_utils::kw("stream"))
            .context(StrContext::Expected(StrContextValue::Description(
                "'stream' in traffic block",
            )))
            .parse_next(input)?;
        ws_skip(input)?;
        let stream = cut_err(ident)
            .context(StrContext::Expected(StrContextValue::Description(
                "stream name",
            )))
            .parse_next(input)?
            .to_string();
        ws_skip(input)?;
        cut_err(crate::parse_utils::kw("gen"))
            .context(StrContext::Expected(StrContextValue::Description(
                "'gen' keyword",
            )))
            .parse_next(input)?;
        ws_skip(input)?;
        let rate_expr = cut_err(parse_rate_expr)
            .context(StrContext::Expected(StrContextValue::Description(
                "rate expression",
            )))
            .parse_next(input)?;
        ws_skip(input)?;
        let _ = opt(literal(";")).parse_next(input)?;

        streams.push(SyntaxStreamDecl {
            stream,
            rate: rate_expr,
        });
    }

    Ok(TrafficBlock { streams })
}

fn parse_rate_expr(input: &mut &str) -> ModalResult<RateExpr> {
    if opt(crate::parse_utils::kw("wave"))
        .parse_next(input)?
        .is_some()
    {
        return parse_wave(input);
    }
    if opt(crate::parse_utils::kw("burst"))
        .parse_next(input)?
        .is_some()
    {
        return parse_burst(input);
    }
    if opt(crate::parse_utils::kw("timeline"))
        .parse_next(input)?
        .is_some()
    {
        return parse_timeline(input);
    }
    Ok(RateExpr::Constant(rate(input)?))
}

fn parse_wave(input: &mut &str) -> ModalResult<RateExpr> {
    ws_skip(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip(input)?;
    let base = parse_named_rate(input, "base")?;
    comma(input)?;
    let amp = parse_named_rate(input, "amp")?;
    comma(input)?;
    let period = parse_named_duration(input, "period")?;
    let shape = parse_wave_shape(input)?;
    ws_skip(input)?;
    cut_err(literal(")")).parse_next(input)?;

    Ok(RateExpr::Wave {
        base,
        amp,
        period,
        shape,
    })
}

/// `,` 字段分隔符（前后允许空白）。
fn comma(input: &mut &str) -> ModalResult<()> {
    ws_skip(input)?;
    cut_err(literal(",")).parse_next(input)?;
    ws_skip(input)
}

/// `keyword =` 字段头。
fn kw_equals(input: &mut &str, keyword: &'static str) -> ModalResult<()> {
    cut_err(crate::parse_utils::kw(keyword)).parse_next(input)?;
    cut_err(literal("=")).parse_next(input)?;
    Ok(())
}

/// `name = <rate>` 字段：供 wave/burst 复用。
fn parse_named_rate(input: &mut &str, name: &'static str) -> ModalResult<Rate> {
    kw_equals(input, name)?;
    cut_err(rate).parse_next(input)
}

/// `name = <duration>` 字段。
fn parse_named_duration(input: &mut &str, name: &'static str) -> ModalResult<Duration> {
    kw_equals(input, name)?;
    cut_err(crate::parse_utils::duration_value).parse_next(input)
}

/// wave 的可选 `, shape = sine|triangle|square`（缺省 Sine）。
fn parse_wave_shape(input: &mut &str) -> ModalResult<WaveShape> {
    let mut shape = WaveShape::Sine;
    ws_skip(input)?;
    if opt(literal(",")).parse_next(input)?.is_some() {
        ws_skip(input)?;
        kw_equals(input, "shape")?;
        shape = cut_err(alt((
            crate::parse_utils::kw("sine").value(WaveShape::Sine),
            crate::parse_utils::kw("triangle").value(WaveShape::Triangle),
            crate::parse_utils::kw("square").value(WaveShape::Square),
        )))
        .parse_next(input)?;
    }
    Ok(shape)
}

fn parse_burst(input: &mut &str) -> ModalResult<RateExpr> {
    ws_skip(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip(input)?;
    let base = parse_named_rate(input, "base")?;
    comma(input)?;
    let peak = parse_named_rate(input, "peak")?;
    comma(input)?;
    let every = parse_named_duration(input, "every")?;
    comma(input)?;
    let hold = parse_named_duration(input, "hold")?;
    ws_skip(input)?;
    cut_err(literal(")")).parse_next(input)?;

    Ok(RateExpr::Burst {
        base,
        peak,
        every,
        hold,
    })
}

fn parse_timeline(input: &mut &str) -> ModalResult<RateExpr> {
    ws_skip(input)?;
    cut_err(literal("{")).parse_next(input)?;
    let mut segments = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        let start = cut_err(crate::parse_utils::duration_value).parse_next(input)?;
        ws_skip(input)?;
        cut_err(literal("..")).parse_next(input)?;
        ws_skip(input)?;
        let end = cut_err(crate::parse_utils::duration_value).parse_next(input)?;
        ws_skip(input)?;
        cut_err(literal("=")).parse_next(input)?;
        ws_skip(input)?;
        let seg_rate = cut_err(rate).parse_next(input)?;
        ws_skip(input)?;
        let _ = opt(literal(";")).parse_next(input)?;
        segments.push(TimelineSegment {
            start,
            end,
            rate: seg_rate,
        });
    }
    Ok(RateExpr::Timeline(segments))
}

fn parse_injection_block(input: &mut &str) -> ModalResult<SyntaxInjectionBlock> {
    ws_skip(input)?;
    cut_err(literal("{"))
        .context(StrContext::Expected(StrContextValue::Description(
            "opening brace for injection block",
        )))
        .parse_next(input)?;
    let mut cases = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        cases.push(parse_injection_case(input)?);
    }
    Ok(SyntaxInjectionBlock { cases })
}

fn parse_injection_case(input: &mut &str) -> ModalResult<SyntaxInjectCase> {
    let mode = alt((
        crate::parse_utils::kw("hit").value(InjectCaseMode::Hit),
        crate::parse_utils::kw("near_miss").value(InjectCaseMode::NearMiss),
        crate::parse_utils::kw("miss").value(InjectCaseMode::Miss),
    ))
    .context(StrContext::Expected(StrContextValue::Description(
        "injection mode (hit, near_miss, miss)",
    )))
    .parse_next(input)?;
    ws_skip(input)?;
    cut_err(literal("<")).parse_next(input)?;
    let pct = cut_err(percent).parse_next(input)?;
    cut_err(literal(">")).parse_next(input)?;
    ws_skip(input)?;
    let target_rule = if opt(crate::parse_utils::kw("for"))
        .parse_next(input)?
        .is_some()
    {
        ws_skip(input)?;
        Some(
            cut_err(ident)
                .context(StrContext::Expected(StrContextValue::Description(
                    "target rule name in injection case",
                )))
                .parse_next(input)?
                .to_string(),
        )
    } else {
        None
    };
    ws_skip(input)?;
    let stream = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "stream name in injection case",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip(input)?;
    cut_err(literal("{")).parse_next(input)?;
    ws_skip(input)?;
    let seq = cut_err(parse_seq_block).parse_next(input)?;
    ws_skip(input)?;
    cut_err(literal("}")).parse_next(input)?;
    Ok(SyntaxInjectCase {
        mode,
        percent: pct,
        target_rule,
        stream,
        seq,
    })
}

fn parse_seq_block(input: &mut &str) -> ModalResult<SeqBlock> {
    let entity = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "entity key for seq",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip(input)?;
    cut_err(crate::parse_utils::kw("seq"))
        .context(StrContext::Expected(StrContextValue::Description(
            "'seq' keyword",
        )))
        .parse_next(input)?;
    ws_skip(input)?;
    cut_err(literal("{")).parse_next(input)?;
    let mut steps = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        steps.push(parse_seq_step(input)?);
    }
    Ok(SeqBlock { entity, steps })
}

fn parse_seq_step(input: &mut &str) -> ModalResult<SeqStep> {
    if opt(crate::parse_utils::kw("then"))
        .parse_next(input)?
        .is_some()
    {
        ws_skip(input)?;
        cut_err(crate::parse_utils::kw("use"))
            .context(StrContext::Expected(StrContextValue::Description(
                "'use' after 'then'",
            )))
            .parse_next(input)?;
        return parse_use_step_after_keyword(input);
    }

    if opt(crate::parse_utils::kw("use"))
        .parse_next(input)?
        .is_some()
    {
        return parse_use_step_after_keyword(input);
    }

    if opt(crate::parse_utils::kw("not"))
        .parse_next(input)?
        .is_some()
    {
        return parse_not_step(input);
    }

    Err(winnow::error::ErrMode::Cut(
        winnow::error::ContextError::new().add_context(
            input,
            &input.checkpoint(),
            StrContext::Expected(StrContextValue::Description(
                "use(...) or not(...) seq step",
            )),
        ),
    ))
}

/// `not (predicates) within (duration)` 序列步骤。
fn parse_not_step(input: &mut &str) -> ModalResult<SeqStep> {
    ws_skip(input)?;
    cut_err(literal("(")).parse_next(input)?;
    let predicates = parse_predicates(input)?;
    cut_err(literal(")")).parse_next(input)?;
    ws_skip(input)?;
    cut_err(crate::parse_utils::kw("within")).parse_next(input)?;
    ws_skip(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip(input)?;
    let within = cut_err(crate::parse_utils::duration_value).parse_next(input)?;
    ws_skip(input)?;
    cut_err(literal(")")).parse_next(input)?;
    ws_skip(input)?;
    let _ = opt(literal(";")).parse_next(input)?;
    Ok(SeqStep::Not { predicates, within })
}

fn parse_use_step_after_keyword(input: &mut &str) -> ModalResult<SeqStep> {
    ws_skip(input)?;
    cut_err(literal("(")).parse_next(input)?;
    let predicates = parse_predicates(input)?;
    cut_err(literal(")")).parse_next(input)?;
    ws_skip(input)?;
    cut_err(crate::parse_utils::kw("with")).parse_next(input)?;
    ws_skip(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip(input)?;
    let count = cut_err(crate::parse_utils::nonneg_integer).parse_next(input)? as u64;
    ws_skip(input)?;
    cut_err(literal(")")).parse_next(input)?;
    ws_skip(input)?;
    let _ = opt(literal(";")).parse_next(input)?;
    Ok(SeqStep::Use { predicates, count })
}

fn parse_predicates(input: &mut &str) -> ModalResult<Vec<FieldPredicate>> {
    let mut predicates = Vec::new();
    predicates.push(parse_predicate(input)?);
    loop {
        ws_skip(input)?;
        if opt(literal(",")).parse_next(input)?.is_some() {
            ws_skip(input)?;
            predicates.push(parse_predicate(input)?);
        } else {
            break;
        }
    }
    Ok(predicates)
}

fn parse_predicate(input: &mut &str) -> ModalResult<FieldPredicate> {
    let field = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "predicate field",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip(input)?;
    cut_err(literal("=")).parse_next(input)?;
    ws_skip(input)?;
    let value = parse_attr_value(input)?;
    Ok(FieldPredicate { field, value })
}

fn parse_expect_block(input: &mut &str) -> ModalResult<ExpectBlock> {
    ws_skip(input)?;
    cut_err(literal("{")).parse_next(input)?;
    let mut checks = Vec::new();
    loop {
        ws_skip(input)?;
        if opt(literal("}")).parse_next(input)?.is_some() {
            break;
        }
        checks.push(parse_expect_stmt(input)?);
    }
    Ok(ExpectBlock { checks })
}

fn parse_expect_stmt(input: &mut &str) -> ModalResult<ExpectCheck> {
    let metric = alt((
        crate::parse_utils::kw("hit").value(ExpectMetric::Hit),
        crate::parse_utils::kw("near_miss").value(ExpectMetric::NearMiss),
        crate::parse_utils::kw("miss").value(ExpectMetric::Miss),
        crate::parse_utils::kw("precision").value(ExpectMetric::Precision),
        crate::parse_utils::kw("recall").value(ExpectMetric::Recall),
        crate::parse_utils::kw("fpr").value(ExpectMetric::Fpr),
        crate::parse_utils::kw("latency_p95").value(ExpectMetric::LatencyP95),
    ))
    .parse_next(input)?;
    ws_skip(input)?;
    cut_err(literal("(")).parse_next(input)?;
    ws_skip(input)?;
    let rule = cut_err(ident)
        .context(StrContext::Expected(StrContextValue::Description(
            "rule name in expect expression",
        )))
        .parse_next(input)?
        .to_string();
    ws_skip(input)?;
    cut_err(literal(")")).parse_next(input)?;
    ws_skip(input)?;
    let op = parse_compare_op(input)?;
    ws_skip(input)?;
    let value = parse_expect_value(input)?;
    ws_skip(input)?;
    let _ = opt(literal(";")).parse_next(input)?;
    Ok(ExpectCheck {
        metric,
        rule,
        op,
        value,
    })
}

fn parse_compare_op(input: &mut &str) -> ModalResult<CompareOp> {
    alt((
        literal(">=").value(CompareOp::Gte),
        literal("<=").value(CompareOp::Lte),
        literal("==").value(CompareOp::Eq),
        literal(">").value(CompareOp::Gt),
        literal("<").value(CompareOp::Lt),
    ))
    .parse_next(input)
}

fn parse_expect_value(input: &mut &str) -> ModalResult<ExpectValue> {
    let percent_saved = *input;
    if let Ok(v) = percent.parse_next(input) {
        return Ok(ExpectValue::Percent(v));
    }
    *input = percent_saved;

    let duration_saved = *input;
    if let Ok(d) = crate::parse_utils::duration_value.parse_next(input) {
        return Ok(ExpectValue::Duration(d));
    }
    *input = duration_saved;

    let n = number_literal(input)?;
    Ok(ExpectValue::Number(n))
}

fn extract_seed(inline_annos: &[ScenarioAttr]) -> Option<u64> {
    inline_annos
        .iter()
        .find(|a| a.key == "seed")
        .and_then(|a| match a.value {
            AttrValue::Number(n) if n >= 0.0 => Some(n as u64),
            _ => None,
        })
}

fn extract_duration(attrs: &[ScenarioAttr]) -> Option<Duration> {
    attrs
        .iter()
        .find(|a| a.key == "duration")
        .and_then(|a| match a.value {
            AttrValue::Duration(d) => Some(d),
            _ => None,
        })
}

fn derive_legacy_streams(traffic: &TrafficBlock) -> Vec<StreamBlock> {
    traffic
        .streams
        .iter()
        .map(|s| StreamBlock {
            alias: s.stream.clone(),
            window: s.stream.clone(),
            rate: rate_from_expr(&s.rate),
            overrides: Vec::new(),
        })
        .collect()
}

fn rate_from_expr(rate_expr: &RateExpr) -> Rate {
    match rate_expr {
        RateExpr::Constant(r) => r.clone(),
        RateExpr::Wave { base, .. } => base.clone(),
        RateExpr::Burst { base, .. } => base.clone(),
        RateExpr::Timeline(segments) => segments.first().map(|s| s.rate.clone()).unwrap_or(Rate {
            count: 1,
            unit: RateUnit::PerSecond,
        }),
    }
}

fn derive_total(traffic: &TrafficBlock, duration: Duration) -> u64 {
    let eps_sum: f64 = traffic.streams.iter().map(|s| s.rate.approx_eps()).sum();
    if eps_sum <= 0.0 {
        return 1;
    }
    let total = (eps_sum * duration.as_secs_f64()).round() as u64;
    total.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rate(count: u64, unit: RateUnit) -> Rate {
        Rate { count, unit }
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn wave_parses_fields_and_optional_shape() {
        let mut input = "( base=80/s, amp=20/s, period=2m, shape=triangle )";
        let r = parse_wave(&mut input).expect("wave with shape");
        assert_eq!(
            r,
            RateExpr::Wave {
                base: rate(80, RateUnit::PerSecond),
                amp: rate(20, RateUnit::PerSecond),
                period: secs(120),
                shape: WaveShape::Triangle,
            }
        );
        // 缺省 shape → Sine
        let mut input2 = "(base=1/s, amp=1/s, period=5s)";
        let r2 = parse_wave(&mut input2).expect("wave default shape");
        assert_eq!(
            r2,
            RateExpr::Wave {
                base: rate(1, RateUnit::PerSecond),
                amp: rate(1, RateUnit::PerSecond),
                period: secs(5),
                shape: WaveShape::Sine,
            }
        );
        // 非法 shape → 解析失败
        let mut bad = "(base=1/s, amp=1/s, period=5s, shape=zigzag)";
        assert!(parse_wave(&mut bad).is_err());
        // 缺必填字段 → 解析失败
        let mut missing = "(base=1/s, amp=1/s)";
        assert!(parse_wave(&mut missing).is_err());
    }

    #[test]
    fn burst_parses_four_fields() {
        let mut input = "(base=40/s, peak=300/s, every=3m, hold=20s)";
        let r = parse_burst(&mut input).expect("burst");
        assert_eq!(
            r,
            RateExpr::Burst {
                base: rate(40, RateUnit::PerSecond),
                peak: rate(300, RateUnit::PerSecond),
                every: secs(180),
                hold: secs(20),
            }
        );
    }

    #[test]
    fn timeline_parses_segments_and_derives_base_rate() {
        let mut input = "{ 0m..2m=20/s; 2m..4m=60/s }";
        let r = parse_timeline(&mut input).expect("timeline");
        let RateExpr::Timeline(segments) = &r else {
            panic!("expected timeline");
        };
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].start, secs(0));
        assert_eq!(segments[0].end, secs(120));
        assert_eq!(segments[0].rate, rate(20, RateUnit::PerSecond));
        assert_eq!(segments[1].rate, rate(60, RateUnit::PerSecond));
        // legacy 路径取首段 rate；空 timeline → 兜底 1/s
        assert_eq!(rate_from_expr(&r), rate(20, RateUnit::PerSecond));
        assert_eq!(
            rate_from_expr(&RateExpr::Timeline(Vec::new())),
            rate(1, RateUnit::PerSecond)
        );
    }

    #[test]
    fn seq_steps_use_and_not() {
        let mut use_input = "use(a=\"1\") with(2)";
        let use_step = parse_seq_step(&mut use_input).expect("use step");
        assert_eq!(
            use_step,
            SeqStep::Use {
                predicates: vec![FieldPredicate {
                    field: "a".to_string(),
                    value: AttrValue::String("1".to_string()),
                }],
                count: 2,
            }
        );
        // `then use(...)` 前缀与 `not(...) within(...)`
        let mut then_input = "then use(b=\"x\") with(1)";
        assert!(matches!(
            parse_seq_step(&mut then_input).expect("then use"),
            SeqStep::Use { .. }
        ));
        let mut not_input = "not(x=1, y=2) within(5s)";
        let not_step = parse_seq_step(&mut not_input).expect("not step");
        match not_step {
            SeqStep::Not { predicates, within } => {
                assert_eq!(predicates.len(), 2);
                assert_eq!(within, secs(5));
            }
            other => panic!("unexpected step {other:?}"),
        }
        // 未知步骤头 → Cut 错误
        let mut bad = "whenever(1)";
        assert!(parse_seq_step(&mut bad).is_err());
    }

    #[test]
    fn injection_case_modes_and_target_rule() {
        let mut hit = "hit<30%> auth_events { k seq { use(a=\"1\") with(2) } }";
        let case = parse_injection_case(&mut hit).expect("hit case");
        assert_eq!(case.mode, InjectCaseMode::Hit);
        assert_eq!(case.percent, 30.0);
        assert_eq!(case.target_rule, None);
        assert_eq!(case.stream, "auth_events");
        assert_eq!(case.seq.entity, "k");
        assert_eq!(case.seq.steps.len(), 1);

        let mut miss = "miss<10%> for guard_rule evs { k seq { then use(a=\"b\") with(1) } }";
        let case2 = parse_injection_case(&mut miss).expect("miss case");
        assert_eq!(case2.mode, InjectCaseMode::Miss);
        assert_eq!(case2.target_rule.as_deref(), Some("guard_rule"));
        assert_eq!(case2.stream, "evs");
    }
}
