use orion_error::conversion::ToStructError;

use crate::vars::{SourceAtom, TracedValue, VarsReason, VarsResult};

#[derive(Debug, Clone, Copy, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Config", module = "Config.ConfigVars")]
enum TemplatePart<'a> {
    Literal(&'a str),
    Var {
        ident: &'a str,
        default: Option<&'a str>,
    },
}

pub(crate) fn expand_template<F>(input: &str, mut resolve_ident: F) -> VarsResult<String>
where
    F: FnMut(&str) -> VarsResult<Option<String>>,
{
    let mut out = String::with_capacity(input.len());
    for part in parse_template_parts(input)? {
        match part {
            TemplatePart::Literal(fragment) => out.push_str(fragment),
            TemplatePart::Var { ident, default } => {
                if let Some(value) = resolve_ident(ident)? {
                    out.push_str(&value);
                } else if let Some(default) = default {
                    out.push_str(default);
                } else {
                    return VarsReason::Template
                        .to_err()
                        .with_detail(format!(
                            "undefined variable '{}' in configuration value {:?}",
                            ident, input
                        ))
                        .err();
                }
            }
        }
    }
    Ok(out)
}

pub(crate) fn expand_template_with_trace<F>(
    input: &str,
    mut resolve_ident: F,
    literal_source: Option<SourceAtom>,
) -> VarsResult<TracedValue>
where
    F: FnMut(&str) -> VarsResult<Option<TracedValue>>,
{
    let mut traced = TracedValue::new(String::with_capacity(input.len()));
    let mut used_literal = false;

    for part in parse_template_parts(input)? {
        match part {
            TemplatePart::Literal(fragment) => {
                traced.value.push_str(fragment);
                used_literal = true;
            }
            TemplatePart::Var { ident, default } => {
                if let Some(value) = resolve_ident(ident)? {
                    traced.value.push_str(&value.value);
                    traced.sources.extend(value.sources);
                } else if let Some(default) = default {
                    traced.value.push_str(default);
                    traced
                        .sources
                        .insert(SourceAtom::Default(ident.to_string()));
                } else {
                    return VarsReason::Template
                        .to_err()
                        .with_detail(format!(
                            "undefined variable '{}' in configuration value {:?}",
                            ident, input
                        ))
                        .err();
                }
            }
        }
    }

    if used_literal && let Some(source) = literal_source {
        traced.sources.insert(source);
    }

    Ok(traced)
}

fn parse_template_parts(input: &str) -> VarsResult<Vec<TemplatePart<'_>>> {
    let bytes = input.as_bytes();
    let len = bytes.len();
    let mut parts = Vec::new();
    let mut literal_start = 0usize;
    let mut i = 0usize;

    while i < len {
        if bytes[i] != b'$' {
            let ch = input[i..].chars().next().unwrap();
            i += ch.len_utf8();
            continue;
        }

        if literal_start < i {
            parts.push(TemplatePart::Literal(&input[literal_start..i]));
        }

        let dollar_pos = i;
        i += 1;

        if i < len && bytes[i] == b'$' {
            parts.push(TemplatePart::Literal("$"));
            i += 1;
            literal_start = i;
            continue;
        }

        if i < len && bytes[i] == b'{' {
            i += 1;
            let ident_start = i;
            if i >= len || !is_ident_start(bytes[i]) {
                return VarsReason::Template
                    .to_err()
                    .with_detail(format!("expected variable name after '${{' in {:?}", input))
                    .err();
            }
            while i < len && is_ident_cont(bytes[i]) {
                i += 1;
            }
            let ident = &input[ident_start..i];

            let default = if i < len && bytes[i] == b':' {
                i += 1;
                let default_start = i;
                while i < len && bytes[i] != b'}' {
                    i += 1;
                }
                Some(&input[default_start..i])
            } else {
                None
            };

            if i >= len || bytes[i] != b'}' {
                return VarsReason::Template
                    .to_err()
                    .with_detail(format!(
                        "unterminated variable reference starting at byte {} in {:?}",
                        dollar_pos, input
                    ))
                    .err();
            }
            i += 1;
            parts.push(TemplatePart::Var { ident, default });
            literal_start = i;
            continue;
        }

        if i < len && is_ident_start(bytes[i]) {
            let ident_start = i;
            while i < len && is_ident_cont(bytes[i]) {
                i += 1;
            }
            let ident = &input[ident_start..i];
            parts.push(TemplatePart::Var {
                ident,
                default: None,
            });
            literal_start = i;
            continue;
        }

        parts.push(TemplatePart::Literal("$"));
        literal_start = i;
    }

    if literal_start < len {
        parts.push(TemplatePart::Literal(&input[literal_start..]));
    } else if input.is_empty() {
        parts.push(TemplatePart::Literal(""));
    }

    Ok(parts)
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}
