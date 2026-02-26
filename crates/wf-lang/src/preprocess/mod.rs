//! Variable preprocessing for WFL source text.
//!
//! Performs `$VAR` / `${VAR:default}` text substitution as a preprocessing
//! step before WFL parsing (compilation pipeline step 1).
//!
//! Context-aware: `$` references inside line comments (`// ...`) and `"..."`
//! string literals are passed through verbatim. Use `$$` to produce a literal
//! `$` in code positions.

use std::collections::HashMap;
use std::fmt;

#[cfg(test)]
mod tests;

/// Error during variable preprocessing.
#[derive(Debug, Clone)]
pub struct PreprocessError {
    /// Byte offset in the original source where the error occurred.
    pub position: usize,
    /// Human-readable description of the error.
    pub message: String,
}

impl fmt::Display for PreprocessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "preprocess error at position {}: {}",
            self.position, self.message
        )
    }
}

impl std::error::Error for PreprocessError {}

/// Perform variable substitution on WFL source text.
///
/// Scans `source` left-to-right and replaces variable references:
/// - `$IDENT` — replaced by value from `vars`; error if undefined.
/// - `${IDENT}` — same as `$IDENT` (braces for disambiguation).
/// - `${IDENT:default}` — replaced by value if defined, otherwise by `default`.
/// - `$$` — produces a literal `$`.
///
/// IDENT matches `[A-Za-z_][A-Za-z0-9_]*`.
///
/// Variable references inside `// ...` line comments and `"..."` string
/// literals are **not** processed — the text is copied verbatim.
/// Note: `#` is reserved for future annotation syntax and is not treated as a comment.
///
/// A bare `$` not followed by IDENT, `{`, or `$` is left as-is.
/// An unterminated `${...` (missing `}`) is an error.
pub fn preprocess_vars(
    source: &str,
    vars: &HashMap<String, String>,
) -> Result<String, PreprocessError> {
    preprocess_impl(source, vars, false)
}

/// Like [`preprocess_vars`], but falls back to environment variables for
/// any variable not found in `vars`. Useful for project tools (explain,
/// lint) where variables may come from the shell environment rather than
/// a config file.
pub fn preprocess_vars_with_env(
    source: &str,
    vars: &HashMap<String, String>,
) -> Result<String, PreprocessError> {
    preprocess_impl(source, vars, true)
}

fn preprocess_impl(
    source: &str,
    vars: &HashMap<String, String>,
    env_fallback: bool,
) -> Result<String, PreprocessError> {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut i = 0;

    while i < len {
        match bytes[i] {
            // --- Line comment: pass through until newline ---
            b'/' if i + 1 < len && bytes[i + 1] == b'/' => {
                while i < len && bytes[i] != b'\n' {
                    out.push(bytes[i] as char);
                    i += 1;
                }
            }

            // --- String literal: pass through including $ ---
            b'"' => {
                out.push('"');
                i += 1;
                while i < len && bytes[i] != b'"' {
                    out.push(source[i..].chars().next().unwrap());
                    i += source[i..].chars().next().unwrap().len_utf8();
                }
                if i < len {
                    out.push('"');
                    i += 1;
                }
            }

            // --- Dollar: variable reference or escape ---
            b'$' => {
                let dollar_pos = i;
                i += 1;

                if i < len && bytes[i] == b'$' {
                    // $$ → literal $
                    out.push('$');
                    i += 1;
                } else if i < len && bytes[i] == b'{' {
                    // Braced form: ${IDENT} or ${IDENT:default}
                    i += 1; // skip '{'
                    let ident_start = i;

                    // Read IDENT
                    if i >= len || !is_ident_start(bytes[i]) {
                        return Err(PreprocessError {
                            position: dollar_pos,
                            message: "expected variable name after ${".to_string(),
                        });
                    }
                    while i < len && is_ident_cont(bytes[i]) {
                        i += 1;
                    }
                    let ident = &source[ident_start..i];

                    // Check for ':default' or '}'
                    let default_val = if i < len && bytes[i] == b':' {
                        i += 1; // skip ':'
                        let default_start = i;
                        while i < len && bytes[i] != b'}' {
                            i += 1;
                        }
                        Some(&source[default_start..i])
                    } else {
                        None
                    };

                    // Expect closing '}'
                    if i >= len || bytes[i] != b'}' {
                        return Err(PreprocessError {
                            position: dollar_pos,
                            message: format!(
                                "unterminated variable reference '${{{}' — missing '}}'",
                                ident
                            ),
                        });
                    }
                    i += 1; // skip '}'

                    // Resolve
                    if let Some(val) = vars.get(ident) {
                        out.push_str(val);
                    } else if let Some(def) = default_val {
                        out.push_str(def);
                    } else if env_fallback {
                        if let Ok(val) = std::env::var(ident) {
                            out.push_str(&val);
                        } else {
                            return Err(PreprocessError {
                                position: dollar_pos,
                                message: format!(
                                    "undefined variable '{}' (not in --var or environment)",
                                    ident
                                ),
                            });
                        }
                    } else {
                        return Err(PreprocessError {
                            position: dollar_pos,
                            message: format!("undefined variable '{}'", ident),
                        });
                    }
                } else if i < len && is_ident_start(bytes[i]) {
                    // Bare form: $IDENT
                    let ident_start = i;
                    while i < len && is_ident_cont(bytes[i]) {
                        i += 1;
                    }
                    let ident = &source[ident_start..i];

                    if let Some(val) = vars.get(ident) {
                        out.push_str(val);
                    } else if env_fallback {
                        if let Ok(val) = std::env::var(ident) {
                            out.push_str(&val);
                        } else {
                            return Err(PreprocessError {
                                position: dollar_pos,
                                message: format!(
                                    "undefined variable '{}' (not in --var or environment)",
                                    ident
                                ),
                            });
                        }
                    } else {
                        return Err(PreprocessError {
                            position: dollar_pos,
                            message: format!("undefined variable '{}'", ident),
                        });
                    }
                } else {
                    // Bare '$' not followed by IDENT, '{', or '$' — leave as-is
                    out.push('$');
                }
            }

            // --- Normal character ---
            _ => {
                // --- Pattern block: skip verbatim (avoid ${param} conflict) ---
                if bytes[i] == b'p' && try_skip_pattern_block(source, &mut i, &mut out) {
                    continue;
                }

                let ch = source[i..].chars().next().unwrap();
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }

    Ok(out)
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_cont(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Detect a `pattern name(...) { ... }` block at position `i`.
///
/// If confirmed, copies the entire block (including the closing `}`) verbatim
/// into `out`, advances `*pos` past the block, and returns `true`.
/// If the text at `*pos` doesn't look like a pattern declaration, returns
/// `false` without modifying `*pos` or `out`.
fn try_skip_pattern_block(source: &str, pos: &mut usize, out: &mut String) -> bool {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let start = *pos;

    // Must start with "pattern" keyword and be at a word boundary.
    let kw = b"pattern";
    if start + kw.len() > len {
        return false;
    }
    if &bytes[start..start + kw.len()] != kw {
        return false;
    }
    // Ensure it's not a prefix of a longer identifier.
    let after_kw = start + kw.len();
    if after_kw < len && is_ident_cont(bytes[after_kw]) {
        return false;
    }
    // Ensure it's not in the middle of a longer identifier (check char before).
    if start > 0 && is_ident_cont(bytes[start - 1]) {
        return false;
    }

    // Look ahead: skip whitespace, expect ident, skip whitespace, expect '('
    let mut j = after_kw;
    // skip whitespace
    while j < len && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    // expect ident
    if j >= len || !is_ident_start(bytes[j]) {
        return false;
    }
    while j < len && is_ident_cont(bytes[j]) {
        j += 1;
    }
    // skip whitespace
    while j < len && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    // expect '('
    if j >= len || bytes[j] != b'(' {
        return false;
    }

    // Confirmed: this is a pattern block. Now copy everything verbatim
    // until we find the opening '{' of the body, then copy the balanced body.

    // First, copy up to and including the opening '{' of the body.
    // Skip past the '(' params ')' section.
    let mut k = j + 1; // past '('
    let mut paren_depth = 1;
    while k < len && paren_depth > 0 {
        match bytes[k] {
            b'(' => paren_depth += 1,
            b')' => paren_depth -= 1,
            _ => {}
        }
        k += 1;
    }
    // Skip whitespace to find '{'
    while k < len && bytes[k].is_ascii_whitespace() {
        k += 1;
    }
    if k >= len || bytes[k] != b'{' {
        return false;
    }
    k += 1; // past '{'

    // Now copy verbatim while tracking brace depth inside the body.
    let mut brace_depth = 1;
    while k < len && brace_depth > 0 {
        match bytes[k] {
            b'{' => brace_depth += 1,
            b'}' => brace_depth -= 1,
            b'"' => {
                // Skip string literal inside body.
                k += 1;
                while k < len && bytes[k] != b'"' {
                    k += 1;
                }
                // k now points at closing '"' or end; the loop increment below handles it.
            }
            b'/' if k + 1 < len && bytes[k + 1] == b'/' => {
                // Skip // comment inside body.
                while k < len && bytes[k] != b'\n' {
                    k += 1;
                }
                // k now points at '\n' or end; the loop increment below handles it.
                continue; // don't double-advance
            }
            _ => {}
        }
        k += 1;
    }

    // Copy the entire block verbatim [start..k).
    out.push_str(&source[start..k]);
    *pos = k;
    true
}
