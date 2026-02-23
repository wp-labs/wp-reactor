//! Variable preprocessing for WFL source text.
//!
//! Performs `$VAR` / `${VAR:default}` text substitution as a preprocessing
//! step before WFL parsing (compilation pipeline step 1).
//!
//! Context-aware: `$` references inside `# ...` comments and `"..."` string
//! literals are passed through verbatim. Use `$$` to produce a literal `$`
//! in code positions.

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
/// Variable references inside `# ...` line comments and `"..."` string
/// literals are **not** processed — the text is copied verbatim.
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
            b'#' => {
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
