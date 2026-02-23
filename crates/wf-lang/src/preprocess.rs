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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn simple_var_substitution() {
        let v = vars(&[("THRESHOLD", "5")]);
        let result = preprocess_vars("count >= $THRESHOLD", &v).unwrap();
        assert_eq!(result, "count >= 5");
    }

    #[test]
    fn braced_var_substitution() {
        let v = vars(&[("VAR", "hello")]);
        let result = preprocess_vars("${VAR}_suffix", &v).unwrap();
        assert_eq!(result, "hello_suffix");
    }

    #[test]
    fn default_value_used() {
        let v = HashMap::new();
        let result = preprocess_vars("count >= ${VAR:10}", &v).unwrap();
        assert_eq!(result, "count >= 10");
    }

    #[test]
    fn default_value_ignored_when_defined() {
        let v = vars(&[("VAR", "42")]);
        let result = preprocess_vars("count >= ${VAR:10}", &v).unwrap();
        assert_eq!(result, "count >= 42");
    }

    #[test]
    fn undefined_var_error() {
        let v = HashMap::new();
        let err = preprocess_vars("$UNDEF", &v).unwrap_err();
        assert!(
            err.message.contains("UNDEF"),
            "error should mention var name: {}",
            err.message
        );
    }

    #[test]
    fn undefined_braced_var_error() {
        let v = HashMap::new();
        let err = preprocess_vars("${UNDEF}", &v).unwrap_err();
        assert!(
            err.message.contains("UNDEF"),
            "error should mention var name: {}",
            err.message
        );
    }

    #[test]
    fn unterminated_brace_error() {
        let v = vars(&[("VAR", "x")]);
        let err = preprocess_vars("${VAR", &v).unwrap_err();
        assert!(
            err.message.contains("unterminated"),
            "error should indicate unterminated: {}",
            err.message
        );
    }

    #[test]
    fn multiple_vars() {
        let v = vars(&[("A", "1"), ("B", "2")]);
        let result = preprocess_vars("$A + $B = 3", &v).unwrap();
        assert_eq!(result, "1 + 2 = 3");
    }

    #[test]
    fn no_vars_passthrough() {
        let v = HashMap::new();
        let result = preprocess_vars("no variables here", &v).unwrap();
        assert_eq!(result, "no variables here");
    }

    // --- P2: context-aware skipping ---

    #[test]
    fn dollar_in_comment_ignored() {
        let v = HashMap::new();
        let result = preprocess_vars("code # $HOME is fine\nnext", &v).unwrap();
        assert_eq!(result, "code # $HOME is fine\nnext");
    }

    #[test]
    fn dollar_in_string_ignored() {
        let v = HashMap::new();
        let result = preprocess_vars(r#"msg = "$HOME/path""#, &v).unwrap();
        assert_eq!(result, r#"msg = "$HOME/path""#);
    }

    #[test]
    fn dollar_dollar_escape() {
        let v = HashMap::new();
        let result = preprocess_vars("price is $$5", &v).unwrap();
        assert_eq!(result, "price is $5");
    }

    #[test]
    fn dollar_dollar_before_ident() {
        let v = vars(&[("X", "val")]);
        // $$ should produce literal $, not try to resolve $$X
        let result = preprocess_vars("$$X", &v).unwrap();
        assert_eq!(result, "$X");
    }

    #[test]
    fn mixed_comment_and_var() {
        let v = vars(&[("THRESHOLD", "3")]);
        let result =
            preprocess_vars("count >= $THRESHOLD # compare against $THRESHOLD\n", &v).unwrap();
        assert_eq!(result, "count >= 3 # compare against $THRESHOLD\n");
    }

    #[test]
    fn string_then_var() {
        let v = vars(&[("N", "5")]);
        let result = preprocess_vars(r#"action == "failed" && count >= $N"#, &v).unwrap();
        assert_eq!(result, r#"action == "failed" && count >= 5"#);
    }

    #[test]
    fn dollar_in_fmt_string_ignored() {
        let v = HashMap::new();
        let result =
            preprocess_vars(r#"message = fmt("$USER failed {} times", fail.sip)"#, &v).unwrap();
        assert_eq!(
            result,
            r#"message = fmt("$USER failed {} times", fail.sip)"#
        );
    }

    // --- Environment variable fallback ---

    #[test]
    fn env_fallback_reads_env_var() {
        let v = HashMap::new();
        unsafe { std::env::set_var("WFL_TEST_ENV_VAR_42", "99") };
        let result = preprocess_vars_with_env("count >= $WFL_TEST_ENV_VAR_42", &v).unwrap();
        assert_eq!(result, "count >= 99");
        unsafe { std::env::remove_var("WFL_TEST_ENV_VAR_42") };
    }

    #[test]
    fn env_fallback_explicit_var_takes_priority() {
        let v = vars(&[("WFL_TEST_ENV_VAR_43", "1")]);
        unsafe { std::env::set_var("WFL_TEST_ENV_VAR_43", "2") };
        let result = preprocess_vars_with_env("$WFL_TEST_ENV_VAR_43", &v).unwrap();
        assert_eq!(result, "1"); // --var wins over env
        unsafe { std::env::remove_var("WFL_TEST_ENV_VAR_43") };
    }

    #[test]
    fn env_fallback_undefined_still_errors() {
        let v = HashMap::new();
        let err = preprocess_vars_with_env("$WFL_TEST_CERTAINLY_UNDEFINED_XYZ", &v).unwrap_err();
        assert!(err.message.contains("WFL_TEST_CERTAINLY_UNDEFINED_XYZ"));
    }

    #[test]
    fn env_fallback_braced_form() {
        let v = HashMap::new();
        unsafe { std::env::set_var("WFL_TEST_ENV_VAR_44", "hello") };
        let result = preprocess_vars_with_env("${WFL_TEST_ENV_VAR_44}_suffix", &v).unwrap();
        assert_eq!(result, "hello_suffix");
        unsafe { std::env::remove_var("WFL_TEST_ENV_VAR_44") };
    }

    // --- Integration: preprocess then parse ---

    #[test]
    fn preprocess_then_parse() {
        use crate::parse_wfl;

        let v = vars(&[("THRESHOLD", "3")]);
        let source = r#"
use "security.wfs"

rule brute_force {
    events {
        fail : auth_events && action == "failed"
    }
    match<sip:5m> {
        on event {
            fail | count >= $THRESHOLD;
        }
    } -> score(70.0)
    entity(ip, fail.sip)
    yield security_alerts (
        sip = fail.sip,
        fail_count = count(fail),
        message = fmt("{} failed {} times", fail.sip, count(fail))
    )
}
"#;
        let processed = preprocess_vars(source, &v).unwrap();
        assert!(
            processed.contains("count >= 3"),
            "variable should be substituted"
        );
        let file = parse_wfl(&processed).unwrap();
        assert_eq!(file.rules.len(), 1);
    }
}
