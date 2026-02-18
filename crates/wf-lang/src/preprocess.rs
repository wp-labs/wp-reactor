//! Variable preprocessing for WFL source text.
//!
//! Performs `$VAR` / `${VAR:default}` text substitution as a preprocessing
//! step before WFL parsing (compilation pipeline step 1).

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
        write!(f, "preprocess error at position {}: {}", self.position, self.message)
    }
}

impl std::error::Error for PreprocessError {}

/// Perform variable substitution on WFL source text.
///
/// Scans `source` left-to-right and replaces variable references:
/// - `$IDENT` — replaced by value from `vars`; error if undefined.
/// - `${IDENT}` — same as `$IDENT` (braces for disambiguation).
/// - `${IDENT:default}` — replaced by value if defined, otherwise by `default`.
///
/// IDENT matches `[A-Za-z_][A-Za-z0-9_]*`.
///
/// A bare `$` not followed by IDENT or `{` is left as-is.
/// An unterminated `${...` (missing `}`) is an error.
pub fn preprocess_vars(
    source: &str,
    vars: &HashMap<String, String>,
) -> Result<String, PreprocessError> {
    let bytes = source.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut i = 0;

    while i < len {
        if bytes[i] == b'$' {
            let dollar_pos = i;
            i += 1;

            if i < len && bytes[i] == b'{' {
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
                        message: format!("unterminated variable reference '${{{}' — missing '}}'", ident),
                    });
                }
                i += 1; // skip '}'

                // Resolve
                if let Some(val) = vars.get(ident) {
                    out.push_str(val);
                } else if let Some(def) = default_val {
                    out.push_str(def);
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
                } else {
                    return Err(PreprocessError {
                        position: dollar_pos,
                        message: format!("undefined variable '{}'", ident),
                    });
                }
            } else {
                // Bare '$' not followed by IDENT or '{' — leave as-is
                out.push('$');
            }
        } else {
            out.push(source[i..].chars().next().unwrap());
            i += source[i..].chars().next().unwrap().len_utf8();
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
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
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
        assert!(err.message.contains("UNDEF"), "error should mention var name: {}", err.message);
    }

    #[test]
    fn undefined_braced_var_error() {
        let v = HashMap::new();
        let err = preprocess_vars("${UNDEF}", &v).unwrap_err();
        assert!(err.message.contains("UNDEF"), "error should mention var name: {}", err.message);
    }

    #[test]
    fn unterminated_brace_error() {
        let v = vars(&[("VAR", "x")]);
        let err = preprocess_vars("${VAR", &v).unwrap_err();
        assert!(err.message.contains("unterminated"), "error should indicate unterminated: {}", err.message);
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

    // --- Integration: preprocess then parse ---

    #[test]
    fn preprocess_then_parse() {
        use crate::parse_wfl;

        let v = vars(&[("THRESHOLD", "3")]);
        let source = r#"
use "security.ws"

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
        assert!(processed.contains("count >= 3"), "variable should be substituted");
        let file = parse_wfl(&processed).unwrap();
        assert_eq!(file.rules.len(), 1);
    }
}
