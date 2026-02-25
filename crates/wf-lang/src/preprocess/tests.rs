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
    let result = preprocess_vars("count >= $THRESHOLD # compare against $THRESHOLD\n", &v).unwrap();
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

// --- Pattern block skipping ---

#[test]
fn pattern_body_not_substituted() {
    // ${key} inside pattern body must NOT be treated as a preprocessor variable.
    // Only the outer $key should be substituted.
    let v = vars(&[("key", "sip")]);
    let source = r#"
pattern burst(alias, key, win, threshold) {
    match<${key}:${win}> {
        on event { ${alias} | count >= ${threshold}; }
    } -> score(50.0)
}

rule r {
    events { e : $key }
    match<sip:5m> { on event { e | count >= 1; } } -> score(1)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let processed = preprocess_vars(source, &v).unwrap();
    // The ${key} inside the pattern body should still be ${key}
    assert!(
        processed.contains("${key}"),
        "pattern body should not have ${{key}} substituted: {}",
        processed
    );
    // The $key outside the pattern body should be substituted
    assert!(
        processed.contains("events { e : sip }"),
        "outer $key should be substituted: {}",
        processed
    );
}

#[test]
fn pattern_body_with_nested_braces() {
    let v = vars(&[("THRESHOLD", "10")]);
    let source = r#"
pattern nested(a, b) {
    match<${a}:5m> {
        on event {
            ${b} | count >= 1;
        }
    } -> score(50.0)
}

rule r {
    events { e : win }
    match<sip:5m> { on event { e | count >= $THRESHOLD; } } -> score(1)
    entity(ip, e.sip)
    yield out (x = e.sip)
}
"#;
    let processed = preprocess_vars(source, &v).unwrap();
    // The ${a} and ${b} inside the pattern body should be preserved
    assert!(
        processed.contains("${a}"),
        "nested pattern body should not substitute ${{a}}: {}",
        processed
    );
    assert!(
        processed.contains("${b}"),
        "nested pattern body should not substitute ${{b}}: {}",
        processed
    );
    // The $THRESHOLD in the rule body should be substituted
    assert!(
        processed.contains("count >= 10"),
        "outer $THRESHOLD should be substituted: {}",
        processed
    );
}
