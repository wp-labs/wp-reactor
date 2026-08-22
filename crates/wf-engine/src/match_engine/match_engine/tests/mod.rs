//! Unit tests for the `match_engine::match_engine` module internals.
//!
//! Lives inside the module so tests can reach the private submodules
//! (key/state/step/close/conv/seq/limits) directly.

mod coverage_extra;
mod coverage_more;

use super::*;

fn make_event(fields: Vec<(&str, Value)>) -> Event {
    Event {
        fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    }
}

#[test]
fn extract_event_str() {
    let e = make_event(vec![
        ("sip", Value::Str("10.0.0.1".into())),
        ("n", Value::Number(5.0)),
        ("flag", Value::Bool(true)),
    ]);
    assert_eq!(CepStateMachine::extract_event_str(&e, "sip"), "10.0.0.1");
    let empty = make_event(vec![]);
    assert_eq!(CepStateMachine::extract_event_str(&empty, "any"), "");
}
