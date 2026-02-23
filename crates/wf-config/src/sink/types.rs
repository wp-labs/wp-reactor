use serde::Deserialize;

pub use wp_connector_api::ParamMap;

// ---------------------------------------------------------------------------
// StringOrArray — deserialize from a single string or a list of strings
// ---------------------------------------------------------------------------

/// Deserializes from either a single TOML string or an array of strings.
///
/// ```toml
/// windows = "security_*"        # single
/// windows = ["sec_*", "auth_*"] # array
/// ```
#[derive(Debug, Clone)]
pub struct StringOrArray(pub Vec<String>);

impl<'de> Deserialize<'de> for StringOrArray {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StringOrArray;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a string or array of strings")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<StringOrArray, E> {
                Ok(StringOrArray(vec![v.to_string()]))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<StringOrArray, A::Error> {
                let mut v = Vec::new();
                while let Some(s) = seq.next_element::<String>()? {
                    v.push(s);
                }
                Ok(StringOrArray(v))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

// ---------------------------------------------------------------------------
// WildArray — compiled wildcard pattern matcher
// ---------------------------------------------------------------------------

/// A set of wildcard patterns compiled from string globs.
///
/// Matches against yield-target window names during sink routing.
#[derive(Debug)]
pub struct WildArray {
    patterns: Vec<String>,
    compiled: Vec<wildmatch::WildMatch>,
}

impl WildArray {
    /// Compile from raw pattern strings.
    pub fn new(patterns: &[String]) -> Self {
        let compiled = patterns.iter().map(|p| wildmatch::WildMatch::new(p)).collect();
        Self {
            patterns: patterns.to_vec(),
            compiled,
        }
    }

    /// Returns `true` if any pattern matches the given name.
    pub fn matches(&self, name: &str) -> bool {
        self.compiled.iter().any(|p| p.matches(name))
    }

    /// Returns `true` if no patterns are present.
    pub fn is_empty(&self) -> bool {
        self.compiled.is_empty()
    }

    /// Returns the raw pattern strings (for rebuilding in another context).
    pub fn raw_patterns(&self) -> &[String] {
        &self.patterns
    }
}

impl From<&StringOrArray> for WildArray {
    fn from(s: &StringOrArray) -> Self {
        Self::new(&s.0)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildarray_single_pattern() {
        let wa = WildArray::new(&["security_*".into()]);
        assert!(wa.matches("security_alerts"));
        assert!(wa.matches("security_"));
        assert!(!wa.matches("network_alerts"));
    }

    #[test]
    fn wildarray_multiple_patterns() {
        let wa = WildArray::new(&["sec_*".into(), "auth_*".into()]);
        assert!(wa.matches("sec_events"));
        assert!(wa.matches("auth_logs"));
        assert!(!wa.matches("net_events"));
    }

    #[test]
    fn wildarray_star_matches_all() {
        let wa = WildArray::new(&["*".into()]);
        assert!(wa.matches("anything"));
        assert!(wa.matches(""));
    }

    #[test]
    fn wildarray_no_match() {
        let wa = WildArray::new(&["specific".into()]);
        assert!(wa.matches("specific"));
        assert!(!wa.matches("other"));
    }

    #[test]
    fn wildarray_empty() {
        let wa = WildArray::new(&[]);
        assert!(wa.is_empty());
        assert!(!wa.matches("anything"));
    }

    #[test]
    fn string_or_array_single() {
        #[derive(Deserialize)]
        struct W { v: StringOrArray }
        let w: W = toml::from_str(r#"v = "hello""#).unwrap();
        assert_eq!(w.v.0, vec!["hello"]);
    }

    #[test]
    fn string_or_array_list() {
        #[derive(Deserialize)]
        struct W { v: StringOrArray }
        let w: W = toml::from_str(r#"v = ["a", "b"]"#).unwrap();
        assert_eq!(w.v.0, vec!["a", "b"]);
    }

    impl TryFrom<toml::Value> for StringOrArray {
        type Error = toml::de::Error;
        fn try_from(v: toml::Value) -> Result<Self, Self::Error> {
            Self::deserialize(v)
        }
    }
}
