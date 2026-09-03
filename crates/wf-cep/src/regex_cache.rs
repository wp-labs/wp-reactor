//! Per-thread compiled-regex cache for the interpreted eval paths.
//!
//! `regex_match`'s second argument is a compile-time-checked string literal
//! (see `check_funcs.rs`), so the same pattern is compiled repeatedly per
//! event — `regex::Regex::new` is ~20µs/ev of pure waste on the fallback
//! (non-columnar) path. The columnar path precompiles once per batch at
//! `compile_expr` time; this cache gives the interpreted fallback the same
//! property without cross-thread locking: a tiny per-thread map keyed by the
//! pattern string, evicted wholesale when it outgrows a fixed cap (pattern
//! counts are rule-scale, so a cap is a safety valve, not a working limit).

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

thread_local! {
    static REGEX_CACHE: RefCell<HashMap<String, Arc<regex::Regex>>> =
        RefCell::new(HashMap::new());
}

/// Maximum compiled patterns cached per thread. Evicting the whole map on
/// overflow keeps this O(1) and bounds memory; rule-scale pattern counts never
/// approach it.
const MAX_ENTRIES: usize = 512;

/// Get the compiled regex for `pat`, compiling and caching on first use.
/// Invalid patterns return `None` (mirroring the previous per-event
/// `Regex::new(&pat).ok()?` behavior).
pub fn cached_regex(pat: &str) -> Option<Arc<regex::Regex>> {
    REGEX_CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        if let Some(re) = cache.get(pat) {
            return Some(re.clone());
        }
        let re = Arc::new(regex::Regex::new(pat).ok()?);
        if cache.len() >= MAX_ENTRIES {
            cache.clear();
        }
        cache.insert(pat.to_string(), re.clone());
        Some(re)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caches_by_pattern_and_reuses_arc() {
        let a = cached_regex("fail.*").unwrap();
        let b = cached_regex("fail.*").unwrap();
        assert!(Arc::ptr_eq(&a, &b), "同一 pattern 应复用同一编译结果");
        assert!(a.is_match("failed_login"));
        assert!(!a.is_match("success"));

        // 不同 pattern 互不影响。
        let c = cached_regex("^\\d+$").unwrap();
        assert!(!Arc::ptr_eq(&a, &c));
        assert!(c.is_match("123"));
    }

    #[test]
    fn invalid_pattern_returns_none() {
        assert!(cached_regex("[").is_none());
        assert!(cached_regex("").is_some()); // 空 pattern 合法（匹配任意串）。
    }
}
