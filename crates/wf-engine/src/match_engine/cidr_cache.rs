//! Per-thread parsed-CIDR cache for the interpreted eval paths.
//!
//! Symmetric to [`super::regex_cache`]: `cidr_match`'s second argument is a
//! compile-time-checked string literal, so the same subnet is parsed
//! repeatedly per event on the fallback (non-columnar) path. The columnar
//! path parses once per batch at `compile_expr` time; this cache gives the
//! interpreted fallback the same property with no cross-thread locking.
//! `Cidr` is `Copy`, so the map stores values directly.

use std::cell::RefCell;
use std::collections::HashMap;

use wf_lang::cidr::Cidr;

thread_local! {
    static CIDR_CACHE: RefCell<HashMap<String, Cidr>> = RefCell::new(HashMap::new());
}

/// Maximum cached subnets per thread. Evicting the whole map on overflow keeps
/// this O(1); rule-scale subnet counts never approach it.
const MAX_ENTRIES: usize = 512;

/// Get the parsed subnet for `cidr`, parsing and caching on first use.
/// Invalid subnets return `None` (mirroring the previous per-event
/// `Cidr::parse(&cidr)?` behavior).
pub(crate) fn cached_cidr(cidr: &str) -> Option<Cidr> {
    CIDR_CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        if let Some(net) = cache.get(cidr) {
            return Some(*net);
        }
        let net = Cidr::parse(cidr)?;
        if cache.len() >= MAX_ENTRIES {
            cache.clear();
        }
        cache.insert(cidr.to_string(), net);
        Some(net)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caches_by_subnet_and_reuses_value() {
        let a = cached_cidr("10.0.0.0/8").unwrap();
        let b = cached_cidr("10.0.0.0/8").unwrap();
        assert_eq!(a, b);
        assert!(a.contains("10.1.2.3"));

        let c = cached_cidr("fe80::/10").unwrap();
        assert_ne!(a, c);
        assert!(c.contains("fe80::1"));
    }

    #[test]
    fn invalid_subnet_returns_none() {
        assert!(cached_cidr("10.0.0.0").is_none());
        assert!(cached_cidr("10.0.0.0/33").is_none());
    }
}
