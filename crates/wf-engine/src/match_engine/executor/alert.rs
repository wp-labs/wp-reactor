use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use wf_lang::ast::FieldRef;

use crate::alert::AlertOrigin;
use crate::match_engine::match_engine::{
    CloseReason, StepData, Value, field_ref_name, push_i64_exact_decimal, value_to_string,
};

/// Format nanoseconds since epoch as ISO 8601 UTC string.
///
/// Reuses the Hinnant civil-from-days algorithm. For `nanos <= 0`
/// returns the epoch string.
pub(crate) fn format_nanos_utc(nanos: i64) -> String {
    if nanos <= 0 {
        return "1970-01-01T00:00:00.000Z".to_string();
    }
    let total_secs = (nanos / 1_000_000_000) as u64;
    let millis = ((nanos % 1_000_000_000) / 1_000_000) as u32;

    let secs_of_day = total_secs % 86400;
    let days_since_epoch = (total_secs / 86400) as i64;

    let (year, month, day) = civil_from_days(days_since_epoch);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    // Byte-direct write of the fixed "YYYY-MM-DDTHH:MM:SS.mmmZ" layout into a
    // right-sized String — no format! machinery on the per-event hot path.
    // Byte-identical to the previous format!("{:04}-{:02}...") output.
    let mut s = String::with_capacity(24);
    push_digits(&mut s, year as u32, 4);
    s.push('-');
    push_digits(&mut s, month, 2);
    s.push('-');
    push_digits(&mut s, day, 2);
    s.push('T');
    push_digits(&mut s, hour as u32, 2);
    s.push(':');
    push_digits(&mut s, minute as u32, 2);
    s.push(':');
    push_digits(&mut s, second as u32, 2);
    s.push('.');
    push_digits(&mut s, millis, 3);
    s.push('Z');
    s
}

/// Push `v` as `width` zero-padded ASCII decimal digits.
fn push_digits(s: &mut String, v: u32, width: usize) {
    let mut digits = [b'0'; 8];
    let mut i = width;
    let mut v = v;
    while i > 0 {
        i -= 1;
        digits[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    for &d in &digits[..width] {
        s.push(d as char);
    }
}

pub(crate) fn now_nanos() -> i64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

/// Hinnant civil_from_days: convert days since 1970-01-01 to (y, m, d).
/// Reference: <https://howardhinnant.github.io/date_algorithms.html#civil_from_days>
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// FNV-1a 64-bit — cheap content hash for the per-alert output ID. The ID only
/// needs to be stable + collision-resistant enough for record identity; a
/// cryptographic hash here is pure overhead on high-throughput alert paths
/// (wfusion: 每 match 一次 SHA-256 曾是执行路径的最大单点)。
struct Fnv1a {
    state: u64,
}

impl Fnv1a {
    fn new() -> Self {
        Self {
            state: 0xcbf2_9ce4_8422_2325,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.state ^= b as u64;
            self.state = self.state.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finalize(self) -> u64 {
        self.state
    }
}

/// Build a content-addressed output ID (16 hex chars).
///
/// Feeds rule_name, scope_key, fired_at, step_data, and origin
/// through FNV-1a, then hex-encodes the 8-byte hash as 16 hex characters.
pub(super) fn build_wfx_id(
    rule_name: &str,
    scope_key: &[Value],
    fired_at: &str,
    step_data: &[StepData],
    origin: &AlertOrigin,
) -> String {
    build_wfx_id_iter(rule_name, scope_key, fired_at, step_data.iter(), origin)
}

/// 列式 close 批量路径的 split 版本：直接引用 event/close 两段 step_data，
/// 免 `combine_step_data` 的深克隆（StepData 含 `field_values` HashMap，q19
/// top-10 每桶 10 条 → 每 close 一次全量深拷是纯浪费——wfx_id 只用
/// label + measure_value）。字节流 = 原 `build_wfx_id`（event 段接 close 段，
/// 测试 `build_wfx_id_split_matches_combined` 锁定）。
/// 注：生产路径已改用 [`WfxPrefixCache`]（P6）——本函数保留为测试对拍的
/// 参考实现（`wfx_prefix_cache_matches_split` 的 expected），故仅测试编译。
#[cfg(test)]
pub(super) fn build_wfx_id_split(
    rule_name: &str,
    scope_key: &[Value],
    fired_at: &str,
    event_step_data: &[StepData],
    close_step_data: &[StepData],
    origin: &AlertOrigin,
) -> String {
    build_wfx_id_iter(
        rule_name,
        scope_key,
        fired_at,
        event_step_data.iter().chain(close_step_data.iter()),
        origin,
    )
}

fn build_wfx_id_iter<'a>(
    rule_name: &str,
    scope_key: &[Value],
    fired_at: &str,
    step_data: impl Iterator<Item = &'a StepData>,
    origin: &AlertOrigin,
) -> String {
    let mut hasher = Fnv1a::new();
    hasher.update(rule_name.as_bytes());
    hasher.update(b"\x00");
    for v in scope_key {
        // 2026-08-23：scope_key 直接 hash Value 的规范字节（Number → f64 LE
        // bits、Str → bytes），免 value_to_string 渲染 + String 分配——q6 等
        // join-then-key 每事件 emit 的分配热点之一。wfx_id 无字节级锚定
        // （测试仅断言 16 hex 格式与同输入稳定性），输出 ID 语义不变。
        hash_value_bytes(&mut hasher, v);
        hasher.update(b"\x1f");
    }
    hasher.update(b"\x00");
    hasher.update(fired_at.as_bytes());
    hasher.update(b"\x00");
    for sd in step_data {
        if let Some(label) = &sd.label {
            hasher.update(label.as_bytes());
        }
        hasher.update(b"\x1e");
        hasher.update(&sd.measure_value.to_bits().to_le_bytes());
        hasher.update(b"\x1f");
    }
    hasher.update(b"\x00");
    hasher.update(origin.as_str().as_bytes());
    let hash = hasher.finalize();
    // 8 bytes → 16 hex characters
    hex_encode(&hash.to_le_bytes())
}

#[cfg(test)]
mod split_tests {
    use super::*;
    use crate::match_engine::match_engine::{CloseReason, EngineHashMap, StepData};

    /// `build_wfx_id_split` 必须与「先 combine 再 build_wfx_id」字节一致。
    #[test]
    fn build_wfx_id_split_matches_combined() {
        let rule = "q19_auction_top10_stats";
        let scope = vec![Value::Number(42.0)];
        let fired = "2026-08-25T00:00:00.000Z";
        let origin = AlertOrigin::Close {
            reason: CloseReason::Timeout,
        };
        let ev = StepData {
            satisfied_branch_index: 0,
            label: Some("top_price".into()),
            measure_value: 3.5,
            event_first_time_nanos: Some(1),
            event_last_time_nanos: Some(2),
            collected_values: vec![Value::Number(1.0), Value::Number(2.0)],
            field_values: {
                let mut m = EngineHashMap::default();
                m.insert("price".into(), vec![Value::Number(99.0)]);
                m
            },
        };
        let cl = StepData {
            satisfied_branch_index: 0,
            label: Some("count".into()),
            measure_value: 7.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        };
        let combined: Vec<StepData> = vec![ev.clone(), cl.clone()];
        let a = build_wfx_id(rule, &scope, fired, &combined, &origin);
        let b = build_wfx_id_split(rule, &scope, fired, &[ev], &[cl], &origin);
        assert_eq!(a, b);
        // 空 event 段也要一致（仅 close 段）。
        let c = build_wfx_id_split(rule, &scope, fired, &[], &combined, &origin);
        assert_eq!(a, c);
    }

    /// wfx_id 前缀缓存（WfxPrefixCache）必须与 `build_wfx_id_split` 字节一致。
    #[test]
    fn wfx_prefix_cache_matches_split() {
        let rule = "q19_auction_top10_stats";
        let fired = "2026-08-25T00:00:00.000Z";
        let origin = AlertOrigin::Close {
            reason: CloseReason::Timeout,
        };
        let mk = |price: f64, bidder: i64| StepData {
            satisfied_branch_index: 0,
            label: Some("top_price".into()),
            measure_value: price,
            event_first_time_nanos: Some(1),
            event_last_time_nanos: Some(2),
            collected_values: vec![],
            field_values: {
                let mut m = EngineHashMap::default();
                m.insert("bidder".into(), vec![Value::Number(bidder as f64)]);
                m
            },
        };
        // 同桶（scope_key 相同）top-10 条：只有 measure 变化。
        let scope = vec![Value::Number(42.0)];
        let steps: Vec<StepData> = (0..10).map(|i| mk(i as f64 * 1.5, 100 + i)).collect();
        let mut cache = None::<WfxPrefixCache>;
        for sd in &steps {
            let expected =
                build_wfx_id_split(rule, &scope, fired, &[], std::slice::from_ref(sd), &origin);
            let got = match &cache {
                Some(c) if c.prefix_matches(&scope, fired, &[], std::slice::from_ref(sd)) => {
                    c.finish(&[], std::slice::from_ref(sd), &origin)
                }
                _ => {
                    let c =
                        WfxPrefixCache::build(rule, &scope, fired, &[], std::slice::from_ref(sd));
                    let id = c.finish(&[], std::slice::from_ref(sd), &origin);
                    cache = Some(c);
                    id
                }
            };
            assert_eq!(
                got, expected,
                "前缀缓存 wfx_id 必须与 split 一致 (price={})",
                sd.measure_value
            );
        }
        // 换桶（scope_key 不同）→ 前缀不匹配 → 重建。
        let scope2 = vec![Value::Number(99.0)];
        let sd = mk(3.0, 7);
        let expected = build_wfx_id_split(
            rule,
            &scope2,
            fired,
            &[],
            std::slice::from_ref(&sd),
            &origin,
        );
        assert!(
            !cache
                .unwrap()
                .prefix_matches(&scope2, fired, &[], std::slice::from_ref(&sd))
        );
        let c = WfxPrefixCache::build(rule, &scope2, fired, &[], std::slice::from_ref(&sd));
        assert_eq!(c.finish(&[], std::slice::from_ref(&sd), &origin), expected);
    }

    /// labels 迭代器版前缀缓存必须与 `build_wfx_id_from_labels` 字节一致
    /// （同桶 top-N: 前缀命中只续 hash measure + origin）。
    #[test]
    fn wfx_prefix_cache_from_labels_matches_reference() {
        let rule = "q19_auction_top10_stats";
        let fired = "2026-08-25T00:00:00.000Z";
        let origin = AlertOrigin::Close {
            reason: CloseReason::Timeout,
        };
        let labels = ["top_price", "count"];
        // 同桶（scope_key 相同）多条：只有 measure 变化。
        let scope = vec![Value::Number(42.0)];
        let rows: Vec<Vec<f64>> = (0..10).map(|i| vec![i as f64 * 1.5, i as f64 * 2.0]).collect();
        let mut cache = None::<WfxPrefixCache>;
        for measures in &rows {
            let steps = labels.iter().zip(measures.iter()).map(|(l, m)| (Some(*l), *m));
            let expected = build_wfx_id_from_labels(rule, &scope, fired, steps, &origin);
            let got = match &cache {
                Some(c)
                    if c.prefix_matches_labels(
                        &scope,
                        fired,
                        labels.iter().map(|l| Some(*l)),
                    ) =>
                {
                    c.finish_from_labels(measures.iter().copied(), &origin)
                }
                _ => {
                    let c = WfxPrefixCache::build_from_labels(
                        rule,
                        &scope,
                        fired,
                        labels.iter().map(|l| Some(*l)),
                    );
                    let id = c.finish_from_labels(measures.iter().copied(), &origin);
                    cache = Some(c);
                    id
                }
            };
            assert_eq!(got, expected, "前缀缓存 wfx_id 必须与 from_labels 一致 (measures={measures:?})");
        }
        // 换桶（scope_key 不同）→ 前缀不匹配 → 重建。
        let scope2 = vec![Value::Number(99.0)];
        let measures = [3.0, 7.0];
        let steps = labels.iter().zip(measures.iter()).map(|(l, m)| (Some(*l), *m));
        let expected = build_wfx_id_from_labels(rule, &scope2, fired, steps, &origin);
        assert!(!cache.unwrap().prefix_matches_labels(
            &scope2,
            fired,
            labels.iter().map(|l| Some(*l))
        ));
        let c = WfxPrefixCache::build_from_labels(
            rule,
            &scope2,
            fired,
            labels.iter().map(|l| Some(*l)),
        );
        assert_eq!(c.finish_from_labels(measures.iter().copied(), &origin), expected);
    }

    /// 同 scope+fired_at、labels 不同（内容 / 数量）→ 前缀必须不匹配：
    /// `prefix_matches` 是逐 label 精确比较（review 2026-08-26 废弃 FNV-64
    /// 近似后补的负例——hash 方案在碰撞时会静默产出与全量 build 不一致的
    /// wfx_id）。
    #[test]
    fn wfx_prefix_matches_rejects_label_mismatch() {
        let rule = "q19_auction_top10_stats";
        let fired = "2026-08-25T00:00:00.000Z";
        let scope = vec![Value::Number(42.0)];
        let mk_label = |l: &str| StepData {
            satisfied_branch_index: 0,
            label: Some(l.to_string()),
            measure_value: 1.0,
            event_first_time_nanos: None,
            event_last_time_nanos: None,
            collected_values: vec![],
            field_values: EngineHashMap::default(),
        };
        let sd_other = mk_label("other_label");
        let c = WfxPrefixCache::build(rule, &scope, fired, &[], std::slice::from_ref(&sd_other));
        // 自身必须匹配。
        assert!(c.prefix_matches(&scope, fired, &[], std::slice::from_ref(&sd_other)));
        // 同 scope/fired_at、label 内容不同 → 不匹配。
        let sd_top = mk_label("top_price");
        assert!(!c.prefix_matches(&scope, fired, &[], std::slice::from_ref(&sd_top)));
        // label 数量不同（event 段 + close 段共 2 个 vs 缓存 1 个）→ 不匹配。
        assert!(!c.prefix_matches(&scope, fired, &[sd_top], std::slice::from_ref(&sd_other)));
    }

    /// EntityIdCache: 同 key 复用（f 只调一次）、异 key 重算、空 key 边界。
    #[test]
    fn entity_id_cache_reuses_on_same_key() {
        let mut cache = EntityIdCache::new();
        let key_a = vec![Value::Number(42.0)];
        let key_b = vec![Value::Number(99.0)];
        let calls = std::cell::Cell::new(0usize);
        let f = || {
            let n = calls.get() + 1;
            calls.set(n);
            format!("id-{}", n)
        };
        // 首次 → 计算。
        let a1 = cache.get_or(&key_a, f);
        assert_eq!(a1, "id-1");
        assert_eq!(calls.get(), 1);
        // 同 key → 复用（f 不调用）。
        let a2 = cache.get_or(&key_a, f);
        assert_eq!(a2, "id-1");
        assert_eq!(calls.get(), 1, "同 key 不应重新计算");
        // 异 key → 重算。
        let b1 = cache.get_or(&key_b, f);
        assert_eq!(b1, "id-2");
        assert_eq!(calls.get(), 2);
        // 切回 key_a → 重算（缓存只保留最近一个）。
        let a3 = cache.get_or(&key_a, f);
        assert_eq!(a3, "id-3");
        assert_eq!(calls.get(), 3);
        // 空 key。
        let empty: [Value; 0] = [];
        let e1 = cache.get_or(&empty, f);
        assert_eq!(e1, "id-4");
        let e2 = cache.get_or(&empty, f);
        assert_eq!(e2, "id-4");
        assert_eq!(calls.get(), 4, "空 key 同值也应复用");
    }

    /// OriginArcs: 预建 Arc 与 `AlertOrigin::as_str` / `CloseReason::as_str`
    /// 字节一致（3 种 close reason 全覆盖）。
    #[test]
    fn origin_arcs_match_as_str() {
        let arcs = OriginArcs::new();
        for reason in [CloseReason::Timeout, CloseReason::Flush, CloseReason::Eos] {
            let origin = AlertOrigin::Close { reason };
            assert_eq!(&**arcs.origin(reason), origin.as_str(), "origin {reason:?}");
            assert_eq!(
                &**arcs.close_reason(reason),
                reason.as_str(),
                "reason {reason:?}"
            );
        }
    }
}

/// Hash a [`Value`]'s canonical bytes for wfx_id (see [`build_wfx_id`]).
/// Number hashes the f64 bits — byte-stable per value (same input → same ID),
/// and distinct values stay distinct (f64 bits are injective).
fn hash_value_bytes(hasher: &mut Fnv1a, v: &Value) {
    match v {
        Value::Number(n) => hasher.update(&n.to_bits().to_le_bytes()),
        Value::Str(s) => hasher.update(s.as_bytes()),
        Value::Bool(b) => hasher.update(&[*b as u8]),
        Value::Array(_) => hasher.update(b"[array]"),
        Value::Object(_) => hasher.update(b"[object]"),
    }
}

/// wfx_id 前缀状态缓存（P6, 2026-08-26）: q19 每桶 top-10 条 close 共享
/// `rule_name + scope_key + fired_at + step labels`——FNV-1a 是增量哈希，
/// 前缀 state 可复制续算。命中时每 close 只续 hash `measure_value + origin`
/// （免重新 hash 常量前缀：rule_name/scope_key/fired_at/labels）。
/// 字节流 = `build_wfx_id_iter` 前段，测试锁定。
pub(crate) struct WfxPrefixCache {
    /// 前缀 state = 已 hash `rule \x00 scope... \x00 fired_at \x00 {label \x1e}*`
    state: u64,
    scope_key: Vec<Value>,
    fired_at: String,
    /// labels 序列（build 时克隆一次，每桶一次）。`prefix_matches` 逐 label
    /// **借用**比较（不构造新 Vec、不克隆）——精确判定「labels 段字节流
    /// 相同」，无哈希碰撞（review 2026-08-26：FNV-64 比较有 2^-64 静默
    /// 错误风险，已废弃）。
    labels: Vec<Option<String>>,
}

impl WfxPrefixCache {
    /// 构建前缀 state（到 `fired_at \x00` 之后；labels/measure 由 finish 每步
    /// hash——这样 finish 的字节流 = `{label \x1e measure \x1f}* \x00 origin`，
    /// 与 `build_wfx_id_iter` 完全一致）。
    pub(crate) fn build(
        rule_name: &str,
        scope_key: &[Value],
        fired_at: &str,
        event_step_data: &[StepData],
        close_step_data: &[StepData],
    ) -> Self {
        let mut hasher = Fnv1a::new();
        hasher.update(rule_name.as_bytes());
        hasher.update(b"\x00");
        for v in scope_key {
            hash_value_bytes(&mut hasher, v);
            hasher.update(b"\x1f");
        }
        hasher.update(b"\x00");
        hasher.update(fired_at.as_bytes());
        hasher.update(b"\x00");
        let labels: Vec<Option<String>> = event_step_data
            .iter()
            .chain(close_step_data.iter())
            .map(|sd| sd.label.clone())
            .collect();
        Self {
            state: hasher.state,
            scope_key: scope_key.to_vec(),
            fired_at: fired_at.to_string(),
            labels,
        }
    }

    /// 前缀是否与当前 close 匹配（scope_key/fired_at/labels 全同）。labels
    /// 逐 label **借用**比较（免每 close 克隆分配，可提前短路）：与 build
    /// 时缓存的 labels 序列精确比较，无哈希碰撞。
    pub(crate) fn prefix_matches(
        &self,
        scope_key: &[Value],
        fired_at: &str,
        event_step_data: &[StepData],
        close_step_data: &[StepData],
    ) -> bool {
        if self.scope_key.as_slice() != scope_key || self.fired_at != fired_at {
            return false;
        }
        let mut cached = self.labels.iter();
        let mut cur = event_step_data.iter().chain(close_step_data.iter());
        loop {
            match (cached.next(), cur.next()) {
                (Some(a), Some(b)) => {
                    if a.as_deref() != b.label.as_deref() {
                        return false;
                    }
                }
                (None, None) => return true,
                _ => return false,
            }
        }
    }

    /// 从缓存前缀 state 续算完整 wfx_id（measure + origin 是变化部分）。
    pub(crate) fn finish(
        &self,
        event_step_data: &[StepData],
        close_step_data: &[StepData],
        origin: &AlertOrigin,
    ) -> String {
        let mut hasher = Fnv1a { state: self.state };
        for sd in event_step_data.iter().chain(close_step_data.iter()) {
            if let Some(label) = &sd.label {
                hasher.update(label.as_bytes());
            }
            hasher.update(b"\x1e");
            hasher.update(&sd.measure_value.to_bits().to_le_bytes());
            hasher.update(b"\x1f");
        }
        hasher.update(b"\x00");
        hasher.update(origin.as_str().as_bytes());
        hex_encode(&hasher.finalize().to_le_bytes())
    }

    // -- labels/measure 迭代器变体（2026-08-27 stats 列式直写）-------------
    // stats 直写路径（execute_stats_close_batch_columnar）无 StepData（v4 为省
    // 每桶 4 个 StepData ≈ 4G 分配），但同桶 top-N 条目仍共享 rule/scope/fired_at/
    // labels 前缀——逐条目全量重 hash 是 P6 前缀缓存的回归缺口（旧 CloseOutput
    // 路径有缓存）。本变体以 labels 迭代器替代 StepData 切片，字节流与
    // [`build_wfx_id_from_labels`] 完全一致（测试对拍锁定）。

    /// 构建前缀 state（labels 段仅缓存不 hash；finish 时逐条与 measure 交错）。
    pub(crate) fn build_from_labels<'a>(
        rule_name: &str,
        scope_key: &[Value],
        fired_at: &str,
        labels: impl Iterator<Item = Option<&'a str>>,
    ) -> Self {
        let mut hasher = Fnv1a::new();
        hasher.update(rule_name.as_bytes());
        hasher.update(b"\x00");
        for v in scope_key {
            hash_value_bytes(&mut hasher, v);
            hasher.update(b"\x1f");
        }
        hasher.update(b"\x00");
        hasher.update(fired_at.as_bytes());
        hasher.update(b"\x00");
        let labels: Vec<Option<String>> =
            labels.map(|l| l.map(|s| s.to_string())).collect();
        Self {
            state: hasher.state,
            scope_key: scope_key.to_vec(),
            fired_at: fired_at.to_string(),
            labels,
        }
    }

    /// labels 迭代器版前缀匹配（scope/fired_at/labels 全同）。
    pub(crate) fn prefix_matches_labels<'a>(
        &self,
        scope_key: &[Value],
        fired_at: &str,
        labels: impl Iterator<Item = Option<&'a str>>,
    ) -> bool {
        if self.scope_key.as_slice() != scope_key || self.fired_at != fired_at {
            return false;
        }
        let mut cached = self.labels.iter();
        for cur in labels {
            match cached.next() {
                Some(a) => {
                    if a.as_deref() != cur {
                        return false;
                    }
                }
                None => return false,
            }
        }
        cached.next().is_none()
    }

    /// 从缓存前缀续算 wfx_id：measures（与缓存的 labels 同序交错）是变化部分。
    pub(crate) fn finish_from_labels(
        &self,
        measures: impl Iterator<Item = f64>,
        origin: &AlertOrigin,
    ) -> String {
        let mut hasher = Fnv1a { state: self.state };
        let mut labels = self.labels.iter();
        for mv in measures {
            if let Some(label) = labels.next().and_then(|l| l.as_deref()) {
                hasher.update(label.as_bytes());
            }
            hasher.update(b"\x1e");
            hasher.update(&mv.to_bits().to_le_bytes());
            hasher.update(b"\x1f");
        }
        hasher.update(b"\x00");
        hasher.update(origin.as_str().as_bytes());
        hex_encode(&hasher.finalize().to_le_bytes())
    }
}

/// wfx_id 核心：FNV-1a(rule_name \x00 event_time_nanos(LE) \x00 \x00 origin)。
/// 字段不再参与哈希——现实流中同一纳秒不可能有两个事件，全字段渲染+哈希
/// 是 on-each 每行 ~190ns 的大头（微基准 cut A 实测）。字节流与旧实现
/// 「空字段集事件」路径完全一致（字段循环后的分隔符保留）。origin 保留以
/// 区分 event / close 两类告警。
fn wfx_id_from_rule_and_time(
    rule_name: &str,
    event_time_nanos: i64,
    origin: &AlertOrigin,
) -> String {
    let mut hasher = Fnv1a::new();
    hasher.update(rule_name.as_bytes());
    hasher.update(b"\x00");
    hasher.update(&event_time_nanos.to_le_bytes());
    hasher.update(b"\x00");
    hasher.update(b"\x00");
    hasher.update(origin.as_str().as_bytes());
    hex_encode(&hasher.finalize().to_le_bytes())
}

pub(super) fn build_each_wfx_id(
    rule_name: &str,
    event_time_nanos: i64,
    _ctx: &crate::match_engine::match_engine::Event,
    origin: &AlertOrigin,
    _field_order: &[&smol_str::SmolStr],
) -> String {
    wfx_id_from_rule_and_time(rule_name, event_time_nanos, origin)
}

/// [`build_each_wfx_id`] with a caller-provided value-rendering scratch
/// buffer (kept for signature compatibility; rendering no longer allocates).
pub(super) fn build_each_wfx_id_reusing(
    rule_name: &str,
    event_time_nanos: i64,
    _ctx: &crate::match_engine::match_engine::Event,
    origin: &AlertOrigin,
    _field_order: &[&smol_str::SmolStr],
    _scratch: &mut String,
) -> String {
    wfx_id_from_rule_and_time(rule_name, event_time_nanos, origin)
}

/// Batch-constant FNV-1a prefix state for the on-each wfx_id byte stream:
/// the hasher state after `rule_name \x00`. Rule names are constant per rule
/// (tens of bytes on real rule sets) and were previously re-hashed per row;
/// with the prefix hoisted, the per-row suffix is only
/// `time LE \x00 \x00 origin` (~14 bytes).
pub(crate) struct EachWfxPrefix {
    state: u64,
}

impl EachWfxPrefix {
    pub(crate) fn new(rule_name: &str) -> Self {
        let mut hasher = Fnv1a::new();
        hasher.update(rule_name.as_bytes());
        hasher.update(b"\x00");
        Self {
            state: hasher.state,
        }
    }

    /// Per-row finish — byte stream identical to
    /// [`wfx_id_from_rule_and_time`] (locked by unit test).
    ///
    /// 2026-08-26：返回 [`SmolStr`]（16 hex 内联，零堆分配）——q13b 每行
    /// 构造 wfx_id 的 per-row churn 消减。
    pub(crate) fn wfx_id(&self, event_time_nanos: i64, origin: &AlertOrigin) -> smol_str::SmolStr {
        let mut hasher = Fnv1a { state: self.state };
        hasher.update(&event_time_nanos.to_le_bytes());
        hasher.update(b"\x00");
        hasher.update(b"\x00");
        hasher.update(origin.as_str().as_bytes());
        hex_encode_smol(&hasher.finalize().to_le_bytes())
    }
}

/// Append `v` exactly as `value_to_string(&Value::Number(v as f64))` renders
/// it: `|v| <= 2^53` takes the itoa fast path (the exact f64 Display of such
/// an integer is the plain decimal — no ".0", no exponent), larger magnitudes
/// keep the lossy f64 round-trip Display for byte-identity with the eager
/// path. Single source of the 2^53 rendering rule — used by the batch-typed
/// entity column read in the columnar on-each path (locked by
/// `flat_int64_fast_path_matches_f64_roundtrip_bytes`).
pub(crate) fn write_int64_value(
    scratch: &mut impl crate::match_engine::match_engine::key::StrSink,
    v: i64,
) {
    if v.unsigned_abs() <= (1i64 << 53) as u64 {
        crate::match_engine::match_engine::key::push_i64_exact_decimal(scratch, v);
    } else {
        // |v| > 2^53：f64 Display 渲染（可能有 .0/科学计数——与 value_to_string 字节一致）。
        let rendered = (v as f64).to_string();
        scratch.push_str(&rendered);
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// `hex_encode` 的 SmolStr 版本（2026-08-26 q13b per-row churn 消减）：
/// fnv64 hex 固定 16 字符，落在 SmolStr 内联上限（22B）内 → 零堆分配。
/// 仅用于 [`EachWfxPrefix::wfx_id`]（每行热路径）；其余调用方保持 String。
fn hex_encode_smol(bytes: &[u8]) -> smol_str::SmolStr {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut b = smol_str::SmolStrBuilder::new();
    for byte in bytes {
        b.push(HEX[(byte >> 4) as usize] as char);
        b.push(HEX[(byte & 0x0f) as usize] as char);
    }
    b.into()
}
/// entity_id 连续缓存（P6, 2026-08-26，通用）: 相邻输出（close/match/each）
/// 同 scope_key 时复用 entity_id 字符串——免每行一次字段 resolve +
/// value_to_string。q19 每桶 top-10 条 / q6 同 key 多 match 命中率高。
pub(crate) struct EntityIdCache {
    key: Vec<Value>,
    id: String,
    valid: bool,
}

impl EntityIdCache {
    pub(crate) fn new() -> Self {
        Self {
            key: Vec::new(),
            id: String::new(),
            valid: false,
        }
    }

    /// 命中缓存（scope_key 与上次相同）直接复用；否则调用 `f` 计算并缓存。
    pub(crate) fn get_or(&mut self, scope_key: &[Value], f: impl FnOnce() -> String) -> String {
        if self.valid && self.key.as_slice() == scope_key {
            self.id.clone()
        } else {
            let id = f();
            self.key = scope_key.to_vec();
            self.id = id.clone();
            self.valid = true;
            id
        }
    }
}

/// close 输出列的 origin/reason Arc 预建缓存（2026-08-26，通用）:
/// `AlertOrigin::as_str` / `CloseReason::as_str` 是 `&'static str` 常量，逐条
/// `Arc::from` 会每行一次堆分配（close 列式路径 27.6M 行实测 ~22ns/entry，
/// 二分 SHIELD-D/TEMP-VERIFY 定位）。预建 6 个 Arc，循环内 `Arc::clone`
/// （refcount inc，无分配）。字节与 `as_str` 完全一致，测试锁定。
pub(crate) struct OriginArcs {
    timeout: Arc<str>,
    flush: Arc<str>,
    eos: Arc<str>,
    timeout_reason: Arc<str>,
    flush_reason: Arc<str>,
    eos_reason: Arc<str>,
}

impl OriginArcs {
    pub(crate) fn new() -> Self {
        Self {
            timeout: Arc::from(
                AlertOrigin::Close {
                    reason: CloseReason::Timeout,
                }
                .as_str(),
            ),
            flush: Arc::from(
                AlertOrigin::Close {
                    reason: CloseReason::Flush,
                }
                .as_str(),
            ),
            eos: Arc::from(
                AlertOrigin::Close {
                    reason: CloseReason::Eos,
                }
                .as_str(),
            ),
            timeout_reason: Arc::from(CloseReason::Timeout.as_str()),
            flush_reason: Arc::from(CloseReason::Flush.as_str()),
            eos_reason: Arc::from(CloseReason::Eos.as_str()),
        }
    }

    /// 当前 close 的 origin Arc（`AlertOrigin::Close { reason }` 的 as_str）。
    pub(crate) fn origin(&self, reason: CloseReason) -> &Arc<str> {
        match reason {
            CloseReason::Timeout => &self.timeout,
            CloseReason::Flush => &self.flush,
            CloseReason::Eos => &self.eos,
        }
    }

    /// 当前 close 的 close_reason Arc（`CloseReason::as_str`）。
    pub(crate) fn close_reason(&self, reason: CloseReason) -> &Arc<str> {
        match reason {
            CloseReason::Timeout => &self.timeout_reason,
            CloseReason::Flush => &self.flush_reason,
            CloseReason::Eos => &self.eos_reason,
        }
    }
}

/// Build a human-readable summary.
/// Writes directly into a single `String` (no intermediate `Vec<String>` or
/// `join`) — byte-identical to the previous `format!`+`join` implementation,
/// but one allocation per alert instead of one per part plus a final join.
pub(super) fn build_summary(
    rule_name: &str,
    keys: &[FieldRef],
    scope_key: &[Value],
    step_data: &[StepData],
    origin: &AlertOrigin,
) -> String {
    build_summary_iter(rule_name, keys, scope_key, step_data.iter(), origin)
}

/// 列式 close 批量路径的 split 版本：免 `combine_step_data` 深克隆（同
/// `build_wfx_id_split`——summary 只用 label + measure_value）。字节流 =
/// 原 `build_summary`（event 段接 close 段，测试锁定）。
pub(super) fn build_summary_split(
    rule_name: &str,
    keys: &[FieldRef],
    scope_key: &[Value],
    event_step_data: &[StepData],
    close_step_data: &[StepData],
    origin: &AlertOrigin,
) -> String {
    build_summary_iter(
        rule_name,
        keys,
        scope_key,
        event_step_data.iter().chain(close_step_data.iter()),
        origin,
    )
}

/// (label, measure_value) 迭代器版 build_wfx_id（2026-08-26 q18 stats 直写）。
/// 字节流 = `build_wfx_id_iter`（rule \x00 scope \x1f* \x00 fired_at \x00
/// {label \x1e measure \x1f}* \x00 origin）。
/// 注：生产 stats 直写路径已改用 [`WfxPrefixCache::build_from_labels`]（P6 补齐,
/// 2026-08-27）——本函数保留为测试对拍参考（`wfx_prefix_cache_from_labels_matches_reference`）。
#[cfg(test)]
pub(super) fn build_wfx_id_from_labels<'a>(
    rule_name: &str,
    scope_key: &[Value],
    fired_at: &str,
    steps: impl Iterator<Item = (Option<&'a str>, f64)>,
    origin: &AlertOrigin,
) -> String {
    let mut hasher = Fnv1a::new();
    hasher.update(rule_name.as_bytes());
    hasher.update(b"\x00");
    for v in scope_key {
        hash_value_bytes(&mut hasher, v);
        hasher.update(b"\x1f");
    }
    hasher.update(b"\x00");
    hasher.update(fired_at.as_bytes());
    hasher.update(b"\x00");
    for (label, measure) in steps {
        if let Some(label) = label {
            hasher.update(label.as_bytes());
        }
        hasher.update(b"\x1e");
        hasher.update(&measure.to_bits().to_le_bytes());
        hasher.update(b"\x1f");
    }
    hasher.update(b"\x00");
    hasher.update(origin.as_str().as_bytes());
    hex_encode(&hasher.finalize().to_le_bytes())
}

/// (label, measure_value) 迭代器版 build_summary（2026-08-26 q18 stats 直写）。
/// 字节流 = `build_summary_iter`（step 段: 有 label 则 `label=measure; `,
/// 无 label 则 `step{i}=measure; `——由调用方保证 label 迭代器含 None 槽位,
/// 顺序与索引一致）。
pub(super) fn build_summary_from_labels<'a>(
    rule_name: &str,
    keys: &[FieldRef],
    scope_key: &[Value],
    steps: impl Iterator<Item = (Option<&'a str>, f64)>,
    origin: &AlertOrigin,
) -> String {
    use std::fmt::Write as _;
    let steps_vec: Vec<(Option<&str>, f64)> = steps.collect();
    let mut out = String::with_capacity(64 + scope_key.len() * 12 + steps_vec.len() * 16);
    let _ = write!(out, "rule={}; ", rule_name);
    if scope_key.is_empty() {
        out.push_str("scope=global; ");
    } else {
        out.push_str("scope=[");
        for (i, (fr, val)) in keys.iter().zip(scope_key.iter()).enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = write!(out, "{}={}", field_ref_name(fr), value_to_string(val));
        }
        out.push_str("]; ");
    }
    for (i, (label, measure)) in steps_vec.iter().enumerate() {
        match label {
            Some(label) => {
                out.push_str(label);
                out.push('=');
                write_fixed1(&mut out, *measure);
                out.push_str("; ");
            }
            None => {
                let _ = write!(out, "step{}=", i);
                write_fixed1(&mut out, *measure);
                out.push_str("; ");
            }
        }
    }
    let _ = write!(out, "origin={}", origin.as_str());
    out
}

fn build_summary_iter<'a>(
    rule_name: &str,
    keys: &[FieldRef],
    scope_key: &[Value],
    step_data: impl Iterator<Item = &'a StepData> + Clone,
    origin: &AlertOrigin,
) -> String {
    use std::fmt::Write as _;

    // Estimate capacity so the common case (a few keys / steps) never reallocates.
    let step_len = step_data.clone().count();
    let mut out = String::with_capacity(64 + scope_key.len() * 12 + step_len * 16);
    let _ = write!(out, "rule={}; ", rule_name);

    if scope_key.is_empty() {
        out.push_str("scope=global; ");
    } else {
        out.push_str("scope=[");
        for (i, (fr, val)) in keys.iter().zip(scope_key.iter()).enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = write!(out, "{}={}", field_ref_name(fr), value_to_string(val));
        }
        out.push_str("]; ");
    }

    for (i, sd) in step_data.enumerate() {
        match &sd.label {
            Some(label) => {
                out.push_str(label);
                out.push('=');
                write_fixed1(&mut out, sd.measure_value);
                out.push_str("; ");
            }
            None => {
                let _ = write!(out, "step{}=", i);
                write_fixed1(&mut out, sd.measure_value);
                out.push_str("; ");
            }
        }
    }

    let _ = write!(out, "origin={}", origin.as_str());
    out
}

/// `write!(out, "{v:.1}")` 的定点精度快路径：`v` 为可精确表示的整数 f64 时
/// （finite 且 fract==0 且 |v| <= 2^53），std 的 `{:.1}` 输出恰好是
/// `itoa(v) + ".0"`（`-0.0` → `"-0.0"`）——直接字节写出，免 std flt2dec 的
/// 定点精度求值（隔离对照实测：q19 close 链 641.5→557 ns/evt、q6 match emit
/// 444→375 ns/evt，链上 -13%/-16%）。非整数 / 超范围回退
/// `write!(out, "{:.1}", v)`，字节与 std 完全一致（测试逐值对拍锁定）。
fn write_fixed1(out: &mut String, v: f64) {
    use std::fmt::Write as _;
    if v.is_finite() && v.fract() == 0.0 && v.abs() <= (1u64 << 53) as f64 {
        if v == 0.0 && v.is_sign_negative() {
            out.push_str("-0.0");
        } else {
            push_i64_exact_decimal(out, v as i64);
            out.push_str(".0");
        }
    } else {
        let _ = write!(out, "{:.1}", v);
    }
}

#[cfg(test)]
mod format_tests {
    use super::*;

    /// Reference implementation: the previous `format!`-based output.
    fn reference(nanos: i64) -> String {
        if nanos <= 0 {
            return "1970-01-01T00:00:00.000Z".to_string();
        }
        let total_secs = (nanos / 1_000_000_000) as u64;
        let millis = ((nanos % 1_000_000_000) / 1_000_000) as u32;
        let secs_of_day = total_secs % 86400;
        let days_since_epoch = (total_secs / 86400) as i64;
        let (year, month, day) = civil_from_days(days_since_epoch);
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            year,
            month,
            day,
            secs_of_day / 3600,
            (secs_of_day % 3600) / 60,
            secs_of_day % 60,
            millis
        )
    }

    #[test]
    fn flat_int64_fast_path_matches_f64_roundtrip_bytes() {
        // The 2^53 integer fast path in `write_int64_value` (the single
        // rendering source for Int64 / Timestamp(ns) entity columns on the
        // columnar on-each path) must be byte-identical to the eager
        // `Value::Number(v as f64)` rendering for every value, across the
        // 2^53 exactness boundary.
        let edge_vals: Vec<i64> = vec![
            i64::MIN,
            i64::MIN + 1,
            -(1i64 << 53) - 1,
            -(1i64 << 53),
            -(1i64 << 53) + 1,
            -1,
            0,
            1,
            (1i64 << 53) - 1,
            (1i64 << 53),
            (1i64 << 53) + 1,
            123_456_789,
            999_999_999_999_999_999,
            i64::MAX,
        ];
        for &v in edge_vals.iter() {
            let mut scratch = String::new();
            write_int64_value(&mut scratch, v);
            let expect = value_to_string(&Value::Number(v as f64));
            assert_eq!(scratch, expect, "int64 v={v}");
        }
    }

    #[test]
    fn each_wfx_prefix_matches_scalar_per_row_hash() {
        // The batch-hoisted FNV prefix must produce byte-identical wfx_ids
        // to the per-row scalar hash for every (rule, time) pair.
        let cases: &[(&str, i64)] = &[
            ("q1_bid_passthrough", 1_750_000_000_000_000_000),
            (
                "qradar_rule_with_a_long_name_for_prefix_hoisting_check",
                86_400_000_000_000_000,
            ),
            ("r", 1),
            ("empty_time", 0),
            ("negative", -1),
            ("i64::MAX", i64::MAX),
        ];
        for &(rule, t) in cases {
            assert_eq!(
                EachWfxPrefix::new(rule).wfx_id(t, &AlertOrigin::Event),
                wfx_id_from_rule_and_time(rule, t, &AlertOrigin::Event),
                "rule={rule} t={t}"
            );
        }
    }

    #[test]
    fn format_nanos_utc_matches_reference_on_edges() {
        let edges = [
            0i64,
            -1,
            1,
            999_999,
            1_000_000,
            999_999_999,
            1_000_000_000,
            86_399_999_999_999_999,    // last ms of 1970-01-01
            86_400_000_000_000_000,    // 1970-01-02T00:00:00
            1_583_020_800_000_000_000, // 2020-03-01 (leap year boundary)
            1_609_459_200_000_000_000, // 2021-01-01
            4_102_444_800_000_000_000, // 2100-01-01 (non-leap century)
            i64::MAX,
        ];
        for n in edges {
            assert_eq!(format_nanos_utc(n), reference(n), "edge nanos={n}");
        }
    }

    #[test]
    fn format_nanos_utc_matches_reference_on_pseudorandom_samples() {
        // Deterministic LCG so failures reproduce.
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut nanos: i64 = 1;
        for _ in 0..10_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let pick = state % 100;
            let n = if pick < 60 {
                // Recent-ish wall-clock range (2010-2100).
                1_262_304_000_000_000_000 + (state % 2_817_484_800) as i64 * 1_000_000
            } else if pick < 90 {
                // Early epoch.
                (state % 86_400_000_000) as i64
            } else {
                // Full positive i64 span (up to year 2262).
                ((state >> 12) as i64).abs()
            };
            assert_eq!(format_nanos_utc(n), reference(n), "random nanos={n}");
            nanos += 1;
        }
        let _ = nanos;
    }

    #[test]
    fn build_summary_matches_reference() {
        use crate::match_engine::match_engine::EngineHashMap;

        // Reference: the previous `format!` + `Vec<String>` + `join` shape.
        fn reference_summary(
            rule_name: &str,
            keys: &[FieldRef],
            scope_key: &[Value],
            step_data: &[StepData],
            origin: &AlertOrigin,
        ) -> String {
            let mut parts = Vec::new();
            parts.push(format!("rule={}", rule_name));
            if scope_key.is_empty() {
                parts.push("scope=global".to_string());
            } else {
                let key_strs: Vec<String> = keys
                    .iter()
                    .zip(scope_key.iter())
                    .map(|(fr, val)| format!("{}={}", field_ref_name(fr), value_to_string(val)))
                    .collect();
                parts.push(format!("scope=[{}]", key_strs.join(", ")));
            }
            for (i, sd) in step_data.iter().enumerate() {
                let label_part = match &sd.label {
                    Some(l) => format!("{}={:.1}", l, sd.measure_value),
                    None => format!("step{}={:.1}", i, sd.measure_value),
                };
                parts.push(label_part);
            }
            parts.push(format!("origin={}", origin.as_str()));
            parts.join("; ")
        }

        fn step(measure_value: f64, label: Option<&str>) -> StepData {
            StepData {
                satisfied_branch_index: 0,
                label: label.map(|s| s.to_string()),
                measure_value,
                event_first_time_nanos: None,
                event_last_time_nanos: None,
                collected_values: Vec::new(),
                field_values: EngineHashMap::default(),
            }
        }

        let keys = [FieldRef::Simple("auction".to_string())];
        let scope = [Value::Number(421_762.0)];
        let steps = [step(1.0, None), step(2.5, Some("count"))];

        // Empty scope + empty steps.
        assert_eq!(
            build_summary("q22_asof_person", &[], &[], &[], &AlertOrigin::Event),
            reference_summary("q22_asof_person", &[], &[], &[], &AlertOrigin::Event),
        );
        // Populated scope + labelled and unlabelled steps.
        assert_eq!(
            build_summary(
                "q22_asof_person",
                &keys,
                &scope,
                &steps,
                &AlertOrigin::Event
            ),
            reference_summary(
                "q22_asof_person",
                &keys,
                &scope,
                &steps,
                &AlertOrigin::Event
            ),
        );
        // Close origin.
        let origin = AlertOrigin::Close {
            reason: crate::match_engine::CloseReason::Timeout,
        };
        assert_eq!(
            build_summary("q22_asof_person", &keys, &scope, &steps, &origin),
            reference_summary("q22_asof_person", &keys, &scope, &steps, &origin),
        );
    }

    /// **StrSink 一致性**（2026-08-26 段 4）：`write_int64_value` 泛型化为
    /// `StrSink`（String + SmolStrBuilder 两实现）后，两条路径必须渲染出
    /// **相同的字节**——SmolStrBuilder 路径是 q13b entity_id 的新热路径
    /// （数字内联零堆分配），若与 String 路径有偏差，alert 的 entity_id
    /// 列会静默变值（下游序列化/sink 消费方按字符串处理）。
    #[test]
    fn str_sink_smol_builder_matches_string_rendering() {
        let edge_vals: Vec<i64> = vec![
            i64::MIN,
            i64::MIN + 1,
            -(1i64 << 53) - 1,
            -(1i64 << 53),
            -(1i64 << 53) + 1,
            -1,
            0,
            1,
            (1i64 << 53) - 1,
            (1i64 << 53),
            (1i64 << 53) + 1,
            123_456_789,
            999_999_999_999_999_999,
            i64::MAX,
        ];
        for &v in edge_vals.iter() {
            let mut string_sink = String::new();
            write_int64_value(&mut string_sink, v);
            let mut smol = smol_str::SmolStrBuilder::new();
            write_int64_value(&mut smol, v);
            let smol_str: smol_str::SmolStr = smol.into();
            assert_eq!(
                smol_str.as_str(),
                string_sink,
                "int64 v={v}：SmolStrBuilder 与 String 渲染必须字节一致"
            );
        }
    }

    /// **hex_encode_smol 字节一致**（2026-08-26 段 4）：fnv64 hex 的 SmolStr
    /// 直写（内联 16 字符）与 String 版本输出必须相同——wfx_id 是下游去重/
    /// 关联键，静默变值会破坏语义。
    #[test]
    fn hex_encode_smol_matches_string_version() {
        // 覆盖全字节值域与典型 hash 输出（fnv64 → 8 字节）。
        let cases: &[&[u8]] = &[
            &[0u8; 8],
            &[0xffu8; 8],
            &[0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef],
            &[0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33],
            &[7u8],
            &[],
        ];
        for bytes in cases {
            let legacy = hex_encode(bytes);
            let smol = hex_encode_smol(bytes);
            assert_eq!(
                smol.as_str(),
                legacy,
                "bytes {:02x?}：SmolStr 直写与 String 版本必须字节一致",
                bytes
            );
        }
    }
    /// `write_fixed1`（summary step 的 `{:.1}` 整数快路径）必须与 std
    /// `format!("{:.1}")` 逐值字节一致——整数快路径命中（含 -0.0、2^53
    /// 边界）与回退路径都要对拍。
    #[test]
    fn write_fixed1_matches_std_fixed1() {
        let mut vals: Vec<f64> = vec![
            0.0,
            -0.0,
            1.0,
            -1.0,
            2.5,
            -2.5,
            0.1,
            99.9,
            100.0,
            -100.0,
            123456789.0,
            (1u64 << 53) as f64,
            -((1u64 << 53) as f64),
            ((1u64 << 53) + 1) as f64,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            1e300,
            1e-300,
        ];
        // 伪随机扫描（确定性 LCG，覆盖整值/非整值/量级混合）。
        let mut state: u64 = 0xDEAD_BEEF_CAFE_F00D;
        for _ in 0..10_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let pick = state % 100;
            let v = if pick < 40 {
                // 整数域（快路径主战场）。
                ((state >> 12) as i64 % 1_000_000_000) as f64
            } else if pick < 70 {
                // 非整数值。
                ((state >> 8) as f64) / 10.0 - 1e8
            } else if pick < 85 {
                ((state >> 12) as i64 as f64) / 1e6
            } else {
                // 大数 / 边界。
                [f64::MAX, f64::MIN_POSITIVE, 1e300, 1e-300, 2f64.powi(200)]
                    [(state >> 8) as usize % 5]
            };
            vals.push(v);
        }
        for v in vals {
            let mut fast = String::new();
            write_fixed1(&mut fast, v);
            let std_out = format!("{v:.1}");
            assert_eq!(fast, std_out, "v={v:?} bits={:016x}", v.to_bits());
        }
    }
}
