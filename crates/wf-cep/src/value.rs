//! 纯标量值层（P4-A 片 1）：`Value` 与热路径哈希别名。
//!
//! `Event` / `FieldSource` 因孤儿规则与 trait 内 ScopeKey 依赖暂留引擎
//! （types.rs 经 `pub use` 别名重导出本模块符号，engine 内路径不变）。

use std::collections::{HashMap, HashSet};

use foldhash::fast::RandomState as FoldRandomState;
use smol_str::SmolStr;

/// HashMap/HashSet over hot-path keys (InstanceKey, field names, event field
/// keys) using foldhash's fast, minimally-DoS-resistant hasher instead of the
/// default SipHash. SipHash was ~3k samples of the match-engine profile; field
/// names / rule keys are internal, and InstanceKey values carry a random seed
/// via `FoldRandomState` so collision attacks stay hard.
pub type EngineHashMap<K, V> = HashMap<K, V, FoldRandomState>;
pub type EngineHashSet<K> = HashSet<K, FoldRandomState>;

/// `f64` 精确整数上限：`|i| < 2^53` 时 `i as f64` 无损（`2^53 == 9007199254740992`）。
///
/// **全家族单一来源**：`wf-cep` 的 `ValueKey` / `ScopeKey` / `DistinctKey` 与
/// `wf-engine` 的列式编译 / stats / alert 导出，整↔浮判界都引用它。此前同一数值
/// 在生产代码里内联了 8 处，任何一处改漏都会让「同一逻辑值」在不同路径上判成整/浮
/// 两类 —— 静默分键（distinct 少计、shard 错配）或静默量化（epoch-ns 丢 ~256ns）。
pub const F64_EXACT_INT_LIMIT: u64 = 1 << 53;

/// [`F64_EXACT_INT_LIMIT`] 的 `f64` 形态，供 `n.abs() < TWO_POW_53` 形式的比较使用
/// （整数域用 `F64_EXACT_INT_LIMIT`，浮点域用本常量，避免每处 `as f64`）。
pub const TWO_POW_53: f64 = 9_007_199_254_740_992.0;

// 两个形态必须指同一个边界：改一个漏一个正是上面说的「静默分键」。
const _: () = assert!(TWO_POW_53 as u64 == F64_EXACT_INT_LIMIT);

/// Field name for machine identifier carried in events and batches
/// for per-machine metrics labeling.
pub const MACHINE_ID: &str = "wp_src_ip";

/// Scalar value carried inside an event row or expression.
///
/// 数值域拆成两个变体：[`Value::Float`] 承载真正的浮点，[`Value::Int`] 精确承载
/// 整数（箭头 Int64/Timestamp 列、digit 字段、整值计数）—— 后者避免 epoch-ns
/// （≈1.77e18 > 2^53）经 f64 往返被量化到 ~256ns（2026-09-18 精度修复的根治处）。
///
/// `#[non_exhaustive]`：外部 crate 匹配必须带兜底分支，后续再加变体不再是破坏性变更。
#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq)]
#[jumo(kind = "state", domain = "Engine", module = "Engine.MatchEngine")]
#[non_exhaustive]
pub enum Value {
    /// 浮点域（JSON 小数、`Float64` 列、除法等运算结果）。刻意不叫 `Number`：
    /// 那个名字暗示「数字都归它管」，正是整数被 f64 量化的温床。
    Float(f64),
    /// 精确整数（`i64`）。语义归一：`|i| < 2^53` 时与 `Float(i as f64)` 视为**同一
    /// 值**（比较/同一性/哈希/序列化漏斗统一处理，见 `cep::eval::cmp` 与 `cep::key`）。
    Int(i64),
    Str(SmolStr),
    Bool(bool),
    Array(Vec<Value>),
    Object(EngineHashMap<SmolStr, Value>),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 两个常量必须指向同一个边界，且这个边界正是 f64 整数精度的终点。
    /// （`const _` 断言已锁一致性，这里锁「边界处的行为」。）
    #[test]
    fn f64_exact_int_limit_is_where_f64_stops_being_exact() {
        let limit = F64_EXACT_INT_LIMIT as f64;
        assert_eq!(limit, TWO_POW_53);
        let at_limit = F64_EXACT_INT_LIMIT as i64;
        assert_eq!((limit - 1.0) as i64, at_limit - 1);
        // 2^53 处 ulp 从 1 跳到 2：+1 不可表示（回落到 2^53），+2 可以。
        assert_eq!((limit + 1.0) as i64, at_limit);
        assert_eq!((limit + 2.0) as i64, at_limit + 2);
    }
}
