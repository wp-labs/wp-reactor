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
