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
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Engine", module = "Engine.MatchEngine")]
pub enum Value {
    Number(f64),
    Str(SmolStr),
    Bool(bool),
    Array(Vec<Value>),
    Object(EngineHashMap<SmolStr, Value>),
}
