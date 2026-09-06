//! Stats executor — 声明式窗口统计执行器（P1: 空键 fixed count/distinct）。
//!
//! 与 CEP(RuleExecutor/match)完全正交: 消费 fanout 的 raw RecordBatch,
//! 内部维护可交换结合的归并状态, 窗口 close 时产出度量值并复用 alert 构建。
//!
//! 设计依据: docs/stats-executor-design.md v6（§6 执行器）。
//!
//! 子模块：`masks`（批级 where mask 缓存）/ `accum`（状态/累加类型）/
//! `state`（桶表 + spill 窗口状态）/ `exec`（StatsExecutor 主实现）/
//! `eval`（归并/求值/读取纯函数簇）。

mod masks;
pub use masks::StatsMaskCache;

mod accum;
pub use accum::{DistinctKey, DistinctSet, StatsAccum};
pub(crate) use accum::{NumericAccum, NumericKind, NumericSoA, NumericSoALayout, TopEntry};

mod state;
pub use state::{StatsBucketAccs, StatsWindowState};
// StatsBucket 仅 stats_soa_bench 经 executor:: 转发消费（lib 无生产引用）——
// pub(crate) 声明 = 可达上限（不触发 unreachable_pub）；lib 下 re-export 链无
// 生产消费者，允许 unused。
#[allow(unused_imports)]
pub(crate) use state::StatsBucket;

mod exec;
pub use exec::StatsExecutor;
// 单行/桶累加自由函数簇已迁入 row_acc.rs（exec.rs 过长拆分）——mod.rs 统一
// re-export 保持对外路径不变（引擎 lib-tests / 分片统计消费）。
#[allow(unused_imports)]
pub(crate) use row_acc::{
    accumulate_bucket_row, accumulate_column_row, accumulate_empty_bucket_classic,
    accumulate_empty_bucket_numeric, accumulate_empty_bucket_row_measures,
    accumulate_row_map_classic, accumulate_row_map_soa, accumulate_soa,
};

mod row_acc;

mod eval;
pub(crate) use eval::*;

pub(crate) use state::{SPILL_DRAIN_CHUNK, SpillCreateSpec, vec_to_bucket_accs};
