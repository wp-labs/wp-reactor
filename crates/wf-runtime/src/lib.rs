//! wf-runtime —— 引擎运行接线：把 wf-engine 内核接到来源/窗口/sink 与任务
//! 生命周期。
//!
//! 任务视图（`*_task.rs`）：`engine_task`（规则驱动）、`alert_task`、
//! `evictor_task`（窗口清理）；编排在 `lifecycle/`（mode=batch/daemon 启动关停）；
//! 数据路径：`receiver/`（来源接收）→ `source/`（connector 适配）→
//! `schema_bridge.rs`（schema 对齐）→ window（wf-engine）→ `sink_build.rs`
//! （sink 工厂与路由）。横切：`metrics/`、`perf_diag.rs`、`memory_probe.rs`、
//! `hot_reload/`（在线 reload/发布）、`external`（外部函数运行时）。

#[macro_use]
mod log_macros;

// 测试构建的全局分配器：包装 `System` 并计数 current/peak，供内存规模
// 对比测试（`memory_probe`）断言「峰值不随数据总量增长」。只影响 lib 测试
// 二进制；生产二进制（warp-fusion 的 wfusion CLI）仍用 mimalloc。
#[cfg(test)]
#[global_allocator]
static TEST_ALLOC: memory_probe::CountingAlloc = memory_probe::CountingAlloc;

pub mod cli;

pub(crate) mod alert_task;
pub(crate) mod engine_task;
pub mod error;
mod evictor_task;
pub mod external;
pub mod hot_reload;
pub mod lifecycle;
/// 分配器级内存度量（仅测试构建）：把「内存峰值是否随数据总量增长」变成
/// 确定性断言。生产二进制不启用（wfusion CLI 用 mimalloc）。
#[cfg(test)]
pub mod memory_probe;
pub mod metrics;
pub mod perf_diag;
pub mod receiver;
mod schema_bridge;
pub mod sink_build;
pub mod source;
pub mod tracing_init;
