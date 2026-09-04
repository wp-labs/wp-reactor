//! wf-cep —— 同步执行核叶模块集合（P4，v0.1 起）。
//!
//! 承载与 async/IO 编排无关的同步执行面：
//! - `time`：时间换算（epoch 归一化等）；
//! - `regex_cache` / `cidr_cache`：解释执行路径的逐线程编译/解析缓存；
//! - `error`：引擎域错误（CoreReason / CoreError）；
//! - `value`：`Value` 与热路径哈希别名（P4-A 片 1）；
//! - `external`：外部函数全局注册（P4-A 片 2）；
//! - `rows`：行字段紧凑存储 `RowFieldLayout` / `RowFields`（P4-A 片 3）；
//! - `masks`：列式 branch-guard 掩码 `GuardMasks`（P4-B0）；
//! - `value_extract`：Arrow 列 → `Value` 值提取核心（P4-B0，structured JSON 判定）；
//! - `cep`：单实例 CEP 状态机 + 逐事件解释求值器（P4-B1，engine 经 shim 重导出）；
//! - `row_views`：列式行/触发视图（P4-B1，ColumnarEvent/JoinRow/TriggerEvent）。
//!
//! 本 crate 为内部构建块，模块即公开面（引擎经 shim 重导出）；
//! 依赖墙：**允许 arrow 纯数据面**；禁止 tokio / async / 网络 / 持久化 IO
//! （CI 校验依赖树，见 .github/workflows/ci.yml wf-cep dependency wall）。
//! 后续 P4 扩展（cep 状态机 / event_bridge 视图 / eval）以此为落点。

#[cfg(test)]
mod sem_tests;

pub mod cep;
pub mod cidr_cache;
pub mod error;
pub mod external;
pub mod masks;
pub mod regex_cache;
pub mod row_views;
pub mod rows;
pub mod time;
pub mod value;
pub mod value_extract;
