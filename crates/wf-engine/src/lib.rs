//! wf-engine —— 引擎执行内核（纯逻辑 + actor 接线之外的全部语义）。
//!
//! 分层与阅读顺序（自上而下依赖）：
//! 1. `match_engine/`：规则执行层（本 crate 主体）——`cep/` 单实例 CEP 状态机
//!    （`match_engine::CepStateMachine`，逐事件解释求值见 `cep::eval`）、`executor/` 批级执行
//!    编排与列式表达式后端（`executor::eval`）、`columnar`/`event_bridge` 列式
//!    批↔行桥、`spill`/`async_persist` 落盘持久化、`contract` 对外契约；
//! 2. `window/`：输入窗状态与保留（actor/buffer/evictor/fanout/join 索引）；
//! 3. `alert/`：告警输出类型与列构建；`pipe/`：窗口流水线注册；
//! 4. `external`/`error`/`time`：外部函数回调、错误、时间工具。
//!
//! 对外契约 = `lib.rs` 顶层模块 + 各模块 `pub use` 面（wf-runtime 全量消费）。
//! 设计文档：`docs/design/`（columnar-match-state-machine / columnar-execution /
//! hot-path-vectorization / async-persist / match-expiry-semantics …）。

pub mod alert;
// error/time 纯叶已下沉 wf-cep；此处 shim 重导出保持公开路径不变
pub mod error {
    pub use wf_cep::error::*;
}
// external 已下沉 wf-cep（P4-A 片 1）；shim 重导出保持公开路径
pub mod external {
    pub use wf_cep::external::*;
}
pub mod match_engine;
pub mod pipe;
pub mod sink;
pub(crate) mod time {
    pub use wf_cep::time::*;
}
pub mod window;

pub use time::normalize_epoch_timestamp_float_nanos;
