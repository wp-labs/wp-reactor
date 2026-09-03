//! wf-cep —— 纯逻辑叶模块集合（P4 v0.1）。
//!
//! 承载与 IO / 列式执行无关的纯单元：
//! - `time`：时间换算（epoch 归一化等）；
//! - `regex_cache` / `cidr_cache`：解释执行路径的逐线程编译/解析缓存；
//! - `error`：引擎域错误（CoreReason / CoreError）。
//!
//! 本 crate 为内部构建块，模块即公开面（引擎经 shim 重导出）；
//! 依赖墙：无 tokio / arrow / IO。后续 P4 扩展（eval/Value/…）以此为落点。

pub mod cidr_cache;
pub mod error;
pub mod external;
pub mod rows;
pub mod regex_cache;
pub mod time;
pub mod value;
