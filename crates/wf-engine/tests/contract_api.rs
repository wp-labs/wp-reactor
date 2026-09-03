//! 黑盒契约测试：只走 wf_engine 公开 API（`contract::run_test` + wf_lang 构造），
//! 与 lib 内单元测试隔离，守护对外契约不被内部重构悄悄破坏。

#[path = "contract_api/contract.rs"]
mod contract;
#[path = "contract_api/contract_r4.rs"]
mod contract_r4;
#[path = "contract_api/seq_examples.rs"]
mod seq_examples;
