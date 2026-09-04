//! 语义层测试套件（P4-B2 归位，2026-09-04）：随 cep 同步执行核从
//! wf-engine `match_engine::tests` 迁移的纯语义测试（G2 顶层 + l2 子集 + l3
//! 整组 + join_key + eval_coverage 纯 eval 段 + helpers 副本）。经 wf_cep 公开面
//! 驱动（CepStateMachine / eval_expr / WindowLookup / helpers plan 构造器）。

mod accu;
mod any_l2;
mod cep_core;
mod close;
mod eval_coverage;
mod helpers;
mod join_key;
mod l2;
mod l3;
mod seq_l2;
mod seq_order;
