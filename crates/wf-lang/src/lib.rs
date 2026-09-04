//! wf-lang —— WFL/WFS/WFG 语言层：解析、检查、编译到执行计划。
//!
//! 管道：`*_parser`（wfl/wfs/wfg 语法）→ `ast/` → `checker/`（语义检查）→
//! `compiler/`（M13：AST → `plan/` 执行计划，`MatchPlan` 等）→ `explain/`。
//! 纯语义 crate：无 IO/runtime 依赖，可独立单测与 fuzz。
//! 配套：`preprocess/`（preset/_global 展开）、`yield_preset.rs`、`wfu_meta.rs`
//! （`__wfu_*` 元字段名）。

pub mod ast;
mod checker;
pub mod cidr;
pub mod columnar;
pub mod compiler;
pub mod diagnostics;
pub mod error;
pub mod explain;
pub mod field_usage;
pub mod parse_utils;
pub mod plan;
pub mod preprocess;
mod schema;
pub mod wfg_ast;
pub mod wfg_parser;
mod wfl_parser;
mod wfs_parser;
pub mod wfu_meta;
mod yield_preset;

pub use checker::lint::lint_wfl;
pub use checker::{
    CheckError, Severity, check_intermediate_target_graph, check_wfl, effective_schemas_for_rules,
};
pub use compiler::compile_wfl;
pub use diagnostics::{
    compile_wfl_with_diagnostics, parse_wfl_with_diagnostics, validate_wfl_with_diagnostics,
};
pub use error::{LangError, LangReason, LangResult};
pub use preprocess::{preprocess_vars, preprocess_vars_with_env};
pub use schema::{BaseType, FieldDef, FieldType, StaticWindowSchema, WindowSchema};
pub use wfg_parser::parse_wfg;
pub use wfl_parser::parse_wfl;
pub use wfs_parser::{parse_static_wfs, parse_wfs};

/// 引擎输出 / `strftime` 无参默认时间格式（2026-09-04 P4-B1 上移公共底座：
/// `wf_config::output` 默认与 `wf_cep::cep::eval` strftime 共用同源，避免
/// cep 语义核反向依赖配置层）。
pub const DEFAULT_OUTPUT_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";
