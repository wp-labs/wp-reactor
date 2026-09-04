//! L2 表达式求值 feature tests。原整文件 1612 行按主题拆出兄弟 `#[path]` 子模块
//! （同目录文件，2026-09-04；共享 harness 在 l2/mod.rs，此处仅保留文件头 import 供各
//! 子模块经 `use super::*` 继承复用）：
//! - `expr_control`：Not 逻辑否定 / IfThenElse 控制流、regex_match / cidr_match 守卫
//! - `expr_time`：time_diff / time_bucket / bucket_end、数值·strftime 内建杂项、now 系
//! - `expr_funcs`：blank / merge、hash / stable_id / join、strptime、str·mv 族、external 分发

use super::*;
use wf_lang::ast::{BinOp, ObjectItem};

#[path = "expr_control.rs"]
mod expr_control;

#[path = "expr_time.rs"]
mod expr_time;

#[path = "expr_funcs.rs"]
mod expr_funcs;
