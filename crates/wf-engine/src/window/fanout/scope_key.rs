//! columnar scope-key 直读（rule_shards）——2026-09-04 P4-B1 下沉
//! `wf_cep::cep::key`（scope_key_from_column/scope_key_columnar）；本文件保留为
//! fanout 子模块转发 shim，`partition_rows_by_key` 等旧路径不变。

pub(crate) use wf_cep::cep::key::{scope_key_columnar, scope_key_from_column};
