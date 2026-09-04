//! 列式事件视图（on-each / join / trigger 免物化读面）——2026-09-04 P4-B1
//! 随同步执行核下沉 `wf_cep::row_views`（ColumnarEvent/JoinRow/TriggerEvent +
//! FieldIndex/批级 join 行构建）；本文件保留为 event_bridge 的子模块转发
//! shim（`#[path] mod views` 声明不变），公开面经根 `pub use views::{…}`
//! 保持原路径与可见级。

pub use wf_cep::row_views::*;
