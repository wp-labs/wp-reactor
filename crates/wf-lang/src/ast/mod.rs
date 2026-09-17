/// 用户输入造成的**嵌套层次**硬上限（表达式与 `let` 引用链共用同一口径）。
///
/// 多处实现按结构递归（表达式解析、`let` 引用链展开：键派生 / 列式 yield /
/// 阈值常量内联、以及解析后各阶段的 AST 遍历），而嵌套深浅完全由用户输入决定。
/// 无上限时深嵌套/深链会耗尽线程栈，而**栈溢出不是 panic、catch 不住**——直接
/// abort 进程且没有位置信息。因此统一在编译期拒绝超过本上限的输入（parse 阶段
/// 兜住表达式嵌套 ⇒ 后续遍历深度有界；checker 兜住 `let` 链 ⇒ 键/yield 展开有界）。
pub(crate) const MAX_NESTING_LEVELS: usize = 5;

mod clauses;
mod contract;
mod conv;
mod events;
mod expr;
mod join;
mod limits;
mod match_;
mod rule;
mod stats;

pub use clauses::*;
pub use contract::*;
pub use conv::*;
pub use events::*;
pub use expr::*;
pub use join::*;
pub use limits::*;
pub use match_::*;
pub use rule::*;
pub use stats::*;
