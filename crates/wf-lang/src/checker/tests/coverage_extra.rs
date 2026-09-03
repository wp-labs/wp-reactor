//! Extra coverage tests for the checker: error branches of check_funcs,
//! rule-level checks (rules/mod.rs), joins, keys, limits, expr type-checking
//! and scope resolution that the focused test files do not reach.
//!
//! 测试已按主题拆入子模块（`#[path]` sibling 文件模块，见 refactor handoff 坑 #24）：
//! coverage_funcs（内建函数 str/join/hash/时间/数值等族参数校验）、coverage_funcs2
//! （mv/聚合/stat.* selector 分支）、coverage_rules（rules/mod + joins + keys + limits）、
//! coverage_expr（check_expr/scope/pipe/lint）；本文件保留子模块共享的 window harness
//! （`pub(super)`，子模块 `use super::*` glob 可见）。

use super::*;
use crate::schema::FieldType;

// ---------------------------------------------------------------------------
// Extra windows used only by these tests
// ---------------------------------------------------------------------------

/// Window with a float field (non-key scalar excluded from join keys).
pub(super) fn float_win() -> WindowSchema {
    make_window(
        "float_win",
        vec!["float_stream"],
        vec![
            ("f", bt(BaseType::Float)),
            ("sip", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Window with structured (object/array) and bool fields.
pub(super) fn obj_win() -> WindowSchema {
    make_window(
        "obj_win",
        vec!["obj_stream"],
        vec![
            ("sip", bt(BaseType::Chars)),
            ("obj", FieldType::Object),
            ("arr", FieldType::Array(BaseType::Chars)),
            ("active", bt(BaseType::Bool)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Static provider window (side input): no streams, no time field, over = 0.
pub(super) fn provider_win() -> WindowSchema {
    WindowSchema {
        name: "prov".to_string(),
        streams: vec![],
        time_field: None,
        over: Duration::ZERO,
        fields: vec![
            FieldDef {
                name: "sip".to_string(),
                field_type: bt(BaseType::Chars),
            },
            FieldDef {
                name: "category".to_string(),
                field_type: bt(BaseType::Chars),
            },
        ],
    }
}

/// Window with a time field for asof/within join tests.
pub(super) fn bid_win() -> WindowSchema {
    make_window(
        "bid_events",
        vec!["bid_stream"],
        vec![
            ("auction", bt(BaseType::Digit)),
            ("bidder", bt(BaseType::Chars)),
            ("price", bt(BaseType::Digit)),
            ("category", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Snapshot-join target carrying `id` / `category` for join-then-key tests.
pub(super) fn auction_win() -> WindowSchema {
    make_window(
        "auction_events",
        vec!["auction_stream"],
        vec![
            ("id", bt(BaseType::Digit)),
            ("category", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// Output window with a broad field set (n, y, x, b, f).
pub(super) fn wide_output_window() -> WindowSchema {
    make_output_window(
        "out",
        vec![
            ("x", bt(BaseType::Ip)),
            ("y", bt(BaseType::Chars)),
            ("n", bt(BaseType::Digit)),
            ("b", bt(BaseType::Bool)),
            ("f", bt(BaseType::Float)),
        ],
    )
}

/// Two windows that both carry a field named `sip` but with different types.
pub(super) fn ip_sip_win() -> WindowSchema {
    make_window(
        "ip_sip",
        vec!["s1"],
        vec![
            ("sip", bt(BaseType::Ip)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

pub(super) fn chars_sip_win() -> WindowSchema {
    make_window(
        "chars_sip",
        vec!["s2"],
        vec![
            ("sip", bt(BaseType::Chars)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

/// A window whose field names collide with the step label used in tests.
pub(super) fn label_win() -> WindowSchema {
    make_window(
        "label_win",
        vec!["l_stream"],
        vec![
            ("fail", bt(BaseType::Digit)),
            ("event_time", bt(BaseType::Time)),
        ],
    )
}

// ===========================================================================
// 测试子模块（与 coverage_extra.rs 同目录；`#[path]` 相对本文件所在目录解析）
// ===========================================================================
#[path = "coverage_funcs.rs"]
mod coverage_funcs;

#[path = "coverage_funcs2.rs"]
mod coverage_funcs2;

#[path = "coverage_rules.rs"]
mod coverage_rules;

#[path = "coverage_expr.rs"]
mod coverage_expr;
