use std::time::Duration;

use super::*;

// ---------------------------------------------------------------------------
// Test block (rule testing)
// ---------------------------------------------------------------------------

/// `test name for rule_name { input { ... } expect { ... } [options { ... }] }`
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangTest")]
pub struct TestBlock {
    pub name: String,
    pub rule_name: String,
    pub input: Vec<InputStmt>,
    pub expect: Vec<ExpectStmt>,
    pub options: Option<TestOptions>,
}

/// Statement inside an `input { ... }` block.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangTest")]
pub enum InputStmt {
    /// `row(alias, field = expr, ...);`
    Row {
        alias: String,
        fields: Vec<FieldAssign>,
    },
    /// `tick(duration);`
    Tick(Duration),
}

/// `name = expr` — field assignment in a `row(...)` statement.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangTest")]
pub struct FieldAssign {
    pub name: String,
    pub value: Expr,
}

/// Statement inside an `expect { ... }` block.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangTest")]
pub enum ExpectStmt {
    /// `hits cmp_op INTEGER;`
    Hits { cmp: CmpOp, count: usize },
    /// `hit[index].assert;`
    HitAssert { index: usize, assert: HitAssert },
}

/// Assertion on a specific hit output.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, ::moju_derive::MoJu)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangTest")]
pub enum HitAssert {
    /// `score cmp_op NUMBER`
    Score { cmp: CmpOp, value: f64 },
    /// `origin == STRING`
    Origin { value: String },
    /// `entity_type == STRING`
    EntityType { value: String },
    /// `entity_id == STRING`
    EntityId { value: String },
    /// `field(STRING) cmp_op expr`
    Field {
        name: String,
        cmp: CmpOp,
        value: Expr,
    },
}

/// Options for a test block.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Default, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangTest")]
pub struct TestOptions {
    pub close_trigger: Option<CloseTrigger>,
    pub eval_mode: Option<EvalMode>,
    pub permutation: Option<PermutationMode>,
    pub runs: Option<usize>,
}

/// Window close trigger mode for test execution.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangMatch")]
pub enum CloseTrigger {
    Timeout,
    Flush,
    Eos,
}

/// Evaluation mode for test execution.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangTest")]
pub enum EvalMode {
    Strict,
    Lenient,
}

/// Input permutation mode for conformance testing.
#[non_exhaustive]
#[derive(::moju_derive::MoJu, Debug, Clone, Copy, PartialEq, Eq)]
#[moju(kind = "state", domain = "Lang", module = "Lang.LangTest")]
pub enum PermutationMode {
    Shuffle,
}
