use std::time::Duration;

use super::*;

// ---------------------------------------------------------------------------
// Contract block (rule contract testing)
// ---------------------------------------------------------------------------

/// `contract name for rule_name { given { ... } expect { ... } [options { ... }] }`
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ContractBlock {
    pub name: String,
    pub rule_name: String,
    pub given: Vec<GivenStmt>,
    pub expect: Vec<ExpectStmt>,
    pub options: Option<ContractOptions>,
}

/// Statement inside a `given { ... }` block.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum GivenStmt {
    /// `row(alias, field = expr, ...);`
    Row {
        alias: String,
        fields: Vec<FieldAssign>,
    },
    /// `tick(duration);`
    Tick(Duration),
}

/// `name = expr` — field assignment in a `row(...)` statement.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FieldAssign {
    pub name: String,
    pub value: Expr,
}

/// Statement inside an `expect { ... }` block.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ExpectStmt {
    /// `hits cmp_op INTEGER;`
    Hits { cmp: CmpOp, count: usize },
    /// `hit[index].assert;`
    HitAssert { index: usize, assert: HitAssert },
}

/// Assertion on a specific hit output.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum HitAssert {
    /// `score cmp_op NUMBER`
    Score { cmp: CmpOp, value: f64 },
    /// `close_reason == STRING`
    CloseReason { value: String },
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

/// Options for a contract block.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ContractOptions {
    pub close_trigger: Option<CloseTrigger>,
    pub eval_mode: Option<EvalMode>,
}

/// Window close trigger mode for contract testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseTrigger {
    Timeout,
    Flush,
    Eos,
}

/// Evaluation mode for contract testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EvalMode {
    Strict,
    Lenient,
}
