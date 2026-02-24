use derive_more::From;
use orion_error::{ErrorCode, StructError, UvsReason};

#[derive(Debug, Clone, PartialEq, thiserror::Error, From)]
pub enum CoreReason {
    #[error("window build error")]
    WindowBuild,
    #[error("rule execution error")]
    RuleExec,
    #[error("alert sink error")]
    AlertSink,
    #[error("data format error")]
    DataFormat,
    #[error("{0}")]
    Uvs(UvsReason),
}

impl ErrorCode for CoreReason {
    fn error_code(&self) -> i32 {
        match self {
            Self::WindowBuild => 1001,
            Self::RuleExec => 1002,
            Self::AlertSink => 1003,
            Self::DataFormat => 1004,
            Self::Uvs(u) => u.error_code(),
        }
    }
}

pub type CoreError = StructError<CoreReason>;
pub type CoreResult<T> = Result<T, CoreError>;
