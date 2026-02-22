use derive_more::From;
use orion_error::{ErrorCode, StructError, UvsReason};
use wf_core::error::CoreReason;

#[derive(Debug, Clone, PartialEq, thiserror::Error, From)]
pub enum RuntimeReason {
    #[error("bootstrap error")]
    Bootstrap,
    #[error("shutdown error")]
    Shutdown,
    #[error("{0}")]
    Core(CoreReason),
    #[error("{0}")]
    Uvs(UvsReason),
}

impl ErrorCode for RuntimeReason {
    fn error_code(&self) -> i32 {
        match self {
            Self::Bootstrap => 2001,
            Self::Shutdown => 2002,
            Self::Core(c) => c.error_code(),
            Self::Uvs(u) => u.error_code(),
        }
    }
}

pub type RuntimeError = StructError<RuntimeReason>;
pub type RuntimeResult<T> = Result<T, RuntimeError>;
