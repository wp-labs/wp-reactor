use orion_error::conversion::ToStructError;
use orion_error::{OrionError, StructError, UnifiedReason};
use wf_engine::error::CoreReason;

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, OrionError)]
#[moju(kind = "state", domain = "Runtime", module = "Runtime.RuntimeError")]
pub enum RuntimeReason {
    #[orion_error(message = "bootstrap error", identity = "sys.wf_runtime.bootstrap")]
    Bootstrap,
    #[orion_error(message = "shutdown error", identity = "sys.wf_runtime.shutdown")]
    Shutdown,
    #[orion_error(transparent)]
    Core(CoreReason),
    #[orion_error(transparent)]
    General(UnifiedReason),
}

impl From<CoreReason> for RuntimeReason {
    fn from(reason: CoreReason) -> Self {
        Self::Core(reason)
    }
}

pub type RuntimeError = StructError<RuntimeReason>;
pub type RuntimeResult<T> = Result<T, RuntimeError>;

impl RuntimeReason {
    pub fn fail<T>(self, detail: impl Into<String>) -> RuntimeResult<T> {
        self.to_err().with_detail(detail).err()
    }
}
