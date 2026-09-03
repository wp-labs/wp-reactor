use crate::error::RuntimeReason;
use orion_error::conversion::ToStructError;
use orion_error::{OrionError, StructError, UnifiedReason};
use wf_config::ConfigReason;

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, OrionError)]
#[moju(kind = "state", domain = "Runtime", module = "Runtime.CliEntry")]
pub enum EngineReason {
    #[orion_error(message = "CLI error", identity = "sys.wf_engine.cli")]
    Cli,
    #[orion_error(transparent)]
    Config(ConfigReason),
    #[orion_error(transparent)]
    Runtime(RuntimeReason),
    #[orion_error(transparent)]
    General(UnifiedReason),
}

impl From<ConfigReason> for EngineReason {
    fn from(reason: ConfigReason) -> Self {
        Self::Config(reason)
    }
}

impl From<RuntimeReason> for EngineReason {
    fn from(reason: RuntimeReason) -> Self {
        Self::Runtime(reason)
    }
}

pub type EngineError = StructError<EngineReason>;
pub type EngineResult<T> = Result<T, EngineError>;

impl EngineReason {
    pub fn fail<T>(self, detail: impl Into<String>) -> EngineResult<T> {
        self.to_err().with_detail(detail).err()
    }
}
