use crate::vars::VarsReason;
use orion_error::conversion::ToStructError;
use orion_error::{OrionError, StructError, UnifiedReason};
use wf_lang::LangReason;

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, OrionError)]
#[moju(kind = "state", domain = "Config", module = "Config.ConfigError")]
pub enum ConfigReason {
    #[orion_error(message = "configuration load error", identity = "conf.wf_config.load")]
    Load,
    #[orion_error(
        message = "configuration parse error",
        identity = "conf.wf_config.parse"
    )]
    Parse,
    #[orion_error(
        message = "configuration validation error",
        identity = "conf.wf_config.validation"
    )]
    Validation,
    #[orion_error(message = "configuration path error", identity = "conf.wf_config.path")]
    Path,
    #[orion_error(message = "sink configuration error", identity = "conf.wf_config.sink")]
    Sink,
    #[orion_error(transparent)]
    Lang(LangReason),
    #[orion_error(transparent)]
    Vars(VarsReason),
    #[orion_error(transparent)]
    General(UnifiedReason),
}

impl From<LangReason> for ConfigReason {
    fn from(reason: LangReason) -> Self {
        Self::Lang(reason)
    }
}

impl From<VarsReason> for ConfigReason {
    fn from(reason: VarsReason) -> Self {
        Self::Vars(reason)
    }
}

pub type ConfigError = StructError<ConfigReason>;
pub type ConfigResult<T> = Result<T, ConfigError>;

impl ConfigReason {
    pub fn fail<T>(self, detail: impl Into<String>) -> ConfigResult<T> {
        self.to_err().with_detail(detail).err()
    }
}
