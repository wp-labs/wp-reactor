use orion_error::{OrionError, StructError, UnifiedReason};

#[derive(::moju_derive::MoJu, Debug, Clone, PartialEq, OrionError)]
#[moju(kind = "state", domain = "Config", module = "Config.ConfigError")]
pub enum VarsReason {
    #[orion_error(
        message = "variable resolution error",
        identity = "conf.wf_vars.resolve"
    )]
    Resolve,
    #[orion_error(
        message = "template expansion error",
        identity = "conf.wf_vars.template"
    )]
    Template,
    #[orion_error(message = "TOML processing error", identity = "conf.wf_vars.toml")]
    Toml,
    #[orion_error(transparent)]
    General(UnifiedReason),
}

pub type VarsError = StructError<VarsReason>;
pub type VarsResult<T> = Result<T, VarsError>;
