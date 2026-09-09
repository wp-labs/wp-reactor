use orion_error::{OrionError, StructError, UnifiedReason};

#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq, OrionError)]
#[jumo(kind = "state", domain = "Engine", module = "Engine.MatchEngine")]
pub enum CoreReason {
    #[orion_error(
        message = "window build error",
        identity = "logic.wf_core.window_build"
    )]
    WindowBuild,
    #[orion_error(message = "rule execution error", identity = "logic.wf_core.rule_exec")]
    RuleExec,
    #[orion_error(message = "data format error", identity = "sys.wf_core.data_format")]
    DataFormat,
    #[orion_error(message = "sink error", identity = "sys.wf_core.sink")]
    Sink,
    #[orion_error(transparent)]
    General(UnifiedReason),
}

pub type CoreError = StructError<CoreReason>;
pub type CoreResult<T> = Result<T, CoreError>;
