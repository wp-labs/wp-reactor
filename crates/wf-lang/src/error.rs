use orion_error::conversion::ToStructError;
use orion_error::{OrionError, StructError, UnifiedReason};

#[derive(::jumo_derive::Jumo, Debug, Clone, PartialEq, OrionError)]
#[jumo(kind = "state", domain = "Lang", module = "Lang.LangChecker")]
pub enum LangReason {
    #[orion_error(message = "parse error", identity = "logic.wf_lang.parse")]
    Parse,
    #[orion_error(message = "validation error", identity = "logic.wf_lang.validation")]
    Validation,
    #[orion_error(message = "compile error", identity = "logic.wf_lang.compile")]
    Compile,
    #[orion_error(transparent)]
    General(UnifiedReason),
}

pub type LangError = StructError<LangReason>;
pub type LangResult<T> = Result<T, LangError>;

pub fn fail<T>(reason: LangReason, detail: impl Into<String>) -> LangResult<T> {
    Err(reason.to_err().with_detail(detail))
}

pub fn error(reason: LangReason, detail: impl Into<String>) -> LangError {
    reason.to_err().with_detail(detail)
}
