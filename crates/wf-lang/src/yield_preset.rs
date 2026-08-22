use std::collections::{HashMap, HashSet};

use crate::ast::{Expr, NamedArg, ObjectItem, RuleDecl, YieldClause, YieldPresetDecl};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum YieldPresetError {
    DuplicatePreset(String),
    DuplicatePresetParam {
        preset: String,
        param: String,
    },
    RequiredParamAfterDefault {
        preset: String,
        param: String,
    },
    UnknownPreset(String),
    DuplicatePresetRef(String),
    MissingPresetArg {
        preset: String,
        arg: String,
    },
    TooManyPresetArgs {
        preset: String,
        expected_min: usize,
        expected_max: usize,
        got: usize,
    },
    UnknownPresetParam {
        preset: String,
        param: String,
    },
    PresetParamOutsidePreset(String),
}

impl YieldPresetError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::DuplicatePreset(name) => format!("duplicate yield preset `{name}`"),
            Self::DuplicatePresetParam { preset, param } => {
                format!("duplicate yield preset parameter `{param}` in `{preset}`")
            }
            Self::RequiredParamAfterDefault { preset, param } => {
                format!(
                    "yield preset `{preset}` required parameter `{param}` cannot follow a defaulted parameter"
                )
            }
            Self::UnknownPreset(name) => format!("unknown yield preset `{name}`"),
            Self::DuplicatePresetRef(name) => {
                format!("yield preset `{name}` is referenced more than once")
            }
            Self::MissingPresetArg { preset, arg } => {
                format!("yield preset `{preset}` missing required argument `{arg}`")
            }
            Self::TooManyPresetArgs {
                preset,
                expected_min,
                expected_max,
                got,
            } => format!(
                "yield preset `{preset}` expects {expected_min}..{expected_max} arguments, got {got}"
            ),
            Self::UnknownPresetParam { preset, param } => {
                format!("unknown yield preset parameter `${param}` in `{preset}`")
            }
            Self::PresetParamOutsidePreset(param) => {
                format!("yield preset parameter `${param}` can only be used inside a yield preset")
            }
        }
    }
}

pub(crate) fn validate_yield_presets(presets: &[YieldPresetDecl]) -> Vec<YieldPresetError> {
    let mut seen = HashSet::new();
    let mut errors = Vec::new();
    for preset in presets {
        if !seen.insert(preset.name.as_str()) {
            errors.push(YieldPresetError::DuplicatePreset(preset.name.clone()));
        }
        let mut seen_params = HashSet::new();
        let mut default_seen = false;
        for param in &preset.params {
            if !seen_params.insert(param.name.as_str()) {
                errors.push(YieldPresetError::DuplicatePresetParam {
                    preset: preset.name.clone(),
                    param: param.name.clone(),
                });
            }
            if param.default.is_some() {
                default_seen = true;
            } else if default_seen {
                errors.push(YieldPresetError::RequiredParamAfterDefault {
                    preset: preset.name.clone(),
                    param: param.name.clone(),
                });
            }
            if let Some(default) = &param.default {
                collect_preset_params(default, &mut |param_name| {
                    errors.push(YieldPresetError::UnknownPresetParam {
                        preset: preset.name.clone(),
                        param: param_name.to_string(),
                    });
                });
            }
        }
    }
    errors
}

pub(crate) fn expand_yield_args(
    presets: &[YieldPresetDecl],
    yield_clause: &YieldClause,
) -> Result<Vec<NamedArg>, YieldPresetError> {
    let preset_map = preset_map(presets);
    let mut seen_refs = HashSet::new();
    let mut merged = Vec::new();

    for preset_ref in &yield_clause.presets {
        if !seen_refs.insert(preset_ref.name.as_str()) {
            return Err(YieldPresetError::DuplicatePresetRef(
                preset_ref.name.clone(),
            ));
        }
        let preset = preset_map
            .get(preset_ref.name.as_str())
            .ok_or_else(|| YieldPresetError::UnknownPreset(preset_ref.name.clone()))?;
        let expanded = expand_preset_args(preset, &preset_ref.args)?;
        merge_args(&mut merged, &expanded);
    }

    for arg in &yield_clause.args {
        reject_preset_params(&arg.value)?;
    }
    merge_args(&mut merged, &yield_clause.args);
    Ok(merged)
}

pub(crate) fn expand_rule_yield_presets(
    rule: &RuleDecl,
    presets: &[YieldPresetDecl],
) -> Result<RuleDecl, YieldPresetError> {
    let mut rule = rule.clone();
    rule.yield_clause.args = expand_yield_args(presets, &rule.yield_clause)?;
    rule.yield_clause.presets.clear();
    Ok(rule)
}

fn preset_map(presets: &[YieldPresetDecl]) -> HashMap<&str, &YieldPresetDecl> {
    let mut map = HashMap::new();
    for preset in presets {
        map.entry(preset.name.as_str()).or_insert(preset);
    }
    map
}

fn merge_args(target: &mut Vec<NamedArg>, incoming: &[NamedArg]) {
    for arg in incoming {
        if let Some(existing) = target.iter_mut().find(|existing| existing.name == arg.name) {
            *existing = arg.clone();
        } else {
            target.push(arg.clone());
        }
    }
}

fn expand_preset_args(
    preset: &YieldPresetDecl,
    args: &[Expr],
) -> Result<Vec<NamedArg>, YieldPresetError> {
    let expected_min = preset
        .params
        .iter()
        .filter(|param| param.default.is_none())
        .count();
    let expected_max = preset.params.len();
    if args.len() > expected_max {
        return Err(YieldPresetError::TooManyPresetArgs {
            preset: preset.name.clone(),
            expected_min,
            expected_max,
            got: args.len(),
        });
    }
    for arg in args {
        reject_preset_params(arg)?;
    }

    let mut bindings: HashMap<&str, Expr> = HashMap::new();
    for (idx, param) in preset.params.iter().enumerate() {
        let value = if let Some(arg) = args.get(idx) {
            arg.clone()
        } else if let Some(default) = &param.default {
            default.clone()
        } else {
            return Err(YieldPresetError::MissingPresetArg {
                preset: preset.name.clone(),
                arg: param.name.clone(),
            });
        };
        bindings.insert(param.name.as_str(), value);
    }

    preset
        .args
        .iter()
        .map(|arg| {
            Ok(NamedArg {
                name: arg.name.clone(),
                value: substitute_preset_params(&arg.value, &bindings, &preset.name)?,
            })
        })
        .collect()
}

fn reject_preset_params(expr: &Expr) -> Result<(), YieldPresetError> {
    let mut error = None;
    collect_preset_params(expr, &mut |param| {
        if error.is_none() {
            error = Some(YieldPresetError::PresetParamOutsidePreset(
                param.to_string(),
            ));
        }
    });
    if let Some(error) = error {
        Err(error)
    } else {
        Ok(())
    }
}

fn substitute_preset_params(
    expr: &Expr,
    bindings: &HashMap<&str, Expr>,
    preset_name: &str,
) -> Result<Expr, YieldPresetError> {
    match expr {
        Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::WfuMeta(_)
        | Expr::Field(_) => Ok(expr.clone()),
        Expr::PresetParam(name) => bindings.get(name.as_str()).cloned().ok_or_else(|| {
            YieldPresetError::UnknownPresetParam {
                preset: preset_name.to_string(),
                param: name.clone(),
            }
        }),
        Expr::BinOp { op, left, right } => Ok(Expr::BinOp {
            op: *op,
            left: Box::new(substitute_preset_params(left, bindings, preset_name)?),
            right: Box::new(substitute_preset_params(right, bindings, preset_name)?),
        }),
        Expr::Neg(inner) => Ok(Expr::Neg(Box::new(substitute_preset_params(
            inner,
            bindings,
            preset_name,
        )?))),
        Expr::FuncCall {
            qualifier,
            name,
            args,
        } => Ok(Expr::FuncCall {
            qualifier: qualifier.clone(),
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| substitute_preset_params(arg, bindings, preset_name))
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Expr::Object(items) => Ok(Expr::Object(
            items
                .iter()
                .map(|item| {
                    Ok(ObjectItem {
                        targets: item.targets.clone(),
                        type_hint: item.type_hint.clone(),
                        value: substitute_preset_params(&item.value, bindings, preset_name)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Expr::Array(items) => Ok(Expr::Array(
            items
                .iter()
                .map(|item| substitute_preset_params(item, bindings, preset_name))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(substitute_preset_params(expr, bindings, preset_name)?),
            list: list
                .iter()
                .map(|item| substitute_preset_params(item, bindings, preset_name))
                .collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        }),
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => Ok(Expr::IfThenElse {
            cond: Box::new(substitute_preset_params(cond, bindings, preset_name)?),
            then_expr: Box::new(substitute_preset_params(then_expr, bindings, preset_name)?),
            else_expr: Box::new(substitute_preset_params(else_expr, bindings, preset_name)?),
        }),
    }
}

fn collect_preset_params<'a>(expr: &'a Expr, on_param: &mut impl FnMut(&'a str)) {
    match expr {
        Expr::PresetParam(name) => on_param(name),
        Expr::BinOp { left, right, .. } => {
            collect_preset_params(left, on_param);
            collect_preset_params(right, on_param);
        }
        Expr::Neg(inner) => collect_preset_params(inner, on_param),
        Expr::FuncCall { args, .. } => {
            for arg in args {
                collect_preset_params(arg, on_param);
            }
        }
        Expr::Object(items) => {
            for item in items {
                collect_preset_params(&item.value, on_param);
            }
        }
        Expr::Array(items) => {
            for item in items {
                collect_preset_params(item, on_param);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_preset_params(expr, on_param);
            for item in list {
                collect_preset_params(item, on_param);
            }
        }
        Expr::IfThenElse {
            cond,
            then_expr,
            else_expr,
        } => {
            collect_preset_params(cond, on_param);
            collect_preset_params(then_expr, on_param);
            collect_preset_params(else_expr, on_param);
        }
        Expr::Number(_)
        | Expr::StringLit(_)
        | Expr::Bool(_)
        | Expr::SystemVar(_)
        | Expr::WfuMeta(_)
        | Expr::Field(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{YieldPresetParam, YieldPresetRef};

    fn preset(name: &str, params: Vec<YieldPresetParam>, args: Vec<NamedArg>) -> YieldPresetDecl {
        YieldPresetDecl {
            name: name.to_string(),
            params,
            args,
        }
    }

    fn param(name: &str, default: Option<Expr>) -> YieldPresetParam {
        YieldPresetParam {
            name: name.to_string(),
            default,
        }
    }

    fn arg(name: &str, value: Expr) -> NamedArg {
        NamedArg {
            name: name.to_string(),
            value,
        }
    }

    fn preset_ref(name: &str, args: Vec<Expr>) -> YieldPresetRef {
        YieldPresetRef {
            name: name.to_string(),
            args,
        }
    }

    fn yield_clause(presets: Vec<YieldPresetRef>, args: Vec<NamedArg>) -> YieldClause {
        YieldClause {
            target: "out".to_string(),
            version: None,
            presets,
            args,
        }
    }

    fn error_messages(errors: &[YieldPresetError]) -> Vec<String> {
        errors.iter().map(YieldPresetError::message).collect()
    }

    #[test]
    fn validate_detects_duplicate_preset() {
        let p = preset("base", vec![], vec![arg("y", Expr::StringLit("x".into()))]);
        let errors = validate_yield_presets(&[p.clone(), p]);
        let msgs = error_messages(&errors);
        assert!(
            msgs.iter()
                .any(|m| m.contains("duplicate yield preset `base`")),
            "got {msgs:?}"
        );
    }

    #[test]
    fn validate_detects_duplicate_and_misordered_params() {
        let p = preset(
            "base",
            vec![
                param("a", None),
                param("b", Some(Expr::Number(1.0))),
                param("a", None),
            ],
            vec![],
        );
        let errors = validate_yield_presets(&[p]);
        let msgs = error_messages(&errors);
        assert!(
            msgs.iter()
                .any(|m| m.contains("duplicate yield preset parameter `a`")),
            "got {msgs:?}"
        );
    }

    #[test]
    fn validate_detects_required_param_after_default() {
        let p = preset(
            "base",
            vec![param("a", Some(Expr::Number(1.0))), param("b", None)],
            vec![],
        );
        let errors = validate_yield_presets(&[p]);
        let msgs = error_messages(&errors);
        assert!(
            msgs.iter()
                .any(|m| m.contains("required parameter `b` cannot follow a defaulted parameter")),
            "got {msgs:?}"
        );
    }

    #[test]
    fn validate_detects_unknown_param_in_default() {
        let p = preset(
            "base",
            vec![param("a", Some(Expr::PresetParam("nope".into())))],
            vec![],
        );
        let errors = validate_yield_presets(&[p]);
        let msgs = error_messages(&errors);
        assert!(
            msgs.iter()
                .any(|m| m.contains("unknown yield preset parameter `$nope`")),
            "got {msgs:?}"
        );
    }

    #[test]
    fn expand_rejects_duplicate_and_unknown_preset_refs() {
        let presets = [preset(
            "base",
            vec![],
            vec![arg("y", Expr::StringLit("x".into()))],
        )];
        let dup = yield_clause(
            vec![preset_ref("base", vec![]), preset_ref("base", vec![])],
            vec![],
        );
        let err = expand_yield_args(&presets, &dup).unwrap_err();
        assert!(err.message().contains("referenced more than once"));

        let unknown = yield_clause(vec![preset_ref("missing", vec![])], vec![]);
        let err = expand_yield_args(&presets, &unknown).unwrap_err();
        assert!(err.message().contains("unknown yield preset `missing`"));
    }

    #[test]
    fn expand_rejects_preset_param_in_inline_args() {
        let presets = [preset(
            "base",
            vec![],
            vec![arg("y", Expr::StringLit("x".into()))],
        )];
        let clause = yield_clause(vec![], vec![arg("z", Expr::PresetParam("p".into()))]);
        let err = expand_yield_args(&presets, &clause).unwrap_err();
        assert!(
            err.message()
                .contains("can only be used inside a yield preset")
        );
    }

    #[test]
    fn expand_missing_and_too_many_args() {
        let presets = [preset(
            "base",
            vec![param("a", None), param("b", Some(Expr::Number(2.0)))],
            vec![arg("y", Expr::PresetParam("a".into()))],
        )];
        let missing = yield_clause(vec![preset_ref("base", vec![])], vec![]);
        let err = expand_yield_args(&presets, &missing).unwrap_err();
        assert!(err.message().contains("missing required argument `a`"));

        let too_many = yield_clause(
            vec![preset_ref(
                "base",
                vec![Expr::Number(1.0), Expr::Number(2.0), Expr::Number(3.0)],
            )],
            vec![],
        );
        let err = expand_yield_args(&presets, &too_many).unwrap_err();
        assert!(err.message().contains("expects 1..2 arguments, got 3"));
    }

    #[test]
    fn expand_substitutes_params_in_nested_exprs() {
        let presets = [preset(
            "base",
            vec![param("s", None)],
            vec![arg(
                "y",
                Expr::IfThenElse {
                    cond: Box::new(Expr::PresetParam("s".into())),
                    then_expr: Box::new(Expr::Array(vec![Expr::PresetParam("s".into())])),
                    else_expr: Box::new(Expr::StringLit("low".into())),
                },
            )],
        )];
        let clause = yield_clause(
            vec![preset_ref("base", vec![Expr::StringLit("high".into())])],
            vec![],
        );
        let merged = expand_yield_args(&presets, &clause).unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].value,
            Expr::IfThenElse {
                cond: Box::new(Expr::StringLit("high".into())),
                then_expr: Box::new(Expr::Array(vec![Expr::StringLit("high".into())])),
                else_expr: Box::new(Expr::StringLit("low".into())),
            }
        );
    }

    #[test]
    fn expand_unknown_param_in_body_rejected() {
        // A body referencing a param that was never bound (e.g. it is not in
        // params and not an argument) errors during substitution.
        let presets = [preset(
            "base",
            vec![],
            vec![arg("y", Expr::PresetParam("ghost".into()))],
        )];
        let clause = yield_clause(vec![preset_ref("base", vec![])], vec![]);
        let err = expand_yield_args(&presets, &clause).unwrap_err();
        assert!(
            err.message()
                .contains("unknown yield preset parameter `$ghost`")
        );
    }

    #[test]
    fn expand_later_values_override_earlier() {
        let presets = [
            preset("base", vec![], vec![arg("y", Expr::StringLit("a".into()))]),
            preset("extra", vec![], vec![arg("y", Expr::StringLit("b".into()))]),
        ];
        let clause = yield_clause(
            vec![preset_ref("base", vec![]), preset_ref("extra", vec![])],
            vec![arg("y", Expr::StringLit("c".into()))],
        );
        let merged = expand_yield_args(&presets, &clause).unwrap();
        // Inline args are merged last and win.
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].value, Expr::StringLit("c".into()));
    }

    #[test]
    fn expand_empty_presets_passthrough() {
        let clause = yield_clause(vec![], vec![arg("y", Expr::StringLit("x".into()))]);
        let merged = expand_yield_args(&[], &clause).unwrap();
        assert_eq!(merged, clause.args);
    }
}
