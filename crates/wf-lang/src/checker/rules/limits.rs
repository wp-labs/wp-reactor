use crate::ast::RuleDecl;

use crate::checker::{CheckError, Severity};

const VALID_LIMIT_KEYS: &[&str] = &["max_memory", "max_instances", "max_throttle", "on_exceed"];

const VALID_ON_EXCEED: &[&str] = &["throttle", "drop_oldest", "fail_rule"];

pub fn check_limits(rule: &RuleDecl, rule_name: &str, errors: &mut Vec<CheckError>) {
    let limits = match &rule.limits {
        Some(l) => l,
        None => {
            errors.push(CheckError {
                severity: Severity::Warning,
                rule: Some(rule_name.to_string()),
                test: None,
                message: "v2.1 requires `limits { ... }` block; omitting limits may become a compile error in a future release".to_string(),
            });
            return;
        }
    };

    for item in &limits.items {
        if !VALID_LIMIT_KEYS.contains(&item.key.as_str()) {
            errors.push(CheckError {
                severity: Severity::Error,
                rule: Some(rule_name.to_string()),
                test: None,
                message: format!(
                    "unknown limits key `{}`; valid keys are: {}",
                    item.key,
                    VALID_LIMIT_KEYS.join(", ")
                ),
            });
            continue;
        }

        match item.key.as_str() {
            "on_exceed" => {
                if !VALID_ON_EXCEED.contains(&item.value.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "on_exceed value `{}` invalid; valid values are: {}",
                            item.value,
                            VALID_ON_EXCEED.join(", ")
                        ),
                    });
                }
            }
            "max_instances" => match item.value.parse::<usize>() {
                Ok(0) | Err(_) => {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "max_instances value `{}` must be a positive integer (> 0)",
                            item.value
                        ),
                    });
                }
                _ => {}
            },
            "max_throttle" => {
                if !item.value.contains('/') {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "max_throttle value `{}` must be in format count/unit (e.g. \"1000/min\")",
                            item.value
                        ),
                    });
                } else {
                    let parts: Vec<&str> = item.value.splitn(2, '/').collect();
                    if parts.len() == 2 {
                        match parts[0].trim().parse::<u64>() {
                            Ok(0) | Err(_) => {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    test: None,
                                    message: format!(
                                        "max_throttle count `{}` must be a positive integer (> 0)",
                                        parts[0].trim()
                                    ),
                                });
                            }
                            _ => {}
                        }
                        let valid_units = ["s", "sec", "m", "min", "h", "hr", "hour", "d", "day"];
                        if !valid_units.contains(&parts[1].trim()) {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "max_throttle unit `{}` invalid; valid units are: s, sec, m, min, h, hr, hour, d, day",
                                    parts[1].trim()
                                ),
                            });
                        }
                    }
                }
            }
            "max_memory" => {
                let s = item.value.to_uppercase();
                if !(s.ends_with("MB") || s.ends_with("GB") || s.ends_with("KB")) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "max_memory value `{}` must end with KB, MB, or GB (e.g. \"256MB\")",
                            item.value
                        ),
                    });
                } else {
                    let num_str = &s[..s.len() - 2];
                    match num_str.trim().parse::<usize>() {
                        Ok(0) | Err(_) => {
                            errors.push(CheckError {
                                severity: Severity::Error,
                                rule: Some(rule_name.to_string()),
                                test: None,
                                message: format!(
                                    "max_memory value `{}` must have a positive numeric prefix (> 0)",
                                    item.value
                                ),
                            });
                        }
                        Ok(n) => {
                            let multiplier: usize = if s.ends_with("GB") {
                                1024 * 1024 * 1024
                            } else if s.ends_with("MB") {
                                1024 * 1024
                            } else {
                                1024
                            };
                            if n.checked_mul(multiplier).is_none() {
                                errors.push(CheckError {
                                    severity: Severity::Error,
                                    rule: Some(rule_name.to_string()),
                                    test: None,
                                    message: format!(
                                        "max_memory value `{}` overflows; maximum representable is {}GB",
                                        item.value,
                                        usize::MAX / (1024 * 1024 * 1024)
                                    ),
                                });
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}
