use crate::ast::RuleDecl;

use crate::checker::{CheckError, Severity};

const VALID_LIMIT_KEYS: &[&str] = &[
    "max_memory",
    "max_instances",
    "max_throttle",
    "on_exceed",
    "disk_provider",
    "spill", // 兼容别名（2026-08-27 改名 disk_provider）
    "max_disk",
    "max_spill_bytes", // 兼容别名（2026-08-27 改名 max_disk）
];

const VALID_ON_EXCEED: &[&str] = &["throttle", "drop_oldest", "fail_rule"];

const VALID_SPILL: &[&str] = &["redb"];

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

    // 场景判定（2026-08-27）: `disk_provider`/`max_disk` 仅 stats 规则可用——
    // spawn 层只有 stats 分支读取（match/on-each 规则静默忽略）; 空键 stats
    // 单桶无驱逐对象（seed_empty_bucket 恒命中, 从不落盘）。静态报错而非
    // 运行时静默降级。
    let is_stats = rule.stats_clause.is_some();
    let stats_empty_key = rule
        .stats_clause
        .as_ref()
        .is_some_and(|s| s.keys.is_empty());
    let has_disk_provider = limits.items.iter().any(|i| {
        // 认键不认值: 值非法已单独报错, 这里避免再叠加「未配置」的误导警告。
        i.key == "disk_provider" || i.key == "spill"
    });

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
            "on_exceed" if !VALID_ON_EXCEED.contains(&item.value.as_str()) => {
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
            "on_exceed" => {}
            "disk_provider" if !VALID_SPILL.contains(&item.value.as_str()) => {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: format!(
                        "disk_provider value `{}` invalid; valid values are: {}",
                        item.value,
                        VALID_SPILL.join(", ")
                    ),
                });
            }
            "disk_provider" => {}
            "spill" => {
                // 兼容别名: 已重命名为 disk_provider（2026-08-27）。旧键仍生效
                // 但将废弃——非法值同样报错, 合法值加迁移警告。
                if !VALID_SPILL.contains(&item.value.as_str()) {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "spill value `{}` invalid; valid values are: {}",
                            item.value,
                            VALID_SPILL.join(", ")
                        ),
                    });
                } else {
                    errors.push(CheckError {
                        severity: Severity::Warning,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: "`spill` 已重命名为 `disk_provider`（2026-08-27）——请迁移配置; 旧键仍生效但将废弃".to_string(),
                    });
                }
            }
            "max_disk" => {
                if crate::compiler::parse_byte_size(&item.value).is_none() {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "max_disk value `{}` must be a byte size (e.g. \"20GB\")",
                            item.value
                        ),
                    });
                }
            }
            "max_spill_bytes" => {
                // 兼容别名: 已重命名为 max_disk（语义同, 规则级磁盘总上限）。
                if crate::compiler::parse_byte_size(&item.value).is_none() {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "max_spill_bytes value `{}` must be a byte size (e.g. \"20GB\")",
                            item.value
                        ),
                    });
                } else {
                    errors.push(CheckError {
                        severity: Severity::Warning,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: "`max_spill_bytes` 已重命名为 `max_disk`（2026-08-27）——请迁移配置; 旧键仍生效但将废弃".to_string(),
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

        // 场景检查（与值校验解耦: 值非法已在上方报错, 这里管「配了但不生效」）。
        match item.key.as_str() {
            "disk_provider" | "spill" => {
                if !is_stats {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "`{}` 仅支持 stats 规则（match/on-each 规则无状态落盘路径, 配置会被忽略）",
                            item.key
                        ),
                    });
                } else if stats_empty_key {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "`{}` 对空键 stats 规则（无 group by）不生效——单桶无驱逐对象, 从不落盘; 请加 group by 或移除配置",
                            item.key
                        ),
                    });
                }
            }
            "max_disk" | "max_spill_bytes" => {
                if !is_stats {
                    errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "`{}` 仅支持 stats 规则（match/on-each 规则无状态落盘路径, 配置会被忽略）",
                            item.key
                        ),
                    });
                } else if !has_disk_provider {
                    errors.push(CheckError {
                        severity: Severity::Warning,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: format!(
                            "`{}` 已配置但未配置 `disk_provider`——落盘未启用, 该上限不生效",
                            item.key
                        ),
                    });
                }
            }
            _ => {}
        }
    }
}
