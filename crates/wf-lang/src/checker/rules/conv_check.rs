use crate::ast::{FieldRef, RuleDecl, WindowMode};

use crate::checker::{CheckError, Severity};

pub fn check_conv(rule: &RuleDecl, rule_name: &str, errors: &mut Vec<CheckError>) {
    // conv 自动聚合窗按 match key 建字段（类型取 schema 字段）；派生/嵌套路径
    // key（issue #83）的值由事件域表达式产生、conv 窗 schema 无法静态定字段名/
    // 类型 → v1 拒绝组合。
    if rule.conv.is_some() {
        for key in &rule.match_clause.keys {
            let derived = matches!(key, FieldRef::Path { .. })
                || matches!(key, FieldRef::Simple(name) if rule.lets.iter().any(|l| &l.name == name));
            if derived {
                errors.push(CheckError {
                    severity: Severity::Error,
                    rule: Some(rule_name.to_string()),
                    test: None,
                    message: "conv block 与派生/嵌套路径 match key 不能组合使用（v1）；请先用顶层字段 key，或在接入层把嵌套值上提".to_string(),
                });
            }
        }
    }
    // conv 消费 close 收口批：fixed 桶与 hop 窗口的 close 均按窗口边界成批
    // （hop 在 slide 对齐时刻收口），语义成立；sliding/session 的 close 时机
    // 由首事件/会话决定，无固定批边界，仍拒绝。
    let conv_ok = matches!(
        rule.match_clause.window_mode,
        WindowMode::Fixed | WindowMode::Hop { .. }
    );
    if rule.conv.is_some() && !conv_ok {
        errors.push(CheckError {
            severity: Severity::Error,
            rule: Some(rule_name.to_string()),
            test: None,
            message: "conv block requires fixed or hop window mode (match<key:dur:fixed> / match<key:hop(size, slide)>)"
                .to_string(),
        });
    }
    // top_ties（并列全输出）要求同一 chain 内有前导 sort 提供并列判定键。
    if let Some(conv) = &rule.conv {
        for chain in &conv.chains {
            let mut has_sort = false;
            for step in &chain.steps {
                match step {
                    crate::ast::ConvStep::Sort(_) => has_sort = true,
                    crate::ast::ConvStep::TopTies(_) if !has_sort => errors.push(CheckError {
                        severity: Severity::Error,
                        rule: Some(rule_name.to_string()),
                        test: None,
                        message: "`top_ties(N)` requires a preceding `sort` in the same conv chain (并列判定依赖排序键)"
                            .to_string(),
                    }),
                    _ => {}
                }
            }
        }
    }
}
