use std::collections::{HashMap, HashSet};

use crate::ast::RuleDecl;
use crate::checker::{CheckError, Severity};
use crate::schema::{BaseType, FieldDef, FieldType, WindowSchema};

const INTERMEDIATE_SYSTEM_FIELDS: &[(&str, BaseType)] = &[
    ("__wfu_score", BaseType::Float),
    ("__wfu_rule_name", BaseType::Chars),
    ("__wfu_entity_type", BaseType::Chars),
    ("__wfu_entity_id", BaseType::Chars),
];

pub fn effective_schemas_for_rules(
    rules: &[RuleDecl],
    schemas: &[WindowSchema],
) -> Vec<WindowSchema> {
    let intermediate_targets = intermediate_targets(rules);
    schemas
        .iter()
        .cloned()
        .map(|mut schema| {
            if intermediate_targets.contains(schema.name.as_str()) {
                for (name, base_type) in INTERMEDIATE_SYSTEM_FIELDS {
                    if !schema.fields.iter().any(|field| field.name == *name) {
                        schema.fields.push(FieldDef {
                            name: (*name).to_string(),
                            field_type: FieldType::Base(base_type.clone()),
                        });
                    }
                }
            }
            schema
        })
        .collect()
}

pub fn check_intermediate_target_graph(
    rules: &[RuleDecl],
    errors: &mut Vec<CheckError>,
    rule_name: Option<&str>,
) {
    let produced: HashSet<&str> = rules
        .iter()
        .map(|rule| rule.yield_clause.target.as_str())
        .collect();
    let producer_by_target: HashMap<&str, &str> = rules
        .iter()
        .map(|rule| (rule.yield_clause.target.as_str(), rule.name.as_str()))
        .collect();
    let adjacency: HashMap<String, Vec<String>> =
        rules.iter().fold(HashMap::new(), |mut acc, rule| {
            let deps = acc.entry(rule.yield_clause.target.clone()).or_default();
            for decl in &rule.events.decls {
                if produced.contains(decl.window.as_str())
                    && !deps.iter().any(|window| window == &decl.window)
                {
                    deps.push(decl.window.clone());
                }
            }
            acc
        });

    let mut visited = HashSet::new();
    let mut active = HashSet::new();
    let mut stack = Vec::new();

    for node in adjacency.keys() {
        if let Some(cycle) = detect_cycle(node, &adjacency, &mut visited, &mut active, &mut stack) {
            let diagnostic_rule = rule_name.or_else(|| {
                cycle
                    .first()
                    .and_then(|target| producer_by_target.get(target.as_str()).copied())
            });
            errors.push(CheckError {
                severity: Severity::Error,
                rule: diagnostic_rule.map(str::to_string),
                test: None,
                message: format!(
                    "yield targets consumed by downstream rules must be acyclic; found cycle: {}",
                    cycle.join(" -> ")
                ),
            });
            return;
        }
    }
}

fn intermediate_targets(rules: &[RuleDecl]) -> HashSet<&str> {
    let consumed_windows: HashSet<&str> = rules
        .iter()
        .flat_map(|rule| rule.events.decls.iter().map(|decl| decl.window.as_str()))
        .collect();

    rules
        .iter()
        .map(|rule| rule.yield_clause.target.as_str())
        .filter(|target| consumed_windows.contains(*target))
        .collect()
}

fn detect_cycle(
    node: &str,
    adjacency: &HashMap<String, Vec<String>>,
    visited: &mut HashSet<String>,
    active: &mut HashSet<String>,
    stack: &mut Vec<String>,
) -> Option<Vec<String>> {
    if active.contains(node) {
        let start = stack
            .iter()
            .position(|entry| entry == node)
            .unwrap_or(stack.len());
        let mut cycle = stack[start..].to_vec();
        cycle.push(node.to_string());
        return Some(cycle);
    }
    if !visited.insert(node.to_string()) {
        return None;
    }

    active.insert(node.to_string());
    stack.push(node.to_string());

    if let Some(neighbors) = adjacency.get(node) {
        for neighbor in neighbors {
            if let Some(cycle) = detect_cycle(neighbor, adjacency, visited, active, stack) {
                return Some(cycle);
            }
        }
    }

    stack.pop();
    active.remove(node);
    None
}
