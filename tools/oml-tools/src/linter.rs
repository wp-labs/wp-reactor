use std::collections::HashMap;
use std::fmt;
use tree_sitter::{Node, Tree};

#[derive(Debug)]
pub enum Severity {
    Error,
    Warning,
    Info,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Error => write!(f, "error"),
            Severity::Warning => write!(f, "warning"),
            Severity::Info => write!(f, "info"),
        }
    }
}

pub struct Diagnostic {
    pub file: String,
    pub line: usize,
    pub col: usize,
    pub severity: Severity,
    pub message: String,
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}: {}: {}",
            self.file, self.line, self.col, self.severity, self.message
        )
    }
}

pub fn lint(tree: &Tree, source: &[u8], file: &str) -> Vec<Diagnostic> {
    let mut ctx = LintContext {
        source,
        file: file.to_string(),
        diagnostics: Vec::new(),
    };
    ctx.check_all(tree.root_node());
    ctx.diagnostics
}

struct LintContext<'a> {
    source: &'a [u8],
    file: String,
    diagnostics: Vec<Diagnostic>,
}

impl<'a> LintContext<'a> {
    fn text(&self, node: Node) -> &str {
        node.utf8_text(self.source).unwrap_or("")
    }

    fn diag(&mut self, node: Node, severity: Severity, message: String) {
        let pos = node.start_position();
        self.diagnostics.push(Diagnostic {
            file: self.file.clone(),
            line: pos.row + 1,
            col: pos.column + 1,
            severity,
            message,
        });
    }

    fn check_all(&mut self, root: Node) {
        self.check_syntax_errors(root);
        self.check_duplicate_targets(root);
        self.check_match_default_position(root);
        self.check_privacy_references(root);
        self.check_empty_read_take(root);
        self.check_type_literal_mismatch(root);
    }

    // ── Rule 1: Syntax errors ───────────────────────────────────────

    fn check_syntax_errors(&mut self, node: Node) {
        if node.is_error() {
            let text: String = self.text(node).chars().take(40).collect();
            self.diag(node, Severity::Error, format!("syntax error near: {text}"));
            return;
        }
        if node.is_missing() {
            self.diag(
                node,
                Severity::Error,
                format!("missing {}", node.kind()),
            );
            return;
        }
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.check_syntax_errors(child);
        }
    }

    // ── Rule 2: Duplicate target names ──────────────────────────────

    fn check_duplicate_targets(&mut self, root: Node) {
        let mut targets: HashMap<String, Vec<Node>> = HashMap::new();

        let mut cursor = root.walk();
        for child in root.children(&mut cursor) {
            if child.kind() == "aggregate_item" {
                self.collect_target_names(child, &mut targets);
            }
        }

        for (name, nodes) in &targets {
            if name == "_" {
                continue; // underscore targets are intentionally discarded
            }
            if nodes.len() > 1 {
                for node in &nodes[1..] {
                    self.diag(
                        *node,
                        Severity::Warning,
                        format!("duplicate target name '{name}'"),
                    );
                }
            }
        }
    }

    fn collect_target_names<'b>(
        &self,
        agg_item: Node<'b>,
        targets: &mut HashMap<String, Vec<Node<'b>>>,
    ) {
        let mut cursor = agg_item.walk();
        for child in agg_item.children(&mut cursor) {
            if child.kind() == "target_list" {
                let mut cursor2 = child.walk();
                for target in child.children(&mut cursor2) {
                    if target.kind() == "target" {
                        if let Some(name_node) = target.child_by_field_name("name") {
                            let name = self.text(name_node).to_string();
                            targets.entry(name).or_default().push(name_node);
                        }
                    }
                }
            }
        }
    }

    // ── Rule 3: Default arm not last in match ───────────────────────

    fn check_match_default_position(&mut self, node: Node) {
        if node.kind() == "match_expr" {
            let mut cursor = node.walk();
            let children: Vec<Node> = node.children(&mut cursor).collect();

            let arms: Vec<&Node> = children
                .iter()
                .filter(|c| {
                    matches!(
                        c.kind(),
                        "case_arm" | "multi_case_arm" | "default_arm"
                    )
                })
                .collect();

            for (i, arm) in arms.iter().enumerate() {
                if arm.kind() == "default_arm" && i < arms.len() - 1 {
                    self.diag(
                        **arm,
                        Severity::Warning,
                        "default arm '_' is not the last arm; arms below it are unreachable"
                            .to_string(),
                    );
                }
            }
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.check_match_default_position(child);
        }
    }

    // ── Rule 4: Privacy fields referencing undefined targets ────────

    fn check_privacy_references(&mut self, root: Node) {
        // collect all target names
        let mut defined_targets: Vec<String> = Vec::new();
        let mut privacy_items: Vec<Node> = Vec::new();

        let mut cursor = root.walk();
        for child in root.children(&mut cursor) {
            match child.kind() {
                "aggregate_item" => {
                    let mut c2 = child.walk();
                    for sub in child.children(&mut c2) {
                        if sub.kind() == "target_list" {
                            let mut c3 = sub.walk();
                            for target in sub.children(&mut c3) {
                                if target.kind() == "target" {
                                    if let Some(name_node) = target.child_by_field_name("name") {
                                        let name = self.text(name_node).to_string();
                                        if name != "_" && !name.contains('*') {
                                            defined_targets.push(name);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                "privacy_item" => {
                    privacy_items.push(child);
                }
                _ => {}
            }
        }

        for item in &privacy_items {
            if let Some(name_node) = item.child_by_field_name("name") {
                let name = self.text(name_node).to_string();
                if !defined_targets.contains(&name) {
                    self.diag(
                        *item,
                        Severity::Warning,
                        format!(
                            "privacy field '{name}' does not match any defined target"
                        ),
                    );
                }
            }
        }
    }

    // ── Rule 5: Empty read/take with no default ─────────────────────

    fn check_empty_read_take(&mut self, node: Node) {
        if node.kind() == "read_expr" || node.kind() == "take_expr" {
            let named: Vec<Node> = {
                let mut c = node.walk();
                node.named_children(&mut c).collect()
            };

            let has_args = named.iter().any(|c| c.kind() == "arg_list");
            let has_default = named.iter().any(|c| c.kind() == "default_body");

            if !has_args && !has_default {
                let kw = if node.kind() == "read_expr" {
                    "read"
                } else {
                    "take"
                };
                // Check if this is inside a var_get (not standalone)
                if let Some(parent) = node.parent() {
                    if parent.kind() == "aggregate_item" || parent.kind() == "map_item" {
                        self.diag(
                            node,
                            Severity::Info,
                            format!(
                                "{kw}() with no arguments and no default — will use implicit source"
                            ),
                        );
                    }
                }
            }
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.check_empty_read_take(child);
        }
    }

    // ── Rule 6: Type-literal mismatch ───────────────────────────────

    fn check_type_literal_mismatch(&mut self, node: Node) {
        if node.kind() == "value_expr" {
            let mut cursor = node.walk();
            let children: Vec<Node> = node.children(&mut cursor).collect();

            let type_node = children.iter().find(|c| c.kind() == "data_type");
            let literal = children
                .iter()
                .find(|c| {
                    matches!(
                        c.kind(),
                        "string" | "number" | "ip_literal" | "boolean" | "identifier" | "path"
                    )
                });

            if let (Some(dt), Some(lit)) = (type_node, literal) {
                let dtype = self.text(*dt);
                let lkind = lit.kind();

                let mismatch = match (dtype, lkind) {
                    ("digit" | "float", "string") => {
                        // digit("123") is valid — the string might be numeric
                        false
                    }
                    ("ip", "number") => true, // ip(123) is wrong
                    ("bool", "number") => true,
                    ("bool", "string") => true,
                    ("digit" | "float", "ip_literal") => true,
                    ("digit" | "float", "boolean") => true,
                    _ => false,
                };

                if mismatch {
                    self.diag(
                        node,
                        Severity::Warning,
                        format!(
                            "type '{dtype}' may not match literal kind '{lkind}' ({})",
                            self.text(*lit)
                        ),
                    );
                }
            }
        }

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.check_type_literal_mismatch(child);
        }
    }
}
