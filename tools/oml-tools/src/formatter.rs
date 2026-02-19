use tree_sitter::{Node, Tree};

const INDENT: &str = "    ";

pub fn format(tree: &Tree, source: &[u8]) -> String {
    let mut f = Formatter {
        source,
        out: String::with_capacity(source.len()),
        depth: 0,
    };
    f.format_node(tree.root_node());
    if !f.out.ends_with('\n') {
        f.out.push('\n');
    }
    f.out
}

fn txt<'a>(source: &'a [u8], node: Node) -> &'a str {
    node.utf8_text(source).unwrap_or("")
}

fn named_children(node: Node) -> Vec<Node> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).collect()
}

fn all_children(node: Node) -> Vec<Node> {
    let mut cursor = node.walk();
    node.children(&mut cursor).collect()
}

struct Formatter<'a> {
    source: &'a [u8],
    out: String,
    depth: usize,
}

impl<'a> Formatter<'a> {
    fn push_indent(&mut self) {
        for _ in 0..self.depth {
            self.out.push_str(INDENT);
        }
    }

    fn ensure_blank_line(&mut self) {
        if !self.out.ends_with("\n\n") {
            if self.out.ends_with('\n') {
                self.out.push('\n');
            } else {
                self.out.push_str("\n\n");
            }
        }
    }

    fn push_text(&mut self, node: Node) {
        self.out.push_str(txt(self.source, node));
    }

    // ── Dispatch ────────────────────────────────────────────────────

    fn format_node(&mut self, node: Node) {
        match node.kind() {
            "source_file" => self.fmt_source_file(node),
            "header" => self.fmt_header(node),
            "name_field" => self.fmt_name_field(node),
            "rule_field" => self.fmt_rule_field(node),
            "separator" => self.out.push_str("---"),
            "aggregate_item" => self.fmt_aggregate_item(node),
            "target_list" => self.fmt_target_list(node),
            "target" => self.fmt_target(node),
            "target_name" => self.fmt_target_name(node),
            "read_expr" => self.fmt_read_take(node, "read"),
            "take_expr" => self.fmt_read_take(node, "take"),
            "pipe_expr" => self.fmt_pipe_expr(node),
            "pipe_fun" => self.fmt_pipe_fun(node),
            "fmt_expr" => self.fmt_fmt_expr(node),
            "object_expr" => self.fmt_object_expr(node),
            "collect_expr" => self.fmt_collect_expr(node),
            "match_expr" => self.fmt_match_expr(node),
            "case_arm" => self.fmt_case_arm(node),
            "multi_case_arm" => self.fmt_multi_case_arm(node),
            "default_arm" => self.fmt_default_arm(node),
            "condition" => self.fmt_condition(node),
            "in_condition" => self.fmt_in_condition(node),
            "not_condition" => self.fmt_not_condition(node),
            "sql_expr" => self.fmt_sql_expr(node),
            "sql_columns" => self.fmt_sql_columns(node),
            "sql_condition" => self.fmt_sql_condition(node),
            "sql_comparison" => self.fmt_sql_comparison(node),
            "sql_not" => self.fmt_sql_not(node),
            "sql_fun_call" => self.fmt_sql_fun_call(node),
            "value_expr" => self.fmt_value_expr(node),
            "fun_call" => self.fmt_fun_call(node),
            "var_get" => self.fmt_var_get(node),
            "at_ref" => self.fmt_at_ref(node),
            "arg_list" => self.fmt_arg_list(node),
            "option_arg" => self.fmt_option_arg(node),
            "keys_arg" => self.fmt_keys_arg(node),
            "get_arg" => self.fmt_get_arg(node),
            "default_body" => self.fmt_default_body(node),
            "map_item" => self.fmt_map_item(node),
            "map_targets" => self.fmt_map_targets(node),
            "privacy_item" => self.fmt_privacy_item(node),
            "comment" => {
                let t = txt(self.source, node).trim().to_string();
                self.out.push_str(&t);
            }
            _ => self.push_text(node),
        }
    }

    // ── Source file ─────────────────────────────────────────────────

    fn fmt_source_file(&mut self, node: Node) {
        let children = all_children(node);
        let mut prev_kind: Option<&str> = None;

        for child in &children {
            let kind = child.kind();

            match (prev_kind, kind) {
                (Some(_), "separator") => self.ensure_blank_line(),
                (Some("separator"), _) => self.out.push('\n'),
                (Some("aggregate_item"), "aggregate_item") => self.out.push('\n'),
                (Some("privacy_item"), "privacy_item") => self.out.push('\n'),
                (Some("comment"), _) => self.out.push('\n'),
                (Some(_), "comment") => self.out.push('\n'),
                _ => {}
            }

            self.format_node(*child);
            prev_kind = Some(kind);
        }
    }

    // ── Header ──────────────────────────────────────────────────────

    fn fmt_header(&mut self, node: Node) {
        let named = named_children(node);
        for (i, child) in named.iter().enumerate() {
            if i > 0 {
                self.out.push('\n');
            }
            self.format_node(*child);
        }
    }

    fn fmt_name_field(&mut self, node: Node) {
        let children = all_children(node);
        self.out.push_str("name");
        for child in &children {
            match child.kind() {
                "name" | ":" => {}
                "identifier" | "path" => {
                    self.out.push_str(" : ");
                    self.push_text(*child);
                }
                "comment" => {
                    self.out.push(' ');
                    let t = txt(self.source, *child).trim().to_string();
                    self.out.push_str(&t);
                }
                _ => {}
            }
        }
    }

    fn fmt_rule_field(&mut self, node: Node) {
        let children = all_children(node);
        self.out.push_str("rule");
        let mut after_colon = false;
        for child in &children {
            match child.kind() {
                "rule" => {}
                ":" => after_colon = true,
                "identifier" | "path" if after_colon => {
                    self.out.push_str(" : ");
                    self.push_text(*child);
                    after_colon = false;
                }
                "identifier" | "path" => {
                    self.out.push(' ');
                    self.push_text(*child);
                }
                _ => {}
            }
        }
    }

    // ── Aggregate items ─────────────────────────────────────────────

    fn fmt_aggregate_item(&mut self, node: Node) {
        self.push_indent();
        let children = all_children(node);
        for child in &children {
            match child.kind() {
                "target_list" => self.format_node(*child),
                "=" => self.out.push_str(" = "),
                ";" => self.out.push_str(" ;"),
                "comment" => {
                    self.out.push(' ');
                    let t = txt(self.source, *child).trim().to_string();
                    self.out.push_str(&t);
                }
                _ if child.is_named() => self.format_node(*child),
                _ => {}
            }
        }
    }

    fn fmt_target_list(&mut self, node: Node) {
        let named = named_children(node);
        for (i, child) in named.iter().enumerate() {
            if i > 0 {
                self.out.push_str(", ");
            }
            self.format_node(*child);
        }
    }

    fn fmt_target(&mut self, node: Node) {
        let children = all_children(node);
        let mut wrote_name = false;
        for child in &children {
            match child.kind() {
                "target_name" => {
                    self.format_node(*child);
                    wrote_name = true;
                }
                ":" if wrote_name => self.out.push_str(" : "),
                "data_type" => self.push_text(*child),
                _ => {}
            }
        }
    }

    fn fmt_target_name(&mut self, node: Node) {
        let children = all_children(node);
        for child in &children {
            self.push_text(*child);
        }
    }

    // ── Read / Take ─────────────────────────────────────────────────

    fn fmt_read_take(&mut self, node: Node, kw: &str) {
        let children = all_children(node);
        self.out.push_str(kw);
        self.out.push('(');
        let mut in_parens = false;
        for child in &children {
            match child.kind() {
                "read" | "take" => {}
                "(" => in_parens = true,
                ")" if in_parens => {
                    self.out.push(')');
                    in_parens = false;
                }
                "arg_list" => self.format_node(*child),
                "default_body" => {
                    self.out.push(' ');
                    self.format_node(*child);
                }
                _ => {}
            }
        }
    }

    fn fmt_default_body(&mut self, node: Node) {
        let children = all_children(node);
        self.out.push_str("{ _ : ");
        for child in &children {
            match child.kind() {
                "{" | "}" | "_" | ":" | ";" => {}
                _ if child.is_named() => self.format_node(*child),
                _ => {}
            }
        }
        self.out.push_str(" }");
    }

    // ── Pipe ────────────────────────────────────────────────────────

    fn fmt_pipe_expr(&mut self, node: Node) {
        let children = all_children(node);
        let has_pipe_kw = children.iter().any(|c| c.kind() == "pipe");

        if has_pipe_kw {
            self.out.push_str("pipe ");
        }

        for child in &children {
            match child.kind() {
                "pipe" => {}
                "var_get" => self.format_node(*child),
                "|" => self.out.push_str(" | "),
                "pipe_fun" => self.format_node(*child),
                _ => {}
            }
        }
    }

    fn fmt_pipe_fun(&mut self, node: Node) {
        let children = all_children(node);
        if children.len() == 1 {
            self.push_text(children[0]);
        } else {
            for child in &children {
                match child.kind() {
                    "(" => self.out.push('('),
                    ")" => self.out.push(')'),
                    "," => self.out.push_str(", "),
                    _ => self.push_text(*child),
                }
            }
        }
    }

    // ── Fmt expression ──────────────────────────────────────────────

    fn fmt_fmt_expr(&mut self, node: Node) {
        let children = all_children(node);
        self.out.push_str("fmt(");
        let mut in_args = false;
        let mut first_arg = true;
        for child in &children {
            match child.kind() {
                "fmt" => {}
                "(" => in_args = true,
                ")" => self.out.push(')'),
                "," => self.out.push_str(", "),
                "string" if in_args && first_arg => {
                    self.push_text(*child);
                    first_arg = false;
                }
                _ if child.is_named() => self.format_node(*child),
                _ => {}
            }
        }
    }

    // ── Object ──────────────────────────────────────────────────────

    fn fmt_object_expr(&mut self, node: Node) {
        let named = named_children(node);
        self.out.push_str("object {\n");
        self.depth += 1;
        for child in &named {
            match child.kind() {
                "map_item" => {
                    self.push_indent();
                    self.format_node(*child);
                    self.out.push('\n');
                }
                "comment" => {
                    self.push_indent();
                    let t = txt(self.source, *child).trim().to_string();
                    self.out.push_str(&t);
                    self.out.push('\n');
                }
                _ => {}
            }
        }
        self.depth -= 1;
        self.push_indent();
        self.out.push('}');
    }

    fn fmt_map_item(&mut self, node: Node) {
        let children = all_children(node);
        for child in &children {
            match child.kind() {
                "map_targets" => self.format_node(*child),
                "=" => self.out.push_str(" = "),
                ";" => self.out.push_str(" ;"),
                _ if child.is_named() => self.format_node(*child),
                _ => {}
            }
        }
    }

    fn fmt_map_targets(&mut self, node: Node) {
        let children = all_children(node);
        for child in &children {
            match child.kind() {
                "identifier" => self.push_text(*child),
                "," => self.out.push_str(", "),
                ":" => self.out.push_str(" : "),
                "data_type" => self.push_text(*child),
                _ => {}
            }
        }
    }

    // ── Collect ─────────────────────────────────────────────────────

    fn fmt_collect_expr(&mut self, node: Node) {
        self.out.push_str("collect ");
        let named = named_children(node);
        for child in &named {
            self.format_node(*child);
        }
    }

    // ── Match ───────────────────────────────────────────────────────

    fn fmt_match_expr(&mut self, node: Node) {
        let children = all_children(node);
        let is_multi = children
            .iter()
            .any(|c| c.kind() == "multi_case_arm");

        // Count var_get nodes to also detect multi via parenthesized syntax
        let var_gets: Vec<Node> = children
            .iter()
            .filter(|c| c.kind() == "var_get")
            .copied()
            .collect();
        let is_multi = is_multi || var_gets.len() > 1;

        self.out.push_str("match ");

        if is_multi {
            self.out.push('(');
            for (i, vg) in var_gets.iter().enumerate() {
                if i > 0 {
                    self.out.push_str(", ");
                }
                self.format_node(*vg);
            }
            self.out.push_str(") {\n");
        } else {
            if let Some(vg) = var_gets.first() {
                self.format_node(*vg);
            }
            self.out.push_str(" {\n");
        }

        self.depth += 1;
        for child in &children {
            match child.kind() {
                "case_arm" | "multi_case_arm" | "default_arm" => {
                    self.push_indent();
                    self.format_node(*child);
                    self.out.push('\n');
                }
                "comment" => {
                    self.push_indent();
                    let t = txt(self.source, *child).trim().to_string();
                    self.out.push_str(&t);
                    self.out.push('\n');
                }
                _ => {}
            }
        }
        self.depth -= 1;
        self.push_indent();
        self.out.push('}');
    }

    fn fmt_case_arm(&mut self, node: Node) {
        let children = all_children(node);
        let mut wrote_arrow = false;
        for child in &children {
            match child.kind() {
                "condition" if !wrote_arrow => self.format_node(*child),
                "=>" => {
                    self.out.push_str(" => ");
                    wrote_arrow = true;
                }
                "," | ";" => {}
                _ if child.is_named() && wrote_arrow => self.format_node(*child),
                _ => {}
            }
        }
        self.out.push_str(" ;");
    }

    fn fmt_multi_case_arm(&mut self, node: Node) {
        let children = all_children(node);
        let conditions: Vec<Node> = children
            .iter()
            .filter(|c| c.kind() == "condition")
            .copied()
            .collect();

        self.out.push('(');
        for (i, cond) in conditions.iter().enumerate() {
            if i > 0 {
                self.out.push_str(", ");
            }
            self.format_node(*cond);
        }
        self.out.push(')');

        let mut after_arrow = false;
        for child in &children {
            match child.kind() {
                "=>" => {
                    self.out.push_str(" => ");
                    after_arrow = true;
                }
                "," | ";" | "(" | ")" | "condition" => {}
                _ if child.is_named() && after_arrow => {
                    self.format_node(*child);
                    after_arrow = false;
                }
                _ => {}
            }
        }
        self.out.push_str(" ;");
    }

    fn fmt_default_arm(&mut self, node: Node) {
        let children = all_children(node);
        self.out.push_str("_ => ");
        let mut after_arrow = false;
        for child in &children {
            match child.kind() {
                "=>" => after_arrow = true,
                "_" | "," | ";" => {}
                _ if child.is_named() && after_arrow => self.format_node(*child),
                _ => {}
            }
        }
        self.out.push_str(" ;");
    }

    fn fmt_condition(&mut self, node: Node) {
        let children = all_children(node);
        for child in &children {
            match child.kind() {
                "|" => self.out.push_str(" | "),
                _ if child.is_named() => self.format_node(*child),
                _ => {}
            }
        }
    }

    fn fmt_in_condition(&mut self, node: Node) {
        let named = named_children(node);
        self.out.push_str("in(");
        for (i, child) in named.iter().enumerate() {
            if i > 0 {
                self.out.push_str(", ");
            }
            self.format_node(*child);
        }
        self.out.push(')');
    }

    fn fmt_not_condition(&mut self, node: Node) {
        self.out.push('!');
        for child in &named_children(node) {
            self.format_node(*child);
        }
    }

    // ── SQL ─────────────────────────────────────────────────────────

    fn fmt_sql_expr(&mut self, node: Node) {
        let children = all_children(node);
        self.out.push_str("select ");
        let mut state = 0u8; // 0=pre-columns, 1=post-columns, 2=post-from
        for child in &children {
            match child.kind() {
                "select" => {}
                "sql_columns" => {
                    self.format_node(*child);
                    state = 1;
                }
                "from" if state == 1 => {
                    self.out.push_str(" from ");
                    state = 2;
                }
                "identifier" if state == 2 => {
                    self.push_text(*child);
                    state = 3;
                }
                "where" => self.out.push_str(" where "),
                "sql_condition" => self.format_node(*child),
                _ => {}
            }
        }
    }

    fn fmt_sql_columns(&mut self, node: Node) {
        let children = all_children(node);
        let mut first = true;
        for child in &children {
            match child.kind() {
                "*" => self.out.push('*'),
                "identifier" => {
                    if !first {
                        self.out.push_str(", ");
                    }
                    self.push_text(*child);
                    first = false;
                }
                _ => {}
            }
        }
    }

    fn fmt_sql_condition(&mut self, node: Node) {
        let children = all_children(node);
        for child in &children {
            match child.kind() {
                "and" => self.out.push_str(" and "),
                "or" => self.out.push_str(" or "),
                "(" => self.out.push('('),
                ")" => self.out.push(')'),
                _ if child.is_named() => self.format_node(*child),
                _ => {}
            }
        }
    }

    fn fmt_sql_comparison(&mut self, node: Node) {
        let children = all_children(node);
        let mut first = true;
        for child in &children {
            match child.kind() {
                "identifier" if first => {
                    self.push_text(*child);
                    first = false;
                }
                "sql_op" => {
                    self.out.push(' ');
                    self.push_text(*child);
                    self.out.push(' ');
                }
                _ if child.is_named() => self.format_node(*child),
                _ => {}
            }
        }
    }

    fn fmt_sql_not(&mut self, node: Node) {
        self.out.push_str("not ");
        for child in &named_children(node) {
            self.format_node(*child);
        }
    }

    fn fmt_sql_fun_call(&mut self, node: Node) {
        let children = all_children(node);
        let mut wrote_name = false;
        for child in &children {
            match child.kind() {
                "identifier" if !wrote_name => {
                    self.push_text(*child);
                    wrote_name = true;
                }
                "(" => self.out.push('('),
                ")" => self.out.push(')'),
                _ if child.is_named() => self.format_node(*child),
                _ => {}
            }
        }
    }

    // ── Value / Fun call / Var get ──────────────────────────────────

    fn fmt_value_expr(&mut self, node: Node) {
        let children = all_children(node);
        for child in &children {
            match child.kind() {
                "data_type" => self.push_text(*child),
                "(" => self.out.push('('),
                ")" => self.out.push(')'),
                _ if child.is_named() => self.push_text(*child),
                _ => {}
            }
        }
    }

    fn fmt_fun_call(&mut self, node: Node) {
        let children = all_children(node);
        for child in &children {
            match child.kind() {
                "(" => self.out.push('('),
                ")" => self.out.push(')'),
                _ => self.push_text(*child),
            }
        }
    }

    fn fmt_var_get(&mut self, node: Node) {
        let children = all_children(node);
        let has_at_ref = children.iter().any(|c| c.kind() == "at_ref");
        if has_at_ref {
            for child in &children {
                if child.kind() == "at_ref" {
                    self.format_node(*child);
                }
            }
        } else {
            for child in &children {
                match child.kind() {
                    "read" | "take" => self.push_text(*child),
                    "(" => self.out.push('('),
                    ")" => self.out.push(')'),
                    "arg_list" => self.format_node(*child),
                    _ => {}
                }
            }
        }
    }

    fn fmt_at_ref(&mut self, node: Node) {
        self.out.push('@');
        if let Some(ident) = named_children(node).first() {
            self.push_text(*ident);
        }
    }

    // ── Arguments ───────────────────────────────────────────────────

    fn fmt_arg_list(&mut self, node: Node) {
        let children = all_children(node);
        for child in &children {
            match child.kind() {
                "," => self.out.push_str(", "),
                _ if child.is_named() => self.format_node(*child),
                _ => {}
            }
        }
    }

    fn fmt_option_arg(&mut self, node: Node) {
        let children = all_children(node);
        self.out.push_str("option:[");
        let idents: Vec<Node> = children
            .iter()
            .filter(|c| c.kind() == "identifier")
            .copied()
            .collect();
        for (i, ident) in idents.iter().enumerate() {
            if i > 0 {
                self.out.push_str(", ");
            }
            self.push_text(*ident);
        }
        self.out.push(']');
    }

    fn fmt_keys_arg(&mut self, node: Node) {
        let children = all_children(node);
        let kw = children
            .iter()
            .find(|c| c.kind() == "in" || c.kind() == "keys")
            .map(|c| txt(self.source, *c).to_string())
            .unwrap_or_else(|| "keys".to_string());
        self.out.push_str(&kw);
        self.out.push_str(":[");
        let items: Vec<Node> = children
            .iter()
            .filter(|c| c.kind() == "identifier" || c.kind() == "wild_key")
            .copied()
            .collect();
        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                self.out.push_str(", ");
            }
            self.push_text(*item);
        }
        self.out.push(']');
    }

    fn fmt_get_arg(&mut self, node: Node) {
        let children = all_children(node);
        self.out.push_str("get:");
        for child in &children {
            match child.kind() {
                "get" | ":" => {}
                _ if child.is_named() => self.push_text(*child),
                _ => {}
            }
        }
    }

    // ── Privacy ─────────────────────────────────────────────────────

    fn fmt_privacy_item(&mut self, node: Node) {
        let children = all_children(node);
        let mut wrote_name = false;
        for child in &children {
            match child.kind() {
                "identifier" if !wrote_name => {
                    self.push_text(*child);
                    wrote_name = true;
                }
                ":" => self.out.push_str(" : "),
                "privacy_type" => self.push_text(*child),
                _ => {}
            }
        }
    }
}
