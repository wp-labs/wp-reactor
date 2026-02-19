use std::env;
use std::fs;
use std::process;

mod formatter;
mod linter;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: oml-tools <command> [options] [files...]");
        eprintln!();
        eprintln!("Commands:");
        eprintln!("  fmt    Format OML files (prints to stdout, or -w to write in-place)");
        eprintln!("  lint   Lint OML files for common issues");
        eprintln!("  check  Syntax check OML files");
        process::exit(1);
    }

    let command = &args[1];
    let rest: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();

    let exit_code = match command.as_str() {
        "fmt" => cmd_fmt(&rest),
        "lint" => cmd_lint(&rest),
        "check" => cmd_check(&rest),
        _ => {
            eprintln!("Unknown command: {command}");
            1
        }
    };
    process::exit(exit_code);
}

fn parse_oml(source: &str) -> Option<tree_sitter::Tree> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_oml::language())
        .expect("Error loading OML grammar");
    parser.parse(source, None)
}

fn cmd_fmt(args: &[&str]) -> i32 {
    let mut write_in_place = false;
    let mut files = Vec::new();

    for arg in args {
        match *arg {
            "-w" | "--write" => write_in_place = true,
            _ => files.push(*arg),
        }
    }

    if files.is_empty() {
        eprintln!("Usage: oml-tools fmt [-w] <file...>");
        return 1;
    }

    let mut exit_code = 0;
    for file in &files {
        let source = match fs::read_to_string(file) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{file}: {e}");
                exit_code = 1;
                continue;
            }
        };

        let tree = match parse_oml(&source) {
            Some(t) => t,
            None => {
                eprintln!("{file}: failed to parse");
                exit_code = 1;
                continue;
            }
        };

        let formatted = formatter::format(&tree, source.as_bytes());

        if write_in_place {
            if formatted != source {
                if let Err(e) = fs::write(file, &formatted) {
                    eprintln!("{file}: {e}");
                    exit_code = 1;
                }
            }
        } else {
            print!("{formatted}");
        }
    }
    exit_code
}

fn cmd_lint(args: &[&str]) -> i32 {
    if args.is_empty() {
        eprintln!("Usage: oml-tools lint <file...>");
        return 1;
    }

    let mut total_issues = 0;
    for file in args {
        let source = match fs::read_to_string(file) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{file}: {e}");
                total_issues += 1;
                continue;
            }
        };

        let tree = match parse_oml(&source) {
            Some(t) => t,
            None => {
                eprintln!("{file}: failed to parse");
                total_issues += 1;
                continue;
            }
        };

        let diagnostics = linter::lint(&tree, source.as_bytes(), file);
        for d in &diagnostics {
            println!("{d}");
        }
        total_issues += diagnostics.len();
    }

    if total_issues > 0 { 1 } else { 0 }
}

fn cmd_check(args: &[&str]) -> i32 {
    if args.is_empty() {
        eprintln!("Usage: oml-tools check <file...>");
        return 1;
    }

    let mut exit_code = 0;
    for file in args {
        let source = match fs::read_to_string(file) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{file}: error: {e}");
                exit_code = 1;
                continue;
            }
        };

        let tree = match parse_oml(&source) {
            Some(t) => t,
            None => {
                eprintln!("{file}: error: failed to parse");
                exit_code = 1;
                continue;
            }
        };

        let root = tree.root_node();
        if root.has_error() {
            let errors = collect_errors(root, source.as_bytes());
            for (row, col, msg) in &errors {
                eprintln!("{file}:{}:{}: error: {msg}", row + 1, col + 1);
            }
            exit_code = 1;
        } else {
            eprintln!("{file}: ok");
        }
    }
    exit_code
}

fn collect_errors(node: tree_sitter::Node, source: &[u8]) -> Vec<(usize, usize, String)> {
    let mut errors = Vec::new();
    if node.is_error() {
        let start = node.start_position();
        let text = node
            .utf8_text(source)
            .unwrap_or("")
            .chars()
            .take(30)
            .collect::<String>();
        errors.push((start.row, start.column, format!("unexpected: {text}")));
    } else if node.is_missing() {
        let start = node.start_position();
        errors.push((
            start.row,
            start.column,
            format!("missing {}", node.kind()),
        ));
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        errors.extend(collect_errors(child, source));
    }
    errors
}
