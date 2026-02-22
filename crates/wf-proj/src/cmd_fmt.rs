use std::path::PathBuf;
use std::process;

use anyhow::Result;

pub fn run(files: Vec<PathBuf>, write: bool, check: bool) -> Result<()> {
    // tree-sitter-wfl is not yet available; provide a stub that reads and
    // echoes input, or reports "not yet implemented" for --write/--check.

    if files.is_empty() {
        anyhow::bail!("no input files specified");
    }

    for file in &files {
        let source = std::fs::read_to_string(file)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", file.display()))?;

        if check {
            // In check mode, just verify the file is already formatted.
            // Since we don't have a real formatter yet, warn and exit 1.
            eprintln!(
                "warning: formatter not yet implemented (requires tree-sitter-wfl); \
                 cannot verify formatting for {}",
                file.display()
            );
            process::exit(1);
        } else if write {
            eprintln!(
                "warning: formatter not yet implemented (requires tree-sitter-wfl); \
                 {} left unchanged",
                file.display()
            );
        } else {
            // Default: print source to stdout (identity transform)
            print!("{source}");
        }
    }

    Ok(())
}
