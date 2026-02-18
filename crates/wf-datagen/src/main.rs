use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};

use wf_datagen::datagen::generate;
use wf_datagen::output::arrow_ipc::write_arrow_ipc;
use wf_datagen::output::jsonl::write_jsonl;
use wf_datagen::validate::validate_wsc;
use wf_datagen::wsc_ast::WscFile;
use wf_datagen::wsc_parser::parse_wsc;

#[derive(Parser)]
#[command(name = "wf-datagen", about = "WarpFusion test data generator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate test data from a .wsc scenario file
    Gen {
        /// Path to the .wsc scenario file
        #[arg(long)]
        scenario: PathBuf,

        /// Output format: "jsonl" or "arrow" ("arrow-ipc"/"ipc" aliases)
        #[arg(long, default_value = "jsonl")]
        format: String,

        /// Output directory
        #[arg(long)]
        out: PathBuf,

        /// Additional .ws schema files (beyond those in `use` declarations)
        #[arg(long)]
        ws: Vec<PathBuf>,

        /// Additional .wfl rule files (beyond those in `use` declarations)
        #[arg(long)]
        wfl: Vec<PathBuf>,
    },
    /// Lint (validate) a .wsc scenario file
    Lint {
        /// Path to the .wsc scenario file
        scenario: PathBuf,

        /// Additional .ws schema files (beyond those in `use` declarations)
        #[arg(long)]
        ws: Vec<PathBuf>,

        /// Additional .wfl rule files (beyond those in `use` declarations)
        #[arg(long)]
        wfl: Vec<PathBuf>,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Gen {
            scenario,
            format,
            out,
            ws,
            wfl,
        } => {
            let normalized_format = match format.as_str() {
                "jsonl" => "jsonl",
                "arrow" | "arrow-ipc" | "ipc" => "arrow",
                _ => "",
            };
            if normalized_format.is_empty() {
                anyhow::bail!(
                    "unsupported format: '{}'. Supported: 'jsonl', 'arrow' ('arrow-ipc' alias).",
                    format
                );
            }

            let wsc_content = std::fs::read_to_string(&scenario).context("reading .wsc file")?;
            let wsc = parse_wsc(&wsc_content).context("parsing .wsc file")?;

            let (mut schemas, mut wfl_files) = load_from_uses(&wsc, &scenario)?;
            schemas.extend(load_ws_files(&ws)?);
            wfl_files.extend(load_wfl_files(&wfl)?);

            let errors = validate_wsc(&wsc, &schemas, &wfl_files);
            if !errors.is_empty() {
                eprintln!("Validation errors:");
                for e in &errors {
                    eprintln!("  {}", e);
                }
                anyhow::bail!("{} validation error(s) found", errors.len());
            }

            let result = generate(&wsc, &schemas)?;

            match normalized_format {
                "jsonl" => {
                    let output_file = out.join(format!("{}.jsonl", wsc.scenario.name));
                    write_jsonl(&result.events, &output_file)?;
                    println!(
                        "Generated {} events -> {}",
                        result.events.len(),
                        output_file.display()
                    );
                }
                "arrow" => {
                    let output_file = out.join(format!("{}.arrow", wsc.scenario.name));
                    write_arrow_ipc(&result.events, &output_file)?;
                    println!(
                        "Generated {} events -> {}",
                        result.events.len(),
                        output_file.display()
                    );
                }
                _ => unreachable!(),
            }

            Ok(())
        }
        Commands::Lint { scenario, ws, wfl } => {
            let wsc_content = std::fs::read_to_string(&scenario).context("reading .wsc file")?;
            let wsc = parse_wsc(&wsc_content).context("parsing .wsc file")?;

            let (mut schemas, mut wfl_files) = load_from_uses(&wsc, &scenario)?;
            schemas.extend(load_ws_files(&ws)?);
            wfl_files.extend(load_wfl_files(&wfl)?);

            let errors = validate_wsc(&wsc, &schemas, &wfl_files);
            if errors.is_empty() {
                println!("OK");
            } else {
                for e in &errors {
                    eprintln!("{}", e);
                }
                std::process::exit(1);
            }
            Ok(())
        }
    }
}

/// Auto-load .ws and .wfl files referenced by `use` declarations in the .wsc file.
///
/// Paths in `use` declarations are resolved relative to the .wsc file's directory.
fn load_from_uses(
    wsc: &WscFile,
    wsc_path: &Path,
) -> anyhow::Result<(Vec<wf_lang::WindowSchema>, Vec<wf_lang::ast::WflFile>)> {
    let base_dir = wsc_path.parent().unwrap_or_else(|| Path::new("."));

    let mut schemas = Vec::new();
    let mut wfl_files = Vec::new();

    for use_decl in &wsc.uses {
        let resolved = base_dir.join(&use_decl.path);
        let ext = resolved.extension().and_then(|e| e.to_str()).unwrap_or("");

        match ext {
            "ws" => {
                let content = std::fs::read_to_string(&resolved).with_context(|| {
                    format!(
                        "reading .ws file from use declaration: {} (resolved: {})",
                        use_decl.path,
                        resolved.display()
                    )
                })?;
                let parsed = wf_lang::parse_ws(&content)
                    .with_context(|| format!("parsing .ws file: {}", resolved.display()))?;
                schemas.extend(parsed);
            }
            "wfl" => {
                let content = std::fs::read_to_string(&resolved).with_context(|| {
                    format!(
                        "reading .wfl file from use declaration: {} (resolved: {})",
                        use_decl.path,
                        resolved.display()
                    )
                })?;
                let parsed = wf_lang::parse_wfl(&content)
                    .with_context(|| format!("parsing .wfl file: {}", resolved.display()))?;
                wfl_files.push(parsed);
            }
            other => {
                anyhow::bail!(
                    "unsupported file extension '{}' in use declaration: {}",
                    other,
                    use_decl.path
                );
            }
        }
    }

    Ok((schemas, wfl_files))
}

fn load_ws_files(paths: &[PathBuf]) -> anyhow::Result<Vec<wf_lang::WindowSchema>> {
    let mut schemas = Vec::new();
    for path in paths {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("reading .ws file: {}", path.display()))?;
        let parsed = wf_lang::parse_ws(&content)
            .with_context(|| format!("parsing .ws file: {}", path.display()))?;
        schemas.extend(parsed);
    }
    Ok(schemas)
}

fn load_wfl_files(paths: &[PathBuf]) -> anyhow::Result<Vec<wf_lang::ast::WflFile>> {
    let mut files = Vec::new();
    for path in paths {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("reading .wfl file: {}", path.display()))?;
        let parsed = wf_lang::parse_wfl(&content)
            .with_context(|| format!("parsing .wfl file: {}", path.display()))?;
        files.push(parsed);
    }
    Ok(files)
}
