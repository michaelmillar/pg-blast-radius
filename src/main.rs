use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use pg_blast_radius::analysis;
use pg_blast_radius::catalog::CatalogInfo;
use pg_blast_radius::output;
use pg_blast_radius::rules::{PgVersion, RuleContext};
use pg_blast_radius::types::RiskLevel;

#[derive(Parser)]
#[command(
    name = "pg-blast-radius",
    version,
    about = "PostgreSQL migration risk analyser"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Analyse {
        files: Vec<PathBuf>,

        #[arg(long, default_value = "16")]
        pg_version: u32,

        #[arg(long, default_value = "terminal")]
        format: OutputFormat,

        #[arg(long, default_value = "high")]
        fail_level: RiskLevel,

        #[cfg(feature = "catalog")]
        #[arg(long)]
        dsn: Option<String>,

        #[arg(long)]
        stats_file: Option<PathBuf>,

        #[arg(long, value_name = "LOW:HIGH")]
        io_throughput: Option<String>,
    },

    #[cfg(feature = "catalog")]
    CollectStats {
        #[arg(long)]
        dsn: String,

        #[arg(long)]
        no_workload: bool,
    },
}

#[derive(Clone, clap::ValueEnum)]
enum OutputFormat {
    Terminal,
    Json,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Analyse {
            files,
            pg_version,
            format,
            fail_level,
            #[cfg(feature = "catalog")]
            dsn,
            stats_file,
            io_throughput,
        } => {
            if files.is_empty() {
                anyhow::bail!("No SQL files provided. Usage: pg-blast-radius analyse <files...>");
            }

            let catalog = load_catalog(
                #[cfg(feature = "catalog")]
                dsn.as_deref(),
                stats_file.as_deref(),
            )?;

            let transaction_baseline = catalog
                .as_ref()
                .and_then(|c| c.workload.as_ref())
                .map(|w| &w.transaction_baseline);

            let io_throughput_parsed = match io_throughput {
                Some(ref s) => {
                    let parts: Vec<&str> = s.split(':').collect();
                    if parts.len() != 2 {
                        anyhow::bail!("Expected format: --io-throughput LOW:HIGH (e.g. 200:800)");
                    }
                    let low: f64 = parts[0].parse()
                        .map_err(|_| anyhow::anyhow!("Invalid low throughput: {}", parts[0]))?;
                    let high: f64 = parts[1].parse()
                        .map_err(|_| anyhow::anyhow!("Invalid high throughput: {}", parts[1]))?;
                    if low > high {
                        anyhow::bail!("Low throughput ({low}) must not exceed high ({high})");
                    }
                    if low <= 0.0 {
                        anyhow::bail!("Throughput must be positive");
                    }
                    Some((low, high))
                }
                None => None,
            };

            let ctx = RuleContext {
                pg_version: PgVersion { major: pg_version },
                catalog: catalog.as_ref(),
                transaction_baseline,
                io_throughput: io_throughput_parsed,
            };

            let workload = catalog.as_ref().and_then(|c| c.workload.as_ref());

            let mut results = Vec::new();
            let mut exit_code = 0;

            for file in &files {
                let source = std::fs::read_to_string(file)
                    .map_err(|e| anyhow::anyhow!("Failed to read {}: {e}", file.display()))?;

                let findings = pg_blast_radius::rules::analyse(&source, &ctx)?;
                let result = analysis::build_result(&file.display().to_string(), findings, workload);

                if result.overall_risk >= fail_level {
                    exit_code = 1;
                }

                results.push(result);
            }

            match format {
                OutputFormat::Terminal => output::terminal::render(&results),
                OutputFormat::Json => output::json::render(&results)?,
            }

            std::process::exit(exit_code);
        }

        #[cfg(feature = "catalog")]
        Commands::CollectStats { dsn, no_workload } => {
            let catalog = pg_blast_radius::catalog::live::fetch_catalog(&dsn, !no_workload)?;
            let tables: Vec<_> = catalog
                .tables
                .into_iter()
                .map(|(name, size)| {
                    serde_json::json!({
                        "table_name": name,
                        "total_bytes": size.total_bytes,
                        "row_estimate": size.row_estimate
                    })
                })
                .collect();
            let output = serde_json::json!({
                "tables": tables,
                "workload": catalog.workload
            });
            println!("{}", serde_json::to_string_pretty(&output)?);
            Ok(())
        }
    }
}

fn load_catalog(
    #[cfg(feature = "catalog")] dsn: Option<&str>,
    stats_file: Option<&std::path::Path>,
) -> Result<Option<CatalogInfo>> {
    if let Some(path) = stats_file {
        return Ok(Some(
            pg_blast_radius::catalog::stats_file::load_stats_file(path)?,
        ));
    }

    #[cfg(feature = "catalog")]
    if let Some(dsn) = dsn {
        return Ok(Some(pg_blast_radius::catalog::live::fetch_catalog(dsn, true)?));
    }

    Ok(None)
}
