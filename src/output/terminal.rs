use owo_colors::OwoColorize;

use crate::types::*;

pub fn render(results: &[AnalysisResult]) {
    for result in results {
        println!("{}", result.file.bold());
        println!();

        if !result.blast_radius.per_table.is_empty() {
            println!("  {}", "Blast Radius".underline());
            for t in &result.blast_radius.per_table {
                let lock_str = format!("{}", t.strongest_lock);
                let coloured_lock = if t.blocks_reads {
                    lock_str.red().to_string()
                } else if t.blocks_writes {
                    lock_str.yellow().to_string()
                } else {
                    lock_str.green().to_string()
                };

                let size_info = t
                    .table_size
                    .as_ref()
                    .map(|s| format!(" ({}, ~{} rows)", s.human_size, s.row_estimate))
                    .unwrap_or_default();

                let stmt_info = if t.statement_count > 1 {
                    format!(" ({} statements combined)", t.statement_count)
                } else {
                    String::new()
                };

                println!(
                    "    {}{} -> {}{}",
                    t.table_name.bold(),
                    size_info.dimmed(),
                    coloured_lock,
                    stmt_info
                );

                if t.blocks_reads {
                    println!("      Blocks: {}", "all reads and writes".red());
                } else if t.blocks_writes {
                    println!("      Blocks: {}", "writes (reads OK)".yellow());
                }

                if let Some(ref dur) = t.estimated_total_duration {
                    println!("      Estimated duration: {}", format!("{dur}").bold());
                }

                if let Some(ref rec) = t.recommendation {
                    println!("      {}", rec.yellow());
                }
            }
            println!();
        }

        for finding in &result.findings {
            let badge = risk_badge(finding.risk_level);
            println!(
                "  {} {} [{}]",
                badge,
                finding.summary,
                finding.rule_id.dimmed()
            );
            println!("    {}", finding.explanation.dimmed());

            if let Some(ref dur) = finding.estimated_duration {
                println!("    Estimated: {}", format!("{dur}").bold());
            }

            if let Some(ref note) = finding.pg_version_note {
                println!("    {}: {}", "PG version note".cyan(), note);
            }

            if !finding.assumptions.is_empty() {
                for a in &finding.assumptions {
                    println!("    {}: {}", "Assumption".dimmed(), a.dimmed());
                }
            }

            if let Some(ref recipe) = finding.recipe {
                println!();
                println!(
                    "    {} {}",
                    "Rollout recipe:".green().bold(),
                    recipe.title
                );
                for (i, step) in recipe.steps.iter().enumerate() {
                    let phase = format!("[{}]", step.phase);
                    println!(
                        "      {}. {} {}",
                        i + 1,
                        phase.cyan(),
                        step.description
                    );
                    for line in step.sql.lines() {
                        println!("         {}", line.dimmed());
                    }
                    if step.separate_transaction {
                        println!(
                            "         {}",
                            "(separate transaction)".yellow()
                        );
                    }
                }
            }

            println!();
        }

        let overall = risk_coloured(&format!("{} RISK", result.overall_risk), result.overall_risk);
        println!(
            "  Overall: {} | Confidence: {}",
            overall,
            result.overall_confidence
        );

        let recipe_count = result.findings.iter().filter(|f| f.recipe.is_some()).count();
        let stmt_count = result.findings.len();
        if recipe_count > 0 {
            println!(
                "  {} statements, {} safer alternatives suggested.",
                stmt_count, recipe_count
            );
        }

        if !result.assumptions.is_empty() {
            for a in &result.assumptions {
                println!("  {}: {}", "Assumption".dimmed(), a.dimmed());
            }
        }

        if result.overall_confidence <= Confidence::NeedsCatalog
            && !result
                .blast_radius
                .per_table
                .iter()
                .any(|t| t.table_size.is_some())
        {
            println!(
                "  {}",
                "Use --dsn for table-size-aware analysis.".dimmed()
            );
        }

        println!();
    }
}

fn risk_badge(level: RiskLevel) -> String {
    match level {
        RiskLevel::Low => " LOW ".on_green().black().to_string(),
        RiskLevel::Medium => " MEDIUM ".on_yellow().black().to_string(),
        RiskLevel::High => " HIGH ".on_red().white().to_string(),
        RiskLevel::Extreme => " EXTREME ".on_bright_red().white().bold().to_string(),
    }
}

fn risk_coloured(text: &str, level: RiskLevel) -> String {
    match level {
        RiskLevel::Low => text.green().bold().to_string(),
        RiskLevel::Medium => text.yellow().bold().to_string(),
        RiskLevel::High => text.red().bold().to_string(),
        RiskLevel::Extreme => text.bright_red().bold().to_string(),
    }
}
