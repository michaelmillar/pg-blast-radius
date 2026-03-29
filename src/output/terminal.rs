use owo_colors::OwoColorize;

use crate::types::*;

pub fn render(results: &[AnalysisResult]) {
    for result in results {
        println!("{}", result.file.bold());
        println!();

        for t in &result.blast_radius.per_table {
            let size_info = t
                .table_size
                .as_ref()
                .map(|s| format!(" ({}, ~{} rows)", s.human_size, s.row_estimate))
                .unwrap_or_default();

            println!("  {}{}", t.table_name.bold(), size_info.dimmed());

            let lock_str = format!("{}", t.strongest_lock);
            let lock_display = if t.blocks_reads {
                format!("{} (blocks all reads and writes)", lock_str.red())
            } else if t.blocks_writes {
                format!("{} (blocks writes)", lock_str.yellow())
            } else {
                format!("{} (non-blocking)", lock_str.green())
            };
            println!("    Lock: {lock_display}");

            if let Some(ref dur) = t.duration_forecast {
                println!("    Duration: {}", format!("{dur}").bold());
            } else if t.table_size.is_none() {
                println!(
                    "    Duration: {}",
                    "unknown (use --dsn or --stats-file for size-aware estimates)".dimmed()
                );
            }

            if !t.blocked_queries.is_empty() {
                let total_qpm: f64 = t.blocked_queries.iter().map(|bq| bq.calls_per_sec * 60.0).sum();
                println!(
                    "    Blocked queries: {} families, {:.0} calls/min combined",
                    t.blocked_queries.len(),
                    total_qpm,
                );
                for bq in &t.blocked_queries {
                    println!(
                        "      {}  {}{:.0}/min  ~{} queued (p50)",
                        bq.query_label.dimmed(),
                        " ".repeat(max_label_pad(&t.blocked_queries, &bq.query_label)),
                        bq.calls_per_sec * 60.0,
                        bq.queued_at_p50,
                    );
                }
            } else if t.total_blocked_qps == 0.0 && t.confidence.grade < ConfidenceGrade::Measured
                && (t.blocks_reads || t.blocks_writes) {
                    println!(
                        "    Blocked queries: {}",
                        "unknown (use --dsn for workload-aware analysis)".dimmed()
                    );
                }

            render_confidence(&t.confidence);

            if t.statement_count > 1 {
                println!(
                    "    {} statements combined",
                    format!("{}", t.statement_count).bold()
                );
            }

            if let Some(ref rec) = t.recommendation {
                println!("    {}", rec.yellow());
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

            if let Some(ref dur) = finding.duration_forecast {
                println!("    Estimated: {}", format!("{dur}").bold());
            }

            if let Some(ref note) = finding.pg_version_note {
                println!("    {}: {}", "PG version note".cyan(), note);
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

        if result.overall_confidence == ConfidenceGrade::Static {
            println!(
                "  {}",
                "Use --dsn for workload-aware blast radius analysis.".dimmed()
            );
        } else if result.overall_confidence == ConfidenceGrade::Estimated {
            println!(
                "  {}",
                "Use --dsn for blocked query forecasting (pg_stat_statements required).".dimmed()
            );
        }

        println!();
    }
}

fn render_confidence(ledger: &ConfidenceLedger) {
    let parts: Vec<String> = [
        if !ledger.from_catalog.is_empty() {
            Some("table size KNOWN".into())
        } else if ledger.unknowns.iter().any(|u| u.contains("table size")) {
            Some("table size UNKNOWN".into())
        } else {
            None
        },
        if !ledger.from_stats.is_empty() {
            Some("query load MEASURED".into())
        } else if ledger.unknowns.iter().any(|u| u.contains("query load")) {
            Some("query load UNKNOWN".into())
        } else {
            None
        },
        if ledger.grade == ConfidenceGrade::Measured {
            Some("lock hold INFERRED".into())
        } else if ledger.grade == ConfidenceGrade::Estimated {
            Some("lock hold ESTIMATED".into())
        } else {
            None
        },
    ]
    .into_iter()
    .flatten()
    .collect();

    if !parts.is_empty() {
        println!("    Confidence: {}", parts.join(", ").dimmed());
    }
}

fn max_label_pad(blocked: &[BlockedQueryForecast], current_label: &str) -> usize {
    let max_len = blocked.iter().map(|bq| bq.query_label.len()).max().unwrap_or(0);
    max_len.saturating_sub(current_label.len()) + 2
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
