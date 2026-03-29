use std::collections::HashMap;

use crate::forecast;
use crate::types::*;
use crate::workload::WorkloadProfile;

pub fn build_result(
    file: &str,
    findings: Vec<Finding>,
    workload: Option<&WorkloadProfile>,
) -> AnalysisResult {
    let mut per_table: HashMap<String, TableBlastRadius> = HashMap::new();

    for f in &findings {
        let Some(ref table) = f.affected_table else {
            continue;
        };
        let entry = per_table
            .entry(table.clone())
            .or_insert_with(|| TableBlastRadius {
                table_name: table.clone(),
                strongest_lock: LockMode::AccessShare,
                blocks_reads: false,
                blocks_writes: false,
                statement_count: 0,
                table_size: None,
                duration_forecast: None,
                blocked_queries: vec![],
                total_blocked_qps: 0.0,
                confidence: ConfidenceLedger::static_only(vec![]),
                recommendation: None,
            });

        if f.lock_mode > entry.strongest_lock {
            entry.strongest_lock = f.lock_mode;
        }
        entry.blocks_reads = entry.strongest_lock.blocks_reads();
        entry.blocks_writes = entry.strongest_lock.blocks_writes();
        entry.statement_count += 1;

        if let Some(ref est) = f.duration_forecast {
            entry.duration_forecast = Some(match entry.duration_forecast.take() {
                Some(existing) => DurationForecast {
                    p50_seconds: existing.p50_seconds + est.p50_seconds,
                    p90_seconds: existing.p90_seconds + est.p90_seconds,
                    worst_seconds: existing.worst_seconds + est.worst_seconds,
                    assumptions: existing.assumptions,
                },
                None => est.clone(),
            });
        }
    }

    for entry in per_table.values_mut() {
        if entry.statement_count > 1 && entry.strongest_lock.blocks_writes() {
            entry.recommendation = Some(format!(
                "{} statements touch \"{}\". Consider splitting into separate migrations.",
                entry.statement_count, entry.table_name
            ));
        }

        if let Some(workload_profile) = workload {
            let families = workload_profile.families_for_table(&entry.table_name);
            if !families.is_empty() {
                if let Some(ref dur) = entry.duration_forecast {
                    entry.blocked_queries = forecast::forecast_blocked_queries(
                        entry.strongest_lock,
                        dur,
                        &families,
                    );
                    entry.total_blocked_qps = entry
                        .blocked_queries
                        .iter()
                        .map(|bq| bq.calls_per_sec)
                        .sum();
                }

                let table_qps = workload_profile.table_qps(&entry.table_name);
                let family_count = families.len();

                let mut doc_facts: Vec<String> = vec![
                    format!("lock mode is {} for this operation", entry.strongest_lock),
                ];
                let catalog_facts: Vec<String> = entry
                    .table_size
                    .as_ref()
                    .map(|s| vec![format!("table size is {} (~{} rows)", s.human_size, s.row_estimate)])
                    .unwrap_or_default();
                let stats_facts = vec![
                    format!("{family_count} query families, {:.0} calls/min combined", table_qps * 60.0),
                ];

                if entry.duration_forecast.is_some() {
                    doc_facts.push("lock hold duration inferred from table size and throughput model".into());
                }

                entry.confidence = ConfidenceLedger::with_workload(doc_facts, catalog_facts, stats_facts);
            } else if entry.duration_forecast.is_some() {
                entry.confidence = ConfidenceLedger::with_catalog(
                    vec![format!("lock mode is {} for this operation", entry.strongest_lock)],
                    entry
                        .table_size
                        .as_ref()
                        .map(|s| vec![format!("table size is {}", s.human_size)])
                        .unwrap_or_default(),
                );
            }
        } else if entry.duration_forecast.is_some() {
            entry.confidence = ConfidenceLedger::with_catalog(
                vec![format!("lock mode is {} for this operation", entry.strongest_lock)],
                entry
                    .table_size
                    .as_ref()
                    .map(|s| vec![format!("table size is {}", s.human_size)])
                    .unwrap_or_default(),
            );
        }
    }

    let mut tables: Vec<TableBlastRadius> = per_table.into_values().collect();
    tables.sort_by(|a, b| b.strongest_lock.cmp(&a.strongest_lock));

    let overall_risk = findings
        .iter()
        .map(|f| f.risk_level)
        .max()
        .unwrap_or(RiskLevel::Low);

    let overall_confidence = findings
        .iter()
        .map(|f| f.confidence.grade)
        .min()
        .unwrap_or(ConfidenceGrade::Measured);

    AnalysisResult {
        file: file.into(),
        findings,
        blast_radius: BlastRadius { per_table: tables },
        overall_risk,
        overall_confidence,
    }
}
