use std::collections::HashMap;

use crate::types::*;

pub fn build_result(file: &str, findings: Vec<Finding>) -> AnalysisResult {
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
                estimated_total_duration: None,
                recommendation: None,
            });

        if f.lock_mode > entry.strongest_lock {
            entry.strongest_lock = f.lock_mode;
        }
        entry.blocks_reads = entry.strongest_lock.blocks_reads();
        entry.blocks_writes = entry.strongest_lock.blocks_writes();
        entry.statement_count += 1;

        if let Some(ref est) = f.estimated_duration {
            entry.estimated_total_duration = Some(match entry.estimated_total_duration.take() {
                Some(existing) => DurationEstimate {
                    low_seconds: existing.low_seconds + est.low_seconds,
                    high_seconds: existing.high_seconds + est.high_seconds,
                    caveats: existing.caveats,
                },
                None => est.clone(),
            });
        }

        if let Some(ref size) = find_table_size(&findings, table) {
            entry.table_size = Some(size.clone());
        }
    }

    for entry in per_table.values_mut() {
        if entry.statement_count > 1 && entry.strongest_lock.blocks_writes() {
            entry.recommendation = Some(format!(
                "{} statements touch \"{}\". Consider splitting into separate migrations.",
                entry.statement_count, entry.table_name
            ));
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
        .map(|f| f.confidence)
        .min()
        .unwrap_or(Confidence::Definite);

    let mut assumptions: Vec<String> = findings
        .iter()
        .flat_map(|f| f.assumptions.iter().cloned())
        .collect();
    assumptions.sort();
    assumptions.dedup();

    AnalysisResult {
        file: file.into(),
        findings,
        blast_radius: BlastRadius { per_table: tables },
        overall_risk,
        overall_confidence,
        assumptions,
    }
}

fn find_table_size(findings: &[Finding], table: &str) -> Option<TableSize> {
    for f in findings {
        if f.affected_table.as_deref() == Some(table) {
            if let Some(ref dur) = f.estimated_duration {
                let _ = dur;
            }
        }
    }
    None
}
