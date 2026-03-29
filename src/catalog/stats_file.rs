use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use serde::Deserialize;

use crate::types::{TableSize, human_size};
use crate::workload::WorkloadProfile;
use super::CatalogInfo;

#[derive(Deserialize)]
struct StatsEntry {
    table_name: String,
    total_bytes: i64,
    row_estimate: i64,
}

#[derive(Deserialize)]
struct StatsFileV2 {
    tables: Vec<StatsEntry>,
    workload: Option<WorkloadProfile>,
}

pub fn load_stats_file(path: &Path) -> Result<CatalogInfo> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read stats file: {e}"))?;

    if let Ok(v2) = serde_json::from_str::<StatsFileV2>(&content) {
        return Ok(CatalogInfo {
            tables: entries_to_map(v2.tables),
            workload: v2.workload,
        });
    }

    let entries: Vec<StatsEntry> = serde_json::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Failed to parse stats file: {e}"))?;

    Ok(CatalogInfo {
        tables: entries_to_map(entries),
        workload: None,
    })
}

fn entries_to_map(entries: Vec<StatsEntry>) -> HashMap<String, TableSize> {
    let mut tables = HashMap::new();
    for entry in entries {
        let size = TableSize {
            total_bytes: entry.total_bytes,
            row_estimate: entry.row_estimate,
            human_size: human_size(entry.total_bytes),
        };
        tables.insert(entry.table_name, size);
    }
    tables
}
