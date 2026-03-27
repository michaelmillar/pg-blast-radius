use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use serde::Deserialize;

use crate::types::{TableSize, human_size};
use super::CatalogInfo;

#[derive(Deserialize)]
struct StatsEntry {
    table_name: String,
    total_bytes: i64,
    row_estimate: i64,
}

pub fn load_stats_file(path: &Path) -> Result<CatalogInfo> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read stats file: {e}"))?;
    let entries: Vec<StatsEntry> = serde_json::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Failed to parse stats file: {e}"))?;

    let mut tables = HashMap::new();
    for entry in entries {
        let size = TableSize {
            total_bytes: entry.total_bytes,
            row_estimate: entry.row_estimate,
            human_size: human_size(entry.total_bytes),
        };
        tables.insert(entry.table_name, size);
    }

    Ok(CatalogInfo { tables })
}
