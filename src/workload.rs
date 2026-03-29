use serde::{Deserialize, Serialize};

use crate::locks::DmlKind;
use crate::types::LockMode;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFamily {
    pub queryid: i64,
    pub normalised_sql: String,
    pub label: String,
    pub tables: Vec<String>,
    pub dml_kind: DmlKind,
    pub lock_mode: LockMode,
    pub calls_per_sec: f64,
    pub mean_exec_ms: f64,
    pub p95_exec_ms: Option<f64>,
}

impl QueryFamily {
    pub fn calls_per_min(&self) -> f64 {
        self.calls_per_sec * 60.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionBaseline {
    pub active_sessions: i64,
    pub idle_in_transaction: i64,
    pub median_age_ms: f64,
    pub p95_age_ms: f64,
    pub max_age_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadProfile {
    pub query_families: Vec<QueryFamily>,
    pub transaction_baseline: TransactionBaseline,
    pub collected_at: String,
    pub stats_reset: Option<String>,
    pub unparseable_queries: usize,
}

impl WorkloadProfile {
    pub fn families_for_table(&self, table: &str) -> Vec<&QueryFamily> {
        self.query_families
            .iter()
            .filter(|qf| qf.tables.iter().any(|t| t == table || t.ends_with(&format!(".{table}"))))
            .collect()
    }

    pub fn table_qps(&self, table: &str) -> f64 {
        self.families_for_table(table)
            .iter()
            .map(|qf| qf.calls_per_sec)
            .sum()
    }
}

pub fn make_label(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.len() <= 60 {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..57])
    }
}
