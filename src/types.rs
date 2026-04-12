use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LockMode {
    AccessShare,
    RowShare,
    RowExclusive,
    ShareUpdateExclusive,
    Share,
    ShareRowExclusive,
    Exclusive,
    AccessExclusive,
}

impl LockMode {
    pub fn blocks_reads(self) -> bool {
        self == LockMode::AccessExclusive
    }

    pub fn blocks_writes(self) -> bool {
        self >= LockMode::Share
    }
}

impl fmt::Display for LockMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccessShare => write!(f, "ACCESS SHARE"),
            Self::RowShare => write!(f, "ROW SHARE"),
            Self::RowExclusive => write!(f, "ROW EXCLUSIVE"),
            Self::ShareUpdateExclusive => write!(f, "SHARE UPDATE EXCLUSIVE"),
            Self::Share => write!(f, "SHARE"),
            Self::ShareRowExclusive => write!(f, "SHARE ROW EXCLUSIVE"),
            Self::Exclusive => write!(f, "EXCLUSIVE"),
            Self::AccessExclusive => write!(f, "ACCESS EXCLUSIVE"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Extreme,
}

impl fmt::Display for RiskLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => write!(f, "LOW"),
            Self::Medium => write!(f, "MEDIUM"),
            Self::High => write!(f, "HIGH"),
            Self::Extreme => write!(f, "EXTREME"),
        }
    }
}


#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RewriteRisk {
    None,
    Required,
    Conditional { reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutPhase {
    Expand,
    Backfill,
    Validate,
    Switch,
    Contract,
}

impl fmt::Display for RolloutPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expand => write!(f, "expand"),
            Self::Backfill => write!(f, "backfill"),
            Self::Validate => write!(f, "validate"),
            Self::Switch => write!(f, "switch"),
            Self::Contract => write!(f, "contract"),
        }
    }
}


#[derive(Debug, Clone, Serialize)]
pub struct RecipeStep {
    pub phase: RolloutPhase,
    pub description: String,
    pub sql: String,
    pub separate_transaction: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RolloutRecipe {
    pub title: String,
    pub steps: Vec<RecipeStep>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    pub rule_id: String,
    pub risk_level: RiskLevel,
    pub confidence: ConfidenceLedger,
    pub lock_mode: LockMode,
    pub rewrite: RewriteRisk,
    pub affected_table: Option<String>,
    pub summary: String,
    pub explanation: String,
    pub recipe: Option<RolloutRecipe>,
    pub pg_version_note: Option<String>,
    pub statement_sql: String,
    pub duration_forecast: Option<DurationForecast>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableSize {
    pub total_bytes: i64,
    pub row_estimate: i64,
    pub human_size: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableBlastRadius {
    pub table_name: String,
    pub strongest_lock: LockMode,
    pub blocks_reads: bool,
    pub blocks_writes: bool,
    pub statement_count: usize,
    pub table_size: Option<TableSize>,
    pub duration_forecast: Option<DurationForecast>,
    pub blocked_queries: Vec<BlockedQueryForecast>,
    pub total_blocked_qps: f64,
    pub confidence: ConfidenceLedger,
    pub recommendation: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlastRadius {
    pub per_table: Vec<TableBlastRadius>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkloadMeta {
    pub stats_reset: Option<String>,
    pub collected_at: String,
    pub stats_window_seconds: Option<f64>,
    pub unparseable_queries: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct AnalysisResult {
    pub file: String,
    pub findings: Vec<Finding>,
    pub blast_radius: BlastRadius,
    pub overall_risk: RiskLevel,
    pub overall_confidence: ConfidenceGrade,
    pub workload_meta: Option<WorkloadMeta>,
}

pub fn human_size(bytes: i64) -> String {
    let bytes = bytes as f64;
    if bytes < 1024.0 {
        format!("{bytes:.0} B")
    } else if bytes < 1024.0 * 1024.0 {
        format!("{:.1} kB", bytes / 1024.0)
    } else if bytes < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1} MB", bytes / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes / (1024.0 * 1024.0 * 1024.0))
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceGrade {
    Static,
    Estimated,
    Measured,
}

impl fmt::Display for ConfidenceGrade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static => write!(f, "STATIC"),
            Self::Estimated => write!(f, "ESTIMATED"),
            Self::Measured => write!(f, "MEASURED"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfidenceLedger {
    pub from_docs: Vec<String>,
    pub from_catalog: Vec<String>,
    pub from_stats: Vec<String>,
    pub unknowns: Vec<String>,
    pub grade: ConfidenceGrade,
}

impl ConfidenceLedger {
    pub fn static_only(doc_facts: Vec<String>) -> Self {
        Self {
            from_docs: doc_facts,
            from_catalog: vec![],
            from_stats: vec![],
            unknowns: vec![
                "table size unknown".into(),
                "query load unknown".into(),
                "cache state unknown".into(),
            ],
            grade: ConfidenceGrade::Static,
        }
    }

    pub fn with_catalog(doc_facts: Vec<String>, catalog_facts: Vec<String>) -> Self {
        Self {
            from_docs: doc_facts,
            from_catalog: catalog_facts,
            from_stats: vec![],
            unknowns: vec![
                "query load unknown".into(),
                "cache state unknown".into(),
            ],
            grade: ConfidenceGrade::Estimated,
        }
    }

    pub fn with_workload(
        doc_facts: Vec<String>,
        catalog_facts: Vec<String>,
        stats_facts: Vec<String>,
    ) -> Self {
        Self {
            from_docs: doc_facts,
            from_catalog: catalog_facts,
            from_stats: stats_facts,
            unknowns: vec!["cache state assumed".into()],
            grade: ConfidenceGrade::Measured,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssumptionSource {
    Documentation,
    Catalog,
    Workload,
    Assumed,
}

#[derive(Debug, Clone, Serialize)]
pub struct ForecastAssumption {
    pub factor: String,
    pub assumed: String,
    pub source: AssumptionSource,
}

#[derive(Debug, Clone, Serialize)]
pub struct DurationForecast {
    pub fast_seconds: f64,
    pub slow_seconds: f64,
    pub worst_seconds: f64,
    pub assumptions: Vec<ForecastAssumption>,
}

impl fmt::Display for DurationForecast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (fast)  {} (slow)  {} (worst)",
            format_seconds(self.fast_seconds),
            format_seconds(self.slow_seconds),
            format_seconds(self.worst_seconds),
        )
    }
}

fn format_seconds(secs: f64) -> String {
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.1}s", secs)
    } else if secs < 3600.0 {
        format!("{:.0}m", secs / 60.0)
    } else {
        format!("{:.1}h", secs / 3600.0)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockedQueryForecast {
    pub query_label: String,
    pub normalised_sql: String,
    pub calls_per_sec: f64,
    pub queued_at_fast: u64,
    pub queued_at_slow: u64,
}

pub fn adjust_risk_for_size(base: RiskLevel, total_bytes: Option<i64>) -> RiskLevel {
    match total_bytes {
        Some(b) if b < 10_000_000 => {
            match base {
                RiskLevel::Extreme => RiskLevel::High,
                RiskLevel::High => RiskLevel::Medium,
                RiskLevel::Medium => RiskLevel::Low,
                RiskLevel::Low => RiskLevel::Low,
            }
        }
        Some(b) if b > 10_000_000_000 => {
            match base {
                RiskLevel::Low => RiskLevel::Medium,
                RiskLevel::Medium => RiskLevel::High,
                RiskLevel::High => RiskLevel::Extreme,
                RiskLevel::Extreme => RiskLevel::Extreme,
            }
        }
        _ => base,
    }
}
