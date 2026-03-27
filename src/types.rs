use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    Contextual,
    NeedsCatalog,
    Definite,
}

impl fmt::Display for Confidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Definite => write!(f, "definite"),
            Self::NeedsCatalog => write!(f, "needs-catalog"),
            Self::Contextual => write!(f, "contextual"),
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
pub struct DurationEstimate {
    pub low_seconds: u64,
    pub high_seconds: u64,
    pub caveats: Vec<String>,
}

impl fmt::Display for DurationEstimate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_duration = |secs: u64| -> String {
            if secs < 60 {
                format!("{secs}s")
            } else if secs < 3600 {
                format!("{}m", (secs + 30) / 60)
            } else {
                format!("{:.1}h", secs as f64 / 3600.0)
            }
        };
        write!(f, "{}..{}", format_duration(self.low_seconds), format_duration(self.high_seconds))
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
    pub confidence: Confidence,
    pub lock_mode: LockMode,
    pub rewrite: RewriteRisk,
    pub affected_table: Option<String>,
    pub summary: String,
    pub explanation: String,
    pub recipe: Option<RolloutRecipe>,
    pub pg_version_note: Option<String>,
    pub statement_sql: String,
    pub estimated_duration: Option<DurationEstimate>,
    pub assumptions: Vec<String>,
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
    pub estimated_total_duration: Option<DurationEstimate>,
    pub recommendation: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlastRadius {
    pub per_table: Vec<TableBlastRadius>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AnalysisResult {
    pub file: String,
    pub findings: Vec<Finding>,
    pub blast_radius: BlastRadius,
    pub overall_risk: RiskLevel,
    pub overall_confidence: Confidence,
    pub assumptions: Vec<String>,
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

pub fn estimate_duration_for_scan(total_bytes: i64, low_mbps: f64, high_mbps: f64) -> DurationEstimate {
    let bytes = total_bytes as f64;
    let low_secs = (bytes / (high_mbps * 1024.0 * 1024.0)).ceil() as u64;
    let high_secs = (bytes / (low_mbps * 1024.0 * 1024.0)).ceil() as u64;
    let low_secs = low_secs.max(1);
    let high_secs = high_secs.max(low_secs);
    DurationEstimate {
        low_seconds: low_secs,
        high_seconds: high_secs,
        caveats: vec!["Throughput estimated; actual duration depends on storage, IO load, and concurrent activity".into()],
    }
}

pub fn estimate_rewrite_duration(total_bytes: i64) -> DurationEstimate {
    estimate_duration_for_scan(total_bytes, 50.0, 100.0)
}

pub fn estimate_index_build_duration(total_bytes: i64) -> DurationEstimate {
    estimate_duration_for_scan(total_bytes, 30.0, 80.0)
}

pub fn estimate_scan_duration(total_bytes: i64) -> DurationEstimate {
    estimate_duration_for_scan(total_bytes, 100.0, 200.0)
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
