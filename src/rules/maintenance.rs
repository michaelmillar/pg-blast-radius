use pg_query::protobuf::{self, node, ReindexObjectType};

use crate::forecast;
use crate::parse::format_relation;
use crate::types::*;

use super::RuleContext;

pub fn analyse_truncate(
    stmt: &protobuf::TruncateStmt,
    stmt_sql: &str,
    ctx: &RuleContext,
) -> Vec<Finding> {
    let tables: Vec<String> = stmt
        .relations
        .iter()
        .filter_map(|n| match n.node.as_ref()? {
            node::Node::RangeVar(rv) => Some(format_relation(rv)),
            _ => None,
        })
        .collect();

    let table_list = tables.join(", ");

    tables
        .iter()
        .map(|table| {
            let table_bytes = ctx
                .catalog
                .and_then(|c| c.tables.get(table.as_str()))
                .map(|s| s.total_bytes);

            Finding {
                rule_id: "truncate".into(),
                risk_level: RiskLevel::Extreme,
                confidence: ConfidenceLedger::static_only(
                    vec!["ACCESS EXCLUSIVE lock for TRUNCATE (blocks all)".into()],
                ),
                lock_mode: LockMode::AccessExclusive,
                rewrite: RewriteRisk::None,
                affected_table: Some(table.clone()),
                summary: format!("TRUNCATE on \"{table_list}\" takes ACCESS EXCLUSIVE lock"),
                explanation: format!(
                    "TRUNCATE acquires ACCESS EXCLUSIVE lock on all listed tables, \
                     blocking all reads and writes. Lock is held briefly (no row-by-row scan), \
                     but all data is removed. This is a destructive operation.{}",
                    match table_bytes {
                        Some(b) => format!(" Table is {}.", human_size(b)),
                        None => String::new(),
                    }
                ),
                recipe: None,
                pg_version_note: None,
                statement_sql: stmt_sql.into(),
                duration_forecast: None,
            }
        })
        .collect()
}

pub fn analyse_vacuum(
    stmt: &protobuf::VacuumStmt,
    stmt_sql: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    let sql_upper = stmt_sql.to_uppercase();
    let is_full = sql_upper.contains("FULL");
    let is_analyze_only = !stmt.is_vacuumcmd && sql_upper.starts_with("ANALYZE");

    let tables: Vec<String> = stmt
        .rels
        .iter()
        .filter_map(|n| match n.node.as_ref()? {
            node::Node::VacuumRelation(vr) => vr.relation.as_ref().map(format_relation),
            _ => None,
        })
        .collect();

    let table_str = if tables.is_empty() {
        "all tables".to_string()
    } else {
        tables.join(", ")
    };

    if is_full {
        vec![Finding {
            rule_id: "vacuum-full".into(),
            risk_level: RiskLevel::Extreme,
            confidence: ConfidenceLedger::static_only(
                vec!["ACCESS EXCLUSIVE lock for VACUUM FULL (rewrites table)".into()],
            ),
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::Required,
            affected_table: tables.first().cloned(),
            summary: format!("VACUUM FULL on \"{table_str}\" rewrites table under ACCESS EXCLUSIVE"),
            explanation: "VACUUM FULL rewrites the entire table to reclaim space. \
                ACCESS EXCLUSIVE lock blocks all reads and writes for the entire duration. \
                Consider pg_repack or pg_squeeze for online table rewrites."
                .into(),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            duration_forecast: None,
        }]
    } else if is_analyze_only {
        vec![Finding {
            rule_id: "analyze".into(),
            risk_level: RiskLevel::Low,
            confidence: ConfidenceLedger::static_only(
                vec!["SHARE UPDATE EXCLUSIVE lock for ANALYZE (non-blocking)".into()],
            ),
            lock_mode: LockMode::ShareUpdateExclusive,
            rewrite: RewriteRisk::None,
            affected_table: tables.first().cloned(),
            summary: format!("ANALYZE on \"{table_str}\" (non-blocking)"),
            explanation: "ANALYZE collects table statistics. SHARE UPDATE EXCLUSIVE lock \
                does not block reads or writes."
                .into(),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            duration_forecast: None,
        }]
    } else {
        vec![Finding {
            rule_id: "vacuum".into(),
            risk_level: RiskLevel::Low,
            confidence: ConfidenceLedger::static_only(
                vec!["SHARE UPDATE EXCLUSIVE lock for VACUUM (non-blocking)".into()],
            ),
            lock_mode: LockMode::ShareUpdateExclusive,
            rewrite: RewriteRisk::None,
            affected_table: tables.first().cloned(),
            summary: format!("VACUUM on \"{table_str}\" (non-blocking)"),
            explanation: "VACUUM reclaims dead tuple space. SHARE UPDATE EXCLUSIVE lock \
                does not block reads or writes. This is a normal maintenance operation."
                .into(),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            duration_forecast: None,
        }]
    }
}

pub fn analyse_reindex(
    stmt: &protobuf::ReindexStmt,
    stmt_sql: &str,
    ctx: &RuleContext,
) -> Vec<Finding> {
    let sql_upper = stmt_sql.to_uppercase();
    let is_concurrent = sql_upper.contains("CONCURRENTLY");

    let kind = ReindexObjectType::try_from(stmt.kind)
        .unwrap_or(ReindexObjectType::Undefined);

    let target = stmt
        .relation
        .as_ref()
        .map(format_relation)
        .unwrap_or_else(|| stmt.name.clone());

    let table_bytes = ctx
        .catalog
        .and_then(|c| c.tables.get(target.as_str()))
        .map(|s| s.total_bytes);

    if is_concurrent {
        vec![Finding {
            rule_id: "reindex-concurrently".into(),
            risk_level: adjust_risk_for_size(RiskLevel::Low, table_bytes),
            confidence: match table_bytes {
                Some(b) => ConfidenceLedger::with_catalog(
                    vec!["SHARE UPDATE EXCLUSIVE lock for REINDEX CONCURRENTLY".into()],
                    vec![format!("table size is {}", human_size(b))],
                ),
                None => ConfidenceLedger::static_only(
                    vec!["SHARE UPDATE EXCLUSIVE lock for REINDEX CONCURRENTLY".into()],
                ),
            },
            lock_mode: LockMode::ShareUpdateExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(target.clone()),
            summary: format!("REINDEX CONCURRENTLY on \"{target}\" (non-blocking)"),
            explanation: "REINDEX CONCURRENTLY rebuilds the index without blocking reads or writes. \
                Cannot run inside a transaction block."
                .into(),
            recipe: None,
            pg_version_note: Some("REINDEX CONCURRENTLY available on PG 12+.".into()),
            statement_sql: stmt_sql.into(),
            duration_forecast: table_bytes.map(|b| forecast::forecast_index_build(b, ctx.transaction_baseline)),
        }]
    } else {
        let kind_label = match kind {
            ReindexObjectType::ReindexObjectIndex => "INDEX",
            ReindexObjectType::ReindexObjectTable => "TABLE",
            ReindexObjectType::ReindexObjectSchema => "SCHEMA",
            ReindexObjectType::ReindexObjectSystem => "SYSTEM",
            _ => "UNKNOWN",
        };

        let risk = adjust_risk_for_size(RiskLevel::High, table_bytes);

        vec![Finding {
            rule_id: "reindex".into(),
            risk_level: risk,
            confidence: match table_bytes {
                Some(b) => ConfidenceLedger::with_catalog(
                    vec![format!("ACCESS EXCLUSIVE lock for REINDEX {kind_label}")],
                    vec![format!("table size is {}", human_size(b))],
                ),
                None => ConfidenceLedger::static_only(
                    vec![format!("ACCESS EXCLUSIVE lock for REINDEX {kind_label}")],
                ),
            },
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(target.clone()),
            summary: format!("REINDEX {kind_label} on \"{target}\" blocks all reads and writes"),
            explanation: format!(
                "REINDEX acquires ACCESS EXCLUSIVE lock, blocking all reads and writes \
                 for the duration of the index rebuild. Use REINDEX CONCURRENTLY on PG 12+ \
                 for a non-blocking rebuild."
            ),
            recipe: None,
            pg_version_note: Some("Use REINDEX CONCURRENTLY (PG 12+) for non-blocking rebuild.".into()),
            statement_sql: stmt_sql.into(),
            duration_forecast: table_bytes.map(|b| forecast::forecast_index_build(b, ctx.transaction_baseline)),
        }]
    }
}

pub fn analyse_refresh_matview(
    stmt: &protobuf::RefreshMatViewStmt,
    stmt_sql: &str,
    ctx: &RuleContext,
) -> Vec<Finding> {
    let view_name = stmt
        .relation
        .as_ref()
        .map(format_relation)
        .unwrap_or_else(|| "unknown".into());

    if stmt.concurrent {
        vec![Finding {
            rule_id: "refresh-matview-concurrently".into(),
            risk_level: RiskLevel::Medium,
            confidence: ConfidenceLedger::static_only(
                vec!["EXCLUSIVE lock for REFRESH MATERIALIZED VIEW CONCURRENTLY".into()],
            ),
            lock_mode: LockMode::Exclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(view_name.clone()),
            summary: format!(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY \"{view_name}\" (blocks writes to view)"
            ),
            explanation: format!(
                "EXCLUSIVE lock on \"{view_name}\" blocks writes but allows reads. \
                 The view must have a UNIQUE index. Slower than a full refresh \
                 because it diffs the old and new data."
            ),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            duration_forecast: None,
        }]
    } else {
        let table_bytes = ctx
            .catalog
            .and_then(|c| c.tables.get(view_name.as_str()))
            .map(|s| s.total_bytes);

        vec![Finding {
            rule_id: "refresh-matview".into(),
            risk_level: adjust_risk_for_size(RiskLevel::High, table_bytes),
            confidence: match table_bytes {
                Some(b) => ConfidenceLedger::with_catalog(
                    vec!["ACCESS EXCLUSIVE lock for REFRESH MATERIALIZED VIEW".into()],
                    vec![format!("view size is {}", human_size(b))],
                ),
                None => ConfidenceLedger::static_only(
                    vec!["ACCESS EXCLUSIVE lock for REFRESH MATERIALIZED VIEW".into()],
                ),
            },
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(view_name.clone()),
            summary: format!(
                "REFRESH MATERIALIZED VIEW \"{view_name}\" blocks all reads and writes"
            ),
            explanation: format!(
                "ACCESS EXCLUSIVE lock on \"{view_name}\" blocks all reads and writes \
                 for the entire duration of the refresh. Use CONCURRENTLY to allow reads \
                 during refresh (requires a UNIQUE index on the view)."
            ),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            duration_forecast: None,
        }]
    }
}
