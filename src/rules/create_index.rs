use pg_query::protobuf;

use crate::forecast;
use crate::parse::format_relation;
use crate::recipe;
use crate::types::*;

use super::RuleContext;

pub fn analyse_index_stmt(
    index: &protobuf::IndexStmt,
    stmt_sql: &str,
    ctx: &RuleContext,
) -> Vec<Finding> {
    let table = index
        .relation
        .as_ref()
        .map(format_relation)
        .unwrap_or_else(|| "unknown".into());

    let idx_name = if index.idxname.is_empty() {
        "unnamed".into()
    } else {
        index.idxname.clone()
    };

    let table_bytes = ctx.catalog.and_then(|c| c.table_bytes(&table));

    if index.concurrent {
        let risk = adjust_risk_for_size(RiskLevel::Low, table_bytes);
        vec![Finding {
            rule_id: "create-index-concurrently".into(),
            risk_level: risk,
            confidence: match table_bytes {
                Some(b) => ConfidenceLedger::with_catalog(
                    vec!["SHARE UPDATE EXCLUSIVE lock (non-blocking) for CREATE INDEX CONCURRENTLY".into()],
                    vec![format!("table size is {}", human_size(b))],
                ),
                None => ConfidenceLedger::static_only(
                    vec!["SHARE UPDATE EXCLUSIVE lock (non-blocking) for CREATE INDEX CONCURRENTLY".into()],
                ),
            },
            lock_mode: LockMode::ShareUpdateExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(table.clone()),
            summary: format!(
                "CREATE INDEX CONCURRENTLY \"{idx_name}\" on \"{table}\" (non-blocking)"
            ),
            explanation: "SHARE UPDATE EXCLUSIVE lock does not block reads or writes. \
                Index is built in the background. Cannot run inside a transaction block. \
                May leave an INVALID index if long-running transactions interfere."
                .into(),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            duration_forecast: table_bytes.map(|b| forecast::forecast_index_build(b, ctx.transaction_baseline, ctx.io_throughput)),
        }]
    } else {
        let base_risk = RiskLevel::High;
        let risk = adjust_risk_for_size(base_risk, table_bytes);

        let columns = extract_index_columns(index);

        vec![Finding {
            rule_id: "create-index".into(),
            risk_level: risk,
            confidence: match table_bytes {
                Some(b) => ConfidenceLedger::with_catalog(
                    vec!["SHARE lock blocks writes for entire index build duration".into()],
                    vec![format!("table size is {}", human_size(b))],
                ),
                None => ConfidenceLedger::static_only(
                    vec!["SHARE lock blocks writes for entire index build duration".into()],
                ),
            },
            lock_mode: LockMode::Share,
            rewrite: RewriteRisk::None,
            affected_table: Some(table.clone()),
            summary: format!(
                "CREATE INDEX \"{idx_name}\" on \"{table}\" without CONCURRENTLY"
            ),
            explanation: format!(
                "Takes SHARE lock on \"{table}\", blocking all INSERT, UPDATE, and DELETE \
                 for the entire duration of the index build.{}",
                match table_bytes {
                    Some(b) => format!(" Table is {}.", human_size(b)),
                    None => " Table size unknown; use --dsn for duration estimate.".into(),
                }
            ),
            recipe: Some(recipe::create_index_concurrently(&table, &columns, &idx_name)),
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            duration_forecast: table_bytes.map(|b| forecast::forecast_index_build(b, ctx.transaction_baseline, ctx.io_throughput)),
        }]
    }
}

fn extract_index_columns(index: &protobuf::IndexStmt) -> String {
    index
        .index_params
        .iter()
        .filter_map(|n| {
            n.node.as_ref().and_then(|inner| match inner {
                pg_query::protobuf::node::Node::IndexElem(elem) => {
                    if elem.name.is_empty() {
                        None
                    } else {
                        Some(elem.name.clone())
                    }
                }
                _ => None,
            })
        })
        .collect::<Vec<_>>()
        .join(", ")
}
