use anyhow::Result;
use pg_query::protobuf::node;

use crate::locks::DmlKind;
use crate::parse::format_relation;
use crate::workload::{QueryFamily, TransactionBaseline, WorkloadProfile, make_label};

#[cfg(feature = "catalog")]
pub async fn fetch_workload(
    client: &tokio_postgres::Client,
    pg_version_num: i32,
) -> Result<WorkloadProfile> {
    let (families, stats_reset) = fetch_query_families(client, pg_version_num).await?;
    let baseline = fetch_transaction_baseline(client).await?;
    let unparseable = families.iter().filter(|f| f.tables.is_empty()).count();

    Ok(WorkloadProfile {
        query_families: families,
        transaction_baseline: baseline,
        collected_at: chrono_now(),
        stats_reset,
        unparseable_queries: unparseable,
    })
}

fn chrono_now() -> String {
    let output = std::process::Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output();
    match output {
        Ok(o) => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        Err(_) => "unknown".into(),
    }
}

#[cfg(feature = "catalog")]
async fn fetch_query_families(
    client: &tokio_postgres::Client,
    pg_version_num: i32,
) -> Result<(Vec<QueryFamily>, Option<String>)> {
    let has_info_view = pg_version_num >= 140000;

    let has_extension: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')",
            &[],
        )
        .await
        .map(|row| row.get(0))
        .unwrap_or(false);

    if !has_extension {
        return Ok((vec![], None));
    }

    let (rows, stats_reset) = if has_info_view {
        let reset_row = client
            .query_opt("SELECT stats_reset::text FROM pg_stat_statements_info", &[])
            .await
            .ok()
            .flatten();
        let stats_reset: Option<String> = reset_row.and_then(|r| r.get(0));

        let rows = client
            .query(
                "SELECT
                    s.queryid,
                    s.query,
                    s.calls,
                    s.mean_exec_time,
                    s.stddev_exec_time,
                    s.calls::float8 / GREATEST(EXTRACT(EPOCH FROM (now() - i.stats_reset)), 1) AS calls_per_sec
                FROM pg_stat_statements s
                CROSS JOIN pg_stat_statements_info i
                WHERE s.query NOT LIKE '%pg_stat%'
                  AND s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                ORDER BY s.calls DESC
                LIMIT 500",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("pg_stat_statements query failed: {e}"))?;

        (rows, stats_reset)
    } else {
        let rows = client
            .query(
                "SELECT
                    s.queryid,
                    s.query,
                    s.calls,
                    s.mean_exec_time,
                    s.stddev_exec_time,
                    NULL::float8 AS calls_per_sec
                FROM pg_stat_statements s
                WHERE s.query NOT LIKE '%pg_stat%'
                  AND s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                ORDER BY s.calls DESC
                LIMIT 500",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("pg_stat_statements query failed: {e}"))?;

        (rows, None)
    };

    let mut families = Vec::new();
    for row in rows {
        let queryid: i64 = row.get(0);
        let query: String = row.get(1);
        let _calls: i64 = row.get(2);
        let mean_exec_time: f64 = row.get(3);
        let stddev_exec_time: f64 = row.get(4);
        let calls_per_sec: Option<f64> = row.get(5);

        let cps = calls_per_sec.unwrap_or(0.0);
        let p95 = if stddev_exec_time > 0.0 {
            Some(mean_exec_time + 1.645 * stddev_exec_time)
        } else {
            None
        };

        let (tables, dml_kind) = extract_tables_and_dml(&query).unwrap_or_default();
        let lock_mode = dml_kind.lock_mode();

        families.push(QueryFamily {
            queryid,
            normalised_sql: query.clone(),
            label: make_label(&query),
            tables,
            dml_kind,
            lock_mode,
            calls_per_sec: cps,
            mean_exec_ms: mean_exec_time,
            p95_exec_ms: p95,
        });
    }

    Ok((families, stats_reset))
}

#[cfg(feature = "catalog")]
async fn fetch_transaction_baseline(
    client: &tokio_postgres::Client,
) -> Result<TransactionBaseline> {
    let row = client
        .query_one(
            "SELECT
                count(*) FILTER (WHERE state = 'active'),
                count(*) FILTER (WHERE state = 'idle in transaction'),
                COALESCE(EXTRACT(EPOCH FROM percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY age(clock_timestamp(), xact_start)
                ) FILTER (WHERE state = 'active' AND xact_start IS NOT NULL)) * 1000, 0),
                COALESCE(EXTRACT(EPOCH FROM percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY age(clock_timestamp(), xact_start)
                ) FILTER (WHERE state = 'active' AND xact_start IS NOT NULL)) * 1000, 0),
                COALESCE(EXTRACT(EPOCH FROM max(age(clock_timestamp(), xact_start))
                ) FILTER (WHERE state = 'active' AND xact_start IS NOT NULL) * 1000, 0)
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()",
            &[],
        )
        .await
        .map_err(|e| anyhow::anyhow!("pg_stat_activity query failed: {e}"))?;

    Ok(TransactionBaseline {
        active_sessions: row.get(0),
        idle_in_transaction: row.get(1),
        median_age_ms: row.get(2),
        p95_age_ms: row.get(3),
        max_age_ms: row.get(4),
    })
}

pub fn extract_tables_and_dml(sql: &str) -> Option<(Vec<String>, DmlKind)> {
    let parsed = pg_query::parse(sql).ok()?;
    let stmt = parsed.protobuf.stmts.first()?;
    let wrapper = stmt.stmt.as_ref()?;
    let n = wrapper.node.as_ref()?;

    match n {
        node::Node::SelectStmt(select) => {
            let tables = extract_from_clause_tables(&select.from_clause);
            let has_locking = !select.locking_clause.is_empty();
            let kind = if has_locking {
                DmlKind::SelectForUpdate
            } else {
                DmlKind::Select
            };
            if tables.is_empty() {
                None
            } else {
                Some((tables, kind))
            }
        }
        node::Node::InsertStmt(insert) => {
            let table = insert.relation.as_ref().map(format_relation)?;
            Some((vec![table], DmlKind::Insert))
        }
        node::Node::UpdateStmt(update) => {
            let table = update.relation.as_ref().map(format_relation)?;
            Some((vec![table], DmlKind::Update))
        }
        node::Node::DeleteStmt(delete) => {
            let table = delete.relation.as_ref().map(format_relation)?;
            Some((vec![table], DmlKind::Delete))
        }
        _ => None,
    }
}

fn extract_from_clause_tables(from_clause: &[pg_query::protobuf::Node]) -> Vec<String> {
    let mut tables = Vec::new();
    for node in from_clause {
        extract_range_vars(node, &mut tables);
    }
    tables.sort();
    tables.dedup();
    tables
}

fn extract_range_vars(node: &pg_query::protobuf::Node, tables: &mut Vec<String>) {
    let Some(ref inner) = node.node else { return };
    match inner {
        node::Node::RangeVar(rv) => {
            tables.push(format_relation(rv));
        }
        node::Node::JoinExpr(join) => {
            if let Some(ref larg) = join.larg {
                extract_range_vars(larg, tables);
            }
            if let Some(ref rarg) = join.rarg {
                extract_range_vars(rarg, tables);
            }
        }
        node::Node::RangeSubselect(_) => {}
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_select_table() {
        let (tables, kind) = extract_tables_and_dml("SELECT * FROM orders WHERE id = $1").unwrap();
        assert_eq!(tables, vec!["orders"]);
        assert_eq!(kind, DmlKind::Select);
    }

    #[test]
    fn extracts_insert_table() {
        let (tables, kind) = extract_tables_and_dml(
            "INSERT INTO orders (customer_id, total) VALUES ($1, $2)",
        )
        .unwrap();
        assert_eq!(tables, vec!["orders"]);
        assert_eq!(kind, DmlKind::Insert);
    }

    #[test]
    fn extracts_update_table() {
        let (tables, kind) =
            extract_tables_and_dml("UPDATE orders SET status = $1 WHERE id = $2").unwrap();
        assert_eq!(tables, vec!["orders"]);
        assert_eq!(kind, DmlKind::Update);
    }

    #[test]
    fn extracts_delete_table() {
        let (tables, kind) =
            extract_tables_and_dml("DELETE FROM orders WHERE created_at < $1").unwrap();
        assert_eq!(tables, vec!["orders"]);
        assert_eq!(kind, DmlKind::Delete);
    }

    #[test]
    fn extracts_join_tables() {
        let (tables, kind) = extract_tables_and_dml(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
        )
        .unwrap();
        assert_eq!(tables, vec!["customers", "orders"]);
        assert_eq!(kind, DmlKind::Select);
    }

    #[test]
    fn extracts_schema_qualified_table() {
        let (tables, kind) =
            extract_tables_and_dml("SELECT * FROM public.orders WHERE id = $1").unwrap();
        assert_eq!(tables, vec!["public.orders"]);
        assert_eq!(kind, DmlKind::Select);
    }

    #[test]
    fn returns_none_for_utility_statements() {
        assert!(extract_tables_and_dml("CREATE TABLE foo (id int)").is_none());
        assert!(extract_tables_and_dml("ALTER TABLE foo ADD COLUMN bar int").is_none());
    }

    #[test]
    fn select_for_update_detected() {
        let (tables, kind) =
            extract_tables_and_dml("SELECT * FROM orders WHERE id = $1 FOR UPDATE").unwrap();
        assert_eq!(tables, vec!["orders"]);
        assert_eq!(kind, DmlKind::SelectForUpdate);
    }
}
