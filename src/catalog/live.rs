use std::collections::HashMap;

use anyhow::Result;

use crate::types::{TableSize, human_size};
use super::CatalogInfo;

pub fn fetch_catalog(dsn: &str) -> Result<CatalogInfo> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await
            .map_err(|e| anyhow::anyhow!("Failed to connect: {e}"))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("catalog connection error: {e}");
            }
        });

        let rows = client
            .query(
                "SELECT
                    schemaname || '.' || relname AS full_name,
                    relname AS table_name,
                    pg_total_relation_size(relid) AS total_bytes,
                    n_live_tup AS row_estimate
                FROM pg_stat_user_tables
                ORDER BY pg_total_relation_size(relid) DESC",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!("Catalog query failed: {e}"))?;

        let mut tables = HashMap::new();
        for row in rows {
            let full_name: String = row.get(0);
            let table_name: String = row.get(1);
            let total_bytes: i64 = row.get(2);
            let row_estimate: i64 = row.get(3);

            let size = TableSize {
                total_bytes,
                row_estimate,
                human_size: human_size(total_bytes),
            };

            tables.insert(full_name, size.clone());
            tables.insert(table_name, size);
        }

        Ok(CatalogInfo { tables })
    })
}
