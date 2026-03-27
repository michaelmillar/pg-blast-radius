use std::collections::HashMap;

use crate::types::TableSize;

#[cfg(feature = "catalog")]
pub mod live;
pub mod stats_file;

#[derive(Debug, Clone, Default)]
pub struct CatalogInfo {
    pub tables: HashMap<String, TableSize>,
}

impl CatalogInfo {
    pub fn get_table(&self, name: &str) -> Option<&TableSize> {
        self.tables.get(name)
    }

    pub fn table_bytes(&self, name: &str) -> Option<i64> {
        self.get_table(name).map(|t| t.total_bytes)
    }
}
