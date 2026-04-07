pub mod alter_table;
pub mod constraints;
pub mod create_index;
pub mod drop;
pub mod maintenance;
pub mod rename;

use crate::catalog::CatalogInfo;
use crate::parse;
use crate::types::Finding;
use crate::workload::TransactionBaseline;
use anyhow::Result;
use pg_query::protobuf::node;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PgVersion {
    pub major: u32,
}

impl PgVersion {
    pub fn new(major: u32) -> Self {
        Self { major }
    }

    pub fn at_least(self, major: u32) -> bool {
        self.major >= major
    }
}

impl Default for PgVersion {
    fn default() -> Self {
        Self { major: 16 }
    }
}

pub struct RuleContext<'a> {
    pub pg_version: PgVersion,
    pub catalog: Option<&'a CatalogInfo>,
    pub transaction_baseline: Option<&'a TransactionBaseline>,
}

pub fn analyse(source: &str, ctx: &RuleContext) -> Result<Vec<Finding>> {
    let parsed = parse::parse(source)?;
    let mut findings = Vec::new();

    for stmt in &parsed.protobuf.stmts {
        let Some(ref wrapper) = stmt.stmt else { continue };
        let Some(ref n) = wrapper.node else { continue };
        let stmt_sql = parse::extract_statement_sql(source, stmt);

        match n {
            node::Node::AlterTableStmt(alter) => {
                findings.extend(alter_table::analyse_alter_table(alter, &stmt_sql, ctx));
            }
            node::Node::IndexStmt(index) => {
                findings.extend(create_index::analyse_index_stmt(index, &stmt_sql, ctx));
            }
            node::Node::DropStmt(drop_stmt) => {
                findings.extend(drop::analyse_drop(drop_stmt, &stmt_sql, ctx));
            }
            node::Node::RenameStmt(rename) => {
                findings.extend(rename::analyse_rename(rename, &stmt_sql, ctx));
            }
            node::Node::TruncateStmt(truncate) => {
                findings.extend(maintenance::analyse_truncate(truncate, &stmt_sql, ctx));
            }
            node::Node::VacuumStmt(vacuum) => {
                findings.extend(maintenance::analyse_vacuum(vacuum, &stmt_sql, ctx));
            }
            node::Node::ReindexStmt(reindex) => {
                findings.extend(maintenance::analyse_reindex(reindex, &stmt_sql, ctx));
            }
            node::Node::RefreshMatViewStmt(refresh) => {
                findings.extend(maintenance::analyse_refresh_matview(refresh, &stmt_sql, ctx));
            }
            node::Node::SelectStmt(_)
            | node::Node::InsertStmt(_)
            | node::Node::UpdateStmt(_)
            | node::Node::DeleteStmt(_) => {
                eprintln!(
                    "warning: DML statement ignored (pg-blast-radius analyses DDL only): {}",
                    stmt_sql.lines().next().unwrap_or("").trim()
                );
            }
            _ => {}
        }
    }

    Ok(findings)
}

pub fn extract_string_value(n: &pg_query::protobuf::Node) -> Option<&str> {
    match n.node.as_ref()? {
        node::Node::String(s) => Some(&s.sval),
        _ => None,
    }
}

pub fn type_name_to_string(tn: &pg_query::protobuf::TypeName) -> String {
    tn.names
        .iter()
        .filter_map(extract_string_value)
        .filter(|s| *s != "pg_catalog")
        .collect::<Vec<_>>()
        .join(".")
}
