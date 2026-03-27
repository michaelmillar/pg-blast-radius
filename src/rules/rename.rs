use pg_query::protobuf::{self, ObjectType};

use crate::parse::format_relation;
use crate::types::*;

use super::RuleContext;

pub fn analyse_rename(
    rename: &protobuf::RenameStmt,
    stmt_sql: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    let obj_type = ObjectType::try_from(rename.rename_type).unwrap_or(ObjectType::Undefined);

    let table = rename
        .relation
        .as_ref()
        .map(format_relation)
        .unwrap_or_else(|| "unknown".into());

    match obj_type {
        ObjectType::ObjectColumn => {
            vec![Finding {
                rule_id: "rename-column".into(),
                risk_level: RiskLevel::Medium,
                confidence: Confidence::Definite,
                lock_mode: LockMode::AccessExclusive,
                rewrite: RewriteRisk::None,
                affected_table: Some(table.clone()),
                summary: format!(
                    "RENAME COLUMN \"{}\" to \"{}\" on \"{table}\"",
                    rename.subname, rename.newname
                ),
                explanation: "ACCESS EXCLUSIVE lock (brief, catalog update only). \
                    No table rewrite. The primary risk is application-level breakage \
                    if code still references the old column name."
                    .into(),
                recipe: None,
                pg_version_note: None,
                statement_sql: stmt_sql.into(),
                estimated_duration: None,
                assumptions: vec![],
            }]
        }
        ObjectType::ObjectTable => {
            vec![Finding {
                rule_id: "rename-table".into(),
                risk_level: RiskLevel::Medium,
                confidence: Confidence::Definite,
                lock_mode: LockMode::AccessExclusive,
                rewrite: RewriteRisk::None,
                affected_table: Some(table.clone()),
                summary: format!(
                    "RENAME TABLE \"{table}\" to \"{}\"",
                    rename.newname
                ),
                explanation: "ACCESS EXCLUSIVE lock (brief, catalog update only). \
                    No table rewrite. The primary risk is application-level breakage \
                    if code still references the old table name."
                    .into(),
                recipe: None,
                pg_version_note: None,
                statement_sql: stmt_sql.into(),
                estimated_duration: None,
                assumptions: vec![],
            }]
        }
        _ => vec![],
    }
}
