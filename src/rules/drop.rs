use pg_query::protobuf::{self, ObjectType};

use crate::recipe;
use crate::types::*;

use super::RuleContext;

pub fn analyse_drop(
    drop_stmt: &protobuf::DropStmt,
    stmt_sql: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    let obj_type = ObjectType::try_from(drop_stmt.remove_type).unwrap_or(ObjectType::Undefined);

    match obj_type {
        ObjectType::ObjectIndex => analyse_drop_index(drop_stmt, stmt_sql),
        ObjectType::ObjectTable => analyse_drop_table(stmt_sql),
        _ => vec![],
    }
}

fn analyse_drop_index(drop_stmt: &protobuf::DropStmt, stmt_sql: &str) -> Vec<Finding> {
    let idx_name = extract_drop_object_name(&drop_stmt.objects);

    if drop_stmt.concurrent {
        vec![Finding {
            rule_id: "drop-index-concurrently".into(),
            risk_level: RiskLevel::Low,
            confidence: Confidence::Definite,
            lock_mode: LockMode::ShareUpdateExclusive,
            rewrite: RewriteRisk::None,
            affected_table: None,
            summary: format!("DROP INDEX CONCURRENTLY \"{idx_name}\" (non-blocking)"),
            explanation: "SHARE UPDATE EXCLUSIVE lock does not block reads or writes. \
                Cannot run inside a transaction block."
                .into(),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            estimated_duration: None,
            assumptions: vec![],
        }]
    } else {
        vec![Finding {
            rule_id: "drop-index".into(),
            risk_level: RiskLevel::High,
            confidence: Confidence::Definite,
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::None,
            affected_table: None,
            summary: format!("DROP INDEX \"{idx_name}\" takes ACCESS EXCLUSIVE lock"),
            explanation: "ACCESS EXCLUSIVE lock blocks all reads and writes on the \
                table until the index is dropped."
                .into(),
            recipe: Some(recipe::drop_index_concurrently(&idx_name)),
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            estimated_duration: None,
            assumptions: vec![],
        }]
    }
}

fn analyse_drop_table(stmt_sql: &str) -> Vec<Finding> {
    vec![Finding {
        rule_id: "drop-table".into(),
        risk_level: RiskLevel::High,
        confidence: Confidence::Definite,
        lock_mode: LockMode::AccessExclusive,
        rewrite: RewriteRisk::None,
        affected_table: None,
        summary: "DROP TABLE takes ACCESS EXCLUSIVE lock".into(),
        explanation: "ACCESS EXCLUSIVE lock blocks all queries. This is a destructive \
            operation that cannot be undone outside of a transaction."
            .into(),
        recipe: None,
        pg_version_note: None,
        statement_sql: stmt_sql.into(),
        estimated_duration: None,
        assumptions: vec![],
    }]
}

fn extract_drop_object_name(objects: &[protobuf::Node]) -> String {
    objects
        .first()
        .and_then(|n| n.node.as_ref())
        .map(|n| match n {
            pg_query::protobuf::node::Node::List(list) => list
                .items
                .iter()
                .filter_map(super::extract_string_value)
                .collect::<Vec<_>>()
                .join("."),
            pg_query::protobuf::node::Node::String(s) => s.sval.clone(),
            _ => "unknown".into(),
        })
        .unwrap_or_else(|| "unknown".into())
}
