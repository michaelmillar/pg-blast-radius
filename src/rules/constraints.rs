use pg_query::protobuf::{self, node, ConstrType};

use crate::recipe;
use crate::types::*;

use super::RuleContext;

pub fn analyse_add_constraint(
    cmd: &protobuf::AlterTableCmd,
    table: &str,
    table_bytes: Option<i64>,
    stmt_sql: &str,
    _ctx: &RuleContext,
) -> Vec<Finding> {
    let constraint = match cmd.def.as_ref().and_then(|d| d.node.as_ref()) {
        Some(node::Node::Constraint(c)) => c,
        _ => return vec![],
    };

    let contype = ConstrType::try_from(constraint.contype).unwrap_or(ConstrType::Undefined);
    let con_name = if constraint.conname.is_empty() {
        "unnamed".into()
    } else {
        constraint.conname.clone()
    };
    let not_valid = constraint.skip_validation;

    match contype {
        ConstrType::ConstrCheck => analyse_check(table, &con_name, not_valid, table_bytes, stmt_sql),
        ConstrType::ConstrForeign => {
            analyse_foreign_key(table, &con_name, not_valid, constraint, table_bytes, stmt_sql)
        }
        ConstrType::ConstrUnique => analyse_unique(table, &con_name, table_bytes, stmt_sql),
        ConstrType::ConstrPrimary => analyse_primary_key(table, &con_name, table_bytes, stmt_sql),
        _ => vec![],
    }
}

fn analyse_check(
    table: &str,
    con_name: &str,
    not_valid: bool,
    table_bytes: Option<i64>,
    stmt_sql: &str,
) -> Vec<Finding> {
    if not_valid {
        vec![Finding {
            rule_id: "add-check-not-valid".into(),
            risk_level: RiskLevel::Low,
            confidence: Confidence::Definite,
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(table.into()),
            summary: format!("ADD CHECK \"{con_name}\" NOT VALID on \"{table}\" (brief lock, no scan)"),
            explanation: "Adding a CHECK constraint with NOT VALID skips the validation scan. \
                The ACCESS EXCLUSIVE lock is held only for the brief catalog update. \
                Remember to VALIDATE CONSTRAINT in a separate transaction."
                .into(),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            estimated_duration: None,
            assumptions: vec![],
        }]
    } else {
        let risk = adjust_risk_for_size(RiskLevel::High, table_bytes);
        vec![Finding {
            rule_id: "add-check-constraint".into(),
            risk_level: risk,
            confidence: if table_bytes.is_some() {
                Confidence::Definite
            } else {
                Confidence::NeedsCatalog
            },
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(table.into()),
            summary: format!(
                "ADD CHECK \"{con_name}\" on \"{table}\" performs full table scan under lock"
            ),
            explanation: "Adding a CHECK constraint without NOT VALID scans every row \
                under ACCESS EXCLUSIVE lock, blocking all reads and writes."
                .into(),
            recipe: Some(recipe::add_check_safe(table, con_name, "<check_expression>")),
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            estimated_duration: table_bytes.map(estimate_scan_duration),
            assumptions: vec![],
        }]
    }
}

fn analyse_foreign_key(
    table: &str,
    con_name: &str,
    not_valid: bool,
    constraint: &protobuf::Constraint,
    table_bytes: Option<i64>,
    stmt_sql: &str,
) -> Vec<Finding> {
    let ref_table = constraint
        .pktable
        .as_ref()
        .map(|r| {
            if r.schemaname.is_empty() {
                r.relname.clone()
            } else {
                format!("{}.{}", r.schemaname, r.relname)
            }
        })
        .unwrap_or_else(|| "unknown".into());

    if not_valid {
        vec![Finding {
            rule_id: "add-foreign-key-not-valid".into(),
            risk_level: RiskLevel::Medium,
            confidence: Confidence::Definite,
            lock_mode: LockMode::ShareRowExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(table.into()),
            summary: format!(
                "ADD FOREIGN KEY \"{con_name}\" NOT VALID on \"{table}\" -> \"{ref_table}\""
            ),
            explanation: format!(
                "ShareRowExclusive lock on both \"{table}\" and \"{ref_table}\" (brief). \
                 No validation scan because NOT VALID is specified. \
                 Remember to VALIDATE CONSTRAINT in a separate transaction."
            ),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            estimated_duration: None,
            assumptions: vec![],
        }]
    } else {
        let risk = adjust_risk_for_size(RiskLevel::High, table_bytes);
        vec![Finding {
            rule_id: "add-foreign-key".into(),
            risk_level: risk,
            confidence: if table_bytes.is_some() {
                Confidence::Definite
            } else {
                Confidence::NeedsCatalog
            },
            lock_mode: LockMode::ShareRowExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(table.into()),
            summary: format!(
                "ADD FOREIGN KEY \"{con_name}\" on \"{table}\" -> \"{ref_table}\" scans table"
            ),
            explanation: format!(
                "ShareRowExclusive lock on both \"{table}\" and \"{ref_table}\". \
                 Full scan of \"{table}\" to verify all rows satisfy the FK. \
                 This blocks writes on both tables for the scan duration."
            ),
            recipe: Some(recipe::add_foreign_key_safe(
                table,
                con_name,
                &stmt_sql.trim_end_matches(';'),
            )),
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            estimated_duration: table_bytes.map(estimate_scan_duration),
            assumptions: vec![],
        }]
    }
}

fn analyse_unique(
    table: &str,
    con_name: &str,
    table_bytes: Option<i64>,
    stmt_sql: &str,
) -> Vec<Finding> {
    let risk = adjust_risk_for_size(RiskLevel::High, table_bytes);
    vec![Finding {
        rule_id: "add-unique-constraint".into(),
        risk_level: risk,
        confidence: if table_bytes.is_some() {
            Confidence::Definite
        } else {
            Confidence::NeedsCatalog
        },
        lock_mode: LockMode::AccessExclusive,
        rewrite: RewriteRisk::None,
        affected_table: Some(table.into()),
        summary: format!(
            "ADD UNIQUE \"{con_name}\" on \"{table}\" builds index under lock"
        ),
        explanation: "Adding a UNIQUE constraint implicitly creates a unique index. \
            This acquires ACCESS EXCLUSIVE lock and builds the index, blocking \
            all reads and writes for the duration. Consider creating the index \
            CONCURRENTLY first, then adding the constraint using the existing index."
            .into(),
        recipe: None,
        pg_version_note: None,
        statement_sql: stmt_sql.into(),
        estimated_duration: table_bytes.map(estimate_index_build_duration),
        assumptions: vec![],
    }]
}

fn analyse_primary_key(
    table: &str,
    con_name: &str,
    table_bytes: Option<i64>,
    stmt_sql: &str,
) -> Vec<Finding> {
    let risk = adjust_risk_for_size(RiskLevel::High, table_bytes);
    vec![Finding {
        rule_id: "add-primary-key".into(),
        risk_level: risk,
        confidence: if table_bytes.is_some() {
            Confidence::Definite
        } else {
            Confidence::NeedsCatalog
        },
        lock_mode: LockMode::AccessExclusive,
        rewrite: RewriteRisk::None,
        affected_table: Some(table.into()),
        summary: format!(
            "ADD PRIMARY KEY \"{con_name}\" on \"{table}\" builds index under lock"
        ),
        explanation: "Adding a PRIMARY KEY implicitly creates a unique index and sets NOT NULL. \
            ACCESS EXCLUSIVE lock held for the index build and full table scan."
            .into(),
        recipe: None,
        pg_version_note: None,
        statement_sql: stmt_sql.into(),
        estimated_duration: table_bytes.map(estimate_index_build_duration),
        assumptions: vec![],
    }]
}

pub fn analyse_validate_constraint(
    cmd: &protobuf::AlterTableCmd,
    table: &str,
    table_bytes: Option<i64>,
    stmt_sql: &str,
) -> Finding {
    let con_name = &cmd.name;

    Finding {
        rule_id: "validate-constraint".into(),
        risk_level: adjust_risk_for_size(RiskLevel::Low, table_bytes),
        confidence: if table_bytes.is_some() {
            Confidence::Definite
        } else {
            Confidence::NeedsCatalog
        },
        lock_mode: LockMode::ShareUpdateExclusive,
        rewrite: RewriteRisk::None,
        affected_table: Some(table.into()),
        summary: format!(
            "VALIDATE CONSTRAINT \"{con_name}\" on \"{table}\" (non-blocking scan)"
        ),
        explanation: format!(
            "SHARE UPDATE EXCLUSIVE lock does not block reads or writes. \
             Scans \"{table}\" to verify all rows satisfy the constraint.{}",
            match table_bytes {
                Some(b) => format!(" Table is {}.", human_size(b)),
                None => " Duration depends on table size.".into(),
            }
        ),
        recipe: None,
        pg_version_note: None,
        statement_sql: stmt_sql.into(),
        estimated_duration: table_bytes.map(estimate_scan_duration),
        assumptions: vec![],
    }
}
