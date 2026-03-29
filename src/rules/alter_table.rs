use pg_query::protobuf::{self, node, AlterTableType, ConstrType};

use crate::forecast;
use crate::parse::format_relation;
use crate::recipe;
use crate::types::*;

use super::{RuleContext, extract_string_value, type_name_to_string};

pub fn analyse_alter_table(
    alter: &protobuf::AlterTableStmt,
    stmt_sql: &str,
    ctx: &RuleContext,
) -> Vec<Finding> {
    let table = alter
        .relation
        .as_ref()
        .map(format_relation)
        .unwrap_or_else(|| "unknown".into());

    let table_bytes = ctx.catalog.and_then(|c| c.table_bytes(&table));
    let mut findings = Vec::new();

    for cmd_node in &alter.cmds {
        let Some(ref inner) = cmd_node.node else {
            continue;
        };
        let node::Node::AlterTableCmd(cmd) = inner else {
            continue;
        };

        let subtype = AlterTableType::try_from(cmd.subtype).unwrap_or(AlterTableType::Undefined);

        match subtype {
            AlterTableType::AtAddColumn => {
                findings.extend(analyse_add_column(cmd, &table, table_bytes, stmt_sql, ctx));
            }
            AlterTableType::AtDropColumn => {
                findings.push(Finding {
                    rule_id: "drop-column".into(),
                    risk_level: RiskLevel::Medium,
                    confidence: ConfidenceLedger::static_only(
                        vec!["ACCESS EXCLUSIVE lock for DROP COLUMN (brief, catalog only)".into()],
                    ),
                    lock_mode: LockMode::AccessExclusive,
                    rewrite: RewriteRisk::None,
                    affected_table: Some(table.clone()),
                    summary: format!("DROP COLUMN on \"{table}\" takes ACCESS EXCLUSIVE lock"),
                    explanation: "Dropping a column marks it as invisible in the catalog. \
                        No table rewrite occurs. The ACCESS EXCLUSIVE lock is brief but blocks \
                        all concurrent queries for the duration of the catalog update."
                        .into(),
                    recipe: Some(recipe::drop_column(&table, &cmd.name)),
                    pg_version_note: None,
                    statement_sql: stmt_sql.into(),
                    duration_forecast: None,
                });
            }
            AlterTableType::AtSetNotNull => {
                findings.push(analyse_set_not_null(cmd, &table, table_bytes, stmt_sql, ctx));
            }
            AlterTableType::AtAlterColumnType => {
                findings.push(analyse_alter_type(cmd, &table, table_bytes, stmt_sql, ctx));
            }
            AlterTableType::AtAddConstraint => {
                findings.extend(
                    super::constraints::analyse_add_constraint(cmd, &table, table_bytes, stmt_sql, ctx),
                );
            }
            AlterTableType::AtValidateConstraint => {
                findings.push(
                    super::constraints::analyse_validate_constraint(cmd, &table, table_bytes, stmt_sql, ctx),
                );
            }
            AlterTableType::AtAttachPartition => {
                let risk = adjust_risk_for_size(RiskLevel::High, table_bytes);
                findings.push(Finding {
                    rule_id: "attach-partition".into(),
                    risk_level: risk,
                    confidence: match table_bytes {
                        Some(b) => ConfidenceLedger::with_catalog(
                            vec!["SHARE UPDATE EXCLUSIVE on parent, ACCESS EXCLUSIVE on partition".into()],
                            vec![format!("table size is {}", human_size(b))],
                        ),
                        None => ConfidenceLedger::static_only(
                            vec!["SHARE UPDATE EXCLUSIVE on parent, ACCESS EXCLUSIVE on partition".into()],
                        ),
                    },
                    lock_mode: LockMode::ShareUpdateExclusive,
                    rewrite: RewriteRisk::None,
                    affected_table: Some(table.clone()),
                    summary: format!("ATTACH PARTITION on \"{table}\" scans the partition"),
                    explanation: "ATTACH PARTITION acquires SHARE UPDATE EXCLUSIVE on the parent \
                        and ACCESS EXCLUSIVE on the partition. It scans the partition to verify \
                        the constraint unless a matching CHECK constraint is already validated."
                        .into(),
                    recipe: Some(recipe::attach_partition_safe(&table)),
                    pg_version_note: None,
                    statement_sql: stmt_sql.into(),
                    duration_forecast: table_bytes.map(|b| forecast::forecast_scan(b, ctx.transaction_baseline)),
                });
            }
            _ => {}
        }
    }

    findings
}

fn analyse_add_column(
    cmd: &protobuf::AlterTableCmd,
    table: &str,
    table_bytes: Option<i64>,
    stmt_sql: &str,
    ctx: &RuleContext,
) -> Vec<Finding> {
    let col_def = match cmd.def.as_ref().and_then(|d| d.node.as_ref()) {
        Some(node::Node::ColumnDef(cd)) => cd,
        _ => return vec![],
    };

    let col_name = &col_def.colname;
    let default_expr = find_default_expr(col_def);
    let has_default = default_expr.is_some();
    let has_not_null = col_def.is_not_null || has_not_null_constraint(&col_def.constraints);
    let volatile = has_default && is_volatile_default(default_expr);

    if !has_default {
        return vec![Finding {
            rule_id: "add-column".into(),
            risk_level: RiskLevel::Low,
            confidence: ConfidenceLedger::static_only(
                vec!["ACCESS EXCLUSIVE lock for ADD COLUMN (metadata only, milliseconds)".into()],
            ),
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(table.into()),
            summary: format!("ADD COLUMN \"{col_name}\" on \"{table}\" (no default)"),
            explanation: "Metadata-only change. ACCESS EXCLUSIVE lock held for milliseconds. \
                No table rewrite. No scan."
                .into(),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            duration_forecast: None,
        }];
    }

    if volatile {
        let risk = adjust_risk_for_size(RiskLevel::Extreme, table_bytes);
        return vec![Finding {
            rule_id: "add-column-volatile-default".into(),
            risk_level: risk,
            confidence: match table_bytes {
                Some(b) => ConfidenceLedger::with_catalog(
                    vec!["volatile DEFAULT forces full table rewrite under ACCESS EXCLUSIVE".into()],
                    vec![format!("table size is {}", human_size(b))],
                ),
                None => ConfidenceLedger::static_only(
                    vec!["volatile DEFAULT forces full table rewrite under ACCESS EXCLUSIVE".into()],
                ),
            },
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::Required,
            affected_table: Some(table.into()),
            summary: format!(
                "ADD COLUMN \"{col_name}\" on \"{table}\" with volatile DEFAULT triggers rewrite"
            ),
            explanation: "A volatile DEFAULT (e.g. now(), random()) forces PostgreSQL to \
                rewrite every row to materialise the value. ACCESS EXCLUSIVE lock held \
                for the entire rewrite."
                .into(),
            recipe: None,
            pg_version_note: Some(
                "Volatile defaults always trigger a rewrite regardless of PG version.".into(),
            ),
            statement_sql: stmt_sql.into(),
            duration_forecast: table_bytes.map(|b| forecast::forecast_rewrite(b, ctx.transaction_baseline)),
        }];
    }

    if ctx.pg_version.at_least(11) {
        let mut findings = vec![Finding {
            rule_id: "add-column-default".into(),
            risk_level: RiskLevel::Low,
            confidence: ConfidenceLedger::static_only(
                vec![
                    "non-volatile DEFAULT on PG 11+ is metadata only".into(),
                    format!("PostgreSQL {} assumed", ctx.pg_version.major),
                ],
            ),
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(table.into()),
            summary: format!(
                "ADD COLUMN \"{col_name}\" on \"{table}\" with non-volatile DEFAULT (safe on PG 11+)"
            ),
            explanation: "Non-volatile DEFAULT on PG 11+ is metadata-only. The default \
                is stored in pg_attribute and applied on read. No table rewrite."
                .into(),
            recipe: None,
            pg_version_note: Some(
                "On PG < 11 this would trigger a full table rewrite.".into(),
            ),
            statement_sql: stmt_sql.into(),
            duration_forecast: None,
        }];

        if has_not_null {
            findings.push(Finding {
                rule_id: "add-column-not-null".into(),
                risk_level: RiskLevel::Low,
                confidence: ConfidenceLedger::static_only(
                    vec!["NOT NULL with non-volatile DEFAULT safe on PG 11+ (no scan)".into()],
                ),
                lock_mode: LockMode::AccessExclusive,
                rewrite: RewriteRisk::None,
                affected_table: Some(table.into()),
                summary: format!(
                    "ADD COLUMN \"{col_name}\" NOT NULL with DEFAULT is safe on PG 11+"
                ),
                explanation: "When adding a NOT NULL column with a non-volatile DEFAULT on \
                    PG 11+, the default guarantees all existing rows satisfy the constraint. \
                    No scan needed."
                    .into(),
                recipe: None,
                pg_version_note: None,
                statement_sql: stmt_sql.into(),
                duration_forecast: None,
            });
        }

        return findings;
    }

    let risk = adjust_risk_for_size(RiskLevel::Extreme, table_bytes);
    vec![Finding {
        rule_id: "add-column-default".into(),
        risk_level: risk,
        confidence: match table_bytes {
            Some(b) => ConfidenceLedger::with_catalog(
                vec![
                    "PG < 11 rewrites table for any DEFAULT under ACCESS EXCLUSIVE".into(),
                    format!("PostgreSQL {} assumed", ctx.pg_version.major),
                ],
                vec![format!("table size is {}", human_size(b))],
            ),
            None => ConfidenceLedger::static_only(
                vec![
                    "PG < 11 rewrites table for any DEFAULT under ACCESS EXCLUSIVE".into(),
                    format!("PostgreSQL {} assumed", ctx.pg_version.major),
                ],
            ),
        },
        lock_mode: LockMode::AccessExclusive,
        rewrite: RewriteRisk::Conditional {
            reason: "PG < 11 rewrites table for any DEFAULT".into(),
        },
        affected_table: Some(table.into()),
        summary: format!(
            "ADD COLUMN \"{col_name}\" on \"{table}\" with DEFAULT triggers rewrite on PG < 11"
        ),
        explanation: "On PostgreSQL versions before 11, adding a column with any DEFAULT \
            value rewrites every row. ACCESS EXCLUSIVE lock held for the entire rewrite."
            .into(),
        recipe: None,
        pg_version_note: Some("Upgrade to PG 11+ to avoid this rewrite.".into()),
        statement_sql: stmt_sql.into(),
        duration_forecast: table_bytes.map(|b| forecast::forecast_rewrite(b, ctx.transaction_baseline)),
    }]
}

fn analyse_set_not_null(
    cmd: &protobuf::AlterTableCmd,
    table: &str,
    table_bytes: Option<i64>,
    stmt_sql: &str,
    ctx: &RuleContext,
) -> Finding {
    let column = &cmd.name;
    let base_risk = RiskLevel::High;
    let risk = adjust_risk_for_size(base_risk, table_bytes);

    let explanation = if ctx.pg_version.at_least(12) {
        "SET NOT NULL acquires ACCESS EXCLUSIVE lock and performs a full table scan \
         to verify no NULLs exist. On PG 12+, the scan is skipped if a pre-validated \
         CHECK (column IS NOT NULL) constraint already exists."
    } else {
        "SET NOT NULL acquires ACCESS EXCLUSIVE lock and performs a full table scan \
         to verify no NULLs exist. On PG < 12 there is no way to avoid this scan."
    };

    let recipe = if ctx.pg_version.at_least(12) {
        Some(recipe::set_not_null_safe(table, column))
    } else {
        None
    };

    Finding {
        rule_id: "set-not-null".into(),
        risk_level: risk,
        confidence: match table_bytes {
            Some(b) => ConfidenceLedger::with_catalog(
                vec![
                    "ACCESS EXCLUSIVE lock with full table scan for SET NOT NULL".into(),
                    format!("PostgreSQL {} assumed", ctx.pg_version.major),
                ],
                vec![format!("table size is {}", human_size(b))],
            ),
            None => ConfidenceLedger::static_only(
                vec![
                    "ACCESS EXCLUSIVE lock with full table scan for SET NOT NULL".into(),
                    format!("PostgreSQL {} assumed", ctx.pg_version.major),
                ],
            ),
        },
        lock_mode: LockMode::AccessExclusive,
        rewrite: RewriteRisk::None,
        affected_table: Some(table.into()),
        summary: format!("SET NOT NULL on \"{table}\".\"{column}\" requires full table scan"),
        explanation: explanation.into(),
        recipe,
        pg_version_note: if ctx.pg_version.at_least(12) {
            Some("PG 12+ can skip the scan with a pre-validated CHECK constraint.".into())
        } else {
            None
        },
        statement_sql: stmt_sql.into(),
        duration_forecast: table_bytes.map(|b| forecast::forecast_scan(b, ctx.transaction_baseline)),
    }
}

fn analyse_alter_type(
    cmd: &protobuf::AlterTableCmd,
    table: &str,
    table_bytes: Option<i64>,
    stmt_sql: &str,
    ctx: &RuleContext,
) -> Finding {
    let column = &cmd.name;

    let new_type = cmd
        .def
        .as_ref()
        .and_then(|d| d.node.as_ref())
        .and_then(|n| match n {
            node::Node::ColumnDef(cd) => cd.type_name.as_ref().map(type_name_to_string),
            _ => None,
        })
        .unwrap_or_else(|| "unknown".into());

    let rewrite_likely = type_change_causes_rewrite(&new_type, stmt_sql);

    if rewrite_likely {
        let risk = adjust_risk_for_size(RiskLevel::High, table_bytes);
        Finding {
            rule_id: "change-column-type".into(),
            risk_level: risk,
            confidence: match table_bytes {
                Some(b) => ConfidenceLedger::with_catalog(
                    vec!["type change requires full table rewrite under ACCESS EXCLUSIVE".into()],
                    vec![format!("table size is {}", human_size(b))],
                ),
                None => ConfidenceLedger::static_only(
                    vec!["type change requires full table rewrite under ACCESS EXCLUSIVE".into()],
                ),
            },
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::Required,
            affected_table: Some(table.into()),
            summary: format!(
                "ALTER COLUMN TYPE on \"{table}\".\"{column}\" to {new_type} triggers table rewrite"
            ),
            explanation: format!(
                "Changing the column type to {new_type} requires rewriting every row \
                 because the binary representation changes. ACCESS EXCLUSIVE lock held \
                 for the entire rewrite, blocking all reads and writes."
            ),
            recipe: Some(recipe::change_column_type(table, column, &new_type)),
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            duration_forecast: table_bytes.map(|b| forecast::forecast_rewrite(b, ctx.transaction_baseline)),
        }
    } else {
        Finding {
            rule_id: "change-column-type".into(),
            risk_level: RiskLevel::Low,
            confidence: ConfidenceLedger::static_only(
                vec!["no rewrite needed, ACCESS EXCLUSIVE lock is brief (catalog only)".into()],
            ),
            lock_mode: LockMode::AccessExclusive,
            rewrite: RewriteRisk::None,
            affected_table: Some(table.into()),
            summary: format!(
                "ALTER COLUMN TYPE on \"{table}\".\"{column}\" to {new_type} (no rewrite)"
            ),
            explanation: format!(
                "Changing to {new_type} does not require a table rewrite. The ACCESS EXCLUSIVE \
                 lock is brief (catalog update only)."
            ),
            recipe: None,
            pg_version_note: None,
            statement_sql: stmt_sql.into(),
            duration_forecast: None,
        }
    }
}

fn type_change_causes_rewrite(new_type: &str, stmt_sql: &str) -> bool {
    let lower = new_type.to_lowercase();
    let sql_lower = stmt_sql.to_lowercase();

    if sql_lower.contains("using") {
        return true;
    }

    let rewrite_types = [
        "bigint", "int8", "smallint", "int2", "integer", "int4", "int",
        "real", "float4", "double precision", "float8",
        "numeric", "decimal",
        "timestamptz", "timestamp with time zone",
        "timetz", "time with time zone",
        "bytea", "boolean", "bool",
    ];

    for t in rewrite_types {
        if lower == t {
            return true;
        }
    }

    false
}

fn is_volatile_default(default_expr: Option<&protobuf::Node>) -> bool {
    let Some(expr) = default_expr else {
        return false;
    };
    let Some(ref inner) = expr.node else {
        return false;
    };

    match inner {
        node::Node::FuncCall(fc) => {
            let func_name: String = fc
                .funcname
                .iter()
                .filter_map(extract_string_value)
                .collect::<Vec<_>>()
                .join(".");

            let volatile_funcs = [
                "now",
                "clock_timestamp",
                "statement_timestamp",
                "transaction_timestamp",
                "current_timestamp",
                "random",
                "gen_random_uuid",
                "uuid_generate_v4",
                "txid_current",
                "timeofday",
            ];

            let bare_name = func_name.rsplit('.').next().unwrap_or(&func_name);
            volatile_funcs.iter().any(|v| bare_name.eq_ignore_ascii_case(v))
        }
        node::Node::SqlvalueFunction(_) => true,
        _ => false,
    }
}

fn find_default_expr(col_def: &protobuf::ColumnDef) -> Option<&protobuf::Node> {
    if let Some(ref rd) = col_def.raw_default {
        return Some(rd.as_ref());
    }
    for c in &col_def.constraints {
        if let Some(node::Node::Constraint(con)) = c.node.as_ref()
            && ConstrType::try_from(con.contype) == Ok(ConstrType::ConstrDefault)
                && let Some(ref expr) = con.raw_expr {
                    return Some(expr.as_ref());
                }
    }
    None
}

fn has_not_null_constraint(constraints: &[protobuf::Node]) -> bool {
    constraints.iter().any(|c| {
        matches!(
            c.node.as_ref(),
            Some(node::Node::Constraint(con)) if ConstrType::try_from(con.contype) == Ok(ConstrType::ConstrNotnull)
        )
    })
}
