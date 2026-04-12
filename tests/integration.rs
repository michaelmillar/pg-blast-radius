use std::collections::HashMap;

use pg_blast_radius::analysis;
use pg_blast_radius::catalog::CatalogInfo;
use pg_blast_radius::locks::DmlKind;
use pg_blast_radius::rules::{PgVersion, RuleContext, analyse};
use pg_blast_radius::types::*;
use pg_blast_radius::workload::{QueryFamily, TransactionBaseline, WorkloadProfile};

fn mock_catalog(tables: &[(&str, i64, i64)]) -> CatalogInfo {
    let mut map = HashMap::new();
    for &(name, bytes, rows) in tables {
        map.insert(
            name.to_string(),
            TableSize {
                total_bytes: bytes,
                row_estimate: rows,
                human_size: human_size(bytes),
            },
        );
    }
    CatalogInfo {
        tables: map,
        workload: None,
    }
}

fn mock_workload(families: Vec<QueryFamily>) -> WorkloadProfile {
    WorkloadProfile {
        query_families: families,
        transaction_baseline: TransactionBaseline {
            active_sessions: 10,
            idle_in_transaction: 2,
            median_age_ms: 50.0,
            p95_age_ms: 200.0,
            max_age_ms: 5000.0,
        },
        collected_at: "2026-03-29T10:00:00Z".into(),
        stats_reset: Some("2026-03-01T00:00:00Z".into()),
        stats_window_seconds: Some(28.0 * 86400.0),
        unparseable_queries: 0,
    }
}

fn analyse_fixture(sql: &str, catalog: Option<&CatalogInfo>) -> AnalysisResult {
    let ctx = RuleContext {
        pg_version: PgVersion { major: 16 },
        catalog,
        transaction_baseline: catalog
            .and_then(|c| c.workload.as_ref())
            .map(|w| &w.transaction_baseline),
        io_throughput: None,
    };
    let findings = analyse(sql, &ctx).unwrap();
    let workload = catalog.and_then(|c| c.workload.as_ref());
    analysis::build_result("test.sql", findings, workload)
}

fn fixture(name: &str) -> String {
    std::fs::read_to_string(format!("testdata/fixtures/{name}")).unwrap()
}

#[test]
fn add_column_simple_is_low_risk() {
    let result = analyse_fixture(&fixture("add_column_simple.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::Low);
    assert_eq!(result.findings.len(), 1);
    assert_eq!(result.findings[0].rule_id, "add-column");
    assert_eq!(result.findings[0].lock_mode, LockMode::AccessExclusive);
    assert!(matches!(result.findings[0].rewrite, RewriteRisk::None));
}

#[test]
fn add_column_with_default_is_low_on_pg16() {
    let result = analyse_fixture(&fixture("add_column_with_default.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::Low);
    assert_eq!(result.findings[0].rule_id, "add-column-default");
}

#[test]
fn add_column_volatile_default_is_extreme() {
    let result = analyse_fixture(&fixture("add_column_volatile_default.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::Extreme);
    assert_eq!(result.findings[0].rule_id, "add-column-volatile-default");
    assert!(matches!(result.findings[0].rewrite, RewriteRisk::Required));
}

#[test]
fn add_column_not_null_default_is_low_on_pg16() {
    let result = analyse_fixture(&fixture("add_column_not_null_default.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::Low);
}

#[test]
fn drop_column_is_medium() {
    let result = analyse_fixture(&fixture("drop_column.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::Medium);
    assert_eq!(result.findings[0].rule_id, "drop-column");
    assert!(result.findings[0].recipe.is_some());
}

#[test]
fn alter_type_safe_is_low() {
    let result = analyse_fixture(&fixture("alter_type_safe.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::Low);
    assert_eq!(result.findings[0].rule_id, "change-column-type");
    assert!(matches!(result.findings[0].rewrite, RewriteRisk::None));
}

#[test]
fn alter_type_rewrite_is_high_without_catalog() {
    let result = analyse_fixture(&fixture("alter_type_rewrite.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::High);
    assert!(matches!(result.findings[0].rewrite, RewriteRisk::Required));
    assert!(result.findings[0].recipe.is_some());
}

#[test]
fn alter_type_rewrite_is_extreme_on_huge_table() {
    let catalog = mock_catalog(&[("orders", 50_000_000_000, 800_000_000)]);
    let result = analyse_fixture(&fixture("alter_type_rewrite.sql"), Some(&catalog));
    assert_eq!(result.overall_risk, RiskLevel::Extreme);
    assert!(result.findings[0].duration_forecast.is_some());
}

#[test]
fn set_not_null_is_high() {
    let result = analyse_fixture(&fixture("set_not_null.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::High);
    assert_eq!(result.findings[0].rule_id, "set-not-null");
    assert!(result.findings[0].recipe.is_some());
}

#[test]
fn set_not_null_safe_pattern_produces_multiple_findings() {
    let result = analyse_fixture(&fixture("set_not_null_safe_pattern.sql"), None);
    assert!(result.findings.len() >= 3);
}

#[test]
fn add_check_constraint_is_high() {
    let result = analyse_fixture(&fixture("add_check_constraint.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::High);
    assert!(result.findings[0].recipe.is_some());
}

#[test]
fn add_check_not_valid_is_low() {
    let result = analyse_fixture(&fixture("add_check_not_valid.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::Low);
    assert_eq!(result.findings[0].rule_id, "add-check-not-valid");
}

#[test]
fn validate_constraint_is_low() {
    let result = analyse_fixture(&fixture("validate_constraint.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::Low);
    assert_eq!(result.findings[0].lock_mode, LockMode::ShareUpdateExclusive);
}

#[test]
fn create_index_is_high() {
    let result = analyse_fixture(&fixture("create_index.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::High);
    assert_eq!(result.findings[0].lock_mode, LockMode::Share);
    assert!(result.findings[0].recipe.is_some());
}

#[test]
fn create_index_concurrently_is_low() {
    let result = analyse_fixture(&fixture("create_index_concurrently.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::Low);
    assert_eq!(result.findings[0].lock_mode, LockMode::ShareUpdateExclusive);
}

#[test]
fn drop_index_is_high() {
    let result = analyse_fixture(&fixture("drop_index.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::High);
    assert_eq!(result.findings[0].lock_mode, LockMode::AccessExclusive);
    assert!(result.findings[0].recipe.is_some());
}

#[test]
fn drop_index_concurrently_is_low() {
    let result = analyse_fixture(&fixture("drop_index_concurrently.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::Low);
}

#[test]
fn add_foreign_key_is_high() {
    let result = analyse_fixture(&fixture("add_foreign_key.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::High);
    assert!(result.findings[0].recipe.is_some());
}

#[test]
fn add_foreign_key_not_valid_is_medium() {
    let result = analyse_fixture(&fixture("add_foreign_key_not_valid.sql"), None);
    let fk_finding = result
        .findings
        .iter()
        .find(|f| f.rule_id.starts_with("add-foreign-key"))
        .unwrap();
    assert_eq!(fk_finding.risk_level, RiskLevel::Medium);
}

#[test]
fn multi_statement_aggregates_blast_radius() {
    let result = analyse_fixture(&fixture("multi_statement_dangerous.sql"), None);
    assert_eq!(result.overall_risk, RiskLevel::High);
    assert!(result.findings.len() >= 3);

    let users_blast = result
        .blast_radius
        .per_table
        .iter()
        .find(|t| t.table_name == "users")
        .unwrap();
    assert_eq!(users_blast.statement_count, 3);
    assert_eq!(users_blast.strongest_lock, LockMode::AccessExclusive);
    assert!(users_blast.recommendation.is_some());
}

#[test]
fn create_index_on_tiny_table_is_downgraded() {
    let catalog = mock_catalog(&[("sessions", 16_384, 50)]);
    let sql = "CREATE INDEX idx_sessions_user ON sessions (user_id);";
    let result = analyse_fixture(sql, Some(&catalog));
    assert_eq!(result.overall_risk, RiskLevel::Medium);
}

#[test]
fn create_index_on_huge_table_is_extreme() {
    let catalog = mock_catalog(&[("orders", 50_000_000_000, 800_000_000)]);
    let sql = "CREATE INDEX idx_orders_cust ON orders (customer_id);";
    let result = analyse_fixture(sql, Some(&catalog));
    assert_eq!(result.overall_risk, RiskLevel::Extreme);
}

#[test]
fn json_output_is_valid() {
    let result = analyse_fixture(&fixture("create_index.sql"), None);
    let json = serde_json::to_string(&[&result]).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(parsed.is_array());
}

#[test]
fn pg10_add_column_default_is_extreme() {
    let ctx = RuleContext {
        pg_version: PgVersion { major: 10 },
        catalog: None,
        transaction_baseline: None,
        io_throughput: None,
    };
    let sql = "ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active';";
    let findings = analyse(sql, &ctx).unwrap();
    let result = analysis::build_result("test.sql", findings, None);
    assert_eq!(result.overall_risk, RiskLevel::Extreme);
}

#[test]
fn confidence_is_static_without_catalog() {
    let result = analyse_fixture(&fixture("create_index.sql"), None);
    assert_eq!(result.overall_confidence, ConfidenceGrade::Static);
    assert_eq!(result.findings[0].confidence.grade, ConfidenceGrade::Static);
}

#[test]
fn confidence_is_estimated_with_catalog() {
    let catalog = mock_catalog(&[("orders", 50_000_000_000, 800_000_000)]);
    let sql = "CREATE INDEX idx_orders_cust ON orders (customer_id);";
    let result = analyse_fixture(sql, Some(&catalog));
    assert_eq!(result.findings[0].confidence.grade, ConfidenceGrade::Estimated);
    assert!(!result.findings[0].confidence.from_catalog.is_empty());
}

#[test]
fn duration_forecast_has_fast_slow_worst() {
    let catalog = mock_catalog(&[("orders", 10_737_418_240, 100_000_000)]);
    let sql = "CREATE INDEX idx_orders_cust ON orders (customer_id);";
    let result = analyse_fixture(sql, Some(&catalog));
    let forecast = result.findings[0].duration_forecast.as_ref().unwrap();
    assert!(forecast.fast_seconds > 0.0);
    assert!(forecast.slow_seconds >= forecast.fast_seconds);
    assert!(forecast.worst_seconds >= forecast.slow_seconds);
}

#[test]
fn workload_aware_blocked_query_forecast() {
    let mut catalog = mock_catalog(&[("orders", 10_737_418_240, 100_000_000)]);
    catalog.workload = Some(mock_workload(vec![
        QueryFamily {
            queryid: 1,
            normalised_sql: "SELECT * FROM orders WHERE customer_id = $1".into(),
            label: "SELECT ... FROM orders WHERE customer_id = $1".into(),
            tables: vec!["orders".into()],
            dml_kind: DmlKind::Select,
            lock_mode: LockMode::AccessShare,
            calls_per_sec: 135.0,
            mean_exec_ms: 5.0,
            p95_exec_ms: Some(15.0),
        },
        QueryFamily {
            queryid: 2,
            normalised_sql: "INSERT INTO orders (customer_id, total) VALUES ($1, $2)".into(),
            label: "INSERT INTO orders (...)".into(),
            tables: vec!["orders".into()],
            dml_kind: DmlKind::Insert,
            lock_mode: LockMode::RowExclusive,
            calls_per_sec: 80.0,
            mean_exec_ms: 3.0,
            p95_exec_ms: Some(10.0),
        },
    ]));

    let sql = "CREATE INDEX idx_orders_status ON orders (status);";
    let result = analyse_fixture(sql, Some(&catalog));

    let orders_blast = result
        .blast_radius
        .per_table
        .iter()
        .find(|t| t.table_name == "orders")
        .unwrap();

    assert!(!orders_blast.blocked_queries.is_empty());
    assert!(orders_blast.total_blocked_qps > 0.0);
    assert_eq!(orders_blast.confidence.grade, ConfidenceGrade::Measured);
}

#[test]
fn share_lock_only_blocks_writes_in_forecast() {
    let mut catalog = mock_catalog(&[("orders", 10_737_418_240, 100_000_000)]);
    catalog.workload = Some(mock_workload(vec![
        QueryFamily {
            queryid: 1,
            normalised_sql: "SELECT * FROM orders WHERE id = $1".into(),
            label: "SELECT ... FROM orders".into(),
            tables: vec!["orders".into()],
            dml_kind: DmlKind::Select,
            lock_mode: LockMode::AccessShare,
            calls_per_sec: 100.0,
            mean_exec_ms: 2.0,
            p95_exec_ms: None,
        },
        QueryFamily {
            queryid: 2,
            normalised_sql: "INSERT INTO orders (...) VALUES (...)".into(),
            label: "INSERT INTO orders".into(),
            tables: vec!["orders".into()],
            dml_kind: DmlKind::Insert,
            lock_mode: LockMode::RowExclusive,
            calls_per_sec: 50.0,
            mean_exec_ms: 3.0,
            p95_exec_ms: None,
        },
    ]));

    let sql = "CREATE INDEX idx_orders_status ON orders (status);";
    let result = analyse_fixture(sql, Some(&catalog));

    let orders_blast = result
        .blast_radius
        .per_table
        .iter()
        .find(|t| t.table_name == "orders")
        .unwrap();

    assert_eq!(orders_blast.strongest_lock, LockMode::Share);
    assert_eq!(orders_blast.blocked_queries.len(), 1);
    assert!(orders_blast.blocked_queries[0].query_label.contains("INSERT"));
}

#[test]
fn insta_multi_statement() {
    let result = analyse_fixture(&fixture("multi_statement_dangerous.sql"), None);
    insta::assert_json_snapshot!(result);
}

#[test]
fn insta_create_index_large_table() {
    let catalog = mock_catalog(&[("orders", 50_000_000_000, 800_000_000)]);
    let sql = "CREATE INDEX idx_orders_customer ON orders (customer_id);";
    let result = analyse_fixture(sql, Some(&catalog));
    insta::assert_json_snapshot!(result);
}

#[test]
fn insta_set_not_null() {
    let result = analyse_fixture(&fixture("set_not_null.sql"), None);
    insta::assert_json_snapshot!(result);
}
