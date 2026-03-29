use crate::locks;
use crate::types::*;
use crate::workload::{QueryFamily, TransactionBaseline};

pub fn forecast_duration(
    total_bytes: i64,
    throughput_low_mbps: f64,
    throughput_high_mbps: f64,
    baseline: Option<&TransactionBaseline>,
) -> DurationForecast {
    let bytes = total_bytes as f64;
    let mb = 1024.0 * 1024.0;

    let p50 = (bytes / (throughput_high_mbps * mb)).max(0.1);
    let p90 = (bytes / (throughput_low_mbps * mb)).max(p50);

    let worst = match baseline {
        Some(b) => p90 + (b.max_age_ms / 1000.0),
        None => p90 * 3.0,
    };

    let mut assumptions = vec![
        ForecastAssumption {
            factor: "table size".into(),
            assumed: human_size(total_bytes).to_string(),
            source: AssumptionSource::Catalog,
        },
        ForecastAssumption {
            factor: "cache state".into(),
            assumed: "warm shared_buffers at p50, cold at p90".into(),
            source: AssumptionSource::Assumed,
        },
    ];

    match baseline {
        Some(b) => {
            assumptions.push(ForecastAssumption {
                factor: "lock acquisition delay".into(),
                assumed: format!(
                    "worst case adds {:.1}s from longest observed transaction",
                    b.max_age_ms / 1000.0
                ),
                source: AssumptionSource::Workload,
            });
        }
        None => {
            assumptions.push(ForecastAssumption {
                factor: "lock acquisition delay".into(),
                assumed: "no workload data, worst = 3x p90 heuristic".into(),
                source: AssumptionSource::Assumed,
            });
        }
    }

    DurationForecast {
        p50_seconds: p50,
        p90_seconds: p90,
        worst_seconds: worst,
        assumptions,
    }
}

pub fn forecast_scan(bytes: i64, baseline: Option<&TransactionBaseline>) -> DurationForecast {
    forecast_duration(bytes, 100.0, 200.0, baseline)
}

pub fn forecast_rewrite(bytes: i64, baseline: Option<&TransactionBaseline>) -> DurationForecast {
    forecast_duration(bytes, 50.0, 100.0, baseline)
}

pub fn forecast_index_build(bytes: i64, baseline: Option<&TransactionBaseline>) -> DurationForecast {
    forecast_duration(bytes, 30.0, 80.0, baseline)
}

pub fn forecast_blocked_queries(
    lock_held: LockMode,
    duration: &DurationForecast,
    families: &[&QueryFamily],
) -> Vec<BlockedQueryForecast> {
    let mut blocked: Vec<BlockedQueryForecast> = families
        .iter()
        .filter(|qf| locks::conflicts(qf.lock_mode, lock_held))
        .map(|qf| {
            let queued_p50 = (qf.calls_per_sec * duration.p50_seconds).ceil() as u64;
            let queued_p90 = (qf.calls_per_sec * duration.p90_seconds).ceil() as u64;
            BlockedQueryForecast {
                query_label: qf.label.clone(),
                normalised_sql: qf.normalised_sql.clone(),
                calls_per_sec: qf.calls_per_sec,
                queued_at_p50: queued_p50,
                queued_at_p90: queued_p90,
            }
        })
        .collect();

    blocked.sort_by(|a, b| b.calls_per_sec.partial_cmp(&a.calls_per_sec).unwrap_or(std::cmp::Ordering::Equal));
    blocked
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::locks::DmlKind;

    fn test_baseline() -> TransactionBaseline {
        TransactionBaseline {
            active_sessions: 10,
            idle_in_transaction: 2,
            median_age_ms: 50.0,
            p95_age_ms: 200.0,
            max_age_ms: 5000.0,
        }
    }

    fn test_family(table: &str, kind: DmlKind, qps: f64) -> QueryFamily {
        QueryFamily {
            queryid: 1,
            normalised_sql: format!("{kind} ... FROM {table}"),
            label: format!("{kind} on {table}"),
            tables: vec![table.into()],
            dml_kind: kind,
            lock_mode: kind.lock_mode(),
            calls_per_sec: qps,
            mean_exec_ms: 5.0,
            p95_exec_ms: Some(15.0),
        }
    }

    #[test]
    fn scan_duration_with_baseline() {
        let baseline = test_baseline();
        let forecast = forecast_scan(10_737_418_240, Some(&baseline));

        assert!(forecast.p50_seconds > 0.0);
        assert!(forecast.p90_seconds >= forecast.p50_seconds);
        assert!(forecast.worst_seconds >= forecast.p90_seconds);
        assert!((forecast.worst_seconds - forecast.p90_seconds - 5.0).abs() < 0.1);
    }

    #[test]
    fn scan_duration_without_baseline_uses_heuristic() {
        let forecast = forecast_scan(10_737_418_240, None);
        let expected_worst = forecast.p90_seconds * 3.0;
        assert!((forecast.worst_seconds - expected_worst).abs() < 0.01);
    }

    #[test]
    fn access_exclusive_blocks_all_dml() {
        let baseline = test_baseline();
        let duration = forecast_scan(1_073_741_824, Some(&baseline));

        let select = test_family("orders", DmlKind::Select, 100.0);
        let insert = test_family("orders", DmlKind::Insert, 50.0);

        let families: Vec<&QueryFamily> = vec![&select, &insert];
        let blocked = forecast_blocked_queries(LockMode::AccessExclusive, &duration, &families);

        assert_eq!(blocked.len(), 2);
        assert!(blocked[0].calls_per_sec >= blocked[1].calls_per_sec);
    }

    #[test]
    fn share_lock_blocks_writes_not_reads() {
        let baseline = test_baseline();
        let duration = forecast_scan(1_073_741_824, Some(&baseline));

        let select = test_family("orders", DmlKind::Select, 100.0);
        let insert = test_family("orders", DmlKind::Insert, 50.0);

        let families: Vec<&QueryFamily> = vec![&select, &insert];
        let blocked = forecast_blocked_queries(LockMode::Share, &duration, &families);

        assert_eq!(blocked.len(), 1);
        assert!(blocked[0].query_label.contains("INSERT"));
    }

    #[test]
    fn share_update_exclusive_does_not_block_dml() {
        let baseline = test_baseline();
        let duration = forecast_scan(1_073_741_824, Some(&baseline));

        let select = test_family("orders", DmlKind::Select, 100.0);
        let insert = test_family("orders", DmlKind::Insert, 50.0);

        let families: Vec<&QueryFamily> = vec![&select, &insert];
        let blocked = forecast_blocked_queries(LockMode::ShareUpdateExclusive, &duration, &families);

        assert_eq!(blocked.len(), 0);
    }

    #[test]
    fn queue_depth_calculation() {
        let duration = DurationForecast {
            p50_seconds: 2.0,
            p90_seconds: 8.0,
            worst_seconds: 12.0,
            assumptions: vec![],
        };

        let family = test_family("orders", DmlKind::Select, 135.0);
        let families: Vec<&QueryFamily> = vec![&family];
        let blocked = forecast_blocked_queries(LockMode::AccessExclusive, &duration, &families);

        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].queued_at_p50, 270);
        assert_eq!(blocked[0].queued_at_p90, 1080);
    }
}
