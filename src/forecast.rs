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

    let fast = (bytes / (throughput_high_mbps * mb)).max(0.1);
    let slow = (bytes / (throughput_low_mbps * mb)).max(fast);

    let worst = match baseline {
        Some(b) => slow + (b.max_age_ms / 1000.0),
        None => slow * 3.0,
    };

    let mut assumptions = vec![
        ForecastAssumption {
            factor: "table size".into(),
            assumed: human_size(total_bytes).to_string(),
            source: AssumptionSource::Catalog,
        },
        ForecastAssumption {
            factor: "IO throughput".into(),
            assumed: format!("{throughput_low_mbps:.0}-{throughput_high_mbps:.0} MB/s"),
            source: AssumptionSource::Assumed,
        },
        ForecastAssumption {
            factor: "cache state".into(),
            assumed: "warm shared_buffers (fast), cold reads (slow)".into(),
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
                assumed: "no workload data, worst = 3x slow heuristic".into(),
                source: AssumptionSource::Assumed,
            });
        }
    }

    DurationForecast {
        fast_seconds: fast,
        slow_seconds: slow,
        worst_seconds: worst,
        assumptions,
    }
}

pub fn forecast_scan(
    bytes: i64,
    baseline: Option<&TransactionBaseline>,
    throughput: Option<(f64, f64)>,
) -> DurationForecast {
    let (low, high) = throughput.unwrap_or((100.0, 200.0));
    forecast_duration(bytes, low, high, baseline)
}

pub fn forecast_rewrite(
    bytes: i64,
    baseline: Option<&TransactionBaseline>,
    throughput: Option<(f64, f64)>,
) -> DurationForecast {
    let (low, high) = throughput.unwrap_or((50.0, 100.0));
    forecast_duration(bytes, low, high, baseline)
}

pub fn forecast_index_build(
    bytes: i64,
    baseline: Option<&TransactionBaseline>,
    throughput: Option<(f64, f64)>,
) -> DurationForecast {
    let (low, high) = throughput.unwrap_or((30.0, 80.0));
    forecast_duration(bytes, low, high, baseline)
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
            let queued_fast = (qf.calls_per_sec * duration.fast_seconds).ceil() as u64;
            let queued_slow = (qf.calls_per_sec * duration.slow_seconds).ceil() as u64;
            BlockedQueryForecast {
                query_label: qf.label.clone(),
                normalised_sql: qf.normalised_sql.clone(),
                calls_per_sec: qf.calls_per_sec,
                queued_at_fast: queued_fast,
                queued_at_slow: queued_slow,
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
        let forecast = forecast_scan(10_737_418_240, Some(&baseline), None);

        assert!(forecast.fast_seconds > 0.0);
        assert!(forecast.slow_seconds >= forecast.fast_seconds);
        assert!(forecast.worst_seconds >= forecast.slow_seconds);
        assert!((forecast.worst_seconds - forecast.slow_seconds - 5.0).abs() < 0.1);
    }

    #[test]
    fn scan_duration_without_baseline_uses_heuristic() {
        let forecast = forecast_scan(10_737_418_240, None, None);
        let expected_worst = forecast.slow_seconds * 3.0;
        assert!((forecast.worst_seconds - expected_worst).abs() < 0.01);
    }

    #[test]
    fn access_exclusive_blocks_all_dml() {
        let baseline = test_baseline();
        let duration = forecast_scan(1_073_741_824, Some(&baseline), None);

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
        let duration = forecast_scan(1_073_741_824, Some(&baseline), None);

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
        let duration = forecast_scan(1_073_741_824, Some(&baseline), None);

        let select = test_family("orders", DmlKind::Select, 100.0);
        let insert = test_family("orders", DmlKind::Insert, 50.0);

        let families: Vec<&QueryFamily> = vec![&select, &insert];
        let blocked = forecast_blocked_queries(LockMode::ShareUpdateExclusive, &duration, &families);

        assert_eq!(blocked.len(), 0);
    }

    #[test]
    fn queue_depth_calculation() {
        let duration = DurationForecast {
            fast_seconds: 2.0,
            slow_seconds: 8.0,
            worst_seconds: 12.0,
            assumptions: vec![],
        };

        let family = test_family("orders", DmlKind::Select, 135.0);
        let families: Vec<&QueryFamily> = vec![&family];
        let blocked = forecast_blocked_queries(LockMode::AccessExclusive, &duration, &families);

        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].queued_at_fast, 270);
        assert_eq!(blocked[0].queued_at_slow, 1080);
    }
}
