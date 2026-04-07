<p align="center">
  <img src="logo.svg" width="160" alt="pg-blast-radius logo" />
</p>

<h1 align="center">pg-blast-radius</h1>

<p align="center">
  Forecast what your PostgreSQL migration will block, for how long, on your actual workload.
</p>

<p align="center">
  <a href="#what-it-does">What it does</a> &middot;
  <a href="#quick-start">Quick start</a> &middot;
  <a href="#how-it-compares">How it compares</a> &middot;
  <a href="#ci-integration">CI</a>
</p>

---

https://github.com/user-attachments/assets/ee5c219c-5be6-4a9f-afe6-a2a8debff332

pg-blast-radius reads your migration SQL, connects read-only to your database, and reports:

- **Blocked query families** from `pg_stat_statements`, showing which production queries queue up and how many
- **Duration forecasts** as P50 / P90 / worst-case ranges, not single-point estimates
- **Rollout recipes** with step-by-step safer SQL when a migration should be split
- **Confidence ledger** stating what is known from docs, observed from catalog, inferred from workload, and assumed

Other tools tell you "this takes ACCESS EXCLUSIVE." This one tells you "this will queue 14,200 queries/min on your busiest table for an estimated 6..12 minutes."

## What it does

```
$ pg-blast-radius analyse migration.sql --stats-file prod-stats.json

  orders (34.0 GB, ~892M rows)
    Lock: ACCESS EXCLUSIVE (blocks all reads and writes)
    Duration: 16m (p50)  37m (p90)  37m (worst)
    Blocked queries: 3 families, 14202 calls/min combined
      SELECT ... FROM orders WHERE customer_id = $1   8100/min  ~129255 queued (p50)
      INSERT INTO orders (...)                        4800/min  ~76596 queued (p50)
      UPDATE orders SET status = $1 WHERE id = $2     1302/min  ~20777 queued (p50)
    Confidence: query load MEASURED, lock hold INFERRED
    3 statements combined

  EXTREME  ALTER COLUMN TYPE on "orders"."total" to numeric triggers table rewrite
    Estimated: 6m (p50)  12m (p90)  12m (worst)
    Rollout recipe: Expand/migrate/contract for "orders"."total"
      1. [expand]    ADD COLUMN "total_new" numeric
      2. [backfill]  UPDATE in batches
      3. [validate]  CREATE TRIGGER to sync during migration
      4. [switch]    Application reads from new column
      5. [contract]  DROP old column, trigger, rename

  EXTREME  CREATE INDEX "idx_orders_status" without CONCURRENTLY
    Estimated: 7m (p50)  19m (p90)  19m (worst)
    Rollout recipe: CREATE INDEX CONCURRENTLY

  EXTREME  ADD FOREIGN KEY scans 34 GB table
    Estimated: 3m (p50)  6m (p90)  6m (worst)
    Rollout recipe: ADD ... NOT VALID + VALIDATE CONSTRAINT

  Overall: EXTREME RISK | Confidence: ESTIMATED
  3 statements, 3 safer alternatives suggested.
```

Without a database connection, it still analyses lock modes, rewrite risk, and generates recipes. With one, it tells you exactly what will hurt.

## Quick start

```sh
cargo install pg-blast-radius

pg-blast-radius analyse migration.sql

pg-blast-radius analyse migration.sql --dsn postgres://readonly@prod-replica:5432/mydb
```

The `--dsn` connection is read-only. It queries `pg_stat_user_tables` for sizes, `pg_stat_statements` for query workload, and `pg_stat_activity` for transaction baseline. No writes, no superuser.

### Offline stats

Export once, use in CI without database access:

```sh
pg-blast-radius collect-stats --dsn postgres://readonly@prod-replica/mydb > prod-stats.json

pg-blast-radius analyse migration.sql --stats-file prod-stats.json
```

## Analysis modes

| Capability | Static (no DB) | With catalog | With workload |
|---|---|---|---|
| Lock mode prediction | Yes | Yes | Yes |
| Table rewrite detection | Yes | Yes | Yes |
| Risk level | Conservative | Size-aware | Size-aware |
| Duration forecast | No | P50/P90/worst | P50/P90/worst + lock delay |
| Blocked query families | No | No | Yes |
| Queue depth estimates | No | No | Yes |
| Rollout recipes | Yes | Yes | Yes |
| Confidence | STATIC | ESTIMATED | MEASURED |

## Rules

| Operation | What it detects |
|---|---|
| `ADD COLUMN` | Default volatility, NOT NULL, PG version-dependent rewrite |
| `ALTER COLUMN TYPE` | Binary format change, rewrite detection |
| `SET NOT NULL` | Full scan risk, PG 12+ safe path |
| `ADD CONSTRAINT CHECK/FK/UNIQUE/PK` | NOT VALID detection, lock analysis |
| `VALIDATE CONSTRAINT` | Non-blocking lock confirmation |
| `CREATE INDEX` | CONCURRENTLY detection, SHARE lock warning |
| `DROP INDEX` | CONCURRENTLY detection |
| `DROP COLUMN` | Lock warning, application breakage risk |
| `DROP CONSTRAINT` | Lock warning, FK dual-table locking |
| `RENAME COLUMN/TABLE` | Lock warning, application breakage risk |
| `ATTACH PARTITION` | Scan risk, pre-validated CHECK optimisation |
| `TRUNCATE` | Destructive operation, ACCESS EXCLUSIVE warning |
| `VACUUM / VACUUM FULL` | Non-blocking vs ACCESS EXCLUSIVE rewrite |
| `ANALYZE` | Non-blocking statistics collection |
| `REINDEX` | CONCURRENTLY detection, lock warning |
| `REFRESH MATERIALIZED VIEW` | CONCURRENTLY detection, lock warning |

## How it compares

Most migration tools lint syntax or execute changes safely. pg-blast-radius does neither. It forecasts operational impact from your actual production workload.

| | pg-blast-radius | squawk | Eugene | pgfence | Atlas lint |
|---|---|---|---|---|---|
| Rules | 28 | 31 | 12+ | 15+ | 20+ |
| Lock mode detection | Full parser | Syntax rules | Parser + trace | Parser | Schema-aware |
| Workload-aware | Yes (`pg_stat_statements`) | No | No | Table size only | No |
| Duration forecast | P50/P90/worst | No | No | No | No |
| Blocked query families | Yes | No | No | No | No |
| Queue depth estimates | Yes | No | No | No | No |
| Safe rewrite recipes | Yes | Partial | No | Yes | Yes |
| Confidence ledger | Yes | No | No | No | No |

**Strengths**: Tells you which queries queue up, how many per minute, and for how long, on your actual workload. Explicit about what it knows vs what it assumes.

**Weaknesses**: Fewer lint rules than squawk. No trace/replay mode (yet). Requires `pg_stat_statements` for full workload analysis.

Use squawk in pre-commit for fast linting. Use pg-blast-radius in CI for operational risk assessment.

## CLI

```
pg-blast-radius analyse <files...>
pg-blast-radius collect-stats --dsn <dsn>
```

| Flag | Default | Purpose |
|---|---|---|
| `--pg-version` | 16 | PostgreSQL version to assume |
| `--format` | terminal | `terminal` or `json` |
| `--fail-level` | high | Exit non-zero if any finding meets this level |
| `--dsn` | none | Database connection (read-only) for catalog + workload |
| `--stats-file` | none | Pre-collected stats JSON (alternative to --dsn) |

### Exit codes

| Code | Meaning |
|---|---|
| 0 | All findings below `--fail-level` |
| 1 | At least one finding meets `--fail-level` |
| 2 | Parse error or invalid input |

## CI integration

### GitHub Actions

```yaml
- name: Check migration risk
  run: |
    pg-blast-radius analyse migrations/*.sql \
      --stats-file .pg-stats.json \
      --format json \
      --fail-level high
```

### JSON output

```sh
pg-blast-radius analyse migration.sql --format json --stats-file stats.json
```

Returns a JSON array with `findings`, `blast_radius` (including `blocked_queries`), `overall_risk`, and `overall_confidence`.

## Building from source

```sh
git clone https://github.com/michaelmillar/pg-blast-radius.git
cd pg-blast-radius
cargo build --release
```

Requires Rust stable and a C compiler (for libpg_query).

## Status

52 tests passing. Production-ready for static and catalog-aware analysis. Workload-aware forecasting is new in v0.2 and should be validated against your environment.

Not yet implemented: trace/replay mode, custom rules.

## Licence

MIT
