<p align="center">
  <img src="logo.svg" width="160" alt="pg-blast-radius logo" />
</p>

<h1 align="center">pg-blast-radius</h1>

<p align="center">
  Know what your PostgreSQL migration will do to your actual database.
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> &middot;
  <a href="#what-it-does">What it does</a> &middot;
  <a href="#how-it-differs-from-squawk">vs squawk</a> &middot;
  <a href="#ci-integration">CI</a>
</p>

---

pg-blast-radius connects to your Postgres instance and reports:

- **Blast radius** per table, showing which locks are held, for how long, blocking what
- **Duration estimates** based on real table sizes from your database
- **Rollout recipes** with step-by-step safer SQL when a migration should be split
- **Confidence levels** stating what it knows, what it cannot know, and what you should check

It is not a linter. Use [squawk](https://squawkhq.com) for that.
It is not a migration runner. It is a risk estimator.

## Quick start

```sh
cargo install pg-blast-radius

# Static analysis (useful, but limited)
pg-blast-radius analyse migration.sql

# Catalog-informed analysis (the real value)
pg-blast-radius analyse migration.sql --dsn postgres://readonly@staging:5432/mydb
```

## Example output

Same SQL, two databases, two verdicts:

**16 kB table (50 rows)**

```
 MEDIUM  CREATE INDEX "idx_sessions_user" on "sessions" without CONCURRENTLY
   Takes SHARE lock. Table is 16.0 kB. Estimated: 1s..1s
   CONCURRENTLY not required for tables this small.
```

**47 GB table (800M rows)**

```
 EXTREME  CREATE INDEX "idx_orders_customer" on "orders" without CONCURRENTLY
   Takes SHARE lock. Table is 47.0 GB. Blocks writes for estimated 10m..27m.

   Rollout recipe: Non-blocking index build on "orders"
     1. [expand] Create index concurrently
        CREATE INDEX CONCURRENTLY "idx_orders_customer" ON "orders" (customer_id);
```

squawk flags both identically. pg-blast-radius tells you one is harmless and the other will take your site down.

## Multi-statement blast radius

```
$ pg-blast-radius analyse migration.sql --dsn postgres://staging/mydb

  Blast Radius
    users (2.3 GB, ~15M rows) -> ACCESS EXCLUSIVE (3 statements combined)
      Blocks: all reads and writes
      Estimated duration: 42s..2m
      3 statements touch "users". Consider splitting into separate migrations.

   LOW  ADD COLUMN "email" on "users" (no default)
     Metadata-only change. Lock held for milliseconds.

   HIGH  SET NOT NULL on "users"."email" requires full table scan
     ACCESS EXCLUSIVE lock held during scan. Estimated: 12s..24s

     Rollout recipe: Safe SET NOT NULL for "users"."email"
       1. [expand]    ADD CONSTRAINT ... CHECK ("email" IS NOT NULL) NOT VALID;
       2. [validate]  VALIDATE CONSTRAINT ...;   -- non-blocking
       3. [switch]    ALTER COLUMN "email" SET NOT NULL;  -- instant (PG 12+)
       4. [contract]  DROP CONSTRAINT ...;

   HIGH  CREATE INDEX "idx_users_email" on "users" without CONCURRENTLY
     SHARE lock. Blocks writes. Estimated: 30s..1m

     Rollout recipe: Non-blocking index
       1. CREATE INDEX CONCURRENTLY idx_users_email ON users (email);

  Overall: HIGH RISK | Confidence: definite
  3 statements, 2 safer alternatives suggested.
```

## What it does

| Feature | Static mode | With `--dsn` / `--stats-file` |
|---------|-------------|-------------------------------|
| Lock mode prediction | Yes | Yes |
| Table rewrite detection | Yes | Yes |
| Risk level (low/medium/high/extreme) | Yes (conservative) | Yes (table-size-aware) |
| Duration estimates | No | Yes (ranges with caveats) |
| Rollout recipes | Yes | Yes |
| Per-table blast radius aggregation | Yes | Yes |
| Confidence scoring | "needs-catalog" | "definite" |

### Rules

| Operation | What it detects |
|-----------|----------------|
| `ADD COLUMN` | Default volatility, NOT NULL, PG version-dependent rewrite |
| `ALTER COLUMN TYPE` | Binary format change -> rewrite detection |
| `SET NOT NULL` | Full scan risk, PG 12+ safe path |
| `ADD CONSTRAINT CHECK/FK/UNIQUE` | NOT VALID detection, lock analysis |
| `VALIDATE CONSTRAINT` | Non-blocking lock confirmation |
| `CREATE INDEX` | CONCURRENTLY detection, SHARE lock warning |
| `DROP INDEX` | CONCURRENTLY detection |
| `DROP COLUMN` | Lock warning, application breakage risk |
| `RENAME COLUMN/TABLE` | Lock warning, application breakage risk |
| `ATTACH PARTITION` | Scan risk, pre-validated CHECK optimisation |

## How it differs from squawk

squawk is a migration linter. It pattern-matches SQL and tells you which anti-patterns you used. It treats every table the same.

pg-blast-radius is a risk estimator. It optionally connects to your database and says "this ALTER TYPE on a 47 GB table will block all queries for an estimated 8..16 minutes." Then it generates the multi-step rollout SQL.

| | squawk | pg-blast-radius |
|---|--------|----------------|
| Static lint rules | 31 | ~15 |
| Catalog-aware risk | No | Yes |
| Environment-sensitive verdicts | No | Yes |
| Duration estimates | No | Yes |
| Rollout recipe generation | No | Yes |
| Per-table blast radius | No | Yes |
| Confidence scoring | No | Yes |

Use both: squawk in pre-commit for fast linting, pg-blast-radius in CI for operational risk assessment.

## CLI

```
pg-blast-radius analyse <files...>           # analyse migration files
pg-blast-radius collect-stats --dsn <dsn>    # export catalog stats for offline CI
```

### Flags

| Flag | Default | Purpose |
|------|---------|---------|
| `--pg-version` | 16 | PostgreSQL version to assume |
| `--format` | terminal | `terminal` or `json` |
| `--fail-level` | high | Exit non-zero if any finding meets this level |
| `--dsn` | none | Database connection for table sizes |
| `--stats-file` | none | Pre-collected stats JSON (alternative to --dsn) |

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | All findings below `--fail-level` |
| 1 | At least one finding meets `--fail-level` |
| 2 | Parse error or invalid input |

## CI integration

### Offline stats workflow

If your CI cannot connect to a database directly, export stats once and check them in:

```sh
pg-blast-radius collect-stats --dsn postgres://staging/mydb > .pg-stats.json
pg-blast-radius analyse migrations/*.sql --stats-file .pg-stats.json --format json
```

### JSON output

```sh
pg-blast-radius analyse migration.sql --format json --stats-file stats.json
```

Returns a JSON array of analysis results, each with `findings`, `blast_radius`, `overall_risk`, and `overall_confidence`.

## Building from source

```sh
git clone https://github.com/michaelmillar/pg-blast-radius.git
cd pg-blast-radius
cargo build --release
```

Requires Rust 1.85+ and a C compiler (for libpg_query).

## Licence

MIT
