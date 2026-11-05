# ADR-004: dbt on DuckDB, with gold written to Iceberg if proven

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

dbt builds the gold models (punctuality, hotspots, reactionary delay) from silver Iceberg tables every few minutes, to meet a 5-minute p95 freshness target (brief). The spec puts gold in the lakehouse and names DuckDB or Trino as the query layer. The deciding constraint is memory: about 7.6 GiB for all of Docker, with Spark streaming and Redpanda already running. From current documentation: DuckDB 1.4+ writes Iceberg through a REST catalog, but the standard **dbt-duckdb adapter for dbt Core 1.x has no built-in Iceberg catalog support**; that is in dbt's newer engine, still in beta ([dbt docs](https://docs.getdbt.com/docs/build/iceberg/adapters/duckdb-iceberg-support)).

## Options considered

1. **dbt-spark** (Spark Thrift server). Writes Iceberg natively. But it is a second always-on Spark JVM (gigabytes) with slow start-up for small models: a poor fit for the memory budget.
2. **dbt-trino.** Mature Iceberg support. But Trino is another JVM service needing gigabytes of heap, for a dataset of millions, not billions, of rows.
3. **dbt-duckdb.** In-process (no service; memory used only while dbt runs), fast on millions of rows, reads silver through the REST catalog (ADR-003). Writing gold back to Iceberg needs the catalog attached in the dbt profile and models materialised into it: plausible given DuckDB's Iceberg writes, not yet proven with dbt Core.

## Decision

Use **dbt-duckdb**. Gold is written **to Iceberg through the REST catalog if a time-boxed spike (≤ 1 h) on Day 9 proves it works** with dbt Core for table and incremental models. **Fallback:** gold materialised as DuckDB tables or Parquet on MinIO and read by the API, recorded as a deviation in architecture.md §6 ("gold outside Iceberg").

## Consequences

- No extra always-on service for transformation; the query layer (FastAPI over DuckDB) uses the same engine.
- If gold ends up outside Iceberg, it is a derived layer rebuilt from silver in minutes, so losing Iceberg features there (time travel) is acceptable.
- Wrong if: dbt on DuckDB cannot keep gold within the freshness target, or DuckDB's memory during dbt runs starves Spark (measured on Days 9 and 13).
