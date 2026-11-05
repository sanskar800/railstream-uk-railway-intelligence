# ADR-002: Apache Iceberg as the table format

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

Bronze, silver and gold live as files on object storage (MinIO locally, S3 in the cloud). Writers are a streaming Spark job (small commits every few seconds), Airflow maintenance jobs and dbt; readers are dbt, DuckDB and the API. We need atomic commits with concurrent readers, schema evolution, time travel ("which data did yesterday's gold see?") and compaction of small files. The spec requires Iceberg; this records why.

## Options considered

1. **Plain Parquet folders.** Simple, but no atomic commits (readers can see half-written data), no schema evolution, no time travel; small files pile up with no table-level way to compact them.
2. **Delta Lake.** Mature, with similar features; strongest inside the Spark/Databricks ecosystem; engine support outside it is narrower.
3. **Apache Iceberg.** Open specification with broad engine support (Spark, DuckDB, Trino, Flink, Snowflake, BigQuery); snapshots, hidden partitioning, schema evolution; the format UK data platforms are converging on.

## Decision

Use **Iceberg** for all lakehouse tables (bronze, silver, reference, and gold where ADR-004 allows), registered in one shared catalog (ADR-003).

## Consequences

- Every Spark micro-batch is an atomic snapshot; readers never see partial batches.
- Small files and snapshot growth become our job: Airflow runs compaction and snapshot expiry (Day 8), and file counts are measured before and after.
- All engines must go through the catalog; none may write table files directly.
