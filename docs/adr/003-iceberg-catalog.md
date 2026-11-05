# ADR-003: An Iceberg REST catalog (Lakekeeper)

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

An Iceberg catalog records where each table's current metadata is; every engine reading or writing the tables must use the same one. Our engines: Spark (streaming writes), DuckDB (queries, and possibly dbt gold writes; ADR-004) and PyIceberg (scripts, maintenance). The spec does not name a catalog. Constraints: about 7.6 GiB for all of Docker; no unnecessary services. As of DuckDB 1.4+, **DuckDB writes Iceberg only through an attached REST catalog**; it cannot write to a plain folder ([DuckDB Iceberg docs](https://duckdb.org/docs/current/core_extensions/iceberg/overview)).

## Options considered

1. **Hadoop (file-system) catalog on MinIO.** No service. But commits rely on atomic renames that object storage does not provide, so it is unsafe with concurrent writers; DuckDB cannot write through it.
2. **JDBC catalog on the shared Postgres.** No extra service; Spark and PyIceberg support it. But DuckDB cannot attach it, so DuckDB could only read by locating metadata files by hand, and could not write gold.
3. **REST catalog: Apache Polaris or Project Nessie.** Full-featured; JVM services with a noticeable memory cost.
4. **REST catalog: the Apache Iceberg test fixture image.** Light, but explicitly a test fixture.
5. **REST catalog: Lakekeeper.** Rust single binary (small footprint), Apache-2.0, stores its state in Postgres (which we already run for Airflow), documented by DuckDB ([catalog docs](https://duckdb.org/docs/current/core_extensions/iceberg/catalogs)); works with Spark and PyIceberg.

## Decision

Use the **Iceberg REST protocol with Lakekeeper**, backed by its own database in the shared Postgres. All engines connect to it. Its memory is measured with the core stack (task 1.4). Because every engine speaks the REST protocol, Lakekeeper can be swapped for another REST implementation without changing engine code.

## Consequences

- One small extra service, but every engine (Spark, DuckDB, PyIceberg, later Trino or a cloud engine) uses one catalog in the same way.
- Enables DuckDB to write Iceberg (ADR-004).
- The catalog becomes a dependency of every read and write: it is health-checked, and its Postgres database is part of what must be backed up.
- Wrong if: Lakekeeper's memory or setup cost is disproportionate (measured on Day 1), or Spark/DuckDB interoperability through it fails (tested on Day 5).
