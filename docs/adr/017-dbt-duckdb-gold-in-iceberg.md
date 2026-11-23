# ADR-017: Gold in Iceberg via dbt-duckdb, a scoped storage key and a transactional materialization

- **Status:** Accepted (resolves the spike planned in ADR-004)
- **Date:** 2026-11-23

## Context

ADR-004 chose dbt on DuckDB for gold and left one question to a time-boxed spike: can dbt Core write gold to Iceberg through the REST catalog? Facts found on 2026-11-23 (DuckDB 1.5.6, dbt-core 1.12.7, dbt-duckdb 1.11.1, Lakekeeper 0.14.2, SeaweedFS):

- Reading through Lakekeeper with its default access delegation fails: Lakekeeper offers **remote signing** (STS is off), DuckDB does not support it, and storage answers **403**.
- With its own S3 key (`ACCESS_DELEGATION_MODE 'none'`) DuckDB reads silver (11.6 M rows in 14 s), applies silver's merge-on-read delete files correctly (0 duplicate keys), and can create, insert into and drop Iceberg tables.
- dbt's standard `table` materialization fails: it renames `<name>__dbt_tmp` into place and DuckDB-Iceberg refuses the rename. `CREATE OR REPLACE TABLE` is "not implemented" for DuckDB-Iceberg. `DELETE` followed by `INSERT` in one transaction works and commits one snapshot.

## Decision

- **dbt runs in the Airflow image** in its own virtual environment (`/opt/dbt`, so dbt's dependencies never touch Airflow's pinned set), inside the Docker network on the processing VM. `make dbt ARGS="build"`.
- **DuckDB holds a storage key**, a dedicated SeaweedFS identity `dbt` limited to `Read/Write/List` on the `warehouse` bucket (never the admin identity Lakekeeper uses). Keys come from `.env`. Every other client keeps remote signing.
- **Gold is written to Iceberg** (`lake.gold.*`) by a custom materialization `iceberg_table` (`transform/macros/iceberg_table.sql`): `CREATE TABLE AS` on the first build or `--full-refresh`; afterwards `DELETE` all rows and `INSERT … BY NAME` the new result inside dbt's transaction, committed once, so readers see the old or the new table, never an empty one.
- Staging models are **ephemeral** (no stored copy of silver). The operator reference is a **dbt seed** derived from the Open Rail Data wiki (CC BY-SA), stored in dbt's local DuckDB file.

## Consequences

- One catalog and one table format for bronze, silver and gold; Spark reads gold written by DuckDB (verified).
- The "no client holds storage keys" rule now has one scoped exception, recorded here. A compromised dbt container could read or write any object in the warehouse bucket (not other buckets, not admin).
- A changed column list needs `dbt build --full-refresh` for that model (`INSERT … BY NAME` fails otherwise); the brief drop-and-create is acceptable for a derived table.
- Each build adds one snapshot per gold table; gold is covered by the 12-hour retention class when added to Iceberg maintenance.
- Found while building: without `adapter.commit()` the materialization reported success while DuckDB rolled the table back (findings F29).
- Full rebuilds take ~55 s on the 2-OCPU VM (delay hotspots ~50 s over all of silver); incremental models are the next step if the 15-minute schedule (Day 10) makes that too heavy.
