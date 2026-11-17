# ADR-015: Airflow drives Spark batch jobs through a Spark Connect server

- **Status:** Accepted
- **Date:** 2026-11-17

## Context

Iceberg maintenance (compaction, snapshot expiry, orphan removal) runs as Iceberg's Spark procedures, scheduled by Airflow (Day 8). Airflow runs in its own container without Java. The streaming jobs are long-running Spark applications in their own containers and must not be burdened with batch work.

## Options considered

1. **Docker socket in Airflow** (DockerOperator or `docker compose run`): common in tutorials, but the socket gives Airflow and any DAG code root-equivalent control of the VM.
2. **Spark Connect server** (Spark 4 client/server mode): one long-running Spark service; Airflow sends SQL over gRPC with the pure-Python `pyspark-client` (chosen).
3. **PyIceberg from Airflow:** no Spark, but no compaction support, and parallel remote-signed I/O raced (findings F22).

## Decision

Service `spark-connect` (profile `airflow`, image `railstream-spark:dev`, `python -m streaming.connect_server`): Spark 4.1.4 Connect on port 15002, inside the Docker network only, 1 GB heap in a 1.5 GB container, sharing the streaming jobs' Spark settings (`streaming.session.spark_conf`). The Airflow image installs `pyspark-client==4.1.4`, matching the server; Airflow's constraint file pins 4.3.0 for its own Spark provider, so only that line is dropped and every other package keeps Airflow's pinned version.

## Consequences

- Airflow connects in ~3.6 s (a fresh Spark start takes ~20 s), holds no Java and no Docker access. Verified: compaction, snapshot expiry (files deleted in storage, not only reported) and orphan listing all work through it.
- Procedure arguments must be literal values: `current_timestamp() - INTERVAL …` inside `CALL` failed to bind. `remove_orphan_files` needs `prefix_listing => true` (the default Hadoop listing has no `s3` FileSystem, ADR-007) and refuses windows under 24 hours.
- A shared Spark is a shared resource: heavy maintenance competes with the streams for the VM's two cores, so maintenance runs nightly and one table at a time.
- The same server can serve later batch jobs if needed; dbt stays on DuckDB (ADR-004).
