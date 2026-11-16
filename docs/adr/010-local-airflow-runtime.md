# ADR-010: Airflow standalone with LocalExecutor for local development

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

Day 7 adds scheduled reference-data workflows. Kafka and Spark already run the
continuous stream. Docker has about 7.6 GiB available, so the official example's
Celery workers and Redis would add unnecessary services. Existing Postgres 18
can hold an isolated Airflow metadata database without another database server.

## Decision

Use the official `apache/airflow:3.4.0-python3.12` image as the base in an optional `airflow`
Compose profile. `airflow standalone` supervises the API/UI, scheduler,
DAG processor and triggerer. LocalExecutor runs at most one task at a time;
one API worker and one parser limit local overhead. No host Airflow installation.

Create the `airflow` database and owner idempotently on new or existing Postgres
volumes. Preserve Lakekeeper and all existing data. Persist Airflow configuration,
logs and local auth state in a named volume; mount DAG code read-only. Bind the
UI only to `127.0.0.1:8086`, with a local SimpleAuthManager login. The database URI
is generated with URL-encoded credentials via Airflow's `_CMD` configuration.

## Consequences

- The same startup command works after a Docker restart; it does not reset the
  metadata database or clear past runs. Existing role credentials are preserved;
  changing a password variable is not an automatic database password rotation.
- A component-heartbeat health check covers the database, scheduler, parser and
  triggerer. A manual platform DAG verifies actual task execution and log storage.
- Airflow's 1.5 GiB limit plus the current core/tools/stream caps total 7.5 GiB.
  This leaves little Docker-wide margin. Keep unused profiles stopped and measure
  future tasks before recommending that every profile run together. Startup and
  a single trivial task are not proof that a CORPUS load fits.
- Standalone and SimpleAuthManager are development choices. Cloud deployment
  needs a separate authentication, process-supervision and resource review.
  The cloud capture VM is unchanged.
- Task dependencies are isolated in an image based on the official runtime (ADR-011).
  Windows `uv.lock` does
  not gain Airflow or its provider dependency tree.

Sources: [Airflow installation](https://airflow.apache.org/docs/apache-airflow/stable/installation.html),
[3.4.0 standalone implementation](https://github.com/apache/airflow/blob/3.4.0/airflow-core/src/airflow/cli/commands/standalone_command.py),
[supported database backends](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html).
