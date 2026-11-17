# ADR-011: Versioned CORPUS refresh with a native Python Iceberg writer

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

Airflow must refresh the small CORPUS location lookup. Running another JVM for
each reference task adds memory pressure to the laptop; giving Airflow access
to the Docker daemon just to launch the existing Spark loader adds coupling.
Spark silver already reads the versioned `ref.locations` table (ADR-009).

## Decision

Use PyIceberg 0.12.0 with PyArrow inside a custom image based on the pinned
official Airflow 3.4.0 image. Install using Airflow's matching Python 3.12
constraints and run `pip check`. Keep these dependencies out of the Windows
environment and reuse `trust.locations` for content hashing and deterministic
STANOX selection. No extra database, Spark server or Docker socket access.

`refresh_corpus` has three tasks: download, validate and load. Schedule it at
04:00 UTC (daily at first; Monday and Thursday since 2026-11-17, as CORPUS changes every few weeks) with `catchup=False`, at most one active run, two retries and
one-minute retry delays. The local scheduler must be running for execution;
this does not move orchestration to the cloud or backfill train movements.

Download over HTTPS using the private Network Rail credentials. Strip Basic
Authorization on cross-origin redirects to signed storage URLs. Do not put
credentials, redirect URLs or full payloads in logs/XCom. Save run-specific
downloads atomically in the persistent Airflow volume; pass only a file path,
content hash and row count between tasks. Revalidate at the write boundary.

Use the same Lakekeeper REST catalog and remotely signed S3 access as Spark.
Append all selected rows of a new content version in one Iceberg commit. For
an existing version, compare the stored rows and skip the write if identical;
fail on a mismatch. Preserve its original `loaded_at`. A retry after a committed
append therefore makes no extra rows or snapshot.

## Consequences

- There is exactly one reference writer at a time. DAG run concurrency enforces
  this within Airflow; do not run the manual Spark loader concurrently. This is
  not a globally enforced primary-key constraint or a multi-writer CAS protocol.
- Previous versions remain immutable. Silver selects the newest loaded version
  on its next batch and refreshes affected current rows on new input or replay;
  a reference refresh does not rewrite all existing movement rows.
- Validate-to-load content changes fail before any write. Invalid downloads
  cannot replace an existing canonical reference version.
- Historical download files remain in `airflow-home`; cleanup/retention is later
  maintenance work. There is no access to files captured before collection began.
- The real refresh loaded 11,133 locations, and repeated runs inserted zero rows.
  A separate Spark reader verified both old/new versions and distinct version
  keys. Development evidence is recorded under `results/dev/`.

Sources: [PyIceberg API](https://py.iceberg.apache.org/api/),
[PyIceberg REST/S3 configuration](https://py.iceberg.apache.org/configuration/),
[Airflow image dependency guidance](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/).
