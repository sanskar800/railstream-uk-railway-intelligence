# ADR-007: Spark runtime, checkpoint location and bronze partitioning

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

The streaming jobs (bronze now, silver on Day 6) run Spark Structured Streaming on the laptop inside Docker, within the ~7.6 GiB Docker memory budget, writing Iceberg through Lakekeeper with remote signing (Spark holds no storage keys). Facts checked on 2026-11-09: Iceberg 1.12.0 is the newest release and its newest Spark runtime is for **Spark 4.1** (none for 4.2/4.3); the official `apache/spark` images ship **Python 3.10**, while the shared code in `trust/` needs 3.12; checkpointing to S3 needs `hadoop-aws` plus the AWS SDK v2 bundle (**612 MB**) and S3 keys in Spark.

## Decisions

1. **Spark 4.1.4 + Iceberg 1.12.0 in our own image** (`infra/spark/Dockerfile`): `python:3.12-slim` + OpenJDK 17 + `pyspark` from `uv.lock` (group `spark`) + six connector jars pinned and SHA-1-checked at build time. Same Python and same lock file as the rest of the project; nothing downloaded at run time. `local[2]`, 1 GB driver heap, 1.5 GB container limit, in the `stream` Compose profile.
2. **Checkpoints on a named Docker volume** (`spark-checkpoints`), not on SeaweedFS. With one container the volume is as durable as the driver itself; S3 checkpoints would add 612 MB of jars and put raw S3 credentials into Spark, undoing the remote-signing model. In a cloud deployment with several drivers or ephemeral hosts, `checkpointLocation` becomes an `s3a://` path (configuration plus the jars).
3. **Bronze partitioned by ingestion day** (`days(kafka_ts)`), not event day. Bronze must accept every record without parsing; event time needs the payload parsed and the BST rule applied, and a malformed record has no event time at all. Silver, which parses, is partitioned by event day. Replayed old data lands in the day it was replayed, which is what "ingested" means.
4. **One file per micro-batch** (`coalesce(1)`; ~700 rows/min needs no parallelism) and automatic deletion of old metadata files (`write.metadata.delete-after-commit.enabled`), because a 1-minute trigger makes ~1,440 commits a day. Data-file compaction is scheduled in Airflow (Day 7).

## Consequences

- Measured on first run (2026-11-09): the 83,533-record backlog in one 6.2 s batch; then ~735 rows per 1.6 s batch each minute; 705 MiB container memory; 84,737 rows in 4 Parquet files, 3.7 MB. Image 1.8 GB.
- Exactly once is checkable: (kafka_partition, kafka_offset) is unique per row, and per partition the offsets are contiguous from 0 (findings F16).
- `DROP TABLE ... PURGE` from Spark errors after the drop has succeeded (Lakekeeper deletes the files itself and refuses to sign for a dropped table): use plain `DROP TABLE`.
- Wrong if: the laptop's memory cannot hold Spark alongside the stack once silver's state store grows (re-measured on Day 6), or 1-minute batches cannot keep end-to-end freshness under 5 minutes p95 (Day 13).
