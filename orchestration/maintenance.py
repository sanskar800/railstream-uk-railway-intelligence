"""Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup, with measurements.

Run nightly by the `iceberg_maintenance` DAG through the Spark Connect server (ADR-015). The SQL
is built by pure functions (unit-tested without Spark); `maintain_table` executes it on a session.

Safety rules, each from a real constraint:
- Compact only partitions that writers have finished with: bronze and revisions before today,
  current calls older than 2 days (silver's MERGE corrections touch recent days).
- Keep bronze snapshots for 3 days (and at least the last 10). Silver streams from bronze and its
  checkpoint records the bronze snapshot it has reached; expiring that snapshot would break it.
  Silver keeps 12 hours: nothing streams from it, and its old versions filled the disk (F28).
- Orphans older than 3 days only (Iceberg refuses under 24 h: an in-flight write looks orphaned),
  listed with prefix_listing, which goes through Iceberg's remote-signed S3 client; the default
  Hadoop listing needs an S3 connector the image deliberately omits (ADR-007).
- Metadata cleanup on: every commit writes a metadata file, and expiry does not remove them.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

CATALOG = "lake"
RUNS_TABLE = "ops.maintenance_runs"
ORPHAN_AGE = timedelta(days=3)
RETAIN_LAST = 10
TARGET_FILE_BYTES = 128 * 1024 * 1024
METADATA_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "100",
}


@dataclass(frozen=True)
class TablePolicy:
    table: str
    partition_column: str | None  # day partitions derive from it; None = compact it whole
    compact_before_days: int | None  # None = no compaction (tiny tables)
    # How long old snapshots (and the files only they reference) are kept. Only bronze needs
    # days: silver streams from it and its checkpoint records the bronze snapshot it reached.
    # Nothing streams from silver, so its old versions are only storage; keeping three days
    # of them filled the 145 GB disk on 2026-11-22 (findings F28).
    snapshot_retention: timedelta = timedelta(hours=12)


POLICIES = [
    TablePolicy("bronze.trust_movement", "kafka_ts", 0, timedelta(days=3)),
    TablePolicy("silver.movement_revisions", "actual_ts", 0),
    TablePolicy("silver.trust_movement", "actual_ts", 2),
    TablePolicy("silver.rejected_messages", None, None),
    TablePolicy("ref.locations", None, None),
    # Every dbt build replaces the last 3 days' rows (ADR-018): small new files and delete
    # files 96 times a day. The tables are small, so each is compacted whole every night.
    TablePolicy("gold.punctuality_hourly", None, 0),
    TablePolicy("gold.delay_hotspots", None, 0),
    TablePolicy("gold.reactionary_delay", None, 0),
]


def literal(ts: datetime) -> str:
    """A Spark TIMESTAMP literal (procedure arguments must be values, not expressions)."""
    return f"TIMESTAMP '{ts.astimezone(UTC):%Y-%m-%d %H:%M:%S}'"


def compaction_sql(policy: TablePolicy, now: datetime) -> str | None:
    if policy.compact_before_days is None:
        return None
    midnight = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = midnight - timedelta(days=policy.compact_before_days)
    column = policy.partition_column
    where = f"{column} < '{cutoff:%Y-%m-%d %H:%M:%S}'" if column else None
    options = {
        "min-input-files": "2",
        "target-file-size-bytes": str(TARGET_FILE_BYTES),
        "partial-progress.enabled": "true",  # commit groups as they finish: less conflict risk
        "partial-progress.max-commits": "10",
        # Merge-on-read tables accumulate delete files; rewrite any data file that has one,
        # so reads stop applying deletes at query time (findings F28).
        "delete-file-threshold": "1",
    }
    opts = ", ".join(f"'{k}', '{v}'" for k, v in options.items())
    return (
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{policy.table}', "
        + (f'where => "{where}", ' if where else "")
        + f"options => map({opts}))"
    )


def expire_sql(policy: TablePolicy, now: datetime) -> str:
    return (
        f"CALL {CATALOG}.system.expire_snapshots(table => '{policy.table}', "
        f"older_than => {literal(now - policy.snapshot_retention)}, retain_last => {RETAIN_LAST})"
    )


def orphan_sql(policy: TablePolicy, now: datetime) -> str:
    return (
        f"CALL {CATALOG}.system.remove_orphan_files(table => '{policy.table}', "
        f"older_than => {literal(now - ORPHAN_AGE)}, prefix_listing => true)"
    )


def properties_sql(table: str) -> str:
    props = ", ".join(f"'{k}' = '{v}'" for k, v in METADATA_PROPERTIES.items())
    return f"ALTER TABLE {table} SET TBLPROPERTIES ({props})"


def table_stats(spark, table: str) -> dict:
    files, size = spark.sql(
        f"SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM {table}.files"
    ).first()
    snapshots = spark.sql(f"SELECT count(*) FROM {table}.snapshots").first()[0]
    return {"data_files": int(files), "data_bytes": int(size), "snapshots": int(snapshots)}


def maintain_table(spark, policy: TablePolicy, now: datetime | None = None) -> dict:
    """Run every step for one table; return the measurements and results (one record)."""
    now = now or datetime.now(UTC)
    started = time.monotonic()
    before = table_stats(spark, policy.table)
    spark.sql(properties_sql(policy.table))
    record = {
        "table_name": policy.table,
        "started_at": now,
        **{f"{k}_before": v for k, v in before.items()},
        "rewritten_files": 0,
        "added_files": 0,
    }
    if (sql := compaction_sql(policy, now)) is not None:
        row = spark.sql(sql).first()
        record["rewritten_files"] = int(row["rewritten_data_files_count"])
        record["added_files"] = int(row["added_data_files_count"])
    row = spark.sql(expire_sql(policy, now)).first()
    record["expired_data_files"] = int(row["deleted_data_files_count"])
    record["orphans_removed"] = len(spark.sql(orphan_sql(policy, now)).collect())
    record.update({f"{k}_after": v for k, v in table_stats(spark, policy.table).items()})
    record["duration_s"] = round(time.monotonic() - started, 1)
    return record


def record_run(spark, record: dict, runs_table: str = RUNS_TABLE) -> None:
    """Append one measurement record to the runs table (created on first use)."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {runs_table.split('.')[0]}")
    frame = spark.createDataFrame([record])
    if spark.catalog.tableExists(runs_table):
        frame.writeTo(runs_table).append()
    else:
        frame.writeTo(runs_table).using("iceberg").create()


def connect():
    """A Spark session on the Spark Connect server (pyspark-client, no JVM here)."""
    from pyspark.sql import SparkSession

    url = os.environ.get("SPARK_CONNECT_URL", "sc://spark-connect:15002")
    spark = SparkSession.builder.remote(url).getOrCreate()
    # No broadcast joins: on 2026-11-20 Spark misjudged a maintenance join as small, tried to
    # broadcast it from the 1 GB driver and failed ("Not enough memory to build and broadcast"),
    # skipping every table that night (findings F34). Sort-merge joins are bounded in memory.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    return spark


def policy_for(table: str) -> TablePolicy:
    return next(p for p in POLICIES if p.table == table)
