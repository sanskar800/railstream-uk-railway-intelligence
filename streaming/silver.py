"""Bronze -> typed, revision-aware silver movements and durable rejects.

Each micro-batch is idempotently merged before its checkpoint is committed.
Kafka DLQ publication is at least once, keyed by the durable reject identity.
Recent live duplicates use bounded watermark state; late reports bypass that
state and use durable revision identity. A versioned CORPUS left join adds names.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import signal
import time

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from streaming.bounds import BatchBounds, actual_predicate, clustered_ranges, match_predicate
from streaming.bronze import ProgressLogger
from streaming.schemas import CLASSIFIED_SCHEMA, MOVEMENT_SCHEMA
from streaming.session import build_session
from streaming.watermarks import prepare_stream
from trust.silver import classify

log = logging.getLogger("silver")


def identifier(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)?", value):
        raise ValueError(f"invalid table/namespace identifier: {value!r}")
    return value


# Every commit writes a metadata file and snapshot expiry does not remove them, so keep only the
# last 100 (as bronze does). Without this, silver accumulated ~1,440 metadata files a day (Day 8).
# Merge-on-read for the current-call table: a MERGE that corrects a few rows writes small
# delete files instead of rewriting every data file it touches. Copy-on-write rewrote today's
# partition file every minute (~40 GB/day), and three days of those versions filled the disk
# (findings F28). Nightly compaction folds the delete files back in.
MERGE_ON_READ = (
    "'write.merge.mode'='merge-on-read', 'write.update.mode'='merge-on-read', "
    "'write.delete.mode'='merge-on-read'"
)
TABLE_PROPERTIES = (
    "'format-version'='2', 'write.metadata.delete-after-commit.enabled'='true', "
    "'write.metadata.previous-versions-max'='100'"
)


def create_tables(spark, namespace):
    identifier(namespace)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    columns = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in MOVEMENT_SCHEMA)
    for name in ("trust_movement", "movement_revisions"):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {namespace}.{name} ({columns}) USING iceberg "
            f"PARTITIONED BY (days(actual_ts)) TBLPROPERTIES ({TABLE_PROPERTIES})"
        )
        existing = set(spark.table(f"{namespace}.{name}").columns)
        for field in MOVEMENT_SCHEMA:
            if field.name not in existing:
                spark.sql(
                    f"ALTER TABLE {namespace}.{name} ADD COLUMN "
                    f"{field.name} {field.dataType.simpleString()}"
                )
    # Existing tables get the setting too (CREATE ... IF NOT EXISTS leaves their properties alone).
    spark.sql(f"ALTER TABLE {namespace}.trust_movement SET TBLPROPERTIES ({MERGE_ON_READ})")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {namespace}.rejected_messages "
        f"(record_id STRING, value STRING) USING iceberg TBLPROPERTIES ({TABLE_PROPERTIES})"
    )


def batch_bounds(movements) -> BatchBounds:
    """Min/max and clustered ranges of the batch's times (distinct values: at most a few
    thousand minutes even in a 200k-row catch-up batch)."""
    pairs = movements.select("actual_ts", "planned_ts").distinct().collect()
    actual = [r["actual_ts"] for r in pairs]
    planned = [r["planned_ts"] for r in pairs if r["planned_ts"] is not None]
    unresolved = [r["actual_ts"] for r in pairs if r["planned_ts"] is None]
    return BatchBounds(
        min(actual), max(actual), min(planned, default=None), max(planned, default=None),
        actual_ranges=clustered_ranges(actual),
        planned_ranges=clustered_ranges(planned),
        unresolved_ranges=clustered_ranges(unresolved),
    )  # fmt: skip


def merge_new(spark, frame, table, key, prune: str | None = None):
    """Insert rows whose key is not in the table yet; `prune` bounds the target scan."""
    spark.catalog.refreshTable(table)
    frame.dropDuplicates([key]).createOrReplaceTempView("silver_insert")
    condition = f"t.{key}=s.{key}" + (f" AND t.{prune}" if prune else "")
    spark.sql(
        f"MERGE INTO {table} t USING silver_insert s ON {condition} WHEN NOT MATCHED THEN INSERT *"
    )


def merge_current(spark, movements, namespace, locations=None, bounds: BatchBounds | None = None):
    # Rank with the existing current row too, so old replay never overwrites a newer revision.
    table = f"{namespace}.trust_movement"
    spark.catalog.refreshTable(table)
    bounds = bounds or batch_bounds(movements)
    keys = movements.select("event_key").distinct()
    existing = (
        spark.table(table).where(match_predicate(bounds)).join(keys, "event_key", "left_semi")
    )
    candidates = movements.unionByName(existing)
    newest = Window.partitionBy("event_key").orderBy(F.col("msg_queue_ts").desc())
    latest = candidates.withColumn("queue_rank", F.dense_rank().over(newest)).where("queue_rank=1")
    conflicts = latest.groupBy("event_key").agg(
        (F.countDistinct("revision_id") > 1).alias("tie"),
        F.max(F.col("revision_conflict").cast("int")).alias("prior_conflict"),
    )
    chosen = (
        latest.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("event_key").orderBy(F.col("revision_id").desc())
            ),
        )
        .where("rn=1")
        .drop("rn", "queue_rank", "revision_conflict")
        .join(conflicts, "event_key")
        .withColumn("revision_conflict", F.col("tie") | (F.col("prior_conflict") == 1))
        .drop("tie", "prior_conflict")
    )
    # Materialize before MERGE: chosen reads the target table. The single silver writer owns it.
    if locations is not None:
        from streaming.locations import enrich

        chosen = enrich(chosen, locations)
    chosen = chosen.localCheckpoint(eager=True, storageLevel=StorageLevel.DISK_ONLY)
    try:
        chosen.createOrReplaceTempView("silver_current")
        spark.sql(
            f"MERGE INTO {table} t USING silver_current s ON t.event_key=s.event_key "
            f"AND {match_predicate(bounds, 't.')} "
            "WHEN MATCHED AND (s.revision_id <> t.revision_id OR "
            "s.revision_conflict <> t.revision_conflict OR "
            "NOT(s.location_reference_version <=> t.location_reference_version)) "
            "THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        chosen.unpersist()


def publish_rejects(durable, topic):
    (
        durable.selectExpr("record_id AS key", "value")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
        .option("topic", topic)
        .option("kafka.acks", "all")
        .save()
    )


def process_batch(batch, batch_id, *, namespace="silver", dlq_topic="dlq.trust.movement"):
    """Fail the batch if either Iceberg or Kafka fails; retry-safe Iceberg merges come first."""
    if batch.isEmpty():
        return
    spark = batch.sparkSession
    classified = spark.createDataFrame(
        batch.rdd.map(lambda row: classify(row.asDict(recursive=True))),
        CLASSIFIED_SCHEMA,
    )
    process_classified_batch(classified, batch_id, namespace=namespace, dlq_topic=dlq_topic)


def process_classified_batch(
    classified,
    batch_id,
    *,
    namespace="silver",
    dlq_topic="dlq.trust.movement",
    reference_table=None,
):
    spark = classified.sparkSession
    classified = classified.persist(StorageLevel.DISK_ONLY)
    # Seconds per phase, logged with each batch: where a slow batch spends its time (F35).
    timing, mark = {}, time.monotonic()

    def lap(phase):
        nonlocal mark
        now = time.monotonic()
        timing[phase] = round(now - mark, 1)
        mark = now

    try:
        counts = {r["status"]: r["count"] for r in classified.groupBy("status").count().collect()}
        lap("read_classify")  # the first action: reads the bronze micro-batch and classifies it
        if counts.get("movement"):
            movements = classified.where("status='movement'").select("movement.*")
            if "_route" in classified.columns:
                movements = classified.where("status='movement'").select("movement.*", "_route")
                movements = movements.withColumn("processing_route", F.col("_route")).drop("_route")
            locations = None
            if reference_table:
                from streaming.locations import enrich, reference_snapshot

                locations = reference_snapshot(spark, reference_table)
                movements = enrich(movements, locations)
            bounds = batch_bounds(movements)
            lap("bounds")
            # A revision's id covers its whole body: an identical one has the same actual time.
            revisions_window = actual_predicate(bounds)
            merge_new(
                spark, movements, f"{namespace}.movement_revisions", "revision_id",
                prune=revisions_window,
            )  # fmt: skip
            lap("revisions")
            merge_current(spark, movements, namespace, locations, bounds)
            lap("current")
        if counts.get("rejected"):
            rejects = classified.where("status='rejected'").select("reject.*")
            merge_new(spark, rejects, f"{namespace}.rejected_messages", "record_id")
            spark.catalog.refreshTable(f"{namespace}.rejected_messages")
            # Reuse the stored record to keep failed_at stable when replaying a failed batch.
            durable = spark.table(f"{namespace}.rejected_messages").join(
                rejects.select("record_id").distinct(),
                "record_id",
                "left_semi",
            )
            publish_rejects(durable, dlq_topic)
            lap("rejects")
        log.info("batch %s: %s", batch_id, counts)
        if counts.get("movement"):
            log.info(
                "batch %s timing %s; actual %s..%s in %d ranges",
                batch_id, timing, bounds.actual_min, bounds.actual_max, len(bounds.actual_ranges),
            )  # fmt: skip
        if "_route" in classified.columns:
            log.info(
                "batch %s routes: %s",
                batch_id,
                {r["_route"]: r["count"] for r in classified.groupBy("_route").count().collect()},
            )
    finally:
        classified.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", default="bronze.trust_movement", type=identifier)
    parser.add_argument("--namespace", default="silver", type=identifier)
    parser.add_argument("--checkpoint", default="/checkpoints/silver_trust_movement_v2")
    parser.add_argument("--reference-table", default="ref.locations", type=identifier)
    parser.add_argument("--watermark-seconds", type=int, default=900)
    # Bound each micro-batch (Iceberg source option). Without it, a catch-up after a replay
    # takes every new bronze snapshot at once: one batch of millions of rows ran the driver out
    # of heap on 2026-11-17 (findings F23). Bronze writes ~200k-row files, so this is ~1 file.
    parser.add_argument("--max-rows-per-batch", type=int, default=200_000)
    parser.add_argument("--trigger", default="1 minute")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_session("silver_trust_movement")
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger("silver"))
    create_tables(spark, args.namespace)
    if not spark.catalog.tableExists(args.reference_table):
        raise ValueError("load CORPUS first with make load-corpus")
    source = spark.readStream.option(
        "streaming-max-rows-per-micro-batch", args.max_rows_per_batch
    ).table(args.source)
    routed = prepare_stream(source, args.watermark_seconds)
    writer = routed.writeStream.option("checkpointLocation", args.checkpoint).foreachBatch(
        lambda batch, bid: process_classified_batch(
            batch,
            bid,
            namespace=args.namespace,
            reference_table=args.reference_table,
        )
    )
    writer = (
        writer.trigger(availableNow=True)
        if args.available_now
        else writer.trigger(processingTime=args.trigger)
    )
    query = writer.start()
    signal.signal(signal.SIGTERM, lambda *_: query.stop())
    query.awaitTermination()


if __name__ == "__main__":
    main()
