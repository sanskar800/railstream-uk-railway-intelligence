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

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {namespace}.rejected_messages "
        f"(record_id STRING, value STRING) USING iceberg TBLPROPERTIES ({TABLE_PROPERTIES})"
    )


def merge_new(spark, frame, table, key):
    spark.catalog.refreshTable(table)
    frame.dropDuplicates([key]).createOrReplaceTempView("silver_insert")
    spark.sql(
        f"MERGE INTO {table} t USING silver_insert s ON t.{key}=s.{key} "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def merge_current(spark, movements, namespace, locations=None):
    # Rank with the existing current row too, so old replay never overwrites a newer revision.
    table = f"{namespace}.trust_movement"
    spark.catalog.refreshTable(table)
    keys = movements.select("event_key").distinct()
    existing = spark.table(table).join(keys, "event_key", "left_semi")
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
    try:
        counts = {r["status"]: r["count"] for r in classified.groupBy("status").count().collect()}
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
            merge_new(spark, movements, f"{namespace}.movement_revisions", "revision_id")
            merge_current(spark, movements, namespace, locations)
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
        log.info("batch %s: %s", batch_id, counts)
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
    spark.streams.addListener(ProgressLogger())
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
