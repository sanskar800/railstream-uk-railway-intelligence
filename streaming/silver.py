"""Bronze -> typed, revision-aware silver movements and durable rejects.

Each micro-batch is idempotently merged before its checkpoint is committed.
Kafka DLQ publication is at least once, keyed by the durable reject identity.
Watermarked hot-path dedupe and CORPUS enrichment are subsequent Day 6 steps.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import signal

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from streaming.bronze import ProgressLogger
from streaming.session import build_session
from trust.silver import classify

log = logging.getLogger("silver")

MOVEMENT_SCHEMA = T.StructType(
    [
        T.StructField(name, dtype)
        for name, dtype in [
            ("train_id", T.StringType()),
            ("current_train_id", T.StringType()),
            ("event_type", T.StringType()),
            ("planned_event_type", T.StringType()),
            ("actual_ts", T.TimestampType()),
            ("planned_ts", T.TimestampType()),
            ("gbtt_ts", T.TimestampType()),
            ("loc_stanox", T.StringType()),
            ("reporting_stanox", T.StringType()),
            ("toc_id", T.StringType()),
            ("variation_status", T.StringType()),
            ("delay_min", T.LongType()),
            ("event_source", T.StringType()),
            ("is_offroute", T.BooleanType()),
            ("is_correction", T.BooleanType()),
            ("is_terminated", T.BooleanType()),
            ("is_delay_monitoring_point", T.BooleanType()),
            ("platform", T.StringType()),
            ("direction", T.StringType()),
            ("msg_queue_ts", T.TimestampType()),
            ("received_at", T.TimestampType()),
            ("ingest_source", T.StringType()),
            ("source_topic", T.StringType()),
            ("source_partition", T.IntegerType()),
            ("source_offset", T.LongType()),
            ("event_date", T.DateType()),
            ("timestamp_issues", T.ArrayType(T.StringType())),
            ("event_key", T.StringType()),
            ("revision_id", T.StringType()),
            ("identity_unresolved", T.BooleanType()),
            ("revision_conflict", T.BooleanType()),
        ]
    ]
)
REJECT_SCHEMA = T.StructType(
    [
        T.StructField("record_id", T.StringType()),
        T.StructField("value", T.StringType()),
    ]
)
CLASSIFIED_SCHEMA = T.StructType(
    [
        T.StructField("status", T.StringType()),
        T.StructField("movement", MOVEMENT_SCHEMA),
        T.StructField("reject", REJECT_SCHEMA),
    ]
)


def identifier(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)?", value):
        raise ValueError(f"invalid table/namespace identifier: {value!r}")
    return value


def create_tables(spark, namespace):
    identifier(namespace)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    columns = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in MOVEMENT_SCHEMA)
    for name in ("trust_movement", "movement_revisions"):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {namespace}.{name} ({columns}) USING iceberg "
            "PARTITIONED BY (days(actual_ts)) TBLPROPERTIES ('format-version'='2')"
        )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {namespace}.rejected_messages "
        "(record_id STRING, value STRING) USING iceberg"
    )


def merge_new(spark, frame, table, key):
    spark.catalog.refreshTable(table)
    frame.dropDuplicates([key]).createOrReplaceTempView("silver_insert")
    spark.sql(
        f"MERGE INTO {table} t USING silver_insert s ON t.{key}=s.{key} "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def merge_current(spark, movements, namespace):
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
    chosen = chosen.localCheckpoint(eager=True)
    try:
        chosen.createOrReplaceTempView("silver_current")
        spark.sql(
            f"MERGE INTO {table} t USING silver_current s ON t.event_key=s.event_key "
            "WHEN MATCHED AND (s.revision_id <> t.revision_id OR "
            "s.revision_conflict <> t.revision_conflict) THEN UPDATE SET * "
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
    ).persist()
    try:
        counts = {r["status"]: r["count"] for r in classified.groupBy("status").count().collect()}
        if counts.get("movement"):
            movements = classified.where("status='movement'").select("movement.*")
            merge_new(spark, movements, f"{namespace}.movement_revisions", "revision_id")
            merge_current(spark, movements, namespace)
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
    finally:
        classified.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", default="bronze.trust_movement", type=identifier)
    parser.add_argument("--namespace", default="silver", type=identifier)
    parser.add_argument("--checkpoint", default="/checkpoints/silver_trust_movement")
    parser.add_argument("--trigger", default="1 minute")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_session("silver_trust_movement")
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger())
    create_tables(spark, args.namespace)
    source = spark.readStream.table(args.source)
    writer = source.writeStream.option("checkpointLocation", args.checkpoint).foreachBatch(
        lambda batch, bid: process_batch(batch, bid, namespace=args.namespace)
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
