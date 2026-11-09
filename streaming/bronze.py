"""Bronze: every record of raw.trust.movement appended to Iceberg, payload untouched.

Kafka -> lake.bronze.trust_movement, one row per Kafka record: the payload exactly as received
plus Kafka metadata and the bridge's headers. Nothing here parses the payload, so a malformed
message can never stop the stream; parsing and validation happen in silver (Day 6).

Exactly once per Kafka offset: Spark records the offsets of each micro-batch in the checkpoint
before running it, and Iceberg's streaming sink stores the query id and batch id in each
snapshot, so a batch re-run after a crash is recognised as already committed and skipped.
(kafka_partition, kafka_offset) is unique per row, which is how duplicates are checked.

Run in the Spark container: `python -m streaming.bronze [--available-now] [--trigger "1 minute"]`.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from streaming.session import build_session

log = logging.getLogger("bronze")

TOPIC = "raw.trust.movement"
TABLE = "bronze.trust_movement"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    kafka_topic     STRING    COMMENT 'Source topic',
    kafka_partition INT       COMMENT 'Source partition; with kafka_offset, unique per row',
    kafka_offset    BIGINT    COMMENT 'Source offset',
    kafka_ts        TIMESTAMP COMMENT 'Kafka record time (when it was produced): partition key',
    kafka_key       STRING    COMMENT 'train_id (the producer key)',
    payload         STRING    COMMENT 'The TRUST message exactly as received (JSON)',
    received_at     TIMESTAMP COMMENT 'Bridge receive time (header; the original time on replay)',
    ingest_source   STRING    COMMENT 'live or replay (header)',
    ingested_at     TIMESTAMP COMMENT 'When this micro-batch ran'
)
USING iceberg
PARTITIONED BY (days(kafka_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '100'
)
"""


def header(name: str):
    """A Kafka header as a string, or NULL if absent (try_: ANSI mode must not throw)."""
    return F.expr(
        f"CAST(try_element_at(filter(headers, h -> h.key = '{name}'), 1).value AS STRING)"
    )


def to_bronze(kafka: DataFrame) -> DataFrame:
    return kafka.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.try_to_timestamp(header("received_at")).alias("received_at"),
        header("source").alias("ingest_source"),
        F.current_timestamp().alias("ingested_at"),
    )


class ProgressLogger(StreamingQueryListener):
    """One log line per micro-batch: rows, rate, duration and the Kafka offsets reached."""

    def onQueryStarted(self, event) -> None:
        log.info("query started: id=%s run=%s", event.id, event.runId)

    def onQueryProgress(self, event) -> None:
        p = event.progress
        if p.numInputRows == 0:
            return
        log.info(
            "batch %d: %d rows in %.1fs (%.0f rows/s); offsets %s",
            p.batchId, p.numInputRows, p.batchDuration / 1000, p.processedRowsPerSecond or 0,
            p.sources[0].endOffset if p.sources else "?",
        )  # fmt: skip

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        log.info("query terminated%s", f": {event.exception}" if event.exception else "")


def start(spark: SparkSession, available_now: bool, trigger: str, checkpoint: str):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
    spark.sql(DDL)
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092"))
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")  # first run only; afterwards the checkpoint rules
        .option("includeHeaders", "true")
        .option("maxOffsetsPerTrigger", 200_000)  # bounds a backfill batch's memory
        # failOnDataLoss stays true (default): if offsets expired before being read (laptop off
        # longer than Kafka's 7-day retention) the query stops loudly; refill by replay.
        .load()
    )
    writer = (
        to_bronze(kafka)
        .coalesce(1)  # ~700 rows/min needs no parallelism; one file per batch, not three
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .queryName("bronze_trust_movement")
    )
    writer = (
        writer.trigger(availableNow=True)
        if available_now
        else writer.trigger(processingTime=trigger)
    )
    return writer.toTable(TABLE)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="process everything available, then stop (backfill, tests)",
    )
    parser.add_argument("--trigger", default=os.environ.get("BRONZE_TRIGGER", "1 minute"))
    parser.add_argument(
        "--checkpoint",
        default=os.path.join(
            os.environ.get("CHECKPOINT_ROOT", "/checkpoints"), "bronze_trust_movement"
        ),
    )
    args = parser.parse_args()  # fmt: skip

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = build_session("bronze_trust_movement")
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger())
    query = start(spark, args.available_now, args.trigger, args.checkpoint)
    log.info("writing %s -> lake.%s; checkpoint %s", TOPIC, TABLE, args.checkpoint)
    signal.signal(signal.SIGTERM, lambda *_: query.stop())  # docker stop: end cleanly
    query.awaitTermination()


if __name__ == "__main__":
    main()
