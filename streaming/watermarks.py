"""Bound recent live duplicate state; bypass it for history, revisions and unsafe clocks.

Only the live branch has a watermark. The bypass branch has no stateful filter,
so a global advance cannot throw away old backfill or corrections before MERGE.
"""

from pyspark.sql import functions as F

from streaming.schemas import CLASSIFIED_SCHEMA
from trust.silver import classify


def classified_stream(source):
    parse = F.udf(lambda row: classify(row.asDict(recursive=True)), CLASSIFIED_SCHEMA)
    return source.select(parse(F.struct(*source.columns)).alias("parsed")).select("parsed.*")


def prepare_stream(source, watermark_seconds=900, clock=None):
    if watermark_seconds < 2:
        raise ValueError("watermark must be at least two seconds")
    now = F.current_timestamp() if clock is None else clock
    parsed = classified_stream(source)
    actual = F.col("movement.actual_ts")
    # Half the horizon leaves margin for the previous batch's watermark. Clock is
    # Spark's micro-batch timestamp (stable on retry), never an unchecked source field.
    cutoff = now - F.expr(f"INTERVAL {int(watermark_seconds // 2)} SECONDS")
    routed = (
        parsed.withColumn("_event_time", actual)
        .withColumn("_revision_id", F.col("movement.revision_id"))
        .withColumn(
            "_route",
            F.when(F.col("status") != "movement", F.col("status"))
            .when(F.col("movement.ingest_source") == "replay", "replay")
            .when(F.col("movement.is_correction"), "correction")
            .when(F.size("movement.timestamp_issues") > 0, "timestamp_review")
            .when(actual > now, "future")
            .when(actual < cutoff, "late")
            .otherwise("live"),
        )
    )
    recent = (
        routed.where("_route='live'")
        .withWatermark("_event_time", f"{int(watermark_seconds)} seconds")
        .dropDuplicatesWithinWatermark(["_revision_id"])
    )
    bypass = routed.where("_route <> 'live'")
    return recent.unionByName(bypass).drop("_event_time", "_revision_id")
