"""Spark schemas of the silver rows, shared by the silver job and its watermark routing."""

from pyspark.sql import types as T

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
            ("location_name", T.StringType()),
            ("crs", T.StringType()),
            ("tiploc", T.StringType()),
            ("location_matched", T.BooleanType()),
            ("location_reference_version", T.StringType()),
            ("processing_route", T.StringType()),
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
