"""Run in the Spark image: isolated retained tables/topic; no drops or kills."""

import copy
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from streaming.bronze import DDL
from streaming.session import build_session
from streaming.silver import create_tables, process_batch


def main():
    tag = uuid.uuid4().hex[:8]
    namespace, topic = f"silver_it_{tag}", f"it.silver.dlq.{tag}"
    checkpoint, bootstrap = f"/checkpoints/silver_it_{tag}", "redpanda:9092"
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(topic, 1, 1, config={"retention.ms": "86400000"})])[
        topic
    ].result()
    spark = build_session("silver_scenario")
    spark.sparkContext.setLogLevel("ERROR")
    create_tables(spark, namespace)
    source = f"{namespace}.input"
    spark.sql(DDL.format(table=source))
    schema = spark.table(source).schema
    samples = {
        r["case"]: r["message"]
        for r in map(
            json.loads, Path("tests/fixtures/trust_valid_samples.jsonl").read_text().splitlines()
        )
    }
    first = copy.deepcopy(samples["movement_on_route"])
    now = datetime(2026, 11, 16, 12, tzinfo=UTC)
    epoch = int(now.timestamp() * 1000)
    first["header"]["msg_queue_timestamp"] = str(epoch)
    first["body"].update(actual_timestamp=str(epoch), planned_timestamp=str(epoch))

    def bronze(message, offset):
        return {
            "kafka_topic": f"it.silver.source.{tag}",
            "kafka_partition": 0,
            "kafka_offset": offset,
            "kafka_ts": now,
            "kafka_key": "test",
            "payload": json.dumps(message) if isinstance(message, dict) else message,
            "received_at": now,
            "ingest_source": "replay",
            "ingested_at": now,
        }

    def append(records):
        spark.createDataFrame(records, schema).writeTo(source).append()

    def run():
        query = (
            spark.readStream.table(source)
            .writeStream.option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .foreachBatch(
                lambda df, bid: process_batch(df, bid, namespace=namespace, dlq_topic=topic)
            )
            .start()
        )
        assert query.awaitTermination(180), "silver query did not finish"

    def current():
        spark.catalog.refreshTable(f"{namespace}.trust_movement")
        rows = spark.table(f"{namespace}.trust_movement").collect()
        assert len(rows) == 1
        return rows[0]

    append([bronze(first, 0), bronze("not json", 1), bronze(samples["activation"], 2)])
    run()
    original_key = current().event_key
    correction = copy.deepcopy(first)
    correction["header"]["msg_queue_timestamp"] = str(epoch + 60000)
    correction["body"].update(actual_timestamp=str(epoch + 120000), correction_ind="true")
    append([bronze(correction, 3), bronze(first, 4), bronze(correction, 5)])
    run()
    row = current()
    assert row.event_key == original_key and row.is_correction
    assert row.actual_ts.isoformat() == "2026-11-16T12:02:00"
    spark.catalog.refreshTable(f"{namespace}.movement_revisions")
    assert spark.table(f"{namespace}.movement_revisions").count() == 2
    tie = copy.deepcopy(correction)
    tie["body"]["actual_timestamp"] = str(epoch + 180000)
    append([bronze(tie, 6)])
    run()
    assert current().revision_conflict
    newer = copy.deepcopy(correction)
    newer["header"]["msg_queue_timestamp"] = str(epoch + 120000)
    newer["body"]["actual_timestamp"] = str(epoch + 240000)
    append([bronze(newer, 7)])
    run()
    assert not current().revision_conflict
    assert current().actual_ts.isoformat() == "2026-11-16T12:04:00"

    # Fail between the durable reject merge and Kafka publication, then retry.
    bad = spark.createDataFrame([bronze("not json", 1)], schema)
    with patch("streaming.silver.publish_rejects", side_effect=RuntimeError("injected failure")):
        try:
            process_batch(bad, 100, namespace=namespace, dlq_topic=topic)
        except RuntimeError as exc:
            assert str(exc) == "injected failure"
        else:
            raise AssertionError("expected sink failure")
    process_batch(bad, 100, namespace=namespace, dlq_topic=topic)
    spark.catalog.refreshTable(f"{namespace}.rejected_messages")
    assert spark.table(f"{namespace}.rejected_messages").count() == 1
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": f"verify-{tag}", "enable.auto.commit": False}
    )
    lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, 0))
    consumer.assign([TopicPartition(topic, 0, lo)])
    records = []
    for _ in range(hi - lo):
        item = consumer.poll(15)
        assert item and not item.error()
        records.append((item.key().decode(), json.loads(item.value())))
    consumer.close()
    assert len(records) >= 2 and len({key for key, _ in records}) == 1
    assert all(value["source_offset"] == 1 for _, value in records)
    assert len({value["failed_at"] for _, value in records}) == 1
    spark.catalog.refreshTable(f"{namespace}.movement_revisions")
    print(
        "SILVER_SCENARIO "
        + json.dumps(
            {
                "namespace": namespace,
                "checkpoint": checkpoint,
                "dlq_topic": topic,
                "current_rows": spark.table(f"{namespace}.trust_movement").count(),
                "revision_rows": spark.table(f"{namespace}.movement_revisions").count(),
                "durable_rejects": 1,
                "kafka_dlq_records": len(records),
                "unique_dlq_keys": 1,
                "checkpoint_restarts": 3,
                "verified": [
                    "correction_updates_call",
                    "old_replay_ignored",
                    "repeat_revision_deduped",
                    "equal_time_conflict_flagged",
                    "newer_revision_clears_conflict",
                    "reject_failure_retry",
                ],
                "cleanup": "isolated tables, checkpoint and topic retained; no deletes or kills",
            }
        ),
        flush=True,
    )
    spark.stop()


if __name__ == "__main__":
    main()
