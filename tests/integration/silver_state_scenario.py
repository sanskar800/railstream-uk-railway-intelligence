"""Synthetic event-time jumps exercise state eviction without wall-clock sleeps or deletes."""

import copy
import json
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import functions as F

from streaming.bronze import DDL
from streaming.locations import load_locations, reference_snapshot
from streaming.session import build_session
from streaming.silver import create_tables, process_classified_batch
from streaming.watermarks import prepare_stream


def main():
    tag = uuid.uuid4().hex[:8]
    ns = f"silver_state_{tag}"
    ref, source, checkpoint = f"{ns}.locations", f"{ns}.input", f"/checkpoints/state_{tag}"
    topic = f"it.silver.state.dlq.{tag}"
    admin = AdminClient({"bootstrap.servers": "redpanda:9092"})
    admin.create_topics([NewTopic(topic, 1, 1)])[topic].result()
    spark = build_session("silver_state_scenario")
    spark.sparkContext.setLogLevel("ERROR")
    create_tables(spark, ns)
    spark.sql(DDL.format(table=source))
    schema = spark.table(source).schema
    samples = {
        r["case"]: r["message"]
        for r in map(
            json.loads, Path("tests/fixtures/trust_valid_samples.jsonl").read_text().splitlines()
        )
    }
    base = datetime(2026, 11, 16, 12, tzinfo=UTC)
    epoch = int(base.timestamp() * 1000)
    progress = []

    def movement(index, seconds, location="74446"):
        m = copy.deepcopy(samples["movement_on_route"])
        m["header"]["msg_queue_timestamp"] = str(epoch + int(seconds * 1000))
        m["body"].update(
            train_id=f"001A{index:02}AA16",
            loc_stanox=location,
            planned_timestamp=str(epoch + index * 60000),
            actual_timestamp=str(epoch + int(seconds * 1000)),
        )
        return m

    def row(message, offset, origin="live"):
        return {
            "payload": json.dumps(message) if isinstance(message, dict) else message,
            "kafka_topic": f"it.source.{tag}",
            "kafka_partition": 0,
            "kafka_offset": offset,
            "kafka_ts": base,
            "kafka_key": "test",
            "received_at": base,
            "ingested_at": base,
            "ingest_source": origin,
        }

    def append(rows):
        spark.createDataFrame(rows, schema).writeTo(source).append()

    def run(seconds):
        data = prepare_stream(
            spark.readStream.table(source),
            watermark_seconds=10,
            clock=F.lit(base + timedelta(seconds=seconds)),
        )
        q = (
            data.writeStream.option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .foreachBatch(
                lambda df, bid: process_classified_batch(
                    df, bid, namespace=ns, dlq_topic=topic, reference_table=ref
                )
            )
            .start()
        )
        assert q.awaitTermination(180), "query did not finish"
        progress.extend(q.recentProgress)

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "corpus.json"
        corpus = {
            "TIPLOCDATA": [
                {"STANOX": "74446", "NLCDESC": "Oxford alias", "3ALPHA": ""},
                {"STANOX": "74446", "NLCDESC": "Oxford", "3ALPHA": "OXF", "TIPLOC": "OXFD"},
            ]
        }
        path.write_text(json.dumps(corpus))
        version = load_locations(spark, path, ref)["reference_version"]
        load_locations(spark, path, ref)
        assert spark.table(ref).count() == 1
        assert reference_snapshot(spark, ref).first().location_name == "Oxford"
        a = movement(1, 1)
        append([row(a, 0), row(a, 1), row("bad json", 2), row(samples["activation"], 3)])
        run(2)
        correction = copy.deepcopy(a)
        correction["header"]["msg_queue_timestamp"] = str(epoch + 60000)
        correction["body"].update(actual_timestamp=str(epoch - 172800000), correction_ind="true")
        b = movement(2, 31)
        append(
            [
                row(b, 4),
                row(correction, 5),
                row(movement(3, -86400, "99999"), 6),
                row(movement(4, 9999999), 7),
                row(movement(5, -604800), 8, "replay"),
            ]
        )
        run(32)
        # Replay the original after its state expires, and duplicate B within its retained state.
        append([row(a, 9), row(b, 10)])
        run(33)
        spark.catalog.refreshTable(f"{ns}.trust_movement")
        rows = {r.train_id: r for r in spark.table(f"{ns}.trust_movement").collect()}
        assert len(rows) == 5
        assert rows[a["body"]["train_id"]].is_correction
        assert rows[a["body"]["train_id"]].actual_ts == (base - timedelta(days=2)).replace(
            tzinfo=None
        )
        unknown = rows["001A03AA16"]
        assert not unknown.location_matched and unknown.location_name is None
        assert unknown.loc_stanox == "99999" and unknown.location_reference_version == version
        assert rows["001A02AA16"].location_name == "Oxford"
        assert rows["001A04AA16"].processing_route == "future"
        assert rows["001A05AA16"].processing_route == "replay"
        assert rows["001A03AA16"].processing_route == "late"
        spark.catalog.refreshTable(f"{ns}.movement_revisions")
        assert spark.table(f"{ns}.movement_revisions").count() == 6
        operators = [op for p in progress for op in p.get("stateOperators", [])]
        duplicates = sum(
            op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0) for op in operators
        )
        removed = sum(op.get("numRowsRemoved", 0) for op in operators)
        assert duplicates >= 2, operators
        assert removed >= 1, operators
        watermarks = [p.get("eventTime", {}).get("watermark") for p in progress]
        watermarks = [v for v in watermarks if v]
        assert max(watermarks) < "2026-11-16T12:01", watermarks  # future row cannot poison it
        # A new reference version preserves history; replay refreshes enrichment, not row count.
        corpus["TIPLOCDATA"][1]["NLCDESC"] = "Oxford updated"
        path.write_text(json.dumps(corpus))
        new_version = load_locations(spark, path, ref)["reference_version"]
        assert new_version != version and spark.table(ref).count() == 2
        append([row(b, 11, "replay")])
        run(34)
        spark.catalog.refreshTable(f"{ns}.trust_movement")
        updated = spark.table(f"{ns}.trust_movement").where("train_id='001A02AA16'").first()
        assert updated.location_name == "Oxford updated"
        assert updated.location_reference_version == new_version
        assert spark.table(f"{ns}.trust_movement").count() == 5

    print(
        "SILVER_STATE_SCENARIO "
        + json.dumps(
            {
                "namespace": ns,
                "checkpoint": checkpoint,
                "current_rows": 5,
                "revision_rows": 6,
                "synthetic_watermark_seconds": 10,
                "dropped_duplicate_rows": duplicates,
                "evicted_state_rows": removed,
                "watermarks": watermarks,
                "verified": [
                    "bounded_live_dedupe",
                    "state_eviction",
                    "late_correction_preserved",
                    "old_replay_after_eviction",
                    "future_clock_guard",
                    "mixed_live_history",
                    "unknown_location_preserved",
                    "duplicate_corpus_keys_collapsed",
                    "reference_load_idempotent",
                    "reference_refresh_on_replay",
                ],
                "cleanup": "isolated tables/topic/checkpoint retained; no deletes or kills",
            }
        ),
        flush=True,
    )
    spark.stop()


if __name__ == "__main__":
    main()
