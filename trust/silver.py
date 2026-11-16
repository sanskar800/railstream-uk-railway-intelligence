"""Stable identities and classification shared with the Spark silver processor."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from trust.contract import dlq_record
from trust.movements import parse_movement


def digest(value) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def classify(record: dict) -> dict:
    """A bronze record becomes a movement, reject, or valid other TRUST message type.

    Off-route identity is unresolved: retain distinct bodies, never merge unrelated
    calls on NULL planned time. They are excluded from punctuality by the metric rules.
    """

    def aware(value):
        return value.replace(tzinfo=UTC) if value and value.tzinfo is None else value

    received = aware(record.get("received_at"))
    result = parse_movement(
        record["payload"],
        topic=record["kafka_topic"],
        partition=record["kafka_partition"],
        offset=record["kafka_offset"],
        received_at=received,
        ingest_source=record.get("ingest_source"),
    )
    if result.rejection:
        identity = [record["kafka_topic"], record["kafka_partition"], record["kafka_offset"]]
        rejected = dlq_record(
            result.rejection,
            payload=record["payload"],
            topic=identity[0],
            partition=identity[1],
            offset=identity[2],
            received_at=received.isoformat() if received else None,
            failed_at=aware(record.get("ingested_at")) or datetime.now(UTC),
        )
        return {
            "status": "rejected",
            "movement": None,
            "reject": {"record_id": digest(identity), "value": json.dumps(rejected)},
        }
    if result.movement is None:
        return {"status": "other", "movement": None, "reject": None}
    row = result.movement
    message = json.loads(record["payload"])
    body = message["body"]
    unresolved = row["is_offroute"] or not body.get("planned_timestamp")
    key = (
        ["unresolved", body]
        if unresolved
        else [
            "planned",
            body["train_id"],
            body["loc_stanox"],
            body["planned_event_type"],
            body["planned_timestamp"],
        ]
    )
    row.update(
        event_key=digest(key),
        revision_id=digest([digest(key), message["header"]["msg_queue_timestamp"], body]),
        identity_unresolved=unresolved,
        revision_conflict=False,
    )
    return {"status": "movement", "movement": row, "reject": None}
