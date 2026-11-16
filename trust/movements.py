"""Validate and type a bronze payload before silver storage.

No Spark dependency: the silver job and unit tests share this conversion. Valid
non-movement messages stay in bronze for their own later transformations.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from trust.contract import Rejection, validate_raw
from trust.timestamps import event_times, parse_ms, utc_from_ms, wall_clock_kind


@dataclass(frozen=True)
class ParsedMovement:
    message_type: str | None = None
    movement: dict | None = None
    rejection: Rejection | None = None


def parse_movement(
    payload: str | bytes | None,
    *,
    topic: str,
    partition: int,
    offset: int,
    received_at: datetime | None = None,
    ingest_source: str | None = None,
) -> ParsedMovement:
    """One typed movement, a rejection, or a valid non-movement (never silently rejected).

    Kafka lineage identifies the input record, not a unique railway event. Event
    identity, revision precedence and late-data routing belong to the silver sink.
    """
    message, rejection = validate_raw(payload)
    if rejection:
        return ParsedMovement(rejection=rejection)
    kind = message["header"]["msg_type"]
    if kind != "0003":
        return ParsedMovement(message_type=kind)
    body, header = message["body"], message["header"]
    try:
        times = event_times(message)
        queued = utc_from_ms(int(header["msg_queue_timestamp"]))
        issues = [
            f"{field}:{quality}"
            for field in sorted(times)
            if (value := parse_ms(body[field])) is not None
            and (quality := wall_clock_kind(value)) != "normal"
        ]
    except (ValueError, TypeError, OverflowError, OSError) as exc:
        return ParsedMovement(
            message_type=kind,
            rejection=Rejection("schema", str(exc)[:500], "body", "timestamp_conversion"),
        )
    variation = body["variation_status"]
    delay = int(body["timetable_variation"]) * {"LATE": 1, "EARLY": -1}.get(variation, 0)
    row = {
        "train_id": body["train_id"],
        "current_train_id": body.get("current_train_id") or None,
        "event_type": body["event_type"],
        "planned_event_type": body["planned_event_type"],
        "actual_ts": times["actual_timestamp"],
        "planned_ts": times.get("planned_timestamp"),
        "gbtt_ts": times.get("gbtt_timestamp"),
        "loc_stanox": body["loc_stanox"],
        "reporting_stanox": body.get("reporting_stanox") or None,
        "toc_id": body["toc_id"],
        "variation_status": variation,
        "delay_min": delay,
        "event_source": body["event_source"],
        "is_offroute": body["offroute_ind"] == "true",
        "is_correction": body["correction_ind"] == "true",
        "is_terminated": body["train_terminated"] == "true",
        "is_delay_monitoring_point": body["delay_monitoring_point"] == "true",
        "platform": body.get("platform", "").strip() or None,
        "direction": body.get("direction_ind") or None,
        "msg_queue_ts": queued,
        "received_at": received_at,
        "ingest_source": ingest_source,
        "source_topic": topic,
        "source_partition": partition,
        "source_offset": offset,
        "event_date": times["actual_timestamp"].date(),
        "timestamp_issues": issues,
    }
    return ParsedMovement(message_type=kind, movement=row)
