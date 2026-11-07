"""Converting TRUST timestamps to UTC (the BST trap, docs/data-sources.md).

TRUST timestamps are epoch milliseconds, but seven fields hold UK *wall-clock* time encoded as
if it were UTC: during British Summer Time they are one hour in the future. The rule:
read the value as a naive wall-clock time, interpret it in Europe/London, convert to UTC.

Two hours a year need a decision:
- Autumn (last Sunday of October): 01:00-01:59 local happens twice, so a wall-clock time in it
  has two UTC candidates. We pick the one closest to a reference instant, the message's
  `msg_queue_timestamp` (true UTC: not on the affected list), because a report is queued seconds
  to minutes after the event, never an hour. Without a reference: the first occurrence (BST).
- Spring (last Sunday of March): 01:00-01:59 local does not exist. A value in it is treated as
  GMT (the offset before the jump) and is reported by `wall_clock_kind` as "nonexistent".
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")
EPOCH = datetime(1970, 1, 1)

# Fields holding UK wall-clock time: the Open Rail Data wiki's list, plus two spellings that occur
# in the data instead of the wiki's (findings F13). Every other epoch field (msg_queue_timestamp,
# creation_timestamp, reinstatement_timestamp, coo_timestamp, event_timestamp) is taken as true
# UTC, as documented; unverifiable until a summer capture (BST from 2027-03-28).
WALL_CLOCK_FIELDS = frozenset(
    {
        "actual_timestamp",
        "planned_timestamp",
        "gbtt_timestamp",
        "canx_timestamp",
        "dep_timestamp",
        "original_loc_timestamp",
        "orig_dep_timestamp",  # wiki name; never seen in a day of data (F13)
        "origin_dep_timestamp",  # 0001 activation: the wiki's orig_dep_timestamp
        "orig_loc_timestamp",  # 0002 cancellation: original_loc_timestamp elsewhere
    }
)


def parse_ms(value: str | int | None) -> int | None:
    """Epoch milliseconds from a TRUST string value; empty or missing -> None."""
    if value is None or value == "":
        return None
    return int(value)


def utc_from_ms(ms: int) -> datetime:
    """A true-UTC epoch value (e.g. msg_queue_timestamp) as an aware datetime."""
    return datetime.fromtimestamp(ms / 1000, UTC)


def _candidates(ms: int) -> tuple[datetime, list[datetime]]:
    """The naive wall-clock time and its valid UTC instants (0, 1 or 2 of them)."""
    wall = EPOCH + timedelta(milliseconds=ms)
    valid = []
    for fold in (0, 1):
        instant = wall.replace(tzinfo=LONDON, fold=fold).astimezone(UTC)
        # Valid only if it maps back to the same wall-clock time (rules out the spring gap).
        if instant.astimezone(LONDON).replace(tzinfo=None) == wall and instant not in valid:
            valid.append(instant)
    return wall, sorted(valid)


def wall_clock_kind(ms: int) -> Literal["normal", "ambiguous", "nonexistent"]:
    _, valid = _candidates(ms)
    return {1: "normal", 2: "ambiguous"}.get(len(valid), "nonexistent")


def wall_clock_to_utc(ms: int, reference: datetime | None = None) -> datetime:
    """Convert a wall-clock TRUST timestamp to UTC (rules in the module docstring)."""
    wall, valid = _candidates(ms)
    if not valid:  # spring gap: the offset in force before the jump, GMT = UTC+0
        return wall.replace(tzinfo=UTC)
    if len(valid) == 1 or reference is None:
        return valid[0]
    return min(valid, key=lambda instant: abs(instant - reference))


def event_times(message: dict) -> dict[str, datetime | None]:
    """UTC instants of every wall-clock field present in a TRUST message's body."""
    header, body = message.get("header", {}), message.get("body", {})
    queued = parse_ms(header.get("msg_queue_timestamp"))
    reference = utc_from_ms(queued) if queued is not None else None
    times = {}
    for field in WALL_CLOCK_FIELDS & body.keys():
        ms = parse_ms(body[field])
        times[field] = None if ms is None else wall_clock_to_utc(ms, reference)
    return times
