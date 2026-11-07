"""BST handling. Synthetic cases around the 2026 changeovers (the next real one is 2027-03-28).

WALL_CLOCK_CASES is the shared table: the Spark silver job (Day 6) is tested against it too.
"""

from datetime import UTC, datetime

import pytest

from trust.timestamps import event_times, parse_ms, wall_clock_kind, wall_clock_to_utc


def ms(wall: str) -> int:
    """Encode a UK wall-clock time the way TRUST does: as if it were UTC."""
    return int(datetime.fromisoformat(wall).replace(tzinfo=UTC).timestamp() * 1000)


def utc(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


# (case, wall-clock value, reference msg_queue time or None, expected UTC, kind)
WALL_CLOCK_CASES = [
    ("winter: wall clock = UTC", "2026-11-06 08:30", None, "2026-11-06 08:30", "normal"),
    ("summer: wall clock = UTC+1", "2026-07-01 10:30", None, "2026-07-01 09:30", "normal"),
    ("spring: last GMT minute", "2026-03-29 00:59", None, "2026-03-29 00:59", "normal"),
    ("spring: first BST minute", "2026-03-29 02:00", None, "2026-03-29 01:00", "normal"),
    ("spring gap: read as GMT", "2026-03-29 01:30", None, "2026-03-29 01:30", "nonexistent"),
    ("autumn: before the repeat", "2026-10-25 00:59", None, "2026-10-24 23:59", "normal"),
    ("autumn: 1st 01:30 (BST)", "2026-10-25 01:30", "2026-10-25 00:31", "2026-10-25 00:30",
     "ambiguous"),
    ("autumn: 2nd 01:30 (GMT)", "2026-10-25 01:30", "2026-10-25 01:32", "2026-10-25 01:30",
     "ambiguous"),
    ("autumn: no reference -> 1st", "2026-10-25 01:30", None, "2026-10-25 00:30", "ambiguous"),
    ("autumn: after the repeat", "2026-10-25 02:00", None, "2026-10-25 02:00", "normal"),
]  # fmt: skip


@pytest.mark.parametrize(
    ("wall", "reference", "expected", "kind"),
    [case[1:] for case in WALL_CLOCK_CASES],
    ids=[case[0] for case in WALL_CLOCK_CASES],
)
def test_wall_clock_to_utc(wall, reference, expected, kind):
    ref = utc(reference) if reference else None
    assert wall_clock_to_utc(ms(wall), ref) == utc(expected)
    assert wall_clock_kind(ms(wall)) == kind


def test_a_late_train_in_the_repeated_hour_resolves_to_the_nearer_occurrence():
    # Planned 01:30 wall clock, but the message was queued at 02:10 UTC (train running late):
    # 01:30 UTC (GMT) is 40 min away, 00:30 UTC (BST) 100 min, so GMT is chosen.
    assert wall_clock_to_utc(ms("2026-10-25 01:30"), utc("2026-10-25 02:10")) == utc(
        "2026-10-25 01:30"
    )


def test_real_november_message_is_unchanged():
    # A replayed record (2026-11-06 17:04:59 UTC): in November wall clock equals UTC.
    message = {
        "header": {"msg_type": "0003", "msg_queue_timestamp": "1793984699000"},
        "body": {
            "train_id": "652A41MS06",
            "actual_timestamp": "1793984700000",
            "planned_timestamp": "1793984670000",
            "gbtt_timestamp": "1793984700000",
            "loc_stanox": "65642",
        },
    }
    times = event_times(message)
    assert times == {
        "actual_timestamp": utc("2026-11-06 17:05:00"),
        "planned_timestamp": utc("2026-11-06 17:04:30"),
        "gbtt_timestamp": utc("2026-11-06 17:05:00"),
    }


def test_summer_message_moves_back_one_hour_and_leaves_queue_time_alone():
    message = {
        "header": {"msg_queue_timestamp": str(int(utc("2026-07-01 09:31").timestamp() * 1000))},
        "body": {"actual_timestamp": str(ms("2026-07-01 10:30")), "gbtt_timestamp": ""},
    }
    assert event_times(message) == {
        "actual_timestamp": utc("2026-07-01 09:30"),
        "gbtt_timestamp": None,  # empty optional field
    }


@pytest.mark.parametrize(
    ("msg_type", "field"),
    [("0001", "origin_dep_timestamp"), ("0002", "orig_loc_timestamp"), ("0002", "canx_timestamp")],
)
def test_wall_clock_fields_of_other_message_types_are_converted(msg_type, field):
    # Real field names from a day of capture (findings F13), with a summer value.
    message = {"header": {"msg_type": msg_type}, "body": {field: str(ms("2026-07-01 10:30"))}}
    assert event_times(message) == {field: utc("2026-07-01 09:30")}


def test_system_record_times_are_taken_as_true_utc():
    message = {"header": {}, "body": {"creation_timestamp": str(ms("2026-07-01 10:30"))}}
    assert event_times(message) == {}  # not a wall-clock field: left to the caller as UTC


@pytest.mark.parametrize(("value", "expected"), [("", None), (None, None), ("1793984700000",
                         1793984700000)])  # fmt: skip
def test_parse_ms(value, expected):
    assert parse_ms(value) == expected
