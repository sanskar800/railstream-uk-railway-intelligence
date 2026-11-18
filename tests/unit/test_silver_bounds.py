"""Silver's scan bounds must never exclude the row a batch row matches (findings F26)."""

from datetime import datetime

from streaming.bounds import BatchBounds, between, match_predicate

A0, A1 = datetime(2026, 11, 16, 12, 0, 1), datetime(2026, 11, 16, 12, 0, 31)
P0, P1 = datetime(2026, 11, 16, 12, 1), datetime(2026, 11, 16, 12, 2)


def test_planned_calls_are_matched_on_their_planned_time_range():
    sql = match_predicate(BatchBounds(A0, A1, P0, P1), "t.")
    assert "t.planned_ts BETWEEN TIMESTAMP '2026-11-16 12:01:00.000000'" in sql
    # A correction may have moved the stored row's actual_ts by days; planned_ts is identity,
    # so the planned branch has no actual_ts restriction at all.
    assert sql.startswith("(t.planned_ts BETWEEN") and " OR (t.planned_ts IS NULL AND " in sql


def test_unresolved_reports_are_matched_on_their_exact_actual_time_range():
    sql = match_predicate(BatchBounds(A0, A1, None, None))
    assert sql == (
        "(planned_ts IS NULL AND actual_ts BETWEEN TIMESTAMP '2026-11-16 12:00:01.000000' "
        "AND TIMESTAMP '2026-11-16 12:00:31.000000')"
    )


def test_between_keeps_microseconds():
    assert between("x", A0.replace(microsecond=5), A1) == (
        "x BETWEEN TIMESTAMP '2026-11-16 12:00:01.000005' "
        "AND TIMESTAMP '2026-11-16 12:00:31.000000'"
    )
