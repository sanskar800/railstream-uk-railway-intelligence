"""Silver's scan bounds must never exclude the row a batch row matches (findings F26)."""

import random
from datetime import datetime, timedelta

from streaming.bounds import (
    BatchBounds,
    actual_predicate,
    between,
    clustered_ranges,
    match_predicate,
)

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


def test_clustered_ranges_split_at_gaps_longer_than_an_hour():
    now = datetime(2026, 11, 26, 12, 0)
    late = datetime(2026, 11, 21, 8, 10)
    values = [now, now + timedelta(minutes=1), late, late + timedelta(minutes=2), None]
    assert clustered_ranges(values) == (
        (late, late + timedelta(minutes=2)),
        (now, now + timedelta(minutes=1)),
    )


def test_every_value_lies_inside_a_range_even_when_capped():
    rng = random.Random(7)
    base = datetime(2026, 11, 1)
    values = [base + timedelta(minutes=rng.randrange(0, 60 * 24 * 20)) for _ in range(3000)]
    ranges = clustered_ranges(values, max_ranges=10)
    assert len(ranges) <= 10
    assert all(lo <= hi for lo, hi in ranges)
    assert all(a[1] < b[0] for a, b in zip(ranges, ranges[1:], strict=False))  # sorted, disjoint
    assert all(any(lo <= v <= hi for lo, hi in ranges) for v in values)  # nothing excluded


def test_ranges_replace_the_wide_min_max_predicates():
    now, late = datetime(2026, 11, 26, 12, 0), datetime(2026, 11, 21, 8, 10)
    bounds = BatchBounds(
        late, now, late, now,
        actual_ranges=((late, late), (now, now)),
        planned_ranges=((late, late), (now, now)),
    )  # fmt: skip
    sql = match_predicate(bounds, "t.")
    assert sql.count("t.planned_ts BETWEEN") == 2 and "IS NULL" not in sql  # no unresolved rows
    assert actual_predicate(bounds).count("actual_ts BETWEEN") == 2


def test_unresolved_rows_keep_their_own_actual_ranges():
    now = datetime(2026, 11, 26, 12, 0)
    bounds = BatchBounds(now, now, None, None, actual_ranges=((now, now),),
                         unresolved_ranges=((now, now),))  # fmt: skip
    assert match_predicate(bounds) == (
        "((planned_ts IS NULL AND (actual_ts BETWEEN TIMESTAMP '2026-11-26 12:00:00.000000' "
        "AND TIMESTAMP '2026-11-26 12:00:00.000000')))"
    )
