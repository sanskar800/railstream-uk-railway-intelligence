"""Scan bounds for silver's MERGEs: pure SQL predicates, testable without Spark."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

# Bounding the target scans. Without bounds, each micro-batch scanned all of silver (millions
# of rows): batches took ~140 s for a 60 s trigger and the driver ran out of heap (findings F26).
# The bounds must never exclude the row a batch row matches, so they use what identity fixes:
# - a planned call's identity includes its planned time, so its current row has exactly the
#   same planned_ts, whatever corrections did to actual_ts;
# - an unresolved report (no planned time) is identified by its whole body, so a match has
#   exactly the same actual_ts.
# Iceberg keeps per-file min/max for both columns, so files outside these ranges are skipped.
#
# One min..max range per column was not enough: a batch is mostly the last minute plus a few
# late corrections from days ago, and one old report stretched the range over days (a 752-row
# batch scanned 3.9 M revisions, findings F35). The ranges below cluster the batch's own
# values, so each late correction costs one narrow range. Every value lies inside a range,
# so the guarantee above holds unchanged.
Range = tuple[datetime, datetime]
CLUSTER_GAP = timedelta(hours=1)  # a longer gap between values starts a new range
MAX_RANGES = 40  # keeps the predicate short; the gap widens until the ranges fit


@dataclass(frozen=True)
class BatchBounds:
    actual_min: datetime
    actual_max: datetime
    planned_min: datetime | None  # None if no row in the batch has a planned time
    planned_max: datetime | None
    # Clustered ranges (empty = use min..max): all actual times, planned times, and actual
    # times of rows without a planned time.
    actual_ranges: tuple[Range, ...] = ()
    planned_ranges: tuple[Range, ...] = ()
    unresolved_ranges: tuple[Range, ...] = ()


def clustered_ranges(
    values: Iterable[datetime | None], gap: timedelta = CLUSTER_GAP, max_ranges: int = MAX_RANGES
) -> tuple[Range, ...]:
    """Sorted, disjoint [lo, hi] ranges that contain every value.

    A new range starts where consecutive values are more than `gap` apart; if that gives more
    than `max_ranges`, the gap doubles until it does not.
    """
    points = sorted({v for v in values if v is not None})
    if not points:
        return ()
    while True:
        ranges, lo, hi = [], points[0], points[0]
        for value in points[1:]:
            if value - hi > gap:
                ranges.append((lo, hi))
                lo = value
            hi = value
        ranges.append((lo, hi))
        if len(ranges) <= max_ranges:
            return tuple(ranges)
        gap *= 2


def any_between(column: str, ranges: tuple[Range, ...]) -> str:
    return "(" + " OR ".join(between(column, lo, hi) for lo, hi in ranges) + ")"


def between(column: str, lo: datetime, hi: datetime) -> str:
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    return f"{column} BETWEEN TIMESTAMP '{lo:{fmt}}' AND TIMESTAMP '{hi:{fmt}}'"


def match_predicate(bounds: BatchBounds, alias: str = "") -> str:
    """Rows a batch row can match: same planned-time range, or unresolved with same actual."""
    planned, actual = f"{alias}planned_ts", f"{alias}actual_ts"
    if bounds.actual_ranges:
        # An unresolved stored row can only match an unresolved batch row (same body), so the
        # branch is needed only if the batch has one.
        parts = []
        if bounds.planned_ranges:
            parts.append(any_between(planned, bounds.planned_ranges))
        if bounds.unresolved_ranges:
            parts.append(f"({planned} IS NULL AND {any_between(actual, bounds.unresolved_ranges)})")
        return "(" + " OR ".join(parts) + ")" if parts else "FALSE"
    unresolved = f"({planned} IS NULL AND {between(actual, bounds.actual_min, bounds.actual_max)})"
    if bounds.planned_min is None:
        return unresolved
    return f"({between(planned, bounds.planned_min, bounds.planned_max)} OR {unresolved})"


def actual_predicate(bounds: BatchBounds, column: str = "actual_ts") -> str:
    """Rows with one of the batch's actual times: an identical revision has the same one."""
    if bounds.actual_ranges:
        return any_between(column, bounds.actual_ranges)
    return between(column, bounds.actual_min, bounds.actual_max)
