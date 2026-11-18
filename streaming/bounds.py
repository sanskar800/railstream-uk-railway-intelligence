"""Scan bounds for silver's MERGEs: pure SQL predicates, testable without Spark."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


# Bounding the target scans. Without bounds, each micro-batch scanned all of silver (millions
# of rows): batches took ~140 s for a 60 s trigger and the driver ran out of heap (findings F26).
# The bounds must never exclude the row a batch row matches, so they use what identity fixes:
# - a planned call's identity includes its planned time, so its current row has exactly the
#   same planned_ts, whatever corrections did to actual_ts;
# - an unresolved report (no planned time) is identified by its whole body, so a match has
#   exactly the same actual_ts.
# Iceberg keeps per-file min/max for both columns, so files outside these ranges are skipped.
@dataclass(frozen=True)
class BatchBounds:
    actual_min: datetime
    actual_max: datetime
    planned_min: datetime | None  # None if no row in the batch has a planned time
    planned_max: datetime | None


def between(column: str, lo: datetime, hi: datetime) -> str:
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    return f"{column} BETWEEN TIMESTAMP '{lo:{fmt}}' AND TIMESTAMP '{hi:{fmt}}'"


def match_predicate(bounds: BatchBounds, alias: str = "") -> str:
    """Rows a batch row can match: same planned-time range, or unresolved with same actual."""
    planned, actual = f"{alias}planned_ts", f"{alias}actual_ts"
    unresolved = f"({planned} IS NULL AND {between(actual, bounds.actual_min, bounds.actual_max)})"
    if bounds.planned_min is None:
        return unresolved
    return f"({between(planned, bounds.planned_min, bounds.planned_max)} OR {unresolved})"
