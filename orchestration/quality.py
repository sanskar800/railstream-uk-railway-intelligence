"""Daily data-quality checks, results kept over time in ops.quality_checks (task 11.1).

Each check is one SQL query returning one number, compared with a threshold. Every run appends
one row per check, so the history shows when something started to go wrong. Plain SQL through
Spark Connect instead of Soda: no new tool or dependency in the Airflow image (accepted
deviation, docs/architecture.md). The checks come from failures actually seen: duplicates after
replays (F34), capture gaps (F28, F36), feed artefacts (F34), stalled jobs (F32, F35).
"""

from __future__ import annotations

import operator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

RESULTS_TABLE = "ops.quality_checks"
OPS = {"==": operator.eq, "<=": operator.le, ">=": operator.ge}


@dataclass(frozen=True)
class Check:
    name: str
    table: str
    sql: str  # one row, one numeric column; {day} = the checked UTC day, {next_day} = day + 1
    rule: tuple[tuple[str, float], ...]  # all must hold, e.g. ((">=", 0.8), ("<=", 1.25))
    description: str


def day_filter(column: str) -> str:
    return f"{column} >= TIMESTAMP '{{day}}' AND {column} < TIMESTAMP '{{next_day}}'"


SILVER_DAY = day_filter("actual_ts")
CHECKS = [
    Check(
        "silver_null_keys", "silver.trust_movement",
        "SELECT count(*) FROM silver.trust_movement WHERE " + SILVER_DAY
        + " AND (event_key IS NULL OR train_id IS NULL OR loc_stanox IS NULL"
        " OR received_at IS NULL)",
        (("==", 0),), "reports without an event key, train, location or receive time",
    ),
    Check(
        "silver_duplicate_event_keys", "silver.trust_movement",
        "SELECT count(*) - count(DISTINCT event_key) FROM silver.trust_movement WHERE "
        + SILVER_DAY,
        (("==", 0),), "current calls stored more than once (silver deduplicates by event key)",
    ),
    Check(
        "silver_unknown_location_pct", "silver.trust_movement",
        "SELECT coalesce(100.0 * sum(CASE WHEN location_matched THEN 0 ELSE 1 END) / count(*), 0) "
        "FROM silver.trust_movement WHERE " + SILVER_DAY + " AND variation_status <> 'OFF ROUTE'",
        (("<=", 0.1),), "reports whose STANOX has no CORPUS name (reference out of date)",
    ),
    Check(
        "silver_future_timestamps", "silver.trust_movement",
        "SELECT count(*) FROM silver.trust_movement "
        "WHERE actual_ts > current_timestamp() + INTERVAL 10 MINUTES",
        (("==", 0),), "reports dated more than 10 minutes in the future (time-zone or clock bugs)",
    ),
    Check(
        "silver_implausible_delays", "silver.trust_movement",
        "SELECT count(*) FROM silver.trust_movement WHERE " + SILVER_DAY
        + " AND abs(delay_min) >= 720",
        (("<=", 10),), "delays of 12 hours or more (feed artefacts, left out of gold; F34)",
    ),
    Check(
        "bronze_offset_holes", "bronze.trust_movement",
        "SELECT coalesce(sum(holes), 0) FROM (SELECT max(kafka_offset) - min(kafka_offset) + 1 "
        "- count(DISTINCT kafka_offset) AS holes FROM bronze.trust_movement "
        "GROUP BY kafka_partition)",
        (("==", 0),), "Kafka offsets missing from bronze (lost messages)",
    ),
    Check(
        "bronze_empty_hours", "bronze.trust_movement",
        "SELECT 24 - count(DISTINCT hour(received_at)) FROM bronze.trust_movement WHERE "
        + day_filter("received_at"),
        (("==", 0),), "hours of the day with no message received (capture outages)",
    ),
    Check(
        "bronze_volume_vs_last_week", "bronze.trust_movement",
        "SELECT sum(CASE WHEN " + day_filter("received_at") + " THEN 1 ELSE 0 END) / "
        "greatest(sum(CASE WHEN received_at >= TIMESTAMP '{day}' - INTERVAL 7 DAYS "
        "AND received_at < TIMESTAMP '{next_day}' - INTERVAL 7 DAYS THEN 1 ELSE 0 END), 1) "
        "FROM bronze.trust_movement WHERE received_at >= TIMESTAMP '{day}' - INTERVAL 7 DAYS",
        ((">=", 0.8), ("<=", 1.25)), "messages received vs the same weekday a week earlier",
    ),
    Check(
        "silver_lag_minutes", "silver.trust_movement",
        "SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(received_at))) / 60 "
        "FROM silver.trust_movement WHERE actual_ts >= current_timestamp() - INTERVAL 1 DAY",
        (("<=", 15),), "minutes since the newest message reached silver (stalled streaming)",
    ),
    Check(
        "gold_freshness_minutes", "gold.punctuality_hourly",
        "SELECT (unix_timestamp(current_timestamp()) - "
        "unix_timestamp(max(latest_received_at))) / 60 FROM gold.punctuality_hourly",
        (("<=", 60),), "minutes since the newest arrival counted in gold (stalled gold builds)",
    ),
]  # fmt: skip


def render(check: Check, day: date) -> str:
    return check.sql.replace("{day}", f"{day} 00:00:00").replace(
        "{next_day}", f"{day + timedelta(days=1)} 00:00:00"
    )


def passes(value: float | None, rule: tuple[tuple[str, float], ...]) -> bool:
    """A missing value (no data at all) fails every rule."""
    return value is not None and all(OPS[op](value, limit) for op, limit in rule)


def run_checks(spark, day: date | None = None, now: datetime | None = None) -> list[dict]:
    """Run every check for `day` (default: yesterday, UTC) and return one result row each."""
    now = now or datetime.now(UTC)
    day = day or (now - timedelta(days=1)).date()
    results = []
    for check in CHECKS:
        raw = spark.sql(render(check, day)).first()[0]
        value = None if raw is None else float(raw)
        results.append(
            {
                "run_at": now,
                "checked_day": day,
                "check_name": check.name,
                "table_name": check.table,
                "value": value,
                "rule": " and ".join(f"{op} {limit:g}" for op, limit in check.rule),
                "passed": passes(value, check.rule),
                "description": check.description,
            }
        )
    return results


def record_results(spark, results: list[dict], table: str = RESULTS_TABLE) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.split('.')[0]}")
    frame = spark.createDataFrame(results)
    if spark.catalog.tableExists(table):
        frame.writeTo(table).append()
    else:
        frame.writeTo(table).using("iceberg").create()
