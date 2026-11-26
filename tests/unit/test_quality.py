"""Data-quality check definitions (task 11.1): rules and SQL rendering, without Spark."""

from datetime import date

import pytest

from orchestration.quality import CHECKS, passes, render


def test_check_names_are_unique():
    names = [c.name for c in CHECKS]
    assert len(names) == len(set(names))


def test_day_placeholders_become_timestamp_literals():
    check = next(c for c in CHECKS if c.name == "silver_null_keys")
    sql = render(check, date(2026, 11, 25))
    assert "actual_ts >= TIMESTAMP '2026-11-25 00:00:00'" in sql
    assert "actual_ts < TIMESTAMP '2026-11-26 00:00:00'" in sql
    assert "{day}" not in sql and "{next_day}" not in sql


@pytest.mark.parametrize("check", CHECKS, ids=lambda c: c.name)
def test_every_check_renders_without_placeholders(check):
    sql = render(check, date(2026, 11, 25))
    assert sql.startswith("SELECT") and "{" not in sql


@pytest.mark.parametrize(
    ("value", "rule", "expected"),
    [
        (0, (("==", 0),), True),
        (1, (("==", 0),), False),
        (0.95, ((">=", 0.8), ("<=", 1.25)), True),
        (0.5, ((">=", 0.8), ("<=", 1.25)), False),
        (None, (("<=", 15),), False),  # no data at all must not pass
    ],
)
def test_rules(value, rule, expected):
    assert passes(value, rule) is expected
