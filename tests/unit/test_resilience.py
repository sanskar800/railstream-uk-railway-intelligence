import logging
from datetime import UTC, datetime, timedelta

import pytest

from ingest.stomp_bridge.bridge import Backoff, QuietDetector, reconnect_message
from ingest.stomp_bridge.config import BridgeSettings

T0 = datetime(2026, 11, 7, 2, 0, tzinfo=UTC)


def test_backoff_ceiling_doubles_up_to_the_cap():
    lowest = Backoff(base_s=1, cap_s=60, rng=lambda: 0.0)  # rng 0 -> half the ceiling
    highest = Backoff(base_s=1, cap_s=60, rng=lambda: 1.0)  # rng 1 -> the full ceiling

    assert [lowest.next_delay() for _ in range(8)] == [0.5, 1, 2, 4, 8, 16, 30, 30]
    assert [highest.next_delay() for _ in range(8)] == [1, 2, 4, 8, 16, 32, 60, 60]


def test_backoff_is_jittered_within_bounds_and_resets():
    backoff = Backoff(base_s=1, cap_s=60)  # real randomness
    delays = [backoff.next_delay() for _ in range(10)]
    ceilings = [min(60, 2**i) for i in range(10)]
    assert all(c / 2 <= d <= c for d, c in zip(delays, ceilings, strict=True))
    assert len(set(delays)) > 1  # not a fixed schedule

    backoff.reset()
    assert backoff.next_delay() <= 1


def test_quiet_feed_is_reported_once_then_resumption_with_its_length():
    detector = QuietDetector(quiet_after_s=60)
    last = T0

    # A normal night gap (the longest in a full day was 20 s) is not quiet.
    assert detector.check(T0 + timedelta(seconds=20), last) is None
    assert detector.check(T0 + timedelta(seconds=61), last) == ("quiet", 61)
    # Reported once, not on every check.
    assert detector.check(T0 + timedelta(seconds=120), last) is None
    resumed_at = T0 + timedelta(seconds=300)
    assert detector.check(resumed_at, resumed_at) == ("resumed", 300)
    assert detector.check(resumed_at + timedelta(seconds=5), resumed_at) is None


def test_quiet_detector_waits_for_a_first_frame():
    assert QuietDetector(60).check(T0, None) is None


@pytest.mark.parametrize(
    ("gap_s", "level", "starts_with"),
    [
        (71, logging.INFO, "reconnected after 71s"),  # the real 2026-11-05 outage (findings F8)
        (300, logging.INFO, "reconnected after 300s"),
        (301, logging.WARNING, "DATA GAP"),  # beyond the broker's 5-minute expiry
    ],
)
def test_reconnect_is_a_data_gap_only_beyond_the_broker_expiry(gap_s, level, starts_with):
    got_level, message = reconnect_message(gap_s, T0)
    assert got_level == level
    assert message.startswith(starts_with)


def test_client_id_defaults_to_the_account_but_can_be_separate():
    default = BridgeSettings(username="me@example.com", password="x", _env_file=None)
    separate = BridgeSettings(
        username="me@example.com", password="x", client_id="me-test", _env_file=None
    )
    assert default.effective_client_id == "me@example.com"
    assert separate.effective_client_id == "me-test"
