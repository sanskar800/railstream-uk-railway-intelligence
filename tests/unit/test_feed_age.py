from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from ops.feed_age import feed_age_seconds, latest_record_time, metric_payload

NOW = datetime(2026, 11, 17, 15, 0, tzinfo=UTC)


def ms(dt):
    return int(dt.timestamp() * 1000)


class FakeConsumer:
    """Partitions with (low, high) watermarks and the timestamp of their newest record."""

    def __init__(self, partitions):
        self.partitions, self.assigned = partitions, None

    def list_topics(self, topic, timeout):
        return SimpleNamespace(topics={topic: SimpleNamespace(partitions=dict(self.partitions))})

    def get_watermark_offsets(self, tp, timeout):
        low, high, _ = self.partitions[tp.partition]
        return low, high

    def assign(self, tps):
        self.assigned = tps[0]

    def poll(self, timeout):
        _, high, newest = self.partitions[self.assigned.partition]
        assert self.assigned.offset == high - 1  # reads only the newest record
        return SimpleNamespace(error=lambda: None, timestamp=lambda: (1, ms(newest)))


def test_newest_record_across_partitions_and_empty_ones_are_skipped():
    consumer = FakeConsumer({
        0: (0, 10, NOW - timedelta(seconds=40)),
        1: (5, 5, None),  # empty partition
        2: (0, 3, NOW - timedelta(seconds=7)),
    })  # fmt: skip
    assert latest_record_time(consumer) == NOW - timedelta(seconds=7)


def test_empty_topic_has_no_age():
    consumer = FakeConsumer({0: (0, 0, None)})
    assert latest_record_time(consumer) is None
    assert feed_age_seconds(None, NOW) is None


@pytest.mark.parametrize(
    ("latest", "age"),
    [(NOW - timedelta(seconds=5), 5.0), (NOW - timedelta(minutes=20), 1200.0),
     (NOW + timedelta(seconds=3), 0.0)],  # clock skew never gives a negative age
)  # fmt: skip
def test_feed_age(latest, age):
    assert feed_age_seconds(latest, NOW) == age


def test_metric_payload_targets_the_railstream_namespace():
    payload = metric_payload(12.34, "ocid1.tenancy.example", NOW)
    assert payload["namespace"] == "railstream"  # the only namespace the policy allows
    assert payload["name"] == "feed_age_seconds"
    assert payload["dimensions"] == {"topic": "raw.trust.movement"}
    assert payload["datapoints"] == [{"timestamp": NOW, "value": 12.3}]
