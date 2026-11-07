import json
import logging
from datetime import UTC, datetime, timedelta

from ingest.stomp_bridge.bridge import produce_message
from ingest.stomp_bridge.config import BridgeSettings
from ingest.stream.producer import TrustProducer, producer_config, to_records

T0 = datetime(2026, 11, 7, 15, 0, 1, tzinfo=UTC)
MOVEMENT = {"header": {"msg_type": "0003"}, "body": {"train_id": "872L09MX03", "loc_stanox": "87"}}
NO_TRAIN = {"header": {"msg_type": "0003"}, "body": {}}


class FakeKafka:
    """Records produce() calls; deliver() then fires the callbacks like poll() would."""

    def __init__(self, full_after=None):
        self.produced, self.pending, self.full_after = [], [], full_after

    def produce(self, topic, value, key, headers, on_delivery):
        if self.full_after is not None and len(self.produced) >= self.full_after:
            raise BufferError("queue full")
        self.produced.append({"topic": topic, "key": key, "value": value, "headers": headers})
        self.pending.append(on_delivery)

    def deliver(self, err=None):
        for callback in self.pending:
            callback(err, None)
        self.pending = []

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        return len(self.pending)


def test_one_record_per_message_keyed_by_train_id():
    records = to_records([MOVEMENT, NO_TRAIN])

    assert [key for key, _ in records] == [b"872L09MX03", None]  # no train id -> no key
    assert json.loads(records[0][1]) == MOVEMENT  # value is the message itself


def test_records_carry_receive_time_and_source_headers():
    kafka = FakeKafka()
    TrustProducer(kafka, source="live").send(T0, [MOVEMENT])

    [record] = kafka.produced
    assert record["topic"] == "raw.trust.movement"
    assert dict(record["headers"]) == {
        "received_at": b"2026-11-07T15:00:01+00:00",
        "source": b"live",
    }


def test_failed_deliveries_report_the_window_to_replay():
    kafka = FakeKafka()
    producer = TrustProducer(kafka, source="live")
    producer.send(T0, [MOVEMENT])
    producer.send(T0 + timedelta(minutes=4), [MOVEMENT, MOVEMENT])
    kafka.deliver(err="Local: Message timed out")

    stats = producer.take_stats()
    assert (stats.sent, stats.delivered, stats.failed) == (3, 0, 3)
    assert (stats.failed_from, stats.failed_to) == (T0, T0 + timedelta(minutes=4))
    level, message = produce_message(stats)
    assert level == logging.WARNING
    assert "2026-11-07 15:00:01–15:04:01 UTC" in message
    assert producer.take_stats().failed == 0  # counters reset per interval


def test_a_full_local_queue_counts_as_failed_instead_of_raising():
    kafka = FakeKafka(full_after=1)
    producer = TrustProducer(kafka, source="live")

    assert producer.send(T0, [MOVEMENT, MOVEMENT]) == 1

    stats = producer.take_stats()
    assert (stats.sent, stats.failed) == (1, 1)


def test_successful_interval_logs_at_info():
    kafka = FakeKafka()
    producer = TrustProducer(kafka, source="live")
    producer.send(T0, [MOVEMENT])
    kafka.deliver()

    assert produce_message(producer.take_stats()) == (
        logging.INFO,
        "kafka sent=1 delivered=1 failed=0",
    )


def test_producer_is_idempotent_with_acks_all():
    config = producer_config("127.0.0.1:19092", "railstream-live")
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"


def test_live_subscription_gets_its_own_client_id():
    archive = BridgeSettings(username="me@example.com", password="x", _env_file=None)
    live = archive.for_subscription("railstream-live")

    assert (live.subscription_name, live.effective_client_id) == (
        "railstream-live",
        "me@example.com-railstream-live",
    )
    assert archive.subscription_name == "railstream-trust"  # original unchanged
