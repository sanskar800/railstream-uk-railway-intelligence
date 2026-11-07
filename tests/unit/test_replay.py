import json
from datetime import UTC, datetime, timedelta

import pytest

from ingest.replay.replay import Frame, Pacer, read_frames, replay
from ingest.stomp_bridge.capture import RawCaptureWriter
from ingest.stream.producer import TrustProducer

T0 = datetime(2026, 11, 6, 8, 59, 50, tzinfo=UTC)


def message(train_id):
    return {"header": {"msg_type": "0003"}, "body": {"train_id": train_id}}


def capture(root, offsets_s):
    writer = RawCaptureWriter(root)
    for i, s in enumerate(offsets_s):
        writer.write(T0 + timedelta(seconds=s), {}, json.dumps([message(f"T{i}")]))
    writer.close()


def test_reads_frames_in_the_window_across_hour_files(tmp_path):
    capture(tmp_path, [0, 5, 15, 20, 3600])  # hours 08 (gz) and 09, plus one in hour 10

    frames = list(read_frames(tmp_path, T0 + timedelta(seconds=5), T0 + timedelta(seconds=20)))

    assert [f.received_at for f in frames] == [T0 + timedelta(seconds=s) for s in (5, 15)]


class FakeClock:
    def __init__(self):
        self.now, self.sleeps = 100.0, []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(round(seconds, 6))
        self.now += seconds


def test_pacer_keeps_relative_timing_divided_by_speed():
    clock = FakeClock()
    pacer = Pacer(10, clock=clock, sleep=clock.sleep)

    for s in (0, 10, 30):  # gaps of 10 s and 20 s in the capture
        pacer.wait_for(T0 + timedelta(seconds=s))

    assert clock.sleeps == [1.0, 2.0]


def test_pacer_does_not_accumulate_drift_when_sending_is_slow():
    clock = FakeClock()
    pacer = Pacer(10, clock=clock, sleep=clock.sleep)
    pacer.wait_for(T0)
    clock.now += 1.5  # sending took longer than the 1 s gap
    pacer.wait_for(T0 + timedelta(seconds=10))  # behind schedule: no sleep
    pacer.wait_for(T0 + timedelta(seconds=20))  # target is t0 + 2 s, now t0 + 1.5 s

    assert clock.sleeps == [0.5]


def test_speed_zero_never_waits_and_negative_is_rejected():
    clock = FakeClock()
    pacer = Pacer(0, clock=clock, sleep=clock.sleep)
    pacer.wait_for(T0)
    pacer.wait_for(T0 + timedelta(hours=1))
    assert clock.sleeps == []
    with pytest.raises(ValueError):
        Pacer(-1)


class QueueFullOnce:
    """Producer whose local queue is full on the first attempt only."""

    def __init__(self):
        self.produced, self.polls, self.raised = [], 0, False

    def produce(self, topic, value, key, headers, on_delivery):
        if not self.raised:
            self.raised = True
            raise BufferError
        self.produced.append((key, dict(headers)))
        on_delivery(None, None)

    def poll(self, _timeout):
        self.polls += 1


def test_replay_sends_every_message_with_its_original_receive_time():
    kafka = QueueFullOnce()
    frames = [
        Frame(T0, json.dumps([message("A"), message("B")])),
        Frame(T0 + timedelta(seconds=1), "not json"),
        Frame(T0 + timedelta(seconds=2), json.dumps([message("A")])),
    ]
    producer = TrustProducer(kafka, source="replay")

    stats = replay(iter(frames), producer, Pacer(0))

    assert (stats.frames, stats.messages, stats.unparseable) == (2, 3, 1)
    assert [k for k, _ in kafka.produced] == [b"A", b"B", b"A"]  # the full queue was waited out
    assert kafka.produced[0][1] == {"received_at": T0.isoformat().encode(), "source": b"replay"}
    assert producer.take_stats().failed == 0


def test_replay_can_be_stopped():
    frames = [Frame(T0 + timedelta(seconds=s), json.dumps([message("A")])) for s in range(5)]
    sent = []
    producer = type("P", (), {"send": lambda self, r, m, block: sent.append(r)})()

    replay(iter(frames), producer, Pacer(0), stop=lambda: len(sent) >= 2)

    assert len(sent) == 2
