from types import SimpleNamespace

import pytest

from ingest.stomp_bridge.bridge import CaptureListener


class FakeConn:
    def __init__(self, events):
        self.events = events

    def ack(self, ack_id):
        self.events.append(("ack", ack_id))


class FakeWriter:
    def __init__(self, events, fail=False):
        self.events, self.fail = events, fail

    def write(self, received_at, headers, body):
        if self.fail:
            raise OSError("disk full")
        self.events.append(("write", body))


def frame(body='[{"header":{}},{"header":{}}]', ack="ack-1"):
    return SimpleNamespace(headers={"ack": ack, "message-id": "m-1"}, body=body)


def test_frame_is_written_before_it_is_acknowledged():
    events = []
    listener = CaptureListener(FakeConn(events), FakeWriter(events))

    listener.on_message(frame())

    assert events == [("write", '[{"header":{}},{"header":{}}]'), ("ack", "ack-1")]


def test_failed_write_is_not_acknowledged():
    events = []
    listener = CaptureListener(FakeConn(events), FakeWriter(events, fail=True))

    with pytest.raises(OSError):
        listener.on_message(frame())

    assert ("ack", "ack-1") not in events  # broker will redeliver it


def test_stats_count_messages_inside_frames_and_tolerate_bad_json():
    events = []
    listener = CaptureListener(FakeConn(events), FakeWriter(events))

    listener.on_message(frame())  # array of 2 messages
    listener.on_message(frame(body="[]"))  # empty arrays are legal
    listener.on_message(frame(body="not json"))  # captured, counted, not fatal

    stats = listener.take_stats()
    assert (stats.frames, stats.messages, stats.unparseable) == (3, 2, 1)
    assert listener.take_stats().frames == 0  # counters reset per interval
