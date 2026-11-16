"""Replay must never signal successful catch-up after incomplete processing."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from ingest.replay import __main__ as cli
from ingest.stomp_bridge.capture import RawCaptureWriter


@pytest.mark.parametrize(
    ("case", "exit_code"),
    [
        ("ok", 0),
        ("missing", 1),
        ("empty", 1),
        ("bad_frame", 1),
        ("bad_line", 1),
        ("failed", 1),
        ("pending", 1),
        ("mismatch", 1),
        ("stopped", 130),
    ],
)
def test_replay_exit_status(case, exit_code, tmp_path, monkeypatch):
    writer = RawCaptureWriter(tmp_path)
    if case != "missing":
        body = "not json" if case == "bad_frame" else json.dumps([{"body": {"train_id": "A"}}])
        if case == "empty":
            body = "[]"
        path = writer.write(datetime(2026, 11, 16, 10, tzinfo=UTC), {}, body)
        writer.close()
        if case == "bad_line":
            with path.open("ab") as fh:
                fh.write(b'{"broken":')

    class Producer:
        delivered = 0
        flushed = False

        def send(self, received_at, messages, block=False):
            self.delivered += len(messages)
            if case == "stopped":
                cli.signal.getsignal(cli.signal.SIGINT)(None, None)

        def flush(self, timeout):
            self.flushed = True
            return int(case == "pending")

        def take_stats(self):
            return SimpleNamespace(
                delivered=0 if case == "mismatch" else self.delivered,
                failed=int(case == "failed"),
            )

    producer = Producer()
    monkeypatch.setattr(cli.TrustProducer, "connect", lambda *a, **kw: producer)
    monkeypatch.setattr(
        "sys.argv",
        [
            "replay",
            "--root",
            str(tmp_path),
            "--from",
            "2026-11-16T10:00",
            "--to",
            "2026-11-16T10:01",
            "--speed",
            "0",
        ],
    )
    # Restore the process handler after the test; main temporarily installs its own.
    old = cli.signal.getsignal(cli.signal.SIGINT)
    try:
        if exit_code:
            with pytest.raises(SystemExit) as exc:
                cli.main()
            assert exc.value.code == exit_code
        else:
            cli.main()
    finally:
        cli.signal.signal(cli.signal.SIGINT, old)
    assert producer.flushed == (case != "missing")
