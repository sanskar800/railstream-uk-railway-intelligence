"""Re-publish captured frames to Kafka, keeping their relative timing at N× speed.

Reads the hourly raw files written by the bridge (the VM archive or the laptop's live capture)
and sends every message through the same TrustProducer as the live bridge, with the original
receive time in the `received_at` header and `source=replay`. Downstream, a replayed message is
indistinguishable from a live one except for that header.
"""

from __future__ import annotations

import gzip
import json
import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from ingest.stomp_bridge.capture import hour_of
from ingest.stomp_bridge.frames import FrameError, split_frame

log = logging.getLogger("replay")


@dataclass(frozen=True)
class Frame:
    received_at: datetime
    body: str


def hour_files(root: Path, start: datetime, end: datetime) -> list[Path]:
    """Hour files whose UTC hour overlaps [start, end), oldest first (.jsonl or .jsonl.gz)."""
    files = []
    hour = hour_of(start)
    while hour < end:
        base = root / f"date={hour:%Y-%m-%d}" / f"hour={hour:%H}.jsonl"
        for path in (base.with_suffix(".jsonl.gz"), base):
            if path.exists():
                files.append(path)
        hour += timedelta(hours=1)
    return files


def read_frames(root: Path, start: datetime, end: datetime) -> Iterator[Frame]:
    """Captured frames received in [start, end), in receive order."""
    for path in hour_files(root, start, end):
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                record = json.loads(line)
                received_at = datetime.fromisoformat(record["received_at"])
                if start <= received_at < end:
                    yield Frame(received_at, record["body"])


class Pacer:
    """Waits so that frame i is sent (t_i - t_0) / speed after the first. speed 0 = no waiting.

    Targets are measured from the start, not from the previous frame, so small sleep overshoots
    do not accumulate into drift over a long replay.
    """

    def __init__(
        self,
        speed: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if speed < 0:
            raise ValueError("speed must be >= 0")
        self.speed, self.clock, self.sleep = speed, clock, sleep
        self.first: tuple[datetime, float] | None = None

    def wait_for(self, received_at: datetime) -> None:
        if self.speed == 0:
            return
        if self.first is None:
            self.first = (received_at, self.clock())
            return
        data_t0, wall_t0 = self.first
        target = wall_t0 + (received_at - data_t0).total_seconds() / self.speed
        delay = target - self.clock()
        if delay > 0:
            self.sleep(delay)


@dataclass
class ReplayStats:
    frames: int = 0
    messages: int = 0
    unparseable: int = 0
    first: datetime | None = None
    last: datetime | None = None


def replay(
    frames: Iterator[Frame],
    producer,
    pacer: Pacer,
    stop: Callable[[], bool] = lambda: False,
    progress_every_s: float = 30,
) -> ReplayStats:
    """Send frames through `producer` (a TrustProducer) at the pacer's speed until done or stop."""
    stats = ReplayStats()
    next_progress = time.monotonic() + progress_every_s
    for frame in frames:
        if stop():
            log.warning("stopped early at %s", frame.received_at.isoformat())
            break
        pacer.wait_for(frame.received_at)
        try:
            messages = split_frame(frame.body)
        except FrameError:
            stats.unparseable += 1
            continue
        producer.send(frame.received_at, messages, block=True)
        stats.frames += 1
        stats.messages += len(messages)
        stats.first = stats.first or frame.received_at
        stats.last = frame.received_at
        if time.monotonic() >= next_progress:
            log.info(
                "replayed %d frames / %d messages; now at %s",
                stats.frames, stats.messages, frame.received_at.isoformat(),
            )  # fmt: skip
            next_progress += progress_every_s
    return stats
