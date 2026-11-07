"""Entry point: `uv run python -m ingest.replay (--day YYYY-MM-DD | --from TS --to TS)
[--speed N] [--root DIR] [--kafka BOOTSTRAP]`.

Examples:
  make replay DAY=2026-11-06 SPEED=10          a day at 10x (2.4 h), for development
  make replay DAY=2026-11-06 SPEED=0           a day as fast as possible, for backfill
  make replay FROM=2026-11-07T15:00 TO=2026-11-07T15:05 SPEED=0
                                               refill a window the live bridge reported
"""

import argparse
import logging
import signal
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingest.replay.replay import Pacer, read_frames, replay
from ingest.stream.producer import TrustProducer


def utc(text: str) -> datetime:
    """Parse an ISO timestamp; one without an offset is taken as UTC (like the file layout)."""
    ts = datetime.fromisoformat(text)
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay captured TRUST frames into Kafka.")
    window = parser.add_mutually_exclusive_group(required=True)
    window.add_argument("--day", help="UTC day, YYYY-MM-DD")
    window.add_argument("--from", dest="start", type=utc, help="receive time from (UTC)")
    parser.add_argument("--to", dest="end", type=utc, help="receive time to, exclusive (UTC)")
    parser.add_argument("--speed", type=float, default=10, help="N x real time; 0 = no waiting")
    parser.add_argument("--root", type=Path, default=Path("data/raw/trust"))
    parser.add_argument("--kafka", default="127.0.0.1:19092", metavar="BOOTSTRAP")
    args = parser.parse_args()
    if args.day:
        start = utc(args.day)
        end = start + timedelta(days=1)
    elif args.end is None:
        parser.error("--from needs --to")
    else:
        start, end = args.start, args.end

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log = logging.getLogger("replay")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    producer = TrustProducer.connect(args.kafka, source="replay")
    log.info("replaying %s .. %s from %s at %s", start.isoformat(), end.isoformat(), args.root,
             f"{args.speed:g}x" if args.speed else "max speed")  # fmt: skip
    stats = replay(read_frames(args.root, start, end), producer, Pacer(args.speed), stop.is_set)
    undelivered = producer.flush(60)
    p = producer.take_stats()
    log.info(
        "done: %d frames, %d messages (%d unparseable frames skipped), received %s .. %s; "
        "kafka delivered=%d failed=%d undelivered=%d",
        stats.frames, stats.messages, stats.unparseable,
        stats.first and stats.first.isoformat(), stats.last and stats.last.isoformat(),
        p.delivered, p.failed, undelivered,
    )  # fmt: skip
    if stats.frames == 0:
        log.warning("no frames found in %s for that window (run `make pull-raw`?)", args.root)


if __name__ == "__main__":
    main()
