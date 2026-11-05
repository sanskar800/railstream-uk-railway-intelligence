"""Entry point: `uv run python -m ingest.stomp_bridge [--out DIR] [--seconds N]`."""

import argparse
import logging
import signal
import threading
from pathlib import Path

from ingest.stomp_bridge.bridge import run
from ingest.stomp_bridge.capture import RawCaptureWriter
from ingest.stomp_bridge.config import BridgeSettings


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture the TRUST feed to hourly raw files.")
    parser.add_argument("--out", type=Path, default=Path("data/raw/trust"))
    parser.add_argument("--seconds", type=float, help="stop after N seconds (default: run forever)")
    parser.add_argument("--stats-interval", type=float, default=60)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("stomp.py").setLevel(logging.WARNING)

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    if args.seconds:
        timer = threading.Timer(args.seconds, stop.set)
        timer.daemon = True  # must not keep the process alive after Ctrl+C
        timer.start()

    run(BridgeSettings(), RawCaptureWriter(args.out), stop, args.stats_interval)


if __name__ == "__main__":
    main()
