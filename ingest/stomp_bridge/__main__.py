"""Entry point: `uv run python -m ingest.stomp_bridge [--out DIR] [--subscription NAME]
[--kafka BOOTSTRAP] [--seconds N]`.

Archive bridge (the VM): the defaults. Live bridge (the laptop): its own subscription plus Kafka,
see `make live`.
"""

import argparse
import logging
import signal
import threading
from pathlib import Path

from ingest.stomp_bridge.bridge import run
from ingest.stomp_bridge.capture import RawCaptureWriter
from ingest.stomp_bridge.config import BridgeSettings
from ingest.stream.producer import TrustProducer


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture the TRUST feed to hourly raw files.")
    parser.add_argument("--out", type=Path, default=Path("data/raw/trust"))
    parser.add_argument("--seconds", type=float, help="stop after N seconds (default: run forever)")
    parser.add_argument("--stats-interval", type=float, default=60)
    parser.add_argument(
        "--subscription", help="use a separate durable subscription with this name (default: the "
        "archive's). Never run two bridges on the same subscription: they split the feed (F6).",
    )  # fmt: skip
    parser.add_argument("--kafka", metavar="BOOTSTRAP", help="also publish to Kafka at BOOTSTRAP")
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

    settings = BridgeSettings()
    if args.subscription:
        settings = settings.for_subscription(args.subscription)
    sink = TrustProducer.connect(args.kafka, source="live") if args.kafka else None
    run(settings, RawCaptureWriter(args.out), stop, args.stats_interval, sink)


if __name__ == "__main__":
    main()
