"""Entry point: `python -m ingest.stomp_bridge [--out DIR] [--kafka BOOTSTRAP] [--seconds N]`.

Runs on the capture VM (infra/capture): writes the raw archive and, when BRIDGE_KAFKA_BOOTSTRAP is
set, also publishes to the processing VM's Kafka (ADR-012). One bridge, one subscription.
"""

import argparse
import logging
import os
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
        "--kafka",
        metavar="BOOTSTRAP",
        default=os.environ.get("BRIDGE_KAFKA_BOOTSTRAP") or None,
        help="also publish to Kafka at BOOTSTRAP (default: $BRIDGE_KAFKA_BOOTSTRAP; the capture VM "
        "sets it to the processing VM's private address, ADR-012)",
    )
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
    sink = TrustProducer.connect(args.kafka, source="live") if args.kafka else None
    run(settings, RawCaptureWriter(args.out), stop, args.stats_interval, sink)


if __name__ == "__main__":
    main()
