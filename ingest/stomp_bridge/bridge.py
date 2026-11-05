"""Durable STOMP subscription to the TRUST feed: write each frame to disk, then ACK it.

Delivery guarantee: at least once. A frame is acknowledged only after RawCaptureWriter has
fsynced it, so a crash can cause a redelivered duplicate (removed later in silver) but not a
loss. The broker keeps unacknowledged and missed messages for 5 minutes only, so a longer
disconnection is logged as a probable data gap.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import stomp

from ingest.stomp_bridge.capture import RawCaptureWriter
from ingest.stomp_bridge.config import BridgeSettings

log = logging.getLogger("stomp_bridge")

MESSAGE_TTL_S = 300  # broker-side expiry of queued messages (Open Rail Data wiki)
MAX_BACKOFF_S = 60


@dataclass
class Stats:
    frames: int = 0
    messages: int = 0
    bytes: int = 0
    unparseable: int = 0
    last_frame_at: datetime | None = None


class CaptureListener(stomp.ConnectionListener):
    """Called on stomp.py's receiver thread for every frame."""

    def __init__(self, conn, writer: RawCaptureWriter) -> None:
        self.conn = conn
        self.writer = writer
        self.stats = Stats()
        self.lock = threading.Lock()
        self.disconnected = threading.Event()

    def on_message(self, frame) -> None:
        received_at = datetime.now(UTC)
        # 1. Durable write first. If it raises, the frame is not acknowledged and the
        #    broker redelivers it after reconnecting.
        self.writer.write(received_at, dict(frame.headers), frame.body)
        # 2. Then acknowledge. STOMP 1.2 acks by the frame's "ack" header.
        self.conn.ack(frame.headers.get("ack") or frame.headers["message-id"])
        with self.lock:
            self.stats.frames += 1
            self.stats.bytes += len(frame.body.encode("utf-8"))
            self.stats.last_frame_at = received_at
            try:
                payload = json.loads(frame.body)
                self.stats.messages += len(payload) if isinstance(payload, list) else 1
            except json.JSONDecodeError:
                self.stats.unparseable += 1  # still captured raw; parsing happens downstream

    def on_error(self, frame) -> None:
        log.error("broker error: %s", frame.body.strip()[:500])

    def on_disconnected(self) -> None:
        self.disconnected.set()

    def take_stats(self) -> Stats:
        with self.lock:
            stats, self.stats = self.stats, Stats(last_frame_at=self.stats.last_frame_at)
        return stats


def _connect(settings: BridgeSettings, writer: RawCaptureWriter):
    hb = settings.heartbeat_ms
    conn = stomp.Connection12([(settings.stomp_host, settings.stomp_port)], heartbeats=(hb, hb))
    listener = CaptureListener(conn, writer)
    conn.set_listener("capture", listener)
    # client-id + activemq.subscriptionName + per-message ACK = durable subscription.
    conn.connect(
        settings.username,
        settings.password.get_secret_value(),
        wait=True,
        headers={"client-id": settings.username},
    )
    conn.subscribe(
        destination=f"/topic/{settings.topic}",
        id=settings.subscription_name,
        ack="client-individual",
        headers={"activemq.subscriptionName": settings.subscription_name},
    )
    return conn, listener


def run(
    settings: BridgeSettings,
    writer: RawCaptureWriter,
    stop: threading.Event,
    stats_interval_s: float = 60,
) -> None:
    """Capture until `stop` is set, reconnecting with exponential backoff."""
    compressed = writer.compress_completed(datetime.now(UTC))
    if compressed:
        log.info("compressed %d completed hour file(s) left from a previous run", len(compressed))

    backoff = 1
    disconnected_at: datetime | None = None
    while not stop.is_set():
        try:
            conn, listener = _connect(settings, writer)
        except Exception as exc:  # network errors, refused login, broker unavailable
            log.warning("connect failed (%s); retrying in %ds", exc, backoff)
            stop.wait(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)
            continue

        backoff = 1
        log.info(
            "subscribed to /topic/%s (durable: %s)", settings.topic, settings.subscription_name
        )
        if disconnected_at is not None:
            gap_s = (datetime.now(UTC) - disconnected_at).total_seconds()
            if gap_s > MESSAGE_TTL_S:
                log.warning(
                    "DATA GAP: disconnected %.0fs since %s; messages older than %ds expired",
                    gap_s,
                    disconnected_at.isoformat(),
                    MESSAGE_TTL_S,
                )
            else:
                log.info("reconnected after %.0fs; queued messages will be redelivered", gap_s)

        next_stats = time.monotonic() + stats_interval_s
        while not stop.is_set() and not listener.disconnected.is_set():
            stop.wait(1)  # short waits keep Ctrl+C responsive on Windows
            if time.monotonic() >= next_stats:
                s = listener.take_stats()
                age = None
                if s.last_frame_at is not None:
                    age = (datetime.now(UTC) - s.last_frame_at).total_seconds()
                log.info(
                    "frames=%d messages=%d bytes=%d unparseable=%d last_frame_age_s=%s",
                    s.frames,
                    s.messages,
                    s.bytes,
                    s.unparseable,
                    f"{age:.0f}" if age is not None else "none",
                )
                next_stats += stats_interval_s

        if listener.disconnected.is_set():
            disconnected_at = datetime.now(UTC)
            log.warning("disconnected from broker; reconnecting")
        if conn.is_connected():
            conn.disconnect()

    writer.close()
    log.info("stopped")
