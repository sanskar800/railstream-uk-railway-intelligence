"""Durable STOMP subscription to the TRUST feed: write each frame to disk, then ACK it.

Delivery guarantee: at least once. A frame is acknowledged only after RawCaptureWriter has
fsynced it, so a crash can cause a redelivered duplicate (removed later in silver) but not a
loss. The broker keeps unacknowledged and missed messages for 5 minutes only, so a longer
disconnection is logged as a data gap. A connection that stays up while frames stop arriving is
logged as a quiet feed.

Optionally (the laptop's live bridge) each frame's messages are then published to Kafka. That
happens after the ACK and asynchronously, so Kafka being slow or down never blocks capture; records
Kafka did not take are reported with their time window and refilled from disk by the replay tool.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import stomp

from ingest.stomp_bridge.capture import RawCaptureWriter
from ingest.stomp_bridge.config import BridgeSettings
from ingest.stomp_bridge.frames import FrameError, split_frame
from ingest.stream.producer import TrustProducer

log = logging.getLogger("stomp_bridge")

MESSAGE_TTL_S = 300  # broker-side expiry of queued messages (Open Rail Data wiki)


class Backoff:
    """Exponential backoff with "equal jitter": each delay is random in [ceiling/2, ceiling].

    Jitter stops many clients that lost the broker at the same moment from retrying in lockstep;
    keeping half the ceiling as a floor avoids near-zero delays that would hammer a broker that
    is refusing connections.
    """

    def __init__(
        self, base_s: float = 1.0, cap_s: float = 60.0, rng: Callable[[], float] = random.random
    ) -> None:
        self.base_s, self.cap_s, self.rng = base_s, cap_s, rng
        self.attempt = 0

    def next_delay(self) -> float:
        ceiling = min(self.cap_s, self.base_s * 2**self.attempt)
        self.attempt += 1
        return ceiling / 2 + self.rng() * ceiling / 2

    def reset(self) -> None:
        self.attempt = 0


class QuietDetector:
    """Detects a connected feed that has stopped sending frames, and when it resumes."""

    def __init__(self, quiet_after_s: float) -> None:
        self.quiet_after_s = quiet_after_s
        self.quiet_since: datetime | None = None

    def check(self, now: datetime, last_frame_at: datetime | None) -> tuple[str, float] | None:
        """Return ("quiet", silent_s) or ("resumed", quiet_s) on a state change, else None."""
        if last_frame_at is None:
            return None
        silent_s = (now - last_frame_at).total_seconds()
        if self.quiet_since is None and silent_s >= self.quiet_after_s:
            self.quiet_since = last_frame_at
            return "quiet", silent_s
        if self.quiet_since is not None and last_frame_at > self.quiet_since:
            quiet_s = (last_frame_at - self.quiet_since).total_seconds()
            self.quiet_since = None
            return "resumed", quiet_s
        return None


def reconnect_message(gap_s: float, since: datetime) -> tuple[int, str]:
    """Log level and message for a reconnection after `gap_s` seconds offline."""
    if gap_s > MESSAGE_TTL_S:
        return logging.WARNING, (
            f"DATA GAP: disconnected {gap_s:.0f}s since {since.isoformat()}; "
            f"messages older than {MESSAGE_TTL_S}s expired"
        )
    return logging.INFO, f"reconnected after {gap_s:.0f}s; queued messages will be redelivered"


@dataclass
class Stats:
    frames: int = 0
    messages: int = 0
    bytes: int = 0
    unparseable: int = 0
    last_frame_at: datetime | None = None


class CaptureListener(stomp.ConnectionListener):
    """Called on stomp.py's receiver thread for every frame."""

    def __init__(self, conn, writer: RawCaptureWriter, sink: TrustProducer | None = None) -> None:
        self.conn = conn
        self.writer = writer
        self.sink = sink
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
        try:
            messages = split_frame(frame.body)
        except FrameError:
            messages = None  # still captured raw; nothing to publish
        with self.lock:
            self.stats.frames += 1
            self.stats.bytes += len(frame.body.encode("utf-8"))
            self.stats.last_frame_at = received_at
            if messages is None:
                self.stats.unparseable += 1
            else:
                self.stats.messages += len(messages)
        # 3. Then publish (optional). The frame is already safe on disk, so a Kafka problem is
        #    logged, never raised into the receiver thread.
        if self.sink is not None and messages:
            try:
                self.sink.send(received_at, messages)
            except Exception:
                log.exception("publishing to Kafka failed; the frame is on disk")

    def on_error(self, frame) -> None:
        log.error("broker error: %s", frame.body.strip()[:500])

    def on_disconnected(self) -> None:
        self.disconnected.set()

    @property
    def last_frame_at(self) -> datetime | None:
        with self.lock:
            return self.stats.last_frame_at

    def take_stats(self) -> Stats:
        with self.lock:
            stats, self.stats = self.stats, Stats(last_frame_at=self.stats.last_frame_at)
        return stats


def _connect(settings: BridgeSettings, writer: RawCaptureWriter, sink: TrustProducer | None):
    hb = settings.heartbeat_ms
    conn = stomp.Connection12([(settings.stomp_host, settings.stomp_port)], heartbeats=(hb, hb))
    listener = CaptureListener(conn, writer, sink)
    conn.set_listener("capture", listener)
    # client-id + activemq.subscriptionName + per-message ACK = durable subscription.
    conn.connect(
        settings.username,
        settings.password.get_secret_value(),
        wait=True,
        headers={"client-id": settings.effective_client_id},
    )
    conn.subscribe(
        destination=f"/topic/{settings.topic}",
        id=settings.subscription_name,
        ack="client-individual",
        headers={"activemq.subscriptionName": settings.subscription_name},
    )
    return conn, listener


def _log_stats(listener: CaptureListener) -> None:
    s = listener.take_stats()
    age = None if s.last_frame_at is None else (datetime.now(UTC) - s.last_frame_at).total_seconds()
    log.info(
        "frames=%d messages=%d bytes=%d unparseable=%d last_frame_age_s=%s",
        s.frames, s.messages, s.bytes, s.unparseable,
        f"{age:.0f}" if age is not None else "none",
    )  # fmt: skip
    if listener.sink is not None:
        log.log(*produce_message(listener.sink.take_stats()))


def produce_message(p) -> tuple[int, str]:
    """Log level and message for one interval of Kafka publishing."""
    line = f"kafka sent={p.sent} delivered={p.delivered} failed={p.failed}"
    if not p.failed:
        return logging.INFO, line
    return logging.WARNING, (
        f"{line}; NOT IN KAFKA: frames received {p.failed_from:%Y-%m-%d %H:%M:%S}"
        f"–{p.failed_to:%H:%M:%S} UTC ({p.last_error}); refill them with the replay tool"
    )


def run(
    settings: BridgeSettings,
    writer: RawCaptureWriter,
    stop: threading.Event,
    stats_interval_s: float = 60,
    sink: TrustProducer | None = None,
) -> None:
    """Capture until `stop` is set, reconnecting with jittered exponential backoff."""
    compressed = writer.compress_completed(datetime.now(UTC))
    if compressed:
        log.info("compressed %d completed hour file(s) left from a previous run", len(compressed))

    backoff = Backoff()
    # "Away since" the last frame on disk, so a gap across a restart is detected too.
    disconnected_at: datetime | None = writer.last_received_at()
    if disconnected_at is not None:
        log.info("last captured frame on disk: %s", disconnected_at.isoformat())
    while not stop.is_set():
        try:
            conn, listener = _connect(settings, writer, sink)
        except Exception as exc:  # network errors, refused login, broker unavailable
            delay = backoff.next_delay()
            log.warning("connect failed (%r); retrying in %.1fs", exc, delay)
            stop.wait(delay)
            continue

        backoff.reset()
        log.info(
            "subscribed to /topic/%s (client-id: %s, durable: %s)",
            settings.topic, settings.effective_client_id, settings.subscription_name,
        )  # fmt: skip
        if disconnected_at is not None:
            gap_s = (datetime.now(UTC) - disconnected_at).total_seconds()
            log.log(*reconnect_message(gap_s, disconnected_at))

        quiet = QuietDetector(settings.quiet_after_s)
        next_stats = time.monotonic() + stats_interval_s
        while not stop.is_set() and not listener.disconnected.is_set():
            stop.wait(1)  # short waits keep Ctrl+C responsive on Windows
            event = quiet.check(datetime.now(UTC), listener.last_frame_at)
            if event is not None:
                kind, seconds = event
                if kind == "quiet":
                    log.warning("FEED QUIET: connected but no frames for %.0fs", seconds)
                else:
                    log.info("feed resumed after %.0fs quiet", seconds)
            if time.monotonic() >= next_stats:
                _log_stats(listener)
                next_stats += stats_interval_s

        if listener.disconnected.is_set():
            disconnected_at = datetime.now(UTC)
            log.warning("disconnected from broker; reconnecting")
        if conn.is_connected():
            conn.disconnect()

    writer.close()
    if sink is not None:
        undelivered = sink.flush()
        log.log(*produce_message(sink.take_stats()))
        if undelivered:
            log.warning("%d records still undelivered to Kafka at shutdown", undelivered)
    log.info("stopped")
