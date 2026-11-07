"""Publishing TRUST messages to Kafka: one record per message, keyed by train id.

Used by the live bridge (after a frame is on disk and acknowledged) and by the replay tool.
Kafka is downstream of the raw files, never in front of them: produce() is asynchronous and a
failure is counted and reported, not retried forever, because the gap can always be refilled
from the raw files with the replay tool.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime

from ingest.stomp_bridge.frames import message_key
from ingest.stream.topics import RAW_TRUST


def producer_config(bootstrap: str, client_id: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        # Idempotence: the broker de-duplicates the producer's retries by sequence number, so a
        # retry after a lost ack can neither duplicate nor reorder records. Implies acks=all.
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "zstd",  # JSON compresses ~10x; batches are compressed as a whole
        "linger.ms": 100,  # wait up to 100 ms to fill a batch (a frame is ~27 messages)
        # Give up on a record after 5 minutes (Kafka down); it is reported and replayable.
        "delivery.timeout.ms": 300_000,
    }


def to_records(messages: list[dict]) -> list[tuple[bytes | None, bytes]]:
    """(key, value) per TRUST message. The value is the message object, compact JSON."""
    records = []
    for message in messages:
        key = message_key(message)
        value = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        records.append((key.encode() if key else None, value.encode()))
    return records


@dataclass
class ProduceStats:
    sent: int = 0
    delivered: int = 0
    failed: int = 0
    # Receive-time window of the failed records: what a replay must cover to fill the gap.
    failed_from: datetime | None = None
    failed_to: datetime | None = None
    last_error: str | None = None


class TrustProducer:
    def __init__(self, producer, source: str, topic: str = RAW_TRUST) -> None:
        self.producer = producer  # confluent_kafka.Producer, or a fake in tests
        self.source = source  # "live" or "replay", carried in a header for lineage
        self.topic = topic
        self.stats = ProduceStats()
        self.lock = threading.Lock()

    @classmethod
    def connect(cls, bootstrap: str, source: str) -> TrustProducer:
        from confluent_kafka import Producer

        return cls(Producer(producer_config(bootstrap, f"railstream-{source}")), source)

    def send(self, received_at: datetime, messages: list[dict], block: bool = False) -> int:
        """Queue the messages of one frame and return how many were queued.

        block=False (live bridge): never waits; a full local queue counts as failed (the frame is
        on disk and replayable). block=True (replay): waits for room instead, so nothing is lost.
        """
        headers = [
            ("received_at", received_at.isoformat().encode()),
            ("source", self.source.encode()),
        ]
        queued = 0
        for key, value in to_records(messages):
            while True:
                try:
                    self.producer.produce(
                        self.topic,
                        value=value,
                        key=key,
                        headers=headers,
                        on_delivery=lambda err, _msg, r=received_at: self._delivered(err, r),
                    )
                    queued += 1
                    break
                except BufferError:  # local queue full: producing faster than Kafka accepts
                    if not block:
                        self._delivered("local producer queue full", received_at)
                        break
                    self.producer.poll(0.5)  # deliveries free up room
        with self.lock:
            self.stats.sent += queued
        self.producer.poll(0)  # serve delivery callbacks of earlier records
        return queued

    def _delivered(self, err, received_at: datetime) -> None:
        with self.lock:
            if err is None:
                self.stats.delivered += 1
                return
            s = self.stats
            s.failed += 1
            s.last_error = str(err)
            s.failed_from = min(s.failed_from or received_at, received_at)
            s.failed_to = max(s.failed_to or received_at, received_at)

    def take_stats(self) -> ProduceStats:
        self.producer.poll(0)
        with self.lock:
            stats, self.stats = self.stats, ProduceStats()
        return stats

    def flush(self, timeout_s: float = 10) -> int:
        """Wait for outstanding deliveries; return how many are still undelivered."""
        return self.producer.flush(timeout_s)
