"""Kafka topics, declared in code and created explicitly.

Auto-creation would give a topic the broker's defaults (partition count, retention) the first
time a producer happens to write to it; declaring them here makes the layout reviewable.

Run `uv run python -m ingest.stream.topics` (part of `make up`); it is idempotent.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

log = logging.getLogger("topics")

RAW_TRUST = "raw.trust.movement"
DLQ_TRUST = "dlq.trust.movement"
DAY_MS = 24 * 3600 * 1000


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    config: dict[str, str] = field(default_factory=dict)


TOPICS = [
    # Key = train_id. 3 partitions: enough parallelism for Spark local[2]; per-train order holds
    # because one train always hashes to one partition. Kafka is a replayable buffer, not the
    # archive (the hourly raw files are), so 7 days of retention is plenty.
    TopicSpec(RAW_TRUST, 3, {"retention.ms": str(7 * DAY_MS), "cleanup.policy": "delete"}),
    # Rejected records with reason and source offset. Low volume, so one partition; kept 30 days
    # because a rejection is investigated, the contract or parser fixed, and the records replayed.
    TopicSpec(DLQ_TRUST, 1, {"retention.ms": str(30 * DAY_MS), "cleanup.policy": "delete"}),
]


def ensure_topics(admin, topics: list[TopicSpec] = TOPICS, replication: int = 1) -> list[str]:
    """Create the topics that do not exist yet; return their names. Existing ones are kept."""
    from confluent_kafka.admin import NewTopic

    existing = admin.list_topics(timeout=10).topics
    for spec in topics:
        if spec.name in existing and len(existing[spec.name].partitions) != spec.partitions:
            log.warning(
                "%s has %d partitions, declared %d (partitions are never changed automatically)",
                spec.name, len(existing[spec.name].partitions), spec.partitions,
            )  # fmt: skip
    new = [
        NewTopic(s.name, s.partitions, replication, config=s.config)
        for s in topics
        if s.name not in existing
    ]
    if not new:
        return []
    for future in admin.create_topics(new).values():
        future.result()  # raises if the broker refused
    return [t.topic for t in new]


def main() -> None:
    from confluent_kafka.admin import AdminClient

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:19092")
    created = ensure_topics(AdminClient({"bootstrap.servers": bootstrap}))
    log.info("created: %s", ", ".join(created) if created else "none (all topics exist)")


if __name__ == "__main__":
    main()
