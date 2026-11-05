# ADR-001: Kafka (Redpanda) between the STOMP feed and Spark

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

The TRUST feed arrives over STOMP from Network Rail's broker. STOMP gives no replay: queued messages expire after 5 minutes and delivery is at least once (data-sources.md). Spark Structured Streaming needs a source it can re-read from a known position after a crash (offsets + checkpoints) to write exactly once into Iceberg. The spec requires Kafka here; this records why, and which Kafka-compatible broker.

## Options considered

1. **Spark reads STOMP directly** (custom source).
   - Pro: one service fewer.
   - Con: no replay; a Spark restart longer than 5 minutes loses data; feed speed and processing speed are coupled; no exactly-once recovery from offsets.
2. **Apache Kafka 4 (KRaft) + a separate Schema Registry** (Confluent, Karapace or Apicurio).
   - Pro: the reference implementation; Apache-2.0.
   - Con: two JVM services to run and size inside a ~7.6 GiB Docker budget.
3. **Redpanda** (Kafka API, built-in Schema Registry and HTTP proxy, single binary).
   - Pro: one container, lighter on memory, same Kafka client code (ports to MSK or Confluent unchanged); Schema Registry included.
   - Con: not Apache Kafka itself; community edition under the Business Source Licence (free to use, not to resell as a service).

## Decision

Put a Kafka-API broker between the bridge and Spark, and use **Redpanda** (single node, memory-capped) with its built-in Schema Registry. Topics: `raw.trust.movement` (key = `train_id`, so each train's events stay in order) and `dlq.trust.movement`. Kafka retention is days, not months: the long-term source of truth is the raw file archive.

## Consequences

- Spark can crash, restart or reprocess from offsets without touching the feed; replay of captured days uses the same topic.
- One more service to monitor (consumer lag is a first-class metric).
- If we ever needed features tied to Apache Kafka internals (e.g. Kafka Streams), we would revisit; nothing in the plan needs them.
