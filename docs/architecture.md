# Architecture

## 1. Purpose

Near-real-time delay visibility for a route control manager (brief.md): live train-movement events in, punctuality, delay hotspots, reactionary delay, cancellations and data health out, with freshness within minutes.

## 2. Overview

```
                  ALWAYS-ON CAPTURE (Oracle Cloud Always Free VM)
Network Rail ───► stomp_bridge (container) ───► raw archive: date=/hour= JSONL(.gz)
STOMP (TRUST)     durable subscription,              │  make pull-raw (SSH)
                  write+fsync, then ACK              ▼
─────────────────────────────────────────────────────────────────────────────────
                  LOCAL STACK (Docker Compose on the laptop)
raw archive / live bridge ──► Redpanda (Kafka API + Schema Registry)
                                 raw.trust.movement ─┬─► dlq.trust.movement
                                                     ▼
                              Spark Structured Streaming
                              validate · dedupe · watermark · enrich
                                                     ▼
          Iceberg on SeaweedFS (S3 API), via the Lakekeeper REST catalog (state in Postgres)
          bronze.trust_messages ─► silver.train_movements / activations / cancellations
                                   ref.locations (CORPUS) · ref.operators
                                                     ▼
                              dbt on DuckDB ─► gold: punctuality, hotspots,
                                               reactionary delay, cancellations
                                                     ▼
                              FastAPI + dashboard (DuckDB)

Batch: Airflow ─ reference data loads, Iceberg compaction and snapshot expiry
Observability: Prometheus + Grafana (lag, freshness, DLQ rate, file counts)
```

Details of each layer's schema: metrics.md (definitions) and data-sources.md (inputs).

## 3. Components

| Component | Role | Decision |
|---|---|---|
| STOMP bridge | Durable subscription; raw capture; (Day 3) produce to Kafka | findings F5, F6 |
| Redpanda | Buffer, replay, ordering per train, data contracts | ADR-001 |
| Spark Structured Streaming | Parse, validate, dedupe, watermark, enrich; exactly-once into Iceberg | spec |
| Iceberg + SeaweedFS | Lakehouse tables on S3-compatible storage | ADR-002, findings F7 |
| Lakekeeper (+ Postgres) | Shared Iceberg REST catalog | ADR-003 |
| dbt on DuckDB | Gold models | ADR-004 |
| Airflow | Reference data, Iceberg maintenance | spec |
| Prometheus + Grafana | Pipeline health | spec |

## 4. Deployment and memory

- **Capture host:** Oracle Cloud Always Free VM (1 GB RAM), one container; runs 24/7 independently of the laptop (development.md).
- **Laptop:** Docker Desktop with ~7.6 GiB. Compose profiles: `core` (Redpanda, SeaweedFS, Postgres, Lakekeeper) always on; `tools`, `airflow`, `monitoring`, `serving` on demand; per-service memory limits. Measured footprints are recorded in development.md.

## 5. Delivery guarantees

| Hop | Guarantee | How |
|---|---|---|
| Network Rail → bridge | At least once | Durable subscription; ACK after fsync (5-minute broker expiry: longer outages are gaps) |
| Bridge → Kafka | At least once | Idempotent producer, acks=all (Day 3) |
| Kafka → Iceberg (Spark) | Exactly once per Kafka offset | Checkpointed offsets + atomic Iceberg commits |
| End to end | Effectively once per real event | Silver dedupes redeliveries and applies corrections (metrics.md §1) |

## 6. Deviations from the original specification

The roadmap spec is the source of truth. Changes, with the reason and the date accepted:

| Spec | Change | Reason | Accepted |
|---|---|---|---|
| Everything runs on Compose locally (cloud only for an optional 48 h run) | The STOMP bridge runs 24/7 on an Oracle Cloud **Always Free** VM; processing stays local | A laptop cannot capture around the clock, and the feed has no history; £0 | 2026-11-05 |
| Reference data includes SMART | SMART is OPTIONAL | Only useful with the TD feed, which is not used | 2026-11-03 |
| Punctuality "by operator, route and hour" | By operator, location and hour | TRUST carries no route names; invented route groupings would be guesswork | 2026-11-05 |
| MinIO as local object storage | **SeaweedFS** (S3-compatible, Apache-2.0) | MinIO's community images are no longer published on Docker Hub or Quay, not even old tags (findings F7); every engine talks plain S3, so only one service and an endpoint changed | 2026-11-05 |
| Validate gold against official delay attribution | Pattern check against the newest period file, labelled as different dates | Attribution is published ~3 months late (findings F4) | 2026-11-05 |

Additions not in the spec (no change to spec scope): Redpanda Console in a `tools` profile (inspection), a generated "follow one train" walkthrough page, and the Lakekeeper catalog (the spec names no catalog; ADR-003).
