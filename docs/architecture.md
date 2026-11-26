# Architecture

## 1. Purpose

Near-real-time delay visibility for a route control manager (brief.md): live train-movement events in, punctuality, delay hotspots, reactionary delay, cancellations and data health out, with freshness within minutes.

## 2. Overview

```
              CAPTURE VM (Oracle Always Free, 1 GB, AD-3) — runs 24/7
Network Rail ───► stomp_bridge (container) ──┬──► raw archive: date=/hour= JSONL(.gz)  (source of truth)
STOMP (TRUST)     durable subscription,      │       └─ hourly, append-only ──► Object Storage bucket (ADR-013)
                  write+fsync, then ACK      │
                                             └──► private network (10.0.0.0/24, TCP 29092 only) ──┐
─────────────────────────────────────────────────────────────────────────────────────────────────┼──
              PROCESSING VM (Oracle Always Free, ARM, 2 OCPU / 12 GB, AD-1) — Docker Compose  ◄───┘
Redpanda (Kafka API + Schema Registry): raw.trust.movement ─┬─► dlq.trust.movement
                                                            ▼
Spark Structured Streaming: bronze (raw + lineage) → silver (validate · dedupe · watermark · enrich)
                                                            ▼
Iceberg on SeaweedFS (S3 API), via the Lakekeeper REST catalog (state in Postgres)
  bronze.trust_movement → silver.trust_movement (current call) · silver.movement_revisions (history)
                          · silver.rejected_messages ;  ref.locations (CORPUS, versioned)
                                                            ▼
dbt on DuckDB → gold: punctuality, hotspots, reactionary delay   (planned, Days 9–10)
                                                            ▼
FastAPI + dashboard (DuckDB)                                     (planned, Day 12)

Batch (Airflow on the same VM): CORPUS refresh (Mon/Thu); later Iceberg maintenance, dbt, quality checks
Monitoring: feed_age_seconds every minute → Oracle Monitoring alarms → email (ADR-014);
            Prometheus + Grafana for pipeline internals (planned, Day 12)
Laptop: code only (commit, push; make tunnel / make sql to look)
```

Details of each layer's schema: metrics.md (definitions) and data-sources.md (inputs).

Current implementation (2026-11-17): capture, bronze and revision-aware silver run on the processing VM, fed by the capture VM's single bridge (ADR-012); the tables were rebuilt there by replaying the archive (F23). Silver retains distinct revisions and current planned calls (ADR-008); Kafka DLQ publication is at least once with stable source keys. Guarded watermarked state and a versioned CORPUS left join are implemented (ADR-009). Airflow runs the twice-weekly CORPUS download/validate/load workflow (ADRs 010/011). The raw archive is backed up hourly off the VM (ADR-013), and ingestion freshness is alarmed (ADR-014). Gold and serving remain planned. Activations and cancellations remain in bronze.

## 3. Components

| Component | Where | Role | Decision |
|---|---|---|---|
| STOMP bridge | Capture VM | Durable subscription; raw capture (disk, then ACK); publishes to Kafka over the private network | findings F5, F6; ADR-005, ADR-012 |
| Archive backup | Capture VM → Object Storage | Hourly, verified, append-only copy of completed hours | ADR-013 |
| Redpanda | Processing VM | Buffer, replay, ordering per train, data contracts | ADR-001 |
| Spark Structured Streaming | Processing VM | Parse, validate, dedupe, watermark, enrich; exactly-once into Iceberg | spec; ADR-007 |
| Iceberg + SeaweedFS | Processing VM | Lakehouse tables on S3-compatible storage | ADR-002, findings F7 |
| Lakekeeper (+ Postgres) | Processing VM | Shared Iceberg REST catalog; signs every storage request | ADR-003 |
| dbt on DuckDB | Processing VM (planned) | Gold models | ADR-004 |
| Airflow | Processing VM | Reference data now; Iceberg maintenance, dbt, quality checks later | spec; ADR-010 |
| Freshness alarm | Processing VM → Oracle Monitoring | `feed_age_seconds` + absence alarm, email | ADR-014 |
| Prometheus + Grafana | Processing VM (planned) | Pipeline internals: lag, batch duration, file counts | spec |

## 4. Deployment and memory

- **Capture VM** (`railstream-capture`): VM.Standard.E2.1.Micro, 1 GB, x86, AD-3. The bridge container (256 MB limit) and the backup timer. Independent of the processing VM: if that VM is down, capture and the archive continue and the gap is replayed later.
- **Processing VM** (`railstream-core`): VM.Standard.A1.Flex, 2 OCPU / 12 GB, ARM, AD-1, 150 GB. The whole Compose stack with per-service memory limits; profiles `stream` (Spark) and `airflow`, `tools` (Console) on demand. Measured 2026-11-17 during the catch-up: ~4.6 GB of 11 GiB used; silver 1.4 GB of its 3 GB limit after F23.
- **Access:** only SSH from outside; Kafka's private listener only from the subnet; UIs on the VM's localhost, reached with `make tunnel`.
- **Laptop:** development and tests only (the same Compose stack runs locally for `make it`); not in the data path.
- **Failure model:** the processing VM is a single point of failure for availability, not for data (everything on it is derived and rebuildable from the archive); capture is protected by the backup (history) and alarms (detection). A second collector is a possible later step.

## 5. Delivery guarantees

| Hop | Guarantee | How |
|---|---|---|
| Network Rail → bridge | At least once | Durable subscription; ACK after fsync (5-minute broker expiry: longer outages are gaps) |
| Bridge → Kafka | At least once | After the ACK, asynchronously (ADR-005); idempotent producer with acks=all, so producer retries add no duplicates; undelivered windows are logged and replayed from disk (F12) |
| Kafka → Iceberg (Spark) | Exactly once per Kafka offset | Checkpointed offsets + atomic Iceberg commits |
| End to end | Effectively once per real event | Silver dedupes redeliveries and applies corrections (metrics.md §1) |

## 6. Deviations from the original specification

The roadmap spec is the source of truth. Changes, with the reason and the date accepted:

| Spec | Change | Reason | Accepted |
|---|---|---|---|
| Everything runs on Compose locally (cloud only for an optional 48 h run) | The STOMP bridge runs 24/7 on an Oracle Cloud **Always Free** VM; processing stays local | A laptop cannot capture around the clock, and the feed has no history; £0 | 2026-11-05 |
| Processing on the laptop (Compose) | The whole Compose stack runs on a second Always Free VM (Ampere A1, 2 OCPU / 12 GB, ARM); the capture VM's bridge also publishes to its Kafka over the private network; no laptop in the data path (ADR-012) | User requirement (no laptop in the middle); £0 indefinitely, unlike trial credits | 2026-11-17 |
| Reference data includes SMART | SMART is OPTIONAL | Only useful with the TD feed, which is not used | 2026-11-03 |
| Punctuality "by operator, route and hour" | By operator, location and hour | TRUST carries no route names; invented route groupings would be guesswork | 2026-11-05 |
| MinIO as local object storage | **SeaweedFS** (S3-compatible, Apache-2.0) | MinIO's community images are no longer published on Docker Hub or Quay, not even old tags (findings F7); every engine talks plain S3, so only one service and an endpoint changed | 2026-11-05 |
| Avro or Protobuf schemas in the registry | **JSON Schema** in the registry (Redpanda supports it, with compatibility checks) | The raw topic keeps the feed's original JSON byte for byte (replay, bronze); a registered Avro schema would not describe the bytes on the topic, and re-encoding at the bridge would lose fidelity. Compatibility checking needed one addition (findings F15, ADR-006) | 2026-11-09 |
| Validate gold against official delay attribution | Pattern check against the newest period file, labelled as different dates | Attribution is published ~3 months late (findings F4) | 2026-11-05 |
| Soda checks for data quality | Plain SQL checks run daily by Airflow through Spark Connect, results kept in `ops.quality_checks` (`orchestration/quality.py`) | Same function (named checks, thresholds, history) with no new tool, check language or connector in the Airflow image, whose dependency set is pinned (findings F37) | 2026-11-26 |
| FastAPI endpoints over gold + basic dashboard; Prometheus + Grafana | The dashboard becomes a product (Network Pulse, live UK map, train journeys with a silver drill-down, rankings, Data Explorer with read-only SQL behind a login); Prometheus + Grafana dropped in favour of the app's status page and Oracle Monitoring alarms; NaPTAN rail stations (OGL) added for coordinates | User requirement (a product for stakeholders, passengers and developers, to be hosted publicly); Grafana would duplicate existing alerting and cost RAM on the 2-OCPU VM; CORPUS has no coordinates | 2026-11-26 |

Additions not in the spec (no change to spec scope): Redpanda Console in a `tools` profile (inspection), a generated "follow one train" walkthrough page, and the Lakekeeper catalog (the spec names no catalog; ADR-003).
