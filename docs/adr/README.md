# Architecture Decision Records

An ADR records **one** significant decision: the context, the options considered, the choice and its consequences. One is written only when a decision is actually made, not in advance.

## Index

| # | Decision | Status |
|---|---|---|
| [001](001-kafka-between-feed-and-spark.md) | A Kafka-API broker (Redpanda, with its built-in Schema Registry) between the STOMP feed and Spark, for replay and exactly-once recovery | Accepted (2026-11-05) |
| [002](002-iceberg-table-format.md) | Apache Iceberg for all lakehouse tables: atomic commits, schema evolution, time travel, compaction | Accepted (2026-11-05) |
| [003](003-iceberg-catalog.md) | An Iceberg REST catalog (Lakekeeper, on the shared Postgres) used by every engine | Accepted (2026-11-05) |
| [004](004-dbt-adapter-and-gold-storage.md) | dbt on DuckDB; gold written to Iceberg if a Day 9 spike proves it, otherwise to DuckDB/Parquet | Accepted (2026-11-05) |
| [005](005-disk-before-kafka.md) | The live bridge writes to disk before acknowledging, then publishes to Kafka asynchronously; in a cloud deployment with replicated Kafka it would acknowledge on Kafka delivery | Accepted (2026-11-07) |
| [006](006-schema-compatibility-mode.md) | BACKWARD_TRANSITIVE for every contract (the archive is replayed from day one), plus a required-field guard where the registry's JSON Schema check is blind | Accepted (2026-11-09) |
| [007](007-spark-runtime-and-bronze-layout.md) | Spark 4.1.4 + Iceberg 1.12.0 in our own Python 3.12 image; checkpoints on a Docker volume; bronze partitioned by ingestion day, one file per batch | Accepted (2026-11-09) |

Smaller decisions and accepted deviations from the spec are listed in [architecture.md §6](../architecture.md#6-deviations-from-the-original-specification); what the build revealed is in [findings.md](../findings.md).

## Template

Copy this to `NNN-short-title.md`:

```markdown
# ADR-NNN: <title>

- **Status:** Proposed | Accepted | Superseded by ADR-XXX
- **Date:** YYYY-MM-DD

## Context
What problem forces a decision? What constraints apply?

## Options considered
1. Option A: pros / cons
2. Option B: pros / cons

## Decision
What we chose, in one or two sentences.

## Consequences
What becomes easier, what becomes harder, what we must monitor.
How we would know this decision was wrong.
```
