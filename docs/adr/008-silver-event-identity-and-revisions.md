# ADR-008: Planned-call identity with durable revision-aware silver writes

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

Replayed events receive new Kafka offsets. In the three-day profile, 474 candidate
planned-call groups with ordinary and correction reports contained different
actual timestamps. Neither a Kafka offset nor actual time is a stable correction
key. Some reports also arrive days late, beyond practical streaming state limits.

## Options considered

1. Dedupe by train/location/actual time: a changed actual time becomes an extra
   event instead of updating the planned call.
2. Keep every report as an arrival: preserves history but double-counts replay
   and revisions in gold.
3. Persist distinct source revisions and maintain a current planned-call table
   through repeatable Iceberg MERGE operations.

## Decision

Option 3. On-route call identity hashes train ID, location, planned event type and
the original planned timestamp. The timestamp includes the date, and different
planned visits stay distinct. Actual time is mutable. Revision identity hashes
the call identity, source queue timestamp and canonicalized body, excluding Kafka
offsets and bridge receive times. Replaying a report therefore keeps its identity.

`silver.movement_revisions` retains distinct revisions. `silver.trust_movement`
holds the newest source queue timestamp per event. Equal-time different bodies
choose a deterministic revision-hash order and set `revision_conflict=true`; their
full revisions remain available. A newer source version clears the current
conflict. Gold must expose or exclude unresolved conflicts explicitly.

Off-route calls lack reliable planned identity. Keep distinct bodies with
`identity_unresolved=true`; do not combine every NULL planned time into one key.
Changes to plan/location/train identity remain a documented limitation: no
unsupported correlation across those changes is inferred.

## Consequences

- Per-batch duplicate removal plus durable revision/current MERGE operations
  make repeated input processing idempotent, including old replay after updates.
- `silver.rejected_messages` stores rejects keyed by source topic/partition/offset
  identity before Kafka publication. Publication is at least once: a retry may
  emit another record with the same key. Count distinct reject identities for
  data-quality rates, not raw DLQ copies. A source topic's lifecycle must be
  preserved; recreating it at offset zero requires a new lineage generation.
- The writes are not one cross-system transaction. A failed micro-batch is retried;
  durable merges tolerate repetition and Kafka sinks may redeliver.
- Exactly one processor/checkpoint owns a silver namespace. Concurrent independent
  writers and mutation of the source bronze history are not supported here.
- This establishes durable correctness. ADR-009 subsequently adds guarded
  watermarked state and CORPUS enrichment; history bypasses the live watermark.
- Verification: the isolated Spark/Kafka scenario updates one call through four
  revisions, restarts its stream checkpoint three times, flags an equal-time
  conflict, preserves newer data during old replay and recovers an injected reject
  publication failure. See the generated summary under `results/dev/`.
