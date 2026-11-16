# ADR-009: Guarded live watermark with durable bypass and versioned references

- **Status:** Accepted
- **Date:** 2026-11-16

## Context

Measured TRUST traffic includes reports days late, corrections and future times.
A watermark over all input would discard real history or let future events
advance the cutoff. CORPUS also has duplicate STANOX keys, which can multiply
movement rows in a naive join. Silver's original 1.5 GiB cap left too little
headroom during the real bronze reread (sampled maximum 1.462 GiB).

## Options considered

1. Apply a watermark to all input: simple but loses history and corrections.
2. Keep every revision in streaming state forever: unbounded memory.
3. Guard a bounded live branch and bypass it for other reports, sharing durable
   revision/current MERGEs. Select one versioned CORPUS row per STANOX.

## Decision

Choose option 3. Only recent ordinary live events at or before the trusted Spark
batch clock enter the 15-minute watermark. Admission uses half that horizon as
an age guard. Other routes bypass state entirely. Both branches reach the same
idempotent sink; no watermark acceptance cutoff is applied to history.

Load immutable CORPUS versions and broadcast a deterministic one-row-per-STANOX
snapshot in each batch. Preserve unknown codes and flag their unmatched join.
Use disk-backed batch materialization, a 768 MiB heap and two shuffle partitions
for the local silver container; raise its cap to 2 GiB inside the existing
~7.6 GiB Docker budget. Core, tools, bronze and silver limits sum to 6 GiB;
future Airflow/monitoring profiles still need their own memory checks.

## Consequences

- State is bounded for live duplicate suppression; durable tables supply lasting
  uniqueness and correction precedence. Monitor state metrics and route counts.
- The event-time watermark stalls when live events stop. The admission guard
  assumes a nondecreasing host/batch clock.
- Reference hashes preserve provenance. Loading a reference does not refresh all
  old rows; processing or replay refreshes affected current rows.
- Disk caching trades memory pressure for local I/O. The reduced heap did not
  eliminate JVM/native overhead: the sampled maximum increased from the earlier
  1.398 GiB observation to 1.462 GiB with the new plan. Larger backlogs still need
  measurement and eventual batch-size controls; this is no capacity benchmark.
- The new plan needs checkpoint v2. Preserve v1 and reread append-only bronze;
  idempotent sinks handle repetition. No concurrent silver writers.
- Focused retained-data scenarios verify state eviction, duplicate suppression,
  late corrections, replay after eviction, a future-clock guard and reference
  refresh. Abrupt-kill testing remains scheduled for Day 13.
