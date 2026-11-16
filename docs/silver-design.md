# Silver correctness design

Status: planned-call identity and bounded-state/durable-correction policy accepted
2026-11-16. Shared parsing is implemented;
the Spark silver job and runtime DLQ routing are now implemented (ADR-008).
Watermarked state and CORPUS enrichment remain pending. This document records the accepted direction; runtime checks remain required.

## Evidence before choosing state

Reproduce from the existing archive (no feed connection or publishing):

```powershell
uv run python -m scripts.profile_movements --day 2026-11-06 --day 2026-11-07 --day 2026-11-08 --output results/dev/2026-11-16-silver-profile/movement-profile.json
```

The generated JSON contains exact counts and quantiles. Development evidence,
not a throughput benchmark. It profiles 1,970,245 movements over a weekday and
two weekend days; presence of hour files is not proof of uninterrupted coverage.

- Receive-minus-event delay: p50 27 seconds; p95 96 seconds; p99 286 seconds;
  p99.9 32,707 seconds. The maximum is 602,022 seconds (almost seven days).
- 778 messages carry `correction_ind=true`. Of 661 candidate planned-call groups
  with both ordinary and correction messages, 474 contain multiple actual times.
  This demonstrates why mutable actual time cannot identify corrections.
- 14,072 movements arrive more than 15 minutes after their event time; a pure
  15-minute acceptance cutoff would lose real data.
- 105,905 movements have an event time more than a minute in the future relative
  to receipt. The furthest is 536 seconds. Keep this visible as a quality issue;
  do not let unchecked future timestamps drive watermark progress.

## Data flow and currently implemented boundary

```text
bronze payload + Kafka lineage
    --> trust.movements.parse_movement (implemented and unit-tested)
          +-- invalid --> Rejection (reason, path, rule)
          +-- valid non-0003 --> identified as another type; retained in bronze
          +-- 0003 --> typed movement dictionary

Implemented: Spark schema + durable rejects/DLQ + canonical silver/revision tables
Next: bounded state/late routing and reference joins
```

The parser reuses raw-contract validation and the existing DST conversion.
Timestamps are UTC datetimes; delay is signed integer minutes; booleans become
booleans; identifiers remain strings so leading zeros survive. Platform values
are trimmed. Kafka lineage and timestamp ambiguity flags accompany each row.
It does not deduplicate or claim to update corrections yet.

## Accepted event and revision identities

The original TASKS plan used actual timestamp in the natural key. Accepted change:
identify an on-route planned call by `(train_id, loc_stanox, planned_event_type,
planned_timestamp)` using the original source values. The planned timestamp
includes a date, avoiding cross-date train-ID collisions. Arrival and departure
must remain distinct. Apply a version only if its source queue timestamp is newer
than the stored version; preserve conflicting equal-time versions for investigation.
A new Kafka offset or replay receipt timestamp must not make an older revision win.

The profile's groups are candidates, not proof that this key uniquely identifies
every call. Before accepting it, inspect repeated visits, planned/location changes
and missing/off-route planned times. Off-route reports must not collapse into one
NULL-planned-time key. Keep their original reports and an explicit unresolved
identity flag until their update policy is supported. Do not infer train-identity
changes from timestamps alone. Replay at newer Kafka offsets must be harmless.

## Proposed watermark and late-data policy

Use an initial **15-minute state horizon**, subject to an actual streaming test.
This is a bounded-memory choice based on the distribution, not a declaration that
older data is invalid. The horizon must not become a permanent data-loss cutoff.
Spark drops state after the watermark and may discard older rows; it does not
provide a durable uniqueness constraint after state eviction.

The silver implementation therefore needs both:

1. Bounded live-stream duplicate state with guarded event times.
2. Durable idempotent correction/backfill handling, with explicit records/counts
   for late reports rather than silently losing them in the stateful operator.

The routing between these paths must be proven in Spark before marking task 6.2
complete. Replaying old days alongside live events must not move them behind a
live watermark and discard them. A separate bounded replay mode can bypass live
state while applying the same parsing, identity and revision-precedence rules.

Source for the distinction between watermark state eviction and persistent data:
[Spark 4.1.4 Structured Streaming guide](https://spark.apache.org/docs/4.1.4/streaming/apis-on-dataframes-and-datasets.html).

## Runtime acceptance checks still required

- Repeated replay and replay after state eviction do not double-count events.
- A changed actual timestamp updates the planned event, not an additional arrival.
- Older revisions replayed later do not undo newer corrections.
- Events behind the watermark and future/ambiguous timestamps are observable and
  follow the documented routing, including same-batch live/history mixtures.
- Invalid records reach the DLQ with original source topic/partition/offset;
  sink retries use that identity, since Iceberg and Kafka writes are not atomic
  together. No claim of exactly-once DLQ publication without proof.
- CORPUS has exactly one selected row per STANOX; a left join preserves unknown
  locations and the movement row count. Refresh/version behaviour is explicit.
- Restarting the silver processor preserves state and canonical results.

## Running the implemented processor

After `make up` and `make stream` have created bronze, run `make silver`.
`make silver-logs` shows batch counts; the Spark UI is at localhost:4041.
The job reads append snapshots of bronze and owns three Iceberg tables:
`silver.trust_movement` (current), `silver.movement_revisions` (history), and
`silver.rejected_messages` (durable rejects before Kafka publication).
Run only one processor per namespace/checkpoint. Valid non-movement TRUST
messages remain in bronze and are counted as `other`, not rejected.

Focused integration test: `uv run pytest -m integration -s tests/integration/test_silver.py`.
It creates isolated retained tables, a topic and a checkpoint; no deletes or kills.
The broader `make it` still includes the destructive bronze kill tests.
