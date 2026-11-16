# Silver correctness design

Status: planned-call identity and bounded-state/durable-correction policy accepted
2026-11-16. Parsing, revision-aware silver writes, durable rejects, guarded
watermark state and versioned CORPUS enrichment are implemented (ADRs 008/009).
Focused integration checks cover corrections, retries, checkpoint restarts,
state eviction and reference refresh. Abrupt-kill testing remains Day 13 work.

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

0003 --> classify by processing route
          +-- recent live --> 15-minute watermark + revision dedupe
          +-- replay / correction / late / future / timestamp_review --> bypass state
       --> versioned CORPUS left join --> revision/current MERGEs
invalid --> durable reject MERGE --> Kafka DLQ (at least once)
```

The parser reuses raw-contract validation and the existing DST conversion.
Timestamps are UTC datetimes; delay is signed integer minutes; booleans become
booleans; identifiers remain strings so leading zeros survive. Platform values
are trimmed. Kafka lineage and timestamp ambiguity flags accompany each row.
The streaming sink applies deduplication and correction precedence separately.

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

## Watermark and late-data policy

Use an initial **15-minute state horizon**, verified with a smaller synthetic
horizon in the real Spark state operator to exercise eviction.
This is a bounded-memory choice based on the distribution, not a declaration that
older data is invalid. The horizon must not become a permanent data-loss cutoff.
Spark drops state after the watermark and may discard older rows; it does not
provide a durable uniqueness constraint after state eviction.

The silver implementation uses both:

1. Bounded live-stream duplicate state with guarded event times.
2. Durable idempotent correction/backfill handling, with explicit records/counts
   for late reports rather than silently losing them in the stateful operator.

Only ordinary live reports whose actual time lies between the Spark micro-batch
clock minus half the horizon (7.5 minutes) and that clock enter state. The margin
keeps admitted reports ahead of the previous batch's watermark. The clock is
Spark's retry-stable batch timestamp, not an unchecked source timestamp. This
assumes a nondecreasing processing clock; large backward host-clock jumps require
operator investigation.

Replay, corrections and timestamp-review rows bypass state first. Remaining
future or older reports also bypass it, with `processing_route` retained in silver
and route counts logged per batch. Both branches share the same durable sink.
Bypassing state preserves old events even when mixed with current live data.
Future events cannot advance the live watermark. A watermark advances with live
event time, not an idle wall clock: old state can remain during inactivity but
cannot grow without new admitted events. Permanent dedupe rests on Iceberg MERGE,
including replay after state eviction.

Spark's `numInputRows` counts both scans in this union query and can double the
physical input count. Use the classified status/route counts for business input
volumes; the generic progress listener's row rate is not unique-message throughput.

Source for the distinction between watermark state eviction and persistent data:
[Spark 4.1.4 Structured Streaming guide](https://spark.apache.org/docs/4.1.4/streaming/apis-on-dataframes-and-datasets.html).

## Runtime acceptance checks

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

These are exercised by `tests/integration/test_silver.py`; generated summaries
are in `results/dev/2026-11-16-silver-sink/` and
`results/dev/2026-11-16-silver-state/`. The state scenario restarts the same
checkpoint across synthetic event-time jumps. This proves state behavior, not
performance or abrupt-kill recovery.

## CORPUS versions and unknown stations

`make load-corpus` selects one row per STANOX, preferring a nonempty CRS, then
name, then TIPLOC, with a canonical lexical tie-break. It hashes uncompressed
content, loads immutable `(stanox, reference_version)` rows into `ref.locations`,
and treats repeating the same file as a no-op. The most recently loaded version
is selected at each micro-batch. A broadcast left join preserves every movement;
unmatched codes retain their STANOX, null names, `location_matched=false` and the
selected `location_reference_version`.

Reference refresh does not rewrite the entire silver table automatically.
New events or replay refresh affected current rows; existing revision history
retains the enrichment originally stored. Replaying an unchanged revision after
a reference update refreshes its current row without increasing its row count.

## Running the implemented processor

After `make up` and `make stream` have created bronze, build the Spark image,
run `make load-corpus`, then `make silver`.
`make silver-logs` shows batch counts; the Spark UI is at localhost:4041.
The job reads append snapshots of bronze and owns three Iceberg tables:
`silver.trust_movement` (current), `silver.movement_revisions` (history), and
`silver.rejected_messages` (durable rejects before Kafka publication).
Run only one processor per namespace/checkpoint. Valid non-movement TRUST
messages remain in bronze and are counted as `other`, not rejected.

The new union/state query uses `/checkpoints/silver_trust_movement_v2`. The original
v1 checkpoint remains intact: its stateless query plan is incompatible with this
stateful plan. On this one-time transition, v2 rereads retained bronze; durable
MERGEs preserve current/revision identity and populate current station names.
Previously published rejects may be republished with their stable keys. Never
delete checkpoints as a routine recovery method.

Focused integration test: `uv run pytest -m integration -s tests/integration/test_silver.py`.
It creates isolated retained tables, a topic and a checkpoint; no deletes or kills.
The broader `make it` still includes the destructive bronze kill tests.
