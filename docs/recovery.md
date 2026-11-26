# Recovery and replay

Updated 2026-11-16. These procedures preserve existing raw data, tables and
checkpoints. Changes to running services or database state require explicit
approval; the handover review did not perform a recovery on live data.

## Capture restart

**Outages over 5 minutes lose data.** The broker holds messages for a disconnected durable subscriber for 300 s only (the bridge logs `DATA GAP: … messages older than 300s expired`; measured redelivery delay up to 302 s). TRUST cannot be replayed, so whatever expires is gone. Keep planned bridge restarts under 4 minutes; the feed-stale alarm fires after 3-4 minutes (ADR-014). Known gap: 2026-11-26 13:38:15-13:42:16 UTC (~2,800 messages), caused by a deliberate 9-minute test (findings F36).

**Storage outage (SeaweedFS down):** nothing to do while it lasts. Kafka keeps receiving (7-day retention); bronze and silver fail, Docker restarts them, and they resume from their checkpoints once storage is back. Tested 2026-11-26 (F36).

The capture directory must have a single writer. Before compressing closed
hours, reopening the current hour or reading the last receive time, the bridge
checks the plain JSONL file:

- Complete records are preserved. A valid last record missing only its newline
  gets a newline before another record can be appended.
- An invalid final record without a newline is treated as an interrupted append.
  Its exact bytes are written to a unique `.damaged-<id>` file and fsynced before
  the source is truncated back to its last complete record. Recovery is logged.
- Newline-terminated or interior corruption is never repaired automatically. The
  whole file is renamed, byte for byte, to `hour=HH.jsonl.corrupt-<id>` and a
  `CAPTURE FILE QUARANTINED` error is logged; **capture continues** (a fresh file is
  started if it was the current hour). Stopping the bridge would lose the live feed,
  which matters more than one damaged hour kept for inspection. Inspect a copy
  before repairing it; until then that hour is missing to `pull-raw` and replay,
  which refuses windows with missing hours, so the gap stays visible.
- Only `.jsonl` and `.jsonl.gz` are capture inputs. `.tmp`, `.damaged-*` and
  `.corrupt-*` files are excluded from last-frame lookup, compression and replay.

This repairs interrupted appends; it is not a guarantee against disk loss or
filesystem corruption. Unacknowledged frames can be redelivered only within the
upstream retention period. A recovered tail does not prove there was no feed gap.

If plain and compressed copies coexist, replay reads the **plain copy only**.
Compression replaces the gzip copy before unlinking the plain source; the plain
file is therefore authoritative while it exists, and may contain later appends.
Replay does not repair or delete inputs. A corrupt input fails replay and needs
inspection or capture-owner recovery before retrying. Do not run capture-file
repair against a directory currently being written by another process.

## Replay completion

`make replay` preserves the existing DAY / FROM / TO and SPEED interface. Before
connecting to Kafka, it checks that every UTC hour overlapping the requested
window has an input file. It does not infer complete feed coverage from file
existence: a partial first/last hour or an upstream outage can still leave gaps.

Exit status:

| Status | Meaning |
|---|---|
| 0 | At least one message replayed; no skipped frames; every intended message confirmed delivered |
| 1 | Missing hour, unreadable/corrupt input, skipped malformed frame, empty message window, failed or unconfirmed deliveries |
| 2 | Invalid command-line arguments |
| 130 | User interruption observed during replay |

Already queued records are flushed even when reading input fails. A failed run
may have delivered some messages; it is not a rollback. Retrying republishes
those events at new offsets. Do not use successful process exit as proof of a
complete railway day, and do not automate retries into gold until silver's
event-level deduplication/correction rules are in place.

## Kafka retention versus Spark checkpoints

Spark's checkpoint records source offsets and query identity; Iceberg records
committed epochs for that identity. Restart with the same intact checkpoint to
recover ordinary interrupted batches. Do not delete or edit checkpoint files.

If Kafka has expired an unread offset, archive replay does **not** restore that
offset: newly published records receive higher offsets. Keep `failOnDataLoss=true`
so the pipeline does not silently skip the gap. Repeated automatic restarts alone
cannot fix this situation.

Recovery plan to review before any service/data changes:

1. Record the failing topic/partition/offset, Kafka retained ranges, checkpoint
   identity, and the existing Iceberg snapshot/table location. Preserve the
   checkpoint, table and raw archive. Account for topic recreation as well as
   normal retention; offsets are not globally unique across topic generations.
2. Verify which archive windows exist and identify genuine capture gaps. A missing
   source event cannot be recreated by changing a Spark option.
3. Prefer a separate recovery topic, table and checkpoint generation. Replay the
   verified archive there and audit expected records and offsets. Preserve the
   original generation as evidence; avoid double-counting when reconciling any
   original bronze rows not represented in the archive.
4. Agree a live cutover boundary and reconcile overlaps using silver's durable
   event identity/correction policy. Validate counts and coverage before moving
   consumers. Silver and this cutover automation are not implemented yet.

Until the silver recovery policy exists, stop and inspect an expired-offset error;
do not disable data-loss checking or reset offsets just to make the job green.

## Verification

Unit tests use temporary directories and fake Kafka clients. They cover partial
UTF-8/JSON tails, reopening/compressing recovered files, missing newlines, interior
corruption, temporary-file exclusion, duplicate hour representations, missing-hour
preflight, and replay exit statuses after failed/partial delivery or interruption.

The integration kill test now requires Docker's kill command to succeed and the
job to exit unsuccessfully. It waits for actual progress before a kill. A random
delay does not prove the exact commit/checkpoint race was hit. This revised
service-mutating test has not been run during recovery hardening; the historical
Day 5 results remain in `results/dev/2026-11-09-bronze-kill-test/`.
