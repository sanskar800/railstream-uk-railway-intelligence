# ADR-016: Merge-on-read for silver's current-call table; per-table snapshot retention

- **Status:** Accepted
- **Date:** 2026-11-23

## Context

`silver.trust_movement` holds one row per planned call and is updated every minute by a MERGE (new calls inserted, corrections applied, ADR-008). Iceberg's default write mode for MERGE is **copy-on-write**: any data file containing a matched row is rewritten in full. Corrections and repeated reports touch today's partition constantly, so its file (tens of MB) was rewritten about 1,440 times a day. Every rewritten version stayed referenced by a snapshot until expiry, and expiry kept 3 days on every table. On 2026-11-22 the processing VM's 145 GB disk filled (findings F28): SeaweedFS reached its volume limit, Kafka stopped accepting messages, and bronze, silver and Airflow failed.

## Options considered

1. **Copy-on-write, shorter retention only:** less retained garbage, but still ~40 GB/day written and churned through storage.
2. **Merge-on-read** (`write.merge.mode`, `write.update.mode`, `write.delete.mode` = `merge-on-read`, format v2): a MERGE writes the changed rows as new files plus small position-delete files; readers apply the deletes; compaction later rewrites data files with their deletes applied.
3. **Append-only silver with a view for "current":** avoids MERGE entirely but moves deduplication and precedence into every reader.

## Decision

Option 2, plus per-table retention:

- `silver.trust_movement` is merge-on-read (set at creation and by an `ALTER` at every silver start). Nightly compaction uses `delete-file-threshold = 1`, so any data file with deletes is rewritten with them applied (finished partitions, ADR-007/Day 8).
- Snapshot retention per table: **bronze 3 days** (silver's checkpoint records the bronze snapshot it has reached, so those must survive any silver outage); **silver 12 hours** (nothing streams from silver; old versions are only storage). At least 10 snapshots are always kept.
- `silver.movement_revisions` keeps copy-on-write: its MERGE only inserts, which Spark plans as an append.

## Consequences

- Writes per batch are proportional to the rows changed, not to the size of the files they live in.
- Reads of recent partitions apply delete files until nightly compaction folds them in; acceptable at this scale, and measurable (Day 13).
- Verified before going live: silver integration scenarios (corrections, late corrections, replay, watermark, enrichment) and the maintenance scenario passed with merge-on-read (3 passed).
- Wrong if: delete files on today's partition slow gold reads noticeably before compaction, in which case compaction of recent partitions moves to a more frequent schedule.
- Detection: the disk is now a published metric with an alarm at 80 % (ADR-014's job, findings F28).
