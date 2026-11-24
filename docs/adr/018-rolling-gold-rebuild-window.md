# ADR-018: Gold rebuilds a rolling 3-day window, and is compacted nightly

- **Status:** Accepted
- **Date:** 2026-11-24

## Context

ADR-017 rebuilt every gold table in full each time: `DELETE` every row, then `INSERT` the new result. Scheduled every 15 minutes (Day 10), every build from 04:45 UTC on 2026-11-24 failed: the Airflow container hit its 3 GB limit and the kernel killed processes inside it (findings F32). Measured:

- DuckDB's `DELETE` on Iceberg writes **delete files**; it never drops the old data files, even when every row in them is deleted. After about 90 builds a day with no gold compaction, `gold.delay_hotspots` held **79.4 M rows in 600 data files and 78.3 M in 599 delete files, for 1.15 M live rows**. Every read and every delete had to load ~78 M delete positions.
- A dbt run of `delay_hotspots` alone: 246 s, peaking at the 3 GiB limit. `reactionary_delay`, which uses the same per-report model but writes a far smaller table, took 21 s.
- Recomputing all of silver every time also grows with silver (~780 k rows a day).

## Decision

1. **Rolling window.** Each build recomputes only the last **3 UK local dates** (today and the two before, enough for late corrections) and replaces exactly those rows: `DELETE … WHERE local_date >= <start>` then `INSERT`, still in one transaction (`transform/macros/rebuild_window.sql`, `rebuild_window_column: local_date` on the gold models). Older dates are final. Silver is read only for the window plus the days `lag()` and midnight-crossing reactionary clusters need, filtered on `actual_ts`, so Iceberg skips old partitions.
2. **History in chunks.** `make gold-backfill FROM=YYYY-MM-DD` rebuilds past dates 7 days per dbt run (`--vars {"rebuild_from":…,"rebuild_to":…}`), so even a full rebuild never needs memory proportional to all history. `--full-refresh` (drop and re-create, for a changed column list) creates the table from the window, then a backfill restores history.
3. **Gold is compacted every night**, whole (the tables are small and unpartitioned), with `delete-file-threshold = 1` so every delete file is folded in; 12 hours of snapshots.
4. **No gold build during 03:00–03:59 UTC** (`*/15 0-2,4-23 * * *`): maintenance compacts gold then, and a concurrent commit would make one of them fail and retry.
5. The reconciliation tests compare inside the window and only events received up to the newest one gold saw (`latest_received_at`), because a percentage tolerance over a 3-day window is smaller than the data arriving during a build.

## Alternatives rejected

- **Raise the container limit.** Postpones the same failure: both the delete files and silver keep growing.
- **dbt `incremental` materialization.** dbt-duckdb's incremental strategies use `MERGE`/temporary relations that DuckDB-Iceberg does not support (same root as ADR-017); the window inside our own materialization keeps one tested code path.
- **Partitioning gold by date and replacing partitions.** DuckDB-Iceberg has no partition overwrite; it would still write delete files.

## Consequences

- Measured after a one-off compaction (hotspots 1,199 files / 1.46 GB → 2 files / 11.2 MB): full `dbt build` **151 s, peak 1.56 GB, 31/31 tests**; `delay_hotspots` 21 s (was 246 s). First scheduled run 3.5 min.
- Delete files still accumulate between nightly compactions (each build deletes 3 days of rows); the last builds before 03:00 are the worst case and are checked on 2026-11-25 (F32).
- A correction to silver older than 3 days no longer reaches gold until a backfill covers that date. Late corrections that old are rare (silver's late-event handling, ADR-011).
- Gold rows for a date older than the window keep the definition they were built with; after a metric change, run a backfill.
