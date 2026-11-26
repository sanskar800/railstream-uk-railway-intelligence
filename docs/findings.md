# Findings

## Recovery follow-up (2026-11-16)

The migration review reproduced two failure cases using temporary files: a partial
last append raised JSONDecodeError during last-frame lookup, and replay read a
plain hour plus its compressed copy twice. The fixes now quarantine an incomplete
tail before truncation, preserve valid records missing only a newline, ignore
compression temporary files, and select the plain hour while it exists. Corruption
inside complete records remains an explicit error. Replay now preflights missing
hours and returns nonzero for skipped input, empty windows or incomplete delivery.
See `recovery.md` and the regression tests in `tests/unit/test_capture.py`,
`test_replay.py` and `test_replay_cli.py`.

The historical kill-test evidence below is unchanged. Its random delay does not
guarantee hitting a commit boundary. The revised test checks that Docker actually
accepted each kill and that the interrupted job did not exit successfully; this
destructive integration test was not rerun in the follow-up.

Notable observations made while building, with the evidence behind each number. Development evidence, **not benchmark results**.

## F1. The movement feed batches messages into frames (2026-11-03)

A 60 s subscription to `TRAIN_MVT_ALL_TOC` starting 18:51:56 UTC (Tuesday, evening peak) received **29 STOMP frames carrying 784 TRUST messages** (≈27 messages per frame; 13.1 messages/s in this window): 754 movements (`0003`), 27 activations (`0001`), 3 cancellations (`0002`), from 36 distinct `toc_id` values.

- Consequence: a frame body is a JSON **array**; the bridge must split frames into messages, and Kafka message counts ≠ STOMP frame counts.
- Not a volume estimate: one minute at peak cannot be scaled to a day. Daily volume comes from the continuous capture.
- Evidence: `results/dev/2026-11-03-feed-minute/frames.jsonl.gz` (one line per frame: `received_at`, broker `headers`, raw `body` string).

## F2. Events arrive late, a few very late, and some "from the future" (2026-11-03)

For the 754 movements in F1, receive time minus `actual_timestamp`: **min −219 s, p50 23 s, p95 92 s, max 2,517 s** (≈42 min).

- Consequence: processing must use event time with a watermark and a documented policy for events beyond it. The watermark will be chosen from the lateness distribution of a full day, not from this minute.
- **Update 2026-11-05:** 43 movements were stamped more than 60 s after receipt, and **all 43 are `AUTOMATIC` reports from `SMART`**, so manual entry is ruled out as the cause. Minute rounding explains up to ~60 s, not 219 s. Working hypothesis (unproven): automatic reports are generated when a train enters a signalling berth and stamped with the berth's SMART offset, i.e. the expected platform time. Consequence: the quality rule must be "not more than X minutes in the future", with X from a full day, not "no future timestamps".
- Resolved from the documentation (2026-11-05, data-sources.md): during BST the event timestamps are one hour in the future (UK local wall-clock encoded as UTC); in November they equal UTC, which is why this sample looked correct. TRUST also rounds to whole minutes, which explains small negative lags but not −219 s.
- Evidence: as F1.

## F3. CORPUS joins cleanly on STANOX but is not unique on it (2026-11-03)

`CORPUSExtract.json.gz` (download of 2026-11-03; file dated 2026-10-02): **55,981 rows**, 11,621 with a STANOX, **11,118 distinct STANOX, of which 356 map to more than one row**. All 757 location-bearing messages in F1 matched a STANOX.

- Consequence: a naive join on STANOX would duplicate movement events. Enrichment needs a deterministic choice of one row per STANOX and a test that the join preserves row count.
- Locations include junctions and freight points without a 3-letter code (e.g. Copyhold Junction); these matter for delay hotspots.
- Evidence: CORPUS is re-downloadable (development.md §2); not committed.

## F4. Official delay attribution is published about 3 months late, per railway period (2026-11-05)

The Rail Data Marketplace "NWR Historic Delay Attribution" product publishes one zip per railway period (13 four-week periods per financial year), named like `Transparency 26-27 P05 20261105.zip`. On 2026-11-05 the newest data was **2026/27 period 5** (summer 2026), published 2026-11-05. Periods are re-issued with corrections (P05 on 2026-10-07 and 2026-11-05; P02 three times). Files are ~19–22 MB each.

- Consequence: attribution for our capture dates (from 2026-11-03, around period 8) will not exist before roughly February 2027, so a like-for-like validation of the gold delay models is impossible within the build. Day 10 can only do a labelled pattern check against the newest period; always use the newest issue of a period.
- Evidence: the product's "View files" listing as seen on 2026-11-05 (no files downloaded).

## F5. The durable subscription redelivers what was missed during a restart (2026-11-05)

The bridge was stopped at 13:26:06 UTC and restarted at 13:26:30 (24 s offline). On resubscribing, the first frames arrived in a burst at 13:26:30 carrying messages queued from 13:26:06 onwards, oldest first (25 s, 23 s, 21 s, 18 s, 15 s, 13 s old). The restart appended to the same hour file (56 frames in total).

- Consequence: short restarts (under the broker's 5-minute message expiry) lose nothing, so the bridge can be restarted for deploys. They also produce a burst of "late" events at reconnect, which freshness metrics and the watermark must tolerate.
- Not yet tested: a disconnection longer than 5 minutes (expected: data gap, logged by the bridge).
- Evidence: `results/dev/2026-11-05-durable-restart/hour=13.jsonl.gz` (compare `received_at` with each message's `msg_queue_timestamp`).

## F6. Two bridges on the same durable subscription split the feed between them (2026-11-05)

When the cloud capture started at 14:18:44 UTC while the laptop capture was still connected with the same `client-id` and subscription name, the broker **did not reject** the second connection: it load-balanced messages between the two. The laptop's rate fell from 28 frames (756 messages) in the minute before to 15 frames (391 messages) in the minute after; the VM received 14 frames in the same minute. After the laptop bridge was stopped, the VM returned to 28 frames (694 messages) per minute.

- Consequence: running two bridges is silent data loss *per archive* (each holds about half), not an error. Exactly one bridge may run; the 14:18–14:21 window is complete only when both archives are combined (silver dedupe handles the union).
- Detection: the per-minute frame count halved. The rate metrics (Day 12) should alert on a sudden drop, not only on silence.
- Evidence: laptop `data/raw/trust-local/date=2026-11-05/hour=14.jsonl` and the VM's hour-14 file (local, not committed); bridge log lines quoted above.

## F7. MinIO community images are no longer available (2026-11-05)

Docker Hub returns "object not found" for the `minio/minio` repository, `docker manifest inspect` finds neither `minio/minio:latest`, a pinned 2025 release tag, nor `quay.io/minio/minio:latest`, and MinIO's Quay repository requires authentication. Lakekeeper's own example stack has switched to a community rebuild of MinIO.

- Consequence: the spec's MinIO cannot be used as published. Replaced with SeaweedFS 4.52 (mature, Apache-2.0, signed images published 2026-11-02); RustFS 1.0.4 was the alternative. Recorded in architecture.md §6.
- Verified: Lakekeeper created the warehouse on SeaweedFS (storage validation write), and a PyIceberg round trip (create table, two appends = two snapshots, read back, drop) succeeded through Lakekeeper with remote signing.
- Lesson: check that a pinned dependency can still be pulled; "pin a version" does not protect against a registry removing it.

## F8. A real broker outage was absorbed without data loss (2026-11-05)

At 21:50:22 UTC the broker dropped the VM bridge's connection and then refused new ones ("Connection refused"). The bridge retried with exponential backoff (1, 2, 4, 8, 16, 32 s), reconnected after **71 s**, was dropped again 30 s later and reconnected after **36 s** (21:52:39). Both outages were under the broker's 5-minute expiry, so the durable subscription redelivered the queued messages; the bridge then ran for ~40 hours without a disconnection.

- Consequence: exponential backoff and the durable subscription work against a real outage. The log line `connect failed ()` carried an empty exception message, so the cause was not visible: failures are now logged with the exception type (`%r`), and retries are jittered (equal jitter) so many clients do not retry in lockstep.
- Evidence: VM bridge log, 2026-11-05 21:50–21:53 UTC (quoted above).

## F9. First full day: 851,834 messages; frames never gap more than 20 s (2026-11-07)

Friday 2026-11-06 (UTC), 24 complete hours captured on the VM: **851,834 messages, 35,572 frames, 38.6 MB compressed**. Messages per hour range from ~2k (02:00–04:00 UTC) to ~52k (08:00 UTC morning peak), with a steady ~44–49k through the day.

- **No empty frames** in the day (0 of 35,572), although the documentation allows them. At night frames arrive on the broker's 5-second batching tick; the **longest gap between frames was 20 s**. Hence "feed quiet" = no frame for **60 s** (3× the worst observed gap); the Sunday ~02:00 outage should trigger it, correctly.
- Evidence: `data/raw/trust/date=2026-11-06/` (24 hourly files; pulled from the VM, not committed); counts recomputed by reading every frame.

## F10. A separate durable subscription receives a full copy of the feed (2026-11-07)

A test bridge on the laptop used a different `client-id` and subscription name (`railstream-test`) while the VM bridge kept its own. In the same minutes both received the whole feed: VM 28 and 27 frames (686 and 668 messages), test bridge 28 and 27 frames (699 and 655 messages; the minute windows are offset by ~3 s). Compare F6, where two bridges sharing *one* subscription each got about half.

- Consequence: the VM can keep the canonical 24/7 archive while the laptop runs its own live subscription for the streaming pipeline (Day 3). A test subscription must be removed afterwards (STOMP UNSUBSCRIBE with `activemq.subscriptionName`), or it lingers on the broker.
- Evidence: VM bridge log 13:43–13:46 UTC; `results/dev/2026-11-07-separate-subscription-and-gap/`.

## F11. The broker keeps exactly the last 5 minutes; longer outages are now detected across restarts (2026-11-07)

The test bridge was stopped at 13:46:52 UTC and restarted at 13:53:12 (380 s). On restart it logged `DATA GAP: disconnected 380s since 13:46:52`. The oldest message it then received had been queued **303 s** before the reconnect; messages queued between 13:46:52 and 13:48:09 (~76 s) were never delivered. The first minute after the restart carried 166 frames (4,224 messages): the 5-minute backlog plus live traffic.

- Consequence: the 5-minute expiry from the documentation is confirmed by measurement. A first version of the bridge detected gaps only *within* one process, so a long restart (e.g. a VM reboot) would have lost data silently; it now recovers the last capture time from the files on disk at startup.
- Evidence: `results/dev/2026-11-07-separate-subscription-and-gap/restart-log.txt` and `hour=13.jsonl.gz` (compare `received_at` with `msg_queue_timestamp`).

## F12. A Kafka outage during live capture lost and duplicated nothing (2026-11-07)

The live bridge (own subscription `railstream-live`) ran 15:11:40–15:15:10 UTC publishing to `raw.trust.movement`; Redpanda was stopped at 15:12:39 and started at 15:13:20 (41 s). Capture did not pause (28 frames in the minute of the outage); the producer buffered and delivered after the restart: `sent` 2,558, `delivered` 2,558, `failed` 0. Comparing every message in the disk capture with every Kafka record: **2,558 = 2,558, identical multisets** (no loss, no duplicate), 1,359 trains each in exactly one partition with the same per-train order as on disk; partitions held 821 / 859 / 878 records.

- Consequence: publishing after the ACK keeps Kafka off the broker's critical path (ADR-005), and idempotence absorbs the producer's retries. An outage over the 5-minute delivery timeout would instead log `NOT IN KAFKA` with the window to replay.
- Also found: about 1 in 20 fresh Kafka connections to `localhost:19092` failed on Windows (`localhost` also resolves to `::1`, where nothing listens); 30/30 succeeded with `127.0.0.1`, which is now both the bootstrap and the advertised address (development.md §4).
- Evidence: `results/dev/2026-11-07-kafka-outage/` (bridge log with the stop/start times; disk-vs-Kafka comparison output).

## F13. The BST field list does not match the field names in the data (2026-11-07)

Every time-like field in a full day (2026-11-06, 851,834 messages), by message type: the wiki's wall-clock list names `orig_dep_timestamp`, which **never occurs**; activations (0001, 32,383) carry `origin_dep_timestamp` instead, and cancellations (0002) spell the original-location time `orig_loc_timestamp` (15) where movements use `original_loc_timestamp`. Further epoch fields are not on the list at all: `creation_timestamp` (0001), `reinstatement_timestamp` (0005), `coo_timestamp` (0006), `event_timestamp` (0007/0008). `tp_origin_timestamp` holds a date string (`2026-11-06`), not epoch milliseconds. No field was ever an empty string: absent optional fields are omitted.

- Consequence: the two renamed fields are treated as wall clock (same meaning as the listed names); system record times are taken as true UTC, as documented. None of these decisions can be checked before BST begins on 2027-03-28 (in November both readings give the same instant), but every gold metric uses only `actual`, `planned`, `gbtt` and `canx` timestamps, which are on the documented list. Converting all 851,834 messages raised 0 errors.
- Evidence: recomputable from `data/raw/trust/date=2026-11-06/` (not committed); the field inventory is quoted in data-sources.md (timestamps).

## F14. The Sunday-morning feed outage lasted 14 minutes and was detected (2026-11-09)

On Sunday 2026-11-08 the VM bridge stayed connected but received no frames: `FEED QUIET: connected but no frames for 60s` at 02:06:02 UTC, `feed resumed after 830s quiet` at 02:18:52 UTC. This matches the documented weekly TRUST outage around 02:00 on Sundays; the connection itself never dropped, so only the quiet-feed detector (60 s threshold, F9) could see it.

- Consequence: a ~14-minute hole every Sunday night is expected upstream, not a pipeline fault. Freshness alerts (Day 12) must not page for it, and gold metrics for that window are computed from fewer reports.
- Evidence: VM bridge log, 2026-11-08 02:06–02:19 UTC (quoted above).

## F15. The registry's JSON Schema compatibility check misses rules inside conditionals; validation needed a compiled validator (2026-11-09)

Probing Redpanda 26.1's JSON Schema compatibility check (BACKWARD) with deliberate changes to the raw contract: adding a required field at the top level or inline, narrowing an enum, and tightening a pattern in a shared definition were all reported **incompatible**, but adding a required field or narrowing an enum **inside an `if/then` branch** (the per-message-type rules) was reported **compatible**. Redpanda also has no "check against all versions" endpoint (404), and unlike Confluent it accepts adding an optional property to an open schema.

- Consequence: the raw contract keeps every field's type rules in one flat `body.properties` block (checked by the registry); the per-type branches only list required fields, and `make schemas` adds its own check that no `required` list anywhere gains members. Transitive checking loops over every version explicitly. All six probe changes are now classified correctly.
- Validation speed: `jsonschema` validated **2,589 messages/s** on one core (a day ≈ 5.5 min; two weeks of backfill ≈ 80 min per rebuild). `fastjsonschema` (the schema compiled to Python) did **52,338 messages/s** with identical verdicts on 50,000 real messages and on all known-bad cases; it now decides, and `jsonschema` only explains rejections.
- Contract result: 0 rejections in 2,061,370 messages over three days (data-sources.md, Data contracts).
- Evidence: probe commands and outputs in this session (reproducible with `uv run python -m ingest.stream.schemas --check` after editing a contract); throughput measured on the first 50,000 messages of 2026-11-06 08:00 UTC.

## F16. Bronze writes every Kafka offset exactly once; three build and catalog lessons (2026-11-09)

First run of the bronze job (Spark 4.1.4, Iceberg 1.12.0) from offset 0 while the live bridge was publishing: batch 0 wrote the 83,533-record backlog in 6.2 s (13,510 rows/s); following 1-minute batches wrote ~470–735 rows in ~1.6 s. After batch 2 the table held **84,737 rows and 84,737 distinct (kafka_partition, kafka_offset) pairs**; per partition the offsets ran from 0 to 27,818 / 28,763 / 28,153 with no gaps, matching the end offsets the job logged. The newest snapshot's summary carries `spark.sql.streaming.queryId` and `epochId` (the batch id Iceberg uses to skip a re-run batch). 84,737 messages took 3.7 MB of zstd Parquet in 4 files; the container used 705 MiB of its 1.5 GiB limit.

- Image size: the first build was 4.64 GB because pyspark is a source distribution and uv's build cache stayed in the layer (1.99 GB); `--no-cache` brought it to 1.81 GB (the uv layer to 531 MB).
- `DROP TABLE ... PURGE` fails after succeeding: Lakekeeper deletes the files itself, then refuses to sign Spark's own delete requests for the dropped table. Use plain `DROP TABLE`.
- Spark's console progress bar (carriage returns) glued log lines together and hid output from line filters; disabled for jobs.
- Evidence: bronze job log 2026-11-09 13:56–13:58 UTC; verification queries in this session (reproducible: `SELECT kafka_partition, min(kafka_offset), max(kafka_offset), count(*) FROM bronze.trust_movement GROUP BY 1`).

## F17. Killing the bronze job mid-batch three times caused no duplicates and no loss (2026-11-09)

Integration test (`make it`), each run on its own topic, table and checkpoint: 5,000 synthetic TRUST messages in, **5,000 rows and 5,000 distinct (partition, offset) pairs** out, offsets contiguous from 0 in all three partitions. Kill test: 40,000 messages processed in batches of 500; the job was **killed with SIGKILL three times** (after 4, 3 and 3 committed batches, then at a random moment inside the next batch) and restarted from the same checkpoint, then run to completion: **40,000 rows, 40,000 distinct offsets, no gaps**; 81 snapshots (80 full batches plus a final partial one), so no interrupted batch left an extra commit.

- Consequence: the spec's recovery claim holds for bronze: checkpointed offsets plus Iceberg's batch-id check make a re-run batch a no-op. The silver job (stateful dedupe) gets its own kill test on Day 13.
- A first version of the kill test used fixed 25–40 s timers and the job finished before the first kill; the test refused to pass with zero kills. Kills are now timed from the job's own committed-batch log.
- Evidence: `results/dev/2026-11-09-bronze-kill-test/pytest-output.txt`.


## F18. Planned-call corrections and durable rejects verified in silver (2026-11-16)

Three captured days yielded 1,970,245 movements and 778 correction messages; 474
matched candidate planned-call groups contained different actual times. Actual
time is therefore mutable and cannot identify a correction (ADR-008). The generated
profile and reproducible script are under `results/dev/2026-11-16-silver-profile/`
and `scripts/profile_movements.py`.

The new Spark silver scenario verified one current row across four revisions,
three stream restarts from the same checkpoint, old/repeated replay without extra
current rows, equal-time conflict visibility, and recovery from a failure between
the durable reject write and Kafka publication. The durable reject table held one
row while Kafka held two copies with the same key and failure timestamp. This is
at-least-once DLQ publication, not exactly once. Summary:
`results/dev/2026-11-16-silver-sink/summary.json`.

A reader in the original Spark session initially saw cached empty-table metadata
after the streaming session wrote rows. Explicit table refreshes fixed that and
are also used before the sink reads existing revisions/current rows.

The real bronze backlog then produced 107,222 typed movements and 4,212 other valid
TRUST messages in the silver job's first batch. Container memory at the sampled
point was 1.398 GiB / 1.5 GiB: near the cap, not a sustained-memory measurement.
Re-check batch sizing and heap before adding stateful watermark processing.
At this point watermark and CORPUS enrichment were pending; F19 records completion.

## F19. Guarded watermark preserves history; CORPUS enriches without multiplying calls (2026-11-16)

The focused state scenario ran the real Spark state operator with a synthetic
ten-second horizon and batch-clock jumps. It dropped two duplicate live reports
and evicted one state entry. Old replay, a late correction, mixed live/history,
future timestamps, checkpoint restarts and reference refresh all passed. Unknown
STANOX values remained present with null names and a false match flag. Duplicate
CORPUS keys collapsed deterministically; loading the same reference was idempotent.

The downloaded reference loaded 11,118 selected STANOX locations. Rereading the
existing bronze archive produced 102,574 current calls with 102,574 distinct keys
and 104,934 distinct revisions. All current calls in this archive matched CORPUS;
that is not a guarantee that every future location will match. Oxford, Birmingham
New Street, Euston and Paddington were observed with their real names and CRS codes.

The real backlog's watermark branch was idle because the reports were historical;
the synthetic scenario verifies live state behavior. Spark's union query reports
two source scans (222,868 input rows for 111,434 physical records), so use sink
status/route counts for business volume, not its generic input rate.

The smaller heap and disk-backed caches still sampled up to 1.462 GiB, leaving
too little headroom under the old 1.5 GiB cap. Silver now has a 2 GiB cap within
the existing Docker budget (ADR-009). No OOM or automatic restart was observed;
this is sampled development evidence, not sustained capacity proof. Larger
backlogs and abrupt-kill silver recovery remain later verification work.

Generated summaries and memory samples: `results/dev/2026-11-16-silver-state/`.

## F20. Airflow's local scheduler and task execution verified (2026-11-16)

Airflow 3.4.0 on Python 3.12 runs in the optional Compose profile using
standalone/LocalExecutor and an isolated database in the retained Postgres
service. A manual platform-check DAG completed through the actual scheduler;
database, scheduler, DAG processor and triggerer heartbeats were healthy.
Local UI login and protected DAG API access passed. Repeated bootstrap/startup
preserved metadata and existing databases. No Airflow installation on Windows.

The first attempt failed because Airflow passed `_CMD` output's trailing newline
into the PostgreSQL database name. Emitting the encoded URI without a newline
fixed it; a unit regression test covers that boundary and reserved password
characters. This brought the unit suite to 138 passing tests.

Memory after the trivial task was sampled around 1.01 GiB under a 1.5 GiB cap;
the generated setup evidence contains later timestamped samples. This does not
prove reference loads fit. All current profile caps sum to 7.5 GiB against about
7.6 GiB available, so unused services stay stopped. CORPUS download/validation/load
automation is the next step; the manual smoke DAG does not perform it.

Evidence: `results/dev/2026-11-16-airflow-setup/`; decision: ADR-010.

## F21. Real daily CORPUS refresh, retry and Spark interoperability verified (2026-11-16)

The three-task Airflow DAG downloaded and validated the actual CORPUS file,
selecting 11,133 STANOX locations. PyIceberg appended its new immutable content
version through Lakekeeper remote signing; repeated successful runs inserted
zero rows. The existing Spark reader then observed 22,251 distinct version keys:
the original 11,118 rows and the new 11,133 rows, with Oxford correctly resolved.

An early direct Compose startup omitted explicit `.env` loading, so the download
task lacked credentials. Starting through `make airflow` fixed the wiring; the
manual run recovered on retry. Failed setup history remains visible. The target
now also initializes bucket/warehouse idempotently for a fresh setup. No account
credentials were put in task results or request logs; cross-origin redirects
strip Basic Auth. The example environment file still contains public defaults.

The focused retained-table integration passed DAG import/wiring and daily cron
checks, stable STANOX selection, repeat-without-new-snapshot, immutable old
versions and refusal to load changed content without validation. Unit checks
cover malformed downloads, failed-download preservation and redirect credentials.
Airflow 3.4's SDK DAG API differs from older DagBag/timetable examples, so the
test uses the installed API and `dag.schedule`. 145 unit tests pass.

Six samples around a repeated real run reached 1.272 GiB under the 1.5 GiB cap;
no OOM or automatic restart was observed. This does not prove simultaneous
full-stack capacity or indefinitely growing-history performance. Schedule
snapshots and automated VM movement catch-up remain outside this implementation.
Evidence: `results/dev/2026-11-16-corpus-refresh/`; decision: ADR-011.

## F22. Review of Days 6–7: the CORPUS writer raced under parallel remote-signed I/O; capture now quarantines instead of stopping (2026-11-17)

Re-running the integration suite during a review of the Day 6–7 work: bronze (2) and silver (2) passed, but the CORPUS workflow scenario passed **1 of 3** runs. Failures varied (`PermissionError: Forbidden` on a manifest write, `AccessDenied` on a scan read) and came from SeaweedFS: Lakekeeper logged every signing request as authorised and refused none. The Spark writer through the same catalog and storage never failed. With PyIceberg limited to one worker thread (`PYICEBERG_MAX_WORKERS=1`) the scenario passed **8 of 8** runs (4 with an override, 4 with the built-in setting). Diagnosis: a race in parallel remote-signed requests through fsspec/s3fs; the exact component is not isolated.

- Consequence: the single reference writer (`orchestration/corpus.py`) runs single-threaded; a CORPUS load is ~11k rows once a day, so speed is irrelevant. The DAG's two retries had masked the flakiness, and one passing run on 2026-11-16 had not revealed it: flaky tests need repeated runs.
- Also changed in the review: a capture file with interior corruption is now quarantined byte for byte (`.corrupt-<id>`) and capture continues, instead of the bridge refusing to start (which on the VM would have stopped the 24/7 archive); the Spark CORPUS loader was removed so the reference table has one writer (`make load-corpus` runs the DAG's writer in the Airflow image).
- Evidence: scenario runs in this session (exit codes and `rows: 4`); Lakekeeper audit log 2026-11-17 12:12 UTC (authorisations only, no refusals).

## F23. Moving to the cloud: 12 days replayed in 10 minutes; an unbounded catch-up batch ran silver out of heap (2026-11-17)

The pipeline moved to an Always Free ARM VM (2 OCPU / 12 GB, ADR-012). The capture VM replayed its whole archive (2026-11-05 14:18 to 2026-11-17 13:44) into the processing VM's Kafka over the private network: **9,128,469 messages in 10 min 3 s, 0 failures**, while live messages flowed in parallel (`kafka sent=722 delivered=722` in the first minute). Bronze processed 200,000-row batches at ~30,000 rows/s, one per minute. Silver routed the first large batch as replay 191,703 / live 490 / future 4 / late 2.

- Silver read bronze with no per-batch limit, so after the replay one micro-batch took millions of rows; its 768 MB driver heap ran out (`OutOfMemoryError: Java heap space` in the revision-ranking window) and the container restarted. A restart re-runs the same logged batch, so it would have crashed in a loop. Fix: `streaming-max-rows-per-micro-batch` = 200,000 (soft: whole files, so ~400k with bronze's ~200k-row files) and a 1.5 GB heap in a 3 GB container. The in-flight batch then completed (1,150,670 movements, 0 restarts) and later batches stayed bounded; silver used ~1.4 GB.
- No data was lost: the crash happened before commit, and the checkpoint re-ran the batch (as in F17).
- Evidence: silver container logs 2026-11-17 13:44–14:23 UTC; replay log on the capture VM.

## F24. The archive backup is verified, idempotent, append-only and restorable (2026-11-17)

First backup of the capture VM's archive to Object Storage (ADR-013): **288 completed hours, 397.5 MiB, in 19 s** (24.7 MiB/s), followed by a one-way check: 288 matching, 0 differences. A second run copied **0 files** (counted for that run's systemd invocation only; a first count over a one-minute journal window had wrongly included the previous run). An attempt from the VM to delete `trust/date=2026-11-06/hour=08.jsonl.gz` was **refused** (Oracle answers 404 to an unauthorised caller rather than revealing the object) and the object was still listed afterwards. Restoring that hour gave a **byte-identical** file (same SHA-256) containing **52,386 messages**, the same count measured for that hour on Day 3.

- Consequence: history now survives loss of the capture VM's disk, with at most ~1 hour at risk. Live-capture gaps are not addressed by a backup; alerting and a second collector are the next items of the single-point-of-failure review.
- The two VMs are in different availability domains (capture AD-3, processing AD-1), read from the instance metadata service.
- Evidence: `journalctl -u railstream-backup` on the capture VM, 2026-11-17 14:4x–14:5x UTC; commands in this session.

## F25. A maintenance run OOM-killed the storage; the cloud stack had no restart policies; nothing was lost (2026-11-17)

The first manual run of `iceberg_maintenance` on the real tables compacted silver partitions through Spark Connect. SeaweedFS, limited to 512 MB, was **OOM-killed** (`OOMKilled=true`) while large rewritten files passed through it, and it **stayed down**: the core services (Redpanda, SeaweedFS, Postgres, Lakekeeper, Console) had no restart policy, a laptop-era assumption that `make up` is run by hand. Without storage, the maintenance task failed (`UnknownHostException: seaweedfs`), bronze and silver crash-looped (11 and 14 restarts), and the capture path was unaffected.

- Fix: `restart: unless-stopped` on every long-running service (also covers VM reboots) and SeaweedFS raised to 1.5 GB. Recovery was automatic once storage returned: bronze resumed from its checkpoint; the capture bridge reported `failed=0` throughout (its buffer absorbed the brief Redpanda recreate: sent 811 / delivered 779, then 849 / 881).
- **No loss, no duplicates:** auditing bronze afterwards, 9,284,095 rows = 9,284,095 distinct offsets, each partition contiguous from 0, so Kafka plus checkpoints carried the stream across the outage, as the spec's "storage unavailable" scenario requires.
- **Monitoring gap:** `feed_age_seconds` watches Kafka, which stayed fresh, so no alarm fired while the lakehouse was down for ~10 minutes. Lakehouse (bronze/silver) freshness is added to Day 12.
- Lessons: an always-on host needs restart policies everywhere; memory limits sized for steady streaming must be re-checked for batch jobs; freshness must be measured where data is consumed, not only where it enters.
- Evidence: container states and restart counts, Spark Connect and silver logs, and the bronze audit, 2026-11-17 16:52–17:07 UTC.

## F26. Silver scanned its whole target on every micro-batch; bounding the scans by identity fixed it (2026-11-18)

Silver restarted three times on 2026-11-18 (09:43, 15:46, 18:52): twice `OutOfMemoryError: Java heap space` while materialising the current-call MERGE, once a knock-on connection error. Batches took **133–148 s** for a 60 s trigger, so silver fell behind and batches grew. Cause: to merge ~1,000 new rows, `merge_current` read the entire `silver.trust_movement` table (7.8 M rows) to find matching calls, and both MERGEs scanned their whole targets; the join on a hash key allowed no pruning. The previous night's compaction (larger files) likely raised per-task memory.

- A first fix bounded the scans to the batch's times ±1 day. The silver state integration scenario failed: after a correction moved a call's actual time two days earlier, a replayed older report fell outside the window and was inserted as a second row. A time window is an assumption; identity is not.
- Final fix (`streaming/bounds.py`): a planned call's current row has exactly the same `planned_ts` (part of its identity), and an unresolved report's match has exactly the same `actual_ts` (its whole body is its identity). The lookup and both MERGEs filter on those ranges; Iceberg's per-file min/max skips the other files. Both silver integration scenarios passed.
- Measured on the live job after deploying: batches **~43 s** (1,256–2,678 rows), shrinking as it caught up; memory 1.9 GB of 3 GB.
- Evidence: silver container logs 2026-11-18; integration run on the processing VM (2 passed).

## F27. First nightly maintenance run: bronze and revisions files cut by three quarters (2026-11-18)

The scheduled `iceberg_maintenance` run at 03:00 UTC (17 minutes, one table at a time): bronze **792 → 188 data files** (average 0.53 → 2.23 MB; 612 rewritten into 4); silver revisions **615 → 156** (1.42 → 5.54 MB); silver current calls 276 → 234 (only calls older than 2 days are compacted, as MERGE corrections touch recent days). Bronze had grown from 191 to 792 files in about ten hours, one per one-minute micro-batch. Snapshots rose rather than fell: none was older than the 3-day retention yet, and each compaction commits one.

- Also built and verified the same day: a host watchdog (`railstream-autoheal.timer`) restarts containers Docker reports as unhealthy, tested with a container whose health check always fails (restarted within one run).
- Evidence: `results/dev/2026-11-18-iceberg-maintenance/maintenance_runs.parquet` (exported from `ops.maintenance_runs`).

## F28. The processing VM's disk filled: copy-on-write churn kept for three days; nothing lost (2026-11-23)

Found on return after five days: the 145 GB disk was 100 % full. SeaweedFS held **109 GB** while the live Iceberg data was a few GB; it had reached its auto-sized limit (115 volumes of 1 GB, 0 free) and refused writes. From **2026-11-22 18:05 UTC** Redpanda logged disk-full errors (7,357 lines) and rejected messages for 1,210 minutes; bronze (61 restarts) and silver (69) crash-looped; Lakekeeper's container filesystem was damaged (its binary "missing") and Docker's internal DNS stopped resolving service names until the Docker service was restarted. The maintenance run of 2026-11-21 had hung since then, so with one run allowed, no maintenance ran afterwards.

- **Causes:** (1) silver's MERGE used copy-on-write, rewriting today's partition file every minute (~40 GB/day); (2) snapshot expiry kept 3 days on every table, retaining those versions; (3) no log rotation on core services (Lakekeeper's log 6.3 GB); (4) Spark scratch directories from crash loops accumulated in container layers (~11 GB); (5) nothing measured disk use. Day 8 had measured file counts, not bytes written.
- **Recovery:** truncated oversized logs; stopped the streams; restarted the Docker service and recreated Lakekeeper (state in Postgres); removed the stopped stream containers and the build cache (19 GB free); restarted SeaweedFS so it could allocate volumes; expired silver snapshots older than 2 hours (`silver.trust_movement` **3,441 → 5** snapshots, 3,103 data files deleted; revisions 3,454 → 5, 1,755); a SeaweedFS vacuum reclaimed the space: **126 GB used → 22 GB**. The stuck maintenance run was marked failed. The missed window was replayed from the archive: **573,778 messages in 43 s, 0 failures**.
- **Fixes:** merge-on-read for the current-call table and per-table retention (ADR-016); compaction folds delete files in; log rotation on every service; Spark removes stale scratch directories at start; `disk_used_percent` published every minute, independently of Kafka, with an alarm at 80 %. Silver and maintenance integration scenarios passed with merge-on-read before the live job was switched.
- **Correction (found 2026-11-23 16:45 UTC, see F31):** the replay window was too narrow. It started when Redpanda began refusing writes (22 Nov 18:04), but bronze had already stopped landing rows at **20 Nov 18:15:18**, when SeaweedFS filled. Bronze's offsets are contiguous across the gap (partition 0: offset 3952605 received 20 Nov 18:15:18, offset 3952606 is a live message of 23 Nov 14:15), so **Redpanda accepted nothing from 20 Nov 18:15**, about 48 hours before it logged disk-full errors (most likely its low-disk protection refusing writes first). Bronze and its checkpoints behaved correctly; nothing was skipped. The feed-stale alarm email of 21 Nov was this. The missing 48 hours were replayed from the archive: **1,287,918 messages in 89 s, 0 failed**, ending exactly where the first replay began. With the first replay (22 Nov 18:04 - 23 Nov 14:16) the whole window is covered. Lesson: take a recovery window from the data (the last offset and row that landed), not from the first error in a log.
- **No data lost:** the capture VM kept writing the archive throughout (431 hours verified in the backup), which is the reason it is separate from the processing VM (ADR-012).
- **Detection gap:** the freshness alarms (ADR-014) should have fired once Kafka stopped accepting messages; whether the emails arrived is to be confirmed. A disk metric would have warned days earlier.
- Evidence: commands and outputs in this session (2026-11-23 14:10–14:55 UTC); capture bridge and Redpanda logs.

## F29. Gold in Iceberg works through DuckDB with a scoped key; first punctuality and hotspot results (2026-11-23)

ADR-004's spike, inside the processing VM's Docker network (DuckDB 1.5.6): reading through Lakekeeper's remote signing failed with **403**; with its own S3 key DuckDB read `silver.trust_movement` (**11,641,378 rows, 14 s**) with **0 duplicate event keys**, so silver's merge-on-read delete files are applied correctly; creating, inserting into and dropping an Iceberg table worked. dbt's standard `table` materialization failed ("can't be renamed"), and `CREATE OR REPLACE` is not implemented for DuckDB-Iceberg; a delete-and-insert transaction worked (ADR-017).

- A first version of the custom materialization reported "OK created" while the table did not exist: dbt opens a transaction and its own materializations call `adapter.commit()`; without it DuckDB rolled the work back at close. Found because the gold tests failed with "table does not exist", also in a fresh process.
- First builds: `dbt build` 9/9 passed (seed tests, two gold models, gold tests), then 5/5 on a rebuild through the atomic-swap path. `delay_hotspots` ~50 s and `punctuality_hourly` ~4 s on the 2-OCPU VM. Spark read the tables DuckDB wrote (127,363 and 976,185 rows); each rebuild added one snapshot.
- Results for Thursday 2026-11-19 (UK local date; PPM-style, at destination): GTR 2,691 arrivals 85.5 % on time, Northern 2,635 / 80.1 %, ScotRail 2,167 / 90.5 %, London Overground 1,593 / 96.0 %. Delay was gained most at York (2,983 min), Colton Junction (1,661; a junction without a passenger code), Stevenage, Southall and Birmingham New Street. No destination arrival lacked a public-timetable time that day (`arrivals_wtt_fallback` = 0).
- Evidence: dbt run output and `make sql` queries in this session (2026-11-23 15:19-15:30 UTC).

## F30. Reactionary delay heuristic v1: results, and the long-cluster limitation measured (2026-11-23)

`gold.reactionary_delay` (metrics.md §6, heuristic v1: delay events of 3+ minutes gained, clustered per location while gaps stay under 30 minutes) for Thursday 2026-11-19, UK local date:

- **8,993 clusters**, **6,097 knock-on trains**, **42,015 reactionary minutes**.
- Morning peak (clusters starting 06:00-09:59, brief Q1): York 1 cluster / 181 trains / 2,692 min; Colton Junction 1 / 39 / 708; Skelton (ACE Skelton Ouse Acres) 3 / 31 / 463; Doncaster 3 / 42 / 369; Oxenholme 2 / 1 / 213.
- **Limitation measured:** York's single cluster ran 07:21 to 22:15. A busy station almost always has another 3-minute gain within 30 minutes, so the cluster never closes and unrelated problems are merged into one "incident". Only **39 of 8,993 clusters (0.4 %) last more than 2 hours, but they hold 25.2 % of the day's reactionary minutes**, and they decide the top of the Q1 ranking.
- Proposal (v2; accepted and applied the same day, see the update below): close a cluster after a maximum length (e.g. 2 hours) and start a new one with a new primary train. The cost is that a genuinely long disruption is split into several clusters, each with its own "primary".
- dbt tests: 30/30 pass (grain uniqueness per gold model, expression tests, relationships to the operator seed, two reconciliation tests gold vs. staging, source freshness on silver). The reconciliation tests first failed on live data: silver commits between dbt reading staging and reading gold, so counts drift by a few rows. They now allow `max(10, 0.1 %)`; they still fail on any real logic error, which differs by far more.
- Scheduled (`build_gold` DAG, every 15 minutes, one run at a time): freshness check then `dbt build`, ~2.5 min per run; Airflow container peaked at ~2.0 GB of its 3 GB limit during a build.
- Evidence: `make sql` queries and Airflow runs in this session (2026-11-23 15:45-16:10 UTC).
- **Update (v2 applied, 16:34 UTC):** clusters split into 2-hour windows from their first event. 19 Nov: 9,051 clusters (was 8,993), 6,041 knock-on trains, 41,543 knock-on minutes (-1 %), longest cluster 1 h 59 min (was 14 h 54 min). Morning peak: York 2 clusters / 46 trains / 1,139 min (was 2,692), Colton Jn 413, Skelton 295, Oxenholme 213, Brough 173. The daily total barely moves (only the new primaries' gains stop counting as knock-on); what changes is when and where the minutes are counted. York stays first, so its morning really was the worst. dbt: 7/7 tests incl. the new `cluster_end - cluster_start < 2 h`.

## F31. Pattern check against official delay attribution: the totals agree, origin late starts and network-wide knock-on are missing (2026-11-23)

Task 10.3. Official file: Rail Data Marketplace historic delay attribution, railway period 2026/27 P05 (26 Jul - 22 Aug 2026, 28 days, 554,770 rows, passenger operators), downloaded by hand. Ours: gold 6-19 Nov 2026 (two full weeks). **Different dates and seasons** (summer vs. autumn leaf fall): a pattern check, not a match. Method in `scripts/attribution_check.py` (`make attribution-check`): official delay-minute rows only (`EVENT_TYPE` M), placed at `END_STANOX`, reactionary = `REACT_REASON` filled; ours = passenger columns; both per day; ranks over the official top 100 locations.

| | Official | Ours |
|---|---|---|
| Delay minutes per day, top-100 locations | 29,472 | 31,265 |
| Reactionary share of delay minutes | 66.5 % | 8.8 % |
| Rank correlation (Spearman), delay by location | | 0.328 |
| Rank correlation, reactionary by location | | 0.413 |
| Top-20 overlap, delay / reactionary | | 5 / 11 |

- **Totals agree** within 6 %: delay gained between consecutive reports measures the right amount of lateness. Through stations match closely (Birmingham New Street 1,037 vs 895 min/day, Crewe 644 vs 547, Cardiff Central 468 vs 471, Sheffield, Woking, Rugby).
- **Origin late starts are invisible to us.** London termini rank 2-4 officially but 78-90 in ours (Victoria 773 vs 92 min/day). **85-87 %** of the official delay at Victoria, Waterloo and Euston is at the train's planned origin, i.e. trains leaving late; network-wide that is **25.7 %** of attributed delay minutes. Our model gives a train's first report zero gain by definition (metrics.md §3). Proposed fix (not applied; changes a metric definition): count the first report's lateness as gained when it is a departure from the train's origin.
- **Our reactionary is a lower bound**, about an eighth of the official share. The heuristic only sees trains queuing at the same place; official attribution follows an incident's minutes along whole routes and through unit and crew links. The *where* partly agrees (11 of the top 20).
- Evidence: `results/dev/2026-11-23-attribution-check/{summary,locations}.parquet`; official-side queries in this session.
- Found on the way: gold had almost no data for 21 Nov; bronze was missing 20 Nov 18:15 - 22 Nov 18:04 UTC. Archive complete; replayed (correction in F28).

## F32. Gold builds ran out of memory: DuckDB's DELETE leaves every old row behind (2026-11-24)

From 04:45 UTC on 24 Nov every scheduled gold build failed (45 runs; last success 04:30). The Airflow container was **OOM-killed** at its 3 GB limit (`OOMKilled=true`; the kernel killed Airflow's API server, so tasks also reported "connection refused").

- **Measured, not guessed:** dbt alone in a separate container: `punctuality_hourly` 49 s / 1.0 GB, `delay_hotspots` 246 s / **3 GiB** (at the limit). Restricting the silver read to 3 days changed nothing for hotspots (203 s, 3 GiB), while `reactionary_delay`, on the same per-report model, took 21 s: the cost was in the *target table*, not in silver.
- **Cause:** `gold.delay_hotspots` held **79,402,516 rows in 600 data files and 78,254,357 in 599 delete files**, for 1,148,159 live rows. DuckDB-Iceberg's `DELETE` writes positional delete files and never drops data files, even fully deleted ones; each full rebuild (about 90 a day) added one of each, and gold had no compaction. Punctuality (19.5 M stored / 0.15 M live) and reactionary (9.9 M / 0.15 M) had the same growth. Silver's own delete files were not the cause (1,085 files but only 7,561 deleted rows).
- **Fix (ADR-018):** rebuild only the last 3 UK dates; `make gold-backfill` for history in 7-day chunks; gold compacted whole every night; no gold build in the maintenance hour. A one-off Spark compaction: hotspots **1,199 files / 1.46 GB → 2 files / 11.2 MB**, punctuality 273 → 2, reactionary 137 → 2.
- **After:** full `dbt build` **151 s, peak 1.56 GB, 31/31 tests**; hotspots 21 s; first scheduled run 3.5 min; Airflow back to 768 MB.
- The first window build failed a reconciliation test: a 0.1 % tolerance over 3 days is ~25 rows, less than the arrivals during a 9-minute build. The tests now compare only events received up to gold's newest `latest_received_at` (exact apart from one micro-batch).
- **Open:** delete files still accumulate between nightly compactions; check the last builds before 03:00 on 25 Nov (time and memory).
- **Detection gap:** 45 failed runs over 11 hours raised no alert; Airflow failures are not wired to the email topic (ADR-014 covers the feed and disk only).
- Evidence: Airflow task logs, `docker inspect`, `dmesg`, `.files` metadata queries and timed dbt runs in this session (2026-11-24 15:20-16:25 UTC).

## F33. Late starts counted; ranking match with official attribution doubles; most "delay gained" is minute-rounding noise (2026-11-25)

**Late starts (accepted 2026-11-24).** Validation before changing the metric, 23 Nov: of trains whose first report was a DEPARTURE, **28,877 were at the origin named by their activation (`0001` `sched_origin_stanox`), 675 elsewhere, 8 without an activation (97.7 %)**; first reports that were arrivals were almost never at the origin (4 of 797). So a first-report departure counts its lateness `max(0, delay)` as gained (metrics.md §3); joining activations would make it exact but needs a new silver table (not done). New hotspots column `origin_delay_min`.

- **Deploying found a Day 9 bug:** `--full-refresh` had never worked on an existing table: DuckDB-Iceberg refuses to re-create a table dropped in the same transaction ("Not implemented"). The transaction rolled back, so nothing was lost; the materialization now commits the drop first. Gold history then rebuilt with `make gold-backfill FROM=2026-10-30`: 4 chunks, **236 s**, no errors.
- A manual run collided with a scheduled build on dbt's DuckDB file lock ("Could not set lock"): pausing a DAG does not stop a run already started. The lock prevented two writers, as it should.
- **Stability after ADR-018:** 70 scheduled builds from 24 Nov 16:45 to 25 Nov 11:00, **all successful**, 32/32 tests each; median 163 s, max 284 s (morning peak, CPU shared with silver). Builds just before 03:00 took ~150 s. **Correction (F34):** no nightly gold compaction had happened yet, because the Airflow image was stale; those builds ran ~11 hours after the one-off manual compaction, so they show 11 hours of delete-file growth is harmless, not that the nightly compaction works. It first ran on 26 Nov (F34). No OOM; disk 20 %.

**Attribution pattern check re-run** (same P05 file, same 6-19 Nov window; `results/dev/2026-11-25-attribution-check-late-starts/`, before: `2026-11-23-…`):

| | Before | After |
|---|---|---|
| Rank correlation, delay by location | 0.328 | **0.639** |
| Rank correlation, reactionary by location | 0.413 | **0.764** |
| Top-20 overlap, delay / reactionary | 5 / 11 | **10 / 13** |
| Our reactionary share (official 66.5 %) | 8.8 % | 13.3 % |
| Victoria / Waterloo / Euston, min per day (official 773 / 746 / 727) | 92 / 132 / 125 | **592 / 634 / 531** |
| Top-100 delay minutes per day (official 29,472) | 31,265 | 42,275 |

- Our late-start share is 12.8 % of delay gained (official 25.7 %): not over-counted; first reports that are arrivals still gain nothing.

**Minute-rounding noise (new, not fixed).** F31's "totals agree within 6 %" held only for the top 100 locations. Network-wide, official attribution has ~70 k delay minutes per day (1,965,036 `M` minutes / 28 days, passenger operators), we count ~314 k gained per day (all trains). Distribution of changes between consecutive reports, 18 Nov: **+1 minute: 169,703 reports = 169,703 min, 56 % of all minutes gained**; +2: 37,126 min; +3 or more: 94,178 min; recovered 263,948 min. TRUST times are whole minutes, so a train running steadily about 2 minutes late reads 2, 3, 2, 3 …: each step up counts as gained and each step down as recovered. Gains of 3+ minutes alone (~94 k all trains, roughly 70 k passenger) are close to the official volume. Proposal: count delay gained only from steps of 3 minutes or more (accepted and applied the same day, below).

- Evidence: activation join and change-distribution queries, Airflow run list (JSON), dbt logs, and the two attribution-check result folders, in sessions 2026-11-24 16:25-16:41 and 2026-11-25 11:05-11:30 UTC.
- **Threshold applied (2026-11-25 11:12-11:32 UTC):** delay gained and recovered count only steps of 3+ minutes (`delay_step_threshold_min`, one variable for hotspots and reactionary); history rebuilt (4 chunks, then `dbt build` 32/32). Re-run, `results/dev/2026-11-25-attribution-check-threshold/`:

| | Original (23 Nov) | + late starts | + 3-minute threshold | Official |
|---|---|---|---|---|
| Rank correlation, delay by location | 0.328 | 0.639 | **0.711** | |
| Rank correlation, reactionary | 0.413 | 0.764 | 0.764 | |
| Top-20 overlap, delay / reactionary | 5 / 11 | 10 / 13 | **12** / 13 | |
| Reactionary share of delay | 8.8 % | 13.3 % | **52.5 %** | 66.5 % |
| Passenger delay gained per day, network | ~220 k | | **57.7 k** | ~70.2 k |
| Late-start share of delay gained | | 12.8 % | **29.0 %** | 25.7 % |
| Top-100 locations, min per day | 31,264 | 42,275 | 20,322 | 29,472 |

  The reactionary ranking is unchanged (it already used 3+ minutes); its *share* rose because the denominator lost the rounding noise. Volume moved from ~3x the official figure to 18 % under it. The remaining gap may be the 1-2-minute creep the threshold ignores by design, and the different season (summer vs. November); neither is tested.

## F34. Full audit before Day 11: every layer reconciled; two non-determinism bugs and a stale image fixed (2026-11-25/26)

Every check was run against the data, not by reading code.

| Layer | Check | Result |
|---|---|---|
| Repo | lint, 169 unit tests, CI on every commit, no tracked secrets | Pass |
| Capture | archive hours 5 Nov 14:00 - 25 Nov 11:00 | **478 of 478, 0 missing**, 0 unparseable frames |
| Backup | Object Storage vs archive | in sync (477 matching, current hour pending) |
| Kafka -> bronze | offsets per partition | **0 holes, 0 duplicate offsets**; bronze ~1 min behind the high watermark; DLQ empty |
| Archive -> bronze | messages per UTC day | distinct bronze messages = archive **on every day: 15,292,596** |
| Bronze -> silver | movement messages vs revisions, one day (18 Nov) | distinct payloads **828,254 = 828,254** revisions; no duplicate revision ids |
| Silver | quality profile, 14.4 M rows | no null actual time, location or delay; 100 % location names; nothing in the future; 0.03 % revision conflicts |
| Gold | **independent Spark recompute of 18 Nov** vs dbt/DuckDB | punctuality: 31 operators, arrivals, on-time and summed delay **identical**; hotspots and reactionary identical after the fixes below |
| Operations | DAG history, timers, containers, disk | feed-age and autoheal timers running; all containers up; disk 20 % |

**Explained, kept:**
- **2,554 duplicate rows in bronze** (same message, same receive time, two offsets): 1,783 on 17 Nov 13:41:36-13:43:59 (migration cutover) and 771 on 23 Nov 14:15:01-14:15:58 (the F28 replay ran to 14:16, live data had resumed at 14:15:01). Each is one `live` and one `replay` copy. Bronze stays the untouched raw record; silver collapses them (revision ids are idempotent). Lesson: end a replay at the first live message's receive time, not at a rounded minute.
- **62,442 fewer revisions than distinct movement messages:** the same message delivered twice by the broker with a different receive time (2,543 on 18 Nov alone); the revision id hashes event key, queue timestamp and body, so silver stores it once. 312,559 fewer current rows than revisions: corrected events (17,601 on 18 Nov).
- The 1.288 % null planned time, unresolved identity and off-route are the same 185,521 off-route rows (excluded from gold).

**Bugs found and fixed:**
1. **Non-deterministic delay changes.** 42,892 report pairs per day share a train's actual and planned time; `lag()` ordered only by those two could pick either order, so the same silver could give different gold (Spark vs DuckDB: 26 of 125,978 minutes, 12 locations). Now a total order: arrival before departure, TRUST queue time, then `event_key`. Afterwards, Spark reading silver at the build's snapshot (Iceberg time travel) and gold: **125,998 = 125,998** gained, 76,537 recovered, 30,308 late starts, 3,046 locations, **0 differing**.
2. **Primary train and its delay chosen by two separate `arg_min` calls**, which could pick different trains when two opened a cluster in the same minute. Now one ordered rule. Afterwards **9,937 clusters, 6,688 knock-on trains, 46,890 knock-on minutes, 77,914 primary minutes, 0 clusters differing**.
3. **Stale Airflow image.** `orchestration/maintenance.py` is copied into the image; after the 23-24 Nov changes only `git pull` had run, so the container (built 23 Nov 15:53) still had no gold compaction: the 25 Nov run shows `rewritten_files = 0` for gold. Rebuilt 25 Nov 15:26; the 26 Nov run compacted punctuality 308 -> 4, hotspots 317 -> 5, reactionary 304 -> 4 files. The claim in F33 that this had already happened is corrected there. New `make verify-deploy` compares every file in each running container with the checkout (development.md §11).
4. **Maintenance failure of 20 Nov** (not recorded before): Spark broadcast a join it judged small and ran out of driver memory, skipping every table that night. Broadcast joins are now disabled in maintenance sessions.

**Open:**
- **Implausible delays:** 32 reports with |delay| over 10 hours are feed artefacts: TRUST's `timetable_variation` stops at 999 (actual minus planned was 1,415 and 2,907 minutes), or a train matched to the next day's schedule (355T452Q21 ran 21 Nov against its 22 Nov plan, "999 minutes early"). One can add ~1,000 false minutes to a location. Early running is otherwise real (48,479 reports 30 min-2 h early, mostly freight). Proposal to decide: treat |delay| of 12 hours or more as a schedule mismatch and exclude those reports from delay metrics. **Applied 2026-11-26** (`implausible_delay_min: 720`, metrics.md §2); reports stay in silver; gold rebuilt.
- The capture bridge runs code from 17 Nov 13:36; the only later change removed an unused option and updated docstrings (behaviour identical). `verify-deploy` reports it stale until the bridge is rebuilt. **Rebuilt 2026-11-26 11:36:** offline ~8 s (last frame 11:37:12, reconnected 11:37:19, the durable subscription redelivered the queue); archived messages per minute 11:30-11:45 all within 670-782 (11:37: 701); `verify-deploy` ok on both VMs.
- `silver.trust_movement` holds 5,843 files after compaction (the two uncompacted recent days); confirm it levels off.
- Evidence: queries and command outputs in sessions 2026-11-25 11:35-15:30 and 2026-11-26 11:25-11:50 UTC; `ops.maintenance_runs`.
- **Silver slowing (found 2026-11-26, open):** median batch time per hour in quiet evening hours rose from 14-16 s (24 Nov) to 26-28 s (25 Nov); during busy daytime hours (gold builds, backfills, audit queries on the same 2 CPUs) 32-54 s with peaks over 400 s, and 174 s while catching up after today's backfill. Batches longer than the 1-minute trigger make silver fall behind. To investigate before Day 11.
- A side effect seen: the final build after the backfill failed `assert_hotspots_reconcile_with_delay_changes` (31/32). The test cuts off at the newest receive time gold saw, assuming silver commits within about a minute; with silver minutes behind, messages received before the cutoff were committed during the 20-minute build. The next scheduled build, with silver caught up (batches ~30 s, ~1,450 rows), passed 32/32. The test therefore also fails when silver lags.

## F35. Silver slowed because three uncompacted days of delete files were indexed by every MERGE (2026-11-26)

Median batch time in quiet evening hours rose from 14-16 s (24 Nov) to 26-28 s (25 Nov); busy hours reached 174 s per batch. Measured step by step:

1. **Spark job times** (silver's monitoring API, 145 min): `localCheckpoint` (the materialized current-call candidates, before the MERGE) 48 % of wall time, median 75 s; the MERGE statements 36 %.
2. **Scan metrics** for one 752-row batch: the revisions MERGE read **3,909,696 rows / 720 files / 396 MB**; the current-call lookup skipped 3,647 files but indexed **2,900 position delete files**.
3. **Per-phase timing** added to silver's log (each batch): typical batch 15-45 s, of which current MERGE 9-30 s (~65 %), revisions 2-8 s, read and classify 1-5 s. One batch whose actual times reached back to 21 Nov: revisions 39 s, current **188 s**.

**First hypothesis, partly wrong.** A batch is the last minute plus a few late reports, and one old report stretched the single min..max prune range over days. Fix: prune with clustered ranges (a new range after a gap over 1 hour, at most 40), keeping the guarantee that every batch value lies inside a range (unit property test). The silver integration scenarios first failed: `merge_new` prefixed the predicate with `t.`, invalid once it became a bracketed OR (fixed before deploying; both scenarios then passed, 6 min). Result: wide-range batches improved (a catch-up batch spanning 2 days: current 86 s), but typical batches did not (current median ~17.5 s vs ~18 s); a batch with one 13-minute range still spent 10.3 s in the current MERGE.

**The cause.** Files per day partition of `silver.trust_movement`: 26 Nov 565 data / 312 delete files; 25 Nov 1,532 / 1,032; 24 Nov 1,616 / 1,055; 23 Nov 26 / 511 (compacted, but its delete files remained: "dangling", pointing at rewritten data); older days 7-16 / 2-7. 312 + 1,032 + 1,055 + 511 = **2,910 = the delete files every lookup indexed**, however narrow its range. Maintenance compacted only days before the day before yesterday, so three days accumulated, ~1,000 delete files a day.

**Experiment on the live table** (same path as the nightly job): compacting 23-25 Nov rewrote 2 + 1,616 + 1,532 data files into 1 per day with **0 failed groups**; `remove-dangling-deletes` failed ("current snapshot changed": it needs no concurrent commit, so it cannot run beside silver), but `rewrite_position_delete_files` then rewrote **2,599 delete files and added 0** (all dangling). Afterwards 23-25 Nov held 1 data file and 0 delete files each. Current MERGE on single-range batches: median **~17 s -> ~12.6 s** (9.8, 9.8, 12.6, 15.3, 15.4).

**Remaining cost, explained:** ~10 s floor (today's own small files, ~60 more an hour, plus the MERGE); +5-20 s when a batch includes genuine late reports from older days (route counts unchanged before and after, 2-17 late per batch: normal feed behaviour), each needing that day's compacted file. Silver keeps up: 15-40 s per batch against the 60 s trigger.

**Fix:** nightly maintenance compacts silver current calls up to yesterday (was: up to the day before yesterday), then rewrites the compacted days' position delete files (dangling ones dropped). Maintenance records delete files before and after; `data_files` in `ops.maintenance_runs` has always counted delete files too.

- Deployed 2026-11-26 13:40: Airflow image rebuilt, `verify-deploy` ok on all containers, maintenance integration scenario passed; a real `ref.locations` run added the four new columns to `ops.maintenance_runs` (older rows null). The first nightly run with the new policy is on 27 Nov 03:00.
- Lesson: measure the phases first; a plausible cause (wide ranges) explained the outliers, not the typical batch.
- Evidence: Spark monitoring API (jobs, SQL scan metrics), silver's per-batch timing log lines (batches 7285-7338), `.files` metadata queries, compaction outputs, 2026-11-26 12:00-13:30 UTC.

## F36. Failure tests (Day 11): storage outage loses nothing, a capture outage over 5 minutes does, and the feed alarm needed fixing (2026-11-26)

Task 11.3, run against the live system.

| Scenario | How | Result |
|---|---|---|
| Malformed message -> DLQ | `test_silver_corrections_replay_and_reject_retry` (passed 2026-11-26) | rejected once into `rejected_messages` and the DLQ topic, also when the batch is retried |
| Very late events | `test_silver_watermark_and_locations` (passed 2026-11-26) | late events take the `late` route; a future-dated message cannot advance the watermark |
| Storage down | SeaweedFS stopped 13:59:22-14:04:33 | bronze failed ("Failed to close current writer") and silver too; Docker restarted both; Kafka kept receiving. Within a minute of storage returning bronze took the backlog (3,692 rows in one batch) and silver resumed the same query from its checkpoint. **Bronze 2,372 + 2,510 + 2,245 = 7,127 messages received 13:55-14:05 = archive 7,127; 0 offset holes or duplicates; silver 388,228 rows = 388,228 event keys today.** No alert (expected: the feed is healthy; see open points) |
| Feed disconnect, 9 min | bridge stopped 13:38:15-13:47:16 | reconnected by itself; bridge logged `DATA GAP: disconnected 544s … messages older than 300s expired`. By TRUST send time: 13:39-13:41 **0 messages**, 13:38 183 and 13:42 512 of ~700: **~2,800 messages lost** (13:38:15-13:42:16); 2,851 redelivered, up to 302 s late. **No alarm fired** |
| Feed disconnect, 4 min (after the alarm fix) | bridge stopped 14:09:58-14:13:58 | `reconnected after 244s; queued messages will be redelivered`, no gap warning; by send time every minute present (703-728 per minute 14:10-14:13), 2,213 messages redelivered up to 245 s late. **FIRING and OK emails received** |

- **The broker holds messages for 5 minutes, then discards them.** Any capture outage longer than that loses data that cannot be replayed. I stopped the bridge for 9 minutes having assumed the durable subscription kept everything, without checking the limit the bridge code already warns about: the lost window is a gap caused by the test. The capture VM is the system's real single point of failure; its alarm has to fire inside those 5 minutes.
- **Why the alarm did not fire:** Oracle's 1-minute series of `feed_age_seconds` read 288, 348, 466, *(no point at 13:47)*, 528: the publisher's start drifts by seconds, so one window got two samples and the next none, which reset the 3-minute pending count; and 300 s + 3 min meant ~8 minutes to alert, after data loss starts. Now `feed_age_seconds[2m].max() > 120`, trigger delay 1 minute (ADR-014 update), proven by the 4-minute test.
- **Open:** a storage or Spark outage raises no alert (the feed stays fresh) and gold failures email nobody (F32). A pipeline-lag metric (silver's newest received_at vs now) with an alarm closes both; planned for Day 12 (observability).
- Evidence: bridge logs, archive counts by TRUST send time (`msg_queue_timestamp`), `feed_age_seconds` journal and Oracle's metric data, Kafka high-water marks, bronze offset and window counts, 2026-11-26 13:38-14:20 UTC.
