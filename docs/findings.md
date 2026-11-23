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
