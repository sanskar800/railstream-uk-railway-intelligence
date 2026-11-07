# Findings

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

