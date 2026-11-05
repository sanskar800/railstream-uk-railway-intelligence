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
