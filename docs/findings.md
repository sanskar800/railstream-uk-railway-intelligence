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
- The negative values need explaining before the "no future timestamps" quality check is written (candidates: manual entries, 47 of the 754 were `event_source = MANUAL`; minute-rounded timestamps; local clock).
- Open: in November UK time equals UTC, so this sample cannot show whether TRUST timestamps are UTC or UK local time. To be confirmed from the documentation and tested for the BST changeover.
- Evidence: as F1.

## F3. CORPUS joins cleanly on STANOX but is not unique on it (2026-11-03)

`CORPUSExtract.json.gz` (download of 2026-11-03; file dated 2026-10-02): **55,981 rows**, 11,621 with a STANOX, **11,118 distinct STANOX, of which 356 map to more than one row**. All 757 location-bearing messages in F1 matched a STANOX.

- Consequence: a naive join on STANOX would duplicate movement events. Enrichment needs a deterministic choice of one row per STANOX and a test that the join preserves row count.
- Locations include junctions and freight points without a 3-letter code (e.g. Copyhold Junction); these matter for delay hotspots.
- Evidence: CORPUS is re-downloadable (development.md §2); not committed.
