# Data sources

RailStream uses Network Rail open data under the [Open Government Licence v3](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). It is an **unofficial, independent project**: no Network Rail branding, and it does not present itself as official.

| Source | Kind | Used for | Access |
|---|---|---|---|
| TRUST train movements (`TRAIN_MVT_ALL_TOC`) | Live, STOMP | The event stream: every reported arrival, departure, activation, cancellation | Network Rail data feeds account (development.md §2) |
| CORPUS | Static file, HTTP | Location names and codes for enrichment | Same account |
| SCHEDULE | Static file, HTTP | Planned timetable (later: planned-vs-actual) | Same account |
| Historic delay attribution | Static files, per railway period | One-off sanity check of the gold delay models | Rail Data Marketplace (development.md §3); ~3 months behind (findings F4) |

Facts about the TRUST feed below are from these Open Rail Data wiki pages (read 2026-11-05; the wiki blocks automated fetching, so they were read in a browser) unless marked as **observed**, meaning seen in our own capture (findings F1–F3):
[Train Movements](https://wiki.openraildata.com/index.php/Train_Movements) (edited 2025-04-23),
[Train Movement](https://wiki.openraildata.com/index.php/Train_Movement) (edited 2023-11-17),
[Durable Subscription](https://wiki.openraildata.com/index.php/Durable_Subscription) (edited 2024-11-19),
[TOC Codes](https://wiki.openraildata.com/index.php?title=TOC_Codes) (edited 2026-10-05). Wiki content is CC BY-SA; anything we derive from it (e.g. the operator lookup) carries attribution.

## TRUST train movements

TRUST is Network Rail's internal train-reporting system. It reports events that have happened; it makes no predictions. Not every location generates a report, only TRUST timing points.

### Delivery

- One topic: `TRAIN_MVT_ALL_TOC` (the old per-operator topics are historical).
- Each STOMP frame body is a **JSON array** of zero or more messages, sent every 5 s or when 32 messages are waiting. **An empty array is legitimate.** Observed: ≈27 messages per frame at evening peak (F1).
- **Weekly planned outage:** about 5 minutes, normally between 02:00 and 02:15 early on Sunday. Missed messages are sent when the feed resumes, so a short gap followed by a burst of late events is expected, not an incident.

### Durable subscription (what the bridge must do)

- CONNECT with a `client-id` header (the account email); SUBSCRIBE with an `activemq.subscriptionName` header, unique per feed (e.g. `railstream-trust`); **ACK every message**. The broker then queues messages while we are disconnected and delivers them on reconnect.
- **Queued messages expire after 5 minutes.** A durable subscription covers restarts and network blips, not long outages (e.g. a sleeping laptop). So: the bridge restarts automatically; any disconnection longer than 5 minutes is recorded as a **data gap** with start and end times; completeness and freshness metrics account for gaps.
- An unacknowledged or lost ACK means redelivery, so delivery is **at least once**: silver must dedupe.

### Message types

Each message is `{"header": {...}, "body": {...}}`; `header.msg_type` says which kind.

| `msg_type` | Meaning | Use in RailStream |
|---|---|---|
| 0001 | Train activation (a scheduled train is "called" into running) | Links `train_id` to its schedule (`train_uid`, origin, departure) |
| 0002 | Train cancellation | Cancellations (counted separately from lateness) |
| **0003** | **Train movement** (arrival/departure at a timing point) | **Core fact table** |
| 0004 | Unidentified train | Not used in production |
| 0005 | Train reinstatement | Undo of a cancellation |
| 0006 | Change of origin | Train starts somewhere other than planned |
| 0007 | Change of identity | `train_id` changes for the same train |
| 0008 | Change of location | Location of a report corrected |

Observed in one peak minute: 0003, 0001 and 0002 only (F1). The other types are rarer; the contract must accept all of them.

### Movement (0003) fields used

All values arrive as **strings**, including numbers and booleans (observed). Several fields are optional ("may not be present"): `gbtt_timestamp`, `original_loc_stanox`, `original_loc_timestamp`, `current_train_id`, `next_report_run_time`, `reporting_stanox`, `train_file_address`, `platform`, `auto_expected`, `direction_ind`, `route`, `next_report_stanox`, `line_ind`. The contract marks them optional.

Header fields: `original_data_source` is one of `GPS`, `SDR`, `SMART`, `TOPS`, `TRUST DA` (observed in one minute: SMART 707, GPS 24, SDR 23). `TRUST DA` is delay-attribution input, a likely source of after-the-fact amendments.

| Field | Example | Meaning |
|---|---|---|
| `train_id` | `872L09MX03` | 10-character TRUST id of this running of the train |
| `event_type` | `DEPARTURE` | `ARRIVAL` or `DEPARTURE` |
| `planned_event_type` | `DEPARTURE` | `ARRIVAL`, `DEPARTURE` or `DESTINATION` (end of journey; observed 27 of 754) |
| `loc_stanox` | `87261` | Location (STANOX code) → CORPUS for the name. Always set in our sample, even though `reporting_stanox` was `00000` (meaning "see `original_loc_stanox`") on 403 of 754 |
| `actual_timestamp` | `1793731860000` | When it happened (see timestamps) |
| `planned_timestamp` | `1793731980000` | **Working**-timetable time (includes passing points); empty on off-route reports |
| `gbtt_timestamp` | | **Passenger** (public) timetable time; only at passenger stops (populated on 477 of 754) |
| `timetable_variation` | `2` | Whole minutes early/late; `0` on off-route reports |
| `variation_status` | `EARLY` | `EARLY`, `ON TIME`, `LATE`, `OFF ROUTE`. **Off-route reports must be excluded from punctuality** (they would count as on time) |
| `delay_monitoring_point` | `true` | Whether the location is a delay monitoring point |
| `toc_id` | `84` | Operator **sector code** (see Operators below) |
| `event_source` | `AUTOMATIC` | `AUTOMATIC` (from SMART, i.e. signalling) or `MANUAL` (from TOPS / TRUST SDR) |
| `correction_ind` | `false` | `true` when this message corrects an earlier report (none in our minute) |
| `route` | `1` | Exit route; `F` means the train failed to stop |
| `next_report_stanox` | `87169` | Next timing point expected |
| `train_terminated` | `false` | Last report of the journey |
| `platform`, `line_ind`, `direction_ind` | `" 7"`, `F`, `DOWN` | Infrastructure detail (note the padded platform) |

### Timestamps: the BST trap

All timestamps are milliseconds since the Unix epoch, **but during British Summer Time the following fields are one hour in the future**: `gbtt_timestamp`, `planned_timestamp`, `actual_timestamp`, `canx_timestamp`, `dep_timestamp`, `orig_dep_timestamp`, `original_loc_timestamp`. In effect they hold UK local wall-clock time encoded as if it were UTC.

Parsing rule for RailStream:
1. Read the epoch value as a naive wall-clock time (as if UTC).
2. Interpret that wall-clock time in `Europe/London`.
3. Convert to UTC for storage.

Edge case: at the autumn change (last Sunday of October) 01:00–02:00 local happens twice, so a wall-clock time in that hour is **ambiguous**. The rule for it must be chosen and tested (the BST unit tests). `msg_queue_timestamp` is not on the affected list; to be confirmed against a summer capture.

Observed in November (UK time = UTC), the sample decodes correctly either way (F2), so the rule cannot be validated on live data until 2027-03-28.

**Decisions (implemented in `trust/timestamps.py`, tests in `tests/unit/test_timestamps.py`):**
- Autumn repeated hour: of the two UTC candidates, take the one **closest to `msg_queue_timestamp`** (true UTC; a report is queued seconds to minutes after the event). No reference: the first occurrence (BST).
- Spring gap (a wall-clock time that never existed): read as GMT, and flagged as `nonexistent` so it can be counted.
- Field names as they occur in the data (findings F13): `origin_dep_timestamp` (0001) is the wiki's `orig_dep_timestamp`, which never occurs; `orig_loc_timestamp` (0002) is 0002's spelling of `original_loc_timestamp`. Both are treated as wall clock. Other epoch fields (`creation_timestamp`, `reinstatement_timestamp`, `coo_timestamp`, `event_timestamp`) are system record times, taken as true UTC. `tp_origin_timestamp` is a **date** string (`YYYY-MM-DD`), not epoch milliseconds.
- Absent optional fields are **omitted** from the body, not sent as empty strings (0 empty values in a day of 851,834 messages); the parser accepts both.

### Data contracts

`contracts/*.schema.json` (JSON Schema draft-07), registered by `make schemas`, validated by `trust/contract.py`:

| Contract | Subject | Describes |
|---|---|---|
| `raw.trust.movement` | `raw.trust.movement-value` | One TRUST message as received, all message types |
| `dlq.trust.movement` | `dlq.trust.movement-value` | A rejected record: reason, failing path and rule, source topic/partition/offset, original payload |
| `silver.trust_movement` | `silver.trust_movement` | One cleaned movement row (typed, UTC, signed delay); the target of the Day 6 silver job |

How the raw rules were set: from two profiled days (2026-11-06 Friday, 2026-11-08 Sunday; 1.3 M messages) cross-checked against the wiki's optional fields. Required = present on 100% of messages of that type. Shapes: epoch timestamps 13 digits; STANOX 5 digits; `toc_id`/`division_code` 2 digits; `train_id` 10 of `[0-9A-Z]`; booleans the strings `"true"`/`"false"`; enums for the fields the pipeline branches on (`event_type`, `planned_event_type`, `event_source`, `variation_status`, `canx_type`). Invariant: `planned_timestamp` and `reporting_stanox` are absent exactly when `offroute_ind` is `"true"`, so they are required on route. Watch-outs for silver: `timetable_variation` is never negative (the sign is in `variation_status`); `platform` and `train_uid` can be space-padded.

Result: **0 rejections in 2,061,370 messages** over 2026-11-06 to 11-08, including the day not used to set the rules (11-07). The schema is open (unknown upstream fields are allowed) so a new feed field never dead-letters messages; strict where the pipeline depends on a field.

### Correctness properties that shape the pipeline

- **Rounding:** TRUST reports at 30-second intervals and rounds to whole minutes; "1 minute late" may be 31 seconds. Small negative lags (event time after receive time) are expected.
- **Timestamps after receipt:** 43 of 754 movements were stamped more than 60 s after we received them (up to 219 s), **all AUTOMATIC from SMART** (F2). Working hypothesis: automatic reports are triggered when a train enters a signalling berth and stamped with the berth's SMART time offset, i.e. the expected platform time. The quality rule is therefore "not more than X minutes in the future", with X from a full day's distribution.
- **Late and amended data:** TRUST can be updated "after the fact". A manually input delay can overwrite an earlier automatic report, **possibly days later**. "TRUST should always be historically correct." So the late-data policy must handle corrections, not only late arrivals, and silver cannot treat the first report as final.
- **Duplicates:** at-least-once delivery from STOMP plus feed resends after outages means the same event can arrive twice; silver dedupes on a natural key.

### Operators (`toc_id`)

`toc_id` (and `division_code`) is the TRUST **sector code** from the wiki's TOC Codes page. Examples from our minute: `88` Govia Thameslink Railway, `80` Southeastern, `30` London Overground, `23` Northern, `60` ScotRail, `84` South Western Railway, `25` Great Western Railway, `71` Transport for Wales.

- `88` is listed for both "Thameslink/Great Northern" and "Southern"; all are run by Govia Thameslink Railway, so RailStream reports them as one operator.
- SCHEDULE uses a different code (ATOC, e.g. `SW`), so the lookup keeps both.
- The lookup table (Day 6) is derived from the wiki and carries a CC BY-SA attribution.

## CORPUS

Location reference data: one JSON object `{"TIPLOCDATA": [...]}`. Observed (F3): 55,981 rows; 11,118 distinct STANOX, 356 of which appear on more than one row.

| Field | Example | Meaning |
|---|---|---|
| `STANOX` | `72410` | Code used by TRUST (`loc_stanox`); the join key |
| `TIPLOC` | `EUSTON` | Code used by SCHEDULE |
| `3ALPHA` | `EUS` | Passenger station code (blank for junctions, yards) |
| `NLC` | `144400` | National Location Code |
| `NLCDESC` | `EUSTON LONDON` | Human-readable name |

Enrichment must pick exactly one row per STANOX (deterministic rule) and is tested to preserve row counts.
